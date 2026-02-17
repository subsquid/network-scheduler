use std::{collections::BTreeMap, sync::Arc};

use anyhow::Context;
use itertools::Itertools;

use crate::{
    cli, clickhouse,
    scheduling::{self, WeightedChunk},
    storage,
    types::{self, FbVersion},
    upload, weight,
};

pub struct Controller {
    config: cli::Config,
}

impl Controller {
    pub fn new(config: cli::Config) -> Self {
        Self { config }
    }

    pub async fn load_workers(
        self,
        db: &clickhouse::ClickhouseClient,
    ) -> anyhow::Result<WithWorkers> {
        let workers = db
            .get_active_workers(
                self.config.worker_inactive_timeout,
                self.config.worker_stale_bytes,
                &self.config.min_supported_worker_version,
            )
            .await
            .context("Can't read active workers from ClickHouse")?;

        tracing::info!("Found {} workers", workers.len());

        Ok(WithWorkers {
            config: self.config,
            workers,
        })
    }

    /// Load workers from a static configuration (CLI mode)
    pub fn load_workers_from_config(
        self,
        workers: Vec<types::Worker>,
    ) -> WithWorkers {
        tracing::info!("Loaded {} workers from config", workers.len());

        WithWorkers {
            config: self.config,
            workers,
        }
    }
}

pub struct WithWorkers {
    config: cli::Config,
    workers: Vec<types::Worker>,
}

fn dataset_names(config: &cli::Config) -> Vec<Arc<String>> {
    config
        .datasets
        .keys()
        .map(|bucket| Arc::<String>::from(format!("s3://{bucket}")))
        .collect_vec()
}

impl WithWorkers {
    pub async fn load_known_chunks(
        self,
        db: &clickhouse::ClickhouseClient,
    ) -> anyhow::Result<WithChunks> {
        let datasets = dataset_names(&self.config);

        tracing::info!("Reading already known chunks from ClickHouse");

        let chunks = db
            .get_existing_chunks(datasets.iter().map(|d| d.as_str()))
            .await
            .context("Can't read existing chunks from ClickHouse")?;

        let last_chunks = chunks
            .iter()
            .map(|(dataset, chunks)| {
                debug_assert!(chunks.iter().is_sorted_by_key(|c| &c.id));
                let last_chunk_id = &chunks.last().expect("Chunks should not be empty").id;
                (dataset.clone(), last_chunk_id)
            })
            .collect::<BTreeMap<_, _>>();
        tracing::debug!("Loaded known chunks up to {:#?}", last_chunks);

        Ok(WithChunks {
            config: self.config,
            workers: self.workers,
            datasets,
            chunks,
        })
    }

    /// Load known chunks from a static configuration (CLI mode)
    pub fn load_known_chunks_from_config(
        self,
        chunks: BTreeMap<Arc<String>, Vec<types::Chunk>>,
    ) -> anyhow::Result<WithChunks> {
        let datasets = dataset_names(&self.config);

        let last_chunks = chunks
            .iter()
            .map(|(dataset, chunks)| {
                debug_assert!(chunks.iter().is_sorted_by_key(|c| &c.id));
                let last_chunk_id = &chunks.last().expect("Chunks should not be empty").id;
                (dataset.clone(), last_chunk_id)
            })
            .collect::<BTreeMap<_, _>>();
        tracing::debug!("Loaded known chunks up to {:#?}", last_chunks);

        Ok(WithChunks {
            config: self.config,
            workers: self.workers,
            datasets,
            chunks,
        })
    }

    pub fn _without_known_chunks(self) -> WithChunks {
        WithChunks {
            datasets: dataset_names(&self.config),
            config: self.config,
            workers: self.workers,
            chunks: BTreeMap::new(),
        }
    }
}

pub struct WithChunks {
    config: cli::Config,
    workers: Vec<types::Worker>,
    datasets: Vec<Arc<String>>,
    chunks: BTreeMap<Arc<String>, Vec<types::Chunk>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum CacheAccess {
    _ReadOnly,
    ReadWrite,
}

impl WithChunks {
    pub async fn load_new_chunks(
        mut self,
        db: Option<&clickhouse::ClickhouseClient>,
        datasets_storage: &storage::S3Storage,
        access: CacheAccess,
    ) -> anyhow::Result<Self> {
        tracing::info!("Reading new chunks from storage");

        let last_chunks = self
            .datasets
            .iter()
            .map(|dataset| {
                let last = self.chunks.get(dataset).and_then(|chunks| chunks.last());
                (dataset.clone(), last)
            })
            .collect::<BTreeMap<_, _>>();

        let new_chunks = datasets_storage
            .load_newer_chunks(last_chunks, self.config.concurrent_dataset_downloads)
            .await
            .context("Can't load datasets")?;

        if let (Some(db), CacheAccess::ReadWrite) = (db, access) {
            tracing::info!(
                "Storing {} new chunks in ClickHouse",
                new_chunks
                    .values()
                    .map(|chunks| chunks.len())
                    .sum::<usize>()
            );
            db.store_new_chunks(new_chunks.values().flat_map(|v| v.iter()).cloned())
                .await
                .context("Can't store new chunks in ClickHouse")?;
        }

        for (dataset, chunks) in new_chunks {
            if let Some(known_chunks) = self.chunks.get_mut(&dataset) {
                known_chunks.extend(chunks);
            } else {
                self.chunks.insert(dataset, chunks);
            }
        }

        Ok(self)
    }

    pub fn weight_chunks(self) -> WithWeightedChunks {
        let datasets_num = self.chunks.len();

        let mut datasets = Vec::with_capacity(datasets_num);
        let mut flat_chunks = Vec::new();
        for (dataset_name, mut chunks) in self.chunks {
            // Chunks order should be deterministic.
            // Here they are sorted by the dataset+chunk_id pair
            debug_assert!(chunks.is_sorted_by_key(|c| &c.id));

            let height = chunks.last().map(|c| *c.blocks.end());
            datasets.push(types::Dataset {
                id: dataset_name.to_string(),
                height,
            });

            flat_chunks.append(&mut chunks);
        }

        tracing::info!(
            "Loaded {} datasets with {} chunks in total",
            datasets_num,
            flat_chunks.len()
        );

        let (weighted_chunks, filtered_chunks) =
            weight::weight_chunks(flat_chunks, &self.config.datasets);

        WithWeightedChunks {
            config: self.config,
            workers: self.workers,
            datasets,
            chunks: filtered_chunks,
            weighted_chunks,
        }
    }
}

pub struct WithWeightedChunks {
    config: cli::Config,
    workers: Vec<types::Worker>,
    datasets: Vec<types::Dataset>,
    chunks: Vec<types::Chunk>,
    weighted_chunks: Vec<WeightedChunk>,
}

impl WithWeightedChunks {
    pub fn schedule(self) -> anyhow::Result<WithAssignment> {
        let assignment = scheduling::schedule(
            &self.weighted_chunks,
            &self.workers,
            scheduling::SchedulingConfig {
                worker_capacity: self.config.worker_storage_bytes,
                saturation: self.config.saturation,
                min_replication: self.config.min_replication,
                ignore_reliability: self.config.ignore_reliability,
            },
        )
        .context("Can't schedule chunks")?;

        assignment.log_stats(&self.chunks, &self.config, &self.workers);

        Ok(WithAssignment {
            config: self.config,
            workers: self.workers,
            datasets: self.datasets,
            chunks: self.chunks,
            assignment,
        })
    }
}

pub struct WithAssignment {
    config: cli::Config,
    workers: Vec<types::Worker>,
    datasets: Vec<types::Dataset>,
    chunks: Vec<types::Chunk>,
    assignment: types::Assignment,
}

impl WithAssignment {
    pub fn serialize_assignment(&self) -> SerializedAssignment {
        tracing::info!("Serializing assignment");
        let fb_v0 =
            self.assignment
                .encode_fb(&self.chunks, &self.config, &self.workers, FbVersion::V0);
        let fb_v1 =
            self.assignment
                .encode_fb(&self.chunks, &self.config, &self.workers, FbVersion::V1);
        tracing::info!("Serialized assignment v0 size: {} bytes", fb_v0.len());
        tracing::info!("Serialized assignment v1 size: {} bytes", fb_v1.len());

        SerializedAssignment { fb_v0, fb_v1 }
    }

    pub fn _save_to_file(&self, filename: &str) -> anyhow::Result<()> {
        let serialized =
            self.assignment
                .encode_fb(&self.chunks, &self.config, &self.workers, FbVersion::V1);
        std::fs::write(filename, &serialized).context("Can't write assignment to file")
    }

    pub async fn upload_metadata(
        self,
        uploader: &upload::Uploader,
        metrics: String,
    ) -> anyhow::Result<()> {
        tracing::info!("Uploading metadata");
        uploader.upload_status(self.workers, self.datasets).await?;

        uploader
            .upload_metrics(metrics)
            .await
            .context("Can't upload metrics")?;

        Ok(())
    }
}

pub struct SerializedAssignment {
    fb_v0: Vec<u8>,
    fb_v1: Vec<u8>,
}

impl SerializedAssignment {
    pub async fn upload(self, uploader: &upload::Uploader) -> anyhow::Result<()> {
        tracing::info!("Uploading assignment");
        let url: String = uploader.upload_assignment(self.fb_v0, self.fb_v1).await?;
        tracing::info!("Assignment uploaded to {url}");
        Ok(())
    }
}
