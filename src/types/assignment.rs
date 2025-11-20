use std::collections::{BTreeMap, HashMap};

use libp2p_identity::PeerId;

use crate::{cli, types::WorkerIndex};

use super::{Chunk, ChunkIndex, Worker, WorkerStatus};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assignment {
    // chunk indexes are sorted
    pub worker_chunks: BTreeMap<PeerId, Vec<ChunkIndex>>,
    pub replication_by_weight: BTreeMap<u16, u16>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum FbVersion {
    #[default]
    V0,
    V1,
}

impl Assignment {
    pub fn log_stats(&self, chunks: &[Chunk], config: &cli::Config, workers: &[Worker]) {
        let statuses: HashMap<_, _> = workers.iter().map(|w: &Worker| (w.id, w.status)).collect();
        let min = self
            .worker_chunks
            .iter()
            .map(|(worker_id, chunk_indexes)| {
                let bytes = chunk_indexes
                    .iter()
                    .map(|i| chunks[*i as usize].size as u64)
                    .sum::<u64>();
                let num_chunks = chunk_indexes.len();

                let status_str = match statuses[worker_id] {
                    WorkerStatus::Online => String::new(),
                    status => format!(" ({status})"),
                };

                tracing::debug!(
                    "Worker {}: {}GB, {} chunks{}",
                    worker_id,
                    bytes / (1 << 30),
                    num_chunks,
                    status_str,
                );

                crate::metrics::report_worker_stats(*worker_id, num_chunks, bytes);

                bytes
            })
            .min();

        let min_bytes_per_worker = config.worker_stale_bytes;
        if let Some(min) = min {
            if min < min_bytes_per_worker {
                tracing::warn!(
                    "Some workers have less than the minimum required storage: {min} < {min_bytes_per_worker}"
                );
                crate::metrics::failure("min_assignment");
            }
        }

        crate::metrics::report_replication_factors(config.datasets.iter().map(|(ds, segments)| {
            (
                format!("s3://{ds}"),
                segments.iter().filter_map(|seg| {
                    self.replication_by_weight
                        .get(&seg.weight)
                        .map(|r| (seg.from, *r))
                }),
            )
        }));
    }

    pub fn encode_fb(
        &self,
        chunks: &[Chunk],
        config: &cli::Config,
        workers: &[Worker],
        version: FbVersion,
    ) -> Vec<u8> {
        let _timer = crate::metrics::Timer::new("serialize_assignment");

        let assigned_worker_ids = self.worker_ids_for_chunk();
        assert_eq!(
            assigned_worker_ids.len(),
            chunks.len(),
            "each chunk must have at least one worker assigned"
        );

        let mut assignment_builder =
            sqd_assignments::AssignmentBuilder::new(&config.cloudflare_storage_secret)
                .check_continuity(config.strict_continuity_check);

        let mut prev_dataset = None;
        for (chunk, worker_ids) in chunks.iter().zip(assigned_worker_ids) {
            if prev_dataset
                .as_ref()
                .is_some_and(|prev| prev != &chunk.dataset)
            {
                tracing::trace!("Finished serializing dataset {}", prev_dataset.unwrap());
                assignment_builder.finish_dataset();
            }
            prev_dataset = Some(chunk.dataset.clone());

            let download_url = format!("https://{}.{}", chunk.bucket(), config.storage_domain);
            let mut builder = assignment_builder
                .new_chunk()
                .id(&chunk.id)
                .dataset_id(&chunk.dataset)
                .block_range(chunk.blocks.clone())
                .size(chunk.size)
                .dataset_base_url(&download_url)
                .worker_indexes(&worker_ids)
                .files(&chunk.files);
            if let Some(summary) = chunk.summary.as_ref() {
                builder = builder
                    .last_block_hash(&summary.last_block_hash)
                    .last_block_timestamp(summary.last_block_timestamp);
            }
            if let Err(e) = builder.finish() {
                if config.strict_continuity_check {
                    panic!("Failed to serialize chunk ({}): {:?}", e, chunk);
                } else {
                    tracing::warn!("Failed to serialize chunk ({}): {:?}", e, chunk);
                }
            }
        }
        tracing::trace!("Finished serializing dataset {}", prev_dataset.unwrap());
        assignment_builder.finish_dataset();

        for worker in workers {
            let status = match worker.status {
                WorkerStatus::Online => sqd_assignments::WorkerStatus::Ok,
                WorkerStatus::Offline => sqd_assignments::WorkerStatus::Unreliable,
                WorkerStatus::UnsupportedVersion => {
                    sqd_assignments::WorkerStatus::UnsupportedVersion
                }
                WorkerStatus::Stale => sqd_assignments::WorkerStatus::Unreliable,
            };
            tracing::trace!("Serializing worker {}", worker.id);
            let indexes = match version {
                FbVersion::V0 => &*self.worker_chunks[&worker.id],
                FbVersion::V1 => &[],
            };
            assignment_builder.add_worker(worker.id, status, indexes);
        }

        assignment_builder.finish()
    }

    fn worker_ids_for_chunk(&self) -> Vec<Vec<WorkerIndex>> {
        let _timer = crate::metrics::Timer::new("serialize_assignment:worker_ids_for_chunk");
        let mut result = Vec::with_capacity(1000);
        for (worker_index, indexes) in self.worker_chunks.values().enumerate() {
            for &index in indexes {
                if index as usize >= result.len() {
                    result.resize(index as usize + 1, Vec::new());
                }
                result[index as usize].push(worker_index as u16);
            }
        }
        result
    }
}
