//! Multistep (MVCC) scheduling entry point (requires the `mvcc-chunks` feature): runs one
//! scheduling cycle against Postgres instead of the ordinary `Controller` pipeline.
//!
//! Only [`SchedulerStorage::run_scheduling_cycle`] runs — no confirmation or visibility cycle yet
//! (see `docs/README.md`). The resulting assignment is only computed and logged, not published.

use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
    time::SystemTime,
};

use anyhow::Context;

use crate::{
    cli, dataset_data_storage, metrics,
    scheduler_storage::{
        NewChunk, SchedulerStorage, Tick, algorithm::MultistepAlgorithm, postgres::PostgresStorage,
    },
    types::{Chunk, Dataset, DatasetSchema, DatasetWatermark, Worker},
};

/// Runs one multistep scheduling cycle; the resulting `WorkerAssignment` is only computed and
/// logged, not published. `workers` is the active set the caller already loaded for the run mode.
pub async fn run(
    args: &cli::Args,
    config: &cli::Config,
    workers: Vec<Worker>,
) -> anyhow::Result<()> {
    let database_url = args
        .database_url
        .as_deref()
        .context("--database-url is required with --multistep-scheduler")?;

    tracing::info!("Multistep: scheduling for {} workers", workers.len());

    let mut storage = PostgresStorage::connect(database_url).context("connect to Postgres")?;
    storage.migrate().context("run Postgres migrations")?;

    let watermarks = bootstrap_datasets(&mut storage, config)?;

    let s3_storage = dataset_data_storage::S3Storage::new(&args.s3.config().await);
    let discovered = s3_storage
        .load_newer_chunks(
            watermarks,
            config.concurrent_dataset_downloads,
            config.dataset_load_timeout,
        )
        .await
        .context("load datasets from S3")?;
    tracing::info!(
        "Multistep: discovered {} new chunks across {} datasets",
        discovered.values().map(Vec::len).sum::<usize>(),
        discovered.len(),
    );

    register_chunks(&mut storage, discovered)?;

    let now = now_ticks();
    storage
        .update_worker_set(&workers, now, args.multistep_worker_gc.as_secs())
        .context("update worker set")?;

    let algorithm = MultistepAlgorithm::new(config.datasets.clone());
    let multistep_config = crate::multistep_scheduler::SchedulingConfig {
        worker_capacity: config.worker_storage_bytes,
        saturation: config.saturation,
        min_replication: config.min_replication,
        ignore_reliability: config.ignore_reliability,
    };
    let (assignment, _) = {
        let _timer = metrics::Timer::new("multistep:schedule");
        storage
            .run_scheduling_cycle(
                &algorithm,
                &multistep_config,
                now,
                args.multistep_drain_window.as_secs(),
            )
            .context("run multistep scheduling cycle")?
    };

    tracing::info!(
        "Multistep scheduling cycle done: assignment {}, {} chunks placed, replication_by_weight={:?}",
        assignment.id,
        assignment.chunk_workers.len(),
        assignment.replication_by_weight,
    );

    Ok(())
}

/// Ensure every configured dataset exists, returning the discovery watermark for every known
/// dataset.
///
/// FIXME: temporary cold-start bootstrap — a missing dataset is created on the fly with an empty
/// schema. Real provisioning (dataset + its read schema) should happen out of band.
fn bootstrap_datasets(
    storage: &mut PostgresStorage,
    config: &cli::Config,
) -> anyhow::Result<Vec<DatasetWatermark>> {
    let _timer = metrics::Timer::new("multistep:bootstrap");
    let mut watermarks = storage.datasets_with_last_chunk()?;
    let missing: Vec<String> = {
        let existing: HashSet<&str> = watermarks.iter().map(|w| w.dataset.id.as_str()).collect();
        config
            .datasets
            .keys()
            .map(|bucket| format!("s3://{bucket}"))
            .filter(|name| !existing.contains(name.as_str()))
            .collect()
    };
    for name in missing {
        storage
            .insert_new_datasets(vec![(name.clone(), DatasetSchema::default())])
            .with_context(|| format!("bootstrap dataset {name}"))?;
        watermarks.push(DatasetWatermark {
            dataset: Dataset {
                id: Arc::new(name),
                height: None,
            },
            last_chunk_id: None,
        });
    }
    Ok(watermarks)
}

/// Insert the freshly discovered chunks (all genuinely new — discovery resumes past what Postgres
/// already holds) and register them for scheduling.
fn register_chunks(
    storage: &mut PostgresStorage,
    discovered: BTreeMap<Arc<String>, Vec<Chunk>>,
) -> anyhow::Result<()> {
    let _timer = metrics::Timer::new("multistep:register_chunks");
    for (dataset, chunks) in discovered {
        if chunks.is_empty() {
            continue;
        }
        // Move fields out instead of cloning; a dataset can hold millions of chunks.
        let new_chunks: Vec<NewChunk> = chunks
            .into_iter()
            .map(|c| NewChunk {
                dataset: c.dataset,
                id: c.id,
                size: c.size,
                blocks: c.blocks,
                schema_id: None,
                tables_present: None,
            })
            .collect();
        storage
            .insert_new_chunks(new_chunks)
            .with_context(|| format!("insert new chunks for {dataset}"))?;
    }

    storage
        .register_new_chunks()
        .context("register new chunks")?;
    Ok(())
}

/// This path's clock: a tick is one wall-clock unix second (elsewhere, e.g. the sim, it's an
/// abstract counter), which is why the drain-window and worker-GC durations convert via `as_secs`.
fn now_ticks() -> Tick {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_secs()
}
