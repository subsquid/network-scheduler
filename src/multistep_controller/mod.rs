//! Multistep (MVCC) scheduling entry points (requires the `mvcc-chunks` feature): the one-shot
//! [`run`] runs one scheduling cycle against Postgres instead of the ordinary `Controller`
//! pipeline; [`service`] wraps the same steps in a long-running service of three periodic tasks.
//!
//! The one-shot path runs only [`SchedulerStorage::run_scheduling_cycle`]; the service also
//! advances the confirmation watermark from worker-echoed ping ids and runs the visibility cycle
//! (see `tasks`). Assignments are computed and logged, not published (`docs/README.md`).

mod ops;
pub mod service;
mod tasks;

use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
    time::SystemTime,
};

use anyhow::Context;

use crate::{
    cli, dataset_data_storage, metrics,
    scheduler_storage::{
        NewChunk, NewDataset, SchedulerStorage, Tick, algorithm::MultistepAlgorithm,
        discovered_chunk, postgres::PostgresStorage,
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
        .as_ref()
        .context("--database-url is required with --multistep-scheduler")?;

    tracing::info!("Multistep: scheduling for {} workers", workers.len());

    // Its own leader; nothing else connects, so the epoch goes nowhere.
    let (mut storage, _epoch) = PostgresStorage::connect(
        database_url.expose_secret(),
        args.leadership_claim_timeout.into(),
        args.batch_size,
    )?;

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
        .update_worker_set(&workers, now)
        .context("update worker set")?;
    storage
        .gc_inactive_workers(now, args.multistep_worker_gc.as_secs())
        .context("gc inactive workers")?;

    let algorithm = MultistepAlgorithm::new(config.datasets.clone());
    let assignment = {
        let _timer = metrics::Timer::new("multistep:schedule");
        storage
            .run_scheduling_cycle(
                &algorithm,
                &config.scheduling,
                now,
                args.multistep_drain_window.as_secs(),
            )
            .context("run multistep scheduling cycle")?
    };
    // Nothing publishes the bundle yet; generating it exercises the production path.
    let bundle = storage
        .generate_schema_bundle()
        .context("generate schema bundle")?;

    tracing::info!(
        assignment_id = %assignment.id,
        chunks_placed = assignment.chunk_workers.len(),
        replication_by_weight = ?assignment.replication_by_weight,
        schema_bundle_id = %bundle.id(),
        write_schemas = bundle.schemas().len(),
        read_schemas = bundle.read_schemas().len(),
        "Multistep scheduling cycle done"
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
        // `name` (`s3://{bucket}`) is the dataset identity the watermark logic above compares
        // against, so keep it as the stored name; location is the same path.
        storage
            .insert_new_datasets(vec![NewDataset::with_name(
                name.clone(),
                name.clone(),
                DatasetSchema::default(),
            )])
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
    // The S3 listing carries no schema info, so discovered chunks pin the schema each dataset was
    // seeded with (`bootstrap_datasets` guarantees the row exists).
    let schema_ids = storage.seeded_schema_ids().context("seeded schema ids")?;
    for (dataset, chunks) in discovered {
        if chunks.is_empty() {
            continue;
        }
        let schema_id = *schema_ids
            .get(dataset.as_str())
            .with_context(|| format!("no seeded schema for dataset {dataset}"))?;
        // Move fields out instead of cloning; a dataset can hold millions of chunks.
        let new_chunks: Vec<NewChunk> = chunks
            .into_iter()
            .map(|c| discovered_chunk(c, schema_id))
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
