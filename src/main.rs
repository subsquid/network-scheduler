use std::{collections::BTreeMap, sync::Arc};

use anyhow::Context;
use clap::Parser;
use itertools::Itertools;
use scheduling::WeightedChunk;
use types::Chunk;

mod cli;
mod clickhouse;
mod metrics;
mod parquet;
mod pool;
mod replication;
mod scheduling;
mod storage;
#[cfg(test)]
mod tests;
mod types;
mod upload;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    let args = cli::Args::parse();

    setup_tracing(&args);

    let config = cli::Config::load(&args.config).context("Can't parse config")?;
    let datasets = config
        .datasets
        .keys()
        .map(|bucket| Arc::<String>::from(format!("s3://{bucket}")))
        .collect_vec();

    let db = clickhouse::ClickhouseClient::new(&args.clickhouse).await?;
    let workers = db
        .get_active_workers(
            config.worker_inactive_timeout,
            &config.supported_worker_versions,
        )
        .await
        .context("Can't read active workers from ClickHouse")?;
    tracing::info!("Found {} workers", workers.len());

    tracing::info!("Reading already known chunks from ClickHouse");
    let mut known_chunks = db
        .get_existing_chunks(datasets.iter().map(|d| d.as_str()))
        .await
        .context("Can't read existing chunks from ClickHouse")?;
    let last_chunks = known_chunks
        .iter()
        .map(|(dataset, chunks)| {
            debug_assert!(chunks.iter().is_sorted_by_key(|c| &c.id));
            let last_chunk = chunks.last().expect("Chunks should not be empty");
            (dataset.clone(), last_chunk)
        })
        .collect::<BTreeMap<_, _>>();
    tracing::debug!(
        "Loaded known chunks up to {:#?}",
        last_chunks
            .iter()
            .map(|(dataset, chunk)| (dataset, &chunk.id))
            .collect::<BTreeMap<_, _>>()
    );

    tracing::info!("Reading new chunks from storage");
    let datasets_storage = storage::S3Storage::new(&args.s3.config().await);
    let new_chunks = datasets_storage
        .load_newer_chunks(
            datasets
                .iter()
                .map(|dataset| (dataset.clone(), last_chunks.get(dataset).map(|d| *d))),
            config.concurrent_dataset_downloads,
        )
        .await
        .context("Can't load datasets")?;
    let new_chunks_count = new_chunks
        .iter()
        .map(|(_, chunks)| chunks.len())
        .sum::<usize>();
    let datasets_num = new_chunks.len();

    tracing::info!("Storing {} new chunks in ClickHouse", new_chunks_count);
    db.store_new_chunks(new_chunks.values().flat_map(|v| v.iter()).cloned())
        .await
        .context("Can't store new chunks in ClickHouse")?;

    for (dataset, chunks) in new_chunks {
        if let Some(known_chunks) = known_chunks.get_mut(&dataset) {
            known_chunks.extend(chunks);
        } else {
            known_chunks.insert(dataset, chunks);
        }
    }
    let chunks = known_chunks
        .into_iter()
        .flat_map(|(_, chunks)| chunks.into_iter())
        .collect_vec();
    tracing::info!(
        "Loaded {} datasets with {} chunks in total",
        datasets_num,
        chunks.len()
    );

    let weighted_chunks = weight_chunks(&chunks, &config);
    debug_assert!(weighted_chunks.iter().is_sorted());

    // blocking the async executor is ok here because no other tasks are running
    let assignment = scheduling::schedule(
        &weighted_chunks,
        &workers,
        scheduling::SchedulingConfig {
            worker_capacity: config.worker_storage_bytes,
            saturation: config.saturation,
            min_replication: config.min_replication,
        },
    )
    .map_err(|e| anyhow::anyhow!("Couldn't schedule chunks: {e}"))?;
    drop(weighted_chunks);

    assignment.log_stats(&chunks);

    tracing::info!("Serializing assignment");
    let encoded = assignment.encode(chunks, &config);

    tracing::info!("Uploading assignment");
    let uploader = upload::Uploader::new(&args.s3.config().await);
    let url = uploader.save_assignment(encoded, &config).await?;

    tracing::info!("Assignment uploaded to {url}");

    Ok(())
}

fn setup_tracing(_args: &cli::Args) {
    use tracing::level_filters::LevelFilter;
    use tracing_subscriber::{EnvFilter, fmt::format::FmtSpan};

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with_span_events(FmtSpan::CLOSE)
        .compact()
        .init();
}

fn weight_chunks(chunks: &[Chunk], config: &cli::Config) -> Vec<WeightedChunk> {
    chunks
        .iter()
        .map(|chunk| WeightedChunk {
            id: format!("{}/{}", chunk.dataset, chunk.id),
            size: chunk.size,
            weight: config.datasets[chunk.bucket()].weight,
        })
        .collect()
}
