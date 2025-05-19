use anyhow::Context;
use clap::Parser;
use itertools::Itertools;
use scheduling::WeightedChunk;
use tracing_subscriber::filter;

mod cli;
mod clickhouse;
mod metrics;
mod parquet;
mod pool;
mod replication;
mod scheduling;
mod storage;
mod tests;
mod types;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    let args = cli::Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(filter::EnvFilter::from_default_env())
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .init();

    let config = cli::Config::load(&args.config).context("Can't parse config")?;

    tracing::info!("Reading datasets...");
    let datasets_storage = storage::S3Storage::new(args.s3.config().await);
    let datasets = datasets_storage
        .load_all_chunks(config.datasets.keys(), 3)
        .await
        .unwrap();
    let datasets_num = datasets.len();
    let chunks = datasets
        .into_iter()
        .flat_map(|(_, chunks)| chunks.into_iter())
        .collect_vec();
    tracing::info!(
        "Loaded {} datasets with {} chunks in total",
        datasets_num,
        chunks.len()
    );

    let db = clickhouse::ClickhouseReader::new(&args.clickhouse);
    let workers = db
        .get_active_workers(
            config.worker_inactive_timeout,
            &config.supported_worker_versions,
        )
        .await
        .context("Can't read active workers from Clickhouse")?;
    tracing::info!("Found {} workers", workers.len());

    let weighted_chunks = chunks
        .iter()
        .map(|chunk| WeightedChunk {
            id: format!("{}/{}", chunk.dataset, chunk.id),
            size: chunk.size,
            weight: config.datasets[chunk.dataset.strip_prefix("s3://").unwrap()].weight,
        })
        .collect_vec();

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

    assignment
        .workers
        .iter()
        .for_each(|(worker_id, chunk_indexes)| {
            tracing::info!(
                "Worker {}: {}GB",
                worker_id,
                chunk_indexes
                    .into_iter()
                    .map(|i| weighted_chunks[*i as usize].size as u64)
                    .sum::<u64>()
                    / (1 << 30)
            );
        });

    Ok(())
}
