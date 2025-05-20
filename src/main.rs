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

    tracing::info!("Reading datasets...");
    let datasets_storage = storage::S3Storage::new(&args.s3.config().await);
    let datasets = datasets_storage
        .load_all_chunks(config.datasets.keys(), config.concurrent_dataset_downloads)
        .await
        .context("Can't load datasets")?;
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

    let weighted_chunks = weight_chunks(&chunks, &config);

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
