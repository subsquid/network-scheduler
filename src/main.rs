use anyhow::Context;
use clap::Parser;

use crate::controller::{CacheAccess, Controller};

mod cli;
mod clickhouse;
mod controller;
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
mod weight;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _timer = crate::metrics::Timer::new("process");

    dotenv::dotenv().ok();
    let args = cli::Args::parse();
    setup_tracing(&args);
    let config = cli::Config::load(&args.config).context("Can't parse config")?;
    let metrics_registry = metrics::register_metrics(config.network.clone());

    let db = clickhouse::ClickhouseClient::new(&args.clickhouse)
        .await
        .context("Can't connect to ClickHouse")?;
    let datasets_storage = storage::S3Storage::new(&args.s3.config().await);

    let controller = Controller::new(config.clone())
        .load_workers(&db)
        .await?
        .load_known_chunks(&db)
        .await?
        .load_new_chunks(&db, &datasets_storage, CacheAccess::ReadWrite)
        .await?
        .weight_chunks()
        // blocking the async executor is ok here because no other tasks are running
        .schedule()?;

    let uploader = upload::Uploader::new(config, &args.s3.config().await);
    controller.serialize_assignment().upload(&uploader).await?;

    drop(_timer);
    let metrics = metrics::encode_metrics(&metrics_registry)?;
    controller.upload_metadata(&uploader, metrics).await?;

    tracing::info!("Done");
    Ok(())
}

fn setup_tracing(_args: &cli::Args) {
    use tracing::level_filters::LevelFilter;
    use tracing_subscriber::EnvFilter;

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .compact()
        .init();
}
