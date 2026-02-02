use anyhow::Context;
use clap::Parser;

use crate::controller::{CacheAccess, Controller, WithAssignment};

mod cli;
mod cli_state;
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

    let controller = match args.mode {
        cli::RunMode::Prod => run_prod_mode(&args, config.clone()).await?,
        cli::RunMode::Cli => run_cli_mode(&args, config.clone()).await?,
    };

    let uploader = upload::Uploader::new(config, &args.s3.config().await);
    controller.serialize_assignment().upload(&uploader).await?;

    drop(_timer);
    let metrics = metrics::encode_metrics(&metrics_registry)?;
    controller.upload_metadata(&uploader, metrics).await?;

    tracing::info!("Done");
    Ok(())
}
        
async fn run_prod_mode(args: &cli::Args, config: cli::Config) -> anyhow::Result<WithAssignment> {
    let db = clickhouse::ClickhouseClient::new(&args.clickhouse)
        .await
        .context("Can't connect to ClickHouse")?;

    let datasets_storage = storage::S3Storage::new(&args.s3.config().await);

    Controller::new(config.clone())
        .load_workers(&db)
        .await?
        .load_known_chunks(&db)
        .await?
        .load_new_chunks(Some(&db), &datasets_storage, CacheAccess::ReadWrite)
        .await?
        .weight_chunks()
        // blocking the async executor is ok here because no other tasks are running
        .schedule()
}

async fn run_cli_mode(args: &cli::Args, config: cli::Config) -> anyhow::Result<WithAssignment> {
    let state_file = args
        .cli_state_file
        .as_ref()
        .context("CLI state file required for cli mode")?;

    let cli_state = cli_state::CliState::load(state_file)
        .context("Failed to load CLI state file")?;

    let known_chunks = cli_state.to_chunks()?;
    let workers = cli_state.workers;

    tracing::info!(
        "CLI mode: loaded {} workers and {} datasets from state file",
        workers.len(),
        known_chunks.len()
    );

    let datasets_storage = storage::S3Storage::new(&args.s3.config().await);

    Controller::new(config.clone())
        .load_workers_from_config(workers)
        .load_known_chunks_from_config(known_chunks)?
        .load_new_chunks(None, &datasets_storage, CacheAccess::_ReadOnly)
        .await?
        .weight_chunks()
        // blocking the async executor is ok here because no other tasks are running
        .schedule()
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
