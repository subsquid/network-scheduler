use anyhow::Context;
use clap::Parser;

use network_scheduler::controller::{CacheAccess, Controller, WithAssignment};
use network_scheduler::{cli, dataset_data_storage, metrics, upload};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _timer = metrics::Timer::new("process");

    dotenv::dotenv().ok();
    let args = cli::Args::parse();
    setup_tracing(&args);
    let config = cli::Config::load(&args.config).context("Can't parse config")?;
    tracing::info!(
        "Scheduler configuration:\n{}",
        serde_yaml::to_string(&config)
            .unwrap_or_else(|e| format!("<failed to serialize config: {e}>"))
    );

    match args.mode {
        cli::RunMode::Prod => run_prod_mode(&args, config).await?,
        cli::RunMode::Cli => run_cli_mode(&args, config).await?,
    }

    tracing::info!("Done");
    Ok(())
}

async fn run_prod_mode(args: &cli::Args, config: cli::Config) -> anyhow::Result<()> {
    let db = network_scheduler::clickhouse::ClickhouseClient::new(&args.clickhouse)
        .await
        .context("Can't connect to ClickHouse")?;

    #[cfg(feature = "mvcc-chunks")]
    if args.multistep_scheduler {
        let workers = db.active_workers(&config).await?;
        return network_scheduler::multistep_controller::run(args, &config, workers).await;
    }

    let datasets_storage = dataset_data_storage::S3Storage::new(&args.s3.config().await);

    let controller = Controller::new(config.clone())
        .load_workers(&db)
        .await?
        .load_known_chunks(&db)
        .await?
        .load_new_chunks(Some(&db), &datasets_storage, CacheAccess::ReadWrite)
        .await?
        .prepare_chunks()
        // blocking the async executor is ok here because no other tasks are running
        .schedule()?;

    publish_assignments(args, config, controller).await
}

async fn run_cli_mode(args: &cli::Args, config: cli::Config) -> anyhow::Result<()> {
    let state_file = args
        .cli_state_file
        .as_ref()
        .context("CLI state file required for cli mode")?;

    let cli_state = network_scheduler::cli_state::CliState::load(state_file)
        .context("Failed to load CLI state file")?;

    #[cfg(feature = "mvcc-chunks")]
    if args.multistep_scheduler {
        return network_scheduler::multistep_controller::run(args, &config, cli_state.workers)
            .await;
    }

    let known_chunks = cli_state.to_chunks()?;
    let workers = cli_state.workers;

    tracing::info!(
        "CLI mode: loaded {} workers and {} datasets from state file",
        workers.len(),
        known_chunks.len()
    );

    let datasets_storage = dataset_data_storage::S3Storage::new(&args.s3.config().await);

    let controller = Controller::new(config.clone())
        .load_workers_from_config(workers)
        .load_known_chunks_from_config(known_chunks)?
        .load_new_chunks(None, &datasets_storage, CacheAccess::_ReadOnly)
        .await?
        .prepare_chunks()
        // blocking the async executor is ok here because no other tasks are running
        .schedule()?;

    publish_assignments(args, config, controller).await
}

/// Serialize and upload the scheduled assignment, then the worker/dataset status and metrics.
async fn publish_assignments(
    args: &cli::Args,
    config: cli::Config,
    controller: WithAssignment,
) -> anyhow::Result<()> {
    let metrics_registry = metrics::register_metrics(config.network.clone());
    let uploader = upload::Uploader::new(config, &args.s3.config().await);

    #[cfg(not(feature = "mvcc-chunks"))]
    controller.serialize_assignment().upload(&uploader).await?;
    #[cfg(feature = "mvcc-chunks")]
    controller.serialize_assignments().upload(&uploader).await?;

    let metrics = metrics::encode_metrics(&metrics_registry)?;
    controller.upload_metadata(&uploader, metrics).await?;

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
