use clap::Parser;
use tracing_subscriber::filter;

mod cli;
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
    let config = cli::Config::load(&args.config)?;

    tracing_subscriber::fmt()
        .with_env_filter(filter::EnvFilter::from_default_env())
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .init();

    let datasets_storage = storage::S3Storage::new(args.s3_config().await);
    let chunks = datasets_storage
        .load_all_chunks(config.datasets.keys(), 3)
        .await
        .unwrap();
    println!("{:?}", chunks.keys());
    Ok(())
}
