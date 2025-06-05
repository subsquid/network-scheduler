use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Duration,
};

use clap::Parser;
use semver::VersionReq;
use serde::{Deserialize, Serialize};
use serde_with::{DurationSeconds, serde_as};

use crate::types::ChunkWeight;

#[derive(Parser, Debug)]
#[command(name = "SQD Network Scheduler")]
pub struct Args {
    /// Path to the config file
    #[arg(
        short,
        long,
        env = "CONFIG_PATH",
        value_name = "FILE",
        default_value = "config.yaml"
    )]
    pub config: PathBuf,

    #[command(flatten)]
    pub s3: S3Args,

    #[command(flatten)]
    pub clickhouse: ClickhouseArgs,
}

#[derive(clap::Args, Debug)]
pub struct S3Args {
    #[arg(env, hide = true)]
    aws_s3_endpoint: String,

    #[arg(env, hide = true)]
    aws_access_key_id: String,

    #[arg(env, hide = true)]
    aws_secret_access_key: String,

    #[arg(env, hide = true, default_value = "auto")]
    aws_region: String,
}

#[derive(clap::Args, Debug)]
pub struct ClickhouseArgs {
    #[arg(long, env)]
    pub clickhouse_url: String,
    #[arg(long, env)]
    pub clickhouse_database: String,
    #[arg(long, env)]
    pub clickhouse_user: String,
    #[arg(long, env)]
    pub clickhouse_password: String,
}

impl S3Args {
    pub async fn config(&self) -> aws_config::SdkConfig {
        aws_config::from_env()
            .endpoint_url(self.aws_s3_endpoint.clone())
            .load()
            .await
    }
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub datasets: BTreeMap<String, DatasetConfig>,

    #[serde_as(as = "DurationSeconds")]
    #[serde(rename = "worker_inactive_timeout_sec")]
    pub worker_inactive_timeout: Duration,

    #[serde(default)]
    pub ignore_reliability: bool,

    pub worker_storage_bytes: u64,

    pub worker_stale_bytes: u64,

    pub min_replication: u16,

    /// The fraction of the worker storage that is actually filled (on average).
    /// The closer it gets to 1, the less consisent the distribution is.
    /// Corresponds to `1 / (1 + epsilon)` from this paper:
    /// https://research.google/blog/consistent-hashing-with-bounded-loads/
    pub saturation: f64,

    pub network: String,

    pub storage_domain: String,

    pub network_state_name: String,

    pub network_state_url: String,

    pub scheduler_state_bucket: String,

    #[serde(skip_serializing)]
    pub cloudflare_storage_secret: String,

    #[serde(default = "default_worker_versions")]
    pub supported_worker_versions: VersionReq,

    #[serde(default = "default_worker_versions")]
    pub recommended_worker_versions: VersionReq,

    #[serde_as(as = "DurationSeconds")]
    #[serde(rename = "assignment_delay_sec", default = "default_assignment_delay")]
    pub assignment_delay: Duration,

    #[serde_as(as = "DurationSeconds")]
    #[serde(rename = "assignment_ttl_sec", default = "default_assignment_ttl")]
    pub assignment_ttl: Duration,

    #[serde(skip_serializing, default = "default_concurrent_downloads")]
    pub concurrent_dataset_downloads: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetConfig {
    #[serde(default = "default_weight")]
    pub weight: ChunkWeight,
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let config = serde_yaml::from_reader(reader)?;
        Ok(config)
    }
}

fn default_worker_versions() -> VersionReq {
    ">=2.0.0".parse().unwrap()
}

fn default_assignment_ttl() -> Duration {
    const DAY: Duration = Duration::from_secs(60 * 60 * 24);
    30 * DAY
}

fn default_assignment_delay() -> Duration {
    Duration::from_secs(60)
}

fn default_weight() -> ChunkWeight {
    1
}

fn default_concurrent_downloads() -> usize {
    20
}
