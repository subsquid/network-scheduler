use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::ensure;
use clap::Parser;
use secrecy::{ExposeSecret, SecretString};
use semver::Version;
use serde::{Deserialize, Serialize};
use serde_with::{DurationSeconds, serde_as};

#[cfg(feature = "mvcc-chunks")]
use crate::scheduler_storage::postgres::{DEFAULT_BATCH_SIZE, DEFAULT_CLAIM_LOCK_TIMEOUT};
use crate::types::{ChunkWeight, SchedulingConfig};

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

    /// Run mode: prod (with ClickHouse), cli (with state file), or service (long-running
    /// multistep scheduler; `mvcc-chunks` builds only).
    /// Take into account that all modes utilise S3
    #[arg(short, long, default_value = "prod")]
    pub mode: RunMode,

    #[command(flatten)]
    pub s3: S3Args,

    #[command(flatten)]
    pub clickhouse: ClickhouseArgs,

    /// Path to CLI mode state file containing workers and known chunks (required for cli mode)
    #[arg(long, env = "CLI_STATE_FILE", required_if_eq("mode", "cli"))]
    pub cli_state_file: Option<PathBuf>,

    /// Run the multistep (MVCC) scheduling cycle instead of the ordinary scheduler.
    #[cfg(feature = "mvcc-chunks")]
    #[arg(long)]
    pub multistep_scheduler: bool,

    /// Postgres connection string for the multistep scheduler's storage backend.
    #[cfg(feature = "mvcc-chunks")]
    #[arg(
        long,
        env = "DATABASE_URL",
        // Clap renders `[env: DATABASE_URL=<value>]` otherwise, printing the password in the
        // connection string to anyone who runs `--help` on a configured deployment. `Secret` only
        // covers `Debug`; clap reads the variable itself.
        hide_env_values = true,
        required_if_eq_any([("multistep_scheduler", "true"), ("mode", "service")])
    )]
    pub database_url: Option<Secret>,

    /// Multistep drain window (the MVCC "M"): how long a dropped `(chunk, worker)` pair keeps being
    /// served after leaving the portal assignment.
    #[cfg(feature = "mvcc-chunks")]
    #[arg(long, env = "MULTISTEP_DRAIN_WINDOW", default_value = "5m", value_parser = humantime::parse_duration)]
    pub multistep_drain_window: Duration,

    /// Multistep departed-worker retention: a worker inactive for longer than this is deleted from
    /// the scheduler's worker set.
    #[cfg(feature = "mvcc-chunks")]
    #[arg(long, env = "MULTISTEP_WORKER_GC", default_value = "24h", value_parser = humantime::parse_duration)]
    pub multistep_worker_gc: Duration,

    /// Service mode: interval between scheduling cycles.
    #[cfg(feature = "mvcc-chunks")]
    #[arg(long, env = "MULTISTEP_SCHEDULE_INTERVAL", default_value = "20m", value_parser = humantime::parse_duration)]
    pub schedule_interval: Duration,

    /// Service mode: interval between worker-set refreshes from ClickHouse.
    #[cfg(feature = "mvcc-chunks")]
    #[arg(long, env = "MULTISTEP_WORKER_UPDATE_INTERVAL", default_value = "7m", value_parser = humantime::parse_duration)]
    pub worker_update_interval: Duration,

    /// Service mode: interval between S3 chunk-discovery passes.
    #[cfg(feature = "mvcc-chunks")]
    #[arg(long, env = "MULTISTEP_CHUNK_DISCOVERY_INTERVAL", default_value = "10m", value_parser = humantime::parse_duration)]
    pub chunk_discovery_interval: Duration,

    /// Service mode: how often an idle task checks that its Postgres connection is still alive,
    /// so a connection that dies between ticks is caught in seconds rather than at the next tick.
    /// 0 disables the check.
    #[cfg(feature = "mvcc-chunks")]
    #[arg(long, env = "MULTISTEP_CONNECTION_PROBE_INTERVAL", default_value = "30s", value_parser = humantime::parse_duration)]
    pub connection_probe_interval: Duration,

    /// Service mode: cap on one connection liveness round trip. Exceeding it means the connection
    /// is gone, which is fatal to the task, so it must comfortably clear a healthy round trip.
    #[cfg(feature = "mvcc-chunks")]
    #[arg(long, env = "MULTISTEP_CONNECTION_PING_TIMEOUT", default_value = "5s", value_parser = humantime::parse_duration)]
    pub connection_ping_timeout: Duration,

    /// Cap on a leadership claim's wait for an in-flight fenced transaction to commit (see
    /// `DEFAULT_CLAIM_LOCK_TIMEOUT`). Exceeding it surfaces as "already running", i.e. retry as a
    /// candidate — a hung startup hides worse than a retryable failure.
    #[cfg(feature = "mvcc-chunks")]
    #[arg(long, env = "MULTISTEP_LEADERSHIP_CLAIM_TIMEOUT", default_value_t = DEFAULT_CLAIM_LOCK_TIMEOUT.into())]
    pub leadership_claim_timeout: humantime::Duration,

    /// Rows per batched write to Postgres — bounds the memory and statement size of the scheduler's
    /// bulk writes (see `DEFAULT_BATCH_SIZE`).
    #[cfg(feature = "mvcc-chunks")]
    #[arg(long, env = "MULTISTEP_BATCH_SIZE", default_value_t = DEFAULT_BATCH_SIZE)]
    pub batch_size: usize,

    /// Service mode: address for the ops surface (`/metrics`, `/health`, `/ready`).
    #[cfg(feature = "mvcc-chunks")]
    #[arg(long, env = "MULTISTEP_OPS_ADDR", default_value = "0.0.0.0:9090")]
    pub ops_addr: std::net::SocketAddr,

    /// Service mode: percentage of online workers that must echo an assignment id in their pings
    /// before the confirmation watermark advances to it. 0 confirms the latest assignment
    /// unconditionally (no worker acks — dev/bootstrap only).
    #[cfg(feature = "mvcc-chunks")]
    #[arg(long, env = "MULTISTEP_CONFIRMATION_QUORUM_PCT", default_value_t = 90, value_parser = clap::value_parser!(u8).range(..=100))]
    pub confirmation_quorum_pct: u8,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum RunMode {
    /// Production mode with ClickHouse
    Prod,
    /// CLI mode with static configuration file
    Cli,
    /// Long-running multistep (MVCC) scheduler service
    #[cfg(feature = "mvcc-chunks")]
    Service,
}

#[derive(clap::Args, Debug)]
pub struct S3Args {
    #[arg(env, hide = true)]
    aws_s3_endpoint: String,

    #[arg(env, hide = true)]
    aws_access_key_id: String,

    #[arg(env, hide = true)]
    aws_secret_access_key: Secret,

    #[arg(env, hide = true, default_value = "auto")]
    aws_region: String,
}

#[derive(clap::Args, Debug)]
pub struct ClickhouseArgs {
    #[arg(long, env, required_if_eq_any([("mode", "prod"), ("mode", "service")]))]
    pub clickhouse_url: Option<String>,
    #[arg(long, env, required_if_eq_any([("mode", "prod"), ("mode", "service")]))]
    pub clickhouse_database: Option<String>,
    #[arg(long, env, required_if_eq_any([("mode", "prod"), ("mode", "service")]))]
    pub clickhouse_user: Option<String>,
    // `hide_env_values`: see `Args::database_url` — `--help` would otherwise print the password.
    #[arg(long, env, hide_env_values = true, required_if_eq_any([("mode", "prod"), ("mode", "service")]))]
    pub clickhouse_password: Option<Secret>,
}

impl S3Args {
    pub async fn config(&self) -> aws_config::SdkConfig {
        aws_config::from_env()
            .endpoint_url(self.aws_s3_endpoint.clone())
            .load()
            .await
    }
}

pub type DatasetsConfig = BTreeMap<String, Vec<DatasetSegmentConfig>>;

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub datasets: DatasetsConfig,

    #[serde_as(as = "DurationSeconds")]
    #[serde(rename = "worker_inactive_timeout_sec")]
    pub worker_inactive_timeout: Duration,

    /// Knobs for both scheduler generations; flattened, so the config-file keys stay top-level
    /// (`worker_storage_bytes`, `saturation`, `min_replication`, `ignore_reliability`).
    #[serde(flatten)]
    pub scheduling: SchedulingConfig,

    pub worker_stale_bytes: u64,

    pub network: String,

    pub storage_domain: String,

    /// Name of the json file, containing the state of the network
    pub network_state_name: String,

    /// URL of the proxy server which exposes assets uploaded to S3
    pub network_state_url: String,

    pub scheduler_state_bucket: String,

    #[serde(skip_serializing)]
    pub cloudflare_storage_secret: Secret,

    #[serde(default = "default_min_worker_version")]
    pub min_supported_worker_version: Version,

    #[serde(default = "default_min_worker_version")]
    pub min_recommended_worker_version: Version,

    /// Epochs a worker's reported on-chain epoch may lag the network's before its contract reads
    /// count as broken. The fleet doesn't roll over simultaneously, so one epoch of lag is routine.
    /// `null` disables the check.
    #[serde(default = "default_max_worker_epoch_lag")]
    pub max_worker_epoch_lag: Option<u32>,

    #[serde_as(as = "DurationSeconds")]
    #[serde(rename = "assignment_delay_sec", default = "default_assignment_delay")]
    pub assignment_delay: Duration,

    #[serde_as(as = "DurationSeconds")]
    #[serde(rename = "assignment_ttl_sec", default = "default_assignment_ttl")]
    pub assignment_ttl: Duration,

    #[serde(skip_serializing, default = "default_concurrent_downloads")]
    pub concurrent_dataset_downloads: usize,

    #[serde_as(as = "Option<DurationSeconds>")]
    #[serde(rename = "dataset_load_timeout_sec", default)]
    pub dataset_load_timeout: Option<Duration>,

    #[serde(default = "default_true")]
    pub strict_continuity_check: bool,

    /// This option can be used purely for local development to create http schemes
    /// with subdomain moved to the path-based routing
    #[serde(default)]
    pub storage_allow_insecure_scheme: bool,

    /// When provided all the hashes except for the head of the dataset will be set to None
    #[serde(default)]
    pub clear_last_block_hash: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetSegmentConfig {
    #[serde(default)]
    pub from: i64,

    #[serde(default = "default_weight")]
    pub weight: ChunkWeight,

    /// Minimum worker version required to serve chunks in this segment.
    /// Workers with a version lower than this will not be assigned chunks from this segment.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub minimum_worker_version: Option<Version>,
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let mut config: Config = serde_yaml::from_reader(reader)?;
        config.fill_defaults();
        config.validate()?;
        Ok(config)
    }

    fn fill_defaults(&mut self) {
        for ds in self.datasets.values_mut() {
            if ds.is_empty() {
                ds.push(DatasetSegmentConfig {
                    from: 0,
                    weight: default_weight(),
                    minimum_worker_version: None,
                });
            }
        }
    }

    fn validate(&self) -> anyhow::Result<()> {
        for segments in self.datasets.values() {
            let mut last = segments[0].from;
            for seg in &segments[1..] {
                if seg.from >= 0 {
                    ensure!(
                        last >= 0,
                        "Negative offsets can't be followed by positive ones"
                    );
                    ensure!(last < seg.from, "Segment 'from' values must be increasing");
                } else if last < 0 {
                    ensure!(last < seg.from, "Segment 'from' values must be increasing");
                }
                last = seg.from;
            }
        }
        Ok(())
    }
}

fn default_true() -> bool {
    true
}

fn default_min_worker_version() -> Version {
    "2.0.0".parse().unwrap()
}

fn default_max_worker_epoch_lag() -> Option<u32> {
    Some(2)
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

/// Secret-bearing CLI/config value: `Debug` prints redacted, so it cannot leak into logs.
/// Newtype over `SecretString` for the `Clone`, serde, and clap integrations it lacks.
#[derive(Debug, Deserialize)]
#[serde(transparent)]
pub struct Secret(SecretString);

impl Clone for Secret {
    fn clone(&self) -> Self {
        Self(SecretString::from(self.0.expose_secret().to_owned()))
    }
}

impl Secret {
    pub fn expose_secret(&self) -> &str {
        self.0.expose_secret()
    }
}

impl From<String> for Secret {
    fn from(s: String) -> Self {
        Self(SecretString::from(s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every secret-bearing arg must suppress clap's `[env: VAR=value]` rendering, or `--help` on a
    /// configured deployment prints the credential. Asserted on the arg definition rather than by
    /// setting the variable, which would race other tests in the same process.
    #[test]
    fn secret_args_hide_their_env_values() {
        use clap::CommandFactory as _;

        let command = Args::command();
        let secrets = ["database_url", "clickhouse_password"];
        for name in secrets {
            let arg = command
                .get_arguments()
                .find(|arg| arg.get_id() == name)
                .unwrap_or_else(|| panic!("no `{name}` arg — was it renamed?"));
            assert!(
                arg.is_hide_env_values_set(),
                "`--{}` would print its env value in --help",
                name.replace('_', "-")
            );
        }
    }

    /// Pins the deployed config-file schema: the scheduling knobs are flattened top-level keys,
    /// and `worker_storage_bytes` maps onto `SchedulingConfig::worker_capacity`.
    #[test]
    fn example_config_parses_with_flat_scheduling_keys() {
        let config: Config =
            serde_yaml::from_str(include_str!("../examples/scheduler_config.yaml"))
                .expect("parse the example config");
        assert_eq!(config.scheduling.worker_capacity, 483_183_820_800);
        assert_eq!(config.scheduling.min_replication, 2);
        assert_eq!(config.scheduling.saturation, 0.99);
        assert!(config.scheduling.ignore_reliability);
    }
}
