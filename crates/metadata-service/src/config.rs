//! CLI/env args and the YAML tokens file.

use std::collections::BTreeSet;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context;
use bytesize::ByteSize;
use clap::Parser;
use secrecy::SecretString;
use serde::Deserialize;

/// Bind addr defaults to 0.0.0.0 so the daemon is reachable on eth0. `database_url` is a
/// `SecretString` (redacted Debug) so `Args` is safe to `{:?}`-print.
#[derive(Parser, Debug)]
#[command(name = "metadata-service")]
pub struct Args {
    #[arg(long, env = "BIND", default_value = "0.0.0.0:8000")]
    pub bind: SocketAddr,

    #[arg(long, env = "DATABASE_URL", value_parser = parse_secret)]
    pub database_url: SecretString,

    #[arg(long, env = "CONFIG_PATH", default_value = "tokens.yaml")]
    pub config: PathBuf,

    /// Max pooled DB connections. Sized for the ingester fleet fronting this service.
    #[arg(long, env = "DB_MAX_CONNECTIONS", default_value_t = 16)]
    pub max_connections: u32,

    /// Wait this long for a pooled connection before failing a request (e.g. `10s`, `500ms`).
    #[arg(long, env = "DB_ACQUIRE_TIMEOUT", value_parser = humantime::parse_duration, default_value = "10s")]
    pub acquire_timeout: Duration,

    /// Dev-only: apply migrations before the schema-version check.
    #[arg(long, env = "MIGRATE", default_value_t = false)]
    pub migrate: bool,

    /// Max request-body size, a chunk batch being the largest body (e.g. `20MiB`, `20MB`).
    #[arg(long, env = "MAX_BODY_SIZE", default_value = "20 MiB")]
    pub max_body_size: ByteSize,

    /// Abort a request that runs longer than this, so a slow query can't pin a connection forever
    /// (e.g. `30s`, `2m`).
    #[arg(long, env = "REQUEST_TIMEOUT", value_parser = humantime::parse_duration, default_value = "30s")]
    pub request_timeout: Duration,
}

fn parse_secret(s: &str) -> Result<SecretString, Infallible> {
    Ok(SecretString::from(s.to_owned()))
}

/// Deserialized tokens file. `SecretString`'s Debug is already redacted. Tokens must be
/// high-entropy random strings (they are the only credential); the process reloads this file on
/// SIGHUP so a token can be rotated or revoked without a restart.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokensConfig {
    #[serde(default)]
    pub admins: Vec<SecretString>,
    #[serde(default)]
    pub ingesters: Vec<IngesterConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngesterConfig {
    pub token: SecretString,
    pub datasets: BTreeSet<String>,
}

impl TokensConfig {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let file = std::fs::File::open(path)
            .with_context(|| format!("open tokens config {}", path.display()))?;
        serde_yaml::from_reader(std::io::BufReader::new(file)).context("parse tokens config")
    }
}
