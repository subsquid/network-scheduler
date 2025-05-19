use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use clickhouse::{Client, Row};
use semver::VersionReq;
use serde::Deserialize;

use crate::{cli::ClickhouseArgs, types::Worker};

const PINGS_TABLE: &str = "worker_pings_v2";

#[derive(Row, Debug, Deserialize)]
struct PingRow {
    worker_id: String,
    timestamp: u64,
    version: String,
    stored_bytes: u64,
}

pub struct ClickhouseReader {
    client: Client,
}

impl ClickhouseReader {
    pub fn new(args: &ClickhouseArgs) -> Self {
        let client = Client::default()
            .with_url(&args.clickhouse_url)
            .with_database(&args.clickhouse_database)
            .with_user(&args.clickhouse_user)
            .with_password(&args.clickhouse_password);
        Self { client }
    }

    pub async fn get_active_workers(
        &self,
        threshold: Duration,
        versions: &VersionReq,
    ) -> Result<Vec<Worker>> {
        let from_time = (SystemTime::now() - threshold)
            .duration_since(UNIX_EPOCH)?
            .as_secs();
        let query = r#"
            SELECT DISTINCT ON (worker_id) worker_id, timestamp, version, stored_bytes
            FROM ?
            WHERE timestamp >= ?
            ORDER BY worker_id, timestamp DESC
        "#;

        let mut cursor = self
            .client
            .query(query)
            .bind(PINGS_TABLE)
            .bind(from_time)
            .fetch::<PingRow>()?;

        let mut results = Vec::new();
        while let Some(row) = cursor.next().await? {
            let version_ok = row.version.parse().is_ok_and(|ver| versions.matches(&ver));
            if !version_ok {
                continue;
            }
            let peer_id = match row.worker_id.parse() {
                Ok(peer_id) => peer_id,
                Err(e) => {
                    tracing::warn!("Failed to parse worker ID \"{}\": {}", row.worker_id, e);
                    continue;
                }
            };
            results.push(Worker {
                id: peer_id,
                reliable: row.stored_bytes > 0, // TODO: set a higher threshold
            });
        }

        Ok(results)
    }
}
