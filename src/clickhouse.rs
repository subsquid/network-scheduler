use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anyhow::Result;
use clickhouse::{Client, Row};
use itertools::Itertools;
use semver::VersionReq;
use serde::{Deserialize, Serialize};
use sqd_messages::assignments::ChunkSummary;

use crate::{
    cli::ClickhouseArgs,
    pool,
    types::{Chunk, Worker, WorkerStatus},
};

const PINGS_TABLE: &str = "worker_pings_v2";
const CHUNKS_TABLE: &str = "dataset_chunks";

#[derive(Row, Debug, Deserialize)]
struct PingRow {
    worker_id: String,
    version: String,
    stored_bytes: u64,
}

pub struct ClickhouseClient {
    client: Client,
}

impl ClickhouseClient {
    pub async fn new(args: &ClickhouseArgs) -> anyhow::Result<Self> {
        let client = Client::default()
            .with_url(&args.clickhouse_url)
            .with_database(&args.clickhouse_database)
            .with_user(&args.clickhouse_user)
            .with_password(&args.clickhouse_password);
        let this = Self { client };
        this.create_tables().await?;
        Ok(this)
    }

    pub async fn get_active_workers(
        &self,
        threshold: Duration,
        versions: &VersionReq,
    ) -> Result<Vec<Worker>> {
        let seconds = threshold.as_secs();
        let query = r"
            SELECT DISTINCT ON (worker_id) worker_id, version, stored_bytes
            FROM ?
            WHERE timestamp >= (SELECT MAX(timestamp) FROM ?) - INTERVAL ? SECOND
            ORDER BY worker_id, timestamp DESC
        ";

        let mut cursor = self
            .client
            .query(query)
            .bind(PINGS_TABLE)
            .bind(PINGS_TABLE)
            .bind(seconds)
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
            let status = if row.stored_bytes > 0 {
                WorkerStatus::Online
            } else {
                WorkerStatus::Stale // TODO: set a higher threshold
            };
            results.push(Worker {
                id: peer_id,
                status,
            });
        }

        Ok(results)
    }

    pub async fn get_existing_chunks(
        &self,
        datasets: impl Iterator<Item = impl Into<&str>>,
    ) -> anyhow::Result<BTreeMap<Arc<String>, Vec<Chunk>>> {
        let query = format!(
            r"
            SELECT dataset, id, size, files, last_block_hash
            FROM {CHUNKS_TABLE}
            WHERE dataset IN ?
            ORDER BY dataset, id
        "
        );

        let mut cursor = self
            .client
            .query(&query)
            .bind(datasets.into_iter().map(Into::into).collect_vec())
            .fetch::<ChunkRow>()?;

        let mut result: BTreeMap<_, Vec<_>> = BTreeMap::new();
        while let Some(row) = cursor.next().await? {
            let chunk = Chunk::try_from(row)?;
            result.entry(chunk.dataset.clone()).or_default().push(chunk);
        }
        Ok(result)
    }

    pub async fn store_new_chunks(&self, chunks: impl IntoIterator<Item = Chunk>) -> Result<()> {
        let mut inserter = self.client.insert(CHUNKS_TABLE)?;
        for chunk in chunks {
            inserter.write(&ChunkRow::from(chunk)).await?;
        }
        inserter.end().await?;

        Ok(())
    }

    async fn create_tables(&self) -> Result<()> {
        let query = format!(
            r"
            CREATE TABLE IF NOT EXISTS {CHUNKS_TABLE} (
                dataset LowCardinality(String) NOT NULL,
                id String NOT NULL,
                size UInt64 NOT NULL,
                files LowCardinality(String) NOT NULL,
                last_block_hash Nullable(String)
            ) ENGINE = ReplacingMergeTree()
            ORDER BY (dataset, id)
        "
        );

        self.client.query(&query).execute().await?;
        Ok(())
    }
}

#[derive(Row, Debug, Serialize, Deserialize)]
struct ChunkRow {
    dataset: String,
    id: String,
    size: u64,
    files: String,
    last_block_hash: Option<String>,
}

impl TryFrom<ChunkRow> for Chunk {
    type Error = anyhow::Error;

    fn try_from(row: ChunkRow) -> Result<Self> {
        let dataset = pool::intern(row.dataset);
        let size: u32 = row.size.try_into()?;
        let mut chunk = Chunk::new(
            dataset,
            row.id,
            size,
            row.files.split(',').map(String::from).collect(),
        )?;
        if let Some(last_block_hash) = row.last_block_hash {
            chunk.summary = Some(ChunkSummary {
                last_block_hash,
                last_block_number: *chunk.blocks.end(),
            });
        }
        Ok(chunk)
    }
}

impl From<Chunk> for ChunkRow {
    fn from(chunk: Chunk) -> Self {
        Self {
            dataset: chunk.dataset.to_string(),
            id: chunk.id,
            size: chunk.size as u64,
            files: chunk.files.join(","),
            last_block_hash: chunk.summary.map(|s| s.last_block_hash),
        }
    }
}
