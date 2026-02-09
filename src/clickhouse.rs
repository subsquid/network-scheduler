use std::{collections::BTreeMap, str::FromStr, sync::Arc, time::Duration};

use anyhow::Result;
use clickhouse::{Client, Row};
use itertools::Itertools;
use semver::Version;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::{
    cli::ClickhouseArgs,
    pool,
    types::{Chunk, ChunkSummary, Worker, WorkerStatus},
};

const PINGS_TABLE: &str = "worker_pings_v2";
const CHUNKS_TABLE: &str = "dataset_chunks";

#[derive(Row, Debug, Deserialize)]
struct PingRow {
    worker_id: String,
    version: String,
    stored_bytes: u64,
    timestamp: u64,
}

pub struct ClickhouseClient {
    client: Client,
}

impl ClickhouseClient {
    pub async fn new(args: &ClickhouseArgs) -> anyhow::Result<Self> {
        let client = Client::default()
            .with_url(
                args.clickhouse_url
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("ClickHouse URL is required"))?,
            )
            .with_database(
                args.clickhouse_database
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("ClickHouse database is required"))?,
            )
            .with_user(
                args.clickhouse_user
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("ClickHouse user is required"))?,
            )
            .with_password(
                args.clickhouse_password
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("ClickHouse password is required"))?,
            );
        let this = Self { client };
        this.create_tables().await?;
        Ok(this)
    }

    #[instrument(skip_all)]
    pub async fn get_active_workers(
        &self,
        inactive_timeout: Duration,
        stale_threshold: u64,
        min_version: &Version,
    ) -> Result<Vec<Worker>> {
        let _timer = crate::metrics::Timer::new("get_active_workers");

        let inactive_threshold = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Failed to get system time")
            - inactive_timeout)
            .as_millis() as u64;

        let query = format!(
            r"
            SELECT DISTINCT ON (worker_id) worker_id, version, stored_bytes, timestamp
            FROM {PINGS_TABLE}
            WHERE timestamp >= (SELECT MAX(timestamp) FROM {PINGS_TABLE}) - INTERVAL 1 DAY
            ORDER BY worker_id, timestamp DESC
            "
        );

        let mut cursor = self.client.query(&query).fetch::<PingRow>()?;

        let mut results = Vec::new();
        while let Some(row) = cursor.next().await? {
            let peer_id = match row.worker_id.parse() {
                Ok(peer_id) => peer_id,
                Err(e) => {
                    tracing::warn!("Failed to parse worker ID \"{}\": {}", row.worker_id, e);
                    crate::metrics::failure("invalid_peer_id");
                    continue;
                }
            };
            let mut status = WorkerStatus::Online;
            if row.stored_bytes < stale_threshold {
                status = WorkerStatus::Stale;
            }
            if !Version::from_str(&row.version).is_ok_and(|ver| ver >= *min_version) {
                status = WorkerStatus::UnsupportedVersion;
            }
            if row.timestamp < inactive_threshold {
                status = WorkerStatus::Offline;
            }
            results.push(Worker {
                id: peer_id,
                status,
            });
        }

        crate::metrics::report_workers(&results);
        Ok(results)
    }

    #[instrument(skip_all)]
    pub async fn get_existing_chunks(
        &self,
        datasets: impl Iterator<Item = impl Into<&str>>,
    ) -> anyhow::Result<BTreeMap<Arc<String>, Vec<Chunk>>> {
        let _timer = crate::metrics::Timer::new("get_existing_chunks");

        let query = format!(
            r"
            SELECT DISTINCT ON (dataset, id) dataset, id, size, files, last_block_hash, last_block_timestamp
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

    #[instrument(skip_all)]
    pub async fn store_new_chunks(&self, chunks: impl IntoIterator<Item = Chunk>) -> Result<()> {
        let _timer = crate::metrics::Timer::new("store_new_chunks");

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
                last_block_hash Nullable(String),
                last_block_timestamp UInt64
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
    last_block_timestamp: u64,
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
        // For historical reasons summary is an option;
        // if one of the contained values is present, it should be 'Some'.
        if row.last_block_hash.is_some() || row.last_block_timestamp != 0 {
            chunk.summary = Some(ChunkSummary {
                last_block_hash: row.last_block_hash.unwrap_or(String::with_capacity(0)),
                last_block_timestamp: row.last_block_timestamp,
            });
        }
        Ok(chunk)
    }
}

impl From<Chunk> for ChunkRow {
    fn from(chunk: Chunk) -> Self {
        // For historical reasons last_block_hash is nullable.
        let (last_block_hash, last_block_timestamp) = chunk.summary.map_or((None, 0), |s| {
            (Some(s.last_block_hash), s.last_block_timestamp)
        });
        Self {
            dataset: chunk.dataset.to_string(),
            id: chunk.id,
            size: chunk.size as u64,
            files: chunk.files.join(","),
            last_block_hash: last_block_hash,
            last_block_timestamp: last_block_timestamp,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::clickhouse::ClickhouseArgs;
    use chrono::{TimeZone, Utc};

    // To run this test, start a local clickhouse instance first
    // docker run --rm \
    //   -e CLICKHOUSE_DB=logs_db \
    //   -e CLICKHOUSE_USER=user \
    //   -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
    //   -e CLICKHOUSE_PASSWORD=password \
    //   --network=host \
    //   --ulimit nofile=262144:262144 \
    //   clickhouse/clickhouse-server
    #[tokio::test]
    #[ignore = "database test"]
    async fn test_clickhouse() {
        let client = ClickhouseClient::new(&ClickhouseArgs {
            clickhouse_url: Some("http://localhost:8123/".to_string()),
            clickhouse_database: Some("logs_db".to_string()),
            clickhouse_user: Some("user".to_string()),
            clickhouse_password: Some("password".to_string()),
        })
        .await
        .expect("Cannot connect to clickhouse");

        client.create_tables().await.expect("Cannot create tables");

        let dataset = "s3://solana-mainnet-1";
        let tstp = Utc.with_ymd_and_hms(2025, 10, 20, 5, 40, 10).unwrap();

        let expected = vec![Chunk {
            dataset: dataset.to_string().into(),
            id: "0018197829/0018246541-0018248424-c7ed95c9".to_string(),
            size: 1000,
            blocks: std::ops::RangeInclusive::new(18246541, 18248424),
            files: Arc::new(vec!["blocks".to_string(), "transactions".to_string()]),
            summary: Some(ChunkSummary {
                last_block_hash: "00BAB10C".to_string(),
                last_block_timestamp: tstp.timestamp_millis() as u64,
            }),
        }];

        client
            .store_new_chunks(expected.clone())
            .await
            .expect("Cannot store chunks");

        let datasets = vec![dataset.to_string()];
        let chunks_of_dataset = client
            .get_existing_chunks(datasets.iter().map(|d| d.as_str()))
            .await
            .expect("Cannot retrieve chunks");

        let have = chunks_of_dataset[&Arc::new(dataset.to_string())].clone();

        println!("to   DB: {:?}", expected);
        println!("from DB: {:?}", have);

        assert_eq!(expected, have);

        assert!(have.len() == 1);
        assert!(have[0].summary.is_some());
        let have_tstp = have[0].summary.as_ref().unwrap().last_block_timestamp;
        assert_eq!(have_tstp, tstp.timestamp_millis() as u64);
    }

    #[test]
    fn test_semver() {
        use semver::{Version, VersionReq};

        let a = Version::parse("1.0.0").unwrap();
        let b = Version::parse("1.0.1-rc1").unwrap();
        assert!(a < b);

        // Faulty behaviour — that's why we compare versions instead of using VersionReq
        let req = VersionReq::parse(">=1.0.0").unwrap();
        assert!(!req.matches(&b));
        // Even * doesn't match pre-releases
        let all = VersionReq::parse("*").unwrap();
        assert!(!all.matches(&b));
    }
}
