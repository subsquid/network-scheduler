use std::{collections::BTreeMap, str::FromStr, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use clickhouse::{Client, Row};
use itertools::Itertools;
use semver::Version;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::{
    cli::{ClickhouseArgs, Config},
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
    /// `None` for workers predating the field or ones that never completed a contract read.
    current_epoch: Option<u32>,
}

/// Config thresholds plus the epoch baseline derived from the run's own rows.
struct StatusThresholds<'a> {
    /// Pings older than this (ms) mean the worker stopped reporting.
    inactive_threshold: u64,
    /// Below this many stored bytes the worker isn't carrying its share.
    stale_threshold: u64,
    min_version: &'a Version,
    /// Highest epoch anyone reported this run; `None` if nobody did.
    max_epoch: Option<u32>,
    /// Epochs a worker may lag [`Self::max_epoch`]; `None` disables the check.
    max_epoch_lag: Option<u32>,
}

/// Derive a worker's status from its latest ping. Each condition overrides the ones before it, so
/// the order below is the precedence order, weakest first.
fn worker_status(row: &PingRow, version: Option<&Version>, t: &StatusThresholds) -> WorkerStatus {
    let mut status = WorkerStatus::Online;
    // No epoch at all fails the same way as a frozen one: the worker never read the contract.
    // Without a baseline there is nothing to compare against, so the check stays silent.
    if let (Some(max_epoch), Some(max_lag)) = (t.max_epoch, t.max_epoch_lag)
        && row
            .current_epoch
            .is_none_or(|epoch| epoch < max_epoch.saturating_sub(max_lag))
    {
        status = WorkerStatus::StaleRpc;
    }
    if row.stored_bytes < t.stale_threshold {
        status = WorkerStatus::Stale;
    }
    if version.is_none_or(|ver| ver < t.min_version) {
        status = WorkerStatus::UnsupportedVersion;
    }
    if row.timestamp < t.inactive_threshold {
        status = WorkerStatus::Offline;
    }
    status
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
        max_epoch_lag: Option<u32>,
    ) -> Result<Vec<Worker>> {
        let _timer = crate::metrics::Timer::new("get_active_workers");

        let inactive_threshold = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Failed to get system time")
            - inactive_timeout)
            .as_millis() as u64;

        let query = format!(
            r"
            SELECT DISTINCT ON (worker_id) worker_id, version, stored_bytes, timestamp, current_epoch
            FROM {PINGS_TABLE}
            WHERE timestamp >= (SELECT MAX(timestamp) FROM {PINGS_TABLE}) - INTERVAL 1 DAY
            ORDER BY worker_id, timestamp DESC
            "
        );

        let mut cursor = self.client.query(&query).fetch::<PingRow>()?;

        // Buffered, not classified inline: the epoch baseline needs every row first.
        let mut rows = Vec::new();
        while let Some(row) = cursor.next().await? {
            let peer_id = match row.worker_id.parse() {
                Ok(peer_id) => peer_id,
                Err(e) => {
                    tracing::warn!("Failed to parse worker ID \"{}\": {}", row.worker_id, e);
                    crate::metrics::failure("invalid_peer_id");
                    continue;
                }
            };
            rows.push((peer_id, row));
        }

        let thresholds = StatusThresholds {
            inactive_threshold,
            stale_threshold,
            min_version,
            max_epoch: rows.iter().filter_map(|(_, row)| row.current_epoch).max(),
            max_epoch_lag,
        };

        let results: Vec<Worker> = rows
            .into_iter()
            .map(|(peer_id, row)| {
                let version = Version::from_str(&row.version).ok();
                let status = worker_status(&row, version.as_ref(), &thresholds);
                Worker {
                    id: peer_id,
                    status,
                    version,
                }
            })
            .collect();

        let stale_rpc = results
            .iter()
            .filter(|w| w.status == WorkerStatus::StaleRpc)
            .count();
        if stale_rpc > 0 {
            tracing::warn!(
                "{stale_rpc} workers report no epoch or lag the network epoch ({}) by more than {}",
                thresholds.max_epoch.unwrap_or_default(),
                thresholds.max_epoch_lag.unwrap_or_default(),
            );
        }

        crate::metrics::report_workers(&results);
        Ok(results)
    }

    /// Active worker set, taking the thresholds from `config`. Shared by the ordinary and
    /// multistep paths.
    pub async fn active_workers(&self, config: &Config) -> Result<Vec<Worker>> {
        self.get_active_workers(
            config.worker_inactive_timeout,
            config.worker_stale_bytes,
            &config.min_supported_worker_version,
            config.max_worker_epoch_lag,
        )
        .await
        .context("Can't read active workers from ClickHouse")
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
            id: chunk.id.to_string(),
            size: chunk.size as u64,
            files: chunk.files.join(","),
            last_block_hash,
            last_block_timestamp,
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
            id: Arc::new("0018197829/0018246541-0018248424-c7ed95c9".to_string()),
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

        let datasets = [dataset.to_string()];
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

    const STALE_BYTES: u64 = 600 << 30;
    const INACTIVE_BEFORE: u64 = 1_000;

    fn ping(stored_bytes: u64, timestamp: u64, current_epoch: Option<u32>) -> PingRow {
        PingRow {
            worker_id: "12D3KooWR1VSmCyVGTc8ewcm3LW82VeT68rjCKjBvGfpkt6ALxPx".to_owned(),
            version: "2.13.0".to_owned(),
            stored_bytes,
            timestamp,
            current_epoch,
        }
    }

    fn status_of(row: &PingRow, max_epoch: Option<u32>) -> WorkerStatus {
        status_of_with_lag(row, max_epoch, Some(2))
    }

    fn status_of_with_lag(
        row: &PingRow,
        max_epoch: Option<u32>,
        max_epoch_lag: Option<u32>,
    ) -> WorkerStatus {
        let version = Version::from_str(&row.version).ok();
        let min_version = Version::parse("2.10.1").unwrap();
        worker_status(
            row,
            version.as_ref(),
            &StatusThresholds {
                inactive_threshold: INACTIVE_BEFORE,
                stale_threshold: STALE_BYTES,
                min_version: &min_version,
                max_epoch,
                max_epoch_lag,
            },
        )
    }

    #[test]
    fn epoch_lag_within_tolerance_stays_online() {
        let healthy = ping(STALE_BYTES, 2_000, Some(58132));
        assert_eq!(status_of(&healthy, Some(58132)), WorkerStatus::Online);

        // One epoch behind is routine: the fleet does not roll over simultaneously.
        let lagging = ping(STALE_BYTES, 2_000, Some(58131));
        assert_eq!(status_of(&lagging, Some(58132)), WorkerStatus::Online);

        // Exactly at the tolerance is still fine; one past it is not.
        let at_limit = ping(STALE_BYTES, 2_000, Some(58130));
        assert_eq!(status_of(&at_limit, Some(58132)), WorkerStatus::Online);
        let over_limit = ping(STALE_BYTES, 2_000, Some(58129));
        assert_eq!(status_of(&over_limit, Some(58132)), WorkerStatus::StaleRpc);
    }

    #[test]
    fn frozen_epoch_is_stale_rpc() {
        let frozen = ping(STALE_BYTES, 2_000, Some(56354));
        assert_eq!(status_of(&frozen, Some(58132)), WorkerStatus::StaleRpc);
    }

    #[test]
    fn missing_epoch_is_stale_rpc() {
        // Never completed a contract read: as broken as a frozen epoch.
        let unknown = ping(STALE_BYTES, 2_000, None);
        assert_eq!(status_of(&unknown, Some(58132)), WorkerStatus::StaleRpc);
    }

    #[test]
    fn without_a_baseline_the_check_stays_silent() {
        // Nobody reports an epoch (e.g. before the field rolled out): the check can't fire.
        let reporting = ping(STALE_BYTES, 2_000, Some(1));
        assert_eq!(status_of(&reporting, None), WorkerStatus::Online);

        let unknown = ping(STALE_BYTES, 2_000, None);
        assert_eq!(status_of(&unknown, None), WorkerStatus::Online);
    }

    #[test]
    fn null_config_disables_the_check() {
        let frozen = ping(STALE_BYTES, 2_000, Some(56354));
        assert_eq!(
            status_of_with_lag(&frozen, Some(58132), None),
            WorkerStatus::Online
        );

        let unknown = ping(STALE_BYTES, 2_000, None);
        assert_eq!(
            status_of_with_lag(&unknown, Some(58132), None),
            WorkerStatus::Online
        );
    }

    #[test]
    fn configured_tolerance_is_honored() {
        let lagging = ping(STALE_BYTES, 2_000, Some(58122));
        assert_eq!(
            status_of_with_lag(&lagging, Some(58132), Some(10)),
            WorkerStatus::Online
        );
        assert_eq!(
            status_of_with_lag(&lagging, Some(58132), Some(9)),
            WorkerStatus::StaleRpc
        );
        // Zero tolerance: anything but the newest epoch is stale.
        assert_eq!(
            status_of_with_lag(&ping(STALE_BYTES, 2_000, Some(58131)), Some(58132), Some(0)),
            WorkerStatus::StaleRpc
        );
    }

    #[test]
    fn stale_rpc_yields_to_the_more_serious_statuses() {
        // Those mean the worker can't serve data; a frozen epoch only matters for one that can.
        let low_storage = ping(STALE_BYTES - 1, 2_000, Some(56354));
        assert_eq!(status_of(&low_storage, Some(58132)), WorkerStatus::Stale);

        let offline = ping(STALE_BYTES, INACTIVE_BEFORE - 1, Some(56354));
        assert_eq!(status_of(&offline, Some(58132)), WorkerStatus::Offline);

        let mut old = ping(STALE_BYTES, 2_000, Some(56354));
        old.version = "2.9.0".to_owned();
        assert_eq!(
            status_of(&old, Some(58132)),
            WorkerStatus::UnsupportedVersion
        );

        // Versions predating the field report no epoch, but only min_supported_worker_version
        // keeps them out of stale_rpc.
        let mut old_without_epoch = ping(STALE_BYTES, 2_000, None);
        old_without_epoch.version = "2.9.0".to_owned();
        assert_eq!(
            status_of(&old_without_epoch, Some(58132)),
            WorkerStatus::UnsupportedVersion
        );
    }

    #[test]
    fn stale_rpc_is_unreliable_and_serializes_as_stale_rpc() {
        let worker = Worker {
            id: "12D3KooWR1VSmCyVGTc8ewcm3LW82VeT68rjCKjBvGfpkt6ALxPx"
                .parse()
                .unwrap(),
            status: WorkerStatus::StaleRpc,
            version: Version::parse("2.13.0").ok(),
        };
        assert!(!worker.reliable());
        assert_eq!(worker.status.to_string(), "stale_rpc");

        let json = serde_json::to_value(&worker).unwrap();
        assert_eq!(json["status"], "stale_rpc");
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
