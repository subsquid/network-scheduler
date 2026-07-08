use std::{borrow::Cow, sync::atomic::AtomicU64};

use libp2p_identity::PeerId;
use prometheus_client::{
    metrics::{family::Family, gauge::Gauge},
    registry::{Registry, Unit},
};

use crate::types::Worker;

type Labels = Vec<(&'static str, String)>;

lazy_static::lazy_static! {
    pub static ref DATASET_CHUNKS: Family<Labels, Gauge> = Default::default();
    pub static ref DATASET_SIZE: Family<Labels, Gauge> = Default::default();
    pub static ref ASSIGNED_CHUNKS: Family<Labels, Gauge> = Default::default();
    pub static ref ASSIGNED_SIZE: Family<Labels, Gauge> = Default::default();
    pub static ref WORKER_STATUS: Family<Labels, Gauge> = Default::default();

    pub static ref REPLICATION_FACTOR: Family<Labels, Gauge> = Default::default();
    pub static ref WEIGHT_MULTIPLIER: Gauge<f64, AtomicU64> = Default::default();

    pub static ref S3_REQUESTS: Gauge = Default::default();
    pub static ref S3_KEYS_LISTED: Gauge = Default::default();
    pub static ref S3_SUMMARY_DOWNLOAD_RETRIES: Gauge = Default::default();

    pub static ref ASSIGNMENT_JSON_SIZE: Gauge = Default::default();
    pub static ref ASSIGNMENT_COMPRESSED_JSON_SIZE: Gauge = Default::default();
    pub static ref ASSIGNMENT_FB_SIZE: Gauge = Default::default();
    pub static ref ASSIGNMENT_COMPRESSED_FB_SIZE: Gauge = Default::default();

    pub static ref PROMOTION_HELD_BACK: Family<Labels, Gauge> = Default::default();
    pub static ref REGISTRATION_REJECTED: Family<Labels, Gauge> = Default::default();

    pub static ref FAILURE: Family<Labels, Gauge> = Default::default();
    pub static ref EXEC_TIMES: Family<Labels, Gauge<f64, AtomicU64>> = Default::default();
    pub static ref ASSIGNMENT_TIMESTAMP: Gauge = Default::default();
}

pub fn report_workers(workers: &[Worker]) {
    WORKER_STATUS.clear();
    for worker in workers {
        WORKER_STATUS
            .get_or_create(&vec![
                ("worker", worker.id.to_string()),
                ("status", worker.status.to_string()),
            ])
            .set(1);
    }
}

pub struct DatasetSegmentStats {
    pub from: i64,
    pub num_chunks: u32,
    pub total_size: u64,
}

pub fn report_chunks(dataset: &str, seg_stats: &[DatasetSegmentStats]) {
    let default = seg_stats.len() == 1 && seg_stats[0].from == 0;
    for stats in seg_stats {
        let labels = vec![
            (
                "dataset_segment",
                format_segment_label(dataset, (!default).then_some(stats.from)),
            ),
            ("dataset", dataset.to_string()),
        ];
        DATASET_SIZE
            .get_or_create(&labels)
            .set(stats.total_size as i64);
        DATASET_CHUNKS
            .get_or_create(&labels)
            .set(stats.num_chunks as i64);
    }
}

pub fn report_worker_stats(worker: PeerId, num_chunks: usize, bytes: u64) {
    let labels = vec![("worker", worker.to_string())];
    ASSIGNED_CHUNKS
        .get_or_create(&labels)
        .set(num_chunks as i64);
    ASSIGNED_SIZE.get_or_create(&labels).set(bytes as i64);
}

pub fn report_replication_factors(
    iter: impl IntoIterator<Item = (String, impl IntoIterator<Item = (i64, u16)>)>,
) {
    REPLICATION_FACTOR.clear();
    for (dataset, segments) in iter {
        let segments: Vec<_> = segments.into_iter().collect();
        let default = segments.len() == 1 && segments[0].0 == 0;
        for (from, factor) in segments {
            REPLICATION_FACTOR
                .get_or_create(&vec![
                    (
                        "dataset_segment",
                        format_segment_label(&dataset, (!default).then_some(from)),
                    ),
                    ("dataset", dataset.clone()),
                ])
                .set(factor as i64);
        }
    }
}

/// Chunks held back from promotion this cycle for overlapping a visible chunk, by dataset.
/// Cleared and re-set each cycle.
pub fn report_promotion_held_back(counts: impl IntoIterator<Item = (String, i64)>) {
    PROMOTION_HELD_BACK.clear();
    for (dataset, count) in counts {
        PROMOTION_HELD_BACK
            .get_or_create(&vec![("dataset", dataset)])
            .set(count);
    }
}

/// Chunks rejected at registration this cycle for overlapping a live chunk, by dataset.
/// Cleared and re-set each cycle.
pub fn report_registration_rejected(counts: impl IntoIterator<Item = (String, i64)>) {
    REGISTRATION_REJECTED.clear();
    for (dataset, count) in counts {
        REGISTRATION_REJECTED
            .get_or_create(&vec![("dataset", dataset)])
            .set(count);
    }
}

pub fn failure(target: impl Into<String>) {
    FAILURE
        .get_or_create(&vec![("target", target.into())])
        .set(1);
}

fn format_segment_label(dataset: &str, from: Option<i64>) -> String {
    if let Some(block) = from {
        format!("{}[{}..]", dataset, block)
    } else {
        dataset.to_string()
    }
}

pub fn register_metrics(network: String) -> Registry {
    let mut registry = Registry::with_prefix_and_labels(
        "scheduler",
        [(Cow::Borrowed("network"), Cow::Owned(network))].into_iter(),
    );

    registry.register(
        "dataset_chunks",
        "Number of total chunks in the dataset",
        DATASET_CHUNKS.clone(),
    );
    registry.register_with_unit(
        "dataset_size",
        "Total size of all chunks of the dataset",
        Unit::Bytes,
        DATASET_SIZE.clone(),
    );
    registry.register(
        "assigned_chunks",
        "Number of chunks assigned to the worker",
        ASSIGNED_CHUNKS.clone(),
    );
    registry.register_with_unit(
        "assigned_size",
        "Total size of all chunks assigned to the worker",
        Unit::Bytes,
        ASSIGNED_SIZE.clone(),
    );
    registry.register(
        "worker_status",
        "Status of the worker",
        WORKER_STATUS.clone(),
    );
    registry.register(
        "replication_factor",
        "Replication factor of the dataset",
        REPLICATION_FACTOR.clone(),
    );
    registry.register(
        "weight_multiplier",
        "How many chunk replicas are equivalent to 1 weight unit",
        WEIGHT_MULTIPLIER.clone(),
    );
    registry.register(
        "s3_requests",
        "Number of S3 listing requests made on the last execution",
        S3_REQUESTS.clone(),
    );
    registry.register(
        "s3_keys_listed",
        "Number of S3 keys listed on the last execution",
        S3_KEYS_LISTED.clone(),
    );
    registry.register(
        "s3_summary_download_retries",
        "Number of chunk summary download retries on the last execution",
        S3_SUMMARY_DOWNLOAD_RETRIES.clone(),
    );
    registry.register_with_unit(
        "assignment_json_size",
        "Size of the raw assignment JSON",
        Unit::Bytes,
        ASSIGNMENT_JSON_SIZE.clone(),
    );
    registry.register_with_unit(
        "assignment_compressed_json_size",
        "Size of the assignment JSON after compression",
        Unit::Bytes,
        ASSIGNMENT_COMPRESSED_JSON_SIZE.clone(),
    );
    registry.register_with_unit(
        "assignment_fb_size",
        "Size of the raw flatbuffer assignment",
        Unit::Bytes,
        ASSIGNMENT_FB_SIZE.clone(),
    );
    registry.register_with_unit(
        "assignment_compressed_fb_size",
        "Size of the compressed flatbuffer assignment",
        Unit::Bytes,
        ASSIGNMENT_COMPRESSED_FB_SIZE.clone(),
    );
    registry.register(
        "promotion_held_back",
        "Chunks held back from portal promotion due to a block-range overlap, by dataset",
        PROMOTION_HELD_BACK.clone(),
    );
    registry.register(
        "registration_rejected",
        "Chunks rejected at registration due to a block-range overlap, by dataset",
        REGISTRATION_REJECTED.clone(),
    );
    registry.register(
        "failure",
        "Failures during the scheduling process",
        FAILURE.clone(),
    );
    registry.register_with_unit(
        "exec_times",
        "Execution times of various procedures",
        Unit::Seconds,
        EXEC_TIMES.clone(),
    );
    registry.register_with_unit(
        "assignment_timestamp",
        "Timestamp of the last assignment",
        Unit::Seconds,
        ASSIGNMENT_TIMESTAMP.clone(),
    );

    registry
}

pub fn encode_metrics(registry: &Registry) -> anyhow::Result<String> {
    let mut buffer = String::new();
    prometheus_client::encoding::text::encode(&mut buffer, registry)?;
    Ok(buffer)
}

pub struct Timer {
    start: std::time::Instant,
    name: &'static str,
}

impl Timer {
    pub fn new(name: &'static str) -> Self {
        Self {
            start: std::time::Instant::now(),
            name,
        }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        EXEC_TIMES
            .get_or_create(&vec![("name", self.name.to_string())])
            .inc_by(elapsed.as_secs_f64());
        tracing::debug!("{} finished in {:?}", self.name, elapsed);
    }
}

/// Times a Postgres storage phase, counting the SQL statements it issues and rows they touch. On
/// drop it emits one `debug` line on the `pg_timing` target (select with
/// `RUST_LOG=error,pg_timing=debug`) plus an `EXEC_TIMES` sample. The statement count distinguishes
/// an N+1 storm (statements ≈ rows) from a single slow query.
pub struct PhaseTimer {
    start: std::time::Instant,
    name: &'static str,
    statements: u64,
    rows: u64,
}

impl PhaseTimer {
    pub fn new(name: &'static str) -> Self {
        Self {
            start: std::time::Instant::now(),
            name,
            statements: 0,
            rows: 0,
        }
    }

    /// Record one executed statement that touched `rows` rows. Call once per round-trip — inside a
    /// per-row loop this is what surfaces the round-trip count.
    pub fn stmt(&mut self, rows: u64) {
        self.statements += 1;
        self.rows += rows;
    }

    /// Add to the row count without counting a statement, so a CPU phase reports `statements = 0`.
    pub fn items(&mut self, rows: u64) {
        self.rows += rows;
    }
}

impl Drop for PhaseTimer {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        EXEC_TIMES
            .get_or_create(&vec![("name", self.name.to_string())])
            .inc_by(elapsed.as_secs_f64());
        tracing::debug!(
            target: "pg_timing",
            phase = self.name,
            statements = self.statements,
            rows = self.rows,
            elapsed_ms = elapsed.as_millis() as u64,
            "pg phase complete"
        );
    }
}
