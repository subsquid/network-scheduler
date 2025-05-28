use std::{
    collections::BTreeMap,
    sync::{Arc, atomic::AtomicU64},
};

use libp2p_identity::PeerId;
use prometheus_client::{
    metrics::{family::Family, gauge::Gauge},
    registry::{Registry, Unit},
};

use crate::types::{Chunk, Worker};

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

    pub static ref ASSIGNMENT_JSON_SIZE: Gauge = Default::default();
    pub static ref ASSIGNMENT_COMPRESSED_JSON_SIZE: Gauge = Default::default();

    pub static ref FAILURE: Family<Labels, Gauge> = Default::default();
    pub static ref EXEC_TIMES: Family<Labels, Gauge<f64, AtomicU64>> = Default::default();
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

pub fn report_chunks(chunks: &BTreeMap<Arc<String>, Vec<Chunk>>) {
    DATASET_CHUNKS.clear();
    DATASET_SIZE.clear();
    for (dataset, chunks) in chunks {
        let labels = vec![("dataset", dataset.to_string())];
        DATASET_CHUNKS
            .get_or_create(&labels)
            .set(chunks.len() as i64);
        DATASET_SIZE
            .get_or_create(&labels)
            .set(chunks.iter().map(|c| c.size as i64).sum());
    }
}

pub fn report_worker_stats(worker: PeerId, num_chunks: usize, bytes: u64) {
    let labels = vec![("worker", worker.to_string())];
    ASSIGNED_CHUNKS
        .get_or_create(&labels)
        .set(num_chunks as i64);
    ASSIGNED_SIZE.get_or_create(&labels).set(bytes as i64);
}

pub fn report_replication_factors(iter: impl IntoIterator<Item = (String, u16)>) {
    REPLICATION_FACTOR.clear();
    for (dataset, factor) in iter {
        REPLICATION_FACTOR
            .get_or_create(&vec![("dataset", dataset.to_string())])
            .set(factor as i64);
    }
}

pub fn failure(target: impl Into<String>) {
    FAILURE
        .get_or_create(&vec![("target", target.into())])
        .set(1);
}

pub fn register_metrics() -> Registry {
    let mut registry = Registry::with_prefix("scheduler");

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
