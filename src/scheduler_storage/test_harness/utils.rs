//! Shared, backend-agnostic test utilities for the correction/MVCC suites: peer/worker/chunk
//! builders and a static [`SchedulingAlgorithm`] stub. Both backends' suites import these so a
//! single definition keeps them in lock-step. Read/lookup convenience lives on the
//! [`StorageInspect`](crate::scheduler_storage::test_harness::inspect::StorageInspect) trait.

use std::collections::BTreeMap;
use std::sync::Arc;

use libp2p_identity::PeerId;
use semver::Version;

use crate::scheduler_storage::algorithm::{IdealMapping, ScheduleOutput, SchedulingAlgorithm};
use crate::scheduler_storage::{ChunkPk, WorkerPk};
use crate::types::{Chunk, Worker, WorkerStatus};

/// Deterministic peer id derived from a small seed.
pub fn peer(seed: u8) -> PeerId {
    let keypair = libp2p_identity::Keypair::ed25519_from_bytes([seed; 32]).unwrap();
    keypair.public().to_peer_id()
}

/// An online worker with peer id seeded by `seed`; `version` is parsed via semver when present.
pub fn worker(seed: u8, version: Option<&str>) -> Worker {
    Worker {
        id: peer(seed),
        status: WorkerStatus::Online,
        version: version.map(|v| Version::parse(v).unwrap()),
    }
}

/// Dataset name with the conventional `s3://` prefix.
pub fn dataset(name: &str) -> String {
    format!("s3://{name}")
}

/// Build a `Chunk` without `Chunk::new`, so tests don't depend on `DataChunk` id parsing. `id_seed`
/// sets the id and a two-block range `[2·seed, 2·seed+1]`, so distinct seeds give distinct,
/// non-overlapping chunks.
pub fn chunk(dataset_name: &str, id_seed: u32, size: u32) -> Chunk {
    let first = id_seed as u64 * 2;
    Chunk {
        dataset: Arc::new(dataset(dataset_name)),
        id: Arc::new(format!(
            "0000000000/{:010}-{:010}-{:08x}",
            id_seed,
            id_seed + 1,
            id_seed
        )),
        size,
        blocks: first..=first + 1,
        files: Arc::new(Vec::new()),
        summary: None,
    }
}

/// Like [`chunk`] but with an explicit block range — lets a correction replacement be same-range as
/// its predecessor (the 1:1 same-range invariant) while keeping a distinct id. (`chunk` derives its
/// range from `id_seed`, so a replacement minted from a distinct id needs its range set explicitly.)
pub fn chunk_with_blocks(
    dataset_name: &str,
    id_seed: u32,
    size: u32,
    blocks: std::ops::RangeInclusive<u64>,
) -> Chunk {
    Chunk {
        blocks,
        ..chunk(dataset_name, id_seed, size)
    }
}

/// A [`SchedulingAlgorithm`] that returns a fixed mapping regardless of inputs, so cycle tests
/// don't depend on the real consistent-hashing algorithm and can control placement exactly.
pub struct StaticSchedulingAlgorithm {
    pub mapping: IdealMapping,
}

impl SchedulingAlgorithm for StaticSchedulingAlgorithm {
    type Config = ();

    fn schedule(
        &self,
        _chunks: Vec<(ChunkPk, Chunk)>,
        _workers: Vec<(WorkerPk, Worker)>,
        _current_placement: &BTreeMap<ChunkPk, Vec<WorkerPk>>,
        _config: &(),
    ) -> anyhow::Result<ScheduleOutput> {
        Ok(ScheduleOutput {
            mapping: self.mapping.clone(),
            replication_by_weight: BTreeMap::new(),
        })
    }
}
