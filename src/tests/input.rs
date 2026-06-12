use itertools::Itertools;
use libp2p_identity::PeerId;
use rand::prelude::*;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use semver::Version;

use crate::{
    scheduling::ScheduledChunk,
    types::{ChunkIndex, ChunkWeight, Worker, WorkerIndex, WorkerStatus},
};

pub struct TestChunkEntry {
    pub chunk_id: String,
    pub size: u32,
    pub weight: ChunkWeight,
    pub minimum_worker_version: Option<Version>,
}

pub struct TestChunks {
    pub dataset: String,
    pub entries: Vec<TestChunkEntry>,
}

impl TestChunks {
    pub fn as_scheduled(&self) -> Vec<ScheduledChunk<'_>> {
        self.entries
            .iter()
            .map(|e| ScheduledChunk {
                dataset: &self.dataset,
                chunk_id: &e.chunk_id,
                size: e.size,
                weight: e.weight,
                minimum_worker_version: e.minimum_worker_version.as_ref(),
                hashes: Vec::new(),
            })
            .collect()
    }
}

#[tracing::instrument(skip_all)]
pub fn generate_input(
    n_workers: WorkerIndex,
    n_chunks: ChunkIndex,
    weights: &[ChunkWeight],
) -> (TestChunks, Vec<Worker>, u64) {
    println!("Generating input");
    let test_chunks = generate_chunks(n_chunks, weights);
    let workers = generate_workers(n_workers)
        .into_iter()
        .map(|id| Worker {
            id,
            status: WorkerStatus::Online,
            version: None,
        })
        .collect_vec();
    let total_size: u64 = test_chunks.entries.iter().map(|e| e.size as u64).sum();
    let per_worker = total_size / workers.len() as u64;
    println!(
        "Total chunks: {}, total size: {}GB, per worker: {}GB",
        test_chunks.entries.len(),
        total_size / (1 << 30),
        per_worker / (1 << 30),
    );
    (test_chunks, workers, total_size)
}

pub fn generate_chunks(n: ChunkIndex, weights: &[ChunkWeight]) -> TestChunks {
    const MAX_CHUNK_SIZE: u32 = 200 << 20; // 200MB

    let entries = (0..n)
        .into_par_iter()
        .map(|i| TestChunkEntry {
            chunk_id: format!("0000000000/{:010}-{:010}-{:08x}", i, i + 1, i),
            size: rand::rng().random_range(0..MAX_CHUNK_SIZE),
            weight: *weights.choose(&mut rand::rng()).unwrap(),
            minimum_worker_version: None,
        })
        .collect();
    TestChunks {
        dataset: "s3://solana-mainnet-0".to_string(),
        entries,
    }
}

pub fn generate_workers(n: WorkerIndex) -> Vec<PeerId> {
    (0..n)
        .map(|i| {
            let mut bytes = vec![0; 32];
            bytes.extend_from_slice(&i.to_le_bytes());
            libp2p_identity::PeerId::from_multihash(
                multihash::Multihash::wrap(0x0, &bytes).unwrap(),
            )
            .expect("The digest size is never too large")
        })
        .collect()
}
