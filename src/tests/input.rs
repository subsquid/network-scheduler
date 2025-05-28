use itertools::Itertools;
use libp2p_identity::PeerId;
use rand::prelude::*;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use crate::{
    scheduling::WeightedChunk,
    types::{ChunkIndex, ChunkWeight, Worker, WorkerIndex, WorkerStatus},
};

#[tracing::instrument(skip_all)]
pub fn generate_input(
    n_workers: WorkerIndex,
    n_chunks: ChunkIndex,
    weights: &[ChunkWeight],
) -> (Vec<WeightedChunk>, Vec<Worker>, u64) {
    println!("Generating input");
    let chunks = generate_chunks(n_chunks, weights);
    let workers = generate_workers(n_workers)
        .into_iter()
        .map(|id| Worker {
            id,
            status: WorkerStatus::Online,
        })
        .collect_vec();
    let total_size: u64 = chunks.iter().map(|chunk| chunk.size as u64).sum();
    let per_worker = total_size / workers.len() as u64;
    println!(
        "Total chunks: {}, total size: {}GB, per worker: {}GB",
        chunks.len(),
        total_size / (1 << 30),
        per_worker / (1 << 30),
    );
    (chunks, workers, total_size)
}

pub fn generate_chunks(n: ChunkIndex, weights: &[ChunkWeight]) -> Vec<WeightedChunk> {
    const MAX_CHUNK_SIZE: u32 = 200 << 20; // 200MB

    (0..n)
        .into_par_iter()
        .map(|i| {
            let id = format!(
                "s3://solana-mainnet-0/0000000000/{:010}-{:010}-{:08x}",
                i,
                i + 1,
                i
            );
            WeightedChunk {
                id,
                size: rand::rng().random_range(0..MAX_CHUNK_SIZE),
                weight: *weights.choose(&mut rand::rng()).unwrap(),
            }
        })
        .collect()
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
