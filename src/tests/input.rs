use libp2p_identity::PeerId;
use rand::Rng;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use crate::{
    scheduling::Chunk,
    types::{ChunkIndex, WorkerIndex},
};

pub fn generate_input(
    n_workers: WorkerIndex,
    n_chunks: ChunkIndex,
) -> (Vec<Chunk>, Vec<PeerId>, u64) {
    println!("Generating input");
    let chunks = generate_chunks(n_chunks);
    let workers = generate_workers(n_workers);
    let total_size: u64 = chunks
        .iter()
        .map(|chunk| chunk.size as u64 * chunk.replication as u64)
        .sum();
    let total_vchunks = chunks
        .iter()
        .map(|chunk| chunk.replication as u64)
        .sum::<u64>();
    let per_worker = total_size / workers.len() as u64;
    println!(
        "Total chunks: {}, virtual chunks: {}, total size: {}GB, per worker: {}GB",
        chunks.len(),
        total_vchunks,
        total_size / (1 << 30),
        per_worker / (1 << 30),
    );
    (chunks, workers, total_size)
}

fn generate_chunks(n: ChunkIndex) -> Vec<Chunk> {
    const MAX_CHUNK_SIZE: u32 = 200 * 1 << 20; // 200MB

    (0..n)
        .into_par_iter()
        .map(|i| {
            let id = format!(
                "s3://solana-mainnet-0/0000000000/{:010}-{:010}-{:08x}",
                i,
                i + 1,
                i
            );
            Chunk {
                id,
                size: rand::rng().random_range(0..MAX_CHUNK_SIZE),
                replication: rand::rng().random_range(1..=60),
            }
        })
        .collect()
}

fn generate_workers(n: WorkerIndex) -> Vec<PeerId> {
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
