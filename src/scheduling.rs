use itertools::Itertools;
use libp2p_identity::PeerId;
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator,
};
use seahash::hash;
use tracing::instrument;

use crate::types::{Assignment, ChunkIndex, WorkerIndex};

type RingIndex = u16;

const N_RINGS: usize = 6000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    pub id: String,
    pub size: u32,
    pub replication: u16,
}

#[instrument(skip_all)]
pub fn distribute(chunks: &[Chunk], workers: Vec<PeerId>, worker_capacity: u64) -> Assignment {
    let rings = hash_workers(&workers);
    let orderings = hash_chunks(chunks, &rings);
    assign_chunks(orderings, chunks, &workers, rings, worker_capacity)
}

#[instrument(skip_all)]
fn hash_workers(workers: &[PeerId]) -> Vec<Vec<(u64, WorkerIndex)>> {
    tracing::info!("Hashing workers");

    (0..N_RINGS as RingIndex)
        .into_par_iter()
        .map(|ring_index| {
            let mut vec = workers
                .iter()
                .enumerate()
                .map(|(worker_index, worker_id)| {
                    let buffer = [
                        worker_id.to_bytes().as_slice(),
                        b":",
                        &ring_index.to_le_bytes(),
                    ]
                    .concat();
                    (hash(&buffer), worker_index as WorkerIndex)
                })
                .collect_vec();
            vec.sort_unstable();
            vec
        })
        .collect()
}

#[instrument(skip_all)]
fn hash_chunks(
    chunks: &[Chunk],
    rings: &[Vec<(u64, u16)>],
) -> Vec<(ChunkIndex, RingIndex, WorkerIndex)> {
    tracing::info!("Hashing chunks");

    chunks
        .par_iter()
        .enumerate()
        .flat_map_iter(|(chunk_index, chunk)| {
            (0..chunk.replication).map(move |tag| {
                let buffer = [chunk.id.as_bytes(), b":", &tag.to_le_bytes()].concat();
                let chunk_hash = hash(&buffer);
                let ring_index = chunk_hash as usize % N_RINGS;
                let ring = &rings[ring_index];
                let first = ring.partition_point(|(x, _)| *x < chunk_hash);
                (
                    chunk_index as ChunkIndex,
                    ring_index as RingIndex,
                    first as WorkerIndex,
                )
            })
        })
        .collect()
}

#[instrument(skip_all)]
fn assign_chunks(
    orderings: Vec<(ChunkIndex, RingIndex, WorkerIndex)>,
    chunks: &[Chunk],
    worker_ids: &[PeerId],
    rings: Vec<Vec<(u64, WorkerIndex)>>,
    worker_capacity: u64,
) -> Assignment {
    tracing::info!("Assigning chunks");

    struct WorkerAssignment {
        chunks: Vec<ChunkIndex>,
        allocated: u64,
    }

    let mut workers: Vec<WorkerAssignment> = worker_ids
        .iter()
        .map(|_| WorkerAssignment {
            chunks: Vec::new(),
            allocated: 0,
        })
        .collect();
    orderings
        .into_iter()
        .for_each(|(chunk_index, ring_index, first)| {
            let chunk = &chunks[chunk_index as usize];
            let ring = &rings[ring_index as usize];
            let first = first as usize;
            let candidates = ring[first..].iter().chain(ring[..first].iter());
            for &(_, worker_index) in candidates {
                let worker = &mut workers[worker_index as usize];

                if worker.allocated + chunk.size as u64 <= worker_capacity
                    // indexes are added in increasing order, so it's enough to check the last one for duplicates
                    && worker.chunks.last() != Some(&chunk_index)
                {
                    worker.allocated += chunk.size as u64;
                    worker.chunks.push(chunk_index);
                    return;
                }
            }
            panic!("No worker found for chunk {}", chunk.id);
        });

    Assignment {
        workers: workers
            .into_iter()
            .enumerate()
            .map(|(id, worker)| (worker_ids[id], worker.chunks))
            .collect(),
    }
}
