use itertools::Itertools;
use libp2p_identity::PeerId;
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator,
};

use crate::types::{Assignment, ChunkIndex, WorkerIndex};

type RingIndex = u16;

const N_RINGS: usize = 6000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    pub id: String,
    pub size: u32,
    pub replication: u8,
}

pub fn distribute(chunks: &[Chunk], workers: Vec<PeerId>, worker_capacity: u64) -> Assignment {
    println!("Hashing workers");
    let rings: Vec<Vec<(u64, WorkerIndex)>> = (0..N_RINGS as RingIndex)
        .into_par_iter()
        .map(|ring_index| {
            let mut vec = workers
                .iter()
                .enumerate()
                .map(|(worker_index, worker_id)| {
                    (
                        hash(&format!("{}:{}", worker_id, ring_index)),
                        worker_index as WorkerIndex,
                    )
                })
                .collect_vec();
            vec.sort_unstable();
            vec
        })
        .collect();

    println!("Hashing chunks");
    let orderings: Vec<(ChunkIndex, RingIndex, WorkerIndex)> = chunks
        .par_iter()
        .enumerate()
        .flat_map_iter(|(chunk_index, chunk)| {
            let rings = &rings;
            (0..chunk.replication).map(move |tag| {
                let chunk_hash = hash(&format!("{}:{}", chunk.id, tag));
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
        .collect();

    println!("Distributing chunks");
    let mut results: Vec<WorkerAssignment> = workers
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
                let worker = &mut results[worker_index as usize];

                // indexes are added in increasing order, so it's enough to check the last one for duplicates
                if worker.allocated + chunk.size as u64 <= worker_capacity
                    && worker.chunks.last() != Some(&chunk_index)
                {
                    worker.allocated += chunk.size as u64;
                    worker.chunks.push(chunk_index);
                    return;
                }
            }
            panic!("No worker found for chunk {}", chunk.id);
        });
    drop(rings);

    Assignment {
        workers: results
            .into_iter()
            .enumerate()
            .map(|(id, worker)| (workers[id], worker.chunks))
            .collect(),
    }
}

struct WorkerAssignment {
    chunks: Vec<ChunkIndex>,
    allocated: u64,
}

fn hash(str: &str) -> u64 {
    seahash::hash(str.as_bytes())
}
