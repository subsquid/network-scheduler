use std::collections::BTreeMap;

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
    let mut capacities: Vec<u64> = workers.iter().map(|_| worker_capacity).collect();
    let distribution: Vec<(ChunkIndex, WorkerIndex)> = orderings
        .into_iter()
        .map(|(chunk_index, ring_index, first)| {
            let chunk = &chunks[chunk_index as usize];
            let ring = &rings[ring_index as usize];
            let first = first as usize;
            let candidates = ring[first..].iter().chain(ring[..first].iter());
            for &(_, worker_index) in candidates {
                let capacity = &mut capacities[worker_index as usize];
                if *capacity >= chunk.size as u64 {
                    *capacity -= chunk.size as u64;
                    return (chunk_index, worker_index);
                }
            }
            panic!("No worker found for chunk {}", chunk.id);
        })
        .collect();
    drop(rings);
    drop(capacities);

    println!("Packing assignments");
    let mut results: BTreeMap<PeerId, Vec<ChunkIndex>> = BTreeMap::new();
    for (chunk_index, worker_index) in distribution {
        results
            .entry(workers[worker_index as usize].clone())
            .or_default()
            .push(chunk_index);
    }
    Assignment { workers: results }
}

fn hash(str: &str) -> u64 {
    seahash::hash(str.as_bytes())
}
