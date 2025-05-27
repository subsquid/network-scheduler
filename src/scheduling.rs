use std::borrow::Cow;

use itertools::Itertools;
use libp2p_identity::PeerId;
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator,
};
use seahash::hash;
use tracing::instrument;

use crate::{
    replication::{ReplicationError, calc_replication_factors},
    types::{Assignment, ChunkIndex, Worker, WorkerIndex},
};

type RingIndex = u16;

const N_RINGS: usize = 6000;

#[derive(Debug, Clone)]
pub struct SchedulingConfig {
    pub worker_capacity: u64,
    pub saturation: f64,
    pub min_replication: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct WeightedChunk {
    pub id: String,
    pub size: u32,
    pub weight: u16,
}

pub fn schedule(
    chunks: &[WeightedChunk],
    workers: &[Worker],
    config: SchedulingConfig,
) -> Result<Assignment, ReplicationError> {
    let mut worker_ids = Vec::with_capacity(workers.len());
    worker_ids.extend(workers.iter().filter(|w| w.reliable).map(|w| w.id));
    let reliable_workers = worker_ids.len();
    worker_ids.extend(workers.iter().filter(|w| !w.reliable).map(|w| w.id));

    tracing::info!(
        "Scheduling {} chunks to {} reliable workers",
        chunks.len(),
        reliable_workers
    );
    let reliable = schedule_to_workers(chunks, &worker_ids[..reliable_workers], &config)?;

    if reliable_workers == workers.len() {
        return Ok(reliable);
    }

    tracing::info!(
        "Scheduling {} chunks to {} total workers",
        chunks.len(),
        workers.len()
    );
    let mut all = schedule_to_workers(chunks, &worker_ids, &config)?;

    // Use assignment from `reliable` for reliable workers and from `all` for unreliable workers
    for (worker_id, chunk_indexes) in reliable.workers {
        all.workers.insert(worker_id, chunk_indexes);
    }
    Ok(all)
}

fn schedule_to_workers(
    chunks: &[WeightedChunk],
    workers: &[PeerId],
    config: &SchedulingConfig,
) -> Result<Assignment, ReplicationError> {
    let size_by_weight = chunks
        .iter()
        .map(|chunk| (chunk.weight, chunk.size as u64))
        .into_grouping_map()
        .sum()
        .into_iter()
        .collect();

    let total_capacity =
        (config.worker_capacity as f64 * workers.len() as f64 * config.saturation) as u64;

    let mapping = calc_replication_factors(size_by_weight, total_capacity, config.min_replication)?;

    let chunks = chunks
        .iter()
        .map(|c| ReplicatedChunk {
            id: Cow::Borrowed(&c.id),
            replication: mapping[&c.weight],
            size: c.size,
        })
        .collect_vec();
    debug_assert!(chunks.is_sorted_by_key(|c| &c.id));

    tracing::info!(
        "Replication factors by weight: {:?}, total vchunks: {}",
        mapping,
        chunks.iter().map(|c| c.replication as u64).sum::<u64>()
    );

    Ok(distribute(&chunks, workers, config.worker_capacity))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplicatedChunk<'id> {
    id: Cow<'id, str>,
    size: u32,
    replication: u16,
}

#[instrument(skip_all)]
fn distribute(chunks: &[ReplicatedChunk], workers: &[PeerId], worker_capacity: u64) -> Assignment {
    let rings = hash_workers(workers);
    let orderings = hash_chunks(chunks, &rings);
    assign_chunks(orderings, chunks, workers, rings, worker_capacity)
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
    chunks: &[ReplicatedChunk],
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
    chunks: &[ReplicatedChunk],
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
            .map(|(index, worker)| (worker_ids[index], worker.chunks))
            .collect(),
    }
}
