use std::collections::BTreeMap;

use itertools::Itertools;
use libp2p_identity::PeerId;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use seahash::hash;
use tracing::instrument;

use semver::Version;

use crate::{
    replication::{ReplicationError, calc_replication_factors},
    types::{Assignment, ChunkIndex, ChunkWeight, ReplicationFactor, Worker, WorkerIndex},
};

type RingIndex = u16;

const N_RINGS: usize = 6000;

/// Chunk indices assigned to each worker.
type WorkerChunks = BTreeMap<PeerId, Vec<ChunkIndex>>;

#[derive(Debug, Clone)]
pub struct SchedulingConfig {
    pub worker_capacity: u64,
    pub saturation: f64,
    pub min_replication: ReplicationFactor,
    pub ignore_reliability: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ScheduledChunk<'a> {
    pub dataset: &'a str,
    pub chunk_id: &'a str,
    pub size: u32,
    pub weight: ChunkWeight,
    pub minimum_worker_version: Option<&'a Version>,
}

pub fn schedule(
    chunks: &[ScheduledChunk<'_>],
    workers: &[Worker],
    config: SchedulingConfig,
) -> Result<Assignment, ReplicationError> {
    let _timer = crate::metrics::Timer::new("schedule");

    // Order workers reliable-first, schedule the reliable subset, then (unless
    // reliability is ignored) overlay a pass over all workers so unreliable workers
    // also get chunks.
    let mut sorted = Vec::with_capacity(workers.len());
    sorted.extend(workers.iter().filter(|w| w.reliable()));
    let reliable = sorted.len();
    sorted.extend(workers.iter().filter(|w| !w.reliable()));

    if config.ignore_reliability {
        tracing::info!(
            "Scheduling {} chunks to {} workers",
            chunks.len(),
            workers.len()
        );
        return schedule_to_workers(chunks, &sorted, &config);
    }

    tracing::info!(
        "Scheduling {} chunks to {} reliable workers",
        chunks.len(),
        reliable
    );
    let mut assignment = schedule_to_workers(chunks, &sorted[..reliable], &config)?;
    if reliable == workers.len() {
        return Ok(assignment);
    }

    tracing::info!(
        "Scheduling {} chunks to {} total workers",
        chunks.len(),
        workers.len()
    );
    let all = schedule_to_workers(chunks, &sorted, &config)?;
    // Reliable workers keep their assignment; unreliable ones take theirs from `all`.
    for (worker_id, chunk_indexes) in all.worker_chunks {
        assignment
            .worker_chunks
            .entry(worker_id)
            .or_insert(chunk_indexes);
    }
    Ok(assignment)
}

fn schedule_to_workers(
    chunks: &[ScheduledChunk<'_>],
    workers: &[&Worker],
    config: &SchedulingConfig,
) -> Result<Assignment, ReplicationError> {
    // The byte-budget replication is a cap; `distribute` places what fits and reports
    // the factor actually achieved.
    let caps = replication_cap(chunks, workers.len(), config)?;
    let replicated = to_replicated(chunks, &caps);
    let (worker_chunks, achieved) = distribute(
        &replicated,
        workers,
        config.worker_capacity,
        config.min_replication,
    )?;
    let replication_by_weight = min_replication_by_weight(chunks, &achieved);
    tracing::info!("Replication by weight: caps {caps:?}, achieved {replication_by_weight:?}");
    Ok(Assignment {
        worker_chunks,
        replication_by_weight,
    })
}

/// Per-weight replication derived from the saturated byte budget (always at least
/// `config.min_replication`).
fn replication_cap(
    chunks: &[ScheduledChunk<'_>],
    n_workers: usize,
    config: &SchedulingConfig,
) -> Result<BTreeMap<ChunkWeight, ReplicationFactor>, ReplicationError> {
    let size_by_weight = chunks
        .iter()
        .map(|chunk| (chunk.weight, chunk.size as u64))
        .into_grouping_map()
        .sum()
        .into_iter()
        .collect();
    let total_capacity =
        (config.worker_capacity as f64 * n_workers as f64 * config.saturation) as u64;
    calc_replication_factors(
        size_by_weight,
        total_capacity,
        config.min_replication,
        n_workers as ReplicationFactor,
    )
}

/// Expand `chunks` into `ReplicatedChunk`s, taking each chunk's replication factor from
/// `replication_by_weight`.
fn to_replicated<'a>(
    chunks: &[ScheduledChunk<'a>],
    replication_by_weight: &BTreeMap<ChunkWeight, ReplicationFactor>,
) -> Vec<ReplicatedChunk<'a>> {
    chunks
        .iter()
        .map(|c| ReplicatedChunk {
            dataset: c.dataset,
            chunk_id: c.chunk_id,
            replication: replication_by_weight[&c.weight],
            size: c.size,
            minimum_worker_version: c.minimum_worker_version,
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReplicatedChunk<'a> {
    pub(crate) dataset: &'a str,
    pub(crate) chunk_id: &'a str,
    pub(crate) size: u32,
    pub(crate) replication: ReplicationFactor,
    pub(crate) minimum_worker_version: Option<&'a Version>,
}

#[instrument(skip_all)]
fn hash_workers(worker_ids: &[PeerId]) -> Vec<Vec<(u64, WorkerIndex)>> {
    tracing::info!("Hashing workers");
    let _timer = crate::metrics::Timer::new("schedule:distribute:hash_workers");

    (0..N_RINGS as RingIndex)
        .into_par_iter()
        .map(|ring_index| {
            let mut vec = worker_ids
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

/// Hash of a chunk's `tag`-th replica, locating it on the ring.
fn replica_hash(chunk: &ReplicatedChunk, tag: ReplicationFactor) -> u64 {
    let buffer = [
        chunk.dataset.as_bytes(),
        b"/",
        chunk.chunk_id.as_bytes(),
        b":",
        &tag.to_le_bytes(),
    ]
    .concat();
    hash(&buffer)
}

/// The guaranteed replication per weight: the fewest replicas actually placed among the
/// chunks of each weight.
fn min_replication_by_weight(
    chunks: &[ScheduledChunk<'_>],
    achieved: &[ReplicationFactor],
) -> BTreeMap<ChunkWeight, ReplicationFactor> {
    let mut by_weight: BTreeMap<ChunkWeight, ReplicationFactor> = BTreeMap::new();
    for (chunk, &got) in chunks.iter().zip(achieved) {
        by_weight
            .entry(chunk.weight)
            .and_modify(|min| *min = (*min).min(got))
            .or_insert(got);
    }
    by_weight
}

/// Multi-step scatter with a dynamically discovered replication factor. Returns the
/// placement (`worker -> chunk indices`) and the replication actually achieved for each
/// chunk (indexed like `chunks`).
///
/// `chunks[i].replication` is the cap (upper bound) for chunk `i`. Placement runs in
/// three phases, each replica walking the chunk's hash ring from its home:
/// 1. restricted chunks up to `min_replication` — they can only land on eligible
///    workers, which are scarce, so they claim that capacity first;
/// 2. unrestricted chunks up to `min_replication`;
/// 3. both kinds (restricted first) from `min_replication` up to each chunk's cap.
///
/// Phases 1 and 2 are mandatory: a replica that can't be placed there means the data
/// doesn't fit at the required minimum (`NotEnoughCapacity`). Phase 3 is best-effort, so
/// an unreachable cap just yields a lower achieved factor instead of a panic.
#[instrument(skip_all)]
pub(crate) fn distribute(
    chunks: &[ReplicatedChunk],
    workers: &[&Worker],
    worker_capacity: u64,
    min_replication: ReplicationFactor,
) -> Result<(WorkerChunks, Vec<ReplicationFactor>), ReplicationError> {
    let _timer = crate::metrics::Timer::new("schedule:distribute");
    let worker_ids = workers.iter().map(|w| w.id).collect_vec();
    let rings = hash_workers(&worker_ids);
    let mut placement = Placement::new(chunks, workers, &rings, worker_capacity);

    // `partition` keeps each group in input order, so placement stays deterministic.
    let (restricted, unrestricted): (Vec<usize>, Vec<usize>) =
        (0..chunks.len()).partition(|&i| chunks[i].minimum_worker_version.is_some());

    placement.ensure_minimum(&restricted, min_replication)?;
    placement.ensure_minimum(&unrestricted, min_replication)?;

    let order = restricted.into_iter().chain(unrestricted).collect_vec();
    placement.fill_to_cap(&order, min_replication);

    let achieved = placement.achieved_replication();
    Ok((placement.into_worker_chunks(), achieved))
}

/// Mutable state for [`distribute`].
struct Placement<'a> {
    chunks: &'a [ReplicatedChunk<'a>],
    workers: &'a [&'a Worker],
    rings: &'a [Vec<(u64, WorkerIndex)>],
    worker_capacity: u64,
    /// Bytes assigned to each worker.
    allocated: Vec<u64>,
    /// Chunks assigned to each worker — the result.
    worker_chunks: Vec<Vec<ChunkIndex>>,
    /// Workers holding each chunk — the inverse of `worker_chunks`, used to keep two
    /// replicas of a chunk off the same worker.
    chunk_workers: Vec<Vec<WorkerIndex>>,
}

impl<'a> Placement<'a> {
    fn new(
        chunks: &'a [ReplicatedChunk<'a>],
        workers: &'a [&'a Worker],
        rings: &'a [Vec<(u64, WorkerIndex)>],
        worker_capacity: u64,
    ) -> Self {
        Self {
            chunks,
            workers,
            rings,
            worker_capacity,
            allocated: vec![0; workers.len()],
            worker_chunks: vec![Vec::new(); workers.len()],
            chunk_workers: vec![Vec::new(); chunks.len()],
        }
    }

    /// Place `min_replication` replicas of every chunk in `subset`, failing if any of
    /// them can't be placed.
    fn ensure_minimum(
        &mut self,
        subset: &[usize],
        min_replication: ReplicationFactor,
    ) -> Result<(), ReplicationError> {
        for tag in 0..min_replication {
            for &chunk_index in subset {
                if !self.place(chunk_index, tag) {
                    return Err(ReplicationError::NotEnoughCapacity);
                }
            }
        }
        Ok(())
    }

    /// Add replicas from `from_tag` up to each chunk's cap, in `order`, best-effort. A
    /// chunk leaves the rounds once it reaches its cap or fails to place — a phase-3
    /// failure can't succeed at a higher tag (capacity only shrinks, the chunk's worker
    /// set is fixed), so retrying is pure waste and dropping it keeps the result identical.
    fn fill_to_cap(&mut self, order: &[usize], from_tag: ReplicationFactor) {
        let max_cap = order
            .iter()
            .map(|&i| self.chunks[i].replication)
            .max()
            .unwrap_or(0);
        // Chunks still below their cap (`cap > tag` holds at each round's start).
        let mut active: Vec<usize> = order
            .iter()
            .copied()
            .filter(|&i| self.chunks[i].replication > from_tag)
            .collect();
        for tag in from_tag..max_cap {
            active.retain(|&chunk_index| {
                self.place(chunk_index, tag) && self.chunks[chunk_index].replication > tag + 1
            });
            if active.is_empty() {
                break;
            }
        }
    }

    /// Place the `tag`-th replica of `chunk_index` on the first eligible worker with free
    /// capacity, walking the chunk's hash ring from its home. Returns whether it landed.
    fn place(&mut self, chunk_index: usize, tag: ReplicationFactor) -> bool {
        let chunk = &self.chunks[chunk_index];
        let chunk_hash = replica_hash(chunk, tag);
        let ring = &self.rings[chunk_hash as usize % N_RINGS];
        let first = ring.partition_point(|(x, _)| *x < chunk_hash);

        for &(_, worker) in ring[first..].iter().chain(&ring[..first]) {
            let w = worker as usize;
            let eligible = chunk
                .minimum_worker_version
                .is_none_or(|min| self.workers[w].version.as_ref().is_some_and(|v| v >= min));
            if !eligible || self.chunk_workers[chunk_index].contains(&worker) {
                continue;
            }
            if self.allocated[w] + chunk.size as u64 <= self.worker_capacity {
                self.allocated[w] += chunk.size as u64;
                self.worker_chunks[w].push(chunk_index as ChunkIndex);
                self.chunk_workers[chunk_index].push(worker);
                return true;
            }
        }
        false
    }

    /// The replication actually achieved for each chunk (indexed like `chunks`): the
    /// number of distinct workers it ended up on.
    fn achieved_replication(&self) -> Vec<ReplicationFactor> {
        self.chunk_workers
            .iter()
            .map(|w| w.len() as ReplicationFactor)
            .collect()
    }

    /// Consume the state into the final `worker -> sorted chunk indices` map.
    fn into_worker_chunks(self) -> WorkerChunks {
        let Placement {
            workers,
            worker_chunks,
            ..
        } = self;
        worker_chunks
            .into_iter()
            .enumerate()
            .map(|(i, mut indices)| {
                indices.sort_unstable();
                (workers[i].id, indices)
            })
            .collect()
    }
}
