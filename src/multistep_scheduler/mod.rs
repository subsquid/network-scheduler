//! Multi-step (reconciliation) scheduling: given the current placement, produces a feasible step
//! toward the ideal rather than a placement-blind ideal.
//!
//! Standalone — not on the production scheduling path.
//!
//! Terminology: **floor** — a chunk's `min_replication` mandatory copies; **bonus** — weight-earned
//! extras above the floor (up to its weight cap), shed first when room is tight; **ideal** —
//! placement on hypothetically-empty workers; **held** — copies physically on a worker now (free to
//! keep).
//! Draining copies are not distinguished from other held copies — that split lives in the caller.
//!
//! Stage 1 ([`ideal_chunk_workers`]) computes the ideal. Stage 2 ([`Reconcile`]) re-walks the same
//! rings against the real footprint: full workers are skipped (spill), held copies cost 0, held
//! copies are added back if fewer than the floor survive, new chunks reach their floor atomically
//! or are excluded, and bonus growth is suppressed while any floor is unmet.

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

/// `current[i]` — **positions into `workers`** of the peers physically holding chunk `i` now,
/// draining copies included. The caller owns the PeerId↔position mapping; copies on a departed
/// worker must be left out (they become a shortfall). Returns the full published placement
/// (new copies plus current copies retained to hold the floor).
pub fn schedule(
    chunks: &[ScheduledChunk<'_>],
    workers: &[Worker],
    config: SchedulingConfig,
    current: &[Vec<WorkerIndex>],
) -> Result<Assignment, ReplicationError> {
    let _timer = crate::metrics::Timer::new("schedule");
    debug_assert_eq!(chunks.len(), current.len());

    // Schedule the reliable subset first, then (unless reliability is ignored) overlay
    // a pass over all workers so unreliable workers also get chunks.
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
        return schedule_to_workers(chunks, &sorted, &config, current);
    }

    tracing::info!(
        "Scheduling {} chunks to {} reliable workers",
        chunks.len(),
        reliable
    );
    let mut assignment = schedule_to_workers(chunks, &sorted[..reliable], &config, current)?;
    if reliable == workers.len() {
        return Ok(assignment);
    }

    tracing::info!(
        "Scheduling {} chunks to {} total workers",
        chunks.len(),
        workers.len()
    );
    let all = schedule_to_workers(chunks, &sorted, &config, current)?;
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
    current: &[Vec<WorkerIndex>],
) -> Result<Assignment, ReplicationError> {
    let caps = replication_cap(chunks, workers.len(), config)?;
    let replicated = to_replicated(chunks, &caps);
    tracing::info!("Replication caps by weight: {caps:?}");

    let worker_ids = workers.iter().map(|w| w.id).collect_vec();
    let rings = hash_workers(&worker_ids);

    // Under `ignore_reliability = true` — the only mode this scheduler is exercised in — the
    // reliable-first sort preserves order, so the caller's positions index `workers` directly.
    let held = current;

    let ideal = ideal_chunk_workers(
        &replicated,
        workers,
        &rings,
        config.worker_capacity,
        config.min_replication,
    )?;

    let mut reconcile = Reconcile::new(
        &replicated,
        workers,
        &rings,
        config.worker_capacity,
        &ideal,
        held,
    );
    let (worker_chunks, achieved) = reconcile.run(config.min_replication);

    Ok(Assignment {
        worker_chunks,
        replication_by_weight: min_replication_by_weight(chunks, &achieved),
    })
}

/// Per-weight replication from the saturated byte budget (at least `config.min_replication`).
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

/// Walk a chunk's hash ring from the home of its `tag`-th replica, offering each worker in ring
/// order (wrapping) to `accept` until one is claimed. Both stages share this traversal; each
/// supplies its own eligibility, cost, and commit logic through `accept`.
fn walk_ring(
    rings: &[Vec<(u64, WorkerIndex)>],
    chunk: &ReplicatedChunk,
    tag: ReplicationFactor,
    mut accept: impl FnMut(WorkerIndex) -> bool,
) -> Option<WorkerIndex> {
    let chunk_hash = replica_hash(chunk, tag);
    let ring = &rings[chunk_hash as usize % N_RINGS];
    let first = ring.partition_point(|(x, _)| *x < chunk_hash);
    ring[first..]
        .iter()
        .chain(&ring[..first])
        .map(|&(_, worker)| worker)
        .find(|&worker| accept(worker))
}

fn version_eligible(chunk: &ReplicatedChunk, worker: &Worker) -> bool {
    chunk
        .minimum_worker_version
        .is_none_or(|min| worker.version.as_ref().is_some_and(|v| v >= min))
}

/// Capacity-bounded fill shared by both stages: every active chunk gets its `tag`-th replica
/// before any gets its `(tag + 1)`-th (load balance); a chunk leaves the active set once capped
/// or unplaceable. Stages differ only in the three hooks.
trait FillToCap {
    /// The replica count this pass may grow a chunk to.
    fn cap_of(&self, chunk_index: usize) -> ReplicationFactor;
    /// Whether a chunk may participate in the fill at all.
    fn fillable(&self, chunk_index: usize) -> bool;
    /// Place one replica of `chunk_index`; `true` if it landed.
    fn place_one(&mut self, chunk_index: usize, tag: ReplicationFactor) -> bool;

    fn fill_to_cap(&mut self, order: &[usize], from_tag: ReplicationFactor) {
        let max_cap = order.iter().map(|&i| self.cap_of(i)).max().unwrap_or(0);
        let mut active: Vec<usize> = order
            .iter()
            .copied()
            .filter(|&i| self.fillable(i) && self.cap_of(i) > from_tag)
            .collect();
        for tag in from_tag..max_cap {
            active.retain(|&chunk_index| {
                self.place_one(chunk_index, tag) && self.cap_of(chunk_index) > tag + 1
            });
            if active.is_empty() {
                break;
            }
        }
    }
}

/// Fewest replicas actually placed among the chunks of each weight.
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

// ===========================================================================
// Stage 1 — ideal generation: consistent hashing with bounded loads.
// ===========================================================================

/// Place every chunk on hypothetically-empty workers; return each chunk's ordered ideal worker list
/// (tag-0 first).
fn ideal_chunk_workers(
    chunks: &[ReplicatedChunk],
    workers: &[&Worker],
    rings: &[Vec<(u64, WorkerIndex)>],
    worker_capacity: u64,
    min_replication: ReplicationFactor,
) -> Result<Vec<Vec<WorkerIndex>>, ReplicationError> {
    let mut placement = Placement::new(chunks, workers, rings, worker_capacity);

    // `partition` keeps each group in input order, so placement stays deterministic.
    let (restricted, unrestricted): (Vec<usize>, Vec<usize>) =
        (0..chunks.len()).partition(|&i| chunks[i].minimum_worker_version.is_some());

    placement.ensure_minimum(&restricted, min_replication)?;
    placement.ensure_minimum(&unrestricted, min_replication)?;

    let order = restricted.into_iter().chain(unrestricted).collect_vec();
    placement.fill_to_cap(&order, min_replication);

    Ok(placement.chunk_workers)
}

/// Stage-1 ideal generator state.
struct Placement<'a> {
    chunks: &'a [ReplicatedChunk<'a>],
    workers: &'a [&'a Worker],
    rings: &'a [Vec<(u64, WorkerIndex)>],
    worker_capacity: u64,
    /// Bytes assigned to each worker.
    allocated: Vec<u64>,
    /// Chunks assigned to each worker.
    worker_chunks: Vec<Vec<ChunkIndex>>,
    /// Workers holding each chunk — the inverse of `worker_chunks`.
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

    fn place(&mut self, chunk_index: usize, tag: ReplicationFactor) -> bool {
        // Split borrows so the `accept` closure doesn't capture all of `self`.
        let chunks = self.chunks;
        let chunk = &chunks[chunk_index];
        let workers = self.workers;
        let rings = self.rings;
        let worker_capacity = self.worker_capacity;
        let allocated = &mut self.allocated;
        let worker_chunks = &mut self.worker_chunks;
        let chunk_workers = &mut self.chunk_workers;

        walk_ring(rings, chunk, tag, |worker| {
            let w = worker as usize;
            if !version_eligible(chunk, workers[w]) || chunk_workers[chunk_index].contains(&worker)
            {
                return false;
            }
            if allocated[w] + chunk.size as u64 <= worker_capacity {
                allocated[w] += chunk.size as u64;
                worker_chunks[w].push(chunk_index as ChunkIndex);
                chunk_workers[chunk_index].push(worker);
                return true;
            }
            false
        })
        .is_some()
    }
}

impl FillToCap for Placement<'_> {
    fn cap_of(&self, chunk_index: usize) -> ReplicationFactor {
        self.chunks[chunk_index].replication
    }

    fn fillable(&self, _chunk_index: usize) -> bool {
        true
    }

    fn place_one(&mut self, chunk_index: usize, tag: ReplicationFactor) -> bool {
        self.place(chunk_index, tag)
    }
}

// ===========================================================================
// Stage 2 — reconciliation against the real footprint.
// ===========================================================================

/// Reconciliation pass state.
struct Reconcile<'a> {
    chunks: &'a [ReplicatedChunk<'a>],
    workers: &'a [&'a Worker],
    rings: &'a [Vec<(u64, WorkerIndex)>],
    worker_capacity: u64,
    /// Bytes physically on each worker (footprint plus new downloads this cycle). Never
    /// decreased: removal is asynchronous, so dropped bytes still occupy disk this cycle.
    allocated: Vec<u64>,
    /// Published placement — the result.
    worker_chunks: Vec<Vec<ChunkIndex>>,
    /// Published holders per chunk (inverse of `worker_chunks`).
    chunk_workers: Vec<Vec<WorkerIndex>>,
    /// Stage-1 ideal worker list per chunk (preference + convergence target).
    ideal: &'a [Vec<WorkerIndex>],
    /// Current holders per chunk, in input order; draining copies included.
    held: &'a [Vec<WorkerIndex>],
    /// `held` sorted for binary search (`is_held`). Already a set, so no dedup.
    held_sorted: Vec<Vec<WorkerIndex>>,
    /// New chunks excluded this cycle because they couldn't reach the floor.
    excluded: Vec<bool>,
}

impl<'a> Reconcile<'a> {
    fn new(
        chunks: &'a [ReplicatedChunk<'a>],
        workers: &'a [&'a Worker],
        rings: &'a [Vec<(u64, WorkerIndex)>],
        worker_capacity: u64,
        ideal: &'a [Vec<WorkerIndex>],
        held: &'a [Vec<WorkerIndex>],
    ) -> Self {
        let n_chunks = chunks.len();
        let mut held_sorted = vec![Vec::new(); n_chunks];
        let mut allocated = vec![0u64; workers.len()];
        for ci in 0..n_chunks {
            let mut holders = held[ci].clone();
            holders.sort_unstable();
            for &w in &holders {
                allocated[w as usize] += chunks[ci].size as u64;
            }
            held_sorted[ci] = holders;
        }
        Self {
            chunks,
            workers,
            rings,
            worker_capacity,
            allocated,
            worker_chunks: vec![Vec::new(); workers.len()],
            chunk_workers: vec![Vec::new(); n_chunks],
            ideal,
            held,
            held_sorted,
            excluded: vec![false; n_chunks],
        }
    }

    fn run(
        &mut self,
        min_replication: ReplicationFactor,
    ) -> (WorkerChunks, Vec<ReplicationFactor>) {
        let _timer = crate::metrics::Timer::new("schedule:reconcile");

        // Version-pinned chunks first: scarcest eligible workers, so they claim capacity
        // before unrestricted ones.
        let (restricted, unrestricted): (Vec<usize>, Vec<usize>) =
            (0..self.chunks.len()).partition(|&i| self.chunks[i].minimum_worker_version.is_some());
        let order = restricted.into_iter().chain(unrestricted).collect_vec();

        // Phase A — floors. Established chunks walk tag-outer (load balance, held copies free).
        // New chunks are placed greedily, one whole chunk to its full floor at a time: tag-outer
        // would let two new chunks competing for the same room each grab a partial floor and
        // *both* roll back to zero, leaving placeable capacity unused.
        let (new_chunks, held_chunks): (Vec<usize>, Vec<usize>) = order
            .iter()
            .copied()
            .partition(|&i| self.held_sorted[i].is_empty());

        for tag in 0..min_replication {
            for &chunk_index in &held_chunks {
                self.place(chunk_index, tag);
            }
        }

        for &chunk_index in &new_chunks {
            for tag in 0..min_replication {
                if self.place(chunk_index, tag).is_none() {
                    break;
                }
            }
            // Atomic floor: roll back partial copies (freeing room for the next new chunk)
            // and exclude — never publish under-replicated.
            if (self.chunk_workers[chunk_index].len() as ReplicationFactor) < min_replication {
                self.rollback_all(chunk_index);
                self.excluded[chunk_index] = true;
            }
        }

        // Phase B — grow toward caps, unless Phase A excluded a chunk: re-publishing bonuses
        // pins them on disk (held copies are free), so the excess would never drain and the
        // excluded floor would starve forever. Suppressing growth lets the bonuses drain;
        // once every floor lands, bonuses regrow, so the converged fixed point is unchanged.
        let floor_unmet = self.excluded.iter().any(|&excluded| excluded);
        if !floor_unmet {
            self.fill_to_cap(&order, min_replication);
        }

        // Phase C — floor add-back (invariant 4).
        for &chunk_index in &order {
            self.floor_add_back(chunk_index, min_replication);
        }

        let achieved = self.achieved_replication();
        (self.take_worker_chunks(), achieved)
    }

    /// Growth cap is the stage-1 ideal count, not the weight cap: stage 1 already shed the bonus
    /// copies that wouldn't fit, and re-taking them here for free (held copies cost 0) would pin
    /// them on disk and starve an unmet floor. Stopping at the ideal lets the bonus drain.
    fn growth_cap(&self, chunk_index: usize) -> ReplicationFactor {
        self.ideal[chunk_index].len() as ReplicationFactor
    }

    /// Place the `tag`-th replica via ring walk: a worker already holding the chunk costs 0,
    /// a full worker is skipped (spill). Returns the landing worker, or `None` if nothing fit.
    fn place(&mut self, chunk_index: usize, tag: ReplicationFactor) -> Option<WorkerIndex> {
        if self.excluded[chunk_index] {
            return None;
        }
        // Split borrows so the `accept` closure doesn't capture all of `self`.
        let chunks = self.chunks;
        let chunk = &chunks[chunk_index];
        let workers = self.workers;
        let rings = self.rings;
        let worker_capacity = self.worker_capacity;
        let held_sorted = &self.held_sorted[chunk_index];
        let allocated = &mut self.allocated;
        let worker_chunks = &mut self.worker_chunks;
        let chunk_workers = &mut self.chunk_workers;

        walk_ring(rings, chunk, tag, |worker| {
            let w = worker as usize;
            if !version_eligible(chunk, workers[w]) || chunk_workers[chunk_index].contains(&worker)
            {
                return false;
            }
            // Held copies are already charged in `allocated`; keeping one costs 0.
            let cost = if held_sorted.binary_search(&worker).is_ok() {
                0
            } else {
                chunk.size as u64
            };
            if allocated[w] + cost <= worker_capacity {
                allocated[w] += cost;
                worker_chunks[w].push(chunk_index as ChunkIndex);
                chunk_workers[chunk_index].push(worker);
                return true;
            }
            false
        })
    }

    /// Guarantee at least `min_replication` held copies survive in the published placement,
    /// added back free in preference order (ideal positions first). New/excluded chunks are
    /// skipped — their floor is owned by Phase A.
    fn floor_add_back(&mut self, chunk_index: usize, min_replication: ReplicationFactor) {
        if self.held_sorted[chunk_index].is_empty() || self.excluded[chunk_index] {
            return;
        }
        let mut current_in_pub = self.chunk_workers[chunk_index]
            .iter()
            .filter(|&&w| self.is_held(chunk_index, w))
            .count() as ReplicationFactor;
        if current_in_pub >= min_replication {
            return;
        }
        for w in self.add_back_candidates(chunk_index) {
            if current_in_pub >= min_replication {
                break;
            }
            if self.chunk_workers[chunk_index].contains(&w) {
                continue;
            }
            // Free: bytes already charged in `allocated`.
            self.worker_chunks[w as usize].push(chunk_index as ChunkIndex);
            self.chunk_workers[chunk_index].push(w);
            current_in_pub += 1;
        }
    }

    /// Held workers in floor-retention preference order, deduped: ideal positions first, then
    /// the rest. Held copies not needed for the floor are released, letting drains finish.
    fn add_back_candidates(&self, chunk_index: usize) -> Vec<WorkerIndex> {
        let ideal = &self.ideal[chunk_index];
        let held = &self.held[chunk_index];
        let mut out: Vec<WorkerIndex> = Vec::new();
        let push = |out: &mut Vec<WorkerIndex>, w: WorkerIndex| {
            if !out.contains(&w) {
                out.push(w);
            }
        };
        for &w in held {
            if ideal.contains(&w) {
                push(&mut out, w);
            }
        }
        for &w in held {
            push(&mut out, w);
        }
        out
    }

    /// Undo every placement of `chunk_index` this cycle, refunding only new-download bytes.
    fn rollback_all(&mut self, chunk_index: usize) {
        let size = self.chunks[chunk_index].size as u64;
        let ci = chunk_index as ChunkIndex;
        let holders = std::mem::take(&mut self.chunk_workers[chunk_index]);
        for w in holders {
            if !self.is_held(chunk_index, w) {
                self.allocated[w as usize] = self.allocated[w as usize].saturating_sub(size);
            }
            self.worker_chunks[w as usize].retain(|&c| c != ci);
        }
    }

    /// Whether worker `w` physically holds chunk `chunk_index` right now.
    fn is_held(&self, chunk_index: usize, w: WorkerIndex) -> bool {
        self.held_sorted[chunk_index].binary_search(&w).is_ok()
    }

    fn achieved_replication(&self) -> Vec<ReplicationFactor> {
        self.chunk_workers
            .iter()
            .map(|w| w.len() as ReplicationFactor)
            .collect()
    }

    fn take_worker_chunks(&mut self) -> WorkerChunks {
        let worker_chunks = std::mem::take(&mut self.worker_chunks);
        worker_chunks
            .into_iter()
            .enumerate()
            .map(|(i, mut indices)| {
                indices.sort_unstable();
                (self.workers[i].id, indices)
            })
            .collect()
    }
}

impl FillToCap for Reconcile<'_> {
    /// Stage-1 ideal count, not the weight cap: see [`growth_cap`](Self::growth_cap).
    fn cap_of(&self, chunk_index: usize) -> ReplicationFactor {
        self.growth_cap(chunk_index)
    }

    /// Chunks excluded in Phase A do not grow.
    fn fillable(&self, chunk_index: usize) -> bool {
        !self.excluded[chunk_index]
    }

    fn place_one(&mut self, chunk_index: usize, tag: ReplicationFactor) -> bool {
        self.place(chunk_index, tag).is_some()
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod sim;
