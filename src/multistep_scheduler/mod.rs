//! Multi-step (reconciliation) scheduling: given the current placement, produces a feasible step
//! toward the ideal rather than a placement-blind ideal.
//!
//! Terminology:
//! - **floor** — a chunk's `min_replication` mandatory copies;
//! - **bonus** — weight-earned extras above the floor (up to its weight cap), shed first when room is tight;
//! - **ideal** — placement on hypothetically-empty workers;
//! - **held** — copies the current assignment keeps on a worker. Workers apply assignments
//!   asynchronously, so a held copy is *instructed*, not necessarily downloaded yet; retaining
//!   one costs no new bytes.
//! Draining copies are not distinguished from other held copies — that split lives in the caller.
//!
//! Stage 1 ([`ideal_chunk_workers`]) computes the ideal. Stage 2 ([`Reconcile`]) re-walks the same
//! rings against the real footprint: full workers are skipped (spill), held copies cost 0, held
//! copies are added back if fewer than the floor survive, new chunks reach their floor atomically
//! or are excluded, and bonus growth is suppressed while any floor is unmet.

use std::cmp::Reverse;
use std::collections::{BTreeMap, HashSet};

use itertools::Itertools;
use libp2p_identity::PeerId;
use seahash::hash;

use semver::Version;

use crate::rings::{Rings, WorkerRingCache, walk_ring_from};
use crate::{
    replication::{ReplicationError, calc_replication_factors},
    types::{Assignment, ChunkIndex, ChunkWeight, ReplicationFactor, Worker, WorkerIndex},
};

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
    pub is_portal_visible: bool,
}

/// Compute this cycle's placement from the current one.
///
/// The three placement views are per-chunk lists of **positions into `workers`**; the caller owns
/// the PeerId↔position mapping:
/// - `current[i]` — the copies the current plan keeps for chunk `i` (assignment rows, draining
///   ones included). Workers apply assignments asynchronously, so these are *instructed* copies,
///   not proven downloads — the only possession proof is `routed`. Leave out copies on departed
///   workers; they count as missing.
/// - `committed[i]` — the previous cycle's committed ideal. Eviction never takes a donor below
///   `min(min_replication, |committed|)` of these copies — its durability floor.
/// - `routed[i]` — who the confirmed routing still points at. Orders eviction victims
///   (still-routed last) but never blocks one: confirmation lag is unbounded, so routing must not
///   pin disk. See `docs/adr/0001-same-cycle-floor-preemption.md`.
///
/// Returns the new placement (fresh copies plus retained current ones), and the `(chunk, holder)`
/// pairs preemption deleted this cycle. Evicted pairs are not part of the assignment — workers
/// delete by omission. The list exists so storage skips re-minting those copies as draining
/// `stale` mappings, which would re-add their bytes and overcommit the worker.
pub fn schedule(
    chunks: &[ScheduledChunk<'_>],
    workers: &[Worker],
    config: &SchedulingConfig,
    current: &[Vec<WorkerIndex>],
    committed: &[Vec<WorkerIndex>],
    routed: &[Vec<WorkerIndex>],
    cache: &mut WorkerRingCache,
) -> Result<(Assignment, Vec<(ChunkIndex, PeerId)>), ReplicationError> {
    let _timer = crate::metrics::Timer::new("schedule");
    debug_assert_eq!(chunks.len(), current.len());
    debug_assert_eq!(chunks.len(), committed.len());
    debug_assert_eq!(chunks.len(), routed.len());

    if config.ignore_reliability {
        tracing::info!(
            "Scheduling {} chunks to {} workers",
            chunks.len(),
            workers.len()
        );
        let workers: Vec<&Worker> = workers.iter().collect();
        return schedule_to_workers(chunks, &workers, config, current, committed, routed, cache);
    }

    // NOTE: untested — the sim and PBT run only with `ignore_reliability = true`, so nothing below
    // (partition, per-view translation, reliable-prefix filtering) has coverage. It extends #53's
    // single-view translation pattern to `committed`/`routed`.
    //
    // The partition reorders the fleet, so every placement view is translated into the
    // partitioned position space (each held copy still points at the same worker).
    let (sorted, reliable_count, new_pos) = partition_reliable(workers);
    let current = translate(current, &new_pos);
    let committed = translate(committed, &new_pos);
    let routed = translate(routed, &new_pos);

    if reliable_count == workers.len() {
        // Fully reliable fleet: the partition is the identity and one pass covers everyone.
        return schedule_to_workers(
            chunks, &sorted, config, &current, &committed, &routed, cache,
        );
    }

    tracing::info!(
        "Scheduling {} chunks to {} reliable workers",
        chunks.len(),
        reliable_count
    );
    // Copies on unreliable workers are outside the prefix this pass schedules to; drop them from
    // each view so they become a shortfall, like copies on a departed worker.
    let prefix = |placement: &[Vec<WorkerIndex>]| -> Vec<Vec<WorkerIndex>> {
        placement
            .iter()
            .map(|held| {
                held.iter()
                    .copied()
                    .filter(|&w| (w as usize) < reliable_count)
                    .collect()
            })
            .collect()
    };
    let (mut assignment, mut evicted) = schedule_to_workers(
        chunks,
        &sorted[..reliable_count],
        config,
        &prefix(&current),
        &prefix(&committed),
        &prefix(&routed),
        cache,
    )?;

    tracing::info!(
        "Scheduling {} chunks to {} total workers",
        chunks.len(),
        workers.len()
    );
    let (all, all_evicted) = schedule_to_workers(
        chunks, &sorted, config, &current, &committed, &routed, cache,
    )?;
    // Reliable workers keep their assignment; unreliable ones take theirs from `all`.
    for (worker_id, chunk_indexes) in all.worker_chunks {
        assignment
            .worker_chunks
            .entry(worker_id)
            .or_insert(chunk_indexes);
    }
    // Partition evictions the same way, so no pair ever names a worker whose published list came
    // from the other pass (which might still hold that chunk). Reliable pass's pairs are all on
    // reliable workers; from `all` keep only pairs on unreliable workers.
    let reliable_ids: HashSet<PeerId> = sorted[..reliable_count].iter().map(|w| w.id).collect();
    evicted.extend(
        all_evicted
            .into_iter()
            .filter(|(_, id)| !reliable_ids.contains(id)),
    );
    Ok((assignment, evicted))
}

/// Reorder the fleet reliable-first. Returns the reordered fleet, the reliable-prefix length, and
/// `new_pos[old] = new` — the map from caller positions to reordered positions, so each placement
/// view can be translated to keep every held copy pointing at the same worker.
fn partition_reliable(workers: &[Worker]) -> (Vec<&Worker>, usize, Vec<WorkerIndex>) {
    // `order[new] = old` — caller positions, reliable first; `partition` keeps input order.
    let (reliable_positions, unreliable_positions): (Vec<usize>, Vec<usize>) =
        (0..workers.len()).partition(|&i| workers[i].reliable());
    let reliable = reliable_positions.len();
    let order: Vec<usize> = reliable_positions
        .into_iter()
        .chain(unreliable_positions)
        .collect();

    let mut new_pos: Vec<WorkerIndex> = vec![0; workers.len()];
    for (new, &old) in order.iter().enumerate() {
        new_pos[old] = new as WorkerIndex;
    }

    let sorted = order.iter().map(|&i| &workers[i]).collect();
    (sorted, reliable, new_pos)
}

/// Remap a placement view's caller positions into the reordered position space (see
/// [`partition_reliable`]).
fn translate(placement: &[Vec<WorkerIndex>], new_pos: &[WorkerIndex]) -> Vec<Vec<WorkerIndex>> {
    placement
        .iter()
        .map(|held| held.iter().map(|&w| new_pos[w as usize]).collect())
        .collect()
}

fn schedule_to_workers(
    chunks: &[ScheduledChunk<'_>],
    workers: &[&Worker],
    config: &SchedulingConfig,
    current: &[Vec<WorkerIndex>],
    committed: &[Vec<WorkerIndex>],
    routed: &[Vec<WorkerIndex>],
    cache: &mut WorkerRingCache,
) -> Result<(Assignment, Vec<(ChunkIndex, PeerId)>), ReplicationError> {
    let caps = replication_cap(chunks, workers.len(), config)?;
    // Hash every replica once here; Stage 1 and Stage 2 both index these instead of re-hashing.
    let hashes = hash_chunks(chunks, &caps, config.min_replication);
    let replicated = to_replicated(chunks, &caps, &hashes);
    tracing::info!("Replication caps by weight: {caps:?}");

    let rings = cache.rings(workers.iter().map(|w| w.id));

    // `current` positions index `workers` — `schedule` translates them whenever it reorders.
    let held = current;

    // Version-pinned chunks first: they compete for the scarcest eligible workers, so they claim
    // capacity before unrestricted ones. `partition` keeps input order, so placement is deterministic.
    // `placement_order` is the restricted chunks then the unrestricted, split at `restricted_count`.
    let (restricted, unrestricted): (Vec<usize>, Vec<usize>) =
        (0..replicated.len()).partition(|&i| replicated[i].minimum_worker_version.is_some());
    let restricted_count = restricted.len();
    let placement_order: Vec<usize> = restricted.into_iter().chain(unrestricted).collect();

    let ideal = ideal_chunk_workers(
        &replicated,
        workers,
        rings,
        config.worker_capacity,
        config.min_replication,
        &placement_order,
        restricted_count,
    )?;

    let mut reconcile = Reconcile::new(
        &replicated,
        workers,
        rings,
        config.worker_capacity,
        PlacementViews {
            ideal: &ideal,
            held,
            committed,
            routed,
        },
    );
    let (worker_chunks, achieved, evicted) =
        reconcile.run(config.min_replication, &placement_order);

    // Positions index `workers` directly here; emit `PeerId` so evicted pairs survive the
    // reliable/all merge intact (the merge re-keys `worker_chunks` by `PeerId` too).
    let evicted = evicted
        .into_iter()
        .map(|(ci, w)| (ci, workers[w as usize].id))
        .collect();

    Ok((
        Assignment {
            worker_chunks,
            replication_by_weight: min_replication_by_weight(chunks, &achieved),
        },
        evicted,
    ))
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
    hashes: &'a [Vec<u64>],
) -> Vec<ReplicatedChunk<'a>> {
    chunks
        .iter()
        .zip(hashes)
        .map(|(c, h)| ReplicatedChunk {
            dataset: c.dataset,
            chunk_id: c.chunk_id,
            replication: replication_by_weight[&c.weight],
            size: c.size,
            minimum_worker_version: c.minimum_worker_version,
            is_portal_visible: c.is_portal_visible,
            hashes: h,
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
    pub(crate) is_portal_visible: bool,
    /// Replica ring hashes by tag, precomputed once (see [`hash_chunks`]) and shared by both stages.
    pub(crate) hashes: &'a [u64],
}

/// Each chunk's replica ring hashes, for tags `0..max(min_replication, weight cap)` — the most any
/// pass walks — so both placement stages index them rather than hash during the ring walk.
fn hash_chunks(
    chunks: &[ScheduledChunk<'_>],
    caps: &BTreeMap<ChunkWeight, ReplicationFactor>,
    min_replication: ReplicationFactor,
) -> Vec<Vec<u64>> {
    use rayon::prelude::*;
    // The hash is over a contiguous `dataset/chunk_id:tag`; reuse one buffer per worker, rewriting
    // only the tag suffix.
    chunks
        .par_iter()
        .map_init(Vec::<u8>::new, |buffer, chunk| {
            buffer.clear();
            buffer.extend_from_slice(chunk.dataset.as_bytes());
            buffer.push(b'/');
            buffer.extend_from_slice(chunk.chunk_id.as_bytes());
            buffer.push(b':');
            let prefix = buffer.len();
            let tags = min_replication.max(caps[&chunk.weight]);
            (0..tags)
                .map(|tag| {
                    buffer.truncate(prefix);
                    buffer.extend_from_slice(&tag.to_le_bytes());
                    hash(buffer)
                })
                .collect()
        })
        .collect()
}

/// Walk the `tag`-th replica's ring from its precomputed hash (see [`walk_ring_from`]). Both
/// placement stages share this; each supplies its own eligibility, cost, and commit logic via `accept`.
fn walk_ring(
    rings: &Rings,
    chunk: &ReplicatedChunk,
    tag: ReplicationFactor,
    accept: impl FnMut(WorkerIndex) -> bool,
) -> Option<WorkerIndex> {
    walk_ring_from(rings, chunk.hashes[tag as usize], accept)
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
    rings: &Rings,
    worker_capacity: u64,
    min_replication: ReplicationFactor,
    placement_order: &[usize],
    restricted_count: usize,
) -> Result<Vec<Vec<WorkerIndex>>, ReplicationError> {
    let mut placement = Placement::new(chunks, workers, rings, worker_capacity);

    // Floor the version-restricted chunks (the `placement_order` prefix) before the unrestricted ones.
    placement.ensure_minimum(&placement_order[..restricted_count], min_replication)?;
    placement.ensure_minimum(&placement_order[restricted_count..], min_replication)?;

    placement.fill_to_cap(placement_order, min_replication);

    Ok(placement.chunk_workers)
}

/// Stage-1 ideal generator state.
struct Placement<'a> {
    chunks: &'a [ReplicatedChunk<'a>],
    workers: &'a [&'a Worker],
    rings: &'a Rings,
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
        rings: &'a Rings,
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

/// The four per-chunk placement views the reconcile reads, all in input order. Grouped into a
/// single [`Reconcile::new`] argument so they can't be swapped at the call site; see the like-named
/// [`Reconcile`] fields for each view's role.
struct PlacementViews<'a> {
    ideal: &'a [Vec<WorkerIndex>],
    held: &'a [Vec<WorkerIndex>],
    committed: &'a [Vec<WorkerIndex>],
    routed: &'a [Vec<WorkerIndex>],
}

/// An eviction candidate on the probed worker (see [`Reconcile::place_by_eviction`]).
struct Victim {
    /// Soft ordering key: the confirmed routing still addresses this copy — sacrificed last.
    still_routed: bool,
    size: u64,
    chunk: ChunkIndex,
}

/// The immutable views the donor-eviction rules consult, snapshot per probed worker. The rules and
/// their rationale are documented on [`Reconcile::place_by_eviction`]; [`Self::evictable`] is their
/// one-home implementation.
struct DonorGuards<'a> {
    chunks: &'a [ReplicatedChunk<'a>],
    /// Holders published so far this cycle (`Reconcile::chunk_workers`).
    published: &'a [Vec<WorkerIndex>],
    committed: &'a [Vec<WorkerIndex>],
    routed: &'a [Vec<WorkerIndex>],
    held: &'a [Vec<WorkerIndex>],
    evicted: &'a [(ChunkIndex, WorkerIndex)],
    min_replication: ReplicationFactor,
}

impl DonorGuards<'_> {
    /// May the copy of donor chunk `x` on `worker` be reclaimed to floor the seated chunk?
    fn evictable(
        &self,
        x: ChunkIndex,
        worker: WorkerIndex,
        seated: usize,
        seated_visible: bool,
    ) -> bool {
        let xi = x as usize;
        // Never the seated chunk itself, a copy it publishes this cycle, or one already evicted.
        if xi == seated
            || self.published[xi].contains(&worker)
            || self.evicted.contains(&(x, worker))
        {
            return false;
        }
        // Routing veto: a not-yet-visible seated chunk never takes a still-routed copy.
        if !seated_visible && self.routed[xi].contains(&worker) {
            return false;
        }
        let survives = |w2: WorkerIndex| w2 != worker && !self.evicted.contains(&(x, w2));
        // Committed floor, net of this cycle's other evictions of X.
        let floor = (self.min_replication as usize).min(self.committed[xi].len());
        if self.committed[xi]
            .iter()
            .filter(|&&w2| survives(w2))
            .count()
            < floor
        {
            return false;
        }
        // Possession: deleting a routed copy needs a surviving committed copy that is itself routed.
        if self.routed[xi].contains(&worker)
            && !self.committed[xi]
                .iter()
                .any(|&w2| survives(w2) && self.routed[xi].contains(&w2))
        {
            return false;
        }
        // A portal-visible donor keeps ≥ 1 held copy.
        if self.chunks[xi].is_portal_visible && !self.held[xi].iter().any(|&w2| survives(w2)) {
            return false;
        }
        true
    }
}

/// Reconciliation pass state.
struct Reconcile<'a> {
    chunks: &'a [ReplicatedChunk<'a>],
    workers: &'a [&'a Worker],
    rings: &'a Rings,
    worker_capacity: u64,
    /// Bytes physically on each worker (footprint plus new downloads this cycle). Drops never
    /// decrease it (removal is asynchronous, the bytes still occupy disk this cycle) — the only
    /// subtractions are same-cycle eviction reclaims and rollback refunds of uncharged downloads.
    allocated: Vec<u64>,
    /// Published placement — the result.
    worker_chunks: Vec<Vec<ChunkIndex>>,
    /// Published holders per chunk (inverse of `worker_chunks`).
    chunk_workers: Vec<Vec<WorkerIndex>>,
    /// Stage-1 ideal worker list per chunk (preference + convergence target).
    ideal: &'a [Vec<WorkerIndex>],
    /// Copies the current plan keeps per chunk (assignment rows, draining included) — instructed,
    /// not necessarily downloaded yet.
    held: &'a [Vec<WorkerIndex>],
    /// Pre-cycle committed-ideal holders per chunk — the durability floor eviction must not breach.
    /// The exact set `step_safety` retention measures against, unlike the drain-and-download-polluted
    /// `held`.
    committed: &'a [Vec<WorkerIndex>],
    /// Pre-cycle confirmed-routing holders per chunk. Soft: only orders eviction victims so a
    /// still-routed copy is sacrificed last; never blocks an eviction (confirmation lag is unbounded
    /// below a full quorum).
    routed: &'a [Vec<WorkerIndex>],
    /// Chunk indices each worker currently holds (inverse of `held`), built once so Pass-2 eviction
    /// can enumerate a worker's held copies without scanning every chunk.
    held_by_worker: Vec<Vec<ChunkIndex>>,
    /// New chunks excluded this cycle because they couldn't reach the floor.
    excluded: Vec<bool>,
    /// Draining copies evicted this cycle to floor a short chunk; surfaced so storage deletes them
    /// (excludes each from `ideal ∪ stale` — the worker drops it rather than draining it).
    evicted: Vec<(ChunkIndex, WorkerIndex)>,
}

impl<'a> Reconcile<'a> {
    fn new(
        chunks: &'a [ReplicatedChunk<'a>],
        workers: &'a [&'a Worker],
        rings: &'a Rings,
        worker_capacity: u64,
        placement: PlacementViews<'a>,
    ) -> Self {
        let PlacementViews {
            ideal,
            held,
            committed,
            routed,
        } = placement;
        let n_chunks = chunks.len();
        let mut allocated = vec![0u64; workers.len()];
        let mut held_by_worker = vec![Vec::new(); workers.len()];
        for ci in 0..n_chunks {
            for &w in &held[ci] {
                allocated[w as usize] += chunks[ci].size as u64;
                held_by_worker[w as usize].push(ci as ChunkIndex);
            }
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
            committed,
            routed,
            held_by_worker,
            excluded: vec![false; n_chunks],
            evicted: Vec::new(),
        }
    }

    fn run(
        &mut self,
        min_replication: ReplicationFactor,
        placement_order: &[usize],
    ) -> (
        WorkerChunks,
        Vec<ReplicationFactor>,
        Vec<(ChunkIndex, WorkerIndex)>,
    ) {
        let _timer = crate::metrics::Timer::new("schedule:reconcile");

        // Phase A — floors. Established chunks walk tag-outer (load balance, held copies free).
        // New chunks are placed greedily, one whole chunk to its full floor at a time: tag-outer
        // would let two new chunks competing for the same room each grab a partial floor and
        // *both* roll back to zero, leaving placeable capacity unused.
        //
        // Not-portal-visible is what makes a chunk new here, not merely holding nothing: a chunk
        // portals already route to can lose its last holder, and the all-or-nothing rule would then
        // exclude it outright instead of re-flooring it best-effort.
        let (new_chunks, held_chunks): (Vec<usize>, Vec<usize>) = placement_order
            .iter()
            .copied()
            .partition(|&i| self.held[i].is_empty() && !self.chunks[i].is_portal_visible);

        for tag in 0..min_replication {
            for &chunk_index in &held_chunks {
                self.place_floor(chunk_index, tag, min_replication);
            }
        }

        for &chunk_index in &new_chunks {
            // Evictions made for this atomic attempt must unwind with it if the floor isn't reached.
            let evict_mark = self.evicted.len();
            for tag in 0..min_replication {
                if self
                    .place_floor(chunk_index, tag, min_replication)
                    .is_none()
                {
                    break;
                }
            }
            // Atomic floor: roll back partial copies (freeing room for the next new chunk)
            // and exclude — never publish under-replicated.
            if (self.chunk_workers[chunk_index].len() as ReplicationFactor) < min_replication {
                self.rollback_all(chunk_index);
                self.undo_evictions_since(evict_mark);
                self.excluded[chunk_index] = true;
            }
        }

        // Phase B — grow toward caps, unless Phase A excluded a chunk: re-publishing bonuses
        // pins them on disk (held copies are free), so the excess would never drain and the
        // excluded floor would starve forever. Suppressing growth lets the bonuses drain;
        // once every floor lands, bonuses regrow, so the converged fixed point is unchanged.
        let floor_unmet = self.excluded.iter().any(|&excluded| excluded);
        if !floor_unmet {
            self.fill_to_cap(placement_order, min_replication);
        }

        // Phase C — floor add-back (Invariant 4, the removal safety window in
        // `docs/mvcc-storage.md`: a pair dropped from routing keeps serving M ticks).
        for &chunk_index in placement_order {
            self.floor_add_back(chunk_index, min_replication);
        }

        let achieved = self.achieved_replication();
        (
            self.take_worker_chunks(),
            achieved,
            std::mem::take(&mut self.evicted),
        )
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
        let chunk_held = &self.held[chunk_index];
        let evicted = &self.evicted;
        let allocated = &mut self.allocated;
        let worker_chunks = &mut self.worker_chunks;
        let chunk_workers = &mut self.chunk_workers;

        walk_ring(rings, chunk, tag, |worker| {
            let w = worker as usize;
            // A copy evicted this cycle must not be re-published on that worker by a later floor or
            // bonus pass — its bytes are committed to the chunk we freed the room for.
            if evicted.contains(&(chunk_index as ChunkIndex, worker)) {
                return false;
            }
            if !version_eligible(chunk, workers[w]) || chunk_workers[chunk_index].contains(&worker)
            {
                return false;
            }
            // Held copies are already charged in `allocated`; keeping one costs 0.
            let cost = if chunk_held.contains(&worker) {
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

    /// Place a **floor** copy: the normal ring walk first (Pass 1); if nothing fit, ring-following
    /// eviction of draining copies (Pass 2). Only floors call this — a floor copy is mandatory, so a
    /// held-not-ideal (draining) copy of some other chunk must yield to it. Bonus growth uses
    /// [`place`](Self::place) and never evicts: shedding a drain to pin a bonus would starve the drain.
    fn place_floor(
        &mut self,
        chunk_index: usize,
        tag: ReplicationFactor,
        min_replication: ReplicationFactor,
    ) -> Option<WorkerIndex> {
        if let Some(w) = self.place(chunk_index, tag) {
            return Some(w);
        }
        self.place_by_eviction(chunk_index, tag, min_replication)
    }

    /// Pass 2: re-walk the chunk's ring in the same order; at the first worker whose free room plus
    /// reclaimable bytes cover the chunk, evict just enough reclaimable copies to fit and place the
    /// chunk the same cycle (the worker deletes before it downloads, so the momentary over-commit is
    /// accepted). See `docs/adr/0001-same-cycle-floor-preemption.md`.
    ///
    /// **Durability is hard; routing consistency is best-effort** (worker-confirmation lag is
    /// unbounded below a full quorum, so hard-protecting routing would pin disk indefinitely).
    /// For a candidate copy of chunk `X` on worker `w`:
    /// - *Committed floor (hard).* X must keep ≥ `min(min_replication, |committed[X]|)` committed
    ///   copies without `w`, net of this cycle's other evictions of X (so several floors preempting
    ///   one donor can't collectively breach it). `committed[X]` is exactly what `step_safety`
    ///   retention measures, so this floor stops eviction from hard-deleting a copy the donor still
    ///   needs; `|committed[X]| = 0` ⇒ no floor ⇒ freely reclaimable (preserves ADR-0001 room).
    ///   Bonus copies and leftover drains above the floor stay reclaimable.
    /// - *Possession (hard).* A committed row doesn't prove the copy was ever fetched, so evicting a
    ///   **routed** copy — the only possession proof — additionally requires a surviving committed
    ///   copy that is itself routed; and a portal-visible donor always keeps ≥ 1 held copy, so
    ///   preemption can't recreate the holderless-visible state it exists to close. Scope: both
    ///   backends feed `routed` visibility-filtered, so the routed-survivor rule is vacuous for a
    ///   not-yet-visible donor — its sole fetched copy stays evictable for an unfetched survivor
    ///   until the per-worker applied-report possession view lands (ADR 0001, possession follow-up).
    ///   Bounded harm: no readers until promotion.
    /// - *Routing (soft).* `routed[X]` orders victims, not-routed copies first. A portal-visible
    ///   starved chunk (the ADR-0001 case) may take a still-routed copy above the floor as a last
    ///   resort — the transient miss is the documented `portal_consistency_misses` cost. For a
    ///   not-yet-visible starved chunk `w ∈ routed[X]` is a hard veto: never trade a live chunk's
    ///   reads for a chunk nobody reads yet; it defers via the atomic-floor exclusion instead.
    fn place_by_eviction(
        &mut self,
        chunk_index: usize,
        tag: ReplicationFactor,
        min_replication: ReplicationFactor,
    ) -> Option<WorkerIndex> {
        // Split borrows so the `accept` closure doesn't capture all of `self`.
        let chunks = self.chunks;
        let chunk = &chunks[chunk_index];
        let size = chunk.size as u64;
        // Decides the routing rule (see doc): visible C treats routing as soft ordering,
        // not-yet-visible C is hard-vetoed from still-routed copies.
        let c_visible = chunk.is_portal_visible;
        let workers = self.workers;
        let rings = self.rings;
        let worker_capacity = self.worker_capacity;
        let committed = self.committed;
        let routed = self.routed;
        let held_all = self.held;
        let held_by_worker = &self.held_by_worker;
        let allocated = &mut self.allocated;
        let worker_chunks = &mut self.worker_chunks;
        let chunk_workers = &mut self.chunk_workers;
        let evicted = &mut self.evicted;

        walk_ring(rings, chunk, tag, |worker| {
            let w = worker as usize;
            if !version_eligible(chunk, workers[w]) || chunk_workers[chunk_index].contains(&worker)
            {
                return false;
            }
            let free = worker_capacity.saturating_sub(allocated[w]);
            // Pass 1 already tried workers with real free room; only evict where it's actually needed.
            if free >= size {
                return false;
            }

            let guards = DonorGuards {
                chunks,
                published: chunk_workers,
                committed,
                routed,
                held: held_all,
                evicted,
                min_replication,
            };
            let mut candidates: Vec<Victim> = held_by_worker[w]
                .iter()
                .copied()
                .filter(|&x| guards.evictable(x, worker, chunk_index, c_visible))
                .map(|x| Victim {
                    still_routed: routed[x as usize].contains(&worker),
                    size: chunks[x as usize].size as u64,
                    chunk: x,
                })
                .collect();
            // Not-routed copies first, largest-first within a routing class: a still-routed copy is
            // sacrificed only when the not-routed ones don't free enough room.
            candidates.sort_unstable_by_key(|v| (v.still_routed, Reverse(v.size)));

            // Cheap pre-sum: never evict on a worker that still can't fit the chunk — spill instead.
            let reclaimable: u64 = candidates.iter().map(|v| v.size).sum();
            if free + reclaimable < size {
                return false;
            }

            let mut freed = free;
            for Victim {
                size: s, chunk: x, ..
            } in candidates
            {
                if freed >= size {
                    break;
                }
                // The victim is held on `w`, so its bytes were charged in `Reconcile::new`;
                // saturating is release-mode defense only.
                debug_assert!(allocated[w] >= s, "evicting uncharged bytes");
                allocated[w] = allocated[w].saturating_sub(s);
                evicted.push((x, worker));
                freed += s;
            }
            allocated[w] += size;
            worker_chunks[w].push(chunk_index as ChunkIndex);
            chunk_workers[chunk_index].push(worker);
            true
        })
    }

    /// Refund and forget evictions recorded since `mark` — an aborted new-chunk floor attempt. Since
    /// eviction never un-publishes the evicted chunk, undo is a pure `allocated` refund plus truncation.
    fn undo_evictions_since(&mut self, mark: usize) {
        for (x, w) in self.evicted.drain(mark..) {
            self.allocated[w as usize] += self.chunks[x as usize].size as u64;
        }
    }

    /// Guarantee at least `min_replication` held copies survive in the published placement,
    /// added back free in preference order (ideal positions first). New/excluded chunks are
    /// skipped — their floor is owned by Phase A.
    fn floor_add_back(&mut self, chunk_index: usize, min_replication: ReplicationFactor) {
        if self.held[chunk_index].is_empty() || self.excluded[chunk_index] {
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
            // A copy evicted this cycle was handed to a short chunk; re-adopting it here would
            // resurrect the draining bytes and desync `allocated` from the storage delete.
            if self.evicted.contains(&(chunk_index as ChunkIndex, w)) {
                continue;
            }
            // Free: bytes already charged in `allocated`.
            self.worker_chunks[w as usize].push(chunk_index as ChunkIndex);
            self.chunk_workers[chunk_index].push(w);
            current_in_pub += 1;
        }
    }

    /// Held workers in floor-retention preference order: ideal positions first, then the rest.
    /// Held copies not needed for the floor are released, letting drains finish.
    fn add_back_candidates(&self, chunk_index: usize) -> Vec<WorkerIndex> {
        let ideal = &self.ideal[chunk_index];
        let held = &self.held[chunk_index];
        held.iter()
            .filter(|w| ideal.contains(w))
            .chain(held.iter().filter(|w| !ideal.contains(w)))
            .copied()
            .collect()
    }

    /// Undo every placement of `chunk_index` this cycle, refunding only new-download bytes.
    fn rollback_all(&mut self, chunk_index: usize) {
        let size = self.chunks[chunk_index].size as u64;
        let ci = chunk_index as ChunkIndex;
        let holders = std::mem::take(&mut self.chunk_workers[chunk_index]);
        for w in holders {
            if !self.is_held(chunk_index, w) {
                // Non-held placements were charged as fresh downloads by `place`; saturating is
                // release-mode defense only.
                debug_assert!(
                    self.allocated[w as usize] >= size,
                    "refunding uncharged bytes"
                );
                self.allocated[w as usize] = self.allocated[w as usize].saturating_sub(size);
            }
            self.worker_chunks[w as usize].retain(|&c| c != ci);
        }
    }

    /// Whether the current plan keeps chunk `chunk_index` on worker `w` (its bytes are already
    /// charged; possession itself is not guaranteed — workers apply asynchronously).
    fn is_held(&self, chunk_index: usize, w: WorkerIndex) -> bool {
        self.held[chunk_index].contains(&w)
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
