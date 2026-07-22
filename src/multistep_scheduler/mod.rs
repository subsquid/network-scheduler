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

/// `current[i]` — **positions into `workers`** of the peers physically holding chunk `i` now,
/// draining copies included. The caller owns the PeerId↔position mapping; copies on a departed
/// worker must be left out (they become a shortfall). Returns the full published placement
/// (new copies plus current copies retained to hold the floor).
///
/// `committed[i]` and `routed[i]` are the same **positions-into-`workers`** shape, splitting out the
/// two roles the flattened `current` mixes together (see `docs/adr/0001-same-cycle-floor-preemption.md`):
/// - `committed[i]` — the pre-cycle *committed ideal* holders: the durability floor `step_safety`
///   retention measures against. Eviction treats this as the **hard** floor: a donor always keeps
///   ≥ `min(min_replication, |committed|)` of its committed copies un-evicted.
/// - `routed[i]` — the holders the *confirmed routing* still addresses. **Best-effort only**: it
///   orders eviction victims (a still-routed copy is sacrificed last) but never blocks an eviction —
///   confirmation lag is unbounded below a full quorum, so hard-protecting routing could pin disk
///   indefinitely.
///
/// Returns the published placement plus the `(chunk, holder)` pairs the reconcile force-dropped this
/// cycle to floor a starved chunk. The evictions are **not** part of the assignment — a worker
/// removes a copy simply because it is absent from its assignment; the list only tells storage not to
/// re-mint those dropped copies as draining `stale` mappings (which would re-add them and overcommit
/// the worker). See `docs/adr/0001-same-cycle-floor-preemption.md`.
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

    // NOTE: untested. The sim and PBT run only with `ignore_reliability = true`, so everything
    // below — the reliable-first partition, the per-view translation, and the reliable-prefix
    // filtering of all three placement views — has no test coverage. It follows #53's single-view
    // translation pattern extended to `committed`/`routed`, but nothing exercises it.
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
    let reliable_ids: std::collections::HashSet<PeerId> =
        sorted[..reliable_count].iter().map(|w| w.id).collect();
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

    // `new_pos[old] = new` — where each caller position landed.
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

/// Reconciliation pass state.
/// The four per-chunk placement views the reconcile reads, all in input order. Grouped into a single
/// [`Reconcile::new`] argument so they can't be swapped at the call site; see the like-named
/// [`Reconcile`] fields for each view's role.
struct PlacementViews<'a> {
    ideal: &'a [Vec<WorkerIndex>],
    held: &'a [Vec<WorkerIndex>],
    committed: &'a [Vec<WorkerIndex>],
    routed: &'a [Vec<WorkerIndex>],
}

struct Reconcile<'a> {
    chunks: &'a [ReplicatedChunk<'a>],
    workers: &'a [&'a Worker],
    rings: &'a Rings,
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
    /// Pre-cycle committed-ideal holders per chunk — the durability floor eviction must not breach.
    /// This is the exact set `step_safety` retention measures against, so counting *these* survivors
    /// (not the drain-and-download-polluted `held`) is what keeps eviction from hard-deleting a copy
    /// the donor's floor still needs.
    committed: &'a [Vec<WorkerIndex>],
    /// Pre-cycle confirmed-routing holders per chunk. Soft: only orders eviction victims so a
    /// still-routed copy is sacrificed last; never blocks an eviction (confirmation lag is unbounded
    /// below a full quorum).
    routed: &'a [Vec<WorkerIndex>],
    /// Chunk indices each worker currently holds (inverse of `held`), built once. Lets Pass-2
    /// eviction enumerate a worker's held-not-ideal copies without scanning every chunk.
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
        // Never-routed is what makes a chunk new here, not merely holding nothing: a chunk portals
        // already route to arrives with no keepable copies once its last holder departs, and the
        // all-or-nothing rule would take it from some copies to none.
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

        // Phase C — floor add-back (invariant 4).
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
        let held = &self.held[chunk_index];
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
            let cost = if held.contains(&worker) {
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

    /// Pass 2: walk the chunk's ring again in the same order, and at the first worker whose free room
    /// plus its reclaimable bytes cover the chunk, evict just enough reclaimable copies to fit, then
    /// place the chunk the same cycle (same-cycle swap — the worker deletes before it downloads, so
    /// accept the momentary over-commit).
    ///
    /// **Durability is the hard invariant; routing consistency is best-effort.** (Worker-confirmation
    /// lag is unbounded below a full quorum — a stalled watermark pins the confirmed routing — so
    /// hard-protecting routing would pin disk and freeze the scheduler's reaction to ingestion and
    /// worker churn.)
    ///
    /// *Hard floor — committed copies.* A held copy of chunk `X` on `w` may be evicted iff
    /// `w ∉ chunk_workers[X]` (X is not publishing `w` this cycle), `(X, w) ∉ evicted`, and X keeps
    /// ≥ `min(min_replication, |committed[X]|)` of its **committed** copies without `w`. `committed[X]`
    /// is the pre-cycle committed ideal — exactly what `step_safety` retention measures — so counting
    /// survivors here is what stops eviction from hard-deleting a copy the donor's durability floor
    /// still needs. `|committed[X]| = 0` ⇒ floor 0 ⇒ freely reclaimable (a holderless/removed chunk has
    /// no floor to breach, preserving ADR-0001 room). Copies of X already evicted this cycle (on other
    /// workers) are subtracted, so several floors preempting one donor can't collectively take it below
    /// the floor. Everything above the committed floor — bonus copies and leftover drains — stays
    /// reclaimable; weight/bonus replication is soft, floors are hard.
    ///
    /// *Soft — routing, differentiated by the starved chunk's visibility.* `routed[X]` orders victims:
    /// copies the routing has already moved off (`w ∉ routed[X]`) are evicted before still-routed ones.
    /// - **C portal-visible** (an existing degraded chunk — the ADR-0001 case): routing is pure
    ///   ordering and never a veto. A still-routed donor copy above the durability floor may be evicted
    ///   as a last resort; the transient read miss is the documented routing-lag cost
    ///   (`portal_consistency_misses`), bounded because the chunk keeps a covered holder elsewhere.
    /// - **C not portal-visible** (a new chunk nobody reads yet): routing is a *hard* veto on the
    ///   donor — a candidate `(X, w)` with `w ∈ routed[X]` is rejected. A new chunk reclaims only
    ///   non-routed space; if that isn't enough it defers via the atomic-floor exclusion
    ///   (backpressure). Principle: don't sacrifice a live chunk's read availability to place a chunk
    ///   nobody reads yet. The durability guard applies in both cases and is never relaxed.
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
        // A new (not-yet-portal-visible) chunk C nobody reads yet must not evict a still-routed copy
        // to seat itself: that would trade a live chunk's read availability for a chunk with no
        // readers. A new C reclaims only non-routed space; if that isn't enough it defers via the
        // atomic-floor exclusion (backpressure), intended. A portal-visible C (an existing degraded
        // chunk — the ADR-0001 case) keeps the soft-routing behavior: a still-routed donor copy above
        // the durability floor may be evicted as a last resort.
        let c_visible = chunk.is_portal_visible;
        let workers = self.workers;
        let rings = self.rings;
        let worker_capacity = self.worker_capacity;
        let committed = self.committed;
        let routed = self.routed;
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

            // Each candidate: whether the confirmed routing still addresses this copy (soft ordering
            // key) and its byte size. `still_routed = false` ⇒ evict first.
            let mut candidates: Vec<(bool, u64, ChunkIndex)> = held_by_worker[w]
                .iter()
                .copied()
                .filter(|&x| {
                    let xi = x as usize;
                    // w already ∈ held[X] (candidates come from held_by_worker[w]).
                    xi != chunk_index
                        && !chunk_workers[xi].contains(&worker)   // X not publishing w this cycle
                        && !evicted.contains(&(x, worker))        // not already evicted
                        // A new chunk reclaims only non-routed space: never sacrifice a live chunk's
                        // routed read for a chunk nobody reads yet. Visible C keeps soft routing.
                        && (c_visible || !routed[xi].contains(&worker))
                        // Durability-hard floor: X keeps ≥ min(min_replication, |committed[X]|) of its
                        // committed copies after removing w. `committed[X]` is the pre-cycle committed
                        // ideal (what step_safety retention measures), so this is the exact floor the
                        // donor must not breach. Copies of X already evicted this cycle (on other
                        // workers) are subtracted, so several floors preempting one donor can't
                        // collectively take it below the floor. |committed[X]| = 0 ⇒ floor 0 ⇒ freely
                        // reclaimable (holderless/removed chunk has no floor).
                        && {
                            let floor = (min_replication as usize).min(committed[xi].len());
                            committed[xi]
                                .iter()
                                .filter(|&&w2| w2 != worker && !evicted.contains(&(x, w2)))
                                .count()
                                >= floor
                        }
                })
                .map(|x| {
                    let still_routed = routed[x as usize].contains(&worker);
                    (still_routed, chunks[x as usize].size as u64, x)
                })
                .collect();
            // Best-effort routing: evict not-routed copies first (primary key), largest-first within a
            // routing class (tiebreak). A still-routed copy is sacrificed only when the not-routed ones
            // don't free enough room for the starved floor.
            candidates.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));

            // Cheap pre-sum: never evict on a worker that still can't fit the chunk — spill instead.
            let reclaimable: u64 = candidates.iter().map(|(_, s, _)| *s).sum();
            if free + reclaimable < size {
                return false;
            }

            let mut freed = free;
            for (_, s, x) in candidates {
                if freed >= size {
                    break;
                }
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
