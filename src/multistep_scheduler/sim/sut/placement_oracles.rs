//! Checks over a storage [`Snapshot`] that flag placements the scheduler should never produce.
//! Three things are checked:
//!   - no worker stores more bytes than its capacity;
//!   - each chunk gets enough copies. A chunk's target copy count is the *floor* (`min_replication`).
//!     A new chunk is made visible only once it has the whole floor — never partially — and an
//!     existing chunk doesn't abruptly lose copies it already holds. The floor is a goal, not a
//!     guarantee: under saturation a chunk may rest below it — an accepted tradeoff, not a bug;
//!   - a portal only routes a query to a worker that actually holds the chunk: while a routed
//!     chunk still has any active, accountable worker, at least one routed worker must really hold
//!     it (within the drain window M). This — not the floor — is what keeps a chunk answerable.
//!
//! Chunk-correction checks live separately, in
//! [`correction_oracle`](crate::scheduler_storage::test_harness::correction_oracle).
//!
//! Each check only detects: it returns the first problem it finds as a ready-to-panic message
//! (plus the offending chunk when the failure dump needs it) and never panics itself — the SUT
//! does the reporting.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::scheduler_storage::test_harness::inspect::Snapshot;
use crate::scheduler_storage::{
    ChunkPk, PortalAssignment, SchemaBundle, Tick, WorkerAssignment, WorkerPk,
};

/// Every chunk the portal routes must be held by at least one of its routed workers — by what the
/// worker actually downloaded (`holds`), not the published assignment (that superset is
/// `published_coverage`'s job). Otherwise a query lands on a worker without the data and fails. Only
/// checked while `snapshot_age < m_ticks`; beyond M a worker may have legitimately deleted it.
///
/// A violation needs a routed worker that is still active and caught up to the snapshot's watermark
/// while none of the routed workers holds the chunk. Routing only to departed workers is a
/// replication concern, not MVCC; below a 100% quorum, routing to stragglers is the documented X%
/// tradeoff (`docs/mvcc-storage.md`, "Slow confirmation").
pub(super) fn portal_consistency(
    snapshot: &PortalAssignment,
    snapshot_age: Tick,
    m_ticks: Tick,
    holds: impl Fn(WorkerPk, &ChunkPk) -> bool,
    is_active: impl Fn(WorkerPk) -> bool,
    accountable: impl Fn(WorkerPk) -> bool,
) -> Result<(), String> {
    if snapshot_age >= m_ticks {
        return Ok(()); // beyond M, degradation is allowed
    }
    for (chunk, routed) in &snapshot.chunk_workers {
        if routed.iter().any(|&w| holds(w, chunk)) {
            continue;
        }
        if routed.iter().any(|&w| is_active(w) && accountable(w)) {
            return Err(format!(
                "portal routes chunk {chunk:?} to workers {routed:?} but no active routed worker \
                 holds it (snapshot age {snapshot_age} < M={m_ticks}) — a query for this chunk would fail",
            ));
        }
    }
    Ok(())
}

/// How many routed chunks a query would actually miss: within the M window, the chunk is routed to
/// at least one active worker yet none of its active routed workers holds it. This is the failure
/// [`portal_consistency`] forbids at a full quorum, but counted rather than raised — and counted
/// regardless of accountability, since below 100% the whole point is that the scheduler routes
/// ahead of confirmation. The count is pure telemetry: it is the documented cost of the X% tradeoff.
pub(super) fn portal_consistency_misses(
    snapshot: &PortalAssignment,
    snapshot_age: Tick,
    m_ticks: Tick,
    holds: impl Fn(WorkerPk, &ChunkPk) -> bool,
    is_active: impl Fn(WorkerPk) -> bool,
) -> usize {
    if snapshot_age >= m_ticks {
        return 0; // beyond M, degradation is allowed
    }
    snapshot
        .chunk_workers
        .iter()
        .filter(|(chunk, routed)| {
            routed.iter().any(|&w| is_active(w))
                && !routed.iter().any(|&w| is_active(w) && holds(w, chunk))
        })
        .count()
}

/// Superset invariant (`docs/mvcc-storage.md`, "Deferred removal"): the published worker
/// assignment (`ideal ∪ stale`) must cover every `(chunk, worker)` pair the portal assignment
/// routes. Otherwise a worker could delete a chunk that portals still route to it.
///
/// Only pairs whose worker is still active are checked. A departed worker's stale rows are purged
/// on departure, so transient routing to a departed worker is a documented availability event, not
/// a coverage bug.
pub(super) fn published_coverage(
    portal: &PortalAssignment,
    worker: Option<&WorkerAssignment>,
    is_active: impl Fn(WorkerPk) -> bool,
) -> Result<(), String> {
    for (chunk, routed) in &portal.chunk_workers {
        for &w in routed {
            if !is_active(w) {
                continue;
            }
            let covered = worker.is_some_and(|assignment| {
                assignment
                    .chunk_workers
                    .get(chunk)
                    .is_some_and(|holders| holders.contains(&w))
            });
            if !covered {
                return Err(format!(
                    "published worker assignment does not cover portal routing: portal routes \
                     chunk {chunk:?} to active worker {w:?}, but the worker assignment \
                     (ideal ∪ stale) no longer includes that pair — the worker may delete data \
                     portals still route to",
                ));
            }
        }
    }
    Ok(())
}

/// Every chunk a published assignment names must resolve under a freshly-generated
/// [`SchemaBundle`]: the schema it was stamped with at insert
/// (`WorkerAssignmentChunk::schema_id`) must still be present in the bundle, or a worker/portal
/// couldn't derive the chunk's file set. Either assignment may be absent (nothing published yet).
///
/// `excluded` (the SUT's `chunks_excluded_from_floor`) skips chunks already shedding:
/// `worker_assignment` is a cache only refreshed on a successful scheduling cycle, so during a
/// shortage streak it can lag behind the freshly-regenerated `bundle` — a lagging chunk's schema
/// may have already been legitimately GC'd from the bundle, which is not a resolution failure.
///
/// FIXME: the exclusion masks a real production hazard rather than a benign lag. During a shortage
/// streak the published worker assignment stays frozen while the bundle keeps advancing, so a
/// worker that joins mid-streak downloads that frozen assignment and then cannot resolve the
/// GC'd schema of a still-named (removing) chunk — the exact failure this oracle exists to catch.
/// Fix: don't advance the schema bundle when a cycle hits `Shortage`; keep it in lockstep with the
/// worker assignment that clients actually consume, so a schema is never GC'd while an assignment
/// still references its chunk. Once that holds, this exclusion can drop.
pub(super) fn schema_bundle_consistency(
    bundle: &SchemaBundle,
    worker_assignment: Option<&WorkerAssignment>,
    portal_assignment: Option<&PortalAssignment>,
    excluded: &BTreeSet<ChunkPk>,
) -> Result<(), String> {
    let sources = [
        ("worker assignment", worker_assignment.map(|a| &a.chunks)),
        ("portal assignment", portal_assignment.map(|a| &a.chunks)),
    ];
    for (label, chunks) in sources {
        let Some(chunks) = chunks else { continue };
        for (chunk_pk, chunk) in chunks {
            if excluded.contains(chunk_pk) {
                continue;
            }
            if !bundle.contains(chunk.schema_id) {
                return Err(format!(
                    "{label} names chunk {chunk_pk:?} with schema {}, absent from a \
                     freshly-generated schema bundle — a worker or portal couldn't resolve this \
                     chunk's file set",
                    chunk.schema_id,
                ));
            }
        }
    }
    Ok(())
}

/// A converged-floor violation: the under-replicated chunk (for the failure dump) and the message.
pub(super) struct FloorViolation {
    pub(super) chunk: usize,
    pub(super) message: String,
}

/// Per-step safety on the post-cycle `after` snapshot. Two guarantees: no worker is over capacity,
/// and floors are retained. `held` is the pre-cycle ideal. Chunks in `removing` are exempt from
/// floors, since whole-chunk removal legitimately sheds every copy; but a removing chunk's bytes
/// still count toward overcommit while it is on disk.
pub(super) fn step_safety(
    held: &BTreeMap<ChunkPk, BTreeSet<WorkerPk>>,
    after: &Snapshot,
    removing: &BTreeSet<ChunkPk>,
    capacity: u64,
    min_replication: usize,
) -> Result<(), String> {
    no_overcommit(after, capacity)?;
    let live_held: BTreeMap<ChunkPk, BTreeSet<WorkerPk>> = held
        .iter()
        .filter(|(pk, _)| !removing.contains(pk))
        .map(|(pk, holders)| (*pk, holders.clone()))
        .collect();
    let mut published = after.physical_holders_by_pk();
    published.retain(|pk, _| !removing.contains(pk));
    retention_and_atomic_publication(&live_held, &published, min_replication)
}

/// Weak converged-floor oracle: a below-floor chunk is a violation only when some worker currently
/// has free room for another copy.
pub(super) fn floor_locally_feasible(
    snapshot: &Snapshot,
    active: &[WorkerPk],
    capacity: u64,
    min_replication: usize,
    saturation: f64,
) -> Result<(), FloorViolation> {
    let load = snapshot.byte_load(&snapshot.ideal);
    for chunk in 0..snapshot.len() {
        let placed = snapshot.ideal[chunk].len();
        if placed >= min_replication {
            continue;
        }
        let size = u64::from(snapshot.chunk_sizes[chunk]);
        let workers_with_room = active
            .iter()
            .copied()
            .filter(|worker| {
                !snapshot.ideal[chunk].contains(worker)
                    && load.get(worker).copied().unwrap_or(0) + size <= capacity
            })
            .count();
        if workers_with_room >= min_replication - placed {
            return Err(FloorViolation {
                chunk,
                message: format!(
                    "chunk {chunk} under-replicated at convergence ({placed}/{min_replication}) yet \
                     {workers_with_room} eligible workers had room for another copy — room existed \
                     but it wasn't placed (chunks={}, workers={}, saturation={saturation})",
                    snapshot.len(),
                    active.len(),
                ),
            });
        }
    }
    Ok(())
}

/// Strong converged-floor oracle: a below-floor chunk is a violation when room could still be made
/// by dropping an over-replicated chunk's bonus copy.
pub(super) fn floors_preempt_bonuses(
    snapshot: &Snapshot,
    active: &[WorkerPk],
    capacity: u64,
    min_replication: usize,
    saturation: f64,
) -> Result<(), FloorViolation> {
    let load = snapshot.byte_load(&snapshot.ideal);
    let mut budget: Vec<usize> = (0..snapshot.len())
        .map(|chunk| snapshot.ideal[chunk].len().saturating_sub(min_replication))
        .collect();

    for chunk in 0..snapshot.len() {
        let placed = snapshot.ideal[chunk].len();
        let Some(needed) = min_replication.checked_sub(placed).filter(|&n| n > 0) else {
            continue;
        };
        let recoverable = active
            .iter()
            .copied()
            .filter(|worker| !snapshot.ideal[chunk].contains(worker))
            .filter(|worker| {
                can_host_after_dropping_bonuses(
                    snapshot,
                    capacity,
                    chunk,
                    *worker,
                    &load,
                    &mut budget,
                )
            })
            .take(needed)
            .count()
            == needed;
        if recoverable {
            return Err(FloorViolation {
                chunk,
                message: format!(
                    "chunk {chunk} under-replicated at convergence ({placed}/{min_replication}) yet \
                     room for {needed} more copy(ies) could be freed by dropping other chunks' bonus \
                     copies — floors must preempt bonuses (chunks={}, workers={}, saturation={saturation})",
                    snapshot.len(),
                    active.len(),
                ),
            });
        }
    }
    Ok(())
}

/// No worker's physical footprint (`ideal ∪ stale`) exceeds its capacity.
fn no_overcommit(snapshot: &Snapshot, capacity: u64) -> Result<(), String> {
    for (peer, bytes) in snapshot.byte_load(&snapshot.physical_holder_sets()) {
        if bytes > capacity {
            return Err(format!(
                "overcommit: worker {peer:?} holds {bytes} > capacity {capacity}"
            ));
        }
    }
    Ok(())
}

/// Per-step retention + publication — not the absolute `min_replication` floor. Two rules:
/// (1) a new chunk publishes all-or-nothing: 0 copies or at least the floor, never a partial
/// 1..floor; (2) an existing chunk keeps at least `min(floor, |held|)` of its copies.
///
/// "New" means absent from `held` (no pre-cycle holder). A chunk whose holders all just departed is
/// still in `held` but empty, so it takes the retention branch at `min(floor, 0) = 0` — nothing to
/// retain, not a waived floor. Adequacy (every chunk reaches `min_replication` when capacity allows)
/// is checked at convergence by [`floor_locally_feasible`] / [`floors_preempt_bonuses`].
fn retention_and_atomic_publication(
    held: &BTreeMap<ChunkPk, BTreeSet<WorkerPk>>,
    published: &BTreeMap<ChunkPk, BTreeSet<WorkerPk>>,
    min_replication: usize,
) -> Result<(), String> {
    for (pk, published_holders) in published {
        match held.get(pk) {
            None => {
                if !published_holders.is_empty() && published_holders.len() < min_replication {
                    return Err(format!(
                        "new chunk {pk:?} published under-replicated: {} copies",
                        published_holders.len(),
                    ));
                }
            }
            Some(held_holders) => {
                let kept = published_holders.intersection(held_holders).count();
                let floor = min_replication.min(held_holders.len());
                if kept < floor {
                    return Err(format!(
                        "chunk {pk:?} kept {kept} current copies, below retention floor {floor}",
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Could `worker` host `chunk` after dropping other chunks' bonus copies that sit on it? On
/// success, commits the drops against `budget`; on failure, leaves `budget` untouched.
fn can_host_after_dropping_bonuses(
    snapshot: &Snapshot,
    capacity: u64,
    chunk: usize,
    worker: WorkerPk,
    load: &HashMap<WorkerPk, u64>,
    budget: &mut [usize],
) -> bool {
    let size = u64::from(snapshot.chunk_sizes[chunk]);
    let mut room = capacity.saturating_sub(load.get(&worker).copied().unwrap_or(0));
    let mut dropped = Vec::new();
    for (other, &remaining) in budget.iter().enumerate() {
        if room >= size {
            break;
        }
        if other != chunk && remaining > 0 && snapshot.ideal[other].contains(&worker) {
            room += u64::from(snapshot.chunk_sizes[other]);
            dropped.push(other);
        }
    }
    if room < size {
        return false;
    }
    for other in dropped {
        budget[other] -= 1;
    }
    true
}

/// Each oracle is tested against a minimal violating state and a near-miss that must pass. An
/// oracle that never fires would turn the property suite into green noise (false confidence).
#[cfg(test)]
mod tests {
    use super::*;

    fn wk(ids: &[i32]) -> Vec<WorkerPk> {
        ids.iter().map(|&w| WorkerPk(w)).collect()
    }

    /// Hand-built snapshot: `sizes[i]` is chunk `i`'s byte size; `ideal[i]` / `stale[i]` are its
    /// holders.
    fn snap(sizes: &[u32], ideal: &[&[i32]], stale: &[&[i32]]) -> Snapshot {
        let holders = |sets: &[&[i32]]| -> Vec<BTreeSet<WorkerPk>> {
            sets.iter()
                .map(|workers| workers.iter().map(|&w| WorkerPk(w)).collect())
                .collect()
        };
        Snapshot {
            chunk_pks: (0..sizes.len()).map(|i| ChunkPk(i as i64)).collect(),
            chunk_ids: (0..sizes.len()).map(|i| format!("c{i}")).collect(),
            chunk_sizes: sizes.to_vec(),
            ideal: holders(ideal),
            stale: holders(stale),
            peer_ids: HashMap::new(),
        }
    }

    fn held(entries: &[(i64, &[i32])]) -> BTreeMap<ChunkPk, BTreeSet<WorkerPk>> {
        entries
            .iter()
            .map(|(pk, workers)| (ChunkPk(*pk), workers.iter().map(|&w| WorkerPk(w)).collect()))
            .collect()
    }

    fn portal(routing: &[(i64, &[i32])]) -> PortalAssignment {
        PortalAssignment {
            id: 1,
            chunk_workers: routing
                .iter()
                .map(|(pk, workers)| (ChunkPk(*pk), wk(workers)))
                .collect(),
            chunks: BTreeMap::new(),
            workers: BTreeMap::new(),
        }
    }

    fn worker_assignment(routing: &[(i64, &[i32])]) -> WorkerAssignment {
        WorkerAssignment {
            id: 1,
            chunk_workers: routing
                .iter()
                .map(|(pk, workers)| (ChunkPk(*pk), wk(workers)))
                .collect(),
            chunks: BTreeMap::new(),
            workers: BTreeMap::new(),
            replication_by_weight: BTreeMap::new(),
        }
    }

    // ---- no_overcommit -------------------------------------------------------------------

    #[test]
    fn no_overcommit_fires_one_byte_over() {
        // 60 ideal + 41 stale = 101 > 100 capacity; stale copies count because they occupy disk.
        let snapshot = snap(&[60, 41], &[&[1], &[]], &[&[], &[1]]);
        assert!(no_overcommit(&snapshot, 100).is_err());
    }

    #[test]
    fn no_overcommit_passes_at_exactly_capacity() {
        let snapshot = snap(&[60, 40], &[&[1], &[]], &[&[], &[1]]);
        assert!(no_overcommit(&snapshot, 100).is_ok());
    }

    #[test]
    fn no_overcommit_does_not_double_charge_ideal_and_stale_overlap() {
        // The same (chunk, worker) pair in both ideal and stale is one physical copy, not two.
        let snapshot = snap(&[100], &[&[1]], &[&[1]]);
        assert!(no_overcommit(&snapshot, 100).is_ok());
    }

    // ---- retention_and_atomic_publication ------------------------------------

    #[test]
    fn retention_and_atomic_publication_fires_on_under_replicated_new_chunk() {
        // New chunk published with 1 copy < floor 2.
        let published = held(&[(0, &[1])]);
        assert!(retention_and_atomic_publication(&held(&[]), &published, 2).is_err());
    }

    #[test]
    fn retention_and_atomic_publication_passes_excluded_new_chunk_and_atomic_floor() {
        // For a new chunk, both 0 copies (excluded) and ≥ floor copies are legal.
        let published = held(&[(0, &[]), (1, &[1, 2])]);
        assert!(retention_and_atomic_publication(&held(&[]), &published, 2).is_ok());
    }

    #[test]
    fn retention_and_atomic_publication_fires_when_retention_floor_broken() {
        // Only the held copy on worker 1 is kept (< floor 2). The new copy on worker 4 does not
        // compensate, because retention counts held copies only (a brand-new copy isn't
        // downloaded yet).
        let previously = held(&[(0, &[1, 2, 3])]);
        let published = held(&[(0, &[1, 4])]);
        assert!(retention_and_atomic_publication(&previously, &published, 2).is_err());
    }

    #[test]
    fn step_safety_exempts_removing_chunks_from_floors() {
        // Shedding every copy breaks the retention floor for a live chunk. But once the chunk is
        // marked removing, this is the legal whole-chunk removal path.
        let previously = held(&[(0, &[1, 2])]);
        let after = snap(&[10], &[&[]], &[&[]]);
        let nothing: BTreeSet<ChunkPk> = BTreeSet::new();
        assert!(step_safety(&previously, &after, &nothing, 100, 2).is_err());
        let removing: BTreeSet<ChunkPk> = [ChunkPk(0)].into_iter().collect();
        assert!(step_safety(&previously, &after, &removing, 100, 2).is_ok());
    }

    #[test]
    fn retention_and_atomic_publication_passes_when_floor_of_held_copies_kept() {
        // Floor is min(min_replication, |held|): a chunk that only ever had one copy keeps that
        // one copy.
        let previously = held(&[(0, &[1, 2, 3]), (1, &[5])]);
        let published = held(&[(0, &[1, 2]), (1, &[5])]);
        assert!(retention_and_atomic_publication(&previously, &published, 2).is_ok());
    }

    #[test]
    fn retention_and_atomic_publication_continuing_chunk_with_empty_held_takes_retention_floor() {
        // A saturated, already-published chunk whose sole holder departed is PRESENT in held with
        // an empty active set. Because it physically pre-existed, it takes the retention branch
        // (floor min(2, 0) = 0). Republishing at 1 copy is a tolerated shortfall, not a violation.
        // Classification is by key PRESENCE — this test pins that contract against regression.
        let previously = held(&[(0, &[])]);
        let published = held(&[(0, &[1])]);
        assert!(retention_and_atomic_publication(&previously, &published, 2).is_ok());
    }

    #[test]
    fn retention_and_atomic_publication_new_chunk_distinguished_from_empty_held_by_key_absence() {
        // Same 1 < floor 2 publication as the previous test, but here the chunk is ABSENT from
        // held (genuinely new, no physical copy pre-cycle). It must still fail the first-
        // publication floor. Contrast: absence-from-held => new; empty-set-in-held => continuing.
        let published = held(&[(0, &[1])]);
        assert!(retention_and_atomic_publication(&held(&[]), &published, 2).is_err());
    }

    // ---- converged floor oracles -----------------------------------------------------------

    #[test]
    fn floor_locally_feasible_fires_when_room_stands_free() {
        // Chunk 1 is at 1/2 while worker 2 (empty) has room for its 50 bytes.
        let snapshot = snap(&[50, 50], &[&[1], &[1]], &[&[], &[]]);
        assert!(floor_locally_feasible(&snapshot, &wk(&[1, 2]), 100, 2, 0.9).is_err());
    }

    #[test]
    fn floor_locally_feasible_passes_when_no_worker_has_room() {
        // Chunk 1 at 1/2, but the only other worker is full; the weak oracle tolerates it.
        let snapshot = snap(&[100, 100], &[&[1, 2], &[1]], &[&[], &[]]);
        assert!(floor_locally_feasible(&snapshot, &wk(&[1, 2]), 100, 2, 0.9).is_ok());
    }

    #[test]
    fn floors_preempt_bonuses_fires_when_dropping_a_bonus_frees_room() {
        // Chunk 0's bonus copy on worker 2 blocks chunk 1 (0/1). The weak oracle passes because
        // disks look full — exactly the gap the strong oracle closes.
        let snapshot = snap(&[100, 100], &[&[1, 2], &[]], &[&[], &[]]);
        assert!(floor_locally_feasible(&snapshot, &wk(&[1, 2]), 100, 1, 0.9).is_ok());
        assert!(floors_preempt_bonuses(&snapshot, &wk(&[1, 2]), 100, 1, 0.9).is_err());
    }

    #[test]
    fn floors_preempt_bonuses_passes_when_no_bonus_exists() {
        // Both workers are full of floor copies, so nothing may be dropped. Therefore chunk 1's
        // under-placement is a genuine shortage and is tolerated.
        let snapshot = snap(&[100, 100, 100], &[&[1], &[], &[2]], &[&[], &[], &[]]);
        assert!(floors_preempt_bonuses(&snapshot, &wk(&[1, 2]), 100, 1, 0.9).is_ok());
    }

    #[test]
    fn floors_preempt_bonuses_passes_when_freed_room_is_still_too_small() {
        // The only droppable bonus (40 bytes) frees too little for chunk 1's 100 bytes on either
        // worker. So the under-placement is genuine and tolerated.
        let snapshot = snap(
            &[40, 100, 60, 60],
            &[&[1, 2], &[], &[1], &[2]],
            &[&[], &[], &[], &[]],
        );
        assert!(floors_preempt_bonuses(&snapshot, &wk(&[1, 2]), 100, 1, 0.9).is_ok());
    }

    // ---- portal_consistency ----------------------------------------------------------------

    #[test]
    fn portal_consistency_fires_when_accountable_worker_lacks_chunk() {
        let snapshot = portal(&[(0, &[1, 2])]);
        let verdict = portal_consistency(&snapshot, 0, 5, |_, _| false, |_| true, |_| true);
        assert!(verdict.is_err());
    }

    #[test]
    fn portal_consistency_passes_when_any_routed_worker_holds() {
        let snapshot = portal(&[(0, &[1, 2])]);
        let verdict =
            portal_consistency(&snapshot, 0, 5, |w, _| w == WorkerPk(2), |_| true, |_| true);
        assert!(verdict.is_ok());
    }

    #[test]
    fn portal_consistency_tolerates_snapshot_at_or_beyond_m() {
        let snapshot = portal(&[(0, &[1])]);
        let verdict = portal_consistency(&snapshot, 5, 5, |_, _| false, |_| true, |_| true);
        assert!(verdict.is_ok());
    }

    #[test]
    fn portal_consistency_tolerates_fully_departed_routing() {
        let snapshot = portal(&[(0, &[1, 2])]);
        let verdict = portal_consistency(&snapshot, 0, 5, |_, _| false, |_| false, |_| true);
        assert!(verdict.is_ok());
    }

    #[test]
    fn portal_consistency_tolerates_unaccountable_stragglers() {
        // Every routed worker is an active straggler — this is the X% tradeoff, not a violation.
        let snapshot = portal(&[(0, &[1, 2])]);
        let verdict = portal_consistency(&snapshot, 0, 5, |_, _| false, |_| true, |_| false);
        assert!(verdict.is_ok());
    }

    #[test]
    fn portal_consistency_fires_when_one_routed_worker_is_caught_up() {
        let snapshot = portal(&[(0, &[1, 2])]);
        let verdict = portal_consistency(
            &snapshot,
            0,
            5,
            |_, _| false,
            |_| true,
            |w| w == WorkerPk(2),
        );
        assert!(verdict.is_err());
    }

    // ---- published_coverage ----------------------------------------------------------------

    #[test]
    fn published_coverage_fires_on_uncovered_active_pair() {
        let pa = portal(&[(0, &[1])]);
        let wa = worker_assignment(&[(0, &[2])]);
        assert!(published_coverage(&pa, Some(&wa), |_| true).is_err());
    }

    #[test]
    fn published_coverage_fires_on_missing_chunk() {
        let pa = portal(&[(0, &[1])]);
        let wa = worker_assignment(&[]);
        assert!(published_coverage(&pa, Some(&wa), |_| true).is_err());
    }

    #[test]
    fn published_coverage_passes_when_worker_assignment_covers_routing() {
        let pa = portal(&[(0, &[1])]);
        let wa = worker_assignment(&[(0, &[1, 2])]);
        assert!(published_coverage(&pa, Some(&wa), |_| true).is_ok());
    }

    #[test]
    fn published_coverage_skips_departed_workers() {
        let pa = portal(&[(0, &[1])]);
        let wa = worker_assignment(&[(0, &[2])]);
        assert!(published_coverage(&pa, Some(&wa), |_| false).is_ok());
    }

    #[test]
    fn published_coverage_with_no_worker_assignment_requires_empty_routing() {
        assert!(published_coverage(&portal(&[]), None, |_| true).is_ok());
        assert!(published_coverage(&portal(&[(0, &[1])]), None, |_| true).is_err());
    }
}
