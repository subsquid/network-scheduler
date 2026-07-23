//! Checks over a storage [`Snapshot`] that flag placements the scheduler must never produce. Each
//! check only *detects*: it returns the first problem as a ready-to-panic message (plus the
//! offending chunk when the failure dump needs it) and never panics itself — the SUT reports.
//!
//! Three families are checked:
//!   - **Capacity** — no worker stores more bytes than its capacity.
//!   - **Floor** — each chunk should reach `min_replication` copies. A new chunk turns visible only
//!     with its whole floor (never partial); an existing chunk never abruptly loses copies it holds.
//!     The floor is a goal, not a guarantee: under saturation a chunk may rest below it — a tradeoff,
//!     not a bug.
//!   - **Routing coverage** — a portal only routes a query to a worker that can answer it. This, not
//!     the floor, is what keeps a chunk answerable. See the coverage model below.
//!
//! # Coverage model (ADR 0001): durability is hard, routing is best-effort
//! Same-cycle floor preemption may delete an above-floor copy that routing still points at, and
//! confirmation lag is unbounded below a full quorum — so routing can trail the durable placement.
//! A chunk that keeps a covered holder *somewhere* is **covered**; a stale route to it is bounded
//! routing-lag, not a failure ([`portal_consistency_misses`] counts these). A chunk with no covered
//! holder at all is **globally uncovered** — an unanswerable query, the only fatal case. Two states
//! are exempt entirely: routing only to departed workers (their stale rows are purged on departure —
//! an availability event) and a recorded shortage (an over-subscribed fleet can't hold every floor).
//!
//! Chunk-correction checks live separately, in
//! [`correction_oracle`](crate::scheduler_storage::test_harness::correction_oracle).

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::scheduler_storage::test_harness::inspect::Snapshot;
use crate::scheduler_storage::{
    ChunkPk, PortalAssignment, SchemaBundle, Tick, WorkerAssignment, WorkerPk,
};

/// Routing coverage by physical possession: within the M window, a routed chunk must be physically
/// held (`holds` — not the published listing, which is [`published_coverage`]'s job) by one of its
/// routed workers, or a query lands on a worker without the data. Beyond M the check stands down (a
/// worker may have legitimately deleted it).
///
/// Fires only when the chunk is globally uncovered *and* a routed worker is still active and caught
/// up to the snapshot's watermark; otherwise it is excused bounded routing-lag (`covered_anywhere`)
/// or the X% straggler tradeoff (see the module coverage model; `docs/mvcc-storage.md`, "Slow
/// confirmation").
pub(super) fn portal_consistency(
    snapshot: &PortalAssignment,
    snapshot_age: Tick,
    m_ticks: Tick,
    holds: impl Fn(WorkerPk, &ChunkPk) -> bool,
    is_active: impl Fn(WorkerPk) -> bool,
    accountable: impl Fn(WorkerPk) -> bool,
    covered_anywhere: impl Fn(&ChunkPk) -> bool,
) -> Result<(), String> {
    if snapshot_age >= m_ticks {
        return Ok(()); // beyond M, degradation is allowed
    }
    for (chunk, routed) in &snapshot.chunk_workers {
        if routed.iter().any(|&w| holds(w, chunk)) {
            continue;
        }
        // Durability intact elsewhere ⇒ bounded routing-lag, not a strand (see the doc above).
        if covered_anywhere(chunk) {
            continue;
        }
        if routed.iter().any(|&w| is_active(w) && accountable(w)) {
            return Err(format!(
                "portal routes chunk {chunk:?} to workers {routed:?} but no active routed worker \
                 holds it and the chunk has no covered holder anywhere (snapshot age \
                 {snapshot_age} < M={m_ticks}) — a query for this chunk would fail",
            ));
        }
    }
    Ok(())
}

/// A published portal assignment never routes a chunk to nobody: an empty holder set is
/// unserveable, and both backends delete the row instead of storing one. The consistency oracles
/// can't see this — they quantify over routed workers, so an empty set passes them vacuously.
pub(super) fn routing_is_non_empty(snapshot: &PortalAssignment) -> Result<(), String> {
    for (chunk, routed) in &snapshot.chunk_workers {
        if routed.is_empty() {
            return Err(format!(
                "portal assignment routes chunk {chunk:?} to no worker — the entry should have \
                 been removed, not published empty",
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

/// Routing coverage by the published assignment (ADR 0001, revised): for every chunk with an active
/// routed worker, the worker assignment (`ideal ∪ stale`) must list an active holder **anywhere** —
/// not necessarily a routed one. Fires only when the chunk is globally uncovered; covered-elsewhere
/// is bounded routing-lag (module coverage model; consistent with ADR 0002). Only active workers
/// count on both sides, and the caller stands the check down during a recorded shortage
/// (`is_infeasible`).
///
/// The default check counts a chunk as covered when it is *listed* — an assignment row names an
/// active holder. A listing is only intent, though: that worker may not have downloaded the bytes
/// yet. `require_physical` tightens "covered" to a listed active holder that *actually* holds the
/// chunk (`holds`); a chunk that is listed but held by nobody — covered on paper only — then fails.
/// Paper-only coverage is legitimate and transient (a rejoined worker is re-listed while its disk is
/// still empty; a committed-but-never-fetched replacement can lag unboundedly), so this mode is
/// **classification-only, at every quorum** — never fatal here. Eviction-caused physical loss is
/// made fatal by the SUT's edge-triggered `assert_physical_retention`.
pub(super) fn published_coverage(
    portal: &PortalAssignment,
    worker: Option<&WorkerAssignment>,
    is_active: impl Fn(WorkerPk) -> bool,
    holds: impl Fn(WorkerPk, &ChunkPk) -> bool,
    require_physical: bool,
) -> Result<(), String> {
    for (chunk, routed) in &portal.chunk_workers {
        // A fully-departed routing is an availability event, not a strand — skip it.
        if !routed.iter().any(|&w| is_active(w)) {
            continue;
        }
        // Covered iff the assignment lists ANY active holder — the routed workers need not be
        // among them.
        let listed_holders = worker.and_then(|assignment| assignment.chunk_workers.get(chunk));
        let covered_anywhere =
            listed_holders.is_some_and(|holders| holders.iter().any(|&w| is_active(w)));
        if !covered_anywhere {
            return Err(format!(
                "published worker assignment strands portal routing: portal routes chunk \
                 {chunk:?} to active workers {routed:?}, but the worker assignment (ideal ∪ stale) \
                 lists no active holder for it anywhere — the chunk has no covered holder at all and \
                 is unanswerable",
            ));
        }
        if require_physical {
            let covered_physically = listed_holders
                .is_some_and(|holders| holders.iter().any(|&w| is_active(w) && holds(w, chunk)));
            if !covered_physically {
                return Err(format!(
                    "published worker assignment covers chunk {chunk:?} on paper only: it lists \
                     active holders, but none of them physically holds the chunk — legitimate \
                     transiently in a departure→rejoin window, a defect when eviction caused it \
                     (which the physical-retention edge check polices)",
                ));
            }
        }
    }
    Ok(())
}

/// Fixed-point routing liveness: at a drained fixed point (ideal stable, stale empty, every routing
/// diff replayed, no shortage) all deliberate routing lag has resolved, so the confirmed routing
/// must agree with the worker assignment **pair-by-pair** — every active routed worker is listed as
/// a holder of the chunk it is routed for. This is what makes "bounded routing-lag" a checked bound:
/// a stale pair that survives quiescence is a stuck routing update (a diff never emitted or
/// misapplied), not lag.
pub(super) fn routing_matches_assignment_at_fixed_point(
    portal: &PortalAssignment,
    worker: Option<&WorkerAssignment>,
    is_active: impl Fn(WorkerPk) -> bool,
) -> Result<(), String> {
    for (chunk, routed) in &portal.chunk_workers {
        for &w in routed {
            if !is_active(w) {
                continue;
            }
            let listed = worker.is_some_and(|assignment| {
                assignment
                    .chunk_workers
                    .get(chunk)
                    .is_some_and(|holders| holders.contains(&w))
            });
            if !listed {
                return Err(format!(
                    "at the drained fixed point the portal still routes chunk {chunk:?} to active \
                     worker {w:?}, which the worker assignment does not list as a holder — a \
                     permanently stale routing pair (stuck routing update), not bounded lag",
                ));
            }
        }
    }
    Ok(())
}

/// Every chunk a published assignment names must resolve under the [`SchemaBundle`] frozen with
/// that worker assignment: the chunk's pinned `schema_id` must be in the bundle, or a worker/portal
/// couldn't derive its file set. Either assignment may be absent (nothing published yet).
pub(super) fn schema_bundle_consistency(
    bundle: &SchemaBundle,
    worker_assignment: Option<&WorkerAssignment>,
    portal_assignment: Option<&PortalAssignment>,
) -> Result<(), String> {
    let sources = [
        ("worker assignment", worker_assignment.map(|a| &a.chunks)),
        ("portal assignment", portal_assignment.map(|a| &a.chunks)),
    ];
    for (label, chunks) in sources {
        let Some(chunks) = chunks else { continue };
        for (chunk_pk, chunk) in chunks {
            if !bundle.contains(chunk.schema_id) {
                return Err(format!(
                    "{label} names chunk {chunk_pk:?} with schema {}, absent from the schema \
                     bundle frozen with the worker assignment — a worker or portal couldn't \
                     resolve this chunk's file set",
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
    let mut live_held = held.clone();
    live_held.retain(|pk, _| !removing.contains(pk));
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
        // No routed worker holds it and it is covered nowhere — a genuine miss, still fatal under
        // the relaxed (ADR 0001) oracle.
        let snapshot = portal(&[(0, &[1, 2])]);
        let verdict =
            portal_consistency(&snapshot, 0, 5, |_, _| false, |_| true, |_| true, |_| false);
        assert!(verdict.is_err());
    }

    #[test]
    fn portal_consistency_passes_when_any_routed_worker_holds() {
        let snapshot = portal(&[(0, &[1, 2])]);
        let verdict = portal_consistency(
            &snapshot,
            0,
            5,
            |w, _| w == WorkerPk(2),
            |_| true,
            |_| true,
            |_| false,
        );
        assert!(verdict.is_ok());
    }

    #[test]
    fn portal_consistency_excused_when_chunk_covered_elsewhere() {
        // No routed worker holds it, but the chunk keeps a covered holder somewhere (durability
        // intact) — a deliberate preemption's bounded routing-lag, not a strand.
        let snapshot = portal(&[(0, &[1, 2])]);
        let verdict =
            portal_consistency(&snapshot, 0, 5, |_, _| false, |_| true, |_| true, |_| true);
        assert!(verdict.is_ok());
    }

    #[test]
    fn portal_consistency_tolerates_snapshot_at_or_beyond_m() {
        let snapshot = portal(&[(0, &[1])]);
        let verdict =
            portal_consistency(&snapshot, 5, 5, |_, _| false, |_| true, |_| true, |_| false);
        assert!(verdict.is_ok());
    }

    #[test]
    fn portal_consistency_tolerates_fully_departed_routing() {
        let snapshot = portal(&[(0, &[1, 2])]);
        let verdict = portal_consistency(
            &snapshot,
            0,
            5,
            |_, _| false,
            |_| false,
            |_| true,
            |_| false,
        );
        assert!(verdict.is_ok());
    }

    #[test]
    fn portal_consistency_tolerates_unaccountable_stragglers() {
        // Every routed worker is an active straggler — this is the X% tradeoff, not a violation.
        let snapshot = portal(&[(0, &[1, 2])]);
        let verdict = portal_consistency(
            &snapshot,
            0,
            5,
            |_, _| false,
            |_| true,
            |_| false,
            |_| false,
        );
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
            |_| false,
        );
        assert!(verdict.is_err());
    }

    // ---- published_coverage ----------------------------------------------------------------

    /// Listing-only variant most tests use: every worker "holds" everything, physical not required.
    fn coverage_listed(
        pa: &PortalAssignment,
        wa: Option<&WorkerAssignment>,
        is_active: impl Fn(WorkerPk) -> bool,
    ) -> Result<(), String> {
        published_coverage(pa, wa, is_active, |_, _| true, false)
    }

    #[test]
    fn published_coverage_fires_on_globally_uncovered_chunk() {
        // Chunk 0 is routed to active worker 1, and the worker assignment's sole holder (2) is
        // departed (inactive) — so the chunk has NO covered holder anywhere. Genuinely unanswerable:
        // the strand oracle still fires.
        let pa = portal(&[(0, &[1])]);
        let wa = worker_assignment(&[(0, &[2])]);
        assert!(coverage_listed(&pa, Some(&wa), |w| w == WorkerPk(1)).is_err());
    }

    #[test]
    fn published_coverage_fires_on_missing_chunk() {
        // Absent from the worker assignment with a live active routed worker is a strand, not a
        // removal — a removed chunk would be de-visibilitied and absent from the portal too.
        let pa = portal(&[(0, &[1])]);
        let wa = worker_assignment(&[]);
        assert!(coverage_listed(&pa, Some(&wa), |_| true).is_err());
    }

    #[test]
    fn published_coverage_passes_when_worker_assignment_covers_routing() {
        let pa = portal(&[(0, &[1])]);
        let wa = worker_assignment(&[(0, &[1, 2])]);
        assert!(coverage_listed(&pa, Some(&wa), |_| true).is_ok());
    }

    #[test]
    fn published_coverage_passes_when_chunk_keeps_a_covered_holder() {
        // Same-cycle preemption deleted the (chunk 0, worker 1) pair while it is still routed, but the
        // chunk keeps a covered active holder on worker 2. The global-coverage rule tolerates this —
        // the chunk is still answerable somewhere (bounded routing-lag, not a strand).
        let pa = portal(&[(0, &[1, 2])]);
        let wa = worker_assignment(&[(0, &[2])]);
        assert!(coverage_listed(&pa, Some(&wa), |_| true).is_ok());
    }

    #[test]
    fn published_coverage_passes_when_routed_uncovered_but_chunk_covered_elsewhere() {
        // Neither routed worker (1, 2) holds the chunk, but the worker assignment covers it on a
        // non-routed active holder (3). Bounded routing-lag: the chunk is answerable via worker 3, so
        // the oracle tolerates the stale routing rather than demanding routing consistency.
        let pa = portal(&[(0, &[1, 2])]);
        let wa = worker_assignment(&[(0, &[3])]);
        assert!(coverage_listed(&pa, Some(&wa), |_| true).is_ok());
    }

    #[test]
    fn published_coverage_skips_departed_workers() {
        let pa = portal(&[(0, &[1])]);
        let wa = worker_assignment(&[(0, &[2])]);
        assert!(coverage_listed(&pa, Some(&wa), |_| false).is_ok());
    }

    #[test]
    fn published_coverage_with_no_worker_assignment_requires_empty_routing() {
        assert!(coverage_listed(&portal(&[]), None, |_| true).is_ok());
        assert!(coverage_listed(&portal(&[(0, &[1])]), None, |_| true).is_err());
    }

    #[test]
    fn published_coverage_fires_on_paper_only_coverage_when_physical_required() {
        // The assignment lists an active holder (2), but nobody physically holds the chunk — a
        // committed-but-never-fetched replacement. Fatal at a full quorum, tolerated below it.
        let pa = portal(&[(0, &[1])]);
        let wa = worker_assignment(&[(0, &[2])]);
        let nobody_holds = |_, _: &ChunkPk| false;
        assert!(published_coverage(&pa, Some(&wa), |_| true, nobody_holds, true).is_err());
        assert!(published_coverage(&pa, Some(&wa), |_| true, nobody_holds, false).is_ok());
    }

    #[test]
    fn published_coverage_physical_requirement_met_by_any_listed_holder() {
        // Worker 2 physically holds the chunk; worker 3 is a fresh listing still downloading. One
        // physical holder anywhere satisfies the requirement.
        let pa = portal(&[(0, &[1])]);
        let wa = worker_assignment(&[(0, &[2, 3])]);
        let holds = |w, _: &ChunkPk| w == WorkerPk(2);
        assert!(published_coverage(&pa, Some(&wa), |_| true, holds, true).is_ok());
    }

    // ---- routing_matches_assignment_at_fixed_point -----------------------------------------

    #[test]
    fn fixed_point_routing_fires_on_stale_pair() {
        // Worker 1 is routed for chunk 0 but no longer listed as its holder — at quiescence that is
        // a stuck routing update, even though the chunk is covered elsewhere (worker 2).
        let pa = portal(&[(0, &[1])]);
        let wa = worker_assignment(&[(0, &[2])]);
        assert!(routing_matches_assignment_at_fixed_point(&pa, Some(&wa), |_| true).is_err());
    }

    #[test]
    fn fixed_point_routing_passes_when_pairs_match_and_skips_departed() {
        let pa = portal(&[(0, &[1, 9])]);
        let wa = worker_assignment(&[(0, &[1, 2])]);
        // Worker 9 is departed — exempt; worker 1 is routed and listed.
        assert!(
            routing_matches_assignment_at_fixed_point(&pa, Some(&wa), |w| w != WorkerPk(9)).is_ok()
        );
    }
}
