//! Observation layer: a [`Portal`] and a [`WorkerFleet`] fetching the storage's *published*
//! assignments on independent, lagging cadences. Pure state — the SUT drives the fetches and
//! reads these back for the consistency oracle. Keyed by backend pks (`WorkerPk`/`ChunkPk`) so
//! observations compare directly with published assignments.

use std::collections::{BTreeMap, BTreeSet};

use crate::scheduler_storage::{
    AssignmentId, ChunkPk, PortalAssignment, Tick, WorkerAssignment, WorkerPk,
};

/// One worker's observed local state.
#[derive(Debug, Default)]
pub(super) struct ObservedWorkerState {
    /// Chunks physically held — this worker's slice of its last fetched `WorkerAssignment`
    /// (a fetch replaces holdings: downloads what's new, deletes what's gone).
    pub(super) chunks: BTreeSet<ChunkPk>,
    /// Highest worker-assignment id applied (0 = never fetched).
    pub(super) last_applied: AssignmentId,
    /// Clock at the last successful fetch. Read only via the derived `Debug` in failure dumps.
    pub(super) fetched_at: Tick,
}

/// The independent workers, keyed by backend worker pk. Membership tracks the active roster.
#[derive(Debug, Default)]
pub(super) struct WorkerFleet {
    workers: BTreeMap<WorkerPk, ObservedWorkerState>,
}

impl WorkerFleet {
    /// Reconcile membership with the active roster. Staying workers keep their state; a worker
    /// that departs and rejoins starts fresh.
    pub(super) fn sync_active_workers(&mut self, active: &[WorkerPk]) {
        let active_set: BTreeSet<WorkerPk> = active.iter().copied().collect();
        self.workers.retain(|id, _| active_set.contains(id));
        for &id in active {
            self.workers.entry(id).or_default();
        }
    }

    /// Record a successful fetch: replace `worker`'s holdings with its slice of `assignment`.
    /// No-op if the worker is not active.
    pub(super) fn observe_assignment(
        &mut self,
        worker: WorkerPk,
        assignment: &WorkerAssignment,
        now: Tick,
    ) {
        let Some(worker_state) = self.workers.get_mut(&worker) else {
            return;
        };
        worker_state.chunks = assignment
            .chunk_workers
            .iter()
            .filter(|(_, holders)| holders.contains(&worker))
            .map(|(chunk, _)| *chunk)
            .collect();
        worker_state.last_applied = assignment.id;
        worker_state.fetched_at = now;
    }

    /// Bring every active worker up to date with `assignment`; the drain and convergence loops
    /// rely on this to keep the confirmation watermark advancing.
    pub(super) fn catch_up_all(
        &mut self,
        active: &[WorkerPk],
        assignment: &WorkerAssignment,
        now: Tick,
    ) {
        for &worker in active {
            self.observe_assignment(worker, assignment, now);
        }
    }

    pub(super) fn holds_chunk(&self, worker: WorkerPk, chunk: &ChunkPk) -> bool {
        self.workers
            .get(&worker)
            .is_some_and(|observed| observed.chunks.contains(chunk))
    }

    /// Highest assignment `worker` has applied; 0 if absent or never fetched.
    pub(super) fn last_applied(&self, worker: WorkerPk) -> AssignmentId {
        self.workers
            .get(&worker)
            .map_or(0, |observed| observed.last_applied)
    }

    /// Confirmation watermark: the highest assignment id that at least [`quorum_size`] active
    /// workers have applied. 0 with no active workers.
    pub(super) fn confirmation_watermark(
        &self,
        threshold_pct: u32,
        active: &[WorkerPk],
    ) -> AssignmentId {
        if active.is_empty() {
            return 0;
        }
        let mut applied_ids: Vec<AssignmentId> =
            active.iter().map(|&w| self.last_applied(w)).collect();
        applied_ids.sort_unstable_by(|a, b| b.cmp(a));
        applied_ids[quorum_size(threshold_pct, active.len()) - 1]
    }
}

/// Workers needed to confirm an assignment: `ceil(threshold_pct% × worker_count)`, at least 1
/// (0 only when `worker_count` is 0). The single definition shared by the watermark and the
/// SUT's laggard designation.
pub(super) fn quorum_size(threshold_pct: u32, worker_count: usize) -> usize {
    let workers = worker_count as u64;
    if workers == 0 {
        return 0;
    }
    ((u64::from(threshold_pct) * workers).div_ceil(100)).clamp(1, workers) as usize
}

/// The single portal: its last fetched assignment, fetch time, and the confirmation watermark at
/// that assignment's publication — the consistency oracle's accountability bound (a worker at or
/// past it must hold everything this snapshot routes to it).
#[derive(Debug, Default)]
pub(super) struct Portal {
    pub(super) snapshot: Option<PortalAssignment>,
    pub(super) fetched_at: Tick,
    pub(super) watermark: AssignmentId,
}

impl Portal {
    /// Record a successful fetch. `watermark` is the confirmation watermark at the assignment's
    /// publication.
    pub(super) fn observer_assignment(
        &mut self,
        assignment: &PortalAssignment,
        watermark: AssignmentId,
        now: Tick,
    ) {
        self.snapshot = Some(assignment.clone());
        self.fetched_at = now;
        self.watermark = watermark;
    }
}

impl Portal {
    /// Ticks since the last fetch. Before the first fetch this returns `now`, so the oracle
    /// treats a never-fetched portal as beyond the M window and skips it.
    pub(super) fn snapshot_age(&self, now: Tick) -> Tick {
        now.saturating_sub(self.fetched_at)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn w(id: i32) -> WorkerPk {
        WorkerPk(id)
    }

    fn c(id: i64) -> ChunkPk {
        ChunkPk(id)
    }

    fn worker_assignment(
        id: AssignmentId,
        routing: &[(ChunkPk, Vec<WorkerPk>)],
    ) -> WorkerAssignment {
        WorkerAssignment {
            id,
            chunk_workers: routing.iter().cloned().collect(),
            chunks: BTreeMap::new(),
            workers: BTreeMap::new(),
            replication_by_weight: BTreeMap::new(),
        }
    }

    #[test]
    fn fetch_replaces_holdings_with_the_workers_slice() {
        let mut fleet = WorkerFleet::default();
        fleet.sync_active_workers(&[w(1), w(2)]);
        let assignment = worker_assignment(5, &[(c(1), vec![w(1), w(2)]), (c(2), vec![w(1)])]);
        fleet.observe_assignment(w(1), &assignment, 10);
        assert!(fleet.holds_chunk(w(1), &c(1)));
        assert!(fleet.holds_chunk(w(1), &c(2)));
        assert!(!fleet.holds_chunk(w(1), &c(3)));
        assert_eq!(fleet.last_applied(w(1)), 5);

        // Assignment 6 drops c2 from worker 1: a fetch must DELETE it (replace, not union).
        let later_assignment = worker_assignment(6, &[(c(1), vec![w(1), w(2)])]);
        fleet.observe_assignment(w(1), &later_assignment, 20);
        assert!(fleet.holds_chunk(w(1), &c(1)));
        assert!(
            !fleet.holds_chunk(w(1), &c(2)),
            "replace semantics: dropped chunk is deleted"
        );
    }

    #[test]
    fn watermark_at_100pct_is_the_min_applied() {
        let mut fleet = WorkerFleet::default();
        fleet.sync_active_workers(&[w(1), w(2), w(3)]);
        let assignment = |id| worker_assignment(id, &[]);
        fleet.observe_assignment(w(1), &assignment(7), 0);
        fleet.observe_assignment(w(2), &assignment(5), 0);
        fleet.observe_assignment(w(3), &assignment(9), 0);
        // All three must confirm -> watermark is the laggard's 5.
        assert_eq!(fleet.confirmation_watermark(100, &[w(1), w(2), w(3)]), 5);
    }

    #[test]
    fn watermark_tolerates_a_straggler_below_threshold() {
        let mut fleet = WorkerFleet::default();
        fleet.sync_active_workers(&[w(1), w(2), w(3), w(4)]);
        let assignment = |id| worker_assignment(id, &[]);
        fleet.observe_assignment(w(1), &assignment(8), 0);
        fleet.observe_assignment(w(2), &assignment(8), 0);
        fleet.observe_assignment(w(3), &assignment(8), 0);
        fleet.observe_assignment(w(4), &assignment(0), 0); // straggler, never fetched
        // 75% of 4 = 3 must apply -> 3rd-highest is 8 (straggler ignored).
        assert_eq!(
            fleet.confirmation_watermark(75, &[w(1), w(2), w(3), w(4)]),
            8
        );
        // 100% would be blocked by the straggler.
        assert_eq!(
            fleet.confirmation_watermark(100, &[w(1), w(2), w(3), w(4)]),
            0
        );
    }

    #[test]
    fn sync_active_clears_state_on_rejoin() {
        let mut fleet = WorkerFleet::default();
        fleet.sync_active_workers(&[w(1)]);
        let assignment = worker_assignment(3, &[(c(1), vec![w(1)])]);
        fleet.observe_assignment(w(1), &assignment, 5);
        assert_eq!(fleet.last_applied(w(1)), 3);
        // Depart then rejoin — must start fresh.
        fleet.sync_active_workers(&[]);
        fleet.sync_active_workers(&[w(1)]);
        assert_eq!(
            fleet.last_applied(w(1)),
            0,
            "rejoined worker must start from zero"
        );
        assert!(
            !fleet.holds_chunk(w(1), &c(1)),
            "rejoined worker must have empty holdings"
        );
    }

    #[test]
    fn portal_fetch_records_snapshot_and_age() {
        let mut portal = Portal::default();
        assert!(portal.snapshot.is_none());
        let pa = PortalAssignment {
            id: 3,
            chunk_workers: BTreeMap::new(),
            chunks: BTreeMap::new(),
            workers: BTreeMap::new(),
        };
        portal.observer_assignment(&pa, 2, 12);
        assert_eq!(portal.fetched_at, 12);
        assert_eq!(portal.watermark, 2);
        assert_eq!(portal.snapshot_age(20), 8);
    }
}
