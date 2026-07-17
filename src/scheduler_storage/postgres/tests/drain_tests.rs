//! Tests for the drain: what happens to a copy the scheduler drops from the ideal, from the moment it
//! starts draining to the moment a cycle's GC expires it. Each test gets a fresh database so cases run
//! concurrently.

use std::collections::{BTreeSet, HashSet};

use semver::Version;

use super::{confirm, fresh_storage, insert_and_register_chunk};
use crate::multistep_scheduler::SchedulingConfig;
use crate::scheduler_storage::algorithm::MultistepAlgorithm;
use crate::scheduler_storage::postgres::PostgresStorage;
use crate::scheduler_storage::test_harness::utils::worker;
use crate::scheduler_storage::{ChunkPk, SchedulerStorage, WorkerAssignment, WorkerPk};
use crate::types::{ChunkWeight, Worker};
use crate::weight::{SchedulingChunk, WeightStrategy};

/// Ticks a superseded copy keeps draining before a cycle's GC may expire it.
const DRAIN_GRACE: u64 = 60;

struct UniformWeight;
impl WeightStrategy for UniformWeight {
    fn prepare<T: SchedulingChunk>(
        &self,
        chunks: Vec<T>,
    ) -> Vec<(T, ChunkWeight, Option<Version>)> {
        chunks.into_iter().map(|c| (c, 1, None)).collect()
    }
}

/// Drives a shed replica through its drain under the real multistep algorithm: 1 target + 2 filler
/// chunks of size 100 over 5 workers. R = clamp(floor(cap·5/300), 1, 5), so cap 270 → R=4 (the target
/// sits on 4 of the 5 workers) and cap 210 → R=3 (it sheds one). Per-worker load stays under capacity.
struct DrainFixture {
    storage: PostgresStorage,
    target: ChunkPk,
}

impl DrainFixture {
    fn new(name: &str) -> Self {
        let mut storage = fresh_storage(name);
        let target = insert_and_register_chunk(&mut storage, "ds", 1, 100);
        insert_and_register_chunk(&mut storage, "ds", 2, 100);
        insert_and_register_chunk(&mut storage, "ds", 3, 100);
        let workers: Vec<Worker> = (1..=5).map(|s| worker(s, None)).collect();
        storage.update_worker_set(&workers, 0, 1000).unwrap();
        Self { storage, target }
    }

    /// One cycle at `cap`: schedule, confirm, then make the result portal-visible (which starts the
    /// drain clock on anything this cycle superseded).
    fn cycle(&mut self, cap: u64, at: u64) -> WorkerAssignment {
        let config = SchedulingConfig {
            worker_capacity: cap,
            saturation: 1.0,
            min_replication: 1,
            ignore_reliability: true,
        };
        self.storage.register_new_chunks().unwrap();
        let (wa, _) = self
            .storage
            .run_scheduling_cycle(
                &MultistepAlgorithm::new(UniformWeight),
                &config,
                at,
                DRAIN_GRACE,
            )
            .expect("scheduling succeeds");
        confirm(&mut self.storage, wa.id, at + 10);
        self.storage.run_visibility_cycle(at + 50).unwrap();
        wa
    }

    /// Workers holding the target on disk in `wa` — the published `chunk_workers` (ideal ∪ stale).
    fn physical(&self, wa: &WorkerAssignment) -> BTreeSet<WorkerPk> {
        wa.chunk_workers
            .get(&self.target)
            .into_iter()
            .flatten()
            .copied()
            .collect()
    }

    /// Workers ideally holding the target: the physical holders minus the ones still draining.
    fn ideal(&self, wa: &WorkerAssignment) -> BTreeSet<WorkerPk> {
        let stale: HashSet<(ChunkPk, WorkerPk)> =
            self.storage.stale_mappings().unwrap().into_iter().collect();
        self.physical(wa)
            .into_iter()
            .filter(|w| !stale.contains(&(self.target, *w)))
            .collect()
    }

    /// Places the target on 4 of the 5 workers (t=100), then drops capacity so it sheds one (t=200).
    /// The shed copy starts draining, and its clock starts at t=250 with that cycle's visibility pass.
    fn shed_one_replica(&mut self) -> Shed {
        let at_r4 = self.cycle(270, 100);
        let ideal_r4 = self.ideal(&at_r4);
        let at_r3 = self.cycle(210, 200);
        let shed: Vec<WorkerPk> = ideal_r4.difference(&self.ideal(&at_r3)).copied().collect();
        assert_eq!(shed.len(), 1, "one bonus replica shed going R=4→3");
        Shed {
            worker: shed[0],
            ideal_r4,
            at_r3,
        }
    }
}

/// The state left by [`DrainFixture::shed_one_replica`].
struct Shed {
    /// The worker whose copy is now draining.
    worker: WorkerPk,
    /// The target's holders before the shed.
    ideal_r4: BTreeSet<WorkerPk>,
    /// The assignment published by the shedding cycle.
    at_r3: WorkerAssignment,
}

/// A shed copy that the cycle expiring it does *not* re-place leaves the published assignment: the
/// worker is told to drop the chunk, and re-downloads it when a later cycle picks it again.
///
/// The control for [`a_copy_expired_and_re_placed_in_one_cycle_never_leaves_the_published_assignment`]:
/// same timeline, but capacity comes back one cycle *after* the expiry instead of on it.
#[test]
fn shed_bonus_replica_drains_then_refetches_onto_the_same_worker() {
    let mut fx = DrainFixture::new("refetch");
    let shed = fx.shed_one_replica();

    // t=400 is past the grace window, so the GC expires the shed copy — and nothing re-places it, so
    // it leaves the published assignment: a real free.
    let at_drain = fx.cycle(210, 400);
    assert!(
        !fx.physical(&at_drain).contains(&shed.worker),
        "the shed worker was told to drop the chunk",
    );

    // Replication restored a cycle later: the ring re-selects the worker, which no longer holds the
    // bytes and must download them again.
    let at_r4_again = fx.cycle(270, 500);
    let ideal_r4_again = fx.ideal(&at_r4_again);
    assert_eq!(
        ideal_r4_again, shed.ideal_r4,
        "the same placement is restored"
    );
    assert!(
        ideal_r4_again.contains(&shed.worker),
        "the drained worker is re-selected — it re-downloads the chunk it dropped",
    );
}

/// One cycle both expires a shed copy and re-places the chunk on that same worker. A worker holds
/// exactly the published `chunk_workers` (ideal ∪ stale), so what decides whether any data moved is
/// whether the copy is listed on *both* sides of that cycle. It is: the worker is never told to drop
/// the chunk, so it cannot download it again.
#[test]
fn a_copy_expired_and_re_placed_in_one_cycle_never_leaves_the_published_assignment() {
    let mut fx = DrainFixture::new("same_cycle_refetch");
    let shed = fx.shed_one_replica();

    // The last assignment before the expiry cycle still lists the shed worker: it is draining, so the
    // bytes are on its disk.
    assert!(
        fx.physical(&shed.at_r3).contains(&shed.worker),
        "the draining copy is still in the published assignment",
    );

    // The GC at t=400 expires that copy — the control test shows it drops out when nothing re-places
    // it — and this time the same cycle restores R=4, so the ring re-selects the worker.
    let at_400 = fx.cycle(270, 400);
    assert!(
        fx.ideal(&at_400).contains(&shed.worker),
        "the expired worker is re-selected by the same cycle that expired it",
    );

    // Decisive: it is in the published assignment on both sides, so it never left the disk.
    assert!(
        fx.physical(&at_400).contains(&shed.worker),
        "an expired-then-re-placed copy never leaves the published assignment: no re-download",
    );
}
