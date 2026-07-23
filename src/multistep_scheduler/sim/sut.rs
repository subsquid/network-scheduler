//! The simulation: the transition generator (model), the system under test (a storage backend
//! driven through the multistep scheduler), and the safety/liveness assertions. `sim.rs` wires
//! these into the runner.
//!
//! The SUT holds no chunk or placement state of its own: the storage's [`Snapshot`] is the sole
//! source of truth. The [`MultistepAlgorithm`](crate::scheduler_storage::algorithm::MultistepAlgorithm) adapter sees
//! `current = ideal ∪ stale`, so a held or draining copy counts as occupied footprint. Storage
//! always accepts an insert (like a real client): a genuine shortage is recorded on the SUT
//! (`is_infeasible`) rather than rejected, so the model can drive the fleet into
//! over-subscription — when feasible every chunk reaches `min_replication`; when not, no room is
//! wasted.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use super::super::SchedulingConfig;
use super::utils::{
    CHUNK_SIZE, CLOCK_STEP, DATASETS, DEPARTURE_DETECTION_TICKS, M_TICKS, SCHEMA_POOL, WEIGHTS,
    WORKER_CAPACITY, WeightTable, chunk_from_view, mint_key, record_weights, sim_datasets,
    storage_chunk, trace_enabled,
};
use crate::scheduler_storage::algorithm::{CurrentPlacement, SchedulingAlgorithm};
use crate::scheduler_storage::in_memory::InMemoryStorage;
use crate::scheduler_storage::test_harness::inspect::{Snapshot as RawSnapshot, StorageInspect};
use crate::scheduler_storage::{
    AlgoChunk, AssignmentId, ChunkPk, PortalAssignment, SchedulerStorage, SchemaBundle,
    StorageError, Tick, WorkerAssignment, WorkerPk,
};
use crate::types::{DatasetSchema, Worker, WorkerStatus};
use crate::weight::WeightStrategy;
use proptest::statistics;
use proptest_state_machine::iterative_runner::IterativeSutStateMachine;

/// The storage surface the simulation drives: lifecycle mutations ([`SchedulerStorage`]) plus
/// read-only inspection ([`StorageInspect`]) and per-case construction ([`fresh`](SimStorage::fresh)).
pub(super) trait SimStorage: SchedulerStorage + StorageInspect {
    /// Build a fresh, empty storage instance for one model-test case.
    fn fresh() -> Self;
}

impl SimStorage for InMemoryStorage {
    fn fresh() -> Self {
        Self::default()
    }
}

/// Unique database id per Postgres case (and per shrink replay) — `CREATE DATABASE` of a
/// duplicate name panics.
static G_PG_MODEL_CASE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

impl SimStorage for crate::scheduler_storage::postgres::PostgresStorage {
    fn fresh() -> Self {
        let id = G_PG_MODEL_CASE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        crate::scheduler_storage::test_harness::pg_harness::fresh_db("pg_model", id)
    }
}

mod churn;
mod flood;
mod guided;
mod observers;
mod placement_oracles;
mod preconditions;
mod trace;
mod zero_quorum;
// The multistep `SchedulingAlgorithm` adapter is production code (beside `DefaultSchedulingAlgorithm`),
// so the sim and any real-algorithm test drive the same scheduler through it.
use crate::scheduler_storage::algorithm::MultistepAlgorithm;
// Re-exported so the test module names the models through `sut`.
pub(super) use churn::ChurnModel;
pub(super) use flood::FloodModel;
pub(super) use guided::SimModel;
use observers::{Portal, WorkerFleet};
use preconditions::standard_preconditions;
use trace::{action_label, format_chunk_list};
pub(super) use zero_quorum::ZeroQuorumModel;

/// A chunk's opaque storage id — read back from storage and compared, never parsed. Only
/// [`mint_key`] constructs a fresh one.
///
/// [`mint_key`]: super::utils::mint_key
pub(super) type ChunkKey = String;

/// Spare workers pre-created beyond the initial fleet, available for an `AddWorker` to switch on.
const WORKER_POOL_HEADROOM: u16 = 4;

#[derive(Clone, Copy, Debug)]
enum ScheduleStatus {
    SchedulerPlaced,
    NotEnoughCapacity,
    NoSchedulerRun,
}

// ===========================================================================
// Reference model — per-case config and shared per-step rules; the models live in `guided`/`flood`.
// ===========================================================================

/// Per-case fleet/replication config, randomised so we explore many fleets rather than one.
#[derive(Clone, Debug)]
pub(super) struct SimConfig {
    pub(super) worker_count: u16,
    pub(super) worker_capacity: u64,
    pub(super) min_replication: u16,
    pub(super) saturation: f64,
    /// Whether a passing `CheckConverged` ends the run (scripted flood); the random walks leave
    /// it `false`.
    pub(super) converge_is_terminal: bool,
    /// Optional `SIM_CHUNKS` debug cap on chunk count (`None` ⇒ no cap).
    pub(super) chunk_cap: Option<usize>,
    /// Datasets this run uses: pre-registered in the storage and the pool the generators draw
    /// from. Owned per run rather than assuming the [`DATASETS`] constant.
    pub(super) datasets: Vec<String>,
    /// X% confirmation quorum: the watermark advances to the highest assignment ≥ this percent of
    /// active workers have applied; 100 = all workers confirm; 0 = watermark skips to latest
    /// published assignment (no fetch required).
    pub(super) confirm_threshold_pct: u32,
    /// Ticks a departed worker's row survives before a membership sync deletes it.
    pub(super) gc_ticks: Tick,
}

impl SimConfig {
    /// Fixed config for the standalone deterministic test.
    fn fixed() -> Self {
        SimConfig {
            worker_count: 6,
            worker_capacity: WORKER_CAPACITY,
            min_replication: 2,
            saturation: 0.9,
            converge_is_terminal: false,
            chunk_cap: None,
            datasets: sim_datasets(),
            confirm_threshold_pct: 100,
            gc_ticks: 0,
        }
    }
}

/// A brand-new chunk for an `Add`. Self-contained so captured regressions replay without a seed.
#[derive(Clone, Debug)]
pub(super) struct NewChunk {
    pub(super) key: ChunkKey,
    /// Size in bytes.
    pub(super) size: u32,
    /// Heavier ⇒ higher replication cap; recorded in the shared `WeightTable`.
    pub(super) weight: u16,
    pub(super) dataset: String,
    pub(super) blocks: std::ops::RangeInclusive<u64>,
}

/// A chunk eligible as a correction's *old* side, carrying what a replacement needs from it: its key
/// (to link the correction) and its block range (which the replacement inherits, same-range).
#[derive(Clone, Debug)]
pub(super) struct CorrectableOld {
    pub(super) chunk_id: ChunkKey,
    pub(super) dataset: String,
    pub(super) blocks: std::ops::RangeInclusive<u64>,
}

/// Which converged-state property a [`Action::CheckConverged`] asserts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ConvergenceCheck {
    /// Weak: a below-floor chunk is tolerated unless a worker has free room *right now*.
    FloorLocallyFeasible,
    /// Strong: a below-floor chunk is tolerated only if room couldn't be freed by dropping another
    /// chunk's surplus copies.
    FloorsPreemptBonuses,
}

#[derive(Clone, Debug)]
pub(super) enum Action {
    AddChunks(Vec<NewChunk>),
    /// Advance the clock by this many ticks and run a cycle. `M_TICKS` flushes everything
    /// draining; smaller values land mid-window. The delta lives in the variant so captured
    /// regressions replay without a seed.
    AdvanceClock(Tick),
    /// Probe converged-state liveness by draining the live storage to a fixed point.
    CheckConverged(ConvergenceCheck),
    /// Switch the pool worker at this index into the active fleet. No-op if already active.
    WorkerJoined(usize),
    /// Switch the pool worker at this index out of the active fleet. No-op if absent.
    WorkerLeft(usize),
    /// A worker polls the published worker assignment; `succeeds = false` models a missed poll
    /// (the worker stays behind).
    WorkerFetchAssignment {
        worker: usize,
        succeeds: bool,
    },
    /// The portal polls the published portal assignment. `succeeds = false` = a missed poll.
    PortalFetchAssignment {
        succeeds: bool,
    },
    /// Register a 1-to-1 correction: insert `replacement` and link it to `old_key` (the backfill
    /// flow of `docs/mvcc-storage.md`); the swap fires once the replacement confirms.
    /// Self-contained so captured regressions replay without a seed; a storage rejection is a
    /// legal no-op.
    RegisterCorrection {
        old_dataset: String,
        old_chunk_id: ChunkKey,
        replacement: NewChunk,
    },
    /// Retune the replication floor mid-run and reschedule. Raising adds demand pressure;
    /// lowering is the recovery lever the random walks pull after a confirmed shortage. The floor
    /// lives in the variant so captured regressions replay without a seed.
    SetMinReplication(u16),
    /// Set `dataset`'s current read schema, drawn from a small canned pool
    /// ([`SCHEMA_POOL`](super::utils::SCHEMA_POOL)). Only affects chunks inserted afterwards —
    /// exercises the schema-bundle consistency invariant as schemas change mid-run.
    SetDatasetSchema {
        dataset: String,
        schema: DatasetSchema,
    },
    /// Do nothing — the idle tail after the run completes.
    NoOp,
}

/// Low-cardinality kind label for per-transition statistics (the trace's `action_label` carries
/// per-instance detail and would explode the table).
fn action_kind(action: &Action) -> &'static str {
    match action {
        Action::AddChunks(_) => "action: add-chunks",
        Action::AdvanceClock(_) => "action: advance-clock",
        Action::CheckConverged(_) => "action: check-converged",
        Action::WorkerJoined(_) => "action: worker-joined",
        Action::WorkerLeft(_) => "action: worker-left",
        Action::WorkerFetchAssignment { .. } => "action: worker-fetch",
        Action::PortalFetchAssignment { .. } => "action: portal-fetch",
        Action::RegisterCorrection { .. } => "action: register-correction",
        Action::SetMinReplication(_) => "action: set-min-replication",
        Action::SetDatasetSchema { .. } => "action: set-dataset-schema",
        Action::NoOp => "action: no-op (idle tail)",
    }
}

/// Outcome of adding a burst of chunks.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum AddOutcome {
    /// Every requested chunk reached the floor this cycle.
    AllPlaced,
    /// Feasible, but only `placed` chunks reached the floor and `deferred` didn't (retry later).
    Deferred { placed: usize, deferred: Vec<usize> },
    /// Shortage: the burst is inserted (storage always accepts) but the floor can't be placed;
    /// recorded on the SUT.
    Infeasible,
}

impl AddOutcome {
    fn label(&self, burst: usize) -> String {
        match self {
            AddOutcome::AllPlaced => format!("Add({burst}): placed {burst}/{burst}"),
            AddOutcome::Deferred { placed, deferred } => format!(
                "Add({burst}): placed {placed}/{burst} — {} deferred {} (retry as stale drains)",
                deferred.len(),
                format_chunk_list(deferred),
            ),
            AddOutcome::Infeasible => format!(
                "Add({burst}): INFEASIBLE — inserted, but the floor can't be met at \
                 min_replication; shortage recorded (need capacity or fewer chunks)"
            ),
        }
    }
}

// ===========================================================================
// System under test — the storage lifecycle + assertions.
// ===========================================================================

impl<D: SimStorage> IterativeSutStateMachine for SimUnderTest<D> {
    type SutState = SimUnderTest<D>;
    type ModelState = SimConfig;
    type Transition = Action;

    fn init_test(config: &SimConfig) -> Self::SutState {
        let sim = SimUnderTest::from_config_with(config, D::fresh());
        sim.trace_config();
        sim
    }

    fn apply(sim: SimUnderTest<D>, config: &SimConfig, transition: Action) -> SimUnderTest<D> {
        sim.apply_transition(config, transition)
    }

    fn check_invariants(sim: &SimUnderTest<D>, _config: &SimConfig) {
        // Sound to skip: the runner checks after every `apply`, so "unchanged this transition"
        // is "unchanged since the last check".
        if !sim.oracle_inputs_changed {
            return;
        }
        sim.record_state_statistics();
        sim.assert_no_orphaned_stale_mappings();
        sim.assert_published_coverage();
        sim.assert_portal_consistency();
        sim.assert_corrections_safety();
    }

    fn teardown(sim: SimUnderTest<D>, _config: SimConfig) {
        // Recorded into the last transition's statistics case for the sweep summary.
        statistics::measure("case: real transitions (non-noop)", sim.real_steps as f64);
        statistics::measure("case: saturation episodes", sim.saturation_episodes as f64);
    }
}

#[derive(Clone, Debug)]
pub(super) struct PooledWorker {
    worker: Worker,
    // Inactive workers must not appear in the worker/portal assignments.
    active: bool,
}

/// The simulated world: a storage backend driven through the multistep scheduler, a logical clock,
/// and the worker roster. Chunk and placement state live only in the storage.
pub(super) struct SimUnderTest<D: SimStorage> {
    storage: D,
    algo: MultistepAlgorithm<WeightTable>,
    /// Shared `chunk-id → weight` table — the scheduler reads it back as its `WeightStrategy`.
    weights: WeightTable,
    /// Fixed worker pool — never resized; churn flips each worker's `active`.
    workers: Vec<PooledWorker>,
    config: SchedulingConfig,
    gc_ticks: Tick,
    /// Logical clock.
    now: Tick,
    workers_state: WorkerFleet,
    portal_state: Portal,
    /// Latest published worker assignment (what a `WorkerFetch` samples); `None` until the first
    /// scheduling cycle.
    latest_worker_assignment: Option<WorkerAssignment>,
    /// Bundle frozen with `latest_worker_assignment`; both stay frozen together across a shortage
    /// streak, so the oracle checks the assignment against the bundle it shipped with.
    latest_worker_bundle: Option<SchemaBundle>,
    /// Latest published portal assignment (what a `PortalFetch` samples).
    latest_portal_assignment: Option<PortalAssignment>,
    /// Confirmation watermark at the latest portal assignment's publication — the consistency
    /// oracle's accountability bound.
    latest_portal_watermark: AssignmentId,
    /// Per departed worker, the newest assignment that still placed it — until the routing
    /// watermark passes it, the oracles hold the worker to nothing (see
    /// [`routing_has_caught_up_with_departure`](Self::routing_has_caught_up_with_departure)).
    placed_until_departure: BTreeMap<WorkerPk, AssignmentId>,
    /// X% confirmation quorum.
    confirm_threshold_pct: u32,
    /// Converged latch: set by a terminal convergence check or one under a recorded shortage;
    /// re-armed by the next successful scheduling cycle. The random walks read it as "recover
    /// now".
    converge_checked: bool,
    /// Non-`NoOp` transitions executed — the case's effective length, measured at teardown.
    real_steps: usize,
    /// Converged probes that confirmed a genuine shortage, measured at teardown.
    saturation_episodes: usize,
    /// Whether the most recent cycle hit a scheduler shortage — recorded, not fatal.
    is_infeasible: bool,
    /// Whether the just-applied transition may have touched an oracle input (`false` only for
    /// `NoOp`) — lets the idle tail skip `check_invariants`.
    oracle_inputs_changed: bool,
    /// Routed chunks that had ≥ 1 physical copy on an active worker at the last transition — the
    /// baseline [`assert_physical_retention`](Self::assert_physical_retention) edges against.
    physically_covered: BTreeSet<ChunkPk>,
    /// Whether the just-applied transition removed a worker (`WorkerLeft`) — the one legitimate
    /// cause for a routed chunk's physical coverage to vanish in a single step.
    last_transition_shed_a_worker: bool,
    /// Per-operation trace toggle, captured from `SIM_TRACE` once at construction.
    trace: bool,
    /// One-shot latch: set once the first idle `NoOp` has been traced.
    noop_traced: bool,
    /// Monotonic step counter for trace ordinals.
    step: usize,
    /// Most recent scheduling outcome, for the trace line; reset per transition.
    schedule_status: ScheduleStatus,
}

impl<D: SimStorage> SimUnderTest<D> {
    // ---- Actions ----------------------------------------------------------------------------

    /// Add `new` chunks and run a cycle. Storage always accepts the insert
    pub(super) fn do_add_chunks(&mut self, new: Vec<NewChunk>) -> AddOutcome {
        let new_pks = self.insert_and_register(&new);
        let burst = new_pks.len();

        self.run_cycle();

        let outcome = if self.is_infeasible {
            AddOutcome::Infeasible
        } else {
            self.classify_add(&new_pks)
        };
        statistics::label(match &outcome {
            AddOutcome::AllPlaced => "add: all placed",
            AddOutcome::Deferred { .. } => "add: deferred (transient)",
            AddOutcome::Infeasible => "add: infeasible (shortage)",
        });
        statistics::measure("add: burst size", burst as f64);
        if let AddOutcome::Deferred { deferred, .. } = &outcome {
            statistics::measure("add: deferred chunks", deferred.len() as f64);
        }
        self.trace_step(&outcome.label(burst));
        outcome
    }

    /// Link `replacement` to the chunk keyed `old_key` and run a cycle. `register_correction`
    /// inserts the replacement and the correction row atomically; the replacement's lifecycle row
    /// is then created by `register_new_chunks`, like any add. Any chunk may be picked as the old
    /// side, so an unsuitable one — rejected, being removed, already corrected, or gone in a shrunk
    /// prefix — comes back as `CorrectionRejected` and is a legal no-op (the replacement is never
    /// inserted). Any *other* storage error is a real bug and panics.
    pub(super) fn do_register_correction(
        &mut self,
        old_dataset: &str,
        old_key: &str,
        replacement: NewChunk,
    ) {
        let new_key = replacement.key.clone();
        // The replacement must record its weight before it can be scheduled.
        record_weights(&self.weights, std::slice::from_ref(&replacement));
        let outcome = match self.pk_of(old_dataset, old_key) {
            Some(old_pk) => {
                match SchedulerStorage::register_correction(
                    &mut self.storage,
                    old_pk,
                    storage_chunk(&replacement),
                    self.now,
                ) {
                    Ok(_new_pk) => {
                        // The scheduler discovers the replacement and creates its lifecycle row,
                        // exactly as for an ordinary add.
                        self.storage
                            .register_new_chunks()
                            .expect("register replacement");
                        statistics::label("correction: registered");
                        "registered".to_string()
                    }
                    Err(StorageError::CorrectionRejected { reason }) => {
                        statistics::label("correction: rejected (legal no-op)");
                        format!("rejected ({reason})")
                    }
                    // Contract: register_correction refuses an invalid correction only via
                    // CorrectionRejected. Any other error means the backend broke that contract (or
                    // a genuine failure) — an invalidated invariant, so fail loud rather than no-op.
                    Err(err) => {
                        panic!("register_correction returned a non-rejection error: {err}")
                    }
                }
            }
            None => {
                statistics::label("correction: skipped (old chunk missing)");
                "skipped (old chunk not found)".to_string()
            }
        };

        self.run_cycle();

        self.trace_step(&format!(
            "RegisterCorrection({old_key} → {new_key}): {outcome}"
        ));
    }

    fn do_advance_clock(&mut self, ticks: Tick) {
        let draining_before = self.draining_pairs();
        let had_pending = !self
            .storage
            .get_stale_mappings(|mapping| mapping.dropped_at_portal_assignment_id.is_none())
            .is_empty();
        self.now += ticks;
        // Drains expire during the scheduling pass at this clock, before `run_cycle`'s own
        // `CLOCK_STEP` advance.
        let expiry_at = self.now;

        self.run_cycle();
        self.record_drain_outcomes(&draining_before, expiry_at, had_pending);

        self.trace_step(&format!("AdvanceClock(+{ticks})"));
    }

    /// A worker polls the latest worker assignment: success applies its slice and advances
    /// confirmation. A fetch is an observation, so the clock does not advance.
    fn do_worker_fetch(&mut self, index: usize, succeeds: bool) {
        if succeeds && let Some(worker) = self.worker_pk_of_index(index) {
            if let Some(assignment) = self.latest_worker_assignment.clone() {
                self.workers_state
                    .observe_assignment(worker, &assignment, self.now);
            }
            let active = self.active_worker_pks();
            self.refresh_confirmation(&active);
        }
        self.trace_step(&format!(
            "WorkerFetch(W{index}, {})",
            if succeeds { "ok" } else { "missed" }
        ));
    }

    /// The portal polls the latest portal assignment: success refreshes its snapshot to age zero.
    fn do_portal_fetch(&mut self, succeeds: bool) {
        if succeeds && let Some(assignment) = self.latest_portal_assignment.clone() {
            self.portal_state.observer_assignment(
                &assignment,
                self.latest_portal_watermark,
                self.now,
            );
        }
        self.trace_step(&format!(
            "PortalFetch({})",
            if succeeds { "ok" } else { "missed" }
        ));
    }

    /// Retune the replication floor and reschedule. Lowering frees capacity, so a recorded
    /// shortage may clear.
    pub(super) fn do_set_min_replication(&mut self, min_replication: u16) {
        let old = self.config.min_replication;
        let recovering = self.converge_checked;
        self.config.min_replication = min_replication;
        self.run_cycle();
        statistics::label(match (min_replication.cmp(&old), recovering) {
            (std::cmp::Ordering::Less, true) => "floor: lowered to recover (saturated)",
            (std::cmp::Ordering::Less, false) => "floor: lowered",
            (std::cmp::Ordering::Greater, _) => "floor: raised (pressure)",
            (std::cmp::Ordering::Equal, _) => "floor: unchanged",
        });
        self.trace_step(&format!("SetMinReplication({min_replication})"));
    }

    /// Retune `dataset`'s current read schema and reschedule. Only chunks inserted afterwards are
    /// stamped with it — existing chunks keep the schema they were inserted under (see
    /// [`SchedulerStorage::set_dataset_schema`]).
    pub(super) fn do_set_dataset_schema(&mut self, dataset: &str, schema: DatasetSchema) {
        self.storage
            .set_dataset_schema(dataset, schema)
            .expect("dataset is pre-registered by the sim config");
        self.run_cycle();
        statistics::label("schema: dataset schema changed");
        self.trace_step(&format!("SetDatasetSchema({dataset})"));
    }

    /// Standard addition flow (record weights → insert → register), shared by adds and test
    /// seeding. Returns the pks of the chunks registered (already-present ones skipped).
    /// Corrections insert their replacement through `register_correction` instead, atomically
    /// with the correction row.
    fn insert_and_register(&mut self, new_chunks: &[NewChunk]) -> Vec<ChunkPk> {
        record_weights(&self.weights, new_chunks);
        // Throw every generated chunk at the storage and let it reject what it must: a duplicate
        // (dataset, id) — including an intra-batch repeat from the random generator — is refused, and
        // that refusal is a legal no-op (a real ingester may re-send an existing chunk). Insert
        // per-chunk so one duplicate doesn't drop the rest of the batch. The generator only names
        // registered datasets, so any other insert error is a contract break → fail loud.
        for chunk in new_chunks {
            match self.storage.insert_new_chunks(vec![storage_chunk(chunk)]) {
                Ok(()) | Err(StorageError::ChunkAlreadyExists) => {}
                Err(err) => panic!("insert_new_chunks failed for a non-duplicate reason: {err}"),
            }
        }
        self.storage
            .register_new_chunks()
            .expect("chunks are inserted before registration")
    }

    /// The pk of the chunk identified by `(dataset, key)`, if present. A `chunk_id` is unique only
    /// within a dataset (mirrors Postgres `UNIQUE (dataset_id, chunk_id)`).
    fn pk_of(&self, dataset: &str, key: &str) -> Option<ChunkPk> {
        self.storage
            .get_chunks(|view| view.dataset == dataset && view.chunk_id == key)
            .into_iter()
            .next()
            .map(|view| view.chunk_pk)
    }

    /// Whether a from-scratch placement of every current chunk's floor exists on `workers`:
    /// probing with an empty placement exercises both shortage sources (replication budget and
    /// stage-1 packing). Removing chunks are excluded — they are no longer demand.
    fn is_ideal_feasible(&self, workers: &[&Worker]) -> bool {
        let worker_pairs: Vec<(WorkerPk, Worker)> = workers
            .iter()
            .copied()
            .enumerate()
            .map(|(i, worker)| (WorkerPk(i as i32 + 1), worker.clone()))
            .collect();
        let excluded = self.chunks_excluded_from_floor();
        let mut chunk_pairs: Vec<(ChunkPk, AlgoChunk)> = self
            .storage
            .get_chunks(|_| true)
            .iter()
            .filter(|view| !excluded.contains(&view.chunk_pk))
            .map(|view| (view.chunk_pk, chunk_from_view(view)))
            .collect();
        chunk_pairs.sort_by(|a, b| a.0.cmp(&b.0));
        self.algo
            .schedule(
                chunk_pairs,
                worker_pairs,
                &CurrentPlacement::default(),
                &CurrentPlacement::default(),
                &CurrentPlacement::default(),
                &self.config,
            )
            .is_ok()
    }

    /// Chunks the placement/convergence oracles must not expect to reach their replication floor:
    /// those being removed plus those rejected at registration. The scheduler skips both.
    fn chunks_excluded_from_floor(&self) -> BTreeSet<ChunkPk> {
        let mut excluded = self.storage.chunks_in_removal();
        excluded.extend(self.storage.rejected_chunks());
        excluded
    }

    /// Classify the just-added burst (its registered pks) by which reached the floor. Matching on the
    /// pk — not the `chunk_id`, which is unique only within a dataset — keeps the count exact, so
    /// `placed` can't underflow when a `chunk_id` repeats across datasets.
    fn classify_add(&self, new_pks: &[ChunkPk]) -> AddOutcome {
        let burst = new_pks.len();
        let min_replication = usize::from(self.config.min_replication);
        let new_pks: HashSet<ChunkPk> = new_pks.iter().copied().collect();
        let excluded = self.chunks_excluded_from_floor();
        let snapshot = self.storage.snapshot();
        let deferred: Vec<usize> = (0..snapshot.len())
            .filter(|&chunk| {
                new_pks.contains(&snapshot.chunk_pks[chunk])
                    && !excluded.contains(&snapshot.chunk_pks[chunk])
                    && snapshot.ideal[chunk].len() < min_replication
            })
            .collect();
        if deferred.is_empty() {
            AddOutcome::AllPlaced
        } else {
            AddOutcome::Deferred {
                placed: burst - deferred.len(),
                deferred,
            }
        }
    }

    /// Drop-stamped stale pairs with their anchor tick — the stamping portal assignment's
    /// `created_at`, the clock the backend's expiry runs on.
    fn draining_pairs(&self) -> Vec<((ChunkPk, WorkerPk), Tick)> {
        self.storage
            .get_stale_mappings(|mapping| mapping.dropped_at_portal_assignment_id.is_some())
            .into_iter()
            .filter_map(|mapping| {
                let anchor = self
                    .storage
                    .get_portal_assignment_created_at(mapping.dropped_at_portal_assignment_id?)?;
                Some(((mapping.chunk_pk, mapping.worker_id), anchor))
            })
            .collect()
    }

    /// Drain-expiry telemetry for an `AdvanceClock` jump: a pre-jump draining pair is *due* iff
    /// its anchor sits ≥ M ticks before `expiry_at` (recomputed independently here), compared
    /// against whether it actually left the stale set. Reclaimed pairs and removing chunks are
    /// excluded — they vanish for non-expiry reasons. The `late`/`early` labels firing would mean
    /// the backend disagrees with the documented M-window; promote to an oracle if they appear.
    fn record_drain_outcomes(
        &self,
        draining_before: &[((ChunkPk, WorkerPk), Tick)],
        expiry_at: Tick,
        had_pending: bool,
    ) {
        if draining_before.is_empty() && !had_pending {
            return;
        }
        let stale_after: BTreeSet<(ChunkPk, WorkerPk)> = self
            .storage
            .get_stale_mappings(|_| true)
            .into_iter()
            .map(|mapping| (mapping.chunk_pk, mapping.worker_id))
            .collect();
        let ideal_after: BTreeSet<(ChunkPk, WorkerPk)> = self
            .storage
            .get_chunk_workers(|_, _| true)
            .into_iter()
            .flat_map(|(pk, workers)| workers.into_iter().map(move |worker| (pk, worker)))
            .collect();
        let removing = self.storage.chunks_in_removal();
        let (mut due_expired, mut due_survived, mut undue_survived, mut undue_vanished) =
            (0u32, 0u32, 0u32, 0u32);
        for &(pair, anchor) in draining_before {
            if removing.contains(&pair.0) {
                continue; // whole-chunk removal deletes pair rows on its own schedule
            }
            let due = expiry_at
                .checked_sub(M_TICKS)
                .is_some_and(|cutoff| anchor <= cutoff);
            let survived = stale_after.contains(&pair);
            if !survived && ideal_after.contains(&pair) {
                continue; // reclaimed: the scheduler re-placed the copy, not an expiry
            }
            match (due, survived) {
                (true, false) => due_expired += 1,
                (true, true) => due_survived += 1,
                (false, true) => undue_survived += 1,
                (false, false) => undue_vanished += 1,
            }
        }
        statistics::classify(due_expired > 0, "drain: due copies expired");
        statistics::classify(due_survived > 0, "drain: DUE COPY SURVIVED (late expiry?)");
        statistics::classify(
            undue_survived > 0,
            "drain: mid-window copies held (partial drain)",
        );
        statistics::classify(
            undue_vanished > 0,
            "drain: UNDUE COPY VANISHED (early expiry?)",
        );
        statistics::classify(had_pending, "drain: pending stale (M clock not started)");
    }

    // ---- Worker churn (membership) ----------------------------------------------------------
    //
    // Membership events update the registry directly; scheduling never syncs the fleet. Doing it
    // here would collapse the window between a departure and the cycles that converge on it.

    /// Switch the pool worker at `index` into the active fleet and register it. No-op if out of
    /// range or already active.
    pub(super) fn do_worker_joined(&mut self, index: usize) -> bool {
        if self.workers.get(index).map(|worker| worker.active) != Some(false) {
            return false;
        }
        self.workers[index].active = true;
        self.sync_worker_set();
        self.trace_step(&format!(
            "AddWorker(W{index}) — {} workers",
            self.active_workers_count()
        ));
        true
    }

    /// Switch the pool worker at `index` out of the active fleet and mark it departed. No-op if
    /// out of range or already absent.
    pub(super) fn do_worker_left(&mut self, index: usize) -> bool {
        if self.workers.get(index).map(|worker| worker.active) != Some(true) {
            return false;
        }
        self.workers[index].active = false;
        // A departure is only observed after a full window of silence, so that time has passed on
        // the clock too. Without the bump a portal snapshot taken before the departure still reads
        // as fresh, and the oracles hold the worker to data it no longer serves.
        self.now += DEPARTURE_DETECTION_TICKS;
        self.sync_worker_set();
        self.trace_step(&format!(
            "RemoveWorker(W{index}) — {} workers",
            self.active_workers_count()
        ));
        true
    }

    /// Hand the active fleet to the storage registry — departure marking and departed-worker
    /// deletion run inside.
    fn sync_worker_set(&mut self) {
        let mut registered_before: BTreeSet<WorkerPk> = BTreeSet::new();
        let mut inactive_before: BTreeSet<WorkerPk> = BTreeSet::new();
        for view in self.storage.get_workers(|_| true) {
            registered_before.insert(view.worker_id);
            if view.inactive_since.is_some() {
                inactive_before.insert(view.worker_id);
            }
        }
        let active: Vec<Worker> = self.active_workers().into_iter().cloned().collect();
        self.storage
            .update_worker_set(&active, self.now, self.gc_ticks)
            .expect("worker set update succeeds");
        let latest_assignment = self
            .latest_worker_assignment
            .as_ref()
            .map_or(0, |assignment| assignment.id);
        let mut survivors = 0usize;
        let mut any_departed = false;
        for view in self.storage.get_workers(|_| true) {
            if registered_before.contains(&view.worker_id) {
                survivors += 1;
            }
            if view.inactive_since.is_some() {
                any_departed = true;
                // Edge-triggered: only on the active -> inactive transition, so the bound stays the
                // newest assignment that could have placed the worker before it left.
                if !inactive_before.contains(&view.worker_id) {
                    self.placed_until_departure
                        .insert(view.worker_id, latest_assignment);
                }
            } else if inactive_before.contains(&view.worker_id) {
                // Rejoined (inactive -> active, same pk): the worker is placeable again, so its stale
                // departure bound no longer applies — clear it, or coverage would keep treating this
                // active holder as an unscrubbed departed route.
                self.placed_until_departure.remove(&view.worker_id);
            }
        }
        statistics::classify(
            survivors < registered_before.len(),
            "churn: worker deleted this sync",
        );
        statistics::classify(any_departed, "churn: departed worker still registered");
        let active_pks = self.active_worker_pks();
        self.workers_state.sync_active_workers(&active_pks);
    }

    /// Every stale mapping must reference an *active* worker. A departed one serves nothing, so
    /// departure purges its mappings and the mint skips it; a deleted one takes its mappings with
    /// it. Either way a mapping naming a non-active worker can never drain.
    fn assert_no_orphaned_stale_mappings(&self) {
        let mappings = self.storage.get_stale_mappings(|_| true);
        if mappings.is_empty() {
            return; // nothing to orphan — skip the registry fetch
        }
        let active: BTreeSet<WorkerPk> = self
            .storage
            .get_workers(|view| view.inactive_since.is_none())
            .into_iter()
            .map(|view| view.worker_id)
            .collect();
        let orphaned: Vec<_> = mappings
            .into_iter()
            .filter(|mapping| !active.contains(&mapping.worker_id))
            .map(|mapping| (mapping.chunk_pk, mapping.worker_id))
            .collect();
        assert!(
            orphaned.is_empty(),
            "stale mappings held by departed or deleted workers: {orphaned:?}",
        );
    }

    // ---- Convergence ------------------------------------------------------------------------

    /// Converged-state liveness: drain the live storage to a fixed point, then assert the chosen
    /// property. The run legitimately continues from the converged state.
    fn check_converged(&mut self, check: ConvergenceCheck) {
        if self.total_chunk_count() == 0 {
            return;
        }
        self.trace_converge_header();
        self.drain_to_fixed_point();
        statistics::classify(
            self.is_infeasible,
            "converged: infeasible (refusal cross-checked)",
        );
        statistics::classify(
            !self.is_infeasible,
            "converged: feasible (floor oracle applied)",
        );
        if self.is_infeasible {
            // Under a shortage the all-or-nothing scheduler commits nothing, so the floor oracle
            // doesn't apply; only cross-check that the refusal is justified from an empty fleet.
            assert!(
                !self.is_ideal_feasible(&self.active_workers()),
                "scheduler recorded a shortage, yet the set is placeable from an empty fleet — \
                 incremental reconcile is weaker than scheduling from scratch (chunks={}, workers={})",
                self.total_chunk_count(),
                self.active_workers_count(),
            );
        } else {
            // Whole set placed: assert no room was wasted reaching the floor. Removing chunks
            // carry no floor obligation.
            let mut converged = self.storage.snapshot();
            let excluded = self.chunks_excluded_from_floor();
            converged.retain_chunks(|pk| !excluded.contains(pk));
            let capacity = self.config.worker_capacity;
            let min_replication = usize::from(self.config.min_replication);
            let saturation = self.config.saturation;
            let active = converged.worker_pks(&self.active_workers());
            let verdict = match check {
                ConvergenceCheck::FloorLocallyFeasible => {
                    placement_oracles::floor_locally_feasible(
                        &converged,
                        &active,
                        capacity,
                        min_replication,
                        saturation,
                    )
                }
                ConvergenceCheck::FloorsPreemptBonuses => {
                    placement_oracles::floors_preempt_bonuses(
                        &converged,
                        &active,
                        capacity,
                        min_replication,
                        saturation,
                    )
                }
            };
            if let Err(violation) = verdict {
                self.dump_floor_failure(&converged, violation.chunk);
                panic!("{}", violation.message);
            }
        }
        // Refresh the portal at the fixed point. Below a 100% quorum the designated laggards are
        // still behind, so even this fetch carries straggler-scoping obligations.
        if let Some(portal_assignment) = self.latest_portal_assignment.clone() {
            self.portal_state.observer_assignment(
                &portal_assignment,
                self.latest_portal_watermark,
                self.now,
            );
        }
        // Fixed-point routing liveness: the per-cycle strand check tolerates preempted-but-still-
        // routed pairs as bounded routing-lag. At a drained, shortage-free fixed point all lag has
        // resolved, so routing must match the assignment pair-by-pair — a mismatch here is a stuck
        // routing update, not lag.
        if let (false, Some(portal_assignment)) =
            (self.is_infeasible, self.latest_portal_assignment.as_ref())
        {
            let active: BTreeSet<WorkerPk> = self.active_worker_pks().into_iter().collect();
            if let Err(message) = placement_oracles::routing_matches_assignment_at_fixed_point(
                portal_assignment,
                self.latest_worker_assignment.as_ref(),
                |worker| active.contains(&worker),
            ) {
                panic!("{message}");
            }
        }
    }

    /// ADR 0001 regression sentinel: every routed chunk — *including* one routed only to departed
    /// workers, which the strand oracle exempts as an availability event — must keep an active
    /// listed holder in the latest worker assignment. After a committed cycle this is exactly what
    /// same-cycle floor preemption restores, and its absence is invisible to the per-step oracles;
    /// regressions call this so deleting the preemption path fails a test, not just an oracle's
    /// silence. No-op under a recorded shortage (nothing committed).
    pub(super) fn assert_all_routed_chunks_have_a_listed_holder(&self) {
        if self.is_infeasible {
            return;
        }
        let Some(portal_assignment) = self.latest_portal_assignment.as_ref() else {
            return;
        };
        let active: BTreeSet<WorkerPk> = self.active_worker_pks().into_iter().collect();
        let worker_assignment = self.latest_worker_assignment.as_ref();
        for chunk in portal_assignment.chunk_workers.keys() {
            let covered = worker_assignment.is_some_and(|assignment| {
                assignment
                    .chunk_workers
                    .get(chunk)
                    .is_some_and(|holders| holders.iter().any(|w| active.contains(w)))
            });
            assert!(
                covered,
                "portal routes chunk {chunk:?} but the worker assignment lists no active holder \
                 for it — same-cycle floor preemption should have re-placed it this cycle",
            );
        }
    }

    /// Copies the chunk with `key` holds at the converged (drained) fixed point — `0` if absent.
    pub(super) fn converged_replication_of(&mut self, key: &str) -> usize {
        self.drain_to_fixed_point();
        let converged = self.storage.snapshot();
        // Correlate by `chunk_id`, not backend pk.
        (0..converged.len())
            .find(|&i| converged.chunk_ids[i] == key)
            .map_or(0, |i| converged.ideal[i].len())
    }

    /// Advance past the drain window and reschedule until the ideal stabilises, no stale remains,
    /// and no whole-chunk removal is mid-flight; panics if not converged within `MAX_SETTLE`
    /// cycles. Works under a sustained shortage too: the per-cycle visibility pass keeps draining
    /// stale.
    ///
    /// The removal-in-flight clause matters under a standing shortage: until the M-window elapses
    /// the scheduler's demand still includes a swapped-out chunk — ideal frozen, stale empty,
    /// diffs empty, yet one more M-advance changes the verdict. Removal is purely time-driven, so
    /// waiting on it never blocks the exit.
    fn drain_to_fixed_point(&mut self) {
        const MAX_SETTLE: usize = 200;
        let mut before = self.storage.snapshot();
        for cycle in 0..MAX_SETTLE {
            self.now += M_TICKS;
            self.run_cycle();
            self.catch_up_and_confirm();
            // Second visibility pass lets the just-advanced confirmation promote, drain stale,
            // and replay the routing diffs this cycle.
            let portal_assignment = self
                .storage
                .run_visibility_cycle(self.now)
                .expect("visibility cycle succeeds");
            self.publish_portal_assignment(portal_assignment);
            let after = self.storage.snapshot();
            let stale_empty = after.stale.iter().all(|holders| holders.is_empty());
            let diffs_empty = self
                .storage
                .get_worker_assignment_diffs(|_| true)
                .is_empty();
            if after.ideal == before.ideal
                && stale_empty
                && diffs_empty
                && !self.removal_in_flight()
            {
                // Early warning for settle time creeping from ~3 cycles toward the cap.
                statistics::measure("drain: cycles to fixed point", (cycle + 1) as f64);
                return;
            }
            before = after;
        }
        let after = self.storage.snapshot();
        let pending_diffs = self.storage.get_worker_assignment_diffs(|_| true).len();
        let pending_stale: usize = after.stale.iter().map(|holders| holders.len()).sum();
        panic!(
            "storage did not reach quiescence within {MAX_SETTLE} cycles \
             (chunks={}, infeasible={}, stale copies still draining={pending_stale}, \
             routing diffs not replayed={pending_diffs}, removal in flight={})",
            self.total_chunk_count(),
            self.is_infeasible,
            self.removal_in_flight(),
        );
    }

    /// Whether any whole-chunk removal is mid-flight (marked but not portal-dropped, or dropped
    /// but not tombstoned). These resolve by time alone, so quiescence can safely wait on them.
    fn removal_in_flight(&self) -> bool {
        !self
            .storage
            .get_chunks_metadata(|meta| {
                (meta.marked_for_removal || meta.dropped_at_portal_assignment_id.is_some())
                    && meta.dropped_from_worker_assignment_at.is_none()
            })
            .is_empty()
    }

    // ---- Storage cycle (shared machinery) ---------------------------------------------------

    /// Run one scheduling pass against the live clock, recording the shortage flag. Returns the new
    /// assignment's id on success, or `None` on a recorded shortage.
    fn run_scheduler(&mut self) -> Option<AssignmentId> {
        match self
            .storage
            .run_scheduling_cycle(&self.algo, &self.config, self.now, M_TICKS)
        {
            Ok((assignment, bundle)) => {
                self.schedule_status = ScheduleStatus::SchedulerPlaced;
                self.is_infeasible = false;
                // A successful schedule ends a saturation episode — re-arm the latch so the walk
                // can saturate, probe, and recover again.
                self.converge_checked = false;
                let id = assignment.id;
                self.latest_worker_assignment = Some(assignment);
                // Freeze with the assignment; on a shortage (below) neither is reassigned.
                self.latest_worker_bundle = Some(bundle);
                Some(id)
            }
            Err(StorageError::Shortage) => {
                self.schedule_status = ScheduleStatus::NotEnoughCapacity;
                self.is_infeasible = true;
                None
            }
            Err(err) => panic!("scheduling cycle failed (not a shortage): {err}"),
        }
    }

    /// One storage cycle: schedule, advance the clock, run visibility, assert per-step safety.
    /// Does not confirm — confirmation comes from worker fetches. On a shortage nothing is
    /// committed; the shortage is recorded.
    ///
    /// The clock and portal visibility advance even on a shortage: skipping them would freeze
    /// draining forever.
    fn run_cycle(&mut self) {
        self.with_step_safety(|sim| {
            let scheduled = sim.run_scheduler();
            // Sync against the freshly published worker set so a departed-then-rejoined worker
            // starts from empty holdings.
            let active_workers = sim.active_worker_pks();
            sim.workers_state.sync_active_workers(&active_workers);
            // At 0% quorum, confirmation advances without worker acks, so the visibility pass
            // immediately promotes the assignment this same cycle. The fleet is NOT caught up
            // here, allowing the portal to route before workers hold chunks — an intentional
            // tradeoff measured in `assert_portal_consistency`, never fatal.
            if sim.confirm_threshold_pct == 0 {
                sim.refresh_confirmation(&active_workers);
            }
            sim.now += CLOCK_STEP;
            let portal_assignment = sim
                .storage
                .run_visibility_cycle(sim.now)
                .expect("visibility cycle succeeds");
            sim.publish_portal_assignment(portal_assignment);
            scheduled.is_some()
        });
    }

    /// Sole setter for `latest_portal_assignment`, so the watermark can never go stale relative
    /// to the assignment it bounds.
    fn publish_portal_assignment(&mut self, assignment: PortalAssignment) {
        // Must run before we overwrite the previous assignment it compares against.
        self.assert_visibility_monotonicity(&assignment);
        // Wired here (not separately inside `run_cycle`) so the schema-bundle invariant is
        // checked after every visibility cycle in the harness, including the convergence-draining
        // loop, from one call site.
        self.assert_schema_bundle_consistency();
        self.latest_portal_watermark =
            self.storage.get_worker_assignment_confirmation() as AssignmentId;
        self.latest_portal_assignment = Some(assignment);
    }

    /// Catch the quorum up to the latest assignment and advance the watermark — routing diffs and
    /// stale only drain once the superseding assignment is confirmed.
    ///
    /// Below a 100% quorum the designated laggards are deliberately left behind: catching them up
    /// would erase the sustained stragglers the consistency oracle's accountability scoping
    /// exists for. At 100% everyone must confirm, or drains would freeze.
    fn catch_up_and_confirm(&mut self) {
        let active = self.active_worker_pks();
        let catching_up: Vec<WorkerPk> = if self.confirm_threshold_pct >= 100 {
            active.clone()
        } else {
            let lagging: BTreeSet<WorkerPk> = self
                .lagging_worker_indexes()
                .iter()
                .filter_map(|&index| self.worker_pk_of_index(index))
                .collect();
            active
                .iter()
                .copied()
                .filter(|pk| !lagging.contains(pk))
                .collect()
        };
        if let Some(assignment) = self.latest_worker_assignment.clone() {
            self.workers_state
                .catch_up_all(&catching_up, &assignment, self.now);
        }
        self.refresh_confirmation(&active);
    }

    /// Recompute the watermark and advance the storage's confirmation (monotonic). At 0% quorum
    /// the watermark jumps to the latest published assignment; fetches feed oracles but don't gate
    /// confirmation.
    fn refresh_confirmation(&mut self, active: &[WorkerPk]) {
        let watermark = if self.confirm_threshold_pct == 0 {
            self.latest_worker_assignment
                .as_ref()
                .map_or(0, |assignment| assignment.id)
        } else {
            self.workers_state
                .confirmation_watermark(self.confirm_threshold_pct, active)
        };
        if watermark != 0 {
            self.storage
                .confirm_worker_assignment(watermark, self.now)
                .expect("watermark assignment id is valid");
        }
    }

    /// Snapshot the pre-cycle holders, run `body`, then assert step safety against the resulting
    /// snapshot — but only when `body` reports a committed schedule (a shortage commits nothing, so
    /// there is nothing to check). `held_before` is captured before `body` mutates the placement:
    /// retention is measured against the pre-cycle ideal, where a copy already draining is leaving
    /// by design but one dropped this step still counts.
    fn with_step_safety(&mut self, body: impl FnOnce(&mut Self) -> bool) -> bool {
        let held_before = self.held_before_by_pk();
        let committed = body(self);
        if committed {
            self.assert_step_safety(&held_before, &self.storage.snapshot());
        }
        committed
    }

    /// Pre-cycle holders per chunk pk that physically pre-existed this cycle (`ideal ∪ stale`
    /// non-empty), restricted to currently-active ideal holders. Chunks not yet placed are omitted,
    /// so `retention_and_atomic_publication` tells a genuinely new chunk (key absent) from one whose
    /// sole holder just departed (key present, holders empty → retention branch, not publication).
    ///
    /// FIXME: relies on a departed holder still appearing in the unfiltered `ideal` (confirmed routing
    /// is not scrubbed on departure today). If that changes, such a chunk would read as new here —
    /// revisit this discriminator with that fix.
    fn held_before_by_pk(&self) -> BTreeMap<ChunkPk, BTreeSet<WorkerPk>> {
        let snapshot = self.storage.snapshot();
        let active: BTreeSet<WorkerPk> = snapshot
            .worker_pks(&self.active_workers())
            .into_iter()
            .collect();
        snapshot
            .chunk_pks
            .iter()
            .copied()
            .zip(snapshot.ideal.iter().zip(snapshot.stale.iter()))
            .filter(|(_, (ideal, stale))| !ideal.is_empty() || !stale.is_empty())
            .map(|(pk, (ideal, _stale))| {
                let held = ideal
                    .iter()
                    .copied()
                    .filter(|worker| active.contains(worker))
                    .collect();
                (pk, held)
            })
            .collect()
    }

    // ---- Per-step safety (detection lives in `placement_oracles`) ---------------------------

    /// See [`placement_oracles::step_safety`]. `held_before` is keyed by pk so a chunk is tracked across a
    /// cycle even when an insert shifts pk-ordered positions. Removing chunks are exempt from the
    /// floors but still count for overcommit.
    fn assert_step_safety(
        &self,
        held_before: &BTreeMap<ChunkPk, BTreeSet<WorkerPk>>,
        after: &RawSnapshot,
    ) {
        if let Err(message) = placement_oracles::step_safety(
            held_before,
            after,
            &self.chunks_excluded_from_floor(),
            self.config.worker_capacity,
            usize::from(self.config.min_replication),
        ) {
            panic!("{message}");
        }
    }

    /// Run the shared correction oracles (O1–O3) against gate-A visibility (promoted and not
    /// dropped). Routing presence is deliberately not the predicate: a promoted replacement whose
    /// confirmed holders all departed drops out of routing without any swap-atomicity violation.
    fn assert_corrections_safety(&self) {
        let corrections = self.storage.get_corrections(|_| true);
        if corrections.is_empty() {
            return;
        }
        // Invariant 5 only bites once a swap has fired; a sweep with no completed correction
        // passes vacuously.
        statistics::classify(
            corrections
                .iter()
                .any(|correction| correction.is_completed()),
            "correction: some swap fired",
        );
        let visible = self.storage.visible_chunks();
        let removing = self.storage.chunks_in_removal();
        if let Err(message) =
            crate::scheduler_storage::test_harness::correction_oracle::corrections_safety(
                &corrections,
                |pk| visible.contains(pk),
                |pk| removing.contains(pk),
            )
        {
            panic!("{message}");
        }
    }

    /// Gate-A visibility monotonicity across published portal assignments: a chunk visible in the
    /// previously published assignment must still be visible in `next`, unless the model removed it
    /// or a completed correction swapped it out. Judged on the published artifact (`chunks`), not the
    /// storage stamps; the previous assignment is the one we are about to replace. The oracle itself
    /// is in `correction_oracle`.
    fn assert_visibility_monotonicity(&self, next: &PortalAssignment) {
        let Some(previous) = self.latest_portal_assignment.as_ref() else {
            return; // nothing published yet — no prior visibility to preserve
        };
        let previously_visible: HashSet<ChunkPk> = previous.chunks.keys().copied().collect();
        let corrections = self.storage.get_corrections(|_| true);
        if let Err(message) =
            crate::scheduler_storage::test_harness::correction_oracle::visibility_monotonicity(
                &previously_visible,
                |pk| next.chunks.contains_key(pk),
                // The sim issues no whole-chunk removals (only corrections retire chunks), so a
                // completed correction is the only sanctioned cause here. NB: do NOT wire this to a
                // storage removal flag (`chunks_in_removal`) — it is set by the same drop
                // pass that retracts visibility and would self-excuse every retraction. An
                // `Action::MarkForRemoval` would instead need the harness to record its own removal
                // intent and pass it here.
                |_| false,
                &corrections,
            )
        {
            panic!("{message}");
        }
    }

    /// Every chunk the latest worker/portal assignments name must resolve under the frozen
    /// `latest_worker_bundle`. `None` until the first successful cycle — nothing to check.
    fn assert_schema_bundle_consistency(&self) {
        let Some(bundle) = self.latest_worker_bundle.as_ref() else {
            return;
        };
        if let Err(message) = placement_oracles::schema_bundle_consistency(
            bundle,
            self.latest_worker_assignment.as_ref(),
            self.latest_portal_assignment.as_ref(),
        ) {
            panic!("{message}");
        }
    }

    // ---- Observation layer (fleet + portal) -------------------------------------------------

    /// Per-transition state telemetry — what regime the walk is in. No-op outside a proptest case.
    fn record_state_statistics(&self) {
        let snapshot = self.storage.snapshot();
        statistics::measure("state: chunks", snapshot.len() as f64);
        let stale_copies: usize = snapshot.stale.iter().map(|holders| holders.len()).sum();
        statistics::measure("state: stale copies", stale_copies as f64);
        if self.active_workers_count() > 0 {
            statistics::measure(
                "state: fleet fill ratio",
                self.fleet_fill_ratio_of(&snapshot),
            );
        }
        statistics::measure(
            "state: effective replication",
            f64::from(self.effective_replication_of(&snapshot)),
        );
        statistics::classify(self.is_infeasible, "state: shortage recorded (saturated)");
        self.record_departed_worker_statistics(&snapshot);
    }

    /// Coverage of the departed-but-registered window. Measure-only: the scheduler no longer places
    /// on departed workers, but the ideal keeps pre-departure placements until a cycle rewrites it.
    fn record_departed_worker_statistics(&self, snapshot: &RawSnapshot) {
        let departed: BTreeSet<WorkerPk> = self
            .storage
            .get_workers(|view| view.inactive_since.is_some())
            .iter()
            .map(|view| view.worker_id)
            .collect();
        // With no departed workers both classifiers are vacuously false — skip the scans but
        // still record the miss, so the sweep percentages stay meaningful.
        let stale_held = !departed.is_empty()
            && !self
                .storage
                .get_stale_mappings(|mapping| departed.contains(&mapping.worker_id))
                .is_empty();
        statistics::classify(stale_held, "stale: held by departed worker");
        let floor = usize::from(self.config.min_replication);
        let phantom_floor = !departed.is_empty() && {
            let excluded = self.chunks_excluded_from_floor();
            snapshot
                .chunk_pks
                .iter()
                .zip(&snapshot.ideal)
                .filter(|(chunk_pk, _)| !excluded.contains(chunk_pk))
                .any(|(_, holders)| {
                    holders.len() >= floor
                        && holders
                            .iter()
                            .filter(|worker| !departed.contains(worker))
                            .count()
                            < floor
                })
        };
        statistics::classify(phantom_floor, "floor: met only via departed holders");
    }

    /// The active workers' worker pks, looked up from the live snapshot's peer-id map.
    fn active_worker_pks(&self) -> Vec<WorkerPk> {
        self.storage.snapshot().worker_pks(&self.active_workers())
    }

    /// Pool index → worker pk via the snapshot's peer-id map; `None` if out of range or
    /// unregistered.
    fn worker_pk_of_index(&self, index: usize) -> Option<WorkerPk> {
        let peer = self.workers.get(index)?.worker.id.to_string();
        let snapshot = self.storage.snapshot();
        snapshot
            .peer_ids
            .iter()
            .find(|(_, id)| id.as_str() == peer)
            .map(|(pk, _)| *pk)
    }

    pub(super) fn has_published_portal_assignment(&self) -> bool {
        self.latest_portal_assignment.is_some()
    }

    /// End-to-end consistency oracle: at 100% quorum it is fatal; below 100% (including 0%)
    /// only *measures* misses because the scheduler may route before workers hold chunks.
    fn assert_portal_consistency(&self) {
        let Some(snapshot) = self.portal_state.snapshot.as_ref() else {
            statistics::label("portal check: skipped (never fetched)");
            return;
        };
        let age = self.portal_state.snapshot_age(self.now);
        let active: BTreeSet<WorkerPk> = self.active_worker_pks().into_iter().collect();
        let strict = self.confirm_threshold_pct >= 100;
        let watermark = self.portal_state.watermark;
        statistics::measure("portal: snapshot age at check", age as f64);
        if age >= M_TICKS {
            statistics::label("portal check: skipped (beyond M)");
            return;
        }
        let holds = |worker, chunk: &ChunkPk| self.workers_state.holds_chunk(worker, chunk);
        let is_active = |worker| active.contains(&worker);
        let accountable = |worker| {
            self.routing_has_caught_up_with_departure(worker, watermark)
                && (strict || self.workers_state.last_applied(worker) >= watermark)
        };
        // Count misses at EVERY quorum, before any excuse or stand-down: the ADR justifies excused
        // misses as "the envelope `portal_consistency_misses` counts", which is only true if the
        // counter runs on the strict path too.
        let misses =
            placement_oracles::portal_consistency_misses(snapshot, age, M_TICKS, holds, is_active);
        if !strict {
            statistics::label("portal check: measured (<100% quorum)");
            let straggler_present = active
                .iter()
                .any(|&worker| self.workers_state.last_applied(worker) < watermark);
            statistics::classify(
                straggler_present,
                "portal check: straggler present (scoping live)",
            );
            statistics::measure(
                "portal: routings a query would miss (<100% quorum)",
                misses as f64,
            );
            statistics::classify(misses > 0, "portal: query miss present (<100% quorum)");
            return;
        }
        statistics::label("portal check: strict (100% quorum)");
        statistics::measure(
            "portal: routings a query would miss (100% quorum)",
            misses as f64,
        );
        statistics::classify(
            misses > 0,
            "portal: excused/masked miss present (100% quorum)",
        );
        // Same shortage exemption as `assert_published_coverage`: an over-subscribed fleet can't
        // hold every floor, so a chunk with no copy anywhere is unavoidable and the within-M read
        // guarantee stands down (misses stay counted above).
        if self.is_infeasible {
            statistics::label("portal check: skipped (shortage)");
            return;
        }
        // Durability-hard, routing best-effort (ADR 0001): a within-M miss is excused while the
        // chunk keeps a covered holder in the latest worker assignment (`ideal ∪ stale`) — the
        // eviction kept the committed floor, only the routing lags. Same predicate as
        // `assert_published_coverage`; only a globally-uncovered chunk fires. Listing-based by
        // necessity (a rejoined worker is legitimately listed with an empty disk); physical loss
        // is policed by the edge check `assert_physical_retention`, not here.
        let covered_anywhere = |chunk: &ChunkPk| {
            self.latest_worker_assignment
                .as_ref()
                .is_some_and(|assignment| {
                    assignment.chunk_workers.get(chunk).is_some_and(|holders| {
                        holders.iter().any(|&w| {
                            active.contains(&w)
                                && self.routing_has_caught_up_with_departure(w, watermark)
                        })
                    })
                })
        };
        if let Err(message) = placement_oracles::portal_consistency(
            snapshot,
            age,
            M_TICKS,
            holds,
            is_active,
            accountable,
            covered_anywhere,
        ) {
            panic!("{message}");
        }
    }

    /// Whether the published routing has caught up with `worker`'s departure. Workers and portals
    /// act on published assignments, so until the assignment that dropped the worker is confirmed
    /// into the routing, portals are expected to still address it — and to find no data there if it
    /// rejoined meanwhile. Nothing retracts routing at departure by design; the next cycle does.
    fn routing_has_caught_up_with_departure(
        &self,
        worker: WorkerPk,
        routing_watermark: AssignmentId,
    ) -> bool {
        self.placed_until_departure
            .get(&worker)
            .is_none_or(|&placed_until| routing_watermark > placed_until)
    }

    /// Strand invariant on the published views: no routed chunk is left **globally uncovered** —
    /// the latest worker assignment (`ideal ∪ stale`) must list at least one holder that is active
    /// and past its departure window
    /// ([`routing_has_caught_up_with_departure`](Self::routing_has_caught_up_with_departure)). A
    /// preempted still-routed pair is tolerated while the chunk keeps some covered holder —
    /// bounded routing-lag, not a strand.
    fn assert_published_coverage(&self) {
        let Some(portal_assignment) = self.latest_portal_assignment.as_ref() else {
            return; // nothing published yet
        };
        if let Err(message) = placement_oracles::routing_is_non_empty(portal_assignment) {
            panic!("{message}");
        }
        let active: BTreeSet<WorkerPk> = self.active_worker_pks().into_iter().collect();
        let is_active = |worker| {
            active.contains(&worker)
                && self.routing_has_caught_up_with_departure(worker, self.latest_portal_watermark)
        };
        let holds = |worker, chunk: &ChunkPk| self.workers_state.holds_chunk(worker, chunk);
        // In a recorded shortage the fleet cannot hold every floor, so a routed chunk can
        // legitimately reach zero coverage while its routing lags (the scrub diff can't confirm
        // under a full-quorum shortage, and stale copies expire) — the strand check stands down.
        // NOT silently: the shortage flag is fleet-global, so a coverage bug on an unrelated chunk
        // would pass unobserved here. Classify what the stand-down masks so "shortage" windows that
        // routinely hide uncovered chunks stay visible in the statistics.
        if self.is_infeasible {
            let masked = placement_oracles::published_coverage(
                portal_assignment,
                self.latest_worker_assignment.as_ref(),
                is_active,
                holds,
                false,
            )
            .is_err();
            statistics::classify(
                masked,
                "strand check: uncovered routed chunk masked by shortage stand-down",
            );
            return;
        }
        if let Err(message) = placement_oracles::published_coverage(
            portal_assignment,
            self.latest_worker_assignment.as_ref(),
            is_active,
            holds,
            false,
        ) {
            panic!("{message}");
        }
        // Paper-only coverage (listed holders, zero physical copies) is legitimate in departure→
        // rejoin windows (a rejoined worker starts with an empty disk), so it can't be fatal by
        // state at any quorum — the *edge* check `assert_physical_retention` makes eviction-caused
        // physical loss fatal. Classified here for visibility.
        let paper_only = placement_oracles::published_coverage(
            portal_assignment,
            self.latest_worker_assignment.as_ref(),
            is_active,
            holds,
            true,
        )
        .is_err();
        statistics::classify(
            paper_only,
            "strand check: covered by listing only (no physical holder)",
        );
    }

    /// Shared by the generic [`IterativeSutStateMachine`] impl and the concrete Postgres impl
    /// (which has no `Default`).
    fn apply_transition(mut self, config: &SimConfig, transition: Action) -> Self {
        self.schedule_status = ScheduleStatus::NoSchedulerRun;
        // Only a `NoOp` is guaranteed to leave every oracle input untouched.
        self.oracle_inputs_changed = !matches!(transition, Action::NoOp);
        self.last_transition_shed_a_worker = matches!(transition, Action::WorkerLeft(_));
        statistics::label(action_kind(&transition));
        if matches!(transition, Action::NoOp) {
            self.trace_noop();
        } else {
            self.real_steps += 1;
            self.trace_operation(&action_label(&transition));
            // Each action logs the state after itself; log the state before here.
            self.trace_step("before");
        }
        match transition {
            Action::AddChunks(new) => {
                self.do_add_chunks(new);
            }
            Action::AdvanceClock(ticks) => {
                self.do_advance_clock(ticks);
            }
            Action::CheckConverged(check) => {
                self.check_converged(check);
                // Latch: the walks read a confirmed shortage as "recover now"; re-armed by the
                // next successful scheduling cycle (see `run_scheduler`).
                if config.converge_is_terminal || self.is_infeasible {
                    if self.is_infeasible {
                        self.saturation_episodes += 1;
                    }
                    self.converge_checked = true;
                }
            }
            Action::WorkerJoined(index) => {
                self.do_worker_joined(index);
            }
            Action::WorkerLeft(index) => {
                self.do_worker_left(index);
            }
            Action::WorkerFetchAssignment { worker, succeeds } => {
                self.do_worker_fetch(worker, succeeds);
            }
            Action::PortalFetchAssignment { succeeds } => {
                self.do_portal_fetch(succeeds);
            }
            Action::RegisterCorrection {
                old_dataset,
                old_chunk_id: old_key,
                replacement,
            } => {
                self.do_register_correction(&old_dataset, &old_key, replacement);
            }
            Action::SetMinReplication(min_replication) => {
                self.do_set_min_replication(min_replication);
            }
            Action::SetDatasetSchema { dataset, schema } => {
                self.do_set_dataset_schema(&dataset, schema);
            }
            Action::NoOp => {}
        }
        self.assert_physical_retention();
        self
    }

    /// Physical-retention oracle (edge-triggered): a routed chunk with ≥ 1 physical copy on an
    /// active worker must not reach zero copies in one transition, unless that transition was a
    /// departure (`WorkerLeft` — the data leaves with the worker, a documented availability event).
    /// This is the check the state-based coverage oracles cannot make: paper-only coverage is
    /// legitimate in departure→rejoin windows, but the *edge* is attributable — outside departures
    /// only a worker applying an assignment deletes a copy, and dropping a routed chunk's last
    /// copy for a replacement that hasn't downloaded yet is precisely the possession bug the
    /// eviction guard exists to prevent.
    ///
    /// At a full quorum the edge is fatal, and the shortage latch is deliberately NOT an excuse:
    /// shortage cycles commit nothing, so every assignment applied during a streak was
    /// guard-vetted while feasible — loss under the latch is a guard bug, not a shortage effect.
    /// Below a full quorum, drain expiry keyed to the quorum watermark can legitimately outrun a
    /// laggard replacement, so the edge is only classified (in or out of shortage).
    fn assert_physical_retention(&mut self) {
        let active = self.active_worker_pks();
        let removing = self.storage.chunks_in_removal();
        let (covered_now, lost) = match self.latest_portal_assignment.as_ref() {
            Some(portal_assignment) => {
                let covered_now: BTreeSet<ChunkPk> = portal_assignment
                    .chunk_workers
                    .keys()
                    .copied()
                    .filter(|chunk| !removing.contains(chunk))
                    .filter(|chunk| {
                        active
                            .iter()
                            .any(|&w| self.workers_state.holds_chunk(w, chunk))
                    })
                    .collect();
                let lost: Vec<ChunkPk> = self
                    .physically_covered
                    .iter()
                    .copied()
                    .filter(|chunk| {
                        portal_assignment.chunk_workers.contains_key(chunk)
                            && !removing.contains(chunk)
                            && !covered_now.contains(chunk)
                    })
                    .collect();
                (covered_now, lost)
            }
            None => (BTreeSet::new(), Vec::new()),
        };
        let strict = self.confirm_threshold_pct >= 100;
        let lost_without_departure = !lost.is_empty() && !self.last_transition_shed_a_worker;
        if lost_without_departure && strict {
            panic!(
                "physical retention breached: routed chunk(s) {lost:?} went from ≥1 physical copy \
                 to zero in a single non-departure transition{} — a worker deleted a routed chunk's \
                 last downloaded copy for a replacement that hasn't downloaded yet",
                if self.is_infeasible {
                    " (during a recorded shortage, which commits nothing and so cannot justify it)"
                } else {
                    ""
                },
            );
        }
        statistics::classify(
            lost_without_departure && self.is_infeasible,
            "physical retention: loss coincided with shortage latch (<100% quorum)",
        );
        statistics::classify(
            lost_without_departure && !self.is_infeasible,
            "physical retention: routed chunk lost its last copy without a departure (<100% quorum)",
        );
        self.physically_covered = covered_now;
    }
}

// ===========================================================================
// Helpers
// ===========================================================================

impl SimUnderTest<InMemoryStorage> {
    /// Fixed-config constructor for the standalone deterministic tests.
    pub(super) fn new() -> Self {
        Self::from_config(&SimConfig::fixed())
    }

    fn from_config(config: &SimConfig) -> Self {
        let storage = InMemoryStorage::default();
        Self::from_config_with(config, storage)
    }
}

impl<D: SimStorage> SimUnderTest<D> {
    /// Pre-registers datasets and syncs the initial worker set through the storage trait, so
    /// the same setup drives any backend.
    fn from_config_with(config: &SimConfig, mut storage: D) -> Self {
        let pool_size = config.worker_count + WORKER_POOL_HEADROOM;
        let workers: Vec<PooledWorker> = crate::tests::input::generate_workers(pool_size)
            .into_iter()
            .enumerate()
            .map(|(i, id)| PooledWorker {
                worker: Worker {
                    id,
                    status: WorkerStatus::Online,
                    version: None,
                },
                active: (i as u16) < config.worker_count,
            })
            .collect();

        let scheduling_config = SchedulingConfig {
            worker_capacity: config.worker_capacity,
            saturation: config.saturation,
            min_replication: config.min_replication,
            ignore_reliability: true,
        };

        // Chunk inserts require their dataset to already exist.
        storage
            .insert_new_datasets(
                config
                    .datasets
                    .iter()
                    .map(|name| (name.clone(), DatasetSchema::default()))
                    .collect(),
            )
            .expect("datasets are fresh");

        let weights = WeightTable::default();
        let mut sim = SimUnderTest {
            algo: MultistepAlgorithm::new(weights.clone()),
            weights,
            storage,
            workers,
            config: scheduling_config,
            gc_ticks: config.gc_ticks,
            now: 0,
            workers_state: WorkerFleet::default(),
            portal_state: Portal::default(),
            latest_worker_assignment: None,
            latest_worker_bundle: None,
            latest_portal_assignment: None,
            latest_portal_watermark: 0,
            placed_until_departure: BTreeMap::new(),
            confirm_threshold_pct: config.confirm_threshold_pct,
            converge_checked: false,
            real_steps: 0,
            saturation_episodes: 0,
            is_infeasible: false,
            // `true` so a pre-transition invariant check (e.g. `test_sequential`'s) runs.
            oracle_inputs_changed: true,
            physically_covered: BTreeSet::new(),
            last_transition_shed_a_worker: false,
            trace: trace_enabled(),
            noop_traced: false,
            step: 0,
            schedule_status: ScheduleStatus::NoSchedulerRun,
        };
        sim.sync_worker_set();
        sim
    }

    fn active_workers(&self) -> Vec<&Worker> {
        self.workers
            .iter()
            .filter(|w| w.active)
            .map(|w| &w.worker)
            .collect()
    }

    fn active_workers_count(&self) -> usize {
        self.workers.iter().filter(|w| w.active).count()
    }

    /// Index in the full pool, not just the active fleet.
    fn pool_index(&self, peer: &str) -> Option<usize> {
        self.workers
            .iter()
            .position(|w| w.worker.id.to_string() == peer)
    }

    fn is_worker_active(&self, index: usize) -> bool {
        self.workers.get(index).is_some_and(|w| w.active)
    }

    /// Pool indexes of the active fleet — `RemoveWorker` candidates.
    pub(super) fn active_worker_indexes(&self) -> Vec<usize> {
        (0..self.workers.len())
            .filter(|&i| self.workers[i].active)
            .collect()
    }

    /// Pool indexes of inactive workers — `AddWorker` candidates.
    pub(super) fn inactive_worker_indexes(&self) -> Vec<usize> {
        (0..self.workers.len())
            .filter(|&i| !self.workers[i].active)
            .collect()
    }

    /// Designated slow workers: the trailing actives beyond the confirmation quorum
    /// ([`observers::quorum_size`], the watermark's own definition), so confirmation keeps
    /// advancing while they lag. Empty at a 100% quorum, where a sustained laggard would freeze
    /// the watermark and deadlock every drain — and empty at a 0% quorum, where confirmation
    /// advances on its own (no quorum to fall behind), so every worker polls on the fast cadence.
    pub(super) fn lagging_worker_indexes(&self) -> Vec<usize> {
        if self.confirm_threshold_pct >= 100 || self.confirm_threshold_pct == 0 {
            return Vec::new();
        }
        let active = self.active_worker_indexes();
        let quorum = observers::quorum_size(self.confirm_threshold_pct, active.len());
        active[quorum..].to_vec()
    }

    /// Whether the fleet could still place every chunk's floor if the pool worker at `index` left.
    /// Two gates: (1) [`is_ideal_feasible`] — the surviving fleet's aggregate/stage-1 packing must
    /// fit every floor from scratch; and (2) [`departure_keeps_a_physical_holder`] — no
    /// portal-visible chunk may lose its last durable (committed-ideal ∩ held) copy with the
    /// departure. Gate 2 closes the departure-outpaces-re-replication residual: a chunk whose sole
    /// durable holder departs needs a fresh re-download, which a saturated fleet can starve across
    /// the next cycles — genuine loss no scheduler can mask. It still admits departures whenever
    /// replication is healthy (≥ 2 durable copies survive losing one), so the churn walk keeps
    /// generating real removals.
    pub(super) fn is_removal_recoverable(&self, index: usize) -> bool {
        let Some(victim) = self.workers.get(index).filter(|w| w.active) else {
            return true; // absent or out of range — removing it changes nothing
        };
        let victim_id = victim.worker.id;
        let survivors: Vec<&Worker> = self
            .active_workers()
            .into_iter()
            .filter(|worker| worker.id != victim_id)
            .collect();
        self.is_ideal_feasible(&survivors)
            && self.departure_keeps_a_physical_holder(&victim_id.to_string(), &survivors)
    }

    /// Whether every portal-visible chunk the victim physically holds keeps at least one *other*
    /// active durable holder after `victim_peer` leaves — one in the chunk's **committed ideal**
    /// that also physically holds it (`holds_chunk`): the reconcile preserves an ideal copy every
    /// cycle, and it is already resident so saturation cannot starve a re-download. A survivor's
    /// merely-physical copy is not enough — it can be a draining stale mapping the next cycles
    /// expire, so counting it green-lights a departure the strand oracle then flags as genuine
    /// loss.
    fn departure_keeps_a_physical_holder(&self, victim_peer: &str, survivors: &[&Worker]) -> bool {
        let pk_by_peer: HashMap<String, WorkerPk> = self
            .storage
            .get_workers(|_| true)
            .into_iter()
            .map(|w| (w.peer_id, w.worker_id))
            .collect();
        let Some(&victim_pk) = pk_by_peer.get(victim_peer) else {
            return true; // victim not in storage — nothing to lose
        };
        let survivor_pks: Vec<WorkerPk> = survivors
            .iter()
            .filter_map(|w| pk_by_peer.get(&w.id.to_string()).copied())
            .collect();

        // Committed ideal per chunk — stale/draining copies are scheduled to expire, so they
        // cannot back a durability guarantee.
        let snapshot = self.storage.snapshot();
        let ideal_by_pk: BTreeMap<ChunkPk, &BTreeSet<WorkerPk>> = (0..snapshot.len())
            .map(|i| (snapshot.chunk_pks[i], &snapshot.ideal[i]))
            .collect();

        let removing = self.storage.chunks_in_removal();
        for pk in self.storage.visible_chunks() {
            if removing.contains(&pk) {
                continue; // a chunk being removed legitimately loses every copy
            }
            // Only the victim's own held chunks are at risk from its departure.
            if !self.workers_state.holds_chunk(victim_pk, &pk) {
                continue;
            }
            let ideal = ideal_by_pk.get(&pk);
            let survivor_holds = survivor_pks.iter().any(|&w| {
                ideal.is_some_and(|holders| holders.contains(&w))
                    && self.workers_state.holds_chunk(w, &pk)
            });
            if !survivor_holds {
                return false; // victim holds this visible chunk's last durable copy — unrecoverable
            }
        }
        true
    }

    pub(super) fn total_chunk_count(&self) -> usize {
        self.storage.get_chunks(|_| true).len()
    }

    /// Total bytes of the chunk *set* (one copy each), independent of replication.
    pub(super) fn total_chunk_bytes(&self) -> u64 {
        self.storage
            .get_chunks(|_| true)
            .iter()
            .map(|view| u64::from(view.size))
            .sum()
    }

    /// Fraction of the whole fleet's disk taken by the established ideal placement.
    pub(super) fn fleet_fill_ratio(&self) -> f64 {
        self.fleet_fill_ratio_of(&self.storage.snapshot())
    }

    fn fleet_fill_ratio_of(&self, snapshot: &RawSnapshot) -> f64 {
        let used: u64 = snapshot.byte_load(&snapshot.ideal).values().sum();
        let capacity = self.config.worker_capacity * self.active_workers_count() as u64;
        used as f64 / capacity as f64
    }

    /// Minimum ideal-copy count over live chunks (removing chunks legitimately sit at zero and
    /// are excluded). Recomputed from the placement because the planner's per-weight
    /// `replication_by_weight` doesn't cross the storage boundary.
    pub(super) fn effective_replication(&self) -> u16 {
        self.effective_replication_of(&self.storage.snapshot())
    }

    fn effective_replication_of(&self, snapshot: &RawSnapshot) -> u16 {
        let removing = self.storage.chunks_in_removal();
        (0..snapshot.len())
            .filter(|&chunk| !removing.contains(&snapshot.chunk_pks[chunk]))
            .map(|chunk| snapshot.ideal[chunk].len())
            .min()
            .unwrap_or(0) as u16
    }

    /// Every existing chunk is a candidate correction *old* side. The generator stays dumb: picking
    /// an unsuitable one (rejected, being removed, or already corrected) is detected and disregarded
    /// by `register_correction` as a legal no-op, rather than pre-filtered here. Each carries its
    /// stored block range so the replacement inherits it, keeping the swap same-range (and threading
    /// a chain onto the root's range).
    pub(super) fn correction_old_chunk_candidates(&self) -> Vec<CorrectableOld> {
        self.storage
            .get_chunks(|_| true)
            .into_iter()
            .map(|view| CorrectableOld {
                chunk_id: view.chunk_id,
                dataset: view.dataset,
                blocks: view.blocks,
            })
            .collect()
    }

    /// Whether a chunk with this `(dataset, key)` identity already exists (mirrors the storage's
    /// `UNIQUE (dataset_id, chunk_id)`).
    pub(super) fn chunk_exists(&self, dataset: &str, key: &str) -> bool {
        self.pk_of(dataset, key).is_some()
    }

    fn has_stale_mappings(&self) -> bool {
        !self.storage.get_stale_mappings(|_| true).is_empty()
    }

    /// Pending = shed from the ideal but the drop not yet stamped. Its M clock hasn't started,
    /// so clock jumps can't expire it; only worker fetches (confirmation) move it along.
    pub(super) fn has_pending_stale(&self) -> bool {
        !self
            .storage
            .get_stale_mappings(|mapping| mapping.dropped_at_portal_assignment_id.is_none())
            .is_empty()
    }

    /// The generators' saturation signal: once the fleet can't place every floor, they stop
    /// adding until a recovery lever (a lower floor, a joined worker) clears the shortage.
    pub(super) fn is_infeasible(&self) -> bool {
        self.is_infeasible
    }

    fn is_at_chunk_cap(&self, cap: Option<usize>) -> bool {
        cap.is_some_and(|cap| self.total_chunk_count() >= cap)
    }
}

#[cfg(test)]
mod pg_tests;
#[cfg(test)]
mod tests;
