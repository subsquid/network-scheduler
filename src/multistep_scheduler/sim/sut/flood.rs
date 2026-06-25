//! Flood model: a scripted scenario for the replication *collapse* path the guided [`SimModel`]
//! is unlikely to sample — fill to maximal replication, flood in one cycle to drive the factor
//! down to the floor, then converge. Shares the guided runner (per-step safety + convergence
//! oracle); `transition` scripts each phase off the live SUT.

use std::marker::PhantomData;

use proptest::prelude::*;

use super::super::utils::{
    MAX_CHUNK_SIZE, MIN_CHUNK_SIZE, WEIGHTS, add_chunk, add_chunks, random_key, sim_datasets,
    worker_capacity,
};
use super::{
    Action, ConvergenceCheck, SimConfig, SimStorage, SimUnderTest, standard_preconditions,
};
use proptest_state_machine::iterative_runner::IterativeModelStateMachine;

/// Scripted flood model. `worker_count` is drawn well above `min_replication` so the factor has
/// room to fall; convergence is terminal so the run idles after the check.
///
/// The arc: (1) **fill** to [`FILL_TARGET`](FloodModel::FILL_TARGET) with heavy-weight chunks,
/// which all replicate maximally while the dataset is small; (2) **flood** a large but
/// still-feasible batch of light-weight chunks in one cycle — the light newcomers settle at the
/// floor while the heavy chunks shed their now-excess copies, the weight split the convergence
/// oracle probes; (3) **converge** under
/// [`FloorsPreemptBonuses`](ConvergenceCheck::FloorsPreemptBonuses), then idle.
///
/// Phases split on the replication factor, not `has_stale`: an over-large flood can record a
/// shortage that places (and so sheds) nothing, leaving no stale yet not pre-flood either.
pub(in super::super) struct FloodModel<D>(PhantomData<D>);

impl<D: SimStorage> FloodModel<D> {
    /// Fill fraction of fleet disk. Below `saturation`, so replication is still maximal at the
    /// crossover — that's what makes the collapse observable.
    const FILL_TARGET: f64 = 0.70;
    /// Flood to this fraction of the floor budget: forces the factor down, yet stage-1 feasible.
    const FLOOD_TARGET: f64 = 0.90;

    /// Phase-1 fill chunk at the heaviest weight. One shared weight keeps replication maximal
    /// until the flood — mixed weights would shed copies mid-fill and trip the phase check — and
    /// maximises the bonus-above-floor the convergence oracle probes.
    fn fill_chunk(config: &SimConfig) -> BoxedStrategy<Action> {
        let dataset = config.datasets[0].clone();
        add_chunk(
            random_key(),
            MIN_CHUNK_SIZE..=MAX_CHUNK_SIZE,
            Just(*WEIGHTS.last().expect("WEIGHTS is non-empty")),
            Just(dataset),
        )
    }

    /// Flood chunk count: brings the dataset to [`FLOOD_TARGET`] of the floor budget (sized off
    /// the mean chunk size), mirroring the planner's
    /// `total_size × min_replication ≤ saturation × total_capacity` — lands without a shortage
    /// yet collapses the factor.
    ///
    /// [`FLOOD_TARGET`]: FloodModel::FLOOD_TARGET
    fn flood_count(config: &SimConfig, sut: &SimUnderTest<D>) -> usize {
        let budget =
            config.saturation * f64::from(config.worker_count) * config.worker_capacity as f64;
        let target_total = (Self::FLOOD_TARGET * budget / f64::from(config.min_replication)) as u64;
        let deficit = target_total.saturating_sub(sut.total_chunk_bytes());
        let mean_chunk_size = u64::from(MIN_CHUNK_SIZE + MAX_CHUNK_SIZE) / 2;
        (deficit / mean_chunk_size).max(1) as usize
    }
}

impl<D: SimStorage> IterativeModelStateMachine for FloodModel<D> {
    type ModelState = SimConfig;
    type Transition = Action;
    type SutState = SimUnderTest<D>;

    /// Hardcodes a 5–8 worker fleet rather than the per-backend [`SimProfile`] fleet: the short
    /// scripted arc converges at any fleet size, so it needs no small-fleet bias.
    fn init_state() -> BoxedStrategy<Self::ModelState> {
        (5u16..=8, 1u16..=2, 85u32..=97)
            .prop_map(
                |(worker_count, min_replication, saturation_pct)| SimConfig {
                    worker_count,
                    worker_capacity: worker_capacity(),
                    min_replication: min_replication.min(worker_count),
                    saturation: f64::from(saturation_pct) / 100.0,
                    converge_is_terminal: true,
                    chunk_cap: None,
                    datasets: sim_datasets(),
                    confirm_threshold_pct: 100,
                },
            )
            .boxed()
    }

    fn transition(config: &SimConfig, sut: &SimUnderTest<D>) -> BoxedStrategy<Self::Transition> {
        // Post-flood convergence check passed — idle for the rest.
        if sut.converge_checked {
            return Just(Action::NoOp).boxed();
        }

        if sut.total_chunk_count() == 0 {
            sut.trace_decision("flood: seeding the first chunk");
            return Self::fill_chunk(config);
        }

        // Phase detection keys on the replication factor, not `has_stale` (see type doc).
        if sut.effective_replication() == config.worker_count {
            // Phase 1: fill one heavy-weight chunk at a time, staying at the peak.
            if sut.fleet_fill_ratio() < Self::FILL_TARGET {
                return Self::fill_chunk(config);
            }
            // Phase 2: fill reached the target — flood in one cycle.
            let count = Self::flood_count(config, sut);
            sut.trace_decision(&format!(
                "flood: filled to {:.0}% at replication {} — flooding {count} chunks in one cycle",
                Self::FILL_TARGET * 100.0,
                config.worker_count,
            ));
            return add_chunks(
                count,
                MIN_CHUNK_SIZE..=MAX_CHUNK_SIZE,
                Just(WEIGHTS[0]),
                Just(config.datasets[0].clone()),
            );
        }

        // Phase 3: replication collapsed below the peak — fetch and converge, then idle.
        let mut choices: Vec<(u32, BoxedStrategy<Action>)> = vec![(
            3,
            Just(Action::CheckConverged(
                ConvergenceCheck::FloorsPreemptBonuses,
            ))
            .boxed(),
        )];
        choices.extend(super::super::utils::fetch_choices(sut));
        proptest::strategy::Union::new_weighted(choices).boxed()
    }

    fn apply(config: Self::ModelState, _transition: &Self::Transition) -> Self::ModelState {
        config
    }

    fn preconditions(
        _config: &SimConfig,
        sut: &SimUnderTest<D>,
        transition: &Self::Transition,
    ) -> bool {
        standard_preconditions(sut, transition)
    }
}
