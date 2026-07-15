//! Guided model: the state-aware random walk behind `guided_simulation`. Model state is just the
//! per-case [`SimConfig`]; everything evolving lives in the SUT and is read straight off it.

use std::marker::PhantomData;

use proptest::prelude::*;
use proptest::strategy::Union;

use super::super::utils::{
    SimProfile, add_random_chunks, advance_clock, default_sim_config, register_correction,
    set_dataset_schema,
};
use super::{
    Action, ConvergenceCheck, SimConfig, SimStorage, SimUnderTest, standard_preconditions,
};
use proptest_state_machine::iterative_runner::IterativeModelStateMachine;

/// Deliberately no capacity estimate / add-wind-down: adds probe straight through saturation
/// (storage accepts every insert; the scheduler records shortages), driving genuine
/// over-subscription, graceful convergence, and add-after-clear recovery.
pub(in super::super) struct SimModel<D>(PhantomData<D>);

impl<D: SimStorage + SimProfile> IterativeModelStateMachine for SimModel<D> {
    type ModelState = SimConfig;
    type Transition = Action;
    type SutState = SimUnderTest<D>;

    fn init_state() -> BoxedStrategy<Self::ModelState> {
        default_sim_config::<D>()
    }

    /// Push up to saturation; once the converged probe confirms a shortage, recover by lowering
    /// the floor and push again. Occasional floor raises bank further episodes, so a run cycles
    /// saturate→probe→recover until its levers run out.
    fn transition(config: &SimConfig, sut: &SimUnderTest<D>) -> BoxedStrategy<Self::Transition> {
        if sut.total_chunk_count() == 0 {
            sut.trace_decision("no chunks yet — seeding the first batch");
            return add_random_chunks(6, &config.datasets);
        }

        // Converged at saturation: lowering the floor sheds now-excess copies and frees budget,
        // so the walk saturates again instead of idling out.
        if sut.converge_checked {
            let floor = sut.config.min_replication;
            if floor > 1 {
                sut.trace_decision("saturated & converged — lowering the floor to recover");
                return Just(Action::SetMinReplication(floor - 1)).boxed();
            }
            return Just(Action::NoOp).boxed();
        }

        let mut choices: Vec<(u32, BoxedStrategy<Action>)> = Vec::new();
        if sut.is_infeasible() {
            sut.trace_decision("saturated (shortage recorded) — no more adds; draining/converging");
        } else if sut.is_at_chunk_cap(config.chunk_cap) {
            sut.trace_decision("chunk cap reached — not adding more (still draining stale)");
        } else {
            choices.push((8, add_random_chunks(5, &config.datasets)));
            // Corrections insert a replacement chunk, so gate them like adds.
            let olds = sut.correction_old_chunk_candidates();
            if !olds.is_empty() {
                choices.push((1, register_correction(olds)));
            }
        }

        if sut.has_stale_mappings() {
            // Clock jumps expire only *stamped* drains; unconfirmed sheds (M clock not started)
            // are moved along by the upweighted fetches in `fetch_choices`.
            choices.push((2, advance_clock()));
        } else if sut.has_published_portal_assignment() {
            // Nothing draining, but time still ages the portal's snapshot across the M boundary.
            choices.push((1, advance_clock()));
        }
        choices.push((
            2,
            Just(Action::CheckConverged(
                ConvergenceCheck::FloorLocallyFeasible,
            ))
            .boxed(),
        ));
        // Floor raises hasten the next saturation episode and bank a recovery lever. Upweighted
        // at floor 1, where a raise is the only thing preventing an idle-out at next saturation.
        let floor = sut.config.min_replication;
        if !sut.is_infeasible() && usize::from(floor) < sut.active_workers_count() {
            let weight = if floor == 1 { 5 } else { 1 };
            choices.push((weight, Just(Action::SetMinReplication(floor + 1)).boxed()));
        }
        choices.push((1, set_dataset_schema(&config.datasets)));
        choices.extend(super::super::utils::fetch_choices(sut));
        Union::new_weighted(choices).boxed()
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
