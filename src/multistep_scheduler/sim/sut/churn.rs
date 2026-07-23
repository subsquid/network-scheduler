//! Churn model: the guided walk of [`SimModel`](super::guided::SimModel) plus infrequent worker
//! joins and departures. The standard precondition rejects departures the fleet couldn't absorb —
//! an aggregate floor shortage, or taking a visible chunk's last durable (committed ideal ∩ held)
//! copy with no re-replication headroom — so the run stays feasible while still churning.

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

/// Worker-churn extension of [`SimModel`](super::guided::SimModel).
pub(in super::super) struct ChurnModel<D>(PhantomData<D>);

impl<D: SimStorage + SimProfile> IterativeModelStateMachine for ChurnModel<D> {
    type ModelState = SimConfig;
    type Transition = Action;
    type SutState = SimUnderTest<D>;

    fn init_state() -> BoxedStrategy<Self::ModelState> {
        default_sim_config::<D>()
    }

    fn transition(config: &SimConfig, sut: &SimUnderTest<D>) -> BoxedStrategy<Self::Transition> {
        if sut.total_chunk_count() == 0 {
            sut.trace_decision("no chunks yet — seeding the first batch");
            return add_random_chunks(6, &config.datasets);
        }

        // Converged at saturation: recover by lowering the floor or joining a spare worker,
        // else idle out the rest of the sequence.
        if sut.converge_checked {
            let mut recovery: Vec<(u32, BoxedStrategy<Action>)> = Vec::new();
            let floor = sut.config.min_replication;
            if floor > 1 {
                recovery.push((1, Just(Action::SetMinReplication(floor - 1)).boxed()));
            }
            let inactive = sut.inactive_worker_indexes();
            if !inactive.is_empty() {
                recovery.push((
                    1,
                    prop::sample::select(inactive)
                        .prop_map(Action::WorkerJoined)
                        .boxed(),
                ));
            }
            if recovery.is_empty() {
                return Just(Action::NoOp).boxed();
            }
            sut.trace_decision("saturated & converged — recovering (lower floor / join worker)");
            return Union::new_weighted(recovery).boxed();
        }

        let mut choices: Vec<(u32, BoxedStrategy<Action>)> = Vec::new();
        // Add until a shortage is recorded; after that drain/converge/churn until the
        // converged probe latches and the recovery above fires.
        if !sut.is_infeasible() && !sut.is_at_chunk_cap(config.chunk_cap) {
            choices.push((8, add_random_chunks(5, &config.datasets)));
            // Low-weight corrections interleave with churn, so swaps race joins/departures.
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

        // Low-weight churn. Departures are vetted by the precondition when drawn, not here: the
        // recoverability probe is a full scheduler run and would execute on every generated step.
        let inactive = sut.inactive_worker_indexes();
        if !inactive.is_empty() {
            choices.push((
                1,
                prop::sample::select(inactive)
                    .prop_map(Action::WorkerJoined)
                    .boxed(),
            ));
        }
        let active = sut.active_worker_indexes();
        if !active.is_empty() {
            choices.push((
                1,
                prop::sample::select(active)
                    .prop_map(Action::WorkerLeft)
                    .boxed(),
            ));
        }

        // Floor raises hasten the next saturation episode and bank a recovery lever. Milder
        // floor-1 boost than the guided model: spare-worker joins already keep this recoverable.
        let floor = sut.config.min_replication;
        if !sut.is_infeasible() && usize::from(floor) < sut.active_workers_count() {
            let weight = if floor == 1 { 2 } else { 1 };
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
