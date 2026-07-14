//! Zero-quorum variant: [`SimModel`](super::guided::SimModel) with `confirm_threshold_pct = 0`.
//! At 0% quorum, no worker confirms yet the SUT advances the watermark to the latest assignment
//! (see [`refresh_confirmation`](super::SimUnderTest)), so promotion never waits on a fetch.
//! Portal consistency is measured, not asserted.

use std::marker::PhantomData;

use proptest::prelude::*;

use super::super::utils::{SimProfile, default_sim_config};
use super::{Action, SimConfig, SimModel, SimStorage, SimUnderTest};
use proptest_state_machine::iterative_runner::IterativeModelStateMachine;

/// [`SimModel`](super::guided::SimModel) with the confirmation quorum pinned to 0.
pub(in super::super) struct ZeroQuorumModel<D>(PhantomData<D>);

impl<D: SimStorage + SimProfile> IterativeModelStateMachine for ZeroQuorumModel<D> {
    type ModelState = SimConfig;
    type Transition = Action;
    type SutState = SimUnderTest<D>;

    /// Generate config with `confirm_threshold_pct = 0`.
    fn init_state() -> BoxedStrategy<Self::ModelState> {
        default_sim_config::<D>()
            .prop_map(|config| SimConfig {
                confirm_threshold_pct: 0,
                ..config
            })
            .boxed()
    }

    fn transition(config: &SimConfig, sut: &SimUnderTest<D>) -> BoxedStrategy<Self::Transition> {
        SimModel::<D>::transition(config, sut)
    }

    fn apply(config: Self::ModelState, _transition: &Self::Transition) -> Self::ModelState {
        config
    }

    fn preconditions(
        config: &SimConfig,
        sut: &SimUnderTest<D>,
        transition: &Self::Transition,
    ) -> bool {
        SimModel::<D>::preconditions(config, sut, transition)
    }
}
