//! Guided, stateful property test of the multi-step scheduler, driven by the *iterative*
//! state-machine runner so the transition generator can read the live SUT rather than an abstract
//! twin.
//!
//! The SUT is a [`SchedulerStorage`](crate::scheduler_storage) MVCC lifecycle with the multistep
//! scheduler underneath; the same models run against both [`InMemoryStorage`] ([`in_memory`]) and
//! [`PostgresStorage`] ([`pg`]). Walks push the fleet toward saturation, probe convergence, then
//! recover (lower the replication floor / join a worker) and saturate again, cycling until the
//! levers run out. An observation layer (`Portal` / `WorkerFleet`, see `sut::observers`) samples
//! published assignments on independent lagging cadences; successful worker fetches advance the
//! X%-quorum confirmation watermark.
//!
//! Feasibility is judged at the *converged* state, not per step: shedding replicas leaves stale
//! copies on disk, so the footprint is transiently inflated and a burst may legitimately not all
//! land this cycle. Once drains clear and the output stabilises, every chunk must hold
//! `min_replication` (or the recorded shortage is cross-checked as genuine — unplaceable even from
//! an empty fleet).
//!
//! Checked on every transition: per-step safety (no-overcommit, retention floor, new-chunk
//! atomicity); portal routing consistency within the M-tick stale window — below a 100% quorum the
//! oracle holds only workers caught up to the snapshot's watermark; the published assignment
//! (`ideal ∪ stale`) covering everything the portal routes (`docs/mvcc-storage.md`); and
//! the correction oracles ([`correction_oracle`](crate::scheduler_storage::test_harness::correction_oracle)).
//!
//! Environment knobs (all optional):
//!
//! - `SIM_IN_MEMORY_CASES` — cases per in-memory sweep (else `PROPTEST_CASES`, else 256). Each
//!   case is a 100–300-transition walk, so this is the CI budget lever.
//! - `SIM_PG_CASES` — cases per Postgres sweep (default 10; fresh database per case).
//! - `SIM_SEED` / `SIM_CASE_SEED` — replay a whole run / a single case (printed on failure).
//! - `SIM_TRACE` — per-operation before→after state trace.
//! - `SIM_WORKERS`, `SIM_CHUNKS`, `SIM_WORKER_CAPACITY`, `SIM_RANDOM_SIZES` — world-shape
//!   overrides for debugging.

use proptest::test_runner::{Config, RngAlgorithm, TestError, TestRng, TestRunner};
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};

mod regression;
mod sut;
mod utils;

use proptest_state_machine::iterative_runner::{Iterative, IterativeRunner};
use sut::{Action, SimConfig, SimUnderTest};
use utils::{
    Seed, SweepStatistics, env_seed, in_memory_sim_cases, master_seed, pg_sim_cases,
    proptest_config, seed_hex, trace_enabled,
};

/// The models against the in-memory backend, swept over the [`in_memory_sim_cases`] budget.
mod in_memory {
    use super::*;
    use crate::scheduler_storage::in_memory::InMemoryStorage;
    use sut::{ChurnModel, FloodModel, SimModel, ZeroQuorumModel};

    type Sut = SimUnderTest<InMemoryStorage>;

    /// Guided walk ([`SimModel`]): the generator reads the live SUT and pushes toward saturation.
    #[test]
    fn guided_simulation() -> Result<(), TestError<(SimConfig, Vec<Action>)>> {
        run_cases(
            &guided(),
            "guided_simulation",
            "in_memory::guided_simulation_case",
            in_memory_sim_cases(),
        )
    }

    /// Replay a single [`guided_simulation`] case from `SIM_CASE_SEED`.
    #[test]
    fn guided_simulation_case() -> Result<(), TestError<(SimConfig, Vec<Action>)>> {
        replay_case(&guided(), "guided_simulation_case")
    }

    /// Replication-collapse stress: [`FloodModel`] scripts the fill→flood→converge arc.
    #[test]
    fn flood_after_max_replication_converges() -> Result<(), TestError<(SimConfig, Vec<Action>)>> {
        run_cases(
            &flood(),
            "flood_after_max_replication_converges",
            "in_memory::flood_after_max_replication_converges_case",
            in_memory_sim_cases(),
        )
    }

    /// Replay a single [`flood_after_max_replication_converges`] case from `SIM_CASE_SEED`.
    #[test]
    fn flood_after_max_replication_converges_case()
    -> Result<(), TestError<(SimConfig, Vec<Action>)>> {
        replay_case(&flood(), "flood_after_max_replication_converges_case")
    }

    /// Guided walk plus infrequent worker join/departure ([`ChurnModel`]). Departures are
    /// precondition-gated to keep every floor holdable, so the run stays feasible.
    #[test]
    fn churn_simulation() -> Result<(), TestError<(SimConfig, Vec<Action>)>> {
        run_cases(
            &churn(),
            "churn_simulation",
            "in_memory::churn_simulation_case",
            in_memory_sim_cases(),
        )
    }

    /// Replay a single [`churn_simulation`] case from `SIM_CASE_SEED`.
    #[test]
    fn churn_simulation_case() -> Result<(), TestError<(SimConfig, Vec<Action>)>> {
        replay_case(&churn(), "churn_simulation_case")
    }

    /// Guided walk at a 0% confirmation quorum ([`ZeroQuorumModel`]): the watermark tracks the
    /// latest assignment without any worker confirming, so portal promotion never waits on a
    /// fetch. The same safety and consistency oracles must still hold.
    #[test]
    fn zero_quorum_simulation() -> Result<(), TestError<(SimConfig, Vec<Action>)>> {
        run_cases(
            &zero_quorum(),
            "zero_quorum_simulation",
            "in_memory::zero_quorum_simulation_case",
            in_memory_sim_cases(),
        )
    }

    /// Replay a single [`zero_quorum_simulation`] case from `SIM_CASE_SEED`.
    #[test]
    fn zero_quorum_simulation_case() -> Result<(), TestError<(SimConfig, Vec<Action>)>> {
        replay_case(&zero_quorum(), "zero_quorum_simulation_case")
    }

    // Walks run out of levers around 100–250 real steps; longer budgets only pad the idle tail.
    fn guided() -> Iterative<Action, SimConfig, Sut> {
        Iterative::new::<SimModel<InMemoryStorage>, Sut>(100usize..300)
    }

    fn zero_quorum() -> Iterative<Action, SimConfig, Sut> {
        Iterative::new::<ZeroQuorumModel<InMemoryStorage>, Sut>(100usize..300)
    }

    // The scripted arc is ~10 real steps; the rest is an idle `NoOp` tail.
    fn flood() -> Iterative<Action, SimConfig, Sut> {
        Iterative::new::<FloodModel<InMemoryStorage>, Sut>(30usize..1000)
    }

    fn churn() -> Iterative<Action, SimConfig, Sut> {
        Iterative::new::<ChurnModel<InMemoryStorage>, Sut>(100usize..300)
    }
}

/// The same models against real Postgres (fresh DB per case via testcontainers; requires
/// Docker), at the reduced [`pg_sim_cases`] budget — the DB is far slower per step.
mod pg {
    use super::*;
    use crate::scheduler_storage::postgres::PostgresStorage;
    use sut::{ChurnModel, FloodModel, SimModel, ZeroQuorumModel};

    type Sut = SimUnderTest<PostgresStorage>;

    #[test]
    fn guided_simulation() -> Result<(), TestError<(SimConfig, Vec<Action>)>> {
        run_cases(
            &guided(),
            "pg_guided_simulation",
            "pg::guided_simulation_case",
            pg_sim_cases(),
        )
    }

    /// Replay a single [`guided_simulation`] case from `SIM_CASE_SEED`.
    #[test]
    fn guided_simulation_case() -> Result<(), TestError<(SimConfig, Vec<Action>)>> {
        replay_case(&guided(), "pg_guided_simulation_case")
    }

    #[test]
    fn flood_after_max_replication_converges() -> Result<(), TestError<(SimConfig, Vec<Action>)>> {
        run_cases(
            &flood(),
            "pg_flood_after_max_replication_converges",
            "pg::flood_after_max_replication_converges_case",
            pg_sim_cases(),
        )
    }

    /// Replay a single [`flood_after_max_replication_converges`] case from `SIM_CASE_SEED`.
    #[test]
    fn flood_after_max_replication_converges_case()
    -> Result<(), TestError<(SimConfig, Vec<Action>)>> {
        replay_case(&flood(), "pg_flood_after_max_replication_converges_case")
    }

    #[test]
    fn churn_simulation() -> Result<(), TestError<(SimConfig, Vec<Action>)>> {
        run_cases(
            &churn(),
            "pg_churn_simulation",
            "pg::churn_simulation_case",
            pg_sim_cases(),
        )
    }

    /// Replay a single [`churn_simulation`] case from `SIM_CASE_SEED`.
    #[test]
    fn churn_simulation_case() -> Result<(), TestError<(SimConfig, Vec<Action>)>> {
        replay_case(&churn(), "pg_churn_simulation_case")
    }

    #[test]
    fn zero_quorum_simulation() -> Result<(), TestError<(SimConfig, Vec<Action>)>> {
        run_cases(
            &zero_quorum(),
            "pg_zero_quorum_simulation",
            "pg::zero_quorum_simulation_case",
            pg_sim_cases(),
        )
    }

    /// Replay a single [`zero_quorum_simulation`] case from `SIM_CASE_SEED`.
    #[test]
    fn zero_quorum_simulation_case() -> Result<(), TestError<(SimConfig, Vec<Action>)>> {
        replay_case(&zero_quorum(), "pg_zero_quorum_simulation_case")
    }

    // Deeper than the old 50..100 so each case spends more steps doing real scheduling work;
    // SIM_WORKERS widens the fleet to keep these steps productive rather than padding an idle tail.
    fn guided() -> Iterative<Action, SimConfig, Sut> {
        Iterative::new::<SimModel<PostgresStorage>, Sut>(100usize..200)
    }

    fn zero_quorum() -> Iterative<Action, SimConfig, Sut> {
        Iterative::new::<ZeroQuorumModel<PostgresStorage>, Sut>(100usize..200)
    }

    fn flood() -> Iterative<Action, SimConfig, Sut> {
        Iterative::new::<FloodModel<PostgresStorage>, Sut>(100usize..200)
    }

    fn churn() -> Iterative<Action, SimConfig, Sut> {
        Iterative::new::<ChurnModel<PostgresStorage>, Sut>(100usize..200)
    }
}

// ===========================================================================
// Shared run helpers
// ===========================================================================

/// Run `cases` independent cases, each seeded from a per-run master seed. `case_test` is the
/// module-qualified path of the per-case replay test, used to build a copy-pasteable replay
/// command. `S` needs no bound — all behaviour comes from the supplied `Iterative<_, _, S>`.
fn run_cases<S>(
    iterative: &Iterative<Action, SimConfig, S>,
    name: &str,
    case_test: &str,
    cases: u32,
) -> Result<(), TestError<(SimConfig, Vec<Action>)>> {
    let config = proptest_config();
    let master = master_seed();
    eprintln!(
        "{name}: master seed — replay the whole run with SIM_SEED={}",
        seed_hex(&master)
    );

    let mut case_rng = StdRng::from_seed(master);
    let mut sweep = SweepStatistics::default();
    for case in 0..cases {
        let mut seed = [0u8; 32];
        case_rng.fill_bytes(&mut seed);
        if trace_enabled() {
            eprintln!("── case {case}: SIM_CASE_SEED={}", seed_hex(&seed));
        }
        if let Err(err) = run_case(iterative, &config, seed, &mut sweep) {
            eprintln!(
                "\n{name} FAILED on case {case}.\n    \
                 replay just this case:  SIM_CASE_SEED={case_seed} cargo test --lib \
                 multistep_scheduler::sim::{case_test} -- --exact --nocapture\n    \
                 replay the whole run:   SIM_SEED={master_seed}\n",
                case_seed = seed_hex(&seed),
                master_seed = seed_hex(&master),
            );
            return Err(err);
        }
    }

    sweep.print(name);
    // Reprint the seed so it's copyable without scrolling back past a long trace.
    eprintln!(
        "{name}: all {cases} cases passed — replay this run with SIM_SEED={}",
        seed_hex(&master),
    );
    Ok(())
}

/// Replay one case from `SIM_CASE_SEED`. No seed set → no-op pass, so it stays green in normal
/// runs.
fn replay_case<S>(
    iterative: &Iterative<Action, SimConfig, S>,
    name: &str,
) -> Result<(), TestError<(SimConfig, Vec<Action>)>> {
    let Some(seed) = env_seed("SIM_CASE_SEED") else {
        eprintln!("{name}: set SIM_CASE_SEED=<64 hex chars> to replay a case");
        return Ok(());
    };
    eprintln!("{name}: replaying SIM_CASE_SEED={}", seed_hex(&seed));
    let mut sweep = SweepStatistics::default();
    let result = run_case(iterative, &proptest_config(), seed, &mut sweep);
    sweep.print(name);
    result
}

/// One case from a seed — deterministic: same seed + config replays the same sequence.
fn run_case<S>(
    iterative: &Iterative<Action, SimConfig, S>,
    config: &Config,
    seed: Seed,
    sweep: &mut SweepStatistics,
) -> Result<(), TestError<(SimConfig, Vec<Action>)>> {
    let rng = TestRng::from_seed(RngAlgorithm::ChaCha, &seed);
    let mut runner = TestRunner::new_with_rng(config.clone(), rng);
    let result = IterativeRunner::incremental_runner(iterative, &mut runner);
    // Skip failing cases: shrinking re-executes passing transitions and would skew the stats.
    if result.is_ok() {
        sweep.absorb(runner.statistics());
    }
    result
}
