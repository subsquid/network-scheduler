//! Captured regressions — shrunk action sequences, each pinning one scheduler property. Chunks
//! carry their own key/size/weight/dataset, so each sequence replays deterministically with no
//! seed. The replay *is* the assertion: it panics iff the property is violated.

use super::sut::{Action, ConvergenceCheck, NewChunk, SimConfig, SimUnderTest};
use super::utils::{
    CHUNK_SIZE, NEVER_GC_TICKS, SCHEMA_POOL, WORKER_CAPACITY, blocks_for_key, mint_key, new_chunk,
    sim_datasets,
};
use crate::scheduler_storage::in_memory::InMemoryStorage;
use proptest_state_machine::iterative_runner::IterativeSutStateMachine;

type Sut = SimUnderTest<InMemoryStorage>;

/// Apply a captured sequence to a fresh SUT via the same `apply` + `check_invariants` path the runner
/// takes after every transition, so the replay asserts the full invariant set (not only the in-`apply`
/// convergence checks).
fn replay(config: &SimConfig, actions: Vec<Action>) -> Sut {
    let mut sim = Sut::init_test(config);
    for action in actions {
        sim = Sut::apply(sim, config, action);
        Sut::check_invariants(&sim, config);
    }
    sim
}

/// Postgres-backed [`replay`], for captures that only reproduce against the real storage (the
/// in-memory oracle takes a different path through the same algorithm).
fn replay_pg(
    config: &SimConfig,
    actions: Vec<Action>,
) -> SimUnderTest<crate::scheduler_storage::postgres::PostgresStorage> {
    use crate::scheduler_storage::postgres::PostgresStorage;
    let mut sim = SimUnderTest::<PostgresStorage>::init_test(config);
    for action in actions {
        sim = SimUnderTest::apply(sim, config, action);
        SimUnderTest::check_invariants(&sim, config);
    }
    sim
}

/// Shared `SimConfig` baseline. Each regression overrides only the fields its property turns on
/// via struct-update (`SimConfig { min_replication: 3, ..base_config() }`), keeping the captured
/// configs to the dimensions that matter.
fn base_config() -> SimConfig {
    SimConfig {
        worker_count: 5,
        worker_capacity: WORKER_CAPACITY,
        min_replication: 1,
        saturation: 0.97,
        converge_is_terminal: false,
        chunk_cap: None,
        datasets: sim_datasets(),
        confirm_threshold_pct: 100,
        gc_ticks: 0,
    }
}

/// Under heterogeneous sizes, the reconcile's Phase A must place each new chunk's floor to
/// completion before the next: a chunk that fits secures its whole floor; only a chunk that
/// cannot fit is excluded. Two of the captured chunks contend for the same scarce room, and
/// `FloorLocallyFeasible` must hold.
#[test]
fn heterogeneous_sizes_converge_without_wasting_room() {
    let config = SimConfig {
        min_replication: 3,
        ..base_config()
    };

    // (seed, size, weight, dataset) for each captured chunk.
    let chunk = |seed: u64, size: u32, weight: u16, dataset: &str| {
        new_chunk((mint_key(seed), size, weight, dataset.to_string()))
    };

    replay(
        &config,
        vec![
            Action::AddChunks(vec![
                chunk(2554686395605887140, 1230244, 12, "s3://sim-2"),
                chunk(5129835052911303274, 776050, 4, "s3://sim-1"),
                chunk(4685309293357663837, 1766228, 4, "s3://sim-1"),
                chunk(2197082911667295967, 1887270, 4, "s3://sim-1"),
                chunk(8222904942282809227, 2032402, 4, "s3://sim-1"),
            ]),
            Action::AddChunks(vec![
                chunk(1273003103595798970, 994159, 4, "s3://sim-1"),
                chunk(12801469714552779976, 1084358, 4, "s3://sim-1"),
                chunk(7635383054763980352, 1959777, 1, "s3://sim-0"),
                chunk(14300246959465492681, 1809509, 4, "s3://sim-1"),
                chunk(347350420042993366, 757941, 4, "s3://sim-1"),
            ]),
            Action::AddChunks(vec![
                chunk(4512640433747941683, 890497, 1, "s3://sim-0"),
                chunk(9927853606695622086, 1343199, 1, "s3://sim-0"),
            ]),
            Action::CheckConverged(ConvergenceCheck::FloorLocallyFeasible),
        ],
    );
}

/// A light chunk's *floor* must preempt a heavy chunk's *bonus* copies: an off-ideal bonus copy is
/// dropped to free room a starved floor needs. A heavy chunk fills every disk to 90%, then a light
/// chunk arrives with no room free this cycle. `FloorLocallyFeasible` can't see this (the bonus
/// copies make every disk look full), so the replay asserts the stronger `FloorsPreemptBonuses`.
#[test]
fn heavy_chunk_bonus_starves_light_chunk_floor() {
    const MIB: u32 = 1024 * 1024;
    let config = SimConfig {
        worker_count: 3,
        ..base_config()
    };

    const HEAVY: u64 = 1;
    const LIGHT: u64 = 2;
    replay(
        &config,
        vec![
            Action::AddChunks(vec![new_chunk((
                mint_key(HEAVY),
                9 * MIB,
                12,
                "s3://sim-0".to_string(),
            ))]),
            Action::AddChunks(vec![new_chunk((
                mint_key(LIGHT),
                3 * MIB / 2,
                1,
                "s3://sim-0".to_string(),
            ))]),
            Action::CheckConverged(ConvergenceCheck::FloorsPreemptBonuses),
        ],
    );
}

/// Multi-copy generalisation of [`heavy_chunk_bonus_starves_light_chunk_floor`]: at
/// `min_replication = 2`, funding one chunk's floor requires dropping bonus copies off *two
/// distinct* workers. Captured from the flood model.
#[test]
fn heavy_bonus_starves_multicopy_light_floor() {
    const MIB: u32 = 1024 * 1024;
    let config = SimConfig {
        worker_count: 7,
        min_replication: 2,
        saturation: 0.96,
        converge_is_terminal: true,
        ..base_config()
    };

    let heavy =
        |key: u64, size: u32| new_chunk((mint_key(key), size, 12, "s3://sim-0".to_string()));
    let light = |key: u64| new_chunk((mint_key(key), MIB, 1, "s3://sim-0".to_string()));

    replay(
        &config,
        vec![
            Action::AddChunks(vec![heavy(0, 788544)]),
            Action::AddChunks(vec![heavy(1, 839705)]),
            Action::AddChunks(vec![heavy(2, 1925904)]),
            Action::AddChunks(vec![heavy(3, 657646)]),
            Action::AddChunks(vec![heavy(4, 1051823)]),
            Action::AddChunks(vec![heavy(5, 1292486)]),
            Action::AddChunks((7..=27).map(light).collect()),
            Action::CheckConverged(ConvergenceCheck::FloorsPreemptBonuses),
        ],
    );
}

/// Captured from a `pg::flood_after_max_replication_converges` failure
/// (`SIM_CASE_SEED=0387582ae5de820164ae4e80144e0fc24d5eb52b295666e57af767dd6994998f`): a harder
/// instance of what [`heavy_bonus_starves_multicopy_light_floor`] guards. Six heavy chunks fill a
/// 6-worker fleet (`min_replication = 2`) with bonus copies, then thirteen light chunks arrive whose
/// floors should preempt those bonuses; instead the scheduler reports a shortage a from-scratch
/// placement would not — incremental reconcile is weaker than scheduling from scratch.
///
/// `#[ignore]`d because the gap is unfixed and the oracle (`FloorsPreemptBonuses`) is correct: parked
/// red as a TODO — fix the algorithm, not the assertion. Un-ignore once reconcile matches from-scratch.
#[ignore = "open reconcile gap: incremental reconcile weaker than from-scratch (FloorsPreemptBonuses)"]
#[test]
fn flood_multicopy_light_floor_starved_by_heavy_bonus_unfixed() {
    let config = SimConfig {
        worker_count: 6,
        min_replication: 2,
        saturation: 0.95,
        converge_is_terminal: true,
        ..base_config()
    };
    let heavy =
        |seed: u64, size: u32| new_chunk((mint_key(seed), size, 12, "s3://sim-0".to_string()));
    let light =
        |seed: u64, size: u32| new_chunk((mint_key(seed), size, 1, "s3://sim-0".to_string()));

    replay_pg(
        &config,
        vec![
            Action::AddChunks(vec![heavy(36399, 1105773)]),
            Action::AddChunks(vec![heavy(33442, 623516)]),
            Action::AddChunks(vec![heavy(11818, 1771546)]),
            Action::AddChunks(vec![heavy(12217, 1080678)]),
            Action::AddChunks(vec![heavy(737, 1185398)]),
            Action::AddChunks(vec![heavy(37487, 2070339)]),
            Action::AddChunks(vec![
                light(64477, 1747677),
                light(5393, 1102784),
                light(24846, 1769675),
                light(18624, 1248314),
                light(50146, 1792169),
                light(32030, 2022997),
                light(41831, 2078627),
                light(62395, 1078324),
                light(41638, 1963798),
                light(63939, 2009171),
                light(20709, 593880),
                light(15157, 929558),
                light(29775, 1805314),
            ]),
            Action::CheckConverged(ConvergenceCheck::FloorsPreemptBonuses),
        ],
    );
}

/// A fired correction's whole-chunk removal (portal drop → M-window → tombstone) must fully
/// resolve before convergence is judged: under a standing shortage the old quiescence conditions
/// (ideal frozen, stale and diff queue empty) all hold while demand still includes the dropped old
/// chunk, so the shortage is transient, not genuine. The drain must keep advancing time until the
/// tombstone lands and demand shrinks back below the budget. The sequence fills to exactly the
/// budget with a correction pending, then pushes one chunk past it.
#[test]
fn correction_removal_completes_before_convergence_is_judged() {
    let config = SimConfig {
        worker_count: 4,
        saturation: 0.96,
        confirm_threshold_pct: 89,
        ..base_config()
    };

    let chunk = |seed: u64| new_chunk((mint_key(seed), CHUNK_SIZE, 1, "s3://sim-0".to_string()));

    replay(
        &config,
        vec![
            Action::AddChunks((0..37).map(chunk).collect()),
            Action::RegisterCorrection {
                old_dataset: "s3://sim-0".to_string(),
                old_chunk_id: mint_key(0),
                replacement: chunk(100),
            },
            Action::AddChunks(vec![chunk(101)]),
            Action::CheckConverged(ConvergenceCheck::FloorLocallyFeasible),
        ],
    );
}

/// Minimised from a `churn_simulation` failure (overlap injection on). A same-range correction chain
/// `A → C → D` in one dataset must collapse cleanly: with each replacement on the superseded chunk's
/// range, the chain stays on `A`'s window — disjoint from sibling `B` (in the `[0, SPAN)` overlap
/// window) — and converges, no link held back at promotion. The replacement ranges are supplied
/// explicitly here; the generator's range-inheritance that produces them is exercised by the proptest
/// churn sweeps.
#[test]
fn correction_chain_replacement_inherits_superseded_range() {
    let config = SimConfig {
        min_replication: 2,
        confirm_threshold_pct: 98,
        ..base_config()
    };
    let nc = |seed: u64, blocks: std::ops::RangeInclusive<u64>| NewChunk {
        key: mint_key(seed),
        size: CHUNK_SIZE,
        weight: 1,
        dataset: "s3://sim-2".to_string(),
        blocks,
    };
    let a_window = blocks_for_key(&mint_key(1));
    let overlap_window = blocks_for_key(&mint_key(20));

    replay(
        &config,
        vec![
            Action::AddChunks(vec![nc(1, a_window.clone()), nc(20, overlap_window)]),
            // A → C → D, every link kept on A's window.
            Action::RegisterCorrection {
                old_dataset: "s3://sim-2".to_string(),
                old_chunk_id: mint_key(1),
                replacement: nc(40, a_window.clone()),
            },
            Action::RegisterCorrection {
                old_dataset: "s3://sim-2".to_string(),
                old_chunk_id: mint_key(40),
                replacement: nc(60, a_window.clone()),
            },
            Action::CheckConverged(ConvergenceCheck::FloorLocallyFeasible),
        ],
    );
}

/// Shrunk from a `churn_simulation` failure. The number of copies required per chunk is raised from
/// 1 to 2 while the workers are already nearly full, then one worker joins and one leaves. The only
/// worker holding a particular chunk leaves before a second copy of it is placed, so that chunk
/// briefly has no copies at all. Two things are checked here.
///
/// 1. How the check treats that moment. The "holders before this cycle" snapshot is taken after the
///    departing worker is gone, so the chunk looks like it has no holders. The check must still
///    treat it as an existing chunk (it had a copy before this cycle), not a brand-new one: a new
///    chunk must appear with all its copies at once, but an existing chunk that just lost its last
///    holder may sit at zero briefly. Earlier this was mistaken for a half-finished new chunk and
///    wrongly flagged.
///
/// 2. Why the second copy isn't placed — and why this is success, not a capacity shortage. Two
///    copies of every chunk would fit if the workers started empty, so the scheduler succeeds. But
///    raising the requirement makes the copies it no longer wants start draining: they stay on disk
///    for a grace period so queries already sent to them still find data. The scheduler counts every
///    copy still on disk — wanted and draining alike — against each worker's space, finds none free
///    (here a large share of all disk space is draining copies), and cannot place the second copy
///    until that draining finishes. The joining worker adds too little space to catch up before the
///    holder leaves.
///
/// A short chunk reclaims that space in the same cycle: when a floor copy can't fit against a full
/// footprint, the Stage-2 reconcile evicts a still-adequate chunk's draining copy to make room
/// (ring-following floor-preemption), so the short chunk never drops to zero.
fn churn_raise_min_replication_underreplicates_new_chunk_case() -> (SimConfig, Vec<Action>) {
    let config = SimConfig {
        worker_count: 8,
        worker_capacity: WORKER_CAPACITY,
        min_replication: 2,
        saturation: 0.88,
        converge_is_terminal: false,
        chunk_cap: None,
        datasets: sim_datasets(),
        confirm_threshold_pct: 96,
        gc_ticks: 0,
    };

    let nc = |key: u64, weight: u16, dataset: &str| {
        new_chunk((mint_key(key), CHUNK_SIZE, weight, dataset.to_string()))
    };

    let actions = vec![
        Action::AddChunks(vec![
            nc(16954961818610243957, 1, "s3://sim-0"),
            nc(17192678508929664996, 4, "s3://sim-0"),
            nc(10410597896074326657, 12, "s3://sim-1"),
        ]),
        Action::AddChunks(vec![nc(10745604078485693560, 1, "s3://sim-0")]),
        Action::AddChunks(vec![
            nc(14225974142692056874, 4, "s3://sim-0"),
            nc(16716503399904949224, 1, "s3://sim-0"),
        ]),
        Action::AddChunks(vec![
            nc(15964094345927118657, 4, "s3://sim-2"),
            nc(10907444126988596830, 1, "s3://sim-1"),
            nc(10088558488371624923, 1, "s3://sim-0"),
            nc(11025351918916638976, 4, "s3://sim-0"),
        ]),
        Action::WorkerJoined(6),
        Action::AddChunks(vec![nc(4768630435428853058, 12, "s3://sim-0")]),
        Action::AddChunks(vec![nc(2989571896298241678, 12, "s3://sim-0")]),
        Action::WorkerJoined(10),
        Action::AddChunks(vec![
            nc(2653943195343285012, 1, "s3://sim-1"),
            nc(3919469687435077483, 1, "s3://sim-2"),
            nc(2128585714501728433, 12, "s3://sim-0"),
            nc(16195951278785098129, 4, "s3://sim-2"),
            nc(13923989993107868323, 1, "s3://sim-1"),
        ]),
        Action::AddChunks(vec![
            nc(7383991124465855767, 4, "s3://sim-0"),
            nc(13124044741015883640, 4, "s3://sim-0"),
            nc(14381497883753118542, 1, "s3://sim-0"),
            nc(8603037258648407345, 12, "s3://sim-0"),
        ]),
        Action::AddChunks(vec![
            nc(1852091475659358006, 1, "s3://sim-1"),
            nc(4768812022606210322, 1, "s3://sim-1"),
            nc(13154924922703163528, 4, "s3://sim-0"),
        ]),
        Action::AddChunks(vec![nc(3032749468128534043, 1, "s3://sim-0")]),
        Action::AddChunks(vec![
            nc(15772499366711579397, 12, "s3://sim-0"),
            nc(15015764490999709691, 12, "s3://sim-2"),
            nc(15896881262948160117, 12, "s3://sim-2"),
            nc(2983592671753481994, 12, "s3://sim-2"),
        ]),
        Action::AddChunks(vec![
            nc(14222294236544597556, 4, "s3://sim-0"),
            nc(6495960531106175075, 4, "s3://sim-2"),
            nc(3326128349095418881, 1, "s3://sim-0"),
        ]),
        Action::AddChunks(vec![
            nc(2915024374768891974, 12, "s3://sim-1"),
            nc(2484565404766071878, 12, "s3://sim-2"),
            nc(7152088397016466779, 4, "s3://sim-0"),
            nc(4790796835249161539, 12, "s3://sim-0"),
            nc(17661028380861810705, 1, "s3://sim-2"),
        ]),
        Action::SetMinReplication(1),
        Action::CheckConverged(ConvergenceCheck::FloorLocallyFeasible),
        Action::SetMinReplication(2),
        Action::WorkerJoined(9),
        Action::WorkerLeft(2),
    ];
    (config, actions)
}

#[test]
fn churn_raise_min_replication_underreplicates_new_chunk() {
    let (config, actions) = churn_raise_min_replication_underreplicates_new_chunk_case();
    replay(&config, actions);
}

/// Postgres twin, for backend parity on the floor-preemption fix.
#[test]
fn churn_raise_min_replication_underreplicates_new_chunk_pg() {
    let (config, actions) = churn_raise_min_replication_underreplicates_new_chunk_case();
    replay_pg(&config, actions);
}

/// Regression: a tombstoned chunk's stale mappings must be dropped, else they keep counting in
/// `ideal ∪ stale` and overcommit a worker (11 copies on a 10-copy worker). A correction's old
/// chunk is removed and its stale can't drain (its superseded assignment is never confirmed), so it
/// must be reaped at tombstone — Postgres lacked this; the in-memory oracle always had it. The
/// replay panics iff a worker overcommits.
/// SIM_CASE_SEED=86415892433a952109298d1aec73e1da062112c371412cf3f0d3f9f88151cf94
#[test]
fn pg_guided_overcommit_capture_pg() {
    use crate::scheduler_storage::postgres::PostgresStorage;

    let config = SimConfig {
        worker_count: 3,
        worker_capacity: WORKER_CAPACITY,
        min_replication: 3,
        saturation: 0.87,
        converge_is_terminal: false,
        chunk_cap: None,
        datasets: sim_datasets(),
        confirm_threshold_pct: 100,
        gc_ticks: 0,
    };
    let nc = |key: u64, weight: u16, dataset: &str| NewChunk {
        key: mint_key(key),
        size: CHUNK_SIZE,
        weight,
        dataset: dataset.to_string(),
        blocks: blocks_for_key(&mint_key(key)),
    };
    let actions = vec![
        Action::AddChunks(vec![
            nc(53966, 1, "s3://sim-0"),
            nc(54515, 12, "s3://sim-0"),
            nc(61061, 12, "s3://sim-0"),
        ]),
        Action::AddChunks(vec![
            nc(25967, 12, "s3://sim-1"),
            nc(14764, 4, "s3://sim-0"),
            nc(37179, 1, "s3://sim-2"),
            nc(26716, 12, "s3://sim-1"),
            nc(551, 4, "s3://sim-0"),
        ]),
        Action::SetMinReplication(1),
        // Same-range replacement: it inherits the old chunk's window (26716), not its own key's.
        Action::RegisterCorrection {
            old_dataset: "s3://sim-1".to_string(),
            old_chunk_id: mint_key(26716),
            replacement: NewChunk {
                key: mint_key(21338),
                size: CHUNK_SIZE,
                weight: 1,
                dataset: "s3://sim-1".to_string(),
                blocks: blocks_for_key(&mint_key(26716)),
            },
        },
        Action::WorkerFetchAssignment {
            worker: 0,
            succeeds: true,
        },
        Action::WorkerFetchAssignment {
            worker: 2,
            succeeds: true,
        },
        Action::AddChunks(vec![nc(16662, 12, "s3://sim-1")]),
        Action::WorkerFetchAssignment {
            worker: 1,
            succeeds: true,
        },
        Action::AddChunks(vec![
            nc(34275, 4, "s3://sim-1"),
            nc(5751, 12, "s3://sim-2"),
            nc(40454, 4, "s3://sim-1"),
        ]),
        Action::AdvanceClock(7),
    ];

    let mut sim = SimUnderTest::<PostgresStorage>::init_test(&config);
    for action in actions {
        sim = SimUnderTest::apply(sim, &config, action);
        SimUnderTest::check_invariants(&sim, &config);
    }
}

/// Regression: worker GC must delete a worker's stale rows with it. Any stale row outliving its
/// worker trips the in-memory integrity oracle, and on Postgres the
/// `sched_stale_mappings_worker_id_fkey` FK aborts the update.
fn worker_gc_orphans_post_departure_stale_rows_case() -> (SimConfig, Vec<Action>) {
    let config = SimConfig {
        worker_count: 4,
        min_replication: 4,
        ..base_config()
    };
    let nc = |key: u64, weight: u16| {
        new_chunk((mint_key(key), CHUNK_SIZE, weight, "s3://sim-0".to_string()))
    };

    let actions = vec![
        // Floor 4 of 4 workers — the leaver is a holder.
        Action::AddChunks(vec![nc(1, 1)]),
        Action::WorkerLeft(0),
        Action::SetMinReplication(1),
        // Heavy adds reshuffle pairs off the departed worker; still registered, it passes
        // the mint filter and collects stale rows.
        Action::AddChunks(vec![nc(2, 12), nc(3, 12)]),
        // This sync GCs the departed worker under its stale rows.
        Action::WorkerJoined(4),
    ];
    (config, actions)
}

#[test]
fn worker_gc_orphans_post_departure_stale_rows() {
    let (config, actions) = worker_gc_orphans_post_departure_stale_rows_case();
    replay(&config, actions);
}

/// Postgres twin: the worker delete must cascade into `sched_stale_mappings`, and the integrity
/// oracle verifies no row survives it.
#[test]
fn worker_gc_orphans_post_departure_stale_rows_pg() {
    let (config, actions) = worker_gc_orphans_post_departure_stale_rows_case();
    replay_pg(&config, actions);
}

/// Regression: reshuffling around a departed worker must not overcommit a survivor — `ideal ∪
/// stale` once reached 11 MiB on a 10 MiB worker, from held copies being charged to the wrong
/// workers after the fleet reorder. Also reproduces on
/// Postgres (`pg_draining_after_churn_reaches_a_fixed_point`). Shrunk from
/// `SIM_CASE_SEED=7b66a64870d2e68ab587e5643db307655cba96d1fdc558e77425dd6a5e7ff9a1`.
///
/// The overcommit was the worker-reorder position bug, now fixed at the source (#53):
/// `partition_reliable` translates every placement view into the reordered position space, so each
/// held copy still charges the worker that actually holds it.
#[test]
fn departure_reshuffle_overcommits_a_survivor() {
    let config = SimConfig {
        worker_count: 6,
        min_replication: 3,
        saturation: 0.94,
        confirm_threshold_pct: 86,
        gc_ticks: 15,
        ..base_config()
    };
    let nc = |key: u64, weight: u16, dataset: &str| {
        new_chunk((mint_key(key), CHUNK_SIZE, weight, dataset.to_string()))
    };

    replay(
        &config,
        vec![
            Action::AddChunks(vec![
                nc(18283, 1, "s3://sim-2"),
                nc(32929, 12, "s3://sim-1"),
                nc(23095, 4, "s3://sim-2"),
                nc(58340, 12, "s3://sim-0"),
            ]),
            Action::AddChunks(vec![nc(50533, 1, "s3://sim-0")]),
            Action::AddChunks(vec![
                nc(1042, 1, "s3://sim-1"),
                nc(11774, 12, "s3://sim-1"),
                nc(29750, 12, "s3://sim-0"),
                nc(32802, 4, "s3://sim-0"),
                nc(12729, 12, "s3://sim-1"),
            ]),
            Action::WorkerLeft(4),
            Action::AddChunks(vec![nc(36932, 1, "s3://sim-0")]),
        ],
    );
}

/// Regression: a departed worker's lingering row must not stall the drain probe. GC waits for a
/// membership sync and the drain triggers none, so the row survives the whole probe; while
/// departed workers were still scheduled onto, the ideal oscillated around it and the probe
/// exhausted its 200-cycle budget. Shrunk by the churn walk.
///
/// Parked for the same reason as [`departure_reshuffle_overcommits_a_survivor`]: the oscillation
/// was the reorder bug, fixed in a separate PR and masked here by the departed-worker filter.
#[ignore = "multistep-scheduler-fix-required"]
#[test]
fn departed_worker_prevents_quiescence() {
    let config = SimConfig {
        worker_count: 6,
        min_replication: 2,
        saturation: 0.73,
        confirm_threshold_pct: 88,
        gc_ticks: 5,
        ..base_config()
    };
    let nc = |key: u64, weight: u16, dataset: &str| {
        new_chunk((mint_key(key), CHUNK_SIZE, weight, dataset.to_string()))
    };

    replay(
        &config,
        vec![
            Action::AddChunks(vec![
                nc(36144, 4, "s3://sim-1"),
                nc(3220, 4, "s3://sim-1"),
                nc(28452, 1, "s3://sim-2"),
            ]),
            Action::SetMinReplication(3),
            Action::WorkerJoined(8),
            Action::AddChunks(vec![
                nc(56552, 1, "s3://sim-2"),
                nc(58127, 4, "s3://sim-0"),
                nc(65478, 12, "s3://sim-0"),
                nc(62366, 4, "s3://sim-0"),
            ]),
            Action::WorkerJoined(7),
            Action::SetMinReplication(1),
            Action::WorkerLeft(7),
            Action::CheckConverged(ConvergenceCheck::FloorLocallyFeasible),
        ],
    );
}

/// Regression: the stale mint must not create a row for a worker that is gone. Worker GC deletes
/// the worker's row (taking its stale rows along), but its pairs are still in the ideal; when a
/// later cycle drops such a pair, the in-memory mint records it as a stale mapping without
/// checking that the worker is still registered — so the new row points at a deleted worker and
/// can never drain. In-memory only: the Postgres mint filters these out with
/// `EXISTS (SELECT 1 FROM sched_workers …)`. Shrunk by the churn walk.
#[test]
fn stale_minted_for_a_gc_deleted_worker() {
    let config = SimConfig {
        min_replication: 2,
        saturation: 0.7,
        gc_ticks: 5,
        ..base_config()
    };

    replay(
        &config,
        vec![
            Action::AddChunks(vec![new_chunk((
                mint_key(13105),
                CHUNK_SIZE,
                1,
                "s3://sim-0".to_string(),
            ))]),
            Action::WorkerLeft(0),
            // Draining advances the clock past the retention; the departed worker keeps its
            // ideal pairs (nothing filters it) until the join's sync GC-deletes the row.
            Action::CheckConverged(ConvergenceCheck::FloorLocallyFeasible),
            Action::WorkerJoined(8),
            // The reshuffle drops the deleted worker's pair from the ideal — and the mint
            // creates a stale row for a worker that no longer exists.
            Action::SetMinReplication(1),
        ],
    );
}

/// Regression: routing that names a departed worker is tolerated until the assignment that dropped
/// it confirms. Nothing retracts routing at departure by design — the scheduler drops the worker
/// from the ideal and the routing follows on confirmation — so a worker that departs and rejoins
/// empty is unaccountable for the routing meanwhile. Guards the oracles against tightening onto
/// that window. Needs no worker GC (`gc_ticks: NEVER_GC_TICKS`). Shrunk by the churn walk.
#[test]
fn portal_routes_to_a_rejoined_worker_with_empty_disk() {
    let config = SimConfig {
        worker_count: 8,
        min_replication: 3,
        saturation: 0.82,
        gc_ticks: NEVER_GC_TICKS,
        ..base_config()
    };
    let nc = |key: u64, weight: u16, dataset: &str| {
        new_chunk((mint_key(key), CHUNK_SIZE, weight, dataset.to_string()))
    };

    replay(
        &config,
        vec![
            Action::AddChunks(vec![nc(7835, 12, "s3://sim-0"), nc(50770, 4, "s3://sim-0")]),
            Action::AddChunks(vec![
                nc(54949, 12, "s3://sim-0"),
                nc(27489, 12, "s3://sim-2"),
                nc(49688, 4, "s3://sim-2"),
                nc(7112, 1, "s3://sim-0"),
            ]),
            Action::AddChunks(vec![
                nc(24126, 4, "s3://sim-2"),
                nc(41662, 4, "s3://sim-0"),
                nc(2559, 12, "s3://sim-2"),
                nc(4623, 4, "s3://sim-0"),
            ]),
            Action::AddChunks(vec![
                nc(20657, 12, "s3://sim-2"),
                nc(12105, 4, "s3://sim-0"),
                nc(34911, 4, "s3://sim-0"),
                nc(20366, 12, "s3://sim-1"),
                nc(4873, 4, "s3://sim-2"),
            ]),
            Action::AddChunks(vec![
                nc(24310, 12, "s3://sim-0"),
                nc(12952, 1, "s3://sim-0"),
                nc(38253, 12, "s3://sim-1"),
            ]),
            Action::SetMinReplication(1),
            Action::CheckConverged(ConvergenceCheck::FloorLocallyFeasible),
            Action::WorkerLeft(4),
            Action::WorkerJoined(4),
        ],
    );
}

/// Regression: diffs queued before a departure still name the worker and replay into the routing
/// as confirmation advances — routing converges through the diff stream, not through a departure
/// hook. So the published assignment legitimately stops covering what the routing claims until the
/// post-departure assignment confirms. Guards `published_coverage`'s departure scoping against
/// tightening onto that window. Shrunk by the churn walk.
#[test]
fn diff_replay_carries_a_departed_workers_routing() {
    let config = SimConfig {
        worker_count: 6,
        min_replication: 4,
        saturation: 0.83,
        gc_ticks: 5,
        ..base_config()
    };
    let nc = |key: u64, weight: u16, dataset: &str| {
        new_chunk((mint_key(key), CHUNK_SIZE, weight, dataset.to_string()))
    };

    replay(
        &config,
        vec![
            Action::AddChunks(vec![
                nc(45634, 12, "s3://sim-0"),
                nc(55285, 12, "s3://sim-2"),
            ]),
            Action::AddChunks(vec![
                nc(14498, 12, "s3://sim-1"),
                nc(62007, 12, "s3://sim-2"),
                nc(30482, 1, "s3://sim-2"),
                nc(53011, 4, "s3://sim-2"),
                nc(47973, 12, "s3://sim-1"),
            ]),
            Action::WorkerLeft(5),
            Action::AddChunks(vec![
                nc(64995, 12, "s3://sim-1"),
                nc(62008, 1, "s3://sim-0"),
                nc(5958, 12, "s3://sim-2"),
                nc(22807, 4, "s3://sim-0"),
            ]),
            Action::CheckConverged(ConvergenceCheck::FloorLocallyFeasible),
            Action::SetMinReplication(3),
            Action::WorkerJoined(5),
        ],
    );
}

/// Regression: a portal polling right after a departure is handed routing that still names the
/// departed worker — freshly fetched, so no staleness tolerance applies — and a rejoin makes those
/// routings resolve to an empty worker. Expected: portals act on published assignments, and the
/// routing only sheds the worker once the assignment that dropped it confirms — republication
/// alone is not the boundary, despite this test's name. Shrunk by the churn walk.
#[test]
fn portal_fetches_pre_departure_routing_before_republication() {
    let config = SimConfig {
        worker_count: 7,
        min_replication: 3,
        saturation: 0.75,
        ..base_config()
    };
    let nc = |key: u64, weight: u16, dataset: &str| {
        new_chunk((mint_key(key), CHUNK_SIZE, weight, dataset.to_string()))
    };

    replay(
        &config,
        vec![
            Action::AddChunks(vec![
                nc(26891, 12, "s3://sim-1"),
                nc(41709, 12, "s3://sim-2"),
                nc(4634, 1, "s3://sim-2"),
                nc(51215, 1, "s3://sim-0"),
            ]),
            Action::AddChunks(vec![
                nc(48172, 12, "s3://sim-0"),
                nc(32642, 12, "s3://sim-2"),
                nc(26162, 12, "s3://sim-0"),
                nc(29040, 1, "s3://sim-2"),
                nc(31984, 12, "s3://sim-1"),
            ]),
            Action::AddChunks(vec![
                nc(37817, 12, "s3://sim-0"),
                nc(60836, 1, "s3://sim-2"),
            ]),
            Action::AddChunks(vec![
                nc(42276, 4, "s3://sim-2"),
                nc(31621, 4, "s3://sim-2"),
                nc(21452, 4, "s3://sim-1"),
                nc(37254, 4, "s3://sim-0"),
            ]),
            Action::SetMinReplication(1),
            Action::CheckConverged(ConvergenceCheck::FloorLocallyFeasible),
            Action::WorkerLeft(2),
            Action::PortalFetchAssignment { succeeds: true },
            Action::WorkerJoined(2),
        ],
    );
}

/// Regression: a departure can leave a portal-visible chunk with no holders at all. A departed
/// worker's copies vanish rather than drain, so when it held the last copy the chunk would drop out
/// of `ideal ∪ stale` — and the published worker assignment, built from that union, would no longer
/// name it. The schema bundle is derived from that assignment, so the still-visible chunk's schema
/// would be missing from it and no client could resolve the chunk's file set.
///
/// The fix: with the surviving fleet momentarily full of draining copies, the Stage-2 reconcile
/// evicts one of those drains to re-floor the holderless chunk the same cycle. It stays in the
/// worker assignment and the bundle, and the schema-bundle oracle passes.
///
/// This state was reached on roughly half of full sim runs. Replay with
/// `SIM_CASE_SEED=348400ec6469d83ffc977f3d665b3b409cd539204649e174bf81b593be5e7bc5`
/// or `SIM_CASE_SEED=8d1b004d7735fecc177bf21a4f134a8c8a1c50455714a0d9cf0f0f9cee324da6` against
/// `in_memory::churn_simulation_case`.
fn departure_leaves_a_visible_chunk_holderless_case() -> (SimConfig, Vec<Action>) {
    let config = SimConfig {
        worker_count: 4,
        min_replication: 3,
        saturation: 0.96,
        confirm_threshold_pct: 72,
        gc_ticks: 15,
        ..base_config()
    };
    let nc = |key: u64, weight: u16, dataset: &str| {
        new_chunk((mint_key(key), CHUNK_SIZE, weight, dataset.to_string()))
    };

    let actions = vec![
        Action::AddChunks(vec![
            nc(11160, 1, "s3://sim-1"),
            nc(8656, 1, "s3://sim-2"),
            nc(39851, 12, "s3://sim-0"),
        ]),
        // A second schema for sim-1: chunks added after this carry a schema id the bundle must
        // still carry when the portal names them.
        Action::SetDatasetSchema {
            dataset: "s3://sim-1".to_string(),
            schema: SCHEMA_POOL[1].clone(),
        },
        Action::AddChunks(vec![nc(32862, 12, "s3://sim-1"), nc(5382, 1, "s3://sim-0")]),
        Action::WorkerJoined(7),
        Action::AddChunks(vec![nc(59633, 4, "s3://sim-1")]),
        Action::AddChunks(vec![
            nc(59055, 4, "s3://sim-1"),
            nc(27815, 12, "s3://sim-0"),
            nc(15889, 12, "s3://sim-0"),
        ]),
        Action::SetMinReplication(1),
        Action::CheckConverged(ConvergenceCheck::FloorLocallyFeasible),
        Action::AddChunks(vec![
            nc(39552, 12, "s3://sim-2"),
            nc(19658, 12, "s3://sim-2"),
            nc(40207, 1, "s3://sim-0"),
        ]),
        Action::AddChunks(vec![
            nc(38657, 12, "s3://sim-0"),
            nc(45711, 12, "s3://sim-2"),
            nc(24142, 1, "s3://sim-1"),
            nc(41432, 4, "s3://sim-1"),
        ]),
        // The departure takes the last copy of an already-visible chunk with it.
        Action::WorkerLeft(7),
        Action::AddChunks(vec![nc(65416, 1, "s3://sim-0")]),
    ];
    (config, actions)
}

#[test]
fn departure_leaves_a_visible_chunk_holderless() {
    let (config, actions) = departure_leaves_a_visible_chunk_holderless_case();
    let sut = replay(&config, actions);
    // ADR 0001 sentinel: the departure-emptied chunk is routed only at the departed worker, which
    // the strand oracles deliberately exempt — so without this probe, deleting the preemption
    // mechanism would leave this replay green. The probe demands what preemption guarantees: after
    // the committed cycle, every routed chunk has an active listed holder again.
    sut.assert_all_routed_chunks_have_a_listed_holder();
}

/// Postgres twin: exercises the eviction delete on the real backend (populated
/// `sched_stale_mappings` DELETE), keeping the two storages at parity on the fix.
#[test]
fn departure_leaves_a_visible_chunk_holderless_pg() {
    let (config, actions) = departure_leaves_a_visible_chunk_holderless_case();
    let sut = replay_pg(&config, actions);
    sut.assert_all_routed_chunks_have_a_listed_holder();
}

/// Captured by the in-memory `churn_simulation`
/// (`SIM_CASE_SEED=e675aea88480f91a54c61dfbc688fbc30d1bac09e4df5a5753c3eb47ba8bb540`). After a floor
/// drop (4→3) and three worker joins, floor-preemption eviction removes the copies covering a
/// weight-1 chunk's (`ChunkPk(1)`) confirmed routing while confirmation lags, so the portal
/// momentarily routes to workers that hold nothing.
///
/// Resolved by the durability-hard / routing-best-effort design: eviction keeps the chunk's committed
/// durability floor (so it stays covered by holders elsewhere), and `published_coverage` no longer
/// fires on a stale route to an evicted copy while the chunk is covered somewhere — that is bounded
/// routing-lag, not a strand. The chunk here is covered elsewhere, so the capture now passes.
#[test]
fn churn_holderless_chunk_strands_portal_routing() {
    let config = SimConfig {
        worker_count: 4,
        min_replication: 4,
        saturation: 0.8,
        gc_ticks: 15,
        ..base_config()
    };
    let nc = |key: u64, weight: u16, dataset: &str| {
        new_chunk((mint_key(key), CHUNK_SIZE, weight, dataset.to_string()))
    };

    replay(
        &config,
        vec![
            Action::AddChunks(vec![
                nc(62123, 1, "s3://sim-1"),
                nc(21247, 4, "s3://sim-0"),
                nc(31989, 4, "s3://sim-0"),
                nc(6557, 1, "s3://sim-1"),
            ]),
            Action::AddChunks(vec![
                nc(24514, 12, "s3://sim-2"),
                nc(43084, 1, "s3://sim-0"),
            ]),
            Action::AddChunks(vec![nc(45588, 1, "s3://sim-0")]),
            Action::AddChunks(vec![
                nc(41443, 12, "s3://sim-0"),
                nc(52150, 12, "s3://sim-1"),
                nc(50962, 12, "s3://sim-1"),
            ]),
            Action::CheckConverged(ConvergenceCheck::FloorLocallyFeasible),
            Action::WorkerJoined(4),
            Action::WorkerJoined(5),
            Action::WorkerJoined(7),
            Action::SetMinReplication(3),
            Action::AddChunks(vec![
                nc(17316, 4, "s3://sim-0"),
                nc(4991, 12, "s3://sim-0"),
                nc(8828, 4, "s3://sim-0"),
                nc(2498, 4, "s3://sim-0"),
                nc(21382, 12, "s3://sim-1"),
            ]),
        ],
    );
}

/// The schema bundle must resolve every chunk the portal can route. A chunk enters the ideal, a later
/// assignment drops it, then it promotes to portal-visible off its earlier confirmed entry — so the
/// portal routes it after the current assignment no longer holds it; a `SetDatasetSchema` made that
/// chunk's schema unique, so dropping it from the ideal would drop the only copy of that schema, and a
/// sustained shortage freezes the bundle so it never self-heals. The bundle covers a chunk's schema for
/// its whole routable lifetime (`entered_worker_assignment ∧ ¬tombstoned`, ADR 0002), so the schema
/// stays resolvable. Delta-debugged from the failing walk (proptest over-shrank it).
fn shortage_schema_bundle_misses_in_flight_chunk_case() -> (SimConfig, Vec<Action>) {
    let config = SimConfig {
        worker_count: 8,
        min_replication: 3,
        saturation: 0.96,
        confirm_threshold_pct: 92,
        gc_ticks: NEVER_GC_TICKS,
        ..base_config()
    };
    let actions = vec![
        Action::AddChunks(vec![
            new_chunk((
                "00000000000000062822".into(),
                1048576,
                4,
                "s3://sim-1".into(),
            )),
            new_chunk((
                "00000000000000008667".into(),
                1048576,
                1,
                "s3://sim-1".into(),
            )),
            new_chunk((
                "00000000000000050543".into(),
                1048576,
                12,
                "s3://sim-2".into(),
            )),
        ]),
        Action::AddChunks(vec![
            new_chunk((
                "00000000000000006631".into(),
                1048576,
                1,
                "s3://sim-2".into(),
            )),
            new_chunk((
                "00000000000000005017".into(),
                1048576,
                4,
                "s3://sim-0".into(),
            )),
            new_chunk((
                "00000000000000057041".into(),
                1048576,
                1,
                "s3://sim-0".into(),
            )),
            new_chunk((
                "00000000000000005596".into(),
                1048576,
                12,
                "s3://sim-2".into(),
            )),
        ]),
        Action::WorkerJoined(9),
        Action::WorkerJoined(11),
        Action::AddChunks(vec![
            new_chunk((
                "00000000000000050796".into(),
                1048576,
                1,
                "s3://sim-2".into(),
            )),
            new_chunk((
                "00000000000000034259".into(),
                1048576,
                12,
                "s3://sim-2".into(),
            )),
            new_chunk((
                "00000000000000001758".into(),
                1048576,
                1,
                "s3://sim-2".into(),
            )),
            new_chunk((
                "00000000000000037011".into(),
                1048576,
                1,
                "s3://sim-2".into(),
            )),
            new_chunk((
                "00000000000000064296".into(),
                1048576,
                12,
                "s3://sim-0".into(),
            )),
        ]),
        Action::AddChunks(vec![
            new_chunk((
                "00000000000000056401".into(),
                1048576,
                12,
                "s3://sim-1".into(),
            )),
            new_chunk((
                "00000000000000046547".into(),
                1048576,
                12,
                "s3://sim-2".into(),
            )),
            new_chunk((
                "00000000000000038358".into(),
                1048576,
                1,
                "s3://sim-0".into(),
            )),
            new_chunk((
                "00000000000000025072".into(),
                1048576,
                12,
                "s3://sim-2".into(),
            )),
            new_chunk((
                "00000000000000024018".into(),
                1048576,
                4,
                "s3://sim-1".into(),
            )),
        ]),
        Action::AddChunks(vec![
            new_chunk((
                "00000000000000015917".into(),
                1048576,
                4,
                "s3://sim-2".into(),
            )),
            new_chunk((
                "00000000000000043358".into(),
                1048576,
                1,
                "s3://sim-1".into(),
            )),
            new_chunk((
                "00000000000000058239".into(),
                1048576,
                1,
                "s3://sim-0".into(),
            )),
            new_chunk((
                "00000000000000017204".into(),
                1048576,
                1,
                "s3://sim-2".into(),
            )),
            new_chunk((
                "00000000000000000220".into(),
                1048576,
                12,
                "s3://sim-0".into(),
            )),
        ]),
        Action::WorkerJoined(10),
        Action::AddChunks(vec![
            new_chunk((
                "00000000000000012800".into(),
                1048576,
                4,
                "s3://sim-2".into(),
            )),
            new_chunk((
                "00000000000000030433".into(),
                1048576,
                4,
                "s3://sim-1".into(),
            )),
            new_chunk((
                "00000000000000016078".into(),
                1048576,
                4,
                "s3://sim-2".into(),
            )),
            new_chunk((
                "00000000000000029109".into(),
                1048576,
                12,
                "s3://sim-2".into(),
            )),
            new_chunk((
                "00000000000000060512".into(),
                1048576,
                4,
                "s3://sim-2".into(),
            )),
        ]),
        Action::AddChunks(vec![
            new_chunk((
                "00000000000000031243".into(),
                1048576,
                1,
                "s3://sim-2".into(),
            )),
            new_chunk((
                "00000000000000031398".into(),
                1048576,
                12,
                "s3://sim-0".into(),
            )),
            new_chunk((
                "00000000000000024095".into(),
                1048576,
                12,
                "s3://sim-1".into(),
            )),
            new_chunk((
                "00000000000000061110".into(),
                1048576,
                12,
                "s3://sim-1".into(),
            )),
        ]),
        Action::AddChunks(vec![
            new_chunk((
                "00000000000000062076".into(),
                1048576,
                4,
                "s3://sim-0".into(),
            )),
            new_chunk((
                "00000000000000048527".into(),
                1048576,
                12,
                "s3://sim-2".into(),
            )),
        ]),
        Action::AddChunks(vec![
            new_chunk((
                "00000000000000059142".into(),
                1048576,
                12,
                "s3://sim-2".into(),
            )),
            new_chunk((
                "00000000000000017311".into(),
                1048576,
                1,
                "s3://sim-1".into(),
            )),
            new_chunk((
                "00000000000000040615".into(),
                1048576,
                1,
                "s3://sim-2".into(),
            )),
            new_chunk((
                "00000000000000003569".into(),
                1048576,
                1,
                "s3://sim-0".into(),
            )),
            new_chunk((
                "00000000000000047652".into(),
                1048576,
                4,
                "s3://sim-1".into(),
            )),
        ]),
        Action::SetMinReplication(2),
        Action::AddChunks(vec![new_chunk((
            "00000000000000007424".into(),
            1048576,
            1,
            "s3://sim-0".into(),
        ))]),
        Action::AddChunks(vec![
            new_chunk((
                "00000000000000055665".into(),
                1048576,
                12,
                "s3://sim-2".into(),
            )),
            new_chunk((
                "00000000000000005210".into(),
                1048576,
                12,
                "s3://sim-2".into(),
            )),
        ]),
        Action::AddChunks(vec![
            new_chunk((
                "00000000000000001122".into(),
                1048576,
                12,
                "s3://sim-2".into(),
            )),
            new_chunk((
                "00000000000000037710".into(),
                1048576,
                4,
                "s3://sim-2".into(),
            )),
            new_chunk((
                "00000000000000041327".into(),
                1048576,
                12,
                "s3://sim-0".into(),
            )),
        ]),
        Action::AddChunks(vec![
            new_chunk((
                "00000000000000065227".into(),
                1048576,
                12,
                "s3://sim-2".into(),
            )),
            new_chunk((
                "00000000000000020805".into(),
                1048576,
                1,
                "s3://sim-2".into(),
            )),
            new_chunk((
                "00000000000000065148".into(),
                1048576,
                1,
                "s3://sim-2".into(),
            )),
            new_chunk((
                "00000000000000027132".into(),
                1048576,
                4,
                "s3://sim-2".into(),
            )),
        ]),
        Action::AddChunks(vec![
            new_chunk((
                "00000000000000019312".into(),
                1048576,
                1,
                "s3://sim-2".into(),
            )),
            new_chunk((
                "00000000000000006606".into(),
                1048576,
                4,
                "s3://sim-2".into(),
            )),
            new_chunk((
                "00000000000000003645".into(),
                1048576,
                1,
                "s3://sim-2".into(),
            )),
            new_chunk((
                "00000000000000031802".into(),
                1048576,
                12,
                "s3://sim-0".into(),
            )),
        ]),
        Action::SetDatasetSchema {
            dataset: "s3://sim-1".into(),
            schema: SCHEMA_POOL[1].clone(),
        },
        Action::CheckConverged(ConvergenceCheck::FloorLocallyFeasible),
        Action::SetMinReplication(1),
        Action::AddChunks(vec![
            new_chunk((
                "00000000000000052877".into(),
                1048576,
                4,
                "s3://sim-0".into(),
            )),
            new_chunk((
                "00000000000000022506".into(),
                1048576,
                4,
                "s3://sim-2".into(),
            )),
            new_chunk((
                "00000000000000007926".into(),
                1048576,
                12,
                "s3://sim-1".into(),
            )),
            new_chunk((
                "00000000000000032987".into(),
                1048576,
                12,
                "s3://sim-0".into(),
            )),
            new_chunk((
                "00000000000000044551".into(),
                1048576,
                1,
                "s3://sim-1".into(),
            )),
        ]),
        Action::WorkerLeft(4),
        Action::AdvanceClock(4),
        Action::SetMinReplication(2),
        Action::CheckConverged(ConvergenceCheck::FloorLocallyFeasible),
    ];
    (config, actions)
}

#[test]
fn shortage_schema_bundle_misses_in_flight_chunk() {
    let (config, actions) = shortage_schema_bundle_misses_in_flight_chunk_case();
    replay(&config, actions);
}

/// Postgres twin: the same case on the real backend, keeping the bundle fix at parity across storages.
#[test]
fn shortage_schema_bundle_misses_in_flight_chunk_pg() {
    let (config, actions) = shortage_schema_bundle_misses_in_flight_chunk_case();
    replay_pg(&config, actions);
}

/// A departed-then-rejoined worker (`WorkerLeft`/`WorkerJoined` on the same index, same pk) covers the
/// chunks it is re-placed for. Its departure records a `placed_until` bound so
/// [`routing_has_caught_up_with_departure`](super::sut::SimUnderTest::routing_has_caught_up_with_departure)
/// can excuse stale routing to a departed worker; that bound must clear on rejoin, or the coverage
/// oracle keeps treating the now-active holder as an unscrubbed departed route and flags a false strand.
#[test]
fn rejoined_worker_covers_its_chunks() {
    let config = SimConfig {
        worker_count: 7,
        min_replication: 2,
        saturation: 0.85,
        confirm_threshold_pct: 93,
        gc_ticks: 15,
        ..base_config()
    };
    replay(
        &config,
        vec![
            Action::AddChunks(vec![
                new_chunk((
                    "00000000000000051262".into(),
                    1048576,
                    12,
                    "s3://sim-1".into(),
                )),
                new_chunk((
                    "00000000000000019230".into(),
                    1048576,
                    12,
                    "s3://sim-0".into(),
                )),
                new_chunk((
                    "00000000000000040079".into(),
                    1048576,
                    12,
                    "s3://sim-0".into(),
                )),
                new_chunk((
                    "00000000000000035074".into(),
                    1048576,
                    12,
                    "s3://sim-1".into(),
                )),
                new_chunk((
                    "00000000000000036465".into(),
                    1048576,
                    12,
                    "s3://sim-2".into(),
                )),
            ]),
            Action::SetMinReplication(1),
            Action::AddChunks(vec![
                new_chunk((
                    "00000000000000048988".into(),
                    1048576,
                    12,
                    "s3://sim-1".into(),
                )),
                new_chunk((
                    "00000000000000011031".into(),
                    1048576,
                    12,
                    "s3://sim-1".into(),
                )),
                new_chunk((
                    "00000000000000023450".into(),
                    1048576,
                    1,
                    "s3://sim-1".into(),
                )),
                new_chunk((
                    "00000000000000048788".into(),
                    1048576,
                    1,
                    "s3://sim-1".into(),
                )),
            ]),
            Action::WorkerFetchAssignment {
                worker: 1,
                succeeds: true,
            },
            Action::AddChunks(vec![
                new_chunk((
                    "00000000000000046781".into(),
                    1048576,
                    4,
                    "s3://sim-1".into(),
                )),
                new_chunk((
                    "00000000000000022076".into(),
                    1048576,
                    4,
                    "s3://sim-1".into(),
                )),
            ]),
            Action::WorkerLeft(2),
            Action::WorkerJoined(2),
            Action::AddChunks(vec![
                new_chunk((
                    "00000000000000009497".into(),
                    1048576,
                    4,
                    "s3://sim-0".into(),
                )),
                new_chunk((
                    "00000000000000001876".into(),
                    1048576,
                    1,
                    "s3://sim-1".into(),
                )),
                new_chunk((
                    "00000000000000047202".into(),
                    1048576,
                    12,
                    "s3://sim-2".into(),
                )),
            ]),
            Action::AddChunks(vec![
                new_chunk((
                    "00000000000000063259".into(),
                    1048576,
                    12,
                    "s3://sim-1".into(),
                )),
                new_chunk((
                    "00000000000000033532".into(),
                    1048576,
                    12,
                    "s3://sim-1".into(),
                )),
            ]),
            Action::AddChunks(vec![new_chunk((
                "00000000000000006850".into(),
                1048576,
                4,
                "s3://sim-0".into(),
            ))]),
            Action::WorkerFetchAssignment {
                worker: 3,
                succeeds: true,
            },
            Action::WorkerFetchAssignment {
                worker: 4,
                succeeds: true,
            },
            Action::AddChunks(vec![
                new_chunk((
                    "00000000000000021701".into(),
                    1048576,
                    4,
                    "s3://sim-0".into(),
                )),
                new_chunk((
                    "00000000000000023690".into(),
                    1048576,
                    12,
                    "s3://sim-0".into(),
                )),
            ]),
            Action::WorkerFetchAssignment {
                worker: 0,
                succeeds: true,
            },
            Action::WorkerFetchAssignment {
                worker: 6,
                succeeds: true,
            },
            Action::WorkerFetchAssignment {
                worker: 5,
                succeeds: true,
            },
            Action::WorkerFetchAssignment {
                worker: 2,
                succeeds: true,
            },
            Action::AddChunks(vec![
                new_chunk((
                    "00000000000000042038".into(),
                    1048576,
                    12,
                    "s3://sim-0".into(),
                )),
                new_chunk((
                    "00000000000000038427".into(),
                    1048576,
                    12,
                    "s3://sim-1".into(),
                )),
                new_chunk((
                    "00000000000000045246".into(),
                    1048576,
                    1,
                    "s3://sim-2".into(),
                )),
                new_chunk((
                    "00000000000000002114".into(),
                    1048576,
                    4,
                    "s3://sim-2".into(),
                )),
            ]),
        ],
    );
}
