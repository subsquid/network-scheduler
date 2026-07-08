//! Captured regressions — shrunk action sequences, each pinning one scheduler property. Chunks
//! carry their own key/size/weight/dataset, so each sequence replays deterministically with no
//! seed. The replay *is* the assertion: it panics iff the property is violated.

use super::sut::{Action, ConvergenceCheck, NewChunk, SimConfig, SimUnderTest};
use super::utils::{
    CHUNK_SIZE, WORKER_CAPACITY, blocks_for_key, mint_key, new_chunk, sim_datasets,
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
    let light = |seed: u64, size: u32| new_chunk((mint_key(seed), size, 1, "s3://sim-0".to_string()));

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
/// FIXME: this behaviour should be fixed. When a chunk has fewer than the required number of copies,
/// placing a copy for it should take priority over keeping a draining copy of a chunk that still has
/// enough — the short chunk should reclaim that space instead of waiting out the grace period, so it
/// never has to drop to zero. The scheduler already avoids making new extra copies while any chunk is
/// short, but that is not enough on its own: a copy already draining keeps its space for the whole
/// grace period.
#[test]
fn churn_raise_min_replication_underreplicates_new_chunk() {
    let config = SimConfig {
        worker_count: 8,
        worker_capacity: WORKER_CAPACITY,
        min_replication: 2,
        saturation: 0.88,
        converge_is_terminal: false,
        chunk_cap: None,
        datasets: sim_datasets(),
        confirm_threshold_pct: 96,
    };

    let nc = |key: u64, weight: u16, dataset: &str| {
        new_chunk((mint_key(key), CHUNK_SIZE, weight, dataset.to_string()))
    };

    replay(
        &config,
        vec![
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
        ],
    );
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
