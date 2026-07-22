//! Integration tests for the MVCC chunk-routing protocol — invariants that
//! span scheduling cycles and visibility cycles together.

use super::*;
use maplit::{btreemap, hashset};
use std::collections::{BTreeMap, HashSet};

/// How long a reassigned-away worker stays in the worker assignment (the `M`
/// in the comments below), giving portals time to refetch.
const GRACE_PERIOD: TimeUnit = 60;

const CYCLE_INTERVAL: TimeUnit = 100;

const DELTA: TimeUnit = 50;

/// Chunk migration w1 → w2: worker and portal views diverge during the grace
/// window (workers keep both; portals jump straight to the new ideal once
/// confirmed) and converge after it elapses. The "chunk migration (worker to
/// worker)" flow in `docs/mvcc-storage.md`.
#[test]
fn chunk_migration_through_grace_period() {
    let Setup {
        mut storage,
        worker_ids,
        chunk_pks,
    } = setup(2, 1);
    let chunk_pk = &chunk_pks[0];
    let (w1, w2) = (worker_ids[0], worker_ids[1]);

    let cycle_1_at = CYCLE_INTERVAL;
    let cycle_2_at = 2 * CYCLE_INTERVAL;
    let cycle_3_at = 3 * CYCLE_INTERVAL;
    let cycle_4_at = 4 * CYCLE_INTERVAL;

    // Cycle 1: chunk on w1. Confirmation gates the first portal promotion.
    let cycle_1 = StaticSchedulingAlgorithm {
        mapping: ideal_mapping([(chunk_pk, vec![w1])]),
    };
    let (worker_assignment_1, _) = storage
        .run_scheduling_cycle(&cycle_1, &(), cycle_1_at, GRACE_PERIOD)
        .expect("scheduling succeeds");

    let portal_pre_confirm_1 = storage.run_visibility_cycle(cycle_1_at).unwrap();
    assert!(
        portal_pre_confirm_1.chunk_workers.is_empty(),
        "pre-confirm: chunk not yet promoted to portal",
    );

    storage
        .confirm_worker_assignment(worker_assignment_1.id, cycle_1_at)
        .unwrap();

    let portal_post_confirm_1 = storage.run_visibility_cycle(cycle_1_at + DELTA).unwrap();
    assert_eq!(
        as_sets(&portal_post_confirm_1.chunk_workers),
        btreemap! { chunk_pk => hashset![w1] },
        "post-confirm: chunk promoted to portal",
    );

    // Cycle 2: reshuffle to w2 — views diverge during the grace window.
    let cycle_2 = StaticSchedulingAlgorithm {
        mapping: ideal_mapping([(chunk_pk, vec![w2])]),
    };
    let (worker_assignment_2, _) = storage
        .run_scheduling_cycle(&cycle_2, &(), cycle_2_at, GRACE_PERIOD)
        .expect("scheduling succeeds");
    assert_eq!(
        as_sets(&worker_assignment_2.chunk_workers),
        btreemap! { chunk_pk => hashset![w1, w2] },
        "grace window: worker still serves both",
    );

    let portal_pre_confirm_2 = storage.run_visibility_cycle(cycle_2_at).unwrap();
    assert_eq!(
        as_sets(&portal_pre_confirm_2.chunk_workers),
        btreemap! { chunk_pk => hashset![w1] },
        "pre-confirm: portal still routes to the old worker — w2 has not confirmed having the data",
    );

    storage
        .confirm_worker_assignment(worker_assignment_2.id, cycle_2_at)
        .unwrap();

    let portal_post_confirm_2 = storage.run_visibility_cycle(cycle_2_at + DELTA).unwrap();
    assert_eq!(
        as_sets(&portal_post_confirm_2.chunk_workers),
        btreemap! { chunk_pk => hashset![w2] },
        "post-confirm: portal reroutes to the new worker",
    );

    // Cycle 3: w1 left the portal at t=250; 300 − 250 < M, holdover retained.
    let cycle_3 = StaticSchedulingAlgorithm {
        mapping: ideal_mapping([(chunk_pk, vec![w2])]),
    };
    let (worker_assignment_3, _) = storage
        .run_scheduling_cycle(&cycle_3, &(), cycle_3_at, GRACE_PERIOD)
        .expect("scheduling succeeds");
    assert_eq!(
        as_sets(&worker_assignment_3.chunk_workers),
        btreemap! { chunk_pk => hashset![w1, w2] },
        "still draining: 300 − 250 < M, worker keeps serving the old worker",
    );

    let portal_pre_confirm_3 = storage.run_visibility_cycle(cycle_3_at).unwrap();
    assert_eq!(
        as_sets(&portal_pre_confirm_3.chunk_workers),
        btreemap! { chunk_pk => hashset![w2] },
        "pre-confirm: portal unchanged",
    );

    storage
        .confirm_worker_assignment(worker_assignment_3.id, cycle_3_at)
        .unwrap();

    let portal_post_confirm_3 = storage.run_visibility_cycle(cycle_3_at + DELTA).unwrap();
    assert_eq!(
        as_sets(&portal_post_confirm_3.chunk_workers),
        btreemap! { chunk_pk => hashset![w2] },
        "post-confirm: portal stayed on [w2] throughout — never saw the grace-period worker",
    );

    // Cycle 4: 400 − 250 ≥ M, the holdover expires and the views converge.
    let cycle_4 = StaticSchedulingAlgorithm {
        mapping: ideal_mapping([(chunk_pk, vec![w2])]),
    };
    let (worker_assignment_4, _) = storage
        .run_scheduling_cycle(&cycle_4, &(), cycle_4_at, GRACE_PERIOD)
        .expect("scheduling succeeds");
    assert_eq!(
        as_sets(&worker_assignment_4.chunk_workers),
        btreemap! { chunk_pk => hashset![w2] },
        "grace expired: worker no longer serves from w1",
    );

    // After full resolution, transient bookkeeping must be empty.
    assert!(
        storage.get_stale_mappings(|_| true).is_empty(),
        "no stale mappings should remain after the grace period expires",
    );

    let active_chunks: HashSet<ChunkPk> = storage
        .get_chunk_workers(|_, _| true)
        .into_iter()
        .map(|(pk, _)| pk)
        .collect();
    for meta in storage.get_chunks_metadata(|_| true) {
        let in_active = active_chunks.contains(&meta.chunk_pk);
        let tombstoned = meta.dropped_from_worker_assignment_at.is_some();
        assert!(
            in_active ^ tombstoned,
            "chunk {:?} must be in chunk_workers XOR tombstoned \
             (in_active={}, tombstoned={})",
            meta.chunk_pk,
            in_active,
            tombstoned,
        );
    }
}

/// While confirmations lag, the portal keeps publishing the routing of the
/// highest *confirmed* worker assignment, no matter how far the ideal advances.
#[test]
fn portal_routing_follows_highest_confirmed_worker_assignment() {
    let Setup {
        mut storage,
        worker_ids,
        chunk_pks,
    } = setup(2, 1);
    let chunk_pk = &chunk_pks[0];
    let (w1, w2) = (worker_ids[0], worker_ids[1]);

    let wa_1 = run_cycle(&mut storage, chunk_pk, vec![w1], 100);
    storage.confirm_worker_assignment(wa_1.id, 100).unwrap();
    let portal_after_cycle_1 = storage.run_visibility_cycle(150).unwrap();
    assert_eq!(
        as_sets(&portal_after_cycle_1.chunk_workers),
        btreemap! { chunk_pk => hashset![w1] },
    );

    let wa_2 = run_cycle(&mut storage, chunk_pk, vec![w1, w2], 200);
    let portal_after_cycle_2 = storage.run_visibility_cycle(250).unwrap();
    assert_eq!(
        as_sets(&portal_after_cycle_2.chunk_workers),
        btreemap! { chunk_pk => hashset![w1] },
        "portal still on wa_1's view — wa_2 not confirmed",
    );

    let wa_3 = run_cycle(&mut storage, chunk_pk, vec![w2], 300);
    let portal_after_cycle_3 = storage.run_visibility_cycle(350).unwrap();
    assert_eq!(
        as_sets(&portal_after_cycle_3.chunk_workers),
        btreemap! { chunk_pk => hashset![w1] },
        "portal still on wa_1's view — wa_2 and wa_3 not confirmed",
    );

    storage.confirm_worker_assignment(wa_2.id, 200).unwrap();
    let portal_after_confirm_wa_2 = storage.run_visibility_cycle(360).unwrap();
    assert_eq!(
        as_sets(&portal_after_confirm_wa_2.chunk_workers),
        btreemap! { chunk_pk => hashset![w1, w2] },
        "portal advanced to wa_2's view",
    );

    storage.confirm_worker_assignment(wa_3.id, 300).unwrap();
    let portal_after_confirm_wa_3 = storage.run_visibility_cycle(370).unwrap();
    assert_eq!(
        as_sets(&portal_after_confirm_wa_3.chunk_workers),
        btreemap! { chunk_pk => hashset![w2] },
        "portal advanced to wa_3's view",
    );
}

/// The reshuffle grace window is anchored on the portal cycle that first
/// publishes without the old worker (invariant 4), not on the scheduling cycle
/// that made the decision: while confirmation lags, the holdover is kept
/// regardless of elapsed time; the M-tick countdown starts at portal
/// publication and the holdover is released when it elapses.
#[test]
fn stale_reshuffle_holdover_lifecycle() {
    let Setup {
        mut storage,
        worker_ids,
        chunk_pks,
    } = setup(2, 1);
    let chunk_pk = &chunk_pks[0];
    let (w1, w2) = (worker_ids[0], worker_ids[1]);

    // Setup: c routed to both workers, wa_1 confirmed, c is portal-visible.
    let wa_1 = run_cycle(&mut storage, chunk_pk, vec![w1, w2], 100);
    storage.confirm_worker_assignment(wa_1.id, 100).unwrap();
    let portal_after_wa_1 = storage.run_visibility_cycle(100 + DELTA).unwrap();
    assert_eq!(
        as_sets(&portal_after_wa_1.chunk_workers),
        btreemap! { chunk_pk => hashset![w1, w2] },
        "baseline: portal routes to [w1, w2] after wa_1 confirm",
    );

    // Phase I: reshuffle c off w2 with no confirmation — holdover held.
    let wa_2 = run_cycle(&mut storage, chunk_pk, vec![w1], 200);
    assert_eq!(
        as_sets(&wa_2.chunk_workers),
        btreemap! { chunk_pk => hashset![w1, w2] },
        "wa_2 publishes [w1, w2] via stale union",
    );

    // 270 − 200 ≥ M, but the M-tick clock hasn't started: no portal cycle
    // has published without w2 yet.
    let wa_3 = run_cycle(&mut storage, chunk_pk, vec![w1], 270);
    assert_eq!(
        as_sets(&wa_3.chunk_workers),
        btreemap! { chunk_pk => hashset![w1, w2] },
        "wa_3 still publishes [w1, w2]: portal hasn't switched, M-tick \
         clock hasn't started",
    );

    // Phase II: confirmation arrives.
    storage.confirm_worker_assignment(wa_2.id, 200).unwrap();

    // First portal assignment publishing c without w2 — the M-tick countdown
    // anchors at its created_at (300).
    let portal_after_confirm = storage.run_visibility_cycle(300).unwrap();
    assert_eq!(
        as_sets(&portal_after_confirm.chunk_workers),
        btreemap! { chunk_pk => hashset![w1] },
        "portal switches to [w1] (gate B) once wa_2 is confirmed",
    );

    // 340 − 300 < M: still within the post-publication grace.
    let wa_4 = run_cycle(&mut storage, chunk_pk, vec![w1], 340);
    assert_eq!(
        as_sets(&wa_4.chunk_workers),
        btreemap! { chunk_pk => hashset![w1, w2] },
        "wa_4 still publishes [w1, w2]: within M-tick post-publication grace",
    );

    // Phase III: 400 − 300 ≥ M — the stale entry drops.
    let wa_5 = run_cycle(&mut storage, chunk_pk, vec![w1], 400);
    assert_eq!(
        as_sets(&wa_5.chunk_workers),
        btreemap! { chunk_pk => hashset![w1] },
        "wa_5 publishes [w1] only: M ticks past portal publication, \
         stale dropped",
    );
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Primed storage; `worker_ids` and `chunk_pks` are in registry order so tests
/// can index them directly.
struct Setup {
    storage: InMemoryStorage,
    worker_ids: Vec<WorkerPk>,
    chunk_pks: Vec<ChunkPk>,
}

fn setup(num_workers: usize, num_chunks: usize) -> Setup {
    let chunks: Vec<NewChunk> = (0..num_chunks)
        .map(|i| chunk("a", (i + 1) as u32, 100))
        .collect();
    let mut storage = storage_with(chunks.clone());
    let chunk_pks: Vec<ChunkPk> = chunks.iter().map(|c| storage.pk_of(c)).collect();

    let workers_to_register: Vec<Worker> = (0..num_workers)
        .map(|i| worker((i + 1) as u8, None))
        .collect();
    storage
        .update_worker_set(&workers_to_register, 0, 1000)
        .unwrap();
    let worker_ids: Vec<WorkerPk> = workers(&storage)
        .iter()
        .map(|view| view.worker_id)
        .collect();

    Setup {
        storage,
        worker_ids,
        chunk_pks,
    }
}

fn run_cycle(
    storage: &mut InMemoryStorage,
    chunk_pk: &ChunkPk,
    workers: Vec<WorkerPk>,
    at: TimeUnit,
) -> WorkerAssignment {
    let algorithm = StaticSchedulingAlgorithm {
        mapping: ideal_mapping([(chunk_pk, workers)]),
    };
    storage
        .run_scheduling_cycle(&algorithm, &(), at, GRACE_PERIOD)
        .expect("scheduling succeeds")
        .0
}

fn ideal_mapping<'a>(
    entries: impl IntoIterator<Item = (&'a ChunkPk, Vec<WorkerPk>)>,
) -> IdealMapping {
    entries.into_iter().map(|(pk, ids)| (*pk, ids)).collect()
}

/// The bundle holds exactly the in-play schemas, with their payloads: a placed (then portal-served)
/// chunk's schema is included, an unplaced chunk's is not, and the id is content-addressed over it.
#[test]
fn schema_bundle_holds_only_in_play_schemas() {
    use crate::scheduler_storage::{BundleId, SchemaBundle};
    use crate::types::{DatasetSchema, TableSchema};

    let one_table =
        |t: &str| DatasetSchema::new(BTreeMap::from([(t.to_owned(), TableSchema::default())]));
    let schema_a = one_table("blocks");

    // Distinct per-dataset schemas so the assertions check the bundle's content, not just its keys.
    // (Trait method, qualified: the inherent `insert_new_datasets` takes schema-less names.)
    let mut storage = InMemoryStorage::default();
    SchedulerStorage::insert_new_datasets(
        &mut storage,
        vec![
            (dataset("a"), schema_a.clone()),
            (dataset("b"), one_table("logs")),
        ],
    )
    .unwrap();
    let a = chunk("a", 1, 100);
    let b = chunk("b", 1, 100);
    storage
        .insert_new_chunks(vec![a.clone(), b.clone()])
        .unwrap();
    let a_pk = storage.pk_of(&a);
    storage
        .update_worker_set(&[worker(1, None)], 0, 1000)
        .unwrap();
    let w1 = workers(&storage)[0].worker_id;

    // Place only "a"; "b" stays unplaced (in no worker or portal assignment).
    let wa = run_cycle(&mut storage, &a_pk, vec![w1], CYCLE_INTERVAL);
    let a_schema = wa.chunks.get(&a_pk).unwrap().schema_id;

    let bundle = SchemaBundle::generate(&storage).unwrap();
    assert_eq!(bundle.schemas(), &BTreeMap::from([(a_schema, schema_a)]));
    assert_eq!(bundle.id(), BundleId::from_schema_ids([a_schema]));

    // Promote "a" to the portal — still in play, so the bundle is unchanged.
    storage
        .confirm_worker_assignment(wa.id, CYCLE_INTERVAL)
        .unwrap();
    let portal = storage
        .run_visibility_cycle(CYCLE_INTERVAL + DELTA)
        .unwrap();
    assert!(portal.chunk_workers.contains_key(&a_pk));
    assert_eq!(SchemaBundle::generate(&storage).unwrap().id(), bundle.id());
}

/// Retirement rides the chunk tombstone clock: a chunk dropped from the portal stays
/// worker-servable — and its schema stays in the bundle — through the M-tick drain window, then
/// leaves when it tombstones. This is what lets `schema_bundle_consistency` drop its exclusion.
#[test]
fn schema_bundle_retains_removing_chunk_until_tombstone() {
    use crate::scheduler_storage::SchemaBundle;

    let Setup {
        mut storage,
        worker_ids,
        chunk_pks,
    } = setup(1, 1);
    let chunk_pk = &chunk_pks[0];
    let w1 = worker_ids[0];

    // Place, confirm, promote to the portal.
    let (wa, _) = storage
        .run_scheduling_cycle(
            &StaticSchedulingAlgorithm {
                mapping: ideal_mapping([(chunk_pk, vec![w1])]),
            },
            &(),
            CYCLE_INTERVAL,
            GRACE_PERIOD,
        )
        .expect("scheduling succeeds");
    let schema_id = wa.chunks.get(chunk_pk).unwrap().schema_id;
    storage
        .confirm_worker_assignment(wa.id, CYCLE_INTERVAL)
        .unwrap();
    storage
        .run_visibility_cycle(CYCLE_INTERVAL + DELTA)
        .unwrap();
    assert!(
        SchemaBundle::generate(&storage)
            .unwrap()
            .contains(schema_id),
        "placed and promoted: schema is in the bundle",
    );

    // Mark for removal and drop it from the portal (dropped_at_portal stamped at `drop_at`).
    let drop_at = 2 * CYCLE_INTERVAL;
    storage.mark_for_removal(*chunk_pk, drop_at).unwrap();
    let portal = storage.run_visibility_cycle(drop_at).unwrap();
    assert!(
        portal.chunk_workers.is_empty(),
        "removing: dropped from the portal",
    );
    assert!(
        SchemaBundle::generate(&storage)
            .unwrap()
            .contains(schema_id),
        "removing but pre-tombstone: worker still serves it, so its schema stays in the bundle",
    );

    // Past M ticks after the portal drop, a scheduling cycle tombstones it — the schema retires,
    // and the bundle returned from that cycle no longer carries it.
    let (_wa2, bundle) = storage
        .run_scheduling_cycle(
            &StaticSchedulingAlgorithm {
                mapping: ideal_mapping([]),
            },
            &(),
            drop_at + GRACE_PERIOD + 1,
            GRACE_PERIOD,
        )
        .expect("scheduling succeeds");
    assert!(
        !bundle.contains(schema_id),
        "tombstoned: schema leaves the bundle",
    );
}

/// ADR 0002: a portal-visible chunk that loses its last holder (departure) keeps its schema in the
/// frozen bundle, even though it falls out of ideal ∪ stale — so portals can still resolve it.
#[test]
fn schema_bundle_covers_holderless_portal_visible_chunk() {
    let Setup {
        mut storage,
        worker_ids,
        chunk_pks,
    } = setup(1, 1);
    let chunk_pk = &chunk_pks[0];
    let w1 = worker_ids[0];

    // Place, confirm, promote to the portal -> portal_visible, one holder (w1).
    let (wa, _) = storage
        .run_scheduling_cycle(
            &StaticSchedulingAlgorithm {
                mapping: ideal_mapping([(chunk_pk, vec![w1])]),
            },
            &(),
            CYCLE_INTERVAL,
            GRACE_PERIOD,
        )
        .expect("scheduling succeeds");
    let schema_id = wa.chunks.get(chunk_pk).unwrap().schema_id;
    storage
        .confirm_worker_assignment(wa.id, CYCLE_INTERVAL)
        .unwrap();
    storage
        .run_visibility_cycle(CYCLE_INTERVAL + DELTA)
        .unwrap();

    // Depart the sole holder: its copy vanishes (not drained), no stale row survives.
    storage
        .update_worker_set(&[], 2 * CYCLE_INTERVAL, 1000)
        .unwrap();

    // Next cycle places nothing (no active workers) -> chunk is holderless (ideal={}, stale={}),
    // still portal_visible. The frozen bundle must still carry its schema.
    let (wa2, bundle) = storage
        .run_scheduling_cycle(
            &StaticSchedulingAlgorithm {
                mapping: ideal_mapping([]),
            },
            &(),
            3 * CYCLE_INTERVAL,
            GRACE_PERIOD,
        )
        .expect("scheduling succeeds");
    assert!(
        !wa2.chunks.contains_key(chunk_pk),
        "chunk is holderless: out of ideal ∪ stale",
    );
    assert!(
        bundle.contains(schema_id),
        "portal-visible chunk keeps its schema in the bundle",
    );
}
/// Order-insensitive view of `chunk_workers` for literal comparison. Panics on
/// duplicate worker ids, which the set conversion would otherwise hide.
fn as_sets(
    chunk_workers: &BTreeMap<ChunkPk, Vec<WorkerPk>>,
) -> BTreeMap<&ChunkPk, HashSet<WorkerPk>> {
    chunk_workers
        .iter()
        .map(|(pk, workers)| {
            let set: HashSet<WorkerPk> = workers.iter().copied().collect();
            assert_eq!(
                set.len(),
                workers.len(),
                "duplicate worker ids in chunk_workers for {:?}: {:?}",
                pk,
                workers,
            );
            (pk, set)
        })
        .collect()
}
