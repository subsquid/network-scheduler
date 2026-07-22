use super::*;
use crate::scheduler_storage::SchedulerStorage;
use crate::scheduler_storage::algorithm::IdealMapping;
use crate::scheduler_storage::test_harness::inspect::{
    StorageInspect, WorkerAssignmentDiffView, WorkerView,
};
pub(crate) use crate::scheduler_storage::test_harness::utils::{
    StaticSchedulingAlgorithm, chunk, chunk_with_blocks, dataset, worker,
};
use claims::{assert_matches, assert_none};
use std::collections::BTreeMap;

mod correction_tests;
mod inspect;
mod mvcc_protocol;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn storage_with(chunks: Vec<NewChunk>) -> InMemoryStorage {
    let mut storage = InMemoryStorage::default();
    let datasets: Vec<Dataset> = chunks
        .iter()
        .map(|chunk| (*chunk.dataset).clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    storage.insert_new_datasets(datasets).unwrap();
    storage.insert_new_chunks(chunks).unwrap();
    storage
}

// ----- StorageInspect-based read helpers -----

fn workers(storage: &InMemoryStorage) -> Vec<WorkerView> {
    storage.get_workers(|_| true)
}

fn worker_by_id(storage: &InMemoryStorage, id: WorkerPk) -> WorkerView {
    storage
        .get_workers(|worker| worker.worker_id == id)
        .into_iter()
        .next()
        .expect("worker not found")
}

// ---------------------------------------------------------------------------
// 1. insert_new_datasets / insert_new_chunks
// ---------------------------------------------------------------------------

#[test]
fn insert_datasets_populates_set() {
    let mut storage = InMemoryStorage::default();
    storage
        .insert_new_datasets(vec![dataset("a"), dataset("b")])
        .unwrap();
    let names = storage.get_datasets(|_| true);
    assert!(names.contains(&dataset("a")));
    assert!(names.contains(&dataset("b")));
}

#[test]
fn insert_duplicate_dataset_errors() {
    let mut storage = InMemoryStorage::default();
    storage.insert_new_datasets(vec![dataset("a")]).unwrap();
    let result = storage.insert_new_datasets(vec![dataset("a")]);
    assert_matches!(
        result,
        Err(InsertDatasetError::UniqueConstraintViolation { .. })
    );
}

#[test]
fn insert_chunk_without_dataset_errors() {
    let mut storage = InMemoryStorage::default();
    let result = storage.insert_new_chunks(vec![chunk("missing", 1, 100)]);
    assert_matches!(result, Err(InsertChunkError::NoDatasetFound { .. }));
}

#[test]
fn insert_duplicate_chunk_errors() {
    let mut storage = storage_with(vec![chunk("a", 1, 100)]);
    // Re-inserting a present (dataset, id) is rejected, mirroring Postgres' UNIQUE constraint.
    let result = storage.insert_new_chunks(vec![chunk("a", 1, 100)]);
    assert_matches!(result, Err(InsertChunkError::ChunkAlreadyExists { .. }));
}

#[test]
fn get_chunks_returns_inserted_chunks() {
    let chunk_1 = chunk("a", 1, 100);
    let chunk_2 = chunk("a", 2, 200);
    let storage = storage_with(vec![chunk_1.clone(), chunk_2.clone()]);

    let views = storage.get_chunks(|_| true);
    assert_eq!(views.len(), 2);
    let by_pk: HashMap<_, _> = views
        .into_iter()
        .map(|view| (view.chunk_pk, view))
        .collect();
    let view_1 = &by_pk[&storage.pk_of(&chunk_1)];
    assert_eq!(view_1.dataset, *chunk_1.dataset);
    assert_eq!(view_1.chunk_id, *chunk_1.id);
    assert_eq!(view_1.size, 100);
    let view_2 = &by_pk[&storage.pk_of(&chunk_2)];
    assert_eq!(view_2.size, 200);
}

// ---------------------------------------------------------------------------
// 2. update_worker_set
// ---------------------------------------------------------------------------

#[test]
fn update_worker_set_inserts_new_peer_with_version() {
    let mut storage = InMemoryStorage::default();
    let worker = worker(1, Some("2.8.0"));
    storage
        .update_worker_set(std::slice::from_ref(&worker), 100, 60)
        .unwrap();

    let registry = workers(&storage);
    assert_eq!(registry.len(), 1);
    let id = registry[0].worker_id;
    let view = worker_by_id(&storage, id);
    assert_eq!(view.peer_id, worker.id.to_string());
    assert_eq!(view.version, Some("2.8.0".to_string()));
    assert_none!(view.inactive_since);
}

#[test]
fn update_worker_set_marks_absent_peer_stale() {
    let mut storage = InMemoryStorage::default();
    storage
        .update_worker_set(&[worker(1, None)], 100, 60)
        .unwrap();

    storage.update_worker_set(&[], 200, 60).unwrap();
    assert_eq!(
        workers(&storage).len(),
        1,
        "absent peer marked stale, not removed"
    );
    assert_eq!(workers(&storage)[0].inactive_since, Some(200));
}

#[test]
fn update_worker_set_revives_stale_and_refreshes_version() {
    let mut storage = InMemoryStorage::default();
    storage
        .update_worker_set(&[worker(1, Some("2.7.0"))], 100, 60)
        .unwrap();
    storage.update_worker_set(&[], 200, 60).unwrap(); // now stale

    storage
        .update_worker_set(&[worker(1, Some("2.8.0"))], 300, 60)
        .unwrap();
    let registry = workers(&storage);
    let view = &registry[0];
    assert_none!(view.inactive_since); // revived, not departed
    assert_eq!(view.version, Some("2.8.0".to_string()));
}

#[test]
fn update_worker_set_refreshes_version_for_continuously_active() {
    let mut storage = InMemoryStorage::default();
    storage
        .update_worker_set(&[worker(1, Some("2.7.0"))], 100, 60)
        .unwrap();
    storage
        .update_worker_set(&[worker(1, Some("2.8.0"))], 200, 60)
        .unwrap();
    let registry = workers(&storage);
    let view = &registry[0];
    assert_eq!(view.version, Some("2.8.0".to_string()));
    assert_none!(view.inactive_since);
}

#[test]
fn update_worker_set_already_stale_is_idempotent() {
    let mut storage = InMemoryStorage::default();
    storage
        .update_worker_set(&[worker(1, None)], 100, 1000)
        .unwrap();
    storage.update_worker_set(&[], 200, 1000).unwrap();
    let stale_at = workers(&storage)[0].inactive_since;

    storage.update_worker_set(&[], 300, 1000).unwrap();
    assert_eq!(
        workers(&storage)[0].inactive_since,
        stale_at,
        "already-stale workers must not re-depart (timestamp unchanged)"
    );
}

#[test]
fn update_worker_set_gc_boundary() {
    let mut storage = InMemoryStorage::default();
    storage
        .update_worker_set(&[worker(1, None)], 100, 60)
        .unwrap();
    storage.update_worker_set(&[], 200, 60).unwrap(); // worker stale at 200

    // At now=260, cutoff = 260 - 60 = 200. 200 < 200 is false → kept.
    storage.update_worker_set(&[], 260, 60).unwrap();
    assert_eq!(workers(&storage).len(), 1, "worker at the boundary is kept");

    // At now=300, cutoff = 300 - 60 = 240. 200 < 240 → evicted.
    storage.update_worker_set(&[], 300, 60).unwrap();
    assert!(
        workers(&storage).is_empty(),
        "worker past the boundary is evicted"
    );
}

#[test]
fn update_worker_set_cleans_stale_mappings_for_departed() {
    let mut storage = InMemoryStorage::default();
    storage
        .update_worker_set(&[worker(1, None)], 100, 60)
        .unwrap();
    let worker_id = workers(&storage)[0].worker_id;
    storage.sched_stale_mappings.insert(
        (ChunkPk(1), worker_id),
        StaleMapping {
            superseded_at_worker_assignment_id: 1,
        },
    );

    storage.update_worker_set(&[], 200, 60).unwrap(); // worker departs
    assert!(storage.get_stale_mappings(|_| true).is_empty());
}

// ---------------------------------------------------------------------------
// 3. tombstone_expired_chunks
// ---------------------------------------------------------------------------

/// One chunk, portal-dropped at `portal_drop_tick`.
fn setup_for_tombstone(portal_drop_tick: TimeUnit) -> (InMemoryStorage, ChunkPk) {
    let chunk = chunk("a", 1, 100);
    let mut storage = storage_with(vec![chunk.clone()]);
    let pk = storage.pk_of(&chunk);

    let portal_aid = 7;
    storage.sched_portal_assignments.insert(
        portal_aid,
        PortalAssignmentEntry {
            created_at: portal_drop_tick,
            confirmed_up_to: 0,
        },
    );
    storage.sched_chunk_metadata.insert(
        pk,
        SchedulerChunkMetadata {
            dropped_at_portal_assignment_id: Some(portal_aid),
            ..Default::default()
        },
    );
    (storage, pk)
}

#[test]
fn tombstone_respects_m_ticks_boundary() {
    // portal-drop at tick 100, m_ticks = 100.
    let (mut storage, pk) = setup_for_tombstone(100);

    // elapsed = 50 < m_ticks → not tombstoned.
    storage.tombstone_expired_chunks(150, 100);
    assert_none!(
        storage
            .get_chunk_metadata_by_pk(pk)
            .dropped_from_worker_assignment_at
    );

    // elapsed = 100 ≥ m_ticks → tombstoned, stamped with the drop tick.
    storage.tombstone_expired_chunks(200, 100);
    assert_eq!(
        storage
            .get_chunk_metadata_by_pk(pk)
            .dropped_from_worker_assignment_at,
        Some(200)
    );
}

#[test]
fn tombstone_skips_when_now_below_m_ticks() {
    let (mut storage, pk) = setup_for_tombstone(0);
    // now < m_ticks → checked_sub fails → nothing eligible.
    storage.tombstone_expired_chunks(50, 100);
    assert_none!(
        storage
            .get_chunk_metadata_by_pk(pk)
            .dropped_from_worker_assignment_at
    );
}

#[test]
fn tombstone_is_idempotent() {
    let (mut storage, pk) = setup_for_tombstone(100);
    storage.tombstone_expired_chunks(300, 100);
    storage.tombstone_expired_chunks(400, 100);
    assert_eq!(
        storage
            .get_chunk_metadata_by_pk(pk)
            .dropped_from_worker_assignment_at,
        Some(300),
        "tombstone tick should be set once and never overwritten"
    );
}

#[test]
fn tombstone_clears_stale_mappings_for_chunk() {
    let (mut storage, pk) = setup_for_tombstone(100);
    let pending = |w_aid: u64| StaleMapping {
        superseded_at_worker_assignment_id: w_aid,
    };
    storage
        .sched_stale_mappings
        .insert((pk, WorkerPk(1)), pending(1));
    storage
        .sched_stale_mappings
        .insert((pk, WorkerPk(2)), pending(1));
    let other_pk = ChunkPk(9999);
    storage
        .sched_stale_mappings
        .insert((other_pk, WorkerPk(3)), pending(1));

    storage.tombstone_expired_chunks(300, 100);
    let tombstoned_pk = pk;
    assert!(
        storage
            .get_stale_mappings(|mapping| mapping.chunk_pk == tombstoned_pk)
            .is_empty(),
        "stale mappings for tombstoned chunk must be gone"
    );
    let surviving_pk = other_pk;
    assert_eq!(
        storage
            .get_stale_mappings(|mapping| mapping.chunk_pk == surviving_pk)
            .len(),
        1,
        "unrelated chunk's stale mappings must survive"
    );
}

#[test]
fn tombstone_does_not_touch_sched_ideal_chunk_workers() {
    let (mut storage, pk) = setup_for_tombstone(100);
    storage
        .sched_ideal_chunk_workers
        .insert(pk, vec![WorkerPk(1), WorkerPk(2)]);

    storage.tombstone_expired_chunks(300, 100);
    let target_pk = pk;
    let rows = storage.get_chunk_workers(|pk, _| *pk == target_pk);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].1,
        vec![WorkerPk(1), WorkerPk(2)],
        "tombstone must leave sched_ideal_chunk_workers to commit_ideal_mappings"
    );
}

// ---------------------------------------------------------------------------
// 4. confirm_worker_assignment
// ---------------------------------------------------------------------------

#[test]
fn confirm_worker_assignment_watermark_is_monotonic() {
    let mut storage = InMemoryStorage::default();
    storage.confirm_worker_assignment(5, 0).unwrap();
    assert_eq!(storage.get_worker_assignment_confirmation(), 5);
    storage.confirm_worker_assignment(7, 0).unwrap();
    assert_eq!(
        storage.get_worker_assignment_confirmation(),
        7,
        "higher id advances watermark"
    );
    storage.confirm_worker_assignment(3, 0).unwrap();
    assert_eq!(
        storage.get_worker_assignment_confirmation(),
        7,
        "lower id does not regress watermark"
    );
}

// ---------------------------------------------------------------------------
// 5. build_worker_assignment
// ---------------------------------------------------------------------------

#[test]
fn build_worker_assignment_unions_ideal_and_stale() {
    let chunk_1 = chunk("a", 1, 100);
    let chunk_2 = chunk("a", 2, 100);
    let mut storage = storage_with(vec![chunk_1.clone(), chunk_2.clone()]);
    let pk_1 = storage.pk_of(&chunk_1);
    let pk_2 = storage.pk_of(&chunk_2);

    storage
        .sched_ideal_chunk_workers
        .insert(pk_1, vec![WorkerPk(10), WorkerPk(11)]);
    storage
        .sched_ideal_chunk_workers
        .insert(pk_2, vec![WorkerPk(20)]);
    storage.sched_stale_mappings.insert(
        (pk_1, WorkerPk(99)),
        StaleMapping {
            superseded_at_worker_assignment_id: 1,
        },
    ); // grace-period holdover

    let assignment = storage.build_worker_assignment(42, BTreeMap::new());
    assert_eq!(assignment.id, 42);

    let workers_for_pk_1 = &assignment.chunk_workers[&pk_1];
    assert!(workers_for_pk_1.contains(&WorkerPk(10)));
    assert!(workers_for_pk_1.contains(&WorkerPk(11)));
    assert!(workers_for_pk_1.contains(&WorkerPk(99)));
    assert_eq!(assignment.chunk_workers[&pk_2], vec![WorkerPk(20)]);
}

#[test]
fn build_worker_assignment_chunks_only_for_keys_in_chunk_workers() {
    let chunk_1 = chunk("a", 1, 100);
    let chunk_2 = chunk("a", 2, 100); // exists in `chunks` but not in any mapping
    let mut storage = storage_with(vec![chunk_1.clone(), chunk_2.clone()]);
    let pk_1 = storage.pk_of(&chunk_1);
    let pk_2 = storage.pk_of(&chunk_2);

    storage
        .sched_ideal_chunk_workers
        .insert(pk_1, vec![WorkerPk(10)]);

    let assignment = storage.build_worker_assignment(42, BTreeMap::new());
    assert!(assignment.chunks.contains_key(&pk_1));
    assert!(
        !assignment.chunks.contains_key(&pk_2),
        "chunks not in published mapping must be excluded from WorkerAssignment.chunks"
    );
}

// ---------------------------------------------------------------------------
// 6. run_visibility_cycle
// ---------------------------------------------------------------------------

#[test]
fn run_visibility_cycle_promotes_confirmed_chunks() {
    let chunk = chunk("a", 1, 100);
    let mut storage = storage_with(vec![chunk.clone()]);
    let pk = storage.pk_of(&chunk);

    storage.sched_worker_assignment_confirmation = 10;
    storage.sched_chunk_metadata.insert(
        pk,
        SchedulerChunkMetadata {
            applied_at_worker_assignment_id: Some(7),
            ..Default::default()
        },
    );
    storage
        .sched_ideal_chunk_workers
        .insert(pk, vec![WorkerPk(1)]);
    storage
        .sched_confirmed_chunk_workers
        .insert(pk, vec![WorkerPk(1)]);

    let portal_assignment = storage.run_visibility_cycle(500).unwrap();
    assert_eq!(
        storage
            .get_chunk_metadata_by_pk(pk)
            .applied_at_portal_assignment_id,
        Some(portal_assignment.id as u64)
    );
    assert!(portal_assignment.chunks.contains_key(&pk));
    assert_eq!(portal_assignment.chunk_workers[&pk], vec![WorkerPk(1)]);
}

#[test]
fn run_visibility_cycle_holds_unconfirmed_chunks() {
    let chunk = chunk("a", 1, 100);
    let mut storage = storage_with(vec![chunk.clone()]);
    let pk = storage.pk_of(&chunk);

    storage.sched_worker_assignment_confirmation = 5;
    storage.sched_chunk_metadata.insert(
        pk,
        SchedulerChunkMetadata {
            applied_at_worker_assignment_id: Some(7), // > confirmed_up_to
            ..Default::default()
        },
    );
    storage
        .sched_ideal_chunk_workers
        .insert(pk, vec![WorkerPk(1)]);

    let portal_assignment = storage.run_visibility_cycle(500).unwrap();
    assert_none!(
        storage
            .get_chunk_metadata_by_pk(pk)
            .applied_at_portal_assignment_id
    );
    assert!(!portal_assignment.chunks.contains_key(&pk));
}

#[test]
fn run_visibility_cycle_drops_marked_for_removal() {
    let chunk = chunk("a", 1, 100);
    let mut storage = storage_with(vec![chunk.clone()]);
    let pk = storage.pk_of(&chunk);

    storage.sched_worker_assignment_confirmation = 10;
    storage.sched_chunk_metadata.insert(
        pk,
        SchedulerChunkMetadata {
            applied_at_worker_assignment_id: Some(7),
            applied_at_portal_assignment_id: Some(3),
            marked_for_removal: true,
            ..Default::default()
        },
    );
    storage
        .sched_ideal_chunk_workers
        .insert(pk, vec![WorkerPk(1)]);
    storage
        .sched_confirmed_chunk_workers
        .insert(pk, vec![WorkerPk(1)]);

    let portal_assignment = storage.run_visibility_cycle(500).unwrap();
    assert_eq!(
        storage
            .get_chunk_metadata_by_pk(pk)
            .dropped_at_portal_assignment_id,
        Some(portal_assignment.id as u64)
    );
    assert!(!portal_assignment.chunks.contains_key(&pk));
}

#[test]
fn run_visibility_cycle_uses_ideal_not_stale() {
    let chunk = chunk("a", 1, 100);
    let mut storage = storage_with(vec![chunk.clone()]);
    let pk = storage.pk_of(&chunk);

    storage.sched_worker_assignment_confirmation = 10;
    storage.sched_chunk_metadata.insert(
        pk,
        SchedulerChunkMetadata {
            applied_at_worker_assignment_id: Some(7),
            applied_at_portal_assignment_id: Some(3),
            ..Default::default()
        },
    );
    storage
        .sched_ideal_chunk_workers
        .insert(pk, vec![WorkerPk(1)]);
    storage
        .sched_confirmed_chunk_workers
        .insert(pk, vec![WorkerPk(1)]);
    storage.sched_stale_mappings.insert(
        (pk, WorkerPk(99)),
        StaleMapping {
            // > watermark: stays pending, never drains
            superseded_at_worker_assignment_id: 999,
        },
    );

    let portal_assignment = storage.run_visibility_cycle(500).unwrap();
    let workers = &portal_assignment.chunk_workers[&pk];
    assert_eq!(
        *workers,
        vec![WorkerPk(1)],
        "portal must not include stale-mapping workers"
    );
    assert!(!workers.contains(&WorkerPk(99)));
}

// ---------------------------------------------------------------------------
// 7. run_scheduling_cycle
// ---------------------------------------------------------------------------

#[test]
fn run_scheduling_cycle_applies_mapping_from_algorithm() {
    let chunk_1 = chunk("a", 1, 100);
    let chunk_2 = chunk("a", 2, 100);
    let mut storage = storage_with(vec![chunk_1.clone(), chunk_2.clone()]);
    let pk_1 = storage.pk_of(&chunk_1);
    let pk_2 = storage.pk_of(&chunk_2);

    // worker_ids are assigned in HashMap iteration order — record them rather
    // than assume a peer→id mapping.
    storage
        .update_worker_set(&[worker(1, None), worker(2, None)], 0, 1000)
        .unwrap();
    let worker_ids: Vec<WorkerPk> = workers(&storage)
        .iter()
        .map(|view| view.worker_id)
        .collect();
    let worker_id_1 = worker_ids[0];
    let worker_id_2 = worker_ids[1];

    let mapping: IdealMapping = vec![
        (pk_1, vec![worker_id_1, worker_id_2]),
        (pk_2, vec![worker_id_1]),
    ];
    let algorithm = StaticSchedulingAlgorithm { mapping };

    let (assignment, _) = storage
        .run_scheduling_cycle(&algorithm, &(), 100, 60)
        .expect("scheduling succeeds");

    let workers_for_pk_1 = &assignment.chunk_workers[&pk_1];
    assert_eq!(workers_for_pk_1.len(), 2);
    assert!(workers_for_pk_1.contains(&worker_id_1));
    assert!(workers_for_pk_1.contains(&worker_id_2));
    assert_eq!(assignment.chunk_workers[&pk_2], vec![worker_id_1]);

    assert!(assignment.chunks.contains_key(&pk_1));
    assert!(assignment.chunks.contains_key(&pk_2));

    // First inclusion in a worker assignment stamps applied_at_worker_assignment_id.
    assert_eq!(
        storage
            .get_chunk_metadata_by_pk(pk_1)
            .applied_at_worker_assignment_id,
        Some(assignment.id as u64)
    );
    assert_eq!(
        storage
            .get_chunk_metadata_by_pk(pk_2)
            .applied_at_worker_assignment_id,
        Some(assignment.id as u64)
    );
}

// ---------------------------------------------------------------------------
// 8. get_worker_assignment_diffs
// ---------------------------------------------------------------------------

#[test]
fn worker_assignment_diffs_recorded_after_cycle_then_cleared_on_confirm() {
    let chunk_1 = chunk("a", 1, 100);
    let mut storage = storage_with(vec![chunk_1.clone()]);
    let pk_1 = storage.pk_of(&chunk_1);
    storage.register_new_chunks().unwrap();

    storage
        .update_worker_set(&[worker(1, None)], 0, 1000)
        .unwrap();
    let worker_id = workers(&storage)[0].worker_id;

    let mapping: IdealMapping = vec![(pk_1, vec![worker_id])];
    let algorithm = StaticSchedulingAlgorithm { mapping };

    let (assignment, _) = storage
        .run_scheduling_cycle(&algorithm, &(), 100, 60)
        .expect("scheduling succeeds");

    let diffs: Vec<WorkerAssignmentDiffView> = storage.get_worker_assignment_diffs(|_| true);
    assert!(
        !diffs.is_empty(),
        "a routing-changing cycle must produce diff rows"
    );
    assert!(
        diffs
            .iter()
            .any(|d| d.worker_assignment_id == assignment.id as u64),
        "diff must carry the assignment id from the cycle"
    );

    // Confirming the assignment replays + drops the diff.
    storage
        .confirm_worker_assignment(assignment.id, 100)
        .unwrap();
    assert!(
        storage.get_worker_assignment_diffs(|_| true).is_empty(),
        "after confirming the assignment, the diff queue must be empty"
    );
}

// ---------------------------------------------------------------------------
// Registration-time non-overlap rejection
// ---------------------------------------------------------------------------

#[test]
fn register_new_chunks_rejects_overlapping_duplicate() {
    let mut lower = chunk("a", 1, 100);
    lower.blocks = 10..=20;
    let mut higher = chunk("a", 2, 100);
    higher.blocks = 15..=25; // overlaps lower at [15,20]
    let mut storage = storage_with(vec![lower.clone(), higher.clone()]);
    let lower_pk = storage.pk_of(&lower);

    // The lower-first_block chunk is admitted; the overlapping one is rejected at registration.
    let admitted = storage.register_new_chunks().unwrap();
    assert_eq!(admitted, vec![lower_pk]);
    // Terminal: a second registration never reconsiders the rejected chunk.
    assert!(storage.register_new_chunks().unwrap().is_empty());
}

#[test]
fn register_new_chunks_admits_non_overlapping() {
    let mut a = chunk("a", 1, 100);
    a.blocks = 10..=20;
    let mut b = chunk("a", 2, 100);
    b.blocks = 21..=30; // adjacent, no overlap
    let mut storage = storage_with(vec![a.clone(), b.clone()]);

    let mut admitted = storage.register_new_chunks().unwrap();
    admitted.sort();
    let mut expected = vec![storage.pk_of(&a), storage.pk_of(&b)];
    expected.sort();
    assert_eq!(
        admitted, expected,
        "non-overlapping chunks are both admitted"
    );
}

#[test]
fn register_new_chunks_admits_correction_replacement_over_its_old_chunk() {
    // A same-range replacement overlaps the old chunk it supersedes, but is exempt from the gate.
    let old = chunk("a", 1, 100);
    let mut storage = storage_with(vec![old.clone()]);
    let old_pk = storage.pk_of(&old);
    storage.register_new_chunks().unwrap(); // admit old

    let mut replacement = chunk("a", 2, 100);
    replacement.blocks = old.blocks.clone(); // same range as old -> overlaps it
    let replacement_pk = storage.register_correction(old_pk, replacement, 1).unwrap();

    let admitted = storage.register_new_chunks().unwrap();
    assert_eq!(
        admitted,
        vec![replacement_pk],
        "correction replacement admitted despite overlapping its old chunk",
    );
}

#[test]
fn register_new_chunks_rejects_non_same_range_replacement() {
    // Admission-gate backstop: register_correction already refuses a range-changing replacement, so
    // this is reached only out-of-band — here by inserting a mismatched correction directly. A
    // replacement whose range differs from its predecessor's is rejected, not admitted.
    let old = chunk("a", 1, 100); // range 2..=3
    let mut storage = storage_with(vec![old.clone()]);
    let old_pk = storage.pk_of(&old);
    storage.register_new_chunks().unwrap();

    let replacement = chunk("a", 2, 100); // range 4..=5, differs from old's 2..=3
    storage
        .insert_new_chunks(vec![replacement.clone()])
        .unwrap();
    let replacement_pk = storage.pk_of(&replacement);
    storage.chunk_corrections.insert(
        old_pk,
        ChunkCorrection {
            new_chunk_pk: replacement_pk,
            dataset: "a".to_string(),
            created_at: 1,
            applied_at_portal_assignment_id: None,
        },
    );

    let admitted = storage.register_new_chunks().unwrap();
    assert!(
        !admitted.contains(&replacement_pk),
        "range-changing replacement must be rejected at the admission gate, not admitted",
    );
}
