//! Tests for [`PostgresStorage::register_correction`] and the correction-aware
//! visibility cycle. Each test gets a fresh database so cases run concurrently.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

use claims::assert_matches;
use proptest::prelude::*;

use crate::scheduler_storage::algorithm::IdealMapping;
use crate::scheduler_storage::postgres::PostgresStorage;
use crate::scheduler_storage::test_harness::assert_portal_chunks_exact;
use crate::scheduler_storage::test_harness::inspect::StorageInspect;
use crate::scheduler_storage::test_harness::pg_harness::fresh_db;
use crate::scheduler_storage::test_harness::utils::{
    StaticSchedulingAlgorithm, chunk, chunk_with_blocks, dataset, worker,
};
use crate::scheduler_storage::{AssignmentId, ChunkPk, SchedulerStorage, StorageError, WorkerPk};
use crate::types::Worker;

static TEST_ID: AtomicU64 = AtomicU64::new(0);

fn next_id() -> u64 {
    TEST_ID.fetch_add(1, Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

fn fresh_storage(name: &str) -> PostgresStorage {
    fresh_db(name, next_id())
}

fn insert_and_register_chunk(
    storage: &mut PostgresStorage,
    dataset_name: &str,
    id_seed: u32,
    size: u32,
) -> ChunkPk {
    // Ignore already-exists errors for shared dataset names.
    let _ = storage.insert_new_datasets(vec![dataset(dataset_name)]);
    storage
        .insert_new_chunks(vec![chunk(dataset_name, id_seed, size)])
        .expect("insert chunk");
    let pks = storage.register_new_chunks().expect("register chunks");
    *pks.last().expect("at least one pk registered")
}

fn schedule_all(
    storage: &mut PostgresStorage,
    chunk_pks: &[ChunkPk],
    worker_ids: &[WorkerPk],
    at: u64,
) -> AssignmentId {
    // Mirror the scheduler loop: discover new chunks (incl. correction replacements, whose
    // sched_chunk_metadata is no longer created by register_correction) before scheduling.
    storage.register_new_chunks().expect("register new chunks");
    let workers: HashSet<WorkerPk> = worker_ids.iter().copied().collect();
    let mapping: IdealMapping = chunk_pks.iter().map(|&pk| (pk, workers.clone())).collect();
    let algorithm = StaticSchedulingAlgorithm { mapping };
    storage
        .run_scheduling_cycle(&algorithm, &(), at, 60)
        .expect("scheduling succeeds")
        .id
}

fn confirm(storage: &mut PostgresStorage, assignment_id: AssignmentId, now: u64) {
    storage
        .confirm_worker_assignment(assignment_id, now)
        .expect("confirm succeeds");
}

/// Simulate the mark_for_removal path by setting the column directly via SQL.
fn set_marked_for_removal(storage: &mut PostgresStorage, chunk_pk: ChunkPk, tick: i64) {
    storage
        .with_conn(async move |conn| {
            sqlx::query(
                "UPDATE sched_chunk_metadata SET marked_for_removal = $2 WHERE chunk_pk = $1",
            )
            .bind(chunk_pk)
            .bind(tick)
            .execute(&mut *conn)
            .await
            .unwrap();
            Ok::<_, StorageError>(())
        })
        .unwrap();
}

/// Insert an FK-anchor row via `insert_sql` (which must `RETURNING id`), then point
/// `metadata_column` on the chunk's `sched_chunk_metadata` row at it.
fn anchor_metadata_column(
    storage: &mut PostgresStorage,
    chunk_pk: ChunkPk,
    insert_sql: &'static str,
    metadata_column: &'static str,
) {
    let update_sql =
        format!("UPDATE sched_chunk_metadata SET {metadata_column} = $2 WHERE chunk_pk = $1");
    storage
        .with_conn(async move |conn| {
            let anchor_id: i64 = sqlx::query_scalar(insert_sql)
                .fetch_one(&mut *conn)
                .await
                .unwrap();
            sqlx::query(sqlx::AssertSqlSafe(update_sql))
                .bind(chunk_pk)
                .bind(anchor_id)
                .execute(&mut *conn)
                .await
                .unwrap();
            Ok::<_, StorageError>(())
        })
        .unwrap();
}

/// Simulate a chunk already dropped at portal; inserts a portal assignment row to satisfy the FK.
fn set_dropped_at_portal(storage: &mut PostgresStorage, chunk_pk: ChunkPk) {
    anchor_metadata_column(
        storage,
        chunk_pk,
        "INSERT INTO sched_portal_assignments (created_at) VALUES (1) RETURNING id",
        "dropped_at_portal_assignment_id",
    );
}

/// Simulate a tombstoned chunk; inserts a worker assignment row to satisfy the FK.
fn set_tombstoned(storage: &mut PostgresStorage, chunk_pk: ChunkPk) {
    anchor_metadata_column(
        storage,
        chunk_pk,
        "INSERT INTO sched_worker_assignments (created_at) VALUES (1) RETURNING id",
        "dropped_at_worker_assignment_id",
    );
}

/// Reach the underlying `DatabaseError` behind a `StorageError::Database`, so tests can assert the
/// *specific* constraint/trigger that rejected the write rather than merely that some DB error
/// occurred. The sqlx error sits in the anyhow chain under the `.context(...)` layer.
fn pg_db_error(result: &Result<ChunkPk, StorageError>) -> &dyn sqlx::error::DatabaseError {
    let Err(StorageError::Database(err)) = result else {
        panic!("expected StorageError::Database, got {result:?}");
    };
    err.chain()
        .find_map(|cause| cause.downcast_ref::<sqlx::Error>())
        .and_then(sqlx::Error::as_database_error)
        .expect("a sqlx database error in the chain")
}

// ---------------------------------------------------------------------------
// Registration guard tests
// ---------------------------------------------------------------------------

#[test]
fn register_correction_rejects_unknown_old_pk() {
    let mut storage = fresh_storage("rc_unknown_old");
    let _ = storage.insert_new_datasets(vec![dataset("ds")]);

    // An unknown old chunk is rejected by the chunk_corrections FK on old_chunk_pk — a DB error,
    // not an application pre-check.
    let result = storage.register_correction(ChunkPk(99999), chunk("ds", 1, 100), 1);
    assert_matches!(
        pg_db_error(&result).kind(),
        sqlx::error::ErrorKind::ForeignKeyViolation
    );
}

#[test]
fn register_correction_rejects_existing_replacement() {
    let mut storage = fresh_storage("rc_existing_repl");
    let old_pk = insert_and_register_chunk(&mut storage, "ds", 1, 100);
    // A chunk id already present in the dataset cannot be minted as a replacement.
    insert_and_register_chunk(&mut storage, "ds", 2, 100);

    // Same-range as the old chunk (id 1 → 2..=3) so the range guard passes; the UNIQUE(dataset_id,
    // chunk_id) violation on the colliding replacement id is translated to CorrectionRejected.
    let result = storage.register_correction(old_pk, chunk_with_blocks("ds", 2, 100, 2..=3), 1);
    assert_matches!(result, Err(StorageError::CorrectionRejected { .. }));
}

#[test]
fn register_correction_rejects_cross_dataset_replacement() {
    let mut storage = fresh_storage("rc_cross_dataset");
    let old_pk = insert_and_register_chunk(&mut storage, "ds-1", 1, 100);
    // Create a second dataset so the replacement's dataset exists; the chunk_corrections composite
    // FK must still reject linking an old and new chunk that live in different datasets.
    insert_and_register_chunk(&mut storage, "ds-2", 2, 100);

    // The same-dataset trigger rejects it (SQLSTATE P0001 from its RAISE), not an application-side
    // check — so the guarantee holds for any client writing chunk_corrections.
    // Same-range as the old chunk (id 1 → 2..=3) so the range guard passes and the same-dataset
    // trigger is what rejects the cross-dataset link.
    let result = storage.register_correction(old_pk, chunk_with_blocks("ds-2", 3, 100, 2..=3), 1);
    assert_eq!(pg_db_error(&result).code().as_deref(), Some("P0001"));
}

#[test]
fn register_correction_rejects_duplicate_old_pk() {
    let mut storage = fresh_storage("rc_dup_old");
    let old_pk = insert_and_register_chunk(&mut storage, "ds", 1, 100);

    // Both replacements are same-range as the old chunk (id 1 → 2..=3) so the range guard passes
    // and the first succeeds; the second then reaches the PRIMARY KEY collision.
    storage
        .register_correction(old_pk, chunk_with_blocks("ds", 2, 100, 2..=3), 1)
        .expect("first registration succeeds");
    // A second correction for the same old chunk hits the chunk_corrections PRIMARY KEY on
    // old_chunk_pk, translated to CorrectionRejected (matching the in-memory DuplicatePending).
    let result = storage.register_correction(old_pk, chunk_with_blocks("ds", 3, 100, 2..=3), 2);
    assert_matches!(result, Err(StorageError::CorrectionRejected { .. }));
}

#[test]
fn register_correction_rejects_old_pk_marked_for_removal() {
    let mut storage = fresh_storage("rc_old_marked");
    let old_pk = insert_and_register_chunk(&mut storage, "ds", 1, 100);

    set_marked_for_removal(&mut storage, old_pk, 42);

    let result = storage.register_correction(old_pk, chunk("ds", 2, 100), 1);
    assert_matches!(result, Err(StorageError::CorrectionRejected { .. }));
}

#[test]
fn register_correction_rejects_old_pk_dropped_at_portal() {
    let mut storage = fresh_storage("rc_old_portal_drop");
    let old_pk = insert_and_register_chunk(&mut storage, "ds", 1, 100);

    set_dropped_at_portal(&mut storage, old_pk);

    let result = storage.register_correction(old_pk, chunk("ds", 2, 100), 1);
    assert_matches!(result, Err(StorageError::CorrectionRejected { .. }));
}

#[test]
fn register_correction_rejects_old_pk_tombstoned() {
    let mut storage = fresh_storage("rc_old_tombstone");
    let old_pk = insert_and_register_chunk(&mut storage, "ds", 1, 100);

    set_tombstoned(&mut storage, old_pk);

    let result = storage.register_correction(old_pk, chunk("ds", 2, 100), 1);
    assert_matches!(result, Err(StorageError::CorrectionRejected { .. }));
}

#[test]
fn register_correction_rejects_rejected_old_chunk() {
    let mut storage = fresh_storage("rc_old_rejected");
    let _ = storage.insert_new_datasets(vec![dataset("ds")]);
    // Two overlapping chunks registered together: the higher (first_block, pk) one is rejected.
    storage
        .insert_new_chunks(vec![
            chunk_with_blocks("ds", 1, 100, 10..=20),
            chunk_with_blocks("ds", 2, 100, 15..=25),
        ])
        .expect("insert");
    let admitted = storage.register_new_chunks().expect("register");
    let rejected_pk = storage
        .get_chunks(|_| true)
        .into_iter()
        .map(|view| view.chunk_pk)
        .find(|pk| !admitted.contains(pk))
        .expect("one chunk rejected at registration");

    // A rejected old chunk is terminal — correcting it is refused before the FK/range guards.
    let result =
        storage.register_correction(rejected_pk, chunk_with_blocks("ds", 3, 100, 15..=25), 1);
    assert_matches!(result, Err(StorageError::CorrectionRejected { .. }));
}

#[test]
fn register_correction_rejects_range_change() {
    let mut storage = fresh_storage("rc_range_change");
    // Healthy old chunk (id 1 → range 2..=3); a replacement with a distinct id in the same dataset
    // carries a different range (chunk(_, 2) → 4..=5), so the range guard rejects it.
    let old_pk = insert_and_register_chunk(&mut storage, "ds", 1, 100);

    let result = storage.register_correction(old_pk, chunk("ds", 2, 100), 1);
    let Err(StorageError::CorrectionRejected { reason }) = &result else {
        panic!("expected CorrectionRejected, got {result:?}");
    };
    // It's the range guard, not a being-removed rejection.
    assert!(
        reason.contains("does not match"),
        "expected a range-change rejection, got: {reason}"
    );
}

// ---------------------------------------------------------------------------
// Visibility-cycle integration tests
// ---------------------------------------------------------------------------

#[test]
fn correction_chain_link_held_until_producer_fires() {
    // Structural dependency: B→C must not fire while A→B — which produces B, B→C's old chunk —
    // is still pending (B unconfirmed), even though C is already confirmed. This is a chain link,
    // not temporal same-dataset ordering.
    let mut storage = fresh_storage("rc_dataset_order");

    let pk_a = insert_and_register_chunk(&mut storage, "ds", 1, 100);

    storage
        .update_worker_set(&[worker(1, None)], 0, 1000)
        .expect("upsert worker");
    let worker_ids: Vec<WorkerPk> = storage
        .get_workers(|_| true)
        .iter()
        .map(|v| v.worker_id)
        .collect();

    // Every link in the chain is a same-range swap, so B and C inherit the root A's range
    // (id 1 → 2..=3).
    let pk_b = storage
        .register_correction(pk_a, chunk_with_blocks("ds", 2, 100, 2..=3), 1)
        .expect("register A→B");
    let pk_c = storage
        .register_correction(pk_b, chunk_with_blocks("ds", 3, 100, 2..=3), 2)
        .expect("register B→C");

    // Confirm a cycle covering only pk_a/pk_c, then schedule pk_b without
    // confirming: pk_c's applied_at ≤ watermark (confirmed), pk_b's > watermark (not).
    let wa1 = schedule_all(&mut storage, &[pk_a, pk_c], &worker_ids, 100);
    confirm(&mut storage, wa1, 110);

    let _wa2 = schedule_all(&mut storage, &[pk_a, pk_b, pk_c], &worker_ids, 200);

    let portal = storage.run_visibility_cycle(250).expect("visibility cycle");

    let meta_a = storage.get_chunk_metadata_by_pk(pk_a);
    assert!(
        !meta_a.marked_for_removal,
        "pk_a must not be marked when A→B has not fired"
    );
    // A→B unfired (B unconfirmed) blocks B→C: pk_b and pk_c stay hidden, only pk_a is visible.
    assert_portal_chunks_exact(&portal, &[pk_a], "while A→B blocks B→C");
}

// ---------------------------------------------------------------------------
// Property-based test
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig { cases: 16, ..ProptestConfig::default() })]

    /// Safety invariants O1–O3 hold for PostgresStorage across correction lifecycles.
    ///
    /// O1: Every portal-visible chunk is routed by at least one worker in the confirmed routing.
    /// O2: Every portal-visible chunk has applied_at_portal set and dropped_at_portal unset.
    /// O3: mark_for_removal is a one-way transition — once set it is never unset.
    #[test]
    fn prop_pg_corrections_safety(
        n_workers in 2u8..=3u8,
        hold_before_confirm in proptest::bool::ANY,
    ) {
        let mut storage = fresh_storage("prop_pg_safety");

        let workers: Vec<Worker> = (1..=n_workers).map(|s| worker(s, None)).collect();
        storage
            .update_worker_set(&workers, 0, 10000)
            .expect("upsert workers");
        let worker_ids: Vec<WorkerPk> = storage
            .get_workers(|_| true)
            .iter()
            .map(|v| v.worker_id)
            .collect();

        let old_pk = insert_and_register_chunk(&mut storage, "ds", 1, 100);
        // The replacement is created by registration, so it can only be confirmed afterwards;
        // the timing freedom is whether a held visibility cycle runs before confirmation.
        let new_pk = storage
            .register_correction(old_pk, chunk_with_blocks("ds", 2, 100, 2..=3), 1)
            .expect("register correction");

        let wa1 = schedule_all(&mut storage, &[old_pk, new_pk], &worker_ids, 100);

        if hold_before_confirm {
            // A cycle while the correction is still pending (new_pk unconfirmed).
            storage
                .run_visibility_cycle(120)
                .expect("held visibility cycle");
        }
        confirm(&mut storage, wa1, 130);

        // Correction fires: new_pk confirmed ≤ watermark.
        let portal = storage
            .run_visibility_cycle(150)
            .expect("visibility cycle");

        for pk in portal.chunks.keys() {
            let routed = portal.chunk_workers.contains_key(pk);
            prop_assert!(
                routed,
                "O1 violated: portal chunk {pk} has no confirmed routing"
            );
        }

        let all_meta = storage.get_chunks_metadata(|_| true);
        for meta in &all_meta {
            if portal.chunks.contains_key(&meta.chunk_pk) {
                prop_assert!(
                    meta.applied_at_portal_assignment_id.is_some(),
                    "O2 violated: portal chunk {} missing applied_at_portal",
                    meta.chunk_pk
                );
                prop_assert!(
                    meta.dropped_at_portal_assignment_id.is_none(),
                    "O2 violated: portal chunk {} has dropped_at_portal set",
                    meta.chunk_pk
                );
            }
        }

        let meta_old = storage.get_chunk_metadata_by_pk(old_pk);
        prop_assert!(
            meta_old.marked_for_removal,
            "O3 violated: old_pk not marked for removal after correction fires"
        );
        prop_assert!(
            !portal.chunks.contains_key(&old_pk),
            "O3 violated: old_pk still portal-visible after correction fires"
        );

        // Cross-backend correction oracles (same definition as the in-memory suite
        // and the multistep sim), judged on gate-A metadata (promoted, not dropped).
        let promoted: HashSet<ChunkPk> = storage
            .get_chunks_metadata(|meta| {
                meta.applied_at_portal_assignment_id.is_some()
                    && meta.dropped_at_portal_assignment_id.is_none()
            })
            .into_iter()
            .map(|meta| meta.chunk_pk)
            .collect();
        let removing: HashSet<ChunkPk> = storage
            .get_chunks_metadata(|meta| {
                meta.marked_for_removal
                    || meta.dropped_at_portal_assignment_id.is_some()
                    || meta.dropped_at_worker_assignment_id.is_some()
            })
            .into_iter()
            .map(|meta| meta.chunk_pk)
            .collect();
        if let Err(violation) = crate::scheduler_storage::test_harness::correction_oracle::corrections_safety(
            &storage.get_corrections(|_| true),
            |pk| promoted.contains(pk),
            |pk| removing.contains(pk),
        ) {
            prop_assert!(false, "{violation}");
        }
    }
}
