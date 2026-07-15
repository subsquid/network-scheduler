//! Postgres-backend tests. Kept a descendant of `postgres` so they retain access to its private
//! connection internals (`with_conn`) without widening that surface to the crate.

mod correction_tests;
mod drain_tests;
mod inspect;
mod registration_tests;
mod schema;
mod worker_tests;

use std::sync::atomic::{AtomicU64, Ordering};

use crate::scheduler_storage::postgres::PostgresStorage;
use crate::scheduler_storage::test_harness::pg_harness::fresh_db;
use crate::scheduler_storage::test_harness::utils::{chunk, dataset};
use crate::scheduler_storage::{AssignmentId, ChunkPk, SchedulerStorage, SchemaId, StorageError};
use crate::types::DatasetSchema;

static TEST_ID: AtomicU64 = AtomicU64::new(0);

/// A migrated storage on its own database, so tests run concurrently.
fn fresh_storage(name: &str) -> PostgresStorage {
    fresh_db(name, TEST_ID.fetch_add(1, Ordering::Relaxed))
}

/// Insert one chunk (`id_seed`) into the already-registered `dataset_name` and register it.
fn register_chunk(
    storage: &mut PostgresStorage,
    dataset_name: &str,
    id_seed: u32,
    size: u32,
) -> ChunkPk {
    storage
        .insert_new_chunks(vec![chunk(dataset_name, id_seed, size)])
        .expect("insert chunk");
    let pks = storage.register_new_chunks().expect("register chunks");
    *pks.last().expect("at least one pk registered")
}

/// [`register_chunk`], registering `dataset_name` first if it isn't already.
fn insert_and_register_chunk(
    storage: &mut PostgresStorage,
    dataset_name: &str,
    id_seed: u32,
    size: u32,
) -> ChunkPk {
    // Ignore already-exists errors for shared dataset names.
    let _ = storage.insert_new_datasets(vec![(dataset(dataset_name), DatasetSchema::default())]);
    register_chunk(storage, dataset_name, id_seed, size)
}

fn confirm(storage: &mut PostgresStorage, assignment_id: AssignmentId, now: u64) {
    storage
        .confirm_worker_assignment(assignment_id, now)
        .expect("confirm succeeds");
}

/// The id of `ds`'s current schema (the row with `superseded_at IS NULL`).
fn current_schema_id(storage: &mut PostgresStorage, ds: String) -> SchemaId {
    storage
        .with_conn(async move |conn| {
            let id: SchemaId = sqlx::query_scalar(
                "SELECT s.id FROM schemas s JOIN datasets d ON d.id = s.dataset_id \
                 WHERE d.name = $1 AND s.superseded_at IS NULL",
            )
            .bind(&ds)
            .fetch_one(&mut *conn)
            .await
            .unwrap();
            Ok::<_, StorageError>(id)
        })
        .unwrap()
}
