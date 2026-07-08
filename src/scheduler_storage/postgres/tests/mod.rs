//! Postgres-backend tests. Kept a descendant of `postgres` so they retain access to its private
//! connection internals (`with_conn`) without widening that surface to the crate.

mod correction_tests;
mod inspect;
mod registration_tests;
mod schema;

use std::sync::atomic::{AtomicU64, Ordering};

use crate::scheduler_storage::postgres::PostgresStorage;
use crate::scheduler_storage::test_harness::pg_harness::fresh_db;
use crate::scheduler_storage::test_harness::utils::chunk;
use crate::scheduler_storage::{ChunkPk, SchedulerStorage, SchemaId, StorageError};

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
