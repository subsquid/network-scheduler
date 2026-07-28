//! Tests for the PG-specific registration helpers ([`PostgresStorage::copy_dataset_chunks`]).

use super::fresh_storage;
use crate::scheduler_storage::SchedulerStorage;
use crate::scheduler_storage::test_harness::utils::{
    chunk, chunk_with_blocks, dataset, new_dataset,
};
use crate::types::DatasetSchema;

#[test]
fn copy_dataset_chunks_clones_live_set_only() {
    let mut storage = fresh_storage("copy_ds");
    storage
        .insert_new_datasets(vec![
            new_dataset("src", DatasetSchema::default()),
            new_dataset("dst", DatasetSchema::default()),
        ])
        .expect("datasets");

    // Two live chunks plus one that loses the overlap gate (same range as the first) — the
    // rejected chunk must not be cloned.
    storage
        .insert_new_chunks(vec![
            chunk("src", 1, 100),
            chunk("src", 2, 100),
            chunk_with_blocks("src", 3, 100, 2..=3),
        ])
        .expect("insert chunks");
    let admitted = storage.register_new_chunks().expect("register");
    assert_eq!(admitted.len(), 2, "overlap loser rejected at registration");

    let copied = storage
        .copy_dataset_chunks(&dataset("src"), &dataset("dst"))
        .expect("copy dataset");
    assert_eq!(copied, 2, "only the live chunks are cloned");

    // The clones enter unregistered; the next registration admits exactly them.
    let clones = storage.register_new_chunks().expect("register clones");
    assert_eq!(clones.len(), 2, "both clones admitted into the new dataset");
}

/// Two concurrent writers each insert chunks into the *same* pair of datasets, presenting the pair
/// in opposite order. The ingest path locks a batch's datasets in ascending-id order, so the two
/// writers can't hold-and-wait crosswise — both batches commit rather than one dying with a Postgres
/// deadlock (40P01). Regression guard for that ordering.
#[test]
fn concurrent_multi_dataset_inserts_do_not_deadlock() {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use scheduler_metadata::pg::registration::insert_chunks;

    use crate::scheduler_storage::{NewChunk, SchemaId};
    use sqlx::Connection;

    use crate::scheduler_storage::postgres::PostgresStorage;
    use crate::scheduler_storage::test_harness::pg_harness::fresh_db_url;

    fn nc(ds: &str, id: String, schema_id: SchemaId) -> NewChunk {
        NewChunk {
            dataset: Arc::new(format!("s3://{ds}")),
            id: Arc::new(id),
            size: 1,
            blocks: 0..=1,
            schema_id,
            tables_present: None,
            last_block_hash: None,
            last_block_timestamp: None,
        }
    }

    let url = fresh_db_url(
        "lock_deadlock",
        super::TEST_ID.fetch_add(1, Ordering::Relaxed),
    );

    // Seed both datasets (each gets a default current schema) through the real path, capture each
    // dataset's current schema id (chunks must pin one), then drop the storage so its scheduler
    // session lock is released before the raw writers connect.
    let (schema_a, schema_b) = {
        let mut seed = PostgresStorage::connect(&url).expect("connect seed");
        seed.insert_new_datasets(vec![
            new_dataset("a", DatasetSchema::default()),
            new_dataset("b", DatasetSchema::default()),
        ])
        .expect("seed datasets");
        (
            super::current_schema_id(&mut seed, "s3://a".to_string()),
            super::current_schema_id(&mut seed, "s3://b".to_string()),
        )
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build runtime");

    rt.block_on(async {
        let mut handles = Vec::new();
        for task in 0..2u32 {
            let url = url.clone();
            handles.push(tokio::spawn(async move {
                let mut conn = sqlx::PgConnection::connect(&url)
                    .await
                    .expect("connect writer");
                // task 0 names a,b; task 1 names b,a — opposite input orders on the same pair.
                let order = if task == 0 { ["a", "b"] } else { ["b", "a"] };
                let schema_of = |d: &str| if d == "a" { schema_a } else { schema_b };
                for i in 0..40u32 {
                    let chunks = vec![
                        nc(
                            order[0],
                            format!("t{task}-i{i}-{}", order[0]),
                            schema_of(order[0]),
                        ),
                        nc(
                            order[1],
                            format!("t{task}-i{i}-{}", order[1]),
                            schema_of(order[1]),
                        ),
                    ];
                    insert_chunks(&mut conn, &chunks, 10_000)
                        .await
                        .expect("concurrent multi-dataset insert must not deadlock");
                }
            }));
        }
        for h in handles {
            h.await.expect("writer task panicked");
        }
    });
}

/// Mirrored by `in_memory/tests/mvcc_protocol.rs::marked_chunk_frees_its_range_for_registration`:
/// the SQL liveness filter (`LIVE_ADMITTED`) and in_memory's `live()` must agree that a chunk
/// marked for removal stops claiming its block range.
#[test]
fn marked_chunk_frees_its_range_for_registration() {
    let mut storage = fresh_storage("marked_frees_range");
    storage
        .insert_new_datasets(vec![new_dataset("ds", DatasetSchema::default())])
        .expect("dataset");
    storage
        .insert_new_chunks(vec![chunk_with_blocks("ds", 1, 100, 0..=100)])
        .expect("insert A");
    let admitted = storage.register_new_chunks().expect("register A");
    assert_eq!(admitted.len(), 1);
    let a_pk = admitted[0];

    // Overlapping B loses to live A.
    storage
        .insert_new_chunks(vec![chunk_with_blocks("ds", 2, 100, 50..=150)])
        .expect("insert B");
    assert_eq!(storage.register_new_chunks().expect("register B").len(), 0);

    // Marked-for-removal A no longer claims its range; same-range C is admitted.
    storage.mark_for_removal(a_pk, 1).expect("mark A");
    storage
        .insert_new_chunks(vec![chunk_with_blocks("ds", 3, 100, 50..=150)])
        .expect("insert C");
    assert_eq!(storage.register_new_chunks().expect("register C").len(), 1);
}
