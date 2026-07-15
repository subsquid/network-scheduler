//! Tests for dataset schemas: chunks are stamped with their dataset's current schema at insert
//! (unless the writer pins one explicitly), schemas are deduped per dataset by canonical content
//! hash, and the DB enforces the pin invariants. Each test gets a fresh database so cases run
//! concurrently.

use std::collections::BTreeMap;

use super::{current_schema_id, fresh_storage, register_chunk};
use crate::scheduler_storage::algorithm::IdealMapping;
use crate::scheduler_storage::postgres::PostgresStorage;
use crate::scheduler_storage::test_harness::inspect::StorageInspect;
use crate::scheduler_storage::test_harness::utils::{StaticSchedulingAlgorithm, dataset, worker};
use crate::scheduler_storage::{
    BundleId, ChunkPk, SchedulerStorage, SchemaBundle, SchemaId, StorageError, WorkerAssignment,
    WorkerPk,
};
use crate::types::{DatasetSchema, TableSchema};

fn schema_with_tables(tables: &[&str]) -> DatasetSchema {
    DatasetSchema::new(
        tables
            .iter()
            .map(|t| ((*t).to_owned(), TableSchema::default()))
            .collect(),
    )
}

fn schema_one_table(table: &str, fields: &[&str], default_fields: &[&str]) -> DatasetSchema {
    let mut tables = BTreeMap::new();
    tables.insert(
        table.to_owned(),
        TableSchema {
            fields: fields.iter().map(|s| (*s).to_owned()).collect(),
            default_fields: default_fields.iter().map(|s| (*s).to_owned()).collect(),
        },
    );
    DatasetSchema::new(tables)
}

/// Total schema rows for a dataset (current + superseded).
fn schema_count(storage: &mut PostgresStorage, ds: String) -> i64 {
    storage
        .with_conn(async move |conn| {
            let n: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM schemas s JOIN datasets d ON d.id = s.dataset_id \
                 WHERE d.name = $1",
            )
            .bind(&ds)
            .fetch_one(&mut *conn)
            .await
            .unwrap();
            Ok::<_, StorageError>(n)
        })
        .unwrap()
}

/// Simulate ingestion stamping a chunk with the schema it was written under.
fn set_chunk_schema(storage: &mut PostgresStorage, pk: ChunkPk, schema_id: SchemaId) {
    storage
        .with_conn(async move |conn| {
            sqlx::query("UPDATE chunks SET schema_id = $1 WHERE chunk_pk = $2")
                .bind(schema_id)
                .bind(pk)
                .execute(&mut *conn)
                .await
                .unwrap();
            Ok::<_, StorageError>(())
        })
        .unwrap();
}

/// Pin a chunk to `schema_id` and set its `tables_present` bitmap (production population is a
/// follow-up; this stands in for it). Both are set together: the bitmap is positional over the
/// schema's tables, so the DB rejects one without the other. Bit `i` = `present[i]`.
fn set_chunk_tables_present(
    storage: &mut PostgresStorage,
    pk: ChunkPk,
    schema_id: SchemaId,
    present: &[bool],
) {
    let mask = bit_vec::BitVec::from_fn(present.len(), |i| present[i]);
    storage
        .with_conn(async move |conn| {
            sqlx::query(
                "UPDATE chunks SET schema_id = $1, tables_present = $2 WHERE chunk_pk = $3",
            )
            .bind(schema_id)
            .bind(mask)
            .bind(pk)
            .execute(&mut *conn)
            .await
            .unwrap();
            Ok::<_, StorageError>(())
        })
        .unwrap();
}

/// Place every given chunk on every given worker and run one cycle.
fn schedule(
    storage: &mut PostgresStorage,
    chunk_pks: &[ChunkPk],
    worker_ids: &[WorkerPk],
    at: u64,
) -> WorkerAssignment {
    storage.register_new_chunks().expect("register new chunks");
    let workers: Vec<WorkerPk> = worker_ids.to_vec();
    let mapping: IdealMapping = chunk_pks.iter().map(|&pk| (pk, workers.clone())).collect();
    let algorithm = StaticSchedulingAlgorithm { mapping };
    storage
        .run_scheduling_cycle(&algorithm, &(), at, 60)
        .expect("scheduling succeeds")
}

fn one_worker(storage: &mut PostgresStorage) -> Vec<WorkerPk> {
    storage
        .update_worker_set(&[worker(1, None)], 0, 10_000)
        .expect("upsert workers");
    storage
        .get_workers(|_| true)
        .iter()
        .map(|v| v.worker_id)
        .collect()
}

#[test]
fn chunk_tables_present_is_returned_with_schema_id() {
    let mut storage = fresh_storage("tables_present");
    storage
        .insert_new_datasets(vec![(
            dataset("ds"),
            schema_with_tables(&["blocks", "logs", "transactions"]),
        )])
        .expect("insert dataset");
    let pk = register_chunk(&mut storage, "ds", 1, 100);
    let schema_id = current_schema_id(&mut storage, dataset("ds"));
    set_chunk_tables_present(&mut storage, pk, schema_id, &[true, false, true]);
    let workers = one_worker(&mut storage);

    let wa = schedule(&mut storage, &[pk], &workers, 100);
    let chunk = wa.chunks[&pk].clone();
    assert_eq!(
        chunk.schema_id, schema_id,
        "the published chunk carries its effective schema id"
    );
    assert_eq!(
        chunk.tables_present,
        Some(bit_vec::BitVec::from_fn(3, |i| i != 1)),
        "and the presence bitmap the worker will interpret"
    );
}

/// `active_schema_bundle` (the DB predicate) holds exactly the in-play schemas: a worker-held
/// (then portal-served) chunk's schema is included, an unplaced chunk's is excluded, and the id is
/// content-addressed over that set.
#[test]
fn active_schema_bundle_holds_only_in_play_schemas() {
    let mut storage = fresh_storage("active_bundle");
    storage
        .insert_new_datasets(vec![
            (dataset("a"), schema_with_tables(&["blocks"])),
            (dataset("b"), schema_with_tables(&["logs"])),
        ])
        .expect("insert datasets");
    let a_pk = register_chunk(&mut storage, "a", 1, 100);
    let _b_pk = register_chunk(&mut storage, "b", 1, 100);
    let a_schema = current_schema_id(&mut storage, dataset("a"));
    let workers = one_worker(&mut storage);

    // Place only "a"; "b" stays registered but unplaced (in no worker or portal assignment).
    let wa = schedule(&mut storage, &[a_pk], &workers, 100);
    let bundle = SchemaBundle::generate(&storage).unwrap();
    // Whole-bundle content: exactly a's schema decoded from jsonb; b's is excluded, unplaced.
    assert_eq!(
        bundle.schemas(),
        &BTreeMap::from([(a_schema, schema_with_tables(&["blocks"]))]),
    );
    assert_eq!(bundle.id(), BundleId::from_schema_ids([a_schema]));

    // Promote "a" to the portal — still in play, so the set is unchanged.
    storage.confirm_worker_assignment(wa.id, 100).unwrap();
    storage.run_visibility_cycle(150).unwrap();
    assert_eq!(
        SchemaBundle::generate(&storage).unwrap().id(),
        bundle.id(),
        "portal-served keeps the same in-play schema set",
    );
}

#[test]
fn load_schemas_returns_by_id_or_all() {
    let mut storage = fresh_storage("schema_load");
    let schema = schema_with_tables(&["blocks", "transactions"]);
    storage
        .insert_new_datasets(vec![(dataset("ds"), schema.clone())])
        .expect("insert dataset");
    let first_id = current_schema_id(&mut storage, dataset("ds"));
    let second = schema_with_tables(&["blocks"]);
    storage
        .set_dataset_schema(&dataset("ds"), second.clone())
        .expect("advance schema");
    let second_id = current_schema_id(&mut storage, dataset("ds"));

    let by_id = storage.load_schemas(Some(&[first_id])).expect("load by id");
    assert_eq!(by_id.get(&first_id), Some(&schema));
    assert_eq!(by_id.len(), 1);

    // None = every schema row, superseded ones included.
    let all = storage.load_schemas(None).expect("load all");
    assert_eq!(all.get(&first_id), Some(&schema));
    assert_eq!(all.get(&second_id), Some(&second));
}

#[test]
fn set_dataset_schema_affects_only_future_chunks() {
    // Also covers stamping: the first chunk is stamped with the (empty default) schema current at
    // its insert, bitmap NULL -> None.
    let mut storage = fresh_storage("schema_set");
    storage
        .insert_new_datasets(vec![(dataset("ds"), DatasetSchema::default())])
        .expect("insert dataset");
    let old_pk = register_chunk(&mut storage, "ds", 1, 100);
    let workers = one_worker(&mut storage);
    let first_schema_id = current_schema_id(&mut storage, dataset("ds"));

    storage
        .set_dataset_schema(&dataset("ds"), schema_with_tables(&["blocks", "logs"]))
        .expect("set schema");
    let new_pk = register_chunk(&mut storage, "ds", 2, 100);
    let second_schema_id = current_schema_id(&mut storage, dataset("ds"));
    assert_ne!(first_schema_id, second_schema_id);

    // The existing chunk keeps the schema it was stamped with; only the new one gets the update.
    let wa = schedule(&mut storage, &[old_pk, new_pk], &workers, 200);
    assert_eq!(wa.chunks[&old_pk].schema_id, first_schema_id);
    assert_eq!(wa.chunks[&old_pk].tables_present, None);
    assert_eq!(wa.chunks[&new_pk].schema_id, second_schema_id);
}

#[test]
fn set_dataset_schema_unknown_dataset_errors() {
    let mut storage = fresh_storage("schema_unknown");
    let err = storage.set_dataset_schema(&dataset("nope"), DatasetSchema::default());
    assert!(matches!(err, Err(StorageError::Database(_))));
}

#[test]
fn schema_dedup_is_per_dataset() {
    let mut storage = fresh_storage("schema_dedup");
    let schema = schema_with_tables(&["blocks", "transactions"]);
    storage
        .insert_new_datasets(vec![
            (dataset("a"), schema.clone()),
            (dataset("b"), schema.clone()),
        ])
        .expect("insert datasets");
    // Re-setting "a" with byte-identical content reuses its row (no new row); dedup is per-dataset.
    storage
        .set_dataset_schema(&dataset("a"), schema.clone())
        .expect("set schema");

    let a_id = current_schema_id(&mut storage, dataset("a"));
    let b_id = current_schema_id(&mut storage, dataset("b"));
    let total: i64 = storage
        .with_conn(async move |conn| {
            Ok::<_, StorageError>(
                sqlx::query_scalar("SELECT count(*) FROM schemas")
                    .fetch_one(&mut *conn)
                    .await
                    .unwrap(),
            )
        })
        .unwrap();

    // One row per dataset: no global sharing, and re-setting "a" added no third row.
    assert_eq!(total, 2, "schemas are scoped per dataset");
    assert_ne!(
        a_id, b_id,
        "identical content in different datasets gets distinct rows"
    );
}

#[test]
fn invalid_schema_rejected() {
    let mut storage = fresh_storage("schema_invalid");
    // default_fields references a column not in fields.
    let bad = schema_one_table("blocks", &["a"], &["missing"]);
    let err = storage.insert_new_datasets(vec![(dataset("ds"), bad)]);
    assert!(matches!(err, Err(StorageError::Database(_))));
}

#[test]
fn one_cycle_resolves_each_chunk_against_its_own_schema() {
    // Mixes a chunk re-stamped to a now-superseded schema (the ingestion path) with an
    // insert-stamped one, published in a single cycle.
    let mut storage = fresh_storage("schema_multi");
    storage
        .insert_new_datasets(vec![
            (dataset("a"), schema_with_tables(&["blocks"])),
            (dataset("b"), schema_with_tables(&["blocks", "logs"])),
        ])
        .expect("insert datasets");
    let pk_a = register_chunk(&mut storage, "a", 1, 100);
    let pk_b = register_chunk(&mut storage, "b", 2, 100);

    // Pin A to a's schema, then advance a's current schema so A's pin points at a superseded row.
    let a_v1 = current_schema_id(&mut storage, dataset("a"));
    set_chunk_schema(&mut storage, pk_a, a_v1);
    storage
        .set_dataset_schema(
            &dataset("a"),
            schema_with_tables(&["blocks", "transactions"]),
        )
        .expect("advance a");

    let workers = one_worker(&mut storage);
    let wa = schedule(&mut storage, &[pk_a, pk_b], &workers, 100);
    assert_eq!(
        wa.chunks[&pk_a].schema_id, a_v1,
        "A is pinned to its (now superseded) written schema"
    );
    let b_current = current_schema_id(&mut storage, dataset("b"));
    assert_eq!(
        wa.chunks[&pk_b].schema_id, b_current,
        "B keeps its insert stamp"
    );
}

#[test]
fn one_current_schema_per_dataset_enforced() {
    let mut storage = fresh_storage("schema_unique");
    storage
        .insert_new_datasets(vec![(dataset("ds"), schema_with_tables(&["blocks"]))])
        .expect("insert dataset");
    // The dataset already has a current schema; a direct second current (superseded_at NULL) row
    // must violate the partial unique index, independent of ensure_current_schema's logic.
    let res = storage.with_conn(async move |conn| -> Result<(), StorageError> {
        let dataset_id: i16 = sqlx::query_scalar("SELECT id FROM datasets WHERE name = $1")
            .bind(dataset("ds"))
            .fetch_one(&mut *conn)
            .await
            .map_err(anyhow::Error::new)?;
        sqlx::query("INSERT INTO schemas (dataset_id, hash, schema) VALUES ($1, $2, '{}'::jsonb)")
            .bind(dataset_id)
            .bind(vec![9u8; 32]) // distinct hash, superseded_at defaults NULL
            .execute(&mut *conn)
            .await
            .map_err(anyhow::Error::new)?;
        Ok(())
    });
    assert!(
        matches!(res, Err(StorageError::Database(_))),
        "a second current schema for one dataset must violate schemas_one_current_per_dataset"
    );
}

/// Run `sql` binding the chunk pk plus one extra value, expecting a constraint violation whose
/// error names `constraint`.
fn assert_chunk_update_rejected(
    storage: &mut PostgresStorage,
    sql: &'static str,
    pk: ChunkPk,
    bind: i32,
    constraint: &str,
) {
    let res = storage.with_conn(async move |conn| -> Result<(), StorageError> {
        sqlx::query(sql)
            .bind(pk)
            .bind(bind)
            .execute(&mut *conn)
            .await
            .map_err(anyhow::Error::new)?;
        Ok(())
    });
    let err = res.expect_err("the update must be rejected");
    assert!(
        format!("{err:?}").contains(constraint),
        "expected a {constraint} violation, got: {err:?}"
    );
}

#[test]
fn chunk_cannot_pin_another_datasets_schema() {
    // schema_id is stamped by clients outside this repo, so the composite FK is the enforcement
    // point for keeping a chunk's schema pin within its dataset.
    let mut storage = fresh_storage("schema_cross_dataset");
    storage
        .insert_new_datasets(vec![
            (dataset("a"), schema_with_tables(&["blocks"])),
            (dataset("b"), schema_with_tables(&["blocks", "logs"])),
        ])
        .expect("insert datasets");
    let pk_a = register_chunk(&mut storage, "a", 1, 100);
    let schema_b = current_schema_id(&mut storage, dataset("b"));

    assert_chunk_update_rejected(
        &mut storage,
        "UPDATE chunks SET schema_id = $2 WHERE chunk_pk = $1",
        pk_a,
        schema_b.0,
        "chunks_schema_same_dataset",
    );
}

#[test]
fn reactivating_an_equivalent_schema_serves_the_originally_stored_payload() {
    // Dedup is by canonical hash but the stored json is first-writer-wins: reverting via a
    // reordered-but-equivalent schema reactivates the old row, whose payload — not the one just
    // passed — is what reads then serve.
    let mut storage = fresh_storage("schema_reactivate_payload");
    storage
        .insert_new_datasets(vec![(
            dataset("ds"),
            schema_one_table("blocks", &["b", "a"], &[]),
        )])
        .expect("insert dataset");
    let id1 = current_schema_id(&mut storage, dataset("ds"));
    storage
        .set_dataset_schema(&dataset("ds"), schema_with_tables(&["other"]))
        .expect("advance");
    storage
        .set_dataset_schema(&dataset("ds"), schema_one_table("blocks", &["a", "b"], &[]))
        .expect("revert via canonically equal schema");

    assert_eq!(current_schema_id(&mut storage, dataset("ds")), id1);
    assert_eq!(schema_count(&mut storage, dataset("ds")), 2);
    let stored: DatasetSchema = storage
        .with_conn(async move |conn| {
            let json: String = sqlx::query_scalar(
                "SELECT s.schema::text FROM schemas s JOIN datasets d ON d.id = s.dataset_id \
                 WHERE d.name = $1 AND s.superseded_at IS NULL",
            )
            .bind(dataset("ds"))
            .fetch_one(&mut *conn)
            .await
            .unwrap();
            Ok::<_, StorageError>(serde_json::from_str(&json).unwrap())
        })
        .unwrap();
    assert_eq!(
        stored.tables()["blocks"].fields,
        vec!["b".to_owned(), "a".to_owned()],
        "the reactivated row keeps its original field order, not the re-applied one"
    );
}
