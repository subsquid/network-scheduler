//! Integration tests for the async pooled [`PgIngest`] facade, driven against a real migrated
//! Postgres via the process-wide harness (`fresh_db_url`).
//!
//! Driving idiom: `fresh_db_url` and `PostgresStorage` both `block_on` their own runtimes, and a
//! nested `block_on` panics — so each case is a sync `#[test]`, `PgIngest` calls run on a test-local
//! runtime, and a scheduler admission sweep (`admit`) runs in the sync gaps between them.

use std::future::Future;
use std::sync::atomic::Ordering;

use sqlx::Connection;

use scheduler_metadata::pg::PgIngest;
use scheduler_metadata::{ChunkStatus, Correction, DatasetOutcome, IngestChunk};
use scheduler_metadata::{
    DatasetPk, DatasetSchema, IngestError, NewDataset, SchemaId, TableSchema,
};

use crate::scheduler_storage::SchedulerStorage;
use crate::scheduler_storage::postgres::PostgresStorage;
use crate::scheduler_storage::test_harness::pg_harness::fresh_db_url;

// --- inputs -----------------------------------------------------------------------------------

fn schema(tables: &[(&str, &[&str])]) -> DatasetSchema {
    let mut m = std::collections::BTreeMap::new();
    for (name, fields) in tables {
        m.insert(
            (*name).to_owned(),
            TableSchema {
                fields: fields.iter().map(|f| (*f).to_owned()).collect(),
                default_fields: Vec::new(),
            },
        );
    }
    DatasetSchema::new(m)
}

/// Default single-table schema for cases that don't exercise table mapping.
fn plain_schema() -> DatasetSchema {
    schema(&[("blocks", &["number"])])
}

fn ic(id: &str, first: u64, last: u64, s: SchemaId) -> IngestChunk {
    IngestChunk {
        id: id.to_owned(),
        first_block: first,
        last_block: last,
        size: 1,
        schema_id: s,
        tables: None,
        last_block_hash: None,
        last_block_timestamp: None,
    }
}

fn ic_tables(id: &str, first: u64, last: u64, s: SchemaId, tables: &[&str]) -> IngestChunk {
    let mut c = ic(id, first, last, s);
    c.tables = Some(tables.iter().map(|t| (*t).to_owned()).collect());
    c
}

// --- harness ----------------------------------------------------------------------------------

/// A fresh DB with one connected `PgIngest` and its own runtime.
struct IngestHarness {
    url: String,
    rt: tokio::runtime::Runtime,
    ing: PgIngest,
}

impl IngestHarness {
    fn new(name: &str) -> Self {
        let url = fresh_db_url(name, super::TEST_ID.fetch_add(1, Ordering::Relaxed));
        let rt = current_rt();
        let ing = rt
            .block_on(PgIngest::connect(&url))
            .expect("connect PgIngest");
        Self { url, rt, ing }
    }

    fn block<F: Future>(&self, f: F) -> F::Output {
        self.rt.block_on(f)
    }

    /// Create `ds` (seeds `sch` as both write and read schema) and return the write schema id.
    /// Re-registering the same content dedups to the seeded write schema's id.
    fn setup(&self, ds: &str, sch: DatasetSchema) -> SchemaId {
        let _ = self
            .block(self.ing.create_dataset(NewDataset::new(ds, sch.clone())))
            .expect("create dataset");
        self.block(self.ing.register_write_schema(ds, sch))
            .expect("register write schema")
    }

    /// Run one scheduler registration sweep (admits/rejects the pending chunks). Sync context: it
    /// owns its own runtime, so calling it inside `block` would nest-panic.
    fn admit(&self) {
        let mut store = PostgresStorage::connect(&self.url).expect("connect scheduler");
        store.register_new_chunks().expect("register_new_chunks");
    }

    /// Insert via the scheduler's lock-free discovery path — the only way left to land an
    /// overlapping *pending* chunk for the admission sweep to reject (the API path rejects it
    /// synchronously). Sync context, like [`Self::admit`].
    fn discovery_insert(&self, ds: &str, id: &str, first: u64, last: u64, s: SchemaId) {
        let mut store = PostgresStorage::connect(&self.url).expect("connect scheduler");
        store
            .insert_new_chunks(vec![crate::scheduler_storage::NewChunk {
                dataset: std::sync::Arc::new(ds.to_owned()),
                id: std::sync::Arc::new(id.to_owned()),
                size: 1,
                blocks: first..=last,
                schema_id: s,
                tables_present: None,
                last_block_hash: None,
                last_block_timestamp: None,
            }])
            .expect("discovery insert");
    }

    /// Run `f` against a fresh raw connection (for committed-state assertions).
    fn raw_query<F, Fut, T>(&self, f: F) -> T
    where
        F: FnOnce(sqlx::PgConnection) -> Fut,
        Fut: Future<Output = T>,
    {
        self.block(async {
            let c = raw(&self.url).await;
            f(c).await
        })
    }

    fn chunk_count(&self, ds: &str, id: &str) -> i64 {
        let (ds, id) = (ds.to_owned(), id.to_owned());
        self.raw_query(|mut c| async move { count_chunk(&mut c, &ds, &id).await })
    }

    fn correction_count(&self) -> i64 {
        self.raw_query(|mut c| async move { correction_count(&mut c).await })
    }
}

fn current_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build runtime")
}

async fn raw(url: &str) -> sqlx::PgConnection {
    sqlx::PgConnection::connect(url).await.expect("connect raw")
}

async fn count_chunk(c: &mut sqlx::PgConnection, ds: &str, id: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM chunks c JOIN datasets d ON d.id = c.dataset_id \
         WHERE d.name = $1 AND c.chunk_id = $2",
    )
    .bind(ds)
    .bind(id)
    .fetch_one(c)
    .await
    .unwrap()
}

async fn tables_present(c: &mut sqlx::PgConnection, ds: &str, id: &str) -> Option<bit_vec::BitVec> {
    sqlx::query_scalar(
        "SELECT c.tables_present FROM chunks c JOIN datasets d ON d.id = c.dataset_id \
         WHERE d.name = $1 AND c.chunk_id = $2",
    )
    .bind(ds)
    .bind(id)
    .fetch_one(c)
    .await
    .unwrap()
}

async fn schema_id_of(c: &mut sqlx::PgConnection, ds: &str, id: &str) -> i32 {
    sqlx::query_scalar(
        "SELECT c.schema_id FROM chunks c JOIN datasets d ON d.id = c.dataset_id \
         WHERE d.name = $1 AND c.chunk_id = $2",
    )
    .bind(ds)
    .bind(id)
    .fetch_one(c)
    .await
    .unwrap()
}

/// Count the dataset's current (non-superseded) READ schemas — must always be exactly one.
async fn current_read_schema_count(c: &mut sqlx::PgConnection, ds: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM read_schemas s JOIN datasets d ON d.id = s.dataset_id \
         WHERE d.name = $1 AND s.superseded_at IS NULL",
    )
    .bind(ds)
    .fetch_one(c)
    .await
    .unwrap()
}

/// Count the dataset's WRITE schemas (the immutable registry).
async fn schema_count(c: &mut sqlx::PgConnection, ds: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM schemas s JOIN datasets d ON d.id = s.dataset_id WHERE d.name = $1",
    )
    .bind(ds)
    .fetch_one(c)
    .await
    .unwrap()
}

/// Whether a READ schema row has been superseded.
async fn is_read_superseded(c: &mut sqlx::PgConnection, read_schema_id: i32) -> bool {
    sqlx::query_scalar("SELECT superseded_at IS NOT NULL FROM read_schemas WHERE id = $1")
        .bind(read_schema_id)
        .fetch_one(c)
        .await
        .unwrap()
}

async fn correction_exists(c: &mut sqlx::PgConnection, old_id: &str, new_id: &str) -> bool {
    sqlx::query_scalar(
        "SELECT EXISTS ( \
           SELECT 1 FROM chunk_corrections cc \
           JOIN chunks oc ON oc.chunk_pk = cc.old_chunk_pk \
           JOIN chunks nc ON nc.chunk_pk = cc.new_chunk_pk \
           WHERE oc.chunk_id = $1 AND nc.chunk_id = $2)",
    )
    .bind(old_id)
    .bind(new_id)
    .fetch_one(c)
    .await
    .unwrap()
}

async fn correction_count(c: &mut sqlx::PgConnection) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM chunk_corrections")
        .fetch_one(c)
        .await
        .unwrap()
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

fn strs(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| (*s).to_owned()).collect()
}

// --- create_dataset ---------------------------------------------------------------------------

#[test]
fn create_dataset_is_idempotent_on_repeat() {
    let h = IngestHarness::new("ingest_create_idem");
    let sch = plain_schema();
    let first = h
        .block(h.ing.create_dataset(NewDataset::new("ds", sch.clone())))
        .unwrap();
    let second = h
        .block(h.ing.create_dataset(NewDataset::new("ds", sch)))
        .unwrap();
    assert_eq!(first, DatasetOutcome::Created);
    assert_eq!(second, DatasetOutcome::AlreadyExists);

    h.raw_query(|mut c| async move {
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM datasets WHERE name = 'ds'")
            .fetch_one(&mut c)
            .await
            .unwrap();
        assert_eq!(n, 1);
        assert_eq!(current_read_schema_count(&mut c, "ds").await, 1);
        assert_eq!(schema_count(&mut c, "ds").await, 1);
    });
}

// --- insert_chunks: duplicates ----------------------------------------------------------------

#[test]
fn insert_chunks_reports_duplicates_not_inserted() {
    let h = IngestHarness::new("ingest_dup");
    let s = h.setup("ds", plain_schema());
    // A fresh pair reports no duplicates (both inserted).
    let first = h
        .block(
            h.ing
                .insert_chunks("ds", vec![ic("A", 0, 9, s), ic("B", 10, 19, s)]),
        )
        .unwrap();
    assert!(first.is_empty());

    // Re-posting the existing pair plus a new chunk: the pair comes back as duplicates (not
    // overwritten); the new one is inserted, so it is absent from the returned list.
    let second = h
        .block(h.ing.insert_chunks(
            "ds",
            vec![ic("A", 0, 9, s), ic("B", 10, 19, s), ic("C", 20, 29, s)],
        ))
        .unwrap();
    assert_eq!(sorted(second), strs(&["A", "B"]));

    assert_eq!(h.chunk_count("ds", "A"), 1);
}

// --- insert_chunks: overlap abort -------------------------------------------------------------

#[test]
fn insert_chunks_aborts_whole_batch_on_existing_live_overlap() {
    let h = IngestHarness::new("ingest_overlap_live");
    let s = h.setup("ds", plain_schema());
    h.block(h.ing.insert_chunks("ds", vec![ic("A", 0, 20, s)]))
        .unwrap();
    h.admit(); // A is now live.

    let err = h
        .block(
            h.ing
                .insert_chunks("ds", vec![ic("B", 10, 30, s), ic("C", 100, 120, s)]),
        )
        .unwrap_err();
    match err {
        IngestError::RangeOverlap {
            chunk_id,
            conflicts_with,
            ..
        } => {
            assert_eq!(chunk_id, "B");
            assert!(conflicts_with.contains(&"A".to_owned()));
        }
        other => panic!("expected RangeOverlap, got {other:?}"),
    }
    h.raw_query(|mut c| async move {
        assert_eq!(count_chunk(&mut c, "ds", "A").await, 1);
        assert_eq!(count_chunk(&mut c, "ds", "B").await, 0);
        assert_eq!(count_chunk(&mut c, "ds", "C").await, 0);
    });
}

#[test]
fn insert_chunks_aborts_on_within_batch_overlap() {
    let h = IngestHarness::new("ingest_overlap_batch");
    let s = h.setup("ds", plain_schema());
    let err = h
        .block(
            h.ing
                .insert_chunks("ds", vec![ic("B", 10, 30, s), ic("C", 20, 40, s)]),
        )
        .unwrap_err();
    assert!(matches!(err, IngestError::RangeOverlap { .. }), "{err:?}");
    h.raw_query(|mut c| async move {
        assert_eq!(count_chunk(&mut c, "ds", "B").await, 0);
        assert_eq!(count_chunk(&mut c, "ds", "C").await, 0);
    });
}

#[test]
fn insert_chunks_rejects_duplicate_id_within_batch() {
    let h = IngestHarness::new("ingest_dup_in_batch");
    let s = h.setup("ds", plain_schema());
    // Same id twice with disjoint ranges: which payload wins is arbitrary, so reject and store
    // nothing (else ON CONFLICT DO NOTHING silently drops one range and misreports it inserted).
    let err = h
        .block(
            h.ing
                .insert_chunks("ds", vec![ic("A", 0, 99, s), ic("A", 200, 299, s)]),
        )
        .unwrap_err();
    assert!(
        matches!(err, IngestError::DuplicateInBatch { .. }),
        "{err:?}"
    );
    assert_eq!(h.chunk_count("ds", "A"), 0, "nothing committed");
}

#[test]
fn insert_chunks_aborts_on_pending_overlap() {
    let h = IngestHarness::new("ingest_overlap_pending");
    let s = h.setup("ds", plain_schema());
    // A stays pending (never admitted). The authoritative probe still counts a committed pending
    // chunk as a claimant, so B — which overlaps A — is rejected synchronously here, instead of both
    // committing and one being silently rejected later by the async admission gate (the closed gap).
    h.block(h.ing.insert_chunks("ds", vec![ic("A", 10, 20, s)]))
        .unwrap();
    let err = h
        .block(h.ing.insert_chunks("ds", vec![ic("B", 15, 25, s)]))
        .unwrap_err();
    match err {
        IngestError::RangeOverlap {
            chunk_id,
            conflicts_with,
            ..
        } => {
            assert_eq!(chunk_id, "B");
            assert!(conflicts_with.contains(&"A".to_owned()));
        }
        other => panic!("expected RangeOverlap, got {other:?}"),
    }
    assert_eq!(h.chunk_count("ds", "B"), 0, "B must not be committed");
}

// --- insert_chunks: tables-by-name ------------------------------------------------------------

#[test]
fn insert_chunks_maps_table_names_to_sorted_positional_bitmap() {
    let h = IngestHarness::new("ingest_tables_map");
    // Sorted order: blocks=0, logs=1, transactions=2.
    let s = h.setup(
        "ds",
        schema(&[
            ("blocks", &["n"]),
            ("logs", &["n"]),
            ("transactions", &["n"]),
        ]),
    );
    // Unsorted, subset input; expect bits 0 and 2 set.
    h.block(h.ing.insert_chunks(
        "ds",
        vec![ic_tables("A", 0, 9, s, &["transactions", "blocks"])],
    ))
    .unwrap();
    h.raw_query(|mut c| async move {
        let bv = tables_present(&mut c, "ds", "A").await.expect("bitmap set");
        let mut want = bit_vec::BitVec::from_elem(3, false);
        want.set(0, true);
        want.set(2, true);
        assert_eq!(bv, want);
    });
}

#[test]
fn insert_chunks_stores_null_bitmap_for_all_present() {
    // "All tables present" is stored as the NULL bitmap, whether `tables` is omitted (A) or lists
    // every table explicitly (B).
    let h = IngestHarness::new("ingest_tables_null");
    let s = h.setup("ds", schema(&[("blocks", &["n"]), ("logs", &["n"])]));
    h.block(h.ing.insert_chunks(
        "ds",
        vec![
            ic("A", 0, 9, s),
            ic_tables("B", 10, 19, s, &["logs", "blocks"]),
        ],
    ))
    .unwrap();
    h.raw_query(|mut c| async move {
        assert_eq!(tables_present(&mut c, "ds", "A").await, None);
        assert_eq!(tables_present(&mut c, "ds", "B").await, None);
    });
}

#[test]
fn insert_chunks_rejects_unknown_table_name() {
    let h = IngestHarness::new("ingest_tables_unknown");
    let s = h.setup("ds", schema(&[("blocks", &["n"])]));
    let err = h
        .block(
            h.ing
                .insert_chunks("ds", vec![ic_tables("A", 0, 9, s, &["blocks", "nope"])]),
        )
        .unwrap_err();
    match err {
        IngestError::UnknownTable { schema_id, table } => {
            assert_eq!(schema_id, s);
            assert_eq!(table, "nope");
        }
        other => panic!("expected UnknownTable, got {other:?}"),
    }
    assert_eq!(h.chunk_count("ds", "A"), 0);
}

// --- insert_chunks: schema pinning + validation -----------------------------------------------

#[test]
fn insert_chunks_accepts_non_latest_write_schema() {
    let h = IngestHarness::new("ingest_nonlatest");
    // Read schema (a,b) covers both write schemas below.
    let s_ab = h.setup("ds", schema(&[("blocks", &["a", "b"])]));
    let s_a = h
        .block(
            h.ing
                .register_write_schema("ds", schema(&[("blocks", &["a"])])),
        )
        .unwrap();
    assert_ne!(s_ab, s_a);
    // s_ab is no longer the latest write schema, but a chunk may still pin it (write schemas are
    // immutable; there is no "current" write pointer).
    h.block(h.ing.insert_chunks("ds", vec![ic("A", 0, 9, s_ab)]))
        .unwrap();
    h.raw_query(|mut c| async move {
        assert_eq!(schema_id_of(&mut c, "ds", "A").await, s_ab.0);
    });
}

#[test]
fn insert_chunks_rejects_nonexistent_schema_id() {
    let h = IngestHarness::new("ingest_bad_schema");
    h.setup("ds", plain_schema());
    let err = h
        .block(
            h.ing
                .insert_chunks("ds", vec![ic("A", 0, 9, SchemaId(9999))]),
        )
        .unwrap_err();
    assert!(
        matches!(err, IngestError::SchemaNotFound { schema_id } if schema_id == SchemaId(9999)),
        "{err:?}"
    );
    assert_eq!(h.chunk_count("ds", "A"), 0);
}

#[test]
fn insert_chunks_rejects_schema_id_of_another_dataset() {
    let h = IngestHarness::new("ingest_cross_schema");
    h.setup("ds1", schema(&[("blocks", &["a"])]));
    let s_b = h.setup("ds2", schema(&[("blocks", &["b"])]));
    let err = h
        .block(h.ing.insert_chunks("ds1", vec![ic("A", 0, 9, s_b)]))
        .unwrap_err();
    assert!(
        matches!(err, IngestError::SchemaNotFound { schema_id } if schema_id == s_b),
        "{err:?}"
    );
}

#[test]
fn insert_chunks_rejects_unknown_dataset() {
    let h = IngestHarness::new("ingest_no_ds");
    let err = h
        .block(
            h.ing
                .insert_chunks("ghost", vec![ic("A", 0, 9, SchemaId(1))]),
        )
        .unwrap_err();
    assert!(
        matches!(err, IngestError::DatasetNotFound { ref name } if name == "ghost"),
        "{err:?}"
    );
}

#[test]
fn insert_chunks_rejects_inverted_range() {
    let h = IngestHarness::new("ingest_bad_range");
    let s = h.setup("ds", plain_schema());
    let err = h
        .block(h.ing.insert_chunks("ds", vec![ic("A", 30, 10, s)]))
        .unwrap_err();
    assert!(
        matches!(err, IngestError::InvalidChunkRange { ref chunk_id, .. } if chunk_id == "A"),
        "{err:?}"
    );
    assert_eq!(h.chunk_count("ds", "A"), 0);
}

// --- write/read schema registry ---------------------------------------------------------------

#[test]
fn create_dataset_seeds_write_and_read_schema() {
    let h = IngestHarness::new("ingest_seed_both");
    let _ = h
        .block(h.ing.create_dataset(NewDataset::new("ds", plain_schema())))
        .unwrap();
    // Exactly one write schema and one current read schema, both = the seed.
    let ws = h.block(h.ing.list_write_schemas("ds")).unwrap();
    assert_eq!(ws.len(), 1);
    let (rid, rs) = h.block(h.ing.current_read_schema("ds")).unwrap();
    assert!(rid.0 > 0);
    assert_eq!(rs, plain_schema());
    // get_write_schema round-trips the seeded write schema by id.
    let got = h
        .block(h.ing.get_write_schema("ds", ws[0].schema_id))
        .unwrap();
    assert_eq!(got, plain_schema());
    h.raw_query(|mut c| async move {
        assert_eq!(schema_count(&mut c, "ds").await, 1);
        assert_eq!(current_read_schema_count(&mut c, "ds").await, 1);
    });
}

#[test]
fn get_write_schema_rejects_unknown_id() {
    let h = IngestHarness::new("ingest_get_write_unknown");
    h.setup("ds", plain_schema());
    let err = h
        .block(h.ing.get_write_schema("ds", SchemaId(9999)))
        .unwrap_err();
    assert!(
        matches!(err, IngestError::SchemaNotFound { schema_id } if schema_id == SchemaId(9999)),
        "{err:?}"
    );
}

#[test]
fn register_write_schema_is_idempotent_by_content_hash() {
    let h = IngestHarness::new("ingest_reg_write_idem");
    let s1 = h.setup("ds", plain_schema());
    let again = h
        .block(h.ing.register_write_schema("ds", plain_schema()))
        .unwrap();
    assert_eq!(s1, again);
    h.raw_query(|mut c| async move {
        assert_eq!(schema_count(&mut c, "ds").await, 1);
    });
}

#[test]
fn register_write_schema_rejects_unknown_dataset() {
    let h = IngestHarness::new("ingest_reg_write_no_ds");
    let err = h
        .block(h.ing.register_write_schema("ghost", plain_schema()))
        .unwrap_err();
    assert!(
        matches!(err, IngestError::DatasetNotFound { .. }),
        "{err:?}"
    );
}

/// v1: no compatibility gate — a write naming a field the read schema doesn't expose is accepted
/// (the field is hidden at read time, decoded from the chunk's pinned write schema).
#[test]
fn register_write_schema_not_gated_in_v1() {
    let h = IngestHarness::new("ingest_reg_write_gate");
    h.setup("ds", schema(&[("blocks", &["a"])])); // read schema = (a)
    // Write adds field `b`, not in the read schema — accepted (b is hidden).
    let s = h
        .block(
            h.ing
                .register_write_schema("ds", schema(&[("blocks", &["a", "b"])])),
        )
        .unwrap();
    assert!(s.0 > 0);
}

#[test]
fn promote_read_schema_supersedes_previous() {
    let h = IngestHarness::new("ingest_promote_read");
    h.setup("ds", schema(&[("blocks", &["a"])]));
    let (r1, _) = h.block(h.ing.current_read_schema("ds")).unwrap();
    let r2 = h
        .block(
            h.ing
                .promote_read_schema("ds", schema(&[("blocks", &["a", "b"])])),
        )
        .unwrap();
    assert_ne!(r1, r2);
    h.raw_query(|mut c| async move {
        assert!(is_read_superseded(&mut c, r1.0).await);
        assert!(!is_read_superseded(&mut c, r2.0).await);
        assert_eq!(current_read_schema_count(&mut c, "ds").await, 1);
    });
}

/// v1: no compatibility gate — promoting a read schema that drops a field a LIVE chunk carries is
/// accepted; the field is hidden at read time, not undecodable (the chunk decodes from its pinned
/// write schema).
#[test]
fn promote_read_schema_not_gated_in_v1() {
    let h = IngestHarness::new("ingest_promote_gate");
    let s = h.setup("ds", schema(&[("blocks", &["a", "b"])]));
    // A live chunk pins the (a,b) write schema.
    h.block(h.ing.insert_chunks("ds", vec![ic("A", 0, 9, s)]))
        .unwrap();
    // Dropping field `b` from the read schema is accepted — b is simply hidden.
    let r = h
        .block(
            h.ing
                .promote_read_schema("ds", schema(&[("blocks", &["a"])])),
        )
        .unwrap();
    assert!(r.0 > 0);
}

#[test]
fn concurrent_promote_read_schema_conflicts_instead_of_clobbering() {
    let h = IngestHarness::new("ingest_promote_race");
    h.setup("ds", schema(&[("blocks", &["a"])]));
    let (r1, _) = h.block(h.ing.current_read_schema("ds")).unwrap();

    // promote_read_schema runs at REPEATABLE READ with no advisory lock, so two writers that both
    // snapshot the same current read schema and then promote *different* ones can't both win: the
    // first commits, the second's supersede hits 40001 -> StorageError::Serialization (surfaced as
    // ConcurrentSchemaChange / 409). Driven at the SQL level to pin the interleaving.
    h.block(async {
        let ds_id: DatasetPk = {
            let mut c = raw(&h.url).await;
            sqlx::query_scalar("SELECT id FROM datasets WHERE name = 'ds'")
                .fetch_one(&mut c)
                .await
                .unwrap()
        };
        let mut c_win = raw(&h.url).await;
        let mut c_lose = raw(&h.url).await;

        // Loser snapshots first, before the winner commits.
        let mut t_lose = c_lose.begin().await.unwrap();
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *t_lose)
            .await
            .unwrap();
        sqlx::query("SELECT 1").execute(&mut *t_lose).await.unwrap(); // establish the snapshot

        // Winner supersedes r1, activates a different read schema, commits.
        let mut t_win = c_win.begin().await.unwrap();
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *t_win)
            .await
            .unwrap();
        let r_win = scheduler_metadata::pg::schema::promote_read_schema(
            &mut t_win,
            ds_id,
            &schema(&[("blocks", &["a", "b"])]),
        )
        .await
        .unwrap();
        t_win.commit().await.unwrap();

        // Loser (its snapshot predates the winner's commit) promotes yet another read schema -> 40001.
        let lost = scheduler_metadata::pg::schema::promote_read_schema(
            &mut t_lose,
            ds_id,
            &schema(&[("blocks", &["a", "c"])]),
        )
        .await;
        assert!(
            matches!(lost, Err(scheduler_metadata::StorageError::Serialization)),
            "expected the second writer to conflict, got {lost:?}",
        );
        drop(t_lose); // roll back the loser

        // Exactly one current read schema, and it is the winner's — not r1, not the loser's.
        let mut c = raw(&h.url).await;
        assert_eq!(current_read_schema_count(&mut c, "ds").await, 1);
        assert!(
            is_read_superseded(&mut c, r1.0).await,
            "original was superseded"
        );
        assert!(
            !is_read_superseded(&mut c, r_win.0).await,
            "winner is current"
        );
    });
}

#[test]
fn list_write_schemas_flags_active() {
    let h = IngestHarness::new("ingest_list_write");
    // Read schema (n, x) covers both write schemas below.
    let v1 = h.setup("ds", schema(&[("blocks", &["n", "x"])]));
    let v2 = h
        .block(
            h.ing
                .register_write_schema("ds", schema(&[("blocks", &["n"])])),
        )
        .unwrap();
    assert_ne!(v1, v2);
    // A chunk pins v2, keeping it active; v1 has no live chunk.
    h.block(h.ing.insert_chunks("ds", vec![ic("A", 0, 9, v2)]))
        .unwrap();

    let schemas = h.block(h.ing.list_write_schemas("ds")).unwrap();
    let by = |id| schemas.iter().find(|s| s.schema_id == id).unwrap();
    assert!(by(v2).active, "v2 is pinned by a live chunk");
    assert!(!by(v1).active, "no chunk pins v1");
}

// --- register_corrections ---------------------------------------------------------------------

#[test]
fn register_corrections_swaps_chunk_by_id() {
    let h = IngestHarness::new("ingest_correct_ok");
    let s = h.setup("ds", plain_schema());
    h.block(h.ing.insert_chunks("ds", vec![ic("A", 0, 20, s)]))
        .unwrap();
    h.block(h.ing.register_corrections(
        "ds",
        vec![Correction {
            old_chunk_id: "A".to_owned(),
            replacement: ic("A2", 0, 20, s),
        }],
    ))
    .unwrap();
    h.raw_query(|mut c| async move {
        assert!(correction_exists(&mut c, "A", "A2").await);
        assert_eq!(count_chunk(&mut c, "ds", "A2").await, 1);
    });
}

#[test]
fn register_corrections_rejects_range_change() {
    let h = IngestHarness::new("ingest_correct_range");
    let s = h.setup("ds", plain_schema());
    h.block(h.ing.insert_chunks("ds", vec![ic("A", 0, 20, s)]))
        .unwrap();
    let err = h
        .block(h.ing.register_corrections(
            "ds",
            vec![Correction {
                old_chunk_id: "A".to_owned(),
                replacement: ic("A2", 0, 25, s),
            }],
        ))
        .unwrap_err();
    match err {
        IngestError::CorrectionRejected { reason } => assert!(reason.contains("range"), "{reason}"),
        other => panic!("expected CorrectionRejected, got {other:?}"),
    }
    assert_eq!(h.chunk_count("ds", "A2"), 0);
}

#[test]
fn register_corrections_rejects_duplicate_replacement_id() {
    let h = IngestHarness::new("ingest_correct_dup");
    let s = h.setup("ds", plain_schema());
    // A and A2 both already exist (disjoint ranges so neither aborts the other).
    h.block(
        h.ing
            .insert_chunks("ds", vec![ic("A", 0, 20, s), ic("A2", 21, 40, s)]),
    )
    .unwrap();
    let err = h
        .block(h.ing.register_corrections(
            "ds",
            vec![Correction {
                old_chunk_id: "A".to_owned(),
                replacement: ic("A2", 0, 20, s),
            }],
        ))
        .unwrap_err();
    assert!(
        matches!(err, IngestError::CorrectionRejected { .. }),
        "{err:?}"
    );
    assert_eq!(h.correction_count(), 0);
}

#[test]
fn register_corrections_rejects_unknown_old_chunk_id() {
    let h = IngestHarness::new("ingest_correct_ghost");
    let s = h.setup("ds", plain_schema());
    let err = h
        .block(h.ing.register_corrections(
            "ds",
            vec![Correction {
                old_chunk_id: "ghost".to_owned(),
                replacement: ic("A2", 0, 20, s),
            }],
        ))
        .unwrap_err();
    match err {
        IngestError::CorrectionRejected { reason } => {
            assert!(reason.contains("unknown old chunk"), "{reason}");
        }
        other => panic!("expected CorrectionRejected, got {other:?}"),
    }
    h.raw_query(|mut c| async move {
        assert_eq!(correction_count(&mut c).await, 0);
        assert_eq!(count_chunk(&mut c, "ds", "A2").await, 0);
    });
}

// --- head -------------------------------------------------------------------------------------

#[test]
fn head_stored_counts_pending_and_admitted_excludes_rejected() {
    let h = IngestHarness::new("ingest_head_stored");
    let s = h.setup("ds", plain_schema());
    h.block(
        h.ing
            .insert_chunks("ds", vec![ic("A", 0, 20, s), ic("C", 100, 120, s)]),
    )
    .unwrap();
    // D and E overlap. The API path would reject E synchronously, so E arrives via the unguarded
    // discovery path; the admission sweep then keeps the earlier D and rejects E.
    h.block(h.ing.insert_chunks("ds", vec![ic("D", 200, 220, s)]))
        .unwrap();
    h.discovery_insert("ds", "E", 205, 225, s);
    h.admit();

    let head = h.block(h.ing.head("ds")).unwrap();
    assert_eq!(head.stored_head, Some(220), "E's 225 is rejected, excluded");

    // A fresh pending chunk lifts stored_head even before admission.
    h.block(h.ing.insert_chunks("ds", vec![ic("F", 300, 320, s)]))
        .unwrap();
    let head = h.block(h.ing.head("ds")).unwrap();
    assert_eq!(head.stored_head, Some(320));
}

#[test]
fn head_of_empty_dataset_is_none() {
    let h = IngestHarness::new("ingest_head_empty");
    h.setup("ds", plain_schema());
    let head = h.block(h.ing.head("ds")).unwrap();
    assert_eq!(head.stored_head, None);
}

#[test]
fn head_of_unknown_dataset_is_notfound() {
    let h = IngestHarness::new("ingest_head_unknown");
    let err = h.block(h.ing.head("ghost")).unwrap_err();
    assert!(
        matches!(err, IngestError::DatasetNotFound { .. }),
        "{err:?}"
    );
}

// --- chunk_status -----------------------------------------------------------------------------

#[test]
fn chunk_status_reflects_lifecycle() {
    let h = IngestHarness::new("ingest_status_lifecycle");
    let s = h.setup("ds", plain_schema());
    // Absent chunk vs unknown dataset.
    assert_eq!(
        h.block(h.ing.chunk_status("ds", "ghost")).unwrap(),
        ChunkStatus::NotFound
    );
    let err = h.block(h.ing.chunk_status("nods", "x")).unwrap_err();
    assert!(
        matches!(err, IngestError::DatasetNotFound { .. }),
        "{err:?}"
    );
    // Two overlapping chunks; D is Pending until admission. The API path would reject E
    // synchronously, so E arrives via the unguarded discovery path.
    h.block(h.ing.insert_chunks("ds", vec![ic("D", 200, 220, s)]))
        .unwrap();
    h.discovery_insert("ds", "E", 205, 225, s);
    assert_eq!(
        h.block(h.ing.chunk_status("ds", "D")).unwrap(),
        ChunkStatus::Pending
    );
    // Registration admits the earlier D and rejects the overlapping E.
    h.admit();
    assert_eq!(
        h.block(h.ing.chunk_status("ds", "D")).unwrap(),
        ChunkStatus::Admitted
    );
    assert_eq!(
        h.block(h.ing.chunk_status("ds", "E")).unwrap(),
        ChunkStatus::Rejected
    );
}

// --- concurrency ------------------------------------------------------------------------------

/// Pins the lock half of the overlap gate: writer A holds the lock with an overlapping chunk
/// uncommitted; API writer B must *park* on it (observed via `pg_stat_activity`'s advisory wait,
/// not timing), then lose the probe once A commits. Deterministically red if the lock is removed
/// or moved after the probe: B never enters an advisory wait and would commit the overlap.
#[test]
fn overlapping_insert_blocks_on_dataset_lock_then_loses_probe() {
    let h = IngestHarness::new("ingest_lock_pinned");
    let s = h.setup("ds", plain_schema());

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let url = h.url.clone();
    rt.block_on(async move {
        let mut c_win = raw(&url).await;
        let ds_id: DatasetPk = sqlx::query_scalar("SELECT id FROM datasets WHERE name = 'ds'")
            .fetch_one(&mut c_win)
            .await
            .unwrap();

        // Writer A: lock taken, overlapping chunk inserted, not yet committed.
        let mut t_win = c_win.begin().await.unwrap();
        scheduler_metadata::pg::lock::lock_dataset(&mut t_win, ds_id)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO chunks (dataset_id, chunk_id, size, schema_id, first_block, last_block_delta) \
             VALUES ($1, 'A', 1, $2, 0, 10)",
        )
        .bind(ds_id)
        .bind(s)
        .execute(&mut *t_win)
        .await
        .unwrap();

        // Writer B: the full API path — distinct id, overlapping range.
        let b = {
            let url = url.clone();
            tokio::spawn(async move {
                let ing = PgIngest::connect(&url).await.expect("connect writer B");
                ing.insert_chunks("ds", vec![ic("B", 5, 15, s)]).await
            })
        };

        // B is provably parked on the advisory lock (each test runs in its own database, so the
        // filter can't pick up a neighbour's wait).
        let mut c_obs = raw(&url).await;
        let mut polls = 0u32;
        loop {
            let waiting: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_activity \
                 WHERE datname = current_database() AND wait_event = 'advisory'",
            )
            .fetch_one(&mut c_obs)
            .await
            .unwrap();
            if waiting > 0 {
                break;
            }
            assert!(
                !b.is_finished(),
                "writer B finished without blocking on the dataset lock"
            );
            polls += 1;
            assert!(polls < 100, "writer B never blocked on the dataset lock");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        t_win.commit().await.unwrap();

        // B unblocks and its now-authoritative probe sees A's committed chunk.
        let lost = b.await.expect("writer B task");
        match lost {
            Err(IngestError::RangeOverlap {
                chunk_id,
                conflicts_with,
                ..
            }) => {
                assert_eq!(chunk_id, "B");
                assert_eq!(conflicts_with, vec!["A".to_owned()]);
            }
            other => panic!("expected RangeOverlap for writer B, got {other:?}"),
        }
        let mut c = raw(&url).await;
        assert_eq!(count_chunk(&mut c, "ds", "A").await, 1);
        assert_eq!(
            count_chunk(&mut c, "ds", "B").await,
            0,
            "loser must not commit"
        );
    });
}

/// Two independent `PgIngest` writers hammer one dataset with opposing read-schema promotions and
/// their own chunk inserts. `promote_read_schema`'s optimistic concurrency (REPEATABLE READ) makes
/// the loser of a race retry (never deadlock/corrupt), and each chunk's bitmap stays aligned to its
/// pinned write schema. Both promoted read schemas are supersets of the seeded write schema, so the
/// live-write gate always passes.
#[test]
fn concurrent_writers_on_one_dataset_serialize() {
    let h = IngestHarness::new("ingest_concurrent");
    let read_a = schema(&[("blocks", &["a", "x"])]); // superset of the write schema
    let read_b = schema(&[("blocks", &["a", "y"])]); // different superset — different hash

    // The sole write schema chunks pin; contention below runs on its own multi-thread runtime.
    let sid = h.setup("ds", schema(&[("blocks", &["a"])]));
    let url = h.url.clone();

    const N: u64 = 40;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let mut handles = Vec::new();
        for task in 0..2u64 {
            let url = url.clone();
            let read = if task == 0 {
                read_a.clone()
            } else {
                read_b.clone()
            };
            handles.push(tokio::spawn(async move {
                let ing = PgIngest::connect(&url).await.expect("connect writer");
                for n in 0..N {
                    // promote_read_schema uses optimistic concurrency (REPEATABLE READ): two writers
                    // promoting different read schemas make the loser get ConcurrentSchemaChange,
                    // which it retries — rather than silently last-writer-winning behind a lock.
                    loop {
                        match ing.promote_read_schema("ds", read.clone()).await {
                            Ok(_) => break,
                            Err(IngestError::ConcurrentSchemaChange { .. }) => continue,
                            Err(e) => panic!("promote_read_schema: {e}"),
                        }
                    }
                    let first = task * 100_000 + n * 10;
                    let id = format!("t{task}-i{n}");
                    ing.insert_chunks(
                        "ds",
                        vec![ic_tables(&id, first, first + 5, sid, &["blocks"])],
                    )
                    .await
                    .expect("insert_chunks");
                }
            }));
        }
        for handle in handles {
            handle.await.expect("writer task");
        }
    });

    // Every chunk's bitmap aligns with its pinned schema — a real bitmap has one bit per table, and
    // a NULL bitmap (all tables present) is treated as aligned; exactly one current read schema.
    let rt2 = current_rt();
    rt2.block_on(async {
        let mut c = raw(&url).await;
        assert_eq!(current_read_schema_count(&mut c, "ds").await, 1);
        let total: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM chunks c JOIN datasets d ON d.id = c.dataset_id WHERE d.name = 'ds'",
        )
        .fetch_one(&mut c)
        .await
        .unwrap();
        assert_eq!(total, (2 * N) as i64);

        let rows: Vec<(Option<i32>, i64)> = sqlx::query_as(
            "SELECT length(c.tables_present) AS blen, \
                    (SELECT count(*) FROM jsonb_object_keys(s.schema->'tables')) AS tcnt \
             FROM chunks c \
             JOIN schemas s ON s.id = c.schema_id \
             JOIN datasets d ON d.id = c.dataset_id \
             WHERE d.name = 'ds'",
        )
        .fetch_all(&mut c)
        .await
        .unwrap();
        assert_eq!(rows.len(), (2 * N) as usize);
        for (blen, tcnt) in rows {
            // A NULL bitmap means all tables present — aligned with the pinned schema by definition.
            assert_eq!(blen.map_or(tcnt, i64::from), tcnt, "bitmap misaligned with pinned schema");
        }
    });
}

/// A scheduler-seeded dataset has a write schema but NO read schema (read_schemas empty), so two+
/// concurrent *first* promotes of different schemas race on read_schemas_one_current_per_dataset.
/// The loser must surface as ConcurrentSchemaChange (retryable 409), never a Database error (500) —
/// a unique violation on the activate INSERT was previously mapped to Database because
/// is_serialization_failure only matches 40001, not the 23505 this race raises.
#[test]
fn first_ever_promote_race_is_conflict_not_500() {
    let url = fresh_db_url(
        "ingest_first_promote_race",
        super::TEST_ID.fetch_add(1, Ordering::Relaxed),
    );
    // Distinct schemas ⇒ distinct hashes, so writers genuinely conflict on the one-current index
    // rather than dedup via ON CONFLICT (dataset_id, hash).
    let reads = [
        schema(&[("blocks", &["a"])]),
        schema(&[("blocks", &["b"])]),
        schema(&[("blocks", &["c"])]),
        schema(&[("blocks", &["d"])]),
    ];
    const ROUNDS: usize = 8;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let writers: Vec<PgIngest> = {
            let mut v = Vec::new();
            for _ in &reads {
                v.push(PgIngest::connect(&url).await.expect("connect writer"));
            }
            v
        };
        for round in 0..ROUNDS {
            let ds = format!("ds{round}");
            // Seed a bare dataset (no read schema), mirroring the scheduler's insert_new_datasets.
            sqlx::query("INSERT INTO datasets (name, location) VALUES ($1, $2)")
                .bind(&ds)
                .bind(format!("s3://{ds}"))
                .execute(&mut raw(&url).await)
                .await
                .unwrap();

            let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(writers.len()));
            let mut handles = Vec::new();
            for (ing, read) in writers.iter().cloned().zip(reads.iter().cloned()) {
                let (ds, barrier) = (ds.clone(), barrier.clone());
                handles.push(tokio::spawn(async move {
                    barrier.wait().await;
                    ing.promote_read_schema(&ds, read).await
                }));
            }
            for h in handles {
                let out = h.await.expect("join promote task");
                assert!(
                    !matches!(out, Err(IngestError::Database(_))),
                    "round {round}: first-promote race produced a 500-class Database error: {out:?}"
                );
            }
        }
    });
}

/// Two writers hammer the *same* `(dataset, chunk_id)` keys in *opposite* array order, round by
/// round. The per-dataset advisory lock serialises each round; the loser's overlap probe skips the
/// winner's identical rows (self-exclusion by chunk id) and `ON CONFLICT DO NOTHING RETURNING`
/// reports every id as a duplicate — a raced re-send is never a self-overlap 409. Asserts I4 (no
/// duplicate rows, nothing lost); the `Database` retry arm stays as a transient-error guard and
/// its count is printed.
#[test]
fn concurrent_same_key_writers_opposite_order_stay_consistent() {
    let h = IngestHarness::new("ingest_samekey");
    let sid = h.setup("ds", schema(&[("blocks", &["a"])]));
    let url = h.url.clone();

    const N: u64 = 25;
    const ROUNDS: u64 = 20;
    let retries = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let mut handles = Vec::new();
        for task in 0..2u64 {
            let url = url.clone();
            let retries = retries.clone();
            let barrier = barrier.clone();
            handles.push(tokio::spawn(async move {
                let ing = PgIngest::connect(&url).await.expect("connect writer");
                for round in 0..ROUNDS {
                    // Identical ids and ranges for both writers; task 1 submits them reversed.
                    let mut idxs: Vec<u64> = (0..N).collect();
                    if task == 1 {
                        idxs.reverse();
                    }
                    let chunks: Vec<IngestChunk> = idxs
                        .iter()
                        .map(|&i| {
                            let first = round * 100_000 + i * 10;
                            ic(&format!("r{round}-c{i}"), first, first + 4, sid)
                        })
                        .collect();
                    barrier.wait().await; // both writers enter each round together
                    loop {
                        match ing.insert_chunks("ds", chunks.clone()).await {
                            Ok(_) => break,
                            Err(IngestError::Database(_)) => {
                                retries.fetch_add(1, Ordering::Relaxed);
                                continue;
                            }
                            Err(e) => panic!("unexpected insert error: {e:?}"),
                        }
                    }
                }
            }));
        }
        for h in handles {
            h.await.expect("writer task");
        }
    });

    let rt2 = current_rt();
    rt2.block_on(async {
        let mut c = raw(&url).await;
        let total: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM chunks c JOIN datasets d ON d.id = c.dataset_id WHERE d.name = 'ds'",
        )
        .fetch_one(&mut c)
        .await
        .unwrap();
        assert_eq!(total, (N * ROUNDS) as i64, "every id inserted exactly once");
        let dups: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM (SELECT chunk_id FROM chunks c JOIN datasets d ON d.id = c.dataset_id \
             WHERE d.name = 'ds' GROUP BY chunk_id HAVING count(*) > 1) t",
        )
        .fetch_one(&mut c)
        .await
        .unwrap();
        assert_eq!(dups, 0, "no duplicate chunk_id survived the race");
    });
    // insert_chunks sorts each batch by id, so two writers of the same ids take the unique-index
    // locks in the same order: no deadlock cycle, hence nothing to retry.
    assert_eq!(
        retries.load(Ordering::Relaxed),
        0,
        "sorting insert batches by key must keep concurrent same-key writers deadlock-free"
    );
}
