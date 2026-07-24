//! Async, pooled ingest facade — the metadata service's write path. Each chunk pins an explicit
//! `schema_id` and names its tables (resolved to a positional bitmap); duplicates are reported rather
//! than aborting, only a live range overlap aborts. Each method is its own short pooled transaction.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Context;
use sqlx::postgres::PgConnection;

use crate::dataset_schema::DatasetSchema;
use crate::error::{IngestError, StorageError};
use crate::ids::{ChunkPk, DatasetPk, ReadSchemaId, SchemaId};
use crate::ingest_api::{ChunkStatus, Correction, DatasetOutcome, Head, IngestChunk, SchemaInfo};
use crate::new_chunk::NewChunk;
use crate::new_dataset::NewDataset;

use super::rows::{ChunkInsertArrays, bind_chunk_arrays};

/// Rows per batched write.
const DEFAULT_BATCH_SIZE: usize = 10_000;

/// Snapshot of the ingest pool's connection counters, for the metrics sampler. Not in
/// `ingest_api`: it mirrors sqlx pool internals, not the API contract.
#[derive(Debug, Clone, Copy)]
pub struct PoolStats {
    pub size: u32,
    pub idle: usize,
}

/// Async, pooled ingest facade. Cheap to clone (shares the pool).
#[derive(Debug, Clone)]
pub struct PgIngest {
    pool: sqlx::PgPool,
}

impl PgIngest {
    #[must_use]
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    /// Connection counts for the metrics sampler; reads the pool's internal counters without acquiring
    /// one, so it cannot deadlock against `max_connections`.
    #[must_use]
    pub fn pool_stats(&self) -> PoolStats {
        PoolStats {
            size: self.pool.size(),
            idle: self.pool.num_idle(),
        }
    }

    /// Connect a fresh pool — no advisory session lock, no GUCs; must not go through `PostgresStorage::connect`.
    pub async fn connect(database_url: &str) -> Result<Self, IngestError> {
        let pool = sqlx::PgPool::connect(database_url)
            .await
            .context("PgIngest: connect")?;
        Ok(Self { pool })
    }

    /// Liveness probe: `SELECT 1` on the pool, for the service's `/health`.
    pub async fn ping(&self) -> Result<(), IngestError> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .context("ping")?;
        Ok(())
    }

    /// Fail fast unless every embedded migration is applied with a matching checksum. Read-only (unlike
    /// [`Self::migrate`]), so the service can call it on boot.
    pub async fn check_schema_version(&self) -> Result<(), IngestError> {
        let applied: Vec<(i64, Vec<u8>)> =
            sqlx::query_as("SELECT version, checksum FROM _sqlx_migrations WHERE success")
                .fetch_all(&self.pool)
                .await
                .context("check_schema_version: read _sqlx_migrations")?;
        let applied: HashMap<i64, Vec<u8>> = applied.into_iter().collect();
        for m in super::MIGRATOR.iter() {
            match applied.get(&m.version) {
                Some(cs) if cs.as_slice() == m.checksum.as_ref() => {}
                Some(_) => {
                    return Err(anyhow::anyhow!(
                        "migration {} checksum mismatch: database schema differs from embedded \
                         migrations",
                        m.version
                    )
                    .into());
                }
                None => {
                    return Err(anyhow::anyhow!(
                        "migration {} not applied: run migrations before starting",
                        m.version
                    )
                    .into());
                }
            }
        }
        Ok(())
    }

    /// Apply any pending migrations. Dev-only convenience; production applies them via the scheduler.
    pub async fn migrate(&self) -> Result<(), IngestError> {
        super::MIGRATOR.run(&self.pool).await.context("migrate")?;
        Ok(())
    }

    /// Create a dataset, seeding `schema` as both its initial write and read schema (equal ⇒ compatible).
    /// Idempotent: a repeat name is [`DatasetOutcome::AlreadyExists`] and leaves both schemas untouched.
    pub async fn create_dataset(&self, dataset: NewDataset) -> Result<DatasetOutcome, IngestError> {
        let mut tx = self.pool.begin().await.context("create_dataset: begin")?;
        // READ COMMITTED + `ON CONFLICT DO NOTHING`: a concurrent same-name create resolves to
        // AlreadyExists, no retry or failure.
        let created: Option<DatasetPk> = sqlx::query_scalar(
            "INSERT INTO datasets (name, location) VALUES ($1, $2) \
             ON CONFLICT (name) DO NOTHING RETURNING id",
        )
        .bind(&dataset.name)
        .bind(&dataset.location)
        .fetch_optional(&mut *tx)
        .await
        .context("create_dataset: insert")?;
        let outcome = match created {
            // A freshly inserted row is invisible until commit, so its first schemas activate
            // without contention; seed both roles from the same schema.
            Some(id) => {
                super::schema::insert_write_schema(&mut tx, id, &dataset.schema).await?;
                super::schema::promote_read_schema(&mut tx, id, &dataset.schema).await?;
                DatasetOutcome::Created
            }
            None => DatasetOutcome::AlreadyExists,
        };
        tx.commit().await.context("create_dataset: commit")?;
        Ok(outcome)
    }

    /// Register `schema` as a WRITE schema; returns its id (idempotent by content hash). No
    /// compatibility gate in v1 — the read schema is a pure projection, so any write is compatible.
    /// The write insert is an order-independent immutable dedup, so READ COMMITTED is fine.
    pub async fn register_write_schema(
        &self,
        dataset: &str,
        schema: DatasetSchema,
    ) -> Result<SchemaId, IngestError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("register_write_schema: begin")?;
        let id = resolve_dataset(&mut tx, dataset).await?;
        let schema_id = super::schema::insert_write_schema(&mut tx, id, &schema).await?;
        tx.commit().await.context("register_write_schema: commit")?;
        Ok(schema_id)
    }

    /// Promote `schema` to the dataset's current READ schema; returns its id (idempotent by content
    /// hash). No compatibility gate in v1 — the read schema is a pure projection.
    ///
    /// REPEATABLE READ, no advisory lock: a concurrent read-schema change fails the supersede with
    /// 40001 → [`IngestError::ConcurrentSchemaChange`] (409); omitting the lock surfaces the conflict
    /// instead of serialising it away.
    pub async fn promote_read_schema(
        &self,
        dataset: &str,
        schema: DatasetSchema,
    ) -> Result<ReadSchemaId, IngestError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("promote_read_schema: begin")?;
        // Must precede the first snapshotting query (`resolve_dataset`'s SELECT).
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *tx)
            .await
            .context("promote_read_schema: set isolation")?;
        let id = resolve_dataset(&mut tx, dataset).await?;
        let read_schema_id = match super::schema::promote_read_schema(&mut tx, id, &schema).await {
            Ok(read_schema_id) => read_schema_id,
            Err(StorageError::Serialization) => {
                return Err(IngestError::ConcurrentSchemaChange {
                    dataset: dataset.to_owned(),
                });
            }
            Err(e) => return Err(e.into()),
        };
        tx.commit().await.context("promote_read_schema: commit")?;
        Ok(read_schema_id)
    }

    /// Append chunks, returning the input ids skipped as already-present duplicates. An overlap with a
    /// non-rejected chunk (existing, pending, or in-batch) aborts the whole call. READ COMMITTED under
    /// the per-dataset advisory lock: the lock serialises same-dataset writers and the overlap probe
    /// is authoritative (it sees committed *pending* chunks, not just admitted ones), so a returned
    /// insert is durably accepted — it will not be silently rejected later by the admission gate.
    ///
    /// Duplicates are decided entirely under the lock: the probe skips a candidate's own committed
    /// row and `ON CONFLICT DO NOTHING ... RETURNING` classifies it, so a re-send racing its
    /// original in-flight request still reports a duplicate, never a self-overlap. Identity is the
    /// chunk id alone — a same-id re-send with a changed range is a duplicate and the stored range
    /// wins.
    pub async fn insert_chunks(
        &self,
        dataset: &str,
        chunks: Vec<IngestChunk>,
    ) -> Result<Vec<String>, IngestError> {
        if chunks.is_empty() {
            return Ok(Vec::new());
        }
        reject_duplicate_ids(chunks.iter().map(|c| c.id.as_str()))?;

        let mut tx = self.pool.begin().await.context("insert_chunks: begin")?;
        let dataset_id = resolve_dataset(&mut tx, dataset).await?;

        let lowerer = ChunkLowerer::load(
            &mut tx,
            dataset,
            dataset_id,
            chunks.iter().map(|c| c.schema_id),
        )
        .await?;
        let mut new_chunks: Vec<NewChunk> = chunks
            .into_iter()
            .map(|ic| lowerer.lower(ic))
            .collect::<Result<_, _>>()?;
        // Sort by id so concurrent writers of overlapping ids take the unique-index tuple locks in the
        // same order and cannot deadlock (belt-and-braces: the per-dataset lock below already
        // serialises same-dataset writers).
        new_chunks.sort_unstable_by(|a, b| a.id.cmp(&b.id));

        // Lossless: `validate_chunk` (in `lower_chunk` above) bounds blocks to `i64::MAX`.
        let firsts: Vec<i64> = new_chunks
            .iter()
            .map(|c| *c.blocks.start() as i64)
            .collect();
        let lasts: Vec<i64> = new_chunks.iter().map(|c| *c.blocks.end() as i64).collect();
        // Borrowed from `new_chunks`; only the cold error paths clone.
        let new_ids: Vec<&str> = new_chunks.iter().map(|c| c.id.as_str()).collect();

        reject_intra_batch_overlap(dataset, &firsts, &lasts, &new_ids)?;

        // Entered last so the lock spans only probe → insert → commit; a concurrent overlapping
        // writer blocks on entry, then loses the probe.
        let inserted: Vec<String> =
            super::lock::with_dataset_lock(tx, dataset_id, "insert_chunks", async |locked| {
                if let Some((i, hit_id)) =
                    super::nonoverlap::overlapping_incoming(locked, &new_ids, &firsts, &lasts)
                        .await?
                {
                    return Err(IngestError::RangeOverlap {
                        dataset: dataset.to_owned(),
                        chunk_id: new_ids[i].to_string(),
                        conflicts_with: vec![hit_id],
                    });
                }

                // Idempotent insert; RETURNING classifies duplicates (the probe above skipped
                // their own rows, so they fall through to ON CONFLICT here).
                let p = ChunkInsertArrays::from_chunks(new_chunks.iter());
                bind_chunk_arrays!(
                    sqlx::query_scalar(
                        r#"
            INSERT INTO chunks
                (dataset_id, chunk_id, size, schema_id, tables_present,
                 first_block, last_block_delta, last_block_hash, last_block_timestamp)
            SELECT $1, x.chunk_id, x.size, x.schema_id, x.tables_present,
                   x.first_block, x.last_block_delta, x.last_block_hash, x.last_block_timestamp
            FROM UNNEST($2::text[], $3::int[], $4::int[], $5::varbit[],
                        $6::bigint[], $7::int[], $8::text[], $9::bigint[])
                 AS x(chunk_id, size, schema_id, tables_present,
                      first_block, last_block_delta, last_block_hash, last_block_timestamp)
            ON CONFLICT (dataset_id, chunk_id) DO NOTHING
            RETURNING chunk_id
            "#,
                    )
                    .bind(dataset_id),
                    p
                )
                .fetch_all(&mut *locked.conn())
                .await
                .context("insert_chunks: insert")
                .map_err(Into::into)
            })
            .await?;

        // Clones only actual duplicates (normally none); return order is unspecified.
        let inserted_set: HashSet<&str> = inserted.iter().map(String::as_str).collect();
        let duplicates: Vec<String> = new_ids
            .iter()
            .filter(|id| !inserted_set.contains(*id))
            .map(|id| (*id).to_owned())
            .collect();
        Ok(duplicates)
    }

    /// Register 1:1 same-range corrections, naming each old chunk by its id. All-or-nothing.
    pub async fn register_corrections(
        &self,
        dataset: &str,
        corrections: Vec<Correction>,
    ) -> Result<(), IngestError> {
        if corrections.is_empty() {
            return Ok(());
        }
        let mut conn = self
            .pool
            .acquire()
            .await
            .context("register_corrections: acquire")?;
        let dataset_id = resolve_dataset(&mut conn, dataset).await?;

        // Resolve old chunk ids → pks (immutable once committed, so safe to pre-resolve here).
        let old_ids: Vec<&str> = corrections
            .iter()
            .map(|c| c.old_chunk_id.as_str())
            .collect();
        let rows: Vec<(String, ChunkPk)> = sqlx::query_as(
            "SELECT chunk_id, chunk_pk FROM chunks WHERE dataset_id = $1 AND chunk_id = ANY($2::text[])",
        )
        .bind(dataset_id)
        .bind(&old_ids)
        .fetch_all(&mut *conn)
        .await
        .context("register_corrections: resolve old chunks")?;
        let pk_by_id: HashMap<String, ChunkPk> = rows.into_iter().collect();

        let lowerer = ChunkLowerer::load(
            &mut conn,
            dataset,
            dataset_id,
            corrections.iter().map(|c| c.replacement.schema_id),
        )
        .await?;
        let mut pairs: Vec<(ChunkPk, NewChunk)> = Vec::with_capacity(corrections.len());
        for c in corrections {
            let old_pk =
                *pk_by_id
                    .get(&c.old_chunk_id)
                    .ok_or_else(|| IngestError::CorrectionRejected {
                        reason: format!("unknown old chunk id {:?}", c.old_chunk_id),
                    })?;
            let new = lowerer.lower(c.replacement)?;
            pairs.push((old_pk, new));
        }

        // Opens its own transaction and enforces same-range / availability / duplicate.
        super::correction::register_corrections(&mut conn, &pairs, now_secs(), DEFAULT_BATCH_SIZE)
            .await?;
        Ok(())
    }

    /// The dataset's resume point: the highest `last_block` over chunks not terminally rejected.
    /// Admitted chunks are non-overlapping with rising ends, so this is a descending top-1 by
    /// `first_block` on `chunks_dataset_first_block` (stops at the first non-rejected row — rare near
    /// the head), not an aggregate. Counting pending chunks can under-count, which is safe for a
    /// resume point (dedup on re-send).
    pub async fn head(&self, dataset: &str) -> Result<Head, IngestError> {
        let (mut conn, dataset_id) = self.acquire_for(dataset, "head: acquire").await?;
        let stored: Option<i64> = sqlx::query_scalar(
            r#"
            SELECT c.first_block + c.last_block_delta
            FROM chunks c
            LEFT JOIN sched_chunk_metadata s ON s.chunk_pk = c.chunk_pk
            WHERE c.dataset_id = $1 AND (s.chunk_pk IS NULL OR NOT s.rejected)
            ORDER BY c.first_block DESC
            LIMIT 1
            "#,
        )
        .bind(dataset_id)
        .fetch_optional(&mut *conn)
        .await
        .context("head: query")?;
        Ok(Head {
            stored_head: stored.map(|v| u64::try_from(v).unwrap_or(0)),
        })
    }

    /// The dataset's current READ schema — the `read_schemas` row with `superseded_at IS NULL`.
    pub async fn current_read_schema(
        &self,
        dataset: &str,
    ) -> Result<(ReadSchemaId, DatasetSchema), IngestError> {
        let (mut conn, dataset_id) = self
            .acquire_for(dataset, "current_read_schema: acquire")
            .await?;
        super::schema::current_read_schema(&mut conn, dataset_id)
            .await?
            .ok_or_else(|| {
                IngestError::Database(anyhow::anyhow!(
                    "dataset {dataset:?} has no current read schema"
                ))
            })
    }

    /// Every WRITE schema for the dataset, each flagged `active` (referenced by a live chunk), by id.
    pub async fn list_write_schemas(&self, dataset: &str) -> Result<Vec<SchemaInfo>, IngestError> {
        let (mut conn, dataset_id) = self
            .acquire_for(dataset, "list_write_schemas: acquire")
            .await?;
        let rows = super::schema::list_write_schemas(&mut conn, dataset_id).await?;
        Ok(rows
            .into_iter()
            .map(|(schema_id, schema, active)| SchemaInfo {
                schema_id,
                schema,
                active,
            })
            .collect())
    }

    /// One WRITE schema by id ([`IngestError::SchemaNotFound`] if unknown or another dataset's).
    pub async fn get_write_schema(
        &self,
        dataset: &str,
        schema_id: SchemaId,
    ) -> Result<DatasetSchema, IngestError> {
        let (mut conn, dataset_id) = self
            .acquire_for(dataset, "get_write_schema: acquire")
            .await?;
        super::schema::get_write_schema(&mut conn, dataset_id, schema_id)
            .await?
            .ok_or(IngestError::SchemaNotFound { schema_id })
    }

    /// The lifecycle status of one chunk. Absent chunk under a known dataset → [`ChunkStatus::NotFound`];
    /// unknown dataset → [`IngestError::DatasetNotFound`].
    pub async fn chunk_status(
        &self,
        dataset: &str,
        chunk_id: &str,
    ) -> Result<ChunkStatus, IngestError> {
        let (mut conn, dataset_id) = self.acquire_for(dataset, "chunk_status: acquire").await?;
        let row: Option<Option<bool>> = sqlx::query_scalar(
            r#"
            SELECT s.rejected
            FROM chunks c
            LEFT JOIN sched_chunk_metadata s ON s.chunk_pk = c.chunk_pk
            WHERE c.dataset_id = $1 AND c.chunk_id = $2
            "#,
        )
        .bind(dataset_id)
        .bind(chunk_id)
        .fetch_optional(&mut *conn)
        .await
        .context("chunk_status: query")?;
        Ok(match row {
            None => ChunkStatus::NotFound,
            Some(None) => ChunkStatus::Pending,
            Some(Some(false)) => ChunkStatus::Admitted,
            Some(Some(true)) => ChunkStatus::Rejected,
        })
    }

    /// Acquire a connection and resolve `dataset` to its pk — shared read-path preamble; `ctx` labels an acquire failure.
    async fn acquire_for(
        &self,
        dataset: &str,
        ctx: &'static str,
    ) -> Result<(sqlx::pool::PoolConnection<sqlx::Postgres>, DatasetPk), IngestError> {
        let mut conn = self.pool.acquire().await.context(ctx)?;
        let dataset_id = resolve_dataset(&mut conn, dataset).await?;
        Ok((conn, dataset_id))
    }
}

/// Resolve a dataset name to its id, or [`IngestError::DatasetNotFound`].
async fn resolve_dataset(conn: &mut PgConnection, name: &str) -> Result<DatasetPk, IngestError> {
    let id: Option<DatasetPk> = sqlx::query_scalar("SELECT id FROM datasets WHERE name = $1")
        .bind(name)
        .fetch_optional(conn)
        .await
        .context("resolve dataset")?;
    id.ok_or_else(|| IngestError::DatasetNotFound {
        name: name.to_owned(),
    })
}

/// Reject a batch that names the same `chunk_id` twice: which payload would win is arbitrary.
fn reject_duplicate_ids<'a>(
    ids: impl ExactSizeIterator<Item = &'a str>,
) -> Result<(), IngestError> {
    let mut seen: HashSet<&str> = HashSet::with_capacity(ids.len());
    for id in ids {
        if !seen.insert(id) {
            return Err(IngestError::DuplicateInBatch {
                chunk_id: id.to_owned(),
            });
        }
    }
    Ok(())
}

/// Per-batch lowering context: the dataset-name `Arc` every lowered `NewChunk` shares plus each
/// pinned schema's table→bit-position map, coupled by construction.
struct ChunkLowerer {
    dataset: Arc<String>,
    pos_by_schema: HashMap<SchemaId, HashMap<String, usize>>,
}

impl ChunkLowerer {
    /// Resolve the pinned schemas (dataset-scoped ⇒ another dataset's id is SchemaNotFound too;
    /// superseded included, missing ids absent) and precompute sorted-name table positions.
    /// `dataset` must be the name `dataset_id` resolved from — unverifiable here.
    async fn load(
        conn: &mut PgConnection,
        dataset: &str,
        dataset_id: DatasetPk,
        pinned: impl IntoIterator<Item = SchemaId>,
    ) -> Result<Self, IngestError> {
        let mut ids: Vec<SchemaId> = pinned.into_iter().collect();
        ids.sort_unstable();
        ids.dedup();
        let rows: Vec<(SchemaId, sqlx::types::Json<DatasetSchema>)> = if ids.is_empty() {
            Vec::new()
        } else {
            sqlx::query_as(
                "SELECT id, schema FROM schemas WHERE dataset_id = $1 AND id = ANY($2::int[])",
            )
            .bind(dataset_id)
            .bind(&ids)
            .fetch_all(conn)
            .await
            .context("load dataset schemas")?
        };
        let pos_by_schema = rows
            .into_iter()
            .map(|(id, j)| {
                // Table name → bit index in sorted-name order (the bit order readers rely on).
                let pos =
                    j.0.tables()
                        .keys()
                        .enumerate()
                        .map(|(i, k)| (k.clone(), i))
                        .collect();
                (id, pos)
            })
            .collect();
        Ok(Self {
            dataset: Arc::new(dataset.to_owned()),
            pos_by_schema,
        })
    }

    /// Validate `ic`, resolve its table names against its pinned schema's positions, and lower it
    /// to a `NewChunk`. A `schema_id` absent from the loaded set → [`IngestError::SchemaNotFound`].
    fn lower(&self, ic: IngestChunk) -> Result<NewChunk, IngestError> {
        validate_chunk(&ic)?;
        let pos = self
            .pos_by_schema
            .get(&ic.schema_id)
            .ok_or(IngestError::SchemaNotFound {
                schema_id: ic.schema_id,
            })?;
        let tables_present = resolve_tables(pos, ic.schema_id, ic.tables.as_deref())?;
        Ok(NewChunk {
            dataset: self.dataset.clone(),
            id: Arc::new(ic.id),
            size: ic.size,
            blocks: ic.first_block..=ic.last_block,
            schema_id: ic.schema_id,
            tables_present,
            last_block_hash: ic.last_block_hash,
            last_block_timestamp: ic.last_block_timestamp,
        })
    }
}

/// Reject a range/size that can't be stored: blocks land in `BIGINT` columns (`as i64` would wrap
/// past `i64::MAX` into a negative range) and `block_range_columns` does `(end - start) as i32`.
fn validate_chunk(ic: &IngestChunk) -> Result<(), IngestError> {
    let span_ok = ic.last_block <= i64::MAX as u64
        && ic.last_block >= ic.first_block
        && ic.last_block - ic.first_block <= i32::MAX as u64;
    let size_ok = ic.size <= i32::MAX as u32;
    if span_ok && size_ok {
        return Ok(());
    }
    Err(IngestError::InvalidChunkRange {
        chunk_id: ic.id.clone(),
        first_block: ic.first_block,
        last_block: ic.last_block,
        size: ic.size,
    })
}

/// Names → positional bitmap: set bit `i` for each named table, `i` being its index in the schema's
/// sorted-name order (the bit order readers rely on). An unknown name → `UnknownTable`. `pos` is
/// prebuilt once per schema and shared across every chunk that pins it.
///
/// `None` is the NULL-bitmap "all present" sentinel and is returned in the two cases that mean every
/// table is present: no list at all, or a list naming every table. Both collapse to the same NULL —
/// sound only because a schema id's table set is immutable (content-hashed), so an all-ones bitmap
/// and NULL are permanently equivalent for that id. A returned `Some(bv)` is therefore never all-ones;
/// `Some([])` (empty list) is the distinct all-zero "none present" bitmap.
fn resolve_tables<K: std::borrow::Borrow<str> + Eq + std::hash::Hash>(
    pos: &HashMap<K, usize>,
    schema_id: SchemaId,
    tables: Option<&[String]>,
) -> Result<Option<bit_vec::BitVec>, IngestError> {
    let Some(names) = tables else {
        // No list ⇒ all present.
        return Ok(None);
    };
    let mut bv = bit_vec::BitVec::from_elem(pos.len(), false);
    for name in names {
        let &i = pos
            .get(name.as_str())
            .ok_or_else(|| IngestError::UnknownTable {
                schema_id,
                table: name.clone(),
            })?;
        bv.set(i, true);
    }
    if bv.all() {
        // Every table named ⇒ the same NULL "all present" sentinel (sound because the set is immutable).
        return Ok(None);
    }
    Ok(Some(bv))
}

/// Reject an intra-batch overlap: sort by `(first_block, id)` and check each against the running
/// max end block. The scheduler sweep's `settle_within_batch` does the same check but rejects each
/// loser individually; aborting the whole batch is intentional here — the client must fix and
/// resend it.
fn reject_intra_batch_overlap(
    dataset: &str,
    firsts: &[i64],
    lasts: &[i64],
    ids: &[&str],
) -> Result<(), IngestError> {
    let mut order: Vec<usize> = (0..ids.len()).collect();
    order.sort_unstable_by(|&a, &b| (firsts[a], ids[a]).cmp(&(firsts[b], ids[b])));
    let (mut max_last, mut cover) = (i64::MIN, 0usize);
    for i in order {
        if firsts[i] <= max_last {
            return Err(IngestError::RangeOverlap {
                dataset: dataset.to_owned(),
                chunk_id: ids[i].to_string(),
                conflicts_with: vec![ids[cover].to_string()],
            });
        }
        if lasts[i] > max_last {
            max_last = lasts[i];
            cover = i;
        }
    }
    Ok(())
}

/// unix-epoch seconds for the correction audit column (audit-only per docs/mvcc-schema.md).
fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `pos` in the same sorted-name order `ChunkLowerer::load` produces.
    fn pos<'a>(list: &[&'a str]) -> HashMap<&'a str, usize> {
        let mut sorted = list.to_vec();
        sorted.sort_unstable();
        sorted
            .into_iter()
            .enumerate()
            .map(|(i, n)| (n, i))
            .collect()
    }

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| (*s).to_owned()).collect()
    }

    #[test]
    fn no_list_is_the_all_present_null() {
        assert_eq!(
            resolve_tables(&pos(&["blocks", "logs"]), SchemaId(1), None).unwrap(),
            None
        );
    }

    #[test]
    fn every_table_named_collapses_to_the_null_sentinel() {
        // The overloaded-None case: a full list is stored identically to no list at all.
        let all = names(&["blocks", "logs"]);
        assert_eq!(
            resolve_tables(&pos(&["blocks", "logs"]), SchemaId(1), Some(&all)).unwrap(),
            None
        );
    }

    #[test]
    fn subset_sets_only_named_bits_in_sorted_order() {
        // blocks=0, logs=1 in sorted order; naming only `logs` sets bit 1, not bit 0.
        let bv = resolve_tables(
            &pos(&["blocks", "logs"]),
            SchemaId(1),
            Some(&names(&["logs"])),
        )
        .unwrap()
        .expect("a proper subset is Some, not the all-present NULL");
        assert_eq!(bv.get(0), Some(false));
        assert_eq!(bv.get(1), Some(true));
    }

    #[test]
    fn empty_list_is_the_all_zero_none_present_bitmap() {
        let bv = resolve_tables(&pos(&["blocks", "logs"]), SchemaId(1), Some(&names(&[])))
            .unwrap()
            .expect("none-present is a real all-zero bitmap, distinct from the all-present NULL");
        assert!(bv.none());
    }

    #[test]
    fn unknown_table_name_is_rejected() {
        assert!(matches!(
            resolve_tables(&pos(&["blocks"]), SchemaId(7), Some(&names(&["nope"]))),
            Err(IngestError::UnknownTable { schema_id: SchemaId(7), table }) if table == "nope"
        ));
    }
}
