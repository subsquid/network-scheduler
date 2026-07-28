//! Schema registry SQL in two disjoint roles:
//!
//! - WRITE registry (`schemas`): immutable, deduped-by-hash rows a chunk pins; the live set is
//!   derived from live chunks, not a "current" pointer.
//! - READ pointer (`read_schemas`): the single superset schema a reader decodes under, one live row
//!   per dataset.
//!
//! Policy-free storage primitives; v1 has no read/write compatibility gate (the read schema is a
//! pure projection over each chunk's pinned write schema).

use std::collections::BTreeMap;

use anyhow::Context;
use sha2::{Digest, Sha256};
use sqlx::{Postgres, Transaction, postgres::PgConnection};

use crate::dataset_schema::DatasetSchema;
use crate::error::StorageError;
use crate::ids::{DatasetPk, ReadSchemaId, SchemaId};

/// Insert `schema` into the WRITE registry for `dataset_id`, deduped by canonical content hash;
/// returns the new-or-existing id. No lock, no compatibility gate — that policy lives in `PgIngest`.
pub async fn insert_write_schema(
    tx: &mut Transaction<'_, Postgres>,
    dataset_id: DatasetPk,
    schema: &DatasetSchema,
) -> Result<SchemaId, StorageError> {
    schema.validate().context("invalid dataset schema")?;
    let payload = serde_json::to_string(schema).context("serialize schema")?;
    let hash = canonical_hash(schema)?;
    // ON CONFLICT DO UPDATE (a no-op SET) so the existing row's id is RETURNED on a dedup hit.
    let id: SchemaId = sqlx::query_scalar(
        "INSERT INTO schemas (dataset_id, hash, schema) VALUES ($1, $2, $3::jsonb) \
         ON CONFLICT (dataset_id, hash) DO UPDATE SET hash = EXCLUDED.hash \
         RETURNING id",
    )
    .bind(dataset_id)
    .bind(&hash)
    .bind(&payload)
    .fetch_one(&mut **tx)
    .await
    .map_err(|e| write_err(e, "insert_write_schema"))?;
    Ok(id)
}

/// Make `schema` the dataset's current READ schema, deduped by canonical content hash: supersede a
/// differing current row, then activate (or reactivate) the target. No lock; run at REPEATABLE READ
/// (as `PgIngest::promote_read_schema` does) so disagreeing concurrent writers collide with
/// [`StorageError::Serialization`] (→ 409) instead of last-writer-wins clobbering.
///
/// `pub` (not `pub(crate)`) only so the concurrency integration tests can pin the race directly.
pub async fn promote_read_schema(
    tx: &mut Transaction<'_, Postgres>,
    dataset_id: DatasetPk,
    schema: &DatasetSchema,
) -> Result<ReadSchemaId, StorageError> {
    schema.validate().context("invalid dataset schema")?;
    let payload = serde_json::to_string(schema).context("serialize schema")?;
    let hash = canonical_hash(schema)?;
    // Demote a differing current row first so the one-current-per-dataset index holds on activate.
    sqlx::query(
        "UPDATE read_schemas SET superseded_at = now() \
         WHERE dataset_id = $1 AND superseded_at IS NULL AND hash <> $2",
    )
    .bind(dataset_id)
    .bind(&hash)
    .execute(&mut **tx)
    .await
    .map_err(|e| write_err(e, "promote_read_schema: supersede previous"))?;
    let id: ReadSchemaId = sqlx::query_scalar(
        "INSERT INTO read_schemas (dataset_id, hash, schema) VALUES ($1, $2, $3::jsonb) \
         ON CONFLICT (dataset_id, hash) DO UPDATE SET superseded_at = NULL \
         RETURNING id",
    )
    .bind(dataset_id)
    .bind(&hash)
    .bind(&payload)
    .fetch_one(&mut **tx)
    .await
    .map_err(|e| {
        // A unique violation on this INSERT is the read_schemas_one_current_per_dataset partial
        // index — a concurrent promote committed a current row for this dataset first. (The
        // `ON CONFLICT (dataset_id, hash)` arbiter already absorbs the same-hash dedup, and this
        // tx's own supersede demoted any differing current row it could see, so a surviving unique
        // violation is always the first-promote race, not a duplicate.) It is the same retryable
        // conflict the supersede raises as 40001 once a current row exists — surface it the same
        // way, so `PgIngest::promote_read_schema` returns ConcurrentSchemaChange (409), not a 500.
        if super::rows::is_unique_violation(&e) {
            StorageError::Serialization
        } else {
            write_err(e, "promote_read_schema: activate target")
        }
    })?;
    Ok(id)
}

/// The dataset's current read schema (`superseded_at IS NULL`), or `None` if never promoted.
pub(crate) async fn current_read_schema(
    conn: &mut PgConnection,
    dataset_id: DatasetPk,
) -> Result<Option<(ReadSchemaId, DatasetSchema)>, StorageError> {
    let row: Option<(ReadSchemaId, sqlx::types::Json<DatasetSchema>)> = sqlx::query_as(
        "SELECT id, schema FROM read_schemas WHERE dataset_id = $1 AND superseded_at IS NULL",
    )
    .bind(dataset_id)
    .fetch_optional(conn)
    .await
    .context("current_read_schema")?;
    Ok(row.map(|(id, j)| (id, j.0)))
}

/// Each dataset's seeded write schema id, by name — what S3 discovery stamps on found chunks
/// (the listing has no schema info; the FK needs a deterministic per-dataset choice).
///
/// `MIN(s.id)` is the schema created with the dataset: both creation paths seed it inside the
/// creating transaction, and nothing can register another before that commits, so the seeded row
/// always holds the dataset's lowest id. "Latest" would drift as ingesters register schemas.
/// A dataset with no schema row yet is absent.
pub async fn seeded_schema_ids(
    conn: &mut PgConnection,
) -> Result<BTreeMap<String, SchemaId>, StorageError> {
    let rows: Vec<(String, SchemaId)> = sqlx::query_as(
        "SELECT d.name, MIN(s.id) FROM datasets d JOIN schemas s ON s.dataset_id = d.id \
         GROUP BY d.name",
    )
    .fetch_all(conn)
    .await
    .context("seeded_schema_ids")?;
    Ok(rows.into_iter().collect())
}

/// `EXISTS(...)` truthy when schema `s` is referenced by a LIVE chunk — not rejected, not tombstoned
/// off every worker; a chunk with no `sched_chunk_metadata` row yet counts as live. Shared by both
/// listing queries (trusted in-module const, never user input) so "live" can't drift.
///
/// Filters on `c.schema_id = s.id` alone: the composite FK `chunks(schema_id, dataset_id) ->
/// schemas(id, dataset_id)` with `schemas.id` a PK makes a chunk's `dataset_id` functionally
/// determined by its `schema_id`, so a `c.dataset_id = s.dataset_id` term would be redundant —
/// omitting it keeps the inner probe on the `chunks_schema_id` index instead of intersecting.
const LIVE_CHUNK_EXISTS: &str = "EXISTS ( \
     SELECT 1 FROM chunks c \
     LEFT JOIN sched_chunk_metadata m ON m.chunk_pk = c.chunk_pk \
     WHERE c.schema_id = s.id \
       AND (m.chunk_pk IS NULL \
            OR (NOT m.rejected AND m.dropped_from_worker_assignment_at IS NULL)) \
 )";

/// Every write schema for the dataset, each flagged `active` (referenced by a live chunk), by id.
pub(crate) async fn list_write_schemas(
    conn: &mut PgConnection,
    dataset_id: DatasetPk,
) -> Result<Vec<(SchemaId, DatasetSchema, bool)>, StorageError> {
    let rows: Vec<(SchemaId, sqlx::types::Json<DatasetSchema>, bool)> =
        sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT s.id, s.schema, {LIVE_CHUNK_EXISTS} AS active \
             FROM schemas s WHERE s.dataset_id = $1 ORDER BY s.id"
        )))
        .bind(dataset_id)
        .fetch_all(conn)
        .await
        .context("list_write_schemas")?;
    Ok(rows
        .into_iter()
        .map(|(id, j, active)| (id, j.0, active))
        .collect())
}

/// One write schema by id, scoped to `dataset_id` (another dataset's id ⇒ `None`).
pub(crate) async fn get_write_schema(
    conn: &mut PgConnection,
    dataset_id: DatasetPk,
    schema_id: SchemaId,
) -> Result<Option<DatasetSchema>, StorageError> {
    let row: Option<sqlx::types::Json<DatasetSchema>> =
        sqlx::query_scalar("SELECT schema FROM schemas WHERE id = $1 AND dataset_id = $2")
            .bind(schema_id)
            .bind(dataset_id)
            .fetch_optional(conn)
            .await
            .context("get_write_schema")?;
    Ok(row.map(|j| j.0))
}

/// Maps a serialization failure (40001) to [`StorageError::Serialization`]; anything else to `Database`.
fn write_err(err: sqlx::Error, context: &'static str) -> StorageError {
    if super::rows::is_serialization_failure(&err) {
        StorageError::Serialization
    } else {
        StorageError::Database(anyhow::Error::new(err).context(context))
    }
}

/// SHA-256 of the canonicalized json: logically-identical schemas dedup to one row; the stored
/// payload keeps its original form.
fn canonical_hash(schema: &DatasetSchema) -> anyhow::Result<Vec<u8>> {
    let bytes =
        serde_json::to_vec(&schema.canonicalized()).context("serialize schema for hashing")?;
    Ok(Sha256::digest(&bytes).to_vec())
}

/// Decode write schemas by id (all when `schema_ids` is `None`); missing ids are omitted.
pub async fn load_schemas(
    conn: &mut PgConnection,
    schema_ids: Option<&[SchemaId]>,
) -> Result<std::collections::BTreeMap<SchemaId, DatasetSchema>, StorageError> {
    let rows: Vec<(SchemaId, sqlx::types::Json<DatasetSchema>)> = match schema_ids {
        None => sqlx::query_as("SELECT id, schema FROM schemas")
            .fetch_all(conn)
            .await
            .context("load_schemas")?,
        Some([]) => return Ok(std::collections::BTreeMap::new()),
        Some(ids) => sqlx::query_as("SELECT id, schema FROM schemas WHERE id = ANY($1)")
            .bind(ids)
            .fetch_all(conn)
            .await
            .context("load_schemas")?,
    };
    Ok(rows.into_iter().map(|(id, json)| (id, json.0)).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset_schema::TableSchema;

    fn schema(fields: &[&str], default_fields: &[&str]) -> DatasetSchema {
        DatasetSchema::new(std::collections::BTreeMap::from([(
            "blocks".to_owned(),
            TableSchema {
                fields: fields.iter().map(|f| (*f).to_owned()).collect(),
                default_fields: default_fields.iter().map(|f| (*f).to_owned()).collect(),
            },
        )]))
    }

    /// Field order and duplicates must not change the hash; any content difference must.
    #[test]
    fn canonical_hash_ignores_field_order_and_duplicates() {
        let base = canonical_hash(&schema(&["a", "b"], &["a", "b"])).unwrap();
        for equivalent in [
            schema(&["b", "a"], &["a", "b"]),
            schema(&["a", "b"], &["b", "a"]),
            schema(&["a", "a", "b"], &["a", "b"]),
        ] {
            assert_eq!(canonical_hash(&equivalent).unwrap(), base);
        }
        for different in [
            schema(&["a", "b", "c"], &["a", "b"]),
            schema(&["a", "b"], &["a"]),
            schema(&["a"], &["a"]),
        ] {
            assert_ne!(canonical_hash(&different).unwrap(), base);
        }
    }
}
