//! Schema registry: make a [`DatasetSchema`] a dataset's current read schema, deduped by
//! canonical content hash.

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use sqlx::{Postgres, Transaction, postgres::PgConnection};

use crate::scheduler_storage::{DatasetId, SchemaId};
use crate::types::DatasetSchema;

/// Make `schema` the current read schema for `dataset_id`, reusing a canonically identical row.
pub(super) async fn ensure_current_schema(
    tx: &mut Transaction<'_, Postgres>,
    dataset_id: DatasetId,
    schema: &DatasetSchema,
) -> Result<SchemaId> {
    schema.validate().context("invalid dataset schema")?;
    let payload = serde_json::to_string(schema).context("serialize schema")?;
    let hash = canonical_hash(schema)?;
    // Demote a differing current schema first, so the one-current-per-dataset index holds when
    // the target is activated below.
    sqlx::query(
        "UPDATE schemas SET superseded_at = now() \
         WHERE dataset_id = $1 AND superseded_at IS NULL AND hash <> $2",
    )
    .bind(dataset_id)
    .bind(&hash)
    .execute(&mut **tx)
    .await
    .context("ensure_current_schema: supersede previous")?;
    // Insert the target as current, or reactivate the identical existing row.
    let id: SchemaId = sqlx::query_scalar(
        "INSERT INTO schemas (dataset_id, hash, schema) VALUES ($1, $2, $3::jsonb) \
         ON CONFLICT (dataset_id, hash) DO UPDATE SET superseded_at = NULL \
         RETURNING id",
    )
    .bind(dataset_id)
    .bind(&hash)
    .bind(&payload)
    .fetch_one(&mut **tx)
    .await
    .context("ensure_current_schema: activate target")?;
    Ok(id)
}

/// SHA-256 of the canonicalized json: logically-identical schemas hash (and dedup) to one row
/// while the stored payload keeps its original form.
fn canonical_hash(schema: &DatasetSchema) -> Result<Vec<u8>> {
    let bytes =
        serde_json::to_vec(&schema.canonicalized()).context("serialize schema for hashing")?;
    Ok(Sha256::digest(&bytes).to_vec())
}

pub(super) async fn load_schemas(
    conn: &mut PgConnection,
    schema_ids: Option<&[SchemaId]>,
) -> Result<std::collections::BTreeMap<SchemaId, DatasetSchema>> {
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
    use crate::types::TableSchema;

    fn schema(fields: &[&str], default_fields: &[&str]) -> DatasetSchema {
        DatasetSchema::new(std::collections::BTreeMap::from([(
            "blocks".to_owned(),
            TableSchema {
                fields: fields.iter().map(|f| (*f).to_owned()).collect(),
                default_fields: default_fields.iter().map(|f| (*f).to_owned()).collect(),
            },
        )]))
    }

    /// The hash's equivalence classes back per-dataset dedup: order and duplicates in the field
    /// lists must not mint new schema rows, while any content difference must.
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
