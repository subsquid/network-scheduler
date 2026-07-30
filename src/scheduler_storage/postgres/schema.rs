//! Scheduler-side schema probe over `sched_chunk_metadata`: which read schemas are still in play.
//! Joins a scheduler-owned table, so it lives here rather than in `scheduler-metadata`.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use sqlx::postgres::PgConnection;

use sqlx::Connection;

use crate::scheduler_storage::{ReadSchemaId, SchemaBundle, SchemaId};
use crate::types::DatasetSchema;

/// Each named dataset's current read-schema id, for the portal assignment to publish. LEFT JOIN
/// from `datasets`, so the result is TOTAL over the named set — `None` means "never promoted",
/// distinct from a key the backend failed to produce. Ids only; the content travels in the bundle.
pub(super) async fn read_schema_ids_by_dataset(
    conn: &mut PgConnection,
    datasets: &[&str],
) -> Result<BTreeMap<crate::types::DatasetId, Option<ReadSchemaId>>> {
    let mut timer = crate::metrics::PhaseTimer::new("read_schema_ids_by_dataset");
    if datasets.is_empty() {
        return Ok(BTreeMap::new());
    }
    let rows: Vec<(String, Option<ReadSchemaId>)> = sqlx::query_as(
        "SELECT d.name, r.id FROM datasets d \
         LEFT JOIN read_schemas r ON r.dataset_id = d.id AND r.superseded_at IS NULL \
         WHERE d.name = ANY($1)",
    )
    .bind(datasets)
    .fetch_all(conn)
    .await
    .context("read_schema_ids_by_dataset")?;
    timer.stmt(rows.len() as u64);
    Ok(rows
        .into_iter()
        .map(|(name, id)| (std::sync::Arc::new(name), id))
        .collect())
}

/// The bundle's read section: current read-schema payloads keyed by id, for the named datasets.
/// Inner join, unlike [`read_schema_ids_by_dataset`] — a dataset with no read schema has no slot in
/// an id-keyed map. One statement, so a concurrent promote costs freshness, never resolvability.
pub(super) async fn read_schemas_by_id(
    conn: &mut PgConnection,
    datasets: &[&str],
) -> Result<BTreeMap<ReadSchemaId, DatasetSchema>> {
    let mut timer = crate::metrics::PhaseTimer::new("read_schemas_by_id");
    if datasets.is_empty() {
        return Ok(BTreeMap::new());
    }
    let rows: Vec<(ReadSchemaId, sqlx::types::Json<DatasetSchema>)> = sqlx::query_as(
        "SELECT r.id, r.schema FROM read_schemas r \
         JOIN datasets d ON d.id = r.dataset_id \
         WHERE r.superseded_at IS NULL AND d.name = ANY($1)",
    )
    .bind(datasets)
    .fetch_all(conn)
    .await
    .context("read_schemas_by_id")?;
    timer.stmt(rows.len() as u64);
    Ok(rows.into_iter().map(|(id, json)| (id, json.0)).collect())
}

/// One bundle from committed rows alone — identical under success, shortage, and on a fresh
/// process. Write section: the persisted set of the last successful cycle, which by construction
/// equals the routable window at that commit and always covers it afterwards (window entry requires
/// a stamp only a successful cycle writes; tombstoning only shrinks it). Read section: the CURRENT
/// read schema of every referenced dataset. One `REPEATABLE READ, READ ONLY` snapshot; strict
/// loads — a referenced id with no `schemas` row is an error, never a silent shrink.
pub(super) async fn generate_bundle(conn: &mut PgConnection) -> Result<SchemaBundle> {
    let _timer = crate::metrics::Timer::new("generate_schema_bundle");
    let mut tx = conn.begin().await.context("generate_bundle: begin")?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await
        .context("generate_bundle: set isolation")?;

    // A few hundred rows by PK joins; the dataset comes through the schema row
    // (`chunks_schema_same_dataset` makes that the chunk's own dataset).
    let refs: Vec<(SchemaId, String)> = sqlx::query_as(
        "SELECT w.schema_id, d.name \
         FROM sched_worker_assignment_schemas w \
         JOIN schemas s ON s.id = w.schema_id \
         JOIN datasets d ON d.id = s.dataset_id",
    )
    .fetch_all(&mut *tx)
    .await
    .context("generate_bundle: collect refs")?;

    let schema_ids: Vec<SchemaId> = refs.iter().map(|(id, _)| *id).collect();
    let datasets: BTreeSet<&str> = refs.iter().map(|(_, d)| d.as_str()).collect();
    let dataset_refs: Vec<&str> = datasets.into_iter().collect();

    let schemas = scheduler_metadata::pg::schema::load_schemas(&mut tx, Some(&schema_ids)).await?;
    let missing: Vec<SchemaId> = schema_ids
        .iter()
        .filter(|id| !schemas.contains_key(id))
        .copied()
        .collect();
    if !missing.is_empty() {
        anyhow::bail!("generate_bundle: schemas table is missing referenced ids {missing:?}");
    }
    let read_schemas = read_schemas_by_id(&mut tx, &dataset_refs).await?;

    tx.commit().await.context("generate_bundle: commit")?;
    Ok(SchemaBundle::from_sections(schemas, read_schemas))
}
