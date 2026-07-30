//! Scheduler-side schema probe over `sched_chunk_metadata`: which read schemas are still in play.
//! Joins a scheduler-owned table, so it lives here rather than in `scheduler-metadata`.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use sqlx::postgres::PgConnection;

use sqlx::Connection;

use crate::scheduler_storage::{ReadSchemaId, SchemaBundle, SchemaId};
use crate::types::DatasetSchema;

/// Each named dataset's current read-schema id, for the portal assignment to publish.
///
/// Driven from `datasets` with a LEFT JOIN, so the result is TOTAL over the named set: a dataset
/// with no read row yields `None`. That is what makes "never promoted" publishable and distinct from
/// a key the backend failed to produce. Compare [`read_schemas_by_id`], which inner-joins because a
/// dataset without a read schema has no payload to carry.
///
/// Ids only — the content travels in the bundle, built in another transaction, so there is nothing
/// here for the id to be atomically consistent with.
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

/// The bundle's read section: payloads keyed by id, for the named datasets. The caller passes the
/// routable window it already scanned, making this a keyed lookup.
///
/// Inner join, unlike [`read_schema_ids_by_dataset`]: a dataset with no read schema contributes no
/// payload, and the bundle is keyed by id, so there is no slot for an absent one.
///
/// Not a per-dataset `EXISTS` over `chunks`. A read schema is a dataset-level pointer no chunk need
/// be written under, so the predicate would be "has this dataset a live chunk" — and proving that
/// negative for a retired dataset walks all its chunk rows, every cycle, forever, since nothing
/// deletes `datasets` or `chunks` and no index matches the routable predicate. (Contrast
/// [`active_schema_bundle`]: its probe is per-*schema* and rides `chunks_schema_id`.)
///
/// One statement, so no concurrent promote can open a resolve-then-load gap; a promote either side
/// of it costs a cycle of freshness, never resolvability.
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

/// The generator: one bundle from committed rows alone, identical under success, shortage, and on
/// a fresh process. Write section: the routable window ∪ the persisted set of the last successful
/// assignment (the window alone can shrink below what the frozen assignment names — Phase A keeps
/// tombstoning during a shortage). Read section: the CURRENT read schema of every dataset either
/// side references, so a promote always reaches the next bundle.
///
/// One `REPEATABLE READ, READ ONLY` transaction: everything is read from a single snapshot, so a
/// concurrent cycle or promote cannot skew the sections against each other. Loads are strict — a
/// referenced id whose `schemas` row is missing is an error, never a silent shrink.
pub(super) async fn generate_bundle(conn: &mut PgConnection) -> Result<SchemaBundle> {
    let _timer = crate::metrics::Timer::new("generate_schema_bundle");
    let mut tx = conn.begin().await.context("generate_bundle: begin")?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await
        .context("generate_bundle: set isolation")?;

    // (schema id, dataset name) over the window ∪ the persisted set. The persisted arm resolves
    // its dataset through the schema row — `chunks_schema_same_dataset` makes that the chunk's own
    // dataset.
    let refs: Vec<(SchemaId, String)> = sqlx::query_as(
        "SELECT DISTINCT c.schema_id, d.name \
         FROM sched_chunk_metadata m \
         JOIN chunks c ON c.chunk_pk = m.chunk_pk \
         JOIN datasets d ON d.id = c.dataset_id \
         WHERE m.applied_at_worker_assignment_id IS NOT NULL \
           AND m.dropped_from_worker_assignment_at IS NULL \
         UNION \
         SELECT w.schema_id, d.name \
         FROM sched_worker_assignment_schemas w \
         JOIN schemas s ON s.id = w.schema_id \
         JOIN datasets d ON d.id = s.dataset_id",
    )
    .fetch_all(&mut *tx)
    .await
    .context("generate_bundle: collect refs")?;

    let schema_ids: Vec<SchemaId> = refs
        .iter()
        .map(|(id, _)| *id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
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
