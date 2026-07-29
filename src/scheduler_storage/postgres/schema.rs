//! Scheduler-side schema probe over `sched_chunk_metadata`: which read schemas are still in play.
//! Joins a scheduler-owned table, so it lives here rather than in `scheduler-metadata`.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use sqlx::postgres::PgConnection;

use crate::scheduler_storage::{ReadSchemaId, SchemaId};
use crate::types::DatasetSchema;

/// Schemas of chunks placed on a worker at some point and not yet tombstoned. Portal-served chunks
/// are already covered: portal promotion requires prior worker placement, and tombstoning requires
/// a prior portal drop. The outer `EXISTS` is a semi-join: for each schema it walks the
/// `chunks_schema_id` index and probes `sched_chunk_metadata` by PK, stopping at the first live
/// chunk. Cheap when a schema has any live chunk; a fully-drained schema costs a full scan of its
/// chunks to prove the negative.
///
/// Approximate on purpose: a chunk that already drained off every worker still counts until
/// `dropped_from_worker_assignment_at` is stamped, which only ever widens the bundle, never
/// narrows it.
pub(super) async fn active_schema_bundle(
    conn: &mut PgConnection,
) -> Result<BTreeMap<SchemaId, DatasetSchema>> {
    let mut timer = crate::metrics::PhaseTimer::new("active_schema_bundle");
    let rows: Vec<(SchemaId, sqlx::types::Json<DatasetSchema>)> = sqlx::query_as(
        "SELECT s.id, s.schema FROM schemas s WHERE EXISTS ( \
             SELECT 1 FROM chunks c \
             JOIN sched_chunk_metadata m ON m.chunk_pk = c.chunk_pk \
             WHERE c.schema_id = s.id \
               AND m.applied_at_worker_assignment_id IS NOT NULL \
               AND m.dropped_from_worker_assignment_at IS NULL \
         )",
    )
    .fetch_all(conn)
    .await
    .context("active_schema_bundle")?;
    timer.stmt(rows.len() as u64);
    Ok(rows.into_iter().map(|(id, json)| (id, json.0)).collect())
}

/// Each named dataset's current read-schema id, for the portal assignment to publish. Keyed on the
/// caller's post-eviction chunk set, so it is total over that set: a dataset with no read row yields
/// `None`, which is an answer, not an omission.
///
/// Ids only — the content travels in the bundle, built in another transaction, so there is nothing
/// here for the id to be atomically consistent with.
pub(super) async fn portal_read_schema_refs(
    conn: &mut PgConnection,
    datasets: &[&str],
) -> Result<BTreeMap<crate::types::DatasetId, Option<ReadSchemaId>>> {
    let mut timer = crate::metrics::PhaseTimer::new("portal_read_schema_refs");
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
    .context("portal_read_schema_refs")?;
    timer.stmt(rows.len() as u64);
    Ok(rows
        .into_iter()
        .map(|(name, id)| (std::sync::Arc::new(name), id))
        .collect())
}

/// The bundle's read section: id and content for each named dataset. The caller passes the datasets
/// from the routable window it already scanned, making this a keyed lookup.
///
/// Not a per-dataset `EXISTS` over `chunks`. A read schema is a dataset-level pointer no chunk need
/// be written under, so the predicate would be "has this dataset a live chunk" — and proving that
/// negative for a retired dataset walks all its chunk rows, every cycle, forever, since nothing
/// deletes `datasets` or `chunks` and no index matches the routable predicate. (Contrast
/// [`active_schema_bundle`]: its probe is per-*schema* and rides `chunks_schema_id`.)
///
/// One statement, so no concurrent promote can open a resolve-then-load gap; a promote either side
/// of it costs a cycle of freshness, never resolvability.
pub(super) async fn read_schemas_for_datasets(
    conn: &mut PgConnection,
    datasets: &[&str],
) -> Result<BTreeMap<ReadSchemaId, DatasetSchema>> {
    let mut timer = crate::metrics::PhaseTimer::new("read_schemas_for_datasets");
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
    .context("read_schemas_for_datasets")?;
    timer.stmt(rows.len() as u64);
    Ok(rows.into_iter().map(|(id, json)| (id, json.0)).collect())
}

/// [`read_schemas_for_datasets`] over the whole routable window, for callers without one already —
/// the trait method, used by tests. The cycle uses the keyed form; both must select the same window.
///
/// Enumerating forward reads better than a per-dataset negative but measured no cheaper (~358 vs
/// ~350 buffers on 50K tombstoned rows): no index matches the routable predicate, so either shape
/// seq-scans `sched_chunk_metadata`. Acceptable only because no production path calls this — the
/// cycle reads 2 buffers. The same missing index is why `fetch_portal_visible_chunks` scans that
/// table every visibility cycle.
pub(super) async fn active_read_schemas(
    conn: &mut PgConnection,
) -> Result<BTreeMap<ReadSchemaId, DatasetSchema>> {
    let mut timer = crate::metrics::PhaseTimer::new("active_read_schemas");
    let rows: Vec<(ReadSchemaId, sqlx::types::Json<DatasetSchema>)> = sqlx::query_as(
        "SELECT DISTINCT r.id, r.schema \
         FROM sched_chunk_metadata m \
         JOIN chunks c ON c.chunk_pk = m.chunk_pk \
         JOIN read_schemas r ON r.dataset_id = c.dataset_id AND r.superseded_at IS NULL \
         WHERE m.applied_at_worker_assignment_id IS NOT NULL \
           AND m.dropped_from_worker_assignment_at IS NULL",
    )
    .fetch_all(conn)
    .await
    .context("active_read_schemas")?;
    timer.stmt(rows.len() as u64);
    Ok(rows.into_iter().map(|(id, json)| (id, json.0)).collect())
}
