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

/// The current read-schema *id* of each named dataset — the reference a portal assignment
/// publishes. Driven by the exact dataset list the caller derived from its (post-eviction) chunk
/// set, so it is total over that set by construction: a dataset with visible chunks and no read
/// row yields `None`, which is a published answer, not an omission.
///
/// Ids only, no payload: the content travels in the bundle, built in a different transaction, so
/// there is nothing here for the id to need atomic consistency with. That is why this can be a
/// plain indexed lookup over a handful of rows rather than a per-dataset `EXISTS` over `chunks` —
/// the latter costs O(chunks) per *retired* dataset, every cycle, forever, since nothing deletes
/// `datasets` rows and no index matches the portal-visible predicate.
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

/// The current read schema of each named dataset, id and content together — the bundle's read
/// section. The caller passes the datasets of the routable window it already scanned, so this is a
/// keyed lookup over a handful of rows.
///
/// Deliberately NOT a per-dataset `EXISTS` over `chunks`. A read schema is a dataset-level pointer
/// that no chunk need be written under, so the natural predicate is "does this dataset have a live
/// chunk" — and proving that *negative* for a retired dataset means walking every one of its chunk
/// rows, every cycle, forever: nothing deletes `datasets` or `chunks` rows, and no index matches
/// the routable predicate. (Contrast [`active_schema_bundle`], whose probe is per-*schema* and
/// rides the purpose-built `chunks_schema_id` index.) Taking the answer from the cycle's own scan
/// costs nothing and cannot drift from it.
///
/// Id and content come from one statement, so there is no resolve-then-load gap a concurrent
/// promote could open. A promote landing either side of it costs one cycle of freshness, never
/// resolvability.
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

/// [`read_schemas_for_datasets`] over the whole routable window, for callers that have not already
/// scanned it — the `SchedulerStorage` trait method, used by tests and by
/// [`SchemaBundle::generate`](crate::scheduler_storage::SchemaBundle). The scheduling cycle uses
/// the keyed form; both must select the same window.
///
/// Enumerating forward (metadata → chunks → read_schemas) rather than proving a per-dataset
/// negative reads better, but measured on 50K tombstoned rows it is **not** cheaper: no index
/// matches `applied_at_worker_assignment_id IS NOT NULL AND dropped_from_worker_assignment_at IS
/// NULL`, so either shape seq-scans `sched_chunk_metadata` (~358 vs ~350 buffers). That is fine
/// here because no production path calls this — the cycle passes its own dataset set and reads 2
/// buffers — but do not reach for this form expecting it to scale. The same missing index is why
/// `fetch_portal_visible_chunks` already scans that table every visibility cycle.
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
