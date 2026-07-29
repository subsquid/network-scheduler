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

/// The current read schema of every dataset with a chunk in the same routable window
/// [`active_schema_bundle`] uses. Scoping to that window (rather than to every row in `datasets`)
/// is what lets the read section shrink: a decommissioned dataset drops out once its last chunk is
/// tombstoned, on the same clock as its write schemas.
///
/// Id and content come from one statement, so there is no resolve-then-load gap a concurrent
/// promote could open. A promote landing either side of it costs one cycle of freshness, never
/// resolvability.
pub(super) async fn active_read_schemas(
    conn: &mut PgConnection,
) -> Result<BTreeMap<ReadSchemaId, DatasetSchema>> {
    let mut timer = crate::metrics::PhaseTimer::new("active_read_schemas");
    let rows: Vec<(ReadSchemaId, sqlx::types::Json<DatasetSchema>)> = sqlx::query_as(
        "SELECT r.id, r.schema FROM read_schemas r \
         WHERE r.superseded_at IS NULL \
           AND EXISTS ( \
               SELECT 1 FROM chunks c \
               JOIN sched_chunk_metadata m ON m.chunk_pk = c.chunk_pk \
               WHERE c.dataset_id = r.dataset_id \
                 AND m.applied_at_worker_assignment_id IS NOT NULL \
                 AND m.dropped_from_worker_assignment_at IS NULL \
           )",
    )
    .fetch_all(conn)
    .await
    .context("active_read_schemas")?;
    timer.stmt(rows.len() as u64);
    Ok(rows.into_iter().map(|(id, json)| (id, json.0)).collect())
}
