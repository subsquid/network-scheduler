//! Phase helpers for [`PostgresStorage::update_worker_set`], run inside the
//! update's transaction.

use anyhow::{Context, Result};
use sqlx::{Postgres, Row, Transaction};

use crate::metrics::PhaseTimer;
use crate::scheduler_storage::WorkerPk;

use super::rows::tick_to_i64;

/// Upsert the active workers: clear `inactive_since` and refresh `version`.
pub(super) async fn upsert_active(
    tx: &mut Transaction<'_, Postgres>,
    peer_ids: &[String],
    versions: &[Option<String>],
) -> Result<()> {
    // peer_ids must be unique within the batch or ON CONFLICT errors on a repeated row; the active
    // set is keyed by PeerId upstream, so they are.
    let mut timer = PhaseTimer::new("update_worker_set:upsert_active");
    let res = sqlx::query(
        r#"
        INSERT INTO sched_workers (peer_id, version)
        SELECT peer_id, version FROM UNNEST($1::text[], $2::text[]) AS w(peer_id, version)
        ON CONFLICT (peer_id) DO UPDATE
            SET inactive_since = NULL,
                version = EXCLUDED.version
        "#,
    )
    .bind(peer_ids)
    .bind(versions)
    .execute(&mut **tx)
    .await
    .context("update_worker_set: upsert")?;
    timer.stmt(res.rows_affected());
    Ok(())
}

/// Stamp `inactive_since` on workers no longer in the active set, returning the ones newly departed.
/// `inactive_since IS NULL` keeps the original timestamp and stops already-inactive workers from
/// being re-reported as departed.
pub(super) async fn mark_departed(
    tx: &mut Transaction<'_, Postgres>,
    peer_ids: &[String],
    now: u64,
) -> Result<Vec<WorkerPk>> {
    let mut timer = PhaseTimer::new("update_worker_set:mark_departed");
    let rows = sqlx::query(
        r#"
        UPDATE sched_workers
        SET inactive_since = $2
        WHERE NOT (peer_id = ANY($1))
          AND inactive_since IS NULL
        RETURNING id
        "#,
    )
    .bind(peer_ids)
    .bind(tick_to_i64(now))
    .fetch_all(&mut **tx)
    .await
    .context("update_worker_set: mark departed")?;
    timer.stmt(rows.len() as u64);
    Ok(rows
        .iter()
        .map(|row| row.get::<WorkerPk, _>("id"))
        .collect())
}

/// Drop stale mappings held by workers that just departed.
pub(super) async fn delete_stale_mappings(
    tx: &mut Transaction<'_, Postgres>,
    departed: &[WorkerPk],
) -> Result<()> {
    if departed.is_empty() {
        return Ok(());
    }
    let mut timer = PhaseTimer::new("update_worker_set:delete_stale_mappings");
    let res = sqlx::query("DELETE FROM sched_stale_mappings WHERE worker_id = ANY($1)")
        .bind(departed)
        .execute(&mut **tx)
        .await
        .context("update_worker_set: delete stale mappings")?;
    timer.stmt(res.rows_affected());
    Ok(())
}

/// Delete workers inactive for longer than `gc_ticks`.
pub(super) async fn gc_inactive(
    tx: &mut Transaction<'_, Postgres>,
    now: u64,
    gc_ticks: u64,
) -> Result<()> {
    let mut timer = PhaseTimer::new("update_worker_set:gc_inactive");
    let res = sqlx::query("DELETE FROM sched_workers WHERE inactive_since < $1 - $2")
        .bind(tick_to_i64(now))
        .bind(tick_to_i64(gc_ticks))
        .execute(&mut **tx)
        .await
        .context("update_worker_set: gc workers")?;
    timer.stmt(res.rows_affected());
    Ok(())
}
