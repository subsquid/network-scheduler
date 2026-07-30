//! Worker-row helpers, split across their callers: [`PostgresStorage::update_worker_set`] (status
//! columns only), the scheduling cycle's phase A ([`delete_inactive_stale_mappings`],
//! [`promote_orphaned_drains`] — keyed on `sched_workers.inactive_since`), and
//! [`PostgresStorage::gc_inactive_workers`].

use anyhow::{Context, Result};
use sqlx::{Postgres, Transaction};

use crate::metrics::PhaseTimer;

use super::rows::tick_to_i64;

/// Register the active workers: insert the unseen ones, reactivate and refresh the rest.
///
/// Two statements, not one `ON CONFLICT DO UPDATE`: Postgres evaluates the `id` sequence default
/// before it detects the conflict, so an upsert burns an id per already-registered worker on every
/// sync — enough to exhaust the 32-bit sequence within a year.
///
/// NOTE: the `NOT EXISTS` screen is not atomic — it leans on the single-scheduler advisory lock. A
/// racing insert would still land correct (`DO NOTHING`, then the UPDATE reactivates it), at the
/// cost of one id.
pub(super) async fn upsert_active(
    tx: &mut Transaction<'_, Postgres>,
    peer_ids: &[String],
    versions: &[Option<String>],
) -> Result<()> {
    let mut timer = PhaseTimer::new("update_worker_set:upsert_active");
    let res = sqlx::query(
        r#"
        INSERT INTO sched_workers (peer_id, version)
        SELECT w.peer_id, w.version
        FROM UNNEST($1::text[], $2::text[]) AS w(peer_id, version)
        WHERE NOT EXISTS (SELECT 1 FROM sched_workers s WHERE s.peer_id = w.peer_id)
        ON CONFLICT (peer_id) DO NOTHING
        "#,
    )
    .bind(peer_ids)
    .bind(versions)
    .execute(&mut **tx)
    .await
    .context("update_worker_set: insert new workers")?;
    timer.stmt(res.rows_affected());

    // Skipping rows already in the wanted state keeps a steady active set from rewriting every
    // worker row each sync.
    let res = sqlx::query(
        r#"
        UPDATE sched_workers s
        SET inactive_since = NULL, version = w.version
        FROM UNNEST($1::text[], $2::text[]) AS w(peer_id, version)
        WHERE s.peer_id = w.peer_id
          AND (s.inactive_since IS NOT NULL OR s.version IS DISTINCT FROM w.version)
        "#,
    )
    .bind(peer_ids)
    .bind(versions)
    .execute(&mut **tx)
    .await
    .context("update_worker_set: reactivate returning workers")?;
    timer.stmt(res.rows_affected());
    Ok(())
}

/// Stamp `inactive_since` on workers no longer in the active set.
/// `inactive_since IS NULL` keeps the original timestamp on already-inactive workers, so a
/// departure's clock starts once. Departed workers' mapping-table state is settled later, by the
/// scheduling cycle's state-based cleanup.
pub(super) async fn mark_departed(
    tx: &mut Transaction<'_, Postgres>,
    peer_ids: &[String],
    now: u64,
) -> Result<()> {
    let mut timer = PhaseTimer::new("update_worker_set:mark_departed");
    let res = sqlx::query(
        r#"
        UPDATE sched_workers
        SET inactive_since = $2
        WHERE NOT (peer_id = ANY($1))
          AND inactive_since IS NULL
        "#,
    )
    .bind(peer_ids)
    .bind(tick_to_i64(now))
    .execute(&mut **tx)
    .await
    .context("update_worker_set: mark departed")?;
    timer.stmt(res.rows_affected());
    Ok(())
}

/// Drop stale mappings held by inactive workers. State-based: any departure detected by an earlier
/// `update_worker_set` is settled here, whether it happened one tick or several cycles ago.
pub(super) async fn delete_inactive_stale_mappings(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<()> {
    let mut timer = PhaseTimer::new("run_scheduling_cycle:delete_inactive_stale_mappings");
    let res = sqlx::query(
        r#"
        DELETE FROM sched_stale_mappings st
        USING sched_workers w
        WHERE w.id = st.worker_id AND w.inactive_since IS NOT NULL
        "#,
    )
    .execute(&mut **tx)
    .await
    .context("run_scheduling_cycle: delete inactive stale mappings")?;
    timer.stmt(res.rows_affected());
    Ok(())
}

/// Turn a draining copy back into a committed holder when every worker it was handing off to has left.
///
/// Why: "confirmed" means a quorum of *active* workers acknowledged the handoff. Once the recipients
/// leave, the quorum stops waiting for them, so the handoff counts as confirmed even though no one
/// ever downloaded the copy (vacuous confirmation — Invariant 2, docs/mvcc-storage.md). The drain's
/// expiry clock trusts that and would delete the fleet's last real copy; promoting it back to
/// committed takes it off that clock and under the retention floor.
///
/// No follow-up needed: leftover copies become ordinary drains next cycle, and the departed workers'
/// ideal rows drop out with the next diff.
///
/// Runs in phase A of every cycle, after [`delete_inactive_stale_mappings`] (every leftover stale
/// row belongs to an active worker, which the promoted CTE also enforces) and structurally before
/// [`expire_drained_stale_mappings`](super::scheduling_cycle::expire_drained_stale_mappings) — the
/// rescue precedes the reaper.
pub(super) async fn promote_orphaned_drains(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    let mut timer = PhaseTimer::new("run_scheduling_cycle:promote_orphaned_drains");
    let res = sqlx::query(
        r#"
        WITH orphaned AS (
            SELECT i.chunk_pk
            FROM sched_ideal_chunk_workers i
            WHERE cardinality(i.worker_ids) > 0
              AND NOT EXISTS (
                  SELECT 1 FROM sched_workers w
                  WHERE w.id = ANY(i.worker_ids) AND w.inactive_since IS NULL
              )
        ),
        promoted AS (
            DELETE FROM sched_stale_mappings st
            USING orphaned o, sched_workers w
            WHERE st.chunk_pk = o.chunk_pk
              AND w.id = st.worker_id
              AND w.inactive_since IS NULL
            RETURNING st.chunk_pk, st.worker_id
        )
        UPDATE sched_ideal_chunk_workers i
        SET worker_ids = (
            SELECT array_agg(DISTINCT v ORDER BY v)
            FROM unnest(i.worker_ids || p.extra) AS v
        )
        FROM (
            SELECT chunk_pk, array_agg(worker_id) AS extra
            FROM promoted
            GROUP BY chunk_pk
        ) p
        WHERE i.chunk_pk = p.chunk_pk
        "#,
    )
    .execute(&mut **tx)
    .await
    .context("run_scheduling_cycle: promote orphaned drains")?;
    timer.stmt(res.rows_affected());
    Ok(())
}

/// Delete workers inactive for longer than `gc_ticks`; the FK cascade takes their stale mappings
/// along. Deliberately a single READ COMMITTED statement, never REPEATABLE READ: a concurrent
/// reactivation then resolves via the row lock — the DELETE waits, re-checks `inactive_since`
/// against the new row version, and skips the returning worker.
pub(super) async fn gc_inactive_workers(
    tx: &mut Transaction<'_, Postgres>,
    now: u64,
    gc_ticks: u64,
) -> Result<()> {
    let mut timer = PhaseTimer::new("gc_inactive_workers");
    let res = sqlx::query("DELETE FROM sched_workers WHERE inactive_since < $1 - $2")
        .bind(tick_to_i64(now))
        .bind(tick_to_i64(gc_ticks))
        .execute(&mut **tx)
        .await
        .context("gc_inactive_workers")?;
    timer.stmt(res.rows_affected());
    Ok(())
}
