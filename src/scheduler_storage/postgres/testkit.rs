//! Server-side helpers for tests and offline tools (`pg-testkit`) — not part of the production
//! scheduling paths.

use anyhow::Context;
use sqlx::postgres::PgConnection;

use crate::metrics::PhaseTimer;
use crate::scheduler_storage::StorageError;

/// Clone every live chunk of dataset `src` into dataset `dst`, server-side. Clones are stamped
/// with dst's latest write schema (src's pin belongs to src — the same-dataset FK) and enter
/// unregistered; the next `register_new_chunks` admits them. Returns the number of clones.
pub(super) async fn copy_dataset_chunks(
    conn: &mut PgConnection,
    src: &str,
    dst: &str,
) -> Result<u64, StorageError> {
    let mut timer = PhaseTimer::new("copy_dataset_chunks");
    // LIVE_ADMITTED: the same liveness set the registration probe checks against.
    let sql = format!(
        r#"
        INSERT INTO chunks (dataset_id, chunk_id, size, schema_id, first_block, last_block_delta, last_block_hash, last_block_timestamp)
        SELECT dd.id, c.chunk_id, c.size, cur.id, c.first_block, c.last_block_delta, c.last_block_hash, c.last_block_timestamp
        FROM chunks c
        JOIN datasets ds ON ds.id = c.dataset_id
        JOIN datasets dd ON dd.name = $2
        JOIN schemas cur ON cur.dataset_id = dd.id
             AND cur.id = (SELECT max(id) FROM schemas WHERE dataset_id = dd.id)
        JOIN sched_chunk_metadata s ON s.chunk_pk = c.chunk_pk
        WHERE ds.name = $1
          AND {}
        "#,
        super::nonoverlap::LIVE_ADMITTED
    );
    let res = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(src)
        .bind(dst)
        .execute(&mut *conn)
        .await
        .context("copy_dataset_chunks")?;
    timer.stmt(res.rows_affected());
    Ok(res.rows_affected())
}
