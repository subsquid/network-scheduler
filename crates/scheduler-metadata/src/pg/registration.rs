//! The scheduler's discovery insert: bulk chunk rows, one transaction. Deliberately lock-free —
//! the async admission gate (`register_new_chunks`, run later) is its only overlap arbiter until
//! the dual-write follow-up (see `pg/lock.rs`).

use anyhow::Context;
use sqlx::Connection;
use sqlx::postgres::PgConnection;

use crate::error::StorageError;
use crate::metrics::PhaseTimer;
use crate::new_chunk::NewChunk;

use super::rows::{ChunkInsertArrays, bind_chunk_arrays, is_unique_violation};

/// Insert raw chunk rows, batched in one transaction; one `UNNEST` statement per batch.
pub async fn insert_chunks(
    conn: &mut PgConnection,
    chunks: &[NewChunk],
    batch_size: usize,
) -> Result<(), StorageError> {
    let mut timer = PhaseTimer::new("insert_new_chunks");
    if chunks.is_empty() {
        return Ok(());
    }
    let mut tx = conn.begin().await.context("insert_new_chunks: begin")?;
    // No dataset lock: the UNIQUE index, composite FK, and single-instance admission gate enforce every invariant; a
    // missing dataset just joins fewer rows than the batch (caught below).
    // Deterministic (dataset, id) order so concurrent batches take the tuple locks in the same order and can't deadlock.
    let mut ordered: Vec<&NewChunk> = chunks.iter().collect();
    ordered.sort_unstable_by(|a, b| a.dataset.cmp(&b.dataset).then_with(|| a.id.cmp(&b.id)));
    for batch in ordered.chunks(batch_size) {
        let datasets: Vec<&str> = batch.iter().map(|c| c.dataset.as_str()).collect();
        let p = ChunkInsertArrays::from_chunks(batch.iter().copied());
        let inserted = bind_chunk_arrays!(
            sqlx::query(
                r#"
            INSERT INTO chunks (dataset_id, chunk_id, size, schema_id, tables_present, first_block, last_block_delta, last_block_hash, last_block_timestamp)
            SELECT d.id, x.chunk_id, x.size, x.schema_id, x.tables_present, x.first_block, x.last_block_delta, x.last_block_hash, x.last_block_timestamp
            FROM UNNEST($1::text[], $2::text[], $3::int[], $4::int[], $5::varbit[], $6::bigint[], $7::int[], $8::text[], $9::bigint[])
                 AS x(dataset, chunk_id, size, schema_id, tables_present, first_block, last_block_delta, last_block_hash, last_block_timestamp)
            JOIN datasets d ON d.name = x.dataset
            "#,
            )
            .bind(&datasets),
            p
        )
        .execute(&mut *tx)
        .await;
        match inserted {
            Ok(res) => {
                timer.stmt(res.rows_affected());
                // INSERT ... SELECT reports inserted rows; a shortfall means a name joined nothing.
                if res.rows_affected() < batch.len() as u64 {
                    return Err(anyhow::anyhow!("dataset not found").into());
                }
            }
            Err(e) if is_unique_violation(&e) => {
                return Err(StorageError::ChunkAlreadyExists);
            }
            Err(e) => return Err(anyhow::Error::new(e).context("insert_new_chunks").into()),
        }
    }
    tx.commit().await.context("insert_new_chunks: commit")?;
    Ok(())
}
