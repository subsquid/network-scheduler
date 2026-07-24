//! Bulk same-range chunk replacement (single-swap `register_correction` is a batch of one); reorgs
//! drive this. Schema enforces most invariants on insert; the guards here cover what no constraint
//! can. One transaction, all-or-nothing.

use std::collections::HashMap;

use anyhow::Context;
use sqlx::postgres::PgConnection;
use sqlx::{Connection, Postgres, Transaction};

use crate::error::StorageError;
use crate::ids::ChunkPk;
use crate::metrics::PhaseTimer;
use crate::new_chunk::NewChunk;

use super::rows::{
    ChunkInsertArrays, Tick, bind_chunk_arrays, block_range_columns, is_unique_violation,
    tick_to_i64,
};

/// Register corrections in bulk: one transaction, all-or-nothing. Returns the replacement pks in
/// input order.
pub async fn register_corrections(
    conn: &mut PgConnection,
    corrections: &[(ChunkPk, NewChunk)],
    now: Tick,
    batch_size: usize,
) -> Result<Vec<ChunkPk>, StorageError> {
    let mut timer = PhaseTimer::new("register_corrections");
    let mut tx = conn.begin().await.context("register_corrections: begin")?;

    // No dataset lock: the chunk UNIQUE/FK, chunk_corrections PK, and admission gate enforce every invariant.
    // Sort by old-chunk pk so concurrent batches lock chunk_corrections PKs in the same order and can't deadlock.
    let mut ordered = corrections.to_vec();
    ordered.sort_unstable_by_key(|(old_pk, _)| old_pk.0);
    let mut new_by_old = HashMap::with_capacity(corrections.len());
    for batch in ordered.chunks(batch_size) {
        reject_unavailable_olds(&mut tx, batch).await?;
        reject_changed_ranges(&mut tx, batch).await?;
        insert_batch(&mut tx, batch, now, &mut new_by_old).await?;
        timer.stmt(batch.len() as u64);
    }

    tx.commit().await.context("register_corrections: commit")?;
    corrections
        .iter()
        .map(|(old_pk, _)| {
            new_by_old
                .get(old_pk)
                .copied()
                .ok_or_else(|| StorageError::CorrectionRejected {
                    reason: format!("no correction row created for old chunk {old_pk}"),
                })
        })
        .collect()
}

/// Reject an old chunk already on its way out (rejected/removed/dropped) — nothing left to supersede.
/// A missing old chunk passes here, caught by the link insert's FK instead.
async fn reject_unavailable_olds(
    tx: &mut Transaction<'_, Postgres>,
    batch: &[(ChunkPk, NewChunk)],
) -> Result<(), StorageError> {
    let old_pks: Vec<ChunkPk> = batch.iter().map(|(pk, _)| *pk).collect();
    let unavailable: Option<ChunkPk> = sqlx::query_scalar(
        r#"
        SELECT chunk_pk FROM sched_chunk_metadata
        WHERE chunk_pk = ANY($1)
          AND (rejected
               OR marked_for_removal IS NOT NULL
               OR dropped_at_portal_assignment_id IS NOT NULL
               OR dropped_from_worker_assignment_at IS NOT NULL)
        LIMIT 1
        "#,
    )
    .bind(&old_pks)
    .fetch_optional(&mut **tx)
    .await
    .context("register_corrections: check old chunk availability")?;
    if let Some(old_pk) = unavailable {
        return Err(StorageError::CorrectionRejected {
            reason: format!("old chunk {old_pk} is not in a supersedable state"),
        });
    }
    Ok(())
}

/// Refuse a range-changing replacement — it would leave a dangling correction or publish an overlap.
async fn reject_changed_ranges(
    tx: &mut Transaction<'_, Postgres>,
    batch: &[(ChunkPk, NewChunk)],
) -> Result<(), StorageError> {
    let mut old_pks = Vec::with_capacity(batch.len());
    let mut firsts = Vec::with_capacity(batch.len());
    let mut deltas = Vec::with_capacity(batch.len());
    for (old_pk, chunk) in batch {
        let (first_block, last_block_delta) = block_range_columns(&chunk.blocks);
        old_pks.push(*old_pk);
        firsts.push(first_block);
        deltas.push(last_block_delta);
    }
    let changed: Option<(ChunkPk, i64, i32, i64, i32)> = sqlx::query_as(
        r#"
        SELECT x.old_pk, x.first_block, x.last_block_delta, oc.first_block, oc.last_block_delta
        FROM UNNEST($1::bigint[], $2::bigint[], $3::int[]) AS x(old_pk, first_block, last_block_delta)
        JOIN chunks oc ON oc.chunk_pk = x.old_pk
        WHERE (oc.first_block, oc.last_block_delta)
              IS DISTINCT FROM (x.first_block, x.last_block_delta)
        LIMIT 1
        "#,
    )
    .bind(&old_pks)
    .bind(&firsts)
    .bind(&deltas)
    .fetch_optional(&mut **tx)
    .await
    .context("register_corrections: check ranges")?;
    if let Some((old_pk, new_first, new_delta, old_first, old_delta)) = changed {
        return Err(StorageError::CorrectionRejected {
            reason: format!(
                "replacement range {new_first}..={} does not match old chunk {old_pk} range {old_first}..={}",
                new_first + i64::from(new_delta),
                old_first + i64::from(old_delta),
            ),
        });
    }
    Ok(())
}

/// Insert one batch's replacement chunks and correction links in a single statement; fills `new_by_old`.
async fn insert_batch(
    tx: &mut Transaction<'_, Postgres>,
    batch: &[(ChunkPk, NewChunk)],
    now: Tick,
    new_by_old: &mut HashMap<ChunkPk, ChunkPk>,
) -> Result<(), StorageError> {
    let old_pks: Vec<ChunkPk> = batch.iter().map(|(old_pk, _)| *old_pk).collect();
    let datasets: Vec<&str> = batch
        .iter()
        .map(|(_, chunk)| chunk.dataset.as_str())
        .collect();
    let p = ChunkInsertArrays::from_chunks(batch.iter().map(|(_, chunk)| chunk));

    // ins CTE inserts the chunks; the outer INSERT joins back on unique (dataset_id, chunk_id) to pair each fresh pk with its old.
    let inserted = bind_chunk_arrays!(
        sqlx::query_as::<_, (ChunkPk, ChunkPk)>(
            r#"
        WITH payload AS (
            SELECT x.old_pk, d.id AS dataset_id, x.chunk_id, x.size,
                   x.schema_id, x.tables_present,
                   x.first_block, x.last_block_delta, x.last_block_hash, x.last_block_timestamp
            FROM UNNEST($1::bigint[], $2::text[], $3::text[], $4::int[],
                        $5::int[], $6::varbit[], $7::bigint[], $8::int[], $9::text[], $10::bigint[])
                 AS x(old_pk, dataset, chunk_id, size,
                      schema_id, tables_present,
                      first_block, last_block_delta, last_block_hash, last_block_timestamp)
            JOIN datasets d ON d.name = x.dataset
        ),
        ins AS (
            INSERT INTO chunks (
                dataset_id, chunk_id, size, schema_id, tables_present,
                first_block, last_block_delta, last_block_hash, last_block_timestamp
            )
            SELECT
                dataset_id, chunk_id, size, schema_id, tables_present,
                first_block, last_block_delta, last_block_hash, last_block_timestamp
            FROM payload
            RETURNING chunk_pk, dataset_id, chunk_id
        )
        INSERT INTO chunk_corrections (old_chunk_pk, new_chunk_pk, dataset_id, created_at)
        SELECT p.old_pk, i.chunk_pk, i.dataset_id, $11
        FROM ins i
        JOIN payload p ON p.dataset_id = i.dataset_id AND p.chunk_id = i.chunk_id
        RETURNING old_chunk_pk, new_chunk_pk
        "#,
        )
        .bind(&old_pks)
        .bind(&datasets),
        p
    )
    .bind(tick_to_i64(now))
    .fetch_all(&mut **tx)
    .await;
    let rows = match inserted {
        Ok(rows) => rows,
        // Constraint name says which uniqueness broke: replacement (dataset_id, chunk_id) or the one-correction-per-old-chunk PK.
        Err(e) if is_unique_violation(&e) => {
            let reason = match e.as_database_error().and_then(|d| d.constraint()) {
                Some("chunk_corrections_pkey") => {
                    "a correction for an old chunk in the batch already exists".to_string()
                }
                _ => "a replacement chunk in the batch already exists".to_string(),
            };
            return Err(StorageError::CorrectionRejected { reason });
        }
        Err(e) => {
            return Err(anyhow::Error::new(e)
                .context("register_corrections: insert batch")
                .into());
        }
    };
    // Fewer rows than inputs: the payload→datasets join dropped a replacement naming an unregistered dataset.
    if rows.len() < batch.len() {
        let unknown = unknown_dataset(tx, batch).await;
        return Err(StorageError::CorrectionRejected {
            reason: format!("replacement chunk dataset {unknown} is unknown"),
        });
    }
    new_by_old.extend(rows);
    Ok(())
}

/// Error-path diagnostic: name a batch dataset missing from `datasets`.
async fn unknown_dataset(
    tx: &mut Transaction<'_, Postgres>,
    batch: &[(ChunkPk, NewChunk)],
) -> String {
    let names: Vec<&str> = batch.iter().map(|(_, c)| c.dataset.as_str()).collect();
    sqlx::query_scalar::<_, String>(
        r#"
        SELECT n.name FROM UNNEST($1::text[]) AS n(name)
        WHERE NOT EXISTS (SELECT 1 FROM datasets d WHERE d.name = n.name)
        LIMIT 1
        "#,
    )
    .bind(&names)
    .fetch_optional(&mut **tx)
    .await
    .ok()
    .flatten()
    .unwrap_or_else(|| "<unresolved>".to_string())
}
