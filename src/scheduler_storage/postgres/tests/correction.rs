//! Test-only [`PostgresStorage::register_correction`]: the trait method in the parent module
//! delegates to [`PostgresStorage::register_correction_impl`] here.
//!
//! A correction is a same-range 1-to-1 chunk swap. The schema enforces most invariants on insert;
//! the guards here cover what no constraint can.

use anyhow::Context;
use sqlx::{Connection, Postgres, Transaction};

use crate::scheduler_storage::{ChunkPk, DatasetId, StorageError, Tick};
use crate::types::Chunk;

use super::super::PostgresStorage;
use super::super::rows::{block_range_columns, is_unique_violation, tick_to_i64};

impl PostgresStorage {
    // `pub` is test-only: the whole `tests` module is `#[cfg(test)]`, so this never reaches production.
    pub fn register_correction_impl(
        &mut self,
        old_pk: ChunkPk,
        new_chunk: Chunk,
        now: Tick,
    ) -> Result<ChunkPk, StorageError> {
        self.with_conn(async move |conn| -> Result<ChunkPk, StorageError> {
            let mut tx = conn.begin().await.context("register_correction: begin")?;

            reject_if_old_chunk_unavailable(&mut tx, old_pk).await?;
            let (first_block, last_block_delta) =
                reject_if_range_changed(&mut tx, old_pk, &new_chunk).await?;
            let (new_pk, dataset_id) =
                insert_replacement(&mut tx, &new_chunk, first_block, last_block_delta).await?;
            link_correction(&mut tx, old_pk, new_pk, dataset_id, now).await?;

            tx.commit().await.context("register_correction: commit")?;
            Ok(new_pk)
        })
    }
}

/// Reject if the old chunk is on its way out — rejected, or being removed/dropped — so nothing
/// portal-visible is left for the replacement to supersede. A missing old chunk passes here and is
/// caught by the link insert's FK instead.
async fn reject_if_old_chunk_unavailable(
    tx: &mut Transaction<'_, Postgres>,
    old_pk: ChunkPk,
) -> Result<(), StorageError> {
    let unavailable: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM sched_chunk_metadata
            WHERE chunk_pk = $1
              AND (rejected
                   OR marked_for_removal IS NOT NULL
                   OR dropped_at_portal_assignment_id IS NOT NULL
                   OR dropped_at_worker_assignment_id IS NOT NULL)
        )
        "#,
    )
    .bind(old_pk)
    .fetch_one(&mut **tx)
    .await
    .context("register_correction: check old chunk availability")?;
    if unavailable {
        return Err(StorageError::CorrectionRejected {
            reason: format!("old chunk {old_pk} is not in a supersedable state"),
        });
    }
    Ok(())
}

/// Refuse a range-changing replacement before any insert — it would leave a dangling pending
/// correction or publish an overlap. Returns the replacement's range columns for the insert to reuse.
async fn reject_if_range_changed(
    tx: &mut Transaction<'_, Postgres>,
    old_pk: ChunkPk,
    new_chunk: &Chunk,
) -> Result<(i64, i32), StorageError> {
    let (first_block, last_block_delta) = block_range_columns(&new_chunk.blocks);
    let old_range: Option<(i64, i32)> =
        sqlx::query_as("SELECT first_block, last_block_delta FROM chunks WHERE chunk_pk = $1")
            .bind(old_pk)
            .fetch_optional(&mut **tx)
            .await
            .context("register_correction: fetch old range")?;
    if let Some((old_first, old_delta)) = old_range
        && (old_first, old_delta) != (first_block, last_block_delta)
    {
        return Err(StorageError::CorrectionRejected {
            reason: format!(
                "replacement range {first_block}..={} does not match old chunk {old_pk} range {old_first}..={}",
                first_block + i64::from(last_block_delta),
                old_first + i64::from(old_delta),
            ),
        });
    }
    Ok((first_block, last_block_delta))
}

/// Insert the replacement into its declared dataset and read back its `(chunk_pk, dataset_id)`. The
/// correction carries this dataset_id; it is never sourced from the old chunk.
///
/// No row back → the dataset name is unknown; a unique violation → the replacement id already
/// exists. Both map to `CorrectionRejected`.
async fn insert_replacement(
    tx: &mut Transaction<'_, Postgres>,
    new_chunk: &Chunk,
    first_block: i64,
    last_block_delta: i32,
) -> Result<(ChunkPk, DatasetId), StorageError> {
    let files: Vec<&str> = new_chunk.files.iter().map(|s| s.as_str()).collect();
    let inserted = sqlx::query_as::<_, (ChunkPk, DatasetId)>(
        r#"
        INSERT INTO chunks (dataset_id, chunk_id, size, files, first_block, last_block_delta)
        SELECT d.id, $2, $3, $4, $5, $6 FROM datasets d WHERE d.name = $1
        RETURNING chunk_pk, dataset_id
        "#,
    )
    .bind(new_chunk.dataset.as_str())
    .bind(new_chunk.id.as_str())
    .bind(new_chunk.size as i32)
    .bind(&files)
    .bind(first_block)
    .bind(last_block_delta)
    .fetch_optional(&mut **tx)
    .await;
    match inserted {
        Ok(Some(row)) => Ok(row),
        Ok(None) => Err(StorageError::CorrectionRejected {
            reason: format!("replacement chunk dataset {} is unknown", new_chunk.dataset),
        }),
        Err(e) if is_unique_violation(&e) => Err(StorageError::CorrectionRejected {
            reason: format!(
                "replacement chunk {}/{} already exists",
                new_chunk.dataset, new_chunk.id
            ),
        }),
        Err(e) => Err(anyhow::Error::new(e)
            .context("register_correction: insert replacement chunk")
            .into()),
    }
}

/// Link the replacement to the old chunk; the schema enforces the correction invariants on this
/// insert. A unique violation means a correction for `old_pk` already exists → `CorrectionRejected`;
/// other violations stay raw DB errors.
///
/// The replacement's metadata row is created later by `register_new_chunks`, not here.
async fn link_correction(
    tx: &mut Transaction<'_, Postgres>,
    old_pk: ChunkPk,
    new_pk: ChunkPk,
    dataset_id: DatasetId,
    now: u64,
) -> Result<(), StorageError> {
    let linked = sqlx::query(
        "INSERT INTO chunk_corrections (old_chunk_pk, new_chunk_pk, dataset_id, created_at) VALUES ($1, $2, $3, $4)",
    )
    .bind(old_pk)
    .bind(new_pk)
    .bind(dataset_id)
    .bind(tick_to_i64(now))
    .execute(&mut **tx)
    .await;
    match linked {
        Ok(_) => Ok(()),
        Err(e) if is_unique_violation(&e) => Err(StorageError::CorrectionRejected {
            reason: format!("a correction for old chunk {old_pk} already exists"),
        }),
        Err(e) => Err(anyhow::Error::new(e)
            .context("register_correction: insert")
            .into()),
    }
}
