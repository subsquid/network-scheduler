//! Helpers for [`PostgresStorage::insert_new_chunks`] and
//! [`PostgresStorage::register_new_chunks`]: bring new chunk rows in, then admit
//! or reject them against the non-overlap rule.

use anyhow::{Context, Result};
use sqlx::postgres::{PgConnection, PgRow};
use sqlx::{Connection, Row};

use crate::metrics::PhaseTimer;
use crate::scheduler_storage::{ChunkPk, StorageError};
use crate::types::Chunk;

use super::nonoverlap::Candidate;
use super::rows::{block_range_columns, is_unique_violation};

/// Insert raw chunk rows, batched in one transaction. Each chunk has its own list of files, which
/// the flat `UNNEST` the cycle uses can't carry, so the payload is jsonb.
pub(super) async fn insert_chunks(
    conn: &mut PgConnection,
    chunks: &[Chunk],
    batch_size: usize,
) -> Result<(), StorageError> {
    let mut timer = PhaseTimer::new("insert_new_chunks");
    if chunks.is_empty() {
        return Ok(());
    }
    let mut tx = conn.begin().await.context("insert_new_chunks: begin")?;
    for batch in chunks.chunks(batch_size) {
        let payload = insert_payload(batch)?;
        let inserted = sqlx::query(
            r#"
            INSERT INTO chunks (dataset_id, chunk_id, size, files, first_block, last_block_delta)
            SELECT d.id, x.chunk_id, x.size, x.files, x.first_block, x.last_block_delta
            FROM jsonb_to_recordset($1::jsonb)
                 AS x(dataset text, chunk_id text, size int, files text[],
                       first_block bigint, last_block_delta int)
            JOIN datasets d ON d.name = x.dataset
            RETURNING 1
            "#,
        )
        .bind(&payload)
        .fetch_all(&mut *tx)
        .await;
        match inserted {
            Ok(rows) => {
                timer.stmt(rows.len() as u64);
                if rows.len() < batch.len() {
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

fn insert_payload(batch: &[Chunk]) -> Result<String, StorageError> {
    let rows_json: Vec<ChunkInsertJson> = batch
        .iter()
        .map(|chunk| {
            let (first_block, last_block_delta) = block_range_columns(&chunk.blocks);
            ChunkInsertJson {
                dataset: chunk.dataset.as_str(),
                chunk_id: chunk.id.as_str(),
                size: chunk.size as i32,
                files: chunk.files.iter().map(|s| s.as_str()).collect(),
                first_block,
                last_block_delta,
            }
        })
        .collect();
    Ok(serde_json::to_string(&rows_json).context("insert_new_chunks: serialize")?)
}

/// `insert_chunks` payload row; field names must match the `jsonb_to_recordset` columns.
#[derive(serde::Serialize)]
struct ChunkInsertJson<'a> {
    dataset: &'a str,
    chunk_id: &'a str,
    size: i32,
    files: Vec<&'a str>,
    first_block: i64,
    last_block_delta: i32,
}

/// New chunks (no metadata row yet) with their range and whether they're a pending correction's
/// replacement. A replacement overlaps the old chunk it supersedes by design, and the correction
/// path owns that swap (corrections are 1:1 same-range — see the design doc).
pub(super) async fn fetch_candidates(conn: &mut PgConnection) -> Result<Vec<PgRow>> {
    let mut timer = PhaseTimer::new("register_new_chunks:fetch_candidates");
    let rows = sqlx::query(
        r#"
        SELECT c.chunk_pk, c.dataset_id, d.name AS dataset_name,
               c.first_block, c.last_block_delta,
               (cc.new_chunk_pk IS NOT NULL) AS is_replacement,
               oc.first_block AS pred_first_block,
               oc.last_block_delta AS pred_last_block_delta
        FROM chunks c
        JOIN datasets d ON d.id = c.dataset_id
        LEFT JOIN chunk_corrections cc
          ON cc.new_chunk_pk = c.chunk_pk AND cc.applied_at_portal_assignment_id IS NULL
        LEFT JOIN chunks oc ON oc.chunk_pk = cc.old_chunk_pk
        WHERE NOT EXISTS (
            SELECT 1 FROM sched_chunk_metadata s WHERE s.chunk_pk = c.chunk_pk
        )
        "#,
    )
    .fetch_all(&mut *conn)
    .await
    .context("register_new_chunks: candidates")?;
    timer.stmt(rows.len() as u64);
    Ok(rows)
}

/// Candidates split by how they enter registration.
pub(super) struct Classified {
    /// Same-range correction replacements — admitted without an overlap check.
    pub(super) exempt: Vec<ChunkPk>,
    /// Ordinary new chunks, still to be overlap-checked.
    pub(super) candidates: Vec<Candidate>,
    /// Range-changing replacements — rejected (backstop; `register_correction` already refuses them).
    pub(super) range_rejected: Vec<Candidate>,
}

pub(super) fn classify(rows: &[PgRow]) -> Classified {
    let mut out = Classified {
        exempt: Vec::new(),
        candidates: Vec::new(),
        range_rejected: Vec::new(),
    };
    for row in rows {
        if !row.get::<bool, _>("is_replacement") {
            out.candidates.push(Candidate::from_row(row));
            continue;
        }
        let first: i64 = row.get("first_block");
        let last_delta: i32 = row.get("last_block_delta");
        let pred = (
            row.get::<Option<i64>, _>("pred_first_block"),
            row.get::<Option<i32>, _>("pred_last_block_delta"),
        );
        if pred == (Some(first), Some(last_delta)) {
            out.exempt.push(row.get::<ChunkPk, _>("chunk_pk"));
        } else {
            out.range_rejected.push(Candidate::from_row(row));
        }
    }
    out
}

/// Log range-changing replacements distinctly rather than counting them as overlap rejections. This
/// should never fire — `register_correction` already rejects them.
pub(super) fn warn_range_rejected(range_rejected: &[Candidate]) {
    if !range_rejected.is_empty() {
        tracing::warn!(
            count = range_rejected.len(),
            "correction replacements rejected at registration: range differs from \
             predecessor (register_correction should have refused these)"
        );
    }
}

/// Give admitted chunks (overlap winners + exempt replacements) a default metadata row.
pub(super) async fn persist_admitted(conn: &mut PgConnection, admitted: &[ChunkPk]) -> Result<()> {
    if admitted.is_empty() {
        return Ok(());
    }
    let mut timer = PhaseTimer::new("register_new_chunks:admit");
    sqlx::query("INSERT INTO sched_chunk_metadata (chunk_pk) SELECT * FROM UNNEST($1::bigint[])")
        .bind(admitted)
        .execute(&mut *conn)
        .await
        .context("register_new_chunks: admit")?;
    timer.stmt(admitted.len() as u64);
    Ok(())
}

/// Mark rejected chunks terminal (`rejected = TRUE`): never scheduled or re-scanned.
pub(super) async fn persist_rejected(conn: &mut PgConnection, rejected: &[ChunkPk]) -> Result<()> {
    if rejected.is_empty() {
        return Ok(());
    }
    let mut timer = PhaseTimer::new("register_new_chunks:reject");
    sqlx::query(
        "INSERT INTO sched_chunk_metadata (chunk_pk, rejected) SELECT *, TRUE FROM UNNEST($1::bigint[])",
    )
    .bind(rejected)
    .execute(&mut *conn)
    .await
    .context("register_new_chunks: reject")?;
    timer.stmt(rejected.len() as u64);
    Ok(())
}
