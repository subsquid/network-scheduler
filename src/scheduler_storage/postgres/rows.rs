//! Row structs and DB-row-to-domain conversions shared by the postgres submodules.

use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::sync::Arc;

use anyhow::{Context, Result};
use libp2p_identity::PeerId;

use crate::scheduler_storage::{AssignmentWorker, ChunkPk, WorkerPk};
use crate::types::{BlockNumber, Chunk, WorkerStatus};

#[derive(sqlx::FromRow)]
pub(super) struct ChunkRow {
    pub(super) chunk_pk: ChunkPk,
    pub(super) dataset_name: String,
    pub(super) chunk_id: String,
    pub(super) size: i32,
    pub(super) files: Vec<String>,
    pub(super) first_block: i64,
    /// last_block - first_block; add to `first_block` to recover the inclusive end.
    pub(super) last_block_delta: i32,
}

#[derive(sqlx::FromRow)]
pub(super) struct WorkerRow {
    pub(super) id: WorkerPk,
    pub(super) peer_id: String,
    pub(super) version: Option<String>,
    pub(super) inactive_since: Option<i64>,
}

#[derive(sqlx::FromRow)]
pub(super) struct IdealChunkWorkersRow {
    pub(super) chunk_pk: ChunkPk,
    pub(super) worker_ids: Vec<WorkerPk>,
}

#[derive(sqlx::FromRow)]
pub(super) struct StaleWorkerRow {
    pub(super) chunk_pk: ChunkPk,
    pub(super) worker_id: WorkerPk,
}

/// Ticks are non-negative logical timestamps, so they always fit in `i64`.
pub(super) fn tick_to_i64(tick: u64) -> i64 {
    i64::try_from(tick).expect("tick value overflows i64")
}

/// Split a chunk's inclusive block range into the stored `(first_block, last_block_delta)`
/// columns. `last_block_delta = last - first` fits in `i32` for any real chunk span.
pub(super) fn block_range_columns(blocks: &RangeInclusive<BlockNumber>) -> (i64, i32) {
    (
        *blocks.start() as i64,
        (blocks.end() - blocks.start()) as i32,
    )
}

/// Inverse of [`block_range_columns`]: recover the inclusive block range from the stored columns.
pub(super) fn block_range_from_columns(
    first_block: i64,
    last_block_delta: i32,
) -> RangeInclusive<BlockNumber> {
    let first = first_block as BlockNumber;
    first..=first + last_block_delta as BlockNumber
}

pub(super) fn chunk_from_row(row: &ChunkRow, dataset: Arc<String>) -> Result<Chunk> {
    let mut files = row.files.clone();
    files.sort_unstable();
    Ok(Chunk {
        dataset,
        id: Arc::new(row.chunk_id.clone()),
        size: row.size as u32,
        blocks: block_range_from_columns(row.first_block, row.last_block_delta),
        files: crate::pool::intern(files),
        summary: None,
    })
}

/// Per-decode dataset-name interner. A chunk batch repeats only a handful of dataset names, so this
/// hands out one shared `Arc<String>` per name instead of allocating one per row.
pub(super) struct DatasetInterner<'a> {
    seen: HashMap<&'a str, Arc<String>>,
}

impl<'a> DatasetInterner<'a> {
    pub(super) fn new() -> Self {
        Self {
            seen: HashMap::new(),
        }
    }

    pub(super) fn intern(&mut self, name: &'a str) -> Arc<String> {
        self.seen
            .entry(name)
            .or_insert_with(|| Arc::new(name.to_owned()))
            .clone()
    }
}

pub(super) fn is_unique_violation(err: &sqlx::Error) -> bool {
    err.as_database_error()
        .is_some_and(|db| db.kind() == sqlx::error::ErrorKind::UniqueViolation)
}

pub(super) fn worker_status_from_row(row: &WorkerRow) -> WorkerStatus {
    if row.inactive_since.is_some() {
        WorkerStatus::Stale
    } else {
        WorkerStatus::Online
    }
}

pub(super) fn assignment_worker_from_row(row: &WorkerRow) -> Result<(WorkerPk, AssignmentWorker)> {
    let peer_id: PeerId = row
        .peer_id
        .parse()
        .with_context(|| format!("invalid peer_id in sched_workers: {}", row.peer_id))?;
    let status = worker_status_from_row(row);
    Ok((row.id, AssignmentWorker { peer_id, status }))
}
