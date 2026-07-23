//! Row structs and DB-row-to-domain conversions shared by the postgres submodules.

use std::collections::HashSet;
use std::ops::RangeInclusive;
use std::sync::Arc;

use anyhow::{Context, Result};
use libp2p_identity::PeerId;

use crate::scheduler_storage::{
    AssignmentWorker, ChunkPk, NewChunk, SchemaId, WorkerAssignmentChunk, WorkerPk,
};
use crate::types::{BlockNumber, WorkerStatus};

/// Chunk row for the published reads (worker/portal assignments), decoding into
/// [`WorkerAssignmentChunk`].
#[derive(sqlx::FromRow)]
pub(super) struct ChunkRow {
    pub(super) chunk_pk: ChunkPk,
    pub(super) dataset_name: String,
    pub(super) chunk_id: String,
    pub(super) size: i32,
    /// The chunk's schema pin (stamped at insert, never NULL).
    pub(super) schema_id: SchemaId,
    pub(super) tables_present: Option<bit_vec::BitVec>,
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

/// An active chunk paired with its current holders (ideal ∪ stale), fetched in one round-trip so the
/// cycle needn't query the placement separately. `worker_ids` is `None` for a chunk with no current
/// placement distinguishing "unplaced" from a placed-but-empty set.
#[derive(sqlx::FromRow)]
pub(super) struct ActiveChunkRow {
    #[sqlx(flatten)]
    pub(super) chunk: ChunkRow,
    ///
    pub(super) worker_ids: Option<Vec<WorkerPk>>,
    /// The committed ideal holders alone (pre-merge, no stale), for the eviction durability floor.
    /// `None` when the chunk has no committed ideal row yet (pending/holderless).
    pub(super) ideal_worker_ids: Option<Vec<WorkerPk>>,
    pub(super) is_portal_visible: bool,
    /// Has ever entered a worker assignment (`applied_at_worker_assignment_id IS NOT NULL`). With the
    /// query's `dropped_from_worker_assignment_at IS NULL` filter, this marks the chunks whose schema
    /// the bundle must carry — the whole routable window.
    pub(super) entered_worker_assignment: bool,
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

pub(super) fn chunk_from_row(row: ChunkRow, dataset: Arc<String>) -> WorkerAssignmentChunk {
    WorkerAssignmentChunk {
        dataset,
        id: Arc::new(row.chunk_id),
        size: row.size as u32,
        blocks: block_range_from_columns(row.first_block, row.last_block_delta),
        schema_id: row.schema_id,
        tables_present: row.tables_present,
    }
}

/// Per-decode dataset-name interner. A chunk batch repeats only a handful of dataset names, so this
/// hands out one shared `Arc<String>` per name instead of allocating one per row.
pub(super) struct DatasetInterner {
    seen: HashSet<Arc<String>>,
}

impl DatasetInterner {
    pub(super) fn new() -> Self {
        Self {
            seen: HashSet::new(),
        }
    }

    /// Takes `&String`, not `&str`, on purpose: probing a `HashSet<Arc<String>>` borrows through
    /// `Arc<String>: Borrow<String>`, so a hit allocates nothing.
    pub(super) fn intern(&mut self, name: &String) -> Arc<String> {
        if let Some(arc) = self.seen.get(name) {
            return arc.clone();
        }
        let arc = Arc::new(name.clone());
        self.seen.insert(arc.clone());
        arc
    }
}

/// Column arrays for the batched chunk inserts, zipped positionally by a multi-array `UNNEST` —
/// native types end-to-end (the bitmap binds as `varbit[]`). Bind order must match the SQL's
/// `UNNEST` column list.
pub(super) struct ChunkInsertArrays<'a> {
    pub(super) datasets: Vec<&'a str>,
    pub(super) chunk_ids: Vec<&'a str>,
    pub(super) sizes: Vec<i32>,
    pub(super) schema_ids: Vec<Option<SchemaId>>,
    pub(super) tables_present: Vec<Option<bit_vec::BitVec>>,
    pub(super) first_blocks: Vec<i64>,
    pub(super) last_block_deltas: Vec<i32>,
}

impl<'a> ChunkInsertArrays<'a> {
    pub(super) fn from_chunks(chunks: impl ExactSizeIterator<Item = &'a NewChunk>) -> Self {
        let n = chunks.len();
        let mut arrays = ChunkInsertArrays {
            datasets: Vec::with_capacity(n),
            chunk_ids: Vec::with_capacity(n),
            sizes: Vec::with_capacity(n),
            schema_ids: Vec::with_capacity(n),
            tables_present: Vec::with_capacity(n),
            first_blocks: Vec::with_capacity(n),
            last_block_deltas: Vec::with_capacity(n),
        };
        for chunk in chunks {
            let (first_block, last_block_delta) = block_range_columns(&chunk.blocks);
            arrays.datasets.push(chunk.dataset.as_str());
            arrays.chunk_ids.push(chunk.id.as_str());
            arrays.sizes.push(chunk.size as i32);
            arrays.schema_ids.push(chunk.schema_id);
            arrays.tables_present.push(chunk.tables_present.clone());
            arrays.first_blocks.push(first_block);
            arrays.last_block_deltas.push(last_block_delta);
        }
        arrays
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
