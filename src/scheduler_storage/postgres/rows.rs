//! Row structs and DB-row-to-domain conversions shared by the postgres submodules.

use std::collections::HashSet;
use std::ops::RangeInclusive;
use std::sync::Arc;

use anyhow::{Context, Result};
use libp2p_identity::PeerId;

use crate::scheduler_storage::{
    AssignmentWorker, ChunkPk, SchemaId, WorkerAssignmentChunk, WorkerPk,
};
use crate::types::{BlockNumber, WorkerStatus};

// The ingest param helpers moved to `scheduler-metadata`; `tick_to_i64` is still used by the cycle
// writes here, so re-export it under the historical `super::rows::tick_to_i64` path.
pub(super) use scheduler_metadata::pg::rows::tick_to_i64;

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
/// placement, distinguishing "unplaced" from a placed-but-empty set.
#[derive(sqlx::FromRow)]
pub(super) struct ActiveChunkRow {
    #[sqlx(flatten)]
    pub(super) chunk: ChunkRow,
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

/// Recover the inclusive block range from the stored `(first_block, last_block_delta)` columns
/// (inverse of the ingest side's `block_range_columns`).
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
