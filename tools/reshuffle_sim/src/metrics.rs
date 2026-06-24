//! Reshuffle metrics and the assignment diffing that produces them.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::time::Duration;

use libp2p_identity::PeerId;

use crate::simulation::StepPlacement;
use crate::{ChunkId, ChunkOwners, ChunkSizeIndex};

/// Metrics for a single simulation step.
pub struct ReshuffleMetrics {
    pub step: u32,
    pub new_chunks_in_step: u32,
    /// How many of this step's new chunks are version-restricted.
    pub new_restricted_in_step: u32,
    pub total_chunks: usize,
    /// Cumulative count of version-restricted chunks.
    pub total_restricted_chunks: usize,
    pub replication_by_weight: BTreeMap<u16, u16>,
    pub data_movement: DataMovement,
    pub total_capacity_bytes: u64,
    pub used_capacity_bytes: u64,
    pub eligible_workers: usize,
    /// False when scheduling failed at this step; the run stops after.
    pub scheduled: bool,
    pub failure_reason: Option<String>,
    /// Data movement restricted to version-restricted chunks only.
    pub restricted_movement: DataMovement,
    /// Wall-clock time the scheduler spent on this step.
    pub schedule_duration: Duration,
}

/// Data movement between two assignments, classified by cause.
#[derive(Default)]
pub struct DataMovement {
    pub new_chunk_bytes: u64,
    pub shuffled_bytes: u64,
    pub shuffled_count: u32,
    pub increased_replication_bytes: u64,
    pub decreased_replication_bytes: u64,
    pub workers_receiving_new: usize,
    pub workers_losing: usize,
    pub workers_shuffled: usize,
}

impl DataMovement {
    pub fn zero() -> Self {
        DataMovement::default()
    }
}

/// Per-step counters shared by the success and failure metric builders.
pub struct StepStats {
    pub step: u32,
    pub new_chunks: u32,
    pub new_restricted: u32,
    pub eligible_workers: usize,
}

/// Computes a step's metrics by diffing the step's placement against the
/// previous one. Returns the current chunk owners for use as the next step's
/// previous.
pub fn measure_reshuffle(
    previous_owners: &ChunkOwners,
    placement: StepPlacement,
    restricted: &HashSet<ChunkId>,
    stats: StepStats,
    total_capacity_bytes: u64,
) -> (ReshuffleMetrics, ChunkOwners) {
    let StepPlacement {
        owners: current_owners,
        chunk_sizes,
        replication_by_weight,
        used_capacity_bytes,
        total_chunks,
        schedule_duration,
    } = placement;

    let data_movement = compute_data_movement(previous_owners, &current_owners, &chunk_sizes);
    let restricted_movement = compute_data_movement(
        &filter_owners(previous_owners, restricted),
        &filter_owners(&current_owners, restricted),
        &chunk_sizes,
    );

    let metrics = ReshuffleMetrics {
        step: stats.step,
        new_chunks_in_step: stats.new_chunks,
        new_restricted_in_step: stats.new_restricted,
        total_chunks,
        total_restricted_chunks: restricted.len(),
        replication_by_weight,
        data_movement,
        total_capacity_bytes,
        used_capacity_bytes,
        eligible_workers: stats.eligible_workers,
        scheduled: true,
        failure_reason: None,
        restricted_movement,
        schedule_duration,
    };

    (metrics, current_owners)
}

/// Metrics for the step at which scheduling failed: zero movement, and the run
/// stops afterwards.
pub fn failed_step_metrics(
    stats: StepStats,
    total_chunks: usize,
    total_restricted_chunks: usize,
    total_capacity_bytes: u64,
    reason: String,
) -> ReshuffleMetrics {
    ReshuffleMetrics {
        step: stats.step,
        new_chunks_in_step: stats.new_chunks,
        new_restricted_in_step: stats.new_restricted,
        total_chunks,
        total_restricted_chunks,
        replication_by_weight: BTreeMap::new(),
        data_movement: DataMovement::zero(),
        total_capacity_bytes,
        used_capacity_bytes: 0,
        eligible_workers: stats.eligible_workers,
        scheduled: false,
        failure_reason: Some(reason),
        restricted_movement: DataMovement::zero(),
        schedule_duration: Duration::ZERO,
    }
}

/// Keeps only the owner entries whose chunk id is in `restricted`.
fn filter_owners(owners: &ChunkOwners, restricted: &HashSet<ChunkId>) -> ChunkOwners {
    owners
        .iter()
        .filter(|(id, _)| restricted.contains(*id))
        .map(|(id, workers)| (id.clone(), workers.clone()))
        .collect()
}

/// Classifies data movement between two assignments by cause:
/// - a chunk only in `current` is new — all its replicas are downloads;
/// - a chunk in both may have gained/lost replicas — gains are split into 1:1
///   swaps (shuffle) and net replication increase;
/// - a chunk only in `previous` was removed — its replicas are freed.
fn compute_data_movement(
    previous: &ChunkOwners,
    current: &ChunkOwners,
    chunk_sizes: &ChunkSizeIndex,
) -> DataMovement {
    let mut movement = Movement::default();

    for (chunk_id, current_workers) in current {
        let size = chunk_size(chunk_sizes, chunk_id);
        match previous.get(chunk_id) {
            Some(previous_workers) => {
                movement.record_existing(size, previous_workers, current_workers)
            }
            None => movement.record_added(size, current_workers),
        }
    }
    for (chunk_id, previous_workers) in previous {
        if !current.contains_key(chunk_id) {
            movement.record_removed(chunk_size(chunk_sizes, chunk_id), previous_workers);
        }
    }

    movement.finish()
}

fn chunk_size(chunk_sizes: &ChunkSizeIndex, chunk_id: &ChunkId) -> u64 {
    *chunk_sizes.get(chunk_id).unwrap_or(&0) as u64
}

/// Accumulates data movement while iterating chunks, tracking distinct affected
/// workers as sets before collapsing to counts in [`Movement::finish`].
#[derive(Default)]
struct Movement {
    new_chunk_bytes: u64,
    shuffled_bytes: u64,
    shuffled_count: u32,
    increased_replication_bytes: u64,
    decreased_replication_bytes: u64,
    workers_receiving_new: BTreeSet<PeerId>,
    workers_losing: BTreeSet<PeerId>,
    workers_shuffled: BTreeSet<PeerId>,
}

impl Movement {
    fn record_existing(
        &mut self,
        size: u64,
        previous: &BTreeSet<PeerId>,
        current: &BTreeSet<PeerId>,
    ) {
        let gained = current.difference(previous).count();
        let lost = previous.difference(current).count();
        let shuffled = gained.min(lost);

        if shuffled > 0 {
            self.shuffled_count += 1;
            self.shuffled_bytes += size * shuffled as u64;
        }
        self.increased_replication_bytes += size * gained.saturating_sub(lost) as u64;
        self.decreased_replication_bytes += size * lost.saturating_sub(gained) as u64;

        self.workers_shuffled
            .extend(current.difference(previous).copied());
        self.workers_losing
            .extend(previous.difference(current).copied());
    }

    fn record_added(&mut self, size: u64, current: &BTreeSet<PeerId>) {
        self.new_chunk_bytes += size * current.len() as u64;
        self.workers_receiving_new.extend(current.iter().copied());
    }

    fn record_removed(&mut self, size: u64, previous: &BTreeSet<PeerId>) {
        self.decreased_replication_bytes += size * previous.len() as u64;
        self.workers_losing.extend(previous.iter().copied());
    }

    fn finish(self) -> DataMovement {
        DataMovement {
            new_chunk_bytes: self.new_chunk_bytes,
            shuffled_bytes: self.shuffled_bytes,
            shuffled_count: self.shuffled_count,
            increased_replication_bytes: self.increased_replication_bytes,
            decreased_replication_bytes: self.decreased_replication_bytes,
            workers_receiving_new: self.workers_receiving_new.len(),
            workers_losing: self.workers_losing.len(),
            workers_shuffled: self.workers_shuffled.len(),
        }
    }
}
