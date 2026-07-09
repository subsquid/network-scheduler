//! Stateless scheduler path: each step re-runs the production single-step scheduler
//! ([`network_scheduler::scheduling`]) on the baseline chunks plus every chunk added so far, so the
//! placement is rebuilt from scratch. Driven through [`StepScheduler`] by the shared loop in
//! [`crate::simulation`]. Honors the simulation's version restrictions and worker upgrades.

use std::any::Any;
use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::time::{Duration, Instant};

use libp2p_identity::PeerId;
use network_scheduler::{
    cli::DatasetsConfig,
    scheduling::{ScheduledChunk, SchedulingConfig, schedule},
    types::{Assignment, Chunk, ChunkWeight, Worker},
    weight::prepare_chunks,
};
use semver::Version;

use crate::{
    ChunkOwners, WorkerIndex,
    simulation::{StepContext, StepOutcome, StepPlacement, StepScheduler},
};

pub struct StatelessScheduler {
    /// The input-file placement, used as step 1's reference.
    initial_owners: ChunkOwners,
    /// Maps each step's placement `PeerId`s to the same compact indices used in
    /// `initial_owners`, so owners stay comparable across steps.
    worker_index: WorkerIndex,
}

impl StatelessScheduler {
    pub fn new(initial_owners: ChunkOwners, worker_peer_ids: Vec<PeerId>) -> Self {
        let mut worker_index = WorkerIndex::default();
        for peer in worker_peer_ids {
            worker_index.intern(peer);
        }
        Self {
            initial_owners,
            worker_index,
        }
    }
}

impl StepScheduler for StatelessScheduler {
    fn take_initial_owners(&mut self) -> ChunkOwners {
        std::mem::take(&mut self.initial_owners)
    }

    fn step(&mut self, ctx: StepContext) -> anyhow::Result<StepOutcome> {
        let (chunks, result, schedule_duration) = schedule_combined_chunks(
            ctx.baseline_chunks,
            ctx.accumulated_new_chunks,
            ctx.workers,
            ctx.datasets_config,
            ctx.scheduling_config,
        );

        let assignment = match result {
            Ok(assignment) => assignment,
            Err(reason) => return Ok(StepOutcome::Failed(reason)),
        };

        let used_capacity_bytes: u64 = assignment
            .worker_chunks
            .values()
            .flatten()
            .map(|&i| chunks[i as usize].size as u64)
            .sum();

        Ok(StepOutcome::Scheduled(StepPlacement {
            owners: chunk_owners_from_assignment(&chunks, &assignment, &mut self.worker_index),
            chunk_sizes: chunks
                .iter()
                .map(|c| ((c.dataset.clone(), c.id.clone()), c.size))
                .collect(),
            // The stateless path has no MVCC drain lifecycle; every step rebuilds from scratch.
            drained: BTreeMap::new(),
            replication_by_weight: assignment.replication_by_weight,
            used_capacity_bytes,
            stale_capacity_bytes: 0,
            total_chunks: chunks.len(),
            schedule_duration,
        }))
    }
}

/// Merges existing and new chunks and runs the scheduler. Each chunk's minimum
/// worker version is whatever its dataset segment in `datasets_config` requires.
///
/// The scheduler panics on infeasible version restrictions, so it runs inside
/// `catch_unwind`; the returned `Err(reason)` signals the caller to stop.
pub fn schedule_combined_chunks(
    existing_chunks: &[Chunk],
    new_chunks: &[Chunk],
    workers: &[Worker],
    datasets_config: &DatasetsConfig,
    scheduling_config: &SchedulingConfig,
) -> (Vec<Chunk>, Result<Assignment, String>, Duration) {
    let prepared = prepare_combined_chunks(existing_chunks, new_chunks, datasets_config);

    let scheduled = to_scheduled_chunks(&prepared);
    let (assignment, schedule_duration) = run_scheduler(&scheduled, workers, scheduling_config);
    drop(scheduled);

    let chunks = prepared.into_iter().map(|(chunk, _, _)| chunk).collect();
    (chunks, assignment, schedule_duration)
}

/// A chunk with its scheduling weight and config-derived minimum worker version,
/// as produced by [`prepare_chunks`].
type PreparedChunk = (Chunk, ChunkWeight, Option<Version>);

/// Merges existing and new chunks, sorts them by dataset then block, and assigns
/// weights/versions via the config.
fn prepare_combined_chunks(
    existing: &[Chunk],
    new: &[Chunk],
    config: &DatasetsConfig,
) -> Vec<PreparedChunk> {
    let mut combined: Vec<Chunk> = existing.to_vec();
    combined.extend_from_slice(new);
    combined.sort_by(|a, b| {
        a.dataset
            .cmp(&b.dataset)
            .then(a.blocks.start().cmp(b.blocks.start()))
    });
    prepare_chunks(combined, config)
}

/// Builds the scheduler input, taking each chunk's minimum worker version from
/// the config-derived version assigned by [`prepare_chunks`].
fn to_scheduled_chunks<'a>(prepared: &'a [PreparedChunk]) -> Vec<ScheduledChunk<'a>> {
    prepared
        .iter()
        .map(|(chunk, weight, config_version)| ScheduledChunk {
            dataset: &chunk.dataset,
            chunk_id: &chunk.id,
            size: chunk.size,
            weight: *weight,
            minimum_worker_version: config_version.as_ref(),
            hashes: Vec::new(),
        })
        .collect()
}

/// Runs the scheduler under `catch_unwind`, returning any panic or error as a
/// message. The default panic hook is silenced so an expected version-restriction
/// panic isn't also dumped to stderr.
fn run_scheduler(
    scheduled: &[ScheduledChunk],
    workers: &[Worker],
    config: &SchedulingConfig,
) -> (Result<Assignment, String>, Duration) {
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let started = Instant::now();
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        schedule(scheduled, workers, config.clone())
    }));
    let schedule_duration = started.elapsed();
    std::panic::set_hook(previous_hook);

    let assignment = match result {
        Ok(Ok(assignment)) => Ok(assignment),
        Ok(Err(error)) => Err(format!("scheduling error: {error}")),
        Err(panic) => Err(panic_message(panic)),
    };
    (assignment, schedule_duration)
}

fn chunk_owners_from_assignment(
    chunks: &[Chunk],
    assignment: &Assignment,
    worker_index: &mut WorkerIndex,
) -> ChunkOwners {
    let mut owners: ChunkOwners = BTreeMap::new();
    for (worker, indexes) in &assignment.worker_chunks {
        let idx = worker_index.intern(*worker);
        for &chunk_index in indexes {
            let chunk = &chunks[chunk_index as usize];
            owners
                .entry((chunk.dataset.clone(), chunk.id.clone()))
                .or_default()
                .push(idx);
        }
    }
    // Holders are pushed in worker-iteration order; sort so `ChunkOwners`' merge-diff
    // invariant holds. Each worker owns a chunk at most once, so no dedup is needed.
    for holders in owners.values_mut() {
        holders.sort_unstable();
    }
    owners
}

/// Extracts a human-readable message from a caught panic payload.
fn panic_message(panic: Box<dyn Any + Send>) -> String {
    if let Some(s) = panic.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "scheduler panicked".to_string()
    }
}
