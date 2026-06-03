//! Runs the network scheduler over the simulation's chunks, applying version
//! restrictions and turning its panics into recoverable errors.

use std::any::Any;
use std::collections::HashSet;
use std::panic::AssertUnwindSafe;

use network_scheduler::{
    cli::DatasetsConfig,
    scheduling::{ScheduledChunk, SchedulingConfig, schedule},
    types::{Assignment, Chunk, ChunkWeight, Worker},
    weight::prepare_chunks,
};
use semver::Version;

use crate::ChunkId;

/// A chunk with its scheduling weight and config-derived minimum worker version,
/// as produced by [`prepare_chunks`].
type PreparedChunk = (Chunk, ChunkWeight, Option<Version>);

/// Merges existing and new chunks and runs the scheduler. Chunks in `restricted`
/// require `new_version`; others keep the config's minimum worker version.
///
/// The scheduler panics on infeasible version restrictions, so it runs inside
/// `catch_unwind`; the returned `Err(reason)` signals the caller to stop.
pub fn schedule_combined_chunks(
    existing_chunks: &[Chunk],
    new_chunks: &[Chunk],
    workers: &[Worker],
    datasets_config: &DatasetsConfig,
    scheduling_config: &SchedulingConfig,
    restricted: &HashSet<ChunkId>,
    new_version: &Version,
) -> (Vec<Chunk>, Result<Assignment, String>) {
    let prepared = prepare_combined_chunks(existing_chunks, new_chunks, datasets_config);

    let scheduled = to_scheduled_chunks(&prepared, restricted, new_version);
    let assignment = run_scheduler(&scheduled, workers, scheduling_config);
    drop(scheduled);

    let chunks = prepared.into_iter().map(|(chunk, _, _)| chunk).collect();
    (chunks, assignment)
}

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

/// Builds the scheduler input, overriding the minimum worker version to
/// `new_version` for restricted chunks.
fn to_scheduled_chunks<'a>(
    prepared: &'a [PreparedChunk],
    restricted: &HashSet<ChunkId>,
    new_version: &'a Version,
) -> Vec<ScheduledChunk<'a>> {
    prepared
        .iter()
        .map(|(chunk, weight, config_version)| {
            let key = (chunk.dataset.clone(), chunk.id.clone());
            let minimum_worker_version = if restricted.contains(&key) {
                Some(new_version)
            } else {
                config_version.as_ref()
            };
            ScheduledChunk {
                dataset: &chunk.dataset,
                chunk_id: &chunk.id,
                size: chunk.size,
                weight: *weight,
                minimum_worker_version,
            }
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
) -> Result<Assignment, String> {
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        schedule(scheduled, workers, config.clone())
    }));
    std::panic::set_hook(previous_hook);

    match result {
        Ok(Ok(assignment)) => Ok(assignment),
        Ok(Err(error)) => Err(format!("scheduling error: {error}")),
        Err(panic) => Err(panic_message(panic)),
    }
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
