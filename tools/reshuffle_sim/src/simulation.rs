use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    sync::Arc,
};

use libp2p_identity::PeerId;
use network_scheduler::{
    scheduling::{ScheduledChunk, SchedulingConfig, schedule},
    types::{Chunk, Worker},
    weight::prepare_chunks,
};
use rand::prelude::*;
use rand::seq::SliceRandom;
use semver::Version;

use crate::{ChunkOwners, ChunkSizeIndex, baseline::DatasetInfo};

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
    // --- version-restriction additions ---
    pub eligible_workers: usize,
    /// False when scheduling failed (panicked) at this step; the run stops after.
    pub scheduled: bool,
    pub failure_reason: Option<String>,
    pub restricted_movement: DataMovement,
}

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
        DataMovement {
            new_chunk_bytes: 0,
            shuffled_bytes: 0,
            shuffled_count: 0,
            increased_replication_bytes: 0,
            decreased_replication_bytes: 0,
            workers_receiving_new: 0,
            workers_losing: 0,
            workers_shuffled: 0,
        }
    }
}

// --- Chunk generation ---

fn build_cumulative_distribution(datasets: &[DatasetInfo]) -> Vec<(usize, f64)> {
    let total: f64 = datasets.iter().map(|d| d.chunk_count as f64).sum();
    let mut cumulative = Vec::with_capacity(datasets.len());
    let mut acc = 0.0;
    for (i, ds) in datasets.iter().enumerate() {
        acc += ds.chunk_count as f64 / total;
        cumulative.push((i, acc));
    }
    cumulative
}

fn sample_dataset_index(cumulative: &[(usize, f64)], rng: &mut impl Rng) -> usize {
    let r: f64 = rng.random();
    cumulative
        .iter()
        .find(|(_, threshold)| r <= *threshold)
        .map(|(i, _)| *i)
        .unwrap_or(cumulative.len() - 1)
}

/// Creates synthetic chunks at the head (latest blocks) of each dataset.
/// Datasets are sampled proportionally to their existing chunk counts —
/// a dataset with 60% of total chunks receives ~60% of new chunks.
/// Each generated chunk extends the dataset's block range using its average block span and size.
///
/// Additionally tags `round(restricted_fraction * count)` of the generated
/// chunks (selected uniformly at random) as "restricted", returning their ids
/// alongside the chunks. Restricted chunks are later forced to require the new
/// worker version.
///
/// `chunk_size` overrides the size of every generated chunk; when `None` each
/// chunk uses its dataset's average chunk size.
pub fn generate_new_chunks(
    datasets: &mut Vec<DatasetInfo>,
    chunks_to_generate: u32,
    restricted_fraction: f64,
    chunk_size: Option<u32>,
    rng: &mut impl Rng,
) -> (Vec<Chunk>, Vec<crate::ChunkId>) {
    let cumulative_distribution = build_cumulative_distribution(datasets);
    let mut new_chunks = Vec::with_capacity(chunks_to_generate as usize);

    for _ in 0..chunks_to_generate {
        let dataset_index = sample_dataset_index(&cumulative_distribution, rng);
        let dataset = &mut datasets[dataset_index];

        let first_block = dataset.last_block + 1;
        let last_block = first_block + dataset.avg_block_span - 1;
        let chunk_id = format!(
            "{:010}/{:010}-{:010}-{:08x}",
            first_block, first_block, last_block, first_block as u32
        );

        new_chunks.push(Chunk {
            dataset: dataset.dataset_id.clone(),
            id: Arc::new(chunk_id),
            size: chunk_size.unwrap_or(dataset.avg_chunk_size),
            blocks: first_block..=last_block,
            files: Arc::new(vec![]),
            summary: None,
        });

        dataset.last_block = last_block;
        dataset.chunk_count += 1;
    }

    let restricted_count =
        ((restricted_fraction * new_chunks.len() as f64).round() as usize).min(new_chunks.len());
    let mut indices: Vec<usize> = (0..new_chunks.len()).collect();
    indices.shuffle(rng);
    let restricted: Vec<crate::ChunkId> = indices[..restricted_count]
        .iter()
        .map(|&i| (new_chunks[i].dataset.clone(), new_chunks[i].id.clone()))
        .collect();

    (new_chunks, restricted)
}

// --- Scheduling ---

/// Merges existing and new chunks and runs the scheduler. Chunks in `restricted`
/// require `new_version`; others keep the config's minimum_worker_version.
///
/// The scheduler panics on infeasible version restrictions, so it's wrapped in
/// `catch_unwind`; the inner `Err(reason)` signals the caller to stop.
pub fn schedule_combined_chunks(
    existing_chunks: &[Chunk],
    new_chunks: &[Chunk],
    workers: &[Worker],
    datasets_config: &network_scheduler::cli::DatasetsConfig,
    scheduling_config: &SchedulingConfig,
    restricted: &HashSet<crate::ChunkId>,
    new_version: &Version,
) -> (
    Vec<Chunk>,
    Result<network_scheduler::types::Assignment, String>,
) {
    let mut combined: Vec<Chunk> = existing_chunks.to_vec();
    combined.extend_from_slice(new_chunks);
    combined.sort_by(|a, b| {
        a.dataset
            .cmp(&b.dataset)
            .then(a.blocks.start().cmp(b.blocks.start()))
    });

    let prepared = prepare_chunks(combined, datasets_config);

    let scheduled_chunks: Vec<ScheduledChunk> = prepared
        .iter()
        .map(|(chunk, weight, mwv)| {
            let key = (chunk.dataset.clone(), chunk.id.clone());
            let minimum_worker_version = if restricted.contains(&key) {
                Some(new_version)
            } else {
                mwv.as_ref()
            };
            ScheduledChunk {
                dataset: &chunk.dataset,
                chunk_id: &chunk.id,
                size: chunk.size,
                weight: *weight,
                minimum_worker_version,
            }
        })
        .collect();

    // Silence the default panic hook so the captured panic isn't also dumped to stderr.
    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        schedule(&scheduled_chunks, workers, scheduling_config.clone())
    }));
    std::panic::set_hook(prev_hook);
    let assignment = match result {
        Ok(Ok(assignment)) => Ok(assignment),
        Ok(Err(err)) => Err(format!("scheduling error: {err}")),
        Err(panic) => Err(panic_message(panic)),
    };
    drop(scheduled_chunks);

    let filtered_chunks: Vec<Chunk> = prepared.into_iter().map(|(c, _, _)| c).collect();

    (filtered_chunks, assignment)
}

/// Extracts a human-readable message from a caught panic payload.
fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = panic.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "scheduler panicked".to_string()
    }
}

// --- Assignment diffing ---

fn chunk_owners_from_assignment(
    chunks: &[Chunk],
    assignment: &network_scheduler::types::Assignment,
) -> ChunkOwners {
    let mut owners: ChunkOwners = BTreeMap::new();
    for (worker, indexes) in &assignment.worker_chunks {
        for &chunk_index in indexes {
            let chunk = &chunks[chunk_index as usize];
            owners
                .entry((chunk.dataset.clone(), chunk.id.clone()))
                .or_default()
                .insert(*worker);
        }
    }
    owners
}

/// Classifies data movement between two assignments by cause.
/// For each chunk in the current assignment:
/// - New chunk (not in previous): all replicas count as new_chunk_bytes.
/// - Existing chunk: workers gained/lost are classified as shuffle (1:1 swap)
///   or replication change (net gain/loss of replicas).
fn compute_data_movement(
    previous: &ChunkOwners,
    current: &ChunkOwners,
    chunk_sizes: &ChunkSizeIndex,
) -> DataMovement {
    let mut result = DataMovement {
        new_chunk_bytes: 0,
        shuffled_bytes: 0,
        shuffled_count: 0,
        increased_replication_bytes: 0,
        decreased_replication_bytes: 0,
        workers_receiving_new: 0,
        workers_losing: 0,
        workers_shuffled: 0,
    };

    let mut workers_receiving_new: BTreeSet<PeerId> = BTreeSet::new();
    let mut workers_losing: BTreeSet<PeerId> = BTreeSet::new();
    let mut workers_shuffled: BTreeSet<PeerId> = BTreeSet::new();

    for (chunk_id, current_workers) in current {
        let size = *chunk_sizes.get(chunk_id).unwrap_or(&0) as u64;

        if let Some(previous_workers) = previous.get(chunk_id) {
            let gained: usize = current_workers.difference(previous_workers).count();
            let lost: usize = previous_workers.difference(current_workers).count();

            let shuffled = gained.min(lost);
            let replication_increase = gained.saturating_sub(lost);
            let replication_decrease = lost.saturating_sub(gained);

            if shuffled > 0 {
                result.shuffled_count += 1;
                result.shuffled_bytes += size * shuffled as u64;
            }
            result.increased_replication_bytes += size * replication_increase as u64;
            result.decreased_replication_bytes += size * replication_decrease as u64;

            if gained > 0 {
                for w in current_workers.difference(previous_workers) {
                    workers_shuffled.insert(*w);
                }
            }
            for w in previous_workers.difference(current_workers) {
                workers_losing.insert(*w);
            }
        } else {
            result.new_chunk_bytes += size * current_workers.len() as u64;
            for w in current_workers {
                workers_receiving_new.insert(*w);
            }
        }
    }

    for (chunk_id, previous_workers) in previous {
        if !current.contains_key(chunk_id) {
            let size = *chunk_sizes.get(chunk_id).unwrap_or(&0) as u64;
            result.decreased_replication_bytes += size * previous_workers.len() as u64;
            for w in previous_workers {
                workers_losing.insert(*w);
            }
        }
    }

    result.workers_receiving_new = workers_receiving_new.len();
    result.workers_losing = workers_losing.len();
    result.workers_shuffled = workers_shuffled.len();

    result
}

/// Keeps only the owner entries whose chunk id is in `restricted`.
fn filter_owners(owners: &ChunkOwners, restricted: &HashSet<crate::ChunkId>) -> ChunkOwners {
    owners
        .iter()
        .filter(|(id, _)| restricted.contains(*id))
        .map(|(id, set)| (id.clone(), set.clone()))
        .collect()
}

/// Computes all reshuffle metrics for a single simulation step.
/// All comparisons are against the previous step (incremental).
/// Returns the current chunk owners for use as the next step's previous.
pub fn measure_reshuffle(
    previous_chunk_owners: &ChunkOwners,
    chunks: &[Chunk],
    assignment: &network_scheduler::types::Assignment,
    step: u32,
    new_chunks_in_step: u32,
    new_restricted_in_step: u32,
    num_workers: usize,
    worker_capacity: u64,
    restricted: &HashSet<crate::ChunkId>,
    eligible_workers: usize,
) -> (ReshuffleMetrics, ChunkOwners) {
    let current_chunk_owners = chunk_owners_from_assignment(chunks, assignment);

    let chunk_sizes: ChunkSizeIndex = chunks
        .iter()
        .map(|c| ((c.dataset.clone(), c.id.clone()), c.size))
        .collect();

    let data_movement =
        compute_data_movement(previous_chunk_owners, &current_chunk_owners, &chunk_sizes);

    let restricted_movement = compute_data_movement(
        &filter_owners(previous_chunk_owners, restricted),
        &filter_owners(&current_chunk_owners, restricted),
        &chunk_sizes,
    );

    let used_capacity_bytes: u64 = assignment
        .worker_chunks
        .values()
        .flat_map(|indexes| indexes.iter())
        .map(|&i| chunks[i as usize].size as u64)
        .sum();

    let total_capacity_bytes = num_workers as u64 * worker_capacity;

    let metrics = ReshuffleMetrics {
        step,
        new_chunks_in_step,
        new_restricted_in_step,
        total_chunks: chunks.len(),
        total_restricted_chunks: restricted.len(),
        replication_by_weight: assignment.replication_by_weight.clone(),
        data_movement,
        total_capacity_bytes,
        used_capacity_bytes,
        eligible_workers,
        scheduled: true,
        failure_reason: None,
        restricted_movement,
    };

    (metrics, current_chunk_owners)
}

/// Builds a metrics record for the step at which scheduling failed. Movement is
/// zero; the run stops after this step.
pub fn failed_step_metrics(
    step: u32,
    new_chunks_in_step: u32,
    new_restricted_in_step: u32,
    total_chunks: usize,
    total_restricted_chunks: usize,
    num_workers: usize,
    worker_capacity: u64,
    eligible_workers: usize,
    reason: String,
) -> ReshuffleMetrics {
    ReshuffleMetrics {
        step,
        new_chunks_in_step,
        new_restricted_in_step,
        total_chunks,
        total_restricted_chunks,
        replication_by_weight: BTreeMap::new(),
        data_movement: DataMovement::zero(),
        total_capacity_bytes: num_workers as u64 * worker_capacity,
        used_capacity_bytes: 0,
        eligible_workers,
        scheduled: false,
        failure_reason: Some(reason),
        restricted_movement: DataMovement::zero(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::baseline::DatasetInfo;
    use rand::{SeedableRng, rngs::StdRng};

    fn dataset(id: &str, count: u32) -> DatasetInfo {
        DatasetInfo {
            dataset_id: Arc::new(id.to_string()),
            chunk_count: count,
            last_block: 1000,
            avg_block_span: 100,
            avg_chunk_size: 1000,
        }
    }

    #[test]
    fn generate_tags_restricted_fraction_of_chunks() {
        let mut datasets = vec![dataset("ds", 100)];
        let mut rng = StdRng::seed_from_u64(42);
        let (chunks, restricted) = generate_new_chunks(&mut datasets, 100, 0.25, None, &mut rng);
        assert_eq!(chunks.len(), 100);
        assert_eq!(restricted.len(), 25);
        let ids: std::collections::HashSet<_> = chunks
            .iter()
            .map(|c| (c.dataset.clone(), c.id.clone()))
            .collect();
        assert!(restricted.iter().all(|r| ids.contains(r)));
    }

    #[test]
    fn generate_with_zero_fraction_tags_nothing() {
        let mut datasets = vec![dataset("ds", 10)];
        let mut rng = StdRng::seed_from_u64(1);
        let (chunks, restricted) = generate_new_chunks(&mut datasets, 10, 0.0, None, &mut rng);
        assert_eq!(chunks.len(), 10);
        assert!(restricted.is_empty());
    }
}
