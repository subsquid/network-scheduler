use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use anyhow::Context;
use libp2p_identity::PeerId;
use network_scheduler::{
    scheduling::{ScheduledChunk, SchedulingConfig, schedule},
    types::{Chunk, Worker},
    weight::prepare_chunks,
};
use rand::prelude::*;

use crate::{ChunkOwners, ChunkSizeIndex, baseline::DatasetInfo};

pub struct ReshuffleMetrics {
    pub step: u32,
    pub new_chunks_in_step: u32,
    pub total_chunks: usize,
    pub replication_by_weight: BTreeMap<u16, u16>,
    pub data_movement: DataMovement,
    pub total_capacity_bytes: u64,
    pub used_capacity_bytes: u64,
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
pub fn generate_new_chunks(
    datasets: &mut Vec<DatasetInfo>,
    chunks_to_generate: u32,
    rng: &mut impl Rng,
) -> Vec<Chunk> {
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
            size: dataset.avg_chunk_size,
            blocks: first_block..=last_block,
            files: Arc::new(vec![]),
            summary: None,
        });

        dataset.last_block = last_block;
        dataset.chunk_count += 1;
    }

    new_chunks
}

// --- Scheduling ---

/// Merges existing and new chunks, assigns weights via the config's dataset segments,
/// and runs the full scheduling algorithm. Chunks are sorted by dataset then block number
/// so that `prepare_chunks` can resolve relative segment boundaries (e.g. `from: -10000000`)
/// against the updated last_block of each dataset.
pub fn schedule_combined_chunks(
    existing_chunks: &[Chunk],
    new_chunks: &[Chunk],
    workers: &[Worker],
    datasets_config: &network_scheduler::cli::DatasetsConfig,
    scheduling_config: &SchedulingConfig,
) -> anyhow::Result<(Vec<Chunk>, network_scheduler::types::Assignment)> {
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
        .map(|(chunk, weight, mwv)| ScheduledChunk {
            dataset: &chunk.dataset,
            chunk_id: &chunk.id,
            size: chunk.size,
            weight: *weight,
            minimum_worker_version: mwv.as_ref(),
        })
        .collect();

    let assignment = schedule(&scheduled_chunks, workers, scheduling_config.clone())
        .context("Scheduling failed")?;
    drop(scheduled_chunks);

    let filtered_chunks: Vec<Chunk> = prepared.into_iter().map(|(c, _, _)| c).collect();

    Ok((filtered_chunks, assignment))
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

/// Computes all reshuffle metrics for a single simulation step.
/// All comparisons are against the previous step (incremental).
/// Returns the current chunk owners for use as the next step's previous.
pub fn measure_reshuffle(
    previous_chunk_owners: &ChunkOwners,
    chunks: &[Chunk],
    assignment: &network_scheduler::types::Assignment,
    step: u32,
    new_chunks_in_step: u32,
    num_workers: usize,
    worker_capacity: u64,
) -> (ReshuffleMetrics, ChunkOwners) {
    let current_chunk_owners = chunk_owners_from_assignment(chunks, assignment);

    let chunk_sizes: ChunkSizeIndex = chunks
        .iter()
        .map(|c| ((c.dataset.clone(), c.id.clone()), c.size))
        .collect();

    let data_movement =
        compute_data_movement(previous_chunk_owners, &current_chunk_owners, &chunk_sizes);

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
        total_chunks: chunks.len(),
        replication_by_weight: assignment.replication_by_weight.clone(),
        data_movement,
        total_capacity_bytes,
        used_capacity_bytes,
    };

    (metrics, current_chunk_owners)
}
