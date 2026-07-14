//! Synthetic chunk generation for the simulation.

use std::sync::Arc;

use network_scheduler::types::Chunk;
use rand::prelude::*;

use crate::baseline::DatasetInfo;

/// Generates `count` synthetic chunks at the head (latest blocks) of the
/// datasets, sampling each dataset proportionally to its current chunk count.
///
/// `chunk_size` overrides every chunk's size; when `None` each chunk uses its
/// dataset's average chunk size.
pub fn generate_new_chunks(
    datasets: &mut [DatasetInfo],
    count: u32,
    chunk_size: Option<u32>,
    rng: &mut impl Rng,
) -> Vec<Chunk> {
    let distribution = cumulative_chunk_distribution(datasets);
    (0..count)
        .map(|_| {
            let index = sample_dataset(&distribution, rng);
            extend_dataset(&mut datasets[index], chunk_size)
        })
        .collect()
}

/// Generates `count` synthetic chunks at the head of a single `dataset` by
/// repeated head-extension. Draws no RNG — there is nothing to sample.
pub fn generate_for_dataset(
    dataset: &mut DatasetInfo,
    count: u32,
    chunk_size: Option<u32>,
) -> Vec<Chunk> {
    (0..count)
        .map(|_| extend_dataset(dataset, chunk_size))
        .collect()
}

fn extend_dataset(dataset: &mut DatasetInfo, chunk_size: Option<u32>) -> Chunk {
    let first_block = dataset.last_block + 1;
    let last_block = first_block + dataset.avg_block_span - 1;
    let id = format!(
        "{:010}/{:010}-{:010}-{:08x}",
        first_block, first_block, last_block, first_block as u32
    );

    dataset.last_block = last_block;
    dataset.chunk_count += 1;

    Chunk {
        dataset: dataset.dataset_id.clone(),
        id: Arc::new(id),
        size: chunk_size.unwrap_or(dataset.avg_chunk_size),
        blocks: first_block..=last_block,
        files: Arc::new(vec![]),
        summary: None,
    }
}

/// Cumulative `(dataset_index, threshold)` weights: each dataset's share is proportional to its
/// chunk count.
fn cumulative_chunk_distribution(datasets: &[DatasetInfo]) -> Vec<(usize, f64)> {
    let total: f64 = datasets.iter().map(|d| d.chunk_count as f64).sum();
    let mut cumulative = Vec::with_capacity(datasets.len());
    let mut acc = 0.0;
    for (index, dataset) in datasets.iter().enumerate() {
        acc += dataset.chunk_count as f64 / total;
        cumulative.push((index, acc));
    }
    cumulative
}

fn sample_dataset(distribution: &[(usize, f64)], rng: &mut impl Rng) -> usize {
    let r: f64 = rng.random();
    distribution
        .iter()
        .find(|(_, threshold)| r <= *threshold)
        .map(|(index, _)| *index)
        .unwrap_or(distribution.len() - 1)
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn chunk_size_override_applies_to_all() {
        let mut datasets = vec![dataset("ds", 10)];
        let mut rng = StdRng::seed_from_u64(1);
        let chunks = generate_new_chunks(&mut datasets, 10, Some(777), &mut rng);
        assert!(chunks.iter().all(|c| c.size == 777));
    }
}
