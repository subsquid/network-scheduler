use itertools::Itertools;

use semver::Version;

use crate::cli::{DatasetSegmentConfig, DatasetsConfig};
use crate::metrics::{self, DatasetSegmentStats};
use crate::types::{Chunk, ChunkWeight};

/// Assigns each chunk its replication weight (and the minimum worker version that may hold it),
/// dropping any chunk the strategy doesn't cover. This is the one seam through which the scheduler
/// learns how heavily to replicate a chunk; production drives it from the [`DatasetsConfig`] (weight
/// by block-range segment), while tests can supply their own mapping.
pub trait WeightStrategy {
    fn prepare(&self, chunks: Vec<Chunk>) -> Vec<(Chunk, ChunkWeight, Option<Version>)>;
}

/// Thin free-function form of [`WeightStrategy::prepare`], so call sites read as
/// `prepare_chunks(chunks, &strategy)`. A [`DatasetsConfig`] is itself a [`WeightStrategy`], so the
/// production call sites pass `&config.datasets` unchanged.
pub fn prepare_chunks(
    chunks: Vec<Chunk>,
    strategy: &impl WeightStrategy,
) -> Vec<(Chunk, ChunkWeight, Option<Version>)> {
    strategy.prepare(chunks)
}

/// The production strategy: each dataset's chunks are walked in ascending block order, and each chunk
/// takes the weight of the block-range segment its first block falls in (chunks before the first
/// segment are dropped). Also emits per-segment chunk/size metrics.
impl WeightStrategy for DatasetsConfig {
    fn prepare(&self, chunks: Vec<Chunk>) -> Vec<(Chunk, ChunkWeight, Option<Version>)> {
        debug_assert!(
            chunks.is_sorted_by(
                |a, b| (&a.dataset, a.blocks.start()) <= (&b.dataset, b.blocks.start())
            ),
            "prepare: chunks must be sorted by (dataset, block)"
        );
        let mut prepared = Vec::with_capacity(chunks.len());
        for (dataset, chunks) in &chunks.into_iter().chunk_by(|chunk| chunk.dataset.clone()) {
            let chunks = chunks.collect_vec();
            let bucket = chunks[0].bucket();
            let last_block = *chunks.last().unwrap().blocks.end();
            let segments = to_absolute_blocks(&self[bucket], last_block);

            let mut cur = 0;
            let mut segment_chunks = 0;
            let mut segment_size = 0;
            let mut stats = Vec::new();
            for chunk in chunks {
                while cur + 1 < segments.len() && chunk.blocks.start() >= &segments[cur + 1].from {
                    stats.push(DatasetSegmentStats {
                        from: segments[cur].original_from,
                        num_chunks: segment_chunks,
                        total_size: segment_size,
                    });
                    segment_chunks = 0;
                    segment_size = 0;

                    cur += 1;
                }

                if chunk.blocks.start() >= &segments[cur].from {
                    segment_chunks += 1;
                    segment_size += chunk.size as u64;
                    prepared.push((
                        chunk,
                        segments[cur].weight,
                        segments[cur].minimum_worker_version.clone(),
                    ));
                }
            }
            stats.push(DatasetSegmentStats {
                from: segments[cur].original_from,
                num_chunks: segment_chunks,
                total_size: segment_size,
            });
            metrics::report_chunks(&dataset, &stats);
        }
        prepared
    }
}

struct Segment {
    from: u64,
    original_from: i64,
    weight: ChunkWeight,
    minimum_worker_version: Option<Version>,
}

fn to_absolute_blocks(segments: &[DatasetSegmentConfig], last_block: u64) -> Vec<Segment> {
    let mut last = 0;
    segments
        .iter()
        .map(|seg| {
            let from = if seg.from < 0 {
                (last_block as i64 + seg.from) as u64
            } else {
                seg.from as u64
            }
            .max(last);
            last = from;

            Segment {
                from,
                original_from: seg.from,
                weight: seg.weight,
                minimum_worker_version: seg.minimum_worker_version.clone(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{DatasetSegmentConfig, DatasetsConfig};
    use crate::types::Chunk;
    use std::sync::Arc;

    fn chunk(dataset: &str, first_block: u64) -> Chunk {
        Chunk {
            dataset: Arc::new(dataset.to_string()),
            id: Arc::new(format!("{first_block:010}")),
            size: 1,
            blocks: first_block..=first_block,
            files: Arc::new(Vec::new()),
            summary: None,
        }
    }

    fn config() -> DatasetsConfig {
        let mut cfg = DatasetsConfig::new();
        cfg.insert(
            "ds".to_string(),
            vec![
                DatasetSegmentConfig {
                    from: 0,
                    weight: 1,
                    minimum_worker_version: None,
                },
                DatasetSegmentConfig {
                    from: 100,
                    weight: 9,
                    minimum_worker_version: None,
                },
            ],
        );
        cfg
    }

    // Sorted by (dataset, block): every chunk kept, weight taken from its block segment.
    #[test]
    fn prepare_accepts_sorted_chunks() {
        let out = prepare_chunks(
            vec![
                chunk("s3://ds", 0),
                chunk("s3://ds", 50),
                chunk("s3://ds", 150),
            ],
            &config(),
        );
        let weights: Vec<ChunkWeight> = out.iter().map(|(_, w, _)| *w).collect();
        assert_eq!(weights, vec![1, 1, 9]);
    }

    // Out of (dataset, block) order trips the debug-only assert.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "sorted by (dataset, block)")]
    fn prepare_rejects_unsorted_chunks() {
        prepare_chunks(vec![chunk("s3://ds", 10), chunk("s3://ds", 0)], &config());
    }
}
