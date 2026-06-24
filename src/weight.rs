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
