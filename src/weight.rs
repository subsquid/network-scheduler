use itertools::Itertools;

use crate::cli::{DatasetSegmentConfig, DatasetsConfig};
use crate::metrics::{self, DatasetSegmentStats};
use crate::scheduling::WeightedChunk;
use crate::types::{Chunk, ChunkWeight};

// TODO: add unit tests
pub fn weight_chunks(
    chunks: Vec<Chunk>,
    config: &DatasetsConfig,
) -> (Vec<WeightedChunk>, Vec<Chunk>) {
    let mut weighted_chunks = Vec::with_capacity(chunks.len());
    let mut filtered_chunks = Vec::with_capacity(chunks.len());
    for (dataset, chunks) in &chunks.into_iter().chunk_by(|chunk| chunk.dataset.clone()) {
        let chunks = chunks.collect_vec();
        let bucket = chunks[0].bucket();
        let last_block = *chunks.last().unwrap().blocks.end();
        let segments = to_absolute_blocks(&config[bucket], last_block);

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
                weighted_chunks.push(WeightedChunk {
                    id: format!("{}/{}", dataset, chunk.id),
                    size: chunk.size,
                    weight: segments[cur].weight,
                });
                filtered_chunks.push(chunk);
            }
        }
        stats.push(DatasetSegmentStats {
            from: segments[cur].original_from,
            num_chunks: segment_chunks,
            total_size: segment_size,
        });
        metrics::report_chunks(&dataset, &stats);
    }
    (weighted_chunks, filtered_chunks)
}

struct Segment {
    from: u64,
    original_from: i64,
    weight: ChunkWeight,
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
            }
        })
        .collect()
}
