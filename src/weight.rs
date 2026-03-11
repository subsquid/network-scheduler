use itertools::Itertools;

use semver::Version;

use crate::cli::{DatasetSegmentConfig, DatasetsConfig};
use crate::metrics::{self, DatasetSegmentStats};
use crate::scheduling::ScheduledChunk;
use crate::types::{Chunk, ChunkWeight};

// TODO: add unit tests
pub fn prepare_chunks(
    chunks: Vec<Chunk>,
    config: &DatasetsConfig,
) -> (Vec<ScheduledChunk>, Vec<Chunk>) {
    let mut scheduled_chunks = Vec::with_capacity(chunks.len());
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
                scheduled_chunks.push(ScheduledChunk {
                    id: format!("{}/{}", dataset, chunk.id),
                    size: chunk.size,
                    weight: segments[cur].weight,
                    minimum_worker_version: segments[cur].minimum_worker_version.clone(),
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
    (scheduled_chunks, filtered_chunks)
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
