use std::collections::BTreeMap;

use libp2p_identity::PeerId;
use sqd_messages::assignments as model;

use crate::cli;

use super::{Chunk, ChunkIndex};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assignment {
    // chunk indexes are sorted
    pub workers: BTreeMap<PeerId, Vec<ChunkIndex>>,
}

impl Assignment {
    pub fn log_stats(&self, chunks: &[Chunk]) {
        self.workers.iter().for_each(|(worker_id, chunk_indexes)| {
            tracing::debug!(
                "Worker {}: {}GB, {} chunks",
                worker_id,
                chunk_indexes
                    .iter()
                    .map(|i| chunks[*i as usize].size as u64)
                    .sum::<u64>()
                    / (1 << 30),
                chunk_indexes.len()
            );
        });
    }

    pub fn encode(self, chunks: Vec<Chunk>, config: &cli::Config) -> model::Assignment {
        let mut assignment = model::Assignment::default();
        for chunk in chunks {
            let download_url = format!("https://{}.{}", chunk.bucket(), config.storage_domain);
            let chunk_str = chunk.id;
            let files = chunk.files.iter().map(|f| (f.clone(), f.clone())).collect();
            let dataset_id = String::from(&*chunk.dataset);
            let chunk = model::Chunk {
                base_url: format!("{download_url}/{chunk_str}"),
                id: chunk_str,
                files,
                size_bytes: chunk.size as u64,
                summary: chunk.summary,
            };

            assignment.add_chunk(chunk, dataset_id, download_url);
        }

        for (peer_id, indexes) in self.workers {
            let jail_reason = None; // TODO: add jail reason

            // delta encode chunk_indexes
            let mut last = 0;
            let mut deltas = Vec::with_capacity(indexes.len());
            for index in indexes {
                deltas.push(index as u64 - last);
                last = index as u64;
            }

            assignment.insert_assignment(peer_id, jail_reason, deltas);
        }
        assignment.regenerate_headers(&config.cloudflare_storage_secret);
        assignment
    }
}
