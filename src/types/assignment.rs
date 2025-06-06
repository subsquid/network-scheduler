use std::collections::{BTreeMap, HashMap};

use libp2p_identity::PeerId;
use sqd_messages::assignments as model;

use crate::cli;

use super::{Chunk, ChunkIndex, Worker, WorkerStatus};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assignment {
    // chunk indexes are sorted
    pub worker_chunks: BTreeMap<PeerId, Vec<ChunkIndex>>,
    pub replication_by_weight: BTreeMap<u16, u16>,
}

impl Assignment {
    pub fn log_stats(&self, chunks: &[Chunk], config: &cli::Config, workers: &[Worker]) {
        let statuses: HashMap<_, _> = workers.iter().map(|w: &Worker| (w.id, w.status)).collect();
        let min = self
            .worker_chunks
            .iter()
            .map(|(worker_id, chunk_indexes)| {
                let bytes = chunk_indexes
                    .iter()
                    .map(|i| chunks[*i as usize].size as u64)
                    .sum::<u64>();
                let num_chunks = chunk_indexes.len();

                let status_str = match statuses[worker_id] {
                    WorkerStatus::Online => String::new(),
                    status => format!(" ({status})"),
                };

                tracing::debug!(
                    "Worker {}: {}GB, {} chunks{}",
                    worker_id,
                    bytes / (1 << 30),
                    num_chunks,
                    status_str,
                );

                crate::metrics::report_worker_stats(*worker_id, num_chunks, bytes);

                bytes
            })
            .min();

        let min_bytes_per_worker = config.worker_stale_bytes;
        if let Some(min) = min {
            if min < min_bytes_per_worker {
                tracing::warn!(
                    "Some workers have less than the minimum required storage: {min} < {min_bytes_per_worker}"
                );
                crate::metrics::failure("min_assignment");
            }
        }

        crate::metrics::report_replication_factors(config.datasets.iter().map(|(ds, config)| {
            (
                format!("s3://{ds}"),
                self.replication_by_weight[&config.weight],
            )
        }));
    }

    pub fn encode(
        self,
        chunks: Vec<Chunk>,
        config: &cli::Config,
        workers: &[Worker],
    ) -> model::Assignment {
        let _timer = crate::metrics::Timer::new("serialize_assignment");
        let statuses: HashMap<_, _> = workers.iter().map(|w| (w.id, w.status)).collect();
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

        for (peer_id, indexes) in self.worker_chunks {
            let jail_reason = match statuses[&peer_id] {
                WorkerStatus::Online => None,
                WorkerStatus::Offline => Some("unreachable".to_string()),
                WorkerStatus::UnsupportedVersion => Some("unsupported_version".to_string()),
                WorkerStatus::Stale => Some("stale".to_string()),
            };

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
