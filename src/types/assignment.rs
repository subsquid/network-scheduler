#[cfg(test)]
use std::collections::BTreeSet;
use std::collections::{BTreeMap, HashMap};

use libp2p_identity::PeerId;

use crate::{cli, types::WorkerIndex};

use super::{Chunk, ChunkIndex, ChunkWeight, ReplicationFactor, Worker, WorkerStatus};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assignment {
    // chunk indexes are sorted
    pub worker_chunks: BTreeMap<PeerId, Vec<ChunkIndex>>,
    pub replication_by_weight: BTreeMap<ChunkWeight, ReplicationFactor>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum FbVersion {
    #[default]
    V0,
    V1,
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
        if let Some(min) = min
            && min < min_bytes_per_worker
        {
            tracing::warn!(
                "Some workers have less than the minimum required storage: {min} < {min_bytes_per_worker}"
            );
            crate::metrics::failure("min_assignment");
        }

        crate::metrics::report_replication_factors(config.datasets.iter().map(|(ds, segments)| {
            (
                format!("s3://{ds}"),
                segments.iter().filter_map(|seg| {
                    self.replication_by_weight
                        .get(&seg.weight)
                        .map(|r| (seg.from, *r))
                }),
            )
        }));
    }

    pub fn encode_fb(
        &self,
        chunks: &[Chunk],
        config: &cli::Config,
        workers: &[Worker],
        version: FbVersion,
    ) -> Vec<u8> {
        let _timer = crate::metrics::Timer::new("serialize_assignment");

        let assigned_worker_ids = self.worker_ids_for_chunk();
        assert_eq!(
            assigned_worker_ids.len(),
            chunks.len(),
            "each chunk must have at least one worker assigned"
        );

        let mut assignment_builder = sqd_assignments::AssignmentBuilder::new(
            config.cloudflare_storage_secret.expose_secret(),
        )
        .check_continuity(config.strict_continuity_check);

        let mut prev_dataset = None;
        let mut iter = chunks.iter().zip(assigned_worker_ids).peekable();
        while let Some((chunk, worker_ids)) = iter.next() {
            if prev_dataset
                .as_ref()
                .is_some_and(|prev| prev != &chunk.dataset)
            {
                tracing::trace!("Finished serializing dataset {}", prev_dataset.unwrap());
                assignment_builder.finish_dataset();
            }
            prev_dataset = Some(chunk.dataset.clone());

            let is_last_in_dataset = iter
                .peek()
                .is_none_or(|(next, _)| next.dataset != chunk.dataset);

            let download_url = if config.storage_allow_insecure_scheme {
                format!("http://{}/{}/", config.storage_domain, chunk.bucket())
            } else {
                format!("https://{}.{}", chunk.bucket(), config.storage_domain)
            };
            let mut builder = assignment_builder
                .new_chunk()
                .id(chunk.id.as_str())
                .dataset_id(&chunk.dataset)
                .block_range(chunk.blocks.clone())
                .size(chunk.size)
                .dataset_base_url(&download_url)
                .worker_indexes(&worker_ids)
                .files(&chunk.files);
            if let Some(summary) = chunk.summary.as_ref() {
                if !config.clear_last_block_hash || is_last_in_dataset {
                    builder = builder.last_block_hash(&summary.last_block_hash);
                }
                builder = builder.last_block_timestamp(summary.last_block_timestamp);
            }
            if let Err(e) = builder.finish() {
                if config.strict_continuity_check {
                    panic!("Failed to serialize chunk ({}): {:?}", e, chunk);
                } else {
                    tracing::warn!("Failed to serialize chunk ({}): {:?}", e, chunk);
                }
            }
        }
        tracing::trace!("Finished serializing dataset {}", prev_dataset.unwrap());
        assignment_builder.finish_dataset();

        for worker in workers {
            let status = match worker.status {
                WorkerStatus::Online => sqd_assignments::WorkerStatus::Ok,
                WorkerStatus::Offline => sqd_assignments::WorkerStatus::Unreliable,
                WorkerStatus::UnsupportedVersion => {
                    sqd_assignments::WorkerStatus::UnsupportedVersion
                }
                WorkerStatus::Stale => sqd_assignments::WorkerStatus::Unreliable,
            };
            tracing::trace!("Serializing worker {}", worker.id);
            let indexes = match version {
                FbVersion::V0 => &*self.worker_chunks[&worker.id],
                FbVersion::V1 => &[],
            };
            assignment_builder.add_worker(worker.id, status, indexes);
        }

        assignment_builder.finish()
    }

    /// Invert `worker_chunks` into `chunk index -> set of holding workers`. `n_chunks` sizes the
    /// result so every chunk is present (chunks with no copies map to an empty set). Test-only: the
    /// inverse view several test suites recompute by hand.
    #[cfg(test)]
    pub fn chunk_holders(&self, n_chunks: usize) -> Vec<BTreeSet<PeerId>> {
        let mut out = vec![BTreeSet::new(); n_chunks];
        for (&worker, chunks) in &self.worker_chunks {
            for &ci in chunks {
                out[ci as usize].insert(worker);
            }
        }
        out
    }

    fn worker_ids_for_chunk(&self) -> Vec<Vec<WorkerIndex>> {
        let _timer = crate::metrics::Timer::new("serialize_assignment:worker_ids_for_chunk");
        let mut result = Vec::with_capacity(1000);
        for (worker_index, indexes) in self.worker_chunks.values().enumerate() {
            for &index in indexes {
                if index as usize >= result.len() {
                    result.resize(index as usize + 1, Vec::new());
                }
                result[index as usize].push(worker_index as u16);
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc, time::Duration};

    use super::*;
    use crate::types::ChunkSummary;

    fn test_config() -> cli::Config {
        cli::Config {
            datasets: BTreeMap::new(),
            worker_inactive_timeout: Duration::from_secs(600),
            ignore_reliability: false,
            worker_storage_bytes: 0,
            worker_stale_bytes: 0,
            min_replication: 1,
            saturation: 0.99,
            network: "test".to_string(),
            storage_domain: "test.io".to_string(),
            network_state_name: "test.json".to_string(),
            network_state_url: "https://test.io".to_string(),
            scheduler_state_bucket: "test".to_string(),
            cloudflare_storage_secret: "secret".to_owned().into(),
            min_supported_worker_version: "2.0.0".parse().unwrap(),
            min_recommended_worker_version: "2.0.0".parse().unwrap(),
            assignment_delay: Duration::from_secs(60),
            assignment_ttl: Duration::from_secs(86400),
            concurrent_dataset_downloads: 1,
            dataset_load_timeout: None,
            strict_continuity_check: false,
            storage_allow_insecure_scheme: false,
            clear_last_block_hash: false,
        }
    }

    fn make_worker() -> PeerId {
        libp2p_identity::Keypair::generate_ed25519()
            .public()
            .to_peer_id()
    }

    fn make_chunk(dataset: &str, first: u64, last: u64, hash: &str) -> Chunk {
        let id = format!("{first:010}/{first:010}-{last:010}-{:08x}", first as u32);
        Chunk {
            dataset: Arc::new(dataset.to_string()),
            id: Arc::new(id),
            size: 1000,
            blocks: first..=last,
            files: Arc::new(vec!["file.parquet".to_string()]),
            summary: Some(ChunkSummary {
                last_block_hash: hash.to_string(),
                last_block_timestamp: 1000,
            }),
        }
    }

    fn read_chunk_hashes(fb_bytes: &[u8]) -> Vec<(String, Option<String>)> {
        let fb = sqd_assignments::Assignment::from_owned_unchecked(fb_bytes.to_vec());
        let mut result = Vec::new();
        for dataset in fb.datasets() {
            for chunk in dataset.chunks() {
                result.push((
                    chunk.id().to_string(),
                    chunk.last_block_hash().map(|s| s.to_string()),
                ));
            }
        }
        result
    }

    #[test]
    fn clear_last_block_hash_keeps_last_chunk_per_dataset() {
        let worker = make_worker();
        let chunks = vec![
            make_chunk("s3://dataset-a", 0, 99, "hash_a1"),
            make_chunk("s3://dataset-a", 100, 199, "hash_a2"),
            make_chunk("s3://dataset-a", 200, 299, "hash_a3"),
            make_chunk("s3://dataset-b", 0, 99, "hash_b1"),
            make_chunk("s3://dataset-b", 100, 199, "hash_b2"),
        ];

        let assignment = Assignment {
            worker_chunks: BTreeMap::from([(worker, vec![0, 1, 2, 3, 4])]),
            replication_by_weight: BTreeMap::new(),
        };
        let workers = vec![Worker {
            id: worker,
            status: WorkerStatus::Online,
            version: None,
        }];

        // With clear_last_block_hash: only last chunk per dataset keeps the hash
        let mut config = test_config();
        config.clear_last_block_hash = true;
        let fb_bytes = assignment.encode_fb(&chunks, &config, &workers, FbVersion::V1);
        let hashes = read_chunk_hashes(&fb_bytes);

        assert_eq!(hashes[0].1, None);
        assert_eq!(hashes[1].1, None);
        assert_eq!(hashes[2].1, Some("hash_a3".to_string()));
        assert_eq!(hashes[3].1, None);
        assert_eq!(hashes[4].1, Some("hash_b2".to_string()));

        // Without clear_last_block_hash: all hashes preserved
        let mut config = test_config();
        config.clear_last_block_hash = false;
        let fb_bytes = assignment.encode_fb(&chunks, &config, &workers, FbVersion::V1);
        let hashes = read_chunk_hashes(&fb_bytes);

        assert_eq!(hashes[0].1, Some("hash_a1".to_string()));
        assert_eq!(hashes[1].1, Some("hash_a2".to_string()));
        assert_eq!(hashes[2].1, Some("hash_a3".to_string()));
        assert_eq!(hashes[3].1, Some("hash_b1".to_string()));
        assert_eq!(hashes[4].1, Some("hash_b2".to_string()));
    }
}
