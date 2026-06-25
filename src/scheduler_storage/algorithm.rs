//! Pluggable scheduling algorithm for the scheduling cycle. [`DefaultSchedulingAlgorithm`]
//! delegates to the single-step [`crate::scheduling`]; [`MultistepAlgorithm`] delegates to the
//! placement-aware [`crate::multistep_scheduler`]. Tests can substitute a mock.

use crate::cli::DatasetsConfig;
use crate::multistep_scheduler::{
    ScheduledChunk as MultistepScheduledChunk, SchedulingConfig as MultistepSchedulingConfig,
    schedule as multistep_schedule,
};
use crate::scheduler_storage::{ChunkPk, WorkerPk};
use crate::scheduling::{ScheduledChunk, SchedulingConfig, schedule_with_per_worker_allocations};
use crate::types::{Chunk, Worker, WorkerIndex};
use crate::weight::{self, WeightStrategy};
use libp2p_identity::PeerId;
use semver::Version;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

/// Chunk → workers assignment, keyed by the storage surrogate pks.
pub type IdealMapping = BTreeMap<ChunkPk, HashSet<WorkerPk>>;

/// A scheduling run's output: the ideal placement and the chosen replication factor per chunk weight.
pub struct ScheduleOutput {
    pub mapping: IdealMapping,
    pub replication_by_weight: BTreeMap<u16, u16>,
}

/// Computes the ideal chunk → workers mapping for one scheduling cycle.
///
/// `current_placement` is the per-chunk holder set physically on disk now
/// (`ideal ∪ stale`). Placement-aware algorithms reconcile against it (held
/// copies are free to keep but occupy footprint); placement-blind ones ignore it.
pub trait SchedulingAlgorithm {
    type Config;

    fn schedule(
        &self,
        chunks: Vec<(ChunkPk, Chunk)>,
        workers: Vec<(WorkerPk, Worker)>,
        current_placement: &BTreeMap<ChunkPk, Vec<WorkerPk>>,
        config: &Self::Config,
    ) -> anyhow::Result<ScheduleOutput>;
}

/// Production implementation: consistent hashing with bounded loads.
pub struct DefaultSchedulingAlgorithm {
    pub datasets_config: Arc<DatasetsConfig>,
}

impl SchedulingAlgorithm for DefaultSchedulingAlgorithm {
    type Config = SchedulingConfig;

    fn schedule(
        &self,
        chunks: Vec<(ChunkPk, Chunk)>,
        workers: Vec<(WorkerPk, Worker)>,
        _current_placement: &BTreeMap<ChunkPk, Vec<WorkerPk>>,
        config: &SchedulingConfig,
    ) -> anyhow::Result<ScheduleOutput> {
        let (chunk_pks, chunks_only): (Vec<ChunkPk>, Vec<Chunk>) = chunks.into_iter().unzip();
        let (worker_ids, workers_only): (Vec<WorkerPk>, Vec<Worker>) = workers.into_iter().unzip();

        let peer_to_index: HashMap<PeerId, usize> = workers_only
            .iter()
            .enumerate()
            .map(|(index, worker)| (worker.id, index))
            .collect();

        let prepared = weight::prepare_chunks(chunks_only, &*self.datasets_config);
        let scheduled_chunks: Vec<ScheduledChunk<'_>> = prepared
            .iter()
            .map(|(chunk, weight, min_ver)| ScheduledChunk {
                dataset: &chunk.dataset,
                chunk_id: &chunk.id,
                size: chunk.size,
                weight: *weight,
                minimum_worker_version: min_ver.as_ref(),
                hashes: Vec::new(),
            })
            .collect();

        let assignment = schedule_with_per_worker_allocations(
            &scheduled_chunks,
            &workers_only,
            config.clone(),
            &HashMap::new(),
        )?;

        let mut mapping: IdealMapping = BTreeMap::new();
        for (peer_id, chunk_indexes) in assignment.worker_chunks {
            let worker_id = &worker_ids[peer_to_index[&peer_id]];
            for chunk_index in chunk_indexes {
                mapping
                    .entry(chunk_pks[chunk_index as usize])
                    .or_default()
                    .insert(*worker_id);
            }
        }
        Ok(ScheduleOutput {
            mapping,
            replication_by_weight: assignment.replication_by_weight,
        })
    }
}

/// Placement-aware implementation: wraps [`crate::multistep_scheduler::schedule`], mapping storage
/// surrogate ids to the scheduler's positions. It reconciles against `current_placement` (held and
/// draining copies count as footprint) and takes chunk weights from the injected [`WeightStrategy`].
pub struct MultistepAlgorithm {
    weight_strategy: Box<dyn WeightStrategy + Send + Sync>,
}

impl MultistepAlgorithm {
    pub fn new(weight_strategy: Box<dyn WeightStrategy + Send + Sync>) -> Self {
        Self { weight_strategy }
    }
}

impl SchedulingAlgorithm for MultistepAlgorithm {
    type Config = MultistepSchedulingConfig;

    fn schedule(
        &self,
        chunks: Vec<(ChunkPk, Chunk)>,
        workers: Vec<(WorkerPk, Worker)>,
        current_placement: &BTreeMap<ChunkPk, Vec<WorkerPk>>,
        config: &MultistepSchedulingConfig,
    ) -> anyhow::Result<ScheduleOutput> {
        let (chunk_pks, chunks_only): (Vec<ChunkPk>, Vec<Chunk>) = chunks.into_iter().unzip();
        let (worker_ids, workers_only): (Vec<WorkerPk>, Vec<Worker>) = workers.into_iter().unzip();

        let worker_id_to_pos: HashMap<WorkerPk, WorkerIndex> = worker_ids
            .iter()
            .enumerate()
            .map(|(i, id)| (*id, i as WorkerIndex))
            .collect();
        let peer_to_index: HashMap<PeerId, usize> = workers_only
            .iter()
            .enumerate()
            .map(|(i, w)| (w.id, i))
            .collect();

        // Holders whose worker is absent from this fleet are dropped (a shortfall).
        let current: Vec<Vec<WorkerIndex>> = chunk_pks
            .iter()
            .map(|pk| {
                current_placement
                    .get(pk)
                    .map(|ws| {
                        ws.iter()
                            .filter_map(|id| worker_id_to_pos.get(id).copied())
                            .collect()
                    })
                    .unwrap_or_default()
            })
            .collect();

        // Weight key is the chunk's own `(dataset, id)`, NOT the storage pk (an unrelated id space).
        let dataset_key = |c: &Chunk| ((*c.dataset).clone(), (*c.id).clone());

        // `prepare` may reorder / drop chunks, so key the result and realign below; it takes
        // ownership, hence the clone.
        let prepared = self.weight_strategy.prepare(chunks_only.clone());
        let weight_by_key: HashMap<(String, String), (u16, Option<Version>)> = prepared
            .into_iter()
            .map(|(c, w, v)| (dataset_key(&c), (w, v)))
            .collect();

        let weights_versions: Vec<(u16, Option<Version>)> = chunks_only
            .iter()
            .map(|c| {
                weight_by_key
                    .get(&dataset_key(c))
                    .cloned()
                    .unwrap_or((1, None))
            })
            .collect();

        let scheduled: Vec<MultistepScheduledChunk<'_>> = chunks_only
            .iter()
            .zip(&weights_versions)
            .map(|(chunk, (weight, version))| MultistepScheduledChunk {
                dataset: &chunk.dataset,
                chunk_id: &chunk.id,
                size: chunk.size,
                weight: *weight,
                minimum_worker_version: version.as_ref(),
            })
            .collect();

        let assignment = multistep_schedule(&scheduled, &workers_only, config.clone(), &current)?;

        let mut mapping: IdealMapping = BTreeMap::new();
        for (peer_id, chunk_indexes) in assignment.worker_chunks {
            let worker_id = &worker_ids[peer_to_index[&peer_id]];
            for chunk_index in chunk_indexes {
                mapping
                    .entry(chunk_pks[chunk_index as usize])
                    .or_default()
                    .insert(*worker_id);
            }
        }
        Ok(ScheduleOutput {
            mapping,
            replication_by_weight: assignment.replication_by_weight,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::DatasetSegmentConfig;
    use crate::tests::input::generate_workers;
    use crate::types::WorkerStatus;

    fn chunk(dataset: &str, seed: u32, size: u32) -> Chunk {
        Chunk {
            dataset: Arc::new(dataset.to_string()),
            id: Arc::new(format!("{seed:010}")),
            size,
            blocks: (seed as u64)..=(seed as u64),
            files: Arc::new(Vec::new()),
            summary: None,
        }
    }

    fn algo() -> MultistepAlgorithm {
        let mut datasets_config = DatasetsConfig::new();
        // Bucket key is the dataset minus the `s3://` prefix.
        datasets_config.insert(
            "w1".to_string(),
            vec![DatasetSegmentConfig {
                from: 0,
                weight: 1,
                minimum_worker_version: None,
            }],
        );
        MultistepAlgorithm::new(Box::new(datasets_config))
    }

    fn config() -> MultistepSchedulingConfig {
        MultistepSchedulingConfig {
            worker_capacity: 1_000_000,
            saturation: 0.9,
            min_replication: 2,
            ignore_reliability: true,
        }
    }

    #[test]
    fn places_floor_on_distinct_workers() {
        let peers = generate_workers(3);
        let workers: Vec<(WorkerPk, Worker)> = peers
            .iter()
            .enumerate()
            .map(|(i, id)| {
                (
                    WorkerPk(i as i64 + 1),
                    Worker {
                        id: *id,
                        status: WorkerStatus::Online,
                        version: None,
                    },
                )
            })
            .collect();
        let pk = ChunkPk(1);
        let chunks = vec![(pk, chunk("s3://w1", 0, 1))];

        let output = algo()
            .schedule(chunks, workers, &BTreeMap::new(), &config())
            .expect("feasible");

        // With near-zero fill the planner may add bonus copies above the floor of 2.
        let holders = &output.mapping[&pk];
        assert!(
            holders.len() >= 2,
            "floor not met: {} copies",
            holders.len()
        );
        assert_eq!(
            holders.len(),
            holders
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            "holders must be distinct workers",
        );
    }
}
