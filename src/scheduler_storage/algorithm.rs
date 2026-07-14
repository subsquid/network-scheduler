//! Pluggable scheduling algorithm for the scheduling cycle. [`DefaultSchedulingAlgorithm`]
//! delegates to the single-step [`crate::scheduling`]; [`MultistepAlgorithm`] delegates to the
//! placement-aware [`crate::multistep_scheduler`]. Tests can substitute a mock.

use crate::cli::DatasetsConfig;
use crate::multistep_scheduler::{
    ScheduledChunk as MultistepScheduledChunk, SchedulingConfig as MultistepSchedulingConfig,
    schedule as multistep_schedule,
};
use crate::rings::WorkerRingCache;
use crate::scheduler_storage::{AlgoChunk, ChunkPk, WorkerPk};
use crate::scheduling::{ScheduledChunk, SchedulingConfig, schedule_with_per_worker_allocations};
use crate::types::{BlockNumber, ChunkIndex, Worker, WorkerIndex};
use crate::weight::{self, SchedulingChunk, WeightStrategy};
use itertools::Itertools;
use libp2p_identity::PeerId;
use parking_lot::Mutex;
use rustc_hash::FxHashMap;
use std::collections::{BTreeMap, HashMap};
use std::ops::RangeInclusive;
use std::sync::Arc;

/// Lets `(ChunkPk, AlgoChunk)` pairs flow through [`WeightStrategy::prepare`]: it weights the chunk
/// but returns the whole pair, so the pk rides along with each result instead of being re-associated.
impl SchedulingChunk for (ChunkPk, AlgoChunk) {
    fn dataset(&self) -> &Arc<String> {
        &self.1.dataset
    }
    fn id(&self) -> &Arc<String> {
        &self.1.id
    }
    fn blocks(&self) -> &RangeInclusive<BlockNumber> {
        &self.1.blocks
    }
    fn size(&self) -> u32 {
        self.1.size
    }
}

/// Chunk → holders, in `chunk_pk` order, each holder list sorted by `worker_pk` — the canonical
/// form the stored ideal keeps for next cycle's `IS DISTINCT FROM` diff.
pub type IdealMapping = Vec<(ChunkPk, Vec<WorkerPk>)>;

/// Per-chunk holders physically on disk now (`ideal ∪ stale`), keyed by the i64 surrogate pk.
/// `FxHashMap`: at up to 10M entries, built and probed once per chunk, SipHash dominates the cost.
pub type CurrentPlacement = FxHashMap<ChunkPk, Vec<WorkerPk>>;

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
        chunks: Vec<(ChunkPk, AlgoChunk)>,
        workers: Vec<(WorkerPk, Worker)>,
        current_placement: &CurrentPlacement,
        config: &Self::Config,
    ) -> anyhow::Result<ScheduleOutput>;
}

/// Production implementation: consistent hashing with bounded loads.
pub struct DefaultSchedulingAlgorithm {
    pub datasets_config: Arc<DatasetsConfig>,
    ring_cache: Mutex<WorkerRingCache>,
}

impl DefaultSchedulingAlgorithm {
    pub fn new(datasets_config: Arc<DatasetsConfig>) -> Self {
        Self {
            datasets_config,
            ring_cache: Mutex::new(WorkerRingCache::new()),
        }
    }
}

impl SchedulingAlgorithm for DefaultSchedulingAlgorithm {
    type Config = SchedulingConfig;

    fn schedule(
        &self,
        chunks: Vec<(ChunkPk, AlgoChunk)>,
        workers: Vec<(WorkerPk, Worker)>,
        _current_placement: &CurrentPlacement,
        config: &SchedulingConfig,
    ) -> anyhow::Result<ScheduleOutput> {
        let (chunk_pks, chunks_only): (Vec<ChunkPk>, Vec<AlgoChunk>) = chunks.into_iter().unzip();

        let (worker_ids, workers_only): (Vec<WorkerPk>, Vec<Worker>) =
            sorted_by_peer_id(workers).into_iter().unzip();

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
            &mut self.ring_cache.lock(),
        )?;

        let mapping = invert_worker_chunks(
            assignment.worker_chunks,
            &peer_to_index,
            &worker_ids,
            &chunk_pks,
        );
        Ok(ScheduleOutput {
            mapping,
            replication_by_weight: assignment.replication_by_weight,
        })
    }
}

/// Placement-aware implementation: wraps [`crate::multistep_scheduler::schedule`], mapping storage
/// surrogate ids to the scheduler's positions. It reconciles against `current_placement` (held and
/// draining copies count as footprint) and takes chunk weights from the injected [`WeightStrategy`].
pub struct MultistepAlgorithm<S>
where
    S: WeightStrategy + Sync + Send,
{
    weight_strategy: S,
    ring_cache: Mutex<WorkerRingCache>,
}

impl<S> MultistepAlgorithm<S>
where
    S: WeightStrategy + Sync + Send,
{
    pub fn new(weight_strategy: S) -> Self {
        Self {
            weight_strategy,
            ring_cache: Mutex::new(WorkerRingCache::new()),
        }
    }

    pub fn set_weight_strategy(&mut self, weight_strategy: S) {
        self.weight_strategy = weight_strategy;
    }
}

impl<S> SchedulingAlgorithm for MultistepAlgorithm<S>
where
    S: WeightStrategy + Sync + Send,
{
    type Config = MultistepSchedulingConfig;

    fn schedule(
        &self,
        chunks: Vec<(ChunkPk, AlgoChunk)>,
        workers: Vec<(WorkerPk, Worker)>,
        current_placement: &CurrentPlacement,
        config: &MultistepSchedulingConfig,
    ) -> anyhow::Result<ScheduleOutput> {
        let (worker_ids, workers_only): (Vec<WorkerPk>, Vec<Worker>) =
            sorted_by_peer_id(workers).into_iter().unzip();

        let worker_pk_to_pos: HashMap<WorkerPk, WorkerIndex> = worker_ids
            .iter()
            .enumerate()
            .map(|(i, id)| (*id, i as WorkerIndex))
            .collect();
        let peer_id_to_pos: HashMap<PeerId, usize> = workers_only
            .iter()
            .enumerate()
            .map(|(i, w)| (w.id, i))
            .collect();

        // `prepare` weights the chunks — dropping any it doesn't cover, possibly reordering — and
        // carries each chunk's pk through in the result tuple. One pass over its output builds the
        // scheduler input alongside the pk and current-placement vectors it must stay index-aligned
        // with: `scheduled[i]`, `scheduled_pks[i]`, and `current[i]` all describe the same chunk. A
        // holder whose worker is absent from this fleet is dropped.
        let prepared = self.weight_strategy.prepare(chunks);

        let mut scheduled: Vec<MultistepScheduledChunk<'_>> = Vec::with_capacity(prepared.len());
        let mut scheduled_pks: Vec<ChunkPk> = Vec::with_capacity(prepared.len());
        let mut current: Vec<Vec<WorkerIndex>> = Vec::with_capacity(prepared.len());
        for ((pk, chunk), weight, version) in &prepared {
            // This chunk's current holders as fleet positions, dropping any whose worker has left.
            let held_positions: Vec<WorkerIndex> = current_placement
                .get(pk)
                .into_iter()
                .flatten()
                .filter_map(|worker| worker_pk_to_pos.get(worker).copied())
                .collect();

            scheduled_pks.push(*pk);
            current.push(held_positions);
            scheduled.push(MultistepScheduledChunk {
                dataset: &chunk.dataset,
                chunk_id: &chunk.id,
                size: chunk.size,
                weight: *weight,
                minimum_worker_version: version.as_ref(),
            });
        }

        let assignment = multistep_schedule(
            &scheduled,
            &workers_only,
            config,
            &current,
            &mut self.ring_cache.lock(),
        )?;

        let mapping = invert_worker_chunks(
            assignment.worker_chunks,
            &peer_id_to_pos,
            &worker_ids,
            &scheduled_pks,
        );
        Ok(ScheduleOutput {
            mapping,
            replication_by_weight: assignment.replication_by_weight,
        })
    }
}

/// Sort by `PeerId` so the worker order is stable across cycles.
/// The cache keys on this order *and* indexes its rings by worker position, so reusing cached rings
/// needs the same order — a different one would point those positions at the wrong peers. This only
/// affects the cache-hit rate, never the placement.
fn sorted_by_peer_id(mut workers: Vec<(WorkerPk, Worker)>) -> Vec<(WorkerPk, Worker)> {
    workers.sort_unstable_by_key(|(_, w)| w.id);
    workers
}

/// Invert `worker -> chunk positions` into `chunk_pk -> holders`, sorted by `chunk_pk` with each
/// holder list sorted by `worker_pk` — the canonical form the stored ideal keeps for the diff.
fn invert_worker_chunks(
    worker_chunks: BTreeMap<PeerId, Vec<ChunkIndex>>,
    peer_to_pos: &HashMap<PeerId, usize>,
    worker_ids: &[WorkerPk],
    pks: &[ChunkPk],
) -> IdealMapping {
    let mut holders_by_pos: Vec<Vec<WorkerPk>> = vec![Vec::new(); pks.len()];
    for (peer_id, chunk_indexes) in worker_chunks {
        let worker_id = worker_ids[peer_to_pos[&peer_id]];
        for chunk_index in chunk_indexes {
            // Distinct by construction: `place()` never puts a chunk on the same worker twice.
            holders_by_pos[chunk_index as usize].push(worker_id);
        }
    }
    let mut mapping: IdealMapping = holders_by_pos
        .into_iter()
        .enumerate()
        .filter(|(_, holders)| !holders.is_empty())
        .map(|(pos, mut holders)| {
            debug_assert!(
                holders.iter().all_unique(),
                "algorithm placed chunk {:?} on the same worker twice: {holders:?}",
                pks[pos],
            );
            // Canonical (sorted by worker_pk) so the COPY write is a plain format.
            holders.sort_unstable_by_key(|w| w.0);
            (pks[pos], holders)
        })
        .collect();
    mapping.sort_unstable_by_key(|(pk, _)| pk.0);
    mapping
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::DatasetSegmentConfig;
    use crate::tests::input::generate_workers;
    use crate::types::WorkerStatus;

    fn chunk(dataset: &str, seed: u32, size: u32) -> AlgoChunk {
        AlgoChunk {
            dataset: Arc::new(dataset.to_string()),
            id: Arc::new(format!("{seed:010}")),
            size,
            blocks: (seed as u64)..=(seed as u64),
        }
    }

    fn algo() -> MultistepAlgorithm<DatasetsConfig> {
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
        MultistepAlgorithm::new(datasets_config)
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
            .schedule(chunks, workers, &CurrentPlacement::default(), &config())
            .expect("feasible");

        // With near-zero fill the planner may add bonus copies above the floor of 2.
        let holders = &output
            .mapping
            .iter()
            .find(|(k, _)| *k == pk)
            .expect("chunk scheduled")
            .1;
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

    fn online_workers(n: u16) -> Vec<(WorkerPk, Worker)> {
        generate_workers(n)
            .into_iter()
            .enumerate()
            .map(|(i, id)| {
                (
                    WorkerPk(i as i64 + 1),
                    Worker {
                        id,
                        status: WorkerStatus::Online,
                        version: None,
                    },
                )
            })
            .collect()
    }

    // `prepare` drops chunks the weight strategy doesn't cover (here, those before the first
    // segment); a dropped chunk must not appear in the output mapping.
    #[test]
    fn skips_chunks_dropped_by_prepare() {
        let mut datasets_config = DatasetsConfig::new();
        // The only segment starts at block 100, so any chunk before it is uncovered.
        datasets_config.insert(
            "w1".to_string(),
            vec![DatasetSegmentConfig {
                from: 100,
                weight: 1,
                minimum_worker_version: None,
            }],
        );
        let algo = MultistepAlgorithm::new(datasets_config);

        // Sorted by (dataset, block): an uncovered chunk (block 0) then a covered one (block 150).
        let dropped = ChunkPk(1);
        let kept = ChunkPk(2);
        let chunks = vec![
            (dropped, chunk("s3://w1", 0, 1)),
            (kept, chunk("s3://w1", 150, 1)),
        ];

        let output = algo
            .schedule(
                chunks,
                online_workers(3),
                &CurrentPlacement::default(),
                &config(),
            )
            .expect("feasible");

        assert!(
            output.mapping.iter().any(|(k, _)| *k == kept),
            "covered chunk must be scheduled",
        );
        assert!(
            !output.mapping.iter().any(|(k, _)| *k == dropped),
            "chunk dropped by `prepare` must not be scheduled",
        );
    }
}
