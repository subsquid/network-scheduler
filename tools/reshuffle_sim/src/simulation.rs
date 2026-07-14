//! Drives the simulation: advances chunk ingestion and worker upgrades step by step, schedules each
//! step through a pluggable [`StepScheduler`], and collects reshuffle metrics.
//!
//! Version restrictions are config-driven: the synthetic restricted dataset carries
//! `minimum_worker_version` on its config segment, so both schedulers honor it identically.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use network_scheduler::{
    cli::{DatasetSegmentConfig, DatasetsConfig},
    scheduling::SchedulingConfig,
    types::{Chunk, Worker},
};
use rand::rngs::StdRng;
use semver::Version;

use anyhow::Context;

use crate::baseline::{Baseline, DatasetInfo};
use crate::metrics::{ReshuffleMetrics, StepStats};
use crate::profile::Profile;
use crate::rate::ChunkRate;
use crate::types::{ChunkId, ChunkOwners, ChunkSizeIndex, bucket_of, dataset_id};
use crate::upgrade::{self, UpgradeSchedule, WorkerVersionState};
use crate::{chunks, metrics, rate};

/// At step `at_step`, clone every current chunk of dataset `src` (bucket) into a new dataset `dst`
/// (identical ids, ranges and sizes).
pub struct CopyPlan {
    pub src: String,
    pub dst: String,
    pub at_step: u32,
}

pub struct SimulationParams {
    pub steps: u32,
    pub chunk_rate: ChunkRate,
    pub chunk_size: Option<u32>,
    pub restricted_fraction: f64,
    pub restricted_dataset_name: String,
    pub initial_new_fraction: f64,
    pub upgrade_schedule: UpgradeSchedule,
    pub lift_restriction_at_step: Option<u32>,
    pub copy: Vec<CopyPlan>,
}

impl SimulationParams {
    /// `copy` is a parameter because only the stateless path replays copies through the loop; the
    /// multistep one clones them inside Postgres.
    pub fn from_profile(run: &Profile, copy: Vec<CopyPlan>) -> anyhow::Result<Self> {
        let chunk_rate = match &run.chunks_shape {
            Some(spec) => rate::ChunkRate::parse(spec).context("Failed to parse chunks_shape")?,
            None => rate::ChunkRate::Constant {
                n: run.chunks_per_step,
            },
        };
        Ok(Self {
            steps: run.steps,
            chunk_rate,
            chunk_size: run
                .chunk_size
                .map(|s| s.as_u64().min(u32::MAX as u64) as u32),
            restricted_fraction: run.restricted_fraction,
            restricted_dataset_name: run.restricted_dataset.clone(),
            initial_new_fraction: run.initial_new_fraction,
            upgrade_schedule: UpgradeSchedule::parse(&run.upgrade_schedule)?,
            lift_restriction_at_step: run.lift_restriction_at_step,
            copy,
        })
    }
}

/// What one step's ingest added to `accumulated_new_chunks`.
struct Ingested {
    /// Where this step's chunks start in `accumulated_new_chunks`.
    start: usize,
    count: u32,
    restricted: u32,
}

pub struct Summary {
    pub total_chunks_added: usize,
    pub restricted_chunks: usize,
    pub upgraded_workers: usize,
    pub total_workers: usize,
    pub new_worker_version: Version,
}

/// One step's resulting placement — everything needed to diff against the previous step.
pub struct StepPlacement {
    /// Physical holders: what each worker has on disk (ideal ∪ stale).
    pub owners: ChunkOwners,
    /// Of those, the copies that are only draining. Empty on the stateless path. Holds the in-flight
    /// drains, not the corpus, so it stays small enough to keep across steps.
    pub stale_owners: ChunkOwners,
    pub chunk_sizes: ChunkSizeIndex,
    pub replication_by_weight: BTreeMap<u16, u16>,
    pub used_capacity_bytes: u64,
    /// Bytes held by draining stale copies (0 for the stateless path).
    pub stale_capacity_bytes: u64,
    /// Every chunk inserted so far, dead rows included — see
    /// [`ReshuffleMetrics::ingested_chunks`](crate::metrics::ReshuffleMetrics::ingested_chunks).
    pub ingested_chunks: usize,
    /// Chunks this step's `copy` and `replace` plans added.
    pub copied_or_replaced_chunks: u32,
    /// Chunks the portal routes; `None` on the stateless path.
    pub portal_chunks: Option<usize>,
    pub schedule_duration: Duration,
}

/// What the host loop hands a scheduler each step. The stateless path uses the full chunk set; the
/// multistep path uses only the step's new chunks (it persists the rest).
pub struct StepContext<'a> {
    pub baseline_chunks: &'a [Chunk],
    pub accumulated_new_chunks: &'a [Chunk],
    pub new_chunks_this_step: &'a [Chunk],
    pub workers: &'a [Worker],
    pub datasets_config: &'a DatasetsConfig,
    pub scheduling_config: &'a SchedulingConfig,
}

pub enum StepOutcome {
    Scheduled(StepPlacement),
    /// Infeasible this step (e.g. a capacity shortage): the run records a failed step and stops.
    Failed(String),
}

/// A scheduler the shared loop drives one step at a time — the only part that differs between the
/// stateless and multistep paths.
pub trait StepScheduler {
    /// Placement that step 1 is diffed against, and the sizes of the chunks in it: a chunk dropped
    /// later still has to be scored as freeing its bytes, and by then it is gone from the placement it
    /// left. Moves the maps out instead of cloning — they are large, and the scheduler never reads
    /// them again.
    fn take_initial_placement(&mut self) -> (metrics::Placement, ChunkSizeIndex);
    /// A returned `Err` is fatal (aborts the run); an infeasible-but-expected outcome is
    /// `Ok(StepOutcome::Failed)`.
    fn step(&mut self, ctx: StepContext) -> anyhow::Result<StepOutcome>;
}

pub struct Simulation {
    params: SimulationParams,
    datasets_config: DatasetsConfig,
    scheduling_config: SchedulingConfig,
    new_worker_version: Version,
    scheduler: Box<dyn StepScheduler>,

    datasets: Vec<DatasetInfo>,
    workers: Vec<Worker>,
    baseline_chunks: Vec<Chunk>,
    accumulated_new_chunks: Vec<Chunk>,
    /// `None` when `restricted_fraction <= 0.0`.
    restricted_dataset: Option<DatasetInfo>,
    restricted_active: bool,
    previous: metrics::Placement,
    /// Sizes of every chunk placed so far. Accumulated across the run: a chunk a correction tombstones
    /// is gone from the current placement, yet its bytes were freed off the workers and must be scored.
    chunk_sizes: ChunkSizeIndex,
    version_state: WorkerVersionState,
    rng: StdRng,
}

impl Simulation {
    pub fn new(
        baseline: Baseline,
        mut datasets_config: DatasetsConfig,
        scheduling_config: SchedulingConfig,
        params: SimulationParams,
        mut scheduler: Box<dyn StepScheduler>,
        mut rng: StdRng,
    ) -> Self {
        let new_worker_version = upgrade::new_version();
        let version_state = WorkerVersionState::new(
            baseline.workers.len(),
            params.initial_new_fraction,
            &mut rng,
        );

        let mut workers = baseline.workers;
        version_state.apply(&mut workers, &new_worker_version);

        // Draws no rng: the restricted data is generated deterministically by head-extension, so
        // enabling it leaves the existing-dataset rng stream unchanged.
        let restricted_dataset = build_restricted_dataset(
            &mut datasets_config,
            &baseline.datasets,
            params.restricted_fraction,
            &params.restricted_dataset_name,
            &new_worker_version,
        );
        let restricted_active = restricted_dataset.is_some();

        let (previous, chunk_sizes) = scheduler.take_initial_placement();

        Self {
            params,
            datasets_config,
            scheduling_config,
            new_worker_version,
            scheduler,
            datasets: baseline.datasets,
            workers,
            baseline_chunks: baseline.chunks,
            accumulated_new_chunks: Vec::new(),
            restricted_dataset,
            restricted_active,
            previous,
            chunk_sizes,
            version_state,
            rng,
        }
    }

    pub fn workers(&self) -> &[Worker] {
        &self.workers
    }

    /// Runs every step, invoking `on_step` after each one. Stops early if a step fails to schedule.
    pub fn run(&mut self, mut on_step: impl FnMut(&[ReshuffleMetrics])) -> Vec<ReshuffleMetrics> {
        let mut all_metrics = Vec::new();
        for step in 1..=self.params.steps {
            let metrics = self.run_step(step);
            let stop = !metrics.scheduled;
            all_metrics.push(metrics);
            on_step(&all_metrics);
            if stop {
                break;
            }
        }
        all_metrics
    }

    pub fn summary(&self) -> Summary {
        Summary {
            total_chunks_added: self.accumulated_new_chunks.len(),
            restricted_chunks: self.restricted_chunk_ids().len(),
            upgraded_workers: self.version_state.eligible_count(),
            total_workers: self.workers.len(),
            new_worker_version: self.new_worker_version.clone(),
        }
    }

    fn is_restricted(&self, chunk: &Chunk) -> bool {
        self.restricted_active
            && self
                .restricted_dataset
                .as_ref()
                .is_some_and(|d| bucket_of(&chunk.dataset) == bucket_of(&d.dataset_id))
    }

    /// Empty once the restriction is lifted.
    fn restricted_chunk_ids(&self) -> HashSet<ChunkId> {
        self.accumulated_new_chunks
            .iter()
            .filter(|c| self.is_restricted(c))
            .map(|c| (c.dataset.clone(), c.id.clone()))
            .collect()
    }

    /// Stateless only: it holds all chunks in memory, so cloning them in is natural. Multistep clones
    /// server-side from Postgres instead.
    fn copy_chunks_due(&self, step: u32) -> Vec<Chunk> {
        let mut copied = Vec::new();
        for copy in self.params.copy.iter().filter(|c| c.at_step == step) {
            let dst_id = Arc::new(dataset_id(&copy.dst));
            copied.extend(
                self.baseline_chunks
                    .iter()
                    .chain(&self.accumulated_new_chunks)
                    .filter(|c| bucket_of(&c.dataset) == copy.src)
                    .map(|c| Chunk {
                        dataset: dst_id.clone(),
                        ..c.clone()
                    }),
            );
        }
        copied
    }

    fn run_step(&mut self, step: u32) -> ReshuffleMetrics {
        self.lift_restrictions_if_due(step);
        let ingested = self.ingest_chunks(step);

        self.version_state
            .advance(&self.params.upgrade_schedule, step);
        self.version_state
            .apply(&mut self.workers, &self.new_worker_version);

        let stats = StepStats {
            step,
            new_chunks: ingested.count,
            new_restricted: ingested.restricted,
            eligible_workers: self.version_state.eligible_count(),
        };
        let total_capacity = self.total_capacity_bytes();
        let restricted = self.restricted_chunk_ids();

        let ctx = StepContext {
            baseline_chunks: &self.baseline_chunks,
            accumulated_new_chunks: &self.accumulated_new_chunks,
            new_chunks_this_step: &self.accumulated_new_chunks[ingested.start..],
            workers: &self.workers,
            datasets_config: &self.datasets_config,
            scheduling_config: &self.scheduling_config,
        };

        let (what, reason) = match self.scheduler.step(ctx) {
            Ok(StepOutcome::Scheduled(placement)) => {
                let (metrics, current) = metrics::measure_reshuffle(
                    &self.previous,
                    &mut self.chunk_sizes,
                    placement,
                    &restricted,
                    stats,
                    total_capacity,
                );
                self.previous = current;
                return metrics;
            }
            Ok(StepOutcome::Failed(reason)) => ("Scheduling failed", reason),
            Err(e) => ("Scheduler error", format!("{e:#}")),
        };
        eprintln!("{what} at step {step}: {reason}. Stopping simulation.");
        metrics::failed_step_metrics(
            stats,
            self.baseline_chunks.len() + self.accumulated_new_chunks.len(),
            restricted.len(),
            total_capacity,
            reason,
        )
    }

    /// Generates this step's new chunks and appends them to the accumulated set, along with any copies
    /// due at `step`.
    fn ingest_chunks(&mut self, step: u32) -> Ingested {
        let total = self.params.chunk_rate.count_at(step, self.params.steps);
        let restricted_count = if self.restricted_active && self.restricted_dataset.is_some() {
            ((self.params.restricted_fraction * total as f64).round() as u32).min(total)
        } else {
            0
        };

        let mut new_chunks = chunks::generate_new_chunks(
            &mut self.datasets,
            total - restricted_count,
            self.params.chunk_size,
            &mut self.rng,
        );
        if let Some(dataset) = self
            .restricted_dataset
            .as_mut()
            .filter(|_| restricted_count > 0)
        {
            new_chunks.extend(chunks::generate_for_dataset(
                dataset,
                restricted_count,
                self.params.chunk_size,
            ));
        }

        let start = self.accumulated_new_chunks.len();
        self.accumulated_new_chunks.extend(new_chunks);
        // Clone after appending this step's ingest, so a copy reflects src as of this step.
        let copies = self.copy_chunks_due(step);
        self.accumulated_new_chunks.extend(copies);

        Ingested {
            count: (self.accumulated_new_chunks.len() - start) as u32,
            restricted: self.accumulated_new_chunks[start..]
                .iter()
                .filter(|chunk| self.is_restricted(chunk))
                .count() as u32,
            start,
        }
    }

    /// Clearing `minimum_worker_version` unrestricts the dataset's existing chunks too; none are
    /// removed.
    fn lift_restrictions_if_due(&mut self, step: u32) {
        if self.params.lift_restriction_at_step != Some(step) {
            return;
        }
        self.restricted_active = false;
        if let Some(dataset) = &self.restricted_dataset {
            let bucket = bucket_of(&dataset.dataset_id);
            if let Some(segments) = self.datasets_config.get_mut(bucket) {
                for seg in segments.iter_mut() {
                    seg.minimum_worker_version = None;
                }
            }
        }
    }

    fn total_capacity_bytes(&self) -> u64 {
        self.workers.len() as u64 * self.scheduling_config.worker_capacity
    }
}

/// Registers the copy's destination dataset, inheriting the source's weight so both schedulers weight
/// it the same.
pub fn register_copy_dataset(
    plan: &CopyPlan,
    baseline: &Baseline,
    datasets: &mut DatasetsConfig,
) -> anyhow::Result<()> {
    let src_id = dataset_id(&plan.src);
    anyhow::ensure!(
        baseline.chunks.iter().any(|c| *c.dataset == src_id),
        "copy source {} not found in the assignment",
        plan.src
    );
    anyhow::ensure!(
        !datasets.contains_key(&plan.dst),
        "copy destination {} already exists",
        plan.dst
    );
    let segments = datasets.get(&plan.src).cloned().unwrap_or_else(|| {
        vec![DatasetSegmentConfig {
            from: 0,
            weight: 1,
            minimum_worker_version: None,
        }]
    });
    datasets.insert(plan.dst.clone(), segments);
    Ok(())
}

/// Fallbacks for the synthetic restricted dataset when there are no baseline datasets to average over.
const DEFAULT_AVG_BLOCK_SPAN: u64 = 1000;
const DEFAULT_AVG_CHUNK_SIZE: u32 = 256 * 1024 * 1024;

/// The synthetic restricted dataset (bucket `name`, requiring `new_version`), shaped like the mean of
/// `datasets`. Returns `None` and touches nothing when `fraction <= 0.0`.
fn build_restricted_dataset(
    config: &mut DatasetsConfig,
    datasets: &[DatasetInfo],
    fraction: f64,
    name: &str,
    new_version: &Version,
) -> Option<DatasetInfo> {
    if fraction <= 0.0 {
        return None;
    }

    let bucket = bucket_of(name).to_string();
    let avg_block_span = if datasets.is_empty() {
        DEFAULT_AVG_BLOCK_SPAN
    } else {
        datasets.iter().map(|d| d.avg_block_span).sum::<u64>() / datasets.len() as u64
    };
    let avg_chunk_size = if datasets.is_empty() {
        DEFAULT_AVG_CHUNK_SIZE
    } else {
        (datasets
            .iter()
            .map(|d| d.avg_chunk_size as u64)
            .sum::<u64>()
            / datasets.len() as u64) as u32
    };

    config.insert(
        bucket.clone(),
        vec![DatasetSegmentConfig {
            from: 0,
            weight: 1,
            minimum_worker_version: Some(new_version.clone()),
        }],
    );

    Some(DatasetInfo {
        dataset_id: Arc::new(dataset_id(&bucket)),
        chunk_count: 0,
        last_block: 0,
        avg_block_span,
        avg_chunk_size,
    })
}
