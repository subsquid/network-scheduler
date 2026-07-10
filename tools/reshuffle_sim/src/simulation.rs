//! Drives the simulation: advances chunk ingestion and worker upgrades step by
//! step, schedules each step through a pluggable [`StepScheduler`], and collects
//! reshuffle metrics.
//!
//! The scheduler is the only part that differs between the **stateless** path
//! (rebuilds the whole placement each step) and the **multistep** path (reuses
//! the stored placement, so only new data moves).
//!
//! Version restrictions are config-driven: a dedicated synthetic restricted
//! dataset gets `minimum_worker_version` set on its config segment, so every
//! chunk of it requires the new version. Both schedulers derive that from the
//! config, so both honor it identically.

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

use crate::baseline::{Baseline, DatasetInfo};
use crate::metrics::{ReshuffleMetrics, StepStats};
use crate::rate::ChunkRate;
use crate::upgrade::{self, UpgradeSchedule, WorkerVersionState};
use crate::{ChunkId, ChunkOwners, ChunkSizeIndex, chunks, metrics};

/// At step `at_step`, clone every current chunk of dataset `src` (bucket) into a new dataset `dst`
/// (identical ids, ranges and sizes).
pub struct CopyPlan {
    pub src: String,
    pub dst: String,
    pub at_step: u32,
}

/// CLI-derived knobs controlling a run.
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

/// End-of-run totals.
pub struct Summary {
    pub total_chunks_added: usize,
    pub restricted_chunks: usize,
    pub upgraded_workers: usize,
    pub total_workers: usize,
    pub new_worker_version: Version,
}

/// One step's resulting placement — everything needed to diff against the
/// previous step and fill the metrics row.
pub struct StepPlacement {
    pub owners: ChunkOwners,
    pub chunk_sizes: ChunkSizeIndex,
    pub replication_by_weight: BTreeMap<u16, u16>,
    pub used_capacity_bytes: u64,
    /// Bytes held by draining stale copies (0 for the stateless path).
    pub stale_capacity_bytes: u64,
    pub total_chunks: usize,
    pub schedule_duration: Duration,
}

/// What the host loop hands a scheduler each step. The stateless path uses the
/// full chunk set; the multistep path uses only the step's new chunks (it
/// persists the rest). Both derive version restrictions from `datasets_config`.
pub struct StepContext<'a> {
    pub baseline_chunks: &'a [Chunk],
    pub accumulated_new_chunks: &'a [Chunk],
    pub new_chunks_this_step: &'a [Chunk],
    pub workers: &'a [Worker],
    pub datasets_config: &'a DatasetsConfig,
    pub scheduling_config: &'a SchedulingConfig,
}

/// Outcome of scheduling one step.
pub enum StepOutcome {
    Scheduled(StepPlacement),
    /// Scheduling was infeasible this step (e.g. a capacity shortage). The run
    /// records a failed step and stops.
    Failed(String),
}

/// A scheduler the shared loop drives one step at a time — the only part that
/// differs between the stateless and multistep paths.
pub trait StepScheduler {
    /// Placement that step 1 is diffed against: the input assignment for the
    /// stateless path, the seeded baseline placement for multistep. Moves the map
    /// out — the scheduler needs it only to seed step 1's reference, so consuming
    /// it avoids cloning (and then permanently retaining) the whole 6M-entry map.
    fn take_initial_owners(&mut self) -> ChunkOwners;
    /// Schedule this step. A returned `Err` is fatal (aborts the run); an
    /// infeasible-but-expected outcome is `Ok(StepOutcome::Failed)`.
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
    /// The dedicated synthetic dataset that requires the new worker version.
    /// `None` when `restricted_fraction <= 0.0`.
    restricted_dataset: Option<DatasetInfo>,
    /// True while the restriction is in force; set false at the lift step.
    restricted_active: bool,
    previous_owners: ChunkOwners,
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

        // Build a dedicated synthetic restricted dataset (its config segment carries the new
        // version requirement) when a fraction is set. Draws NO rng — the restricted data is
        // generated deterministically by head-extension, so a fraction-0 (and even a fraction>0)
        // run keeps the existing-dataset rng stream unchanged.
        let restricted_dataset = build_restricted_dataset(
            &mut datasets_config,
            &baseline.datasets,
            params.restricted_fraction,
            &params.restricted_dataset_name,
            &new_worker_version,
        );
        let restricted_active = restricted_dataset.is_some();

        // The scheduler defines step 1's reference placement. Moved out, not cloned:
        // the baseline owners map is ~5 GB at 6M chunks, and the scheduler never reads
        // it again, so cloning would both spike and permanently double that footprint.
        let previous_owners = scheduler.take_initial_owners();

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
            previous_owners,
            version_state,
            rng,
        }
    }

    /// The worker fleet, reflecting the current version distribution.
    pub fn workers(&self) -> &[Worker] {
        &self.workers
    }

    /// Runs every step, invoking `on_step` with the metrics collected so far
    /// after each one. Stops early if a step fails to schedule. Returns the
    /// full metrics series.
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

    /// Whether `chunk` belongs to the (still-restricted) dedicated dataset.
    fn is_restricted(&self, chunk: &Chunk) -> bool {
        self.restricted_active
            && self
                .restricted_dataset
                .as_ref()
                .is_some_and(|d| bucket_of(&chunk.dataset) == bucket_of(&d.dataset_id))
    }

    /// Ids of accumulated chunks in restricted datasets. Empty once lifted.
    fn restricted_chunk_ids(&self) -> HashSet<ChunkId> {
        self.accumulated_new_chunks
            .iter()
            .filter(|c| self.is_restricted(c))
            .map(|c| (c.dataset.clone(), c.id.clone()))
            .collect()
    }

    /// Clones every chunk of each `src` (baseline + accumulated) into its `dst` for the copies due at
    /// `step`. Stateless only: it holds all chunks in memory, so cloning + injecting as new chunks is
    /// natural. Multistep clones from its Postgres assignment instead, so copy stays per-scheduler.
    fn copy_chunks_due(&self, step: u32) -> Vec<Chunk> {
        let mut copied = Vec::new();
        for copy in self.params.copy.iter().filter(|c| c.at_step == step) {
            let dst_id = Arc::new(format!("s3://{}", copy.dst));
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

        let total = self.params.chunk_rate.count_at(step, self.params.steps);
        // A fixed share of each step's new chunks goes into the dedicated restricted dataset; the
        // rest keep the existing size-weighted distribution among real datasets.
        let restricted_n = if self.restricted_active && self.restricted_dataset.is_some() {
            ((self.params.restricted_fraction * total as f64).round() as u32).min(total)
        } else {
            0
        };
        let normal_n = total - restricted_n;

        let mut new_chunks = chunks::generate_new_chunks(
            &mut self.datasets,
            normal_n,
            self.params.chunk_size,
            &mut self.rng,
        );
        if restricted_n > 0 {
            let dataset = self.restricted_dataset.as_mut().unwrap();
            new_chunks.extend(chunks::generate_for_dataset(
                dataset,
                restricted_n,
                self.params.chunk_size,
            ));
        }
        let start = self.accumulated_new_chunks.len();
        self.accumulated_new_chunks.extend(new_chunks);
        // Clone after appending this step's ingest, so a copy reflects src as of this step.
        let copies = self.copy_chunks_due(step);
        self.accumulated_new_chunks.extend(copies);
        let new_chunks_count = (self.accumulated_new_chunks.len() - start) as u32;
        let new_restricted_count = self.accumulated_new_chunks[start..]
            .iter()
            .filter(|c| self.is_restricted(c))
            .count() as u32;

        self.version_state
            .advance(&self.params.upgrade_schedule, step);
        self.version_state
            .apply(&mut self.workers, &self.new_worker_version);

        let stats = StepStats {
            step,
            new_chunks: new_chunks_count,
            new_restricted: new_restricted_count,
            eligible_workers: self.version_state.eligible_count(),
        };
        let total_capacity = self.total_capacity_bytes();
        let restricted = self.restricted_chunk_ids();

        let ctx = StepContext {
            baseline_chunks: &self.baseline_chunks,
            accumulated_new_chunks: &self.accumulated_new_chunks,
            new_chunks_this_step: &self.accumulated_new_chunks[start..],
            workers: &self.workers,
            datasets_config: &self.datasets_config,
            scheduling_config: &self.scheduling_config,
        };

        match self.scheduler.step(ctx) {
            Ok(StepOutcome::Scheduled(placement)) => {
                let (metrics, owners) = metrics::measure_reshuffle(
                    &self.previous_owners,
                    placement,
                    &restricted,
                    stats,
                    total_capacity,
                );
                self.previous_owners = owners;
                metrics
            }
            Ok(StepOutcome::Failed(reason)) => {
                eprintln!("Scheduling failed at step {step}: {reason}. Stopping simulation.");
                metrics::failed_step_metrics(
                    stats,
                    self.baseline_chunks.len() + self.accumulated_new_chunks.len(),
                    restricted.len(),
                    total_capacity,
                    reason,
                )
            }
            Err(e) => {
                let reason = format!("{e:#}");
                eprintln!("Scheduler error at step {step}: {reason}. Stopping simulation.");
                metrics::failed_step_metrics(
                    stats,
                    self.baseline_chunks.len() + self.accumulated_new_chunks.len(),
                    restricted.len(),
                    total_capacity,
                    reason,
                )
            }
        }
    }

    /// At the lift step, drop the restriction: deactivate it and clear the
    /// `minimum_worker_version` on the restricted dataset's config segment, so
    /// its existing chunks become unrestricted and no new restricted chunks are
    /// generated. Both schedulers then treat that data as unrestricted; no
    /// chunks are removed.
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

/// Config key for a chunk's dataset id: the bucket without the `s3://` prefix.
fn bucket_of(dataset_id: &str) -> &str {
    dataset_id.strip_prefix("s3://").unwrap_or(dataset_id)
}

/// Default per-chunk values for the synthetic restricted dataset when there are
/// no baseline datasets to average over.
const DEFAULT_AVG_BLOCK_SPAN: u64 = 1000;
const DEFAULT_AVG_CHUNK_SIZE: u32 = 256 * 1024 * 1024;

/// Builds the dedicated synthetic restricted dataset (bucket `name`, requiring
/// `new_version`) and registers its config segment. Its per-chunk shape is the
/// mean over `datasets`, falling back to sane defaults when there are none.
///
/// Draws no RNG; returns `None` (and touches nothing) when `fraction <= 0.0`.
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
        dataset_id: Arc::new(format!("s3://{bucket}")),
        chunk_count: 0,
        last_block: 0,
        avg_block_span,
        avg_chunk_size,
    })
}
