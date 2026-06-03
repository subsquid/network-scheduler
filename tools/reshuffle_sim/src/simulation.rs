//! Drives the simulation: advances chunk ingestion and worker upgrades step by
//! step, schedules each step, and collects reshuffle metrics.

use std::collections::HashSet;

use network_scheduler::{
    cli::DatasetsConfig,
    scheduling::SchedulingConfig,
    types::{Chunk, Worker},
};
use rand::rngs::StdRng;
use semver::Version;

use crate::baseline::{Baseline, DatasetInfo};
use crate::metrics::{ReshuffleMetrics, StepStats};
use crate::rate::ChunkRate;
use crate::upgrade::{self, UpgradeSchedule, WorkerVersionState};
use crate::{ChunkId, ChunkOwners, chunks, metrics, scheduler};

/// CLI-derived knobs controlling a run.
pub struct SimulationParams {
    pub steps: u32,
    pub chunk_rate: ChunkRate,
    pub chunk_size: Option<u32>,
    pub restricted_fraction: f64,
    pub initial_new_fraction: f64,
    pub upgrade_schedule: UpgradeSchedule,
    pub lift_restriction_at_step: Option<u32>,
}

/// End-of-run totals.
pub struct Summary {
    pub total_chunks_added: usize,
    pub restricted_chunks: usize,
    pub upgraded_workers: usize,
    pub total_workers: usize,
    pub new_worker_version: Version,
}

pub struct Simulation {
    params: SimulationParams,
    datasets_config: DatasetsConfig,
    scheduling_config: SchedulingConfig,
    new_worker_version: Version,

    datasets: Vec<DatasetInfo>,
    workers: Vec<Worker>,
    baseline_chunks: Vec<Chunk>,
    accumulated_new_chunks: Vec<Chunk>,
    restricted_ids: HashSet<ChunkId>,
    previous_owners: ChunkOwners,
    version_state: WorkerVersionState,
    rng: StdRng,
}

impl Simulation {
    pub fn new(
        baseline: Baseline,
        datasets_config: DatasetsConfig,
        scheduling_config: SchedulingConfig,
        params: SimulationParams,
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

        Self {
            params,
            datasets_config,
            scheduling_config,
            new_worker_version,
            datasets: baseline.datasets,
            workers,
            baseline_chunks: baseline.chunks,
            accumulated_new_chunks: Vec::new(),
            restricted_ids: HashSet::new(),
            previous_owners: baseline.chunk_owners,
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
            restricted_chunks: self.restricted_ids.len(),
            upgraded_workers: self.version_state.eligible_count(),
            total_workers: self.workers.len(),
            new_worker_version: self.new_worker_version.clone(),
        }
    }

    fn run_step(&mut self, step: u32) -> ReshuffleMetrics {
        self.lift_restrictions_if_due(step);

        let restricted_fraction = self.restricted_fraction(step);
        let chunks_to_generate = self.params.chunk_rate.count_at(step, self.params.steps);
        let (new_chunks, restricted) = chunks::generate_new_chunks(
            &mut self.datasets,
            chunks_to_generate,
            restricted_fraction,
            self.params.chunk_size,
            &mut self.rng,
        );
        let new_chunks_count = new_chunks.len() as u32;
        let new_restricted_count = restricted.len() as u32;
        self.accumulated_new_chunks.extend(new_chunks);
        self.restricted_ids.extend(restricted);

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

        let (chunks, schedule_result) = scheduler::schedule_combined_chunks(
            &self.baseline_chunks,
            &self.accumulated_new_chunks,
            &self.workers,
            &self.datasets_config,
            &self.scheduling_config,
            &self.restricted_ids,
            &self.new_worker_version,
        );

        let total_capacity = self.total_capacity_bytes();
        match schedule_result {
            Ok(assignment) => {
                let (metrics, owners) = metrics::measure_reshuffle(
                    &self.previous_owners,
                    &chunks,
                    &assignment,
                    &self.restricted_ids,
                    stats,
                    total_capacity,
                );
                self.previous_owners = owners;
                metrics
            }
            Err(reason) => {
                eprintln!("Scheduling failed at step {step}: {reason}. Stopping simulation.");
                metrics::failed_step_metrics(
                    stats,
                    chunks.len(),
                    self.restricted_ids.len(),
                    total_capacity,
                    reason,
                )
            }
        }
    }

    /// At the lift step, remove the previously-restricted chunks; afterwards no
    /// new chunks are tagged (see [`Self::restricted_fraction`]).
    fn lift_restrictions_if_due(&mut self, step: u32) {
        if self.params.lift_restriction_at_step != Some(step) {
            return;
        }
        let restricted = &self.restricted_ids;
        self.accumulated_new_chunks
            .retain(|c| !restricted.contains(&(c.dataset.clone(), c.id.clone())));
        self.restricted_ids.clear();
    }

    fn restricted_fraction(&self, step: u32) -> f64 {
        let lifted = self
            .params
            .lift_restriction_at_step
            .is_some_and(|n| step >= n);
        if lifted {
            0.0
        } else {
            self.params.restricted_fraction
        }
    }

    fn total_capacity_bytes(&self) -> u64 {
        self.workers.len() as u64 * self.scheduling_config.worker_capacity
    }
}
