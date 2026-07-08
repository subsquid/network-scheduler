//! Multistep scheduler driven through the Postgres-backed [`SchedulerStorage`]. Unlike the stateless
//! path (which rebuilds the placement each step), it reuses the stored placement, so a worker keeps
//! the copies it already has and only new data moves. Each step adds the step's chunks and runs one
//! scheduling cycle; the shared loop in [`crate::simulation`] does the diffing and reporting.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

use anyhow::{Context, anyhow};
use libp2p_identity::PeerId;
use network_scheduler::{
    cli::Config,
    multistep_scheduler::SchedulingConfig as MultistepConfig,
    scheduler_storage::{
        SchedulerStorage, StorageError, WorkerAssignment, algorithm::MultistepAlgorithm,
        postgres::PostgresStorage,
    },
};
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;

use crate::{
    ChunkOwners, ChunkSizeIndex,
    baseline::Baseline,
    simulation::{StepContext, StepOutcome, StepPlacement, StepScheduler},
};

/// Result of one scheduling cycle: either a published assignment, or a capacity
/// shortage (the run records a failed step and stops).
enum CycleResult {
    Scheduled(WorkerAssignment, Duration),
    Shortage(String),
}

/// Removal delay in logical ticks (matches the storage tests' `M_TICKS`): a clock jump this large
/// finishes removing every copy that is on its way out.
const M_TICKS: u64 = 5;
/// Per-cycle clock step, kept below `M_TICKS` so a cycle's removed copies start their delay.
const CLOCK_STEP: u64 = 1;
const GC_TICKS: u64 = 0;

struct Backend {
    storage: PostgresStorage,
    container: Option<ContainerAsync<Postgres>>,
    rt: Option<tokio::runtime::Runtime>,
}

impl Drop for Backend {
    fn drop(&mut self) {
        // `ContainerAsync`'s async drop panics without a running reactor, so reap it explicitly
        // inside our runtime instead.
        if let (Some(rt), Some(container)) = (self.rt.take(), self.container.take()) {
            let _ = rt.block_on(container.rm());
        }
    }
}

/// Connect to an existing, user-supplied database and migrate it.
fn existing_database(url: &str) -> anyhow::Result<Backend> {
    tracing::info!("Connecting to Postgres at {url}");
    Ok(Backend {
        storage: connect_migrated(url)?,
        container: None,
        rt: None,
    })
}

/// Start an ephemeral Postgres container, then connect and migrate it.
fn ephemeral_database() -> anyhow::Result<Backend> {
    tracing::info!("Starting ephemeral Postgres container (pass --database-url to use your own)");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("build container runtime")?;
    let (container, url) = rt.block_on(async {
        use testcontainers::ImageExt;
        use testcontainers::runners::AsyncRunner;
        let container = Postgres::default()
            .with_tag("18.4-alpine")
            // Parallel-query workers put their dynamic-shared-memory segments in /dev/shm, which
            // Docker caps at 64 MiB by default. Set-based statements over the full placement blow
            // past that, so give the container 1 GiB. Bump if you scale the dataset further.
            .with_shm_size(1024 * 1024 * 1024)
            // Throwaway cluster — skip durability for speed.
            .with_cmd([
                "postgres",
                "-c",
                "fsync=off",
                "-c",
                "synchronous_commit=off",
            ])
            .start()
            .await
            .context("start postgres container")?;
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .context("postgres host port")?;
        let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
        anyhow::Ok((container, url))
    })?;

    Ok(Backend {
        // `connect`/`migrate` `block_on` their own runtime, so they must run outside `rt.block_on`.
        storage: connect_migrated(&url)?,
        container: Some(container),
        rt: Some(rt),
    })
}

fn connect_migrated(url: &str) -> anyhow::Result<PostgresStorage> {
    let mut storage = PostgresStorage::connect(url)?;
    storage.migrate()?;
    Ok(storage)
}

pub struct MultistepScheduler {
    backend: Backend,
    algo: MultistepAlgorithm,
    config: MultistepConfig,
    clock: u64,
    total_chunks: usize,
    initial_owners: ChunkOwners,
}

impl MultistepScheduler {
    /// Provision Postgres, seed it from `baseline`, and schedule the baseline once as step 1's
    /// reference. Takes `baseline`'s chunks (its `datasets` are left for the chunk generator).
    pub fn build(
        config: &Config,
        baseline: &mut Baseline,
        database_url: Option<&str>,
        restricted_dataset: Option<String>,
    ) -> anyhow::Result<Self> {
        let mut backend = match database_url {
            Some(url) => existing_database(url)?,
            None => ephemeral_database()?,
        };
        let algo = MultistepAlgorithm::new(Box::new(config.datasets.clone()));
        // The multistep scheduler is only tested with reliability ignored, so force it on here
        // whatever the config says.
        let scheduling_config = MultistepConfig {
            worker_capacity: config.worker_storage_bytes,
            saturation: config.saturation,
            min_replication: config.min_replication,
            ignore_reliability: true,
        };

        // Seed: datasets must exist before their chunks; workers before the first cycle. The
        // simulation's synthetic restricted dataset has no baseline chunks, so register it here
        // too (it starts empty and receives chunks from step 1 on).
        let mut dataset_names: BTreeSet<String> = baseline
            .chunks
            .iter()
            .map(|c| (*c.dataset).clone())
            .collect();
        dataset_names.extend(restricted_dataset);
        let dataset_count = dataset_names.len();
        backend
            .storage
            .insert_new_datasets(dataset_names.into_iter().collect())?;
        backend
            .storage
            .update_worker_set(&baseline.workers, 0, GC_TICKS)?;
        tracing::info!(
            "Seeded {dataset_count} datasets and {} workers",
            baseline.workers.len()
        );

        let total_chunks = baseline.chunks.len();

        tracing::info!("Inserting {total_chunks} baseline chunks...");
        backend
            .storage
            .insert_new_chunks(std::mem::take(&mut baseline.chunks))?;
        let registered = backend.storage.register_new_chunks()?;
        tracing::info!(
            "Registered {} baseline chunks ({} rejected as overlapping)",
            registered.len(),
            total_chunks - registered.len()
        );

        let mut scheduler = Self {
            backend,
            algo,
            config: scheduling_config,
            clock: 0,
            total_chunks,
            initial_owners: ChunkOwners::new(),
        };

        tracing::info!("Scheduling baseline placement...");
        let wa = match scheduler
            .run_cycle()
            .context("scheduling baseline placement")?
        {
            CycleResult::Scheduled(wa, _) => wa,
            CycleResult::Shortage(reason) => {
                anyhow::bail!("baseline placement infeasible: {reason}")
            }
        };
        scheduler.initial_owners = owners_from_assignment(&wa).0;
        tracing::info!(
            "Baseline placed {} chunks; replication by weight {:?}",
            scheduler.initial_owners.len(),
            wa.replication_by_weight
        );
        Ok(scheduler)
    }

    /// Run one scheduling cycle. Advances the clock by the removal delay (so earlier steps' removed
    /// copies age out), then confirms the assignment fleet-wide and runs visibility, so the copies
    /// it replaces start their removal timer.
    fn run_cycle(&mut self) -> anyhow::Result<CycleResult> {
        let storage = &mut self.backend.storage;
        self.clock += M_TICKS;
        let started = Instant::now();
        let wa = match storage.run_scheduling_cycle(&self.algo, &self.config, self.clock, M_TICKS) {
            Ok(wa) => wa,
            Err(StorageError::Shortage) => {
                return Ok(CycleResult::Shortage(
                    "scheduling shortage: worker capacity cannot satisfy all replication floors \
                     (lower min_replication, raise worker_storage_bytes, or add workers)"
                        .to_string(),
                ));
            }
            Err(e) => return Err(anyhow!("scheduling cycle failed: {e}")),
        };
        let schedule_duration = started.elapsed();
        self.clock += CLOCK_STEP;
        storage.confirm_worker_assignment(wa.id, self.clock)?;
        storage.run_visibility_cycle(self.clock)?;
        Ok(CycleResult::Scheduled(wa, schedule_duration))
    }
}

impl StepScheduler for MultistepScheduler {
    fn initial_owners(&self) -> ChunkOwners {
        self.initial_owners.clone()
    }

    fn step(&mut self, ctx: StepContext) -> anyhow::Result<StepOutcome> {
        // Rebuild the weight/version strategy from the (possibly mutated) config each step so
        // dataset-level restrictions and their lifting flow through `ctx`, matching the stateless
        // path. Cheap: it just boxes a clone of the config.
        self.algo = MultistepAlgorithm::new(Box::new(ctx.datasets_config.clone()));
        // Refresh worker versions in place so the modeled rollout grows eligibility over time.
        // With an unchanged worker set this is inert (no departures/churn).
        self.backend
            .storage
            .update_worker_set(ctx.workers, self.clock, GC_TICKS)?;

        let new_chunks = ctx.new_chunks_this_step.to_vec();
        self.total_chunks += new_chunks.len();
        self.backend.storage.insert_new_chunks(new_chunks)?;
        self.backend.storage.register_new_chunks()?;
        let (wa, schedule_duration) = match self.run_cycle()? {
            CycleResult::Scheduled(wa, d) => (wa, d),
            CycleResult::Shortage(reason) => return Ok(StepOutcome::Failed(reason)),
        };

        let (owners, chunk_sizes, used_capacity_bytes) = owners_from_assignment(&wa);
        Ok(StepOutcome::Scheduled(StepPlacement {
            owners,
            chunk_sizes,
            // The scheduler's chosen factors (ideal); excludes the draining copies in `owners`.
            replication_by_weight: wa.replication_by_weight.clone(),
            used_capacity_bytes,
            total_chunks: self.total_chunks,
            schedule_duration,
        }))
    }
}

/// From a published assignment, build the `(dataset, chunk_id) → holding workers` map we compare
/// between steps, plus per-chunk sizes and the total bytes held.
fn owners_from_assignment(wa: &WorkerAssignment) -> (ChunkOwners, ChunkSizeIndex, u64) {
    let mut owners: ChunkOwners = BTreeMap::new();
    let mut sizes: ChunkSizeIndex = BTreeMap::new();
    let mut used_bytes = 0u64;

    for (chunk_pk, worker_pks) in &wa.chunk_workers {
        let Some(chunk) = wa.chunks.get(chunk_pk) else {
            continue;
        };
        let key = (chunk.dataset.clone(), chunk.id.clone());
        sizes.insert(key.clone(), chunk.size);
        used_bytes += chunk.size as u64 * worker_pks.len() as u64;
        let holders: BTreeSet<PeerId> = worker_pks
            .iter()
            .filter_map(|w| wa.workers.get(w).map(|aw| aw.peer_id))
            .collect();
        owners.insert(key, holders);
    }

    (owners, sizes, used_bytes)
}
