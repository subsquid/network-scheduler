//! Multistep scheduler driven through the Postgres-backed [`SchedulerStorage`]. Unlike the stateless
//! path (which rebuilds the placement each step), it reuses the stored placement, so a worker keeps
//! the copies it already has and only new data moves. Each step adds the step's chunks and runs one
//! scheduling cycle; the shared loop in [`crate::simulation`] does the diffing and reporting.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, anyhow};
use network_scheduler::{
    cli::{Config, DatasetsConfig},
    multistep_scheduler::SchedulingConfig as MultistepConfig,
    scheduler_storage::{
        AssignmentId, ChunkPk, NewChunk, SchedulerStorage, StorageError, WorkerAssignment,
        WorkerAssignmentChunk, WorkerPk,
        algorithm::MultistepAlgorithm,
        postgres::PostgresStorage,
        test_harness::pg_harness::{self, PgData},
    },
    types::{Chunk, DatasetSchema},
};

use crate::baseline::Baseline;
use crate::metrics::Placement;
use crate::simulation::{CopyPlan, StepContext, StepOutcome, StepPlacement, StepScheduler};
use crate::types::{ChunkOwners, ChunkSizeIndex, WorkerIndex, dataset_id};

enum CycleResult {
    Scheduled {
        assignment: WorkerAssignment,
        /// Draining copies in the published placement.
        stale: Vec<(ChunkPk, WorkerPk)>,
        /// Chunks the portal routes after this cycle's visibility pass. Trails the placement by the
        /// confirm and portal lags.
        portal_chunks: usize,
        schedule_duration: Duration,
    },
    /// A capacity shortage: the run records a failed step and stops.
    Shortage(String),
}

/// Removal delay in logical ticks (matches the storage tests' `M_TICKS`): a clock jump this large
/// finishes removing every copy that is on its way out.
const M_TICKS: u64 = 5;
/// Per-cycle clock step, kept below `M_TICKS` so a cycle's removed copies start their delay.
const CLOCK_STEP: u64 = 1;
const GC_TICKS: u64 = 0;
/// Suffix distinguishing a replacement chunk's id from the one it supersedes.
const REPLACE_SUFFIX: &str = "-replaced";

/// At step `at_step`, replace every chunk of `bucket` with a same-range copy (new id).
pub struct ReplacePlan {
    pub bucket: String,
    pub at_step: u32,
}

/// When ephemeral, the container is the shared `pg_harness` one (reaped at process exit), so there's
/// nothing to hold or drop here.
struct Backend {
    storage: PostgresStorage,
}

fn existing_database(url: &str) -> anyhow::Result<Backend> {
    tracing::info!("Connecting to Postgres at {url}");
    Ok(Backend {
        storage: connect_migrated(url)?,
    })
}

/// Disk-backed because the mainnet DB overflows the harness tmpfs.
fn ephemeral_database() -> Backend {
    tracing::info!("Starting ephemeral Postgres container (pass --database-url to use your own)");
    Backend {
        storage: pg_harness::fresh_db_with(PgData::Disk, "reshuffle_sim", 0),
    }
}

fn connect_migrated(url: &str) -> anyhow::Result<PostgresStorage> {
    let mut storage = PostgresStorage::connect(url)?;
    storage.migrate()?;
    Ok(storage)
}

/// What [`MultistepScheduler::build`] does with the baseline data before scheduling.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Ingestion {
    /// Seed, then continue to the baseline placement.
    Run,
    /// Seed nothing — the data is already in the database (e.g. a restored backup).
    Skip,
    /// Seed, then stop before scheduling (snapshot the database here).
    Only,
}

pub struct MultistepScheduler {
    backend: Backend,
    algo: MultistepAlgorithm<DatasetsConfig>,
    config: MultistepConfig,
    clock: u64,
    /// Every chunk inserted so far, dead rows included.
    ingested_chunks: usize,
    initial_placement: Placement,
    initial_sizes: ChunkSizeIndex,
    replace: Vec<ReplacePlan>,
    copy: Vec<CopyPlan>,
    /// Cycles a published assignment waits before the fleet has downloaded it (uniform latency).
    confirm_lag_steps: u32,
    /// Extra cycles before the portal routes a confirmed assignment, gating when its superseded
    /// copies may drain.
    portal_lag_steps: u32,
    /// Worker-assignment ids in publication order (index 0 = baseline). The confirmation watermark
    /// trails the newest publication by `confirm_lag_steps + portal_lag_steps` entries.
    published_ids: Vec<AssignmentId>,
    current_step: u32,
    /// Placed chunks of datasets still awaiting a replace — a same-cycle replace reads its target's
    /// pre-cycle placement from here. Kept only while a replace lies ahead.
    latest_chunks: BTreeMap<ChunkPk, WorkerAssignmentChunk>,
    /// Shared by the baseline seed and every step, so indices stay stable across the run. (This path
    /// ignores the baseline's own index table — it derives owners from its storage cycles.)
    worker_index: WorkerIndex,
}

/// Everything [`MultistepScheduler::build`] needs beyond the baseline itself.
pub struct Setup<'a> {
    pub config: &'a Config,
    /// An existing database to use; `None` starts an ephemeral container.
    pub database_url: Option<&'a str>,
    /// The synthetic restricted dataset to register, if the run has one (it has no baseline chunks).
    pub restricted_dataset: Option<String>,
    pub replace: Vec<ReplacePlan>,
    pub copy: Vec<CopyPlan>,
    pub confirm_lag_steps: u32,
    pub portal_lag_steps: u32,
    pub ingestion: Ingestion,
}

impl MultistepScheduler {
    /// `None` under [`Ingestion::Only`], which stops once the database is seeded.
    pub fn build(setup: Setup, baseline: &mut Baseline) -> anyhow::Result<Option<Self>> {
        let Setup {
            config,
            database_url,
            restricted_dataset,
            replace,
            copy,
            confirm_lag_steps,
            portal_lag_steps,
            ingestion,
        } = setup;
        let mut backend = match database_url {
            Some(url) => existing_database(url)?,
            None => ephemeral_database(),
        };

        let ingested_chunks = seed_baseline(&mut backend, baseline, restricted_dataset, ingestion)?;
        if ingestion == Ingestion::Only {
            // Stop before scheduling, so a later `Ingestion::Skip` run restores the snapshot and
            // benchmarks from here.
            log_table_sizes(&backend.storage);
            tracing::info!("Ingest-only: baseline data loaded; stopping before scheduling");
            return Ok(None);
        }

        let mut scheduler = Self {
            backend,
            algo: MultistepAlgorithm::new(config.datasets.clone()),
            config: MultistepConfig {
                worker_capacity: config.worker_storage_bytes,
                saturation: config.saturation,
                min_replication: config.min_replication,
                ignore_reliability: true,
            },
            clock: 0,
            ingested_chunks,
            initial_placement: Placement::default(),
            initial_sizes: ChunkSizeIndex::new(),
            replace,
            copy,
            confirm_lag_steps,
            portal_lag_steps,
            published_ids: Vec::new(),
            current_step: 0,
            latest_chunks: BTreeMap::new(),
            worker_index: WorkerIndex::default(),
        };
        scheduler.place_baseline()?;
        Ok(Some(scheduler))
    }

    /// Runs the first cycle and keeps its placement as the one step 1 is diffed against.
    fn place_baseline(&mut self) -> anyhow::Result<()> {
        tracing::info!("Scheduling baseline placement...");
        let (assignment, stale) = match self.run_cycle().context("scheduling baseline placement")? {
            CycleResult::Scheduled {
                assignment, stale, ..
            } => (assignment, stale),
            CycleResult::Shortage(reason) => {
                anyhow::bail!("baseline placement infeasible: {reason}")
            }
        };

        let snapshot = Snapshot::of(&assignment, &stale, &mut self.worker_index);
        self.initial_sizes = snapshot.sizes;
        self.initial_placement = Placement {
            owners: snapshot.owners,
            stale: snapshot.stale_owners,
        };

        for plan in &self.replace {
            let dataset = dataset_id(&plan.bucket);
            anyhow::ensure!(
                assignment.chunks.values().any(|c| *c.dataset == dataset),
                "replace: dataset {dataset} not found in baseline"
            );
        }
        self.retain_replace_targets(&assignment);
        tracing::info!(
            "Baseline placed {} chunks; replication by weight {:?}",
            self.initial_placement.owners.len(),
            assignment.replication_by_weight
        );
        log_table_sizes(&self.backend.storage);
        Ok(())
    }

    /// Advances the clock by the removal delay (so earlier steps' removed copies age out), then
    /// confirms the fleet up to the lagged watermark and runs visibility — so only assignments the
    /// fleet and portal have caught up to let their superseded copies drain.
    fn run_cycle(&mut self) -> anyhow::Result<CycleResult> {
        self.clock += M_TICKS;
        let started = Instant::now();
        let assignment = match self.backend.storage.run_scheduling_cycle(
            &self.algo,
            &self.config,
            self.clock,
            M_TICKS,
        ) {
            Ok(assignment) => assignment,
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
        // Before confirm/visibility expire or drain stale, so the snapshot matches `assignment`.
        let stale = self.backend.storage.stale_mappings()?;
        self.clock += CLOCK_STEP;
        // Confirming only up to the lagged watermark keeps this cycle's superseded copies occupying
        // capacity until the assignment that replaced them clears both lags. Lags of 0 confirm the
        // just-published assignment.
        self.published_ids.push(assignment.id);
        let watermark = self.confirmation_watermark();
        self.backend
            .storage
            .confirm_worker_assignment(watermark, self.clock)?;
        let portal = self.backend.storage.run_visibility_cycle(self.clock)?;
        Ok(CycleResult::Scheduled {
            assignment,
            stale,
            portal_chunks: portal.chunks.len(),
            schedule_duration,
        })
    }

    /// The publication `confirm_lag_steps + portal_lag_steps` cycles back.
    fn confirmation_watermark(&self) -> AssignmentId {
        let lag = (self.confirm_lag_steps + self.portal_lag_steps) as usize;
        lagged_watermark(&self.published_ids, lag)
    }

    /// Supersede every chunk of `bucket` with a same-range correction, sourced from `latest_chunks`
    /// (a valid target is fully placed, so that set is its live chunks). Returns how many it made.
    fn replace_dataset(&mut self, bucket: &str) -> anyhow::Result<u32> {
        let target = dataset_id(bucket);
        let replacements: Vec<(ChunkPk, NewChunk)> = self
            .latest_chunks
            .iter()
            .filter(|(_, chunk)| *chunk.dataset == target)
            .map(|(pk, chunk)| {
                let new_chunk = NewChunk {
                    dataset: chunk.dataset.clone(),
                    id: Arc::new(format!("{}{REPLACE_SUFFIX}", chunk.id)),
                    size: chunk.size,
                    blocks: chunk.blocks.clone(),
                    schema_id: Some(chunk.schema_id),
                    tables_present: chunk.tables_present.clone(),
                };
                (*pk, new_chunk)
            })
            .collect();
        anyhow::ensure!(
            !replacements.is_empty(),
            "replace-dataset: dataset {bucket} has no chunks to replace"
        );

        let count = replacements.len();
        self.backend
            .storage
            .register_corrections(replacements, self.clock)
            .context("registering replacement corrections")?;
        self.ingested_chunks += count;
        tracing::info!(
            "Replaced {count} chunks in dataset {bucket} at step {}",
            self.current_step
        );
        Ok(count as u32)
    }

    /// Clones server-side, so the chunk payloads never round-trip through the client.
    /// Returns how many clones it made.
    fn copy_dataset(&mut self, plan: &CopyPlan) -> anyhow::Result<u32> {
        let src = dataset_id(&plan.src);
        let dst = dataset_id(&plan.dst);
        // Destination isn't seeded at build (no baseline chunks), so register it before its clones.
        self.backend
            .storage
            .insert_new_datasets(vec![(dst.clone(), DatasetSchema::default())])?;
        let count = self.backend.storage.copy_dataset_chunks(&src, &dst)?;
        anyhow::ensure!(
            count > 0,
            "copy-dataset: source {} has no chunks to copy",
            plan.src
        );
        self.ingested_chunks += count as usize;
        tracing::info!(
            "Copied {count} chunks from {} to {} at step {}",
            plan.src,
            plan.dst,
            self.current_step
        );
        Ok(count as u32)
    }

    /// Keeps the placed chunks of datasets whose replace still lies ahead, so that replace can source
    /// its target's pre-cycle placement.
    fn retain_replace_targets(&mut self, assignment: &WorkerAssignment) {
        let targets: HashSet<String> = self
            .replace
            .iter()
            .filter(|p| p.at_step > self.current_step)
            .map(|p| dataset_id(&p.bucket))
            .collect();
        self.latest_chunks = if targets.is_empty() {
            BTreeMap::new()
        } else {
            assignment
                .chunks
                .iter()
                .filter(|(_, c)| targets.contains(c.dataset.as_str()))
                .map(|(pk, c)| (*pk, c.clone()))
                .collect()
        };
    }
}

impl StepScheduler for MultistepScheduler {
    fn take_initial_placement(&mut self) -> (Placement, ChunkSizeIndex) {
        (
            std::mem::take(&mut self.initial_placement),
            std::mem::take(&mut self.initial_sizes),
        )
    }

    fn step(&mut self, ctx: StepContext) -> anyhow::Result<StepOutcome> {
        self.algo.set_weight_strategy(ctx.datasets_config.clone());
        self.backend
            .storage
            .update_worker_set(ctx.workers, self.clock, GC_TICKS)?;

        // Replace before this step's new chunks, so one `register_new_chunks` covers both.
        self.current_step += 1;
        let due_replaces: Vec<String> = self
            .replace
            .iter()
            .filter(|p| p.at_step == self.current_step)
            .map(|p| p.bucket.clone())
            .collect();
        let mut copied_or_replaced_chunks = 0u32;
        for bucket in &due_replaces {
            copied_or_replaced_chunks += self.replace_dataset(bucket)?;
        }

        // Copy before the cycle too: the clones register alongside this step's new chunks, so one
        // cycle places everything.
        let (due_copies, rest): (Vec<CopyPlan>, Vec<CopyPlan>) = std::mem::take(&mut self.copy)
            .into_iter()
            .partition(|p| p.at_step == self.current_step);
        self.copy = rest;
        for plan in &due_copies {
            copied_or_replaced_chunks += self.copy_dataset(plan)?;
        }

        let new_chunks: Vec<NewChunk> = ctx
            .new_chunks_this_step
            .iter()
            .cloned()
            .map(to_storage_chunk)
            .collect();
        self.ingested_chunks += new_chunks.len();
        self.backend.storage.insert_new_chunks(new_chunks)?;
        self.backend.storage.register_new_chunks()?;
        let (assignment, stale, portal_chunks, schedule_duration) = match self.run_cycle()? {
            CycleResult::Scheduled {
                assignment,
                stale,
                portal_chunks,
                schedule_duration,
            } => (assignment, stale, portal_chunks, schedule_duration),
            CycleResult::Shortage(reason) => return Ok(StepOutcome::Failed(reason)),
        };

        log_table_sizes(&self.backend.storage);

        let snapshot = Snapshot::of(&assignment, &stale, &mut self.worker_index);
        self.retain_replace_targets(&assignment);
        Ok(StepOutcome::Scheduled(StepPlacement {
            owners: snapshot.owners,
            stale_owners: snapshot.stale_owners,
            chunk_sizes: snapshot.sizes,
            replication_by_weight: assignment.replication_by_weight.clone(),
            used_capacity_bytes: snapshot.used_bytes,
            stale_capacity_bytes: snapshot.stale_bytes,
            ingested_chunks: self.ingested_chunks,
            copied_or_replaced_chunks,
            portal_chunks: Some(portal_chunks),
            schedule_duration,
        }))
    }
}

/// The id `lag` publications before the newest, clamped to the baseline (index 0).
fn lagged_watermark(published_ids: &[AssignmentId], lag: usize) -> AssignmentId {
    let latest = published_ids.len() - 1;
    published_ids[latest.saturating_sub(lag)]
}

fn log_table_sizes(storage: &PostgresStorage) {
    if !tracing::enabled!(tracing::Level::DEBUG) {
        return;
    }
    match storage.table_sizes() {
        Err(e) => tracing::debug!("failed to read scheduler table sizes: {e:#}"),
        Ok(mut sizes) => {
            sizes.sort_by_key(|&(table, _)| table);
            let rows: String = sizes
                .iter()
                .map(|(table, count)| format!("\n  {count:>12}  {table}"))
                .collect();
            tracing::debug!("scheduler table sizes:{rows}");
        }
    }
}

/// Seeds datasets, workers and chunks: datasets must exist before their chunks, workers before the
/// first cycle. The synthetic restricted dataset has no baseline chunks, so it is registered here too
/// and fills up from step 1. Returns how many chunks the database now holds.
fn seed_baseline(
    backend: &mut Backend,
    baseline: &mut Baseline,
    restricted_dataset: Option<String>,
    ingestion: Ingestion,
) -> anyhow::Result<usize> {
    let total_chunks = baseline.chunks.len();
    if ingestion == Ingestion::Skip {
        // Postgres owns the chunks now.
        baseline.chunks = Vec::new();
        tracing::info!(
            "Skipping ingestion; reusing {total_chunks} baseline chunks from the database"
        );
        return Ok(total_chunks);
    }

    let mut dataset_names: BTreeSet<String> = baseline
        .chunks
        .iter()
        .map(|chunk| (*chunk.dataset).clone())
        .collect();
    dataset_names.extend(restricted_dataset);
    let dataset_count = dataset_names.len();
    backend.storage.insert_new_datasets(
        dataset_names
            .into_iter()
            .map(|name| (name, DatasetSchema::default()))
            .collect(),
    )?;
    backend
        .storage
        .update_worker_set(&baseline.workers, 0, GC_TICKS)?;
    tracing::info!(
        "Seeded {dataset_count} datasets and {} workers",
        baseline.workers.len()
    );

    tracing::info!("Inserting {total_chunks} baseline chunks...");
    let storage_chunks: Vec<NewChunk> = std::mem::take(&mut baseline.chunks)
        .into_iter()
        .map(to_storage_chunk)
        .collect();
    backend.storage.insert_new_chunks(storage_chunks)?;
    let registered = backend.storage.register_new_chunks()?;
    tracing::info!(
        "Registered {} baseline chunks ({} rejected as overlapping)",
        registered.len(),
        total_chunks - registered.len()
    );
    Ok(total_chunks)
}

/// What the metrics need from one published assignment.
struct Snapshot {
    /// Physical holders (`chunk_workers` = ideal ∪ stale): what each worker has on disk. A copy that
    /// flips between the two ledgers stays on disk, so it must score as neither a free nor a download.
    owners: ChunkOwners,
    /// Of those, the draining copies. The diff subtracts them to recover the ideal, which is what tells
    /// a move apart from an extra replica.
    stale_owners: ChunkOwners,
    sizes: ChunkSizeIndex,
    used_bytes: u64,
    stale_bytes: u64,
}

impl Snapshot {
    fn of(
        assignment: &WorkerAssignment,
        stale: &[(ChunkPk, WorkerPk)],
        worker_index: &mut WorkerIndex,
    ) -> Self {
        let (owners, sizes, used_bytes) = physical_holders(assignment, worker_index);
        let (stale_owners, stale_bytes) = stale_holders(assignment, stale, worker_index);
        Snapshot {
            owners,
            stale_owners,
            sizes,
            used_bytes,
            stale_bytes,
        }
    }
}

/// Every chunk's holders, its size, and the total bytes they occupy.
fn physical_holders(
    assignment: &WorkerAssignment,
    worker_index: &mut WorkerIndex,
) -> (ChunkOwners, ChunkSizeIndex, u64) {
    let mut owners: ChunkOwners = BTreeMap::new();
    let mut sizes: ChunkSizeIndex = BTreeMap::new();
    let mut used_bytes = 0u64;
    for (chunk_pk, worker_pks) in &assignment.chunk_workers {
        let Some(chunk) = assignment.chunks.get(chunk_pk) else {
            continue;
        };
        let key = (chunk.dataset.clone(), chunk.id.clone());
        sizes.insert(key.clone(), chunk.size);
        used_bytes += chunk.size as u64 * worker_pks.len() as u64;

        let holders = worker_index.intern_holders(worker_pks.iter().filter_map(|worker_pk| {
            assignment
                .workers
                .get(worker_pk)
                .map(|worker| worker.peer_id)
        }));
        owners.insert(key, holders);
    }
    (owners, sizes, used_bytes)
}

/// The draining copies, one per stale row, and the bytes they hold.
fn stale_holders(
    assignment: &WorkerAssignment,
    stale: &[(ChunkPk, WorkerPk)],
    worker_index: &mut WorkerIndex,
) -> (ChunkOwners, u64) {
    let mut stale_owners: ChunkOwners = BTreeMap::new();
    let mut stale_bytes = 0u64;
    for (chunk_pk, worker_pk) in stale {
        let (Some(chunk), Some(worker)) = (
            assignment.chunks.get(chunk_pk),
            assignment.workers.get(worker_pk),
        ) else {
            continue;
        };
        stale_bytes += chunk.size as u64;
        stale_owners
            .entry((chunk.dataset.clone(), chunk.id.clone()))
            .or_default()
            .push(worker_index.intern(worker.peer_id));
    }
    // The diff binary-searches these against the sorted physical holders.
    for holders in stale_owners.values_mut() {
        holders.sort_unstable();
    }
    (stale_owners, stale_bytes)
}

/// Storage-boundary conversion: the sim models chunks as the legacy `types::Chunk`; the MVCC
/// storage takes `NewChunk` (no files; the sim carries no schema metadata).
fn to_storage_chunk(chunk: Chunk) -> NewChunk {
    NewChunk {
        dataset: chunk.dataset,
        id: chunk.id,
        size: chunk.size,
        blocks: chunk.blocks,
        schema_id: None,
        tables_present: None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use libp2p_identity::PeerId;
    use network_scheduler::scheduler_storage::{AssignmentWorker, ChunkPk, SchemaId, WorkerPk};
    use network_scheduler::types::WorkerStatus;

    use super::*;

    fn chunk(dataset: &str, id: &str, size: u32) -> WorkerAssignmentChunk {
        WorkerAssignmentChunk {
            dataset: Arc::new(dataset.to_string()),
            id: Arc::new(id.to_string()),
            size,
            blocks: 0..=0,
            schema_id: SchemaId(1),
            tables_present: None,
        }
    }

    fn worker(pk: i64) -> (WorkerPk, AssignmentWorker) {
        (
            WorkerPk(pk),
            AssignmentWorker {
                peer_id: PeerId::random(),
                status: WorkerStatus::Online,
            },
        )
    }

    #[test]
    fn lagged_watermark_trails_publications_and_clamps_to_baseline() {
        // Publication history: baseline (id 10), then steps publishing 11, 12, 13.
        let ids = [10, 11, 12, 13];

        // Zero lag confirms the newest publication immediately.
        assert_eq!(lagged_watermark(&ids, 0), 13);
        // A lag trails by that many publications.
        assert_eq!(lagged_watermark(&ids, 1), 12);
        assert_eq!(lagged_watermark(&ids, 2), 11);
        // A lag past the start clamps to the baseline, never underflows.
        assert_eq!(lagged_watermark(&ids, 3), 10);
        assert_eq!(lagged_watermark(&ids, 99), 10);

        // Early in the run only the baseline exists, so every lag resolves to it.
        assert_eq!(lagged_watermark(&[10], 0), 10);
        assert_eq!(lagged_watermark(&[10], 5), 10);
    }

    #[test]
    fn owners_are_physical_and_stale_bytes_are_the_overhang() {
        let c1 = ChunkPk(1);
        let c2 = ChunkPk(2);
        let (worker1_pk, worker1) = worker(1);
        let (worker2_pk, worker2) = worker(2);
        let (worker3_pk, worker3) = worker(3);

        let assignment = WorkerAssignment {
            id: 7,
            // c1 ideal on worker 1, plus a draining stale copy on worker 3; c2 ideal on worker 2.
            chunk_workers: BTreeMap::from([
                (c1, vec![worker1_pk, worker3_pk]),
                (c2, vec![worker2_pk]),
            ]),
            chunks: BTreeMap::from([(c1, chunk("ds", "c1", 100)), (c2, chunk("ds", "c2", 50))]),
            workers: BTreeMap::from([
                (worker1_pk, worker1.clone()),
                (worker2_pk, worker2),
                (worker3_pk, worker3.clone()),
            ]),
            replication_by_weight: BTreeMap::new(),
        };
        let stale = [(c1, worker3_pk)];

        let mut worker_index = WorkerIndex::default();
        let snapshot = Snapshot::of(&assignment, &stale, &mut worker_index);

        // c1's physical holders include the draining copy on worker 3 (else re-promoting it later
        // would read as a fresh download). `intern_holders` sorts, so both indices in order.
        let c1_key = (Arc::new("ds".to_string()), Arc::new("c1".to_string()));
        let mut expected = vec![
            worker_index.intern(worker1.peer_id),
            worker_index.intern(worker3.peer_id),
        ];
        expected.sort_unstable();
        assert_eq!(snapshot.owners[&c1_key], expected);
        assert_eq!(snapshot.owners.len(), 2);

        // Worker 3's copy is the draining one, and only it: the diff subtracts these to get the ideal.
        assert_eq!(
            snapshot.stale_owners[&c1_key],
            vec![worker_index.intern(worker3.peer_id)],
        );

        // Physical use counts the stale copy (100*2 + 50); the overhang is c1's extra copy (100).
        assert_eq!(snapshot.used_bytes, 100 * 2 + 50);
        assert_eq!(snapshot.stale_bytes, 100);
    }

    const SIZE: u32 = 100;

    /// Drives published assignments through the metrics the way [`crate::simulation::Simulation`] does,
    /// and re-checks the ledger on every step: used capacity must move by exactly
    /// `total_download - freed_bytes`. That invariant ties the diff (which classifies) to the storage's
    /// own byte count (which doesn't), so a miscounted download or free cannot pass unnoticed.
    #[derive(Default)]
    struct Harness {
        worker_index: WorkerIndex,
        sizes: ChunkSizeIndex,
        previous: Placement,
        used: u64,
    }

    impl Harness {
        /// The placement step 1 is diffed against; not itself measured.
        fn seed(&mut self, assignment: &WorkerAssignment, stale: &[(ChunkPk, WorkerPk)]) {
            let snapshot = Snapshot::of(assignment, stale, &mut self.worker_index);
            self.used = snapshot.used_bytes;
            self.sizes = snapshot.sizes;
            self.previous = Placement {
                owners: snapshot.owners,
                stale: snapshot.stale_owners,
            };
        }

        fn step(
            &mut self,
            assignment: &WorkerAssignment,
            stale: &[(ChunkPk, WorkerPk)],
        ) -> crate::metrics::DataMovement {
            use std::time::Duration;

            use crate::metrics::{self, StepStats};
            use crate::types::ChunkId;

            let snapshot = Snapshot::of(assignment, stale, &mut self.worker_index);
            let used_now = snapshot.used_bytes;
            let placement = StepPlacement {
                owners: snapshot.owners,
                stale_owners: snapshot.stale_owners,
                chunk_sizes: snapshot.sizes,
                replication_by_weight: BTreeMap::new(),
                used_capacity_bytes: used_now,
                stale_capacity_bytes: snapshot.stale_bytes,
                ingested_chunks: 1,
                copied_or_replaced_chunks: 0,
                portal_chunks: None,
                schedule_duration: Duration::ZERO,
            };
            let (metrics, placed) = metrics::measure_reshuffle(
                &self.previous,
                &mut self.sizes,
                placement,
                &HashSet::<ChunkId>::new(),
                StepStats {
                    step: 1,
                    new_chunks: 0,
                    new_restricted: 0,
                    eligible_workers: 2,
                },
                1_000_000,
            );

            let movement = metrics.data_movement;
            assert_eq!(
                used_now as i64 - self.used as i64,
                movement.total_download() as i64 - movement.freed_bytes as i64,
                "the ledger must balance: used capacity moves by exactly (downloaded - freed)",
            );
            self.used = used_now;
            self.previous = placed;
            movement
        }
    }

    /// One chunk on `holders`, with `workers` as the fleet.
    fn one_chunk(
        holders: &[WorkerPk],
        workers: &[(WorkerPk, AssignmentWorker)],
    ) -> WorkerAssignment {
        WorkerAssignment {
            id: 1,
            chunk_workers: BTreeMap::from([(ChunkPk(1), holders.to_vec())]),
            chunks: BTreeMap::from([(ChunkPk(1), chunk("ds", "c1", SIZE))]),
            workers: workers.iter().cloned().collect(),
            replication_by_weight: BTreeMap::new(),
        }
    }

    /// A move is one shuffle, and its bytes land on either side of the drain: the replacement is
    /// downloaded in the step the scheduler drops the holder, and that holder's copy is freed later,
    /// when its drain expires. Only the ideal sees the two together, which is what identifies the move.
    #[test]
    fn a_move_across_the_drain_is_one_shuffle_downloaded_then_freed() {
        let c1 = ChunkPk(1);
        let (a_pk, a) = worker(1);
        let (b_pk, b) = worker(2);
        let fleet = [(a_pk, a), (b_pk, b)];

        let mut h = Harness::default();
        h.seed(&one_chunk(&[a_pk], &fleet), &[]); // c1 on A

        // The scheduler moves c1 A → B: B downloads it now, A's copy starts draining (still on disk).
        let supersede = h.step(&one_chunk(&[a_pk, b_pk], &fleet), &[(c1, a_pk)]);
        assert_eq!(supersede.shuffled_count, 1, "one chunk moved");
        assert_eq!(
            supersede.shuffled_bytes, SIZE as u64,
            "B downloaded the copy"
        );
        assert_eq!(
            supersede.shuffle_share(),
            100.0,
            "the whole download is a move"
        );
        assert_eq!(
            supersede.increased_replication_bytes, 0,
            "replication did not grow — the download replaced a dropped holder",
        );
        assert_eq!(
            supersede.freed_bytes, 0,
            "A still holds its copy while draining"
        );

        // A's drain expires: its bytes are freed, and nothing is downloaded or moved.
        let drain = h.step(&one_chunk(&[b_pk], &fleet), &[]);
        assert_eq!(drain.freed_bytes, SIZE as u64, "A deleted its copy");
        assert_eq!(drain.total_download(), 0, "the drain downloads nothing");
        assert_eq!(drain.shuffled_count, 0, "the move was already counted");
    }

    /// A download with no holder dropped is an extra replica, not a move — the distinction the
    /// shuffle share depends on.
    #[test]
    fn a_download_with_no_holder_dropped_is_a_replication_increase() {
        let (a_pk, a) = worker(1);
        let (b_pk, b) = worker(2);
        let fleet = [(a_pk, a), (b_pk, b)];

        let mut h = Harness::default();
        h.seed(&one_chunk(&[a_pk], &fleet), &[]);

        let grow = h.step(&one_chunk(&[a_pk, b_pk], &fleet), &[]);
        assert_eq!(grow.increased_replication_bytes, SIZE as u64);
        assert_eq!(grow.shuffled_count, 0, "A kept its copy — nothing moved");
        assert_eq!(
            grow.shuffle_share(),
            0.0,
            "none of the download is reshuffling"
        );
    }

    /// The copy that goes stale and comes back: it is in the published assignment the whole way, so the
    /// worker never drops it and no byte moves. The storage test
    /// `a_copy_expired_and_re_placed_in_one_cycle_never_leaves_the_published_assignment` pins why.
    #[test]
    fn a_copy_that_goes_stale_and_comes_back_moves_no_data() {
        let c1 = ChunkPk(1);
        let (a_pk, a) = worker(1); // stable holder
        let (b_pk, b) = worker(2); // rides supersede → drain → re-place
        let fleet = [(a_pk, a), (b_pk, b)];
        let both = one_chunk(&[a_pk, b_pk], &fleet);

        let mut h = Harness::default();
        h.seed(&both, &[]);

        // Supersede: ideal={A}, stale={B}; physical still {A,B}.
        let supersede = h.step(&both, &[(c1, b_pk)]);
        assert_eq!(
            supersede.freed_bytes, 0,
            "a superseded-but-on-disk copy must NOT read as freed",
        );
        assert_eq!(supersede.total_download(), 0, "nothing was fetched");
        assert_eq!(
            supersede.shed_replication_bytes, SIZE as u64,
            "the scheduler shed a replica; the bytes free when the drain expires",
        );

        // Lag hold: unchanged.
        let hold = h.step(&both, &[(c1, b_pk)]);
        assert_eq!(
            hold.total_download() + hold.freed_bytes,
            0,
            "hold is a no-op"
        );

        // B is back in the ideal, and it never lost the bytes: a promotion, not a download.
        let repromote = h.step(&both, &[]);
        assert_eq!(repromote.total_download(), 0, "B never dropped the chunk");
        assert_eq!(repromote.shuffled_count, 0, "nothing moved");
        assert_eq!(
            repromote.freed_bytes, 0,
            "c1 survives with the same holders"
        );
    }

    /// A chunk a correction tombstones leaves the placement entirely, and its bytes really are freed
    /// off the workers. It is absent from that step's own `chunk_sizes` (the placement it left), so a
    /// size index rebuilt per step scores it as freeing 0 B — the sizes accumulate to prevent that.
    #[test]
    fn a_chunk_dropped_from_the_placement_frees_its_bytes() {
        let (a_pk, a) = worker(1);
        let (b_pk, b) = worker(2);
        let fleet = [(a_pk, a), (b_pk, b)];

        let mut h = Harness::default();
        h.seed(&one_chunk(&[a_pk, b_pk], &fleet), &[]);

        let tombstoned = WorkerAssignment {
            id: 2,
            chunk_workers: BTreeMap::new(),
            chunks: BTreeMap::new(),
            workers: fleet.iter().cloned().collect(),
            replication_by_weight: BTreeMap::new(),
        };
        let dropped = h.step(&tombstoned, &[]);
        assert_eq!(
            dropped.freed_bytes,
            SIZE as u64 * 2,
            "both copies of the chunk were freed",
        );
    }
}
