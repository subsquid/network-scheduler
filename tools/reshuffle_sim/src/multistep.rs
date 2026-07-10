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
        ChunkPk, NewChunk, SchedulerStorage, StorageError, WorkerAssignment, WorkerAssignmentChunk,
        WorkerPk,
        algorithm::MultistepAlgorithm,
        postgres::PostgresStorage,
        test_harness::pg_harness::{self, PgData},
    },
    types::{Chunk, DatasetSchema},
};

use crate::{
    ChunkOwners, ChunkSizeIndex, WorkerIdx, WorkerIndex,
    baseline::Baseline,
    simulation::{CopyPlan, StepContext, StepOutcome, StepPlacement, StepScheduler},
};

/// Result of one scheduling cycle: a published assignment with its stale-mapping snapshot, or a
/// capacity shortage (the run records a failed step and stops).
enum CycleResult {
    Scheduled(WorkerAssignment, Vec<(ChunkPk, WorkerPk)>, Duration),
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

/// Owns the run's storage. When ephemeral, the container is the shared `pg_harness` one (reaped at
/// process exit), so there's nothing to hold or drop here.
struct Backend {
    storage: PostgresStorage,
}

/// Connect to an existing, user-supplied database and migrate it.
fn existing_database(url: &str) -> anyhow::Result<Backend> {
    tracing::info!("Connecting to Postgres at {url}");
    Ok(Backend {
        storage: connect_migrated(url)?,
    })
}

/// Clone a fresh, migrated database from the shared `pg_harness` container, started disk-backed
/// because the mainnet DB overflows the test tmpfs. The container is reaped at process exit (or left
/// running under `SIM_SQL_EXPLAIN`); both are handled by the harness.
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
    /// Seed datasets, workers and chunks, then continue to the baseline placement (normal run).
    Run,
    /// Seed nothing — the data is already in the database (e.g. a restored backup) — and go
    /// straight to the baseline placement.
    Skip,
    /// Seed datasets, workers and chunks, then stop before scheduling (snapshot the database here).
    Only,
}

pub struct MultistepScheduler {
    backend: Backend,
    algo: MultistepAlgorithm<DatasetsConfig>,
    config: MultistepConfig,
    clock: u64,
    total_chunks: usize,
    initial_owners: ChunkOwners,
    replace: Vec<ReplacePlan>,
    copy: Vec<CopyPlan>,
    /// 1-based step counter, advanced once per `step`.
    current_step: u32,
    /// Placed chunks of datasets still awaiting a replace — a same-cycle replace reads its target's
    /// pre-cycle placement from here. Kept only while a replace lies ahead.
    latest_chunks: BTreeMap<ChunkPk, WorkerAssignmentChunk>,
    /// Interns this scheduler's worker `PeerId`s to compact indices for the owners
    /// maps it produces. Self-contained: the baseline seed and every step share it, so
    /// indices are stable across the run. (The multistep path ignores the baseline's
    /// own index table — it derives owners from its own storage cycles.)
    worker_index: WorkerIndex,
}

impl MultistepScheduler {
    pub fn build(
        config: &Config,
        baseline: &mut Baseline,
        database_url: Option<&str>,
        restricted_dataset: Option<String>,
        replace: Vec<ReplacePlan>,
        copy: Vec<CopyPlan>,
        ingestion: Ingestion,
    ) -> anyhow::Result<Option<Self>> {
        let mut backend = match database_url {
            Some(url) => existing_database(url)?,
            None => ephemeral_database(),
        };
        let algo = MultistepAlgorithm::new(config.datasets.clone());
        let scheduling_config = MultistepConfig {
            worker_capacity: config.worker_storage_bytes,
            saturation: config.saturation,
            min_replication: config.min_replication,
            ignore_reliability: true,
        };

        // Seed: datasets must exist before their chunks; workers before the first cycle. The
        // simulation's synthetic restricted dataset has no baseline chunks, so register it here
        // too (it starts empty and receives chunks from step 1 on). `Ingestion::Skip` reuses the
        // data already in the database (e.g. a restored backup) and seeds nothing.
        let total_chunks = baseline.chunks.len();
        match ingestion {
            Ingestion::Skip => {
                // Drop the in-memory chunk set; Postgres owns it now
                baseline.chunks = Vec::new();
                tracing::info!(
                    "Skipping ingestion; reusing {total_chunks} baseline chunks from the database"
                );
            }
            Ingestion::Run | Ingestion::Only => {
                let mut dataset_names: BTreeSet<String> = baseline
                    .chunks
                    .iter()
                    .map(|c| (*c.dataset).clone())
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
            }
        }

        // `Ingestion::Only`: the seeded database is the deliverable — snapshot it now. Stop before
        // scheduling, so a later `Ingestion::Skip` run restores it and benchmarks from here.
        if ingestion == Ingestion::Only {
            log_table_sizes(&backend.storage);
            tracing::info!("Ingest-only: baseline data loaded; stopping before scheduling");
            return Ok(None);
        }

        let mut scheduler = Self {
            backend,
            algo,
            config: scheduling_config,
            clock: 0,
            total_chunks,
            initial_owners: ChunkOwners::new(),
            replace,
            copy,
            current_step: 0,
            latest_chunks: BTreeMap::new(),
            worker_index: WorkerIndex::default(),
        };

        tracing::info!("Scheduling baseline placement...");
        let (assignment, stale) = match scheduler
            .run_cycle()
            .context("scheduling baseline placement")?
        {
            CycleResult::Scheduled(assignment, stale, _) => (assignment, stale),
            CycleResult::Shortage(reason) => {
                anyhow::bail!("baseline placement infeasible: {reason}")
            }
        };
        scheduler.initial_owners =
            owners_from_assignment(&assignment, &stale, &mut scheduler.worker_index).0;
        for plan in &scheduler.replace {
            let ds = format!("s3://{}", plan.bucket);
            anyhow::ensure!(
                assignment.chunks.values().any(|c| *c.dataset == ds),
                "replace: dataset {ds} not found in baseline"
            );
        }
        scheduler.retain_replace_targets(&assignment);
        tracing::info!(
            "Baseline placed {} chunks; replication by weight {:?}",
            scheduler.initial_owners.len(),
            assignment.replication_by_weight
        );
        log_table_sizes(&scheduler.backend.storage);
        Ok(Some(scheduler))
    }

    /// Run one scheduling cycle. Advances the clock by the removal delay (so earlier steps' removed
    /// copies age out), then confirms the assignment fleet-wide and runs visibility, so the copies
    /// it replaces start their removal timer.
    fn run_cycle(&mut self) -> anyhow::Result<CycleResult> {
        let storage = &mut self.backend.storage;
        self.clock += M_TICKS;
        let started = Instant::now();
        let assignment =
            match storage.run_scheduling_cycle(&self.algo, &self.config, self.clock, M_TICKS) {
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
        let stale = storage.stale_mappings()?;
        self.clock += CLOCK_STEP;
        storage.confirm_worker_assignment(assignment.id, self.clock)?;
        storage.run_visibility_cycle(self.clock)?;
        Ok(CycleResult::Scheduled(assignment, stale, schedule_duration))
    }

    /// Supersede every chunk of `bucket` with a same-range correction, sourced from `latest_chunks`
    /// (a valid target is fully placed, so that set is its live chunks). The caller's
    /// `register_new_chunks` makes the replacements eligible; they swap in this cycle.
    fn replace_dataset(&mut self, bucket: &str) -> anyhow::Result<()> {
        let target = format!("s3://{bucket}");
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
        self.total_chunks += count;
        tracing::info!(
            "Replaced {count} chunks in dataset {bucket} at step {}",
            self.current_step
        );
        Ok(())
    }

    /// Clone every live chunk of `plan.src` into the new dataset `plan.dst` (same id, range, size),
    /// server-side — Postgres is this scheduler's source of truth (see `Simulation::copy_chunks_due`),
    /// so the copy never round-trips 147K chunk payloads through the client. Runs before the step's
    /// cycle: the caller's `register_new_chunks` admits the clones and the one cycle places them.
    fn copy_dataset(&mut self, plan: &CopyPlan) -> anyhow::Result<()> {
        let src = format!("s3://{}", plan.src);
        let dst = format!("s3://{}", plan.dst);
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
        self.total_chunks += count as usize;
        tracing::info!(
            "Copied {count} chunks from {} to {} at step {}",
            plan.src,
            plan.dst,
            self.current_step
        );
        Ok(())
    }

    /// Keep in `latest_chunks` the placed chunks of datasets with a replace still ahead (at_step >
    /// current step), so a later replace can source its target's pre-cycle placement.
    fn retain_replace_targets(&mut self, assignment: &WorkerAssignment) {
        let targets: HashSet<String> = self
            .replace
            .iter()
            .filter(|p| p.at_step > self.current_step)
            .map(|p| format!("s3://{}", p.bucket))
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
    fn take_initial_owners(&mut self) -> ChunkOwners {
        std::mem::take(&mut self.initial_owners)
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
        for bucket in &due_replaces {
            self.replace_dataset(bucket)?;
        }

        // Copy before the cycle: the clones (src's live set, server-side) register alongside this
        // step's new chunks and the one cycle places everything — no second full cycle for the copy.
        let (due_copies, rest): (Vec<CopyPlan>, Vec<CopyPlan>) = std::mem::take(&mut self.copy)
            .into_iter()
            .partition(|p| p.at_step == self.current_step);
        self.copy = rest;
        for plan in &due_copies {
            self.copy_dataset(plan)?;
        }

        let new_chunks: Vec<NewChunk> = ctx
            .new_chunks_this_step
            .iter()
            .cloned()
            .map(to_storage_chunk)
            .collect();
        self.total_chunks += new_chunks.len();
        self.backend.storage.insert_new_chunks(new_chunks)?;
        self.backend.storage.register_new_chunks()?;
        let (assignment, stale, schedule_duration) = match self.run_cycle()? {
            CycleResult::Scheduled(assignment, stale, duration) => (assignment, stale, duration),
            CycleResult::Shortage(reason) => return Ok(StepOutcome::Failed(reason)),
        };

        log_table_sizes(&self.backend.storage);

        let (owners, chunk_sizes, used_capacity_bytes, stale_capacity_bytes) =
            owners_from_assignment(&assignment, &stale, &mut self.worker_index);
        self.retain_replace_targets(&assignment);
        Ok(StepOutcome::Scheduled(StepPlacement {
            owners,
            chunk_sizes,
            replication_by_weight: assignment.replication_by_weight.clone(),
            used_capacity_bytes,
            stale_capacity_bytes,
            total_chunks: self.total_chunks,
            schedule_duration,
        }))
    }
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

/// Returns the ideal owners we diff between steps (`chunk_workers` minus stale, which is exact since
/// ideal and stale are disjoint per chunk), per-chunk sizes, total physical bytes, and stale bytes.
fn owners_from_assignment(
    assignment: &WorkerAssignment,
    stale: &[(ChunkPk, WorkerPk)],
    worker_index: &mut WorkerIndex,
) -> (ChunkOwners, ChunkSizeIndex, u64, u64) {
    let stale: HashSet<(ChunkPk, WorkerPk)> = stale.iter().copied().collect();

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

        let holders: Vec<WorkerIdx> = worker_index.intern_holders(
            worker_pks
                .iter()
                .filter(|worker_pk| !stale.contains(&(*chunk_pk, **worker_pk)))
                .filter_map(|worker_pk| {
                    assignment
                        .workers
                        .get(worker_pk)
                        .map(|worker| worker.peer_id)
                }),
        );
        owners.insert(key, holders);
    }

    // Each stale row is one draining copy; sum their chunk sizes.
    let stale_bytes: u64 = stale
        .iter()
        .filter_map(|(chunk_pk, _)| {
            assignment
                .chunks
                .get(chunk_pk)
                .map(|chunk| chunk.size as u64)
        })
        .sum();

    (owners, sizes, used_bytes, stale_bytes)
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
    fn owners_are_ideal_only_and_stale_bytes_are_the_overhang() {
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
                (worker3_pk, worker3),
            ]),
            replication_by_weight: BTreeMap::new(),
        };
        let stale = [(c1, worker3_pk)];

        let mut worker_index = WorkerIndex::default();
        let (owners, _sizes, used_bytes, stale_bytes) =
            owners_from_assignment(&assignment, &stale, &mut worker_index);

        // c1's ideal holders exclude the stale worker 3. `intern` is idempotent, so
        // re-interning worker 1 yields the same index the owners list stored.
        let c1_key = (Arc::new("ds".to_string()), Arc::new("c1".to_string()));
        assert_eq!(owners[&c1_key], vec![worker_index.intern(worker1.peer_id)]);
        assert_eq!(owners.len(), 2);

        // Physical use counts the stale copy (100*2 + 50); the overhang is c1's extra copy (100).
        assert_eq!(used_bytes, 100 * 2 + 50);
        assert_eq!(stale_bytes, 100);
    }
}
