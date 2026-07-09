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

use crate::{
    ChunkOwners, ChunkSizeIndex, DrainedOwners, WorkerIdx, WorkerIndex,
    baseline::Baseline,
    simulation::{CopyPlan, StepContext, StepOutcome, StepPlacement, StepScheduler},
};

/// Result of one scheduling cycle: a published assignment with its stale-mapping snapshot and the
/// `(chunk, worker)` copies the cycle physically expired, or a capacity shortage (the run records a
/// failed step and stops).
enum CycleResult {
    Scheduled(
        WorkerAssignment,
        Vec<(ChunkPk, WorkerPk)>,
        Vec<(ChunkPk, WorkerPk)>,
        Duration,
    ),
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
    /// Cycles a published assignment waits before the fleet has downloaded it (uniform latency).
    confirm_lag_steps: u32,
    /// Extra cycles before the portal routes a confirmed assignment, gating when its superseded
    /// copies may drain.
    portal_lag_steps: u32,
    /// Worker-assignment ids in publication order (index 0 = baseline). The confirmation watermark
    /// trails the newest publication by `confirm_lag_steps + portal_lag_steps` entries.
    published_ids: Vec<AssignmentId>,
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
    #[allow(clippy::too_many_arguments)]
    pub fn build(
        config: &Config,
        baseline: &mut Baseline,
        database_url: Option<&str>,
        restricted_dataset: Option<String>,
        replace: Vec<ReplacePlan>,
        copy: Vec<CopyPlan>,
        confirm_lag_steps: u32,
        portal_lag_steps: u32,
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
            confirm_lag_steps,
            portal_lag_steps,
            published_ids: Vec::new(),
            current_step: 0,
            latest_chunks: BTreeMap::new(),
            worker_index: WorkerIndex::default(),
        };

        tracing::info!("Scheduling baseline placement...");
        let (assignment, stale) = match scheduler
            .run_cycle()
            .context("scheduling baseline placement")?
        {
            CycleResult::Scheduled(assignment, stale, _drains, _) => (assignment, stale),
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
    /// copies age out), then confirms the fleet up to the lagged watermark and runs visibility, so
    /// only assignments the fleet+portal have caught up to let their superseded copies drain.
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
        // The copies this cycle's GC physically expired: a copy re-fetched onto the same worker it
        // drained from is a real download the placement snapshot alone can't see.
        let drains = self.backend.storage.take_last_cycle_drains();
        // Before confirm/visibility expire or drain stale, so the snapshot matches `assignment`.
        let stale = self.backend.storage.stale_mappings()?;
        self.clock += CLOCK_STEP;
        // Confirm only up to the lagged watermark: the copies this cycle superseded keep occupying
        // capacity until the assignment that replaced them clears both lags. Both lags 0 confirms
        // the just-published assignment — the previous confirm-immediately behaviour.
        self.published_ids.push(assignment.id);
        let watermark = self.confirmation_watermark();
        self.backend
            .storage
            .confirm_worker_assignment(watermark, self.clock)?;
        self.backend.storage.run_visibility_cycle(self.clock)?;
        Ok(CycleResult::Scheduled(
            assignment,
            stale,
            drains,
            schedule_duration,
        ))
    }

    /// The publication `confirm_lag_steps + portal_lag_steps` cycles back.
    fn confirmation_watermark(&self) -> AssignmentId {
        let lag = (self.confirm_lag_steps + self.portal_lag_steps) as usize;
        lagged_watermark(&self.published_ids, lag)
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
        let (assignment, stale, drains, schedule_duration) = match self.run_cycle()? {
            CycleResult::Scheduled(assignment, stale, drains, duration) => {
                (assignment, stale, drains, duration)
            }
            CycleResult::Shortage(reason) => return Ok(StepOutcome::Failed(reason)),
        };

        log_table_sizes(&self.backend.storage);

        let (owners, chunk_sizes, used_capacity_bytes, stale_capacity_bytes) =
            owners_from_assignment(&assignment, &stale, &mut self.worker_index);
        let drained = drained_owners(&assignment, &drains, &mut self.worker_index);
        self.retain_replace_targets(&assignment);
        Ok(StepOutcome::Scheduled(StepPlacement {
            owners,
            chunk_sizes,
            drained,
            replication_by_weight: assignment.replication_by_weight.clone(),
            used_capacity_bytes,
            stale_capacity_bytes,
            total_chunks: self.total_chunks,
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

/// Returns the *physical* owners we diff between steps — every worker that holds a copy of the chunk,
/// whether it's the live ideal copy or a superseded one still draining (stale). Also returns
/// per-chunk sizes, total physical bytes, and stale bytes.
///
/// Diffing physical placement (`chunk_workers`, i.e. ideal ∪ stale) rather than the ideal alone
/// means a copy that only flips between the ideal and stale ledgers — superseded, then later
/// re-promoted — never leaves disk and so never shows up as a free or a download. A same-worker
/// drain→refetch is the one physical move a boundary diff still misses; the `drained` set carried
/// alongside these owners recovers it (see [`drained_owners`]).
fn owners_from_assignment(
    assignment: &WorkerAssignment,
    stale: &[(ChunkPk, WorkerPk)],
    worker_index: &mut WorkerIndex,
) -> (ChunkOwners, ChunkSizeIndex, u64, u64) {
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

        // Intern every physical holder (ideal ∪ stale); `chunk_workers` already merges both.
        let holders: Vec<WorkerIdx> =
            worker_index.intern_holders(worker_pks.iter().filter_map(|worker_pk| {
                assignment
                    .workers
                    .get(worker_pk)
                    .map(|worker| worker.peer_id)
            }));
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

/// Translate the storage's drained `(chunk_pk, worker_pk)` pairs into `(ChunkId, WorkerIdx)`, interned
/// with the SAME `worker_index` as [`owners_from_assignment`] so indices line up in the diff. Pairs
/// whose chunk or worker is absent from the published assignment (a chunk that fully drained away, a
/// departed worker) are skipped: they never appear in the current owners, so they can never be scored
/// as a refetch download — `freed = prev \ cur` already accounts for them.
fn drained_owners(
    assignment: &WorkerAssignment,
    drains: &[(ChunkPk, WorkerPk)],
    worker_index: &mut WorkerIndex,
) -> DrainedOwners {
    let mut out: DrainedOwners = BTreeMap::new();
    for (chunk_pk, worker_pk) in drains {
        let (Some(chunk), Some(worker)) = (
            assignment.chunks.get(chunk_pk),
            assignment.workers.get(worker_pk),
        ) else {
            continue;
        };
        out.entry((chunk.dataset.clone(), chunk.id.clone()))
            .or_default()
            .push(worker_index.intern(worker.peer_id));
    }
    // `compute_data_movement` binary-searches these, so keep each holder list sorted and deduped.
    for holders in out.values_mut() {
        holders.sort_unstable();
        holders.dedup();
    }
    out
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

        // Zero lag confirms the newest publication immediately (the prior behaviour).
        assert_eq!(lagged_watermark(&ids, 0), 13);
        // Lag 1/2 trail by that many publications.
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
        let (owners, _sizes, used_bytes, stale_bytes) =
            owners_from_assignment(&assignment, &stale, &mut worker_index);

        // c1's PHYSICAL holders include the draining copy on worker 3: a diff must see it as still
        // present so re-promoting it later is not mis-scored as a fresh download. `intern` is
        // idempotent and `intern_holders` sorts, so the list is both indices in order.
        let c1_key = (Arc::new("ds".to_string()), Arc::new("c1".to_string()));
        let mut expected = vec![
            worker_index.intern(worker1.peer_id),
            worker_index.intern(worker3.peer_id),
        ];
        expected.sort_unstable();
        assert_eq!(owners[&c1_key], expected);
        assert_eq!(owners.len(), 2);

        // Physical use counts the stale copy (100*2 + 50); the overhang is c1's extra copy (100).
        assert_eq!(used_bytes, 100 * 2 + 50);
        assert_eq!(stale_bytes, 100);
    }

    /// End-to-end guard for "only real downloads count", walking the full metric pipeline
    /// (`owners_from_assignment` → `measure_reshuffle` → `compute_data_movement`) through the exact
    /// lifecycle that produced the heavy-profile artifacts:
    ///   phase 1  supersede: a copy leaves the ideal but stays on disk (stale)  → must NOT read as freed
    ///   phase 2  lag hold:  nothing moves                                       → no-op
    ///   phase 3  drain + same-worker refetch: the copy is expired then re-placed → a REAL download
    /// Without the drained set a step-boundary diff would miss phase 3 entirely (the copy is present
    /// at both boundaries); the `drained` input is what recovers it.
    #[test]
    fn superseded_copy_is_not_freed_and_a_drained_refetch_is_a_real_download() {
        use std::time::Duration;

        use crate::ChunkId;
        use crate::metrics::{self, DataMovement, StepStats};

        let c1 = ChunkPk(1);
        let c1_key: ChunkId = (Arc::new("ds".to_string()), Arc::new("c1".to_string()));
        let (a_pk, a) = worker(1); // stable holder
        let (b_pk, b) = worker(2); // rides supersede → drain → same-worker refetch

        // One chunk (size 100). `owners_from_assignment` returns PHYSICAL owners (ideal ∪ stale).
        let assignment = |holders: Vec<WorkerPk>| WorkerAssignment {
            id: 1,
            chunk_workers: BTreeMap::from([(c1, holders)]),
            chunks: BTreeMap::from([(c1, chunk("ds", "c1", 100))]),
            workers: BTreeMap::from([(a_pk, a.clone()), (b_pk, b.clone())]),
            replication_by_weight: BTreeMap::new(),
        };
        let placement =
            |owners: ChunkOwners, sizes: ChunkSizeIndex, drained: DrainedOwners| StepPlacement {
                owners,
                chunk_sizes: sizes,
                drained,
                replication_by_weight: BTreeMap::new(),
                used_capacity_bytes: 0,
                stale_capacity_bytes: 0,
                total_chunks: 1,
                schedule_duration: Duration::ZERO,
            };
        let restricted: HashSet<ChunkId> = HashSet::new();
        let stats = || StepStats {
            step: 1,
            new_chunks: 0,
            new_restricted: 0,
            eligible_workers: 2,
        };
        let diff = |prev: &ChunkOwners, place: StepPlacement| -> (DataMovement, ChunkOwners) {
            let (m, owners) =
                metrics::measure_reshuffle(prev, place, &restricted, stats(), 1_000_000);
            (m.data_movement, owners)
        };

        let mut wi = WorkerIndex::default();

        // Baseline: c1 physically on {A, B}, nothing stale.
        let (owners0, ..) = owners_from_assignment(&assignment(vec![a_pk, b_pk]), &[], &mut wi);
        let idx_b = wi.intern(b.peer_id); // idempotent; names B's drained copy in phase 3

        // PHASE 1 — supersede: ideal={A}, stale={B}; physical still {A,B}.
        let (phys1, s1, ..) =
            owners_from_assignment(&assignment(vec![a_pk, b_pk]), &[(c1, b_pk)], &mut wi);
        let (dm1, owners1) = diff(&owners0, placement(phys1, s1, ChunkOwners::new()));
        assert_eq!(
            dm1.decreased_replication_bytes, 0,
            "a superseded-but-on-disk copy must NOT read as freed"
        );
        assert_eq!(dm1.total_download(), 0, "nothing was fetched");

        // PHASE 2 — lag hold: unchanged.
        let (phys2, s2, ..) =
            owners_from_assignment(&assignment(vec![a_pk, b_pk]), &[(c1, b_pk)], &mut wi);
        let (dm2, owners2) = diff(&owners1, placement(phys2, s2, ChunkOwners::new()));
        assert_eq!(
            dm2.total_download() + dm2.decreased_replication_bytes,
            0,
            "hold is a no-op"
        );

        // PHASE 3 — drain + same-worker refetch: B's copy is expired this cycle, then the ring
        // re-places c1 back on B. Published ideal={A,B}, stale={}; drained={(c1,B)}.
        let (phys3, s3, ..) = owners_from_assignment(&assignment(vec![a_pk, b_pk]), &[], &mut wi);

        // (a) WITHOUT the drained set a boundary diff HIDES the refetch (documents the bug).
        let (dm3_hidden, _) = diff(
            &owners2,
            placement(phys3.clone(), s3.clone(), ChunkOwners::new()),
        );
        assert_eq!(
            dm3_hidden.total_download(),
            0,
            "without the drained set the same-worker refetch is invisible"
        );

        // (b) WITH drained {c1:[B]}: the refetch is scored as a genuine S3 download.
        let drained = BTreeMap::from([(c1_key.clone(), vec![idx_b])]);
        let (dm3, _) = diff(&owners2, placement(phys3, s3, drained));
        assert_eq!(
            dm3.refetched_bytes, 100,
            "a drained-then-refetched copy is a real download"
        );
        assert_eq!(
            dm3.total_download(),
            100,
            "and it counts toward total download"
        );
        assert_eq!(
            dm3.increased_replication_bytes, 0,
            "replication did NOT grow — B leaves as stale and returns as ideal"
        );
        assert_eq!(
            dm3.decreased_replication_bytes, 0,
            "c1 survives with the same holders"
        );
    }
}
