//! Simulates chunk ingestion over multiple steps and measures reshuffling impact.
//!
//! Reads a real assignment (flatbuffer), generates new chunks at the head of each
//! dataset proportionally to existing sizes, then runs a scheduler and diffs
//! against the previous step.
//!
//! Run parameters come from a `--profile` YAML (steps, `scheduler`, `copy`/`replace`,
//! version-restriction and worker-upgrade modeling, …); see [`profile`]. Two schedulers exist:
//! stateless (rebuilds the placement each step) and multistep (Postgres-backed, reuses it).
//! Restricted data lives in a dedicated dataset carrying a `minimum_worker_version`, honored by both.
//!
//! Usage:
//!   cargo run -p reshuffle-sim -- -c config.yaml --profile run.yaml input.fb.gz

mod baseline;
mod chunks;
mod metrics;
mod multistep;
mod profile;
mod rate;
mod report;
mod simulation;
mod stateless;
mod upgrade;
mod util;

use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::Arc,
};

use anyhow::Context;
use bytesize::ByteSize;
use clap::Parser;
use libp2p_identity::PeerId;
use network_scheduler::{
    cli::{Config, DatasetSegmentConfig},
    scheduling::SchedulingConfig,
};
use rand::prelude::*;

use crate::multistep::{Ingestion, ReplacePlan};
use crate::simulation::{CopyPlan, Simulation, SimulationParams, StepScheduler};

#[cfg(not(feature = "dhat-heap"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

type ChunkId = (Arc<String>, Arc<String>);
/// Run-stable compact worker identity
type WorkerIdx = u16;
type ChunkOwners = BTreeMap<ChunkId, Vec<WorkerIdx>>;
type ChunkSizeIndex = BTreeMap<ChunkId, u32>;

/// Interns worker `PeerId`s to compact [`WorkerIdx`]es, assigned in first-seen order
/// and never reused, so a worker keeps the same index for the whole run.
#[derive(Default)]
struct WorkerIndex {
    to_idx: HashMap<PeerId, WorkerIdx>,
}

impl WorkerIndex {
    /// The index for `peer`, assigning the next one on first sight.
    fn intern(&mut self, peer: PeerId) -> WorkerIdx {
        if let Some(&idx) = self.to_idx.get(&peer) {
            return idx;
        }
        let idx = WorkerIdx::try_from(self.to_idx.len())
            .expect("reshuffle-sim: more than u16::MAX distinct workers in one run");
        self.to_idx.insert(peer, idx);
        idx
    }

    fn intern_holders(&mut self, peers: impl IntoIterator<Item = PeerId>) -> Vec<WorkerIdx> {
        let mut holders: Vec<WorkerIdx> = peers.into_iter().map(|p| self.intern(p)).collect();
        holders.sort_unstable();
        holders.dedup();
        holders
    }
}

#[derive(Parser, Debug)]
#[command(about = "Simulate chunk ingestion and measure reshuffling")]
struct Args {
    /// Input assignment file (flatbuffer, optionally .gz)
    #[arg(value_name = "INPUT")]
    input: PathBuf,

    /// Scheduler config file
    #[arg(long, short)]
    config: PathBuf,

    /// Run parameters as a YAML file (steps, scheduler, copy/replace, …). Defaults apply when unset.
    #[arg(long)]
    profile: Option<PathBuf>,

    /// Postgres URL for the multistep scheduler. If omitted, an ephemeral Postgres container is
    /// started (requires Docker). The database must be empty for a normal run (migrations run on
    /// connect); pass `--skip-ingest` to reuse an already-seeded one.
    #[arg(long)]
    database_url: Option<String>,

    /// Seed the baseline datasets, workers and chunks into `--database-url`, then stop before
    /// scheduling — snapshot the database here and reuse it with `--skip-ingest`. Multistep +
    /// `--database-url` only.
    #[arg(long, conflicts_with = "skip_ingest")]
    ingest_only: bool,

    /// Skip baseline ingestion and reuse the data already in `--database-url` (e.g. a restored
    /// `--ingest-only` snapshot), going straight to scheduling. Multistep + `--database-url` only.
    #[arg(long)]
    skip_ingest: bool,

    /// Enable dhat heap profiling for this run (writes dhat-heap.json on exit).
    /// Requires building with `--features dhat-heap`; ignored otherwise.
    /// View with https://nnethercote.github.io/dh_view/dh_view.html
    #[arg(long)]
    dhat: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Scheduler {
    /// Rebuild the whole placement from scratch each step (`src/scheduling.rs`).
    Stateless,
    /// Keep the stored placement and move only what's needed (multistep, backed by Postgres).
    Multistep,
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let profile = match &args.profile {
        Some(path) => profile::Profile::load(path)?,
        None => profile::Profile::default(),
    };
    let run = RunConfig::from_profile(profile)?;

    let ingestion = if args.ingest_only {
        Ingestion::Only
    } else if args.skip_ingest {
        Ingestion::Skip
    } else {
        Ingestion::Run
    };
    if ingestion != Ingestion::Run {
        anyhow::ensure!(
            run.scheduler == Scheduler::Multistep,
            "--ingest-only/--skip-ingest require the multistep scheduler"
        );
        anyhow::ensure!(
            args.database_url.is_some(),
            "--ingest-only/--skip-ingest require --database-url (a persistent database to snapshot and restore)"
        );
    }

    #[cfg(feature = "dhat-heap")]
    let _profiler = args.dhat.then(dhat::Profiler::new_heap);
    #[cfg(not(feature = "dhat-heap"))]
    if args.dhat {
        eprintln!(
            "--dhat ignored: rebuild with `--features dhat-heap` to profile the heap \
             (the default build uses jemalloc, not dhat's allocator)"
        );
    }
    init_tracing();

    let mut config = Config::load(&args.config).context("Failed to load config")?;

    tracing::info!("Loading baseline assignment from {:?}", args.input);
    let mut baseline = baseline::load_baseline(&args.input, &config.min_supported_worker_version)?;

    // Register every copy's destination up front so both schedulers weight it.
    for plan in &run.copies {
        register_copy_dataset(plan, &baseline, &mut config.datasets)?;
    }

    let scheduling_config = SchedulingConfig {
        worker_capacity: config.worker_storage_bytes,
        saturation: config.saturation,
        min_replication: config.min_replication,
        ignore_reliability: config.ignore_reliability,
    };

    // Copies are scheduler-specific: multistep clones them from Postgres, stateless replays them.
    let mut sim_copies = Vec::new();
    let scheduler: Box<dyn StepScheduler> = match run.scheduler {
        Scheduler::Stateless => {
            sim_copies = run.copies;
            let worker_peer_ids = baseline.workers.iter().map(|w| w.id).collect();
            Box::new(stateless::StatelessScheduler::new(
                std::mem::take(&mut baseline.chunk_owners),
                worker_peer_ids,
            ))
        }
        Scheduler::Multistep => match multistep::MultistepScheduler::build(
            &config,
            &mut baseline,
            args.database_url.as_deref(),
            (run.restricted_fraction > 0.0).then(|| format!("s3://{}", run.restricted_dataset)),
            run.replaces,
            run.copies,
            run.confirm_lag_steps,
            run.portal_lag_steps,
            ingestion,
        )? {
            Some(scheduler) => Box::new(scheduler),
            None => {
                // --ingest-only: the database is seeded; stop before scheduling.
                println!("Ingestion complete; database seeded. Stopping (--ingest-only).");
                return Ok(());
            }
        },
    };

    let chunk_rate = match &run.chunks_shape {
        Some(spec) => rate::ChunkRate::parse(spec).context("Failed to parse chunks_shape")?,
        None => rate::ChunkRate::Constant {
            n: run.chunks_per_step,
        },
    };
    let params = SimulationParams {
        steps: run.steps,
        chunk_rate,
        chunk_size: run
            .chunk_size
            .map(|s| s.as_u64().min(u32::MAX as u64) as u32),
        restricted_fraction: run.restricted_fraction,
        restricted_dataset_name: run.restricted_dataset,
        initial_new_fraction: run.initial_new_fraction,
        upgrade_schedule: upgrade::UpgradeSchedule::parse(&run.upgrade_schedule)?,
        lift_restriction_at_step: run.lift_restriction_at_step,
        copy: sim_copies,
    };

    let rng = StdRng::seed_from_u64(run.seed);
    let mut simulation = Simulation::new(
        baseline,
        config.datasets.clone(),
        scheduling_config,
        params,
        scheduler,
        rng,
    );

    println!(
        "Worker version distribution: {}",
        util::format_version_distribution(simulation.workers())
    );
    if run.scheduler == Scheduler::Multistep {
        println!(
            "Confirmation lag: {} worker step(s) + {} portal step(s) before superseded copies drain",
            run.confirm_lag_steps, run.portal_lag_steps
        );
    }

    let all_metrics = simulation.run(|metrics| {
        if let Some(m) = metrics.last() {
            util::print_step(m);
        }
    });
    util::print_table(&all_metrics);

    print_summary(&simulation.summary());

    if let Some(report_path) = &run.report {
        report::generate_html(&all_metrics, report_path)?;
    }

    Ok(())
}

/// Resolved run settings from the `--profile` file (or its defaults).
struct RunConfig {
    chunks_per_step: u32,
    chunks_shape: Option<String>,
    chunk_size: Option<ByteSize>,
    steps: u32,
    seed: u64,
    scheduler: Scheduler,
    report: Option<PathBuf>,
    restricted_fraction: f64,
    restricted_dataset: String,
    upgrade_schedule: String,
    initial_new_fraction: f64,
    lift_restriction_at_step: Option<u32>,
    confirm_lag_steps: u32,
    portal_lag_steps: u32,
    copies: Vec<CopyPlan>,
    replaces: Vec<ReplacePlan>,
}

impl RunConfig {
    /// From a `--profile` file; unset fields fall back to built-in defaults.
    fn from_profile(p: profile::Profile) -> anyhow::Result<Self> {
        let copies = p
            .copy
            .into_iter()
            .map(|e| CopyPlan {
                src: strip_s3(&e.src),
                dst: strip_s3(&e.dst),
                at_step: e.at_step,
            })
            .collect();
        let replaces = p
            .replace
            .into_iter()
            .map(|e| ReplacePlan {
                bucket: strip_s3(&e.dataset),
                at_step: e.at_step,
            })
            .collect();
        let run = RunConfig {
            chunks_per_step: p.chunks_per_step.unwrap_or(1000),
            chunks_shape: p.chunks_shape,
            chunk_size: p.chunk_size,
            steps: p.steps.unwrap_or(10),
            seed: p.seed.unwrap_or(42),
            scheduler: p.scheduler.unwrap_or(Scheduler::Stateless),
            report: p.report,
            restricted_fraction: p.restricted_fraction.unwrap_or(0.0),
            restricted_dataset: p
                .restricted_dataset
                .unwrap_or_else(|| "restricted".to_string()),
            upgrade_schedule: p.upgrade_schedule.unwrap_or_default(),
            initial_new_fraction: p.initial_new_fraction.unwrap_or(0.0),
            lift_restriction_at_step: p.lift_restriction_at_step,
            confirm_lag_steps: p.confirm_lag_steps.unwrap_or(0),
            portal_lag_steps: p.portal_lag_steps.unwrap_or(0),
            copies,
            replaces,
        };
        run.validate()?;
        Ok(run)
    }

    /// Range and consistency checks on the resolved settings.
    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (0.0..=1.0).contains(&self.restricted_fraction),
            "restricted_fraction must be in [0.0, 1.0], got {}",
            self.restricted_fraction
        );
        anyhow::ensure!(
            (0.0..=1.0).contains(&self.initial_new_fraction),
            "initial_new_fraction must be in [0.0, 1.0], got {}",
            self.initial_new_fraction
        );
        let mut seen_dst = std::collections::HashSet::new();
        for c in &self.copies {
            anyhow::ensure!(
                (1..=self.steps).contains(&c.at_step),
                "copy at_step must be in 1..={}, got {}",
                self.steps,
                c.at_step
            );
            anyhow::ensure!(c.src != c.dst, "copy source and destination must differ");
            anyhow::ensure!(
                seen_dst.insert(&c.dst),
                "duplicate copy destination {}",
                c.dst
            );
        }
        anyhow::ensure!(
            self.replaces.is_empty() || self.scheduler == Scheduler::Multistep,
            "replace requires the multistep scheduler"
        );
        anyhow::ensure!(
            (self.confirm_lag_steps == 0 && self.portal_lag_steps == 0)
                || self.scheduler == Scheduler::Multistep,
            "confirm_lag_steps/portal_lag_steps require the multistep scheduler (the stateless \
             path rebuilds the placement each step and has no confirmation watermark)"
        );
        for r in &self.replaces {
            anyhow::ensure!(
                (1..=self.steps).contains(&r.at_step),
                "replace at_step must be in 1..={}, got {}",
                self.steps,
                r.at_step
            );
        }
        Ok(())
    }
}

/// Bucket name without the `s3://` scheme.
fn strip_s3(s: &str) -> String {
    s.trim_start_matches("s3://").to_string()
}

/// Registers the copy's destination dataset in the config (inheriting the source's weight) so both
/// schedulers can weight it, and checks the source exists and the destination does not.
fn register_copy_dataset(
    plan: &CopyPlan,
    baseline: &baseline::Baseline,
    datasets: &mut network_scheduler::cli::DatasetsConfig,
) -> anyhow::Result<()> {
    let src_id = format!("s3://{}", plan.src);
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

fn print_summary(summary: &simulation::Summary) {
    let non_restricted = summary.total_chunks_added - summary.restricted_chunks;
    println!(
        "\nChunks added: {} (version-restricted: {}, non-restricted: {non_restricted})",
        summary.total_chunks_added, summary.restricted_chunks
    );
    println!(
        "Workers upgraded to {}: {} of {}",
        summary.new_worker_version, summary.upgraded_workers, summary.total_workers
    );
}
