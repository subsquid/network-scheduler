//! Simulates chunk ingestion over multiple steps and measures reshuffling impact.
//!
//! Reads a real assignment (flatbuffer), generates new chunks at the head of each
//! dataset proportionally to existing sizes, then runs a scheduler and diffs
//! against the previous step.
//!
//! Two schedulers are available via `--scheduler`: the stateless single-step
//! scheduler (rebuilds the placement each step) and the multistep scheduler
//! (Postgres-backed, reuses the stored placement). Version-restriction and
//! worker-upgrade modeling (`--restricted-fraction`, `--upgrade-schedule`, …)
//! is config-driven — the restricted data lives in a dedicated dataset that
//! carries a `minimum_worker_version` — and honored by both schedulers.
//!
//! Usage:
//!   cargo run -p reshuffle-sim -- -c config.yaml --chunks-per-step 1000 --steps 10 input.fb.gz

mod baseline;
mod chunks;
mod metrics;
mod multistep;
mod rate;
mod report;
mod simulation;
mod stateless;
mod upgrade;
mod util;

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
};

use anyhow::Context;
use bytesize::ByteSize;
use clap::Parser;
use libp2p_identity::PeerId;
use network_scheduler::{cli::Config, scheduling::SchedulingConfig};
use rand::prelude::*;

use crate::simulation::{Simulation, SimulationParams, StepScheduler};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

type ChunkId = (Arc<String>, Arc<String>);
type ChunkOwners = BTreeMap<ChunkId, BTreeSet<PeerId>>;
type ChunkSizeIndex = BTreeMap<ChunkId, u32>;

#[derive(Parser, Debug)]
#[command(about = "Simulate chunk ingestion and measure reshuffling")]
struct Args {
    /// Input assignment file (flatbuffer, optionally .gz)
    #[arg(value_name = "INPUT")]
    input: PathBuf,

    /// Scheduler config file
    #[arg(long, short)]
    config: PathBuf,

    /// Number of new chunks to generate per step (used when --chunks-shape is unset)
    #[arg(long, default_value = "1000")]
    chunks_per_step: u32,

    /// Per-step ingestion shape, overriding --chunks-per-step. Examples:
    /// "triangle:peak=5000", "pulse:size=50000,at=50", "bursts:size=10000,every=5,until=30",
    /// "ramp:from=0,to=2000", "gaussian:peak=5000,sigma=8", "points:1=0,50=5000,100=0"
    #[arg(long)]
    chunks_shape: Option<String>,

    /// Size of each generated chunk (e.g. 256MiB); defaults to the dataset's
    /// average chunk size
    #[arg(long)]
    chunk_size: Option<ByteSize>,

    /// Number of simulation steps
    #[arg(long, default_value = "10")]
    steps: u32,

    /// Random seed for reproducibility
    #[arg(long, default_value = "42")]
    seed: u64,

    /// Which scheduler to run: the stateless single-step scheduler, or the multistep scheduler
    /// backed by Postgres.
    #[arg(long, value_enum, default_value_t = Scheduler::Stateless)]
    scheduler: Scheduler,

    /// Postgres URL for `--scheduler multistep`. If omitted, an ephemeral Postgres container is
    /// started (requires Docker). The target database must be empty — migrations run on connect.
    #[arg(long)]
    database_url: Option<String>,

    /// Proportion (0.0–1.0) of each step's new chunks generated into the restricted dataset
    /// (which requires the new worker version).
    #[arg(long, default_value = "0.0")]
    restricted_fraction: f64,

    /// Bucket name of the synthetic restricted dataset (only used when --restricted-fraction > 0).
    #[arg(long, default_value = "restricted")]
    restricted_dataset: String,

    /// Worker upgrade schedule: ascending `step:fraction` breakpoints, e.g.
    /// "1:0,5:0.25,50:0.5". The fraction of workers on the new version is held
    /// constant between breakpoints.
    #[arg(long, default_value = "")]
    upgrade_schedule: String,

    /// Fraction (0.0–1.0) of workers already on the new version at step 0.
    #[arg(long, default_value = "0.0")]
    initial_new_fraction: f64,

    /// Step at which to lift version restrictions: the restriction is dropped, so the
    /// previously restricted datasets become servable by all workers (no chunks are removed).
    #[arg(long)]
    lift_restriction_at_step: Option<u32>,

    /// Write an HTML report with charts to this path
    #[arg(long)]
    report: Option<PathBuf>,

    /// Enable dhat heap profiling for this run (writes dhat-heap.json on exit).
    /// View with https://nnethercote.github.io/dh_view/dh_view.html
    #[arg(long)]
    dhat: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
enum Scheduler {
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
    anyhow::ensure!(
        (0.0..=1.0).contains(&args.restricted_fraction),
        "--restricted-fraction must be in [0.0, 1.0], got {}",
        args.restricted_fraction
    );
    anyhow::ensure!(
        (0.0..=1.0).contains(&args.initial_new_fraction),
        "--initial-new-fraction must be in [0.0, 1.0], got {}",
        args.initial_new_fraction
    );
    let _profiler = args.dhat.then(dhat::Profiler::new_heap);
    init_tracing();

    let config = Config::load(&args.config).context("Failed to load config")?;

    tracing::info!("Loading baseline assignment from {:?}", args.input);
    let mut baseline = baseline::load_baseline(&args.input, &config.min_supported_worker_version)?;

    let scheduling_config = SchedulingConfig {
        worker_capacity: config.worker_storage_bytes,
        saturation: config.saturation,
        min_replication: config.min_replication,
        ignore_reliability: config.ignore_reliability,
    };

    let scheduler: Box<dyn StepScheduler> = match args.scheduler {
        Scheduler::Stateless => Box::new(stateless::StatelessScheduler::new(
            baseline.chunk_owners.clone(),
        )),
        Scheduler::Multistep => Box::new(multistep::MultistepScheduler::build(
            &config,
            &mut baseline,
            args.database_url.as_deref(),
            (args.restricted_fraction > 0.0).then(|| format!("s3://{}", args.restricted_dataset)),
        )?),
    };

    let chunk_rate = match &args.chunks_shape {
        Some(spec) => rate::ChunkRate::parse(spec).context("Failed to parse --chunks-shape")?,
        None => rate::ChunkRate::Constant {
            n: args.chunks_per_step,
        },
    };
    let params = SimulationParams {
        steps: args.steps,
        chunk_rate,
        chunk_size: args
            .chunk_size
            .map(|s| s.as_u64().min(u32::MAX as u64) as u32),
        restricted_fraction: args.restricted_fraction,
        restricted_dataset_name: args.restricted_dataset,
        initial_new_fraction: args.initial_new_fraction,
        upgrade_schedule: upgrade::UpgradeSchedule::parse(&args.upgrade_schedule)?,
        lift_restriction_at_step: args.lift_restriction_at_step,
    };

    let rng = StdRng::seed_from_u64(args.seed);
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

    let mut live_table = util::LiveTable::new();
    let all_metrics = simulation.run(|metrics| live_table.update(metrics));
    live_table.finish(&all_metrics);

    print_summary(&simulation.summary());

    if let Some(report_path) = &args.report {
        report::generate_html(&all_metrics, report_path)?;
    }

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
