/// Simulates chunk ingestion over multiple steps and measures reshuffling impact.
///
/// Reads a real assignment (flatbuffer), generates new chunks at the head of each
/// dataset proportionally to existing sizes, then runs the scheduler and diffs
/// against the original assignment.
///
/// Usage:
///   cargo run -p reshuffle-sim -- -c config.yaml --chunks-per-step 1000 --steps 10 input.fb.gz
mod baseline;
mod report;
mod simulation;
mod upgrade;
mod util;

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    path::PathBuf,
    sync::Arc,
};

use anyhow::Context;
use bytesize::ByteSize;
use clap::Parser;
use libp2p_identity::PeerId;
use network_scheduler::{cli::Config, scheduling::SchedulingConfig, types::Chunk};
use rand::prelude::*;

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

    /// Number of new chunks to generate per step
    #[arg(long, default_value = "1000")]
    chunks_per_step: u32,

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

    /// Fraction (0.0–1.0) of each step's new chunks that require the new worker version
    #[arg(long, default_value = "0.0")]
    restricted_fraction: f64,

    /// Worker upgrade schedule: ascending `step:fraction` breakpoints, e.g.
    /// "1:0,5:0.25,50:0.5". The fraction of workers on the new version is held
    /// constant between breakpoints.
    #[arg(long, default_value = "")]
    upgrade_schedule: String,

    /// Fraction (0.0–1.0) of workers already on the new version at step 0
    #[arg(long, default_value = "0.0")]
    initial_new_fraction: f64,

    /// Step at which to lift version restrictions: previously restricted chunks
    /// are removed and no further restricted chunks are generated (new
    /// unrestricted chunks keep being generated).
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
    let config = Config::load(&args.config).context("Failed to load config")?;

    eprintln!("Loading baseline assignment from {:?}", args.input);
    let mut baseline = baseline::load_baseline(&args.input, &config.min_supported_worker_version)?;

    let scheduling_config = SchedulingConfig {
        worker_capacity: config.worker_storage_bytes,
        saturation: config.saturation,
        min_replication: config.min_replication,
        ignore_reliability: config.ignore_reliability,
    };

    let chunk_size = args
        .chunk_size
        .map(|s| s.as_u64().min(u32::MAX as u64) as u32);

    let mut rng = StdRng::seed_from_u64(args.seed);
    let mut accumulated_new_chunks: Vec<Chunk> = Vec::new();
    let mut previous_chunk_owners = baseline.chunk_owners;
    let mut all_metrics = Vec::new();

    let new_worker_version = upgrade::new_version();
    let upgrade_schedule = upgrade::UpgradeSchedule::parse(&args.upgrade_schedule)?;
    let mut version_state = upgrade::WorkerVersionState::new(
        baseline.workers.len(),
        args.initial_new_fraction,
        &mut rng,
    );
    let mut restricted_ids: HashSet<ChunkId> = HashSet::new();

    // Stamp the initial fleet versions before reporting the starting distribution.
    version_state.apply(&mut baseline.workers, &new_worker_version);
    println!(
        "Worker version distribution: {}",
        util::format_version_distribution(&baseline.workers)
    );

    let mut live_table = util::LiveTable::new();

    for step in 1..=args.steps {
        // At the lift step, remove the previously-restricted chunks and stop
        // tagging new ones; unrestricted chunks keep being generated.
        let lifted = args.lift_restriction_at_step.is_some_and(|n| step >= n);
        if args.lift_restriction_at_step == Some(step) {
            accumulated_new_chunks
                .retain(|c| !restricted_ids.contains(&(c.dataset.clone(), c.id.clone())));
            restricted_ids.clear();
        }
        let restricted_fraction = if lifted {
            0.0
        } else {
            args.restricted_fraction
        };

        let (step_chunks, step_restricted) = simulation::generate_new_chunks(
            &mut baseline.datasets,
            args.chunks_per_step,
            restricted_fraction,
            chunk_size,
            &mut rng,
        );
        let new_count = step_chunks.len() as u32;
        let new_restricted = step_restricted.len() as u32;
        accumulated_new_chunks.extend(step_chunks);
        restricted_ids.extend(step_restricted);

        version_state.advance(&upgrade_schedule, step);
        version_state.apply(&mut baseline.workers, &new_worker_version);
        let eligible_workers = version_state.eligible_count();

        let (filtered_chunks, schedule_result) = simulation::schedule_combined_chunks(
            &baseline.chunks,
            &accumulated_new_chunks,
            &baseline.workers,
            &config.datasets,
            &scheduling_config,
            &restricted_ids,
            &new_worker_version,
        );

        match schedule_result {
            Ok(assignment) => {
                let (metrics, current_chunk_owners) = simulation::measure_reshuffle(
                    &previous_chunk_owners,
                    &filtered_chunks,
                    &assignment,
                    step,
                    new_count,
                    new_restricted,
                    baseline.workers.len(),
                    scheduling_config.worker_capacity,
                    &restricted_ids,
                    eligible_workers,
                );
                previous_chunk_owners = current_chunk_owners;
                all_metrics.push(metrics);
                live_table.update(&all_metrics);
            }
            // Scheduling failed (panicked on infeasible version restrictions);
            // record the step and stop.
            Err(reason) => {
                let metrics = simulation::failed_step_metrics(
                    step,
                    new_count,
                    new_restricted,
                    filtered_chunks.len(),
                    restricted_ids.len(),
                    baseline.workers.len(),
                    scheduling_config.worker_capacity,
                    eligible_workers,
                    reason.clone(),
                );
                all_metrics.push(metrics);
                live_table.update(&all_metrics);
                eprintln!("Scheduling failed at step {step}: {reason}. Stopping simulation.");
                break;
            }
        }
    }

    live_table.finish(&all_metrics);

    let total_added = accumulated_new_chunks.len();
    let restricted_added = restricted_ids.len();
    println!(
        "\nChunks added: {total_added} (version-restricted: {restricted_added}, non-restricted: {})",
        total_added - restricted_added
    );
    println!(
        "Workers upgraded to {new_worker_version}: {} of {}",
        version_state.eligible_count(),
        baseline.workers.len()
    );

    if let Some(report_path) = &args.report {
        report::generate_html(&all_metrics, report_path)?;
    }

    Ok(())
}
