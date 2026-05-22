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
mod util;

use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
};

use anyhow::Context;
use clap::Parser;
use libp2p_identity::PeerId;
use network_scheduler::{cli::Config, scheduling::SchedulingConfig, types::Chunk};
use rand::prelude::*;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

type ChunkId = (Arc<String>, Arc<String>);
type ChunkIndex = HashMap<ChunkId, ChunkEntry>;

pub struct ChunkEntry {
    pub owners: HashSet<PeerId>,
    pub size: u32,
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

    /// Number of new chunks to generate per step
    #[arg(long, default_value = "1000")]
    chunks_per_step: u32,

    /// Number of simulation steps
    #[arg(long, default_value = "10")]
    steps: u32,

    /// Random seed for reproducibility
    #[arg(long, default_value = "42")]
    seed: u64,

    /// Write an HTML report with charts to this path
    #[arg(long, default_missing_value = "report.html", num_args = 0..=1)]
    report: Option<PathBuf>,

    /// Enable dhat heap profiling for this run (writes dhat-heap.json on exit).
    /// View with https://nnethercote.github.io/dh_view/dh_view.html
    #[arg(long)]
    dhat: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
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

    let mut rng = StdRng::seed_from_u64(args.seed);
    let mut accumulated_new_chunks: Vec<Chunk> = Vec::new();
    let mut previous_index = baseline.chunk_index;
    let mut all_metrics = Vec::new();

    util::print_header();

    for step in 1..=args.steps {
        let step_chunks =
            simulation::generate_new_chunks(&mut baseline.datasets, args.chunks_per_step, &mut rng);
        let new_count = step_chunks.len() as u32;
        accumulated_new_chunks.extend(step_chunks);

        let (filtered_chunks, assignment) = simulation::schedule_combined_chunks(
            &baseline.chunks,
            &accumulated_new_chunks,
            &baseline.workers,
            &config.datasets,
            &scheduling_config,
        )?;

        let (metrics, current_index) = simulation::measure_reshuffle(
            &previous_index,
            &filtered_chunks,
            &assignment,
            step,
            new_count,
            baseline.workers.len(),
            scheduling_config.worker_capacity,
        );

        util::print_step(&metrics);
        all_metrics.push(metrics);

        previous_index = current_index;
    }

    if let Some(report_path) = &args.report {
        report::generate_html(&all_metrics, report_path)?;
    }

    Ok(())
}
