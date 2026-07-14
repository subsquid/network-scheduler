//! Simulates chunk ingestion over multiple steps and measures reshuffling impact.
//!
//! Reads a real assignment (flatbuffer), generates new chunks at the head of each dataset
//! proportionally to existing sizes, then runs a scheduler and diffs against the previous step. The
//! run is described by a `--profile` YAML; see [`profile`].
//!
//! Usage:
//!   cargo run -p reshuffle-sim -- -c config.yaml --profile run.yaml input.fb.gz

mod baseline;
mod chunks;
mod cli;
mod metrics;
mod multistep;
mod profile;
mod rate;
mod report;
mod simulation;
mod stateless;
mod types;
mod upgrade;
mod util;

use anyhow::Context;
use clap::Parser;
use network_scheduler::{cli::Config, scheduling::SchedulingConfig};
use rand::prelude::*;

use crate::profile::Scheduler;
use crate::simulation::{Simulation, SimulationParams, StepScheduler};
use crate::types::dataset_id;

#[cfg(not(feature = "dhat-heap"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn main() -> anyhow::Result<()> {
    let args = cli::Args::parse();
    let run = args.profile()?;
    let ingestion = args.ingestion(&run)?;

    #[cfg(feature = "dhat-heap")]
    let _profiler = args.dhat.then(dhat::Profiler::new_heap);
    #[cfg(not(feature = "dhat-heap"))]
    if args.dhat {
        eprintln!(
            "--dhat ignored: rebuild with `--features dhat-heap` to profile the heap \
             (the default build uses jemalloc, not dhat's allocator)"
        );
    }
    cli::init_tracing();

    let mut config = Config::load(&args.config).context("Failed to load config")?;

    tracing::info!("Loading baseline assignment from {:?}", args.input);
    let mut baseline = baseline::load_baseline(&args.input, &config.min_supported_worker_version)?;

    // Register every copy's destination up front so both schedulers weight it.
    let copies = run.copy_plans();
    for plan in &copies {
        simulation::register_copy_dataset(plan, &baseline, &mut config.datasets)?;
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
            sim_copies = copies;
            let worker_peer_ids = baseline.workers.iter().map(|w| w.id).collect();
            Box::new(stateless::StatelessScheduler::new(
                std::mem::take(&mut baseline.chunk_owners),
                worker_peer_ids,
            ))
        }
        Scheduler::Multistep => {
            let setup = multistep::Setup {
                config: &config,
                database_url: args.database_url.as_deref(),
                restricted_dataset: (run.restricted_fraction > 0.0)
                    .then(|| dataset_id(&run.restricted_dataset)),
                replace: run.replace_plans(),
                copy: copies,
                confirm_lag_steps: run.confirm_lag_steps,
                portal_lag_steps: run.portal_lag_steps,
                ingestion,
            };
            match multistep::MultistepScheduler::build(setup, &mut baseline)? {
                Some(scheduler) => Box::new(scheduler),
                None => {
                    // --ingest-only: the database is seeded; stop before scheduling.
                    println!("Ingestion complete; database seeded. Stopping (--ingest-only).");
                    return Ok(());
                }
            }
        }
    };

    let params = SimulationParams::from_profile(&run, sim_copies)?;
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
    util::print_summary(&simulation.summary());

    if let Some(report_path) = &run.report {
        report::generate_html(&all_metrics, report_path)?;
    }

    Ok(())
}
