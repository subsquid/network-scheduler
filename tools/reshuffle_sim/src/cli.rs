//! Command-line arguments: what the run needs from outside the `--profile` (input, config, database),
//! plus the checks that the flags and the profile agree.

use std::path::PathBuf;

use clap::Parser;

use crate::multistep::Ingestion;
use crate::profile::{Profile, Scheduler};

#[derive(Parser, Debug)]
#[command(about = "Simulate chunk ingestion and measure reshuffling")]
pub(crate) struct Args {
    /// Input assignment file (flatbuffer, optionally .gz)
    #[arg(value_name = "INPUT")]
    pub(crate) input: PathBuf,

    /// Scheduler config file
    #[arg(long, short)]
    pub(crate) config: PathBuf,

    /// Run parameters as a YAML file (steps, scheduler, copy/replace, …). Defaults apply when unset.
    #[arg(long)]
    pub(crate) profile: Option<PathBuf>,

    /// Postgres URL for the multistep scheduler. If omitted, an ephemeral Postgres container is
    /// started (requires Docker). The database must be empty for a normal run (migrations run on
    /// connect); pass `--skip-ingest` to reuse an already-seeded one.
    #[arg(long)]
    pub(crate) database_url: Option<String>,

    /// Seed the baseline datasets, workers and chunks into `--database-url`, then stop before
    /// scheduling — snapshot the database here and reuse it with `--skip-ingest`. Multistep +
    /// `--database-url` only.
    #[arg(long, conflicts_with = "skip_ingest")]
    pub(crate) ingest_only: bool,

    /// Skip baseline ingestion and reuse the data already in `--database-url` (e.g. a restored
    /// `--ingest-only` snapshot), going straight to scheduling. Multistep + `--database-url` only.
    #[arg(long)]
    pub(crate) skip_ingest: bool,

    /// Enable dhat heap profiling for this run (writes dhat-heap.json on exit).
    /// Requires building with `--features dhat-heap`; ignored otherwise.
    /// View with https://nnethercote.github.io/dh_view/dh_view.html
    #[arg(long)]
    pub(crate) dhat: bool,
}

impl Args {
    /// The run's `--profile`, or the built-in defaults when it has none.
    pub(crate) fn profile(&self) -> anyhow::Result<Profile> {
        match &self.profile {
            Some(path) => Profile::load(path),
            None => Ok(Profile::default()),
        }
    }

    /// What to do with the baseline data, and the checks that the ingestion flags suit `run`: both
    /// snapshot the database, which only the multistep path has.
    pub(crate) fn ingestion(&self, run: &Profile) -> anyhow::Result<Ingestion> {
        let ingestion = if self.ingest_only {
            Ingestion::Only
        } else if self.skip_ingest {
            Ingestion::Skip
        } else {
            return Ok(Ingestion::Run);
        };
        anyhow::ensure!(
            run.scheduler == Scheduler::Multistep,
            "--ingest-only/--skip-ingest require the multistep scheduler"
        );
        anyhow::ensure!(
            self.database_url.is_some(),
            "--ingest-only/--skip-ingest require --database-url (a persistent database to snapshot and restore)"
        );
        Ok(ingestion)
    }
}

pub(crate) fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
}
