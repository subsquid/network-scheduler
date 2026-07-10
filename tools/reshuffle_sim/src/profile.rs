//! The `--profile` YAML: a whole run (steps, scheduler, copy/replace, …) in one file, as an
//! alternative to the tuning flags. When set, those flags are ignored.

use std::path::{Path, PathBuf};

use anyhow::Context;
use bytesize::ByteSize;
use serde::Deserialize;

use crate::Scheduler;

/// Run parameters; each field defaults to its flag's default. `copy`/`replace` are lists so a run
/// can do several. Unknown keys are rejected.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Profile {
    pub(crate) chunks_per_step: Option<u32>,
    pub(crate) chunks_shape: Option<String>,
    pub(crate) chunk_size: Option<ByteSize>,
    pub(crate) steps: Option<u32>,
    pub(crate) seed: Option<u64>,
    pub(crate) scheduler: Option<Scheduler>,
    pub(crate) report: Option<PathBuf>,
    pub(crate) restricted_fraction: Option<f64>,
    pub(crate) restricted_dataset: Option<String>,
    pub(crate) upgrade_schedule: Option<String>,
    pub(crate) initial_new_fraction: Option<f64>,
    pub(crate) lift_restriction_at_step: Option<u32>,
    #[serde(default)]
    pub(crate) copy: Vec<CopyEntry>,
    #[serde(default)]
    pub(crate) replace: Vec<ReplaceEntry>,
}

/// Clone dataset `src` into a fresh `dst` at `at_step`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CopyEntry {
    pub(crate) src: String,
    pub(crate) dst: String,
    pub(crate) at_step: u32,
}

/// Replace every chunk of `dataset` with a same-range copy at `at_step` (multistep only).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReplaceEntry {
    pub(crate) dataset: String,
    pub(crate) at_step: u32,
}

impl Profile {
    pub(crate) fn load(path: &Path) -> anyhow::Result<Self> {
        let file = std::fs::File::open(path)
            .with_context(|| format!("opening profile {}", path.display()))?;
        serde_yaml::from_reader(std::io::BufReader::new(file))
            .with_context(|| format!("parsing profile {}", path.display()))
    }
}
