//! The `--profile` YAML: a whole run (steps, scheduler, copy/replace, …) in one file. Every field is
//! optional; missing ones take the value from [`Profile::default`], so an absent `--profile` and an
//! empty one mean the same thing.

use std::path::{Path, PathBuf};

use anyhow::Context;
use bytesize::ByteSize;
use serde::Deserialize;

use crate::multistep::ReplacePlan;
use crate::simulation::CopyPlan;
use crate::types::bucket_of;

/// Which scheduler the run drives.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Scheduler {
    /// Rebuild the whole placement from scratch each step (`src/scheduling.rs`).
    Stateless,
    /// Keep the stored placement and move only what's needed (multistep, backed by Postgres).
    Multistep,
}

/// Run parameters. `copy`/`replace` are lists so a run can do several. Unknown keys are rejected.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub(crate) struct Profile {
    pub(crate) chunks_per_step: u32,
    pub(crate) chunks_shape: Option<String>,
    pub(crate) chunk_size: Option<ByteSize>,
    pub(crate) steps: u32,
    pub(crate) seed: u64,
    pub(crate) scheduler: Scheduler,
    pub(crate) report: Option<PathBuf>,
    pub(crate) restricted_fraction: f64,
    pub(crate) restricted_dataset: String,
    pub(crate) upgrade_schedule: String,
    pub(crate) initial_new_fraction: f64,
    pub(crate) lift_restriction_at_step: Option<u32>,
    pub(crate) confirm_lag_steps: u32,
    pub(crate) portal_lag_steps: u32,
    pub(crate) copy: Vec<CopyEntry>,
    pub(crate) replace: Vec<ReplaceEntry>,
}

impl Default for Profile {
    fn default() -> Self {
        Self {
            chunks_per_step: 1000,
            chunks_shape: None,
            chunk_size: None,
            steps: 10,
            seed: 42,
            scheduler: Scheduler::Stateless,
            report: None,
            restricted_fraction: 0.0,
            restricted_dataset: "restricted".to_string(),
            upgrade_schedule: String::new(),
            initial_new_fraction: 0.0,
            lift_restriction_at_step: None,
            confirm_lag_steps: 0,
            portal_lag_steps: 0,
            copy: Vec::new(),
            replace: Vec::new(),
        }
    }
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
        let profile: Profile = serde_yaml::from_reader(std::io::BufReader::new(file))
            .with_context(|| format!("parsing profile {}", path.display()))?;
        profile.validate()?;
        Ok(profile)
    }

    /// Plans in the schedulers' own terms: buckets, not `s3://` ids.
    pub(crate) fn copy_plans(&self) -> Vec<CopyPlan> {
        self.copy
            .iter()
            .map(|e| CopyPlan {
                src: bucket_of(&e.src).to_string(),
                dst: bucket_of(&e.dst).to_string(),
                at_step: e.at_step,
            })
            .collect()
    }

    pub(crate) fn replace_plans(&self) -> Vec<ReplacePlan> {
        self.replace
            .iter()
            .map(|e| ReplacePlan {
                bucket: bucket_of(&e.dataset).to_string(),
                at_step: e.at_step,
            })
            .collect()
    }

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
        for c in &self.copy {
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
            self.replace.is_empty() || self.scheduler == Scheduler::Multistep,
            "replace requires the multistep scheduler"
        );
        anyhow::ensure!(
            (self.confirm_lag_steps == 0 && self.portal_lag_steps == 0)
                || self.scheduler == Scheduler::Multistep,
            "confirm_lag_steps/portal_lag_steps require the multistep scheduler (the stateless \
             path rebuilds the placement each step and has no confirmation watermark)"
        );
        for r in &self.replace {
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
