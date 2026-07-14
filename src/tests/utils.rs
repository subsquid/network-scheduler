use std::collections::{BTreeMap, BTreeSet};

use itertools::Itertools;
use libp2p_identity::PeerId;

use crate::{
    scheduling::ScheduledChunk,
    types::{Assignment, ChunkIndex},
};

pub fn std(values: impl Iterator<Item = u64> + Clone) -> f64 {
    let num = values.clone().count();
    let avg = values.clone().sum::<u64>() as f64 / num as f64;
    let variance = values.map(|x| (x as f64 - avg).powi(2)).sum::<f64>() / num as f64;
    variance.sqrt()
}

pub struct Stats {
    pub avg: f64,
    pub std: f64,
    pub max: u64,
    pub min: u64,
}

impl Stats {
    pub fn new(values: impl Iterator<Item = impl Into<u64>> + Clone) -> Self {
        let values = values.map(Into::into);
        let num = values.clone().count();
        let sum = values.clone().sum::<u64>() as f64;
        let avg = sum / num as f64;
        let std = std(values.clone());
        let max = values.clone().max().unwrap();
        let min = values.clone().min().unwrap();
        Stats { avg, std, max, min }
    }

    pub fn format(&self, unit: &str, scale: u64) -> String {
        format!(
            "avg: {:.2}{unit}, std: {:.2}%, max: {}{unit} ({:.2}%), min: {}{unit} ({:.2}%)",
            self.avg / scale as f64,
            self.std / self.avg * 100.,
            self.max / scale,
            self.max as f64 / self.avg * 100.,
            self.min / scale,
            self.min as f64 / self.avg * 100.,
        )
    }
}

#[derive(Debug, Default)]
pub struct CompareResult {
    pub removed: BTreeMap<PeerId, u64>,
    pub added: BTreeMap<PeerId, u64>,
    pub before: BTreeMap<PeerId, u64>,
    pub after: BTreeMap<PeerId, u64>,
}

impl CompareResult {
    pub fn display_stats(&self, unit: &str, scale: u64) {
        let removed = Stats::new(self.removed.values().copied());
        let added = Stats::new(self.added.values().copied());
        let before = Stats::new(self.before.values().copied());
        let after = Stats::new(self.after.values().copied());

        println!("Removed: {}", removed.format(unit, scale));
        println!("Added: {}", added.format(unit, scale));
        println!("Before: {}", before.format(unit, scale));
        println!("After: {}", after.format(unit, scale));
    }
}

pub fn compare_intersection(
    chunks: &[ScheduledChunk<'_>],
    assignment1: &Assignment,
    assignment2: &Assignment,
) -> CompareResult {
    let mut result = CompareResult::default();
    for (worker_id, chunks1) in &assignment1.worker_chunks {
        let Some(chunks2) = assignment2.worker_chunks.get(worker_id) else {
            continue;
        };
        let chunks1 = chunks1.iter().copied().collect::<BTreeSet<_>>();
        let chunks2 = chunks2.iter().copied().collect::<BTreeSet<_>>();
        let removed_chunks = chunks1.difference(&chunks2).copied().collect_vec();
        let added_chunks = chunks2.difference(&chunks1).copied().collect_vec();

        let removed = calculate_size(chunks, removed_chunks.iter().copied());
        let added = calculate_size(chunks, added_chunks.iter().copied());
        let before = calculate_size(chunks, chunks1.iter().copied());
        let after = calculate_size(chunks, chunks2.iter().copied());
        result.removed.insert(*worker_id, removed);
        result.added.insert(*worker_id, added);
        result.before.insert(*worker_id, before);
        result.after.insert(*worker_id, after);
    }

    result
}

fn calculate_size(chunks: &[ScheduledChunk<'_>], indexes: impl Iterator<Item = ChunkIndex>) -> u64 {
    indexes
        .map(|chunk_index| chunks[chunk_index as usize].size as u64)
        .sum()
}

/// Per-worker reshuffling analysis for a scenario where the worker set may change.
/// Compares two assignments and produces detailed per-worker statistics.
#[allow(dead_code)] // analysis helper, not yet wired into a test
pub struct ReshuffleReport {
    /// For each worker present in BOTH assignments:
    /// (worker_id, bytes_before, bytes_after, bytes_removed, bytes_added)
    pub per_worker: Vec<WorkerReshuffle>,
    /// Workers only in assignment1 (departed)
    pub departed_workers: usize,
    /// Workers only in assignment2 (new)
    pub new_workers: usize,
}

#[allow(dead_code)] // analysis helper, not yet wired into a test
pub struct WorkerReshuffle {
    pub worker: PeerId,
    pub bytes_before: u64,
    pub bytes_after: u64,
    pub bytes_removed: u64,
    pub bytes_added: u64,
}

#[allow(dead_code)] // analysis helper, not yet wired into a test
impl WorkerReshuffle {
    /// Bytes evicted from this worker, as a percentage of the bytes it held
    /// before the reshuffle. Returns 0 if the worker held nothing before.
    pub fn removed_percentage(&self) -> f64 {
        if self.bytes_before == 0 {
            return 0.;
        }
        self.bytes_removed as f64 / self.bytes_before as f64 * 100.
    }

    /// Bytes newly assigned to this worker, as a percentage of the bytes it
    /// held before the reshuffle. Returns 0 if the worker held nothing before.
    pub fn added_percentage(&self) -> f64 {
        if self.bytes_before == 0 {
            return 0.;
        }
        self.bytes_added as f64 / self.bytes_before as f64 * 100.
    }

    /// Total churn (removed + added bytes), as a percentage of the bytes this
    /// worker held before the reshuffle. This is the amount of work the worker
    /// must do (delete + download) to converge to the new assignment.
    /// Returns 0 if the worker held nothing before.
    pub fn churn_percentage(&self) -> f64 {
        if self.bytes_before == 0 {
            return 0.;
        }
        (self.bytes_removed + self.bytes_added) as f64 / self.bytes_before as f64 * 100.
    }
}

#[allow(dead_code)] // analysis helper, not yet wired into a test
impl ReshuffleReport {
    pub fn analyze(chunks: &[ScheduledChunk], a1: &Assignment, a2: &Assignment) -> Self {
        let workers_1: BTreeSet<_> = a1.worker_chunks.keys().collect();
        let workers_2: BTreeSet<_> = a2.worker_chunks.keys().collect();
        let common: BTreeSet<_> = workers_1.intersection(&workers_2).copied().collect();

        let departed_workers = workers_1.difference(&workers_2).count();
        let new_workers = workers_2.difference(&workers_1).count();

        let mut per_worker = Vec::new();
        for &&worker in &common {
            let c1: BTreeSet<_> = a1.worker_chunks[&worker].iter().copied().collect();
            let c2: BTreeSet<_> = a2.worker_chunks[&worker].iter().copied().collect();

            let removed: BTreeSet<_> = c1.difference(&c2).copied().collect();
            let added: BTreeSet<_> = c2.difference(&c1).copied().collect();

            per_worker.push(WorkerReshuffle {
                worker,
                bytes_before: calculate_size(chunks, c1.iter().copied()),
                bytes_after: calculate_size(chunks, c2.iter().copied()),
                bytes_removed: calculate_size(chunks, removed.iter().copied()),
                bytes_added: calculate_size(chunks, added.iter().copied()),
            });
        }

        ReshuffleReport {
            per_worker,
            departed_workers,
            new_workers,
        }
    }

    pub fn workers_with_any_churn(&self) -> usize {
        self.per_worker
            .iter()
            .filter(|w| w.bytes_removed > 0 || w.bytes_added > 0)
            .count()
    }

    pub fn total_workers(&self) -> usize {
        self.per_worker.len()
    }

    pub fn print_summary(&self, label: &str) {
        let total = self.total_workers();
        let affected = self.workers_with_any_churn();
        let removed_pcts: Vec<f64> = self
            .per_worker
            .iter()
            .map(|w| w.removed_percentage())
            .collect();
        let added_pcts: Vec<f64> = self
            .per_worker
            .iter()
            .map(|w| w.added_percentage())
            .collect();
        let churn_pcts: Vec<f64> = self
            .per_worker
            .iter()
            .map(|w| w.churn_percentage())
            .collect();

        let avg = |v: &[f64]| v.iter().sum::<f64>() / v.len() as f64;
        let max = |v: &[f64]| v.iter().cloned().fold(0f64, f64::max);
        let min_nonzero = |v: &[f64]| {
            v.iter()
                .cloned()
                .filter(|x| *x > 0.)
                .fold(f64::INFINITY, f64::min)
        };

        println!("=== {label} ===");
        println!(
            "  Workers: {total} common, {affected} affected ({:.1}%), {} departed, {} new",
            affected as f64 / total as f64 * 100.,
            self.departed_workers,
            self.new_workers,
        );
        if affected > 0 {
            println!(
                "  Removed: avg {:.1}%, max {:.1}%, min(nonzero) {:.1}%",
                avg(&removed_pcts),
                max(&removed_pcts),
                min_nonzero(&removed_pcts),
            );
            println!(
                "  Added:   avg {:.1}%, max {:.1}%, min(nonzero) {:.1}%",
                avg(&added_pcts),
                max(&added_pcts),
                min_nonzero(&added_pcts),
            );
            println!(
                "  Total churn (removed+added): avg {:.1}%, max {:.1}%",
                avg(&churn_pcts),
                max(&churn_pcts),
            );

            let total_removed: u64 = self.per_worker.iter().map(|w| w.bytes_removed).sum();
            let total_added: u64 = self.per_worker.iter().map(|w| w.bytes_added).sum();
            let total_before: u64 = self.per_worker.iter().map(|w| w.bytes_before).sum();
            println!(
                "  Aggregate: {:.0}GB removed, {:.0}GB added out of {:.0}GB total",
                total_removed as f64 / (1u64 << 30) as f64,
                total_added as f64 / (1u64 << 30) as f64,
                total_before as f64 / (1u64 << 30) as f64,
            );
        } else {
            println!("  No churn.");
        }
    }
}
