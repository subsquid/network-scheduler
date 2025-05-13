use std::collections::{BTreeMap, BTreeSet};

use itertools::Itertools;
use libp2p_identity::PeerId;

use crate::{
    scheduling::WeightedChunk,
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
    chunks: &[WeightedChunk],
    assignment1: &Assignment,
    assignment2: &Assignment,
) -> CompareResult {
    let mut result = CompareResult::default();
    for (worker_id, chunks1) in &assignment1.workers {
        let Some(chunks2) = assignment2.workers.get(worker_id) else {
            continue;
        };
        let chunks1 = chunks1.iter().copied().collect::<BTreeSet<_>>();
        let chunks2 = chunks2.iter().copied().collect::<BTreeSet<_>>();
        let removed_chunks = chunks1.difference(&chunks2).copied().collect_vec();
        let added_chunks = chunks2.difference(&chunks1).copied().collect_vec();

        let removed = calculate_size(&chunks, removed_chunks.iter().copied());
        let added = calculate_size(&chunks, added_chunks.iter().copied());
        let before = calculate_size(&chunks, chunks1.iter().copied());
        let after = calculate_size(&chunks, chunks2.iter().copied());
        result.removed.insert(worker_id.clone(), removed);
        result.added.insert(worker_id.clone(), added);
        result.before.insert(worker_id.clone(), before);
        result.after.insert(worker_id.clone(), after);
    }

    result
}

fn calculate_size(chunks: &[WeightedChunk], indexes: impl Iterator<Item = ChunkIndex>) -> u64 {
    indexes
        .map(|chunk_index| chunks[chunk_index as usize].size as u64)
        .sum()
}
