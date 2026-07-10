use std::collections::BTreeMap;
use std::time::Duration;

use bytesize::ByteSize;
use network_scheduler::types::Worker;
use semver::Version;
use tabled::builder::Builder;
use tabled::settings::span::{ColumnSpan, RowSpan};
use tabled::settings::{Alignment, Modify, Style};

use crate::metrics::ReshuffleMetrics;

/// Counts workers by version (ascending), rendering a missing version as
/// "none". Example: `"1.0.0:50 2.0.0:17"`.
pub fn format_version_distribution(workers: &[Worker]) -> String {
    let mut dist: BTreeMap<Option<Version>, usize> = BTreeMap::new();
    for worker in workers {
        *dist.entry(worker.version.clone()).or_insert(0) += 1;
    }
    dist.into_iter()
        .map(|(version, count)| match version {
            Some(v) => format!("{v}:{count}"),
            None => format!("none:{count}"),
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Human-readable scheduling time, in milliseconds with one decimal.
fn format_duration(d: Duration) -> String {
    format!("{:.1} ms", d.as_secs_f64() * 1000.0)
}

/// Index of the first of the three "Workers" sub-columns (new / lost / shuffled).
const WORKERS_COL: usize = 11;
/// Single-value columns whose header should span both header rows.
const SINGLE_COLS: [usize; 14] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 14, 15, 16];

/// Builds the metrics table. The three worker counts sit under one spanning
/// `Workers` header (`new / lost / shuffled`); other headers are stacked across
/// lines so each column is only as wide as its longest word.
fn build_table(metrics: &[ReshuffleMetrics]) -> tabled::Table {
    let mut builder = Builder::default();
    builder.push_record([
        "Step",
        "New\nchunks\n(restr)",
        "Total\nchunks\n(restr)",
        "Repl.",
        "New\ndownload",
        "Shuffled\nchunks\n(download)",
        "Repl chg\n+gain/\n-freed",
        "Total\ndownload",
        "Restr.\ndownload",
        "Free cap\n(used %)",
        "Stale\n(% cap)",
        "Workers",
        "",
        "",
        "Upgraded\nworkers",
        "Sched.",
        "Sched\ntime",
    ]);
    builder.push_record([
        "", "", "", "", "", "", "", "", "", "", "", "new", "lost", "shuffled", "", "", "",
    ]);

    for m in metrics {
        let dm = &m.data_movement;
        let total_download = dm.total_download();
        let free_capacity = m.total_capacity_bytes.saturating_sub(m.used_capacity_bytes);
        let used_pct = if m.total_capacity_bytes > 0 {
            m.used_capacity_bytes as f64 / m.total_capacity_bytes as f64 * 100.0
        } else {
            0.0
        };
        let stale_pct = if m.total_capacity_bytes > 0 {
            m.stale_capacity_bytes as f64 / m.total_capacity_bytes as f64 * 100.0
        } else {
            0.0
        };
        let restricted_download = m.restricted_movement.total_download();
        let replication = m
            .replication_by_weight
            .iter()
            .map(|(weight, factor)| format!("{weight}:{factor}"))
            .collect::<Vec<_>>()
            .join(", ");

        builder.push_record([
            m.step.to_string(),
            format!("{} ({})", m.new_chunks_in_step, m.new_restricted_in_step),
            format!("{} ({})", m.total_chunks, m.total_restricted_chunks),
            replication,
            ByteSize(dm.new_chunk_bytes).to_string(),
            {
                let mut cell = format!("{} ({})", dm.shuffled_count, ByteSize(dm.shuffled_bytes));
                if dm.refetched_count > 0 {
                    cell += &format!(
                        " +rf {} ({})",
                        dm.refetched_count,
                        ByteSize(dm.refetched_bytes)
                    );
                }
                cell
            },
            format!(
                "+{} / -{}",
                ByteSize(dm.increased_replication_bytes),
                ByteSize(dm.decreased_replication_bytes)
            ),
            ByteSize(total_download).to_string(),
            ByteSize(restricted_download).to_string(),
            format!("{} ({used_pct:.1}%)", ByteSize(free_capacity)),
            format!("{stale_pct:.1}%"),
            dm.workers_receiving_new.to_string(),
            dm.workers_losing.to_string(),
            dm.workers_shuffled.to_string(),
            m.eligible_workers.to_string(),
            if m.scheduled { "yes" } else { "NO" }.to_string(),
            format_duration(m.schedule_duration),
        ]);
    }

    let mut table = builder.build();
    table.with(Style::modern()).with(Alignment::right());
    // "Workers" spans its three sub-columns and is centered above them.
    table.with(Modify::new((0, WORKERS_COL)).with(ColumnSpan::new(3)));
    table.with(Modify::new((0, WORKERS_COL)).with(Alignment::center()));
    // Single-value headers span both header rows so they aren't split.
    for col in SINGLE_COLS {
        table.with(Modify::new((0, col)).with(RowSpan::new(2)));
    }
    table
}

/// One-line summary of a step, printed as it finishes. A per-step record in the log means a run
/// killed mid-way (e.g. OOM) still shows every step it completed, not just the final table.
pub fn print_step(m: &ReshuffleMetrics) {
    let dm = &m.data_movement;
    let total_download = dm.total_download();
    let free_capacity = m.total_capacity_bytes.saturating_sub(m.used_capacity_bytes);
    let pct = |n: u64| {
        if m.total_capacity_bytes > 0 {
            n as f64 / m.total_capacity_bytes as f64 * 100.0
        } else {
            0.0
        }
    };
    println!(
        "step {} | new {} (r{}) | total {} (r{}) | download {} (new {}, shuffled {}/{}, refetch {}/{}, repl +{}/-{}) \
         | free {} ({:.1}% used) | stale {:.1}% | workers +{}/-{}/{} | upgraded {} | sched {} {}",
        m.step,
        m.new_chunks_in_step,
        m.new_restricted_in_step,
        m.total_chunks,
        m.total_restricted_chunks,
        ByteSize(total_download),
        ByteSize(dm.new_chunk_bytes),
        dm.shuffled_count,
        ByteSize(dm.shuffled_bytes),
        dm.refetched_count,
        ByteSize(dm.refetched_bytes),
        ByteSize(dm.increased_replication_bytes),
        ByteSize(dm.decreased_replication_bytes),
        ByteSize(free_capacity),
        pct(m.used_capacity_bytes),
        pct(m.stale_capacity_bytes),
        dm.workers_receiving_new,
        dm.workers_losing,
        dm.workers_shuffled,
        m.eligible_workers,
        if m.scheduled { "yes" } else { "NO" },
        format_duration(m.schedule_duration),
    );
}

/// Prints the full metrics table once — the end-of-run summary.
pub fn print_table(metrics: &[ReshuffleMetrics]) {
    println!("{}", build_table(metrics));
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p_identity::PeerId;
    use network_scheduler::types::WorkerStatus;

    fn worker(version: Option<Version>) -> Worker {
        Worker {
            id: PeerId::random(),
            status: WorkerStatus::Online,
            version,
        }
    }

    #[test]
    fn counts_workers_per_version_ascending() {
        let workers = vec![
            worker(Some(Version::new(2, 0, 0))),
            worker(Some(Version::new(1, 0, 0))),
            worker(Some(Version::new(2, 0, 0))),
            worker(None),
        ];
        assert_eq!(
            format_version_distribution(&workers),
            "none:1 1.0.0:1 2.0.0:2"
        );
    }

    #[test]
    fn empty_workers_render_empty() {
        assert_eq!(format_version_distribution(&[]), "");
    }
}
