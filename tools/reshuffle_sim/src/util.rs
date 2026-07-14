use std::collections::BTreeMap;
use std::time::Duration;

use bytesize::ByteSize;
use network_scheduler::types::Worker;
use semver::Version;
use tabled::builder::Builder;
use tabled::settings::span::ColumnSpan;
use tabled::settings::{Alignment, Modify, Style};

use crate::metrics::ReshuffleMetrics;
use crate::simulation::Summary;

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

fn format_duration(d: Duration) -> String {
    format!("{:.1} ms", d.as_secs_f64() * 1000.0)
}

/// A table column. Consecutive columns sharing a `head` render under one spanning header (`Chunks`,
/// `Workers`); an ungrouped column has an empty `sub`.
///
/// `show` decides, over the whole run, whether the column carries information: one that is constant or
/// zero throughout (no restricted data, no upgrades, no drain) is dropped instead of printed as a
/// stripe of zeroes.
///
/// A head is never merged into the sub row with a `RowSpan`. A merged cell is only as tall as the rows
/// it spans, and tabled silently drops the overflowing lines of a multi-line head when the visible
/// columns make those rows short.
struct Col {
    head: &'static str,
    sub: &'static str,
    cell: fn(&ReshuffleMetrics) -> String,
    show: fn(&[ReshuffleMetrics]) -> bool,
}

const ALWAYS: fn(&[ReshuffleMetrics]) -> bool = |_| true;

#[rustfmt::skip]
const COLS: &[Col] = &[
    Col { head: "Step", sub: "", show: ALWAYS,
          cell: |m| if m.scheduled { m.step.to_string() } else { format!("{} ✗", m.step) } },
    Col { head: "New\nchunks", sub: "", show: ALWAYS,
          cell: |m| m.new_chunks_in_step.to_string() },
    Col { head: "Copied/\nreplaced", sub: "",
          show: |ms| ms.iter().any(|m| m.copied_or_replaced_chunks > 0),
          cell: |m| m.copied_or_replaced_chunks.to_string() },
    Col { head: "New\nrestr.", sub: "", show: has_restricted,
          cell: |m| m.new_restricted_in_step.to_string() },

    // The chunk funnel: ingested ⊇ placed ⊇ portal. The gaps are dead rows (rejected, or tombstoned by
    // a correction) and chunks not yet confirmed to the portal.
    Col { head: "Chunks", sub: "ingested", show: ALWAYS,
          cell: |m| m.ingested_chunks.to_string() },
    Col { head: "Chunks", sub: "placed", show: ALWAYS,
          cell: |m| m.placed_chunks.to_string() },
    Col { head: "Chunks", sub: "portal", show: has_portal,
          cell: |m| m.portal_chunks.map_or_else(|| "-".to_string(), |n| n.to_string()) },
    Col { head: "Restr.\nchunks", sub: "", show: has_restricted,
          cell: |m| m.total_restricted_chunks.to_string() },

    Col { head: "Repl.", sub: "", show: replication_varies,
          cell: |m| m.replication_label() },

    // The download, split by cause: new data, chunks moving, extra replicas. The three sum to the
    // total, so "shuffle %" is the share of the step's traffic that reshuffling cost.
    Col { head: "Download", sub: "new", show: ALWAYS,
          cell: |m| ByteSize(m.data_movement.new_chunk_bytes).to_string() },
    Col { head: "Download", sub: "shuffle", show: ALWAYS,
          cell: |m| ByteSize(m.data_movement.shuffled_bytes).to_string() },
    Col { head: "Download", sub: "+repl", show: ALWAYS,
          cell: |m| ByteSize(m.data_movement.increased_replication_bytes).to_string() },
    Col { head: "Download", sub: "total", show: ALWAYS,
          cell: |m| ByteSize(m.total_download()).to_string() },
    Col { head: "Download", sub: "shuffle %", show: ALWAYS,
          cell: |m| format!("{:.1}%", m.shuffle_share()) },

    Col { head: "Chunks\nmoved", sub: "", show: ALWAYS,
          cell: |m| m.data_movement.shuffled_count.to_string() },
    Col { head: "Freed", sub: "", show: ALWAYS,
          cell: |m| ByteSize(m.freed_bytes()).to_string() },
    Col { head: "Repl.\nshed", sub: "", show: |ms| ms.iter().any(|m| m.data_movement.shed_replication_bytes > 0),
          cell: |m| ByteSize(m.data_movement.shed_replication_bytes).to_string() },
    Col { head: "Restr.\ndownload", sub: "", show: has_restricted,
          cell: |m| ByteSize(m.restricted_download()).to_string() },

    Col { head: "Free cap\n(used %)", sub: "", show: ALWAYS,
          cell: |m| format!("{} ({:.1}%)", ByteSize(m.free_capacity()), m.used_pct()) },
    Col { head: "Stale\n(% cap)", sub: "", show: |ms| ms.iter().any(|m| m.stale_capacity_bytes > 0),
          cell: |m| format!("{:.1}%", m.stale_pct()) },

    Col { head: "Workers", sub: "new", show: ALWAYS,
          cell: |m| m.data_movement.workers_receiving_new.to_string() },
    Col { head: "Workers", sub: "existing", show: ALWAYS,
          cell: |m| m.data_movement.workers_receiving_existing.to_string() },
    Col { head: "Workers", sub: "lost", show: ALWAYS,
          cell: |m| m.data_movement.workers_losing.to_string() },

    Col { head: "Upgraded\nworkers", sub: "", show: |ms| ms.iter().any(|m| m.eligible_workers > 0),
          cell: |m| m.eligible_workers.to_string() },
    Col { head: "Sched\ntime", sub: "", show: ALWAYS,
          cell: |m| format_duration(m.schedule_duration) },
];

fn has_restricted(ms: &[ReshuffleMetrics]) -> bool {
    ms.iter().any(|m| m.total_restricted_chunks > 0)
}

fn has_portal(ms: &[ReshuffleMetrics]) -> bool {
    ms.iter().any(|m| m.portal_chunks.is_some())
}

/// The replication factor is normally the same every step; then it belongs under the table, not in a
/// column of its own.
fn replication_varies(ms: &[ReshuffleMetrics]) -> bool {
    let mut labels = ms
        .iter()
        .filter(|m| m.scheduled)
        .map(|m| m.replication_label());
    let first = labels.next();
    labels.any(|l| Some(&l) != first.as_ref())
}

fn build_table(metrics: &[ReshuffleMetrics]) -> tabled::Table {
    let cols: Vec<&Col> = COLS.iter().filter(|c| (c.show)(metrics)).collect();

    // Grouped columns (same `head`) put the head over the first of the group and blank it on the rest;
    // ungrouped ones carry their head alone and leave the sub-header row empty.
    let mut heads = Vec::with_capacity(cols.len());
    let mut subs = Vec::with_capacity(cols.len());
    for (i, col) in cols.iter().enumerate() {
        let continues_group = i > 0 && cols[i - 1].head == col.head;
        heads.push(if continues_group { "" } else { col.head });
        subs.push(col.sub);
    }

    let mut builder = Builder::default();
    builder.push_record(heads.iter().copied());
    builder.push_record(subs.iter().copied());
    for m in metrics {
        builder.push_record(cols.iter().map(|c| (c.cell)(m)));
    }

    let mut table = builder.build();
    table.with(Style::modern()).with(Alignment::right());
    let mut i = 0;
    while i < cols.len() {
        let width = cols[i..]
            .iter()
            .take_while(|c| c.head == cols[i].head)
            .count();
        if !cols[i].sub.is_empty() {
            table.with(Modify::new((0, i)).with(ColumnSpan::new(width as isize)));
            table.with(Modify::new((0, i)).with(Alignment::center()));
        }
        i += width;
    }
    table
}

/// One-line summary of a step, printed as it finishes. A per-step record in the log means a run
/// killed mid-way (e.g. OOM) still shows every step it completed, not just the final table.
pub fn print_step(m: &ReshuffleMetrics) {
    let dm = &m.data_movement;
    let injected = if m.copied_or_replaced_chunks > 0 {
        format!(" +{}", m.copied_or_replaced_chunks)
    } else {
        String::new()
    };
    let portal = m
        .portal_chunks
        .map_or_else(String::new, |n| format!("/portal {n}"));
    println!(
        "step {} | new {}{} (r{}) | chunks {} ingested/placed {}{} | download {} (new {}, shuffle {} = {:.1}%, +repl {}) \
         | moved {} chunks | freed {} | free {} ({:.1}% used) | stale {:.1}% | workers +{}/~{}/-{} | upgraded {} | sched {} {}",
        m.step,
        m.new_chunks_in_step,
        injected,
        m.new_restricted_in_step,
        m.ingested_chunks,
        m.placed_chunks,
        portal,
        ByteSize(m.total_download()),
        ByteSize(dm.new_chunk_bytes),
        ByteSize(dm.shuffled_bytes),
        m.shuffle_share(),
        ByteSize(dm.increased_replication_bytes),
        dm.shuffled_count,
        ByteSize(m.freed_bytes()),
        ByteSize(m.free_capacity()),
        m.used_pct(),
        m.stale_pct(),
        dm.workers_receiving_new,
        dm.workers_receiving_existing,
        dm.workers_losing,
        m.eligible_workers,
        if m.scheduled { "yes" } else { "NO" },
        format_duration(m.schedule_duration),
    );
}

pub fn print_table(metrics: &[ReshuffleMetrics]) {
    println!("{}", build_table(metrics));
    if !replication_varies(metrics)
        && let Some(m) = metrics.iter().find(|m| m.scheduled)
    {
        println!("Replication by weight: {}", m.replication_label());
    }
}

pub fn print_summary(summary: &Summary) {
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
