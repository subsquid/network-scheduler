//! HTML report: fills `templates/report.html` with the per-step table and the chart series.
//!
//! A metric reaches the report as one row of [`COLUMNS`] (header + cell) or one of [`SERIES`] (JS name
//! + value); headers, cells and JS arrays all come from those tables, so they can't drift apart.

use std::path::Path;

use bytesize::ByteSize;

use crate::metrics::ReshuffleMetrics;

const TEMPLATE: &str = include_str!("../templates/report.html");

/// A column of the raw-data table: its header, and how to render one step's cell.
type Column = (&'static str, fn(&ReshuffleMetrics) -> String);

#[rustfmt::skip]
const COLUMNS: &[Column] = &[
    ("Step",                                 |m| m.step.to_string()),
    ("New chunks",                           |m| m.new_chunks_in_step.to_string()),
    ("Copied/replaced",                      |m| m.copied_or_replaced_chunks.to_string()),
    ("New restricted",                       |m| m.new_restricted_in_step.to_string()),
    ("Chunks ingested",                      |m| m.ingested_chunks.to_string()),
    ("Chunks placed (on workers)",           |m| m.placed_chunks.to_string()),
    ("Chunks in portal",                     |m| m.portal_chunks.map_or_else(|| "-".to_string(), |n| n.to_string())),
    ("Total restricted",                     |m| m.total_restricted_chunks.to_string()),
    ("Replication factor",                   |m| m.replication_label()),
    ("New chunks download (all replicas)",   |m| bytes(m.data_movement.new_chunk_bytes)),
    ("Shuffled chunks",                      |m| m.data_movement.shuffled_count.to_string()),
    ("Shuffled chunks download (all replicas)", |m| bytes(m.data_movement.shuffled_bytes)),
    ("Replication increase download",        |m| bytes(m.data_movement.increased_replication_bytes)),
    ("Total S3 download (all replicas)",     |m| bytes(m.total_download())),
    ("Shuffle % of download",                |m| format!("{:.1}%", m.shuffle_share())),
    ("Freed (all replicas)",                 |m| bytes(m.freed_bytes())),
    ("Replication shed (frees later)",       |m| bytes(m.data_movement.shed_replication_bytes)),
    ("Free capacity",                        |m| bytes(m.free_capacity())),
    ("Used %",                               |m| format!("{:.1}%", m.used_pct())),
    ("Stale %",                              |m| format!("{:.1}%", m.stale_pct())),
    ("Workers receiving new",                |m| m.data_movement.workers_receiving_new.to_string()),
    ("Workers losing",                       |m| m.data_movement.workers_losing.to_string()),
    ("Workers receiving existing",           |m| m.data_movement.workers_receiving_existing.to_string()),
    ("Eligible workers",                     |m| m.eligible_workers.to_string()),
    ("Scheduled",                            |m| if m.scheduled { "yes" } else { "NO" }.to_string()),
    ("Restricted download (all replicas)",   |m| bytes(m.restricted_download())),
    ("Scheduling time",                      |m| format!("{:.1} ms", m.schedule_ms())),
];

/// A chart series: its name in the template's `series` object, and one step's value as a JS number.
type Series = (&'static str, fn(&ReshuffleMetrics) -> String);

#[rustfmt::skip]
const SERIES: &[Series] = &[
    ("steps",               |m| m.step.to_string()),
    ("newChunks",           |m| m.new_chunks_in_step.to_string()),
    ("ingestedChunks",      |m| m.ingested_chunks.to_string()),
    ("placedChunks",        |m| m.placed_chunks.to_string()),
    // `null` (not 0) on the stateless path, so the chart draws no portal line rather than a flat zero.
    ("portalChunks",        |m| m.portal_chunks.map_or_else(|| "null".to_string(), |n| n.to_string())),
    ("newChunkBytes",       |m| m.data_movement.new_chunk_bytes.to_string()),
    ("shuffledBytes",       |m| m.data_movement.shuffled_bytes.to_string()),
    ("shuffledCount",       |m| m.data_movement.shuffled_count.to_string()),
    ("increasedRepl",       |m| m.data_movement.increased_replication_bytes.to_string()),
    ("freedBytes",          |m| m.data_movement.freed_bytes.to_string()),
    ("shedRepl",            |m| m.data_movement.shed_replication_bytes.to_string()),
    ("shuffleShare",        |m| float(m.shuffle_share())),
    ("totalDownload",       |m| m.total_download().to_string()),
    ("freeCapacity",        |m| m.free_capacity().to_string()),
    ("usedPct",             |m| float(m.used_pct())),
    ("stalePct",            |m| float(m.stale_pct())),
    ("workersReceivingNew", |m| m.data_movement.workers_receiving_new.to_string()),
    ("workersLosing",       |m| m.data_movement.workers_losing.to_string()),
    ("workersReceivingExisting", |m| m.data_movement.workers_receiving_existing.to_string()),
    ("eligibleWorkers",     |m| m.eligible_workers.to_string()),
    ("scheduled",           |m| u8::from(m.scheduled).to_string()),
    ("restrictedDownload",  |m| m.restricted_download().to_string()),
    ("scheduleMs",          |m| float(m.schedule_ms())),
];

fn bytes(n: u64) -> String {
    ByteSize(n).to_string()
}

/// A JS number literal that round-trips the f64 exactly (`96.66666666666667`, not `96.7`).
fn float(v: f64) -> String {
    format!("{v:?}")
}

pub fn generate_html(metrics: &[ReshuffleMetrics], path: &Path) -> anyhow::Result<()> {
    let headers: String = COLUMNS
        .iter()
        .map(|(header, _)| format!("<th>{header}</th>"))
        .collect();

    let rows: Vec<String> = metrics
        .iter()
        .map(|m| {
            let cells: String = COLUMNS
                .iter()
                .map(|(_, cell)| format!("<td>{}</td>", cell(m)))
                .collect();
            format!("<tr>{cells}</tr>")
        })
        .collect();

    let series: Vec<String> = SERIES
        .iter()
        .map(|(name, value)| {
            let values: Vec<String> = metrics.iter().map(value).collect();
            format!("  {name}: [{}]", values.join(", "))
        })
        .collect();

    let html = TEMPLATE
        .replace("{{STOP_BANNER}}", &stop_banner(metrics))
        .replace("{{TABLE_HEADERS}}", &headers)
        .replace("{{TABLE_ROWS}}", &rows.join("\n"))
        .replace("{{SERIES}}", &format!("{{\n{}\n}}", series.join(",\n")));

    std::fs::write(path, html)?;
    eprintln!("Report written to {}", path.display());
    Ok(())
}

/// If the run stopped because scheduling failed, the banner naming the step and reason.
fn stop_banner(metrics: &[ReshuffleMetrics]) -> String {
    let Some(failed) = metrics.iter().find(|m| !m.scheduled) else {
        return String::new();
    };
    let reason = failed
        .failure_reason
        .as_deref()
        .unwrap_or("scheduling failed")
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;");
    format!(
        r#"<div class="stop-banner"><strong>Simulation stopped at step {}:</strong> {reason}</div>"#,
        failed.step
    )
}
