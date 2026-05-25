use std::fmt::Write;
use std::path::Path;

use bytesize::ByteSize;

use crate::simulation::ReshuffleMetrics;

const TEMPLATE: &str = include_str!("../templates/report.html");

pub fn generate_html(metrics: &[ReshuffleMetrics], path: &Path) -> anyhow::Result<()> {
    let html = TEMPLATE
        .replacen("__DATA__", &build_data_json(metrics), 1)
        .replacen("__TABLE_ROWS__", &build_table_rows(metrics), 1);

    std::fs::write(path, html)?;
    eprintln!("Report written to {}", path.display());
    Ok(())
}

fn build_data_json(metrics: &[ReshuffleMetrics]) -> String {
    let steps: Vec<u32> = metrics.iter().map(|m| m.step).collect();
    let new_chunks: Vec<u32> = metrics.iter().map(|m| m.new_chunks_in_step).collect();
    let total_chunks: Vec<usize> = metrics.iter().map(|m| m.total_chunks).collect();
    let new_chunk_bytes: Vec<u64> = metrics
        .iter()
        .map(|m| m.data_movement.new_chunk_bytes)
        .collect();
    let shuffled_bytes: Vec<u64> = metrics
        .iter()
        .map(|m| m.data_movement.shuffled_bytes)
        .collect();
    let shuffled_count: Vec<u32> = metrics
        .iter()
        .map(|m| m.data_movement.shuffled_count)
        .collect();
    let increased_repl: Vec<u64> = metrics
        .iter()
        .map(|m| m.data_movement.increased_replication_bytes)
        .collect();
    let decreased_repl: Vec<u64> = metrics
        .iter()
        .map(|m| m.data_movement.decreased_replication_bytes)
        .collect();
    let total_download: Vec<u64> = metrics
        .iter()
        .map(|m| m.data_movement.total_download())
        .collect();
    let free_capacity: Vec<u64> = metrics.iter().map(|m| m.free_capacity()).collect();
    let used_pct: Vec<f64> = metrics.iter().map(|m| m.used_pct()).collect();
    let workers_receiving_new: Vec<usize> = metrics
        .iter()
        .map(|m| m.data_movement.workers_receiving_new)
        .collect();
    let workers_losing: Vec<usize> = metrics
        .iter()
        .map(|m| m.data_movement.workers_losing)
        .collect();
    let workers_shuffled: Vec<usize> = metrics
        .iter()
        .map(|m| m.data_movement.workers_shuffled)
        .collect();

    format!(
        r#"{{
"steps":{steps:?},
"newChunks":{new_chunks:?},
"totalChunks":{total_chunks:?},
"newChunkBytes":{new_chunk_bytes:?},
"shuffledBytes":{shuffled_bytes:?},
"shuffledCount":{shuffled_count:?},
"increasedRepl":{increased_repl:?},
"decreasedRepl":{decreased_repl:?},
"totalDownload":{total_download:?},
"freeCapacity":{free_capacity:?},
"usedPct":{used_pct:?},
"workersReceivingNew":{workers_receiving_new:?},
"workersLosing":{workers_losing:?},
"workersShuffled":{workers_shuffled:?}
}}"#,
    )
}

fn build_table_rows(metrics: &[ReshuffleMetrics]) -> String {
    let mut out = String::new();
    for m in metrics {
        let dm = &m.data_movement;
        let _ = write!(
            out,
            "<tr>\
             <td>{}</td><td>{}</td><td>{}</td><td>{}</td>\
             <td>{}</td><td>{}</td><td>{}</td>\
             <td>{}</td><td>{}</td><td>{}</td>\
             <td>{}</td><td>{:.1}%</td>\
             <td>{}</td><td>{}</td><td>{}</td>\
             </tr>\n",
            m.step,
            m.new_chunks_in_step,
            m.total_chunks,
            m.replication_summary(),
            ByteSize(dm.new_chunk_bytes),
            dm.shuffled_count,
            ByteSize(dm.shuffled_bytes),
            ByteSize(dm.increased_replication_bytes),
            ByteSize(dm.decreased_replication_bytes),
            ByteSize(dm.total_download()),
            ByteSize(m.free_capacity()),
            m.used_pct(),
            dm.workers_receiving_new,
            dm.workers_losing,
            dm.workers_shuffled,
        );
    }
    out
}
