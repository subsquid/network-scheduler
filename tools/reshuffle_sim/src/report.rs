use std::path::Path;

use bytesize::ByteSize;

use crate::simulation::ReshuffleMetrics;

pub fn generate_html(metrics: &[ReshuffleMetrics], path: &Path) -> anyhow::Result<()> {
    let steps: Vec<u32> = metrics.iter().map(|m| m.step).collect();
    let new_chunks: Vec<u32> = metrics.iter().map(|m| m.new_chunks_in_step).collect();
    let total_chunks: Vec<usize> = metrics.iter().map(|m| m.total_chunks).collect();

    let new_chunk_bytes_raw: Vec<u64> = metrics
        .iter()
        .map(|m| m.data_movement.new_chunk_bytes)
        .collect();
    let shuffled_bytes_raw: Vec<u64> = metrics
        .iter()
        .map(|m| m.data_movement.shuffled_bytes)
        .collect();
    let shuffled_count: Vec<u32> = metrics
        .iter()
        .map(|m| m.data_movement.shuffled_count)
        .collect();
    let increased_repl_raw: Vec<u64> = metrics
        .iter()
        .map(|m| m.data_movement.increased_replication_bytes)
        .collect();
    let decreased_repl_raw: Vec<u64> = metrics
        .iter()
        .map(|m| m.data_movement.decreased_replication_bytes)
        .collect();
    let total_download_raw: Vec<u64> = metrics
        .iter()
        .map(|m| {
            let dm = &m.data_movement;
            dm.new_chunk_bytes + dm.shuffled_bytes + dm.increased_replication_bytes
        })
        .collect();
    let free_capacity_raw: Vec<u64> = metrics
        .iter()
        .map(|m| m.total_capacity_bytes.saturating_sub(m.used_capacity_bytes))
        .collect();
    let used_pct: Vec<f64> = metrics
        .iter()
        .map(|m| {
            if m.total_capacity_bytes > 0 {
                m.used_capacity_bytes as f64 / m.total_capacity_bytes as f64 * 100.0
            } else {
                0.0
            }
        })
        .collect();
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
    let eligible_workers: Vec<usize> = metrics.iter().map(|m| m.eligible_workers).collect();
    let scheduled_flags: Vec<u8> = metrics.iter().map(|m| m.scheduled as u8).collect();
    let restricted_download_raw: Vec<u64> = metrics
        .iter()
        .map(|m| {
            let r = &m.restricted_movement;
            r.new_chunk_bytes + r.shuffled_bytes + r.increased_replication_bytes
        })
        .collect();

    let new_chunk_bytes_fmt: Vec<String> = new_chunk_bytes_raw
        .iter()
        .map(|b| ByteSize(*b).to_string())
        .collect();
    let shuffled_bytes_fmt: Vec<String> = shuffled_bytes_raw
        .iter()
        .map(|b| ByteSize(*b).to_string())
        .collect();
    let increased_repl_fmt: Vec<String> = increased_repl_raw
        .iter()
        .map(|b| ByteSize(*b).to_string())
        .collect();
    let decreased_repl_fmt: Vec<String> = decreased_repl_raw
        .iter()
        .map(|b| ByteSize(*b).to_string())
        .collect();
    let total_download_fmt: Vec<String> = total_download_raw
        .iter()
        .map(|b| ByteSize(*b).to_string())
        .collect();
    let free_capacity_fmt: Vec<String> = free_capacity_raw
        .iter()
        .map(|b| ByteSize(*b).to_string())
        .collect();
    let used_pct_fmt: Vec<String> = used_pct.iter().map(|p| format!("{p:.1}%")).collect();

    let replication_labels: Vec<String> = metrics
        .iter()
        .map(|m| {
            m.replication_by_weight
                .iter()
                .map(|(w, f)| format!("{w}:{f}"))
                .collect::<Vec<_>>()
                .join(", ")
        })
        .collect();

    // If the run stopped because scheduling failed, show the recorded reason.
    let stop_banner = metrics
        .iter()
        .find(|m| !m.scheduled)
        .map(|m| {
            let reason = m
                .failure_reason
                .as_deref()
                .unwrap_or("scheduling failed")
                .replace('&', "&amp;")
                .replace('<', "&lt;")
                .replace('>', "&gt;");
            format!(
                r#"<div class="stop-banner"><strong>Simulation stopped at step {}:</strong> {}</div>"#,
                m.step, reason
            )
        })
        .unwrap_or_default();

    let html = format!(
        r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Reshuffle Simulation Report</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
  body {{ font-family: system-ui, -apple-system, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }}
  h1 {{ text-align: center; color: #333; }}
  .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; max-width: 1400px; margin: 0 auto; }}
  .card {{ background: white; border-radius: 8px; padding: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
  .card.wide {{ grid-column: 1 / -1; }}
  canvas {{ width: 100% !important; }}
  table {{ border-collapse: collapse; width: 100%; font-size: 13px; }}
  th, td {{ border: 1px solid #ddd; padding: 6px 10px; text-align: right; }}
  .stop-banner {{ max-width: 1400px; margin: 0 auto 16px; padding: 12px 16px; background: #ffe5e5; border: 1px solid #ffb3b3; border-radius: 8px; color: #900; }}
  th {{ background: #f0f0f0; position: sticky; top: 0; }}
  td:first-child, th:first-child {{ text-align: center; }}
</style>
</head>
<body>
<h1>Reshuffle Simulation Report</h1>
{stop_banner}
<div class="grid">

<div class="card">
  <canvas id="chunksChart"></canvas>
</div>

<div class="card">
  <canvas id="dataBreakdownChart"></canvas>
</div>

<div class="card">
  <canvas id="shuffledChart"></canvas>
</div>

<div class="card">
  <canvas id="replicationChart"></canvas>
</div>

<div class="card">
  <canvas id="workersChart"></canvas>
</div>

<div class="card">
  <canvas id="shuffledCountChart"></canvas>
</div>

<div class="card">
  <canvas id="totalDownloadChart"></canvas>
</div>

<div class="card">
  <canvas id="capacityChart"></canvas>
</div>

<div class="card">
  <canvas id="eligibleWorkersChart"></canvas>
</div>

<div class="card">
  <canvas id="restrictedDownloadChart"></canvas>
</div>

<div class="card wide">
<h3 style="margin-top:0">Raw Data</h3>
<div style="overflow-x:auto">
<table>
<tr>
  <th>Step</th><th>New chunks</th><th>New restricted</th><th>Total chunks</th><th>Total restricted</th><th>Replication factor</th>
  <th>New chunks download (all replicas)</th><th>Shuffled chunks</th><th>Shuffled chunks download (all replicas)</th>
  <th>Replication increase download</th><th>Replication decrease freed</th><th>Total S3 download (all replicas)</th>
  <th>Free capacity</th><th>Used %</th>
  <th>Workers receiving new</th><th>Workers losing</th><th>Workers receiving shuffled</th>
  <th>Eligible workers</th><th>Scheduled</th><th>Restricted download (all replicas)</th>
</tr>
{table_rows}
</table>
</div>
</div>

</div>

<script>
const steps = {steps:?};
const newChunks = {new_chunks:?};
const totalChunks = {total_chunks:?};
const newChunkBytes = {new_chunk_bytes_raw:?};
const shuffledBytes = {shuffled_bytes_raw:?};
const shuffledCount = {shuffled_count:?};
const increasedRepl = {increased_repl_raw:?};
const decreasedRepl = {decreased_repl_raw:?};
const totalDownload = {total_download_raw:?};
const freeCapacity = {free_capacity_raw:?};
const usedPct = {used_pct:?};
const workersReceivingNew = {workers_receiving_new:?};
const workersLosing = {workers_losing:?};
const workersShuffled = {workers_shuffled:?};
const eligibleWorkers = {eligible_workers:?};
const scheduledFlags = {scheduled_flags:?};
const restrictedDownload = {restricted_download_raw:?};

function formatBytes(b) {{
  if (b === 0) return '0 B';
  const units = ['B','KB','MB','GB','TB'];
  const i = Math.floor(Math.log(b)/Math.log(1024));
  return (b/Math.pow(1024,i)).toFixed(1) + ' ' + units[i];
}}

const bytesTooltip = {{ callbacks: {{ label: function(ctx) {{ return ctx.dataset.label + ': ' + formatBytes(ctx.raw); }} }} }};
const bytesScale = {{ beginAtZero: true, ticks: {{ callback: function(v) {{ return formatBytes(v); }} }} }};
const stepLabels = steps.map(s => 'Step ' + s);

new Chart(document.getElementById('chunksChart'), {{
  type: 'bar',
  data: {{
    labels: stepLabels,
    datasets: [
      {{ label: 'New chunks', data: newChunks, backgroundColor: 'rgba(54,162,235,0.7)' }},
      {{ label: 'Total chunks', data: totalChunks, type: 'line', borderColor: '#ff6384', backgroundColor: 'transparent', yAxisID: 'y1' }}
    ]
  }},
  options: {{
    plugins: {{ title: {{ display: true, text: 'Chunk Counts' }} }},
    scales: {{
      y: {{ beginAtZero: true, title: {{ display: true, text: 'New per step' }} }},
      y1: {{ position: 'right', title: {{ display: true, text: 'Total' }}, grid: {{ drawOnChartArea: false }} }}
    }}
  }}
}});

new Chart(document.getElementById('dataBreakdownChart'), {{
  type: 'bar',
  data: {{
    labels: stepLabels,
    datasets: [
      {{ label: 'New chunks download (all replicas)', data: newChunkBytes, backgroundColor: 'rgba(54,162,235,0.7)' }},
      {{ label: 'Shuffled chunks download (all replicas)', data: shuffledBytes, backgroundColor: 'rgba(255,99,132,0.7)' }},
      {{ label: 'Replication increase download', data: increasedRepl, backgroundColor: 'rgba(255,206,86,0.7)' }}
    ]
  }},
  options: {{
    plugins: {{ title: {{ display: true, text: 'S3 Download Breakdown (chunk size × number of downloading workers)' }}, tooltip: bytesTooltip }},
    scales: {{ y: bytesScale }}
  }}
}});

new Chart(document.getElementById('shuffledChart'), {{
  type: 'bar',
  data: {{
    labels: stepLabels,
    datasets: [
      {{ label: 'Shuffled chunks download (all replicas)', data: shuffledBytes, backgroundColor: 'rgba(255,99,132,0.7)' }}
    ]
  }},
  options: {{
    plugins: {{ title: {{ display: true, text: 'Shuffled Chunks Download (all replicas, existing chunks moved to different workers)' }}, tooltip: bytesTooltip }},
    scales: {{ y: bytesScale }}
  }}
}});

new Chart(document.getElementById('replicationChart'), {{
  type: 'bar',
  data: {{
    labels: stepLabels,
    datasets: [
      {{ label: 'Replication increase download', data: increasedRepl, backgroundColor: 'rgba(75,192,192,0.7)' }},
      {{ label: 'Replication decrease freed', data: decreasedRepl, backgroundColor: 'rgba(255,159,64,0.7)' }}
    ]
  }},
  options: {{
    plugins: {{ title: {{ display: true, text: 'Replication Changes (download from gained replicas / freed from lost replicas)' }}, tooltip: bytesTooltip }},
    scales: {{ y: bytesScale }}
  }}
}});

new Chart(document.getElementById('workersChart'), {{
  type: 'bar',
  data: {{
    labels: stepLabels,
    datasets: [
      {{ label: 'Receiving new chunks', data: workersReceivingNew, backgroundColor: 'rgba(54,162,235,0.7)' }},
      {{ label: 'Losing chunks', data: workersLosing, backgroundColor: 'rgba(255,99,132,0.7)' }},
      {{ label: 'Receiving shuffled chunks', data: workersShuffled, backgroundColor: 'rgba(255,206,86,0.7)' }}
    ]
  }},
  options: {{
    plugins: {{ title: {{ display: true, text: 'Workers Affected by Category' }} }},
    scales: {{ y: {{ beginAtZero: true }} }}
  }}
}});

new Chart(document.getElementById('shuffledCountChart'), {{
  type: 'line',
  data: {{
    labels: stepLabels,
    datasets: [
      {{ label: 'Chunks shuffled', data: shuffledCount, borderColor: '#ff6384', backgroundColor: 'rgba(255,99,132,0.1)', fill: true }}
    ]
  }},
  options: {{
    plugins: {{ title: {{ display: true, text: 'Existing Chunks Reshuffled (count)' }} }},
    scales: {{ y: {{ beginAtZero: true }} }}
  }}
}});

new Chart(document.getElementById('totalDownloadChart'), {{
  type: 'bar',
  data: {{
    labels: stepLabels,
    datasets: [
      {{ label: 'Total S3 download (all replicas)', data: totalDownload, backgroundColor: 'rgba(153,102,255,0.7)' }}
    ]
  }},
  options: {{
    plugins: {{ title: {{ display: true, text: 'Total S3 Download per Step (all replicas: new + shuffled + replication increase)' }}, tooltip: bytesTooltip }},
    scales: {{ y: bytesScale }}
  }}
}});

new Chart(document.getElementById('capacityChart'), {{
  type: 'bar',
  data: {{
    labels: stepLabels,
    datasets: [
      {{ label: 'Free capacity', data: freeCapacity, backgroundColor: 'rgba(75,192,192,0.7)', yAxisID: 'y' }},
      {{ label: 'Used %', data: usedPct, type: 'line', borderColor: '#ff6384', backgroundColor: 'transparent', yAxisID: 'y1' }}
    ]
  }},
  options: {{
    plugins: {{ title: {{ display: true, text: 'Remaining Free Capacity Across All Workers' }}, tooltip: {{
      callbacks: {{ label: function(ctx) {{
        if (ctx.dataset.yAxisID === 'y1') return ctx.dataset.label + ': ' + ctx.raw.toFixed(1) + '%';
        return ctx.dataset.label + ': ' + formatBytes(ctx.raw);
      }} }}
    }} }},
    scales: {{
      y: {{ ...bytesScale, title: {{ display: true, text: 'Free capacity' }} }},
      y1: {{ position: 'right', min: 0, max: 100, title: {{ display: true, text: 'Used %' }}, grid: {{ drawOnChartArea: false }}, ticks: {{ callback: function(v) {{ return v + '%'; }} }} }}
    }}
  }}
}});

new Chart(document.getElementById('eligibleWorkersChart'), {{
  type: 'line',
  data: {{
    labels: stepLabels,
    datasets: [
      {{ label: 'Workers on new version', data: eligibleWorkers, borderColor: '#36a2eb', backgroundColor: 'rgba(54,162,235,0.1)', fill: true }}
    ]
  }},
  options: {{
    plugins: {{ title: {{ display: true, text: 'Worker Version Upgrade Progress (eligible workers)' }} }},
    scales: {{ y: {{ beginAtZero: true }} }}
  }}
}});

new Chart(document.getElementById('restrictedDownloadChart'), {{
  type: 'bar',
  data: {{
    labels: stepLabels,
    datasets: [
      {{
        label: 'Restricted chunk download (all replicas)',
        data: restrictedDownload,
        backgroundColor: scheduledFlags.map(f => f ? 'rgba(153,102,255,0.7)' : 'rgba(255,99,132,0.9)')
      }}
    ]
  }},
  options: {{
    plugins: {{ title: {{ display: true, text: 'Version-Restricted Download per Step (red = scheduling failed)' }}, tooltip: bytesTooltip }},
    scales: {{ y: bytesScale }}
  }}
}});
</script>
</body>
</html>"##,
        table_rows = build_table_rows(
            metrics,
            &replication_labels,
            &new_chunk_bytes_fmt,
            &shuffled_bytes_fmt,
            &increased_repl_fmt,
            &decreased_repl_fmt,
            &total_download_fmt,
            &free_capacity_fmt,
            &used_pct_fmt
        ),
    );

    std::fs::write(path, html)?;
    eprintln!("Report written to {}", path.display());
    Ok(())
}

fn build_table_rows(
    metrics: &[ReshuffleMetrics],
    replication_labels: &[String],
    new_chunk_bytes: &[String],
    shuffled_bytes: &[String],
    increased_repl: &[String],
    decreased_repl: &[String],
    total_download: &[String],
    free_capacity: &[String],
    used_pct: &[String],
) -> String {
    metrics
        .iter()
        .enumerate()
        .map(|(i, m)| {
            let dm = &m.data_movement;
            let r = &m.restricted_movement;
            let restricted_dl = r.new_chunk_bytes + r.shuffled_bytes + r.increased_replication_bytes;
            let scheduled = if m.scheduled { "yes" } else { "NO" };
            format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                m.step,
                m.new_chunks_in_step,
                m.new_restricted_in_step,
                m.total_chunks,
                m.total_restricted_chunks,
                replication_labels[i],
                new_chunk_bytes[i],
                dm.shuffled_count,
                shuffled_bytes[i],
                increased_repl[i],
                decreased_repl[i],
                total_download[i],
                free_capacity[i],
                used_pct[i],
                dm.workers_receiving_new,
                dm.workers_losing,
                dm.workers_shuffled,
                m.eligible_workers,
                scheduled,
                ByteSize(restricted_dl),
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}
