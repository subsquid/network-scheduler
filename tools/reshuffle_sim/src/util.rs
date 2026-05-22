use bytesize::ByteSize;

use crate::simulation::ReshuffleMetrics;

pub fn print_header() {
    println!(
        "{:>5} | {:>12} | {:>14} | {:>30} | {:>12} | {:>17} | {:>14} | {:>14} | {:>12} | {:>12} | {:>6} | {:>10} | {:>10} | {:>10}",
        "Step",
        "New chunks",
        "Total chunks",
        "Replication",
        "New DL",
        "Shuffled",
        "Shuffle DL",
        "Repl DL change",
        "Total DL(*)",
        "Free cap",
        "Used%",
        "W new",
        "W lost",
        "W shuffled"
    );
    println!("{}", "-".repeat(230));
}

pub fn print_step(metrics: &ReshuffleMetrics) {
    let dm = &metrics.data_movement;

    println!(
        "{:>5} | {:>12} | {:>14} | {:>30} | {:>12} | {:>17} | {:>14} | {:>6}+ / {:<5}- | {:>12} | {:>12} | {:>6.1}% | {:>10} | {:>10} | {:>10}",
        metrics.step,
        metrics.new_chunks_in_step,
        metrics.total_chunks,
        metrics.replication_summary(),
        ByteSize(dm.new_chunk_bytes),
        format!("{} chunks", dm.shuffled_count),
        ByteSize(dm.shuffled_bytes),
        ByteSize(dm.increased_replication_bytes),
        ByteSize(dm.decreased_replication_bytes),
        ByteSize(dm.total_download()),
        ByteSize(metrics.free_capacity()),
        metrics.used_pct(),
        dm.workers_receiving_new,
        dm.workers_losing,
        dm.workers_shuffled,
    );
}
