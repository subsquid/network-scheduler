use itertools::Itertools;
use types::Worker;

mod assignment_fb;
mod replication;
mod scheduling;
mod tests;
mod types;

fn main() {
    tracing_subscriber::fmt()
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .init();

    let chunks = tests::input::generate_chunks(10_000_000, &[1, 2, 6, 12]);
    let workers = tests::input::generate_workers(2000)
        .into_iter()
        .map(|id| Worker {
            id,
            reliable: rand::random_bool(0.9),
        })
        .collect_vec();
    let worker_capacity = 1 << 43; // 8TB
    scheduling::schedule(
        &chunks,
        &workers,
        scheduling::SchedulingConfig {
            worker_capacity,
            saturation: 0.95,
            min_replication: 5,
        },
    )
    .unwrap();
}
