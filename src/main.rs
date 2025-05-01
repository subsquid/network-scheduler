mod assignment_fb;
mod scheduling;
mod tests;
mod types;

fn main() {
    let (chunks, workers, total_size) = tests::input::generate_input(2000, 10_000_000);
    let worker_capacity = (total_size as f64 / workers.len() as f64 * 1.1) as u64;
    scheduling::distribute(&chunks, workers, worker_capacity);
}
