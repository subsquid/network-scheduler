# Reshuffle Simulation

Simulates chunk ingestion over multiple steps against a real assignment, runs the scheduler each step, and measures how much data movement (S3 downloads) the reshuffling causes.

## Usage

```sh
cargo run -p reshuffle-sim -- -c config.yaml --chunks-per-step 1000 --steps 10 --report report.html input.fb.gz
```
