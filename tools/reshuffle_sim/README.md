# Reshuffle Simulation

Simulates chunk ingestion over multiple steps against a real assignment, runs the scheduler each step, and measures how much data movement (S3 downloads) the reshuffling causes.

## Usage

```sh
cargo run -p reshuffle-sim -- -c config.yaml --chunks-per-step 1000 --steps 10 --report report.html input.fb.gz
```

## Version-restricted chunks & worker upgrades

The simulator can mark a fraction of newly ingested chunks as requiring a newer
worker version, and model the worker fleet upgrading to that version over time.
This shows how reshuffling behaves while a version rollout is in progress.

```sh
cargo run -p reshuffle-sim -- -c config.yaml \
  --chunks-per-step 200 --steps 60 \
  --restricted-fraction 0.3 \
  --upgrade-schedule "1:0,5:0.25,30:0.6,50:1.0" \
  --report report.html input.fb.gz
```

- `--restricted-fraction` — fraction (0.0–1.0) of each step's new chunks that
  require the new worker version. Default `0.0` (feature off).
- `--upgrade-schedule` — ascending `step:fraction` breakpoints for the share of
  workers on the new version, held constant between breakpoints (e.g.
  `1:0,5:0.25,50:1.0`). Default empty.
- `--initial-new-fraction` — share (0.0–1.0) of workers already on the new
  version at step 0. Default `0.0`.

With all three at their defaults the run is identical to the basic simulation
above. The report and console table gain three columns: workers eligible for
restricted data, whether the step scheduled successfully, and the restricted
download volume.

The scheduler enforces version restrictions by panicking when restricted data
cannot fit the eligible-worker pool. The simulator catches that, records the
failed step, and stops — so an early, under-upgraded fleet ends the run cleanly
(with the report written) rather than crashing.
