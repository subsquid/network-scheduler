# Reshuffle Simulation

Simulates chunk ingestion over multiple steps against a real assignment, runs the scheduler each step, and measures how much data movement (S3 downloads) the reshuffling causes.

## Usage

```sh
cargo run -p reshuffle-sim -- -c config.yaml --chunks-per-step 1000 --steps 10 --report report.html input.fb.gz
```

`--chunk-size` overrides the size of each generated chunk (e.g. `--chunk-size 256MiB`);
by default each chunk uses its dataset's average chunk size.

## Chunk ingestion shapes

By default the simulator ingests `--chunks-per-step` chunks every step. Use
`--chunks-shape` to vary that count over the run instead — it's a `name:key=value,…`
spec where **every parameter has a default and can be overridden**, so the shape name
alone already works. The shape only changes *how many* chunks are added per step; it
composes with all the other options.

| Shape | Spec (with defaults) | Behaviour |
|-------|----------------------|-----------|
| `constant` | `constant:n=1000` | flat (same as `--chunks-per-step`) |
| `ramp` | `ramp:from=0,to=1000` | linear from the first to the last step |
| `triangle` | `triangle:peak=1000,center=<middle>` | rises to `center`, then falls |
| `gaussian` | `gaussian:peak=1000,sigma=<steps/6>,center=<middle>` | smooth bump |
| `pulse` | `pulse:size=10000,at=<middle>,base=0,width=1` | one bulk insert |
| `bursts` | `bursts:size=10000,every=5,base=0,until=<end>` | periodic bulk inserts |
| `points` | `points:1=0,50=5000,100=0` | arbitrary piecewise-linear profile |

Examples:

```sh
# Slow ramp up to the middle, then declining
cargo run -p reshuffle-sim -- -c config.yaml --steps 100 \
  --chunks-shape triangle:peak=5000 input.fb.gz

# One large bulk insert in the middle of the run
cargo run -p reshuffle-sim -- -c config.yaml --steps 100 \
  --chunks-shape pulse:size=50000 input.fb.gz

# Small bulk every 5 steps for the first half, then nothing
cargo run -p reshuffle-sim -- -c config.yaml --steps 100 \
  --chunks-shape bursts:size=10000,every=5,until=50 input.fb.gz

# Hand-drawn profile: nothing, ramp to 5000 by step 50, back to nothing by 100
cargo run -p reshuffle-sim -- -c config.yaml --steps 100 \
  --chunks-shape points:1=0,50=5000,100=0 input.fb.gz
```

`--chunks-shape` takes precedence over `--chunks-per-step`; without it the run uses
the constant rate.

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
