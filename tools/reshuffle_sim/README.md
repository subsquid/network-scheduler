# Reshuffle Simulation

Simulates chunk ingestion over multiple steps against a real assignment, runs the scheduler each step, and measures how much data movement (S3 downloads) the reshuffling causes.

`--scheduler` selects the algorithm:

- `stateless` (default) — the production scheduler (`src/scheduling.rs`); rebuilds the placement from scratch each step.
- `multistep` — the placement-aware scheduler over the Postgres-backed `SchedulerStorage`; reuses the stored placement so only new data is downloaded.

## Usage

```sh
# stateless (no database)
cargo run -p reshuffle-sim -- -c config.yaml --steps 10 --report report.html input.fb.gz

# multistep — starts an ephemeral Postgres container (needs Docker)
cargo run -p reshuffle-sim -- -c config.yaml --scheduler multistep --steps 10 input.fb.gz

# multistep against your own (empty) database
cargo run -p reshuffle-sim -- -c config.yaml --scheduler multistep \
    --database-url postgres://user:pass@localhost:5432/db input.fb.gz
```

## Notes

- Multistep seeds the baseline and schedules it from empty, so step 1's reference is the scheduler's own placement, not the input file's; each step measures movement relative to the previous step.
- Chunks are inserted one row per `INSERT`, so large inputs are slow against Postgres — prefer smaller `--chunks-per-step` / `--steps`.
- The version-restriction and worker-upgrade modeling below (`--restricted-fraction`,
  `--upgrade-schedule`, `--initial-new-fraction`, `--lift-restriction-at-step`) is
  config-driven (a dedicated restricted dataset carries a `minimum_worker_version`) and
  honored by **both** schedulers.

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

The simulator can route a fraction of each step's **new chunks** into a dedicated
**restricted dataset** that requires a newer worker version. That dataset's config
segment carries a `minimum_worker_version` both schedulers read, so the restriction
is honored identically by the stateless and multistep paths. The existing datasets
keep their normal size-weighted generation; only the restricted share is split off
into the synthetic dataset. The simulator also models the worker fleet upgrading to
that version over time, showing how reshuffling behaves while a rollout is in
progress.

```sh
cargo run -p reshuffle-sim -- -c config.yaml \
  --chunks-per-step 200 --steps 60 \
  --restricted-fraction 0.3 \
  --upgrade-schedule "1:0,5:0.25,30:0.6,50:1.0" \
  --report report.html input.fb.gz
```

- `--restricted-fraction` — share (0.0–1.0) of each step's new chunks generated
  into the restricted dataset (which requires the new worker version). Default
  `0.0` (feature off).
- `--restricted-dataset` — bucket name of the synthetic restricted dataset.
  Default `restricted`. Only used when `--restricted-fraction > 0`.
- `--upgrade-schedule` — ascending `step:fraction` breakpoints for the share of
  workers on the new version, held constant between breakpoints (e.g.
  `1:0,5:0.25,50:1.0`). Default empty.
- `--initial-new-fraction` — share (0.0–1.0) of workers already on the new
  version at step 0. Default `0.0`.

With all three at their defaults the run is identical to the basic simulation
above. The report and console table gain columns for workers eligible for
restricted data, whether the step scheduled successfully, and the restricted
download volume.

When restricted data cannot fit the eligible-worker pool the scheduler backs off
(it returns a capacity error rather than placing the data). The simulator records
the failed step and stops — so an early, under-upgraded fleet ends the run cleanly
(with the report written).
