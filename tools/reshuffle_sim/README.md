# Reshuffle Simulation

Simulates chunk ingestion over multiple steps against a real assignment, runs the scheduler each step, and measures how much data movement (S3 downloads) the reshuffling causes.

The scheduler is chosen by the `scheduler` profile field:

- `stateless` (default) — the production scheduler (`src/scheduling.rs`); rebuilds the placement from scratch each step.
- `multistep` — the placement-aware scheduler over the Postgres-backed `SchedulerStorage`; reuses the stored placement so only new data is downloaded.

## Usage

The CLI takes just the scheduler config, the input assignment, and (optionally) a run profile:

```sh
# defaults (stateless, no database)
cargo run -p reshuffle-sim -- -c config.yaml input.fb.gz

# a run profile (steps, scheduler, copy/replace, …)
cargo run -p reshuffle-sim -- -c config.yaml --profile run.yaml input.fb.gz

# multistep against your own (empty) database instead of an ephemeral container
cargo run -p reshuffle-sim -- -c config.yaml --profile run.yaml \
    --database-url postgres://user:pass@localhost:5432/db input.fb.gz
```

`--database-url` (multistep only), `--ingest-only`/`--skip-ingest` (see below) and `--dhat` stay CLI
flags; everything else is set in the profile. When `--profile` is omitted every field takes its default.

### Seeding a reusable database

Loading a large baseline into Postgres dominates a multistep run. Split it out so the benchmark can be
repeated against a restored snapshot instead of re-ingesting each time (multistep + `--database-url` only):

```sh
# 1. Load datasets, workers and chunks, then stop before scheduling.
cargo run -p reshuffle-sim -- -c config.yaml --profile run.yaml \
    --database-url "$DB" --ingest-only input.fb.gz
# 2. Snapshot the seeded database (e.g. pg_dump / a volume snapshot).
# 3. Restore it and benchmark straight from the placement — no re-ingest.
cargo run -p reshuffle-sim -- -c config.yaml --profile run.yaml \
    --database-url "$DB" --skip-ingest input.fb.gz
```

`--skip-ingest` reuses whatever is already in the database, so run it with the **same input and profile**
you ingested with. The two flags are mutually exclusive.

## Profile fields

All fields are optional. `copy`/`replace` are lists, so a run can do several. Unknown keys are rejected.

| Field | Default | Meaning |
|-------|---------|---------|
| `chunks_per_step` | `1000` | New chunks per step (when `chunks_shape` is unset). |
| `chunks_shape` | — | Per-step ingestion shape, overriding `chunks_per_step` (see below). |
| `chunk_size` | dataset avg | Size of each generated chunk, e.g. `256MiB`. |
| `steps` | `10` | Number of simulation steps. |
| `seed` | `42` | RNG seed. |
| `scheduler` | `stateless` | `stateless` or `multistep`. |
| `report` | — | Path to write an HTML report with charts. |
| `restricted_fraction` | `0.0` | Share of each step's new chunks routed to the restricted dataset. |
| `restricted_dataset` | `restricted` | Bucket of the synthetic restricted dataset. |
| `upgrade_schedule` | — | `step:fraction` breakpoints for the share of upgraded workers. |
| `initial_new_fraction` | `0.0` | Share of workers already on the new version at step 0. |
| `lift_restriction_at_step` | — | Step at which the version restriction is dropped. |
| `copy` | — | List of `{ src, dst, at_step }` — see below. |
| `replace` | — | List of `{ dataset, at_step }` — see below (multistep only). |

```yaml
# run.yaml
chunks_per_step: 2000
steps: 7
scheduler: multistep
copy:
  - { src: base-mainnet, dst: base-mainnet-2, at_step: 3 }
replace:
  - { dataset: binance-mainnet, at_step: 5 }
```

See `deploy/scenarios/` for ready-made profiles.

## Notes

- Multistep seeds the baseline and schedules it from empty, so step 1's reference is the scheduler's own placement, not the input file's; each step measures movement relative to the previous step.
- Chunks are inserted one row per `INSERT`, so large inputs are slow against Postgres — prefer smaller `chunks_per_step` / `steps`.
- The version-restriction and worker-upgrade modeling is config-driven (a dedicated restricted dataset carries a `minimum_worker_version`) and honored by **both** schedulers.

## Copying and replacing datasets

- `copy: [{ src, dst, at_step }]` — at step `at_step`, clone dataset `src` into a new dataset `dst`
  (identical chunk ids, ranges and sizes; inherits `src`'s weight). Both schedulers.
- `replace: [{ dataset, at_step }]` — at step `at_step`, replace every chunk of `dataset` with a
  same-range copy (new id), driving the scheduler's correction path. Multistep only.

```yaml
copy:
  - { src: src-bucket, dst: copy-bucket, at_step: 10 }
replace:
  - { dataset: some-bucket, at_step: 10 }
```

## Chunk ingestion shapes

By default the simulator ingests `chunks_per_step` chunks every step. Set `chunks_shape` to vary
that count over the run instead — it's a `name:key=value,…` spec where **every parameter has a
default and can be overridden**, so the shape name alone already works. The shape only changes *how
many* chunks are added per step; it composes with all the other options and takes precedence over
`chunks_per_step`.

| Shape | Spec (with defaults) | Behaviour |
|-------|----------------------|-----------|
| `constant` | `constant:n=1000` | flat (same as `chunks_per_step`) |
| `ramp` | `ramp:from=0,to=1000` | linear from the first to the last step |
| `triangle` | `triangle:peak=1000,center=<middle>` | rises to `center`, then falls |
| `gaussian` | `gaussian:peak=1000,sigma=<steps/6>,center=<middle>` | smooth bump |
| `pulse` | `pulse:size=10000,at=<middle>,base=0,width=1` | one bulk insert |
| `bursts` | `bursts:size=10000,every=5,base=0,until=<end>` | periodic bulk inserts |
| `points` | `points:1=0,50=5000,100=0` | arbitrary piecewise-linear profile |

```yaml
steps: 100
chunks_shape: "triangle:peak=5000"          # ramp up to the middle, then decline
# chunks_shape: "pulse:size=50000"          # one large bulk insert in the middle
# chunks_shape: "bursts:size=10000,every=5,until=50"   # bulk every 5 steps for the first half
# chunks_shape: "points:1=0,50=5000,100=0"  # hand-drawn piecewise-linear profile
```

## Version-restricted chunks & worker upgrades

The simulator can route a fraction of each step's **new chunks** into a dedicated **restricted
dataset** that requires a newer worker version. That dataset's config segment carries a
`minimum_worker_version` both schedulers read, so the restriction is honored identically by the
stateless and multistep paths. The existing datasets keep their normal size-weighted generation;
only the restricted share is split off. The simulator also models the worker fleet upgrading to
that version over time, showing how reshuffling behaves while a rollout is in progress.

```yaml
chunks_per_step: 200
steps: 60
restricted_fraction: 0.3
upgrade_schedule: "1:0,5:0.25,30:0.6,50:1.0"
report: report.html
```

- `restricted_fraction` — share (0.0–1.0) of each step's new chunks generated into the restricted
  dataset (which requires the new worker version). Default `0.0` (feature off).
- `restricted_dataset` — bucket name of the synthetic restricted dataset. Default `restricted`.
  Only used when `restricted_fraction > 0`.
- `upgrade_schedule` — ascending `step:fraction` breakpoints for the share of workers on the new
  version, held constant between breakpoints (e.g. `1:0,5:0.25,50:1.0`). Default empty.
- `initial_new_fraction` — share (0.0–1.0) of workers already on the new version at step 0.
  Default `0.0`.

With these at their defaults the run is identical to the basic simulation. The report and console
table gain columns for workers eligible for restricted data, whether the step scheduled
successfully, and the restricted download volume.

When restricted data cannot fit the eligible-worker pool the scheduler backs off (it returns a
capacity error rather than placing the data). The simulator records the failed step and stops — so
an early, under-upgraded fleet ends the run cleanly (with the report written).
