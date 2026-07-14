# reshuffle-sim deploy

Run `reshuffle-sim` as a one-shot Kubernetes pod against a Cloud SQL Postgres.

The image is built and pushed by the `reshuffle-sim docker` GitHub workflow
(`docker-reshuffle-sim.yml`): trigger it from the Actions tab to push `:latest` and `:<commit SHA>`
to GCR. `run-pod.sh` runs `:latest` by default; override with `IMAGE_TAG` to pin a SHA.

## Usage

```sh
# 1. Provision DB + user (prompts for the Cloud SQL instance and namespace); writes env.sh.
./setup-db.sh

# 2. Launch: creates the secret, applies the pod, copies config + profile + input in, tails logs.
CONFIG_FILE=./production_config.yaml \
  PROFILE_FILE=./scenarios/copy-replace.yaml \
  INPUT_FILE=./mainnet.fb.1.gz ./run-pod.sh
```

`setup-db.sh` generates a random password and a unique empty database per run (the sim migrates on
connect and requires an empty DB). The pod blocks on a start signal until `run-pod.sh` has copied the
files in, so there's no race with a half-copied input.

## Run modes

`MODE` selects what the pod does with the baseline data (needs a multistep profile for `ingest`/`resume`):

| `MODE` | Sim flag | Does |
|--------|----------|------|
| `full` (default) | — | Seeds the DB and runs the whole simulation. |
| `ingest` | `--ingest-only` | Seeds datasets/workers/chunks, then stops — snapshot the DB here. |
| `resume` | `--skip-ingest` | Skips seeding and runs against an already-seeded DB. |

Ingesting mainnet dominates a multistep run, so seed once and reuse the snapshot:

```sh
MODE=ingest ./run-pod.sh   # seed a fresh DB and stop
# snapshot the DB (Cloud SQL backup or pg_dump), restore it, point env.sh's DATABASE_URL at it
MODE=resume ./run-pod.sh   # run without re-ingesting
```

Use the **same input + profile** for `resume` that you seeded with.

## Logs & memory

`entrypoint.sh` tees stdout+stderr to `/logs/reshuffle-sim.log` on the 100Mi `reshuffle-sim-logs`
PVC, so the per-step record survives an OOM kill (the sim is capped at `limits.memory: 10Gi`).

```sh
# While the pod still exists:
kubectl -n "$NAMESPACE" logs reshuffle-sim

# After it's gone — read the durable copy off the PVC with a throwaway pod:
kubectl -n "$NAMESPACE" run reshuffle-sim-log-reader --rm -i --restart=Never --image=busybox \
  --overrides='{"spec":{"containers":[{"name":"r","image":"busybox","args":["cat","/logs/reshuffle-sim.log"],"volumeMounts":[{"name":"logs","mountPath":"/logs"}]}],"volumes":[{"name":"logs","persistentVolumeClaim":{"claimName":"reshuffle-sim-logs"}}]}}'
```

## Knobs

- `setup-db.sh`: `PROJECT`, `DB_USER`, `DB_HOST`, `DB_PORT`, `DB_SSLMODE` (env overrides).
- `run-pod.sh`: `MODE` (`full`/`ingest`/`resume`), `IMAGE_TAG` (defaults to `latest`; pin a commit SHA to run a specific build), `POD`, `CONFIG_FILE`, `PROFILE_FILE`, `INPUT_FILE`.
- Run parameters live in the `--profile` file (`scenarios/`); `RUST_LOG` / `LOG_FILE` in `pod.yaml` env.
