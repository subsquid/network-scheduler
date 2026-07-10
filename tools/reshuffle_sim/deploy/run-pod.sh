#!/usr/bin/env bash
#
# Drive the one-shot reshuffle-sim pod end to end, using env.sh (from setup-db.sh) for DATABASE_URL
# and NAMESPACE: refresh the secret, apply the pod, copy config + profile + input in, release the
# start signal, follow the logs.
#
# Override host-side files via CONFIG_FILE / PROFILE_FILE / INPUT_FILE; PROFILE_FILE picks the
# scenario (see scenarios/).
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"

# Host-side inputs (gitignored, not in the repo); abort early if a run is missing one.
CONFIG_FILE="${CONFIG_FILE:-./production_config.yaml}"
INPUT_FILE="${INPUT_FILE:-./mainnet.fb.1.gz}"
for f in "$CONFIG_FILE" "$INPUT_FILE"; do
    [ -f "$f" ] || { echo "missing input file: $f" >&2; exit 1; }
done

# DATABASE_URL, NAMESPACE, ... — gitignored; run ./setup-db.sh first to create it.
# Capture a caller's DATABASE_URL (run-scenarios.sh) before sourcing — env.sh's unconditional
# `export DATABASE_URL` would clobber it, pinning every scenario to one database.
CALLER_DATABASE_URL="${DATABASE_URL:-}"
# shellcheck source=/dev/null
source "$DIR/env.sh"
DATABASE_URL="${CALLER_DATABASE_URL:-$DATABASE_URL}"

POD="${POD:-reshuffle-sim}"
PROFILE_FILE="${PROFILE_FILE:-$DIR/scenarios/copy-replace.yaml}"
# Image tag; docker workflow pushes :latest and :<commit SHA>. Fills pod.yaml's {IMAGE_TAG}.
IMAGE_TAG="${IMAGE_TAG:-latest}"
# Durable log path on the /logs PVC; per-run unique names (run-scenarios.sh) avoid overwrites.
# Fills pod.yaml's {LOG_FILE} — this default is the source of truth.
LOG_FILE="${LOG_FILE:-/logs/reshuffle-sim.log}"

# MODE (ingest/resume need a multistep profile):
#   full   — seed and run the whole sim (default).
#   ingest — seed and stop; snapshot the DB to reuse it.
#   resume — skip seeding, run against an already-seeded DB.
MODE="${MODE:-full}"
case "$MODE" in
    full | ingest | resume) ;;
    *) echo "MODE must be full | ingest | resume (got '$MODE')" >&2; exit 1 ;;
esac

echo "Creating/refreshing the reshuffle-sim-db secret in namespace '$NAMESPACE' ..."
kubectl -n "$NAMESPACE" create secret generic reshuffle-sim-db \
    --from-literal=url="$DATABASE_URL" \
    --dry-run=client -o yaml | kubectl -n "$NAMESPACE" apply -f -

# Delete a prior run's pod first: its name is fixed, so `apply` would no-op (or reject an immutable
# change like SIM_MODE). The PVC and its log persist; deleting frees the ReadWriteOnce volume.
echo "Deleting any existing pod/$POD ..."
kubectl -n "$NAMESPACE" delete pod "$POD" --ignore-not-found

echo "Applying the pod (mode: $MODE, image tag: $IMAGE_TAG, log: $LOG_FILE) ..."
# Fill pod.yaml's placeholders before applying ('|' for the log path — it has slashes).
sed -e "s/{SIM_MODE}/$MODE/" \
    -e "s/{IMAGE_TAG}/$IMAGE_TAG/" \
    -e "s|{LOG_FILE}|$LOG_FILE|" \
    "$DIR/pod.yaml" | kubectl -n "$NAMESPACE" apply -f -

echo "Waiting for pod/$POD to be Ready ..."
kubectl -n "$NAMESPACE" wait --for=condition=Ready "pod/$POD" --timeout=180s

echo "Copying config, profile and input into the pod ..."
kubectl cp "$CONFIG_FILE"  "$NAMESPACE/$POD:/app/config.yaml"
kubectl cp "$PROFILE_FILE" "$NAMESPACE/$POD:/app/profile.yaml"
kubectl cp "$INPUT_FILE"   "$NAMESPACE/$POD:/app/mainnet.fb.1.gz"

echo "Releasing start signal ..."
kubectl -n "$NAMESPACE" exec "$POD" -- touch /app/START

# FOLLOW=false (run-scenarios.sh) detaches after launch — no hours-long `logs -f` to drop; the sim
# tees to the /logs PVC regardless. Default true keeps the interactive single-run behaviour.
if [ "${FOLLOW:-true}" = true ]; then
    kubectl -n "$NAMESPACE" logs -f "$POD"
else
    echo "Detached (FOLLOW=false). Durable log: $LOG_FILE (on the reshuffle-sim-logs PVC)."
    # Liveness peek only — too early for parse errors/results; use `logs -f` or the durable log.
    echo "First log lines (liveness peek, not a completion/validation check):"
    kubectl -n "$NAMESPACE" logs "$POD" --tail=20 || true
fi
