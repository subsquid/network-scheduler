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
# DATABASE_URL, NAMESPACE, ... — gitignored; run ./setup-db.sh first to create it.
# shellcheck source=/dev/null
source "$DIR/env.sh"

POD="${POD:-reshuffle-sim}"
CONFIG_FILE="${CONFIG_FILE:-./production_config.yaml}"
PROFILE_FILE="${PROFILE_FILE:-$DIR/scenarios/copy-replace.yaml}"
INPUT_FILE="${INPUT_FILE:-./mainnet.fb.1.gz}"
# Image tag to run; the docker workflow pushes :latest and :<12-char commit SHA>. Pin a SHA to run a
# specific build, else latest.
IMAGE_TAG="${IMAGE_TAG:-latest}"

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

echo "Applying the pod (mode: $MODE, image tag: $IMAGE_TAG) ..."
# Rewrite SIM_MODE ("full" is its only occurrence) and the image tag before applying.
sed -e "s/value: \"full\"/value: \"$MODE\"/" \
    -e "s|subsquid/reshuffle-sim:[^[:space:]]*|subsquid/reshuffle-sim:$IMAGE_TAG|" \
    "$DIR/pod.yaml" | kubectl -n "$NAMESPACE" apply -f -

echo "Waiting for pod/$POD to be Ready ..."
kubectl -n "$NAMESPACE" wait --for=condition=Ready "pod/$POD" --timeout=180s

echo "Copying config, profile and input into the pod ..."
kubectl cp "$CONFIG_FILE"  "$NAMESPACE/$POD:/app/config.yaml"
kubectl cp "$PROFILE_FILE" "$NAMESPACE/$POD:/app/profile.yaml"
kubectl cp "$INPUT_FILE"   "$NAMESPACE/$POD:/app/mainnet.fb.1.gz"

echo "Releasing start signal ..."
kubectl -n "$NAMESPACE" exec "$POD" -- touch /app/START

kubectl -n "$NAMESPACE" logs -f "$POD"
