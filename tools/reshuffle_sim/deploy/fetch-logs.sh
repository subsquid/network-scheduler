#!/usr/bin/env bash
#
# Copy every scenario log off the /logs PVC into results/. Idempotent — re-run any time. Run after
# the scenarios finish: the RWO /logs volume needs the sim pod gone (deleted below if it lingers).
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=/dev/null
source "$DIR/env.sh"   # NAMESPACE, ...

RESULTS_DIR="${RESULTS_DIR:-./results}"
POD="reshuffle-sim"
READER="reshuffle-sim-log-reader"
LOG_PVC="reshuffle-sim-logs"

mkdir -p "$RESULTS_DIR"

# Free the RWO /logs volume: a terminated sim pod can still pin it to its node.
kubectl -n "$NAMESPACE" delete pod "$POD" --ignore-not-found
kubectl -n "$NAMESPACE" delete pod "$READER" --ignore-not-found

echo "Starting reader pod mounting PVC '$LOG_PVC' ..."
kubectl -n "$NAMESPACE" run "$READER" --image=busybox --restart=Never \
    --overrides="{\"spec\":{\"containers\":[{\"name\":\"r\",\"image\":\"busybox\",\"args\":[\"sleep\",\"3600\"],\"volumeMounts\":[{\"name\":\"logs\",\"mountPath\":\"/logs\"}]}],\"volumes\":[{\"name\":\"logs\",\"persistentVolumeClaim\":{\"claimName\":\"$LOG_PVC\"}}]}}"

kubectl -n "$NAMESPACE" wait --for=condition=Ready "pod/$READER" --timeout=120s

echo "Copying /logs -> $RESULTS_DIR ..."
# tar-pipe: robust and predictable (files land directly in $RESULTS_DIR); busybox ships tar.
kubectl -n "$NAMESPACE" exec "$READER" -- tar cf - -C /logs . | tar xf - -C "$RESULTS_DIR"

kubectl -n "$NAMESPACE" delete pod "$READER" --ignore-not-found

echo "Done. Logs in $RESULTS_DIR/:"
ls -la "$RESULTS_DIR"
