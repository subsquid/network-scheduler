#!/usr/bin/env bash
#
# Open psql against the Cloud SQL private-IP Postgres from your machine, via a socat relay pod
# port-forwarded to localhost (DATABASE_URL from env.sh, host swapped to 127.0.0.1). Quitting psql
# tears down the forward and deletes the socat pod. Extra args go to psql.
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=/dev/null
source "$DIR/env.sh"   # NAMESPACE, DATABASE_URL

LOCAL_PORT="${LOCAL_PORT:-5432}"

# Pull host:port out of DATABASE_URL. The password is URL-safe (no '@'), so the first '@' ends the
# userinfo and the next '/' ends the host:port.
hostport="${DATABASE_URL#*@}"; hostport="${hostport%%/*}"
DB_HOST="${hostport%%:*}"
DB_PORT="${hostport##*:}"

LOCAL_DATABASE_URL="${DATABASE_URL/@$DB_HOST:$DB_PORT/@127.0.0.1:$LOCAL_PORT}"

PF_PID=""
cleanup() {
    [ -n "$PF_PID" ] && kill "$PF_PID" 2>/dev/null || true
    kubectl delete pod pg-fwd -n "$NAMESPACE" --ignore-not-found >/dev/null 2>&1 || true
}
# Clean up on exit and on Ctrl-C/SIGTERM (the INT/TERM trap exits, firing the EXIT trap). Foreground
# psql handles SIGINT itself, so our trap only fires after it exits — Ctrl-C won't kill the tunnel mid-query.
trap cleanup EXIT
trap 'exit 130' INT TERM

echo "Starting socat relay pod -> $DB_HOST:$DB_PORT ..."
kubectl delete pod pg-fwd -n "$NAMESPACE" --ignore-not-found >/dev/null 2>&1 || true
kubectl run pg-fwd -n "$NAMESPACE" --image=alpine/socat --restart=Never -- \
    tcp-listen:5432,fork,reuseaddr "tcp-connect:$DB_HOST:$DB_PORT"

echo "Waiting for pg-fwd to be Ready ..."
kubectl wait --for=condition=Ready pod/pg-fwd -n "$NAMESPACE" --timeout=60s

echo "Port-forwarding localhost:$LOCAL_PORT -> pg-fwd:5432 ..."
kubectl port-forward -n "$NAMESPACE" pod/pg-fwd "$LOCAL_PORT:5432" >/dev/null 2>&1 &
PF_PID=$!

# Wait until the local port accepts connections before launching psql.
for _ in $(seq 1 30); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$LOCAL_PORT") 2>/dev/null; then
        exec 3>&-
        break
    fi
    sleep 0.5
done

echo "Opening psql (localhost:$LOCAL_PORT) ..."
psql "$LOCAL_DATABASE_URL" "$@"
