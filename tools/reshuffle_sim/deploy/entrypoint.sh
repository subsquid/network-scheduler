#!/usr/bin/env bash
#
# reshuffle-sim image entrypoint. Runs the sim against $DATABASE_URL (secret env; unset fails under
# set -u), with config/profile/input copied to the fixed /app paths below before the start signal.
set -euo pipefail

CONFIG=/app/config.yaml
PROFILE=/app/profile.yaml
INPUT=/app/mainnet.fb.1.gz
LOG_FILE="${LOG_FILE:-/logs/reshuffle-sim.log}"

# Non-interactive: no coloured output.
export NO_COLOR=true
export RUST_LOG="${RUST_LOG:-error,pg_timing=debug}"

mkdir -p "$(dirname "$LOG_FILE")"
exec > >(tee "$LOG_FILE") 2>&1
echo "reshuffle-sim: logging to ${LOG_FILE}"

# Start gate: block until $START_SIGNAL exists, so a large input is fully copied in before we run.
if [ -n "${START_SIGNAL:-}" ]; then
    echo "reshuffle-sim: waiting for start signal at ${START_SIGNAL} ..."
    until [ -e "${START_SIGNAL}" ]; do sleep 2; done
    echo "reshuffle-sim: start signal received; launching."
fi

# SIM_MODE -> sim flags (ingest/resume need a multistep profile):
#   full   — seed the DB and run the whole sim (default).
#   ingest — seed and stop, so the DB can be snapshotted.
#   resume — skip seeding, run against an already-seeded DB.
MODE_ARGS=()
case "${SIM_MODE:-full}" in
    full)   ;;
    ingest) MODE_ARGS+=(--ingest-only) ;;
    resume) MODE_ARGS+=(--skip-ingest) ;;
    *) echo "reshuffle-sim: unknown SIM_MODE '${SIM_MODE}' (want: full | ingest | resume)" >&2; exit 1 ;;
esac

exec /app/reshuffle-sim \
    --database-url "$DATABASE_URL" \
    --config "$CONFIG" \
    --profile "$PROFILE" \
    "$INPUT" \
    "${MODE_ARGS[@]}" \
    "$@"
