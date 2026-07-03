#!/usr/bin/env bash
#
# Container entrypoint for the reshuffle-sim image. Replays the simulation that we otherwise
# run by hand:
#
#   NO_COLOR=true RUST_LOG=error,pg_timing=debug reshuffle-sim \
#       --database-url "$DATABASE_URL" -c "$CONFIG" \
#       --chunks-per-step 2000 --steps 5 --scheduler multistep "$INPUT"
#
# What comes from where:
#   * DATABASE_URL   — the Postgres connection string, injected as a secret env var. No default;
#                      if it is unset the run fails loudly (set -u), which is what we want.
#   * config + input — copied into the container at the fixed CONFIG/INPUT paths below (via
#                      `kubectl cp`, or a volume mount) before the start signal is released, so
#                      those paths are hard-coded rather than configurable.
# The tuning knobs (CHUNKS_PER_STEP / STEPS / SCHEDULER / RUST_LOG) default to the values from our
# manual runs and can each be overridden via the environment. Any extra flags passed to the
# container ("$@") are appended verbatim, so one-off options like `--report /tmp/report.html`,
# `--seed 7` or `--dhat` just work.
set -euo pipefail

# Fixed drop paths: the config and input are placed here before the run starts, so hard-code them.
CONFIG=/app/config.yaml
INPUT=/app/mainnet.fb.1.gz

: "${CHUNKS_PER_STEP:=2000}"
: "${STEPS:=5}"
: "${SCHEDULER:=multistep}"

# Always disable coloured output — this only ever runs non-interactively.
export NO_COLOR=true
export RUST_LOG="${RUST_LOG:-error,pg_timing=debug}"

# Optional start gate. When $START_SIGNAL is set, block until that path exists before running.
# This lets you start the pod, `kubectl cp` the (potentially large) input file in, and only then
# release the run. Keeping the signal a *separate* path from the input means there's no race with a
# half-copied input — you create the signal only after the cp has finished:
#   kubectl exec POD -- touch /app/START
if [ -n "${START_SIGNAL:-}" ]; then
    echo "reshuffle-sim: waiting for start signal at ${START_SIGNAL} ..."
    until [ -e "${START_SIGNAL}" ]; do sleep 2; done
    echo "reshuffle-sim: start signal received; launching."
fi

exec /app/reshuffle-sim \
    --database-url "$DATABASE_URL" \
    --config "$CONFIG" \
    --chunks-per-step "$CHUNKS_PER_STEP" \
    --steps "$STEPS" \
    --scheduler "$SCHEDULER" \
    "$INPUT" \
    "$@"
