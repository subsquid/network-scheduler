#!/usr/bin/env bash
#
# Run the reshuffle-sim scenarios in the cloud, one at a time: fresh empty DB, launch detached, sleep
# SLEEP_SECONDS, next. Each run tees to a uniquely-named /logs PVC file; collect with fetch-logs.sh.
#
# Prereqs: ./setup-db.sh wrote env.sh. Run from the repo root.
# Env: SLEEP_SECONDS (default 5400=1.5h), SCENARIOS, CONFIG_FILE, INPUT_FILE, IMAGE_TAG.
# SLEEP_SECONDS must exceed a scenario's runtime — the next launch deletes the still-running pod.
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"

# Host-side inputs (gitignored, not in the repo); abort early if a run is missing one.
CONFIG_FILE="${CONFIG_FILE:-./production_config.yaml}"
INPUT_FILE="${INPUT_FILE:-./mainnet.fb.1.gz}"
for f in "$CONFIG_FILE" "$INPUT_FILE"; do
    [ -f "$f" ] || { echo "missing input file: $f" >&2; exit 1; }
done

# shellcheck source=/dev/null
source "$DIR/env.sh"   # PROJECT, INSTANCE, NAMESPACE, DB_USER, DATABASE_URL, ...

SLEEP_SECONDS="${SLEEP_SECONDS:-5400}"  # 1.5h per scenario
# Image tag for every scenario; pin a commit SHA to test this PR's build. Default latest.
IMAGE_TAG="${IMAGE_TAG:-latest}"

# In order; each maps to scenarios/<name>.yaml -> /logs/<name>.log. Controls (lag=0) first.
read -ra SCENARIOS <<< "${SCENARIOS:-baseline baseline-lag copy-replace copy-replace-lag copy-replace-lag-heavy}"

# Swap the db name in DATABASE_URL (…/<db>?query) for $1; password is '?'-free (setup-db.sh).
url_with_db() {
    local db="$1" head query
    head="${DATABASE_URL%%\?*}"       # …@host:port/<db>
    query="${DATABASE_URL#"$head"}"   # ?query (empty if none)
    echo "${head%/*}/$db$query"
}

echo "Namespace=$NAMESPACE, instance=$INSTANCE, sleep=${SLEEP_SECONDS}s/scenario"
echo "Scenarios: ${SCENARIOS[*]}"
echo

count=${#SCENARIOS[@]}
i=0
for s in "${SCENARIOS[@]}"; do
    i=$((i + 1))
    profile="$DIR/scenarios/$s.yaml"
    if [ ! -f "$profile" ]; then
        echo "[$s] missing profile $profile — skipping" >&2
        continue
    fi
    db="nsim_${s//-/_}_$(openssl rand -hex 3)"

    echo "==================== [$i/$count $s] $(date -u '+%Y-%m-%dT%H:%M:%SZ') ===================="
    echo "[$s] creating empty database $db and launching pod (log=/logs/$s.log) ..."
    # Fresh empty DB => MODE=full. Guard the launch so one failure drops just this scenario, not the
    # whole run; sleep only when it succeeded.
    if gcloud sql databases create "$db" --instance="$INSTANCE" --project="$PROJECT" \
        && DATABASE_URL="$(url_with_db "$db")" \
           LOG_FILE="/logs/$s.log" MODE=full FOLLOW=false IMAGE_TAG="$IMAGE_TAG" \
           CONFIG_FILE="$CONFIG_FILE" PROFILE_FILE="$profile" INPUT_FILE="$INPUT_FILE" \
           "$DIR/run-pod.sh"; then
        if [ "$i" -lt "$count" ]; then
            echo "[$s] sleeping ${SLEEP_SECONDS}s before the next scenario ..."
            sleep "$SLEEP_SECONDS"
        fi
    else
        echo "[$s] launch failed — skipping to the next scenario (any partial log stays on the PVC)" >&2
    fi
done

echo
echo "All $count scenarios launched. The last one is still running (~${SLEEP_SECONDS}s); once its pod"
echo "reaches a terminal state (no pod holding the /logs volume), collect every scenario's log with:"
echo "  ./tools/reshuffle_sim/deploy/fetch-logs.sh"
