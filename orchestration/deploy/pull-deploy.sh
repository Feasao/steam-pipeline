#!/usr/bin/env bash
# Pull-based deploy for the VM.
# Brings the clone up to date with origin/main. DAG and script changes take effect without a
# restart since the repo is mounted to the scheduler. Changes to what is baked into the image or
# to the compose setup trigger `astro dev restart`. Does not deploy while a DAG run is active.
set -euo pipefail

REPO="${REPO:-$HOME/steam-pipeline}"
DAG_ID="steam_daily"
REBUILD_PATHS='^orchestration/(Dockerfile|requirements\.txt|packages\.txt|include/|docker-compose\.override\.yml|\.astro/config\.yaml)'

dag_run_active() {
    local scheduler state
    scheduler=$(docker ps --format '{{.Names}}' | grep -- '-scheduler-1$' | head -n 1) || return 1
    for state in queued running; do
        if docker exec "$scheduler" airflow dags list-runs "$DAG_ID" --state "$state" -o json 2>/dev/null \
            | grep -q '"run_id"'; then
            return 0
        fi
    done
    return 1
}

main() {
    cd "$REPO"
    git fetch --quiet origin main

    local current new rebuild=no
    current=$(git rev-parse HEAD)
    new=$(git rev-parse origin/main)
    [ "$current" = "$new" ] && exit 0 # nothing new

    if git diff --name-only "$current" "$new" | grep -qE "$REBUILD_PATHS"; then
        rebuild=yes
    fi

    if dag_run_active; then
        echo "new commits ${new:0:7} waiting: a $DAG_ID run is active, retrying on the next tick"
        exit 0
    fi

    git merge --ff-only --quiet origin/main
    echo "deployed ${current:0:7} -> ${new:0:7} (rebuild: $rebuild)"

    if [ "$rebuild" = yes ]; then
        cd orchestration
        astro dev restart --no-browser
        echo "Airflow restarted with the new image"
    fi
}

# Calling main from here and exiting makes bash read the whole file before running it,
# so `git merge` replacing this script mid-run cannot break the running copy.
main "$@"
exit