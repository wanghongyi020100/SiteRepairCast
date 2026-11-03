#!/usr/bin/env bash
set -euo pipefail

RUN_ID="${1:?usage: trim_run_large_files.sh RUN_ID}"

if [[ ! "$RUN_ID" =~ ^[A-Za-z0-9._-]+$ ]]; then
    echo "invalid run_id: $RUN_ID"
    exit 1
fi

SSH_USER="sky3141"
SSH_KEY="$HOME/.ssh/id_ed25519"

PROXY="10.100.27.64"
R1="10.100.14.9"
R2="10.100.20.143"

LOCAL_ROOT="$HOME/share/site/test-runs/lab/runs/$RUN_ID"

trim_local()
{
    local root="$1"
    local role="$2"

    echo
    echo "===== $role ====="

    if [ ! -d "$root" ]; then
        echo "run directory absent: $root"
        return 0
    fi

    echo "--- before ---"
    du -sh "$root" || true

    echo "--- deleting files >16MiB ---"

    find "$root" \
        -type f \
        -size +16M \
        -print \
        -delete

    echo "--- after ---"
    du -sh "$root" || true
}

trim_remote()
{
    local host="$1"
    local role="$2"

    echo
    echo "===== $role $host ====="

    ssh \
        -o BatchMode=yes \
        -o ConnectTimeout=10 \
        -i "$SSH_KEY" \
        "$SSH_USER@$host" \
        bash -s -- "$RUN_ID" <<'REMOTE'
set -euo pipefail

RUN_ID="$1"
ROOT="$HOME/share/site/test-runs/lab/runs/$RUN_ID"

if [ ! -d "$ROOT" ]; then
    echo "run directory absent: $ROOT"
    exit 0
fi

echo "--- before ---"
du -sh "$ROOT" || true

echo "--- deleting files >16MiB ---"

find "$ROOT" \
    -type f \
    -size +16M \
    -print \
    -delete

echo "--- after ---"
du -sh "$ROOT" || true
REMOTE
}

trim_local "$LOCAL_ROOT" "CENTRAL"
trim_remote "$PROXY" "PROXY"
trim_remote "$R1" "RECEIVER-HOST-1"
trim_remote "$R2" "RECEIVER-HOST-2"

echo
echo "large payload trim OK: $RUN_ID"
