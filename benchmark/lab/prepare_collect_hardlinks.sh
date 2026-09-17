#!/usr/bin/env bash
set -euo pipefail

RUN_ID="${1:?usage: prepare_collect_hardlinks.sh RUN_ID SOURCE_FILE}"
SOURCE="${2:?usage: prepare_collect_hardlinks.sh RUN_ID SOURCE_FILE}"

ROOT="$HOME/share/site/test-runs/lab/runs/$RUN_ID"
VERIFY="$ROOT/remote_sha256.tsv"

if [ ! -f "$SOURCE" ]; then
    echo "ERROR: source missing: $SOURCE"
    exit 1
fi

if [ ! -f "$VERIFY" ]; then
    echo "ERROR: verification result missing: $VERIFY"
    exit 1
fi

TOTAL="$(awk 'NR>1{n++} END{print n+0}' "$VERIFY")"
PASS="$(awk -F '\t' 'NR>1 && $6=="PASS"{n++} END{print n+0}' "$VERIFY")"

echo "verified_total=$TOTAL"
echo "verified_pass=$PASS"

if [ "$TOTAL" -le 0 ] || [ "$TOTAL" -ne "$PASS" ]; then
    echo "ERROR: remote SHA result is not all PASS"
    exit 1
fi

SRC_DEV="$(stat -c%d "$SOURCE")"
ROOT_DEV="$(stat -c%d "$ROOT")"

if [ "$SRC_DEV" != "$ROOT_DEV" ]; then
    echo "ERROR: source and run directory are on different filesystems"
    exit 1
fi

while IFS=$'\t' read -r role host rel size sha status remote
do
    [ "$role" = "role" ] && continue
    [ "$status" = "PASS" ] || continue

    dest="$ROOT/$role/$rel"

    mkdir -p "$(dirname "$dest")"
    rm -f "$dest"

    # 100个路径共享中央原始1GiB文件的同一个inode，
    # 不额外占100GiB。
    ln "$SOURCE" "$dest"
done < "$VERIFY"

NAME="$(basename "$SOURCE")"

COUNT="$(
    find \
        "$ROOT/receiver-host-1/receivers" \
        "$ROOT/receiver-host-2/receivers" \
        -type f \
        -name "$NAME" \
        -links +1 \
        2>/dev/null |
    wc -l
)"

echo "hardlinks_created=$COUNT"

if [ "$COUNT" -ne "$TOTAL" ]; then
    echo "ERROR: hardlink count mismatch"
    exit 1
fi

echo "===== SOURCE INODE ====="
stat -c 'inode=%i links=%h size=%s %n' "$SOURCE"

echo "COLLECT_HARDLINKS_READY"
