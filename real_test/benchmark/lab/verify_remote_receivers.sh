#!/usr/bin/env bash
set -euo pipefail

RUN_ID="${1:?usage: verify_remote_receivers.sh RUN_ID SOURCE_FILE EXPECTED_COUNT}"
SOURCE="${2:?usage: verify_remote_receivers.sh RUN_ID SOURCE_FILE EXPECTED_COUNT}"
EXPECTED_COUNT="${3:?usage: verify_remote_receivers.sh RUN_ID SOURCE_FILE EXPECTED_COUNT}"

SSH_USER="sky3141"
SSH_KEY="$HOME/.ssh/id_ed25519"

R1="10.100.14.9"
R2="10.100.20.143"

if [[ ! "$RUN_ID" =~ ^[A-Za-z0-9._-]+$ ]]; then
    echo "invalid run_id: $RUN_ID"
    exit 1
fi

if [ ! -f "$SOURCE" ]; then
    echo "source does not exist: $SOURCE"
    exit 1
fi

NAME="$(basename "$SOURCE")"
EXPECTED_SHA="$(sha256sum "$SOURCE" | awk '{print $1}')"
EXPECTED_SIZE="$(stat -c%s "$SOURCE")"

LOCAL_RUN="$HOME/share/site/test-runs/lab/runs/$RUN_ID"
mkdir -p "$LOCAL_RUN"

OUT="$LOCAL_RUN/remote_sha256.tsv"
TMP1="$(mktemp)"
TMP2="$(mktemp)"

cleanup()
{
    rm -f "$TMP1" "$TMP2"
}
trap cleanup EXIT

verify_host()
{
    local host="$1"
    local role="$2"
    local output="$3"

    ssh \
        -o BatchMode=yes \
        -o ConnectTimeout=10 \
        -i "$SSH_KEY" \
        "$SSH_USER@$host" \
        bash -s -- \
        "$RUN_ID" \
        "$NAME" \
        "$EXPECTED_SHA" \
        "$EXPECTED_SIZE" \
        "$role" \
        "$host" > "$output" <<'REMOTE'

set -euo pipefail

RUN_ID="$1"
NAME="$2"
EXPECTED_SHA="$3"
EXPECTED_SIZE="$4"
ROLE="$5"
HOST="$6"

ROOT="$HOME/share/site/test-runs/lab/runs/$RUN_ID"

if [ ! -d "$ROOT/receivers" ]; then
    exit 0
fi

find "$ROOT/receivers" \
    -mindepth 2 \
    -maxdepth 2 \
    -type f \
    -name "$NAME" \
    -print0 |
while IFS= read -r -d '' file
do
    size="$(stat -c%s "$file")"
    sha="$(sha256sum "$file" | awk '{print $1}')"

    rel="${file#$ROOT/}"

    status="FAIL"

    if [ "$size" = "$EXPECTED_SIZE" ] &&
       [ "$sha" = "$EXPECTED_SHA" ]
    then
        status="PASS"
    fi

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$ROLE" \
        "$HOST" \
        "$rel" \
        "$size" \
        "$sha" \
        "$status" \
        "$file"
done
REMOTE
}

echo "============================================"
echo "REMOTE RECEIVER SHA256 VERIFY"
echo "run_id=$RUN_ID"
echo "source=$SOURCE"
echo "expected_count=$EXPECTED_COUNT"
echo "expected_size=$EXPECTED_SIZE"
echo "expected_sha=$EXPECTED_SHA"
echo "============================================"

# 两台物理 Receiver 主机同时计算 SHA。
verify_host "$R1" "receiver-host-1" "$TMP1" &
P1=$!

verify_host "$R2" "receiver-host-2" "$TMP2" &
P2=$!

RC1=0
RC2=0

wait "$P1" || RC1=$?
wait "$P2" || RC2=$?

if [ "$RC1" -ne 0 ] || [ "$RC2" -ne 0 ]; then
    echo "ERROR: remote verification command failed"
    exit 1
fi

{
    printf 'role\thost\trelative_path\tsize\tsha256\tstatus\tremote_path\n'
    cat "$TMP1"
    cat "$TMP2"
} > "$OUT"

TOTAL="$(
    tail -n +2 "$OUT" |
    awk 'END {print NR+0}'
)"

PASS="$(
    tail -n +2 "$OUT" |
    awk -F '\t' '$6=="PASS"{n++} END{print n+0}'
)"

FAIL=$((TOTAL-PASS))

echo
echo "total=$TOTAL"
echo "pass=$PASS"
echo "fail=$FAIL"
echo "result=$OUT"

if [ "$TOTAL" -ne "$EXPECTED_COUNT" ]; then
    echo "ERROR: expected $EXPECTED_COUNT files, found $TOTAL"
    exit 1
fi

if [ "$PASS" -ne "$EXPECTED_COUNT" ]; then
    echo "ERROR: receiver SHA verification failed"
    awk -F '\t' 'NR==1 || $6!="PASS"' "$OUT"
    exit 1
fi

echo "REMOTE_SHA256_ALL_PASS"
