#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-"$ROOT_DIR/build-tcp-backfill-smoke"}"
WORK_DIR="$(mktemp -d)"

PROXY_PID=""
RECEIVER1_PID=""
RECEIVER2_PID=""

cleanup() {
    for pid in "$PROXY_PID" "$RECEIVER1_PID" "$RECEIVER2_PID"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

cmake -S "$ROOT_DIR" -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE=Debug
cmake --build "$BUILD_DIR" -j"$(nproc)"

dd if=/dev/urandom of="$WORK_DIR/input.bin" bs=1024 count=256 status=none
mkdir -p "$WORK_DIR/cache" "$WORK_DIR/receiver-1" "$WORK_DIR/receiver-2"

SRCAST_SLOW_MISSING_THRESHOLD=8 "$BUILD_DIR/site_proxy_sender" \
    239.255.42.99 5000 127.0.0.1 \
    6000 2 100 0 3 \
    --central-listen 7000 "$WORK_DIR/cache" \
    >"$WORK_DIR/proxy.log" 2>&1 &
PROXY_PID="$!"

for _ in {1..50}; do
    if grep -q "waiting for central sender on port 7000" \
        "$WORK_DIR/proxy.log"; then
        break
    fi
    if ! kill -0 "$PROXY_PID" 2>/dev/null; then
        echo "proxy exited before accepting receivers" >&2
        cat "$WORK_DIR/proxy.log" >&2
        exit 1
    fi
    sleep 0.1
done

"$BUILD_DIR/receiver_agent" \
    239.255.42.99 5000 "$WORK_DIR/receiver-1" \
    127.0.0.1 6000 1 127.0.0.1 \
    >"$WORK_DIR/receiver-1.log" 2>&1 &
RECEIVER1_PID="$!"

DROP_MANY="$(seq -s, 0 39)"
SRCAST_DROP_INITIAL_BLOCKS="$DROP_MANY" "$BUILD_DIR/receiver_agent" \
    239.255.42.99 5000 "$WORK_DIR/receiver-2" \
    127.0.0.1 6000 2 127.0.0.1 \
    >"$WORK_DIR/receiver-2.log" 2>&1 &
RECEIVER2_PID="$!"

for _ in {1..50}; do
    if grep -q "registered receiver_id=2" "$WORK_DIR/proxy.log"; then
        break
    fi
    if ! kill -0 "$PROXY_PID" 2>/dev/null; then
        echo "proxy exited while waiting for receivers" >&2
        cat "$WORK_DIR/proxy.log" >&2
        exit 1
    fi
    sleep 0.1
done

"$BUILD_DIR/central_sender" 127.0.0.1 7000 0 "$WORK_DIR/input.bin" \
    >"$WORK_DIR/central.log" 2>&1

wait "$PROXY_PID"
PROXY_PID=""
wait "$RECEIVER1_PID"
RECEIVER1_PID=""
wait "$RECEIVER2_PID"
RECEIVER2_PID=""

INPUT_SHA="$(sha256sum "$WORK_DIR/input.bin" | awk '{print $1}')"
CACHED="$(find "$WORK_DIR/cache" -maxdepth 1 -name 'central-transfer-*.bin' -print -quit)"
OUTPUT1="$(find "$WORK_DIR/receiver-1" -maxdepth 1 -name 'transfer-*.bin' -print -quit)"
OUTPUT2="$(find "$WORK_DIR/receiver-2" -maxdepth 1 -name 'transfer-*.bin' -print -quit)"

if [[ -z "$CACHED" || -z "$OUTPUT1" || -z "$OUTPUT2" ]]; then
    echo "expected output missing" >&2
    cat "$WORK_DIR/central.log" >&2
    cat "$WORK_DIR/proxy.log" >&2
    cat "$WORK_DIR/receiver-1.log" >&2
    cat "$WORK_DIR/receiver-2.log" >&2
    exit 1
fi

[[ "$(sha256sum "$CACHED" | awk '{print $1}')" == "$INPUT_SHA" ]]
[[ "$(sha256sum "$OUTPUT1" | awk '{print $1}')" == "$INPUT_SHA" ]]
[[ "$(sha256sum "$OUTPUT2" | awk '{print $1}')" == "$INPUT_SHA" ]]

grep -q "receiver_id=2 isolated for TCP backfill" "$WORK_DIR/proxy.log"
grep -q "TCP backfill receiver_id=2 blocks=" "$WORK_DIR/proxy.log"
grep -q "TCP backfill completed receiver_id=2" "$WORK_DIR/proxy.log"
grep -q "TCP backfill begins" "$WORK_DIR/receiver-2.log"
grep -q "TCP backfill completed; waiting for COMPLETE_ACK" \
    "$WORK_DIR/receiver-2.log"
grep -q "central confirmed cached transfer_id=" "$WORK_DIR/central.log"

echo "tcp backfill smoke test passed"

