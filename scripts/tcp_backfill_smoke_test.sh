#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-"$ROOT_DIR/test-runs/builds/smoke/tcp-backfill-smoke"}"
TEST_TMP_DIR="${TMPDIR:-"$ROOT_DIR/test-runs/tmp/smoke"}"
mkdir -p "$TEST_TMP_DIR"
WORK_DIR="$(mktemp -d "$TEST_TMP_DIR/tcp-backfill.XXXXXX")"

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

SRCAST_BACKFILL_ROUNDS=0 "$BUILD_DIR/site_proxy_sender" \
    --multicast-ip 239.255.42.99 --udp-port 5000 \
    --interface-ip 127.0.0.1 --control-port 6000 \
    --receiver-count 2 --pace-us 100 --gap-ms 0 --max-repair-rounds 3 \
    --central-listen 7000 --cache-dir "$WORK_DIR/cache" \
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
    --multicast-ip 239.255.42.99 --udp-port 5000 \
    --output-dir "$WORK_DIR/receiver-1" --proxy-ip 127.0.0.1 \
    --control-port 6000 --interface-ip 127.0.0.2 \
    >"$WORK_DIR/receiver-1.log" 2>&1 &
RECEIVER1_PID="$!"

DROP_MANY="$(seq -s, 0 39)"
SRCAST_DROP_INITIAL_BLOCKS="$DROP_MANY" "$BUILD_DIR/receiver_agent" \
    --multicast-ip 239.255.42.99 --udp-port 5000 \
    --output-dir "$WORK_DIR/receiver-2" --proxy-ip 127.0.0.1 \
    --control-port 6000 --interface-ip 127.0.0.3 \
    >"$WORK_DIR/receiver-2.log" 2>&1 &
RECEIVER2_PID="$!"

for _ in {1..50}; do
    if [[ "$(grep -c 'registered receiver=' "$WORK_DIR/proxy.log" 2>/dev/null || true)" -ge 2 ]]; then
        break
    fi
    if ! kill -0 "$PROXY_PID" 2>/dev/null; then
        echo "proxy exited while waiting for receivers" >&2
        cat "$WORK_DIR/proxy.log" >&2
        exit 1
    fi
    sleep 0.1
done

"$BUILD_DIR/central_sender" --proxy 127.0.0.1 7000 --pace-us 0 \
    --file "$WORK_DIR/input.bin" \
    >"$WORK_DIR/central.log" 2>&1

wait "$PROXY_PID"
PROXY_PID=""
wait "$RECEIVER1_PID"
RECEIVER1_PID=""
wait "$RECEIVER2_PID"
RECEIVER2_PID=""

INPUT_SHA="$(sha256sum "$WORK_DIR/input.bin" | awk '{print $1}')"
CACHED="$(find "$WORK_DIR/cache" -maxdepth 1 -name 'central-transfer-*.bin' -print -quit)"
OUTPUT1="$WORK_DIR/receiver-1/input.bin"
OUTPUT2="$WORK_DIR/receiver-2/input.bin"

if [[ -z "$CACHED" || ! -f "$OUTPUT1" || ! -f "$OUTPUT2" ]]; then
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

grep -q "receiver=127.0.0.3:.* isolated for TCP backfill" "$WORK_DIR/proxy.log"
grep -q "TCP backfill receiver=127.0.0.3:.* blocks=" "$WORK_DIR/proxy.log"
grep -q "TCP backfill completed receiver=127.0.0.3:" "$WORK_DIR/proxy.log"
grep -q "TCP backfill begins" "$WORK_DIR/receiver-2.log"
grep -q "TCP backfill completed; waiting for COMPLETE_ACK" \
    "$WORK_DIR/receiver-2.log"
grep -q "central confirmed cached transfer_id=" "$WORK_DIR/central.log"

echo "TCP backfill smoke test passed"
