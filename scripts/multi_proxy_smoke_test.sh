#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-"$ROOT_DIR/test-runs/builds/smoke/multi-proxy-smoke"}"
TEST_TMP_DIR="${TMPDIR:-"$ROOT_DIR/test-runs/tmp/smoke"}"
mkdir -p "$TEST_TMP_DIR"
WORK_DIR="$(mktemp -d "$TEST_TMP_DIR/multi-proxy.XXXXXX")"
PIDS=()

cleanup() {
    for pid in "${PIDS[@]}"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

wait_for_log() {
    local pattern="$1"
    local file="$2"
    for _ in {1..100}; do
        if grep -q "$pattern" "$file" 2>/dev/null; then
            return 0
        fi
        sleep 0.1
    done
    cat "$file" >&2 || true
    return 1
}

cmake -S "$ROOT_DIR" -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE=Debug
cmake --build "$BUILD_DIR" -j"$(nproc)"

mkdir -p "$WORK_DIR/cache-1" "$WORK_DIR/cache-2" \
    "$WORK_DIR/receiver-1" "$WORK_DIR/receiver-2"
dd if=/dev/urandom of="$WORK_DIR/input.bin" bs=1024 count=64 status=none

"$BUILD_DIR/site_proxy_sender" \
    --multicast-ip 239.255.42.99 --udp-port 5000 \
    --interface-ip 127.0.0.1 --control-port 6000 \
    --receiver-count 1 --pace-us 0 --gap-ms 0 --max-repair-rounds 3 \
    --central-listen 7000 --cache-dir "$WORK_DIR/cache-1" \
    >"$WORK_DIR/proxy-1.log" 2>&1 &
PIDS+=("$!")

"$BUILD_DIR/site_proxy_sender" \
    --multicast-ip 239.255.42.99 --udp-port 5000 \
    --interface-ip 127.0.0.1 --control-port 6001 \
    --receiver-count 1 --pace-us 0 --gap-ms 0 --max-repair-rounds 3 \
    --central-listen 7001 --cache-dir "$WORK_DIR/cache-2" \
    >"$WORK_DIR/proxy-2.log" 2>&1 &
PIDS+=("$!")

wait_for_log "waiting for central sender on port 7000" "$WORK_DIR/proxy-1.log"
wait_for_log "waiting for central sender on port 7001" "$WORK_DIR/proxy-2.log"

"$BUILD_DIR/receiver_agent" \
    --multicast-ip 239.255.42.99 --udp-port 5000 \
    --output-dir "$WORK_DIR/receiver-1" --proxy-ip 127.0.0.1 \
    --control-port 6000 --interface-ip 127.0.0.2 \
    >"$WORK_DIR/receiver-1.log" 2>&1 &
PIDS+=("$!")

"$BUILD_DIR/receiver_agent" \
    --multicast-ip 239.255.42.99 --udp-port 5000 \
    --output-dir "$WORK_DIR/receiver-2" --proxy-ip 127.0.0.1 \
    --control-port 6001 --interface-ip 127.0.0.3 \
    >"$WORK_DIR/receiver-2.log" 2>&1 &
PIDS+=("$!")

wait_for_log "registered receiver=127.0.0.2:" "$WORK_DIR/proxy-1.log"
wait_for_log "registered receiver=127.0.0.3:" "$WORK_DIR/proxy-2.log"

"$BUILD_DIR/central_sender" \
    --pace-us 0 \
    --proxy 127.0.0.1 7000 \
    --proxy 127.0.0.1 7001 \
    --file "$WORK_DIR/input.bin" \
    >"$WORK_DIR/central.log" 2>&1

for pid in "${PIDS[@]}"; do
    wait "$pid"
done

input_sha="$(sha256sum "$WORK_DIR/input.bin" | awk '{print $1}')"
for output in "$WORK_DIR/cache-1"/central-transfer-*.bin \
    "$WORK_DIR/cache-2"/central-transfer-*.bin \
    "$WORK_DIR/receiver-1"/transfer-*.bin \
    "$WORK_DIR/receiver-2"/transfer-*.bin; do
    [[ -f "$output" ]]
    [[ "$(sha256sum "$output" | awk '{print $1}')" == "$input_sha" ]]
done

grep -q "central session ended" "$WORK_DIR/central.log"
echo "multi-proxy smoke test passed"
