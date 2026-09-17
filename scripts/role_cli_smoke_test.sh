#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-"$ROOT_DIR/test-runs/builds/smoke/role-cli-smoke"}"
TEST_TMP_DIR="${TMPDIR:-"$ROOT_DIR/test-runs/tmp/smoke"}"
mkdir -p "$TEST_TMP_DIR"
WORK_DIR="$(mktemp -d "$TEST_TMP_DIR/role-cli.XXXXXX")"

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

wait_for_log() {
    local pattern="$1"
    local file="$2"
    for _ in {1..100}; do
        if grep -q "$pattern" "$file" 2>/dev/null; then return 0; fi
        sleep 0.1
    done
    echo "timed out waiting for $pattern" >&2
    cat "$file" >&2 || true
    return 1
}

expect_failure() {
    local expected="$1"
    shift
    local log="$WORK_DIR/argument-error.log"
    if "$@" >"$log" 2>&1; then
        echo "command unexpectedly succeeded: $*" >&2
        return 1
    fi
    grep -Fq -- "$expected" "$log" || {
        echo "missing expected error: $expected" >&2
        cat "$log" >&2
        return 1
    }
}

cmake -S "$ROOT_DIR" -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE=Debug
cmake --build "$BUILD_DIR" -j"$(nproc)"

for role in central_sender site_proxy_sender receiver_agent; do
    "$BUILD_DIR/$role" --help >"$WORK_DIR/$role-help.log"
    grep -q "ROLE:" "$WORK_DIR/$role-help.log"
    grep -q "Usage:" "$WORK_DIR/$role-help.log"
    grep -q "Required options" "$WORK_DIR/$role-help.log"
    grep -q "Optional options" "$WORK_DIR/$role-help.log"
    grep -q "default" "$WORK_DIR/$role-help.log"
    grep -q "Examples:" "$WORK_DIR/$role-help.log"
done

expect_failure "invalid --proxy port: 70000" \
    "$BUILD_DIR/central_sender" --proxy 127.0.0.1 70000 --file input.bin
expect_failure "--interface-ip is required" \
    "$BUILD_DIR/site_proxy_sender" --receiver-count 2 --central-listen 17000 \
    --cache-dir "$WORK_DIR/cache"
expect_failure "invalid --multicast-ip: 192.168.1.1 is not multicast" \
    "$BUILD_DIR/site_proxy_sender" --multicast-ip 192.168.1.1 \
    --interface-ip 127.0.0.1 --receiver-count 2 --file input.bin
expect_failure "invalid --receiver-count: 0" \
    "$BUILD_DIR/site_proxy_sender" --interface-ip 127.0.0.1 \
    --receiver-count 0 --file input.bin
expect_failure "invalid --section-blocks: 0" \
    "$BUILD_DIR/central_sender" --proxy 127.0.0.1 17000 \
    --section-blocks 0 --file input.bin
expect_failure "unknown option: --unknown" \
    "$BUILD_DIR/central_sender" --unknown
expect_failure "missing --proxy-ip value" \
    "$BUILD_DIR/receiver_agent" --interface-ip 127.0.0.2 --proxy-ip

mkdir -p "$WORK_DIR/cache" "$WORK_DIR/receiver-1" "$WORK_DIR/receiver-2"
dd if=/dev/urandom of="$WORK_DIR/input.bin" bs=1024 count=256 status=none

SRCAST_REPAIR_MODE=baseline SRCAST_SECTION_BLOCKS=17 \
"$BUILD_DIR/site_proxy_sender" \
    --multicast-ip 239.255.42.99 \
    --udp-port 15000 \
    --interface-ip 127.0.0.1 \
    --control-port 16000 \
    --receiver-count 2 \
    --pace-us 100 \
    --gap-ms 0 \
    --max-repair-rounds 3 \
    --section-blocks 64 \
    --repair-mode peer \
    --central-listen 17000 \
    --cache-dir "$WORK_DIR/cache" \
    >"$WORK_DIR/proxy.log" 2>&1 &
PROXY_PID="$!"
wait_for_log "waiting for central sender on port 17000" "$WORK_DIR/proxy.log"

"$BUILD_DIR/receiver_agent" \
    --multicast-ip 239.255.42.99 \
    --udp-port 15000 \
    --interface-ip 127.0.0.2 \
    --proxy-ip 127.0.0.1 \
    --control-port 16000 \
    --output-dir "$WORK_DIR/receiver-1" \
    >"$WORK_DIR/receiver-1.log" 2>&1 &
RECEIVER1_PID="$!"

"$BUILD_DIR/receiver_agent" \
    --multicast-ip 239.255.42.99 \
    --udp-port 15000 \
    --interface-ip 127.0.0.3 \
    --proxy-ip 127.0.0.1 \
    --control-port 16000 \
    --output-dir "$WORK_DIR/receiver-2" \
    >"$WORK_DIR/receiver-2.log" 2>&1 &
RECEIVER2_PID="$!"

for _ in {1..100}; do
    if [[ "$(grep -c 'registered receiver=' "$WORK_DIR/proxy.log" 2>/dev/null || true)" -ge 2 ]]; then
        break
    fi
    sleep 0.1
done
[[ "$(grep -c 'registered receiver=' "$WORK_DIR/proxy.log" 2>/dev/null || true)" -ge 2 ]]

SRCAST_SECTION_BLOCKS=17 "$BUILD_DIR/central_sender" \
    --proxy 127.0.0.1 17000 \
    --pace-us 0 \
    --section-blocks 64 \
    --file "$WORK_DIR/input.bin" \
    >"$WORK_DIR/central.log" 2>&1

wait "$PROXY_PID"
PROXY_PID=""
wait "$RECEIVER1_PID"
RECEIVER1_PID=""
wait "$RECEIVER2_PID"
RECEIVER2_PID=""

input_sha="$(sha256sum "$WORK_DIR/input.bin" | awk '{print $1}')"
for output in "$WORK_DIR/cache"/central-transfer-*.bin \
    "$WORK_DIR/receiver-1/input.bin" \
    "$WORK_DIR/receiver-2/input.bin"; do
    [[ -f "$output" ]]
    [[ "$(sha256sum "$output" | awk '{print $1}')" == "$input_sha" ]]
done

grep -q "ROLE: CENTRAL" "$WORK_DIR/central.log"
grep -q "ROLE: PROXY" "$WORK_DIR/proxy.log"
grep -q "ROLE: RECEIVER" "$WORK_DIR/receiver-1.log"
grep -q "ROLE: RECEIVER" "$WORK_DIR/receiver-2.log"
grep -q "section_blocks=64" "$WORK_DIR/central.log"
grep -q "section_blocks=64" "$WORK_DIR/proxy.log"
grep -q "repair_mode=peer" "$WORK_DIR/proxy.log"

echo "role CLI smoke test passed"
