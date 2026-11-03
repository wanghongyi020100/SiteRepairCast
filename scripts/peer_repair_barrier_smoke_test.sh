#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-"$ROOT_DIR/test-runs/builds/smoke/peer-repair-barrier-smoke"}"
TEST_TMP_DIR="${TMPDIR:-"$ROOT_DIR/test-runs/tmp/smoke"}"
mkdir -p "$TEST_TMP_DIR"
WORK_DIR="$(mktemp -d "$TEST_TMP_DIR/peer-repair-barrier.XXXXXX")"

PIDS=()

cleanup() {
    for pid in "${PIDS[@]}"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill -CONT "$pid" 2>/dev/null || true
            kill "$pid" 2>/dev/null || true
        fi
    done
    if [[ "${SRCAST_KEEP_TEST_ARTIFACTS:-0}" == "1" ]]; then
        echo "Peer repair barrier smoke artifacts: $WORK_DIR" >&2
    else
        rm -rf "$WORK_DIR"
    fi
}
trap cleanup EXIT

wait_for_log() {
    local pattern="$1"
    local file="$2"
    for _ in {1..100}; do
        if grep -q "$pattern" "$file" 2>/dev/null; then
            return 0
        fi
        sleep 0.05
    done
    echo "timed out waiting for '$pattern'" >&2
    cat "$file" >&2 || true
    return 1
}

cmake -S "$ROOT_DIR" -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE=Debug
cmake --build "$BUILD_DIR" -j"$(nproc)"

dd if=/dev/urandom of="$WORK_DIR/input.bin" bs=1024 count=256 status=none
mkdir -p "$WORK_DIR/source" "$WORK_DIR/target"

"$BUILD_DIR/site_proxy_sender" \
    --multicast-ip 239.255.42.99 --udp-port 5200 \
    --interface-ip 127.0.0.1 --control-port 6200 \
    --receiver-count 2 --pace-us 0 --gap-ms 0 --max-repair-rounds 3 \
    --section-blocks 64 --file "$WORK_DIR/input.bin" \
    >"$WORK_DIR/proxy.log" 2>&1 &
PROXY_PID="$!"
PIDS+=("$PROXY_PID")
wait_for_log "waiting for 2 receiver control connections" "$WORK_DIR/proxy.log"

"$BUILD_DIR/receiver_agent" \
    --multicast-ip 239.255.42.99 --udp-port 5200 \
    --output-dir "$WORK_DIR/source" --proxy-ip 127.0.0.1 \
    --control-port 6200 --interface-ip 127.0.0.2 \
    >"$WORK_DIR/source.log" 2>&1 &
SOURCE_PID="$!"
PIDS+=("$SOURCE_PID")
wait_for_log "registered receiver=127.0.0.2:" "$WORK_DIR/proxy.log"

SRCAST_DROP_INITIAL_BLOCKS=0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15 \
    "$BUILD_DIR/receiver_agent" \
    --multicast-ip 239.255.42.99 --udp-port 5200 \
    --output-dir "$WORK_DIR/target" --proxy-ip 127.0.0.1 \
    --control-port 6200 --interface-ip 127.0.0.3 \
    >"$WORK_DIR/target.log" 2>&1 &
TARGET_PID="$!"
PIDS+=("$TARGET_PID")

wait "$PROXY_PID"
wait "$SOURCE_PID"
wait "$TARGET_PID"
PIDS=()

input_sha="$(sha256sum "$WORK_DIR/input.bin" | awk '{print $1}')"
[[ "$(sha256sum "$WORK_DIR/source/input.bin" | awk '{print $1}')" == "$input_sha" ]]
[[ "$(sha256sum "$WORK_DIR/target/input.bin" | awk '{print $1}')" == "$input_sha" ]]

grep -q "PEER_TASK task=" "$WORK_DIR/proxy.log"
grep -Eq "PEER_TASK_DONE task=.* sent_blocks=16 skipped_blocks=0" "$WORK_DIR/source.log"
grep -Eq "PEER_DONE task=.* sent_blocks=16 skipped_blocks=0" "$WORK_DIR/proxy.log"
grep -q "REPAIR_METRIC round=1 mode=peer .*peer_blocks=16 .*peer_scheduled_blocks=16 .*peer_done_blocks=16 .*peer_failed_blocks=0 .*peer_timeout_blocks=0 .*peer_tasks=1 .*peer_done_tasks=1 .*peer_timeout_tasks=0" \
    "$WORK_DIR/proxy.log"
grep -q "SECTION_END_SEND section=0 round=1" "$WORK_DIR/proxy.log"
grep -q "DRAIN_METRIC section=0 round=1" "$WORK_DIR/target.log"
grep -q "section=0 round=1 receiver=.* status=COMPLETE" "$WORK_DIR/proxy.log"

task_line="$(grep -n -m1 'PEER_TASK task=' "$WORK_DIR/proxy.log" | cut -d: -f1)"
done_line="$(grep -n -m1 'PEER_DONE task=' "$WORK_DIR/proxy.log" | cut -d: -f1)"
end_line="$(grep -n -m1 'SECTION_END_SEND section=0 round=1' "$WORK_DIR/proxy.log" | cut -d: -f1)"
if (( task_line >= done_line || done_line >= end_line )); then
    echo "peer repair barrier log order is invalid" >&2
    cat "$WORK_DIR/proxy.log" >&2
    exit 1
fi

if grep -Eq "PEER_TIMEOUT|status=NO_REPORT|unsupported protocol version|invalid control message type|invalid PEER_REPAIR_DONE|control disconnected" \
    "$WORK_DIR/proxy.log" "$WORK_DIR/source.log" "$WORK_DIR/target.log"; then
    echo "unexpected peer repair barrier error" >&2
    grep -En "PEER_TIMEOUT|status=NO_REPORT|unsupported protocol version|invalid control message type|invalid PEER_REPAIR_DONE|control disconnected" \
        "$WORK_DIR/proxy.log" "$WORK_DIR/source.log" "$WORK_DIR/target.log" >&2 || true
    exit 1
fi

mkdir -p "$WORK_DIR/timeout/source" "$WORK_DIR/timeout/target"
stdbuf -oL -eL "$BUILD_DIR/site_proxy_sender" \
    --multicast-ip 239.255.42.99 --udp-port 5201 \
    --interface-ip 127.0.0.1 --control-port 6201 \
    --receiver-count 2 --pace-us 0 --gap-ms 0 --max-repair-rounds 3 \
    --section-blocks 64 --file "$WORK_DIR/input.bin" \
    >"$WORK_DIR/timeout/proxy.log" 2>&1 &
PROXY_PID="$!"
PIDS+=("$PROXY_PID")
wait_for_log "waiting for 2 receiver control connections" "$WORK_DIR/timeout/proxy.log"

SRCAST_DRAIN_MODE=rtt "$BUILD_DIR/receiver_agent" \
    --multicast-ip 239.255.42.99 --udp-port 5201 \
    --output-dir "$WORK_DIR/timeout/source" --proxy-ip 127.0.0.1 \
    --control-port 6201 --interface-ip 127.0.0.2 \
    >"$WORK_DIR/timeout/source.log" 2>&1 &
SOURCE_PID="$!"
PIDS+=("$SOURCE_PID")
wait_for_log "registered receiver=127.0.0.2:" "$WORK_DIR/timeout/proxy.log"

SRCAST_DRAIN_MODE=fixed \
SRCAST_DROP_INITIAL_BLOCKS=0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15 \
    "$BUILD_DIR/receiver_agent" \
    --multicast-ip 239.255.42.99 --udp-port 5201 \
    --output-dir "$WORK_DIR/timeout/target" --proxy-ip 127.0.0.1 \
    --control-port 6201 --interface-ip 127.0.0.3 \
    >"$WORK_DIR/timeout/target.log" 2>&1 &
TARGET_PID="$!"
PIDS+=("$TARGET_PID")

wait_for_log "section=0 round=0 receiver=127.0.0.2:.* status=COMPLETE" \
    "$WORK_DIR/timeout/proxy.log"
kill -STOP "$SOURCE_PID"
wait_for_log "PEER_TIMEOUT task=" "$WORK_DIR/timeout/proxy.log"
kill -CONT "$SOURCE_PID"

wait "$PROXY_PID"
wait "$SOURCE_PID"
wait "$TARGET_PID"
PIDS=()

[[ "$(sha256sum "$WORK_DIR/timeout/source/input.bin" | awk '{print $1}')" == "$input_sha" ]]
[[ "$(sha256sum "$WORK_DIR/timeout/target/input.bin" | awk '{print $1}')" == "$input_sha" ]]
grep -q "PEER_TIMEOUT task=.* blocks=16" "$WORK_DIR/timeout/proxy.log"
grep -q "PEER_FALLBACK task=.* blocks=16 reason=timeout" "$WORK_DIR/timeout/proxy.log"
grep -q "REPAIR_METRIC round=1 mode=peer .*proxy_repair_payload_bytes=19200 .*proxy_repair_pread_blocks=16 .*peer_blocks=0 .*peer_scheduled_blocks=16 .*peer_done_blocks=0 .*peer_failed_blocks=0 .*peer_timeout_blocks=16 .*peer_tasks=1 .*peer_done_tasks=0 .*peer_timeout_tasks=1 .*proxy_unicast_packets=16" \
    "$WORK_DIR/timeout/proxy.log"
grep -q "SECTION_END_SEND section=0 round=1" "$WORK_DIR/timeout/proxy.log"
grep -q "section=0 round=1 receiver=127.0.0.3:.* status=COMPLETE" \
    "$WORK_DIR/timeout/proxy.log"

if grep -Eq "status=NO_REPORT|unsupported protocol version|invalid control message type|invalid PEER_REPAIR_DONE|control disconnected" \
    "$WORK_DIR/timeout/proxy.log" "$WORK_DIR/timeout/source.log" "$WORK_DIR/timeout/target.log"; then
    echo "unexpected peer timeout fallback error" >&2
    grep -En "status=NO_REPORT|unsupported protocol version|invalid control message type|invalid PEER_REPAIR_DONE|control disconnected" \
        "$WORK_DIR/timeout/proxy.log" "$WORK_DIR/timeout/source.log" "$WORK_DIR/timeout/target.log" >&2 || true
    exit 1
fi

echo "Peer repair barrier smoke test passed"
