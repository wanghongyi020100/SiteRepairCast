#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-"$ROOT_DIR/build-fault-recovery-smoke"}"
WORK_DIR="$(mktemp -d)"

PIDS=()
STARTED_PID=""

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
    local label="$3"
    for _ in {1..100}; do
        if grep -q "$pattern" "$file" 2>/dev/null; then
            return 0
        fi
        sleep 0.1
    done
    echo "timed out waiting for $label" >&2
    cat "$file" >&2 || true
    return 1
}

assert_sha() {
    local input="$1"
    local cache_dir="$2"
    local receiver1="$3"
    local receiver2="$4"

    local input_sha
    input_sha="$(sha256sum "$input" | awk '{print $1}')"
    local cached
    cached="$(find "$cache_dir" -maxdepth 1 -name 'central-transfer-*.bin' -print -quit)"
    local output1
    output1="$(find "$receiver1" -maxdepth 1 -name 'transfer-*.bin' -print -quit)"
    local output2
    output2="$(find "$receiver2" -maxdepth 1 -name 'transfer-*.bin' -print -quit)"

    [[ -n "$cached" && -n "$output1" && -n "$output2" ]]
    [[ "$(sha256sum "$cached" | awk '{print $1}')" == "$input_sha" ]]
    [[ "$(sha256sum "$output1" | awk '{print $1}')" == "$input_sha" ]]
    [[ "$(sha256sum "$output2" | awk '{print $1}')" == "$input_sha" ]]
}

start_proxy() {
    local log="$1"
    local cache="$2"
    shift 2
    if (($# > 0)); then
        env "$@" "$BUILD_DIR/site_proxy_sender" \
            239.255.42.99 5000 127.0.0.1 \
            6000 2 100 0 3 \
            --central-listen 7000 "$cache" \
            >"$log" 2>&1 &
    else
        "$BUILD_DIR/site_proxy_sender" \
            239.255.42.99 5000 127.0.0.1 \
            6000 2 100 0 3 \
            --central-listen 7000 "$cache" \
            >"$log" 2>&1 &
    fi
    STARTED_PID="$!"
    PIDS+=("$STARTED_PID")
    wait_for_log "waiting for central sender on port 7000" "$log" "proxy listen"
}

start_receiver() {
    local receiver_id="$1"
    local out_dir="$2"
    local log="$3"
    shift 3
    if (($# > 0)); then
        env "$@" "$BUILD_DIR/receiver_agent" \
            239.255.42.99 5000 "$out_dir" \
            127.0.0.1 6000 "$receiver_id" 127.0.0.1 \
            >"$log" 2>&1 &
    else
        "$BUILD_DIR/receiver_agent" \
            239.255.42.99 5000 "$out_dir" \
            127.0.0.1 6000 "$receiver_id" 127.0.0.1 \
            >"$log" 2>&1 &
    fi
    STARTED_PID="$!"
    PIDS+=("$STARTED_PID")
}

cmake -S "$ROOT_DIR" -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE=Debug
cmake --build "$BUILD_DIR" -j"$(nproc)"

FAULT_DIR="$WORK_DIR/faults"
mkdir -p "$FAULT_DIR/cache" "$FAULT_DIR/receiver-1" "$FAULT_DIR/receiver-2"
dd if=/dev/urandom of="$FAULT_DIR/input.bin" bs=1024 count=256 status=none

PROXY_FAULT_LOG="$FAULT_DIR/proxy.log"
start_proxy "$PROXY_FAULT_LOG" "$FAULT_DIR/cache" \
    SRCAST_REVERSE_INITIAL_DATA=1 \
    SRCAST_DUPLICATE_DATA_BLOCKS=5 \
    SRCAST_CORRUPT_DATA_BLOCKS=3
PROXY_PID="$STARTED_PID"

start_receiver 1 "$FAULT_DIR/receiver-1" "$FAULT_DIR/receiver-1.log"
RECEIVER1_PID="$STARTED_PID"
start_receiver 2 "$FAULT_DIR/receiver-2" "$FAULT_DIR/receiver-2.log"
RECEIVER2_PID="$STARTED_PID"
wait_for_log "registered receiver_id=2" "$PROXY_FAULT_LOG" "fault receivers"

"$BUILD_DIR/central_sender" 127.0.0.1 7000 0 "$FAULT_DIR/input.bin" \
    >"$FAULT_DIR/central.log" 2>&1

wait "$PROXY_PID"
wait "$RECEIVER1_PID"
wait "$RECEIVER2_PID"

assert_sha \
    "$FAULT_DIR/input.bin" \
    "$FAULT_DIR/cache" \
    "$FAULT_DIR/receiver-1" \
    "$FAULT_DIR/receiver-2"

grep -q "fault injection reversed DATA section=0" "$PROXY_FAULT_LOG"
grep -q "fault injection duplicated DATA block_id=5" "$PROXY_FAULT_LOG"
grep -q "fault injection corrupted DATA block_id=3" "$PROXY_FAULT_LOG"
grep -q "repair round=1" "$PROXY_FAULT_LOG"
grep -Eq "duplicates=[1-9]" "$FAULT_DIR/receiver-1.log"

BAD_DIR="$WORK_DIR/invalid-protocol"
mkdir -p "$BAD_DIR/cache"
BAD_PROXY_LOG="$BAD_DIR/proxy.log"
start_proxy "$BAD_PROXY_LOG" "$BAD_DIR/cache"
BAD_PROXY_PID="$STARTED_PID"

python3 - <<'PY'
import socket
import struct

with socket.create_connection(("127.0.0.1", 6000), timeout=3) as sock:
    payload = b"badframe"
    sock.sendall(struct.pack("!I", len(payload)) + payload)
PY

wait_for_log "rejected receiver registration" \
    "$BAD_PROXY_LOG" \
    "invalid protocol rejection"
kill "$BAD_PROXY_PID" 2>/dev/null || true
wait "$BAD_PROXY_PID" 2>/dev/null || true

KILL_DIR="$WORK_DIR/kill-recovery"
mkdir -p "$KILL_DIR/cache" "$KILL_DIR/receiver-1" "$KILL_DIR/receiver-2"
dd if=/dev/urandom of="$KILL_DIR/input.bin" bs=1024 count=256 status=none

PROXY_KILL_FIRST_LOG="$KILL_DIR/proxy-first.log"
start_proxy "$PROXY_KILL_FIRST_LOG" "$KILL_DIR/cache"
PROXY_PID="$STARTED_PID"
start_receiver 1 "$KILL_DIR/receiver-1" "$KILL_DIR/receiver-1-first.log"
RECEIVER1_PID="$STARTED_PID"
start_receiver 2 "$KILL_DIR/receiver-2" "$KILL_DIR/receiver-2-first.log"
RECEIVER2_PID="$STARTED_PID"
wait_for_log "registered receiver_id=2" "$PROXY_KILL_FIRST_LOG" "kill receivers"

"$BUILD_DIR/central_sender" 127.0.0.1 7000 0 "$KILL_DIR/input.bin" \
    >"$KILL_DIR/central-first.log" 2>&1 &
CENTRAL_PID="$!"
PIDS+=("$CENTRAL_PID")

wait_for_log "central section cached section=1" \
    "$PROXY_KILL_FIRST_LOG" \
    "second checkpoint"

kill -9 "$PROXY_PID" 2>/dev/null || true
kill "$RECEIVER1_PID" "$RECEIVER2_PID" 2>/dev/null || true
wait "$PROXY_PID" 2>/dev/null || true
wait "$RECEIVER1_PID" 2>/dev/null || true
wait "$RECEIVER2_PID" 2>/dev/null || true
wait "$CENTRAL_PID" 2>/dev/null || true

sleep 0.2

PROXY_KILL_RESUME_LOG="$KILL_DIR/proxy-resume.log"
start_proxy "$PROXY_KILL_RESUME_LOG" "$KILL_DIR/cache"
PROXY_PID="$STARTED_PID"
start_receiver 1 "$KILL_DIR/receiver-1" "$KILL_DIR/receiver-1-resume.log"
RECEIVER1_PID="$STARTED_PID"
start_receiver 2 "$KILL_DIR/receiver-2" "$KILL_DIR/receiver-2-resume.log"
RECEIVER2_PID="$STARTED_PID"
wait_for_log "registered receiver_id=2" "$PROXY_KILL_RESUME_LOG" "resume receivers"

"$BUILD_DIR/central_sender" 127.0.0.1 7000 0 "$KILL_DIR/input.bin" \
    >"$KILL_DIR/central-resume.log" 2>&1

wait "$PROXY_PID"
wait "$RECEIVER1_PID"
wait "$RECEIVER2_PID"

assert_sha \
    "$KILL_DIR/input.bin" \
    "$KILL_DIR/cache" \
    "$KILL_DIR/receiver-1" \
    "$KILL_DIR/receiver-2"

grep -q "resume_section=2/4" "$PROXY_KILL_RESUME_LOG"
grep -q "central resume transfer_id=.*next_section=2/4" \
    "$KILL_DIR/central-resume.log"
grep -Eq "recovered_blocks=[1-9]" "$KILL_DIR/receiver-1-resume.log"
grep -Eq "recovered_blocks=[1-9]" "$KILL_DIR/receiver-2-resume.log"
grep -q "central confirmed cached transfer_id=" "$KILL_DIR/central-resume.log"

echo "fault recovery smoke test passed"
