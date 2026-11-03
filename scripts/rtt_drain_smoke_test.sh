#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-"$ROOT_DIR/test-runs/builds/smoke/rtt-drain-smoke"}"
TEST_TMP_DIR="${TMPDIR:-"$ROOT_DIR/test-runs/tmp/smoke"}"
mkdir -p "$TEST_TMP_DIR"
WORK_DIR="$(mktemp -d "$TEST_TMP_DIR/rtt-drain.XXXXXX")"

PIDS=()

cleanup() {
    for pid in "${PIDS[@]}"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    if [[ "${SRCAST_KEEP_TEST_ARTIFACTS:-0}" == "1" ]]; then
        echo "RTT drain smoke artifacts: $WORK_DIR" >&2
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

dd if=/dev/urandom of="$WORK_DIR/input.bin" bs=1024 count=384 status=none
mkdir -p "$WORK_DIR/real/output"

"$BUILD_DIR/site_proxy_sender" \
    --multicast-ip 239.255.42.99 --udp-port 5101 \
    --interface-ip 127.0.0.1 --control-port 6101 \
    --receiver-count 1 --pace-us 0 --gap-ms 0 --max-repair-rounds 3 \
    --section-blocks 64 --file "$WORK_DIR/input.bin" \
    >"$WORK_DIR/real/proxy.log" 2>&1 &
PROXY_PID="$!"
PIDS+=("$PROXY_PID")
wait_for_log "waiting for 1 receiver control connections" "$WORK_DIR/real/proxy.log"

SRCAST_DROP_INITIAL_BLOCKS=3 "$BUILD_DIR/receiver_agent" \
    --multicast-ip 239.255.42.99 --udp-port 5101 \
    --output-dir "$WORK_DIR/real/output" --proxy-ip 127.0.0.1 \
    --control-port 6101 --interface-ip 127.0.0.2 \
    >"$WORK_DIR/real/receiver.log" 2>&1 &
RECEIVER_PID="$!"
PIDS+=("$RECEIVER_PID")

wait "$PROXY_PID"
wait "$RECEIVER_PID"

INPUT_SHA="$(sha256sum "$WORK_DIR/input.bin" | awk '{print $1}')"
[[ "$(sha256sum "$WORK_DIR/real/output/input.bin" | awk '{print $1}')" == "$INPUT_SHA" ]]
grep -Eq "RTT_METRIC receiver=.* app_sample_us=[0-9]+ app_srtt_us=[0-9]+ next_wait_ms=[0-9]+" \
    "$WORK_DIR/real/proxy.log"
grep -q "DRAIN_WAIT section=0 round=0 wait_ms=200" "$WORK_DIR/real/receiver.log"

awk '
    /SECTION_END_SEND/ {
        wait=-1
        for (field=1; field<=NF; field++) {
            split($field, pair, "=")
            if (pair[1]=="wait_ms") wait=pair[2]+0
        }
        if (send_count==0 && wait!=200) bad=1
        if (have_expected && wait!=expected) bad=1
        have_expected=0
        send_count++
    }
    /RTT_METRIC/ {
        next_wait=-1
        for (field=1; field<=NF; field++) {
            split($field, pair, "=")
            if (pair[1]=="next_wait_ms") next_wait=pair[2]+0
        }
        if (next_wait<10 || next_wait>200) bad=1
        if (next_wait<200) adapted=1
        expected=next_wait
        have_expected=1
        metric_count++
    }
    END {exit send_count>=2 && metric_count>=2 && adapted && !bad ? 0 : 1}
' "$WORK_DIR/real/proxy.log"

awk '
    /DRAIN_WAIT/ {
        wait=-1
        for (field=1; field<=NF; field++) {
            split($field, pair, "=")
            if (pair[1]=="wait_ms") wait=pair[2]+0
        }
        if (count==0 && wait!=200) bad=1
        if (wait<10 || wait>200) bad=1
        if (wait<200) adapted=1
        count++
    }
    END {exit count>=2 && adapted && !bad ? 0 : 1}
' "$WORK_DIR/real/receiver.log"

dd if=/dev/urandom of="$WORK_DIR/control-a.bin" bs=1024 count=192 status=none
dd if=/dev/urandom of="$WORK_DIR/control-b.bin" bs=1024 count=192 status=none
"$BUILD_DIR/site_proxy_sender" \
    --multicast-ip 239.255.42.99 --udp-port 5102 \
    --interface-ip 127.0.0.1 --control-port 6102 \
    --receiver-count 1 --pace-us 0 --gap-ms 0 --max-repair-rounds 0 \
    --section-blocks 64 --file "$WORK_DIR/control-a.bin" --file "$WORK_DIR/control-b.bin" \
    >"$WORK_DIR/control-proxy.log" 2>&1 &
CONTROL_PROXY_PID="$!"
PIDS+=("$CONTROL_PROXY_PID")
wait_for_log "waiting for 1 receiver control connections" "$WORK_DIR/control-proxy.log"
python3 "$ROOT_DIR/scripts/rtt_control_protocol_test.py" --port 6102
wait "$CONTROL_PROXY_PID"

grep -q "ignore stale or mismatched SECTION_END_ACK" "$WORK_DIR/control-proxy.log"
grep -Eq "RTT_METRIC receiver=.* next_wait_ms=200" "$WORK_DIR/control-proxy.log"
grep -Eq "RTT_METRIC receiver=.* next_wait_ms=([1-9]|[1-9][0-9]|1[0-9][0-9])$" \
    "$WORK_DIR/control-proxy.log"

if grep -Eq "status=NO_REPORT|unsupported control protocol version|invalid SECTION_END|control disconnected" \
    "$WORK_DIR/real/proxy.log" "$WORK_DIR/real/receiver.log" "$WORK_DIR/control-proxy.log"; then
    echo "RTT drain smoke detected a control-path failure" >&2
    exit 1
fi

echo "application RTT drain smoke test passed"
