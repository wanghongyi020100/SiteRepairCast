#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-"$ROOT_DIR/build-benchmark-smoke"}"
ARTIFACT_DIR="${2:-"$ROOT_DIR/artifacts/benchmark"}"
WORK_DIR="$(mktemp -d)"

MCAST_ADDR="239.255.42.99"
MCAST_PORT=5000
IFACE_ADDR="127.0.0.1"
CONTROL_PORT=6000
CENTRAL_PORT=7000
MAX_ROUNDS=3
BLOCKS_PER_SECTION=100
PAYLOAD_BYTES=1200

PIDS=()
STARTED_PID=""
ROWS=()

cleanup() {
    for pid in "${PIDS[@]}"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

on_error() {
    local status=$?
    mkdir -p "$ARTIFACT_DIR/failure"
    cp -r "$WORK_DIR"/. "$ARTIFACT_DIR/failure/" 2>/dev/null || true
    echo "benchmark benchmark failed; preserved logs in $ARTIFACT_DIR/failure" >&2
    find "$ARTIFACT_DIR/failure" -name '*.log' -maxdepth 3 -print -exec sh -c '
        echo "== $1 =="
        tail -80 "$1"
    ' sh {} \; >&2 || true
    exit "$status"
}
trap on_error ERR

wait_for_log() {
    local pattern="$1"
    local file="$2"
    local label="$3"
    for _ in {1..150}; do
        if grep -q "$pattern" "$file" 2>/dev/null; then
            return 0
        fi
        sleep 0.1
    done
    echo "timed out waiting for $label" >&2
    cat "$file" >&2 || true
    return 1
}

sum_matches() {
    local pattern="$1"
    local file="$2"
    awk -v pattern="$pattern" '
        {
            rest = $0
            while (match(rest, pattern)) {
                value = substr(rest, RSTART, RLENGTH)
                sub(/.*=/, "", value)
                total += value
                rest = substr(rest, RSTART + RLENGTH)
            }
        }
        END { print total + 0 }
    ' "$file"
}

sum_line_matches() {
    local line_pattern="$1"
    local value_pattern="$2"
    local file="$3"
    awk -v line_pattern="$line_pattern" -v value_pattern="$value_pattern" '
        $0 ~ line_pattern {
            rest = $0
            while (match(rest, value_pattern)) {
                value = substr(rest, RSTART, RLENGTH)
                sub(/.*=/, "", value)
                total += value
                rest = substr(rest, RSTART + RLENGTH)
            }
        }
        END { print total + 0 }
    ' "$file"
}

assert_sha() {
    local input="$1"
    local cache_dir="$2"
    shift 2

    local input_sha
    input_sha="$(sha256sum "$input" | awk '{print $1}')"
    local cached
    cached="$(find "$cache_dir" -maxdepth 1 -name 'central-transfer-*.bin' -print -quit)"
    [[ -n "$cached" ]]
    [[ "$(sha256sum "$cached" | awk '{print $1}')" == "$input_sha" ]]

    local receiver_dir
    for receiver_dir in "$@"; do
        local output
        output="$(find "$receiver_dir" -maxdepth 1 -name 'transfer-*.bin' -print -quit)"
        [[ -n "$output" ]]
        [[ "$(sha256sum "$output" | awk '{print $1}')" == "$input_sha" ]]
    done
}

start_proxy() {
    local receiver_count="$1"
    local cache_dir="$2"
    local log_file="$3"
    shift 3

    if (($# > 0)); then
        env "$@" "$BUILD_DIR/site_proxy_sender" \
            "$MCAST_ADDR" "$MCAST_PORT" "$IFACE_ADDR" \
            "$CONTROL_PORT" "$receiver_count" "$BLOCKS_PER_SECTION" 0 "$MAX_ROUNDS" \
            --central-listen "$CENTRAL_PORT" "$cache_dir" \
            >"$log_file" 2>&1 &
    else
        "$BUILD_DIR/site_proxy_sender" \
            "$MCAST_ADDR" "$MCAST_PORT" "$IFACE_ADDR" \
            "$CONTROL_PORT" "$receiver_count" "$BLOCKS_PER_SECTION" 0 "$MAX_ROUNDS" \
            --central-listen "$CENTRAL_PORT" "$cache_dir" \
            >"$log_file" 2>&1 &
    fi
    STARTED_PID="$!"
    PIDS+=("$STARTED_PID")
    wait_for_log "waiting for central sender on port $CENTRAL_PORT" \
        "$log_file" \
        "proxy listen"
}

start_receiver() {
    local receiver_id="$1"
    local output_dir="$2"
    local log_file="$3"
    shift 3

    if (($# > 0)); then
        env "$@" "$BUILD_DIR/receiver_agent" \
            "$MCAST_ADDR" "$MCAST_PORT" "$output_dir" \
            "$IFACE_ADDR" "$CONTROL_PORT" "$receiver_id" "$IFACE_ADDR" \
            >"$log_file" 2>&1 &
    else
        "$BUILD_DIR/receiver_agent" \
            "$MCAST_ADDR" "$MCAST_PORT" "$output_dir" \
            "$IFACE_ADDR" "$CONTROL_PORT" "$receiver_id" "$IFACE_ADDR" \
            >"$log_file" 2>&1 &
    fi
    STARTED_PID="$!"
    PIDS+=("$STARTED_PID")
}

run_scenario() {
    local name="$1"
    local receiver_count="$2"
    local input_kib="$3"
    local proxy_env="$4"
    shift 4

    local scenario_dir="$WORK_DIR/$name"
    mkdir -p "$scenario_dir/cache"

    local input="$scenario_dir/input.bin"
    dd if=/dev/urandom of="$input" bs=1024 count="$input_kib" status=none

    local proxy_log="$scenario_dir/proxy.log"
    if [[ -n "$proxy_env" ]]; then
        # shellcheck disable=SC2086
        start_proxy "$receiver_count" "$scenario_dir/cache" "$proxy_log" $proxy_env
    else
        start_proxy "$receiver_count" "$scenario_dir/cache" "$proxy_log"
    fi
    local proxy_pid="$STARTED_PID"

    local receiver_dirs=()
    local receiver_pids=()
    for receiver_id in $(seq 1 "$receiver_count"); do
        local receiver_dir="$scenario_dir/receiver-$receiver_id"
        local receiver_log="$scenario_dir/receiver-$receiver_id.log"
        mkdir -p "$receiver_dir"
        local receiver_env="${1:-}"
        shift || true
        if [[ -n "$receiver_env" ]]; then
            # shellcheck disable=SC2086
            start_receiver "$receiver_id" "$receiver_dir" "$receiver_log" $receiver_env
        else
            start_receiver "$receiver_id" "$receiver_dir" "$receiver_log"
        fi
        receiver_dirs+=("$receiver_dir")
        receiver_pids+=("$STARTED_PID")
    done

    wait_for_log "registered receiver_id=$receiver_count" \
        "$proxy_log" \
        "$name receivers"

    local started_ms
    started_ms="$(date +%s%3N)"
    "$BUILD_DIR/central_sender" "$IFACE_ADDR" "$CENTRAL_PORT" 0 "$input" \
        >"$scenario_dir/central.log" 2>&1
    wait "$proxy_pid"
    local receiver_pid
    for receiver_pid in "${receiver_pids[@]}"; do
        wait "$receiver_pid"
    done
    local ended_ms
    ended_ms="$(date +%s%3N)"

    assert_sha "$input" "$scenario_dir/cache" "${receiver_dirs[@]}"

    mkdir -p "$ARTIFACT_DIR/logs/$name"
    cp "$scenario_dir"/*.log "$ARTIFACT_DIR/logs/$name/"

    local file_bytes
    file_bytes="$(stat -c '%s' "$input")"
    local repair_multicast
    repair_multicast="$(sum_matches 'multicast_packets=[0-9]+' "$proxy_log")"
    local repair_unicast
    repair_unicast="$(sum_matches 'unicast_packets=[0-9]+' "$proxy_log")"
    local backfill_blocks
    backfill_blocks="$(
        sum_line_matches \
            'TCP backfill receiver_id=' \
            'blocks=[0-9]+' \
            "$proxy_log"
    )"
    local duplicates=0
    local rejected=0
    local receiver_log
    for receiver_log in "$scenario_dir"/receiver-*.log; do
        duplicates=$((duplicates + $(sum_matches 'duplicates=[0-9]+' "$receiver_log")))
        rejected=$((rejected + $(sum_matches 'rejected=[0-9]+' "$receiver_log")))
    done

    local local_repair_packets=$((repair_multicast + repair_unicast + backfill_blocks))
    local direct_wan=$((file_bytes * receiver_count))
    local forward_only_wan=$((file_bytes + local_repair_packets * PAYLOAD_BYTES))
    local srcast_wan="$file_bytes"
    local wan_saved=$((forward_only_wan - srcast_wan))
    local lan_initial="$file_bytes"
    local duration_ms=$((ended_ms - started_ms))

    ROWS+=("$name,$receiver_count,$file_bytes,$direct_wan,$forward_only_wan,$srcast_wan,$wan_saved,$lan_initial,$repair_multicast,$repair_unicast,$backfill_blocks,$duplicates,$rejected,$duration_ms,yes")
}

cmake -S "$ROOT_DIR" -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE=Debug
cmake --build "$BUILD_DIR" -j"$(nproc)"

rm -rf "$ARTIFACT_DIR"
mkdir -p "$ARTIFACT_DIR"

run_scenario "normal-2-receivers" 2 512 ""
run_scenario "mixed-lan-loss-2-receivers" 2 512 \
    "SRCAST_CORRUPT_DATA_BLOCKS=3 SRCAST_DUPLICATE_DATA_BLOCKS=5" \
    "" \
    ""
run_scenario "slow-backfill-4-receivers" 4 512 "SRCAST_SLOW_MISSING_THRESHOLD=8" \
    "" \
    "SRCAST_DROP_INITIAL_BLOCKS=0,1,2,3,4,5,6,7,8,9,10,11" \
    "SRCAST_DROP_INITIAL_BLOCKS=12,13,14,15,16,17,18,19,20,21,22,23" \
    ""

CSV="$ARTIFACT_DIR/benchmark_metrics.csv"
{
    echo "scenario,receivers,file_bytes,direct_tcp_wan_payload_bytes,forward_only_wan_payload_bytes,srcast_wan_payload_bytes,srcast_wan_repair_bytes_saved_vs_forward_only,lan_initial_payload_bytes,lan_repair_multicast_packets,lan_repair_unicast_packets,tcp_backfill_blocks,receiver_duplicates,receiver_rejected,duration_ms,sha_ok"
    printf '%s\n' "${ROWS[@]}"
} >"$CSV"

REPORT="$ARTIFACT_DIR/benchmark_report.md"
{
    echo "# SiteRepairCast Week 8 Performance Snapshot"
    echo
    echo "All byte counts are payload-level estimates from a local loopback run. They exclude TCP/UDP/IP headers and control-frame overhead."
    echo
    echo "| Scenario | Receivers | File bytes | Direct TCP WAN | Forward-only WAN | SiteRepairCast WAN | WAN repair saved | LAN repair multicast | LAN repair unicast | TCP backfill blocks | Duration ms | SHA |"
    echo "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |"
    awk -F, 'NR > 1 {
        printf "| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n", $1, $2, $3, $4, $5, $6, $7, $9, $10, $11, $14, $15
    }' "$CSV"
    echo
    echo "## Comparison Modes"
    echo
    echo "- Mode A direct TCP sends the full file once per receiver: file_size * receiver_count."
    echo "- Mode B proxy-forward-only assumes the proxy cannot repair locally, so LAN repair/backfill payload must cross the WAN again."
    echo "- Mode C SiteRepairCast sends the file to the proxy once; receiver loss is repaired from the proxy cache inside the site."
    echo "- This loopback benchmark uses multicast repair and TCP backfill. Per-receiver UDP unicast repair needs distinct receiver IPs, so it is covered by implementation logic but not by the single-host benchmark."
    echo
    echo "## Generated Artifacts"
    echo
    echo "- Metrics CSV: \`$CSV\`"
    echo "- Scenario logs: \`$ARTIFACT_DIR/logs/\`"
} >"$REPORT"

cat "$REPORT"
echo "benchmark benchmark passed"
