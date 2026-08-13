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
    echo "benchmark failed; preserved logs in $ARTIFACT_DIR/failure" >&2
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

CSV="$ARTIFACT_DIR/metrics.csv"
{
    echo "场景,接收端数量,文件字节数,直接TCP跨园区流量,普通转发跨园区流量,SiteRepairCast跨园区流量,相比普通转发节省流量,局域网初始流量,组播修复包数,单播修复包数,TCP补传块数,重复包数,拒绝包数,耗时毫秒,校验通过"
    printf '%s\n' "${ROWS[@]}"
} >"$CSV"

REPORT="$ARTIFACT_DIR/report.md"
{
    echo "# SiteRepairCast 第 8 周性能概览"
    echo
    echo "以下字节数均为本机回环测试中的数据载荷估算值，不包含 TCP、UDP、IP 头部以及控制帧开销。"
    echo
    echo "| 场景 | 接收端数量 | 文件字节数 | 直接 TCP 跨园区流量 | 普通转发跨园区流量 | SiteRepairCast 跨园区流量 | 节省的跨园区流量 | 局域网组播修复 | 局域网单播修复 | TCP 补传块数 | 耗时（毫秒） | 校验 |"
    echo "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |"
    awk -F, 'NR > 1 {
        printf "| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n", $1, $2, $3, $4, $5, $6, $7, $9, $10, $11, $14, $15
    }' "$CSV"
    echo
    echo "## 对比方式"
    echo
    echo "- 方式 A：直接 TCP 向每个接收端发送一份完整文件，流量为 file_size * receiver_count。"
    echo "- 方式 B：普通代理转发，假设代理不能在本地修复，因此修复数据和补传数据还需要再次经过跨园区链路。"
    echo "- 方式 C：SiteRepairCast 只向代理发送一份文件，接收端缺失的数据由站点内的代理缓存完成修复。"
    echo "- 本次回环测试覆盖组播修复和 TCP 补传。按接收端分别进行 UDP 单播修复需要不同的接收端 IP，因此这里只验证实现逻辑，不在单机测试中单独测量。"
    echo
    echo "## 生成结果"
    echo
    echo "- 指标 CSV：\`$CSV\`"
    echo "- 场景日志：\`$ARTIFACT_DIR/logs/\`"
} >"$REPORT"

cat "$REPORT"
echo "benchmark passed"
