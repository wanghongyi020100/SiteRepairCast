#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-"$ROOT_DIR/test-runs/builds/smoke/file-name-smoke"}"
TEST_TMP_DIR="${TMPDIR:-"$ROOT_DIR/test-runs/tmp/smoke"}"
mkdir -p "$TEST_TMP_DIR"
WORK_DIR="$(mktemp -d "$TEST_TMP_DIR/file-name.XXXXXX")"

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

cmake -S "$ROOT_DIR" -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE=Debug
cmake --build "$BUILD_DIR" -j"$(nproc)"

NORMAL_DIR="$WORK_DIR/normal"
mkdir -p "$NORMAL_DIR/cache" "$NORMAL_DIR/receiver" "$NORMAL_DIR/source"
printf 't128 payload\n%.0s' {1..128} >"$NORMAL_DIR/source/t128.txt"
printf 'hello\n' >"$NORMAL_DIR/source/hello.txt"
printf 'archive payload\n%.0s' {1..32} >"$NORMAL_DIR/source/archive.tar"
printf 'hello space\n' >"$NORMAL_DIR/source/hello world.txt"
printf 'abc' >"$NORMAL_DIR/source/a.txt"
printf 'abc' >"$NORMAL_DIR/source/b.txt"

"$BUILD_DIR/site_proxy_sender" \
    --multicast-ip 239.255.42.99 --udp-port 5500 \
    --interface-ip 127.0.0.1 --control-port 6500 \
    --receiver-count 1 --pace-us 0 --gap-ms 0 --max-repair-rounds 3 \
    --central-listen 7500 --cache-dir "$NORMAL_DIR/cache" \
    >"$NORMAL_DIR/proxy.log" 2>&1 &
PROXY_PID="$!"
PIDS+=("$PROXY_PID")
wait_for_log "waiting for central sender on port 7500" "$NORMAL_DIR/proxy.log" "proxy listen"

"$BUILD_DIR/receiver_agent" \
    --multicast-ip 239.255.42.99 --udp-port 5500 \
    --output-dir "$NORMAL_DIR/receiver" --proxy-ip 127.0.0.1 \
    --control-port 6500 --interface-ip 127.0.0.1 \
    >"$NORMAL_DIR/receiver.log" 2>&1 &
RECEIVER_PID="$!"
PIDS+=("$RECEIVER_PID")
wait_for_log "registered receiver=" "$NORMAL_DIR/proxy.log" "receiver registration"

"$BUILD_DIR/central_sender" --proxy 127.0.0.1 7500 --pace-us 0 \
    --file "$NORMAL_DIR/source/t128.txt" \
    --file "$NORMAL_DIR/source/hello.txt" \
    --file "$NORMAL_DIR/source/archive.tar" \
    --file "$NORMAL_DIR/source/hello world.txt" \
    --file "$NORMAL_DIR/source/a.txt" \
    --file "$NORMAL_DIR/source/b.txt" \
    >"$NORMAL_DIR/central.log" 2>&1

wait "$PROXY_PID"
wait "$RECEIVER_PID"

for name in "t128.txt" "hello.txt" "archive.tar" "hello world.txt" "a.txt" "b.txt"; do
    [[ -f "$NORMAL_DIR/receiver/$name" ]]
    [[ "$(sha256sum "$NORMAL_DIR/source/$name" | awk '{print $1}')" == \
       "$(sha256sum "$NORMAL_DIR/receiver/$name" | awk '{print $1}')" ]]
done

A_TRANSFER_ID="$(awk -F'transfer_id=' '/central sending file=a.txt / {split($2, parts, " "); print parts[1]}' "$NORMAL_DIR/central.log")"
B_TRANSFER_ID="$(awk -F'transfer_id=' '/central sending file=b.txt / {split($2, parts, " "); print parts[1]}' "$NORMAL_DIR/central.log")"
[[ -n "$A_TRANSFER_ID" && -n "$B_TRANSFER_ID" && "$A_TRANSFER_ID" != "$B_TRANSFER_ID" ]]

if find "$NORMAL_DIR/receiver" -maxdepth 1 -name 'transfer-*.bin' -print -quit | grep -q .; then
    echo "old transfer-id final output name still exists" >&2
    find "$NORMAL_DIR/receiver" -maxdepth 1 -type f -print >&2
    exit 1
fi

if find "$NORMAL_DIR/receiver" -maxdepth 1 -name '*.part-*' -print -quit | grep -q .; then
    echo "temporary receiver part file was left behind" >&2
    find "$NORMAL_DIR/receiver" -maxdepth 1 -type f -print >&2
    exit 1
fi

grep -q "COMPLETED output=.*t128.txt" "$NORMAL_DIR/receiver.log"
grep -q "COMPLETED output=.*hello.txt" "$NORMAL_DIR/receiver.log"

run_invalid_name_case() {
    local index="$1"
    local name="$2"
    local label="$3"
    local case_dir="$WORK_DIR/reject-$index"
    local ready_file="$case_dir/proxy.ready"
    local control_port=$((6600 + index))
    local udp_port=$((5600 + index))
    mkdir -p "$case_dir/receiver"

    python3 - "$control_port" "$name" "$ready_file" <<'PY' \
        >"$case_dir/fake-proxy.log" 2>&1 &
import os
import socket
import struct
import sys
import time

port = int(sys.argv[1])
name = sys.argv[2].encode()
ready_file = sys.argv[3]

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind(("127.0.0.1", port))
server.listen(1)
open(ready_file, "w").close()

conn, _ = server.accept()
with conn:
    def recvn(size):
        data = b""
        while len(data) < size:
            chunk = conn.recv(size - len(data))
            if not chunk:
                return data
            data += chunk
        return data

    header = recvn(4)
    if len(header) == 4:
        frame_size = struct.unpack("!I", header)[0]
        recvn(frame_size)

    frame = (
        struct.pack(
            "!IHHQIQIIIH",
            0x53524343,
            10,
            10,
            900000 + port,
            0,
            1,
            1200,
            1,
            64,
            len(name),
        )
        + name
        + bytes(32)
    )
    conn.sendall(struct.pack("!I", len(frame)) + frame)
    time.sleep(0.2)
server.close()
PY
    local proxy_pid="$!"
    PIDS+=("$proxy_pid")

    for _ in {1..50}; do
        [[ -f "$ready_file" ]] && break
        sleep 0.05
    done
    [[ -f "$ready_file" ]]

    "$BUILD_DIR/receiver_agent" \
        --multicast-ip 239.255.42.99 --udp-port "$udp_port" \
        --output-dir "$case_dir/receiver" --proxy-ip 127.0.0.1 \
        --control-port "$control_port" --interface-ip 127.0.0.1 \
        >"$case_dir/receiver.log" 2>&1 &
    local receiver_pid="$!"
    PIDS+=("$receiver_pid")

    wait "$proxy_pid"
    wait_for_log "invalid file name\\|invalid received file name\\|file name contains path components" \
        "$case_dir/receiver.log" "invalid file name rejection for $label"

    if find "$case_dir/receiver" -maxdepth 1 -type f -print -quit | grep -q .; then
        echo "receiver created output for invalid file name: $label" >&2
        find "$case_dir/receiver" -maxdepth 1 -type f -print >&2
        exit 1
    fi

    kill "$receiver_pid" 2>/dev/null || true
    wait "$receiver_pid" 2>/dev/null || true
}

invalid_names=("../x" "../../x" "/tmp/x" "dir/x" "." ".." "")
for index in "${!invalid_names[@]}"; do
    run_invalid_name_case "$index" "${invalid_names[$index]}" "case-$index"
done

echo "file name smoke test passed"
