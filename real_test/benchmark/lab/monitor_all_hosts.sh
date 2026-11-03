#!/bin/bash

if [ $# -ne 3 ]; then
    echo "usage: $0 <proxy_ip> <receiver1_ip> <receiver2_ip>"
    exit 1
fi

PROXY="$1"
R1="$2"
R2="$3"

OUT="test-runs/lab/bottleneck-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUT"

monitor_cmd='
while true
do
    echo "===== $(hostname) $(date "+%F %T") ====="

    echo "[LOAD]"
    cat /proc/loadavg

    echo "[CPU]"
    grep "^cpu " /proc/stat

    echo "[SOFTIRQ]"
    grep "^softirq" /proc/stat

    echo "[CONTEXT]"
    grep -E "^(ctxt|procs_running|procs_blocked)" /proc/stat

    echo "[UDP]"
    grep -A1 "^Udp:" /proc/net/snmp

    echo "[TCP]"
    grep -A1 "^Tcp:" /proc/net/snmp

    echo "[MEM]"
    grep -E "MemAvailable|Dirty|Writeback" /proc/meminfo

    echo "[NET]"
    cat /proc/net/dev

    echo "[DISK]"
    cat /proc/diskstats

    echo "[TOP]"
    ps -eo pid,comm,%cpu,%mem,rss,stat --sort=-%cpu | head -20

    echo
    sleep 1
done
'

echo "logs: $OUT"

bash -c "$monitor_cmd" > "$OUT/central.log" 2>&1 &
P1=$!

ssh -i ~/.ssh/id_ed25519 "$PROXY" "$monitor_cmd" > "$OUT/proxy.log" 2>&1 &
P2=$!

ssh -i ~/.ssh/id_ed25519 "$R1" "$monitor_cmd" > "$OUT/receiver1.log" 2>&1 &
P3=$!

ssh -i ~/.ssh/id_ed25519 "$R2" "$monitor_cmd" > "$OUT/receiver2.log" 2>&1 &
P4=$!

cleanup()
{
    echo
    echo "stopping monitors..."

    kill "$P1" "$P2" "$P3" "$P4" 2>/dev/null

    wait "$P1" "$P2" "$P3" "$P4" 2>/dev/null

    echo "logs saved in: $OUT"
}

trap cleanup EXIT INT TERM

wait
