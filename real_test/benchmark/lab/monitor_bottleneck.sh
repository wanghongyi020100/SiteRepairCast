#!/bin/bash

OUT=${1:-test-runs/lab/bottleneck}
mkdir -p "$OUT"

while true
do
    TS=$(date '+%F %T')

    echo "===== $TS =====" >> "$OUT/system.log"

    echo "[load]" >> "$OUT/system.log"
    cat /proc/loadavg >> "$OUT/system.log"

    echo "[cpu]" >> "$OUT/system.log"
    grep '^cpu ' /proc/stat >> "$OUT/system.log"

    echo "[softirq]" >> "$OUT/system.log"
    grep '^softirq' /proc/stat >> "$OUT/system.log"

    echo "[context]" >> "$OUT/system.log"
    grep -E '^(ctxt|procs_running|procs_blocked)' /proc/stat >> "$OUT/system.log"

    echo "[memory]" >> "$OUT/system.log"
    grep -E 'MemAvailable|Dirty|Writeback' /proc/meminfo >> "$OUT/system.log"

    echo "[udp]" >> "$OUT/system.log"
    grep -A1 '^Udp:' /proc/net/snmp >> "$OUT/system.log"

    echo "[net]" >> "$OUT/system.log"
    cat /proc/net/dev >> "$OUT/system.log"

    echo "[disk]" >> "$OUT/system.log"
    cat /proc/diskstats >> "$OUT/system.log"

    sleep 1
done
