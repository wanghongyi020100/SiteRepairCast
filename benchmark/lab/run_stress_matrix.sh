#!/usr/bin/env bash

set -u
set -o pipefail

# EXIT_CLEANUP_PATCH_V1
CURRENT_RUN_ID=""

cleanup_on_script_exit()
{
    local rc=$?

    trap - EXIT INT TERM

    if [ -n "${CURRENT_RUN_ID:-}" ]; then
        echo
        echo "===== EXIT STOP: $CURRENT_RUN_ID ====="

        # 关键：先停进程。
        # 否则进程仍持有已unlink文件时，磁盘空间不会真正释放。
        python3 benchmark/lab/labctl.py stop \
            --run-id "$CURRENT_RUN_ID" || true

        sleep 2

        echo "===== EXIT CLEANUP: $CURRENT_RUN_ID ====="

        benchmark/lab/finalize_run_cleanup.sh \
            "$CURRENT_RUN_ID" || true
    else
        benchmark/lab/finalize_run_cleanup.sh || true
    fi

    CURRENT_RUN_ID=""

    exit "$rc"
}

trap cleanup_on_script_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM


ROOT="$HOME/share/site"
cd "$ROOT" || exit 1

STRESS_CONFIG="benchmark/lab_configs/stress_1g.json"
EXPERIMENT_CONFIG="benchmark/lab_configs/experiment.json"
RUNS_CSV="test-runs/lab/history/runs.csv"
STRESS_CSV="test-runs/lab/history/stress_runs.csv"

PROXY="10.100.27.64"
R1="10.100.14.9"
R2="10.100.20.143"

SSH_KEY="$HOME/.ssh/id_ed25519"
SSH_USER="sky3141"

mkdir -p test-runs/lab/history

if [ ! -f "$STRESS_CONFIG" ]; then
    echo "ERROR: missing $STRESS_CONFIG"
    exit 1
fi

if [ ! -f "$EXPERIMENT_CONFIG" ]; then
    echo "ERROR: missing $EXPERIMENT_CONFIG"
    exit 1
fi

if [ ! -f "$EXPERIMENT_CONFIG.before-stress" ]; then
    cp "$EXPERIMENT_CONFIG" "$EXPERIMENT_CONFIG.before-stress"
fi

read_config()
{
    python3 - "$STRESS_CONFIG" "$1" <<'PY'
import json
import sys

p,key=sys.argv[1:3]
d=json.load(open(p))
v=d[key]

if isinstance(v,list):
    print(" ".join(str(x) for x in v))
else:
    print(v)
PY
}

FILE="$(read_config file)"
RECEIVER_COUNTS=( $(read_config receiver_counts) )
LOSS_RATES=( $(read_config loss_percent) )
MODES=( $(read_config modes) )

TARGET="$(read_config target_success_runs)"
MAX_ATTEMPTS="$(read_config max_attempts_per_case)"

PAYLOAD="$(read_config payload_size)"
UDP_PACE="$(read_config udp_pace_us)"
CENTRAL_PACE="$(read_config central_pace_us)"
MAX_REPAIR_ROUNDS="$(read_config max_repair_rounds)"

READY_TIMEOUT="$(read_config ready_timeout_seconds)"
FINISH_TIMEOUT="$(read_config finish_timeout_seconds)"
BETWEEN_RUNS="$(read_config between_runs_seconds)"

if [ ! -f "$FILE" ]; then
    echo "ERROR: source file does not exist:"
    echo "$FILE"
    exit 1
fi

FILE_SIZE="$(stat -c %s "$FILE")"

TOTAL_BLOCKS="$(
python3 - "$FILE_SIZE" "$PAYLOAD" <<'PY'
import sys
size=int(sys.argv[1])
payload=int(sys.argv[2])
print((size+payload-1)//payload)
PY
)"

SOURCE_SHA="$(sha256sum "$FILE" | awk '{print $1}')"

echo
echo "=================================================="
echo "STRESS TEST CONFIG"
echo "=================================================="
echo "file=$FILE"
echo "file_size=$FILE_SIZE"
echo "sha256=$SOURCE_SHA"
echo "payload=$PAYLOAD"
echo "total_blocks=$TOTAL_BLOCKS"
echo "receivers=${RECEIVER_COUNTS[*]}"
echo "loss_percent=${LOSS_RATES[*]}"
echo "modes=${MODES[*]}"
echo "target_success_runs=$TARGET"
echo "max_attempts=$MAX_ATTEMPTS"
echo "=================================================="
echo

ssh_preflight()
{
    for host in "$PROXY" "$R1" "$R2"
    do
        echo "SSH check $host"

        ssh \
            -o BatchMode=yes \
            -o ConnectTimeout=5 \
            -i "$SSH_KEY" \
            "$SSH_USER@$host" \
            'hostname >/dev/null' || return 1
    done

    return 0
}

loss_blocks()
{
    local pct="$1"

    python3 - "$TOTAL_BLOCKS" "$pct" <<'PY'
import sys
blocks=int(sys.argv[1])
pct=float(sys.argv[2])

# 使用 floor。
# 对100 Receiver + 1%时保证仍满足当前稀疏不重叠丢块计划约束。
n=int(blocks*pct/100.0)

if pct>0 and n<1:
    n=1

print(n)
PY
}

actual_loss_percent()
{
    local blocks="$1"

    python3 - "$TOTAL_BLOCKS" "$blocks" <<'PY'
import sys
total=int(sys.argv[1])
lost=int(sys.argv[2])
print("%.8f" % (100.0*lost/total))
PY
}

set_experiment()
{
    local mode="$1"
    local n="$2"
    local file="$3"
    local drops="$4"

    python3 - \
        "$EXPERIMENT_CONFIG" \
        "$mode" \
        "$n" \
        "$file" \
        "$drops" \
        "$TOTAL_BLOCKS" \
        "$UDP_PACE" \
        "$CENTRAL_PACE" \
        "$MAX_REPAIR_ROUNDS" <<'PY'

import json
import sys
from pathlib import Path

(
    path,
    mode,
    receivers,
    file_path,
    drops,
    total_blocks,
    udp_pace,
    central_pace,
    max_rounds
)=sys.argv[1:]

p=Path(path)
d=json.loads(p.read_text())

d["name"]="stress-1g"
d["receiver_count"]=int(receivers)

# 两个都写，避免之前 modes=baseline 但 mode=peer 的问题。
d["mode"]=mode
d["modes"]=[mode]

d["repetitions"]=1

# 使用已经存在的文件，不再生成128MiB测试文件。
d["files"]=[file_path]
d["generated_files"]=[]

# 为了让当前 deterministic sparse loss 能覆盖1GiB文件中的
# n% 缺块，并满足 receiver_count * drops 约束，
# 本压力实验把整个文件作为一个 Section。
d["section_blocks"]=int(total_blocks)

d["udp_pace_us"]=int(udp_pace)
d["central_pace_us"]=int(central_pace)
d["max_repair_rounds"]=int(max_rounds)

loss=d.setdefault("loss",{})
loss["enabled"]=True
loss["blocks_per_receiver"]=int(drops)

p.write_text(json.dumps(d,indent=2)+"\n")
PY
}

validate_loss()
{
    local receivers="$1"
    local drops="$2"

    python3 - "$receivers" "$drops" "$TOTAL_BLOCKS" <<'PY'
import sys

r=int(sys.argv[1])
drops=int(sys.argv[2])
total=int(sys.argv[3])

needed=r*drops

# 当前稀疏测试生成器需要给不同Receiver分离缺块。
limit=total-17

print("receiver_count =",r)
print("drop_blocks_per_receiver =",drops)
print("all_receiver_drop_assignments =",needed)
print("available_sparse_blocks =",limit)

if needed>limit:
    print()
    print("ERROR: loss rate too high for current deterministic sparse loss generator.")
    print("For this receiver count, reduce loss_percent.")
    raise SystemExit(1)
PY
}

loss_tag()
{
    echo "$1" | sed 's/\./p/g'
}

prefix_for()
{
    local mode="$1"
    local n="$2"
    local loss="$3"

    echo "${mode}-${n}-loss$(loss_tag "$loss")-"
}

next_run_id()
{
    local prefix="$1"

    python3 - "$prefix" <<'PY'
import sys
from pathlib import Path
import re

prefix=sys.argv[1]
root=Path("test-runs/lab/runs")

mx=0

if root.exists():
    pattern=re.compile(
        "^"+re.escape(prefix)+r"r(\d+)$"
    )

    for p in root.iterdir():
        m=pattern.match(p.name)
        if m:
            mx=max(mx,int(m.group(1)))

print("%sr%03d"%(prefix,mx+1))
PY
}

count_valid()
{
    local prefix="$1"

    python3 - "$RUNS_CSV" "$prefix" "$FILE_SIZE" <<'PY'
import csv
import sys
from pathlib import Path

path=Path(sys.argv[1])
prefix=sys.argv[2]
size=int(sys.argv[3])

if not path.exists():
    print(0)
    raise SystemExit

count=0

with path.open(newline="") as f:
    for r in csv.DictReader(f):
        if not r.get("run_id","").startswith(prefix):
            continue

        if r.get("status")!="PASS":
            continue

        if r.get("sha256_success")!="True":
            continue

        try:
            if int(r.get("total_file_bytes") or 0)!=size:
                continue
        except:
            continue

        if not r.get("transfer_time_seconds"):
            continue

        count+=1

print(count)
PY
}

wait_ready()
{
    local n="$1"
    local loops=$((READY_TIMEOUT/2))

    for ((i=1;i<=loops;i++))
    do
        status="$(
            python3 benchmark/lab/labctl.py status 2>&1 || true
        )"

        echo "$status"

        if echo "$status" | grep -q '"ready": true' &&
           echo "$status" | grep -q "\"registered\": $n"
        then
            return 0
        fi

        sleep 2
    done

    return 1
}

wait_finished()
{
    local loops=$((FINISH_TIMEOUT/2))

    for ((i=1;i<=loops;i++))
    do
        status="$(
            python3 benchmark/lab/labctl.py status 2>&1 || true
        )"

        echo "$status"

        proxy_done=0
        receiver_done=0

        if echo "$status" | grep -q '"lifecycle": "COMPLETED"'
        then
            proxy_done=1
        fi

        receiver_done="$(
            echo "$status" |
            grep -c 'receiver_lifecycle=DONE' || true
        )"

        if [ "$proxy_done" -eq 1 ] &&
           [ "$receiver_done" -ge 2 ]
        then
            return 0
        fi

        sleep 2
    done

    return 1
}

verify_precollect_result()
{
    local run_id="$1"

    python3 - "$run_id" <<'PY'
import json
import sys
from pathlib import Path

run_id=sys.argv[1]

p=Path("test-runs/lab/runs")/run_id/"result.json"

if not p.exists():
    print("missing result.json")
    raise SystemExit(1)

d=json.loads(p.read_text())

print("status =",d.get("status"))
print("transfer_status =",d.get("transfer_status"))
print("sha256_success =",d.get("sha256_success"))

ok=(
    d.get("status")=="PASS"
    and d.get("transfer_status")=="PASS"
    and d.get("sha256_success") is True
)

raise SystemExit(0 if ok else 1)
PY
}

verify_final_result()
{
    local run_id="$1"

    python3 - "$run_id" <<'PY'
import json
import sys
from pathlib import Path

run_id=sys.argv[1]
p=Path("test-runs/lab/runs")/run_id/"result.json"

if not p.exists():
    raise SystemExit(1)

d=json.loads(p.read_text())

print("status =",d.get("status"))
print("sha256_success =",d.get("sha256_success"))
print("metrics_valid =",d.get("metrics_valid"))
print("transfer_time_seconds =",d.get("transfer_time_seconds"))
print("metrics =",d.get("metrics"))

ok=(
    d.get("status")=="PASS"
    and d.get("sha256_success") is True
    and d.get("metrics_valid") is True
)

raise SystemExit(0 if ok else 1)
PY
}

write_case_metadata()
{
    local run_id="$1"
    local mode="$2"
    local n="$3"
    local requested_loss="$4"
    local drops="$5"
    local actual_loss="$6"

    python3 - \
        "$run_id" \
        "$mode" \
        "$n" \
        "$requested_loss" \
        "$drops" \
        "$actual_loss" \
        "$FILE" \
        "$FILE_SIZE" \
        "$SOURCE_SHA" \
        "$TOTAL_BLOCKS" <<'PY'

import json
import sys
from pathlib import Path

(
 run_id,
 mode,
 receivers,
 requested,
 drops,
 actual,
 file_path,
 size,
 sha,
 total_blocks
)=sys.argv[1:]

p=Path("test-runs/lab/runs")/run_id
p.mkdir(parents=True,exist_ok=True)

data={
    "run_id":run_id,
    "mode":mode,
    "receiver_count":int(receivers),
    "source_file":file_path,
    "source_file_bytes":int(size),
    "source_sha256":sha,
    "payload_size":1200,
    "total_blocks":int(total_blocks),
    "requested_loss_percent":float(requested),
    "drop_blocks_per_receiver":int(drops),
    "actual_configured_loss_percent":float(actual)
}

(p/"stress_case.json").write_text(
    json.dumps(data,indent=2)+"\n"
)
PY
}

record_stress_row()
{
    local run_id="$1"

    python3 - "$run_id" "$STRESS_CSV" <<'PY'
import csv
import json
import sys
from pathlib import Path

run_id=sys.argv[1]
out=Path(sys.argv[2])

meta_path=Path("test-runs/lab/runs")/run_id/"stress_case.json"

if not meta_path.exists():
    raise SystemExit(1)

m=json.loads(meta_path.read_text())

fields=[
    "run_id",
    "mode",
    "receiver_count",
    "source_file_bytes",
    "requested_loss_percent",
    "actual_configured_loss_percent",
    "drop_blocks_per_receiver"
]

rows=[]

if out.exists():
    with out.open(newline="") as f:
        rows=list(csv.DictReader(f))

rows=[r for r in rows if r.get("run_id")!=run_id]

rows.append({
    k:m.get(k,"")
    for k in fields
})

with out.open("w",newline="") as f:
    w=csv.DictWriter(f,fieldnames=fields)
    w.writeheader()
    w.writerows(rows)
PY
}

# ALWAYS_CLEANUP_PATCH_V1
run_one()
{
    local mode="$1"
    local n="$2"
    local loss="$3"
    local drops="$4"
    local actual_loss="$5"

    local prefix
    prefix="$(prefix_for "$mode" "$n" "$loss")"

    local run_id
    run_id="$(next_run_id "$prefix")"
    CURRENT_RUN_ID="$run_id"

    echo
    echo "=================================================="
    echo "RUN $run_id"
    echo "mode=$mode"
    echo "receivers=$n"
    echo "requested_loss=$loss%"
    echo "actual_loss=$actual_loss%"
    echo "drop_blocks_per_receiver=$drops"
    echo "=================================================="

    python3 benchmark/lab/labctl.py stop >/dev/null 2>&1 || true

    if ! benchmark/lab/auto_disk_cleanup.sh
    then
        echo "ERROR: disk preflight failed"
        benchmark/lab/finalize_run_cleanup.sh "$run_id" || true
        CURRENT_RUN_ID=""
        return 2
    fi

    set_experiment "$mode" "$n" "$FILE" "$drops"

    echo "===== EXPERIMENT ====="
    python3 - <<'PY'
import json
d=json.load(open("benchmark/lab_configs/experiment.json"))
for k in [
    "receiver_count",
    "mode",
    "modes",
    "files",
    "section_blocks",
    "udp_pace_us",
    "central_pace_us",
    "max_repair_rounds",
    "loss"
]:
    print(k,"=",d.get(k))
PY

    echo "===== DEPLOY ====="

    if ! python3 benchmark/lab/labctl.py deploy
    then
        echo "ERROR: deploy failed"
        benchmark/lab/finalize_run_cleanup.sh "$run_id" || true
        CURRENT_RUN_ID=""
        return 2
    fi

    echo "===== START ====="

    start_output="$(
        python3 benchmark/lab/labctl.py start \
            --run-id "$run_id" 2>&1
    )"

    start_rc=$?

    echo "$start_output"

    if [ "$start_rc" -ne 0 ]
    then
        python3 benchmark/lab/labctl.py stop || true
        benchmark/lab/finalize_run_cleanup.sh "$run_id" || true
        CURRENT_RUN_ID=""
        return 1
    fi

    write_case_metadata \
        "$run_id" \
        "$mode" \
        "$n" \
        "$loss" \
        "$drops" \
        "$actual_loss"

    echo "===== WAIT READY ====="

    if ! wait_ready "$n"
    then
        echo "ERROR: READY timeout"
        python3 benchmark/lab/labctl.py stop || true
        benchmark/lab/finalize_run_cleanup.sh "$run_id" || true
        CURRENT_RUN_ID=""
        return 1
    fi

    echo "===== RUN CENTRAL ====="

    timeout 30m \
        python3 benchmark/lab/labctl.py run-central

    central_rc=$?

    if [ "$central_rc" -ne 0 ]
    then
        echo "ERROR: run-central failed rc=$central_rc"

        python3 benchmark/lab/labctl.py collect || true
        python3 benchmark/lab/labctl.py stop || true

        benchmark/lab/finalize_run_cleanup.sh "$run_id" || true

        CURRENT_RUN_ID=""
        return 1
    fi

    echo "===== WAIT RECEIVER FINALIZATION ====="

    if ! wait_finished
    then
        echo "WARNING: finalization status timeout"
    fi

    echo "===== REMOTE RECEIVER SHA256 VERIFY ====="

    if ! benchmark/lab/verify_remote_receivers.sh \
        "$run_id" "$FILE" "$n"
    then
        echo "ERROR: remote receiver SHA verification failed"
        python3 benchmark/lab/labctl.py stop || true
        benchmark/lab/finalize_run_cleanup.sh "$run_id" || true
        CURRENT_RUN_ID=""
        return 1
    fi

    echo "===== PREPARE COLLECT HARDLINKS ====="

    if ! benchmark/lab/prepare_collect_hardlinks.sh \
        "$run_id" "$FILE"
    then
        echo "ERROR: collector hardlink preparation failed"
        python3 benchmark/lab/labctl.py stop || true
        benchmark/lab/finalize_run_cleanup.sh "$run_id" || true
        CURRENT_RUN_ID=""
        return 2
    fi

    echo "===== SAFE COLLECT ====="

    if ! PATH="$PWD/benchmark/lab/safe-bin:$PATH" \
        python3 benchmark/lab/labctl.py collect \
            --run-id "$run_id"
    then
        echo "ERROR: safe collect failed"
        python3 benchmark/lab/labctl.py stop || true
        benchmark/lab/finalize_run_cleanup.sh "$run_id" || true
        CURRENT_RUN_ID=""
        return 2
    fi

    echo "===== FINAL VERIFY ====="

    if ! verify_final_result "$run_id"
    then
        echo "ERROR: final result invalid"
        python3 benchmark/lab/labctl.py stop || true
        benchmark/lab/finalize_run_cleanup.sh "$run_id" || true
        CURRENT_RUN_ID=""
        return 1
    fi

    record_stress_row "$run_id"

    echo "===== DELETE LARGE PAYLOAD ====="

    if ! benchmark/lab/finalize_run_cleanup.sh "$run_id"
    then
        echo "ERROR: large payload cleanup failed"
        python3 benchmark/lab/labctl.py stop || true
        benchmark/lab/finalize_run_cleanup.sh "$run_id" || true
        CURRENT_RUN_ID=""
        return 2
    fi

    echo "===== STOP ====="
    python3 benchmark/lab/labctl.py stop || true

    echo
    echo "PASS: $run_id"

    sleep "$BETWEEN_RUNS"

    benchmark/lab/finalize_run_cleanup.sh "$run_id" || true

    CURRENT_RUN_ID=""
    return 0
}

echo "===== SSH PREFLIGHT ====="

if ! ssh_preflight
then
    echo "ERROR: SSH preflight failed"
    exit 1
fi

echo "===== INITIAL DISK PREFLIGHT ====="

if ! benchmark/lab/auto_disk_cleanup.sh
then
    exit 1
fi

echo
echo "=================================================="
echo "VALIDATING LOSS MATRIX"
echo "=================================================="

for n in "${RECEIVER_COUNTS[@]}"
do
    for loss in "${LOSS_RATES[@]}"
    do
        drops="$(loss_blocks "$loss")"
        actual="$(actual_loss_percent "$drops")"

        echo
        echo "receivers=$n requested=$loss% blocks=$drops actual=$actual%"

        validate_loss "$n" "$drops" || exit 1
    done
done

echo
echo "=================================================="
echo "START STRESS MATRIX"
echo "=================================================="

for n in "${RECEIVER_COUNTS[@]}"
do
    for loss in "${LOSS_RATES[@]}"
    do
        drops="$(loss_blocks "$loss")"
        actual="$(actual_loss_percent "$drops")"

        for mode in "${MODES[@]}"
        do
            prefix="$(prefix_for "$mode" "$n" "$loss")"
            attempts=0

            while true
            do
                valid="$(count_valid "$prefix")"

                echo
                echo "--------------------------------------------------"
                echo "mode=$mode receivers=$n loss=$loss%"
                echo "valid=$valid/$TARGET"
                echo "attempts_this_session=$attempts/$MAX_ATTEMPTS"
                echo "--------------------------------------------------"

                if [ "$valid" -ge "$TARGET" ]
                then
                    echo "CASE COMPLETE"
                    break
                fi

                attempts=$((attempts+1))

                if [ "$attempts" -gt "$MAX_ATTEMPTS" ]
                then
                    echo "ERROR: max attempts reached"
                    echo "mode=$mode receivers=$n loss=$loss%"
                    exit 1
                fi

                run_one \
                    "$mode" \
                    "$n" \
                    "$loss" \
                    "$drops" \
                    "$actual"

                rc=$?

                if [ "$rc" -eq 2 ]
                then
                    echo "FATAL INFRASTRUCTURE ERROR"
                    exit 1
                fi
            done
        done
    done
done

echo
echo "=================================================="
echo "GENERATE STRESS SUMMARY"
echo "=================================================="

python3 - <<'PY'
import csv
import statistics
from collections import defaultdict
from pathlib import Path

stress_path=Path("test-runs/lab/history/stress_runs.csv")
runs_path=Path("test-runs/lab/history/runs.csv")
out_path=Path("test-runs/lab/history/stress_summary.csv")

if not stress_path.exists() or not runs_path.exists():
    raise SystemExit("missing stress/runs csv")

with stress_path.open(newline="") as f:
    stress={
        r["run_id"]:r
        for r in csv.DictReader(f)
    }

with runs_path.open(newline="") as f:
    runs={
        r["run_id"]:r
        for r in csv.DictReader(f)
    }

groups=defaultdict(list)

for run_id,m in stress.items():
    r=runs.get(run_id)

    if not r:
        continue

    if r.get("status")!="PASS":
        continue

    if r.get("sha256_success")!="True":
        continue

    key=(
        m["mode"],
        int(m["receiver_count"]),
        float(m["requested_loss_percent"])
    )

    groups[key].append((m,r))

fields=[
    "mode",
    "receiver_count",
    "loss_percent",
    "runs",
    "avg_transfer_seconds",
    "avg_proxy_cpu_percent",
    "avg_proxy_peak_rss_mb",
    "avg_proxy_repair_payload_bytes",
    "avg_proxy_repair_pread_blocks",
    "avg_peer_blocks"
]

rows=[]

def avg(items,key):
    vals=[]
    for _,r in items:
        try:
            vals.append(float(r.get(key) or 0))
        except:
            pass
    return statistics.mean(vals) if vals else 0

for (mode,n,loss),items in sorted(
    groups.items(),
    key=lambda x:(x[0][1],x[0][2],x[0][0])
):
    rows.append({
        "mode":mode,
        "receiver_count":n,
        "loss_percent":loss,
        "runs":len(items),
        "avg_transfer_seconds":"%.6f"%avg(items,"transfer_time_seconds"),
        "avg_proxy_cpu_percent":"%.6f"%avg(items,"proxy_avg_cpu_percent"),
        "avg_proxy_peak_rss_mb":"%.6f"%avg(items,"proxy_peak_rss_mb"),
        "avg_proxy_repair_payload_bytes":"%.3f"%avg(items,"proxy_repair_payload_bytes"),
        "avg_proxy_repair_pread_blocks":"%.3f"%avg(items,"proxy_repair_pread_blocks"),
        "avg_peer_blocks":"%.3f"%avg(items,"peer_blocks")
    })

with out_path.open("w",newline="") as f:
    w=csv.DictWriter(f,fieldnames=fields)
    w.writeheader()
    w.writerows(rows)

print()
for r in rows:
    print(
        "%-8s receivers=%3d loss=%6.3f%% runs=%d "
        "time=%8ss proxy_payload=%12s pread=%10s peer=%10s"
        %(
            r["mode"],
            r["receiver_count"],
            r["loss_percent"],
            r["runs"],
            r["avg_transfer_seconds"],
            r["avg_proxy_repair_payload_bytes"],
            r["avg_proxy_repair_pread_blocks"],
            r["avg_peer_blocks"]
        )
    )

print()
print("summary =",out_path)
PY

echo
echo "=================================================="
echo "ALL STRESS TESTS FINISHED"
echo "=================================================="
