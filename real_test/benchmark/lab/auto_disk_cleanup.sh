#!/usr/bin/env bash
set -euo pipefail

MAX_USED_PERCENT="${MAX_USED_PERCENT:-85}"
MIN_FREE_GB="${MIN_FREE_GB:-5}"
MIN_FREE_KB=$((MIN_FREE_GB * 1024 * 1024))

REPO="$HOME/share/site"
RUN_ROOT="$REPO/test-runs/lab/runs"
HISTORY="$REPO/test-runs/lab/history"
SSH_KEY="$HOME/.ssh/id_ed25519"

PROXY="sky3141@10.100.27.64"
RECEIVER1="sky3141@10.100.14.9"
RECEIVER2="sky3141@10.100.20.143"

RUN_ID="${1:-}"

mkdir -p "$HISTORY"

LOG="$HISTORY/cleanup.jsonl"

now()
{
    date -u +"%Y-%m-%dT%H:%M:%SZ"
}

record()
{
    local host="$1"
    local action="$2"
    local before_used="$3"
    local before_free="$4"
    local after_used="$5"
    local after_free="$6"

    printf '{"time":"%s","host":"%s","run_id":"%s","action":"%s","used_percent_before":%s,"free_kb_before":%s,"used_percent_after":%s,"free_kb_after":%s}\n' \
        "$(now)" \
        "$host" \
        "$RUN_ID" \
        "$action" \
        "$before_used" \
        "$before_free" \
        "$after_used" \
        "$after_free" \
        >> "$LOG"
}

disk_used()
{
    df -Pk "$HOME" | awk 'NR==2 {gsub("%","",$5); print $5}'
}

disk_free()
{
    df -Pk "$HOME" | awk 'NR==2 {print $4}'
}

disk_too_full()
{
    local used
    local free

    used="$(disk_used)"
    free="$(disk_free)"

    if [ "$used" -ge "$MAX_USED_PERCENT" ] ||
       [ "$free" -lt "$MIN_FREE_KB" ]; then
        return 0
    fi

    return 1
}

purge_trash()
{
    if [ -d "$HOME/.local/share/Trash/files" ]; then
        find "$HOME/.local/share/Trash/files" \
            -mindepth 1 -maxdepth 1 \
            -exec rm -rf -- {} +
    fi

    if [ -d "$HOME/.local/share/Trash/info" ]; then
        find "$HOME/.local/share/Trash/info" \
            -mindepth 1 -maxdepth 1 \
            -exec rm -rf -- {} +
    fi
}

delete_payloads()
{
    local dir="$1"

    [ -d "$dir" ] || return 0

    find "$dir" \
        -type d \( -name receivers -o -name proxy-cache \) \
        -prune -exec rm -rf -- {} +
}

verify_current_run()
{
    [ -n "$RUN_ID" ] || return 1

    if [[ ! "$RUN_ID" =~ ^[A-Za-z0-9._-]+$ ]]; then
        echo "invalid run_id: $RUN_ID"
        exit 1
    fi

    local result="$RUN_ROOT/$RUN_ID/result.json"

    [ -f "$result" ] || return 1

    python3 - "$result" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    d = json.load(f)

ok = (
    d.get("status") == "PASS"
    and d.get("sha256_success") is True
)

sys.exit(0 if ok else 1)
PY
}

cleanup_old_completed_local()
{
    while disk_too_full
    do
        local found=0

        while IFS= read -r dir
        do
            [ -d "$dir" ] || continue

            # 正在运行的一律不碰
            if find "$dir" -name ACTIVE -print -quit | grep -q .; then
                continue
            fi

            # 只处理已经有 DONE 的测试
            if ! find "$dir" -name DONE -print -quit | grep -q .; then
                continue
            fi

            if find "$dir" \
                -type d \( -name receivers -o -name proxy-cache \) \
                -print -quit | grep -q .; then

                echo "cleanup old completed run: $dir"
                delete_payloads "$dir"
                found=1
                break
            fi
        done < <(
            find "$RUN_ROOT" \
                -mindepth 1 -maxdepth 1 -type d \
                -printf '%T@ %p\n' 2>/dev/null |
            sort -n |
            cut -d' ' -f2-
        )

        if [ "$found" -eq 0 ]; then
            break
        fi
    done
}

cleanup_local()
{
    local before_used
    local before_free
    local after_used
    local after_free
    local action="check"

    before_used="$(disk_used)"
    before_free="$(disk_free)"

    echo "===== CENTRAL before ====="
    df -h "$HOME"

    # 当前成功 run 每轮都立即删除 payload
    if verify_current_run; then
        echo "delete completed current run payload: $RUN_ID"
        delete_payloads "$RUN_ROOT/$RUN_ID"
        action="current-run-payload"
    fi

    # 超阈值：垃圾桶 + 最老成功 run
    if disk_too_full; then
        echo "CENTRAL disk threshold reached"

        purge_trash
        action="${action}+trash"

        cleanup_old_completed_local
        action="${action}+old-completed"
    fi

    after_used="$(disk_used)"
    after_free="$(disk_free)"

    echo "===== CENTRAL after ====="
    df -h "$HOME"

    record \
        "central" \
        "$action" \
        "$before_used" \
        "$before_free" \
        "$after_used" \
        "$after_free"

    if disk_too_full; then
        echo "ERROR: CENTRAL still above disk threshold"
        return 2
    fi
}

remote_cleanup()
{
    local host="$1"
    local role="$2"

    local output

    output="$(
        ssh -i "$SSH_KEY" "$host" \
            bash -s -- \
            "$MAX_USED_PERCENT" \
            "$MIN_FREE_KB" \
            "${RUN_ID:-__NONE__}" \
            "$role" <<'REMOTE'
set -euo pipefail

MAX_USED_PERCENT="$1"
MIN_FREE_KB="$2"
RUN_ID="${3:-__NONE__}"
ROLE="${4:-unknown}"

if [ "$RUN_ID" = "__NONE__" ]; then
    RUN_ID=""
fi

if [ "$RUN_ID" = "__NONE__" ]; then
    RUN_ID=""
fi

REPO="$HOME/share/site"
RUN_ROOT="$REPO/test-runs/lab/runs"

disk_used()
{
    df -Pk "$HOME" | awk 'NR==2 {gsub("%","",$5); print $5}'
}

disk_free()
{
    df -Pk "$HOME" | awk 'NR==2 {print $4}'
}

too_full()
{
    local used
    local free

    used="$(disk_used)"
    free="$(disk_free)"

    if [ "$used" -ge "$MAX_USED_PERCENT" ] ||
       [ "$free" -lt "$MIN_FREE_KB" ]; then
        return 0
    fi

    return 1
}

purge_trash()
{
    if [ -d "$HOME/.local/share/Trash/files" ]; then
        find "$HOME/.local/share/Trash/files" \
            -mindepth 1 -maxdepth 1 \
            -exec rm -rf -- {} +
    fi

    if [ -d "$HOME/.local/share/Trash/info" ]; then
        find "$HOME/.local/share/Trash/info" \
            -mindepth 1 -maxdepth 1 \
            -exec rm -rf -- {} +
    fi
}

delete_payloads()
{
    local dir="$1"

    [ -d "$dir" ] || return 0

    find "$dir" \
        -type d \( -name receivers -o -name proxy-cache \) \
        -prune -exec rm -rf -- {} +
}

cleanup_old()
{
    while too_full
    do
        found=0

        while IFS= read -r dir
        do
            [ -d "$dir" ] || continue

            if find "$dir" -name ACTIVE -print -quit | grep -q .; then
                continue
            fi

            if ! find "$dir" -name DONE -print -quit | grep -q .; then
                continue
            fi

            if find "$dir" \
                -type d \( -name receivers -o -name proxy-cache \) \
                -print -quit | grep -q .; then

                echo "cleanup old completed run: $dir"
                delete_payloads "$dir"
                found=1
                break
            fi
        done < <(
            find "$RUN_ROOT" \
                -mindepth 1 -maxdepth 1 -type d \
                -printf '%T@ %p\n' 2>/dev/null |
            sort -n |
            cut -d' ' -f2-
        )

        if [ "$found" -eq 0 ]; then
            break
        fi
    done
}

before_used="$(disk_used)"
before_free="$(disk_free)"
action="check"

echo "===== $ROLE before ====="
df -h "$HOME"

# RUN_ID 由中央已经验证 PASS 后传入。
if [ -n "$RUN_ID" ] &&
   [[ "$RUN_ID" =~ ^[A-Za-z0-9._-]+$ ]] &&
   [ -d "$RUN_ROOT/$RUN_ID" ]; then

    delete_payloads "$RUN_ROOT/$RUN_ID"
    action="current-run-payload"
fi

if too_full; then
    echo "$ROLE disk threshold reached"

    purge_trash
    action="${action}+trash"

    cleanup_old
    action="${action}+old-completed"
fi

after_used="$(disk_used)"
after_free="$(disk_free)"

echo "===== $ROLE after ====="
df -h "$HOME"

echo "RESULT|$before_used|$before_free|$after_used|$after_free|$action"

if too_full; then
    exit 2
fi
REMOTE
    )"

    local rc=$?

    echo "$output"

    local result
    result="$(echo "$output" | grep '^RESULT|' | tail -1 || true)"

    if [ -n "$result" ]; then
        local marker
        local before_used
        local before_free
        local after_used
        local after_free
        local action

        IFS='|' read -r \
            marker \
            before_used \
            before_free \
            after_used \
            after_free \
            action <<< "$result"

        record \
            "$role" \
            "$action" \
            "$before_used" \
            "$before_free" \
            "$after_used" \
            "$after_free"
    fi

    return "$rc"
}

echo
echo "============================================"
echo "AUTO DISK CLEANUP"
echo "threshold=${MAX_USED_PERCENT}%"
echo "minimum_free=${MIN_FREE_GB}GiB"
echo "run_id=${RUN_ID:-none}"
echo "============================================"
echo

cleanup_local
central_rc=$?

remote_cleanup "$PROXY" "proxy"
proxy_rc=$?

remote_cleanup "$RECEIVER1" "receiver-host-1"
r1_rc=$?

remote_cleanup "$RECEIVER2" "receiver-host-2"
r2_rc=$?

echo
echo "===== CLEANUP RESULT ====="
echo "central=$central_rc"
echo "proxy=$proxy_rc"
echo "receiver1=$r1_rc"
echo "receiver2=$r2_rc"
echo "history=$LOG"

if [ "$central_rc" -ne 0 ] ||
   [ "$proxy_rc" -ne 0 ] ||
   [ "$r1_rc" -ne 0 ] ||
   [ "$r2_rc" -ne 0 ]; then
    echo "ERROR: at least one host is still above disk threshold"
    exit 2
fi

echo "disk cleanup OK"
