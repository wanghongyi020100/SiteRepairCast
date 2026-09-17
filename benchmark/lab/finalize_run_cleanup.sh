#!/usr/bin/env bash
set -u

RUN_ID="${1:-}"

SSH_USER="sky3141"
SSH_KEY="$HOME/.ssh/id_ed25519"

PROXY="10.100.27.64"
R1="10.100.14.9"
R2="10.100.20.143"

RC=0

echo
echo "============================================"
echo "FINALIZE RUN CLEANUP"
echo "run_id=${RUN_ID:-none}"
echo "============================================"

if [ -n "$RUN_ID" ]; then
    if [[ ! "$RUN_ID" =~ ^[A-Za-z0-9._-]+$ ]]; then
        echo "ERROR: invalid run_id: $RUN_ID"
        exit 1
    fi

    echo "===== DELETE RUN LARGE FILES ====="

    benchmark/lab/trim_run_large_files.sh "$RUN_ID" || RC=1


    # SMALL_PARTIAL_CLEANUP_V1
    # trim_run_large_files.sh 会删除 >16MiB 文件；
    # 这里额外处理刚启动就失败时留下的小 .part 文件，
    # 以及 proxy-cache 中不论大小的缓存文件。
    LOCAL_RUN="$HOME/share/site/test-runs/lab/runs/$RUN_ID"

    if [ -d "$LOCAL_RUN" ]; then
        find "$LOCAL_RUN" \
            -type f \
            -name '*.part-*' \
            -print \
            -delete 2>/dev/null || true

        find "$LOCAL_RUN" \
            -type f \
            -path '*/proxy-cache/*' \
            -print \
            -delete 2>/dev/null || true
    fi

    for host in "$PROXY" "$R1" "$R2"
    do
        ssh \
            -o BatchMode=yes \
            -o ConnectTimeout=10 \
            -i "$SSH_KEY" \
            "$SSH_USER@$host" \
            bash -s -- "$RUN_ID" <<'REMOTE' || RC=1
set -u

RUN_ID="$1"
ROOT="$HOME/share/site/test-runs/lab/runs/$RUN_ID"

if [ -d "$ROOT" ]; then
    find "$ROOT" \
        -type f \
        -name '*.part-*' \
        -print \
        -delete 2>/dev/null || true

    find "$ROOT" \
        -type f \
        -path '*/proxy-cache/*' \
        -print \
        -delete 2>/dev/null || true
fi
REMOTE
    done
fi

purge_local_trash()
{
    echo "===== CENTRAL TRASH ====="

    mkdir -p \
        "$HOME/.local/share/Trash/files" \
        "$HOME/.local/share/Trash/info"

    echo "before:"
    du -sh "$HOME/.local/share/Trash" 2>/dev/null || true

    find "$HOME/.local/share/Trash/files" \
        -mindepth 1 -maxdepth 1 \
        -exec rm -rf -- {} + 2>/dev/null || true

    find "$HOME/.local/share/Trash/info" \
        -mindepth 1 -maxdepth 1 \
        -exec rm -rf -- {} + 2>/dev/null || true

    echo "after:"
    du -sh "$HOME/.local/share/Trash" 2>/dev/null || true
}

purge_remote_trash()
{
    local host="$1"
    local role="$2"

    echo "===== $role TRASH ====="

    ssh \
        -o BatchMode=yes \
        -o ConnectTimeout=10 \
        -i "$SSH_KEY" \
        "$SSH_USER@$host" \
        'mkdir -p "$HOME/.local/share/Trash/files" "$HOME/.local/share/Trash/info";
         echo before:;
         du -sh "$HOME/.local/share/Trash" 2>/dev/null || true;
         find "$HOME/.local/share/Trash/files" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} + 2>/dev/null || true;
         find "$HOME/.local/share/Trash/info" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} + 2>/dev/null || true;
         echo after:;
         du -sh "$HOME/.local/share/Trash" 2>/dev/null || true' || RC=1
}

purge_local_trash
purge_remote_trash "$PROXY" "PROXY"
purge_remote_trash "$R1" "RECEIVER-HOST-1"
purge_remote_trash "$R2" "RECEIVER-HOST-2"

echo
echo "===== FINAL DISK ====="

echo "CENTRAL"
df -h ~

for host in "$PROXY" "$R1" "$R2"
do
    echo "$host"

    ssh \
        -o BatchMode=yes \
        -o ConnectTimeout=10 \
        -i "$SSH_KEY" \
        "$SSH_USER@$host" \
        'df -h ~' || RC=1
done

echo
if [ "$RC" -eq 0 ]; then
    echo "FINALIZE CLEANUP OK"
else
    echo "FINALIZE CLEANUP FINISHED WITH WARNING"
fi

exit "$RC"
