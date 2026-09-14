#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
    cat <<'EOF'
Usage: run_central.sh [--config FILE] --proxy-ip IPv4 --file PATH [options]

Options:
  --proxy-ip IPv4       Proxy IPv4 address (required).
  --proxy-port PORT     Proxy Central TCP port (default: 7000).
  --file PATH           Source file; may be repeated (required).
  --pace-us N           Per-block delay (default: 0).
  --section-blocks N    Blocks per section (default: 4096).
  --build-dir DIR       Binary directory (default: <repo>/build).
  --config FILE         Trusted shell configuration file.
  -h, --help            Show this help.
EOF
}

CONFIG_FILE=""
args=("$@")
for ((index=0; index<${#args[@]}; index++)); do
    if [[ "${args[index]}" == "--config" ]]; then
        ((index + 1 < ${#args[@]})) || { echo "missing --config value" >&2; exit 2; }
        CONFIG_FILE="${args[index + 1]}"
        ((index+=1))
    fi
done

BUILD_DIR="$ROOT_DIR/build"
PROXY_IP=""
PROXY_PORT=7000
PACE_US=0
SECTION_BLOCKS=4096
FILE=""
if [[ -n "$CONFIG_FILE" ]]; then
    [[ -f "$CONFIG_FILE" ]] || { echo "config file not found: $CONFIG_FILE" >&2; exit 2; }
    set -a
    # shellcheck disable=SC1090
    source "$CONFIG_FILE"
    set +a
fi

files=()
[[ -z "${FILE:-}" ]] || files+=("$FILE")
cli_files=false
while (($#)); do
    case "$1" in
        --config) [[ $# -ge 2 ]] || { echo "missing --config value" >&2; exit 2; }; shift 2 ;;
        --proxy-ip) [[ $# -ge 2 ]] || { echo "missing --proxy-ip value" >&2; exit 2; }; PROXY_IP="$2"; shift 2 ;;
        --proxy-port) [[ $# -ge 2 ]] || { echo "missing --proxy-port value" >&2; exit 2; }; PROXY_PORT="$2"; shift 2 ;;
        --file)
            [[ $# -ge 2 ]] || { echo "missing --file value" >&2; exit 2; }
            if [[ "$cli_files" == false ]]; then files=(); cli_files=true; fi
            files+=("$2")
            shift 2
            ;;
        --pace-us) [[ $# -ge 2 ]] || { echo "missing --pace-us value" >&2; exit 2; }; PACE_US="$2"; shift 2 ;;
        --section-blocks) [[ $# -ge 2 ]] || { echo "missing --section-blocks value" >&2; exit 2; }; SECTION_BLOCKS="$2"; shift 2 ;;
        --build-dir) [[ $# -ge 2 ]] || { echo "missing --build-dir value" >&2; exit 2; }; BUILD_DIR="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

[[ -n "${PROXY_IP:-}" ]] || { echo "--proxy-ip is required" >&2; exit 2; }
((${#files[@]} > 0)) || { echo "--file is required" >&2; exit 2; }

binary="$BUILD_DIR/central_sender"
if [[ ! -x "$binary" ]]; then
    cat >&2 <<EOF
binary not found, build first:
cmake -S "$ROOT_DIR" -B "$ROOT_DIR/build" -DCMAKE_BUILD_TYPE=Release
cmake --build "$ROOT_DIR/build" -j\$(nproc)
EOF
    exit 1
fi

cmd=(
    "$binary"
    --proxy "$PROXY_IP" "$PROXY_PORT"
    --pace-us "$PACE_US"
    --section-blocks "$SECTION_BLOCKS"
)
for file in "${files[@]}"; do cmd+=(--file "$file"); done
exec "${cmd[@]}"
