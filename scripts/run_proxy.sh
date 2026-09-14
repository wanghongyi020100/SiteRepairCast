#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
    cat <<'EOF'
Usage: run_proxy.sh [--config FILE] --interface-ip IPv4 --receivers N [options]

Options:
  --interface-ip IPv4      Local LAN IPv4 address (required).
  --receivers N            Expected Receiver process count (required).
  --multicast-ip IPv4      Multicast group (default: 239.255.42.99).
  --udp-port PORT           Shared multicast UDP port (default: 5000).
  --control-port PORT       Receiver control TCP port (default: 6000).
  --central-port PORT       Central listener TCP port (default: 7000).
  --cache-dir DIR           Proxy cache (default: <repo>/proxy-cache).
  --pace-us N               UDP delay (default: 0).
  --gap-ms N                File gap (default: 0).
  --max-repair-rounds N     Repair rounds (default: 3).
  --section-blocks N        Blocks per section (default: 4096).
  --repair-mode MODE        baseline or peer (default: peer).
  --build-dir DIR           Binary directory (default: <repo>/build).
  --config FILE             Trusted shell configuration file.
  -h, --help                Show this help.
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
INTERFACE_IP=""
MULTICAST_IP=239.255.42.99
UDP_PORT=5000
CONTROL_PORT=6000
CENTRAL_PORT=7000
RECEIVER_COUNT=""
PACE_US=0
GAP_MS=0
MAX_REPAIR_ROUNDS=3
SECTION_BLOCKS=4096
REPAIR_MODE=peer
CACHE_DIR="$ROOT_DIR/proxy-cache"
if [[ -n "$CONFIG_FILE" ]]; then
    [[ -f "$CONFIG_FILE" ]] || { echo "config file not found: $CONFIG_FILE" >&2; exit 2; }
    set -a
    # shellcheck disable=SC1090
    source "$CONFIG_FILE"
    set +a
fi

while (($#)); do
    case "$1" in
        --config) [[ $# -ge 2 ]] || { echo "missing --config value" >&2; exit 2; }; shift 2 ;;
        --interface-ip) [[ $# -ge 2 ]] || { echo "missing --interface-ip value" >&2; exit 2; }; INTERFACE_IP="$2"; shift 2 ;;
        --receivers) [[ $# -ge 2 ]] || { echo "missing --receivers value" >&2; exit 2; }; RECEIVER_COUNT="$2"; shift 2 ;;
        --multicast-ip) [[ $# -ge 2 ]] || { echo "missing --multicast-ip value" >&2; exit 2; }; MULTICAST_IP="$2"; shift 2 ;;
        --udp-port) [[ $# -ge 2 ]] || { echo "missing --udp-port value" >&2; exit 2; }; UDP_PORT="$2"; shift 2 ;;
        --control-port) [[ $# -ge 2 ]] || { echo "missing --control-port value" >&2; exit 2; }; CONTROL_PORT="$2"; shift 2 ;;
        --central-port) [[ $# -ge 2 ]] || { echo "missing --central-port value" >&2; exit 2; }; CENTRAL_PORT="$2"; shift 2 ;;
        --cache-dir) [[ $# -ge 2 ]] || { echo "missing --cache-dir value" >&2; exit 2; }; CACHE_DIR="$2"; shift 2 ;;
        --pace-us) [[ $# -ge 2 ]] || { echo "missing --pace-us value" >&2; exit 2; }; PACE_US="$2"; shift 2 ;;
        --gap-ms) [[ $# -ge 2 ]] || { echo "missing --gap-ms value" >&2; exit 2; }; GAP_MS="$2"; shift 2 ;;
        --max-repair-rounds) [[ $# -ge 2 ]] || { echo "missing --max-repair-rounds value" >&2; exit 2; }; MAX_REPAIR_ROUNDS="$2"; shift 2 ;;
        --section-blocks) [[ $# -ge 2 ]] || { echo "missing --section-blocks value" >&2; exit 2; }; SECTION_BLOCKS="$2"; shift 2 ;;
        --repair-mode) [[ $# -ge 2 ]] || { echo "missing --repair-mode value" >&2; exit 2; }; REPAIR_MODE="$2"; shift 2 ;;
        --build-dir) [[ $# -ge 2 ]] || { echo "missing --build-dir value" >&2; exit 2; }; BUILD_DIR="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

[[ -n "${INTERFACE_IP:-}" ]] || { echo "--interface-ip is required" >&2; exit 2; }
[[ -n "${RECEIVER_COUNT:-}" ]] || { echo "--receivers is required" >&2; exit 2; }

binary="$BUILD_DIR/site_proxy_sender"
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
    --multicast-ip "$MULTICAST_IP"
    --udp-port "$UDP_PORT"
    --interface-ip "$INTERFACE_IP"
    --control-port "$CONTROL_PORT"
    --receiver-count "$RECEIVER_COUNT"
    --pace-us "$PACE_US"
    --gap-ms "$GAP_MS"
    --max-repair-rounds "$MAX_REPAIR_ROUNDS"
    --section-blocks "$SECTION_BLOCKS"
    --repair-mode "$REPAIR_MODE"
    --central-listen "$CENTRAL_PORT"
    --cache-dir "$CACHE_DIR"
)
exec "${cmd[@]}"
