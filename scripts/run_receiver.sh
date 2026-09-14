#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
    cat <<'EOF'
Usage: run_receiver.sh [--config FILE] --interface-ip IPv4 --proxy-ip IPv4 [options]

Options:
  --interface-ip IPv4  This Receiver machine's local LAN IPv4 (required).
  --proxy-ip IPv4      Proxy IPv4 address (required).
  --output-dir DIR     Output directory (default: <repo>/received).
  --multicast-ip IPv4  Multicast group (default: 239.255.42.99).
  --udp-port PORT       Shared multicast UDP port (default: 5000).
  --control-port PORT   Proxy Receiver-control TCP port (default: 6000).
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
INTERFACE_IP=""
PROXY_IP=""
OUTPUT_DIR="$ROOT_DIR/received"
MULTICAST_IP=239.255.42.99
UDP_PORT=5000
CONTROL_PORT=6000
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
        --proxy-ip) [[ $# -ge 2 ]] || { echo "missing --proxy-ip value" >&2; exit 2; }; PROXY_IP="$2"; shift 2 ;;
        --output-dir) [[ $# -ge 2 ]] || { echo "missing --output-dir value" >&2; exit 2; }; OUTPUT_DIR="$2"; shift 2 ;;
        --multicast-ip) [[ $# -ge 2 ]] || { echo "missing --multicast-ip value" >&2; exit 2; }; MULTICAST_IP="$2"; shift 2 ;;
        --udp-port) [[ $# -ge 2 ]] || { echo "missing --udp-port value" >&2; exit 2; }; UDP_PORT="$2"; shift 2 ;;
        --control-port) [[ $# -ge 2 ]] || { echo "missing --control-port value" >&2; exit 2; }; CONTROL_PORT="$2"; shift 2 ;;
        --build-dir) [[ $# -ge 2 ]] || { echo "missing --build-dir value" >&2; exit 2; }; BUILD_DIR="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

[[ -n "${INTERFACE_IP:-}" ]] || { echo "--interface-ip is required" >&2; exit 2; }
[[ -n "${PROXY_IP:-}" ]] || { echo "--proxy-ip is required" >&2; exit 2; }

binary="$BUILD_DIR/receiver_agent"
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
    --proxy-ip "$PROXY_IP"
    --control-port "$CONTROL_PORT"
    --output-dir "$OUTPUT_DIR"
)
exec "${cmd[@]}"
