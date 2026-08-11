#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

bash "$ROOT_DIR/scripts/benchmark_report.sh" \
    "$ROOT_DIR/build-benchmark-smoke" \
    "$ROOT_DIR/artifacts/benchmark"

