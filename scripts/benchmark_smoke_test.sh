#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-"$ROOT_DIR/test-runs/builds/smoke/benchmark"}"
ARTIFACT_DIR="${2:-"$ROOT_DIR/test-runs/artifacts/benchmark"}"

bash "$ROOT_DIR/scripts/benchmark_report.sh" \
    "$BUILD_DIR" \
    "$ARTIFACT_DIR"
