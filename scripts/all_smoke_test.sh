#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEST_ROOT="${1:-"$ROOT_DIR/test-runs"}"
mkdir -p "$TEST_ROOT/builds/smoke" "$TEST_ROOT/tmp/smoke" "$TEST_ROOT/artifacts"
export TMPDIR="$TEST_ROOT/tmp/smoke"

tests=(
    role_cli_smoke_test.sh
    local_repair_smoke_test.sh
    central_cache_smoke_test.sh
    section_flow_smoke_test.sh
    central_resume_smoke_test.sh
    tcp_backfill_smoke_test.sh
    fault_recovery_smoke_test.sh
)

for test_script in "${tests[@]}"; do
    test_name="${test_script%_test.sh}"
    bash "$ROOT_DIR/scripts/$test_script" "$TEST_ROOT/builds/smoke/$test_name"
done

bash "$ROOT_DIR/scripts/benchmark_smoke_test.sh" \
    "$TEST_ROOT/builds/smoke/benchmark" "$TEST_ROOT/artifacts/benchmark"
SRCAST_SECTION_BLOCKS=17 bash "$ROOT_DIR/scripts/central_cache_smoke_test.sh" \
    "$TEST_ROOT/builds/smoke/central-cache-section-17"
bash "$ROOT_DIR/scripts/multi_proxy_smoke_test.sh" "$TEST_ROOT/builds/smoke/multi-proxy"
echo "all SiteRepairCast smoke tests passed"
