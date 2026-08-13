#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

tests=(
    local_repair_smoke_test.sh
    central_cache_smoke_test.sh
    section_flow_smoke_test.sh
    central_resume_smoke_test.sh
    tcp_backfill_smoke_test.sh
    fault_recovery_smoke_test.sh
)

for test_script in "${tests[@]}"; do
    bash "$ROOT_DIR/scripts/$test_script"
done

bash "$ROOT_DIR/scripts/benchmark_smoke_test.sh"
SRCAST_SECTION_BLOCKS=17 bash "$ROOT_DIR/scripts/central_cache_smoke_test.sh"
bash "$ROOT_DIR/scripts/multi_proxy_smoke_test.sh"
echo "all SiteRepairCast smoke tests passed"
