#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

for week in 2 3 4 5 6 7; do
    bash "$ROOT_DIR/scripts/week${week}_smoke_test.sh"
done

bash "$ROOT_DIR/scripts/benchmark_smoke_test.sh"
SRCAST_SECTION_BLOCKS=17 bash "$ROOT_DIR/scripts/central_cache_smoke_test.sh"
bash "$ROOT_DIR/scripts/multi_proxy_smoke_test.sh"
echo "all SiteRepairCast smoke tests passed"
