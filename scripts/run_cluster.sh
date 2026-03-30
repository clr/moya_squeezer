#!/bin/zsh

set -euo pipefail

echo "[deprecated] cluster deployment moved to /Users/clr/moya_harness"
exec /Users/clr/moya_harness/scripts/run_cluster.sh "$@"
