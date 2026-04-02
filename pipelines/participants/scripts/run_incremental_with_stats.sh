#!/usr/bin/env bash

set -euo pipefail

PIPE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$PIPE_ROOT/../.." && pwd)"
cd "$REPO_ROOT"

printf "[info] run_incremental_with_stats.sh is now a thin wrapper around run_all.sh; run_all.sh already prints before/after statistics.\n"
MODE="${MODE:-api}" API_MAX="${API_MAX:-1000}" bash "$REPO_ROOT/pipelines/participants/scripts/run_all.sh"
