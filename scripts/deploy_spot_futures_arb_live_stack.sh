#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="$REPO_ROOT/logs/server"
mkdir -p "$LOG_DIR"

stamp="$(date +%Y%m%d_%H%M%S)"
log_file="$LOG_DIR/deploy_spot_futures_arb_live_stack_${stamp}.log"

cd "$REPO_ROOT"

set +e
{
  echo "started_at=$(date -Is)"
  echo "repo_root=$REPO_ROOT"
  echo "log_file=$log_file"
  echo "command=spot-futures deploy-live-stack"
  echo
  ssh -o BatchMode=yes -o ConnectTimeout=8 "${RUSTCTA_SSH_TARGET:-cta@45.77.253.180}" \
    "mkdir -p '${RUSTCTA_REMOTE_ROOT:-/home/cta/rustcta}/logs/spot_futures_arbitrage'"
  RUSTCTA_LIVE_SERVICE=spot-futures-arb-live \
  RUSTCTA_LIVE_BIN=spot-futures-arbitrage-runtime \
  RUSTCTA_LIVE_CONFIG=config/spot_futures_arbitrage_usdt.yml \
    scripts/rustcta_server.sh deploy-cross-arb-live-stack "$@"
  status=$?
  echo
  echo "finished_at=$(date -Is)"
  echo "exit_status=$status"
  exit "$status"
} 2>&1 | tee "$log_file"
status="${PIPESTATUS[0]}"
set -e

printf '\nDone. Exit status: %s\nLog: %s\n' "$status" "$log_file"
exit "$status"
