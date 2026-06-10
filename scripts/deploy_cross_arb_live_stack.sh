#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="$REPO_ROOT/logs/server"
mkdir -p "$LOG_DIR"

if [[ -z "${RUSTCTA_DEPLOY_IN_TERMINAL:-}" && ! -t 1 ]]; then
  for terminal in konsole gnome-terminal xfce4-terminal mate-terminal xterm; do
    if command -v "$terminal" >/dev/null 2>&1; then
      case "$terminal" in
        konsole)
          exec "$terminal" --workdir "$REPO_ROOT" -e env RUSTCTA_DEPLOY_IN_TERMINAL=1 "$0" "$@"
          ;;
        gnome-terminal)
          exec "$terminal" --working-directory="$REPO_ROOT" -- env RUSTCTA_DEPLOY_IN_TERMINAL=1 "$0" "$@"
          ;;
        xfce4-terminal|mate-terminal)
          exec "$terminal" --working-directory="$REPO_ROOT" -e "env RUSTCTA_DEPLOY_IN_TERMINAL=1 '$0' '$*'"
          ;;
        xterm)
          exec "$terminal" -e env RUSTCTA_DEPLOY_IN_TERMINAL=1 "$0" "$@"
          ;;
      esac
    fi
  done
fi

stamp="$(date +%Y%m%d_%H%M%S)"
log_file="$LOG_DIR/deploy_cross_arb_live_stack_${stamp}.log"

cd "$REPO_ROOT"

set +e
{
  echo "started_at=$(date -Is)"
  echo "repo_root=$REPO_ROOT"
  echo "log_file=$log_file"
  echo "command=scripts/rustcta_server.sh deploy-cross-arb-live-stack"
  echo
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

if [[ -n "${RUSTCTA_DEPLOY_IN_TERMINAL:-}" || -t 0 ]]; then
  printf 'Press Enter to close...'
  read -r _ || true
fi

exit "$status"
