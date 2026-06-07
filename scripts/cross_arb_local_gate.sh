#!/usr/bin/env bash
set -euo pipefail

profile="${1:-quick}"
config_path="${CROSS_ARB_CONFIG:-config/cross_exchange_arbitrage_usdt.yml}"
timeout_ms="${CROSS_ARB_TIMEOUT_MS:-5000}"

usage() {
  cat <<'USAGE'
Usage:
  scripts/cross_arb_local_gate.sh quick
  scripts/cross_arb_local_gate.sh full
  scripts/cross_arb_local_gate.sh network
  scripts/cross_arb_local_gate.sh private
  scripts/cross_arb_local_gate.sh server-smoke

Environment:
  CROSS_ARB_CONFIG       config path, default config/cross_exchange_arbitrage_usdt.yml
  CROSS_ARB_TIMEOUT_MS  network timeout, default 5000
  CARGO_TARGET_DIR      optional persistent cargo target dir

Profiles:
  quick        local fast gate: fmt check, cargo check, lib tests
  full         local full gate before server handoff
  network      public WS/REST and public preflight only
  private      private preflight; reads API env vars, does not submit orders
  server-smoke minimal server-side smoke after pulling local-validated code
USAGE
}

run() {
  printf '\n==> %s\n' "$*"
  "$@"
}

case "$profile" in
  quick)
    run cargo fmt --check
    run cargo check
    run cargo test --lib
    ;;
  full)
    run cargo fmt --check
    run cargo check
    run cargo test --lib
    run cargo test --all-features
    run python3 -m py_compile scripts/public_connectivity_probe.py
    ;;
  network)
    run python3 scripts/public_connectivity_probe.py --timeout-ms "$timeout_ms" --json
    run cargo run --bin cross_arb_preflight -- --config "$config_path" --timeout-ms "$timeout_ms"
    ;;
  private)
    run cargo run --bin cross_arb_preflight -- --config "$config_path" --timeout-ms "$timeout_ms" --private
    ;;
  server-smoke)
    run cargo check --bin cross_arb_preflight --bin cross_arb_observe --bin cross_arb_live
    run python3 -m py_compile scripts/public_connectivity_probe.py
    run cargo run --bin cross_arb_preflight -- --config "$config_path" --timeout-ms "$timeout_ms"
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
