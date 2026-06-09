#!/usr/bin/env bash
set -euo pipefail

profile="${1:-quick}"
config_path="${CROSS_ARB_CONFIG:-config/cross_exchange_arbitrage_usdt.yml}"
timeout_ms="${CROSS_ARB_TIMEOUT_MS:-15000}"
ws_frames="${CROSS_ARB_WS_FRAMES:-80}"
ws_max_symbols="${CROSS_ARB_WS_MAX_SYMBOLS:-100}"
full_ws_frames="${CROSS_ARB_FULL_WS_FRAMES:-120}"
full_ws_max_symbols="${CROSS_ARB_FULL_WS_MAX_SYMBOLS:-0}"
private_ws_frames="${CROSS_ARB_PRIVATE_WS_FRAMES:-8}"
private_ws_exchange="${CROSS_ARB_PRIVATE_WS_EXCHANGE:-all}"

usage() {
  cat <<'USAGE'
Usage:
  scripts/cross_arb_local_gate.sh quick
  scripts/cross_arb_local_gate.sh full
  scripts/cross_arb_local_gate.sh network
  scripts/cross_arb_local_gate.sh ws-config
  scripts/cross_arb_local_gate.sh ws-config-full
  scripts/cross_arb_local_gate.sh private
  scripts/cross_arb_local_gate.sh private-ws
  scripts/cross_arb_local_gate.sh server-smoke

Environment:
  CROSS_ARB_CONFIG       config path, default config/cross_exchange_arbitrage_usdt.yml
  CROSS_ARB_TIMEOUT_MS  websocket timeout, default 15000
  CROSS_ARB_WS_FRAMES   frames per connection for sample WS probes, default 80
  CROSS_ARB_WS_MAX_SYMBOLS sample WS symbol cap, default 100
  CROSS_ARB_FULL_WS_FRAMES frames per connection for full WS probe, default 120
  CROSS_ARB_FULL_WS_MAX_SYMBOLS full WS symbol cap, default 0 for all symbols
  CROSS_ARB_PRIVATE_WS_EXCHANGE private WS exchange filter, default all
  CROSS_ARB_PRIVATE_WS_FRAMES private WS frames to sample, default 8
  CARGO_TARGET_DIR      optional persistent cargo target dir

Profiles:
  quick        local fast gate: fmt check, cargo check, lib tests
  full         local full gate before server handoff
  network      public websocket-only proxy and config sample checks
  ws-config    websocket-only config sample check
  ws-config-full websocket-only full config check
  private      local mock gateway smoke and API-key route tests; no live exchange calls
  private-ws   live private websocket login/subscribe probe; no orders
  server-smoke minimal server-side websocket-only smoke after pulling local-validated code
USAGE
}

run() {
  printf '\n==> %s\n' "$*"
  "$@"
}

run_ws_config_probe() {
  local max_symbols="$1"
  local frames="$2"
  local probe_args=(
    -p rustcta-tools-ops
    --
    probe ws-config
    --config "$config_path"
    --timeout-ms "$timeout_ms"
    --max-frames-per-connection "$frames"
  )
  if [[ "$max_symbols" != "0" ]]; then
    probe_args+=(--max-symbols "$max_symbols")
  fi
  run cargo run "${probe_args[@]}"
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
    run cargo run -p rustcta-tools-ops -- probe ws-proxy --exchange binance --region local --timeout-ms "$timeout_ms" --frames 1
    run cargo run -p rustcta-tools-ops -- probe ws-proxy --exchange bitget --region local --timeout-ms "$timeout_ms" --frames 1
    run cargo run -p rustcta-tools-ops -- probe ws-proxy --exchange gate --region local --timeout-ms "$timeout_ms" --frames 1
    run_ws_config_probe "$ws_max_symbols" "$ws_frames"
    ;;
  ws-config)
    run_ws_config_probe "$ws_max_symbols" "$ws_frames"
    ;;
  ws-config-full)
    run_ws_config_probe "$full_ws_max_symbols" "$full_ws_frames"
    ;;
  private)
    run cargo run -p rustcta-gateway --bin local-gateway-smoke
    run cargo test -p rustcta-control-api-app exchange_api_key_routes -- --nocapture
    run cargo test -p rustcta-control-api credentials -- --nocapture
    ;;
  private-ws)
    run cargo run -p rustcta-tools-ops -- probe private-ws --exchange "$private_ws_exchange" --timeout-ms "$timeout_ms" --frames "$private_ws_frames"
    ;;
  server-smoke)
    run cargo check -p rustcta-gateway --bin local-gateway-smoke -p rustcta-tools-ops
    run python3 -m py_compile scripts/public_connectivity_probe.py
    run_ws_config_probe "$ws_max_symbols" "$ws_frames"
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
