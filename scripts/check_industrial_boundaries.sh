#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

fail() {
  printf 'industrial boundary check failed: %s\n' "$1" >&2
  exit 1
}

require_file_contains() {
  local file="$1"
  local needle="$2"
  local reason="$3"

  if [[ ! -f "$file" ]] || ! grep -Fq -- "$needle" "$file"; then
    fail "$reason: $file must contain $needle"
  fi
}

file_contains_any() {
  local file="$1"
  shift

  [[ -f "$file" ]] || return 1
  for needle in "$@"; do
    if grep -Fq -- "$needle" "$file"; then
      return 0
    fi
  done
  return 1
}

require_file_contains_any() {
  local file="$1"
  local reason="$2"
  shift 2

  if ! file_contains_any "$file" "$@"; then
    fail "$reason: $file must contain one of: $*"
  fi
}

require_snapshot_route_evidence() {
  local route_name="$1"
  local handler_name="$2"
  local route_path="$3"
  local snapshot_key="$4"

  local route_evidence=(
    "crates/rustcta-control-api/src/models.rs|pub ${route_name}: JsonRowsView"
    "crates/rustcta-control-api/src/read_models.rs|snapshot.${route_name} = json_rows_from_legacy_snapshot(legacy, \"${snapshot_key}\")"
    "crates/rustcta-control-api/src/router.rs|.route(\"/api/${route_path}\", get(routes::${handler_name}))"
    "crates/rustcta-control-api/src/routes.rs|pub async fn ${handler_name}"
    "crates/rustcta-control-api/src/lib.rs|.uri(\"/api/${route_path}\")"
  )

  for entry in "${route_evidence[@]}"; do
    IFS='|' read -r file needle <<<"$entry"
    require_file_contains "$file" "$needle" "migrated control API ${snapshot_key} route evidence missing"
  done
}

require_snapshot_route_evidence_if_present() {
  local route_name="$1"
  local handler_name="$2"
  local route_path="$3"
  local snapshot_key="$4"

  local route_evidence=(
    "crates/rustcta-control-api/src/models.rs|pub ${route_name}: JsonRowsView"
    "crates/rustcta-control-api/src/read_models.rs|snapshot.${route_name} = json_rows_from_legacy_snapshot(legacy, \"${snapshot_key}\")"
    "crates/rustcta-control-api/src/router.rs|\"/api/${route_path}\""
    "crates/rustcta-control-api/src/router.rs|get(routes::${handler_name})"
    "crates/rustcta-control-api/src/routes.rs|pub async fn ${handler_name}"
    "crates/rustcta-control-api/src/lib.rs|.uri(\"/api/${route_path}\")"
  )

  local found_any=0
  for entry in "${route_evidence[@]}"; do
    IFS='|' read -r file needle <<<"$entry"
    if [[ -f "$file" ]] && grep -Fq -- "$needle" "$file"; then
      found_any=1
      break
    fi
  done

  if ((found_any)); then
    for entry in "${route_evidence[@]}"; do
      IFS='|' read -r file needle <<<"$entry"
      require_file_contains "$file" "$needle" "partial control API ${snapshot_key} route migration must include complete route evidence"
    done
  fi
}

require_opportunities_aggregate_evidence_if_present() {
  local found_any=0

  if file_contains_any crates/rustcta-control-api/src/models.rs \
    "pub opportunities: OpportunityAggregateView" \
    "pub opportunities: OpportunitiesView" \
    "pub opportunities: serde_json::Value"; then
    found_any=1
  elif file_contains_any crates/rustcta-control-api/src/read_models.rs \
    "snapshot.opportunities =" \
    "opportunities_from_legacy_snapshot"; then
    found_any=1
  elif file_contains_any crates/rustcta-control-api/src/router.rs \
    '"/api/opportunities"' \
    "get(routes::opportunities)"; then
    found_any=1
  elif file_contains_any crates/rustcta-control-api/src/routes.rs \
    "pub async fn opportunities" \
    "state.snapshot().await.opportunities"; then
    found_any=1
  elif file_contains_any crates/rustcta-control-api/src/lib.rs \
    '.uri("/api/opportunities")' \
    '"recent"' \
    '"arbitrage"' \
    '"statistics"'; then
    found_any=1
  fi

  if ! ((found_any)); then
    return 0
  fi

  require_file_contains_any \
    crates/rustcta-control-api/src/models.rs \
    "partial control API aggregate opportunities migration must define a public aggregate view field" \
    "pub opportunities: OpportunityAggregateView" \
    "pub opportunities: OpportunitiesView" \
    "pub opportunities: serde_json::Value"
  require_file_contains \
    crates/rustcta-control-api/src/read_models.rs \
    "snapshot.opportunities =" \
    "partial control API aggregate opportunities migration must populate the snapshot field"
  require_file_contains \
    crates/rustcta-control-api/src/read_models.rs \
    '"opportunities"' \
    "partial control API aggregate opportunities migration must read the legacy recent opportunities key"
  require_file_contains \
    crates/rustcta-control-api/src/read_models.rs \
    '"arbitrage_opportunities"' \
    "partial control API aggregate opportunities migration must read the legacy arbitrage opportunities key"
  require_file_contains \
    crates/rustcta-control-api/src/read_models.rs \
    '"arbitrage_statistics"' \
    "partial control API aggregate opportunities migration must read the legacy arbitrage statistics key"
  require_file_contains \
    crates/rustcta-control-api/src/router.rs \
    '"/api/opportunities"' \
    "partial control API aggregate opportunities migration must register the public route"
  require_file_contains \
    crates/rustcta-control-api/src/router.rs \
    "get(routes::opportunities)" \
    "partial control API aggregate opportunities migration must wire the aggregate handler"
  require_file_contains \
    crates/rustcta-control-api/src/routes.rs \
    "pub async fn opportunities" \
    "partial control API aggregate opportunities migration must define the aggregate handler"
  require_file_contains \
    crates/rustcta-control-api/src/routes.rs \
    "state.snapshot().await.opportunities" \
    "partial control API aggregate opportunities migration must serve the aggregate snapshot field"
  require_file_contains \
    crates/rustcta-control-api/src/lib.rs \
    '.uri("/api/opportunities")' \
    "partial control API aggregate opportunities migration must include route-level test coverage"
  require_file_contains \
    crates/rustcta-control-api/src/lib.rs \
    '"recent"' \
    "partial control API aggregate opportunities migration test must assert recent shape"
  require_file_contains \
    crates/rustcta-control-api/src/lib.rs \
    '"arbitrage"' \
    "partial control API aggregate opportunities migration test must assert arbitrage shape"
  require_file_contains \
    crates/rustcta-control-api/src/lib.rs \
    '"statistics"' \
    "partial control API aggregate opportunities migration test must assert statistics shape"
}

require_symbols_evidence_if_present() {
  local found_any=0

  if file_contains_any crates/rustcta-control-api/src/models.rs \
    "pub symbols:" \
    "SymbolsView"; then
    found_any=1
  elif file_contains_any crates/rustcta-control-api/src/read_models.rs \
    "snapshot.symbols =" \
    '"spot_symbol_rules"' \
    '"spot_control"' \
    '"five_exchange_scanner"' \
    '"/five_exchange_scanner/symbol_coverage"' \
    '"/five_exchange_scanner/recommendations"'; then
    found_any=1
  elif file_contains_any crates/rustcta-control-api/src/router.rs \
    '"/api/symbols"' \
    "get(routes::symbols)"; then
    found_any=1
  elif file_contains_any crates/rustcta-control-api/src/routes.rs \
    "pub async fn symbols" \
    "state.snapshot().await.symbols"; then
    found_any=1
  elif file_contains_any crates/rustcta-control-api/src/lib.rs \
    '.uri("/api/symbols")' \
    '"symbol_rules"' \
    '"spot_control"' \
    '"scanner"' \
    '"symbol_coverage"' \
    '"recommendations"'; then
    found_any=1
  fi

  if ! ((found_any)); then
    return 0
  fi

  require_file_contains_any \
    crates/rustcta-control-api/src/models.rs \
    "partial control API symbols migration must define a public symbols view field" \
    "pub symbols: SymbolsView" \
    "pub symbols:"
  require_file_contains \
    crates/rustcta-control-api/src/read_models.rs \
    "snapshot.symbols =" \
    "partial control API symbols migration must populate the snapshot field"
  require_file_contains \
    crates/rustcta-control-api/src/read_models.rs \
    '"spot_symbol_rules"' \
    "partial control API symbols migration must read the legacy symbol rules key"
  require_file_contains \
    crates/rustcta-control-api/src/read_models.rs \
    '"spot_control"' \
    "partial control API symbols migration must read the legacy spot control key"
  require_file_contains_any \
    crates/rustcta-control-api/src/read_models.rs \
    "partial control API symbols migration must read and expose scanner symbol coverage" \
    '"/five_exchange_scanner/symbol_coverage"' \
    '"symbol_coverage"'
  require_file_contains_any \
    crates/rustcta-control-api/src/read_models.rs \
    "partial control API symbols migration must read and expose scanner recommendations" \
    '"/five_exchange_scanner/recommendations"' \
    '"recommendations"'
  require_file_contains \
    crates/rustcta-control-api/src/router.rs \
    '"/api/symbols"' \
    "partial control API symbols migration must register the public route"
  require_file_contains \
    crates/rustcta-control-api/src/router.rs \
    "get(routes::symbols)" \
    "partial control API symbols migration must wire the symbols handler"
  require_file_contains \
    crates/rustcta-control-api/src/routes.rs \
    "pub async fn symbols" \
    "partial control API symbols migration must define the symbols handler"
  require_file_contains \
    crates/rustcta-control-api/src/routes.rs \
    "state.snapshot().await.symbols" \
    "partial control API symbols migration must serve the symbols snapshot field"
  require_file_contains \
    crates/rustcta-control-api/src/lib.rs \
    '.uri("/api/symbols")' \
    "partial control API symbols migration must include route-level test coverage"
  require_file_contains \
    crates/rustcta-control-api/src/lib.rs \
    '"symbol_rules"' \
    "partial control API symbols migration test must assert symbol_rules shape"
  require_file_contains \
    crates/rustcta-control-api/src/lib.rs \
    '"spot_control"' \
    "partial control API symbols migration test must assert spot_control shape"
  require_file_contains \
    crates/rustcta-control-api/src/lib.rs \
    '"scanner"' \
    "partial control API symbols migration test must assert scanner shape"
  require_file_contains \
    crates/rustcta-control-api/src/lib.rs \
    '"symbol_coverage"' \
    "partial control API symbols migration test must assert scanner symbol_coverage shape"
  require_file_contains \
    crates/rustcta-control-api/src/lib.rs \
    '"recommendations"' \
    "partial control API symbols migration test must assert scanner recommendations shape"
}

workflow_file=".github/workflows/non-gateway-industrial.yml"
if [[ ! -f "$workflow_file" ]]; then
  fail "missing focused non-gateway workflow: $workflow_file"
fi

full_migration_workflow_file=".github/workflows/industrial-migration-gates.yml"
if [[ ! -f "$full_migration_workflow_file" ]]; then
  fail "missing full industrial migration gate workflow: $full_migration_workflow_file"
fi

required_workflow_entries=(
  "apps/cli/tests/cli_contract_smoke.rs"
  "apps/supervisor/tests/cli_contract.rs"
  "tools/ops/tests/cli_smoke.rs"
  "crates/rustcta-control-api/src/lib.rs"
  "cargo test -p rustcta-control-api"
  "cargo check -p rustcta-control-api-app"
  "cargo test -p rustcta-supervisor-app"
  "cargo run -q -p rustcta-supervisor-app --bin rustcta-supervisor -- --validate-spec config/supervisor/spot_spot_live_dry_run.spec.json"
  "cargo test -p rustcta-tools-ops"
  "cargo check -p rustcta-industrial-cli"
  "cargo test -p rustcta-industrial-cli"
  "cargo run -q -p rustcta-tools-ops -- verify-legacy-bins --src-bin-dir src/bin"
  "cargo run -q -p rustcta-tools-ops -- probe ws-proxy --help"
  "cargo run -q -p rustcta-industrial-cli --bin rustcta-industrial -- supervisor validate-registry --path /tmp/rustcta-missing-registry.json"
  "cargo run -q -p rustcta-industrial-cli --bin rustcta-industrial -- supervisor readiness --spec-dir config/supervisor"
  "scripts/check_industrial_boundaries.sh"
)

for entry in "${required_workflow_entries[@]}"; do
  if ! grep -Fq -- "$entry" "$workflow_file"; then
    fail "focused non-gateway workflow is missing required entry: $entry"
  fi
done

required_full_migration_workflow_entries=(
  "cargo fmt --check"
  "cargo test --all-features"
  "cargo clippy --all-targets --all-features"
  "scripts/check_industrial_boundaries.sh"
  "cargo run -q -p rustcta-tools-ops -- verify-legacy-bins --src-bin-dir src/bin"
  "cargo run -q -p rustcta-tools-ops -- legacy-bin-plan"
  "cd web-ui/dioxus"
  "cargo build --release"
)

for entry in "${required_full_migration_workflow_entries[@]}"; do
  if ! grep -Fq -- "$entry" "$full_migration_workflow_file"; then
    fail "full industrial migration workflow is missing required entry: $entry"
  fi
done

required_supervisor_specs=(
  "config/supervisor/cross_arb_live.spec.json"
  "config/supervisor/funding_arb_live.spec.json"
  "config/supervisor/spot_spot_live_dry_run.spec.json"
  "config/supervisor/trend_report.spec.json"
  "config/supervisor/account_position_reporter.spec.json"
)

for spec in "${required_supervisor_specs[@]}"; do
  if [[ ! -f "$spec" ]]; then
    fail "missing checked-in supervisor spec: $spec"
  fi
  require_file_contains \
    apps/supervisor/tests/cli_contract.rs \
    "$(basename "$spec")" \
    "supervisor app contract test must validate checked-in spec: $spec"
  require_file_contains \
    "$workflow_file" \
    "--validate-spec $spec" \
    "focused non-gateway workflow must validate checked-in spec: $spec"
done

require_file_contains \
  apps/supervisor/tests/cli_contract.rs \
  "spot_spot_live_dry_run_config_should_remain_manual_until_spec_safety_is_tighter" \
  "supervisor app contract test must pin spot_spot live dry-run safety"
require_file_contains \
  apps/cli/tests/cli_contract_smoke.rs \
  "command_tree_help_should_cover_operator_surfaces" \
  "industrial CLI smoke must cover command tree help"
require_file_contains \
  apps/cli/tests/cli_contract_smoke.rs \
  '"migration"' \
  "industrial CLI smoke must include migration help"
require_file_contains \
  apps/cli/tests/cli_contract_smoke.rs \
  '"ledger"' \
  "industrial CLI smoke must include ledger help"
require_file_contains \
  apps/cli/tests/cli_contract_smoke.rs \
  '"supervisor"' \
  "industrial CLI smoke must include supervisor help"
require_file_contains \
  apps/cli/tests/cli_contract_smoke.rs \
  '"ops"' \
  "industrial CLI smoke must include ops help"
require_file_contains \
  apps/cli/tests/cli_contract_smoke.rs \
  "cross_arb_preflight_bridge_should_emit_offline_plan_without_network_paths" \
  "industrial CLI smoke must prove cross-arb preflight bridge stays offline"
require_file_contains \
  apps/cli/tests/cli_contract_smoke.rs \
  '"network_access"' \
  "industrial CLI preflight smoke must assert network boundary"
require_file_contains \
  apps/cli/tests/cli_contract_smoke.rs \
  '"live_order_access"' \
  "industrial CLI preflight smoke must assert live order boundary"
require_file_contains \
  apps/cli/src/main.rs \
  "network_access: \"disabled\"" \
  "industrial CLI preflight bridge must not run live network paths"
require_file_contains \
  apps/cli/src/main.rs \
  "live_order_access: \"disabled\"" \
  "industrial CLI preflight bridge must not run live order paths"
require_file_contains \
  apps/cli/src/main.rs \
  "legacy_cross_arb_preflight_args" \
  "industrial CLI preflight bridge must preserve legacy argument mapping"
require_file_contains \
  docs/industrial_cli_command_tree.md \
  "rustcta-industrial cross-arb preflight" \
  "industrial CLI command tree doc must describe the preflight bridge"
require_file_contains \
  docs/industrial_cli_command_tree.md \
  "network_access=disabled" \
  "industrial CLI command tree doc must document offline preflight boundary"
require_file_contains \
  docs/industrial_cli_command_tree.md \
  "migration verify-legacy-bins" \
  "industrial CLI command tree doc must document legacy bin classification checks"
require_file_contains \
  apps/cli/tests/cli_contract_smoke.rs \
  "supervisor_readiness_should_report_checked_in_specs_in_run_order" \
  "industrial CLI smoke must pin supervisor readiness output"
require_file_contains \
  apps/cli/tests/cli_contract_smoke.rs \
  '"readiness"' \
  "industrial CLI smoke must call supervisor readiness for checked-in specs"
require_file_contains \
  apps/cli/tests/cli_contract_smoke.rs \
  '"--spec-dir"' \
  "industrial CLI smoke must pass the checked-in spec dir flag"
require_file_contains \
  apps/cli/tests/cli_contract_smoke.rs \
  '"config/supervisor"' \
  "industrial CLI smoke must pass the checked-in spec dir"
require_file_contains \
  apps/cli/tests/cli_contract_smoke.rs \
  '"trend_report"' \
  "industrial CLI readiness smoke must include trend_report"
require_file_contains \
  apps/cli/tests/cli_contract_smoke.rs \
  '"account_position_reporter"' \
  "industrial CLI readiness smoke must include account_position_reporter"
require_file_contains \
  apps/cli/tests/cli_contract_smoke.rs \
  '"cross_arb_live"' \
  "industrial CLI readiness smoke must include cross_arb_live"
require_file_contains \
  apps/cli/tests/cli_contract_smoke.rs \
  '"funding_arb_live"' \
  "industrial CLI readiness smoke must include funding_arb_live"
require_file_contains \
  apps/cli/tests/cli_contract_smoke.rs \
  '"spot_spot_live_dry_run"' \
  "industrial CLI readiness smoke must pin checked-in spec order"
require_file_contains \
  apps/cli/tests/cli_contract_smoke.rs \
  '"config_exists"' \
  "industrial CLI readiness smoke must assert config_exists for each spec"
require_file_contains \
  apps/cli/tests/cli_contract_smoke.rs \
  '"operator_gated"' \
  "industrial CLI readiness smoke must assert operator_gated metadata"
require_file_contains \
  apps/cli/tests/cli_contract_smoke.rs \
  '"first_run"' \
  "industrial CLI readiness smoke must assert spot_spot is not the first run"
require_file_contains \
  apps/cli/tests/cli_contract_smoke.rs \
  "live_dry_run.submit_orders=false:true" \
  "industrial CLI readiness smoke must assert spot_spot submit_orders stays false"
require_file_contains \
  apps/cli/tests/cli_contract_smoke.rs \
  "inventory_rebalance.allow_lossy_rebalance_when_blocked=true:true" \
  "industrial CLI readiness smoke must expose spot_spot lossy rebalance risk"
require_file_contains \
  config/supervisor/spot_spot_live_dry_run.spec.json \
  '"config_path": "config/spot_spot_arbitrage_live_dry_run_2ex_5symbols.yml"' \
  "spot_spot checked-in supervisor spec must point at live dry-run config"
require_file_contains \
  config/supervisor/spot_spot_live_dry_run.spec.json \
  '"strategy_kind": "spot_spot_taker_arbitrage"' \
  "spot_spot checked-in supervisor spec must keep strategy kind"
require_file_contains \
  config/supervisor/spot_spot_live_dry_run.spec.json \
  '"log_path": "logs/supervisor/spot_spot_live_dry_run.log"' \
  "spot_spot checked-in supervisor spec must keep supervisor log path"

ownership_files=(
  crates/AGENTS.md
  apps/AGENTS.md
  strategies/AGENTS.md
  tools/AGENTS.md
)

for file in "${ownership_files[@]}"; do
  if [[ ! -f "$file" ]]; then
    fail "missing directory ownership file: $file"
  fi
done

strategy_manifests=()
for path in strategies/*/Cargo.toml; do
  if [[ -f "$path" ]]; then
    strategy_manifests+=("$path")
  fi
done

if ((${#strategy_manifests[@]})) \
  && rg -n 'rustcta-exchange-(api|gateway)|rustcta-execution-router|rustcta-control-api|rustcta-core-compat|^[[:space:]]*rustcta[[:space:]]*=' "${strategy_manifests[@]}"; then
  fail "strategy manifests must use the strategy SDK instead of gateway/router/control/compat or legacy root crates"
fi

if rg -n '^(use|extern crate)\s+rustcta_exchange_(api|gateway)|^(use|extern crate)\s+rustcta_execution_router|^(use|extern crate)\s+rustcta::|crate::exchanges::|src/exchanges/' strategies --glob '*.rs'; then
  fail "strategy crates must not depend on exchange APIs, concrete adapters, router internals, or the legacy root crate"
fi

if [[ "${RUSTCTA_STRICT_STRATEGY_MIGRATION:-0}" == "1" ]] \
  && rg -n 'wrapper_only[[:space:]]*"[[:space:]]*,[[:space:]]*json!\(true\)|Thin SDK wrapper for the legacy' strategies --glob '*.rs'; then
  fail "strict strategy migration check failed: wrapper-only strategy crates remain"
fi

app_manifests=()
non_gateway_app_manifests=()
for path in apps/*/Cargo.toml; do
  if [[ -f "$path" ]]; then
    app_manifests+=("$path")
    if [[ "$path" != "apps/gateway/Cargo.toml" ]]; then
      non_gateway_app_manifests+=("$path")
    fi
  fi
done

if ((${#app_manifests[@]})) \
  && rg -n 'rustcta-strategy-|path[[:space:]]*=[[:space:]]*".*strategies/' "${app_manifests[@]}"; then
  fail "app manifests must not depend directly on strategy crates"
fi

if ((${#non_gateway_app_manifests[@]})) \
  && rg -n 'rustcta-exchange-gateway' "${non_gateway_app_manifests[@]}"; then
  fail "only the gateway app may depend on the exchange gateway crate"
fi

industrial_manifests=()
for path in crates/*/Cargo.toml apps/*/Cargo.toml strategies/*/Cargo.toml tools/*/Cargo.toml; do
  if [[ -f "$path" ]]; then
    industrial_manifests+=("$path")
  fi
done

legacy_root_forbidden_manifests=()
for path in "${industrial_manifests[@]}"; do
  case "$path" in
    crates/rustcta-backtest/Cargo.toml)
      # Temporary backtest facade while src/backtest/* remains in the legacy
      # root package. Retire this bridge as implementation modules move into
      # crates/rustcta-backtest.
      ;;
    *)
      legacy_root_forbidden_manifests+=("$path")
      ;;
  esac
done

if ((${#legacy_root_forbidden_manifests[@]})) \
  && rg -n '^[[:space:]]*rustcta[[:space:]]*=|package[[:space:]]*=[[:space:]]*"rustcta"' "${legacy_root_forbidden_manifests[@]}"; then
  fail "industrial workspace crates/apps/strategies must not depend on the legacy root rustcta package"
fi

adapter_boundary_targets=()
for path in \
  apps \
  strategies \
  web-ui \
  crates/rustcta-control-api \
  crates/rustcta-core-compat \
  crates/rustcta-event-ledger \
  crates/rustcta-exchange-api \
  crates/rustcta-execution-api \
  crates/rustcta-execution-router \
  crates/rustcta-strategy-sdk \
  crates/rustcta-supervisor \
  crates/rustcta-types; do
  if [[ -e "$path" ]]; then
    adapter_boundary_targets+=("$path")
  fi
done

forbidden_adapter_paths='rustcta::exchanges::[[:space:]]*(trading_adapters|mexc|coinex|gateio|bitget|binance|kucoin|okx|paper)\b|rustcta::exchanges::[[:space:]]*\{[^}]*\b(trading_adapters|mexc|coinex|gateio|bitget|binance|kucoin|okx|paper)\b|crate::exchanges::|super::exchanges::|src/exchanges/|rustcta_exchange_gateway::[[:space:]]*adapters\b|rustcta_exchange_gateway::[[:space:]]*\{[^}]*\badapters\b'

if ((${#adapter_boundary_targets[@]})) \
  && rg -n "$forbidden_adapter_paths" "${adapter_boundary_targets[@]}" \
    --glob '*.rs' \
    --glob '*.toml' \
    --glob '*.ts' \
    --glob '*.tsx' \
    --glob '*.js' \
    --glob '*.jsx' \
    --glob '*.html' \
    --glob '!**/target/**' \
    --glob '!**/dist/**' \
    --glob '!**/Cargo.lock'; then
  fail "apps, strategies, platform crates, and web UI must not reference old concrete adapters or gateway private adapters"
fi

control_api_secret_patterns='api_key|api_secret|passphrase|authorization|ExchangeApiKey|exchange-api-keys|exchange_api_key_store|with_exchange_api_key_store|read_env_store|write_env_store'

if rg -n "$control_api_secret_patterns" crates/rustcta-control-api --glob '!**/src/lib.rs'; then
  fail "control API crate must not expose or persist raw exchange secrets"
fi

if sed -n '1,/^mod tests /p' crates/rustcta-control-api/src/lib.rs \
  | rg -n "$control_api_secret_patterns"; then
  fail "control API crate must not expose or persist raw exchange secrets"
fi

require_snapshot_route_evidence inventory inventory inventory inventory
require_snapshot_route_evidence books books books books
require_snapshot_route_evidence exchanges exchanges exchanges exchanges
require_snapshot_route_evidence recent_trades recent_trades trades/recent trades
require_snapshot_route_evidence_if_present recent_opportunities recent_opportunities opportunities/recent opportunities
require_opportunities_aggregate_evidence_if_present
require_symbols_evidence_if_present

raw_credential_promotion_targets=(
  apps/control-api
  .github/workflows/non-gateway-industrial.yml
)

raw_credential_route_patterns='exchange-api-keys|exchange_api_key_store|with_exchange_api_key_store|read_env_store|write_env_store|update_exchange_api_keys|delete_exchange_api_keys|apply_exchange_api_key_update|clear_exchange_api_keys'

for target in "${raw_credential_promotion_targets[@]}"; do
  if [[ -e "$target" ]] \
    && rg -n "$raw_credential_route_patterns" "$target" \
      --glob '*.rs' \
      --glob '*.toml' \
      --glob '*.yml' \
      --glob '*.yaml' \
      --glob '!**/target/**' \
      --glob '!**/dist/**'; then
    fail "raw exchange credential write/delete routes must stay frozen in legacy control_api.rs until a gateway/agent-owned credential service exists"
  fi
done

if rg -n 'std::env::var|dotenv' strategies crates/rustcta-strategy-sdk crates/rustcta-execution-api crates/rustcta-execution-router; then
  fail "strategies and execution API/router must not read environment secrets directly"
fi

if [[ -e tools ]]; then
  cargo run -q -p rustcta-tools-ops -- verify-legacy-bins --src-bin-dir src/bin
  cargo run -q -p rustcta-industrial-cli --bin rustcta-industrial -- \
    migration verify-legacy-bins --src-bin-dir src/bin

  legacy_bin_plan_json="$(mktemp)"
  cargo run -q -p rustcta-tools-ops -- legacy-bin-plan > "$legacy_bin_plan_json"
  if ! grep -Fq '"compatibility"' "$legacy_bin_plan_json" \
    || ! grep -Fq '"new_command"' "$legacy_bin_plan_json" \
    || ! grep -Fq '"retirement_milestone"' "$legacy_bin_plan_json" \
    || ! grep -Fq '"keep_wrapper"' "$legacy_bin_plan_json" \
    || ! grep -Fq '"warn"' "$legacy_bin_plan_json" \
    || ! grep -Fq '"alias"' "$legacy_bin_plan_json" \
    || ! grep -Fq '"remove_later"' "$legacy_bin_plan_json"; then
    rm -f "$legacy_bin_plan_json"
    fail "legacy bin migration plan must expose the compatibility retirement matrix"
  fi
  rm -f "$legacy_bin_plan_json"

  if rg -n 'src/exchanges/|crate::exchanges::|rustcta_exchange_gateway::[[:space:]]*adapters\b|rustcta_exchange_gateway::[[:space:]]*\{[^}]*\badapters\b' \
    tools \
    --glob '*.rs' \
    --glob '*.toml' \
    --glob '!**/target/**'; then
    fail "tools must not import concrete exchange adapter internals or gateway private adapters"
  fi

  if rg -n '^(use|extern crate)[[:space:]]+rustcta::|rustcta::' \
    tools \
    --glob '*.rs' \
    --glob '!**/target/**'; then
    fail "tools must not depend on the legacy root rustcta crate; extract a non-root helper crate first"
  fi

  if rg -n '^[[:space:]]*rustcta[[:space:]]*=|package[[:space:]]*=[[:space:]]*"rustcta"' \
    tools \
    --glob '*.toml' \
    --glob '!**/target/**'; then
    fail "tools manifests must not depend on the legacy root rustcta crate"
  fi

  if rg -n 'struct .*Runtime|async fn .*_task|tokio::spawn|axum::serve' \
    tools \
    --glob '*.rs' \
    --glob '!**/target/**'; then
    fail "tools must stay operator/analysis commands, not long-running runtime or service owners"
  fi
fi

printf 'industrial boundary checks passed\n'
