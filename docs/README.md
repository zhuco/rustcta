# RustCTA Documentation Index

Status date: 2026-06-08

This directory is now organized around current-state architecture, active
operations, and the industrial workspace migration. The migration status file is
the best starting point when multiple AI agents are working in parallel.

## Start Here

- `industrial_workspace_migration_status.md` - current migration progress,
  verified checks, remaining work, and next batch scope.
- `exchange_gateway_next_40_parallel_tasks_zh.md` - 2026-06-08 close-out for
  the next 40 exchange gateway adapters and validation status.
- `industrial_workspace_update_2026-06-07_v0.3.9.md` - previous workspace
  cleanup note, version bump, validation result, and current directory
  structure.
- `industrial_cta_platform_architecture_assessment.md` - target industrial
  architecture and long-term migration direction.
- `industrial_directory_migration_plan.md` - app/tool/strategy/runtime ownership
  map for legacy `src/bin/*.rs` entrypoints.
- `industrial_migration_final_gates.md` - CI gates, compatibility retirement
  matrix, and local paper end-to-end checklist.
- `control_web_directory_migration_plan.md` - control API and Dioxus workspace
  migration boundaries.
- `backtest_app_migration_plan.md` - `apps/backtest` and `rustcta-backtest`
  migration plan.
- `tools_ops_migration_plan.md` - `tools/ops` command taxonomy and legacy binary
  migration matrix.

## Current Runtime Entrypoints

Industrial workspace apps:

```bash
cargo run -p rustcta-gateway --bin rustcta-gateway
cargo run -p rustcta-control-api-app --bin rustcta-control-api
cargo run -p rustcta-supervisor-app --bin rustcta-supervisor -- --serve --bind 127.0.0.1:18181
cargo run -p rustcta-backtest-app --bin rustcta-backtest -- --help
cargo run -p rustcta-industrial-cli --bin rustcta-industrial -- doctor
cargo run -p rustcta-industrial-cli --bin rustcta-industrial -- migration legacy-bin-plan --target tool-ops
cargo run -p rustcta-industrial-cli --bin rustcta-industrial -- supervisor validate-spec --path config/supervisor/trend_report.spec.json
cargo run -p rustcta-industrial-cli --bin rustcta-industrial -- ledger validate --path logs/events.jsonl
cargo run -p rustcta-tools-ops -- legacy-bin-plan
```

Legacy compatibility runtime:

```bash
cargo run -- --strategy spot_spot_taker_arbitrage --config config/spot_spot_taker_arbitrage.yml
cargo run --bin <legacy-bin> -- --help
```

Legacy root binaries must remain classified while migration continues:

```bash
cargo run -p rustcta-industrial-cli --bin rustcta-industrial -- migration verify-legacy-bins --src-bin-dir src/bin
cargo run -p rustcta-tools-ops -- verify-legacy-bins --src-bin-dir src/bin
```

## Architecture And Safety

- `architecture_module_layout.md` - legacy root module map and current
  functional paths.
- `exchange_abstraction.md` - exchange-facing contract summary.
- `exchange_adapter_interface_status.md` - current adapter layout after
  compatibility cleanup.
- `exchange_api_completion_matrix.md` - code-first adapter capability matrix.
- `exchange_support_matrix.md` - visual exchange support list, separated into
  centralized/custodial venues and decentralized/on-chain perpetual venues with
  CoinGecko/CoinGlass/official icon sources.
- `exchange_adapter_toolchain_completion_zh.md` - endpoint mapping, request
  spec, stream runtime, reconciliation, and adapter migration task split.
- `exchange_gateway_expansion_30_venues_zh.md` - three-batch plan for 30 new
  exchange gateway adapters and Binance-parity interface checklist.
- `exchange_gateway_next_40_parallel_tasks_zh.md` - completed 40-exchange
  gateway expansion split into 20 two-exchange AI task packets.
- `bitkan_adapter.md` - BitKan conservative gateway registration, capability
  boundaries, and OpenAPI upgrade gate.
- `blofin_adapter.md` - BloFin USDT perpetual gateway adapter endpoint mapping,
  signing, WS heartbeat, and unsupported Spot trading boundary.
- `api_key_security.md` - read-only key requirements and validation safety.
- `control_plane_security.md` - control-plane write and audit rules.
- `kill_switch.md` - kill-switch state and safety semantics.
- `live_preflight.md` - read-only readiness gate.
- `live_dry_run.md` - non-submitting live order plan path.
- `client_order_id_policy.md` - client order id generation and validation.
- `order_reconciliation.md` - REST reconciliation behavior.
- `fee_model.md` - fee source priority and fallback behavior.
- `symbol_management.md` - internal/exchange symbol mapping rules.
- `websocket_market_data.md` - shared websocket book-cache path.

## Strategies And Operations

- `multi_exchange_spot_arbitrage.md` - Spot-to-Spot arbitrage runtime overview.
- `spot_spot_inventory_rebalance_flow.md` - inventory rebalance rules.
- `spot_spot_inventory_rebalance_flow_zh.md` - Chinese version of the inventory
  rebalance flow.
- `hedged_dual_direction_grid.md` - multi-symbol hedged grid internals.
- `dioxus_control_panel.md` - current local control panel and Dioxus UI notes.

## Exchange References

- `spot_exchange_adapters.md` - Spot adapter architecture notes.
- `exchange_support_matrix.md` - gateway exchange support matrix with icons and
  centralized/decentralized grouping.
- `ascendex_adapter.md` - AscendEX Spot/Cash and futures gateway adapter reference.
- `bitget_adapter.md` - Bitget Spot adapter reference.
- `bitkan_adapter.md` - BitKan conservative gateway adapter reference.
- `gateio_adapter.md` - Gate.io Spot adapter reference.
- `hashkey_global_adapter.md` - HashKey Global Spot and futures gateway adapter reference.
- `hyperliquid_api.md` - Hyperliquid API and strategy integration notes.
- `mexc_adapter.md` - MEXC Spot adapter reference.
- `xt_adapter.md` - XT.com Spot and USDT-M gateway adapter reference.

## Cleanup Policy

Removed historical docs include old one-off runbooks, release notes,
remediation plans, disabled-symbol notes, old dashboard docs, old smart-money
candidate notes, and archived `docs/superpowers/*` plans/specs. They were
superseded by the active index above and by the migration status file.

Do not add new root-level runbooks without linking them here. Do not restore
deleted historical docs as active documentation unless the commands, package
names, safety assumptions, and workspace ownership are updated first.

## Exchange Mapping Validation

Endpoint mapping files live beside each gateway adapter as
`crates/rustcta-exchange-gateway/src/adapters/<exchange>/endpoint_mapping.yaml`.
They are validated by the shared schema in
`format_schemas/exchange_endpoint_mapping.schema.json`. The default command
validates the Binance and OKX baseline mappings for the shared toolchain task;
pass explicit paths to validate additional adapter mappings as they are migrated.

```bash
python3 scripts/validate_exchange_endpoint_mapping.py
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/binance/endpoint_mapping.yaml
```
