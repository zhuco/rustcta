# RustCTA Documentation Index

Status date: 2026-06-08

This directory is now organized around current-state architecture, active
operations, and the industrial workspace migration. The migration status file is
the best starting point when multiple AI agents are working in parallel.

## Start Here

- `industrial_workspace_migration_status.md` - current migration progress,
  verified checks, remaining work, and next batch scope.
- `交易所网关/README.md` - exchange gateway docs, adapter index, interface
  checklist, WebSocket market-data dimensions, and per-exchange references.
- `交易所网关/总览/exchange_gateway_next_40_parallel_tasks_zh.md` - 2026-06-08
  close-out for the next 40 exchange gateway adapters and validation status.
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
- `control_plane_security.md` - control-plane write and audit rules.
- `kill_switch.md` - kill-switch state and safety semantics.
- `live_preflight.md` - read-only readiness gate.
- `live_dry_run.md` - non-submitting live order plan path.

Exchange gateway architecture, adapter capability matrices, API key policy,
client order id policy, fee model, order reconciliation, symbol mapping, and
WebSocket market-data rules now live under `交易所网关/`.

## Strategies And Operations

- `multi_exchange_spot_arbitrage.md` - Spot-to-Spot arbitrage runtime overview.
- `spot_spot_inventory_rebalance_flow.md` - inventory rebalance rules.
- `spot_spot_inventory_rebalance_flow_zh.md` - Chinese version of the inventory
  rebalance flow.
- `hedged_dual_direction_grid.md` - multi-symbol hedged grid internals.
- `dioxus_control_panel.md` - current local control panel and Dioxus UI notes.

## Exchange References

- `交易所网关/README.md` - Chinese gateway documentation entrypoint.
- `交易所网关/接口盘点维度.md` - capability dimensions for filling exchange
  docs, including product-line support and WebSocket depth/speed requirements.
- `交易所网关/交易所接口补全文档模板.md` - per-exchange interface template.
- `交易所网关/适配器索引.md` - Chinese index for adapter file names.
- `交易所网关/适配器/` - one adapter document per exchange.

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
