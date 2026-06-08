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
- `industrial_directory_migration_plan.md` - historical app/tool/strategy/runtime
  ownership map for retired root binary entrypoints.
- `industrial_migration_final_gates.md` - CI gates, final root-source retirement
  checks, and local paper end-to-end checklist.
- `control_web_directory_migration_plan.md` - control API and Dioxus workspace
  migration boundaries.
- `tools_ops_migration_plan.md` - `tools/ops` command taxonomy and historical
  migration notes.

## Current Runtime Entrypoints

Industrial workspace apps:

```bash
cargo run -p rustcta-gateway --bin rustcta-gateway
cargo run -p rustcta-control-api-app --bin rustcta-control-api
cargo run -p rustcta-supervisor-app --bin rustcta-supervisor -- --serve --bind 127.0.0.1:18181
cargo run -p rustcta-industrial-cli --bin rustcta-industrial -- doctor
cargo run -p rustcta-industrial-cli --bin rustcta-industrial -- migration verify-retired-src
cargo run -p rustcta-industrial-cli --bin rustcta-industrial -- supervisor validate-spec --path config/supervisor/trend_report.spec.json
cargo run -p rustcta-industrial-cli --bin rustcta-industrial -- ledger validate --path logs/events.jsonl
cargo run -p rustcta-tools-ops -- verify-retired-src
```

Retired root-source checks:

```bash
cargo run -p rustcta-industrial-cli --bin rustcta-industrial -- migration verify-retired-src
cargo run -p rustcta-tools-ops -- verify-retired-src
```

The root package, root `src/` tree, and legacy root binaries are retired. Use
workspace apps, supervisor specs, strategy crates, and `tools/ops` commands for
runtime entrypoints.

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
- `交易所网关/adapter工作包索引.md` - per-adapter work package index
  combining current implementation, confirmed tasks, and remaining checks.
- `交易所网关/交易所网关补全任务清单.md` - prioritized implementation
  backlog derived from the matrix and official verification notes.
- `交易所网关/产品线官方核验_P1_链上合约交易所.md` - product-line
  verification for on-chain perpetual venues, Orderly profiles, and stopped
  venues.
- `交易所网关/产品线官方核验_P2_衍生品交易所.md` - product-line
  verification for derivatives venues where Spot may be unsupported or merely
  unimplemented in the project.
- `交易所网关/产品线官方核验_P3_CEX合约边界.md` - product-line
  verification for centralized exchanges where derivatives may be supported,
  unsupported, or split into a separate adapter.
- `交易所网关/产品线官方核验_P4_区域现货交易所.md` - product-line
  verification for regional spot exchanges, margin/leveraged boundaries, and
  standard contract unsupported markers.
- `交易所网关/产品线官方核验_P5_区域现货_CEX第二批.md` - product-line
  verification for the next regional spot CEX batch, including margin/perps
  project gaps and standard contract unsupported markers.
- `交易所网关/产品线官方核验_P6_剩余区域现货_CEX.md` - product-line
  verification for the remaining regional spot CEX batch, clearing the current
  product-line official-check queue into project gaps or unsupported markers.
- `交易所网关/WebSocket极速盘口能力汇总.md` - official low-latency order-book
  summary for 10ms/20ms L1/BBO, 50ms batch feeds, and dated rollout notes.
- `交易所网关/WebSocket官方核验_P2_区域现货交易所.md` - official
  WebSocket order-book details for regional spot exchanges, including push
  interval, depth, subscription style, and sequence/rebuild risks.
- `交易所网关/WebSocket官方核验_P3_P2公共WS缺口交易所.md` - official
  WebSocket verification for P2 public-WS gaps, including supported feeds,
  unsupported public-WS markers, push interval, depth, and rebuild risks.
- `交易所网关/WebSocket官方核验_P4_CEX盘口细项.md` - official WebSocket
  order-book details for CEX adapters with existing native/spec/payload
  evidence, including 10ms/20ms candidates and sequence/checksum risks.
- `交易所网关/WebSocket官方核验_P5_衍生品链上盘口细项.md` - official
  WebSocket order-book details for derivatives, on-chain perpetual, and
  Orderly-profile adapters, including 50ms/100ms/200ms candidates and rebuild
  risks.
- `交易所网关/WebSocket官方核验_P6_补充交易所盘口细项.md` - official
  WebSocket order-book details for supplemental CEX, broker, and hybrid
  profiles, including unsupported public-WS markers and sequence/rebuild risks.
- `交易所网关/剩余官方核验队列.md` - generated queue of official docs still
  needing verification before more adapter tasks can be declared.
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
