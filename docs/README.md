# RustCTA Documentation Index

Status date: 2026-06-16

This directory documents the current workspace. Historical migration notes that
describe removed root binaries or retired strategy entrypoints are not active
runbooks.

## Start Here

- `dioxus_control_panel.md` - WebUI and `rustcta-control-api` entrypoint.
- `control_web_directory_migration_plan.md` - current control API/Web boundary.
- `supervisor_process_specs.md` - supervisor spec conventions and active specs.
- `unified_arbitrage_strategy_plan_zh.md` - unified arbitrage strategy design.
- `multi_exchange_spot_arbitrage.md` - spot-to-spot arbitrage runtime notes.
- `control_plane_security.md` - write, audit, and secret-free control rules.
- `交易所网关/README.md` - exchange gateway documentation index.

## Active Entrypoints

```bash
scripts/rustcta_server.sh deploy-unified-arb-live-stack
scripts/rustcta_server.sh deploy-control-panel
scripts/rustcta_server.sh deploy-systemd-units
```

Direct local debugging:

```bash
cargo run -p rustcta-control-api-app --bin rustcta-control-api
cargo run -p rustcta-gateway --bin rustcta-gateway
cargo run -p rustcta-supervisor-app --bin rustcta-supervisor -- --serve --bind 127.0.0.1:18181
cargo run -p rustcta-industrial-cli --bin rustcta-industrial -- doctor
cargo run -p rustcta-tools-ops -- verify-retired-src
```

The active Web control panel is always served by `rustcta-control-api` on
`127.0.0.1:8091` for normal operation.

## Unified Runtime

The active arbitrage runtime is:

```text
strategy kind: unified_arbitrage
strategy id:   unified_arb_live
binary:        unified-arbitrage-runtime
config:        config/unified_arbitrage_usdt.yml
snapshot:      logs/unified_arbitrage/dashboard.json
command queue: data/control_api/unified_arb_control_commands.jsonl
```

Control routes:

```text
/api/unified-arb/*
/api/local-agent/unified-arb/*
```

Do not create new docs or UI paths for retired cross-arb, funding-arb, or
spot-futures strategy services.

## Credential Docs

API keys are managed through the WebUI exchange configuration page:

```text
/api/exchange-api-keys
data/control_api/exchange_api_keys.env
config/accounts.yml
```

`rustcta-gateway` and `unified-arb-live` read the same env-store through systemd
`EnvironmentFile` entries. Browser/API responses expose only masked status.

## Safety And Architecture

- `architecture_module_layout.md`
- `control_plane_security.md`
- `kill_switch.md`
- `live_preflight.md`
- `live_dry_run.md`
- `spot_spot_inventory_rebalance_flow.md`
- `spot_spot_inventory_rebalance_flow_zh.md`
- `hedged_dual_direction_grid.md`

## Exchange Gateway

Gateway docs live under `交易所网关/`:

- `交易所网关/README.md`
- `交易所网关/总览/exchange_adapter_interface_status.md`
- `交易所网关/总览/exchange_api_completion_matrix.md`
- `交易所网关/总览/exchange_support_matrix.md`
- `交易所网关/通用机制/api_key_security.md`
- `交易所网关/通用机制/market_type_contract.md`
- `交易所网关/适配器/`

Validate endpoint mapping files with:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py
python3 scripts/generate_exchange_gateway_matrix.py
```

## Verification

```bash
cargo fmt --all --check
CARGO_TARGET_DIR=target/codex-workspace-check cargo check --workspace --message-format short
CARGO_TARGET_DIR=target/codex-control-api-test cargo test -p rustcta-control-api-app --lib --message-format short
CARGO_TARGET_DIR=target/codex-industrial-check scripts/check_industrial_boundaries.sh
```

Do not restore deleted historical docs as current documentation unless their
commands, paths, package names, and safety assumptions match this workspace.
