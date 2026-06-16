# RustCTA

RustCTA is a Rust workspace for multi-exchange market data, exchange gateway
adapters, execution safety, strategy runtimes, and a local Web control panel.

Current workspace version: `0.3.24`.

## Current Shape

The old root `src/` tree is retired. Active code lives in workspace crates:

```text
apps/        process entrypoints: gateway, supervisor, control-api, cli
crates/      reusable API, gateway, control-plane, supervisor, and runtime crates
strategies/  strategy runtime crates
tools/       operator and verification tools
web-ui/      Dioxus control panel
config/      runtime configs, account catalog, supervisor specs, systemd units
docs/        current architecture and operations documentation
scripts/     build, deploy, validation, and audit helpers
tests/       integration tests and exchange fixtures
```

Do not add new runbooks or code paths that reference retired `src/bin/*` or
`src/web/*` entrypoints.

## Normal Operations

Use one stack entrypoint for the production-like local/server flow:

```bash
scripts/rustcta_server.sh deploy-unified-arb-live-stack
```

That command builds and deploys:

- `unified-arbitrage-runtime`
- `rustcta-control-api`
- `rustcta-gateway`
- Dioxus static assets from `web-ui/dioxus/dist`
- systemd user units under `config/systemd/user/`

Useful operator commands:

```bash
scripts/rustcta_server.sh status unified-arb-live
scripts/rustcta_server.sh logs unified-arb-live 200
scripts/rustcta_server.sh status control-api
scripts/rustcta_server.sh logs control-api 200
scripts/rustcta_server.sh status rustcta-gateway
scripts/rustcta_server.sh logs rustcta-gateway 200
```

## Control Panel

The WebUI has a single backend entrypoint:

```bash
RUSTCTA_CONTROL_API_BIND=127.0.0.1:8091 \
RUSTCTA_CONTROL_API_AGENT_ID=local-agent \
RUSTCTA_CONTROL_API_TENANT_ID=local \
RUSTCTA_CONTROL_API_SUPERVISOR_REGISTRY_PATH=run/supervisor/registry.json \
RUSTCTA_CONTROL_API_AUDIT_LEDGER_PATH=data/control_api/audit.jsonl \
RUSTCTA_CONTROL_API_EXCHANGE_API_KEY_STORE=data/control_api/exchange_api_keys.env \
RUSTCTA_CONTROL_API_ACCOUNTS_CONFIG=config/accounts.yml \
RUSTCTA_CONTROL_API_STATIC_DIR=web-ui/dioxus/dist \
RUSTCTA_CONTROL_API_LEGACY_SNAPSHOT_PATH=logs/unified_arbitrage/dashboard.json \
cargo run -p rustcta-control-api-app --bin rustcta-control-api
```

Open:

```text
http://127.0.0.1:8091
```

The control panel is served by `rustcta-control-api`; do not use `dx serve`,
port `8080`, or a second Web service for normal operation.

## API Keys

Exchange API keys have one normal operator path:

```text
WebUI -> /api/exchange-api-keys -> data/control_api/exchange_api_keys.env
```

Rules:

- `config/accounts.yml` is the account catalog.
- `data/control_api/exchange_api_keys.env` is the single local env-store.
- `control-api`, `unified-arb-live`, and `rustcta-gateway` all point at that
  store.
- Web responses return configured/masked status only, never raw secrets.
- Restart gateway and strategy services after saving or deleting keys, because
  they read credentials from process environment.
- Do not keep a second live credential copy in per-service `.env` files.

## Strategy Runtime

The active multi-exchange arbitrage runtime is `unified_arbitrage`:

```bash
cargo run -p rustcta-strategy-unified-arbitrage --bin unified-arbitrage-runtime -- \
  --config config/unified_arbitrage_usdt.yml \
  --strategy-id unified_arb_live \
  --dashboard-snapshot-path logs/unified_arbitrage/dashboard.json \
  --command-queue data/control_api/unified_arb_control_commands.jsonl
```

The WebUI strategy console and local-agent routes use:

```text
/api/unified-arb/*
/api/local-agent/unified-arb/*
```

Legacy cross-arb, funding-arb, and spot-futures runtime entrypoints have been
removed from the active workspace.

## Gateway

Start a local gateway directly for debugging:

```bash
RUSTCTA_GATEWAY_BIND=127.0.0.1:18081 \
RUSTCTA_GATEWAY_ADAPTERS=binance,bitget,gateio \
cargo run -p rustcta-gateway --bin rustcta-gateway
```

Gateway credentials come from `data/control_api/exchange_api_keys.env` in
systemd operation. Adapter support and per-exchange API boundaries are
documented under `docs/交易所网关/`.

## Verification

Run focused checks before pushing:

```bash
cargo fmt --all --check
CARGO_TARGET_DIR=target/codex-workspace-check cargo check --workspace --message-format short
CARGO_TARGET_DIR=target/codex-control-api-test cargo test -p rustcta-control-api-app --lib --message-format short
CARGO_TARGET_DIR=target/codex-industrial-check scripts/check_industrial_boundaries.sh
```

Broader checks for release branches:

```bash
cargo clippy --workspace --all-targets --all-features
cargo test --workspace --all-features
cargo build --release
```

## Documentation

Start with:

- `docs/README.md`
- `docs/dioxus_control_panel.md`
- `docs/control_web_directory_migration_plan.md`
- `docs/supervisor_process_specs.md`
- `docs/交易所网关/README.md`

Historical docs that describe removed root binaries or retired strategy
entrypoints should not be restored as active runbooks.

## Safety

- Do not commit secrets.
- Control-plane writes must be audited and secret-free.
- Strategy snapshots and WebUI responses must not expose raw API keys,
  passphrases, authorization headers, or private keys.
- Live order submission remains gated by runtime config, live preflight,
  risk controls, and exchange adapter capability checks.

## License

MIT License. See `LICENSE`.
