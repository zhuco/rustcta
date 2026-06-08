# RustCTA

RustCTA is a Rust trading system for multi-exchange market data, execution
safety, arbitrage research, and controlled live operation.

## Current Status

The repository is in the `industrial-workspace-migration` program. The target
shape is a Cargo workspace with process-oriented app crates, reusable platform
crates, strategy crates, and strategy/runtime entrypoints outside the retired
root package.

Use these documents as the current source of truth:

- `docs/README.md` for the active documentation index
- `docs/industrial_workspace_migration_status.md` for migration progress
- `docs/交易所网关/总览/exchange_gateway_next_40_parallel_tasks_zh.md` for the 2026-06-08
  exchange gateway expansion close-out
- `docs/industrial_workspace_update_2026-06-07_v0.3.9.md` for the previous
  workspace cleanup/update note
- `docs/industrial_cta_platform_architecture_assessment.md` for the target
  architecture

The legacy root package `rustcta` and root `src/` tree are retired. New work
must stay inside the workspace boundaries below.

Current workspace version: `0.3.14`. This update expands the exchange gateway
surface with additional adapter modules, endpoint mapping fixtures, signing and
request-spec validation assets, and refreshed gateway example configs. See
`docs/README.md`, `docs/交易所网关/总览/exchange_adapter_toolchain_completion_zh.md`, and
`docs/交易所网关/总览/exchange_api_completion_matrix.md` for the active adapter documentation
and validation commands.

## Exchange Gateway Support

The local gateway now registers 77 real exchange adapters plus the local `paper`
adapter. Supported adapter ids:

`apex`, `ascendex`, `aster`, `backpack`, `biconomy`, `bigone`, `binance`,
`binancecoinm`, `bingx`, `bitbank`, `bitfinex`, `bitflyer`, `bitget`,
`bithumb`, `bitkan`, `bitmart`, `bitmex`, `bitrue`, `bitso`, `bitstamp`,
`bitunix`, `bitvavo`, `blofin`, `btcmarkets`, `btcturk`, `bullish`, `bybit`,
`coinbase`, `coinbaseexchange`, `coincheck`, `coindcx`, `coinex`, `coinone`,
`coinsph`, `coinspot`, `coinstore`, `cointr`, `coinw`, `cryptocom`,
`deepcoin`, `delta`, `deribit`, `derive`, `digifinex`, `dydx`, `gateio`,
`gemini`, `grvt`, `hashkey_global`, `htx`, `huobi`, `hyperliquid`,
`independentreserve`, `indodax`, `kraken`, `krakenfutures`, `kucoin`,
`kucoinfutures`, `lbank`, `lighter`, `luno`, `mercado`, `mexc`, `okx`,
`orangex`, `oxfun`, `pacifica`, `paradex`, `phemex`, `poloniex`, `tapbit`,
`toobit`, `upbit`, `weex`, `whitebit`, `woo`, and `xt`.

The visual support matrix lives in `docs/交易所网关/总览/exchange_support_matrix.md`. It splits
venues into centralized/custodial exchanges and decentralized or on-chain
perpetual venues, with remote icons sourced first from CoinGecko exchange
images and then from CoinGlass or official-domain fallbacks when CoinGecko does
not list a venue.

| Function surface | Current support |
| --- | --- |
| Public REST | Symbol rules and order book snapshots where each venue exposes stable public endpoints. |
| Private REST read | Balances, fees, open orders, query order, and recent fills where credential scopes and official APIs allow it. |
| Private REST write | Place order, quote market order, cancel order, cancel-all, batch place/cancel, and order-list only where the venue exposes compatible native semantics; otherwise explicitly `Unsupported`. |
| WebSocket | Public/private subscribe payloads, auth payloads, heartbeat policy, parser fixtures, push interval, order book depth levels, subscription modes, sequence/checksum, and REST reconciliation fallback per venue. |
| Derivatives | Futures, perpetuals, options, funding, mark/open-interest, leverage/margin/position mode where the adapter and official API support lossless mapping. |
| Safety boundary | Wallet, payment rails, fiat deposit/withdrawal, transfers, tax reports, and non-trading account operations remain outside gateway runtime unless a venue document explicitly narrows that boundary. |

Per-exchange details live in `docs/交易所网关/适配器/<adapter>_adapter.md` and
`crates/rustcta-exchange-gateway/src/adapters/<adapter>/endpoint_mapping.yaml`.

## Repository Layout

```text
apps/        process entrypoints: gateway, supervisor, control-api, cli
crates/      reusable platform crates and API contracts
strategies/  strategy wrapper crates and migrating strategy cores
tools/       operator tools and migration inventory
web-ui/      Dioxus control panel
src/         legacy root crate, compatibility runtime, strategy implementation
config/      active runtime configuration examples and supervisor specs
docs/        active architecture, operations, migration, and reference docs
scripts/     local automation and validation helpers
tests/       unit, integration, fixture, and live-readonly tests
logs/        local runtime logs and validation output
```

## Primary Entrypoints

### Industrial Workspace Apps

Gateway:

```bash
RUSTCTA_GATEWAY_BIND=127.0.0.1:18081 \
RUSTCTA_GATEWAY_ADAPTERS=paper \
cargo run -p rustcta-gateway --bin rustcta-gateway
```

Control API:

```bash
RUSTCTA_CONTROL_API_BIND=127.0.0.1:8091 \
RUSTCTA_CONTROL_API_AGENT_ID=local-agent \
RUSTCTA_CONTROL_API_TENANT_ID=local \
RUSTCTA_CONTROL_API_SUPERVISOR_REGISTRY_PATH=run/supervisor/registry.json \
cargo run -p rustcta-control-api-app --bin rustcta-control-api
```

Supervisor read-only service:

```bash
cargo run -p rustcta-supervisor-app --bin rustcta-supervisor -- \
  --serve \
  --bind 127.0.0.1:18181 \
  --registry-path run/supervisor/registry.json
```

Industrial CLI scaffold:

```bash
cargo run -p rustcta-industrial-cli --bin rustcta-industrial -- doctor
cargo run -p rustcta-industrial-cli --bin rustcta-industrial -- migration verify-retired-src
cargo run -p rustcta-industrial-cli --bin rustcta-industrial -- ledger validate --path logs/events.jsonl
cargo run -p rustcta-industrial-cli --bin rustcta-industrial -- ledger summary --path logs/events.jsonl --from-sequence 1
cargo run -p rustcta-industrial-cli --bin rustcta-industrial -- cross-arb preflight --help
```

Operator tools:

```bash
cargo run -p rustcta-tools-ops -- verify-retired-src
cargo run -p rustcta-tools-ops -- symbols gateio-bitget-spot --help
cargo run -p rustcta-tools-ops -- ws-proxy-probe --help
```

### Retired Legacy Entrypoints

The root package, root `src/` tree, and legacy root binaries are retired. Use
workspace apps, strategy crates, supervisor specs, and `tools/ops` commands
instead of root-package invocations.

```bash
cargo run -p rustcta-industrial-cli --bin rustcta-industrial -- migration verify-retired-src
cargo run -p rustcta-tools-ops -- verify-retired-src
```

Adding new business implementation under root `src/` is blocked by the
industrial boundary checks.

## Control Panel

For the current legacy local dry-run console, use:

```bash
export RUSTCTA_MONITOR_TOKEN="$(openssl rand -hex 32)"
scripts/separated_control_panel.sh build
scripts/separated_control_panel.sh start
```

Open `http://127.0.0.1:8091` and paste the monitor token into the UI. This
script still launches the legacy local control API path by default.

The migrating workspace control API entrypoint is:

```bash
cargo run -p rustcta-control-api-app --bin rustcta-control-api
```

See `docs/dioxus_control_panel.md` and
`docs/control_web_directory_migration_plan.md` before changing control-plane
routes, auth, or static hosting.

## Build And Validation

During parallel migration, prefer focused checks first:

```bash
cargo check -p rustcta
cargo check --workspace
scripts/check_industrial_boundaries.sh
```

Current focused migration checks are documented in
`docs/industrial_workspace_migration_status.md`.

Before review, run the broad checks when the parallel workstreams have settled:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features
cargo test --workspace --all-features
```

Optional release build:

```bash
cargo build --release
```

## Active Strategy Runtime Notes

The production-facing Spot-to-Spot arbitrage path is
`spot_spot_taker_arbitrage`. It is backed by unified Spot adapters, symbol
mapping, fee modeling, disabled-symbol controls, paper/live-dry-run execution,
replay, and monitoring outputs.

The older `cross_exchange_arbitrage` path is USDT perpetual oriented. Do not
treat it as the Spot arbitrage runtime.

Live spot order submission remains gated by configuration:

- `dry_run`
- `live_trading_enabled`
- `trading_mode`
- `live_dry_run.submit_orders`
- live preflight checks
- disabled-symbol and unmanaged-inventory controls

## Documentation

Start with `docs/README.md`. Historical migration/remediation documents that
were superseded during this workspace migration have been removed from the
active docs set; do not restore them as runbooks unless they are updated to the
current workspace entrypoints.

## License

MIT License. See `LICENSE`.
