# RustCTA

RustCTA is a Rust trading system focused on multi-exchange market data,
execution safety, arbitrage research, and controlled live operation.

## Version

- Current version: `0.3.8`
- Release date: `2026-06-05`
- Previous version: `0.3.7`

## Current Status

Yes, the current system supports multi-exchange Spot-to-Spot arbitrage.
The production-facing path is `spot_spot_taker_arbitrage`, backed by unified
Spot adapters, symbol mapping, fee modeling, disabled-symbol controls,
paper/live-dry-run execution, replay, and monitoring outputs.

The older `cross_exchange_arbitrage` path is still USDT perpetual oriented.
Do not treat it as the Spot arbitrage runtime.

Live spot order submission remains gated by configuration:

- `dry_run`
- `live_trading_enabled`
- `trading_mode`
- `live_dry_run.submit_orders`
- live preflight checks
- disabled-symbol and unmanaged-inventory controls

## Repository Layout

```text
src/
  main.rs          main strategy CLI
  bin/             operational CLIs, observers, audits, servers
  core/            shared config, errors, types, requests, infrastructure
  data/            shared order-book events, cache, health, recorder, WS books
  market/          market contracts, symbols, precision, routing, funding
  exchanges/       unified spot clients, legacy core clients, gateways
  execution/       order commands, routing, fee model, reconciliation, dry run
  scanner/         read-only exchange coverage and spot scanner analytics
  control/         spot control plane, snapshots, audit, lifecycle, liquidation
  risk/            kill switch, disabled registry, hedge policy
  strategies/      active strategy implementations
  smart_money/     wallet analytics and portfolio services
  web/             monitoring/control HTTP models and routes
  utils/           logging, time sync, signing, notifications, order ids
config/            examples and runtime configuration
docs/              current architecture, operations, and reference notes
sql/               ClickHouse/Postgres schemas
scripts/           local automation and service helpers
tests/             unit, integration, fixture, and live-readonly tests
logs/              runtime logs and local validation output
```

## Active Strategy Entrypoints

Run the main binary with:

```bash
cargo run -- --strategy <name> --config <file>
```

Supported main CLI strategy names:

| Strategy | Module | Purpose |
| --- | --- | --- |
| `spot_spot_taker_arbitrage` | `strategies/spot_spot_taker_arbitrage` | Multi-exchange spot arbitrage |
| `cross_exchange_arbitrage` | `strategies/cross_exchange_arbitrage` | USDT perpetual cross-exchange runtime |
| `funding_rate_arbitrage` | `strategies/funding_rate_arbitrage` | Funding-rate workflow |
| `trend_intraday` | `strategies/trend` | Intraday trend |
| `trend_grid` | `strategies/trend_grid_v2` | Trend grid |
| `hedged_grid` | `strategies/hedged_grid` | Hedged grid |
| `solusdc_hedged_grid` | `strategies/solusdc_hedged_grid` | SOLUSDC hedged grid |
| `multi_hedged_grid` | `strategies/solusdc_hedged_grid::multi` | Multi-symbol hedged grid |
| `short_ladder_live` | `strategies/short_ladder_live` | Short-cycle ladder |
| `range_grid` | `strategies/range_grid` | Range grid |
| `mean_reversion` | `strategies/mean_reversion` | Mean reversion |
| `accumulation` | `strategies/accumulation` | Accumulation |
| `poisson` | `strategies/poisson_market_maker` | Poisson quote engine |
| `avellaneda_stoikov` | `strategies/avellaneda_stoikov` | A-S quote engine |
| `beta_hedge_market_maker` | `strategies/beta_hedge_market_maker` | Beta-hedged quoting |

Removed legacy strategy modules are no longer exported or launchable.

## Spot Arbitrage Components

Primary files:

- `src/strategies/spot_spot_taker_arbitrage/`
- `src/exchanges/unified.rs`
- `src/exchanges/{binance,okx,bitget,gateio,mexc,coinex,kucoin,paper}/`
- `src/execution/fee_model.rs`
- `src/execution/live_dry_run.rs`
- `src/execution/order_reconciliation.rs`
- `src/control/spot_control/`
- `src/scanner/five_exchange_spot.rs`
- `config/spot_spot_taker_arbitrage.yml`
- `config/five_exchange_spot_scanner.yml`
- `config/symbol_mappings.yml`
- `config/fees.yml`
- `config/disabled_symbols.yml`

Useful commands:

```bash
cargo run -- --strategy spot_spot_taker_arbitrage \
  --config config/spot_spot_taker_arbitrage.yml

cargo run -- --strategy spot_spot_taker_arbitrage \
  --config config/spot_spot_taker_arbitrage.yml \
  --preflight

cargo test five_exchange_spot_scanner
cargo test live_preflight_cli
```

## Exchange Layer

Spot adapters use the `ExchangeClient` contract in `src/exchanges/unified.rs`.
Perpetual execution uses `TradingAdapter` in `src/execution/adapter.rs` and
registered gateways in `src/exchanges/registry.rs`.

Current compatibility cleanup:

- The old `src/exchanges/adapters/` flat compatibility path is removed.
- Venue-specific files now live under `src/exchanges/<exchange>/`.
- `GatewayExchange` remains only as a bridge for code still expecting the
  legacy `core::exchange::Exchange` trait.
- New strategy work should use unified clients and execution adapters directly.

## Operational CLIs

Common binaries:

- `backtest`
- `cross_arb_observe`
- `cross_arb_preflight`
- `cross_arb_live`
- `cross_arb_server`
- `cross_arb_account_audit`
- `cross_arb_fee_audit`
- `cross_arb_order_admin`
- `funding_arb_observe`
- `funding_arb_live`
- `monitor_server`
- `account_position_reporter`
- `exchange_order_canary`
- `bitget_order_canary`
- `smart_money_monitor`
- `smart_money_portfolio_service`
- `url_manager`

Run with:

```bash
cargo run --bin <name> -- --help
```

## Development

Required checks before review:

```bash
cargo fmt
cargo check
cargo clippy --all-targets --all-features
cargo test --all-features
```

Optional release build:

```bash
cargo build --release
```

When touching configs, load them through the corresponding `serde_yaml` or
`serde_json` path and keep generated logs under `logs/`.

## Documentation

Current high-level docs:

- `docs/architecture_module_layout.md`
- `docs/multi_exchange_spot_arbitrage.md`
- `docs/exchange_abstraction.md`
- `docs/exchange_adapter_interface_status.md`
- `docs/spot_exchange_adapters.md`
- `docs/live_preflight.md`
- `docs/live_dry_run.md`
- `docs/fee_model.md`
- `docs/symbol_management.md`
- `docs/websocket_market_data.md`

## License

MIT License. See `LICENSE`.
