# Multi-Exchange Spot Arbitrage

## Support Answer

The current system supports multi-exchange Spot-to-Spot arbitrage through
`spot_spot_taker_arbitrage`.

Supported functions:

- scan multiple Spot venues
- normalize exchange symbols to canonical symbols
- compare directed buy/sell opportunities
- include configured and exchange-reported fee rates
- reject stale or insufficient-depth books
- apply disabled-symbol and lifecycle controls
- run paper execution
- run live-dry-run planning
- record opportunities and replay/report outputs
- publish monitoring read models when enabled

Not supported as default behavior:

- ungated live Spot order submission
- automatic unmanaged-inventory liquidation without control-plane workflow
- treating the USDT perpetual `cross_exchange_arbitrage` runtime as Spot
  arbitrage

## Main Runtime

```text
retired strategy tree/spot_spot_taker_arbitrage/
  config.rs
  market_data.rs
  websocket_market_data.rs
  spread_engine.rs
  opportunity.rs
  risk.rs
  inventory.rs
  paper_execution.rs
  recorder.rs
  replay.rs
  report.rs
```

Primary config:

```text
config/spot_spot_taker_arbitrage.yml
config/spot_spot_taker_arbitrage_gateio_bitget.live-dry-run.example.yml
config/five_exchange_spot_scanner.yml
config/spot_exchanges_example.yml
config/symbol_mappings.yml
config/fees.yml
config/disabled_symbols.yml
```

## Execution Modes

| Mode | Behavior |
| --- | --- |
| Replay | Reads recorded books and produces deterministic reports |
| Scan/observe | Computes opportunities without order mutation |
| Paper | Simulates fills against order-book snapshots |
| Live dry run | Builds live-shaped order plans while keeping submission disabled unless explicitly configured |
| Live | Reserved for explicitly gated future use after preflight and reconciliation pass |

## Safety Gates

Executable paths must pass:

- `dry_run` and `live_trading_enabled` checks
- `trading_mode` checks
- live preflight
- fee model loading
- symbol mapping validation
- disabled-symbol registry checks
- control-plane lifecycle checks
- kill-switch checks
- book freshness and depth checks
- inventory ownership and reservation checks
- reconciliation checks for order/fill state

## Validation Commands

```bash
cargo check
cargo test five_exchange_spot_scanner
cargo test live_preflight_cli
cargo test live_runtime_publisher
cargo test live_websocket_books
```

For a full release validation:

```bash
cargo fmt
cargo clippy --all-targets --all-features
cargo test --all-features
```

## Operational Notes

- Use scan-only mode for a new venue until symbols, fees, and book health are
  stable.
- Mark unsupported live behavior explicitly instead of routing around missing
  adapter capabilities.
- Keep per-exchange API keys in environment variables or `.env`; never commit
  secrets.
- Store ad-hoc validation output under `logs/`.
