# Live Preflight

Live preflight is a read-only readiness gate for future small-capital taker/taker testing. It reports whether RustCTA appears ready for a controlled dry-run or small-live exercise, but it never enables live trading and never places orders.

## What It Checks

The preflight report covers:

- Config safety: `trading_mode`, `dry_run`, `live_trading_enabled`, explicit exchanges/symbols, and small notional limits.
- API credentials: key/secret presence, read permission, trade permission if explicitly required, withdraw permission absence when introspection is available.
- Exchange connectivity: dashboard/health read model connectivity.
- Symbol rules: tradability, market type, tick size, step size, min quantity, min notional, order type, time-in-force, and client order ID policy.
- Balances: quote balance availability and unmanaged-position exclusion.
- Fee model: maker/taker fee availability and source.
- WebSocket books: cached book presence, freshness, best bid/ask, spread sanity, latency, and health counters.
- Disabled/unmanaged registry: disabled symbols, disabled exchanges, disabled exchange-symbols, and unmanaged overlap.
- Risk limits: trade/total notional, daily loss, stale/latency thresholds, emergency stop, and kill-switch state.
- Recorder and monitoring: dashboard enabled when required and recorders healthy.

Unknown exchange permission state is not treated as pass. If an exchange cannot expose permission introspection, the check returns `Unknown` or `Warn`.

## Config

```yaml
live_preflight:
  enabled: true
  target_mode: small_live_taker_taker
  exchanges: [mexc, coinex]
  market_type: Spot
  symbols: [CUDISUSDT, BTCUSDT]
  max_live_notional_per_trade: 5
  max_total_live_notional: 20
  require_monitoring_enabled: true
  require_recorder_enabled: true
  require_websocket_fresh: true
  max_book_age_ms: 1000
  require_fee_model: true
  require_disabled_registry: true
  require_kill_switch: true
  require_balances: true
  require_symbol_rules: true
  require_order_validation: true
  require_private_stream: false
  allow_rest_order_polling_fallback: true
  require_api_key_read_permission: true
  require_api_key_trade_permission: false
  require_withdraw_permission_absent: true
  minimum_quote_balance_usdt: 10
  minimum_base_inventory_usdt: 0
  fail_on_unmanaged_position_overlap: true
```

`require_api_key_trade_permission` is configurable but defaults to `false`. Withdraw permission is considered dangerous; when detected as enabled it fails the gate.

## CLI

Run:

```bash
cargo run -- --preflight --config config/spot_spot_taker_arbitrage.yml
```

The CLI prints a human-readable report and exits:

- `0` if there are no fail/critical checks.
- `1` if any fail/critical check exists.

The CLI path is conservative and read-only. It loads config, fee config, disabled registry, and local initial balances. It does not place orders and does not require network in tests. Runtime-only checks such as fresh WebSocket books and loaded exchange symbol rules will block unless disabled or supplied by the running strategy/dashboard path.

Example:

```text
LIVE PREFLIGHT REPORT
Decision: Blocked

Critical failures:
- live_trading_enabled must remain false in this stage
- MEXC CUDISUSDT book is stale: age 4300ms

Warnings:
- withdraw permission introspection is not available

Ready:
- client_order_id_policy
- fee_model_available
```

## Dashboard

When monitoring is enabled, these endpoints expose the latest report:

- `GET /api/live_preflight`
- `GET /api/live_preflight/checks`
- `GET /api/live_preflight/summary`

`GET /api/status` also includes:

- `live_preflight_enabled`
- `live_readiness_decision`
- `live_preflight_last_run_at`
- `live_preflight_fail_count`
- `live_preflight_warn_count`

## Decisions

- `ReadyForPaperOnly`: suitable only for paper/observation.
- `ReadyForLiveDryRun`: no critical blockers, but warnings or unknowns remain, or trade permission is not required.
- `ReadyForSmallLive`: all required checks pass and trade permission was explicitly required and satisfied.
- `Blocked`: at least one fail/critical check exists.

The decision never changes config and never enables live trading.

## Preparing for 5-10 USDT Testing

Before any small-capital live taker/taker test, clear all `Blocked` items, review every warning, keep withdraw permission disabled, verify fresh WebSocket books, confirm symbol rules and local order validation, and run the dashboard preflight while the strategy is operating in paper mode. Move next to live dry-run only after the preflight report is clean and operator controls are documented.
