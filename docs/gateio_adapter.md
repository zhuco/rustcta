# Gate.io Spot Adapter

`src/exchanges/gateio/mod.rs` implements `ExchangeClient` for Gate.io Spot.

## Scope

- REST public: spot symbols and order books.
- REST private: balances, place order, cancel order, query order, open orders, recent fills, fee rate.
- Public WebSocket: `spot.order_book` snapshots/updates normalized into `OrderBookSnapshot`.
- Safety default: `GateIoSpotConfig::default().dry_run == true`; dry-run `place_order` returns a synthetic order and does not submit.

## Symbol Projection

Gate.io exchange symbols use underscore pairs such as `BTC_USDT`. The adapter exposes compact internal symbols such as `BTCUSDT` through `SymbolRule.internal_symbol` and keeps the native pair in `exchange_symbol`.

Loaded symbol rules include base/quote asset, tick size, step size, min quantity, min notional, and trade status.

## Credentials

Private REST requires:

```bash
export GATEIO_API_KEY=...
export GATEIO_API_SECRET=...
```

`GATE_API_KEY` and `GATE_API_SECRET` are also accepted as fallbacks. Use read/trade-only keys for live dry-run; withdrawal permission must be disabled.

## Live-Dry-Run Use

For `spot_spot_taker_arbitrage`, configure `exchanges: [gateio, bitget]`, keep `dry_run: true`, `live_trading_enabled: false`, and `live_dry_run.submit_orders: false`.

The adapter supports order reconciliation primitives through `get_order`, `get_recent_fills`, and `get_open_orders`.
