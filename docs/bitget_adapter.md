# Bitget Spot Adapter

`src/exchanges/bitget/mod.rs` implements `ExchangeClient` for Bitget Spot v2.

## Scope

- REST public: spot symbols and order books.
- REST private: balances, place order, cancel order, query order, open orders, recent fills, fee rate.
- Public WebSocket: `books5` SPOT books normalized into `OrderBookSnapshot`.
- Safety default: `BitgetSpotConfig::default().dry_run == true`; dry-run `place_order` returns a synthetic order and does not submit.

## Symbol Projection

Bitget spot symbols are compact, for example `BTCUSDT`. The adapter stores the same value as both internal and exchange symbol unless Bitget metadata says otherwise.

Loaded symbol rules include base/quote asset, tick size, step size, min quantity, min notional, max quantity, and trade status.

## Credentials

Private REST requires:

```bash
export BITGET_API_KEY=...
export BITGET_API_SECRET=...
export BITGET_API_PASSPHRASE=...
```

`BITGET_PASSPHRASE` is accepted as a fallback. Use read/trade-only keys for live dry-run; withdrawal permission must be disabled.

## Live-Dry-Run Use

For `spot_spot_taker_arbitrage`, configure `exchanges: [gateio, bitget]`, keep `dry_run: true`, `live_trading_enabled: false`, and `live_dry_run.submit_orders: false`.

The adapter supports order reconciliation primitives through `get_order`, `get_recent_fills`, and `get_open_orders`.
