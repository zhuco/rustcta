# Exchange Adapter Interface Status

This note records the current adapter layout after compatibility cleanup.

## Summary

Multi-exchange Spot arbitrage is supported through unified Spot clients and the
`spot_spot_taker_arbitrage` runtime. USDT perpetual arbitrage remains on the
execution gateway path used by `cross_exchange_arbitrage`.

The old `retired exchange tree/adapters/` compatibility directory has been removed.

## Current Adapter Layout

| Area | Path | Role |
| --- | --- | --- |
| Unified client contracts | `retired exchange tree/unified.rs` | Spot/Perpetual client model, requests, responses, user stream events |
| Venue modules | `retired exchange tree/<exchange>/` | Exchange-specific Spot or legacy core clients |
| Market adapters | `retired exchange tree/market_adapters/` | Public perpetual market-data adapters |
| Private perpetual protocols | `retired exchange tree/private_perp/` | Shared private REST/WebSocket implementation for linear perpetual venues |
| Trading adapters | `retired exchange tree/trading_adapters/` | Bridge legacy/core clients into `TradingAdapter` |
| Gateway registry | `retired exchange tree/registry.rs` | Builds gateways, market adapters, trading adapters, auth, and position mode |
| Legacy bridge | `retired exchange tree/gateway_exchange.rs` | Wraps gateway adapters for code still requiring `core::exchange::Exchange` |

## Spot Support

The Spot path is used by:

- `retired strategy tree/spot_spot_taker_arbitrage/`
- `src/scanner/five_exchange_spot.rs`
- `src/control/spot_control/`
- `src/execution/live_dry_run.rs`
- `src/execution/order_reconciliation.rs`

Current Spot-related venue modules include Binance, OKX, Bitget, Gate.io, MEXC,
CoinEx, KuCoin, BitMEX public/private REST, and Paper. Capability depth varies by
venue; configs and tests should decide whether a venue is scan-only,
paper-capable, live-dry-run capable, or eligible for future live submission.

## Perpetual Support

The perpetual path uses:

- `TradingAdapter`
- `MarketDataAdapter`
- `ExchangeGateway`
- private perpetual protocol adapters

Current perpetual coverage includes Binance, OKX, Bitget, Gate, Bybit, MEXC,
HTX market-data/private-protocol paths where registered, and BitMEX public/private
REST symbol, book, account, position, order, and fill support in
`rustcta-exchange-gateway`.

## Compatibility Layer Cleanup

Removed:

- old flat `retired exchange tree/adapters/` modules
- stale strategy references to deleted legacy strategy families
- migration/remediation documents superseded by current architecture docs

Kept:

- `GatewayExchange`, because parts of the existing CTA stack still accept
  `core::exchange::Exchange`
- legacy core clients where active strategies still use them
- explicit conversion helpers between legacy and unified market/order types

New work should not add another broad compatibility facade. Add a narrow bridge
only when a current caller needs it and document why it cannot use the unified
contract directly.

## Validation Expectations

Before enabling a venue for executable arbitrage:

- verify symbol mapping and precision rules
- verify min quantity and min notional handling
- verify fee source and fallback source
- run read-only balance/orderbook validation
- run dry-run or live-dry-run order planning
- verify private stream or reconciliation behavior
- confirm disabled-symbol and kill-switch handling

For scan-only support, the minimum bar is correct symbol normalization, fresh
book data, fee annotation, and explicit non-executable labeling.
