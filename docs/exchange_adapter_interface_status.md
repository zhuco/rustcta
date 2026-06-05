# Exchange Adapter Interface Status

This note records the current adapter layout after compatibility cleanup.

## Summary

Multi-exchange Spot arbitrage is supported through unified Spot clients and the
`spot_spot_taker_arbitrage` runtime. USDT perpetual arbitrage remains on the
execution gateway path used by `cross_exchange_arbitrage`.

The old `src/exchanges/adapters/` compatibility directory has been removed.

## Current Adapter Layout

| Area | Path | Role |
| --- | --- | --- |
| Unified client contracts | `src/exchanges/unified.rs` | Spot/Perpetual client model, requests, responses, user stream events |
| Venue modules | `src/exchanges/<exchange>/` | Exchange-specific Spot or legacy core clients |
| Market adapters | `src/exchanges/market_adapters/` | Public perpetual market-data adapters |
| Private perpetual protocols | `src/exchanges/private_perp/` | Shared private REST/WebSocket implementation for linear perpetual venues |
| Trading adapters | `src/exchanges/trading_adapters/` | Bridge legacy/core clients into `TradingAdapter` |
| Gateway registry | `src/exchanges/registry.rs` | Builds gateways, market adapters, trading adapters, auth, and position mode |
| Legacy bridge | `src/exchanges/gateway_exchange.rs` | Wraps gateway adapters for code still requiring `core::exchange::Exchange` |

## Spot Support

The Spot path is used by:

- `src/strategies/spot_spot_taker_arbitrage/`
- `src/scanner/five_exchange_spot.rs`
- `src/control/spot_control/`
- `src/execution/live_dry_run.rs`
- `src/execution/order_reconciliation.rs`

Current Spot-related venue modules include Binance, OKX, Bitget, Gate.io, MEXC,
CoinEx, KuCoin, and Paper. Capability depth varies by venue; configs and tests
should decide whether a venue is scan-only, paper-capable, live-dry-run capable,
or eligible for future live submission.

## Perpetual Support

The perpetual path uses:

- `TradingAdapter`
- `MarketDataAdapter`
- `ExchangeGateway`
- private perpetual protocol adapters

Current perpetual coverage includes Binance, OKX, Bitget, Gate, Bybit, MEXC,
and HTX market-data/private-protocol paths where registered.

## Compatibility Layer Cleanup

Removed:

- old flat `src/exchanges/adapters/` modules
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
