# Exchange Abstraction

RustCTA has two exchange-facing contracts:

- `ExchangeClient` in `src/exchanges/unified.rs` for Spot and market-neutral
  client behavior.
- `TradingAdapter` plus `MarketDataAdapter` in `src/execution/` and
  `src/market/` for cross-exchange execution and market-data routing.

The old broad compatibility layer is intentionally reduced. It remains only
where legacy strategy code still depends on `core::exchange::Exchange`.

## Unified Client Model

`ExchangeClient` normalizes:

- `MarketType`: `Spot`, `Perpetual`
- `OrderSide`: `Buy`, `Sell`
- `PositionSide`: `None`, `Net`, `Long`, `Short`
- `OrderType`: `Market`, `Limit`, `PostOnly`, `IOC`, `FOK`
- `TimeInForce`: `GTC`, `IOC`, `FOK`, `GTX`
- `OrderStatus`: `New`, `PartiallyFilled`, `Filled`, `Cancelled`, `Rejected`,
  `Expired`, `Unknown`

Spot orders should use `PositionSide::None` or `PositionSide::Net` and must not
set `reduce_only`. Perpetual orders may use directional position sides when the
venue supports them.

## Required Operations

The client surface covers:

- normalize symbol
- read balances
- read order book
- place order
- cancel order
- query one order
- query open orders
- read fee rate
- subscribe public order books
- subscribe private user stream

Adapters may return `UnsupportedCapability` for unavailable features, but they
should not silently emulate unsupported live behavior.

## Spot Adapter Coverage

Spot arbitrage currently uses these adapter families:

| Exchange | Spot client path | Notes |
| --- | --- | --- |
| Binance | `src/exchanges/binance/spot.rs` | REST orders, balances, fees, public/private streams, dry-run acks |
| OKX | `src/exchanges/okx/spot.rs` | REST orders, balances, fees, public/private streams, dry-run acks |
| Bitget | `src/exchanges/bitget/mod.rs` | Spot client support for scanner/arbitrage paths |
| Gate.io | `src/exchanges/gateio/mod.rs` | Spot client support for scanner/arbitrage paths |
| MEXC | `src/exchanges/mexc/mod.rs` | Spot client support for scanner/arbitrage paths |
| CoinEx | `src/exchanges/coinex/mod.rs` | Spot client support for scanner/arbitrage paths |
| KuCoin | `src/exchanges/kucoin/mod.rs` | Spot client support for scanner/arbitrage paths |
| Paper | `src/exchanges/paper/mod.rs` | Deterministic paper execution and user-stream events |

The Spot arbitrage runtime can scan and compare multiple exchanges. Live
trading is still controlled per exchange by config, dry-run mode, preflight, and
control-plane state.

## Perpetual Adapter Coverage

USDT perpetual market data and execution are routed through:

- `src/exchanges/market_adapters/`
- `src/exchanges/private_perp/`
- `src/exchanges/trading_adapters/`
- `src/exchanges/registry.rs`

This path is used by `cross_exchange_arbitrage` and funding-rate tooling, not by
the Spot-to-Spot arbitrage runtime.

## Compatibility Boundary

The compatibility layer now consists of:

- `GatewayExchange`, which maps execution/market adapters back to the legacy
  `core::exchange::Exchange` trait.
- legacy core clients for strategies that have not migrated.
- explicit conversion helpers in `unified.rs`.

Removed compatibility:

- `src/exchanges/adapters/`
- deleted single-file exchange shims that were superseded by directory modules
- legacy strategy exports for removed strategy families

New exchange work should avoid adding new legacy wrappers unless a specific
active strategy still requires them.

## Safety Requirements

All adapters should:

- classify exchange errors instead of returning opaque strings where possible
- validate precision, min quantity, and min notional before live submission
- preserve client order ids
- expose maker/taker fee rates or mark a configured fallback
- reject stale books for executable decisions
- keep dry-run behavior explicit and visible in returned acknowledgements
- surface unsupported capabilities as errors

These requirements are especially important for multi-exchange Spot arbitrage,
where stale books, fee drift, symbol mismatch, or partial inventory ownership can
turn an apparent spread into realized loss.
