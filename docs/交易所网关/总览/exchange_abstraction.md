# Exchange Abstraction

RustCTA has two exchange-facing contracts:

- `ExchangeClient` in `crates/rustcta-exchange-api/src/client.rs` for Spot,
  Perpetual, and market-neutral client behavior.
- `TradingAdapter` in `crates/rustcta-execution-api/src/lib.rs`, plus gateway
  adapters under `crates/rustcta-exchange-gateway/src/adapters/`, for
  cross-exchange execution and market-data routing.

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
| Binance | `crates/rustcta-exchange-gateway/src/adapters/binance/` | REST orders, balances, fees, public/private streams, dry-run acks |
| OKX | `crates/rustcta-exchange-gateway/src/adapters/okx/` | REST orders, balances, fees, public/private streams, dry-run acks |
| Bitget | `crates/rustcta-exchange-gateway/src/adapters/bitget/` | Spot client support for scanner/arbitrage paths |
| Gate.io | `crates/rustcta-exchange-gateway/src/adapters/gateio/` | Spot client support for scanner/arbitrage paths |
| MEXC | `crates/rustcta-exchange-gateway/src/adapters/mexc/` | Spot client support for scanner/arbitrage paths |
| CoinEx | `crates/rustcta-exchange-gateway/src/adapters/coinex/` | Spot client support for scanner/arbitrage paths |
| KuCoin | `crates/rustcta-exchange-gateway/src/adapters/kucoin/` | Spot client support for scanner/arbitrage paths |
| Paper | `crates/rustcta-exchange-gateway/src/mock.rs` | Deterministic paper execution and user-stream events |

The Spot arbitrage runtime can scan and compare multiple exchanges. Live
trading is still controlled per exchange by config, dry-run mode, preflight, and
control-plane state.

## Perpetual Adapter Coverage

USDT perpetual market data and execution are routed through:

- `crates/rustcta-exchange-api/`
- `crates/rustcta-exchange-gateway/src/adapters/`
- `crates/rustcta-exchange-gateway/src/client.rs`
- `crates/rustcta-execution-api/`

This path is used by unified arbitrage and funding-rate tooling, not by
the Spot-to-Spot arbitrage runtime.

## Compatibility Boundary

The compatibility layer now consists of:

- `GatewayExchange`, which maps execution/market adapters back to the legacy
  `core::exchange::Exchange` trait.
- legacy core clients for strategies that have not migrated.
- explicit conversion helpers in `unified.rs`.

Removed compatibility:

- old flat adapter compatibility modules outside `crates/rustcta-exchange-gateway`
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
