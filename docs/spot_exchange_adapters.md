# Spot Exchange Adapters

This document describes the unified Spot adapter work for MEXC Spot and CoinEx Spot.
The adapters are intended for later cross-exchange arbitrage, inventory balancing, and
spot hedging, but they do not implement strategy logic and they do not place live
orders by default.

References:

- MEXC Spot API: https://mexcdevelop.github.io/apidocs/spot_v3_en/
- CoinEx API v2: https://docs.coinex.com/api/v2

## Architecture

RustCTA currently has both legacy and newer exchange abstractions:

- `src/core/exchange.rs` defines the legacy `Exchange` trait.
- `src/exchanges/gateway.rs` defines the layered `MarketDataAdapter`,
  `TradingAdapter`, and `ExchangeGateway` traits.
- `src/exchanges/unified.rs` defines the newer async `ExchangeClient` trait and
  normalized Spot/Perpetual types.
- `src/exchanges/gateway_exchange.rs` bridges newer gateway adapters back into the
  legacy `Exchange` surface.

The MEXC and CoinEx Spot adapters implement `ExchangeClient` and reuse the unified
types in `src/exchanges/unified.rs`. This keeps the implementation reviewable while
preserving the existing legacy strategy code.

## Modules

MEXC Spot:

- `src/exchanges/mexc/mod.rs`
- `config/mexc_spot_example.yml`

CoinEx Spot:

- `src/exchanges/coinex/mod.rs`
- `config/coinex_spot_example.yml`

Shared Spot support:

- `src/exchanges/unified.rs`
- `src/exchanges/spot_reservation.rs`
- `config/spot_exchanges_example.yml`

## Unified Types

The shared model includes:

- `MarketType`: `Spot`, `Perpetual`
- `OrderSide`: `Buy`, `Sell`
- `PositionSide`: `Long`, `Short`, `Net`, `None`
- `OrderType`: `Market`, `Limit`, `PostOnly`, `IOC`, `FOK`
- `TimeInForce`: `GTC`, `IOC`, `FOK`, `GTX`
- `OrderStatus`: `New`, `PartiallyFilled`, `Filled`, `Cancelled`, `Rejected`,
  `Expired`, `Unknown`
- `LiquidityRole`: `Maker`, `Taker`, `Unknown`
- `SymbolRule`, `AssetBalance`, `BalanceSnapshot`, `FeeRate`,
  `OrderBookSnapshot`, `TradeFill`, `OrderRequest`, `OrderResponse`,
  `CancelOrderRequest`, `CancelOrderResponse`, `OpenOrder`,
  `ExchangeHealthStatus`, and `ExchangeError`

`AssetBalance` distinguishes exchange and local accounting:

- `total`
- `available`
- `locked_by_exchange`
- `locally_reserved`
- `effective_available`

`effective_available = available - locally_reserved`.

## Symbol Normalization

Internal Spot symbols use compact uppercase format:

- `BTCUSDT`
- `ETHUSDT`
- `CUDISUSDT`

MEXC Spot uses the same compact format for REST and WebSocket symbols.

CoinEx Spot is normalized through a generic mapping table. The default behavior
removes `/`, `-`, and `_`, then uppercases the symbol. Config can provide explicit
entries when a venue-specific symbol must map to a RustCTA internal symbol.

## Symbol Rules and Validation

Adapters load exchange metadata into `SymbolRule`:

- base and quote assets
- tick size and price precision
- step size and quantity precision
- min quantity
- min notional
- max quantity when available
- supported order types
- supported time-in-force values
- trading status
- raw metadata

Before submitting an order, adapters validate:

- symbol exists and is tradable
- price conforms to tick size
- quantity conforms to step size
- quantity is above min quantity
- notional is above min notional
- order type is supported
- time-in-force is supported
- local balance reservation is possible when live submission is enabled

Post-only orders are not silently downgraded. If native support is not implemented
or verified, the adapter returns `Unsupported`.

IOC/FOK orders are not silently downgraded to GTC.

## Balance Reservation

`BalanceReservationManager` provides per-exchange-per-asset local reservation.
Reservation keys are effectively:

- `mexc:USDT`
- `mexc:DKA`
- `coinex:USDT`
- `coinex:CUDIS`

For sell orders, the adapter reserves base asset quantity.

For buy orders, the adapter reserves quote asset notional plus a fee buffer.

Reservations are released when an order is rejected or the request fails before
exchange acceptance. Filled quantity is settled as fills arrive, and unfilled
quantity remains reserved until the order is cancelled, rejected, expired, or
filled.

This prevents concurrent local order submission from double-spending the same
effective available balance.

## MEXC Spot

Implemented:

- symbol metadata through exchange info
- balances
- order book snapshot
- public order book WebSocket subscription
- market, limit, post-only, IOC, and FOK request mapping
- cancel order
- query order
- query open orders
- fee-rate loading with config fallback
- recent fills through REST
- HMAC request signing
- client order IDs in `MXSPT_<timestamp>_<counter>` format
- structured error classification, including code `30005` / `Oversold`

Private user stream support currently returns `Unsupported`. REST polling can be
used as a safe fallback until private stream behavior is implemented and tested.

## CoinEx Spot

Implemented:

- symbol metadata through market list
- balances
- order book snapshot
- public order book WebSocket subscription
- market, limit, IOC, and FOK request mapping
- cancel order
- query order
- query open orders
- fee-rate loading with config fallback
- recent fills through REST
- HMAC request signing
- client order IDs in `CXSPT_<timestamp>_<counter>` format
- structured error classification for balance, rate limit, invalid symbol,
  precision, notional, auth, and order-not-found paths

Post-only currently returns `Unsupported` until native support is verified for the
target CoinEx API behavior.

Private user stream support currently returns `Unsupported`. REST polling can be
used as a safe fallback until private stream authentication and order/account event
parsing are implemented and tested.

## Order Book Staleness

`OrderBookSnapshot` includes:

- exchange
- market type
- symbol
- bids and asks
- best bid and best ask
- exchange timestamp when available
- local receipt timestamp
- latency in milliseconds when computable
- sequence/update ID when available
- `is_stale`

Adapters mark snapshots stale when exchange timestamp latency exceeds
`stale_book_ms`. WebSocket loops reconnect when no message arrives within the
configured stale timeout.

Sequence handling is implemented only where the exchange payload provides a usable
sequence/update ID. If sequence support is not available or cannot be recovered
safely, the adapter reconnects or relies on fresh REST snapshots.

## Config

Examples:

- `config/mexc_spot_example.yml`
- `config/coinex_spot_example.yml`
- `config/spot_exchanges_example.yml`

All examples use:

- `dry_run: true`
- `enable_private_stream: false`
- environment-variable placeholders for secrets
- `log_raw_messages: false`

Do not commit real API keys. Do not log API secrets, signatures, or private
headers.

## Testing

Unit and mocked-response tests:

```bash
cargo test exchanges::unified --all-features
cargo test exchanges::spot_reservation --all-features
cargo test exchanges::mexc --all-features
cargo test exchanges::coinex --all-features
```

Full suite:

```bash
cargo test --all-features
```

Ignored live health tests:

```bash
MEXC_API_KEY=... MEXC_API_SECRET=... \
  cargo test mexc_live_health_check_requires_credentials --all-features -- --ignored

COINEX_API_KEY=... COINEX_API_SECRET=... \
  cargo test coinex_live_health_check_requires_credentials --all-features -- --ignored
```

Live order tests must also require:

```bash
ENABLE_LIVE_ORDER_TESTS=true
```

No live order test should submit an order without this explicit flag and a tiny
notional safety limit.

## Current Limitations

- MEXC private user stream is not implemented.
- CoinEx private user stream is not implemented.
- CoinEx post-only returns `Unsupported` until native support is verified.
- Fee endpoints can vary by account permissions; config fee overrides are supported.
- REST polling should be used for order/balance reconciliation until private
  streams are implemented.
- Exchange-specific min order, lot, and tick metadata should be revalidated during
  live preflight for every enabled symbol.

## Remaining Work Before Arbitrage

- Wire these clients into strategy dependency construction or an exchange client
  registry.
- Add live preflight checks for symbol rules, balances, fees, private streams, and
  database recorder health.
- Implement private user streams with order, fill, and balance reconciliation.
- Add maker/taker execution only after paper trading and replay validation.
- Add inventory balancing and residual exposure handling before enabling live
  arbitrage.
