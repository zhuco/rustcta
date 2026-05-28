# Exchange Adapter Interface Status

This note records the current exchange-adapter layout and the unified execution
contracts used by the USDT perpetual cross-exchange arbitrage runtime.

## Current Layout

- `src/execution/adapter.rs` defines the unified private trading interface:
  `TradingAdapter`, command/ack types, account snapshots, fills, leverage,
  position mode, close-position, amendments, and countdown cancel-all.
- `src/execution/router.rs` routes unified execution commands to registered
  `TradingAdapter` implementations and preserves dry-run semantics.
- `src/market/adapter.rs` defines the unified market data interface:
  `MarketDataAdapter`, public WebSocket subscriptions, funding, instruments,
  and orderbook snapshots.
- `src/exchanges/adapters/*_market.rs` contains per-exchange market adapters
  for Binance, OKX, Bitget, and Gate.
- `src/exchanges/adapters/trading.rs` bridges older `core::Exchange`
  implementations into `TradingAdapter` for Binance and OKX.
- `src/exchanges/adapters/private_perp.rs` contains Bitget and Gate private
  USDT perpetual REST/WebSocket protocol implementations behind the same
  `PrivatePerpProtocol` and `TradingAdapter` surfaces.

The project is therefore unified at the strategy and execution boundary, but it
is not yet fully organized as one directory per exchange. Bitget and Gate
private perpetual code currently shares one protocol file because the strategy
needs common request signing, REST response normalization, private WebSocket
event parsing, precision handling, and Gate contract-size conversion.

## Unified Private Trading Surface

The following operations are currently expressed through the common
`TradingAdapter` contract:

- Place market/limit orders.
- Cancel one order.
- Cancel all orders by exchange or symbol.
- Cancel a batch of orders.
- Read one order.
- Read open orders.
- Read balances.
- Read positions.
- Read recent fills.
- Read trade fee snapshots.
- Read symbol account configuration.
- Amend an order.
- Set leverage.
- Set position mode where supported.
- Close a position.
- Set countdown/dead-man cancel-all where supported.
- Load symbol rules from registered metadata.

Capabilities are exposed by `TradingCapabilities`, so strategy code should check
support instead of relying on exchange names.

## Current Exchange Coverage

| Exchange | Market Data | Private Trading | Notes |
| --- | --- | --- | --- |
| Binance | `BinanceMarketAdapter` | `ExchangeTradingAdapter` over `BinanceExchange` | Production key is currently configured by the operator. |
| OKX | `OkxMarketAdapter` | `ExchangeTradingAdapter` over `OkxExchange` | Excluded from the current live-small plan due to insufficient simulation data. |
| Bitget | `BitgetMarketAdapter` | `PrivatePerpTradingAdapter<BitgetPrivatePerpProtocol>` | Supports Demo Trading through `demo_trading` and demo WebSocket URL overrides. Countdown cancel-all is not exposed by this adapter. |
| Gate | `GateMarketAdapter` | `PrivatePerpTradingAdapter<GatePrivatePerpProtocol>` | Supports Futures TestNet endpoint overrides, decimal-size REST header, price-only order amendments, and countdown cancel-all. Gate is treated as one-way/net-position for strategy safety. |

## Recent Low-Frequency Safety Interfaces

Gate-specific additions:

- `PATCH /futures/usdt/orders/{order_id}` for price-only amendments and
  `amend_text` updates.
- `POST /futures/usdt/countdown_cancel_all` for dead-man cancel-all.
- `X-Gate-Size-Decimal: 1` on Gate private REST requests.

The Gate amendment implementation intentionally rejects quantity amendments.
Gate futures order size has signed direction semantics, while the current
unified `AmendOrderCommand` does not carry original order side. A safe quantity
amend needs either original-side context or a pre-read of the existing order
before sending the amendment.

## Organization Recommendation

The next structural cleanup should split `private_perp.rs` into modules without
changing strategy-facing APIs:

- `src/exchanges/adapters/private_perp/mod.rs`
- `src/exchanges/adapters/private_perp/common.rs`
- `src/exchanges/adapters/private_perp/bitget.rs`
- `src/exchanges/adapters/private_perp/gate.rs`
- `src/exchanges/adapters/private_perp/ws.rs`
- `src/exchanges/adapters/private_perp/tests.rs`

Do this after the live-small smoke test path is stable. The current single file
keeps behavior centralized during rapid hardening, but it is already large
enough that per-exchange modules will reduce accidental cross-exchange changes.
