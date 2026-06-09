# OrangeX Gateway Adapter

Status date: 2026-06-08

Adapter id: `orangex`

Implementation status: JSON-RPC REST gateway adapter for Spot and USDT
perpetual request-spec validation. Public symbol rules, order book snapshots,
fee-rate readback from instrument metadata, gateway-composed batch
place/cancel, public WebSocket subscribe/unsubscribe plus heartbeat request
specs, private WebSocket spec-only payload/parser helpers, and venue-specific
perpetual leverage/margin-mode helpers are implemented. Private REST request
construction is behind `enabled_private_rest`; it can use either a configured
bearer access token or one-shot `/public/auth` client-signature token
acquisition. OrangeX has published a WebSocket API deprecation notice, so the
adapter does not advertise or open private WebSocket runtime support.

## Product Lines

| Product | MarketType | Status |
| --- | --- | --- |
| Spot | `Spot` | Public REST + private REST request specs |
| USDT perpetual | `Perpetual` | Public REST + private REST request specs |
| WebSocket market data | n/a | Public subscribe/unsubscribe and heartbeat request specs |
| Private WebSocket execution/account | n/a | Unsupported as runtime capability because of OrangeX WebSocket API deprecation; historical/spec-only private subscribe/unsubscribe payloads and parser helpers remain covered by offline tests |

REST base URL: `https://api.orangex.com/api/v1`

WebSocket base URL documented by OrangeX: `wss://api.orangex.com/ws/api/v1`

Public order book WebSocket remains spec-only because OrangeX published a
deprecation notice. The reviewed stable channel is `book.{instrument}.raw`;
legacy `book.{instrument}.100ms` evidence is retained for audit only. Both are
recorded as depth: unspecified / no fixed depth because the current verified
docs do not expose a stable depth selector; continuity depends on
`change_id`/`prev_change_id` and REST `/public/get_order_book` snapshot rebuild.

## Authentication

OrangeX private HTTP calls use JSON-RPC plus bearer authorization:

- Header: `Authorization: bearer <token>`
- Auth method: `/public/auth`
- Supported grant documented by OrangeX: `client_credentials`, `client_signature`, `refresh_token`
- Client signature prehash: `clientId + "\n" + timestamp + "\n" + nonce + "\n"`
- Signature: lowercase hex `HMAC-SHA256(clientSecret, prehash)`

The adapter includes the signing helper and request-spec tests. Private REST
prefers `ORANGEX_ACCESS_TOKEN`; if absent, it can call `/public/auth` using
`ORANGEX_CLIENT_ID` + `ORANGEX_CLIENT_SECRET` or the legacy
`ORANGEX_API_KEY` + `ORANGEX_API_SECRET` pair. Persistent token caching/refresh
is intentionally not enabled in this first pass.

## Endpoint Mapping

| Standard capability | OrangeX JSON-RPC method | Current gateway behavior |
| --- | --- | --- |
| symbol rules | `/public/get_instruments` | Spot uses `currency=SPOT, kind=spot`; USDT perpetual uses `currency=USDT, kind=perpetual`; parses tick, quantity step, min quantity, active status |
| order book | `/public/get_order_book` | Snapshot parser with depth clamped to 1..100, sequence `version`, and millisecond timestamp |
| balances | `/private/get_assets_info` | Request-spec + parser for Spot asset details and perpetual margin account values |
| positions | `/private/get_positions` | Request-spec + parser for perpetual positions |
| place order | `/private/buy`, `/private/sell` | Limit/market/post-only/IOC/FOK mapping, reduce-only, position side, custom order id validation |
| quote-sized market order | `/private/buy`, `/private/sell` | Request-spec mapping with `market_amount_order=true` |
| batch place | no verified native batch endpoint | Gateway sequential fallback over signed `/private/buy` and `/private/sell` calls; non-atomic |
| cancel order | `/private/cancel` | Exchange order id required |
| batch cancel | no verified native batch endpoint | Gateway sequential fallback over signed `/private/cancel` calls; exchange order id required; non-atomic |
| cancel all | `/private/cancel_all_by_currency`, `/private/cancel_all_by_instrument` | Currency-scoped or instrument-scoped request construction |
| query order | `/private/get_order_state` | Exchange order id required |
| open orders | `/private/get_open_orders_by_currency`, `/private/get_open_orders_by_instrument` | Request-spec + basic parser |
| fills | `/private/get_user_trades_by_currency`, `/private/get_user_trades_by_instrument`, `/private/get_user_trades_by_order` | Request-spec + parser |
| fees | `/public/get_instruments` metadata | Parses `maker_commission` and `taker_commission` by requested symbol |
| public streams | `/public/subscribe`, `/public/unsubscribe`, `/public/ping`, text `PING` | Request-spec ack for order book, trades, ticker, and candles; control parser covers subscription ack/event and ping/pong heartbeat |
| private streams | `/private/subscribe`, `/private/unsubscribe`, `/public/ping`, text `PING` | Spec-only helpers for deprecated OrangeX WebSocket API; runtime `subscribe_private_stream` returns `Unsupported("orangex.subscribe_private_stream.deprecated_official_websocket_api")`; REST balances/positions/open-orders/fills/order-state paths provide reconciliation fallback |
| perpetual margin mode | `/private/adjust_perpetual_margin_type` | Venue-specific helper for USDT perpetual `cross`/`isolated`; not part of the current standard `ExchangeClient` trait |
| perpetual leverage | `/private/adjust_perpetual_leverage` | Venue-specific helper for USDT perpetual leverage changes; rejects zero leverage |

## Validation

Offline tests cover JSON-RPC public instruments/order book, fee metadata,
private bearer request construction, one-shot `/public/auth`, balance/cancel,
batch place/cancel request specs, public stream subscribe/unsubscribe, spec-only
private stream subscribe/unsubscribe payloads, heartbeat ping/pong control
parsing, private user change event conversion, perpetual margin/leverage
request specs, client-signature prehash, private WebSocket deprecation
`Unsupported`, and unsupported private REST behavior without a token.

Validation notes:

- Targeted `rustfmt --edition 2021 crates/rustcta-exchange-gateway/src/adapters/orangex/*.rs` passed in the earlier OrangeX delivery.
- `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/gateway-clean-check cargo test -p rustcta-exchange-gateway orangex --lib --message-format short` passed 22 OrangeX tests with existing workspace warnings. Under the managed sandbox, mock REST tests that bind `127.0.0.1` require an unsandboxed run.
- Gateway app env wiring supports `RUSTCTA_ORANGEX_REST_BASE_URL`,
  `RUSTCTA_ORANGEX_CLIENT_ID`, `RUSTCTA_ORANGEX_CLIENT_SECRET`,
  `RUSTCTA_ORANGEX_ACCESS_TOKEN`, `RUSTCTA_ORANGEX_API_KEY`, and
  `RUSTCTA_ORANGEX_API_SECRET`.

Sources:

- <https://openapi-docs.orangex.com/>
- <https://www.orangex.com/support/help/articles/1247-websocket-api-deprecation>
