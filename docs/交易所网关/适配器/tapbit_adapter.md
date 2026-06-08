# Tapbit Gateway Adapter

Status date: 2026-06-07.

The `tapbit` gateway adapter implements the `rustcta_exchange_api::ExchangeClient`
surface for Tapbit Spot plus USDT perpetual REST, venue-specific public market
helpers, plus public WebSocket subscription specs for order book and ticker
streams. Public order-book pushes and ping/pong controls can be converted into
standard `ExchangeStreamEvent` values; ticker pushes remain typed parser output
because the shared stream model has no ticker event variant.

## Configuration

- Spot REST base URL: `https://openapi.tapbit.com/spot`
- USDT perpetual REST base URL: `https://openapi.tapbit.com/swap`
- Public WebSocket URL: `wss://ws-openapi.tapbit.com/stream/ws`
- Private REST env vars: `TAPBIT_API_KEY`, `TAPBIT_API_SECRET`
- Private REST can be disabled with `TAPBIT_PRIVATE_REST_ENABLED=false`
- No stable public testnet is configured by default.

## Signing

Private REST uses:

- `ACCESS-KEY`
- `ACCESS-SIGN`
- `ACCESS-TIMESTAMP`
- `Content-Type: application/json`

The signature payload is:

```text
timestamp + UPPERCASE(method) + request_path + optional("?"+query) + body
```

`request_path` starts at `/api/...` and does not include the Spot or swap base
URL. GET requests use an empty body. POST requests sign the exact JSON body
string sent on the wire. The signature is lowercase hex HMAC-SHA256.

## Endpoint Mapping

| Gateway capability | Tapbit endpoint | Notes |
| --- | --- | --- |
| Spot symbol rules | `GET /api/spot/instruments/trade_pair_list` | Parses precision, min amount/notional, maker/taker fee metadata |
| Perp symbol rules | `GET /api/usdt/instruments/list` | Public scan-only metadata |
| Spot server time | `GET /api/spot/instruments/current/timestamp` | Venue-specific helper for clock checks |
| Perp server time | `GET /api/v1/usdt/time` | Venue-specific helper for clock checks |
| Spot order book | `GET /api/spot/instruments/depth` | Snapshot-only, depth normalized to 5/10/50/100/200 |
| Perp order book | `GET /api/usdt/instruments/depth` | Snapshot-only public scan, depth normalized to 5/10/50/100/200 |
| Spot ticker/trades/candles | `GET /api/spot/instruments/ticker_one`, `/trade_list`, `/candles` | Venue-specific helpers and parser tests |
| Perp ticker/trades/candles/funding | `GET /api/usdt/instruments/ticker_one`, `/trade_list`, `/candles`, `/funding_rate` | Venue-specific helpers and parser tests |
| Spot balances | `GET /api/v1/spot/account/list` | Requires API credentials |
| Perp balances | `GET /api/v1/usdt/account` | Requires API credentials |
| Perp positions | `GET /api/v1/usdt/position_list` | Parses size, entry, mark, PnL, leverage |
| Spot fees | `GET /api/spot/instruments/trade_pair_list` | Public fee metadata fallback |
| Spot place order | `POST /api/v1/spot/order` | Limit GTC only |
| Perp place order | `POST /api/v1/usdt/order` | Limit GTC, reduce-only and position side mapped |
| Spot batch place | `POST /api/v1/spot/batch_order` | Native array body, limit GTC only |
| Spot cancel order | `POST /api/v1/spot/cancel_order` | Exchange order id only |
| Perp cancel order | `POST /api/v1/usdt/cancel_order` | Exchange order id only |
| Spot batch cancel | `POST /api/v1/spot/batch_cancel_order` | Native `orderIds` list |
| Spot cancel all | `GET /api/v1/spot/open_order_list` + `POST /api/v1/spot/batch_cancel_order` | Composed by fetching open ids, then batch cancel |
| Perp cancel all | `GET /api/v1/usdt/open_order_list` + `POST /api/v1/usdt/cancel_order` | Composed by fetching open ids, then single-cancel loop |
| Perp batch place/cancel | `POST /api/v1/usdt/order`, `POST /api/v1/usdt/cancel_order` | Standard gateway batch API is supported by sequential signed single-order fallback because official native batch endpoints are not listed in the public USDT perpetual docs |
| Spot/perp query/open orders | `GET /api/v1/spot/order_info`, `GET /api/v1/spot/open_order_list`, `GET /api/v1/usdt/order_info`, `GET /api/v1/usdt/open_order_list` | REST reconciliation path |
| Perp fills | `GET /api/v1/usdt/fills` | Parses fill id, fee, side, timestamp |
| Spot public WS book | `spot/orderBook.{instrument_id}.[depth]` | `subscribe_public_stream` returns JSON spec with subscribe payload; depth 200 |
| Spot public WS ticker | `spot/ticker.{instrument_id}` | `subscribe_public_stream` returns JSON spec |
| Perp public WS book | `usdt/orderBook.{instrument_id}.[depth]` | `subscribe_public_stream` returns JSON spec; depth 200 |
| Perp public WS ticker | `usdt/ticker.{instrument_id}` | `subscribe_public_stream` returns JSON spec |

## Validation

- `rustfmt --edition 2021 crates/rustcta-exchange-gateway/src/adapters/tapbit/streams.rs crates/rustcta-exchange-gateway/src/adapters/tapbit/stream_tests.rs`
- `python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/tapbit/endpoint_mapping.yaml`
- `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/gateway-clean-check cargo test -p rustcta-exchange-gateway tapbit --lib --message-format short`
  passed: 20 tests passed, 0 failed, 732 filtered out. The managed sandbox blocks local mock REST binding on `127.0.0.1`, so the full target run was rerun outside the sandbox.

## WebSocket Heartbeat

Tapbit public WebSocket sends a `ping` frame every 5 seconds. Clients must
respond with `pong`; the server disconnects after two unanswered pings. The
adapter exposes this heartbeat contract in the public stream spec. This is
currently spec-only metadata for the gateway caller to run; the adapter does not
own a native Tapbit WebSocket task:

```json
{
  "server_ping_interval_seconds": 5,
  "max_missed_server_pings": 2,
  "client_pong_payload": "pong"
}
```

## Explicit Unsupported

Spot market, quote-sized market, post-only, IOC, FOK, client order id, Spot
dedicated private fills, amend/order-list APIs, leverage/margin/position-mode
mutations, TP/SL endpoints, public trades/candle WebSocket topics, private
WebSocket auth/subscriptions, and full socket task orchestration remain explicit
`Unsupported` or outside the current gateway trait. Private WebSocket returns
`Unsupported` as `tapbit.subscribe_private_stream.no_official_private_ws_topics`.
Tapbit's current official
Spot/USDT perpetual WebSocket docs list public order book/ticker topics and
heartbeat behavior, but do not publish private order/account stream topics.
