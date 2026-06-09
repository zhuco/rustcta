# Upbit Gateway Adapter

Status date: 2026-06-09

## Scope

- Adapter id: `upbit`
- Product line: Korean Spot only
- Markets: KRW, BTC, and USDT spot markets using `QUOTE-BASE` exchange symbols, for example `KRW-BTC`, `BTC-ETH`, and `USDT-BTC`
- REST base URL: `https://api.upbit.com`
- Public WS: `wss://api.upbit.com/websocket/v1`
- Private WS: `wss://api.upbit.com/websocket/v1/private`

This adapter is offline-valid for public REST, private REST request construction, JWT signing vectors, and WebSocket payload/parser specs. It does not perform live trading validation.

## Official API Notes

- Public market list and order book use `/v1/market/all` and `/v1/orderbook`.
- Private REST uses `Authorization: Bearer <jwt>`.
- JWT payload includes `access_key` and `nonce`; when query or body parameters are present it also includes `query_hash` and `query_hash_alg=SHA512`.
- The Upbit request-spec fixture explicitly covers query hash construction, as required by Task 12.

## Rate Limits

The mapping keeps conservative local buckets:

- `upbit_rest`: 10 requests per second
- `upbit_orders`: 8 order requests per second
- `upbit_ws`: conservative connection/message budget

Upbit publishes endpoint-group rate policies and can apply account or region controls. Runtime config must keep these local limits at or below the active official policy. This adapter does not implement region bypass.

## Implemented Offline Surface

- Symbol rules: KRW/BTC/USDT market list parser
- Order book: REST snapshot parser and public WS orderbook payload spec
- Private read REST: balances, fee snapshot via order chance, order query, open orders, recent fills
- Private write REST: place order, quote market buy, cancel order request construction
- Private WS: order/fill/balance subscription payloads and REST reconciliation fallback

## Official Public WS Order Book Details

Upbit official orderbook WebSocket uses region-specific public URLs such as
`wss://sg-api.upbit.com/websocket/v1`. Subscription requests are JSON arrays with
ticket/type/format objects. Orderbook unit count can be specified as
`{code}.{count}` with supported counts 1, 5, 15, and 30; unsupported counts
default to 30. Official docs do not state a fixed millisecond interval and do not
publish a sequence/checksum for orderbook continuity.

| Surface | Upbit public orderbook WS |
| --- | --- |
| URLs | Korea/global `wss://api.upbit.com/websocket/v1`; regional examples `wss://sg-api.upbit.com/websocket/v1`, `wss://id-api.upbit.com/websocket/v1`, `wss://th-api.upbit.com/websocket/v1` |
| Protocol | Native WebSocket, JSON array subscription payload |
| Subscription | `[{"ticket": ...}, {"type": "orderbook", "codes": ["KRW-BTC.30"], "is_only_realtime": true}, {"format": "DEFAULT"}]` |
| Unit count / depth | `{code}.{count}`; supported counts/depths are `1`, `5`, `15`, and `30`; adapter helper defaults orderbook subscriptions to `30` |
| Cadence | No fixed millisecond interval documented |
| Sequence/timestamp | `timestamp` and `stream_type` are present; no official sequence/update id field |
| Checksum | No official checksum field; no checksum mismatch risk, but the adapter cannot checksum-validate continuity |
| Resync | Reconnect/stale/parse-error recovery must rebuild from REST `/v1/orderbook` snapshot |

Unsupported runtime boundary: the adapter records the orderbook subscription
shape and parser fixture, but it does not maintain a live incremental local book
because Upbit does not provide an official sequence/checksum contract for public
orderbook continuity.

## Unsupported Boundary

- Futures, perpetuals, options, and standard contract trading: `交易所不支持合约` under the current official API reference scope.
- Positions, leverage, margin mode, position mode: unsupported because Upbit Spot has no derivative position model.
- Amend order: unsupported until a native endpoint is verified against the shared Spot amend contract.
- Batch place/cancel and cancel-all: unsupported; no native atomic batch endpoint is claimed.
- Fiat funding, withdrawal, transfer, and bank operations are not part of the gateway runtime.

## Validation

Allowed commands:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/upbit/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway upbit --lib --message-format short
```
