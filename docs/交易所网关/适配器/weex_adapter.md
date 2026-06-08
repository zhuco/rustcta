# WEEX Gateway Adapter

Status date: 2026-06-07

Adapter id: `weex`

Implementation status: Spot + USDT-M futures REST adapter is implemented behind
`rustcta_exchange_api::ExchangeClient`. Native Spot/Futures batch place/cancel
REST routes are implemented where official WEEX endpoints exist. WebSocket
subscription payloads, private header-auth specs, heartbeat ping/pong helpers,
subscription ack handling, public book parsing, and private order/account parser
hooks are implemented. Amend orders, order lists, leverage/margin/position-mode
mutations, and reduce-only order placement are intentionally not advertised.
The local gateway app can register `weex` from `RUSTCTA_GATEWAY_ADAPTERS` and
load WEEX REST overrides plus private credentials from `RUSTCTA_WEEX_*` env
vars without exposing secrets in debug output.

## Product Lines

| Product | MarketType | Status |
| --- | --- | --- |
| Spot | `Spot` | Public REST + private REST |
| USDT-M futures | `Perpetual` | Public REST + private REST |
| Simulation | n/a | Not enabled by default; futures sim endpoints are not wired |

REST base URLs:

- Spot: `https://api-spot.weex.com`
- Futures: `https://api-contract.weex.com`

WebSocket base URLs:

- Spot public: `wss://ws-spot.weex.com/v3/ws/public`
- Spot private: `wss://ws-spot.weex.com/v3/ws/private`
- Futures public: `wss://ws-contract.weex.com/v3/ws/public`
- Futures private: `wss://ws-contract.weex.com/v3/ws/private`

Machine-readable endpoint and capability metadata lives at
`crates/rustcta-exchange-gateway/src/adapters/weex/endpoint_mapping.yaml` and
`crates/rustcta-exchange-gateway/src/adapters/weex/capabilities_v2.yaml`.
The runtime `capabilities()` response also populates `capabilities_v2` with
native batch atomicity, pagination, stream heartbeat/auth renewal,
reconciliation, and credential-scope declarations while preserving the legacy
boolean fields.

## Authentication

Private requests use WEEX V3 headers:

- `ACCESS-KEY`
- `ACCESS-SIGN`
- `ACCESS-PASSPHRASE`
- `ACCESS-TIMESTAMP`
- `Content-Type: application/json`

The signature prehash is:

```text
timestamp + METHOD_UPPER + requestPath + optional("?"+queryString) + body
```

The adapter signs with HMAC-SHA256 and Base64 encodes the digest. Tests assert
that API secrets never enter the request path, query, or body.

## Endpoint Mapping

| Standard capability | Spot endpoint | USDT-M endpoint |
| --- | --- | --- |
| symbol rules | `GET /api/v3/exchangeInfo` | `GET /capi/v3/market/exchangeInfo` |
| order book | `GET /api/v3/market/depth` | `GET /capi/v3/market/depth` |
| balances | `GET /api/v2/account/assets` | `GET /capi/v3/account/balance` |
| positions | Unsupported | `GET /capi/v3/account/position/allPosition` |
| fees | parsed from `GET /api/v3/exchangeInfo` fee metadata | `GET /capi/v3/account/commissionRate` |
| place order | `POST /api/v3/order` | `POST /capi/v3/order` |
| cancel order | `DELETE /api/v3/order` | `DELETE /capi/v3/order` |
| batch place | `POST /api/v2/trade/batch-orders` | `POST /capi/v3/batchOrders` |
| batch cancel | `DELETE /api/v3/order/batch` | `DELETE /capi/v3/batchOrders` |
| cancel all | `DELETE /api/v3/openOrders` | `DELETE /capi/v3/allOpenOrders` |
| query order | `GET /api/v3/order` | `GET /capi/v3/order` |
| open orders | `GET /api/v3/openOrders` | `GET /capi/v3/openOrders` |
| fills | `GET /api/v3/myTrades` | `GET /capi/v3/userTrades` |
| WebSocket subscribe | `{"method":"SUBSCRIBE","params":["BTCUSDT@depth15"]}` | `{"method":"SUBSCRIBE","params":["BTCUSDT@depth15"]}` |
| WebSocket private auth | connection headers over `/v3/ws/private` | same |
| WebSocket heartbeat | server `event/type=ping`, reply helper `{"method":"PONG","id":...}` | same |

## WebSocket Boundary

The current gateway trait exposes `subscribe_public_stream` and
`subscribe_private_stream` as subscription acknowledgements/spec identifiers,
not as an owned long-running socket task. The WEEX adapter therefore implements
the exchange-specific payload builders, auth payload, heartbeat helper, and
message parsers used by the gateway runtime/supervisor layer. A future runtime
can wire those helpers into a concrete socket loop without changing the public
`ExchangeClient` interface.

## Reconciliation And Unsupported Boundaries

- REST reconciliation after a private stream gap uses open orders, order query,
  recent fills, balances, and futures positions.
- Native batch place/cancel is declared partial/non-atomic, capped at 20 items,
  and constrained to one market type per request. Batch cancel also requires one
  symbol.
- Private streams use ACCESS header login and require relogin/resubscribe on
  reconnect; there is no listen-key renewal.
- Quote-sized market orders, reduce-only placement, post-only placement, amend
  orders, order lists/OCO, leverage, margin mode, and position mode mutations
  are explicitly unsupported in this adapter pass.

## Error Classification

The transport maps HTTP `401` and WEEX auth codes such as `-1040..-1043` to
authentication failures, HTTP `403` and permission codes to permission errors,
HTTP `429` to rate limits, HTTP `5xx` to exchange unavailable, `-1121` to
invalid symbol, order-not-found codes to order not found, and message
heuristics cover insufficient balance, duplicate client order id, symbol,
precision, and rate-limit wording.

## Validation

Offline request-spec tests cover public Spot rules, public futures depth,
private readbacks, private order mutations, gateway-level batch place/cancel
native routing, WebSocket subscribe/auth/heartbeat/message parsing, unsupported
private behavior without credentials, WEEX Base64 HMAC generation, and
sanitized fixtures under `tests/fixtures/exchanges/weex/`.

Validation attempted:

- `rustfmt --edition 2021 crates/rustcta-exchange-gateway/src/adapters/weex/*.rs`
- `python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/weex/endpoint_mapping.yaml`
- `CARGO_TARGET_DIR=/tmp/rustcta-weex-target cargo test -p rustcta-exchange-gateway weex --lib`
- `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/weex-task-check cargo check -p rustcta-exchange-gateway --lib`
- `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/weex-task-check cargo test -p rustcta-exchange-gateway weex -- --nocapture`
- `CARGO_TARGET_DIR=target/weex-task15-check2 cargo test -p rustcta-exchange-gateway weex --lib --message-format short`
- `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/weex-app-check cargo test -p rustcta-gateway config_should_wire_weex_private_gateway_adapter -- --nocapture`
- `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/weex-app-check cargo test -p rustcta-gateway config_should_parse_adapters_and_redirection_urls_without_secret_fields -- --nocapture`

The 2026-06-08 endpoint mapping validation passed for the WEEX mapping. The
latest WEEX filtered Rust test rerun is currently blocked before WEEX tests can
execute by unrelated parallel adapter compile failures and missing fixtures in
`blofin`, `cryptocom`, `poloniex`, `whitebit`, plus several non-WEEX
`StreamRuntimeCapability`/`HeartbeatCapability` initializer mismatches. No
WEEX-specific compile error appeared in that latest filtered output after the
WEEX v2 and fixture test updates.
