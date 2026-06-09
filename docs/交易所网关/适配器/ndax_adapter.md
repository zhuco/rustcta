# NDAX Gateway Adapter

Status: A-28 NDAX CAD spot scan-only adapter for `rustcta-exchange-gateway`.

## Scope

This adapter covers NDAX Canada spot markets, with CAD pairs first. Runtime
support is deliberately limited to public market data plus credential-gated
read-only private order/fill readbacks:

- `get_symbol_rules` from `GetInstruments`.
- `get_order_book` from `GetL2Snapshot`.
- `query_order`, `get_open_orders`, and `get_recent_fills` through signed REST
  only when `NDAX_PRIVATE_REST_ENABLED` and API key/secret/user/account/OMS
  scope are present.
- Private writes, private WebSocket, fiat ledger, transfers, withdrawals,
  futures, perpetuals, margin, leverage, and batch order operations are
  `Unsupported` at runtime.
- P6 official product-line verification found margin-enabled/lending/profit-loss
  account fields; if the venue/account exposes those semantics they are
  `项目未实现 Margin account semantics`. Standard futures/perpetual/options are
  `交易所不支持合约`.
NDAX 的 Margin/Margin-like account semantics 在当前 adapter 中属于 `项目未实现`，标准 futures/perpetual/options 仍按当前官方 API 范围写 `交易所不支持合约`。
Mapping 中 `margin_product` 已写 `status: project_unimplemented`、
`official_gap: margin_account_semantics`、`boundary: project_unimplemented_product_line`；
并绑定
`tests/fixtures/exchanges/ndax/request_specs/product_line_source_boundary.json`
记录 required audit gaps；
`perpetual_product` 和 `futures_product` 是明确 unsupported operation，表示当前
standard spot instruments 之外未接标准合约产品线。`options_product` 同样按当前
官方/API 范围写 explicit unsupported。
状态建议：Margin-like account semantics 保持 `project_unimplemented`，直到
account mode/eligibility、leverage/risk、balances/positions/PnL parsers、
product-scoped private lifecycle 和 reconciliation 完成；标准
futures/perpetual/options 继续保持 explicit unsupported。

## Official Sources

| Surface | Source | Adapter treatment |
| --- | --- | --- |
| Public/user API | `https://apidoc.ndax.io/#getinstruments`, `#getproducts`, `#getl2snapshot` | Implemented as scan-only public gateway calls with parser fixtures |
| Orders/account API | `https://apidoc.ndax.io/#getopenorders`, `#getorderstatus`, `#gettradeshistory` | Credential-gated read-only runtime |
| Order write API | `https://apidoc.ndax.io/#sendorder`, `#cancelorder` | Offline request-spec only |
| Current REST signing/base URLs | `https://docs.ndax.in/v3/private-endpoints/rest-api` | HMAC signing vector, redacted headers, and signed readback tests |
| WebSocket gateway | `wss://api.ndax.io/WSGateway/` | Public/private payload helpers only; no live private stream |

The public API documentation uses an AlphaPoint-style `{m,i,n,o}` envelope. The
newer GitBook REST documentation uses `https://api.ndax.in` and sandbox hosts.
Because these surfaces differ, private writes are not promoted to live runtime.

## Configuration

Default public REST gateway base URL: `https://api.ndax.in`

Default WebSocket URL: `wss://api.ndax.io/WSGateway/`

Environment overrides:

- `RUSTCTA_NDAX_REST_BASE_URL`
- `RUSTCTA_NDAX_WS_URL`
- `RUSTCTA_NDAX_OMS_ID`
- `RUSTCTA_NDAX_API_KEY`
- `RUSTCTA_NDAX_API_SECRET`
- `RUSTCTA_NDAX_API_PASSPHRASE`
- `RUSTCTA_NDAX_USER_ID`
- `RUSTCTA_NDAX_ACCOUNT_ID`
- `RUSTCTA_NDAX_PRIVATE_REST_ENABLED`

## Auth

The offline REST signing vector follows the current NDAX REST pattern:

```text
payload = timestamp + uppercase(method) + request_path + body
signature = base64(hmac_sha256(base64_decode(api_secret), payload))
```

Request-spec fixtures use redacted `X-NDAX-APIKEY`, `X-NDAX-SIGNATURE`, and
`X-NDAX-TIMESTAMP` headers. The adapter never logs or stores real credentials in
fixtures.

## Endpoint Mapping

See
`crates/rustcta-exchange-gateway/src/adapters/ndax/endpoint_mapping.yaml`.

Implemented runtime endpoints:

- `POST /WSGateway/` with `n=GetInstruments`
- `POST /WSGateway/` with `n=GetL2Snapshot`

Credential-gated read-only private runtime endpoints:

- `GET /orders/{order_id}` for `query_order`.
- `GET /orders` for symbol-scoped `get_open_orders`.
- `GET /fills` for symbol-scoped `get_recent_fills`.

Offline-only private endpoints:

- balances/accounts read
- limit order placement
- order cancel

## WebSocket

Payload helpers cover:

- public `SubscribeLevel2`, `SubscribeTrades`, `SubscribeTicker`
- private `AuthenticateUser` payload shape
- heartbeat policy: 30s ping interval, 45s pong timeout, 60s stale-message
  threshold

Private streams remain disabled. Private state reconciliation uses REST
readbacks when the private REST guard is enabled.

## Official WebSocket Order Book Detail

P9 official verification confirms NDAX WSGateway supports `SubscribeLevel1`,
`SubscribeLevel2`, and `GetL2Snapshot` through
`wss://api.ndax.io/WSGateway/`. Level 1 is BBO, Level 2 snapshot accepts a
user-selected `Depth`, and the public docs do not state a fixed push interval or
checksum. The mapping records the WSGateway envelope, `SubscribeLevel1` depth1
BBO, `SubscribeLevel2`, the `Depth` parameter, no-fixed-ms/no-sequence/
no-checksum risk, and resync by requesting a fresh `GetL2Snapshot` rebuild.

## Unsupported Boundary

Unsupported runtime surfaces:

- private write REST
- private WebSocket runtime
- fiat ledger/deposit/withdraw/transfer
- withdrawals and transfers
- futures, perpetuals, margin, leverage, positions, funding, mark price, open
  interest, risk tiers
- native batch place/cancel and order lists

## Validation

Allowed validation commands:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/ndax/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway ndax --lib --message-format short
cargo test -p rustcta-gateway ndax --message-format short
```

Do not run `cargo build`, release builds, live private streams, real orders,
withdrawals, transfers, or app/web builds for this task.

## Fee Boundary

交易所不支持当前费率接口 runtime：当前 adapter/API 证据只到 public/user API、risk tiers 和 offline private specs，未核到 maker/taker fee readback。
