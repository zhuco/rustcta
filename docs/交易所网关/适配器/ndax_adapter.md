# NDAX Gateway Adapter

Status: A-28 NDAX CAD spot scan-only adapter for `rustcta-exchange-gateway`.

## Scope

This adapter covers NDAX Canada spot markets, with CAD pairs first. Runtime
support is deliberately limited to public market data:

- `get_symbol_rules` from `GetInstruments`.
- `get_order_book` from `GetL2Snapshot`.
- Private REST, private WebSocket, fiat ledger, transfers, withdrawals,
  futures, perpetuals, margin, leverage, and batch order operations are
  `Unsupported` at runtime.
- P6 official product-line verification found margin-enabled/lending/profit-loss
  account fields; if the venue/account exposes those semantics they are
  `项目未实现 Margin account semantics`. Standard futures/perpetual/options are
  `交易所不支持合约`.

## Official Sources

| Surface | Source | Adapter treatment |
| --- | --- | --- |
| Public/user API | `https://apidoc.ndax.io/#getinstruments`, `#getproducts`, `#getl2snapshot` | Implemented as scan-only public gateway calls with parser fixtures |
| Orders/account API | `https://apidoc.ndax.io/#sendorder`, `#cancelorder`, `#getopenorders` | Offline request-spec only |
| Current REST signing/base URLs | `https://docs.ndax.in/v3/private-endpoints/rest-api` | HMAC signing vector and redacted headers only |
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

Offline-only private endpoints:

- balances/accounts read
- open orders
- fills/history
- limit order placement
- order cancel/query

## WebSocket

Payload helpers cover:

- public `SubscribeLevel2`, `SubscribeTrades`, `SubscribeTicker`
- private `AuthenticateUser` payload shape
- heartbeat policy: 30s ping interval, 45s pong timeout, 60s stale-message
  threshold

Private streams remain disabled. Private state reconciliation is documented as
REST fallback only after a separate live-dry-run promotion.

## Official WebSocket Order Book Detail

P9 official verification confirms NDAX WSGateway supports `SubscribeLevel1`,
`SubscribeLevel2`, and `GetL2Snapshot` through
`wss://api.ndax.io/WSGateway/`. Level 1 is BBO, Level 2 snapshot accepts a
user-selected `Depth`, and the public docs do not state a fixed push interval or
checksum. Mapping should record the WSGateway envelope, Level1/Level2 channels,
Depth parameter, no-fixed-ms/no-checksum risk, and resync by requesting a fresh
L2 snapshot.

## Unsupported Boundary

Unsupported runtime surfaces:

- private write REST
- private read REST
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
