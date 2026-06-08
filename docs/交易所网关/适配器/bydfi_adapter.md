# BYDFi Gateway Adapter

Adapter id: `bydfi`

## Scope

This adapter implements the verified BYDFi USDT perpetual API surface for the
RustCTA gateway. BYDFi Spot is audited but kept unsupported because the current
official Spot section does not document a stable market/order lifecycle API.

Official sources reviewed on 2026-06-08:

- Introduction: https://developers.bydfi.com/en/intro
- API domain: https://developers.bydfi.com/en/domainName
- Signature: https://developers.bydfi.com/en/signature
- Request/rate-limit rules: https://developers.bydfi.com/en/requestInteraction
- Futures market: https://developers.bydfi.com/en/futures/market
- Futures account: https://developers.bydfi.com/en/futures/user
- Futures trade: https://developers.bydfi.com/en/futures/trade
- Futures public WS: https://developers.bydfi.com/en/futures/websocket-market
- Futures account WS: https://developers.bydfi.com/en/futures/websocket-account

## Domains

- REST: `https://api.bydfi.com/api`
- Public futures WebSocket: `wss://stream.bydfi.com/v1/public/fapi`
- Testnet: unsupported; no official sandbox URL found.
- Private WebSocket: unsupported at runtime; docs show `LOGIN` and event payloads
  but not a stable connection URL.

## Authentication

Private REST uses headers:

- `X-API-KEY`
- `X-API-TIMESTAMP`
- `X-API-SIGNATURE`
- `Content-Type: application/json`
- `Accept-Language: en-US`

The signature is lowercase hex HMAC-SHA256 over
`accessKey + timestamp + queryString + body`. The adapter keeps GET requests
query-only and POST requests body-only to avoid ambiguous mixed signing.

Private WebSocket login payload is implemented as an offline request-spec helper:
`apiKey + timestamp` signed with HMAC-SHA256. It is not advertised as a live
runtime capability until BYDFi documents the private WS URL.

## Implemented

- Perpetual symbol rules: `GET /v1/fapi/market/exchange_info`
- Perpetual order book snapshot: `GET /v1/fapi/market/depth`
- Mark price, current funding and funding history helpers
- Private REST balances, positions, open/query orders, recent fills
- Private REST place, amend, cancel, batch place, batch cancel and cancel-all
- Public WS subscribe/unsubscribe payloads, ping/pong policy and depth parser
- Private WS login payload and parser fixtures for account/order events
- Named gateway registration, app config wiring, disabled config example and
  endpoint mapping

## Official WebSocket Order Book Detail

官方核验见 [WebSocket 官方核验 P7 补充交易所盘口细项二](../WebSocket官方核验_P7_补充交易所盘口细项二.md)。BYDFi Futures public WS 支持 path stream 和 JSON `SUBSCRIBE` 两种形式，例如 `wss://stream.bydfi.com/v1/public/fapi/BTC-USDT@depth@100ms` 或 `BTC-USDT@depth10@100ms`。

增量 depth 支持 1000ms 或 100ms；limited depth 支持 10/50/100 档，也可加 `@100ms`。payload 是 `depthUpdate`，包含 event time、bids、asks；官方未见 update id 或 checksum。实现时必须用 REST `/v1/fapi/market/depth` 初始化/重建，断线后重新订阅。

## Unsupported Boundaries

- Spot trading, spot order book and spot private order lifecycle
- Real private WebSocket runtime connection
- Testnet/sandbox
- Withdrawals, transfers, copy/grid/funding account movements
- Leverage, margin mode and position mode mutations in the unified trait
- Order lists/OCO/OTO and quote-sized market orders

## Validation

Allowed commands:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bydfi/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway bydfi --lib --message-format short
cargo test -p rustcta-gateway bydfi --message-format short
```

Do not run `cargo build` for this task.
