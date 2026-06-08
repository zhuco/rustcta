# ZebPay Gateway Adapter

Status date: 2026-06-08

`zebpay` is the A-39 regional spot adapter from
`docs/交易所网关/总览/exchange_gateway_remaining_venues_one_ai_one_exchange_zh.md`. It is a
scan-only `MarketType::Spot` adapter: public REST market metadata and order book
snapshots are implemented, while private REST, trading, and WebSocket runtime
remain unsupported until API onboarding, KYC, permission scopes, and bearer-token
lifecycle are verified. P6 official product-line verification confirms ZebPay
also documents Futures/Perpetual API surfaces; those are `项目未实现
Futures/Perpetual`, not `交易所不支持合约`.

## Official Sources

| Area | Source | Notes |
| --- | --- | --- |
| Spot market REST | `https://docs.zebpay.com/` | Documents `/market`, `/market/{trade_pair}/ticker`, `/market/{trade_pair}/book`, and `/market/{trade_pair}/book_long`. |
| Private spot REST | `https://docs.zebpay.com/` | Documents `/orders`, `/orders/{orderId}`, `/orders/{orderId}/fills`, `/orders/CancelAll`, `/wallet/balance`, and `/tradefees/{trade_pair}`. |
| API service / onboarding | `https://zebpay.com/in/api-services-for-spot-and-futures` | Public material says API access is reviewed by ZebPay and covers spot/futures markets; this adapter does not enable futures. |

## Product Scope

- Adapter id: `zebpay`
- Product: spot implemented; Futures/Perpetual `项目未实现`
- Default REST base URL: `https://www.zebapi.com`
- Default group query: `singapore`
- Exchange symbol format: `BASE-QUOTE`, for example `BTC-INR`
- Unsupported: futures/perpetual runtime in this spot adapter, live private REST, live trading, public WS,
  private WS, withdrawals, transfers, and token refresh runtime. 当前官方 docs 覆盖 Spot/Futures REST market/book/orderBook，未见公共订单簿 WS stream，写 `交易所不支持公共 WS 行情`。

## Authentication

Private REST request specs use the documented header shape:

- `client_id`
- `timestamp`
- `RequestId`
- `Authorization: Bearer <access_token>`

The adapter stores `client_id`, `client_secret`, and `access_token` fields for
offline request-spec validation, but capabilities keep private REST disabled.
`client_secret` is treated as token-onboarding material and is never emitted in
fixtures.

## Endpoint Mapping

The authoritative mapping is
`crates/rustcta-exchange-gateway/src/adapters/zebpay/endpoint_mapping.yaml`.

Implemented public endpoints:

- `GET /market?group={group}` -> `get_symbol_rules`
- `GET /market/{trade_pair}/book?group={group}&converted=0` -> shallow
  `get_order_book`
- `GET /market/{trade_pair}/book_long?group={group}&converted=0` -> depth above
  15, capped at 50

Private request-spec-only endpoints:

- `GET /wallet/balance?trade_pair={trade_pair}`
- `POST /orders`
- `DELETE /orders/{order_id}`
- `DELETE /orders/CancelAll?trade_pair={trade_pair}`
- `GET /orders?trade_pair={trade_pair}&status=pending&orderid=0&page=1&limit=500`
- `GET /orders/{order_id}/fills`

## Official Core Trading Detail

官方核心交易核验见 [核心交易官方核验 P2 第三批](../核心交易官方核验_P2_第三批.md)。ZebPay Trade API 支持 `POST /orders` 新建 limit order、`DELETE /orders/{orderId}` 撤单、`DELETE /orders/CancelAll`、`GET /orders` 和 `GET /orders/{orderId}/fills`。

当前 private REST/trading 仍是 request-spec-only，runtime unsupported。这是 `项目未实现 bearer token/onboarding/private runtime`，不是 `交易所不支持下单/撤单`；Futures/Perpetual 仍按产品线文档写 `项目未实现`。

## Capability Boundary

- `supports_public_rest = true`
- `supports_symbol_rules = true`
- `supports_order_book_snapshot = true`
- `supports_private_rest = false`
- `supports_place_order = false`
- `supports_cancel_order = false`
- `supports_public_streams = false`
- `supports_private_streams = false`

Private operations return explicit `Unsupported` errors such as
`zebpay.place_order_request_spec_only`. REST reconciliation fallback is documented
over `/orders` and `/orders/{order_id}/fills`, but remains request-spec-only.

## Fixtures

- `tests/fixtures/exchanges/zebpay/symbol_rules_success.json`
- `tests/fixtures/exchanges/zebpay/order_book_success.json`
- `tests/fixtures/exchanges/zebpay/request_specs/*.json`
- `tests/fixtures/exchanges/zebpay/signing_vectors/bearer_auth.json`
- `tests/fixtures/exchanges/zebpay/ws/public_streams_unsupported.json`
- `tests/fixtures/exchanges/zebpay/unsupported_boundary.json`

All credentials, tokens, request ids, and order ids are synthetic or redacted.

## Validation

Allowed commands for this adapter:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/zebpay/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway zebpay --lib --message-format short
cargo test -p rustcta-gateway zebpay --message-format short
```

Do not run `cargo build`, release builds, app/web builds, live `cargo run`, real
orders, cancels, withdrawals, transfers, or long-running private streams.
