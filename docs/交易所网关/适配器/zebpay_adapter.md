# ZebPay Gateway Adapter

Status date: 2026-06-08

`zebpay` is the A-39 regional spot adapter from
`docs/交易所网关/总览/exchange_gateway_remaining_venues_one_ai_one_exchange_zh.md`. It is a
`MarketType::Spot` adapter: public REST market metadata, order book snapshots,
and guarded private order/fill readbacks are implemented. Private writes,
balances, fees, and WebSocket runtime remain offline/unsupported until API
onboarding, KYC, permission scopes, parsers, and bearer-token lifecycle are
verified. P6 official product-line verification confirms ZebPay
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
- Guarded private REST readbacks: `query_order`, `get_open_orders`, and
  `get_recent_fills`; require `ZEBPAY_PRIVATE_REST_ENABLED` or
  `RUSTCTA_ZEBPAY_PRIVATE_REST_ENABLED` plus client id, client secret, and bearer
  access token.
- Unsupported/offline: futures/perpetual runtime in this spot adapter, live write
  trading, balances, fees, public WS, private WS, withdrawals, transfers, and
  token refresh runtime. 当前官方 docs 覆盖 Spot/Futures REST market/book/orderBook，未见公共订单簿 WS stream，写 `交易所不支持公共 WS 行情`。

## Authentication

Private REST request specs use the documented header shape:

- `client_id`
- `timestamp`
- `RequestId`
- `Authorization: Bearer <access_token>`

The adapter stores `client_id`, `client_secret`, and `access_token` fields.
Read-only order/fill REST runtime fails closed unless the explicit private REST
guard and all three credential fields are configured. `client_secret` is treated
as token-onboarding material and is never emitted in fixtures.

## Endpoint Mapping

The authoritative mapping is
`crates/rustcta-exchange-gateway/src/adapters/zebpay/endpoint_mapping.yaml`.

Implemented public endpoints:

- `GET /market?group={group}` -> `get_symbol_rules`
- `GET /market/{trade_pair}/book?group={group}&converted=0` -> shallow
  `get_order_book`
- `GET /market/{trade_pair}/book_long?group={group}&converted=0` -> depth above
  15, capped at 50

Guarded private readback endpoints:

- `GET /orders?trade_pair={trade_pair}&status=pending&orderid=0&page=1&limit=500`
- `GET /orders?trade_pair={trade_pair}&orderid={order_id}&page=1&limit=100`
- `GET /orders/{order_id}/fills`

Private offline/spec-only endpoints:

- `GET /wallet/balance?trade_pair={trade_pair}`
- `POST /orders`
- `DELETE /orders/{order_id}`
- `DELETE /orders/CancelAll?trade_pair={trade_pair}`

账户/余额接口 `/wallet/balance` 已补 `get_balances` 离线 request-spec、bearer/client headers 和响应样例；shared `get_balances` runtime 仍属未启用，剩 bearer token read-only auth、wallet balance parser 和 onboarding/region guard，不能写成交易所不支持余额。

## Official Core Trading Detail

官方核心交易核验见 [核心交易官方核验 P2 第三批](../核心交易官方核验_P2_第三批.md)。ZebPay Trade API 支持 `POST /orders` 新建 limit order、`DELETE /orders/{orderId}` 撤单、`DELETE /orders/CancelAll`、`GET /orders` 和 `GET /orders/{orderId}/fills`。

当前 private REST 仅提升 `GET /orders` query/open readbacks 和
`GET /orders/{orderId}/fills` fills readback；运行时默认 fail closed，必须显式开启
`ZEBPAY_PRIVATE_REST_ENABLED` 或 `RUSTCTA_ZEBPAY_PRIVATE_REST_ENABLED` 并提供
client id、client secret、bearer access token。`POST /orders`、`DELETE
/orders/{orderId}` 和 `DELETE /orders/CancelAll` 继续写 `离线`，不是 live runtime，
也不是 `交易所不支持下单/撤单`；Futures/Perpetual 仍按产品线文档写 `项目未实现`。

2026-06-09 产品线边界收窄：`contract_product` 和 `futures_product`
绑定 `tests/fixtures/exchanges/zebpay/request_specs/product_line_source_boundary.json`。
当前 scan-only Spot adapter 不启用 futures/perpetual runtime；后续必须经过 ZebPay API
onboarding、product-scope guard、futures endpoint specs、positions/margin parser 和 bearer-token
dry-run validation。本轮补强 `capabilities_v2` 中的 `contract_product` /
`futures_product` runtime-disabled endpoint placeholders，并在 fixture 中列明
futures/perpetual market metadata、public book/ticker/trades、margin/positions/risk
和 order/open/fills 等 required endpoints；非 Spot `get_positions` 请求继续被
`zebpay.non_spot_market_type` 拦截。
状态建议：继续保留 `contract_product` / `futures_product = 项目未实现`；ZebPay
API service 对 Spot/Futures 的官方线索不能写成交易所不支持，但 futures/perpetual
endpoint specs、onboarding、positions/margin parser 和 bearer-token dry-run guard
补齐前不能启用 runtime。

## Capability Boundary

- `supports_public_rest = true`
- `supports_symbol_rules = true`
- `supports_order_book_snapshot = true`
- `supports_private_rest = guarded read-only`
- `supports_place_order = false`
- `supports_cancel_order = false`
- `supports_query_order = guarded read-only`
- `supports_open_orders = guarded read-only`
- `supports_recent_fills = guarded read-only`
- `supports_public_streams = false`
- `supports_private_streams = false`

Private operations return explicit `Unsupported` errors such as
`zebpay.place_order_request_spec_only` for writes and account/fee boundaries.
REST reconciliation over `/orders` and `/orders/{order_id}/fills` is available
only when the private readback guard and credentials are configured.

## Fixtures

- `tests/fixtures/exchanges/zebpay/symbol_rules_success.json`
- `tests/fixtures/exchanges/zebpay/order_book_success.json`
- `tests/fixtures/exchanges/zebpay/order_success.json`
- `tests/fixtures/exchanges/zebpay/open_orders_success.json`
- `tests/fixtures/exchanges/zebpay/recent_fills_success.json`
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

## Fee Boundary

ZebPay `/tradefees/{trade_pair}` 可作为费率来源；当前已补 `request_specs/get_fees_tradefees.json` 离线 request-spec 边界。shared `get_fees` runtime 仍属项目未实现/未启用；补齐前需完成 bearer/signature auth guard、maker/taker parser 和 onboarding/region guard。
