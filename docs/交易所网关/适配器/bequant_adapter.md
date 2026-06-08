# Bequant Gateway Adapter

Status date: 2026-06-08

This adapter covers Bequant as task A-04 from
`docs/交易所网关/总览/exchange_gateway_remaining_venues_one_ai_one_exchange_zh.md`.

## Scope

- Adapter id: `bequant`
- Product mapped to `ExchangeClient`: Spot
- API family: Bequant v3 / HitBTC v3-style REST and WebSocket profile
- REST base URL: `https://api.bequant.io/api/3`
- Public WebSocket: `wss://api.bequant.io/api/3/ws/public`
- Trading WebSocket: `wss://api.bequant.io/api/3/ws/trading`
- Wallet WebSocket: `wss://api.bequant.io/api/3/ws/wallet`
- Official docs: `https://api.bequant.io/`
- API help entry: `https://support.bequant.io/en/articles/6442537-websocket-and-rest-api`

Bequant and HitBTC share the same v3 API family shape, but this adapter does
not assume byte-for-byte HitBTC coverage. Host, product scope, request fixtures,
and Unsupported boundaries are Bequant-specific.

## Authentication

Private REST uses HTTP Basic auth with `api_key:api_secret` base64 encoded in
the `Authorization` header. The official API also documents HS256 auth, but this
adapter implements Basic auth only because it is simpler to verify offline and
does not require clock-window signing.

Fixtures:

- `tests/fixtures/exchanges/bequant/signing_vectors/basic_auth.json`
- `tests/fixtures/exchanges/bequant/request_specs/place_order_limit.json`
- `tests/fixtures/exchanges/bequant/request_specs/cancel_order.json`
- `tests/fixtures/exchanges/bequant/request_specs/cancel_all_orders.json`

## Capabilities

Implemented:

- Spot symbol rules from `GET /public/symbol`
- Spot order book snapshots from `GET /public/orderbook/{symbol}`
- Spot balances from `GET /spot/balance`
- Spot place order from `POST /spot/order`
- Spot cancel order by client order id from `DELETE /spot/order/{client_order_id}`
- Spot cancel-all from `DELETE /spot/order`
- Spot query order from `GET /spot/order/{client_order_id}`
- Spot open orders from `GET /spot/order`
- Spot fills from `GET /spot/history/trade`
- Spot fee readback from `GET /spot/fee/{symbol}`
- Public WS subscription payloads and order book parser fixture
- Private WS auth/subscription payload specs, disabled by default

Unsupported or deferred:

- Margin and futures/perpetual trading are `项目未实现`, because official Bequant v3 documents margin and futures endpoint families.
- Quote-sized market orders
- Amend order
- Batch place and batch cancel-by-list
- Transfers, withdrawals, subaccounts, custody, and wallet funding actions
- Production private WebSocket runtime promotion without live-key validation

## Official Product-Line Boundary

官方核验见 [产品线官方核验 P5 区域现货 CEX 第二批](../产品线官方核验_P5_区域现货_CEX第二批.md)。Bequant 官方 v3 API 有 Spot、Margin、Futures account/order/position 以及 trading WebSocket 的 margin/futures 方法。当前 adapter 只声明 Spot，所以 Margin/Futures/Perpetual 必须写 `项目未实现`，不能写成 `交易所不支持合约`。

## Rate Limits

The endpoint mapping records the official sliding one-second model: public REST
examples use 30 requests with 50 burst, general private REST uses 20/30, order
paths use 300/450, and WebSocket paths use 10/10. Runtime throttling is a follow
up; tests only verify request construction and parsers.

## Reconciliation

Private WebSocket runtime is disabled by default. Order and account state should
use REST reconciliation through `query_order`, `get_open_orders`,
`get_recent_fills`, and `get_balances` after unknown order or stream states.

## Validation

Allowed commands for this task:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bequant/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway bequant --lib --message-format short
```

Do not run `cargo build` or any live trading command for this adapter.
