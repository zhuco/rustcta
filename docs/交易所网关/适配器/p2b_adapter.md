# P2B Gateway Adapter

Status date: 2026-06-09

Adapter id: `p2b`

Implementation status: A-32 Spot adapter. Public REST support is implemented
for Spot market metadata and two-sided order-book snapshots. Private REST
readbacks for `query_order`, `get_open_orders`, and `get_recent_fills` are
credential-gated runtime. Trading writes, batch/order-list, balances, fees, and
private streams remain offline/unsupported.

## Official Materials

| Area | Source | Adapter use |
| --- | --- | --- |
| REST base URL | `https://api.p2pb2b.com` in the P2B API docs. | Default `rest_base_url`. |
| Public markets | `GET /api/v2/public/markets`. | `get_symbol_rules` parser fixture. |
| Public order book | `GET /api/v2/public/book?market=...&side=...&offset=0&limit=...`. | `get_order_book` issues one buy request and one sell request, then merges the two sides into a snapshot. |
| Private REST | `POST /api/v2/account/balances`, `/api/v2/order/new`, `/api/v2/order/cancel`, `/api/v2/account/order_history`, `/api/v2/orders`, `/api/v2/account/market_deals`. | Read-only order history/open orders/fills are runtime behind `P2B_PRIVATE_REST_ENABLED`/`P2PB2B_PRIVATE_REST_ENABLED` plus credentials; writes and balances stay offline. |
| Auth | Private examples use `X-TXC-APIKEY`, `X-TXC-PAYLOAD`, and `X-TXC-SIGNATURE`. | `signing.rs` builds base64 JSON payload and HMAC-SHA512 hex signature from sanitized fixture keys. |
| WebSocket | P2B WSS public docs list `wss://apiws.p2pb2b.com/`, `server.ping`, and `depth.subscribe`. | Public depth subscribe runtime is implemented as best-effort order book updates; `depth.update` has no documented sequence/checksum, so REST snapshot rebuild is required after suspected loss. |

## Product Lines

| Product | MarketType | Status |
| --- | --- | --- |
| Spot | `Spot` | Public REST for symbol rules/order book plus guarded read-only private order readbacks. |
| P2P | n/a | Unsupported; not a central exchange order-book/trading surface. |
| Perpetual/futures | n/a | `交易所不支持合约`; P6 official verification found only spot/P2P API surfaces. |
| Testnet | n/a | Unsupported; no stable public sandbox host verified. |

Default REST base URL: `https://api.p2pb2b.com`

## Endpoint Mapping

| Gateway capability | P2B endpoint | Current status |
| --- | --- | --- |
| Symbol rules | `GET /api/v2/public/markets` | Implemented for Spot parser fixtures. |
| Order book | `GET /api/v2/public/book` with `side=buy` and `side=sell` | Implemented for snapshot parser fixtures. |
| Balances | `POST /api/v2/account/balances` | Request-spec-only; runtime returns `Unsupported("p2b.balances_request_spec_only")`. |
| Place order | `POST /api/v2/order/new` | Request-spec-only; runtime returns `Unsupported("p2b.place_order_request_spec_only")`. |
| Cancel order | `POST /api/v2/order/cancel` | Request-spec-only; runtime returns `Unsupported("p2b.cancel_order_request_spec_only")`. |
| Query order | `POST /api/v2/account/order_history` | Runtime readback; requires `exchange_order_id`, symbol scope, private REST env guard, and API key/secret. |
| Open orders | `POST /api/v2/orders` | Runtime readback; requires symbol scope, private REST env guard, and API key/secret. |
| Recent fills | `POST /api/v2/account/market_deals` | Runtime readback; requires symbol scope, context tenant/account, private REST env guard, and API key/secret. |
| Batch place/cancel | Not verified | Unsupported. |
| WebSocket | `wss://apiws.p2pb2b.com/` | Public depth runtime supported without sequence/checksum; private streams unsupported. |

## Official Core Trading Detail

官方核心交易核验见 [核心交易官方核验 P1 第二批](../核心交易官方核验_P1_第二批.md)。P2B 官方 REST API 支持 `POST /api/v2/order/new`、`POST /api/v2/order/cancel`、`POST /api/v2/orders` 和 `POST /api/v2/account/order_history`。

账户/余额接口 `POST /api/v2/account/balances` 已补 `get_balances` 离线 request-spec、脱敏签名形状和响应样例；shared `get_balances` runtime 仍属未启用，剩 signed private read runtime、balances parser、read-only auth smoke 和 REST reconciliation，不能写成交易所不支持余额。

当前 private readback endpoints 已作为 guarded runtime 接入：`query_order` 使用
`/api/v2/account/order_history` 并按 `exchange_order_id` 过滤，`get_open_orders`
使用 `/api/v2/orders`，`get_recent_fills` 使用
`/api/v2/account/market_deals`。下单、撤单、批量、订单列表、余额和费率仍是离线/未启用边界；
不能把写接口描述成 live runtime，也不能把余额描述成交易所不支持。

## Authentication

Private REST examples require a JSON body containing `request` and `nonce`.
P2B signs the base64-encoded JSON payload with HMAC-SHA512 and sends:

- `X-TXC-APIKEY`
- `X-TXC-PAYLOAD`
- `X-TXC-SIGNATURE`

Fixtures:

- signing vector: `tests/fixtures/exchanges/p2b/signing_vectors/private_headers.json`
- request specs: `tests/fixtures/exchanges/p2b/request_specs/*.json`

Private REST remains disabled by default even when credentials are present.
Readbacks fail closed unless `P2B_PRIVATE_REST_ENABLED` or
`P2PB2B_PRIVATE_REST_ENABLED` is true and the adapter has
`P2B_SPOT_API_KEY`/`P2B_SPOT_API_SECRET`, `P2B_API_KEY`/`P2B_API_SECRET`, or
`P2PB2B_API_KEY`/`P2PB2B_API_SECRET`.

## Capability Boundary

Default `capabilities()` returns:

- `market_types = [Spot]`
- public REST, symbol rules, order-book snapshots, and public depth stream supported
- private REST, `query_order`, `get_open_orders`, and `get_recent_fills` supported
  as fail-closed readbacks
- balances, fees, place/cancel, batch, cancel-all, order-list, amend, quote-market
  order, and private streams unsupported/offline

This adapter must not promote write support until a separate validation task
proves write request semantics without real order placement.

## Validation

Validation for this readback promotion:

```bash
cargo test -p rustcta-exchange-gateway p2b --lib --message-format short
cargo check -p rustcta-exchange-gateway --lib --message-format short
python3 scripts/validate_exchange_endpoint_mapping.py
```

## Fee Boundary

交易所不支持当前费率接口 runtime：当前 private fees unsupported，未验证 maker/taker fee readback。
