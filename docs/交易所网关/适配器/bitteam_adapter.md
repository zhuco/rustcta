# BIT.TEAM Gateway Adapter

Status date: 2026-06-09

Adapter id: `bitteam`

Implementation status: A-09 Spot adapter. Public REST support is implemented
for Spot pair metadata and order-book snapshots. Private REST readbacks for
`query_order`, `get_open_orders`, and `get_recent_fills` are credential-gated
runtime behind `BITTEAM_PRIVATE_REST_ENABLED` plus Basic-auth credentials.
Writes, balances, fees, batch, order-list, and WebSocket runtime remain
offline/unsupported.

## Official Materials

| Area | Source | Adapter use |
| --- | --- | --- |
| Product scope | `https://bit.team/docs` describes BIT.TEAM Spot and P2P products and CCXT/API support. | Adapter declares `MarketType::Spot` only. |
| Swagger/OpenAPI | `https://bit.team/trade/api/documentation` and `https://bit.team/trade/api/documentation.json` list CCXT pairs, balance, order, order book, orders, and trades endpoints. | Public pairs/orderbook and read-only order/fill readbacks are enabled; writes and balances remain offline. |
| Auth | Swagger description documents HTTP Basic auth using `base64(key1:key2)`. | `signing.rs` builds a Basic authorization vector with sanitized fixture keys; private readbacks send Basic auth only after the runtime guard passes. |
| Spot guide | `https://bit.team/docs/api/spot_api_new/currencies_rates` links the Spot API section. | Used only as product/API navigation evidence. |

## Product Lines

| Product | MarketType | Status |
| --- | --- | --- |
| Spot | `Spot` | Public REST G1 for symbol rules and order book. |
| P2P | n/a | Unsupported; not a central exchange order-book/trading surface. |
| Perpetual/futures/options | n/a | `交易所不支持合约`; current official docs/API list Spot/P2P and CCXT-style spot endpoints, with no stable standard contracts product scope verified for this adapter. |
| Testnet | n/a | Unsupported; no stable public sandbox host verified. |

官方核验见 [产品线官方核验 P5 区域现货 CEX 第二批](../产品线官方核验_P5_区域现货_CEX第二批.md)。P2P 不属于中心化订单簿交易 surface；标准 futures/perpetual/options 写 `交易所不支持合约`。

Default REST base URL: `https://bit.team`

## Endpoint Mapping

| Gateway capability | BIT.TEAM endpoint | Current status |
| --- | --- | --- |
| Symbol rules | `GET /trade/api/ccxt/pairs` | Implemented for Spot parser fixtures. |
| Order book | `GET /trade/api/orderbooks/{symbol}` | Implemented for snapshot parser fixtures. |
| Balances | `GET /trade/api/ccxt/balance` | Request-spec-only; runtime returns `Unsupported("bitteam.balances_request_spec_only")`. |
| Place order | `POST /trade/api/ccxt/ordercreate` | Request-spec-only; runtime returns `Unsupported("bitteam.place_order_request_spec_only")`. |
| Cancel order | `POST /trade/api/ccxt/cancelorder` | Request-spec-only; runtime returns `Unsupported("bitteam.cancel_order_request_spec_only")`. |
| Query order | `GET /trade/api/ccxt/order` | Credential-gated read-only runtime; requires `exchange_order_id`. |
| Open orders | `GET /trade/api/ccxt/ordersOfUser` | Credential-gated read-only runtime; requires symbol scope. |
| Recent fills | `GET /trade/api/ccxt/tradesOfUser` | Credential-gated read-only runtime; requires symbol and context tenant/account ids. |
| Amend/order-list/batch place/cancel | Not verified | Unsupported. |
| WebSocket | 交易所不支持公共 WS 行情 | 当前官方 docs/OpenAPI 只给 REST/CCXT-style endpoints，未给 WSS endpoint 和订单簿订阅规格；REST reconciliation fallback documented for future private promotion. |

## Official Core Trading Detail

官方核验见 [核心交易官方核验 P3 第四批](../核心交易官方核验_P3_第四批.md)。BIT.TEAM OpenAPI 列出 `ordercreate`、`cancelorder`、open orders 和 user trades，所以不能写成 `交易所不支持下单/撤单`。

账户/余额接口 `GET /trade/api/ccxt/balance` 已补 `get_balances` 离线 request-spec、脱敏认证形状和响应样例；shared `get_balances` runtime 仍属未启用，剩 Basic auth read-only smoke、balance parser、credential validation 和 REST reconciliation，不能写成交易所不支持余额。

当前 public REST 已接，private read-only 的 `query_order`、`get_open_orders`、`get_recent_fills` 已补 Basic-auth runtime、parser fixtures 和 mock REST tests。`place_order`、`cancel_order`、order-list/batch 写接口仍保持离线/unsupported；补写接口前必须完成 live-dry-run guard 和真实下单风险控制。

## Authentication

BIT.TEAM documents HTTP Basic authorization for CCXT methods. The adapter has
`basic_authorization_header(api_key, api_secret)` and a sanitized signing vector:

- fixture: `tests/fixtures/exchanges/bitteam/signing_vectors/basic_auth.json`
- request specs: `tests/fixtures/exchanges/bitteam/request_specs/*.json`

Private REST remains disabled by default. Read-only runtime requires
`BITTEAM_PRIVATE_REST_ENABLED=true` and either
`BITTEAM_SPOT_API_KEY`/`BITTEAM_SPOT_API_SECRET` or
`BITTEAM_API_KEY`/`BITTEAM_API_SECRET`. Without the guard and credentials,
private readbacks fail closed with `Unsupported`; writes are still blocked even
when the read-only guard is enabled.

## Capability Boundary

Default `capabilities()` returns:

- `market_types = [Spot]`
- public REST, symbol rules, and order-book snapshots supported
- private REST, query order, open orders, and recent fills supported only when
  `BITTEAM_PRIVATE_REST_ENABLED` plus Basic-auth credentials are present
- balances, fees, place/cancel writes, batch, cancel-all, public streams, and
  private streams unsupported/offline
- `capabilities_v2.private_rest`, order history, and fills history become
  native only under the read-only runtime guard

This adapter must not promote writes until a separate validation task proves
write request semantics without real order placement.

## Validation

Non-compiling validation used for this task:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bitteam/endpoint_mapping.yaml
```

When compilation is explicitly allowed, run:

```bash
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway bitteam --lib --message-format short
```

Do not run `cargo build` for this task. `cargo test -p rustcta-gateway bitteam`
is only relevant if a future change wires Bitteam into `apps/gateway` env config.

## Fee Boundary

交易所不支持当前费率接口 runtime：private REST 为 request-spec-only，capabilities 默认把 fees 标为 unsupported。
