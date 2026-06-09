# EXMO Gateway Adapter

Status: `rustcta-exchange-gateway` conservative Spot REST adapter with public/private WebSocket request specs.

## Scope

- Adapter id: `exmo`
- Product line: Spot only
- Default REST base URL: `https://api.exmo.com`
- REST API version: `/v1.1`
- Public WebSocket URL: `wss://ws-api.exmo.com/v1/public`
- Private WebSocket URL: `wss://ws-api.exmo.com/v1/private`
- Config env: `RUSTCTA_EXMO_REST_BASE_URL`, `RUSTCTA_EXMO_API_KEY`, `RUSTCTA_EXMO_API_SECRET`

Official references:

| Topic | Source |
| --- | --- |
| REST API and endpoint fields | https://documenter.getpostman.com/view/10287440/SzYXWKPi |
| API documentation entry | https://support.exmo.com/hc/en-us/articles/14338236557084-API-documentation |
| Rate limits | https://support.exmo.com/hc/en-us/articles/14338264303644-API-Rate-Limits |
| WebSocket overview | https://support.exmo.com/hc/en-us/articles/14338305227676-Websocket-API |
| WebSocket client examples | https://github.com/exmo-dev/web-socket-api-php-client |

## Signing

Private REST uses `application/x-www-form-urlencoded` POST bodies. Every private request includes a monotonic numeric `nonce`. Headers:

- `Key`: API key
- `Sign`: hex HMAC-SHA512 over the exact form body using the API secret

Private WebSocket login uses `base64(HMAC-SHA512(api_key + nonce, api_secret))`.

## Endpoint Mapping

The machine-readable mapping is at `crates/rustcta-exchange-gateway/src/adapters/exmo/endpoint_mapping.yaml`.

| Standard capability | EXMO endpoint or topic | Status |
| --- | --- | --- |
| Symbol rules | `POST /v1.1/pair_settings` | Implemented |
| Order book snapshot | `POST /v1.1/order_book` | Implemented, max depth 1000 |
| Fee rates | `POST /v1.1/pair_settings` commission fields | Implemented |
| Balances | `POST /v1.1/user_info` | Implemented |
| Place order | `POST /v1.1/order_create` | Implemented for limit, market, post-only, IOC, FOK |
| Quote-sized market order | `POST /v1.1/order_create` with `market_buy_total` / `market_sell_total` | Implemented |
| Cancel order | `POST /v1.1/order_cancel` | Implemented |
| Query order | `POST /v1.1/order_trades` | Limited filled-order reconciliation |
| Open orders | `POST /v1.1/user_open_orders` | Implemented; optional pair filter; no pagination |
| Recent fills | `POST /v1.1/user_trades` | Implemented; `limit` max 100 and offset cursor |
| Public WS | `spot/trades`, `spot/ticker`, `spot/order_book_snapshots`, `spot/order_book_updates` | Subscription payload specs |
| Private WS | `spot/user_trades`, `spot/wallet`, `spot/orders` | Login/subscription payload specs |

## Official WebSocket Order Book Detail

官方核验见 [WebSocket 官方核验 P7 补充交易所盘口细项二](../WebSocket官方核验_P7_补充交易所盘口细项二.md)。EXMO public WS URL 是 `wss://ws-api.exmo.com/v1/public`，订单簿相关 stream 是 `spot/order_book_snapshots` 和 `spot/order_book_updates`。

官方摘要说明 public WS 提供 top-25 positions 和 top-400 order book changes；mapping 分别记录 snapshot depth 25 和 change depth 400。官方未给固定推流毫秒、sequence 或 checksum，记录为 no fixed ms。断线、错包或 missed ping 后必须回 REST `POST /v1.1/order_book` 重建，本 adapter 的 private WS 仍只作为低延迟提示通道。

## Unsupported Boundary

The adapter does not expose margin, futures, perpetuals, positions, native batch place/cancel, cancel-all, amend, OCO/OTO order lists, EX-CODE, wallet withdrawal/deposit, transfers, or P2P flows. These are either outside the Spot gateway contract, require high-risk wallet permissions/support approval, or have no lossless shared mapping in EXMO Spot REST v1.1.

| Capability | Current adapter status | Reason |
| --- | --- | --- |
| `amend_order` | `unsupported` | EXMO Spot REST v1.1 has no shared in-place amend endpoint mapped by the adapter. |
| `place_order_list` / OCO / OTO | `unsupported` | No lossless OCO/OTO/order-list mapping is exposed for the shared gateway contract. |
| `batch_place_orders` | `unsupported` | EXMO Spot REST v1.1 has no documented native batch place endpoint. |
| `batch_cancel_orders` | `unsupported` | EXMO Spot REST v1.1 has no documented native batch cancel endpoint. |

官方核验见 [产品线官方核验 P6 剩余区域现货 CEX](../产品线官方核验_P6_剩余区域现货_CEX.md)。EXMO 官方资料确认 EXMO Margin 和 Margin API/费率资料，因此 Margin 写 `项目未实现 Margin`。官方 perpetual futures 资料当前写 active development，未见已上线标准 futures/perpetual API；当前标准 futures/perpetual/options 写 `交易所不支持合约`，上线后重核。
Mapping 中 `margin_product` 已写 `status: project_unimplemented`、
`official_gap: exmo_margin_api`、`boundary: project_unimplemented_product_line`；
`perpetual_product` 和 `futures_product` 现在是明确的 unsupported operation，
用于表达当前未见稳定 live 标准合约 API，而不是否定 Margin 缺口。
状态建议：EXMO Margin 保持 `project_unimplemented`，直到 live API stability、
account eligibility、leverage/collateral/risk policy、margin balance/position
parsers 和 product-scoped order lifecycle 完成；perpetual/futures 资料上线后
再从标准合约 unsupported 重新审计。

`query_order` is intentionally documented as limited: EXMO Spot REST has `order_trades` for an order's deals but no general single-order status endpoint. Ambiguous states should reconcile through `get_open_orders`, `query_order`, and `get_recent_fills`.

## Reconciliation And Safety

Timeouts or ambiguous order states must reconcile through:

1. `get_open_orders` for active orders.
2. `query_order` via `order_trades` for filled-order evidence.
3. `get_recent_fills` using `limit`/`offset`.

Private WebSocket is a latency path only. REST remains the source of truth after reconnects, missed pings, or payload gaps.

## Validation

Allowed targeted validation:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/exmo/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway exmo --lib --message-format short
cargo test -p rustcta-gateway exmo --message-format short
```

Do not run `cargo build` for this task.
