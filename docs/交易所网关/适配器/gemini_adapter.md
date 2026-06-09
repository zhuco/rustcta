# Gemini Gateway Adapter

Status date: 2026-06-08.

Gemini is implemented as a Spot-only gateway adapter. REST base URL defaults to `https://api.gemini.com`; public market-data WebSocket defaults to `wss://api.gemini.com/v1/marketdata`; private order-events WebSocket defaults to `wss://api.gemini.com/v1/order/events`.

Derivatives/Perpetuals are 项目未实现, not `交易所不支持合约`: Gemini official
developer docs expose derivatives REST endpoints for perpetual contracts and
derivatives-specific account operations. Region/product eligibility must be
handled separately before implementation.

2026-06-09 产品线边界收窄：`contract_product`、`derivatives_product` 和
`perpetual_product` 均保留 `project_unimplemented`，并绑定
`tests/fixtures/exchanges/gemini/request_specs/product_line_source_boundary.json`。
当前 Spot adapter 不复用到 derivatives/perpetual；后续必须先加 region/product
eligibility guard、derivatives symbol metadata、account/position/risk parser 和
separate private order runtime。
状态建议：继续保留 `contract_product` / `derivatives_product` /
`perpetual_product = 项目未实现`；Gemini Derivatives/Perpetual 官方 endpoint 线索
存在，但需要区域和产品资格 guard，不能由 Spot runtime 直接启用。

## Implemented Surface

- Public REST: symbol details and order book snapshots.
- Private REST request construction: balances, order lifecycle, cancel session orders, active orders, and trade fills.
- Signing: private REST and private WS auth use JSON payloads containing `request` and `nonce`, base64-encoded into `Gemini-Payload`, then HMAC-SHA384 hex in `Gemini-Signature`.
- WebSocket specs: public market-data URL construction and private order-events header construction.

## Capability Matrix

| Surface | Runtime |
| --- | --- |
| Products | Spot only; Derivatives/Perpetuals 项目未实现 |
| Public REST | symbol details, order book snapshot |
| Private REST | balances, limit order lifecycle, active orders, recent fills |
| Public WS | market-data URL construction |
| Private WS | order-events auth headers |
| Order types | limit, post-only |
| Market/quote-market | Unsupported |
| Amend/order-list/OCO/OTO | Unsupported; no shared spot runtime mapping |
| Batch | Unsupported; not advertised, and composed batch behavior is not exposed as a capability |
| Fees/positions/reduce-only | Unsupported |

## Official WebSocket Order Book Detail

官方核验见 [WebSocket 官方核验 P8 补充交易所盘口细项三](../WebSocket官方核验_P8_补充交易所盘口细项三.md)。Gemini public WS stream host 是 `wss://ws.gemini.com`，支持 `{symbol}@bookTicker`、`{symbol}@depth5/10/20`、`{symbol}@depth5/10/20@100ms`、`{symbol}@depth` 和 `{symbol}@depth@100ms`。

`bookTicker` 是 real-time L1/BBO；partial depth 支持 5/10/20 档，1s 或 100ms；diff depth 支持 1s 或 100ms，可用 snapshot 参数获取初始全量或 top N。partial 有 `lastUpdateId`，diff 有 `U/u`，未见 checksum；断档后用 REST order book 或 snapshot 连接参数重建。YAML 已结构化记录 `{symbol}@bookTicker`、`{symbol}@depth5/10/20@100ms|1s`、`{symbol}@depth@100ms|1s`、100ms 最快推流、1/5/10/20 档、`lastUpdateId/U/u` 和 REST `/v1/book/{symbol}` 重建边界。

## Endpoint Mapping

`crates/rustcta-exchange-gateway/src/adapters/gemini/endpoint_mapping.yaml` maps:

- `GET /v1/symbols/details`
- `GET /v1/book/{symbol}`
- `POST /v1/balances`
- `POST /v1/order/new`
- `POST /v1/order/cancel`
- `POST /v1/order/cancel/session`
- `POST /v1/order/status`
- `POST /v1/orders`
- `POST /v1/mytrades`

Private REST operations require request specs under `tests/fixtures/exchanges/gemini/request_specs/`.

## Rate Limits

The adapter tags public REST as `rest_ip`, private account reads as `key`, and order writes/cancels as `orders` in the endpoint mapping. Runtime does not implement a separate local throttler; callers should enforce Gemini's current published rate limits outside this adapter.

## Unsupported Boundary

Auction, block trading, travel-rule, withdrawals, deposits, fiat transfers, custody/clearing workflows, staking, and other funding/compliance surfaces are documented Unsupported and are not connected to runtime. Derivatives/Perpetuals are 项目未实现 in this spot adapter. Positions, market orders, quote-market orders, fees, and reduce-only are also Unsupported in the current shared spot runtime. P4 advanced order capabilities are explicit unsupported boundaries: `amend_order`, `place_order_list`/OCO/OTO, `batch_place_orders`, and `batch_cancel_orders` are not exposed. The executable boundary fixture is `tests/fixtures/exchanges/gemini/unsupported_boundary.json`; its `advanced_order_boundaries` block pins the mapping/runtime guard reasons and keeps composed batch/amend/order-list promotion disabled.

## Official References

| Topic | URL |
| --- | --- |
| REST market data | `https://docs.gemini.com/rest/market-data` |
| REST orders | `https://docs.gemini.com/rest/orders` |
| REST account | `https://docs.gemini.com/rest/account` |
| REST derivatives | `https://developer.gemini.com/trading/rest-api/derivatives` |
| WebSocket order events | `https://docs.gemini.com/websocket/order-events` |

## Validation

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/gemini/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway gemini --lib --message-format short
```

`cargo build` is intentionally not part of Task 14 validation.

## Fee Boundary

交易所不支持当前费率接口 runtime：当前 shared spot runtime 中 fees 明确 Unsupported。
