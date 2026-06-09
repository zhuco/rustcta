# Coinmetro Gateway Adapter

Status date: 2026-06-08.

Coinmetro is implemented as a conservative Spot-only gateway adapter. REST base URL defaults to `https://api.coinmetro.com`; demo can be selected by overriding the base URL to `https://api.coinmetro.com/open`. WebSocket URL defaults to `wss://api.coinmetro.com/ws`.

## Implemented Surface

- Public REST: markets and order book snapshots.
- Private REST request construction/runtime hooks: balances, place order, cancel order, query order, open orders, recent fills.
- WebSocket specs: public query-string `pairs` subscription URL, private JWT query-token URL, parser fixtures for `bookUpdate` and `tick`.
- Auth: JWT `Authorization: Bearer <token>`; optional `X-Device-ID` for long-lived device tokens.

## Public WebSocket Order Book

Official WebSocket subscriptions use the `pairs` query string, for example `wss://api.coinmetro.com/ws?pairs=BTCEUR`. Subscribed pairs receive `bookUpdate` changes with `seqNumber` and CRC32 `checksum`. The checksum source is the lexicographically sorted non-zero `ask` price/quantity concatenation followed by the same sorted `bid` concatenation, without separators. `tick` messages include current `ask` and `bid` and are treated as BBO.

The reviewed docs do not publish a fixed millisecond interval or a fixed depth parameter. Runtime must initialize the local book with `GET /exchange/book/{pair}`, apply `bookUpdate` messages in monotonically increasing `seqNumber` order, compare CRC32 against the maintained book after each update, and rebuild from REST after reconnect, stale stream, sequence duplicate/regression, parse error, or checksum mismatch.

## Capability Matrix

| Surface | Runtime |
| --- | --- |
| Products | Spot implemented; Margin/Swap/TRAM-like product boundary is `项目未实现`; standard futures/perpetual/options are `交易所不支持合约` under current official sources |
| Public REST | markets, order book snapshot |
| Private REST | balances, order lifecycle, open orders, recent fills when bearer token is configured |
| Public WS | native query-string `pairs` subscription; REST order book snapshot is the resync fallback |
| Private WS | JWT query-token URL for order/wallet updates; REST reconciliation fallback |
| Order types | market, limit |
| Batch/cancel-all | Unsupported |
| Margin/swap/TRAM/funding | `项目未实现` or outside gateway scope depending on product; not connected by current adapter |
| Positions/reduce-only/post-only/client order id | Unsupported |

## Endpoint Mapping

`crates/rustcta-exchange-gateway/src/adapters/coinmetro/endpoint_mapping.yaml` maps:

- `GET /markets`
- `GET /exchange/book/{pair}`
- `GET /users/balances`
- `POST /exchange/orders/create`
- `PUT /exchange/orders/cancel/{orderID}`
- `GET /exchange/orders/status/{orderID}`
- `GET /exchange/orders/active`
- `GET /exchange/fills/{since?}`

Private REST operations require request specs under `tests/fixtures/exchanges/coinmetro/request_specs/`.

## Rate Limits

Coinmetro documents a general 500 calls per 10 seconds limit and stricter endpoint buckets. The mapping tags order creation/cancel as `orders`, balances as `account_slow`, public book as `market_data`, and fills/history as `history_slow`. Runtime does not add a local throttler; callers must enforce the current published limits.

## Product-Line And Unsupported Boundary

官方核验见 [产品线官方核验 P5 区域现货 CEX 第二批](../产品线官方核验_P5_区域现货_CEX第二批.md)。Coinmetro 官方资料确认 Margin 平台，API docs repo 也记录 `/swap` 相关限制；这些不能简单写成交易所不支持，应按 `项目未实现 Margin/Swap/TRAM-like product boundary` 或“当前 adapter 不接入”处理。
Mapping 中 `margin_product` 已写 `status: project_unimplemented`、
`official_gap: margin_swap_tram_like_product_boundary`、
`boundary: project_unimplemented_product_line`；标准 futures/perpetual/options
仍由 `contract_product=unsupported` 表示。
状态建议：Margin/Swap/TRAM-like surfaces 保持
`margin_product=project_unimplemented`，直到 product taxonomy、account
eligibility、collateral/position/risk/fee parsers、private order lifecycle 和
REST/WS reconciliation 完成；不要把它并入标准合约 unsupported。

Standard futures/perpetual/options are `交易所不支持合约` under current official sources. Margin orders, margin collateral, swaps, hedge/close endpoints, TRAM orders, deposits, withdrawals, fiat payment rails, transfers, positions, reduce-only, post-only, client order ids, cancel-all, shared amend, OCO/OTO/order-list, native batch place, and native batch cancel are not connected. The executable boundary fixture is `tests/fixtures/exchanges/coinmetro/unsupported_boundary.json`.

Advanced order unsupported boundary:
- `amend_order` returns `coinmetro.modify_order_unmapped_requires_native_qty_fields` because shared amend does not safely capture Coinmetro's side/currency quantity model for replacement.
- `place_order_list`, `batch_place_orders`, `batch_cancel_orders`, and `cancel_all_orders` return adapter-specific `Unsupported.operation` strings and do not call private REST.
- `endpoint_mapping.yaml` records each unsupported advanced operation with `auth: unsupported` and `native_batch: false`; `tests/fixtures/exchanges/coinmetro/unsupported_boundary.json` records the executable fixture evidence.
- `tests.rs` covers capability flags, runtime unsupported errors, mapping support, and the unsupported fixture.

## Official References

| Topic | URL |
| --- | --- |
| REST/Postman API | `https://documenter.getpostman.com/view/3653795/SVfWN6KS` |
| API docs repository / WebSocket notes | `https://github.com/CoinMetro/API-DOCS` |
| Trading API help | `https://intercom-help.eu/coinmetrohelpcenter/en/articles/317654-how-to-use-coinmetro-s-trading-api` |
| Rate limits | `https://intercom-help.eu/coinmetrohelpcenter/en/articles/317656-coinmetro-s-api-rate-limits-what-you-need-to-know` |
| API token creation | `https://intercom-help.eu/coinmetrohelpcenter/en/articles/317655-how-to-create-an-api-access-token` |

## Validation

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/coinmetro/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway coinmetro --lib --message-format short
```

`cargo build` is intentionally not part of Task A-17 validation.

## Fee Boundary

交易所不支持当前费率接口 runtime：当前 Coinmetro profile 未映射稳定 fee endpoint。
