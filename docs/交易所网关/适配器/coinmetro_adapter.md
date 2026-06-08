# Coinmetro Gateway Adapter

Status date: 2026-06-08.

Coinmetro is implemented as a conservative Spot-only gateway adapter. REST base URL defaults to `https://api.coinmetro.com`; demo can be selected by overriding the base URL to `https://api.coinmetro.com/open`. WebSocket URL defaults to `wss://api.coinmetro.com/ws`.

## Implemented Surface

- Public REST: markets and order book snapshots.
- Private REST request construction/runtime hooks: balances, place order, cancel order, query order, open orders, recent fills.
- WebSocket specs: public query-string `pairs` subscription URL, private JWT query-token URL, parser fixtures for `bookUpdate` and `tick`.
- Auth: JWT `Authorization: Bearer <token>`; optional `X-Device-ID` for long-lived device tokens.

## Public WebSocket Order Book

Official WebSocket subscriptions use the `pairs` query string, for example `wss://api.coinmetro.com/ws?pairs=BTCEUR`. Subscribed pairs receive `bookUpdate` deltas with `seqNumber` and CRC32 `checksum`; checksum generation has exchange-specific rounding rules. `tick` messages include current ask and bid and are useful as BBO. The reviewed docs do not publish a fixed millisecond interval or fixed depth parameter, so REST book snapshot plus checksum/sequence validation remains the rebuild path.

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

Standard futures/perpetual/options are `交易所不支持合约` under current official sources. Margin orders, margin collateral, swaps, hedge/close endpoints, TRAM orders, deposits, withdrawals, fiat payment rails, transfers, positions, reduce-only, post-only, client order ids, cancel-all, and batch operations are not connected. The executable boundary fixture is `tests/fixtures/exchanges/coinmetro/unsupported_boundary.json`.

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
