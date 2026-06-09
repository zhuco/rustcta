# Bitbns Gateway Adapter

Status date: 2026-06-08

Adapter id: `bitbns`

Task A-07 scope from `docs/交易所网关/总览/exchange_gateway_remaining_venues_one_ai_one_exchange_zh.md`: India/region spot exchange, public REST plus credential-gated private readback runtime; private writes remain disabled.

## Official Sources

| Area | Source | Notes |
| --- | --- | --- |
| Official SDK/docs | `https://github.com/bitbns-official/node-bitbns-api` | Documents public/private methods and API access permissions. |
| Public REST paths | `https://github.com/bitbns-official/node-bitbns-api/blob/master/index.js` | Source maps public methods to `https://bitbns.com` paths. |
| Private signing | same SDK source | Base64 JSON payload signed with HMAC-SHA512 headers. |
| KYC/region | Bitbns FAQ / how-it-works / terms pages | Trading requires KYC; terms allow compliance holds and exclude sanctioned jurisdictions. |
| Product-line follow-up | `https://bitbns.com/trade/`, `https://bitbns.com/trade-mechanism/` | Official site lists spot trading and Margin Trading/API Trading links. |

## Product Lines

| Product | MarketType | Current adapter status |
| --- | --- | --- |
| INR/USDT spot | `Spot` | G1 public REST plus guarded P2 private readbacks: symbol rules, order book snapshot, query/open/fills. |
| Margin Trading | n/a | `项目未实现`; official SDK/site document margin trading API surface, but adapter does not connect it. |
| FIP, payment, withdrawal | n/a | Outside exchange trading runtime. |
| Futures/perpetual/options | n/a | `交易所不支持合约` under the current official API/product scope; no stable standard contracts API verified. |

官方核验见 [产品线官方核验 P5 区域现货 CEX 第二批](../产品线官方核验_P5_区域现货_CEX第二批.md)。Margin Trading 不能写成交易所不支持，应写 `项目未实现 Margin Trading`；标准 futures/perpetual/options 写 `交易所不支持合约`。
Mapping 中 `margin_product` 已写 `status: project_unimplemented`、
`official_gap: margin_trading_api`、`boundary: project_unimplemented_product_line`；
`contract_product` 仅表示标准 futures/perpetual/options 未见稳定官方 API。
状态建议：`margin_product` 继续保持 `project_unimplemented`，直到 private
HMAC smoke、KYC/region eligibility、margin balance/position/risk parser、
product-scoped order lifecycle 和 reconciliation 完成；标准合约缺失仍只写
`contract_product=unsupported`。

Default public REST base URL: `https://bitbns.com`

Documented private REST bases:

- `https://api.bitbns.com/api/trade/v1`
- `https://api.bitbns.com/api/trade/v2`

## Implemented Public REST

| Gateway operation | Bitbns path | Status |
| --- | --- | --- |
| `get_symbol_rules` | `GET /order/fetchMarkets/` | Implemented. Parser accepts object/array market shapes, inactive markets are filtered. |
| `get_order_book` | `GET /exchangeData/orderBook?coin={COIN}&market={INR|USDT}` | Implemented. Depth is locally truncated, matching official SDK behavior. |
| ticker/trades/OHLCV | `/order/getTickerWithVolume`, `/exchangeData/tradedetails`, `/exchangeData/ohlc` | Mapped in endpoint audit only; not exposed through current shared trait. |

Symbols normalize to `BASE_QUOTE` (`BTC_INR`, `ETH_USDT`). The adapter rejects non-Spot market types.

## Authentication Boundary

The official SDK signs private POST requests with:

- `X-BITBNS-APIKEY`
- `X-BITBNS-PAYLOAD`
- `X-BITBNS-SIGNATURE`

`X-BITBNS-PAYLOAD` is base64 JSON containing `symbol`, `timeStamp_nonce`, and a JSON-string `body`; signature is HMAC-SHA512 over that payload. Read-only private REST is fail-closed unless `BITBNS_PRIVATE_REST_ENABLED` or `RUSTCTA_BITBNS_PRIVATE_REST_ENABLED` is true and `BITBNS_API_KEY` / `BITBNS_API_SECRET` or `RUSTCTA_`-prefixed equivalents are present.

## Official Core Trading Detail

官方核验见 [核心交易官方核验 P3 第四批](../核心交易官方核验_P3_第四批.md)。Bitbns 官方 SDK/site 有 API Trading、私有签名和交易接口线索，且产品线里 Margin Trading 写 `项目未实现`。

因此下单/撤单不能写成 `交易所不支持`。当前项目已启用 guarded read-only private runtime：`query_order` 使用 `POST /orderStatus/{symbol}`，`get_open_orders` 使用 `POST /listOpenOrders/{symbol}`，`get_recent_fills` 使用 `POST /listExecutedOrders/{symbol}`。补写侧交易接口前仍必须完成 KYC/region guard、reconciliation 和 live dry-run controls；`place_order` / `cancel_order` 不得因只读 runtime 而假启用。

账户/余额 readback 已补 `currentCoinBalance` 离线 request-spec、Bitbns HMAC header/payload 形状和响应样例；shared `get_balances` runtime 仍属未启用，剩 private signing promotion、balance parser、KYC/region guard 和 reconciliation，不能写成交易所不支持余额。

## WebSocket Boundary

Official SDK Socket.IO helpers show:

- public order book: `https://ws{market}mv2.bitbns.com/?coin={COIN}`
- ticker: `https://ws{market}mv2.bitbns.com/?withTicker=true&onlyTicker=true`
- private executed orders: `https://wsorderv2.bitbns.com/?token={token}`

This adapter only ships payload/parser fixtures and heartbeat policy notes. Runtime public/private streams remain `Unsupported("bitbns.public_streams_spec_only")` and `Unsupported("bitbns.private_streams_disabled")`.

Official WebSocket detail remains weak: the old official Python repo is
deprecated and explicitly says that repo does not cover WebSockets, while SDK
examples only expose `getOrderBookSocket` without stable endpoint, interval,
depth, sequence, or checksum documentation. Keep this adapter spec-only; use
REST orderbook snapshot for rebuild and do not promote live Socket.IO runtime
until a current stable official specification is obtained. Source batch:
[WebSocket 官方核验 P6 补充交易所盘口细项](../WebSocket官方核验_P6_补充交易所盘口细项.md).

Structured boundary: orderbook channel unverified / no stable orderbook channel;
old SDK Socket.IO examples only provide a URL hint. Record no fixed ms, no fixed
depth, no documented sequence/checksum, and REST `/exchangeData/orderBook`
snapshot rebuild. This is spec-only evidence, not a stable runtime declaration.

## Unsupported Boundaries

- Private balances, fees and live private write runtime are disabled.
- Query/open/fills are credential-gated read-only runtime; place/cancel remain offline request-spec/source boundaries; cancel-all/amend/order-list/batch operations are disabled or unsupported.
- Public Socket.IO runtime is spec-only, not advertised as a stable stream.
- Private Socket.IO token stream is disabled.
- FIP, swap, deposits, withdrawals, bank rails, payment gateway and transfer APIs are outside the trading adapter.
- Standard futures/perpetual/options are `交易所不支持合约` as of the current official docs.

## Limits And Region Notes

The SDK documents API usage counters (`readLimit`, `writeLimit`) but not a stable window. The config example uses conservative public throttling. SDK docs list minimum order values of `10 INR` and `0.1 USDT`; dynamic market fields are preferred when present.

Trading requires Bitbns account eligibility and KYC. Read-only private REST now requires explicit env opt-in plus credentials; do not promote live writes until a separate validation confirms region eligibility, write semantics, reconciliation, and dry-run controls without submitting unintended orders.

## Local Artifacts

- Adapter: `crates/rustcta-exchange-gateway/src/adapters/bitbns/`
- Endpoint mapping: `crates/rustcta-exchange-gateway/src/adapters/bitbns/endpoint_mapping.yaml`
- Fixtures: `tests/fixtures/exchanges/bitbns/`
- Config example: `config/bitbns_gateway_example.yml`

## Validation

Current validation:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bitbns/endpoint_mapping.yaml
cargo test -p rustcta-exchange-gateway bitbns --lib --message-format short
cargo check -p rustcta-exchange-gateway --lib --message-format short
```

## Fee Boundary

交易所不支持当前费率接口 runtime：private fees 被关闭，未完成账户 readback 验证。
