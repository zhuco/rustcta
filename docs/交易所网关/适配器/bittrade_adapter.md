# BitTrade Gateway Adapter

Status: A-10 BitTrade Japan spot adapter, offline-verifiable. Public REST market
metadata and order book snapshots are implemented. Private REST readbacks for
order query, open orders, and recent fills are available only when explicitly
enabled with credentials. Private writes and private WS remain request-spec or
fixture boundaries.

## Official Sources

| Area | Source |
| --- | --- |
| REST/WS overview | https://api-doc.bittrade.co.jp/ |
| API key page | https://www.bittrade.co.jp/ja-jp/user/api/ |
| Support article | https://bittrade.zendesk.com/hc/ja/articles/25592844881945 |

Important product boundary: the official API introduction states that leverage
trading is currently unsupported. This adapter therefore declares only
`MarketType::Spot`; futures/perpetual/options are `交易所不支持合约` under the current official API scope.

## Products And URLs

| Product | Base URL |
| --- | --- |
| Spot REST | `https://api-cloud.bittrade.co.jp` |
| Public WebSocket | `wss://api-cloud.bittrade.co.jp/ws` |
| Private WebSocket | `wss://api-cloud.bittrade.co.jp/ws/v2` |

BitTrade symbols are lowercase compact pairs such as `btcjpy`. The parser
accepts common user input variants like `BTC/JPY` and `btc_jpy`, then normalizes
requests to the venue symbol.

## Implemented Surface

| Gateway operation | BitTrade endpoint | Runtime |
| --- | --- | --- |
| `get_symbol_rules` | `GET /v1/common/symbols` | live public REST |
| `get_order_book` | `GET /market/depth?symbol={symbol}&type=step0` | live public REST |
| `query_order` | `GET /v1/order/orders/{order-id}` | guarded private REST readback |
| `get_open_orders` | `GET /v1/order/openOrders?symbol={symbol}` | guarded private REST readback |
| `get_recent_fills` | `GET /v1/order/matchresults?symbol={symbol}` | guarded private REST readback |
| public WS subscribe payloads | `market.{symbol}.depth.step0`, `market.{symbol}.bbo`, `market.{symbol}.trade.detail` | offline payload spec |

Public WS frames are gzip-compressed JSON in production. Heartbeat uses
server/client ping-pong where the client returns the same timestamp in `pong`.

Official public WS supports `market.<symbol>.depth.<type>` with
`step0`-`step5` and `market.<symbol>.bbo` for best bid/ask. BBO messages include
`seqId`; depth docs do not publish checksum or fixed push milliseconds. The
mapping records depth/BBO channels, step0-step5, gzip, BBO `seqId`, no fixed ms,
and REST depth rebuild. Source batch:
[WebSocket 官方核验 P6 补充交易所盘口细项](../WebSocket官方核验_P6_补充交易所盘口细项.md).

## Private REST Boundary

BitTrade uses HMAC-SHA256 signatures over:

```text
METHOD
lowercase-host
path
ASCII-sorted URL-encoded query
```

The resulting HMAC is Base64 encoded and sent as query parameter `Signature`.
This adapter includes request specs and signing vectors for account, balance,
place, cancel, batch-cancel, and readback endpoints. Runtime private REST is
fail-closed unless `BITTRADE_PRIVATE_REST_ENABLED` or
`RUSTCTA_BITTRADE_PRIVATE_REST_ENABLED` is true and `BITTRADE_API_KEY` /
`BITTRADE_API_SECRET` or their `RUSTCTA_`-prefixed forms are present.

Only read-only private REST is promoted in this revision:

- `query_order` requires `exchange_order_id`; `client_order_id` lookup remains
  unsupported.
- `get_open_orders` requires a spot symbol scope and uses signed GET query
  authentication.
- `get_recent_fills` requires a spot symbol scope plus request context tenant
  and account ids so normalized `Fill` rows remain attributable.

## Unsupported

- Leverage, futures, perpetuals, margin mode, position mode, funding, and open
  interest: `交易所不支持合约` according to the current official API introduction.
- Live private write REST execution in this adapter revision. `place_order`,
  `cancel_order`, and `batch_cancel_orders` remain offline request-spec only.
- Retail/sales-office ordering APIs; they are not mapped into exchange spot
  order routing.
- Fiat deposits, withdrawals, bank transfers, crypto withdrawals, and funding
  ledgers.
- In-place amend and cancel-all. BitTrade exposes cancel and batch-cancel
  request specs, not a verified gateway cancel-all mapping.

官方核验见 [产品线官方核验 P5 区域现货 CEX 第二批](../产品线官方核验_P5_区域现货_CEX第二批.md)。

## Validation

Allowed commands:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bittrade/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway bittrade --lib --message-format short
cargo test -p rustcta-gateway bittrade --message-format short
```

Do not run `cargo build` for this task.

## Fee Boundary

交易所不支持当前费率接口 runtime：官方/API 资料未核到 account-effective maker/taker fee endpoint；fee 页面不能直接映射 runtime readback。
