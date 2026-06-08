# BitTrade Gateway Adapter

Status: A-10 BitTrade Japan spot adapter, offline-verifiable. Public REST market
metadata and order book snapshots are implemented. Private REST and private WS
are documented with request specs/signing vectors only and are not exposed as
live gateway trading capability.

## Official Sources

| Area | Source |
| --- | --- |
| REST/WS overview | https://api-doc.bittrade.co.jp/ |
| API key page | https://www.bittrade.co.jp/ja-jp/user/api/ |
| Support article | https://bittrade.zendesk.com/hc/ja/articles/25592844881945 |

Important product boundary: the official API introduction states that leverage
trading is currently unsupported. This adapter therefore declares only
`MarketType::Spot`; futures/perpetuals are `Unsupported`.

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
| public WS subscribe payloads | `market.{symbol}.depth.step0`, `market.{symbol}.bbo`, `market.{symbol}.trade.detail` | offline payload spec |

Public WS frames are gzip-compressed JSON in production. Heartbeat uses
server/client ping-pong where the client returns the same timestamp in `pong`.

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
place, cancel, batch-cancel, and query-order endpoints. Runtime methods return
`Unsupported` because private live read/write validation was not part of A-10.

## Unsupported

- Leverage, futures, perpetuals, margin mode, position mode, funding, and open
  interest.
- Live private write/read REST execution in this adapter revision.
- Retail/sales-office ordering APIs; they are not mapped into exchange spot
  order routing.
- Fiat deposits, withdrawals, bank transfers, crypto withdrawals, and funding
  ledgers.
- In-place amend and cancel-all. BitTrade exposes cancel and batch-cancel
  request specs, not a verified gateway cancel-all mapping.

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
