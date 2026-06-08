# Bitstamp Gateway Adapter

Adapter id: `bitstamp`

Status: Task 11 spot-only adapter for Bitstamp V2. The adapter covers symbol rules, public order books, HMAC-SHA256 V2 private REST, spot order lifecycle, fills, public WebSocket subscriptions, and private WebSocket token session setup.

## Scope

- REST: `https://www.bitstamp.net`
- Public WebSocket: `wss://ws.bitstamp.net`
- Market types: `Spot`
- Derivatives/Contracts: 项目未实现。Bitstamp official API changelog contains derivatives public/private endpoints, funding-rate endpoints and derivatives trade-history support, so this must not be written as `交易所不支持合约`.
- Symbols: Bitstamp market symbols are lowercase pairs such as `btcusd`.
- Signing: Bitstamp V2 HMAC-SHA256 headers over method, host, path, query, content type, nonce, timestamp, version and form body.

## Endpoint Mapping

Machine-readable mapping:

`crates/rustcta-exchange-gateway/src/adapters/bitstamp/endpoint_mapping.yaml`

| Gateway capability | Bitstamp endpoint | Status |
| --- | --- | --- |
| Symbol rules | `GET /api/v2/markets/` | Native parser |
| Order book | `GET /api/v2/order_book/{market}/?group=1` | Native snapshot parser |
| Balances | `POST /api/v2/account_balances/` | Signed REST |
| Fees | `POST /api/v2/fees/trading/{market}/` | Signed REST |
| Place order | `POST /api/v2/buy|sell/{market}/`, `buy|sell/market/{market}/` | Signed REST |
| Cancel, cancel all, replace | `POST /api/v2/cancel_order/`, `cancel_all_orders/`, `replace_order/` | Signed REST |
| Query/open orders | `POST /api/v2/order_status/`, `open_orders/all/` | Signed REST |
| Recent fills | `POST /api/v2/user_transactions/{market}/` | Signed REST, symbol scoped |
| Public WS | `order_book_`, `diff_order_book_`, `live_trades_` channels | Payload helpers |
| Private WS | `POST /api/v2/websockets_token/` then `private-my_orders` or `private-user` | Session spec helper |

## Public WebSocket Order Book

Official WebSocket v2 is the public real-time feed at `wss://ws.bitstamp.net`. The project already records `order_book_{market}` and `diff_order_book_{market}` payload helpers, but the reviewed official material did not expose a fixed millisecond interval, fixed depth parameter, or stable checksum contract. Treat REST `order_book` snapshots and Bitstamp order-data gap recovery endpoints as the resync path before using this feed for arbitrage.

## Unsupported Boundaries

- Derivatives, margin positions and contract metadata are 项目未实现 in this spot adapter.
- Sell quote-sized market orders, native order lists and batch place are `Unsupported`.
- Private WebSocket positions are `Unsupported` because Bitstamp spot has no shared position model.

## Validation

Recommended targeted validation:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bitstamp/endpoint_mapping.yaml
rustfmt --edition 2021 --check crates/rustcta-exchange-gateway/src/adapters/bitstamp/*.rs
cargo test -p rustcta-exchange-gateway bitstamp_signing --lib --message-format short
cargo test -p rustcta-exchange-gateway bitstamp_private_parsers --lib --message-format short
cargo test -p rustcta-exchange-gateway bitstamp_private_ws_payload --lib --message-format short
```

Do not enable private REST or private streams until API scopes, subaccount routing and trading limits are verified.
