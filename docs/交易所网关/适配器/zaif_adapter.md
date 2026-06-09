# Zaif Adapter Migration

A-38 scope is Zaif Japanese Spot markets. Venue symbols are lowercase
underscore pairs such as `btc_jpy`; JPY quote markets are first-class Spot
markets and the adapter does not expose fiat funding, bank transfer,
withdrawal, or ledger write operations.

## Official Sources

| Area | Source |
| --- | --- |
| Public REST | `https://zaif-api-document.readthedocs.io/ja/latest/PublicAPI.html` |
| Trading REST | `https://zaif-api-document.readthedocs.io/ja/latest/TradingAPI.html` |
| WebSocket | `https://zaif-api-document.readthedocs.io/ja/latest/WebSocket_API.html` |

## Coverage

| Area | Status |
| --- | --- |
| Endpoint mapping | `crates/rustcta-exchange-gateway/src/adapters/zaif/endpoint_mapping.yaml` |
| REST signing | Private form body signed with HMAC-SHA512; headers are `Key` and `Sign` |
| Public REST | Symbol rules via `/api/1/currency_pairs/all`; order book snapshot via `/api/1/depth/{currency_pair}` |
| Private REST | Guarded read-only TAPI runtime for `active_orders` and `trade_history`; `get_info2`, `trade`, and `cancel_order` remain request-spec/signing-vector only |
| Public WS | `wss://ws.zaif.jp/stream?currency_pair={pair}` policy documented with REST snapshot resync |
| Private WS | Unsupported; use REST reconciliation after dedicated validation |
| Fixtures | Parser fixtures, private request specs, HMAC signing vector, and WS policy fixture live under `tests/fixtures/exchanges/zaif/` |

## Official WebSocket Order Book Detail

P9 official verification confirms Zaif WebSocket provides real-time board data
and last price at `wss://ws.zaif.jp/stream?currency_pair={currency_pair}`.
Payloads include `asks`, `bids`, `trades`, `timestamp`, `last_price`, and
`currency_pair`. The official page does not state a fixed push interval, depth
limit, sequence, or checksum. Reconnect should treat the next full board
payload or REST depth as the fresh book state.

## Runtime Notes

The adapter supports Spot public REST and guarded read-only TAPI runtime for
open orders and recent fills when `ZAIF_PRIVATE_REST_ENABLED`,
`ZAIF_API_KEY`, and `ZAIF_API_SECRET` are present. Private balances, place
order, and cancel order return explicit `Unsupported` boundaries because write
ack parsing, dry-run/live-write guard, query-order semantics, and post-write
reconciliation still need dedicated validation. P6 official verification found
Zaif margin/credit trading is currently stopped and no current standard
futures/perpetual/options API is available, so standard contracts are
`交易所不支持合约`; if credit trading resumes it must be rechecked before writing
`项目未实现`. Fiat funding, withdrawal, transfer, client order IDs, amend,
cancel-all, and batch operations are not exposed.

## Validation

Allowed commands:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/zaif/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway zaif --lib --message-format short
cargo test -p rustcta-gateway zaif --message-format short
```

Do not run `cargo build`.

## Fee Boundary

交易所不支持当前费率接口 runtime：当前 Zaif profile 明确不提供稳定 get_fees runtime。
