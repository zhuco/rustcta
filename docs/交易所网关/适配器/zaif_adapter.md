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
| Private REST | `get_info2`, `active_orders`, `trade_history`, `trade`, and `cancel_order` are offline request-spec/signing-vector only |
| Public WS | `wss://ws.zaif.jp/stream?currency_pair={pair}` policy documented with REST snapshot resync |
| Private WS | Unsupported; use REST reconciliation after dedicated validation |
| Fixtures | Parser fixtures, private request specs, HMAC signing vector, and WS policy fixture live under `tests/fixtures/exchanges/zaif/` |

## Runtime Notes

The adapter supports Spot public REST only at runtime. Private balances, open
orders, fills, place order, and cancel order return explicit `Unsupported`
boundaries even when credentials are present because A-38 only validates
offline request construction. P6 official verification found Zaif margin/credit
trading is currently stopped and no current standard futures/perpetual/options
API is available, so standard contracts are `交易所不支持合约`; if credit trading
resumes it must be rechecked before writing `项目未实现`. Fiat funding,
withdrawal, transfer, client order IDs, amend, cancel-all, and batch operations
are not exposed.

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
