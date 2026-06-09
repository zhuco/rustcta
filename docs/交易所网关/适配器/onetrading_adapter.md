# One Trading Gateway Adapter

`onetrading` covers the One Trading F.A.S.T. exchange API as a conservative
European spot adapter. Public REST market metadata and order-book snapshots are
implemented. Private order/fill readbacks are available as guarded bearer-token
runtime, while private writes, balances, fees and WebSocket trading remain
offline request specs.

## Official Sources

| Area | Source |
| --- | --- |
| Public REST instruments | https://docs.onetrading.com/rest/public/instruments |
| Public REST order book | https://docs.onetrading.com/rest/public/orderbook |
| Trading REST balances/orders | https://docs.onetrading.com/rest/trading/balances |
| Trading REST create order | https://docs.onetrading.com/rest/trading/create-order |
| WebSocket introduction | https://docs.onetrading.com/websocket |
| WebSocket auth | https://docs.onetrading.com/websocket/authenticate |

## Product Boundary

One Trading documents spot, futures/perpetual, private REST, and WebSocket
trading operations under one API family. This adapter is intentionally spot
only:

- `GET /instruments?type=SPOT` is parsed into `SymbolRules`.
- `GET /order-book/{instrument_code}` is parsed into order-book snapshots.
- Non-`SPOT` instruments are filtered out even when present in the public
  instruments response.
- Futures/perpetual instruments, margin, positions, and futures order paths are
  official One Trading surfaces but are `项目未实现 Futures/Perpetual` in this
  spot adapter; do not document them as `交易所不支持合约`.
- Withdrawals, WebSocket trading writes, dead-man-switch, and amend/move order
  are explicit runtime `Unsupported` boundaries.
One Trading 的 Futures/Perpetual 产品线在当前 spot adapter 中属于 `项目未实现`，不能写成 `交易所不支持合约`。

2026-06-09 产品线边界收窄：`contract_product` 和 `futures_product`
绑定 `tests/fixtures/exchanges/onetrading/request_specs/product_line_source_boundary.json`。
当前 adapter 继续强制 `GET /instruments?type=SPOT` 过滤；Futures/Perpetual 需要独立
instrument mapping、order book/market-data scope、positions/margin/fees readback、
order lifecycle、bearer-token permission audit 和 reconciliation。
状态建议：继续保留 `contract_product` / `futures_product = 项目未实现`；官方
non-Spot instrument 线索不能写成交易所不支持，也不能让 non-`SPOT` instruments
穿透现有 Spot parser、余额或订单 runtime。

## URLs

| Surface | URL |
| --- | --- |
| REST | `https://api.onetrading.com/fast/v1` |
| Public WS | `wss://streams.fast.onetrading.com` |
| Private WS auth docs | `wss://streams.onetrading.com` |

The WebSocket docs currently show two hostnames. Runtime WS usage stays disabled
until a live dry-run task validates the canonical hostname, auth behavior,
heartbeat, and resync rules.

## Authentication

Private REST and private WebSocket auth use bearer tokens generated in the One
Trading UI:

- REST header: `Authorization: Bearer <token>`
- Private WS auth message: `{ "type": "AUTHENTICATE", "api_token": "..." }`

No HMAC, nonce, or timestamp signing scheme is documented for the current
F.A.S.T. API. Token fixtures are redacted and private readback calls remain
disabled by default unless `ONETRADING_PRIVATE_REST_ENABLED` and
`ONETRADING_API_TOKEN` (or `RUSTCTA_...`) are configured.

## Capabilities

| Capability | Status |
| --- | --- |
| Symbol rules | Native public REST |
| Order-book snapshot | Native public REST |
| Query order / open orders / recent fills | Guarded private REST runtime |
| Balances / fees | Offline request spec only |
| Place/cancel/cancel-all | Offline request spec only |
| Public WS order book | Payload/fixture only, disabled by default |
| Private WS | Unsupported until auth/resync validation |
| Batch order APIs | Unsupported |

## Official WebSocket Order Book Detail

P9 official verification confirms One Trading public WS supports `ORDER_BOOK`
snapshot/update and `BOOK_TICKER` BBO on `wss://streams.fast.onetrading.com`.
The public docs do not publish a fixed push interval, depth limit, sequence, or
checksum for these channels. The mapping records `ORDER_BOOK`, `BOOK_TICKER`
depth1 BBO availability, no-fixed-ms/no-sequence/no-checksum risk, and resync by
re-subscribing to receive a fresh `ORDER_BOOK_SNAPSHOT`.

## Files

| Artifact | Path |
| --- | --- |
| Adapter | `crates/rustcta-exchange-gateway/src/adapters/onetrading/` |
| Endpoint mapping | `crates/rustcta-exchange-gateway/src/adapters/onetrading/endpoint_mapping.yaml` |
| Fixtures | `tests/fixtures/exchanges/onetrading/` |
| Config example | `config/onetrading_gateway_example.yml`, disabled by default |

## Validation

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/onetrading/endpoint_mapping.yaml
python3 scripts/sanitize_exchange_fixture.py --check tests/fixtures/exchanges/onetrading
python3 scripts/audit_gateway_adapters.py --exchange onetrading --required-exchanges onetrading --check --strict
cargo test -p rustcta-exchange-gateway onetrading --lib --message-format short
cargo check -p rustcta-exchange-gateway --lib --message-format short
```
