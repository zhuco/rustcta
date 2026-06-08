# One Trading Gateway Adapter

`onetrading` covers the One Trading F.A.S.T. exchange API as a conservative
European spot adapter. The first implementation enables public REST market
metadata and order-book snapshots, while private REST and WebSocket trading are
kept as offline request specs.

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
F.A.S.T. API. Token fixtures are redacted and private live calls remain disabled
by default.

## Capabilities

| Capability | Status |
| --- | --- |
| Symbol rules | Native public REST |
| Order-book snapshot | Native public REST |
| Balances / fees / orders / fills | Offline request spec only |
| Place/cancel/cancel-all | Offline request spec only |
| Public WS order book | Payload/fixture only, disabled by default |
| Private WS | Unsupported until auth/resync validation |
| Batch order APIs | Unsupported |

## Files

| Artifact | Path |
| --- | --- |
| Adapter | `crates/rustcta-exchange-gateway/src/adapters/onetrading/` |
| Endpoint mapping | `crates/rustcta-exchange-gateway/src/adapters/onetrading/endpoint_mapping.yaml` |
| Fixtures | `tests/fixtures/exchanges/onetrading/` |
| Config example | `config/onetrading_gateway_example.yml`, disabled by default |

## Validation

The task was completed under a no-compile instruction. Use non-build checks for
this handoff:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/onetrading/endpoint_mapping.yaml
python3 scripts/sanitize_exchange_fixture.py --check tests/fixtures/exchanges/onetrading
python3 scripts/audit_gateway_adapters.py --exchange onetrading --required-exchanges onetrading --check --strict
```

`cargo check`, `cargo test`, and any build command were intentionally not run.
