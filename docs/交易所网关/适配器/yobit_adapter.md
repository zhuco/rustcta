# YoBit Gateway Adapter

Status date: 2026-06-08

Adapter id: `yobit`

Implementation status: A-37 scan-only Spot adapter. Public REST v3 support is
implemented for Spot pair metadata and order-book snapshots. Private REST,
trading, batch, account readbacks, withdrawal/deposit address operations,
Yobicode operations, Defi swap, and WebSocket runtime are deliberately kept
`Unsupported` until a separate validation task promotes any private surface.

## Official Materials

| Area | Source | Adapter use |
| --- | --- | --- |
| Public API | `https://www.yobit.net/en/api/` documents Public API v3 `info`, `ticker`, `depth`, and `trades`. | Adapter uses `GET /api/3/info` and `GET /api/3/depth/{pair}`. |
| Trade API | `https://www.yobit.net/en/api/` documents `/tapi/`, `Key` and `Sign` headers, HMAC-SHA512, nonce, `getInfo`, `Trade`, `ActiveOrders`, `OrderInfo`, `CancelOrder`, and `TradeHistory`. | Request-spec/signing-vector only; runtime capabilities stay unsupported. |
| API rules | `https://www.yobit.net/en/rules/` documents API use and a 100 requests/minute limit. | Endpoint mapping uses conservative 100/min buckets. |

## Product Lines

| Product | MarketType | Status |
| --- | --- | --- |
| Spot | `Spot` | Public REST G1 for symbol rules and order book. |
| Defi swap | n/a | Unsupported; not mapped to central limit order book semantics. |
| Deposit/withdraw/Yobicode | n/a | Unsupported and out of gateway trading scope. |
| Perpetual/futures | n/a | Unsupported; no stable official perpetual product scope verified for this adapter. |
| Testnet | n/a | Unsupported; no stable public sandbox host verified. |

Default REST base URL: `https://yobit.net`

## Endpoint Mapping

| Gateway capability | YoBit endpoint | Current status |
| --- | --- | --- |
| Symbol rules | `GET /api/3/info` | Implemented for Spot parser fixtures. |
| Order book | `GET /api/3/depth/{pair}?limit={depth}` | Implemented for snapshot parser fixtures. |
| Balances | `POST /tapi/ method=getInfo` | Request-spec-only; runtime returns `Unsupported("yobit.balances_request_spec_only")`. |
| Place order | `POST /tapi/ method=Trade` | Request-spec-only; runtime returns `Unsupported("yobit.place_order_request_spec_only")`. |
| Cancel order | `POST /tapi/ method=CancelOrder` | Request-spec-only; runtime returns `Unsupported("yobit.cancel_order_request_spec_only")`. |
| Query order | `POST /tapi/ method=OrderInfo` | Request-spec-only. |
| Open orders | `POST /tapi/ method=ActiveOrders` | Request-spec-only REST reconciliation candidate. |
| Recent fills | `POST /tapi/ method=TradeHistory` | Request-spec-only REST reconciliation candidate. |
| Batch place/cancel | Not verified | Unsupported. |
| WebSocket | No official WS section verified | Public/private streams unsupported; REST reconciliation fallback documented for future private promotion. |

## Authentication

YoBit Trade API signs the URL-encoded POST body with HMAC-SHA512 and sends:

- `Key: <api key>`
- `Sign: <hex hmac sha512>`

The adapter includes `hmac_sha512_hex` and sanitized fixtures only:

- fixture: `tests/fixtures/exchanges/yobit/signing_vectors/rest_tapi_hmac_sha512.json`
- request specs: `tests/fixtures/exchanges/yobit/request_specs/*.json`

Private REST remains disabled by default even when credentials are present.
`apps/gateway` can parse `RUSTCTA_YOBIT_API_KEY` and
`RUSTCTA_YOBIT_API_SECRET` for redacted config/request-spec coverage, but
`enabled_private_rest` is forced to false.

## Capability Boundary

Default `capabilities()` returns:

- `market_types = [Spot]`
- public REST, symbol rules, and order-book snapshots supported
- private REST, balances, fees, trading, cancel, batch, cancel-all, query,
  open orders, fills, public streams, and private streams unsupported
- `capabilities_v2.private_rest` unsupported with a request-spec-only reason

This adapter must not be promoted to live-dry-run until a separate validation
task proves private read-only endpoints with sanitized credentials and confirms
write request semantics without real order placement.

## Validation

Allowed validation for this task:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/yobit/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway yobit --lib --message-format short
cargo test -p rustcta-gateway yobit --message-format short
```

Do not run `cargo build` for this task.
