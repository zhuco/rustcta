# ApolloX DEX Gateway Adapter

Status date: 2026-06-08

Adapter id: `apollox_dex`

## Scope

`apollox_dex` targets APX/ApolloX V1 orderbook perpetual contracts. The V1 API is Binance Futures style: public REST market metadata and order book snapshots are enabled; signed private REST is enabled for credential-gated readbacks (`query_order`, `get_open_orders`, `get_recent_fills`) and existing `batch_place_orders` local mock/request-spec/parser coverage.

Out of scope:

- ApolloX spot API family at `https://www.apollox.finance/api/v1` is 项目未实现 Spot for this adapter, not `交易所不支持现货`.
- APX V2 fully on-chain trading, wallet signing, gas, chain profiles and contract execution.
- Broker SDK proxy/UI paths, deposits, withdrawals, transfers and custody operations.
- Stable V1 testnet routing; no stable REST/WS testnet base is declared.

Spot 边界写入 `spot_product status: project_unimplemented`：当前 `apollox_dex` 只接 `fapi` V1 futures/perpetual profile。补 Spot 前需要 spot API host、exchangeInfo/depth/ticker/trades、signed account/order/open-order/fill endpoints 和 spot private-stream/listen-key 对账。

## Official Sources

| Topic | Source |
| --- | --- |
| V1 API docs | `https://apollox-finance.gitbook.io/apollox-finance/api/api-documentation.md` |
| Product overview | `https://apollox-finance.gitbook.io/apollox-finance` |
| V2 trading docs | `https://apollox-finance.gitbook.io/apollox-finance/welcome/trading-on-v2.md` |
| API GitHub mirror | `https://github.com/apollox-finance/apollox-finance-api-docs` |
| Broker SDK boundary | `https://github.com/apollox-finance/broker-web-sdk` |

## Endpoints

| Capability | Runtime | Endpoint |
| --- | --- | --- |
| Symbol rules | Native public REST | `GET /fapi/v1/exchangeInfo` |
| Order book snapshot | Native public REST | `GET /fapi/v1/depth` |
| Funding / premium index | Spec-only | `GET /fapi/v1/premiumIndex`, `GET /fapi/v1/fundingRate` |
| Balances / positions | Request-spec only | `GET /fapi/v2/balance`, `GET /fapi/v2/positionRisk` |
| Place / cancel / cancel-all | Request-spec only | `POST/DELETE /fapi/v1/order`, `DELETE /fapi/v1/allOpenOrders` |
| Batch place | Request-spec/parser only | `POST /fapi/v1/batchOrders`; fixture-only, max 5, partial item errors parsed offline |
| Query / open orders / fills | Guarded native private REST | `GET /fapi/v1/order`, `GET /fapi/v1/openOrders`, `GET /fapi/v1/userTrades` |

## Auth

Signed `TRADE` and `USER_DATA` endpoints use `X-MBX-APIKEY` plus HMAC-SHA256 over the canonical query/body parameters. Fixtures use sanitized `test-key` and `test-secret`; no real credential or account identifier is present.

Most private REST runtime remains disabled even if credentials are present. Readbacks use `/fapi/v1/order`, `/fapi/v1/openOrders`, and `/fapi/v1/userTrades` behind `RUSTCTA_APOLLOX_DEX_PRIVATE_REST_ENABLED` plus API key/secret, with mock tests covering signed query construction and parsers. `batch_place_orders` also uses `/fapi/v1/batchOrders` behind the same guard with partial failure parser coverage. Account availability, region restrictions, balances, fees, positions and other write permissions remain unvalidated.

## WebSocket

Public WS base is `wss://fstream.apollox.finance`. The adapter includes subscription payload fixtures for Binance-style streams such as `btcusdt@depth`. Runtime WS is disabled until snapshot/delta resync, 24-hour reconnect and stream limits are verified.

Official APX/ApolloX Futures streams are Binance Futures style. bookTicker is
real-time; partial and diff depth can be 100ms/250ms/500ms; partial depth
supports 5/10/20 levels. Diff depth uses `U/u/pu` plus REST `lastUpdateId`
snapshot replay; no checksum is documented. Mapping should add `@bookTicker`,
`@depth@100ms`/`@depth@250ms`/`@depth@500ms`, partial depth
`@depth5@100ms`/`@depth10@100ms`/`@depth20@100ms`, REST snapshot buffering and
`pu` gap resync. Source batch:
[WebSocket 官方核验 P6 补充交易所盘口细项](../WebSocket官方核验_P6_补充交易所盘口细项.md).

Private WS uses listen keys from `POST|PUT|DELETE /fapi/v1/listenKey` in the docs, but runtime private streams remain unsupported and require REST reconciliation fallback.

## Capability Boundary

- `MarketType::Perpetual` only.
- Spot: 项目未实现 Spot；当前 `apollox_dex` 不接 ApolloX spot API family。
- Public REST symbol rules and order book snapshot are enabled.
- Private REST order/fill readbacks are guarded native runtime. Private WS, balances, fees, positions, cancel-all and non-batch writes are request-spec only or `Unsupported`.
- Advanced-order boundary: native V1 batch place now has request-spec, HMAC
  signing vector, parser fixture, and credential-gated shared runtime. Mock REST
  tests verify signed request shape and partial failure reporting; no live write
  success is asserted.
  because private signed transport, dry-run guard, and post-write reconciliation
  are not enabled. Batch cancel, in-place amend and OCO/order-list remain
  explicit unsupported pseudo-endpoints because the reviewed V1 API scope does
  not expose shared equivalents.
- APX V2 on-chain trading is explicitly `Unsupported`.

## Validation

Allowed validation commands:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/apollox_dex/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway apollox_dex --lib --message-format short
cargo test -p rustcta-gateway apollox_dex --message-format short
```

Do not run `cargo build`, release builds or live trading commands for this task.

## P2 Product Line Boundary (2026-06-09)

`spot_product` is an official-source project boundary, not an exchange-unsupported row. ApolloX spot API material exists separately from the fapi futures surface, while this adapter is scoped to ApolloX/APX V1 futures-style perpetual APIs.

Do not promote Spot runtime by reusing fapi perpetual request builders. Promotion requires spot host/exchangeInfo/depth/ticker public specs, signed spot account private specs, spot order lifecycle/open-order/fill private specs, listen-key or private stream reconciliation, product-scope signing, and parser coverage.
