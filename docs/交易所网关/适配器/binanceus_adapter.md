# Binance.US Gateway Adapter

Status: task A-05 spot-only Binance.US profile for `rustcta-exchange-gateway`.

## Scope

- Product: Binance.US Spot.
- REST base URL: `https://api.binance.us`.
- Public REST: symbol rules and order book snapshots.
- Private REST: balances, trading fees, place order, quote-sized market buy/sell, cancel order, cancel all open orders for a symbol, query order, open orders, and recent fills.
- Public WebSocket spec: Spot `bookTicker`, partial depth, and diff depth are documented in `endpoint_mapping.yaml`; runtime subscription is still project-unimplemented.
- Private WebSocket runtime: not wired in this adapter; use REST snapshots and reconciliation.

## Official References

- API portal: https://docs.binance.us
- REST reference: https://github.com/binance-us/binance-us-api-docs/blob/master/rest-api.md
- WebSocket reference: https://github.com/binance-us/binance-us-api-docs/blob/master/web-socket-api.md
- Trading fee SAPI announcement: https://support.binance.us/en/articles/9843370-introducing-new-api-endpoints-trading-fee-retrieval-convert-dust-staking-and-more

## Toolchain Artifacts

- Endpoint mapping: `crates/rustcta-exchange-gateway/src/adapters/binanceus/endpoint_mapping.yaml`.
- Capabilities declaration: `crates/rustcta-exchange-gateway/src/adapters/binanceus/toolchain.rs`.
- Request specs: `tests/fixtures/exchanges/binanceus/request_specs/`.
- Signing vector: `tests/fixtures/exchanges/binanceus/signing_vectors/place_order_limit.json`.
- Parser fixtures: `tests/fixtures/exchanges/binanceus/parser/`.
- Gateway app config example: `config/binanceus_gateway_example.yml`.

## Endpoint Mapping

| Operation | Binance.US endpoint | Status |
| --- | --- | --- |
| `get_symbol_rules` | `GET /api/v3/exchangeInfo` | Native public REST |
| `get_order_book` | `GET /api/v3/depth` | Native public REST |
| `get_balances` | `GET /api/v3/account` | Native signed REST |
| `get_fees` | `GET /sapi/v1/asset/query/trading-fee` | Native signed REST |
| `place_order` | `POST /api/v3/order` | Native signed REST |
| `place_quote_market_order` | `POST /api/v3/order` with `quoteOrderQty` | Native signed REST |
| `cancel_order` | `DELETE /api/v3/order` | Native signed REST |
| `cancel_all_orders` | `DELETE /api/v3/openOrders` | Native signed REST, symbol required |
| `query_order` | `GET /api/v3/order` | Native signed REST |
| `get_open_orders` | `GET /api/v3/openOrders` | Native signed REST |
| `get_recent_fills` | `GET /api/v3/myTrades` | Native signed REST |
| `amend_order` | `POST /api/v3/order/cancelReplace` | Unsupported shared semantics; fixture records cancel-replace boundary, not keep-priority amend |
| `place_order_list` | `POST /api/v3/order/oco` | Unsupported shared semantics; fixture records legacy OCO boundary, not generic order-list |
| `batch_place_orders` | none mapped | Unsupported shared semantics; no verified Binance.US native batch-place route maps losslessly to shared batch placement |
| `batch_cancel_orders` | none mapped | Unsupported shared semantics; no verified Binance.US native batch-cancel route maps losslessly to shared batch cancellation |

## Signing And Credentials

Private REST uses Binance-compatible HMAC-SHA256 signing over the query string and sends `X-MBX-APIKEY`. The app config accepts both compact and separated US aliases:

- API key: `RUSTCTA_BINANCEUS_API_KEY`, `RUSTCTA_BINANCE_US_API_KEY`, `BINANCEUS_SPOT_API_KEY`, `BINANCEUS_API_KEY`, `BINANCE_US_SPOT_API_KEY`, `BINANCE_US_API_KEY`.
- API secret: `RUSTCTA_BINANCEUS_API_SECRET`, `RUSTCTA_BINANCE_US_API_SECRET`, `BINANCEUS_SPOT_API_SECRET`, `BINANCEUS_API_SECRET`, `BINANCE_US_SPOT_API_SECRET`, `BINANCE_US_API_SECRET`.

`supports_private_rest` and all private operation flags are enabled only when private REST is enabled and a non-empty key/secret pair is present.

## Unsupported Boundaries

- 交易所不支持合约：当前 Binance.US 官方 API profile 只见 Spot；后续如官方开放合约需重核。
- Non-spot market types return `Unsupported`.
- Futures/perpetual balances, positions, funding, leverage, margin mode, and position mode are outside this profile.
- Binance.US cancel-replace and legacy OCO/order-list clues are fixture-backed in `tests/fixtures/exchanges/binanceus/request_specs/cancel_replace_not_amend_boundary.json` and `oco_not_order_list_boundary.json`, but they are not lossless equivalents for the shared keep-priority amend/order-list request shapes. `amend_order` and `place_order_list` therefore remain unsupported shared semantics.
- Batch place/cancel are fixture-backed unsupported boundaries in `tests/fixtures/exchanges/binanceus/request_specs/batch_place_orders_unsupported.json` and `batch_cancel_orders_unsupported.json`; do not alias Binance.com batch routes into Binance.US without a Binance.US-specific official endpoint audit.
- Public WebSocket subscriptions are recorded as Spot `spec_only` because runtime wiring is not implemented in this adapter yet; private WebSocket subscriptions remain `Unsupported`.
- The adapter does not request or require withdrawal/transfer permissions.

## Public WebSocket Order Book Spec

官方核验见 [WebSocket 官方核验 P0 第二批](../WebSocket官方核验_P0_第二批.md)。Binance.US Spot public streams follow the Binance Spot-style order book surface:

- `bookTicker`: `<symbol>@bookTicker`, real-time best bid/ask, depth 1.
- Partial depth: `<symbol>@depth5@100ms`, `<symbol>@depth10@100ms`, and `<symbol>@depth20@100ms`; 1000ms variants are also valid for partial depth.
- Diff depth: `<symbol>@depth@100ms` and `<symbol>@depth@1000ms`.
- Sequencing: REST `/api/v3/depth` snapshot carries `lastUpdateId`; diff events carry `U/u`.
- Rebuild rule: buffer WS deltas, fetch REST snapshot, discard events where `u <= lastUpdateId`, apply the first event covering `lastUpdateId + 1`, then require each next event to start at previous `u + 1`; otherwise reconnect and rebuild from REST snapshot.

This is a public market-data boundary only. It does not enable private user-data streams or signed WebSocket behavior.

## Reconciliation

When private streams are unavailable, reconcile from signed REST readbacks:

- `get_balances`
- `query_order`
- `get_open_orders`
- `get_recent_fills`

Recent fills accept `startTime`, `endTime`, `fromId`, and `limit`, capped at 1000.

## Validation

Focused validation for this profile should avoid build/deploy commands:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/binanceus/endpoint_mapping.yaml
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway binanceus --lib --message-format short
cargo test -p rustcta-gateway binanceus --message-format short
```
