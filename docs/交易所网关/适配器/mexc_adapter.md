# MEXC Gateway Adapter

`mexc` implements the RustCTA gateway `ExchangeClient` surface for MEXC Spot and
USDT perpetual/contract REST in `crates/rustcta-exchange-gateway/src/adapters/mexc`.

## Scope

Supported:

- Spot public REST: exchange info and order book snapshots.
- Contract public REST: contract detail and order book snapshots.
- Spot private REST: balances, fees, place order, quote-sized market buy,
  cancel order, native cancel all by symbol, query order, open orders, and
  recent fills.
- Contract private REST: balances, open positions, tiered fee rate, place order,
  cancel order, cancel all by symbol, query order, open orders, and recent fills.
- Contract public REST funding rate through the shared `get_funding_rates` API.
- Contract account control supports shared `set_leverage`; the adapter applies
  the configured leverage to both long and short MEXC position types.
- Contract account control supports shared `get_position_mode` and
  `set_position_mode` through MEXC `positionMode`, with the venue precondition
  that there are no active orders, plan orders, or unfinished positions.
- Public WebSocket subscription specs and parsers for Spot `bookTicker` 10ms,
  aggregated depth 10ms, limit depth 5/10/20, and Contract depth topics.
- Contract private WebSocket event normalization has parser coverage for
  order, fill, asset, position, and heartbeat payloads. The live private WS
  connection is still not wired and remains REST readback fallback.
- Binance-like HMAC-SHA256 query signing with `X-MEXC-APIKEY`.

Project-unimplemented product boundaries:

- Delivery-state contract semantics are `项目未实现`; current contract
  instruments are normalized as perpetual contracts. Promotion requires
  delivery-specific metadata/status filters, settlement state, account/position
  parsers and private order lifecycle reconciliation.

Explicitly unsupported:

- Private WebSocket runtime. Private state reconciliation uses REST readbacks.
- Shared amend order and order-list/OCO methods.
- Standard options are `交易所不支持合约` under the current Spot/Contract API
  references reviewed for this adapter.
- Transfers, deposits, withdrawals, and account-management endpoints.

## Environment

```bash
MEXC_SPOT_API_KEY=...
MEXC_SPOT_API_SECRET=...
MEXC_SPOT_RECV_WINDOW_MS=5000
MEXC_SPOT_PRIVATE_REST_ENABLED=true
```

`MEXC_API_KEY` and `MEXC_API_SECRET` are accepted as fallback variable names.
Use read/trade keys with withdrawal permission disabled.

## Endpoint Mapping

Machine-readable mapping:
`crates/rustcta-exchange-gateway/src/adapters/mexc/endpoint_mapping.yaml`.

| Capability | Spot endpoint | Contract endpoint |
| --- | --- | --- |
| symbol rules | `GET /api/v3/exchangeInfo` | `GET /api/v1/contract/detail` |
| order book | `GET /api/v3/depth` | `GET /api/v1/contract/depth/{symbol}` |
| balances | `GET /api/v3/account` | `GET /api/v1/private/account/assets` |
| positions | `交易所不支持现货仓位` | `GET /api/v1/private/position/open_positions` |
| fees | `GET /api/v3/tradeFee` | `GET /api/v1/private/account/tiered_fee_rate` |
| place / quote market order | `POST /api/v3/order` | `POST /api/v1/private/order/create` |
| cancel order | `DELETE /api/v3/order` | `POST /api/v1/private/order/cancel` |
| cancel all by symbol | `DELETE /api/v3/openOrders` | `POST /api/v1/private/order/cancel_all` |
| query order | `GET /api/v3/order` | `GET /api/v1/private/order/get/{order_id}` |
| open orders | `GET /api/v3/openOrders` | `GET /api/v1/private/order/list/open_orders/{symbol}` |
| recent fills | `GET /api/v3/myTrades` | `GET /api/v1/private/order/list/order_deals` |
| funding rate | n/a | `GET /api/v1/contract/funding_rate/{symbol}` |
| set leverage | n/a | `POST /api/v1/private/position/change_leverage` |
| get position mode | n/a | `GET /api/v1/private/position/position_mode` |
| set position mode | n/a | `POST /api/v1/private/position/change_position_mode` |

## WebSocket 行情

| 通道 | 产品线 | 状态 | 推流间隔 | 档位 | 序列/校验 | 重建策略 |
| --- | --- | --- | --- | --- | --- | --- |
| `spot@public.aggre.bookTicker.v3.api.pb@10ms@{symbol}` | Spot | 已补 | 10ms | 1 | `version` / no checksum | REST snapshot fallback |
| `spot@public.aggre.depth.v3.api.pb@10ms@{symbol}` | Spot | 已补 | 10ms | incremental | `fromVersion`/`toVersion` / no checksum | REST snapshot + delta replay |
| `spot@public.limit.depth.v3.api.pb@{symbol}@5/10/20` | Spot | 已补 | event-driven | 5/10/20 | `version` / no checksum | REST snapshot fallback |
| `sub.depth` / `sub.depth.full` | Contract | 已补 parser fixture | venue-defined | 5/10/20 | `version` / no checksum | REST contract depth snapshot + delta gap resync |

## Capability V2

- Product boundary: Spot + Perpetual.
- Delivery-state contracts are explicit `project_unimplemented` boundaries;
  standard options remain unsupported under the current official API scope.
- Public REST: native. Private REST: native when credentials are configured.
- Public streams: native subscription metadata/parser support. Private streams:
  parser-only normalization is covered, but live auth/session runtime remains
  `RestFallback`.
- Order book: strict delta capable, max depth 1000; reconnect/resync uses REST
  snapshot.
- Recent fills pagination: `fromId`, `startTime`, `endTime`, `limit <= 1000`.
- Batch place: composed sequential planner, non-atomic, max 20, partial failure
  possible. Batch cancel/cancel-all are native same-symbol partial operations.
- Account control: `supports_leverage=true` and
  `supports_position_mode_change=true` when private REST is enabled.

## Runtime Policies

- Public WS heartbeat: client ping every 30s, pong timeout 10s, stale message
  threshold 60s.
- Reconciliation: unknown order outcomes should query order, then open orders,
  then fills. Balances and positions are read from private REST.
- Private WS login/filter specs and parser payloads are covered only as
  normalized test/runtime inputs; the adapter must not advertise live private
  stream support until ready gate, reconnect, resubscribe, and REST resync are
  implemented.
- Rate limits: mapping declares public, private, order, contract public, contract
  private, and contract order buckets.
- Live dry-run gates: require reconciliation enabled, kill-switch, disabled
  symbol list, and max-notional limits before enabling any live private REST
  mutation.

## Fixtures And Tests

- Request-spec coverage is in `private_tests.rs` and asserts method, path,
  query/body, auth header, recvWindow/timestamp/signature, and secret-free
  requests for Spot and Contract.
- Public WS fixtures live under `tests/fixtures/exchanges/mexc/ws/` and cover
  Spot aggregated depth 10ms, limit depth 20, bookTicker 10ms, Contract
  `sub.depth`, and Contract `sub.depth.full`.
- Private WS login/filter and parser coverage is in
  `mexc_private_stream_parser_should_normalize_order_fill_balance_and_position_events`.
- Signing vector coverage is `mexc_signing_should_match_known_hmac`, backed by
  `tests/fixtures/exchanges/mexc/toolchain/signing_vector.json`.
- Parser fixtures live under `tests/fixtures/exchanges/mexc/toolchain/` and
  cover success, empty response, error response, and missing required fields.

## Validation

```bash
cargo test -p rustcta-exchange-gateway mexc --lib --message-format short
cargo test -p rustcta-exchange-gateway mexc_adapter_should_load_perpetual_funding_rate_from_contract_public_rest -- --nocapture
cargo test -p rustcta-exchange-gateway mexc_adapter_should_set_perpetual_leverage_for_both_position_types -- --nocapture
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/mexc/endpoint_mapping.yaml
python3 scripts/audit_gateway_adapters.py --exchange mexc
```
