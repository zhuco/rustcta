# BigONE Gateway Adapter

Status: implemented as `bigone` in `rustcta-exchange-gateway`.

Task 26 scope status: BigONE is now declared with endpoint mapping, adapter-local
capability v2 data, request-spec fixtures, JWT signing vector, parser fixtures,
WS runtime policy, rate-limit/pagination/reconciliation plans, and explicit
batch/unsupported boundaries.

## Scope

- Spot and contract/perpetual market types are exposed where the BigONE API shape is available.
- Public REST: symbol rules and order book snapshots.
- Private REST: balances, positions, fee defaults, place/cancel/query/open orders, recent fills, batch place/cancel, and cancel-all.
- WebSocket specs: official Spot request-object public depth/trade/ticker/candle subscriptions, contract session specs, private order/fill/balance/account/position subscriptions, ping heartbeat, and parser/session helpers.
- Unsupported by design: quote-sized market order, amend order, and order-list/OCO style workflows.

## Config

Default REST base URL: `https://api.big.one`.

Environment keys:

- `RUSTCTA_BIGONE_REST_BASE_URL`
- `RUSTCTA_BIGONE_SPOT_REST_BASE_URL`
- `RUSTCTA_BIGONE_CONTRACT_REST_BASE_URL`
- `RUSTCTA_BIGONE_API_KEY` or `BIGONE_API_KEY`
- `RUSTCTA_BIGONE_API_SECRET` or `BIGONE_API_SECRET`

Gateway app adapter names: `bigone`, `big_one`.

## Endpoint Mapping

Authoritative mapping file:
`crates/rustcta-exchange-gateway/src/adapters/bigone/endpoint_mapping.yaml`.

| Gateway capability | BigONE endpoint/channel | Implementation notes |
| --- | --- | --- |
| Spot symbol rules | `GET /api/v3/asset_pairs` | Parses asset pair name, base/quote, precision, increments, min notional, status. |
| Contract symbol rules | `GET /api/contract/v2/instruments` | Parses instrument id and contract precision fields with perpetual market type. |
| Spot order book | `GET /api/v3/asset_pairs/{asset_pair_name}/depth` | Uses the documented asset-pair path and normalizes depth to max 200. |
| Contract order book | `GET /api/contract/v2/depth` | Uses `instrument_id` and the shared order book parser. |
| Private REST auth | JWT bearer token | `signing.rs` builds compact HMAC-SHA256 JWT tokens for Spot and Contract requests. |
| Balances | `/api/v3/viewer/accounts`, `/api/contract/v2/accounts` | Maps to standard balance snapshots with tenant/account context. |
| Positions | `GET /api/contract/v2/positions` | Perpetual-only; Spot position requests are unsupported. |
| Orders | `/api/v3/viewer/orders`, `/api/contract/v2/orders` | Standard place/cancel/query/open-orders lifecycle, client id where supplied; Spot cancel uses `POST /api/v3/viewer/orders/{id}/cancel`. |
| Batch orders | Spot native multi endpoint, contract sequential fallback | Gateway exposes batch capability while preserving per-order standard results. |
| Fills | `/api/v3/viewer/trades`, `/api/contract/v2/fills` | Maps fill id/order id/fees/liquidity/timestamps when present. |
| Public WS | Spot `wss://api.big.one/ws/v2` request objects; contract URL-style channels | Spot subscriptions use `subscribeMarketDepthRequest`, `subscribeMarketTradesRequest`, `subscribeMarketsTickerRequest`, and `subscribeMarketCandlesRequest`. Parser covers order book plus typed trade/ticker/candle messages. Contract public docs use URL-style `depth@`, `trades@`, `candlesticks/{period}@`, `instruments@`, and `stream` channels, so contract public streams remain session-spec bounded. |
| Private WS | Spot/contract private WS URLs | Subscription specs for orders/fills/accounts/positions; private parser emits standard stream events. |

## Capability v2 / Operational Plans

- Public/private WS runtime: public and private connections are separate;
  adapter-local sessions support Spot request-object subscribe payloads, parser
  helpers, ping heartbeat and reconnect/resubscribe policy. Order books emit
  shared stream events; public trade/ticker/candle messages remain typed BigONE
  parser outputs because the shared stream event model does not yet expose
  public trade/ticker/candle variants. Shared runtime integration remains
  parser/session-level, so the mapping marks streams `parser_only`.
- Heartbeat: client sends `{"request":"ping"}` every 15s, treats 30s without
  pong as timed out, and 45s without any message as stale.
- Private auth renewal: JWT auth payload is rebuilt on reconnect and planned for
  refresh/re-login every 25 minutes; renewal failure requires reconnect and
  resubscribe. REST reconciliation remains source of truth.
- Rate limits: fixed-window plan with public REST, private account, order and WS
  buckets is declared in code and mapping. Values are conservative until live
  header validation is added.
- Pagination: order book, open orders and recent fills expose `limit` with max
  200; cursor/time-window pagination is not exposed by the current adapter.
- Reconciliation: ambiguous submit/cancel, stream disconnects and partial batch
  failure should query order, open orders and recent fills; no order replay.
- Batch: Spot batch place uses native `/api/v3/viewer/orders/multi`; contract
  batch place is composed sequentially. Contract batch cancel uses native
  `/api/contract/v2/orders/cancel`; Spot batch cancel is composed sequentially.
  Atomicity is partial/non-atomic and partial failure requires reconciliation.
- Unsupported: quote-sized market order, amend order, order-list/OCO, cursor
  pagination, and shared WS supervisor integration are not enabled.

## Fixtures and Offline Coverage

Fixtures live under `tests/fixtures/exchanges/bigone/`.

- Parser fixtures cover success, empty response, exchange error, missing order
  fields, order book, balances, positions, fills, public order book/trade/ticker/candle
  WS events, and private WS events.
- Request-spec fixtures cover balances, place, cancel, query, open orders,
  recent fills, Spot batch place and contract batch cancel.
- Signing vector fixture covers BigONE OpenAPI JWT HMAC-SHA256 with fixed nonce;
  tests verify both the shared signing vector and the adapter's base64url JWT.

## Validation

- `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/gateway-clean-check cargo test -p rustcta-exchange-gateway bigone --lib --message-format short`
- `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/gateway-clean-check cargo test -p rustcta-exchange-gateway bigone_ws_parser --lib --message-format short`
- `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/gateway-clean-check cargo check -p rustcta-exchange-gateway --lib --message-format short`
- `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/gateway-clean-check cargo check -p rustcta-gateway --message-format short`

Latest targeted validation passed for BigONE adapter tests, BigONE WS parser
tests, `rustcta-exchange-gateway` library check, and `rustcta-gateway` app check
with existing workspace warnings. The BigONE adapter test opens local mock TCP
listeners, so it may need local-network sandbox permission in restricted
environments.

Additional pre-release checks for config/mapping-only changes:

- `python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bigone/endpoint_mapping.yaml`
- `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/gateway-clean-check cargo test -p rustcta-gateway config_should_wire_bigone_private_gateway_adapter -- --nocapture`

Official WebSocket references used for this adapter revision:

- BigONE Spot Pusher: <https://open.big.one/docs/spot/pusher/>
- BigONE Contract Pusher: <https://open.big.one/docs/contract/pusher/>

## Official WebSocket Order Book Detail

Official Spot WS `subscribeMarketDepthRequest` sends an immediate snapshot and
then incremental updates. `changeId` and `prevId` are sequential; gaps must
trigger snapshot rebuild. The official docs do not provide a fixed millisecond
interval or fixed depth parameter. JSON is supported and protobuf is documented
as the lower-latency option. Mapping should add MarketDepth, snapshot/update,
`changeId/prevId`, no fixed ms/depth, and REST snapshot fallback. Source batch:
[WebSocket 官方核验 P6 补充交易所盘口细项](../WebSocket官方核验_P6_补充交易所盘口细项.md).

Structured boundary: MarketDepth / `subscribeMarketDepthRequest` is recorded as
snapshot-then-update, `changeId`/`prevId` continuity is mandatory, protobuf is
an official option, interval is no fixed ms, depth is unspecified, and gaps
rebuild from the REST order-book snapshot.

## Fee Boundary

BigONE `/viewer/trading_fees` 可作为账户 maker/taker 费率来源；当前已补 `request_specs/get_fees_trading_fees.json` 离线 request-spec 边界。shared `get_fees` runtime 仍属项目未实现/未启用；补齐前需完成 balance permission guard、asset-pair filter policy 和 maker/taker parser。
