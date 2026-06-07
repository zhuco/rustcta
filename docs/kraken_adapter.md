# Kraken Adapter

Status: `rustcta-exchange-gateway` Spot + Kraken Futures adapter migrated for task 1. The adapter has endpoint mapping, capabilities v2 declarations, request-spec/signing/parser fixture tests, native batch capability declarations, public/private WebSocket subscription/session helpers, public order-book stream conversion, private order/fill/balance/position stream conversion, rate-limit planning notes, pagination mapping, and reconciliation guidance.

## Products And Symbols

| Product | Exchange symbol | Canonical symbol | Notes |
| --- | --- | --- | --- |
| Spot | `XBTUSD` | `BTC/USD` | Uses Kraken `altname` for REST and `BTC/USD` slash form for Spot v2 WebSocket. `XBT -> BTC`, `XDG -> DOGE`, and leading `X`/`Z` asset prefixes are normalized. |
| Perpetual | `PF_XBTUSDT` | `BTC/USDT` | Futures symbols keep `PF_`/`PI_` exchange ids for REST/WS and strip the prefix for canonical symbols. |

Account semantics are split by product. Spot balances use `BalanceEx` with `Balance` fallback and no positions. Futures balances use `accounts`; positions use `openpositions`. Request context must provide `tenant_id` and `account_id` for balance/fill/position normalization.

## Endpoint Mapping

The authoritative adapter-local mapping is `crates/rustcta-exchange-gateway/src/adapters/kraken/endpoint_mapping.yaml`.

Covered private REST operations include `AddOrder`, `CancelOrder`, `CancelAll`, `QueryOrders`, `OpenOrders`, `TradesHistory`, `TradeVolume`, `AddOrderBatch`, `CancelOrderBatch`, `GetWebSocketsToken`, and the Futures `sendorder`, `cancelorder`, `cancelallorders`, `orders`, `openorders`, `fills`, `accounts`, `openpositions`, and `batchorder` endpoints.

## Signing

Spot signing uses Kraken HMAC-SHA512 base64 over `url_path + SHA256(nonce + form_body)`, with `API-Key` and `API-Sign` headers. Futures REST signing uses HMAC-SHA512 base64 over `form_query_or_body + nonce + /derivatives/api/v3/{endpoint}`, with `APIKey`, `Nonce`, and `Authent` headers. Futures private WebSocket signs `SHA256(challenge)`.

Signing vector tests:

- `kraken_spot_signature_should_match_official_vector`
- `kraken_spot_signed_request_should_build_form_body_and_headers`
- `kraken_futures_signed_request_should_include_new_auth_inputs`
- `kraken_futures_ws_challenge_signature_should_be_deterministic`

## Pagination

`get_recent_fills` maps existing request fields to Kraken pagination without adding shared API types:

| Product | Endpoint | Supported fields |
| --- | --- | --- |
| Spot | `POST /0/private/TradesHistory` | `limit -> count`, `from_trade_id/page offset/id/token -> ofs`, `start_time/end_time -> start/end`, timestamp/time-range cursor -> `start/end` |
| Perpetual | `GET /derivatives/api/v3/fills` | `limit -> count`, `from_trade_id/page id/token/timestamp -> lastFillTime` |

Futures offset and time-range cursors are rejected because the Futures fills endpoint is time-cursor based. `OpenOrdersRequest` has no page field, so open-order reconciliation is full readback plus symbol filter.

## WebSocket Policy

Public streams use separate Spot and Futures connections. Spot v2 channels map to `trade`, `ticker`, `book`, and `ohlc`; Futures maps to `trade`, `ticker`, and `book`, with candles marked unsupported. Heartbeat sends `{"method":"ping"}` and accepts `pong`, `channel=heartbeat`, and `event=heartbeat`. Spot/Futures `book` messages are normalized into standard `ExchangeStreamEvent::OrderBookSnapshot` events and reuse the REST order-book parsers.

Private Spot streams require `GetWebSocketsToken` and subscribe to `executions` or `balances`. Spot `executions` updates emit standard order or fill events according to the requested private stream kind; Spot `balances` emits `BalanceSnapshot`. The token must be refreshed before expiry; renewal failure requires reconnect and resubscribe. Spot private positions are unsupported and require REST reconciliation fallback.

Private Futures streams request a challenge, sign it, and subscribe to `open_orders`, `fills`, `balances`, or `open_positions`. Futures private messages emit standard order, fill, balance, or position stream events. Reconnect requires a new challenge and resubscribe.

Orderbook resync policy is REST snapshot after reconnect, stale heartbeat, or sequence/orderbook gap.

## Batch And Reconciliation

Batch place/cancel are native but partial:

| Product | Place | Cancel | Constraints |
| --- | --- | --- | --- |
| Spot | `AddOrderBatch` | `CancelOrderBatch` | Place requires 2-15 orders for one pair; cancel supports up to 50 ids. |
| Perpetual | `batchorder` send rows | `batchorder` cancel rows | One market type per batch; partial row failures require reconciliation. |

Reconciliation source of truth is `query_order`, then `get_open_orders`, then `get_recent_fills`. Use reconciliation for place/cancel timeout, missing batch item, private stream disconnect, unknown order state, and orderbook resync. Live-dry-run requires enabled private REST, disabled-symbol gate, max-notional gate, and cancel-all availability for the target market type.

## Tests And Fixtures

Fixtures live in `tests/fixtures/exchanges/kraken/` and cover successful parser payloads, empty responses, error envelopes, and missing critical fields. Request-spec tests cover private REST paths, headers, form/query payloads, batch endpoints, WebSocket token fetch, Spot/Futures public subscription payloads, private token/challenge payloads, public order-book stream conversion, private order/fill/balance/position stream conversion, heartbeat handling, auth renewal behavior, and pagination parameters.

Recommended targeted validation:

```bash
cargo fmt
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/gateway-clean-check cargo test -p rustcta-exchange-gateway kraken --lib --message-format short
```

Latest targeted validation: 41 Kraken tests passed with existing workspace warnings.

## Known Boundaries

- Spot symbol-scoped cancel-all remains unsupported because the adapter only exposes Kraken `CancelAll`.
- Futures private stream auth renewal is modeled as challenge-on-reconnect; there is no persistent listen-key keepalive.
- Open orders cannot expose cursor pagination until the shared `OpenOrdersRequest` gains a page field.
- Rate-limit buckets are conservative planning declarations in the endpoint mapping; live admission should still obey Kraken account-tier behavior.
