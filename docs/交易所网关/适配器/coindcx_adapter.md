# CoinDCX Gateway Adapter

CoinDCX adapter id: `coindcx`

Status: Spot + Futures REST and Socket.IO request-spec/parser first pass for `rustcta-exchange-gateway`.

Task 18 migration artifacts:

- Endpoint mapping: `crates/rustcta-exchange-gateway/src/adapters/coindcx/endpoint_mapping.yaml`
- Capabilities v2 equivalent declaration: `crates/rustcta-exchange-gateway/src/adapters/coindcx/capabilities_v2.yaml` plus runtime `capabilities().capabilities_v2`
- Request-spec fixtures: legacy `tests/fixtures/exchanges/coindcx/request_spec/` plus standard `tests/fixtures/exchanges/coindcx/request_specs/private_rest.json`
- Signing vector: legacy `tests/fixtures/exchanges/coindcx/signing/hmac_compact_json.json` plus standard `tests/fixtures/exchanges/coindcx/signing_vectors/hmac_compact_json.json`
- Parser fixtures: `tests/fixtures/exchanges/coindcx/parser/`

## Endpoint Mapping

| Standard capability | CoinDCX endpoint / channel | Implementation |
| --- | --- | --- |
| Spot symbol rules | `GET /exchange/v1/markets_details` | Parses base/quote, tick/step, min notional and active state |
| Futures symbol rules | `GET /exchange/v1/derivatives/futures/data/active_instruments` | Parses active futures instruments as `MarketType::Perpetual` |
| Spot order book | `GET https://public.coindcx.com/market_data/orderbook?pair=...` | Snapshot parser supports object and array depth levels |
| Futures order book | `GET https://public.coindcx.com/market_data/v3/orderbook/{instrument}-futures/{depth}` | Snapshot parser, max depth declared as 50 |
| Private signing | HMAC-SHA256 of compact JSON body | Sends `X-AUTH-APIKEY` and `X-AUTH-SIGNATURE`; timestamp injected into JSON body |
| Spot balances | `POST /exchange/v1/users/balances` | Parses free/locked/total balances |
| Futures wallets | `POST /exchange/v1/derivatives/futures/wallets` | Parsed through balance snapshot model |
| Futures positions | `POST /exchange/v1/derivatives/futures/positions` | Parses quantity, side, entry, mark, liquidation, PnL, leverage |
| Place order | Spot `/exchange/v1/orders/create`; Futures `/exchange/v1/derivatives/futures/orders/create` | Limit/market mapping; Spot client order id; Futures client id explicitly unsupported |
| Batch place | Spot `/exchange/v1/orders/create_multiple` | Native Spot batch only; Futures batch place explicitly `Unsupported` |
| Cancel order | Spot `/exchange/v1/orders/cancel`; Futures `/exchange/v1/derivatives/futures/orders/cancel` | Spot id or client id; Futures id only |
| Batch cancel | Spot `/exchange/v1/orders/cancel_by_ids` | Native Spot ids/client ids; Futures multi-id cancel explicitly `Unsupported` |
| Cancel all | Spot `/exchange/v1/orders/cancel_all`; Futures `/exchange/v1/derivatives/futures/positions/cancel_all_open_orders` | Symbol-scoped cancel-all |
| Amend | Spot `/exchange/v1/orders/edit`; Futures `/exchange/v1/derivatives/futures/orders/edit` | Standard quantity amend request body; new client id unsupported |
| Query/open/fills | Spot status/active/trade-history; Futures active-orders/trades | Standard order/fill parser |
| Public streams | Socket.IO channels such as official Spot `B-BTC_USDT@orderbook@20`, project Spot `{symbol}@orderbook@50`, Futures `{instrument}@orderbook@50-futures`, trades, prices, candles | Session helper emits Socket.IO `join` payload; `depth-update` order book parser emits standard snapshot event; trade/ticker/candle channels are subscribed but have no separate standard event type in `ExchangeStreamEvent` |
| Private streams | Socket.IO `coindcx` channel with `order-update`, `trade-update`, `balance-update`, `df-position-update` | HMAC auth join payload, heartbeat, order/fill/balance/position event parser |
| Heartbeat | Socket.IO ping every 25s | `ping` payload helper and heartbeat stream event |

## Public Order Book WebSocket

| Scope | Socket.IO subscribe / update | Depth | Cadence | Sequence / checksum | Recovery |
| --- | --- | --- | --- | --- | --- |
| Spot official example | `{"event":"join","channelName":"B-BTC_USDT@orderbook@20"}`; update event `depth-update` | `20` in official sample | No fixed millisecond interval documented | No documented sequence; no checksum | Reconnect, resubscribe, rebuild from REST `get_order_book` snapshot |
| Spot project boundary | `{"event":"join","channelName":"{symbol}@orderbook@50"}` | Project default/max `50` | No fixed millisecond interval documented | No documented sequence; no checksum | Same REST snapshot rebuild policy |
| Futures project boundary | `{"event":"join","channelName":"{instrument}@orderbook@50-futures"}` | Project default/max `50` | No fixed millisecond interval documented | No documented sequence; no checksum | Same REST snapshot rebuild policy |

Focused fixtures live under `tests/fixtures/exchanges/coindcx/ws/` and cover the official Spot `@orderbook@20` join, project Spot/Futures `@orderbook@50` joins, and a Socket.IO `42["depth-update", ...]` payload. Because CoinDCX public depth updates do not document a monotonic sequence or checksum, the adapter treats the stream as best-effort delta and requires REST snapshot rebuild after reconnect, stale-message detection, parse failure, or any suspected gap.

## Capabilities v2 / runtime policy

- Public REST: native for Spot and perpetual symbol metadata plus order book snapshots.
- Private REST: native when `api_key` and `api_secret` are configured.
- Public stream runtime: Socket.IO transport with join payloads for order book, trades, prices and candles. It requires REST snapshot resync after reconnect or stale stream.
- Private stream runtime: Socket.IO `coindcx` channel with HMAC join payload. Private auth is re-created on reconnect; policy declares `ReLogin`, 30 minute renewal interval, reconnect on renewal failure and resubscribe after renewal. There is no listen-key lease.
- Heartbeat policy: application-level ping every 25,000 ms, pong timeout 35,000 ms, stale message threshold 45,000 ms.

## Rate limit plan

The adapter declares conservative fixed-window buckets in `endpoint_mapping.yaml`:

| Bucket | Scope | Limit/window | Notes |
| --- | --- | --- | --- |
| `coindcx.public_rest` | IP | 300 / 60s | Symbol metadata and public instruments |
| `coindcx.public_market` | IP | 600 / 60s | Order book market-data snapshots |
| `coindcx.private_rest` | Account | 300 / 60s | Balances, positions, query/open/fills |
| `coindcx.orders` | Orders | 120 / 60s | Place/cancel/amend/batch/cancel-all |

These are adapter-side planning limits, not exchange header accounting. Production deployment should tighten them if CoinDCX account tier limits are lower.

## Pagination capability

- Cursor pagination is unsupported.
- Recent fills support a `limit` body field, capped by the adapter at 1000.
- Open orders, query order, balances and positions are non-paginated in the current implementation.
- Order history beyond active orders and recent fills is not declared.

## Reconciliation plan

- Place timeout or unknown place response: query order by client order id when available, otherwise exchange order id; then check open orders and recent fills. The adapter does not replay orders automatically.
- Cancel timeout: query by exchange order id or client id for Spot, then check open orders. Futures client-order-id reconciliation is unsupported because futures client ids are not declared.
- Private stream disconnect or auth renewal: reconnect, login/join again, resubscribe, fetch REST order book snapshots, open orders, recent fills, balances and futures positions.
- Duplicate client order id: treat as reconciliation-required and query by client order id for Spot.

## Batch capability

| Operation | Product | Mode | Atomicity | Max items | Partial failure |
| --- | --- | --- | --- | --- | --- |
| Place | Spot | Native `/exchange/v1/orders/create_multiple` | Partial | 10 | Supported |
| Cancel | Spot | Native `/exchange/v1/orders/cancel_by_ids` | Partial | 50 | Supported |
| Place | Perpetual | Unsupported | N/A | N/A | N/A |
| Cancel | Perpetual | Unsupported | N/A | N/A | N/A |

## Boundaries

CoinDCX streams are Socket.IO style. The adapter exposes subscription/session helpers and parser coverage, but does not pretend the endpoint is a plain JSON WebSocket.

Official public depth examples use Socket.IO `join` with channel names such as `B-BTC_USDT@orderbook@20`; the current project mapping also records Spot `{symbol}@orderbook@50` and Futures `{instrument}@orderbook@50-futures` boundaries. The reviewed official docs did not expose a fixed millisecond interval, sequence, or checksum for public `depth-update` messages, so public stream runtime must rebuild from REST order book snapshots after reconnect or stale-message detection.

Futures batch place and multi-id batch cancel are not exposed because the official API surface only confirmed single order create/cancel plus cancel-all. Futures client order id, standard reduce-only flag, reliable futures post-only, and Binance-style OCO/OTO order lists are explicitly unsupported.

The fee endpoint was not confirmed in the official CoinDCX public docs reviewed for this adapter, so fee snapshots are marked with a placeholder source and should not be used for production fee accounting without account-specific reconciliation.

## Validation

Targeted validation:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/coindcx/endpoint_mapping.yaml
python3 -m json.tool tests/fixtures/exchanges/coindcx/request_spec/cancel_order_spot.json
rustfmt --edition 2021 --check crates/rustcta-exchange-gateway/src/adapters/coindcx/*.rs
CARGO_TARGET_DIR=target/task18-gateway-tests-final cargo test -p rustcta-exchange-gateway coindcx --lib --message-format short
```

The parser tests cover success, empty response, error fixture presence and key missing-field failures. Request-spec tests assert generated CoinDCX JSON bodies match fixture expectations. Signing tests assert HMAC-SHA256 hex output against the sanitized fixture vector.

## Fee Boundary

交易所不支持当前费率接口 runtime：fee endpoint 未确认，当前只返回零费率占位。
