# Coinone Adapter

Coinone is implemented as a Korea regional spot adapter scoped to KRW markets. The adapter normalizes exchange symbols as `BASE-KRW` and rejects non-KRW quotes before request construction.

## Supported Surfaces

- Public REST: KRW market metadata and order book snapshots.
- Public WebSocket: ticker, trades, and order book channels.
- Private REST: balances, trade fees, place order, quote market buy, cancel, cancel all, query order, open orders, and recent fills.
- Private WebSocket: order/fill and balance/account subscriptions with Coinone payload signature headers.

## Official Public WS Order Book Details

Coinone public WebSocket uses `wss://stream.coinone.co.kr`. ORDERBOOK
subscription payloads use `request_type=SUBSCRIBE`, `channel=ORDERBOOK`, and a
topic with `quote_currency` and `target_currency`. Official docs say the server
sends the last order book once on initial subscription and then sends updates
when the order book changes. No fixed millisecond interval or configurable depth
parameter is published (`depth: unspecified`). Messages include an order book `id`; a larger id is more
recent, but no checksum is documented.

| Surface | Channel / Request | First Message | Updates | Depth | Interval | Sequence / ID | Checksum | Resync |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Public order book WS | `ORDERBOOK`; `SUBSCRIBE` with `topic.quote_currency` and `topic.target_currency` | Last order book snapshot once after subscription | Change-only pushes after the first snapshot | Not documented; no request depth parameter | Not documented; event-driven on book changes | `data.id` / short `d.i`; larger id is newer, but it is not a documented contiguous sequence | None documented | REST `GET /public/v2/orderbook/KRW?target_currency={base}` snapshot after connect/reconnect/stale stream/suspected loss, then resubscribe |

## Authentication Boundary

Private REST/WS requires `COINONE_SPOT_ACCESS_TOKEN` and `COINONE_SPOT_SECRET_KEY` (or the non-spot-prefixed fallback names). Requests are signed by JSON-serializing the private payload, base64-encoding it into `X-COINONE-PAYLOAD`, then HMAC-SHA512 signing that encoded payload into `X-COINONE-SIGNATURE`.

Only read-only and trade credential scopes are modeled. Transfer and withdraw scopes are intentionally not requested or surfaced.

## Unsupported Boundary

Standard futures, perpetuals, and options are `交易所不支持合约` under the current
official API v2/v2.1 scope.

Payment, wallet withdrawal/deposit, deposit address, and fiat-transfer behavior is explicitly unsupported. The adapter should return `Unsupported` for those boundaries and should not route those endpoints through generic private REST helpers.

Advanced order runtime is explicitly unsupported for shared amend, OCO/OTO/order-list, native batch place, and native batch cancel. `cancel_all_orders` remains a separate native single endpoint and is not treated as native batch-cancel support.

Executable P4 boundary evidence:
- `endpoint_mapping.yaml` maps `amend_order`, `place_order_list`, `batch_place_orders`, and `batch_cancel_orders` as `support: unsupported`, `auth: unsupported`, and `native_batch: false`.
- `unsupported_boundary.json` records the expected runtime `Unsupported.operation` values and fixture evidence.
- `private_tests.rs` asserts private-enabled capabilities still keep amend/order-list/batch disabled, while preserving native `cancel_all_orders`.
- Runtime is not promoted because Coinone v2/v2.1 documents single place/cancel and cancel-all, but not shared amend, OCO/OTO/order-list, native batch place, or native batch cancel semantics.

## Fixtures

Coinone fixtures live under `tests/fixtures/exchanges/coinone/`:

- `request_specs/` documents private request paths and signed-header expectations.
- `signing_vectors/` validates payload base64 and HMAC-SHA512 signatures.
- `parser/markets_krw.json`, `orderbook.json`, `fill.json`, and `balance.json` cover public and private parser surfaces.
- `ws_public_orderbook.json`, `ws_public_orderbook_snapshot.json`, and `ws_public_orderbook_update.json` cover public ORDERBOOK subscribe, initial snapshot, and later change-only update shapes.
- `unsupported_boundary.json` documents the payment/wallet/fiat-transfer and advanced-order unsupported boundaries.
