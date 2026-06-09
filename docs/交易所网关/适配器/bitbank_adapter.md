# bitbank Adapter Migration

Task 13 bitbank scope is Japanese Spot markets. Venue symbols are lowercase
underscore pairs such as `btc_jpy`; JPY quote markets are first-class and the
adapter does not expose fiat funding, bank transfer, withdrawal, or ledger write
operations.

## Coverage

| Area | Status |
| --- | --- |
| Endpoint mapping | `crates/rustcta-exchange-gateway/src/adapters/bitbank/endpoint_mapping.yaml` |
| REST signing | `ACCESS-KEY`, `ACCESS-NONCE`, `ACCESS-SIGNATURE`; GET signs `nonce + /v1 + path + query`, POST signs `nonce + json_body` |
| Public REST | Order book snapshot via `https://public.bitbank.cc/{pair}/depth`; pair/rule metadata via unauthenticated `https://api.bitbank.cc/v1/spot/pairs` with `/spot/status` parser fallback |
| Private REST | Credential-gated balances, query/open/fills readback, and native same-pair batch cancel are implemented; other private write specs remain guarded pending dry-run policy |
| Public WS | Socket.IO room policy for depth, trades, ticker, and candles; REST snapshot is the resync fallback |
| Private WS | PubNub policy documented via `/user/subscribe`; runtime returns stream specs and relies on REST reconciliation fallback |
| Fixtures | Offline parser fixtures, private request specs, and HMAC signing vectors live under `tests/fixtures/exchanges/bitbank/` |

## Runtime Notes

The adapter supports Spot only. Private `GET /v1/user/assets` is implemented as
a credential-gated read-only balance runtime and parses `data.assets` into
shared Balance snapshots. Limit, market, and post-only limit order specs are
documented, but live single-order writes remain guarded. `client_order_id`,
IOC/FOK, reduce-only, quote-quantity market order, in-place amend, and
cancel-all are intentionally unsupported.

Product-line boundary: bitbank official REST API also documents Margin
status/positions. The current project has `项目未实现 Margin`; standard
futures/perpetual/options are `交易所不支持合约` under the current official API
scope.
`endpoint_mapping.yaml` records this split as `margin_product` with
`status: project_unimplemented`, `official_gap: margin_status_positions`, and
`boundary: project_unimplemented_product_line`, while `contract_product`
remains `unsupported` for standard futures/perpetual/options only.
Status recommendation: keep `margin_product=project_unimplemented` until
Margin account eligibility, status/position parsers, risk policy, product
order lifecycle, and reconciliation are implemented. Keep
`contract_product=unsupported` only for standard futures/perpetual/options.

Official public WebSocket order book details: `depth_diff_{pair}` and
`depth_whole_{pair}` are Socket.IO `join-room` channels. Official docs do not
state a fixed millisecond interval. `depth_whole` is capped at 200 asks and 200
bids in normal mode, while `depth_diff.s` and `depth_whole.sequenceId` are
monotonic but not always consecutive.

Current implementation note: `endpoint_mapping.yaml` now declares the
structured Socket.IO order book fields, and `streams.rs` exposes
`public_order_book_ws_policy()` for `depth_whole_{pair}`/`depth_diff_{pair}`,
200 levels per side, no fixed ms interval, non-contiguous sequence, and
whole/diff rebuild. The adapter capability marks the book as best-effort delta
with sequence support and REST snapshot resync.

Native same-pair batch cancel is represented as a runtime-backed signed request via
`batch_cancel_orders_spec` and
`tests/fixtures/exchanges/bitbank/request_specs/batch_cancel_orders.json`.
The adapter now also exposes this as a `capabilities_v2.batch_cancel_orders`
native boundary with same-symbol and partial-failure metadata. Runtime is guarded
by explicit private REST credentials, rejects mixed-pair/client-id cancels, and
uses `tests/fixtures/exchanges/bitbank/batch_cancel_orders_success.json` in the
mock parser test.
Batch place, in-place amend, and order-list/OCO remain unsupported because the
reviewed Spot REST surface does not expose native equivalents.

Private WebSocket capability is declared only when REST credentials are present.
Production routing should reconcile private stream gaps with `query_order`,
`get_open_orders`, and `get_recent_fills`.
