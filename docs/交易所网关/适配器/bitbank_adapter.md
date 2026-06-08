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
| Private REST | Balances, fees, place order, cancel order, native same-pair batch cancel, query order, open orders, recent fills |
| Public WS | Socket.IO room policy for depth, trades, ticker, and candles; REST snapshot is the resync fallback |
| Private WS | PubNub policy documented via `/user/subscribe`; runtime returns stream specs and relies on REST reconciliation fallback |
| Fixtures | Offline parser fixtures, private request specs, and HMAC signing vectors live under `tests/fixtures/exchanges/bitbank/` |

## Runtime Notes

The adapter supports Spot only. Limit, market, and post-only limit orders are
mapped to bitbank's Spot order endpoint. `client_order_id`, IOC/FOK, reduce-only,
quote-quantity market order, in-place amend, and cancel-all are intentionally
unsupported.

Product-line boundary: bitbank official REST API also documents Margin
status/positions. The current project has `项目未实现 Margin`; standard
futures/perpetual/options are `交易所不支持合约` under the current official API
scope.

Official public WebSocket order book details: `depth_diff_{pair}` and
`depth_whole_{pair}` are Socket.IO `join-room` channels. Official docs do not
state a fixed millisecond interval. `depth_whole` is capped at 200 asks and 200
bids in normal mode, while `depth_diff.s` and `depth_whole.sequenceId` are
monotonic but not always consecutive. Runtime mapping still needs structured
channel/depth/interval/rebuild fields.

Private WebSocket capability is declared only when REST credentials are present.
Production routing should reconcile private stream gaps with `query_order`,
`get_open_orders`, and `get_recent_fills`.
