# Independent Reserve gateway adapter

Adapter id: `independentreserve`

## Scope

- Spot markets only.
- Official leveraged trading is `项目未实现 Leveraged trading/Margin-like product`; standard futures/perpetual/options are `交易所不支持合约` under the current official API boundary.
- `endpoint_mapping.yaml` records `leveraged_product` as
  `status: project_unimplemented`, `official_gap: leveraged_trading`, and
  `boundary: project_unimplemented_product_line`; `contract_product` remains
  `unsupported` for standard futures/perpetual/options only.
- Status recommendation: keep leveraged trading as `project_unimplemented`
  until account eligibility, leverage/collateral/position/risk parsers,
  product-scoped private lifecycle, and reconciliation gates are complete.
- AUD, SGD, and USD quote markets are normalized as canonical symbols such as `BTC/AUD`, `BTC/SGD`, and `BTC/USD`.
- Public REST surfaces: market discovery fallback and order book snapshots.
- Private REST surfaces: accounts/balances, place order, cancel order, query order, open orders, and recent fills.
- WebSocket support is exposed as request-spec/session metadata for public order book/trade and private order/trade/account channels.

## Authentication

Independent Reserve private endpoints are POST JSON calls under `/Private/...`. The adapter builds a nonce for each private request, signs the full method URL plus `apiKey`, `nonce`, and endpoint parameters in sorted-key order, and includes the signature in the request body.

This intentionally differs from timestamp-header exchanges. The endpoint version boundary is the public/private path namespace rather than a single `/v3` prefix, so the adapter keeps each Independent Reserve method path explicit.

## Official Public WS Order Book Details

Independent Reserve WebSockets use `wss://websockets.independentreserve.com`.
Order book subscriptions are channel strings such as `orderbook-xbt`, either in
the connection query (`?subscribe=orderbook-xbt`) or in an `Event=Subscribe`
message with `Data: ["orderbook-xbt"]`. The order book channel publishes
order-level `NewOrder`, `OrderChanged`, and `OrderCanceled` events. `NewOrder`
inserts an order, `OrderChanged` updates remaining volume and deletes when
`Volume` is zero, and `OrderCanceled` deletes the order. Each channel and
currency has an increasing `Nonce`; gaps or rewinds require state recovery.
Official docs do not state a fixed millisecond interval or fixed depth. Heartbeat
is currently 60 seconds but may change.

| Channel | Status | Subscription | Interval | Depth | Sequence/checksum | Rebuild |
| --- | --- | --- | --- | --- | --- | --- |
| `orderbook-{primary}` | Spec/parser ready | Connection query `?subscribe=orderbook-xbt` or JSON `Event=Subscribe` / `Data=["orderbook-xbt"]` | No fixed interval documented | No fixed depth documented | `Nonce` strictly increases per channel/currency; no checksum documented | Start from `GET /Public/GetOrderBook`; reconnect and rebuild on nonce gap/regression, reconnect, stale stream, parse error, or suspected message loss |
| `trade` | Spec boundary | Same subscribe envelope | No fixed interval documented | N/A | N/A | N/A |

The fixture `tests/fixtures/exchanges/independentreserve/ws/public_orderbook_new_order.json`
captures a public order book event with `Nonce` so local-book consumers can test
continuity and REST rebuild triggers.

## Fiat and accounting boundary

AUD, SGD, and USD are handled as spot quote assets and read-only account balance assets. The adapter does not implement:

- Fiat deposits or withdrawals.
- Bank payment rails, PayID/FAST/SWIFT metadata, or address generation.
- Tax reports, statement export, or realized tax-lot accounting.
- Account transfers outside the exchange API order/fill/balance read model.

Any future fiat-ledger expansion must be read-only by default and must not reuse trading credentials for payment operations.
