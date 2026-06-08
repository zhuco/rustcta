# Crypto.com Gateway Adapter

`crates/rustcta-exchange-gateway/src/adapters/cryptocom/` implements the
industrial gateway `ExchangeClient` adapter for Crypto.com Exchange Spot and
perpetual markets.

## Scope

- REST public: instruments, order book snapshots, ticker, public trades,
  candlesticks, and perpetual valuation history for mark/index/funding data.
- REST private: Spot balances, perpetual positions, Spot fee rate, place,
  cancel, cancel-all, amend quantity, query, open orders, recent fills,
  quote-sized Spot market buys, native batch place/cancel, and OCO/OTO order
  lists.
- REST signing: JSON-RPC `method + id + api_key + ordered params + nonce`
  HMAC-SHA256 hex signature.
- Streams: public book/trade/ticker/candlestick subscription request specs and
  parsers, private `user.order`, `user.trade`, and `user.balance`
  subscription specs and parsers, private `public/auth`, heartbeat response,
  runtime state, and text-message-to-standard-event session helpers.

## Public WebSocket Order Book

Official Exchange WS `book.{instrument_name}.{depth}` supports explicit depth 10 or 50. For arbitrage, use delta mode `SNAPSHOT_AND_UPDATE` with `book_update_frequency=10` or `100` ms; snapshot-only mode is 500ms and the old depth-less `book.{instrument_name}` form is deprecated. Book updates carry `u` and `pu`; if `pu` does not match the previous `u`, resubscribe to receive a fresh snapshot before trusting local book state.

## Endpoint Mapping

| Standard capability | Crypto.com Exchange v1 endpoint or channel |
| --- | --- |
| `get_symbol_rules` | `public/get-instruments` |
| `get_order_book` | `public/get-book` |
| ticker/trades/candles | `public/get-ticker`, `public/get-trades`, `public/get-candlestick` |
| perp valuations | `public/get-valuations` |
| `get_balances` | `private/user-balance` |
| `get_positions` | `private/get-positions` |
| `get_fees` | `private/get-fee-rate` |
| `place_order` | `private/create-order` |
| `place_quote_market_order` | `private/create-order` with `notional` |
| `cancel_order` | `private/cancel-order` |
| `cancel_all_orders` | `private/cancel-all-orders` |
| `amend_order` | `private/amend-order` after current-order price lookup |
| `query_order` | `private/get-order-detail` |
| `get_open_orders` | `private/get-open-orders` |
| `get_recent_fills` | `private/get-trades` |
| `batch_place_orders` | `private/create-order-list` |
| `batch_cancel_orders` | `private/cancel-order-list` |
| OCO / OTO | `private/advanced/create-oco`, `private/advanced/create-oto` |
| public WS | `book.*`, `trade.*`, `ticker.*`, `candlestick.*` |
| private WS | `public/auth`, then `user.order`, `user.trade`, `user.balance` |
| heartbeat | `public/heartbeat` -> `public/respond-heartbeat` |

## Symbol Projection

Spot symbols use Crypto.com Exchange names such as `BTC_USDT`. Perpetual
symbols are accepted through the same normalized `SymbolScope` path and are
routed as `MarketType::Perpetual` when instrument metadata identifies a
derivative contract.

The parser normalizes base/quote assets, price tick, quantity step, minimum
quantity, minimum notional, instrument state, and depth snapshot sequence/time
where the response includes those fields.

## Credentials

Private REST and private WebSocket require:

```bash
export CRYPTOCOM_API_KEY=...
export CRYPTOCOM_API_SECRET=...
```

The gateway app also accepts `RUSTCTA_CRYPTOCOM_API_KEY` and
`RUSTCTA_CRYPTOCOM_API_SECRET`. `RUSTCTA_CRYPTOCOM_REST_BASE_URL` can override
the REST base URL for UAT or local mock testing.

## Capability Notes

- Private REST remains disabled unless credentials are present and
  `enabled_private_rest` is true.
- `amend_order` keeps the shared interface narrow: Crypto.com requires both
  `new_price` and `new_quantity`, so the adapter reads the current price first
  and submits current price plus replacement quantity. Replacement client order
  IDs are not advertised.
- Perpetual reduce-only orders are supported through `exec_inst`.
- Private position WebSocket is not declared because the implemented user
  channels expose orders, trades, balances, and account updates only.
- The machine-readable endpoint mapping lives at
  `crates/rustcta-exchange-gateway/src/adapters/cryptocom/endpoint_mapping.yaml`.
- `capabilities_v2` declares native partial batch atomicity, max 10 native
  create/cancel list items, server heartbeat response, relogin on reconnect,
  and REST reconciliation through query/open-orders/recent-fills.
- Mark price, funding, open interest, leverage, margin mode, position mode, and
  dead-man switch mutations are outside the current `ExchangeClient` trait or
  return explicit `Unsupported` where no verified native mapping is present.

## Validation

Targeted local validation:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/cryptocom/endpoint_mapping.yaml
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/cryptocom-task-check cargo test -p rustcta-exchange-gateway cryptocom --lib --message-format short
```

The endpoint mapping validator passed. The targeted Crypto.com suite passed 19
tests with 733 filtered out and existing workspace warnings. The tests cover
signing, public REST request specs/parsers, private readback parsers, order
mutations, quote-sized market orders, native batch place/cancel, OCO/OTO
order-list request construction, public/private WebSocket subscription payloads,
authentication payloads, heartbeat response, runtime state hooks, and stream
message conversion. Tests that start local mock REST servers must run outside
the managed sandbox because binding `127.0.0.1` is blocked there.
