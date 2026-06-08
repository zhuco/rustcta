# bitFlyer Adapter

Task 13 scope covers bitFlyer Japanese Spot and Lightning FX product-code
boundaries. Venue symbols are preserved as bitFlyer product codes, for example
`BTC_JPY` for Spot and `FX_BTC_JPY` for Lightning FX.

## Coverage

| Area | Status |
| --- | --- |
| Endpoint mapping | `crates/rustcta-exchange-gateway/src/adapters/bitflyer/endpoint_mapping.yaml` |
| Public REST | `GET /v1/getmarkets`, `GET /v1/getboard` |
| Private REST | Balances, child-order place/cancel/cancel-all/query/open-orders, executions/fills, trading commission |
| Signing | `X-API-KEY`, `X-API-TIMESTAMP`, `X-API-SIGN`; prehash is `timestamp + method + request_path + body` |
| Public WS | JSON-RPC subscribe specs for board snapshot/delta, ticker and executions channels |
| Private WS | `child_order_events` subscribe spec with REST reconciliation fallback |
| Fixtures | Offline parser, request-spec and signing vectors under `tests/fixtures/exchanges/bitflyer/` |

## Market Boundary

Spot products such as `BTC_JPY` are exposed as `MarketType::Spot`.
Lightning FX/CFD-style product codes such as `FX_BTC_JPY` are kept distinct and
represented at the adapter boundary as `MarketType::Margin`. They are not
advertised as standard `Futures` or `Perpetual` contracts because bitFlyer
Lightning FX does not match the gateway's standard futures/perpetual contract
model.

## Safety Boundary

The adapter intentionally does not implement fiat funding operations. JPY
deposits, withdrawals, bank transfer flows, ledger writes and other fiat account
administration endpoints remain outside gateway runtime scope.

Private order methods require explicit API credentials and do not advertise
client order IDs because bitFlyer child-order acceptance IDs are exchange
generated and are used for cancel/query reconciliation.
