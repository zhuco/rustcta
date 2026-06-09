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

## Public WebSocket Order Book

Official Realtime API uses JSON-RPC 2.0 over `wss://ws.lightstream.bitflyer.com/json-rpc`.

| Field | bitFlyer detail |
| --- | --- |
| Snapshot channel | `lightning_board_snapshot_{product_code}` |
| Delta channel | `lightning_board_{product_code}` |
| Subscribe method | JSON-RPC `subscribe` with `params.channel` |
| Unsubscribe method | JSON-RPC `unsubscribe` with `params.channel` |
| Fixed interval | Not documented |
| Depth selector | `depth: unspecified`; no fixed depth selector is documented |
| Sequence | Not documented |
| Checksum | Not documented |
| Rebuild | `GET /v1/getboard?product_code={product_code}` plus fresh snapshot/delta subscriptions |

Because the public order-book stream has no replayable sequence or checksum,
disconnects, stale streams and suspected message loss require a REST `getboard`
snapshot rebuild before resubscribing.

## Market Boundary

Spot products such as `BTC_JPY` are exposed as `MarketType::Spot`.
Lightning FX/CFD-style product codes such as `FX_BTC_JPY` are kept distinct and
represented at the adapter boundary as `MarketType::Margin`. They are not
advertised as standard `Futures` or `Perpetual` contracts because bitFlyer
Lightning FX does not match the gateway's standard futures/perpetual contract
model.

Standard Futures/Perpetual is 项目未实现 / adapter-specific boundary, not
`交易所不支持合约`: official bitFlyer docs state Crypto CFD succeeds Lightning
FX with `market_type = FX`. A unified futures/perpetual mapping would need a
separate contract-model decision.

2026-06-09 产品线边界收窄：`endpoint_mapping.yaml` 的
`contract_product` 和 `cfd_fx_product` 均标为 `project_unimplemented`，并绑定
`tests/fixtures/exchanges/bitflyer/request_specs/product_line_source_boundary.json`。
当前 Spot/Lightning FX adapter 不把 `FX_BTC_JPY` 暴露为标准 futures/perpetual；
后续若接入 Crypto CFD，需要独立 contract model、position/margin parser 和
product-scope guard。
状态建议：继续保留 `contract_product` / `cfd_fx_product = 项目未实现`；不要把
Crypto CFD/FX 官方线索写成 `不支持`，也不要把现有 Spot/Lightning product_code
处理提升成标准 Futures/Perpetual runtime。

## Safety Boundary

The adapter intentionally does not implement fiat funding operations. JPY
deposits, withdrawals, bank transfer flows, ledger writes and other fiat account
administration endpoints remain outside gateway runtime scope.

Private order methods require explicit API credentials and do not advertise
client order IDs because bitFlyer child-order acceptance IDs are exchange
generated and are used for cancel/query reconciliation.
