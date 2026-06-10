# KuCoin Futures Gateway Adapter

Status: `rustcta-exchange-gateway` KuCoin Futures perpetual REST and WebSocket-spec adapter for Task 6.

## Scope

- Adapter id and registration name: `kucoinfutures`
- Product line: KuCoin Futures USDT/USD perpetual contracts.
- REST base URL: `https://api-futures.kucoin.com`
- WebSocket endpoint: `wss://ws-api-futures.kucoin.com/endpoint`
- Private REST signing: KuCoin V2 HMAC-SHA256 base64 with `KC-API-KEY`, `KC-API-SIGN`, `KC-API-TIMESTAMP`, signed passphrase, and `KC-API-KEY-VERSION: 2`.
- Private WS auth: REST `POST /api/v1/bullet-private` token, then futures private topics.

## Endpoint Mapping

Machine-readable mapping: `crates/rustcta-exchange-gateway/src/adapters/kucoinfutures/endpoint_mapping.yaml`.

| Gateway capability | KuCoin Futures endpoint/channel | Status |
| --- | --- | --- |
| Contracts/symbol rules | `GET /api/v1/contracts/active` | Implemented |
| Order book snapshot | `GET /api/v1/level2/snapshot` | Implemented |
| Account balances | `GET /api/v1/account-overview` | Implemented |
| Positions | `GET /api/v1/positions` | Implemented |
| Funding history | `GET /api/v1/funding-history` | Implemented through shared `get_funding_rates` |
| Place/cancel/cancel-all | `POST /api/v1/orders`, `DELETE /api/v1/orders/{id}`, `DELETE /api/v1/orders` | Implemented |
| Open orders/query/fills | `GET /api/v1/orders`, `GET /api/v1/orders/{id}`, `GET /api/v1/fills` | Implemented |
| Public WS | `/contractMarket/ticker`, `/contractMarket/level2`, `/contractMarket/execution` | Subscription specs |
| Private WS | `/contractMarket/tradeOrders`, `/contractAccount/wallet`, `/contract/position` | Token/session specs |

## Official WebSocket Order Book Detail

官方核验见 [WebSocket 官方核验 P8 补充交易所盘口细项三](../WebSocket官方核验_P8_补充交易所盘口细项三.md)。KuCoin Futures classic WS 支持 `/contractMarket/level2:{symbol}` increment、`/contractMarket/level2Depth5:{symbol}` 和 `/contractMarket/level2Depth50:{symbol}`；public token endpoint 返回 futures WS server。

increment 是 real-time，5/50 档是 100ms；payload 有 `sequence` 或 `sequenceStart/sequenceEnd`。新 UTA `obu` 也支持 futures BBO real-time、5/50 档 100ms、increment real-time。断档用 REST `/api/v1/level2/snapshot` 缓存回放重建；未见 checksum。YAML 已结构化记录 classic `/contractMarket/level2`、`/contractMarket/level2Depth5/50`、UTA `obu` BBO/5/50、100ms 最快推流、1/5/50 档、`sequence/sequenceStart/sequenceEnd` 和 REST snapshot + buffered delta replay 重建边界。

## Boundaries

交易所不支持现货：该 adapter/profile 只对应 KuCoin Futures API，现货仍由 `kucoin` adapter 承接。

Spot remains in `kucoin`; this adapter advertises `MarketType::Perpetual` only. Gateway order quantity is sent to KuCoin Futures as contract `size`; callers must use contract-size semantics until a shared quantity conversion model exists. Quote-sized market orders, fiat funding operations, transfers, withdrawals, shared leverage mutation, and unverified position/margin-mode mutations are not exposed.

Funding history is available through `ExchangeClient::get_funding_rates` and returns the latest public funding snapshots normalized into `FundingRateSnapshot`.

Batch place/cancel is gateway-composed from sequential REST calls and is non-atomic. REST reconciliation fallback uses query order, open orders, recent fills, positions, and order book snapshots after ambiguous mutations or stream gaps.

## Validation

- `python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/kucoinfutures/endpoint_mapping.yaml`
- `cargo test -p rustcta-exchange-gateway kucoinfutures --lib --message-format short`
- `cargo test -p rustcta-exchange-gateway kucoinfutures_adapter_should_load_latest_funding_rate_from_public_rest -- --nocapture`
- `cargo test -p rustcta-gateway kucoinfutures --message-format short`
- `cargo check -p rustcta-exchange-gateway --lib --message-format short`

Current workspace note: package-wide Rust checks may be blocked by unrelated in-progress adapters outside `kucoinfutures`.
