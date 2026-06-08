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
| Funding history | `GET /api/v1/funding-history` | Mapped/audited; no shared gateway funding model yet |
| Place/cancel/cancel-all | `POST /api/v1/orders`, `DELETE /api/v1/orders/{id}`, `DELETE /api/v1/orders` | Implemented |
| Open orders/query/fills | `GET /api/v1/orders`, `GET /api/v1/orders/{id}`, `GET /api/v1/fills` | Implemented |
| Public WS | `/contractMarket/ticker`, `/contractMarket/level2`, `/contractMarket/execution` | Subscription specs |
| Private WS | `/contractMarket/tradeOrders`, `/contractAccount/wallet`, `/contract/position` | Token/session specs |

## Boundaries

Spot remains in `kucoin`; this adapter advertises `MarketType::Perpetual` only. Gateway order quantity is sent to KuCoin Futures as contract `size`; callers must use contract-size semantics until a shared quantity conversion model exists. Quote-sized market orders, fiat funding operations, transfers, withdrawals, and unverified position/margin-mode mutations are not exposed.

Batch place/cancel is gateway-composed from sequential REST calls and is non-atomic. REST reconciliation fallback uses query order, open orders, recent fills, positions, and order book snapshots after ambiguous mutations or stream gaps.

## Validation

- `python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/kucoinfutures/endpoint_mapping.yaml`
- `cargo test -p rustcta-exchange-gateway kucoinfutures --lib --message-format short`
- `cargo test -p rustcta-gateway kucoinfutures --message-format short`
- `cargo check -p rustcta-exchange-gateway --lib --message-format short`

Current workspace note: package-wide Rust checks may be blocked by unrelated in-progress adapters outside `kucoinfutures`.
