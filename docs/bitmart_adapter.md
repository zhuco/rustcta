# BitMart Gateway Adapter

Status: `rustcta-exchange-gateway` Spot + USDT perpetual REST adapter with offline request-spec and WebSocket payload/parser coverage.

## Scope

- Adapter id: `bitmart`
- Spot REST: `https://api-cloud.bitmart.com`
- Futures REST: `https://api-cloud-v2.bitmart.com`
- Market types: `Spot`, `Perpetual`
- WebSocket: Spot/perpetual subscribe payloads, login payload metadata and text `ping`/`pong` heartbeat parser. Live socket orchestration remains outside this adapter.
- Sandbox: no stable public sandbox base URL wired; base URLs are configurable.

## Endpoint Mapping

| Gateway capability | BitMart endpoint | Notes |
| --- | --- | --- |
| Symbol rules | `GET /spot/v1/symbols/details`, `GET /contract/public/details` | Parses Spot `BASE_QUOTE` and perpetual compact symbols. |
| Order book | `GET /spot/v1/symbols/book`, `GET /contract/public/depth` | REST snapshot is the WS resync source. |
| Balances | `GET /spot/v1/wallet`, `GET /contract/private/assets-detail` | Private REST only; requires API key/secret and optional memo. |
| Positions | `GET /contract/private/position` | Perpetual only. |
| Order lifecycle | `POST /spot/v2/submit_order`, `POST /spot/v3/cancel_order`, `GET /spot/v2/order_detail`, `GET /spot/v2/orders` | Perpetual routes use `/contract/private/*`; request construction is covered by offline tests. |
| Quote market buy | `POST /spot/v2/submit_order` | Spot market buy maps quote quantity to BitMart `notional`; Spot sell and perpetual quote-sized market orders stay unsupported. |
| Batch cancel / cancel all | `POST /contract/private/cancel-orders`, `POST /spot/v3/cancel_orders` | Futures batch cancel is native. Cancel-all uses Spot cancel-all or futures symbol/all cancel semantics. |
| Fills | `GET /spot/v2/trades`, `GET /contract/private/trades` | Used for REST reconciliation after private stream gaps. |

## Authentication

Private REST uses `X-BM-KEY`, `X-BM-TIMESTAMP`, `X-BM-SIGN` and optional `X-BM-BROKER-ID`. The signing fixture covers HMAC-SHA256 over `timestamp#memo#body` for v3-style memo credentials.

## Unsupported / Boundary

- Withdraw, transfer, internal transfer and funding movement APIs.
- Margin loan and funding book runtime features.
- Shared runtime batch place/amend/order-list. BitMart futures `/contract/private/submit-plan-order` is a plan/trigger-order endpoint, so it is not exposed as regular shared batch-place.
- WS-only private state. REST account, positions, orders and fills remain the reconciliation source.

## Validation

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bitmart/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway bitmart --lib --message-format short
```
