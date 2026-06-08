# CEX.IO Gateway Adapter

Status date: 2026-06-08

## Scope

Adapter id: `cex`.

This adapter covers CEX.IO spot public REST at G1:

- `get_symbol_rules` via `GET /currency_limits`
- `get_order_book` via `GET /order_book/{symbol1}/{symbol2}/`
- offline REST and WebSocket signing/payload fixtures
- explicit `Unsupported` boundaries for private runtime trading, fiat ledger operations, margin, futures and transfers
- Contract/Futures/Options: 交易所不支持合约（当前 CEX.IO Spot Trading API 口径）。

## Official Interfaces

| Surface | URL | Status |
| --- | --- | --- |
| Legacy REST API | `https://cex.io/api` | Used for public REST implementation |
| WebSocket API | `wss://ws.cex.io/ws/` | Payload and heartbeat helpers only |
| Spot Trading Public WS | `wss://trade.cex.io/api/spot/ws-public` | Officially supports `order_book_subscribe` snapshot and `order_book_increment`; 项目未实现公共 WS runtime |
| Private REST | JSON POST body with `nonce`, `key`, `signature` | Request-spec boundary only |

The REST docs publish `currency_limits` and `order_book` public endpoints. Private REST uses HMAC-SHA256 over `nonce + user_id + api_key`; WebSocket auth signs `timestamp + api_key`.

## Capability

| Capability | Status | Notes |
| --- | --- | --- |
| Spot symbol rules | Native | Parsed from `currency_limits.data.pairs` |
| Spot order book snapshot | Native | `depth` query is bounded to 1-100 locally |
| Private balances/orders/fills | Unsupported | Requires follow-up read-only request-spec promotion |
| Private order writes | Unsupported | Fixture documents request shape; adapter does not send signed writes |
| Batch place/cancel | Unsupported | CEX.IO mass-cancel-place is not mapped to generic batch semantics |
| WebSocket runtime | 项目未实现公共 WS runtime | Payload/auth/pong helpers are present; official Spot Trading WS has order book snapshot/increment with `seqId`, but no production socket runtime |
| Fiat ledger, deposit, withdrawal, transfer | Unsupported | Not a trading runtime operation |
| Contract/Futures/Options | 交易所不支持合约 | Current public trading documentation is CEX.IO Spot Trading REST/WS. |

## Official Core Trading Detail

P0 core-trading verification confirms CEX.IO Spot Trading supports private
trading through REST and WebSocket APIs: `do_new_order`,
`do_cancel_my_order`, and `do_cancel_all_orders`. Spot Trading supports Market,
Limit, and Stop Limit orders; TIF includes GTC, IOC, and GTD, with Market
orders restricted to IOC. `clientOrderId` and `cancelRequestId` are part of the
official flow. Current project private writes remain request-spec/unsupported
runtime, so this is `项目未实现核心交易接口`, not `交易所不支持下单/撤单`.

## Files

- Adapter: `crates/rustcta-exchange-gateway/src/adapters/cex/`
- Mapping: `crates/rustcta-exchange-gateway/src/adapters/cex/endpoint_mapping.yaml`
- Fixtures: `tests/fixtures/exchanges/cex/`
- Disabled config: `config/cex_gateway_example.yml`

## Validation

Allowed commands:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/cex/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway cex --lib --message-format short
cargo test -p rustcta-gateway cex --message-format short
```

Do not run `cargo build` for this task.
