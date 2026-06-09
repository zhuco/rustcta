# Kraken Futures Gateway Adapter

Status: `rustcta-exchange-gateway` perpetual REST and WebSocket-spec adapter split from the mixed Kraken adapter for Task 6.

## Scope

- Adapter id and registration name: `krakenfutures`
- Product line: Kraken Futures crypto perpetual contracts.
- REST base URL: `https://futures.kraken.com/derivatives/api/v3`
- WebSocket URL: `wss://futures.kraken.com/ws/v1`
- Private REST signing: HMAC-SHA512 base64 with Kraken Futures `APIKey`, `Nonce`, and `Authent` headers.
- Private WS signing: signed challenge response using the Futures API secret.

## Endpoint Mapping

Machine-readable mapping: `crates/rustcta-exchange-gateway/src/adapters/krakenfutures/endpoint_mapping.yaml`.

| Gateway capability | Kraken Futures endpoint/channel | Status |
| --- | --- | --- |
| Symbol rules/contracts | `GET /instruments` | Implemented |
| Order book snapshot | `GET /orderbook` | Implemented |
| Balances/accounts | `GET /accounts` | Implemented |
| Positions | `GET /openpositions` | Implemented |
| Place/cancel/cancel-all | `POST /sendorder`, `POST /cancelorder`, `POST /cancelallorders` | Implemented |
| Batch place/cancel | `POST /batchorder` | Native partial result |
| Query/open orders | `GET /openorders` | Implemented |
| Recent fills | `GET /fills` | Implemented |
| Public/private WS | Futures v1 feeds with signed challenge auth | Subscription/auth/parser fixtures |

## Official WebSocket Order Book Detail

官方核验见 [WebSocket 官方核验 P8 补充交易所盘口细项三](../WebSocket官方核验_P8_补充交易所盘口细项三.md)。Kraken Futures public WS `book` feed 使用 `wss://futures.kraken.com/ws/v1`，订阅 payload 为 `{"event":"subscribe","feed":"book","product_ids":["PI_XBTUSD"]}`。

订阅后有 `book_snapshot`，后续 `book` delta；snapshot 和 delta 都有 `seq`，官方未给固定 ms、无固定档位/depth 参数，也未给 checksum。`seq` 不连续时必须重订阅并以 REST `/derivatives/api/v3/orderbook` snapshot 重建。

## Boundaries

交易所不支持现货：该 adapter/profile 只对应 Kraken Futures API，现货仍由 `kraken` adapter 承接。

Spot remains in `kraken`; this adapter advertises `MarketType::Perpetual` only. Quote-sized spot market orders, spot balances/fees, spot WS token auth, transfers, withdrawals, portfolio margin mutations, and option-style metadata are outside this adapter and remain `Unsupported` or adapter-specific documentation only.

费率项目未实现/未启用：Kraken Futures fee schedule volumes source 已记录到 `tests/fixtures/exchanges/krakenfutures/request_specs/get_fees_source_boundary.json`，适用 Perpetual/Futures。该边界是 signed/private fee schedule source spec，尚未完成 private auth smoke、fee schedule volume parser 和 product fee-schedule mapping；public/default table 只可作为 backtest/config source，生产账户有效费率需要 private readback 或显式 override。

REST reconciliation fallback uses query order, open orders, recent fills, and positions after ambiguous writes or private stream disconnects.

## Validation

- `python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/krakenfutures/endpoint_mapping.yaml`
- `cargo test -p rustcta-exchange-gateway krakenfutures --lib --message-format short`
- `cargo test -p rustcta-gateway krakenfutures --message-format short`
- `cargo check -p rustcta-exchange-gateway --lib --message-format short`

Current workspace note: package-wide Rust checks may be blocked by unrelated in-progress adapters outside `krakenfutures`.
