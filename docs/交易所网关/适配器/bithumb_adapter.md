# Bithumb Gateway Adapter

Status date: 2026-06-08

## Scope

- Adapter id: `bithumb`
- Product line: Korean Spot only
- Markets: KRW spot markets using `QUOTE-BASE` exchange symbols, for example `KRW-BTC`
- REST base URL: `https://api.bithumb.com`
- Public WS: `wss://ws-api.bithumb.com/websocket/v1`
- Private WS: `wss://ws-api.bithumb.com/websocket/v1/private`

This adapter is offline-valid for public REST, private REST request construction, JWT signing vectors, and WebSocket payload/parser specs. It does not perform live trading validation.

## Official API Notes

- Public market list and order book use `/v1/market/all` and `/v1/orderbook`.
- Private REST uses `Authorization: Bearer <jwt>`.
- JWT payload includes `access_key`, `nonce`, Bithumb `timestamp`, and `query_hash`/`query_hash_alg=SHA512` when query or body parameters are present.
- Private write request specs are fixture-backed under `tests/fixtures/exchanges/bithumb/request_specs/`.

## Rate Limits

The mapping keeps conservative local buckets:

- `bithumb_rest`: 10 requests per second
- `bithumb_orders`: 5 order requests per second
- `bithumb_ws`: conservative connection/message budget

If Bithumb account-level policy or region controls are stricter for a key, runtime config must lower these limits. This adapter does not implement region bypass.

## Implemented Offline Surface

- Symbol rules: Spot market list parser
- Order book: REST snapshot parser and public WS orderbook payload spec
- Private read REST: balances, fee snapshot via order chance, order query, open orders, recent fills
- Private write REST: place order, quote market buy, cancel order request construction
- Private WS: order/fill/balance subscription payloads and REST reconciliation fallback

## Official Public WS Order Book Details

- Public URL: `wss://ws-api.bithumb.com/websocket/v1`.
- Protocol: WebSocket JSON array request with `ticket`, `type`, and `format` objects.
- Order book channel: request type `orderbook`, `codes: ["KRW-BTC"]`, `is_only_snapshot` for snapshot-only mode, and `is_only_realtime` for realtime-only updates.
- Push interval: official docs do not state a fixed millisecond interval.
- Depth and aggregation: response contains `orderbook_units`; `level` is a price grouping unit, not a fixed 10/20/50-depth selector. Adapter policy uses `level: 0` by default.
- Sequence/checksum: official orderbook page exposes timestamp fields but no sequence/checksum; runtime must use REST snapshot/stale-book fallback.

| 通道 | 产品线 | 状态 | 订阅方式 | 官方 channel/stream | 推流间隔 | 档位 | 序列/校验 | 重建策略 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Public order book snapshot | Spot | 已结构化 | `wss://ws-api.bithumb.com/websocket/v1` JSON array; `is_only_snapshot: true` | `orderbook` | 官方未给固定 ms | `orderbook_units`; `level` controls price aggregation, default `0` | 无官方 sequence/checksum | connect/reconnect/stale stream 后取 REST `GET /v1/orderbook?markets={market}` snapshot 并重订阅 | `stream_type: SNAPSHOT` |
| Public order book realtime | Spot | 已结构化 | `wss://ws-api.bithumb.com/websocket/v1` JSON array; `is_only_realtime: true` | `orderbook` | 官方未给固定 ms | `orderbook_units`; no fixed depth selector | 无官方 sequence/checksum，无法从 WS 单独证明缺包 | REST snapshot + resubscribe fallback | `stream_type: REALTIME` |

Fixtures:

- `tests/fixtures/exchanges/bithumb/ws/public_orderbook_subscribe.json`
- `tests/fixtures/exchanges/bithumb/ws/public_orderbook_snapshot_subscribe.json`
- `tests/fixtures/exchanges/bithumb/ws/orderbook_snapshot.json`
- `tests/fixtures/exchanges/bithumb/ws/orderbook_realtime.json`

## Unsupported Boundary

- Futures, perpetuals, options, and standard contract trading: `交易所不支持合约` under the current official Open API scope.
- Positions, leverage, margin mode, position mode: unsupported because Bithumb Spot has no standard derivative position model.
- Amend order: unsupported until a native endpoint is verified against the shared Spot amend contract.
- OCO/OTO/order-list, batch place/cancel, and cancel-all: unsupported; no native atomic batch or shared order-list endpoint is claimed.
- Fiat funding, withdrawal, transfer, and bank operations are not part of the gateway runtime.
- Public WS limitation: Bithumb orderbook WS is usable as best-effort realtime depth, but without official sequence/checksum it is not a self-verifying incremental book. Consumers must rebuild from REST on reconnect, stale stream, or suspected message loss.

## Validation

Allowed commands:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bithumb/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway bithumb --lib --message-format short
```
