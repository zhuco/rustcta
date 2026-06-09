# Cryptomus Adapter

Task 18 scope: `cryptomus` is treated as a Spot exchange adapter only. Cryptomus
also exposes merchant payment, payout and wallet APIs; those rails are not
registered in the exchange gateway.

## Boundary

| Area | Status |
| --- | --- |
| Public REST | Market asset metadata and order book snapshot |
| Private REST | Credential-gated account balance, tariffs fee read, active/history order reads, limit place and cancel runtime |
| Public WS | Payload helpers for depth, trades, ticker and heartbeat |
| Private WS | Spec-only subscription payloads; live activation requires one-time token handling |
| Payment/payout | Explicitly unsupported in this adapter |
| Withdraw/transfer | Explicitly unsupported |
| Standard contracts | `交易所不支持合约`; current official Exchange docs cover spot orderbook/orders/WS, not futures/perpetual/options |

## Endpoint Notes

- REST base URL: `https://api.cryptomus.com`.
- Public WS URL: `wss://api-ws.cryptomus.com/ws`.
- Trading pairs are sourced from the Exchange "List of available trading pairs"
  endpoint, not from merchant payment assets.
- Private REST signing uses `sign = md5(base64(json_body) + api_key)` and sends
  `userId` plus `sign` headers.
- Private REST and private streams are disabled by default even when credentials
  are present. This keeps the adapter scan-only unless a caller opts in through
  `CryptomusGatewayConfig`.
- Limit place/cancel runtime is disabled by default and requires
  `CRYPTOMUS_PRIVATE_REST_ENABLED` plus userId/API key credentials. Live
  market-order writes are gated as unsupported.
- Advanced order runtime is explicitly unsupported for shared amend,
  OCO/OTO/order-list, native batch place, and native batch cancel.
- `cancel_all_orders` is also explicitly unsupported; the reviewed Exchange order
  API does not expose a safe shared cancel-all operation.

官方产品线核验见 [产品线官方核验 P6 剩余区域现货 CEX](../产品线官方核验_P6_剩余区域现货_CEX.md)。Cryptomus payment、payout、conversions 不并入交易所合约产品线。

Official documentation used for this task:

- https://doc.cryptomus.com/methods/exchange/list-of-available-trading-pairs
- https://doc.cryptomus.com/methods/market-cap/orderbook
- https://doc.cryptomus.com/methods/exchange/list-of-active-orders
- https://doc.cryptomus.com/methods/exchange/wallet-balances
- https://doc.cryptomus.com/methods/exchange/websockets

## Official WebSocket Order Book Detail

官方核验见 [WebSocket 官方核验 P7 补充交易所盘口细项二](../WebSocket官方核验_P7_补充交易所盘口细项二.md)。Cryptomus public WS depth 订阅方法是 `depth_subscribe`，参数形如 `BTC_USDT:0`，其中冒号后是 market scale index；也支持 `all`。

`depth_update` payload 有 `full_reload`：false 表示 partial update，true 表示全量订单簿刷新。官方未给固定推流毫秒、固定档位、sequence 或 checksum；mapping 记录为 no fixed ms、depth unspecified。实现时应把 `scale_index` 和 `full_reload` 写进 mapping，并用 REST orderbook 作为断线/异常重建源。

## Fixtures

Fixtures live under `tests/fixtures/exchanges/cryptomus/`:

- `markets.json` and `orderbook.json` cover public parser behavior.
- `balances.json`, `orders_active.json`, `orders_history.json` and
  `tariffs.json` cover private parser behavior; mock REST tests cover guarded
  place/cancel/query/open/fills request routing.
- `request_specs/` and `signing_vectors/` cover signed request shape.
- `unsupported_boundary.json` records payment/payout exclusion.
- `unsupported_boundary.json` also records P4 advanced-order unsupported
  operations, capability flags, and runtime `Unsupported.operation` names.
- `ws_public_depth.json` and `ws_private_order.json` document stream sample
  payloads.

## Validation

Run these checks for this adapter without compiling deployment binaries:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/cryptomus/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway cryptomus --lib --message-format short
```

If `apps/gateway/src/config.rs` changes, also run:

```bash
cargo test -p rustcta-gateway cryptomus --message-format short
```
