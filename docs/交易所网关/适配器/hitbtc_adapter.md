# HitBTC Adapter

Scope: A-23 `hitbtc`, canonical HitBTC v3 family adapter.

Runtime status:

- Implemented: spot public REST symbol rules and order book snapshots.
- Offline only: HS256 private REST request specs, signing vectors, WS subscribe/unsubscribe/auth payloads, and parser fixtures.
- Unsupported at runtime: private trading, private readbacks, WebSocket supervisors, margin, futures, wallet transfer and withdrawal.

Official API sources:

| Area | Source |
| --- | --- |
| REST base URL | `https://api.hitbtc.com/api/3` |
| Public WS | `wss://api.hitbtc.com/api/3/ws/public` |
| Trading WS | `wss://api.hitbtc.com/api/3/ws/trading` |
| Wallet WS | `wss://api.hitbtc.com/api/3/ws/wallet` |
| Documentation | <https://api.hitbtc.com/> |

Product line:

- `MarketType::Spot` only in runtime.
- HitBTC v3 documents spot, margin, futures and wallet endpoints. Futures/Margin derivatives are 项目未实现 in this spot runtime, not `交易所不支持合约`. Wallet transfers and withdrawals stay outside trading runtime scope.

Authentication:

- REST HS256 signs `<method> + <URL path> + [?query] + [body] + <timestamp> + [window]` with HMAC-SHA256.
- The `Authorization` header is `HS256 ` plus base64 of `api_key:signature:timestamp[:window]`.
- WS HS256 login signs `timestamp + window`.
- Fixtures use only `test-key` and `test-secret`.

Endpoint mapping:

- `crates/rustcta-exchange-gateway/src/adapters/hitbtc/endpoint_mapping.yaml`
- Public REST endpoints are native:
  - `GET /public/symbol`
  - `GET /public/orderbook/{symbol}`
- Private endpoints are mapped for request-spec and reconciliation boundaries but remain runtime `Unsupported`.
- Fixture coverage includes public REST samples, private write/read request specs, HS256 signing vectors, a private parser sample, WS parser sample, and `unsupported_boundary.json`.

Official Core Trading Detail:

官方核心交易核验见 [核心交易官方核验 P1 第二批](../核心交易官方核验_P1_第二批.md)。HitBTC v3 Spot 支持 `POST /api/3/spot/order`、`DELETE /api/3/spot/order`、查单、open/history/trade；支持 limit、market、stop/take-profit、GTC/IOC/FOK 和 `client_order_id`。

当前项目仍把 private trading runtime 关在 `Unsupported` 边界内，所以这是 `项目未实现下单/撤单`，不能写成 `交易所不支持下单/撤单`。后续要补 Spot order/cancel/query/open/fills request specs、HMAC auth、parser 和 private trading runtime。

WebSocket boundary:

- Public order book subscribe/unsubscribe payloads are covered by tests.
- Server ping heartbeat policy is documented as 30 seconds.
- REST `get_order_book` is the order book resync fallback.
- Private WS login payload is covered, but no production private stream is enabled.

Official WebSocket order book detail:

官方核验见 [WebSocket 官方核验 P8 补充交易所盘口细项三](../WebSocket官方核验_P8_补充交易所盘口细项三.md)。HitBTC v3 public WS 支持 `orderbook/full`、`orderbook/D5|D10|D20/{100ms|500ms|1000ms}` 和 `orderbook/top/{100ms|500ms|1000ms}`。

Partial orderbook 有 sequence `s`，top 是 L1/BBO；未见 checksum。当前 adapter 只保留 spec/parser 边界，实盘 book-cache 要补 100ms channel、sequence 校验和 REST `/public/orderbook/{symbol}` 重建。

Validation:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/hitbtc/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway hitbtc --lib --message-format short
```

If app config wiring is touched:

```bash
cargo test -p rustcta-gateway hitbtc --message-format short
```
