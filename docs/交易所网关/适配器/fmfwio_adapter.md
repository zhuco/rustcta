# FMFW.io Adapter

Scope: A-20 `fmfwio`, HitBTC-family FMFW.io profile.

Runtime status:

- Implemented: spot public REST symbol rules and order book snapshots.
- Offline only: HS256 private REST request specs, signing vectors, WS subscribe/unsubscribe/auth payloads, and parser fixtures.
- Unsupported at runtime: private trading, private readbacks, WebSocket supervisors, margin, futures, wallet transfer and withdrawal.

Official API sources:

| Area | Source |
| --- | --- |
| REST base URL | `https://api.fmfw.io/api/3` |
| Public WS | `wss://api.fmfw.io/api/3/ws/public` |
| Trading WS | `wss://api.fmfw.io/api/3/ws/trading` |
| Wallet WS | `wss://api.fmfw.io/api/3/ws/wallet` |
| Documentation | <https://api.fmfw.io/> |

Product line:

- `MarketType::Spot` only in runtime.
- FMFW.io documents margin and futures endpoints, but A-20 does not enable them because the task is an alias/profile audit and shared HitBTC-family core ownership belongs to parallel tasks.
- Official product-line conclusion: Margin/Futures/Perpetual are `项目未实现`, not `交易所不支持合约`.

官方核验见 [产品线官方核验 P6 剩余区域现货 CEX](../产品线官方核验_P6_剩余区域现货_CEX.md)。

Authentication:

- REST HS256 signs `<method> + <URL path> + [?query] + [body] + <timestamp> + [window]` with HMAC-SHA256.
- The `Authorization` header is `HS256 ` plus base64 of `api_key:signature:timestamp[:window]`.
- WS HS256 login signs `timestamp + window`.
- Fixtures use only `test-key` and `test-secret`.

Endpoint mapping:

- `crates/rustcta-exchange-gateway/src/adapters/fmfwio/endpoint_mapping.yaml`
- Public REST endpoints are native:
  - `GET /public/symbol`
  - `GET /public/orderbook/{symbol}`
- Private endpoints are mapped for request-spec and reconciliation boundaries but remain runtime `Unsupported`.

WebSocket boundary:

- Public order book subscribe/unsubscribe payloads are covered by tests.
- Server ping heartbeat policy is documented as 30 seconds.
- REST `get_order_book` is the order book resync fallback.
- Private WS login payload is covered, but no production private stream is enabled.

Official WebSocket order book detail:

官方核验见 [WebSocket 官方核验 P8 补充交易所盘口细项三](../WebSocket官方核验_P8_补充交易所盘口细项三.md)。FMFW.io 使用 HitBTC v3 family public WS，支持 `orderbook/full`、`orderbook/D5|D10|D20/{100ms|500ms|1000ms}` 和 `orderbook/top/{100ms|500ms|1000ms}`。

Partial orderbook 有 sequence `s`，top 是 L1/BBO；未见 checksum。当前 adapter 只保留 spec/parser 边界，实盘 book-cache 要补 100ms channel、sequence 校验和 REST `/public/orderbook/{symbol}` 重建。

Validation:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/fmfwio/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway fmfwio --lib --message-format short
```

If app config wiring is touched:

```bash
cargo test -p rustcta-gateway fmfwio --message-format short
```
