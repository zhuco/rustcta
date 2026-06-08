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

WebSocket boundary:

- Public order book subscribe/unsubscribe payloads are covered by tests.
- Server ping heartbeat policy is documented as 30 seconds.
- REST `get_order_book` is the order book resync fallback.
- Private WS login payload is covered, but no production private stream is enabled.

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
