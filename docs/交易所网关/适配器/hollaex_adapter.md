# HollaEx Adapter

Scope: A-24 `hollaex`, official HollaEx demo/API profile only.

Runtime status:

- Implemented: spot public REST symbol rules from `GET /v2/constants` and order book snapshots from `GET /v2/orderbook?symbol=...`.
- Offline only: HMAC-SHA256 private REST request specs, signing vectors, WS subscribe/unsubscribe/auth payloads, and parser fixtures.
- Unsupported at runtime: private trading, private readbacks, WebSocket supervisors, HollaEx operator/admin APIs, funds movement, and arbitrary white-label exchange profiles.

Official API sources:

| Area | Source |
| --- | --- |
| REST base URL | `https://api.hollaex.com/v2` |
| WebSocket URL | `wss://api.hollaex.com/stream` |
| Documentation | <https://apidocs.hollaex.com/> |

Product line:

- `MarketType::Spot` only in runtime.
- HollaEx is a white-label exchange framework. This adapter maps only the official `api.hollaex.com` demo/API surface and does not generate profiles for arbitrary HollaEx-based venues.
- Official P6 product-line verification found no standard futures/perpetual/options
  surface for this official demo/API profile, so standard contracts are
  `交易所不支持合约` for this adapter. Arbitrary HollaEx white-label venues require
  separate verification before they can be mapped.

Authentication:

- REST private requests use `api-key`, `api-expires`, and `api-signature` headers.
- The fixture signature payload is `METHOD + path_with_query + api_expires + json_body`.
- The signature algorithm is HMAC-SHA256 hex.
- Private WS auth is documented as HMAC query params over `CONNECT + /stream + api_expires`.
- Fixtures use only `test-key` and `test-secret`.

Endpoint mapping:

- `crates/rustcta-exchange-gateway/src/adapters/hollaex/endpoint_mapping.yaml`
- Native public REST:
  - `GET /v2/constants`
  - `GET /v2/orderbook?symbol={symbol}`
- Private endpoints are mapped for request-spec and reconciliation boundaries but remain runtime `Unsupported`.

WebSocket boundary:

- Public order book subscribe/unsubscribe payloads use `orderbook:{symbol}`.
- Client ping heartbeat policy is 30 seconds.
- REST `get_order_book` is the public order book resync fallback.
- Private WS auth URL construction is covered by an offline fixture, but no production private stream is enabled.

Official WebSocket order book detail:

官方核验见 [WebSocket 官方核验 P8 补充交易所盘口细项三](../WebSocket官方核验_P8_补充交易所盘口细项三.md)。HollaEx public WS 支持 `orderbook` channel，也可按 symbol 订阅 `orderbook:xht-usdt`；实际 endpoint 是对应白标 exchange 的 `wss://<exchange>/stream`。

官方公开资料未给固定推流毫秒、固定 depth、sequence 或 checksum。HollaEx 是白标框架，任意具体 venue 的实时盘口质量需要单独核验；当前 adapter 以 REST `/v2/orderbook` 作为重建源。

Validation:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/hollaex/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway hollaex --lib --message-format short
```

If app config wiring is touched:

```bash
cargo test -p rustcta-gateway hollaex --message-format short
```
