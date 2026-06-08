# CoinTR Gateway Adapter

Status: `rustcta-exchange-gateway` Spot + USDT futures REST adapter with public/private WebSocket request and parser specs.

## Scope

- Adapter id: `cointr`
- REST base URL: `https://api.cointr.com`
- Public WebSocket: `wss://ws.cointr.com/v2/ws/public`
- Private WebSocket: `wss://ws.cointr.com/v2/ws/private`
- Market types: `Spot`, `Perpetual`
- Futures product type: `USDT-FUTURES`
- Official docs checked: 2026-06-08, CoinTR API docs for domain, signature, Spot order place/cancel, common WebSocket intro, Spot public WebSocket trade/ticker/candle/depth pages, and USDT futures public WebSocket trade/ticker/candle/depth pages.

## Endpoint Mapping

| Gateway capability | CoinTR endpoint | Notes |
| --- | --- | --- |
| Spot symbol rules | `GET /api/v2/spot/public/symbols` | Parses symbol/base/quote, precision, tick/step, min quantity and min notional fields where present. |
| Futures symbol rules | `GET /api/v2/mix/market/contracts?productType=USDT-FUTURES` | Parses contract metadata into `MarketType::Perpetual`. |
| Spot order book | `GET /api/v2/spot/market/orderbook` | Snapshot parser; depth is clamped to the adapter capability. |
| Futures order book | `GET /api/v2/mix/market/orderbook?productType=USDT-FUTURES` | Snapshot parser for USDT futures. |
| Balances | `GET /api/v2/spot/account/assets`, `GET /api/v2/mix/account/accounts` | Spot balances and futures account balances. |
| Positions | `GET /api/v2/mix/position/all-position` | Futures positions only; Spot returns an empty position set. |
| Order lifecycle | Spot `POST /api/v2/spot/trade/place-order`, `/cancel-order`; futures `POST /api/v2/mix/order/place-order`, `/cancel-order` | Spot request bodies use documented `side`, `orderType`, `force`, `size`, and `clientOid` fields. Futures includes `productType`, `marginCoin`, `size`, `reduceOnly`, and optional position side mapping. |
| Quote market order | Spot `POST /api/v2/spot/trade/place-order` market buy | Spot market-buy uses `size` as quote amount. Spot sell quote-market and futures quote-market remain explicit `Unsupported`. |
| Native batch place | Spot `POST /api/v2/spot/trade/batch-orders`; futures `POST /api/v2/mix/order/batch-place-order` | Homogeneous market-type batches only. |
| Native batch cancel | Spot `POST /api/v2/spot/trade/batch-cancel-order`; futures `POST /api/v2/mix/order/batch-cancel-orders` | Homogeneous market-type batches only. |
| Cancel all | Open-order sweep plus single cancel | Requires a symbol-scoped request; avoids unbounded account-wide cancellation. |
| Query/open orders/fills | Spot `/api/v2/spot/trade/orderInfo`, `/unfilled-orders`, `/fills`; futures `/api/v2/mix/order/detail`, `/orders-pending`, `/fills` | Implemented as V2 readback routes with offline request/parser coverage; live private readback should be verified with read-only keys before promotion. |
| Public WebSocket | `op=subscribe` with `books5`, `trade`, `ticker`, `candle*`; `instType=SPOT` or `USDT-FUTURES` | Subscription payload/session helpers, ping/pong handling, standard order-book event conversion, and typed trade/ticker/candle parsers are implemented. Depth pushes include timestamp/checksum but no sequence id in the documented envelope. |
| Private WebSocket | login + `user.orders`, `user.fills`, `user.account`, `user.positions` | Session startup sends a signed login payload before subscribe; order/fill/balance/position stream parsers are covered by offline tests. Persistent socket supervisor integration remains a platform follow-up. |

## Authentication

Signed REST requests use CoinTR `ACCESS-*` headers:

- `ACCESS-KEY`
- `ACCESS-SIGN`
- `ACCESS-TIMESTAMP`
- `ACCESS-PASSPHRASE`
- `Content-Type: application/json`
- `locale: en-US`

The signature payload is:

```text
timestamp + method.toUpperCase() + requestPath + optional("?"+sorted_query) + body
```

The adapter signs that payload with HMAC-SHA256 and Base64 encodes the result.

Private WebSocket login signs `timestamp + GET + /user/verify` with the same
Base64 HMAC-SHA256 helper and sends `op=login` before channel subscription.

## Explicit Boundaries

- No production WebSocket task runner is started by this adapter; it exposes request/session/parser specs for the shared runtime to consume.
- Public trade/ticker/candle streams are typed CoinTR parser outputs because the shared `ExchangeStreamEvent` enum does not yet expose public trade, ticker, or candle variants.
- The documented depth channel includes a CRC32 checksum. The adapter parses order-book snapshots/deltas into standard order-book events, but live checksum validation, REST snapshot merge, and reconnect/resync are still shared runtime work.
- Amend order, OCO/OTO order lists, leverage/margin/position-mode mutations, and funding/open-interest helpers are not part of the current shared implementation and remain explicit `Unsupported` or venue-specific follow-ups.
- Spot reduce-only orders are rejected locally.
- `cancel_all_orders` is implemented as a symbol-scoped open-order sweep rather than an unverified native account-wide endpoint.
- Use read-only private REST preflight and live-dry-run before enabling credentials for production trading.

## Official WebSocket Order Book Detail

官方核验见 [WebSocket 官方核验 P7 补充交易所盘口细项二](../WebSocket官方核验_P7_补充交易所盘口细项二.md)。CoinTR depth channel 支持 `books` full data、`books1`、`books5`、`books15`，官方推流频率为 200-300ms。订阅示例为 `{"op":"subscribe","args":[{"instType":"SPOT","channel":"books5","instId":"BTCUSDT"}]}`。

首包为 snapshot，之后 `books` 推 update，`books1/5/15` 推 snapshot。payload 有 CRC32 checksum，按本地前 25 档 bid/ask 交错字符串计算；没有独立 sequence id。实盘 book-cache 必须补 checksum 校验和 REST snapshot 重建。

## Validation

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/cointr/endpoint_mapping.yaml
rustfmt --edition 2021 --check crates/rustcta-exchange-gateway/src/adapters/cointr/*.rs
CARGO_TARGET_DIR=target/task18-gateway-tests-final cargo test -p rustcta-exchange-gateway cointr --lib --message-format short
```

Latest focused result: CoinTR adapter tests pass, including request-spec, signing vector, parser fixture, public/private WebSocket payload, heartbeat and auth login coverage. The local mock REST tests bind `127.0.0.1`; under the managed sandbox they require an unsandboxed test run.

## References

- CoinTR API domain: <https://www.cointr.com/api-doc/common/domain>
- CoinTR REST signature: <https://www.cointr.com/api-doc/common/signature>
- CoinTR Spot place order: <https://www.cointr.com/api-doc/spot/trade/place-order>
- CoinTR Spot cancel order: <https://www.cointr.com/api-doc/spot/trade/cancel-order>
- CoinTR WebSocket intro: <https://www.cointr.com/api-doc/common/websocket-intro>
- CoinTR Spot public WebSocket market channel: <https://www.cointr.com/api-doc/spot/websocket/public/tickers-channel>
- CoinTR Spot public WebSocket candlestick channel: <https://www.cointr.com/api-doc/spot/websocket/public/candlesticks-channel>
- CoinTR Spot public WebSocket trade channel: <https://www.cointr.com/api-doc/spot/websocket/public/trades-channel>
- CoinTR Spot public WebSocket depth channel: <https://www.cointr.com/api-doc/spot/websocket/public/depth-channel>

## Task 18 Toolchain Completion

Adapter-local artifacts added for the task-18 CoinTR migration:

- Endpoint mapping: `crates/rustcta-exchange-gateway/src/adapters/cointr/endpoint_mapping.yaml`
- Capability v2 equivalent declaration: `toolchain.rs`, applied from `CointrGatewayAdapter::capabilities()`
- Request-spec fixtures: `tests/fixtures/exchanges/cointr/request_specs/`
- Signing vector fixtures: `tests/fixtures/exchanges/cointr/signing_vectors/`
- Parser fixtures: `tests/fixtures/exchanges/cointr/parser/`
- WS parser fixtures: `tests/fixtures/exchanges/cointr/ws/`

Declared runtime policy:

- Public/private stream transport is native WebSocket JSON, not Socket.IO.
- Public WS supports subscribe and heartbeat parser/session helpers for `books5`, `trade`, `ticker`, and `candle*`.
- Private WS supports signed login plus `user.orders`, `user.fills`, `user.account`, and `user.positions`; reconnect requires re-login and resubscribe.
- Heartbeat policy is client ping every 15s, pong timeout 30s, stale-message threshold 30s.
- Auth renewal policy is re-login on reconnect/renewal; no listen-key lease is used.

Declared execution and safety plans:

- Rate limit plan is conservative fixed-window buckets: public REST, private REST, orders, and WS.
- Fills pagination supports `limit` up to 100; order history is current-state/id readback rather than cursor history.
- Reconciliation after unknown order state or private stream reconnect requires query order, open orders, and recent fills; replay is not allowed.
- Batch place/cancel use native CoinTR endpoints, partial atomicity, max 20 items, same market type required, client order id supported.

Validation notes:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/cointr/endpoint_mapping.yaml
python3 - <<'PY'
import json
from pathlib import Path
for path in sorted(Path("tests/fixtures/exchanges/cointr").rglob("*.json")):
    json.loads(path.read_text())
PY
rustfmt --edition 2021 --check crates/rustcta-exchange-gateway/src/adapters/cointr/*.rs
CARGO_TARGET_DIR=target/task18-gateway-tests-final cargo test -p rustcta-exchange-gateway cointr --lib --message-format short
```
