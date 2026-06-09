# Binance COIN-M Gateway Adapter

Status: `rustcta-exchange-gateway` COIN-M futures/perpetual REST adapter with private REST trading, native batch place/cancel runtime, request-spec/signing fixtures, and WebSocket parser fixtures.

## Product Boundary

- Adapter id: `binancecoinm`.
- Products: Binance COIN-M inverse perpetual and dated futures.
- Market types: `MarketType::Perpetual` for `PERPETUAL`; `MarketType::Futures` for quarterly or delivery contracts.
- REST base URL: `https://dapi.binance.com`.
- Testnet REST base URL: `https://testnet.binancefuture.com`.
- Public/private WebSocket runtime is not opened by this adapter yet. The endpoint mapping records the official COIN-M public market-stream spec for order-book wiring; runtime users must reconcile through REST after reconnect until a WS client is connected.

Official references:

- COIN-M general info: https://developers.binance.com/docs/derivatives/coin-margined-futures/general-info
- Exchange info: https://developers.binance.com/docs/derivatives/coin-margined-futures/market-data/rest-api/Exchange-Information
- COIN-M WebSocket market streams: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Connect
- COIN-M partial book depth streams: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Partial-Book-Depth-Streams
- COIN-M diff book depth streams: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams
- COIN-M local order book rebuild: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly
- New order: https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/rest-api/New-Order

## Contract Semantics

COIN-M contracts are inverse and coin-margined. `BTCUSD_PERP` is represented as canonical `BTC/USD`, while margin/settle asset is `BTC`. Quantities sent to `/dapi/v1/order` are contract counts, not base-coin size. The endpoint mapping and fixtures include `contractSize` so execution layers do not mistake contract quantity for spot size.

## Implemented Gateway Surface

- Public REST: symbol rules from `/dapi/v1/exchangeInfo`, order book snapshot from `/dapi/v1/depth`.
- Public WebSocket spec: official COIN-M market streams are rooted at `wss://dstream.binance.com`; raw streams use `/ws/<streamName>` and combined streams use `/stream?streams=...`.
- Public order book WS spec: partial depth uses `{symbol}@depth5`, `{symbol}@depth10`, `{symbol}@depth20` plus `@100ms` or `@500ms`; diff depth uses `{symbol}@depth`, `{symbol}@depth@100ms`, `{symbol}@depth@250ms`, `{symbol}@depth@500ms`. Stream payloads include `U`, `u`, and `pu`.
- Public order book rebuild: buffer deltas, fetch REST `/dapi/v1/depth`, align against `lastUpdateId`, then require each event's `pu` to equal the previous event's `u`; on a gap or reconnect, repeat REST snapshot plus delta replay.
- Funding-history and open-interest REST endpoints are mapped with fixture coverage; the shared gateway API does not expose first-class response types for those reads yet.
- Private read REST: balances from `/dapi/v1/balance`, positions from `/dapi/v1/positionRisk`, open orders, query order, recent fills, fee-rate request shape.
- Private write REST: place order, cancel order, cancel all, amend order, native batch place and native batch cancel.
- Native batch place/cancel: `POST /dapi/v1/batchOrders` supports max 5 and `DELETE /dapi/v1/batchOrders` supports max 10 same-symbol cancels. Runtime returns per-item reports for partial exchange failures and marks missing response items for REST readback reconciliation.
- Signing: Binance HMAC-SHA256 hex over sorted query string, with `timestamp`, `recvWindow`, and `signature`, authenticated by `X-MBX-APIKEY`.

## Unsupported Boundary

- 交易所不支持现货：该 adapter/profile 只对应 Binance COIN-M derivatives API。
- Spot and USD-M futures are not served by this adapter.
- Quote-sized market orders and Spot OCO/order-list helpers are `Unsupported`.
- Native batch place/cancel are runtime-wired for COIN-M and covered by mock REST request-spec/signing tests; live success is not assumed without external dry-run controls.
- COIN-M amend is implemented through `PUT /dapi/v1/order`; Spot OCO/order-list helpers remain unsupported semantics.
- Dead-man countdown, leverage, margin type, and position mode are documented in endpoint mapping as future G4/G6 work unless wired through a dedicated adapter-specific method.
- Public/private WebSocket runtime is not connected; public COIN-M order-book WS is documented as `spec_only`, and REST reconciliation is the fallback.

## Validation

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/binancecoinm/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway binancecoinm --lib --message-format short
cargo test -p rustcta-gateway binancecoinm --message-format short
```
