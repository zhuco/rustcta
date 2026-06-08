# Binance COIN-M Gateway Adapter

Status: `rustcta-exchange-gateway` COIN-M futures/perpetual REST adapter with offline request-spec, signing-vector, and WebSocket parser fixtures.

## Product Boundary

- Adapter id: `binancecoinm`.
- Products: Binance COIN-M inverse perpetual and dated futures.
- Market types: `MarketType::Perpetual` for `PERPETUAL`; `MarketType::Futures` for quarterly or delivery contracts.
- REST base URL: `https://dapi.binance.com`.
- Testnet REST base URL: `https://testnet.binancefuture.com`.
- Public/private WebSocket runtime is not opened by this adapter yet. Fixture coverage documents public order book and private order event shapes; runtime users must reconcile through REST after reconnect.

Official references:

- COIN-M general info: https://developers.binance.com/docs/derivatives/coin-margined-futures/general-info
- Exchange info: https://developers.binance.com/docs/derivatives/coin-margined-futures/market-data/rest-api/Exchange-Information
- New order: https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/rest-api/New-Order

## Contract Semantics

COIN-M contracts are inverse and coin-margined. `BTCUSD_PERP` is represented as canonical `BTC/USD`, while margin/settle asset is `BTC`. Quantities sent to `/dapi/v1/order` are contract counts, not base-coin size. The endpoint mapping and fixtures include `contractSize` so execution layers do not mistake contract quantity for spot size.

## Implemented Gateway Surface

- Public REST: symbol rules from `/dapi/v1/exchangeInfo`, order book snapshot from `/dapi/v1/depth`.
- Funding-history and open-interest REST endpoints are mapped with fixture coverage; the shared gateway API does not expose first-class response types for those reads yet.
- Private read REST: balances from `/dapi/v1/balance`, positions from `/dapi/v1/positionRisk`, open orders, query order, recent fills, fee-rate request shape.
- Private write REST: place order, cancel order, cancel all, amend order request shapes.
- Signing: Binance HMAC-SHA256 hex over sorted query string, with `timestamp`, `recvWindow`, and `signature`, authenticated by `X-MBX-APIKEY`.

## Unsupported Boundary

- Spot and USD-M futures are not served by this adapter.
- Quote-sized market orders and Spot OCO/order-list helpers are `Unsupported`.
- Native batch place/cancel, dead-man countdown, leverage, margin type, and position mode are documented in endpoint mapping as future G4/G6 work unless wired through a dedicated adapter-specific method.
- Public/private WebSocket runtime is not connected; REST reconciliation is the fallback.

## Validation

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/binancecoinm/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway binancecoinm --lib --message-format short
cargo test -p rustcta-gateway binancecoinm --message-format short
```
