# Bybit Gateway Adapter

Status: `rustcta-exchange-gateway` Bybit V5 Spot + linear perpetual/futures REST adapter with WebSocket request-spec fixtures.

## Product Boundary

- Adapter id: `bybit`.
- Products: Spot, USDT/USDC linear perpetual and futures through Bybit V5 categories.
- REST base URL: `https://api.bybit.com`.
- Testnet REST base URL: `https://api-testnet.bybit.com`.
- Public WS URL: `wss://stream.bybit.com/v5/public/linear` for linear contracts.
- Private WS URL: `wss://stream.bybit.com/v5/private`.

Official references:

- V5 market instruments: https://bybit-exchange.github.io/docs/v5/market/instrument
- V5 order create: https://bybit-exchange.github.io/docs/v5/order/create-order
- V5 WebSocket auth: https://bybit-exchange.github.io/docs/v5/ws/connect

## Implemented Gateway Surface

- Public REST symbol rules through `/v5/market/instruments-info`.
- Public REST order book snapshots through `/v5/market/orderbook`.
- Private REST balances, positions, place order, cancel order, cancel all, query/open orders, and recent fills.
- Funding-history and open-interest REST endpoints are mapped with fixture coverage; the shared gateway API does not expose first-class response types for those reads yet.
- V5 REST signing uses `timestamp + api_key + recv_window + query_or_json_body` with `X-BAPI-*` headers.
- WebSocket subscribe/auth/heartbeat payload helpers are present; runtime connection is still spec-only and uses REST reconciliation fallback.

## Unsupported Boundary

- Native batch create/cancel, leverage mutation, margin mode, position mode, disconnected cancel-all/dead-man, and options metadata are not wired through the standard gateway trait yet. The endpoint mapping records the Bybit V5 leverage, margin-mode, and position-mode routes as explicit unsupported boundaries.
- Private WS runtime is not long-running in this adapter; fixtures document order/execution/position/wallet channels.
- Web page or unofficial endpoints are not used.

## Validation

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bybit/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway bybit --lib --message-format short
cargo test -p rustcta-gateway bybit --message-format short
```
