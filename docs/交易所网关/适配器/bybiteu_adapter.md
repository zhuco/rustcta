# Bybit EU Gateway Adapter

Status: `rustcta-exchange-gateway` Bybit EU profile adapter for Bybit V5 public REST and WebSocket specs. Private trading is deliberately disabled.

## Product Boundary

- Adapter id: `bybiteu`.
- Profile: Bybit V5 local-site profile, reusing the existing `bybit` parser, signing shape, and transport semantics.
- Products: Spot, USDT/USDC linear perpetual, and futures where the EU site exposes the same V5 market-data categories.
- REST base URL: `https://api.bybit.eu`.
- Public WS URL: `wss://stream.bybit.eu/v5/public/linear`.
- Private WS URL recorded for audit only: `wss://stream.bybit.eu/v5/private`.

Official references:

- Bybit V5 integration guidance: https://bybit-exchange.github.io/docs/v5/guide
- Bybit V5 WebSocket connect: https://bybit-exchange.github.io/docs/v5/ws/connect
- Bybit V5 market instruments: https://bybit-exchange.github.io/docs/v5/market/instrument

The V5 integration guidance lists `https://api.bybit.eu` for EEA users and states that the EU site API is limited to broker third-party application connectivity. The WebSocket guide documents Bybit V5 public/private stream shapes and regional local-site host handling. This adapter therefore treats Bybit EU as a regulatory profile, not a separate exchange implementation.

## Implemented Gateway Surface

- Named gateway registration for `bybiteu`, `bybit_eu`, and `bybit-eu`.
- Public REST symbol rules through `/v5/market/instruments-info`, using the Bybit V5 parser.
- Public REST order book snapshots through `/v5/market/orderbook`, using the Bybit V5 parser.
- Public WebSocket subscription payload specs for Bybit V5 topics on the EU stream host.
- Fixture-backed boundary audit under `tests/fixtures/exchanges/bybiteu/`.
- Disabled config example at `config/bybiteu_gateway_example.yml`.

## Unsupported Boundary

- Private REST reads, private REST writes, private WebSocket auth/subscriptions, real order placement, real cancels, batch operations, leverage, margin mode, position mode, and dead-man/cancel-all-after are `Unsupported`.
- The adapter does not read `RUSTCTA_BYBITEU_API_KEY`, `RUSTCTA_BYBITEU_API_SECRET`, or Bybit global credentials. This prevents accidental live trading on a regulatory profile whose credential scope has not been verified.
- No web page endpoints, unofficial APIs, live orders, live cancels, withdrawals, or transfers are used.

## Endpoint Mapping

`crates/rustcta-exchange-gateway/src/adapters/bybiteu/endpoint_mapping.yaml` records:

- EU REST and WebSocket base URLs.
- Bybit V5 public market-data endpoints as supported/spec-covered.
- Private account/order/fills endpoints as explicit `Unsupported`.
- REST reconciliation fallback only for public stream resync; private reconciliation remains unsupported.

## Validation

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bybiteu/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway bybiteu --lib --message-format short
cargo test -p rustcta-gateway bybiteu --message-format short
```

`cargo build` is intentionally not part of this task.
