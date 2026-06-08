# Bitso Adapter

Task 16 adds a conservative Bitso gateway adapter for Latin America spot markets.

## Scope

- Products: spot only.
- Native symbol format: `major_minor`, for example `btc_mxn` and `btc_brl`.
- Canonical symbols preserve fiat quotes such as `BTC/MXN`, `BTC/BRL`, `ETH/ARS`, and `USDC/COP`.
- Public REST and public WebSocket are represented by request/payload helpers and fixtures.
- Private REST order, balance, open order, and fill surfaces are offline request-spec only until sandbox/live validation promotes them.

## Boundaries

- Fiat ledger is read-audit-only.
- Withdrawals, bank payments, SPEI, card operations, transfers, and margin funding are unsupported.
- Private streams are not promoted; private state should reconcile through REST request specs.
- Fixtures use placeholder keys and synthetic IDs only.

## Verification

Allowed checks:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bitso/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway bitso --lib --message-format short
```
