# Mercado Bitcoin Adapter

Task 16 adds a conservative Mercado Bitcoin gateway adapter for Brazil and Latin America spot markets.

## Scope

- Products: spot only.
- Native symbol format: `BASE-QUOTE`, for example `BTC-BRL`.
- Canonical symbols preserve fiat quotes such as `BTC/BRL`; slash, underscore, dash, and compact forms normalize to `BASE-QUOTE`.
- Public REST and public WebSocket are represented by request/payload helpers and fixtures.
- Private REST order, balance, open order, and fill surfaces are offline request-spec only until auth and sandbox/live behavior are verified.

## Boundaries

- Official P6 product-line verification found Mercado Bitcoin API v4 covers spot
  symbols, order book, accounts, balances, orders, fees, and wallet. The
  `positions` wording maps to open orders, not derivative positions; standard
  futures/perpetual/options are `交易所不支持合约`.
- Fiat ledger is read-audit-only.
- Withdrawals, bank payments, transfers, and internal movements are unsupported.
- Private streams are not promoted; private state should reconcile through REST request specs.
- Fixtures use redacted bearer placeholders and synthetic IDs only.

## Verification

Allowed checks:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/mercado/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway mercado --lib --message-format short
```
