# Architecture Module Layout

This document describes the current repository architecture after the exchange
adapter cleanup and legacy strategy removal.

## Layer Map

```text
src/
  core/        shared config, errors, common types, retry/request utilities
  data/        book events, book cache, book health, recording, websocket books
  market/      symbols, order books, instruments, precision, routing, funding
  exchanges/   exchange clients, gateways, registry, protocol adapters
  scanner/     read-only spot coverage and cross-exchange scan analytics
  execution/   order commands, router, fee model, reconciliation, dry-run gates
  control/     spot control plane, lifecycle, audit, snapshots, liquidation
  risk/        kill switch, disabled-symbol registry, hedge policy
  strategies/  trading intent and strategy runtimes
  web/         sanitized monitoring/control read models and HTTP routes
  bin/         executable composition and operational CLIs
```

The system supports two arbitrage families:

- Spot-to-Spot arbitrage: `src/strategies/spot_spot_taker_arbitrage/`.
- USDT perpetual cross-exchange arbitrage: `src/strategies/cross_exchange_arbitrage/`.

These are separate paths. Spot arbitrage should not import perpetual-only
position semantics, and perpetual arbitrage should not rely on Spot inventory
liquidation controls.

## Dependency Direction

Dependencies should remain one-way:

```text
core
market
data      -> core, market
exchanges -> core, market, execution contracts
scanner   -> market/data read models, exchange metadata, fee model
execution -> exchanges traits, risk, reconciliation
control   -> execution gates, risk, snapshots, audit
strategies -> scanner/data/execution/risk/control abstractions
web       -> read models only
bin       -> composition only
```

Rules:

- `scanner` is read-only and must never place or cancel orders.
- `strategies` should produce intent and delegate mutation to `execution`.
- `exchanges` should not depend on `strategies`, `control`, or `web`.
- `web` should not be an authority for balances, books, fees, or orders.
- Real exchange mutation must stay behind dry-run, live-dry-run, preflight, and
  kill-switch gates.

## Exchange Modules

The old flat `src/exchanges/adapters/` compatibility path has been removed.
Production-facing exchange code now uses:

```text
src/exchanges/<exchange>/      venue-specific Spot or core client
src/exchanges/market_adapters/ public market-data adapters
src/exchanges/private_perp/    shared private perpetual protocol support
src/exchanges/trading_adapters/ legacy-to-execution trading bridge
src/exchanges/registry.rs      gateway and adapter registration
src/exchanges/unified.rs       unified Spot/Perpetual client contract
```

`GatewayExchange` remains only as a compatibility bridge for code that still
expects `core::exchange::Exchange`. New code should prefer `ExchangeClient`,
`MarketDataAdapter`, or `TradingAdapter` directly.

## Spot Arbitrage Runtime

`spot_spot_taker_arbitrage` owns the current multi-exchange Spot arbitrage
runtime.

Responsibilities:

- Load exchange list, symbol mappings, fees, disabled symbols, and inventory.
- Read books through REST polling, replay, or shared WebSocket cache.
- Compute directed taker/taker opportunities after fees and depth checks.
- Reject stale, disabled, insufficient-depth, or control-plane-blocked symbols.
- Execute paper or live-dry-run plans through the shared execution layer.
- Record opportunities, reports, replay files, and dashboard snapshots.

Non-responsibilities:

- It does not own perpetual positions.
- It does not bypass the control plane for manual enable/disable operations.
- It does not submit live orders unless live gates are explicitly enabled.

## Control Plane

`src/control/spot_control/` owns Spot symbol lifecycle and operational safety:

- runtime snapshots
- symbol enable/disable
- unmanaged inventory handling
- liquidation planning
- operation locks
- audit records
- publisher health

Strategies must check these controls before opening new arbitrage.

## Documentation Policy

Keep architecture and functional docs as current-state references, not
development plans. Historical migration/remediation documents should be removed
once the migration is complete or superseded by current-state docs.
