# Architecture Module Layout

Status date: 2026-06-16

This document describes the current repository architecture after the workspace
split, exchange adapter cleanup, root `src/` removal, and unified arbitrage
runtime consolidation.

## Workspace Layout

```text
apps/        thin process entrypoints: gateway, supervisor, control-api, cli
crates/      reusable platform crates and API contracts
strategies/  independent strategy crates and adapter-free migrating cores
tools/       operator, audit, migration, and diagnostic commands
web-ui/      Dioxus control panel workspace
config/      active runtime configs, exchange examples, and supervisor specs
docs/        current-state architecture, operations, and migration docs
scripts/     local automation and validation helpers
tests/       integration, regression, fixture, and live-readonly tests
```

The old root `src/` tree has been removed. New implementation belongs in
`apps/`, `crates/`, `strategies/`, `tools/`, or `web-ui/`.

## Layer Map

The old root module map is no longer a runnable layout. Current runnable
entrypoints are workspace app binaries under `apps/`; current reusable contracts
and implementations live under `crates/` and strategy packages.

The system keeps these arbitrage families as current workspace-owned paths:

- Spot-to-Spot arbitrage: `strategies/spot-spot-arbitrage/`.
- Unified arbitrage: `strategies/unified-arbitrage/`.

Unified arbitrage is the normal operator-facing path for cross-exchange
perpetual, funding, spot-perp basis, and settlement routes. Retired split
strategy trees should not be reintroduced as separate live entrypoints.

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

Production-facing exchange code now uses the gateway crate and exchange API
contracts:

```text
crates/rustcta-exchange-api/                    shared exchange contracts
crates/rustcta-exchange-gateway/src/adapters/   venue-specific adapters
apps/gateway/                                   runnable gateway process
```

New strategy and execution code should call shared exchange/execution contracts
instead of reaching into adapter internals.

## Spot Arbitrage Runtime

`strategies/spot-spot-arbitrage/` owns the current multi-exchange Spot
arbitrage runtime.

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

## Unified Arbitrage Runtime

`strategies/unified-arbitrage/` owns the normal live arbitrage runtime for
cross-exchange perpetual, funding, spot-perp basis, and settlement routes.

Responsibilities:

- Load `config/unified_arbitrage_usdt.yml` and validate active routes.
- Publish `logs/unified_arbitrage/dashboard.json` for the Web control panel.
- Consume audited local operator commands from
  `data/control_api/unified_arb_control_commands.jsonl` when configured.
- Keep live execution behind explicit execution-mode, risk, and command gates.

## Control Plane

`crates/rustcta-runtime-control/src/control/spot_control/` owns Spot symbol
lifecycle and operational safety:

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
