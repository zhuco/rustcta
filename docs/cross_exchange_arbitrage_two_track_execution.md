# Cross-Arb Two-Track Execution Plan

The project should move in two coordinated tracks:

- Track A: simulation, replay, dashboard, analytics, risk reports.
- Track B: exchange private interfaces, user streams, preflight, live-small readiness.

Both tracks must use the same contracts:

- `rustcta::market::*`
- `rustcta::execution::*`
- `rustcta::strategies::cross_exchange_arbitrage::*`

Strategy code must not call exchange order APIs directly. It emits signals. The
execution layer owns order commands, private adapters, reconciliation, recovery,
and runtime mode gates.

## Track A: Simulation And Web Analytics First

Goal: prove the strategy before any real order.

Required work:

- Persist opportunity, signal, bundle, order, reconcile, funding settlement, and
  alert events.
- Add replay inputs from captured public market data.
- Add simulation reports:
  - gross spread PnL;
  - maker/taker fee PnL;
  - funding PnL;
  - slippage;
  - missed fill;
  - orphan loss;
  - route downtime;
  - symbol/exchange ranking.
- Extend `cross_arb_server` from a skeleton into read-only analytics:
  - latest opportunities;
  - rejected opportunities and reject reasons;
  - exchange route health;
  - funding windows;
  - simulated bundles;
  - PnL breakdown;
  - reconciliation reports;
  - network probe status.
- Add JSONL reader for local analysis before ClickHouse is required.
- Only after simulation produces stable reports should ClickHouse/dashboard
  ingestion be optimized.

Local gate:

```bash
scripts/cross_arb_local_gate.sh quick
cargo run --bin cross_arb_observe -- --config config/cross_exchange_arbitrage_usdt.yml
```

## Track B: Exchange Interfaces In Parallel

Goal: make each exchange independently safe before enabling live routes.

Required work per exchange:

- Public REST and WS:
  - instruments;
  - order book;
  - trades;
  - funding;
  - mark price;
  - sequence/recovery behavior.
- Private REST:
  - balance;
  - positions;
  - open orders;
  - order query;
  - create order;
  - cancel order;
  - close position;
  - leverage;
  - position mode.
- Private WS/user stream:
  - accepted order;
  - partial fill;
  - full fill;
  - cancel;
  - reject;
  - position update;
  - funding settlement.
- Parameter matrix:
  - one-way mode;
  - hedge mode;
  - long close;
  - short close;
  - reduce-only behavior;
  - symbol precision;
  - min notional;
  - post-only/IOC/FOK behavior.

Rules:

- Binance and OKX can proceed first.
- Bitget and Gate private trading stay disabled until verified core private
  implementations are added.
- Every new private adapter must pass `cross_arb_preflight --private` before
  any live-small test.
- No exchange adapter may bypass `RuntimeMode` or `dry_run`.

Local/network gate:

```bash
scripts/cross_arb_local_gate.sh network
scripts/cross_arb_local_gate.sh private
```

## How To Work On Both Tracks Without Conflict

Use strict ownership:

- Track A owns:
  - `src/strategies/cross_exchange_arbitrage/*`
  - `src/bin/cross_arb_server.rs`
  - analytics docs and reports
  - JSONL/ClickHouse read models
- Track B owns:
  - `src/exchanges/*`
  - `src/exchanges/adapters/*`
  - `src/execution/*`
  - `src/bin/cross_arb_preflight.rs`
  - exchange connectivity scripts

Shared files require coordination:

- `src/market/*`
- `config/cross_exchange_arbitrage_usdt.yml`
- `sql/cross_exchange_arbitrage.sql`

Before editing shared files, update the contract first, then have both tracks
consume it. Do not let strategy code add exchange-specific parameters.

## Recommended Order

1. Fix network/VPN routing until public probes pass for enabled exchanges.
2. Run observer and persist public market/opportunity data locally.
3. Build dashboard analytics from persisted simulation data.
4. Complete Binance private preflight and user stream.
5. Complete OKX private preflight and user stream.
6. Run 7-day shadow mode with no live orders.
7. Run Binance/OKX minimum-size live-small tests one route at a time.
8. Add Bitget/Gate private adapters only after Binance/OKX workflow is stable.

## Server Rule

The server should not be the development machine.

Local machine:

- full tests;
- simulation runs;
- analytics iteration;
- code generation;
- heavy builds.

Server:

- `server-smoke`;
- public/private preflight;
- release build for required binaries;
- long-running shadow/live-small process.

