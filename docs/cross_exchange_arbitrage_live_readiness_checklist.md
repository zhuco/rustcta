# Cross-Exchange Arbitrage Live Readiness Checklist

This checklist summarizes the implementation state against
`cross_exchange_arbitrage_industrial_development_plan.md` and
`cross_exchange_arbitrage_usdt_perp.md` before live testing.

## Completed in Code

- Market contracts are exposed through `rustcta::market::*`.
- Order book depth is normalized to top 5 levels with empty, crossed, invalid,
  stale, and sequence-gap quality checks.
- Public market adapters exist for Binance, OKX, Bitget, and Gate spot/futures
  USDT perpetual data paths used by the cross-arbitrage observer.
- Strategy scanning computes executable VWAP, maker/taker fees, funding
  direction, route status, stale quote checks, depth checks, and net edge.
- Funding direction is explicit:
  - long leg funding = `-notional * funding_rate`
  - short leg funding = `+notional * funding_rate`
- New opens are rejected when expected funding is negative and the nearest
  settlement is inside `no_open_before_funding_mins`.
- Funding settlements can be recorded in a dedicated strategy funding ledger
  with per-bundle funding PnL.
- Execution is separated from strategy signals through command, router, engine,
  adapter, idempotency, bundle, reconciler, recovery, and state-machine modules.
- `RuntimeMode::Simulation` remains the default; only `LiveSmall` and
  `LiveScaled` allow live orders, and router-level dry-run still blocks adapter
  side effects.
- Exchange trading adapter normalizes precision before sending orders.
- Exchange trading adapter supports leverage command, position-mode command,
  close-position command, and symbol-rule loading.
- Binance and OKX private order parameter handling now distinguishes one-way
  and hedge modes:
  - one-way mode omits `positionSide` / `posSide`;
  - hedge mode sends the requested side;
  - Binance hedge-mode close orders omit `reduceOnly` to avoid exchange
    rejection.
- Binance and OKX position-mode switching methods are implemented on the core
  exchange adapters.
- Bitget and Gate private trading are explicitly disabled in the trading
  adapter registry until verified core `Exchange` private implementations are
  wired. Their public market adapters remain available for observation.
- The public observer passes `next_funding_time` from funding snapshots into
  opportunity scanning.
- WebSocket supervision primitives cover heartbeat, reconnect decision, and
  sequence-gap recovery requests.

## Passed Local Validation

- `cargo check`
- `cargo test --lib`
- `cargo test --all-features`

Known non-blocking warning:

- None recorded for the current arbitrage-focused build.

## Must Complete Before Real Orders

- Validate Binance and OKX order parameters on testnet or minimum-size live
  dry-run transition:
  - one-way open and close;
  - hedge long open and close;
  - hedge short open and close;
  - leverage change;
  - position-mode change.
- Add or wire verified core `Exchange` private implementations for Bitget and
  Gate, then enable them in the trading adapter registry before using them for
  execution. Current multi-exchange coverage is stronger for public market data
  than for private order execution.
- Wire private account/order user streams into the cross-arbitrage runtime:
  - order accepted;
  - partial fill;
  - full fill;
  - cancel;
  - reject;
  - position update;
  - funding settlement.
- Persist funding settlement events into the same durable storage path used for
  bundle/order/fill state, not only the in-memory strategy funding ledger.
- Add a live preflight command that verifies, per exchange and symbol:
  - API permission scope;
  - account position mode;
  - leverage;
  - margin mode;
  - balance;
  - tradeable instrument status;
  - tick size, step size, min quantity, and min notional;
  - configured fee assumptions versus account fee tier;
  - route status is `Healthy`.
- Connect reconciler and recovery actions to the runtime loop so position drift
  can trigger automatic close, hedge repair, and alerts.
- Add alert sinks for:
  - orphan leg;
  - position drift;
  - order reject;
  - funding source stale;
  - WebSocket repeated recovery;
  - route changed to `CloseOnly` or `Offline`;
  - live mode enabled.

## Recommended Test Order

1. Run public observer with real public REST data in `Observe` mode.
2. Run simulation and shadow mode for at least 7 continuous days.
3. Run exchange private preflight against each enabled account.
4. Run Binance and OKX minimum-notional live tests with `LiveSmall`.
5. Enable only one symbol and one exchange pair for initial production.
6. Scale to more pairs only after reconciliation, funding settlement, and alert
   records are complete for every opened bundle.
