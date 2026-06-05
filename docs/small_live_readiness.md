# Small Live Readiness

This repository now has a safe foundation for a future 1-5 USDT taker/taker live test. It still does not implement full live arbitrage execution and does not enable live trading by default.

## Gap Analysis

Already present:

- Unified `ExchangeClient` order/cancel/query/open-orders/recent-fills surfaces.
- MEXC Spot, CoinEx Spot, Gate.io Spot, and Bitget Spot REST adapters.
- Central client order id policy.
- Unified symbol registry and symbol-rule validation.
- Central fee model.
- Disabled registry and unmanaged positions.
- WebSocket book cache and book health.
- Live preflight and dashboard backend.
- Paper-only `spot_spot_taker_arbitrage` with JSONL/CSV recording.

Missing before real small live:

- Real exchange confirmation of final MEXC/CoinEx order status semantics.
- API-key permission audit on the real accounts, especially withdrawal disabled.
- Real balance/fill reconciliation after test orders.
- Separate explicit live execution implementation.

Implemented now:

- Live dry-run order plans that never submit.
- REST order reconciliation primitives.
- Balance reconciliation report.
- Kill switch state and dashboard endpoints.
- SmallLiveGate readiness report.
- Dashboard blocker fields and endpoints.

Still blocked:

- `trading_mode: live`
- `live_trading_enabled: true`
- `live_dry_run.submit_orders: true`
- Maker/taker execution
- Any test that places orders or requires API keys

## SmallLiveGate

`SmallLiveGate` reports readiness only. It does not start live trading.

Required checks include preflight `ReadyForSmallLive`, valid dry-run plans, kill switch allowing live orders, max notional per order <= 5 USDT, max total exposure <= 50 USDT, explicit symbols/exchanges, fresh books, clean balances, fee model, order reconciliation, recorder, dashboard, explicit confirmation flag, and:

```bash
RUSTCTA_ENABLE_SMALL_LIVE=true
```

Dashboard:

- `GET /api/small_live_gate`

## Operator Checklist

- WebSocket books fresh.
- Preflight passes.
- Fee model loaded.
- Disabled registry reviewed.
- Unmanaged positions reviewed.
- Balance reconciliation clean.
- Live dry-run orders valid.
- Kill switch ready and tested.
- Recorder enabled.
- Dashboard enabled.
- Max order notional <= 5 USDT.
- Max total exposure <= 50 USDT.
- API key has no withdrawal permission.
- `RUSTCTA_ENABLE_SMALL_LIVE=true` set only when ready.
- REST reconciliation config enabled.
- Real live execution remains disabled until separately implemented.

## Dashboard Endpoints

- `GET /api/status`
- `GET /api/live_preflight`
- `GET /api/live_dry_run/orders`
- `GET /api/order_reconciliation/status`
- `GET /api/balance_reconciliation`
- `GET /api/kill_switch`
- `GET /api/small_live_gate`
