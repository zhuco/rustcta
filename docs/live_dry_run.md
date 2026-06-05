# Live Dry-Run

`live_dry_run` builds the exact local `OrderRequest` that a future small live taker order would send, but it never submits orders.

Modes:

- `paper`: simulates fills and mutates paper inventory.
- `live_dry_run`: reads live/public books and local/private balance snapshots when available, validates order requests, records plans, and does not call `place_order`.
- Future `live`: intentionally not implemented for `spot_spot_taker_arbitrage`.

Safety rules:

- `live_dry_run.submit_orders: true` hard-fails config validation.
- `would_submit` is always `false`.
- `live_trading_enabled: true` still fails.
- `trading_mode: live` still fails.

Default config:

```yaml
live_dry_run:
  enabled: true
  build_order_requests: true
  submit_orders: false
  max_notional_per_order: 5
  max_total_notional: 50
  require_preflight_pass: true
  require_fresh_books: true
  max_book_age_ms: 1000
  output_path: data/live_dry_run_orders.jsonl
```

Each `LiveDryRunOrderPlan` records exchange, symbol, side, IOC price, quantity, notional, generated client order id, fee estimate, required balance asset/amount, symbol-rule snapshot, validation checks, rejection reason, and the exact order request.

Gate.io and Bitget Spot are supported through the unified `ExchangeClient` path. Their dry-run plans use real symbol rules, real books, configured/private balances, `FeeModel`, `DisabledRegistry`, client order id validation, and book freshness checks.

Dashboard:

- `GET /api/live_dry_run/orders`
- `/api/status` includes `live_dry_run_last_plan_at`
