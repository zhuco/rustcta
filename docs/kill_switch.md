# Kill Switch

The kill switch is central read/write safety state for operator and preflight blockers. Default behavior blocks live orders.

State fields:

- `enabled`
- `active`
- `reason`
- `triggered_by`
- `triggered_at`
- `allow_paper_trading`
- `allow_live_dry_run`
- `allow_live_orders`

Default config:

```yaml
kill_switch:
  enabled: true
  initial_state: safe
  allow_paper_trading: true
  allow_live_dry_run: true
  allow_live_orders: false
```

Trigger sources supported by the model:

- Config initial state
- Manual operator/API trigger
- Preflight critical failure
- Too many order failures
- Too many stale books
- Future daily loss breach

Dashboard:

- `GET /api/kill_switch`
- `POST /api/kill_switch/trigger`
- `POST /api/kill_switch/reset`

POST endpoints require monitoring token auth. If token auth is disabled, POST endpoints return `404` and are unavailable. No kill-switch endpoint can place orders.
