# REST Order Reconciliation

MEXC Spot and CoinEx Spot private user streams are not supported in this repository. Any future small live taker/taker test must confirm orders through REST.

Reusable reconciliation primitives live in:

- `crates/rustcta-exchange-gateway/src/reconciliation.rs`
- `crates/rustcta-execution-api/src/lib.rs`
- `crates/rustcta-execution-router/src/lib.rs`

They do not place orders.

Polling behavior:

- Query `get_order` by exchange order id or client order id when the adapter supports it.
- Use `recent_fills` fallback when enabled.
- Use `open_orders` fallback when enabled.
- Return structured outcomes: `Filled`, `PartiallyFilled`, `Cancelled`, `Rejected`, `Expired`, `Unknown`, `OrderNotFound`, `Timeout`, `RateLimited`, or `InconsistentStatus`.
- Never panic on not found, timeout, unsupported fallback, or rate limit.

Default config:

```yaml
order_reconciliation:
  enabled: true
  poll_interval_ms: 500
  max_poll_attempts: 20
  order_timeout_ms: 10000
  allow_recent_fills_fallback: true
  allow_open_orders_fallback: true
  unknown_status_is_critical: true
```

Dashboard:

- `GET /api/order_reconciliation/status`
- `/api/status` includes `order_reconciliation_ready`

Future live execution should call this immediately after a real order submission is explicitly implemented and enabled by separate live gates.
