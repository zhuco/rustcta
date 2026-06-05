# Runtime Publisher Production Calibration

The live read-only runtime-publisher test runs `SpotControlRuntimePublisher` against real Gate.io and Bitget read-only sources while live execution remains disabled.

It validates:

- Balances and open orders are polled through `ExchangeClient` REST methods.
- Public books come from WebSocket-fed `BookCache`, not REST polling.
- Symbol rules and fees refresh conservatively.
- Snapshots are generated, persisted immutably, loaded by `snapshot_id`, and replayed offline.
- Dashboard runtime-publisher and snapshot routes return sanitized responses.
- Startup reconciliation restores historical snapshots as stale state and keeps write actions blocked.
- Mutation guard fails the test if `place_order` or `cancel_order` is called.

Start with conservative settings:

```yaml
runtime_publisher:
  polling:
    balances_interval_ms: 5000
    open_orders_interval_ms: 5000
    recent_fills_interval_ms: 10000
    symbol_rules_interval_seconds: 3600
    fee_interval_seconds: 1800
    reconciliation_interval_ms: 5000
    snapshot_interval_ms: 1000
  limits:
    maximum_concurrent_requests_per_exchange: 1
    minimum_request_spacing_ms: 2000
    jitter_ms: 100
    exponential_backoff_initial_ms: 1000
    exponential_backoff_max_ms: 30000
    pause_after_rate_limit_ms: 60000
```

Treat these as calibration defaults, not universal venue limits. Production values should be adjusted only after observing real endpoint latency, rate-limit headers where safely available, and rate-limit error codes without intentionally triggering rate limits.

The runtime test writes sanitized reports under `data/live_readonly_tests/`. Use the reported latency, request counts, consistency blockers, and backoff events to choose production polling values. Do not weaken snapshot consistency just to make a test pass.
