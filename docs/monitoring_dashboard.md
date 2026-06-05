# Monitoring Dashboard Backend

RustCTA exposes an optional read-only monitoring API for operator visibility. It is designed for paper strategy monitoring and market-data health checks. It does not enable live trading, expose order controls, or call private exchange streams.

## Enabling

The dashboard is disabled by default. Enable it in the strategy config:

```yaml
monitoring:
  enabled: true
  bind_addr: "127.0.0.1:8080"
  expose_publicly: false
  require_token: true
  token_env: "RUSTCTA_MONITOR_TOKEN"
  max_recent_opportunities: 500
  max_recent_trades: 500
  max_recent_risk_events: 500
  refresh_interval_ms: 1000
```

Set the bearer token before starting the strategy:

```bash
export RUSTCTA_MONITOR_TOKEN='replace-with-a-long-random-token'
```

By default the server binds only to `127.0.0.1`. Non-loopback bind addresses are rejected unless `expose_publicly: true` is set. If exposed beyond localhost, put it behind a reverse proxy with authentication and TLS. The API is operator-only and must not be exposed directly to the public internet.

## Security

- Bearer token auth is required by default.
- Tokens are read from the environment and are not logged.
- API keys, API secrets, signatures, cookies, and raw private exchange payloads are not included in responses.
- `/api/config/summary` returns only sanitized operational settings and marks `secrets_redacted: true`.
- There are no endpoints that place orders, cancel orders, or change live trading mode.

## Endpoints

All endpoints return JSON.

- `GET /api/status` - service version, uptime, trading mode, dry-run/live flags, strategy status, loop timestamp, error and warning counts.
- `GET /api/exchanges` - exchange and public WebSocket health, reconnects, parse errors, sequence gaps, heartbeat timeouts, and latency.
- `GET /api/books` - current top-of-book read model. Optional query params: `exchange`, `market_type`, `symbol`, `stale_only`.
- `GET /api/opportunities/recent` - bounded recent opportunity records with fees, net spread, PnL estimate, acceptance, rejection reason, and book ages.
- `GET /api/trades/recent` - bounded recent paper trade records.
- `GET /api/inventory` - paper inventory, effective available balances, and unmanaged quantities.
- `GET /api/fees` - fee model summary including source and platform-token discount settings.
- `GET /api/disabled` - global symbol, exchange, and exchange-symbol disables with active/expired status.
- `GET /api/unmanaged_positions` - unmanaged positions excluded from automated inventory.
- `GET /api/risk/events` - recent structured risk/rejection events.
- `GET /api/recorder` - book/opportunity/trade recorder settings and dropped book-event counts.
- `GET /api/config/summary` - redacted strategy configuration summary.

## Curl Examples

```bash
curl -H "Authorization: Bearer $RUSTCTA_MONITOR_TOKEN" \
  http://127.0.0.1:8080/api/status
```

```bash
curl -H "Authorization: Bearer $RUSTCTA_MONITOR_TOKEN" \
  "http://127.0.0.1:8080/api/books?stale_only=true"
```

```bash
curl -H "Authorization: Bearer $RUSTCTA_MONITOR_TOKEN" \
  http://127.0.0.1:8080/api/opportunities/recent
```

```bash
curl -H "Authorization: Bearer $RUSTCTA_MONITOR_TOKEN" \
  http://127.0.0.1:8080/api/disabled
```

## Strategy Publishing

`spot_spot_taker_arbitrage` publishes to the dashboard only when `monitoring.enabled` is true. Publishing uses bounded in-memory vectors and nonblocking `try_write` updates for hot-path events. If the dashboard read lock is busy, the strategy may skip a dashboard update rather than block opportunity detection.

The strategy publishes:

- Runtime status and last loop timestamp.
- WebSocket book health and BookCache top-of-book snapshots.
- Recent opportunities and paper trades.
- Risk/rejection events.
- Paper inventory snapshots.
- Fee model, disabled registry, unmanaged positions, and recorder health.

## HAR Reference Safety

A HAR from another monitoring page can be useful for understanding operator workflows, but it must be sanitized before inspection. Do not commit HAR files. Redact cookies, authorization headers, query tokens, request bodies, account identifiers, and response payloads containing private data. Use only high-level hints such as endpoint naming, polling cadence, and table-like response keys.

This stage does not require a HAR utility. The dashboard API is implemented from RustCTA read models, not from private FMZ assets or captured credentials.

## Not Implemented Yet

- No frontend UI.
- No database-backed dashboard history.
- No live trading controls.
- No maker/taker execution controls.
- No private user stream monitoring.
- Opportunity/trade recorder drop counts are currently reported as zero; book recorder drops are tracked.

## Gate.io and Bitget Spot

For Gate.io and Bitget Spot, `/api/exchanges` reports public WebSocket connection state, last message time, stale/fresh symbol counts, heartbeat timeouts, sequence gap count, and latency stats. `/api/books` reports normalized compact symbols such as `BTCUSDT` regardless of native exchange symbol format.
