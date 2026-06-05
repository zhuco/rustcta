# Spot Control Polling And Rate Limits

The runtime publisher uses `RuntimePollingScheduler` to poll each exchange independently. Poll intervals are configured per component:

- balances,
- open orders,
- recent fills,
- symbol rules,
- fee probe,
- reconciliation,
- snapshot build.

Safety behavior:

- Account-wide endpoints are preferred; polling is not per symbol when an account-wide endpoint exists.
- WebSocket `BookCache` is used for books; REST orderbook polling is not used when valid WebSocket data exists.
- Per-exchange backoff prevents one unhealthy exchange from blocking another.
- Rate-limit errors trigger a longer pause.
- Repeated exchange errors use exponential backoff.
- Minimum request spacing and jitter reduce synchronized bursts.
- Failed polls preserve the last valid value as stale instead of replacing it with zero or empty state.

Normal tests use mock/local state only. Real exchange polling tests must be ignored by default.
