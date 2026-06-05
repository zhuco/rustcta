# Spot Control Runtime Consistency

Runtime snapshots combine components fetched at different times. The snapshot carries `SnapshotConsistencyReport` with oldest/newest component time, maximum age, skew, warnings, critical errors, and status:

- `StrongEnoughForControl`
- `SuitableForObservation`
- `Stale`
- `Inconsistent`
- `MissingCriticalData`

Control decisions requiring current account state must reject stale, inconsistent, or missing-critical snapshots. Freeze-only flows may tolerate less freshness than liquidation preview, but liquidation preview requires fresh books, balances, ownership, and open-order state. Future small-live mode requires `StrongEnoughForControl`.

`ConfigFallback` can be useful for observation or conservative fee estimates, but it is never authoritative live account state.
