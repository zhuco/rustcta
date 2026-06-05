# Spot Control Startup Reconciliation

At startup the publisher can load latest persisted snapshots as historical state. Historical snapshots are marked stale and cannot unblock write actions.

Startup behavior:

- Load latest persisted snapshots.
- Restore symbols as historical/stale runtime state.
- Poll exchanges for fresh balances and open orders.
- Rebuild current snapshots.
- Detect unknown orders, balance mismatches, ownership changes, and reservation mismatches.
- Keep write actions blocked until required runtime state is fresh.
- Never automatically resume liquidation after restart.

`RuntimePublisherStartupReport` records loaded snapshot count, restored symbols, fresh/stale exchanges, unknown orders, balance mismatches, ownership changes, write-unblock state, and blockers.
