# Spot Control Runtime Snapshot

The control panel never trusts browser-provided trading state. API payloads describe operator intent only. Before a write command is accepted, the server builds a `SpotControlRuntimeSnapshot` from server-side runtime publishers.

Snapshot contents include symbol rules, books, book and exchange health, fees, balances, reservations, unmanaged inventory, disabled state, open orders, reconciliation state, KillSwitch, LivePreflight, SmallLiveGate, recorder health, direction readiness, and liquidation preview.

Every component carries `SnapshotComponentStatus`: `Fresh`, `Stale`, `Missing`, `Error`, or `Unsupported`. Commands and audit events store `snapshot_id` so later reviews can explain why a command was accepted or rejected.

Missing or stale required components fail safely. Live execution remains blocked even when a snapshot is clean.
