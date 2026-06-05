# Spot Control Snapshot Replay

Snapshot replay is offline. Given a `snapshot_id`, `SpotControlSnapshotReplay` loads the immutable snapshot from `SpotControlSnapshotStore` and re-runs:

- enable validation projection,
- direction readiness,
- liquidation preview comparison.

Replay does not perform network calls and does not touch exchange clients. It compares original recorded decisions against replayed decisions and returns `SnapshotReplayReport` with differences and the current model version.

Use replay to audit rejected commands, verify liquidation previews, detect behavior changes after code updates, and reproduce incidents from persisted runtime state.
