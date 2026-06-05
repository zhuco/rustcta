# Spot Control Snapshot Store

`SpotControlSnapshotStore` persists immutable full `SpotControlRuntimeSnapshot` records. The initial backend is JSONL at `data/spot_control_snapshots.jsonl`, matching the existing command and audit store style.

Each record includes schema version, `snapshot_id`, `generated_at`, component timestamps, warnings, critical errors, source metadata, consistency report, open-order ownership, fill ownership, and the normalized runtime fields required to reproduce decisions.

Store rules:

- A duplicate `snapshot_id` is rejected.
- Secret-like keys such as `api_key`, `api_secret`, `secret`, `passphrase`, and `authorization` are rejected.
- Persistence failures are reported through publisher health.
- If `require_persistence_for_write_commands` is true, write commands cannot rely on an unpersisted runtime snapshot.

The store contains normalized state only. It must not contain authenticated headers, raw credentials, or request signing material.
