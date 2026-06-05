# Control Plane Security

Control-plane writes are authenticated, idempotent, versioned, validated, audited, and visible through read APIs.

Security rules:

- Bearer token authentication is required for writes.
- Write APIs are unavailable when token auth is disabled.
- `Idempotency-Key` is required for every write.
- Payloads carry `expected_version` for optimistic lifecycle concurrency protection.
- Accepted and rejected commands are persisted to `control_commands.jsonl`.
- Audit events are persisted to `control_audit.jsonl`.
- Secrets are not returned in API responses.
- DisabledRegistry always overrides lifecycle state.
- KillSwitch, LivePreflight, and SmallLiveGate are not bypassed.
- No automatic live enablement, wildcard exchange enablement, unmanaged liquidation, unsafe market order, or PostOnly downgrade is permitted.

Real live liquidation submission is intentionally blocked until a separate small-live execution path is implemented and tested.
