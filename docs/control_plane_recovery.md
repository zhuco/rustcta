# Control Plane Recovery

On restart, the control service reloads persisted symbols, commands, and audit history. Incomplete commands and stale locks are reported through `ControlPlaneRecoveryReport`.

Liquidation is not automatically resumed after restart. Uncertain liquidation, cancellation, or planning states are moved toward manual intervention and audited.

Recovery must verify open orders, reservations, and current runtime snapshots before any operator command can advance. Live order submission remains blocked until a separately reviewed execution path is implemented.
