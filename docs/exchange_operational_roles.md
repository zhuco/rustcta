# Exchange Operational Roles

Roles separate market observation from execution eligibility:

- `ScanOnly`: public books, symbol rules, fees, and analytics.
- `ReadOnlyValidated`: scan plus authenticated read-only balances, orders, and fills.
- `LiveDryRunEligible`: may participate in dry-run order planning.
- `FutureLiveExecutionCandidate`: may be considered for future live execution after validation.
- `LiveExecutionEnabled`: forbidden in this task.
- `Disabled`: excluded.

Configured roles:

- Gate.io: `future_live_execution_candidate`
- Bitget: `future_live_execution_candidate`
- MEXC: `scan_only`
- CoinEx: `scan_only`
- KuCoin: `scan_only`

A scan-only venue must never be used to build an executable live order plan.
