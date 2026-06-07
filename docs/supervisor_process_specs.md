# Supervisor Process Specs

Status date: 2026-06-07

This document records the current supervisor process-spec bridge for legacy
long-running runtimes and reporters. It does not move strategy code and does
not change exchange gateway behavior.

## Current Bridge

`rustcta-supervisor` owns schema-versioned `StrategyProcessSpec` records and
now includes root-free templates for the legacy processes that should become
supervisor-managed before their runtime internals move:

| Template | Legacy binary | Strategy kind | Default config |
| --- | --- | --- | --- |
| `cross_arb_live` | `cross_arb_live` | `cross_exchange_arbitrage` | `config/cross_exchange_arbitrage_usdt.yml` |
| `funding_arb_live` | `funding_arb_live` | `funding_arbitrage` | `config/funding_rate_arbitrage_usdt.yml` |
| `spot_spot_live_dry_run` | `rustcta` | `spot_spot_taker_arbitrage` | `config/spot_spot_arbitrage_live_dry_run_2ex_5symbols.yml` |
| `trend_report` | `trend_report` | `trend_report` | `config/trend_report.yml` |
| `account_position_reporter` | `account_position_reporter` | `account_position_report` | `config/account_position_reporter.yml` |

The app can print a JSON spec without starting the process:

```bash
cargo run -q -p rustcta-supervisor-app --bin rustcta-supervisor -- \
  --print-legacy-spec cross-arb-live \
  --strategy-id cross-live-main \
  --run-id local \
  --tenant-id local \
  --config config/cross_exchange_arbitrage_usdt.yml \
  --working-dir . \
  --log-dir logs/supervisor \
  --restart-backoff-ms 5000
```

Checked-in examples live under `config/supervisor/`:

```text
config/supervisor/cross_arb_live.spec.json
config/supervisor/funding_arb_live.spec.json
config/supervisor/spot_spot_live_dry_run.spec.json
config/supervisor/trend_report.spec.json
config/supervisor/account_position_reporter.spec.json
```

That output can be passed back to the existing supervisor start path:

```bash
cargo run -q -p rustcta-supervisor-app --bin rustcta-supervisor -- \
  --registry-path run/supervisor/registry.json \
  --spec config/supervisor/cross_arb_live.spec.json \
  --run-once-ms 250
```

Use `--validate-spec` for CI/runbook checks that must not start legacy
processes:

```bash
cargo run -q -p rustcta-supervisor-app --bin rustcta-supervisor -- \
  --validate-spec config/supervisor/cross_arb_live.spec.json
```

Use `--validate-registry` for read-only registry checks. Missing or empty
registry files are treated as a valid empty registry; malformed process records
are reported in `invalid_processes` and return a non-zero exit status:

```bash
cargo run -q -p rustcta-supervisor-app --bin rustcta-supervisor -- \
  --registry-path run/supervisor/registry.json \
  --validate-registry
```

## Heartbeat, Snapshot, And Recovery Views

The supervisor registry records runtime heartbeat timestamps, optional runtime
snapshot metadata, and a local recovery policy per process. The checked-in
specs keep recovery disabled by default:

```json
{
  "restart_on_exit": false,
  "restart_on_stale_heartbeat": false,
  "max_restart_attempts": 0,
  "heartbeat_timeout_ms": null,
  "snapshot_timeout_ms": null,
  "restart_backoff_ms": 5000
}
```

Read-only HTTP mode exposes these local registry views without lifecycle
mutation routes:

```bash
cargo run -q -p rustcta-supervisor-app --bin rustcta-supervisor -- \
  --serve \
  --bind 127.0.0.1:18181 \
  --registry-path run/supervisor/registry.json
```

Routes:

- `GET /api/health`
- `GET /api/snapshot`
- `GET /api/processes`
- `GET /api/processes/:id`
- `GET /api/heartbeats`
- `GET /api/runtime-snapshots`
- `GET /api/recovery`

Runtime heartbeat ingestion is intentionally separated under a local lifecycle
route and is registered only when an explicit local token is configured:

```bash
RUSTCTA_SUPERVISOR_LOCAL_LIFECYCLE_TOKEN=<local-token> \
cargo run -q -p rustcta-supervisor-app --bin rustcta-supervisor -- \
  --serve \
  --bind 127.0.0.1:18181 \
  --registry-path run/supervisor/registry.json
```

The route is `POST /api/local/lifecycle/heartbeat` and requires the
`x-rustcta-supervisor-token` header. Without that token configuration the route
does not exist. This is local lifecycle mutation groundwork only; remote
unauthenticated start, stop, or restart APIs are still out of scope.

The industrial CLI exposes the same offline helpers:

```bash
cargo run -q -p rustcta-industrial-cli --bin rustcta-industrial -- \
  supervisor validate-spec --path config/supervisor/cross_arb_live.spec.json
cargo run -q -p rustcta-industrial-cli --bin rustcta-industrial -- \
  supervisor validate-registry --path run/supervisor/registry.json
```

## Boundaries

- The generated spec uses `cargo run --bin <legacy-binary> -- --config <path>`
  so current operator flags and binary names remain compatible.
- Spec generation is a planning/composition step. It does not start a process,
  open exchange connections, place orders, or send webhooks.
- Spec and registry validation read and validate JSON shape only. They also do
  not start a process or touch exchange credentials.
- Strategy internals remain in their current legacy modules until the strategy
  migration workstream moves them behind SDK market-data/execution contracts.
- Backtest processes are intentionally not part of this bridge while backtest
  expansion is paused for the fast migration batch.

## Next Slices

- Add supervisor-managed startup smoke tests with harmless commands before
  replacing legacy operator runbooks.
- Add authenticated lifecycle mutation routes only after the control plane owns
  the corresponding safety and audit requirements.
