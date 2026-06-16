# Supervisor Process Specs

Status date: 2026-06-15

This document records the current supervisor process specs. Older split
arbitrage launch paths have been retired from operator-facing specs.

## Current Specs

| Strategy id | Runtime | Strategy kind | Default config |
| --- | --- | --- | --- |
| `unified_arb_live` | `rustcta-strategy-unified-arbitrage` / `unified-arbitrage-runtime` | `unified_arbitrage` | `config/unified_arbitrage_usdt.yml` |
| `spot_spot_live_dry_run` | `rustcta-strategy-spot-spot-arbitrage` / `spot-spot-arbitrage-runtime` | `spot_spot_arbitrage` | `config/spot_spot_arbitrage_live_dry_run_2ex_5symbols.yml` |
| `trend_report` | `rustcta-strategy-trend` | `trend_report` | `config/trend_report.yml` |
| `account_position_reporter` | `rustcta-tools-ops` | `account_position_report` | `config/account_position_reporter.yml` |

Checked-in examples live under `config/supervisor/`:

```text
config/supervisor/unified_arb_live.spec.json
config/supervisor/spot_spot_live_dry_run.spec.json
config/supervisor/trend_report.spec.json
config/supervisor/account_position_reporter.spec.json
```

Validate the unified arbitrage spec without starting the process:

```bash
cargo run -q -p rustcta-supervisor-app --bin rustcta-supervisor -- \
  --validate-spec config/supervisor/unified_arb_live.spec.json
```

The industrial CLI exposes the same offline checks:

```bash
cargo run -q -p rustcta-industrial-cli --bin rustcta-industrial -- \
  supervisor validate-spec --path config/supervisor/unified_arb_live.spec.json
cargo run -q -p rustcta-industrial-cli --bin rustcta-industrial -- \
  supervisor readiness --spec-dir config/supervisor
```

Generate sharded unified arbitrage specs when one route universe needs to be
split across processes:

```bash
cargo run -q -p rustcta-industrial-cli --bin rustcta-industrial -- \
  supervisor print-unified-arb-shard-specs \
  --config config/unified_arbitrage_usdt.yml \
  --shard-count 3
```

## Runtime Snapshot

The unified arbitrage runtime writes dashboard snapshots under
`logs/unified_arbitrage/` and receives local operator commands from
`data/control_api/unified_arb_control_commands.jsonl` when that command queue is
configured. The snapshot is expected to expose per-route position value, PnL,
cost, quantity, and liquidation-distance fields.

## Boundaries

- Spec generation and validation are offline composition checks. They do not
  start processes, open exchange connections, place orders, or send webhooks.
- Long-running strategy lifecycle remains owned by the supervisor process, not
  the industrial CLI.
- Retired strategy configs and launch specs should not be reintroduced. New
  arbitrage work should extend `unified_arbitrage`.
