# Industrial CLI Command Tree

Status date: 2026-06-07

`rustcta-industrial` is the operator-facing command root for the current
workspace migration. It aggregates read-only or offline command surfaces that
are already owned by industrial crates and deliberately avoids live private
account mutation paths.

## Command Tree

```text
rustcta-industrial
  doctor
  migration
    legacy-bin-plan [--target <target>]
    verify-legacy-bins [--src-bin-dir src/bin]
  ledger
    validate --path <jsonl> [--from-sequence <n>]
    summary --path <jsonl> [--from-sequence <n>]
  supervisor
    print-legacy-spec --template <template> [spec overrides]
    readiness [--spec-dir config/supervisor]
    validate-registry [--path <registry.json>]
    validate-spec --path <spec.json>
  ops
    smart-money
      binance-collector --config <yaml>
      hyperliquid-wallet-ingestion --config <yaml>
      portfolio-service --config <yaml>
    reporter
      account-position render --input <json>
    symbols
      gateio-bitget-spot [symbol flags]
  cross-arb
    preflight [legacy preflight flags]
```

## Boundary Rules

- `migration legacy-bin-plan` and `migration verify-legacy-bins` reuse the
  `rustcta-tools-ops` legacy binary matrix. `migration verify-legacy-bins`
  fails when a new `src/bin/*.rs` file is not classified or when the matrix has
  stale entries.
- `ledger validate` and `ledger summary` replay local JSONL ledgers only. They
  do not write ledger records and do not open exchange connections.
- `supervisor` commands validate local specs, print local spec templates, or
  read local registry JSON. They do not start processes through the industrial
  CLI command surface.
- `ops` exposes only the low-risk commands already migrated into
  `rustcta-tools-ops`: dry-run smart-money summaries, local account-position
  report rendering, and the existing symbol command help/surface. Canary,
  admin, cancel, close, and private audit commands remain out of this CLI tree.
- `rustcta-industrial cross-arb preflight` is an offline bridge for the legacy
  `cross_arb_preflight` invocation. It preserves the legacy argument mapping
  and emits a JSON plan with `network_access=disabled` and
  `live_order_access=disabled`. Run the legacy `cross_arb_preflight` binary
  directly when an operator intentionally wants public-network or private
  read-only checks.

## Boundary Checks

`scripts/check_industrial_boundaries.sh` enforces the CLI migration boundary:

- The industrial CLI smoke tests must cover help for `migration`, `ledger`,
  `supervisor`, `ops`, and `cross-arb preflight`.
- The offline preflight smoke must assert `network_access=disabled` and
  `live_order_access=disabled`.
- The script runs both `rustcta-tools-ops verify-legacy-bins` and
  `rustcta-industrial migration verify-legacy-bins`, so unclassified new
  `src/bin/*.rs` files fail the boundary check.
- App crates outside `apps/gateway` must not depend on
  `rustcta-exchange-gateway`, and no app crate may import gateway-private
  adapter paths such as `rustcta_exchange_gateway::adapters`.
