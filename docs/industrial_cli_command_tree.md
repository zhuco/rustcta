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
    verify-retired-src [--target <target>]
    verify-retired-src [--src-bin-dir src/bin]
  ledger
    validate --path <jsonl> [--from-sequence <n>]
    summary --path <jsonl> [--from-sequence <n>]
  supervisor
    print-legacy-spec --template <template> [spec overrides]
    print-unified-arb-shard-specs [--shard-count <n>] [spec overrides]
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
```

## Boundary Rules

- `migration verify-retired-src` reuses the
  `rustcta-tools-ops` retired binary matrix. `migration verify-retired-src`
  fails when a new retired `src/bin/*.rs` file is not classified or when the
  matrix has stale entries.
- `ledger validate` and `ledger summary` replay local JSONL ledgers only. They
  do not write ledger records and do not open exchange connections.
- `supervisor` commands validate local specs, print local spec templates, or
  read local registry JSON. They do not start processes through the industrial
  CLI command surface.
- `ops` exposes only the low-risk commands already migrated into
  `rustcta-tools-ops`: dry-run smart-money summaries, local account-position
  report rendering, and the existing symbol command help/surface. Canary,
  admin, cancel, close, and private audit commands remain out of this CLI tree.
- `supervisor print-unified-arb-shard-specs` generates root-free specs for the
  unified arbitrage runtime. It does not start a process or open exchange
  connections.

## Boundary Checks

`scripts/check_industrial_boundaries.sh` enforces the CLI migration boundary:

- The industrial CLI smoke tests must cover help for `migration`, `ledger`,
  `supervisor`, and `ops`.
- Unified arbitrage readiness must be validated through checked-in supervisor
  specs instead of retired bridge commands.
- The script runs both `rustcta-tools-ops verify-retired-src` and
  `rustcta-industrial migration verify-retired-src`, so unclassified new
  retired `src/bin/*.rs` files fail the boundary check.
- App crates outside `apps/gateway` must not depend on
  `rustcta-exchange-gateway`, and no app crate may import gateway-private
  adapter paths such as `rustcta_exchange_gateway::adapters`.
