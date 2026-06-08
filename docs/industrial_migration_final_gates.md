# Industrial Migration Final Gates

Status date: 2026-06-08

This runbook is the Task 10 closeout checklist for the industrial workspace
migration. It makes the migration measurable without changing runtime behavior
owned by the feature migration tasks.

## CI Gates

The full migration workflow is `.github/workflows/industrial-migration-gates.yml`.
It must stay focused on regression prevention:

```bash
cargo fmt --check
cargo test --all-features
cargo clippy --all-targets --all-features
cargo run -q -p rustcta-tools-ops -- verify-retired-src
cargo run -q -p rustcta-industrial-cli --bin rustcta-industrial -- migration verify-retired-src
scripts/check_industrial_boundaries.sh
cd web-ui/dioxus && cargo build --release
```

The existing `.github/workflows/non-gateway-industrial.yml` remains a faster
focused gate for the non-gateway migration slice. The boundary script checks
that both workflows keep their required migration entries.

## Retired Root Source Runbook

Prefer workspace commands in operator docs and scripts. The root package, root
`src/` tree, and root binary compatibility surface are retired.

Current closure audit result: final root-source deletion is complete when
`src/` is absent, the root manifest is a virtual workspace manifest, no package
depends on the removed root package, and the retired-source verifiers report
`retired=true`.

The machine-readable source of truth is:

```bash
cargo run -q -p rustcta-tools-ops -- verify-retired-src
cargo run -q -p rustcta-industrial-cli --bin rustcta-industrial -- migration verify-retired-src
```

Important active commands:

| Surface | Command |
| --- | --- |
| Control API | `cargo run -p rustcta-control-api-app --bin rustcta-control-api` |
| Cross-arb preflight | `cargo run -p rustcta-industrial-cli --bin rustcta-industrial -- cross-arb preflight` |
| Symbol lookup | `cargo run -p rustcta-tools-ops -- symbols gateio-bitget-spot` |
| Trend report | `cargo run -p rustcta-tools-ops -- reporter trend` |
| WebSocket proxy probe | `cargo run -p rustcta-tools-ops -- ws-proxy-probe` |
| Smart-money collector | `cargo run -p rustcta-tools-ops -- smart-money binance-collector` |
| Smart-money wallet ingestion | `cargo run -p rustcta-tools-ops -- smart-money hyperliquid-wallet-ingestion` |
| Smart-money portfolio service | `cargo run -p rustcta-tools-ops -- smart-money portfolio-service` |

Live-order canaries, private audits, order administration, and strategy runtime
launches must stay on their current workspace-owned surfaces. Do not reintroduce
root-package wrappers for those paths.

Final cleanup invariants:

- Root `src/` remains absent.
- Root `Cargo.toml` remains a virtual workspace manifest.
- Workspace crates, apps, strategies, and tools do not depend on the removed
  root package.
- Docs, scripts, and workflows use workspace package commands instead of
  root-package commands.

Validation:

```bash
cargo run -q -p rustcta-tools-ops -- verify-retired-src
cargo test -p rustcta-tools-ops --all-features
scripts/check_industrial_boundaries.sh
```

## Local Paper End-To-End Checklist

This is a manual smoke path for the migrated process boundaries. It uses paper
and read-only surfaces; do not add raw exchange credential write/delete behavior.

1. Build the workspace:

   ```bash
   cargo build --all-features
   ```

2. Start the local paper gateway:

   ```bash
   cargo run -p rustcta-gateway --bin rustcta-gateway -- --bind 127.0.0.1:18080
   ```

3. Run execution-router tests that cover dry-run and gateway-routed paper
   commands:

   ```bash
   cargo test -p rustcta-execution-router --all-features
   ```

4. Validate supervisor specs without starting live strategy processes:

   ```bash
   cargo run -q -p rustcta-supervisor-app --bin rustcta-supervisor -- --validate-spec config/supervisor/trend_report.spec.json
   cargo run -q -p rustcta-industrial-cli --bin rustcta-industrial -- supervisor readiness --spec-dir config/supervisor
   ```

5. Start the read-only supervisor registry service with a local registry file:

   ```bash
   cargo run -p rustcta-supervisor-app --bin rustcta-supervisor -- --serve --bind 127.0.0.1:18181 --registry-path logs/supervisor-registry.json
   ```

6. Start the generic control API:

   ```bash
   RUSTCTA_CONTROL_API_BIND=127.0.0.1:18081 cargo run -p rustcta-control-api-app --bin rustcta-control-api
   ```

7. Build the web UI:

   ```bash
   cd web-ui/dioxus && cargo build --release
   ```

8. Run the read-only smoke scripts and boundary checks:

   ```bash
   scripts/control_api_smoke_test.sh
   scripts/check_industrial_boundaries.sh
   ```

## Intentionally Paused Areas

These are paused until separately assigned:

- Further root-heavy backtest extraction beyond compile and facade work.
- Production event-ledger database backend selection.
- Raw credential administration path promotion into the public control API.
- Exchange gateway feature work and concrete adapter behavior changes.
- Full strategy live runtime relocation before SDK, execution, supervisor, and
  ledger boundaries have shared parity tests.

## Closeout Validation

Task 10 acceptance remains:

```bash
cargo fmt --check
cargo test --all-features
cargo clippy --all-targets --all-features
scripts/check_industrial_boundaries.sh
```
