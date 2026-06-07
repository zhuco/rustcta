# Industrial Migration Final Gates

Status date: 2026-06-07

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
cargo run -q -p rustcta-tools-ops -- verify-legacy-bins --src-bin-dir src/bin
cargo run -q -p rustcta-tools-ops -- legacy-bin-plan
scripts/check_industrial_boundaries.sh
cd web-ui/dioxus && cargo build --release
```

The existing `.github/workflows/non-gateway-industrial.yml` remains a faster
focused gate for the non-gateway migration slice. The boundary script checks
that both workflows keep their required migration entries.

## Legacy Command Runbook

Prefer the new workspace commands in operator docs and scripts. Keep legacy
binary names available until the retirement milestone in the inventory is met.

Current closure audit result: the migration is not ready for deletion-style
final closure. The legacy binary inventory is classified and boundary checks
pass, but many entries are still intentionally retained as `keep_wrapper`,
`warn`, or `remove_later`. Passing inventory checks means the old entrypoints
are accounted for; it does not mean they are safe to delete.

The machine-readable source of truth is:

```bash
cargo run -q -p rustcta-tools-ops -- legacy-bin-plan
cargo run -q -p rustcta-industrial-cli --bin rustcta-industrial -- migration legacy-bin-plan
```

Important replacements:

| Legacy name | New command | Current decision |
| --- | --- | --- |
| `control_api` | `cargo run -p rustcta-control-api-app --bin rustcta-control-api` | keep wrapper |
| `cross_arb_preflight` | `cargo run -p rustcta-industrial-cli --bin rustcta-industrial -- cross-arb preflight` | alias |
| `gateio_bitget_spot_symbols` | `cargo run -p rustcta-tools-ops -- symbols gateio-bitget-spot` | alias |
| `trend_report` | `cargo run -p rustcta-tools-ops -- reporter trend` | alias |
| `ws_proxy_probe` | `cargo run -p rustcta-tools-ops -- ws-proxy-probe` | alias |
| `smart_money_binance_collector` | `cargo run -p rustcta-tools-ops -- smart-money binance-collector` | alias |
| `smart_money_hyperliquid_wallet_ingestion` | `cargo run -p rustcta-tools-ops -- smart-money hyperliquid-wallet-ingestion` | alias |
| `smart_money_portfolio_service` | `cargo run -p rustcta-tools-ops -- smart-money portfolio-service` | alias |
| `backtest` | `cargo run -p rustcta-backtest-app --bin rustcta-backtest` | remove later |
| `short_ladder_mtf_grid` | `cargo run -p rustcta-backtest-app --bin rustcta-backtest -- short-ladder-mtf-grid` | remove later |

Live-order canaries, private audits, order administration, legacy long-running
strategy runtimes, and the legacy control API stay in compatibility paths until
their safety and parity tests exist. They must not be silently removed.

Do not delete these compatibility groups during final cleanup:

- `src/bin/control_api.rs`: still owns the large legacy local-console surface,
  raw local exchange API key env-store write/delete behavior, static UI
  compatibility, legacy command aliases, and snapshot-backed dashboard routes.
- live canary/admin/audit binaries: still own confirmation flags, private
  account access, or live mutation safety gates.
- strategy runtime binaries such as `cross_arb_live`, `funding_arb_live`, and
  observe commands: supervisor specs and current runbooks still launch legacy
  processes while strategy runtime orchestration remains rooted in
  `src/strategies/*`.
- root backtest compatibility binaries: keep until docs/scripts stop invoking
  `cargo run --bin backtest` and `cargo run --bin short_ladder_mtf_grid`.
- Dioxus legacy endpoint fallbacks: keep while the panel still references
  legacy routes such as `/api/exchange-api-keys`, `/api/spot-arb/dashboard`,
  `/api/cross-arb/dashboard`, `/api/control/symbols`,
  `/api/strategy-config`, and `/api/cross-arb/settings`.

Deletion is allowed only after `legacy-bin-plan` marks the specific entry as
`alias` or `remove_later`, the replacement command has contract tests, and
operator docs/scripts no longer depend on the old name.

## Compatibility Retirement Matrix

Every direct `src/bin/*.rs` file must have one inventory row with:

- `target`: intended workspace owner.
- `compatibility`: one of `keep_wrapper`, `warn`, `alias`, `remove_later`.
- `new_command`: current or planned replacement command.
- `retirement_milestone`: condition required before changing the old name.

Validation:

```bash
cargo run -q -p rustcta-tools-ops -- verify-legacy-bins --src-bin-dir src/bin
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
   cargo run -p rustcta-control-api-app --bin rustcta-control-api -- --bind 127.0.0.1:18081
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
