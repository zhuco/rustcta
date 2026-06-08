# Industrial Workspace Update 2026-06-07 v0.3.9

Status date: 2026-06-07

This update closes the current refactor cleanup pass and records the repository
shape that should be treated as current after the industrial workspace split.

## Version

- Root package version: `0.3.9`
- Workspace package version: `0.3.9`
- Runtime template version in `config/config.toml`: `2.0.1`

## Current Directory Structure

```text
apps/        process entrypoints: gateway, supervisor, control-api, cli, backtest
crates/      reusable platform crates and API contracts
strategies/  independent strategy crates and migrating adapter-free cores
tools/       operator tools, diagnostics, migration inventory, and safe reports
web-ui/      Dioxus control panel workspace
src/         legacy root crate and compatibility runtime still under migration
config/      active runtime configs, exchange examples, and supervisor specs
docs/        active architecture, operations, migration, and reference docs
scripts/     local automation and validation helpers
tests/       integration, fixture, regression, and live-readonly tests
data/sql/    database seeds and analytics SQL
logs/        ignored local runtime logs and validation output
```

`apps/`, `crates/`, `strategies/`, and `tools/` are now the preferred
workspace boundaries. The legacy root `src/` crate remains because concrete
runtime behavior is still being extracted in staged slices.

## Cleanup

- Removed ignored generated artifacts from the working tree:
  `scripts/__pycache__/` and `web-ui/dioxus/dist/`.
- Removed local runtime scratch state from the working tree: `run/` and
  `help/*.log*`.
- Removed empty legacy directories left after migration cleanup:
  `retired exchange tree/adapters/` and `src/spot_control/`.
- Kept the intentional source and documentation removals that are already
  represented as tracked deletes in this refactor.

## Fixes

- Updated `config/config.toml` so the strategy config directory points at the
  current `config/` root instead of the deleted `config/strategies` path.
- Replaced a hard-coded Grafana password placeholder with an empty value and an
  environment-variable note.
- Updated active documentation links for the new workspace layout and the
  current supervisor spec set, including `spot_spot_live_dry_run`.

## Validation

Current local validation for this cleanup pass:

```bash
cargo fmt --all --check
cargo check --workspace
scripts/check_industrial_boundaries.sh
cargo clippy --workspace --all-targets --all-features
cargo test --workspace --all-features
```

These checks pass. `cargo clippy --workspace --all-targets --all-features
-- -D warnings` is still blocked by existing lint debt, mainly large error enum
warnings in exchange/execution gateway boundaries and older style warnings in
legacy strategy/backtest modules. Web UI release build remains part of the
broader migration acceptance gate described in
`docs/industrial_workspace_migration_status.md`.
