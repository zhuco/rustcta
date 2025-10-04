# Repository Guidelines

## Project Structure & Module Organization
The Rust core lives under `src/`, with reusable modules grouped by domain and additional binaries in `src/bin/` (e.g., `supervisor`, `web-server`, `order_sync`). Integration fixtures and regression checks sit in `tests/`, while SQL seeds reside in `sql/` and HTTP or configuration templates are kept under `config/`, `format_schemas/`, and `preprocessed_configs/`. Automation helpers live in `scripts/` and `docs/` records feature notes.

## Build, Test, and Development Commands
- `cargo check` – fast type and dependency verification for all targets.
- `cargo build --release` – optimized binaries for deployment (default feature set).
- `cargo build --features postgres-collector` – include optional collectors when Postgres is required.
- `cargo test --all-features` – run unit and integration suites, including `tests/` fixtures.
- `cargo fmt` / `cargo clippy --all-targets --all-features` – enforce formatting and lints before review.
- `python check_orders.py` and `python check_ltc_orders.py` – ad-hoc data validation scripts; keep outputs under `logs/`.
- `./scripts/*` and `start_*.sh` – review README headers inside each script before invoking in production environments.

## Coding Style & Naming Conventions
Follow Rust 2021 idioms: modules and files in `snake_case`, types and traits in `PascalCase`, async tasks ending with `_task`. Run `cargo fmt` prior to commits; address all warnings from `cargo clippy --all-targets --all-features`. Python utilities in `scripts/` should remain PEP 8 compliant.

## Testing Guidelines
Prefer focused unit tests next to the module being exercised and broaden coverage in `tests/` only when cross-component behavior matters. Name tests `mod_name::scenario_should_*` to match existing conventions. Run `cargo test --all-features` locally before pushing and use `test_queries.sh` for ClickHouse/SQL sanity checks when touching analytics pathways. Highlight significant coverage gaps in the pull request.

## Commit & Pull Request Guidelines
Commit history mixes Conventional Commits (`chore:`, `feat:`) with bilingual summaries—stick to the English prefix plus optional localized detail. Write descriptive bodies outlining rationale, data sources, and follow-up TODOs. Pull requests must include: a concise goals summary, validation steps (`cargo test`, script runs), linked issues or tickets, and screenshots or log excerpts for dashboard or trading-impacting changes. Flag breaking configuration updates in the title using `[BREAKING]`.

## Security & Configuration Tips
Credentials live in environment variables loaded via `.env`; never commit secrets. Validate new config files with `serde_yaml`/`serde_json` loaders or `scripts/validate_config.sh` if introduced. When integrating exchanges or collectors, document required API scopes inside `docs/` and review `notify` watchers to avoid leaking PII in log messages.
