# Repository Guidelines

## Project Structure & Module Organization
- Core Rust crates live in `src/`; additional binaries sit under `src/bin/` (e.g., `supervisor`, `web-server`, `order_sync`).
- Integration and regression fixtures are in `tests/`; consult `docs/` for feature notes and design records.
- Configuration templates reside in `config/`, `format_schemas/`, and `preprocessed_configs/`; SQL seeds go in `sql/`.
- Automation helpers are under `scripts/`; logs and ad-hoc validation outputs belong in `logs/`.

## Build, Test, and Development Commands
- `cargo check` – fast dependency and type validation for all targets.
- `cargo build --release` – optimized binaries for deployment.
- `cargo build --features postgres-collector` – include optional Postgres collectors.
- `cargo test --all-features` – run unit and integration suites (including `tests/` fixtures).
- `cargo fmt` and `cargo clippy --all-targets --all-features` – format and lint before review.
- `python check_orders.py` / `python check_ltc_orders.py` – data sanity checks; keep outputs in `logs/`.

## Coding Style & Naming Conventions
- Rust 2021 idioms: modules/files in `snake_case`, types/traits in `PascalCase`, async functions ending with `_task`.
- Keep Python utilities PEP 8 compliant.
- Always run `cargo fmt` before commits; fix warnings from `cargo clippy --all-targets --all-features`.

## Testing Guidelines
- Favor focused unit tests colocated with modules; use `tests/` for cross-component behavior.
- Name tests `mod_name::scenario_should_*` to match existing convention.
- Run `cargo test --all-features` locally before pushing; use `test_queries.sh` when touching ClickHouse/SQL analytics paths.

## Commit & Pull Request Guidelines
- Follow mixed history style: Conventional Commit prefixes (`chore:`, `feat:`) with optional bilingual summaries.
- Commit bodies should note rationale, data sources, and TODO follow-ups.
- Pull requests must include a concise goal summary, validation steps (e.g., `cargo test`), linked issues/tickets, and screenshots or logs for UI/trading-impacting changes. Flag breaking config updates with `[BREAKING]` in the title.

## Security & Configuration Tips
- Never commit secrets; load credentials from `.env` and document required API scopes in `docs/`.
- Validate new configs with `serde_yaml`/`serde_json` loaders or `scripts/validate_config.sh`; review `notify` watchers to avoid PII leakage in logs.
