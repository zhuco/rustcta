# Smart Money Storage Schemas

This directory contains storage-only artifacts for the Smart Money Alpha Platform described in
`docs/smart_money_alpha_platform_development.md`.

- `postgres_schema.sql` defines canonical state tables for traders, wallets, profiles, scores, rankings, clusters, regimes, target portfolios, risk decisions, and backtest summaries.
- `clickhouse_schema.sql` defines high-volume fact tables for Binance market data, Hyperliquid wallet facts, alpha snapshots, execution fills, NAV snapshots, and backtest result time series.

These files intentionally do not modify Rust source code or introduce a migration runner. Apply them with the deployment tooling chosen for PostgreSQL and ClickHouse.
