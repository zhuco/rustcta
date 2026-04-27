# Industrial Backtest Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first industrial-grade backtest foundation for `rustcta`, starting with Binance futures Kline acquisition, local dataset persistence, and the initial backtest module/CLI scaffold.

**Architecture:** Add a dedicated `backtest` subsystem inside the Rust crate, beginning with an exchange-backed Kline ingestion path that can fetch and persist historical candles in a replay-friendly layout. Then add a thin backtest CLI/runtime skeleton that consumes the stored datasets and establishes stable interfaces for later order book replay, matching, and strategy integration.

**Tech Stack:** Rust 2021, Tokio, Reqwest, Serde, Chrono, Clap, filesystem-backed local datasets, cargo test

---

## File Map

- Create: `src/backtest/mod.rs`
- Create: `src/backtest/data/mod.rs`
- Create: `src/backtest/data/kline_dataset.rs`
- Create: `src/backtest/runtime/mod.rs`
- Create: `src/bin/backtest.rs`
- Modify: `src/lib.rs`
- Modify: `src/main.rs`
- Modify: `src/exchanges/binance.rs`
- Modify: `src/strategies/mean_reversion/data.rs`
- Modify: `Cargo.toml`
- Create: `tests/backtest_kline_dataset.rs`
- Create: `tests/backtest_cli.rs`

### Task 1: Add a tested Kline dataset model and persistence layer

**Files:**
- Create: `src/backtest/mod.rs`
- Create: `src/backtest/data/mod.rs`
- Create: `src/backtest/data/kline_dataset.rs`
- Test: `tests/backtest_kline_dataset.rs`

- [ ] **Step 1: Write the failing tests for dataset writing, sorting, and manifest generation**

Write tests that:
- build a small unordered `Vec<Kline>`
- persist it into a temporary directory
- assert the output data file and manifest exist
- assert rows are stored chronologically and deduplicated by close time

- [ ] **Step 2: Run the focused tests to verify they fail**

Run: `cargo test --test backtest_kline_dataset`
Expected: FAIL because `backtest::data::kline_dataset` does not exist yet

- [ ] **Step 3: Write the minimal dataset implementation**

Implement:
- a `KlineDatasetWriter`
- stable directory layout under `data/normalized/...`
- chronological sort + dedupe helpers
- a small manifest struct serialized to JSON

- [ ] **Step 4: Run the focused tests to verify they pass**

Run: `cargo test --test backtest_kline_dataset`
Expected: PASS

- [ ] **Step 5: Commit**

Run:
`git add src/backtest src/lib.rs tests/backtest_kline_dataset.rs`
`git commit -m "feat: add backtest kline dataset storage"`

### Task 2: Extend Binance Kline fetching for historical windows

**Files:**
- Modify: `src/exchanges/binance.rs`
- Test: `tests/backtest_kline_dataset.rs`

- [ ] **Step 1: Write the failing test for request parameter generation**

Add a test that validates a helper builds Binance Kline query parameters including:
- symbol
- interval
- limit
- optional start time
- optional end time

- [ ] **Step 2: Run the focused test to verify it fails**

Run: `cargo test backtest_kline_query`
Expected: FAIL because the helper does not exist yet

- [ ] **Step 3: Write the minimal implementation**

Add a focused helper in `src/exchanges/binance.rs` to build query params for historical Kline requests, then update the Kline fetch path to accept optional `start_time` and `end_time`.

- [ ] **Step 4: Run the focused tests to verify they pass**

Run: `cargo test backtest_kline_query`
Expected: PASS

- [ ] **Step 5: Commit**

Run:
`git add src/exchanges/binance.rs tests/backtest_kline_dataset.rs`
`git commit -m "feat: support windowed binance kline fetches"`

### Task 3: Add a backtest CLI command that fetches and stores exchange Klines

**Files:**
- Create: `src/bin/backtest.rs`
- Modify: `Cargo.toml`
- Modify: `src/lib.rs`
- Test: `tests/backtest_cli.rs`

- [ ] **Step 1: Write the failing CLI parsing tests**

Write tests that validate:
- required arguments for exchange, market, symbol, interval, start, and end
- a `fetch-klines` command shape
- dataset output root parsing

- [ ] **Step 2: Run the focused CLI tests to verify they fail**

Run: `cargo test --test backtest_cli`
Expected: FAIL because the binary command parser does not exist yet

- [ ] **Step 3: Write the minimal CLI implementation**

Implement a `backtest` binary with:
- `fetch-klines` subcommand
- Binance futures exchange bootstrap
- Kline fetch call
- dataset writer call

- [ ] **Step 4: Run the focused CLI tests to verify they pass**

Run: `cargo test --test backtest_cli`
Expected: PASS

- [ ] **Step 5: Commit**

Run:
`git add Cargo.toml src/bin/backtest.rs src/lib.rs tests/backtest_cli.rs`
`git commit -m "feat: add backtest kline fetch cli"`

### Task 4: Add a backtest runtime skeleton and connect it to the crate

**Files:**
- Create: `src/backtest/runtime/mod.rs`
- Modify: `src/backtest/mod.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Write the failing smoke test for the runtime scaffold**

Add a test that constructs a minimal runtime config or marker type and verifies the crate exposes the backtest module.

- [ ] **Step 2: Run the focused tests to verify they fail**

Run: `cargo test backtest_runtime_smoke`
Expected: FAIL because the runtime module does not exist yet

- [ ] **Step 3: Write the minimal runtime scaffold**

Implement:
- `BacktestRuntimeConfig`
- `BacktestRunManifest`
- a placeholder `BacktestRuntime`

- [ ] **Step 4: Run the focused tests to verify they pass**

Run: `cargo test backtest_runtime_smoke`
Expected: PASS

- [ ] **Step 5: Commit**

Run:
`git add src/backtest`
`git commit -m "feat: add backtest runtime scaffold"`

### Task 5: Verify the milestone end-to-end

**Files:**
- Modify: `src/strategies/mean_reversion/data.rs`
- Test: `tests/backtest_kline_dataset.rs`
- Test: `tests/backtest_cli.rs`

- [ ] **Step 1: Add a small integration point for Kline loading reuse**

Expose or adapt shared Kline utilities so the future backtest runtime can reuse chronological merge helpers without duplicating logic.

- [ ] **Step 2: Run focused tests**

Run:
- `cargo test --test backtest_kline_dataset`
- `cargo test --test backtest_cli`

Expected: PASS

- [ ] **Step 3: Run crate verification**

Run:
- `cargo check`

Expected: PASS

- [ ] **Step 4: Run a real fetch command against Binance**

Run:
`cargo run --bin backtest -- fetch-klines --exchange binance --market futures --symbol BTCUSDT --interval 1m --start 2026-04-01T00:00:00Z --end 2026-04-01T01:00:00Z --output data`

Expected:
- fetch succeeds
- local dataset files are written
- manifest is generated

- [ ] **Step 5: Commit**

Run:
`git add src/backtest src/bin/backtest.rs src/strategies/mean_reversion/data.rs tests`
`git commit -m "feat: add backtest phase1 kline ingestion milestone"`
