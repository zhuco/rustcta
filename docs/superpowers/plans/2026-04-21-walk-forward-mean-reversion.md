# Walk Forward Mean Reversion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an industrial-grade `walk-forward-mean-reversion` backtest command that performs rolling in-sample parameter search plus out-of-sample validation and writes a JSON report.

**Architecture:** Reuse the existing scan pipeline instead of introducing a new dataset or execution engine. The runtime will derive rolling windows from the Kline replay, run the current parameter scan on each training window, re-run the top parameter sets on the validation window, then aggregate out-of-sample results into a stable report and manifest.

**Tech Stack:** Rust 2021, Tokio, Clap, Chrono, Serde, existing backtest runtime/matching/replay modules, cargo test

---

## File Map

- Modify: `src/backtest/runtime/mod.rs`
- Modify: `src/bin/backtest.rs`
- Modify: `tests/backtest_cli.rs`
- Modify: `tests/backtest_runtime.rs`
- Create: `config/mean_reversion_walk_forward.example.yml`

### Task 1: Add CLI surface for walk-forward execution

**Files:**
- Modify: `src/backtest/runtime/mod.rs`
- Modify: `src/bin/backtest.rs`
- Test: `tests/backtest_cli.rs`

- [ ] **Step 1: Keep the failing CLI parser test in place**
- [ ] **Step 2: Add `WalkForwardMeanReversionArgs`, command enum variant, output enum variant, and runtime config mapping**
- [ ] **Step 3: Update the backtest binary output branch**
- [ ] **Step 4: Run `cargo test --test backtest_cli parses_walk_forward_mean_reversion_command` and verify it passes**

### Task 2: Implement rolling train/validation execution

**Files:**
- Modify: `src/backtest/runtime/mod.rs`
- Test: `tests/backtest_runtime.rs`

- [ ] **Step 1: Keep the failing runtime report test in place**
- [ ] **Step 2: Add walk-forward spec/report/manifest structs and validation helpers**
- [ ] **Step 3: Reuse scan expansion and parameter application helpers to run training windows and validation windows**
- [ ] **Step 4: Aggregate out-of-sample performance by parameter set and persist the JSON report**
- [ ] **Step 5: Run `cargo test --test backtest_runtime walk_forward_mean_reversion_writes_out_of_sample_report` and verify it passes**

### Task 3: Verification and usability polish

**Files:**
- Modify: `tests/backtest_runtime.rs`
- Create: `config/mean_reversion_walk_forward.example.yml`

- [ ] **Step 1: Remove the temporary config fallback from the runtime test**
- [ ] **Step 2: Add an example walk-forward config for local use**
- [ ] **Step 3: Run fmt/check plus the backtest test matrix**
- [ ] **Step 4: Record remaining industrial-grade gaps after test evidence**
