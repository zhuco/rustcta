# Mean Reversion Analysis Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an `analyze-mean-reversion` command that converts scan and optional walk-forward reports into a research-friendly analysis report.

**Architecture:** Reuse the existing `MeanReversionScanReport` and `MeanReversionWalkForwardReport` JSON files as inputs, then compute stable summary metrics in the runtime layer and expose them through a new CLI command. The analyzer stays read-only and independent from the execution engine so it can be used offline after large scan batches finish.

**Tech Stack:** Rust 2021, Tokio, Clap, Serde, existing backtest runtime report structs, cargo test

---

## File Map

- Modify: `src/backtest/runtime/mod.rs`
- Modify: `src/bin/backtest.rs`
- Modify: `tests/backtest_cli.rs`
- Modify: `tests/backtest_runtime.rs`
- Create: `config/mean_reversion_analysis.example.yml`

### Task 1: Add analyzer CLI surface

**Files:**
- Modify: `src/backtest/runtime/mod.rs`
- Modify: `tests/backtest_cli.rs`

- [ ] **Step 1: Write the failing CLI test for `analyze-mean-reversion`**
- [ ] **Step 2: Run `cargo test --test backtest_cli parses_analyze_mean_reversion_command` and verify it fails**
- [ ] **Step 3: Add CLI args, command enum variant, output enum variant, and config plumbing**
- [ ] **Step 4: Re-run the focused CLI test and verify it passes**

### Task 2: Add analyzer runtime and JSON report

**Files:**
- Modify: `src/backtest/runtime/mod.rs`
- Modify: `tests/backtest_runtime.rs`

- [ ] **Step 1: Write the failing runtime test for analysis report generation**
- [ ] **Step 2: Run `cargo test --test backtest_runtime analyze_mean_reversion_combines_scan_and_walk_forward_reports` and verify it fails**
- [ ] **Step 3: Implement report loading, summary metrics, optional walk-forward consistency checks, and JSON persistence**
- [ ] **Step 4: Re-run the focused runtime test and verify it passes**

### Task 3: Wire binary output and ship an example

**Files:**
- Modify: `src/bin/backtest.rs`
- Create: `config/mean_reversion_analysis.example.yml`

- [ ] **Step 1: Print a concise analyzer result summary in the backtest binary**
- [ ] **Step 2: Add an example config or invocation template for local usage**
- [ ] **Step 3: Run fmt/check and the backtest test matrix**
- [ ] **Step 4: Capture remaining gaps after verification**
