# Trend Factor Scan Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a reusable Rust trend-factor indicator scanning framework on top of the existing backtest dataset runtime.

**Architecture:** Extract indicator computations into focused modules, define factor rule configs and parameter expansion separately, run reusable trend-factor strategies from `scan-trend-factor`, and rank results with composite scoring.

**Tech Stack:** Rust 2021, serde/serde_yaml, existing backtest Kline dataset and CLI runtime, cargo tests.

---

### Task 1: Indicator Module

**Files:**
- Create: `src/backtest/indicators/mod.rs`
- Modify: `src/backtest/mod.rs`
- Test: `tests/backtest_trend_factor.rs`

- [ ] Add tests for EMA, Donchian, Bollinger width, SuperTrend, Volume Ratio, ROC.
- [ ] Implement indicator functions using `&[Kline]` inputs and `Vec<Option<f64>>` outputs.
- [ ] Export module from `src/backtest/mod.rs`.

### Task 2: Factor Config and Expansion

**Files:**
- Create: `src/backtest/factors/mod.rs`
- Modify: `src/backtest/mod.rs`
- Test: `tests/backtest_trend_factor.rs`

- [ ] Add tests for grid expansion count and parameter names.
- [ ] Implement fixed rule enum config for strategy templates.
- [ ] Implement `expand_trend_factor_runs`.

### Task 3: Scoring

**Files:**
- Create: `src/backtest/scoring/mod.rs`
- Modify: `src/backtest/mod.rs`
- Test: `tests/backtest_trend_factor.rs`

- [ ] Add tests for composite score and low-trade-count penalty.
- [ ] Implement score calculation and risk flags.

### Task 4: Strategy Runner and CLI

**Files:**
- Create: `src/backtest/strategy/trend_factor.rs`
- Modify: `src/backtest/strategy/mod.rs`
- Modify: `src/backtest/runtime/mod.rs`
- Modify: `src/bin/backtest.rs`
- Test: `tests/backtest_cli.rs`, `tests/backtest_trend_factor.rs`

- [ ] Add `scan-trend-factor` CLI parse test.
- [ ] Implement strategy runner over Kline replay.
- [ ] Persist JSON report with run summaries and trades.

### Task 5: Example Config and Verification

**Files:**
- Create: `config/trend_factor_scan.example.yml`
- Test: cargo test commands and a BTC/DOGE/SUI smoke run.

- [ ] Add example YAML with five strategy templates.
- [ ] Run targeted tests.
- [ ] Run smoke scan on existing 90-day datasets.
