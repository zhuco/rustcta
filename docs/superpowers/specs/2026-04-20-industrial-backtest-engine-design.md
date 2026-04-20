# Industrial Backtest Engine Design

## Context

`rustcta` is already a Rust-based live trading framework with:

- a unified exchange interface
- strategy modules with shared dependencies and risk evaluation
- an offline/mock exchange path
- isolated simulation logic for selected strategies

The project now needs an industrial-grade local backtesting platform that can:

- ingest and preserve Binance perpetual futures market data
- support both live capture and historical file import
- replay market events deterministically
- reconstruct market depth with gap detection and repair
- simulate realistic order execution with latency, queueing, fees, and funding
- run the existing strategy stack against the backtest runtime
- use `mean_reversion` as the first production strategy integration
- leave a clean extension path for trend strategies in phase two

This document defines the first-phase architecture and acceptance boundaries.

## Goals

- Build a backtesting system inside the main repository, not as an external script.
- Keep strategy decision logic as shared as practical between live trading and backtesting.
- Make data ingestion, normalization, replay, matching, and ledger logic auditable and testable.
- Optimize for local high-performance offline replay on a strong workstation.
- Provide deterministic runs and reproducible outputs for research and debugging.

## Non-Goals

Phase one does not include:

- full L3 per-order book reconstruction
- market impact modeling
- multi-account competition inside the same market
- portfolio margin replication
- cross-exchange joint simulation
- a web UI or operations console
- one-shot migration of every existing strategy

## Recommended Approach

Use a shared strategy core with a dedicated backtest runtime.

Rejected alternatives:

- Separate backtest-only strategy implementations: fast to start, but guarantees divergence from live logic.
- Full live/runtime unification before delivery: cleaner long-term, but too slow and high-risk for phase one.

Recommended model:

- keep the existing live runtime intact
- add a dedicated `backtest` subsystem
- refactor `mean_reversion` to separate decision logic from execution plumbing
- introduce strategy-facing ports that can be implemented by both live and backtest runtimes

## Phase-One Scope

Primary market:

- Binance perpetual futures

Data modes:

- real-time market data capture
- historical file import

Execution fidelity:

- order book reconstruction from snapshots and deltas
- deterministic event replay
- high-fidelity L2-based matching
- support for latency, partial fills, maker/taker fees, and funding settlement

First integrated strategy:

- `mean_reversion`

Deferred to phase two:

- trend strategy integration

## Architecture Overview

Create a new `src/backtest` subsystem with the following modules:

### `src/backtest/data`

Responsibilities:

- Binance real-time capture
- historical file import
- raw data preservation
- conversion into normalized internal events

### `src/backtest/schema`

Responsibilities:

- define stable internal event schemas
- separate exchange-specific payloads from internal event models
- preserve enough timestamp information for replay and latency modeling

### `src/backtest/storage`

Responsibilities:

- raw-layer persistence
- normalized-layer persistence
- partitioning, metadata, and completeness markers
- local read efficiency for replay and research

### `src/backtest/replay`

Responsibilities:

- merge event streams into a single ordered timeline
- expose deterministic replay iterators
- validate temporal ordering and depth continuity
- support single-symbol and multi-symbol playback

### `src/backtest/matching`

Responsibilities:

- rebuild the local order book
- model order lifecycle and execution
- simulate latency, queueing, partial fills, and cancellations
- emit execution events to the ledger and strategy runtime

### `src/backtest/runtime`

Responsibilities:

- orchestrate replay, matching, ledger, and strategy execution
- run one-off backtests and batch parameter sweeps
- export structured run artifacts and reports

## Data Model and Storage Design

### Raw Layer

The raw layer is the system of record for preservation and auditability.

Preserve:

- live-captured Binance futures streams
- imported historical exchange files or user-supplied archives

Recommended raw layout:

```text
data/
  raw/
    binance_futures/
      BTCUSDT/
        2026-04-20/
          trades/
          depth/
          book_ticker/
          mark_price/
          funding/
```

Recommended raw formats:

- `ndjson.zst` for captured streams
- original exchange archives when imported directly

Each raw segment should have metadata that records:

- source
- capture/import time
- checksum
- time range
- row count
- parse version
- completeness status

### Normalized Layer

The normalized layer is the main input to replay and backtesting.

Use a stable internal schema with at least the following event families:

- `TradeEvent`
- `DepthSnapshotEvent`
- `DepthDeltaEvent`
- `BookTickerEvent`
- `MarkPriceEvent`
- `FundingRateEvent`
- `KlineEvent`
- `BacktestEvent` as the unified enum

Each event should preserve at least:

- `exchange_ts`
- `local_recv_ts`
- `logical_ts`

This supports:

- idealized exchange-time replay
- receive-time replay
- latency-aware simulation

### Storage Format

Recommended normalized storage:

- Parquet as the main normalized format

Rationale:

- efficient columnar reads for replay and analysis
- strong compression
- practical interoperability with Python analysis tools
- scalable partitioning for local batch runs

Recommended normalized layout:

```text
data/
  normalized/
    exchange=binance_futures/
      symbol=BTCUSDT/
        date=2026-04-20/
          event_type=trade/
          event_type=depth_delta/
          event_type=depth_snapshot/
          event_type=book_ticker/
          event_type=mark_price/
```

Each normalized partition should have a lightweight index containing:

- file path
- time range
- update-id range when relevant
- record count
- snapshot coverage
- continuity status

### Completeness Rules

Depth data must be validated before replay:

- snapshots and deltas must align on update identifiers
- delta continuity gaps must be detected explicitly
- invalid book state must not be silently replayed
- replay must require a new valid snapshot after a gap

## Replay Engine Design

The replay module only reconstructs and emits ordered events. It does not own strategy logic or account state.

Required replay modes:

- `exchange_time`
- `receive_time`

Core responsibilities:

- merge multiple event streams into one ordered sequence
- expose deterministic iteration
- support time-window filtering
- support single-symbol and multi-symbol playback
- halt or repair on depth continuity failures

Determinism requirements:

- same data
- same parameters
- same seed
- same output

## Matching Engine Design

### Book Reconstruction

Maintain a local `OrderBookState` that supports:

- initialization from depth snapshots
- sequential application of depth deltas
- validation after each update
- configurable retained depth, defaulting to 100 levels

Mark the book invalid and stop matching if:

- update identifiers are discontinuous
- the applied book becomes crossed
- snapshot/delta handoff fails

### Order Lifecycle

Model at least the following states:

- `PendingNew`
- `Accepted`
- `PartiallyFilled`
- `Filled`
- `PendingCancel`
- `Canceled`
- `Rejected`
- `Expired`

Track at least:

- `submitted_at`
- `accepted_at`
- `first_fill_at`
- `last_fill_at`
- `cancel_requested_at`
- `canceled_at`

### Latency Model

Phase one should support:

- send latency
- cancel latency
- market data latency

Each should support:

- fixed values
- bounded random jitter
- symbol/time scoped configuration

Default first implementation:

- fixed values with optional jitter

### Matching Model

Phase one should use an L2.5-style matching model:

- L2 order book as the visible market
- explicit placement of strategy orders at price levels
- queue-position estimation rather than full L3 simulation

Queue models:

- `ConservativeQueueModel`
- `EstimatedQueueModel` as a future extension point

Phase-one implementation recommendation:

- implement the conservative model first

### Order Type Support

Required in phase one:

- market orders
- limit orders
- post-only orders

Required behaviors:

- market orders walk the visible book
- limit orders fill only when price and queue conditions permit
- post-only orders reject or reprice based on configuration
- partial fills are fully supported

### Fees and Funding

Fees and funding must be handled centrally, not in strategy code.

Required:

- maker/taker fee settlement per fill
- funding settlement at funding timestamps
- mark-price-based notional for funding

## Ledger and Portfolio Accounting

Create a dedicated ledger component to track:

- cash balance
- reserved margin
- realized PnL
- unrealized PnL
- cumulative fees
- cumulative funding
- position size and average entry
- liquidation-threshold checks

The matching engine emits fills and order-state changes.
The ledger owns the account-level financial truth.

## Strategy Integration Design

### Core Principle

Backtests should reuse the same strategy decision logic as live trading wherever possible.

Do not create a second backtest-only implementation of `mean_reversion`.

### Strategy Ports

Introduce a strategy-facing interface layer with ports such as:

- `MarketClock`
- `MarketDataView`
- `ExecutionPort`
- `PortfolioView`
- `StrategyEvent`
- `StrategyAction`

This allows the same strategy core to run on:

- a live runtime adapter
- a backtest runtime adapter

### `mean_reversion` Refactor Target

Refactor `mean_reversion` into:

- a decision core for signals and position intent
- an execution adapter for order translation
- a runtime controller for event handling and scheduling

Phase-one backtest integration should extract and reuse:

- indicators
- deviation and entry/exit logic
- directional position decisions
- pre-trade risk checks

Live-only exchange details must be isolated behind execution adapters.

### Trend Strategy Extension Path

The same port layer should later support trend strategies by reserving interface support for:

- candle-window access
- multi-timeframe indicators
- session filters
- stop/take-profit logic
- position management actions

## Validation Strategy

The system must prove:

- data correctness
- matching correctness
- deterministic reproducibility
- result traceability

### Test Layers

Required test layers:

- schema/parser unit tests
- order-book reconstruction tests
- matching state-machine tests
- ledger and risk-accounting tests
- end-to-end strategy backtest tests

### Golden Fixtures

Create fixed replay fixtures that pin known expected outputs for:

- book reconstruction
- partial fills
- funding settlement
- a small `mean_reversion` end-to-end run

Expected fixture outputs should include:

- fills
- final position
- final equity
- fee totals
- funding totals
- drawdown metrics
- order-state sequences

### Performance Validation

Track at least:

- events per second
- single-symbol replay throughput
- multi-symbol replay throughput
- import throughput
- memory footprint
- time spent in replay, matching, strategy, and ledger paths

Phase one should demonstrate:

- replay faster than wall-clock market time
- stable batch execution for repeated runs
- no sustained memory growth during long replays

### Debug Artifacts

Each run should emit:

- a manifest with dataset, config, commit, seed, and time range
- optional event and action traces
- structured execution and equity reports

## Deliverables

Phase one deliverables:

1. Binance perpetual futures data ingestion for live capture and historical import
2. Raw and normalized data storage with continuity checks
3. Deterministic replay engine
4. High-fidelity L2-based matching and account ledger
5. `mean_reversion` integration on the new backtest runtime
6. Backtest CLI for configuring market, symbol, time range, data path, and output path
7. Tests, golden fixtures, and baseline performance benchmarks

## Acceptance Criteria

Phase one is considered complete when all of the following are true:

- real Binance perpetual data can be captured or imported locally
- normalized datasets pass continuity validation
- the order book can be replayed correctly from snapshots and deltas
- `mean_reversion` can run end-to-end on the backtest runtime
- fills, positions, fees, funding, equity, and drawdown are exported
- rerunning with the same inputs yields identical results
- the repository includes automated tests for core backtest modules
- offline replay runs materially faster than real-time market progression on the target workstation

## Implementation Sequence

Recommended execution order:

1. add `backtest` module skeleton and shared schemas
2. implement raw capture and historical import
3. implement normalized storage and indexing
4. implement replay and depth continuity handling
5. implement matching engine and ledger
6. refactor `mean_reversion` behind strategy ports
7. add CLI, reports, tests, fixtures, and benchmarks

## Open Follow-On Work

Planned after phase one:

- trend strategy integration
- richer queue-position models
- optional L3 support where data exists
- broader exchange support
- research workflow improvements and analysis tooling
