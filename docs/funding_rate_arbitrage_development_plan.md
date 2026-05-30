# Funding Rate Arbitrage Development Plan

## Goal

Build a standalone funding-rate arbitrage workflow for USDT perpetual contracts.
At each funding cycle, the system scans every enabled exchange, selects the
single contract with the lowest funding rate on that exchange, opens a small
position just before settlement when the rate is sufficiently negative, closes
the position just after settlement, calculates realized arbitrage PnL, and sends
a WeCom notification.

Initial live sizing is intentionally small:

- one selected symbol per exchange per funding event;
- `10 USDT` target notional per order;
- open before funding settlement;
- close after funding settlement;
- initial exchange scope is Binance, Bitget, and Gate;
- every enabled account is expected to run in hedge/dual-position mode;
- open orders are submitted concurrently across selected exchanges, and close
  orders are submitted concurrently across selected exchanges after settlement;
- default mode remains observe or dry-run until preflight passes.

The strategy should follow the same operational pattern as cross-exchange
arbitrage: independent binary entrypoints, independent config, public observer,
private preflight, and a live runner with explicit safety gates.

## Strategy Interpretation

Funding direction must be handled explicitly.

For linear USDT perpetuals, when `funding_rate < 0`, shorts usually pay longs.
The strategy should therefore open a long position before settlement and close
that long after settlement. This rule must be validated per exchange adapter
before enabling live orders because exchanges can expose current, predicted, or
last funding rates with slightly different semantics.

Initial target condition:

```text
funding_rate <= -0.005
```

`-0.005` means `-0.5%` for that funding interval, not annualized rate.

Expected gross funding income for a 10 USDT long is:

```text
gross_funding = notional * abs(funding_rate)
```

Example:

```text
10 * 0.005 = 0.05 USDT
```

Net result must subtract taker fees, slippage, and any position PnL caused by
price movement during the short hold window.

## Repository Layout

Add a new strategy module:

```text
src/strategies/funding_rate_arbitrage/
  mod.rs
  config.rs
  scanner.rs
  selection.rs
  schedule.rs
  execution.rs
  accounting.rs
  notification.rs
  state.rs
```

Add standalone binaries:

```text
src/bin/funding_arb_observe.rs
src/bin/funding_arb_preflight.rs
src/bin/funding_arb_live.rs
```

Add config:

```text
config/funding_rate_arbitrage_usdt.yml
```

Add tests:

```text
tests/funding_rate_arbitrage_selection.rs
tests/funding_rate_arbitrage_accounting.rs
tests/funding_rate_arbitrage_schedule.rs
```

## Binary Responsibilities

### `funding_arb_observe`

Safe public-data command. It never submits orders.

Responsibilities:

- load config;
- load instruments for each enabled exchange;
- load funding snapshots for all eligible USDT perpetual symbols;
- group candidates by exchange;
- select the lowest funding-rate candidate per exchange;
- apply threshold, freshness, settlement-window, volume, and notional filters;
- print JSON summary with selected and rejected candidates.

Example command:

```bash
cargo run --bin funding_arb_observe -- --config config/funding_rate_arbitrage_usdt.yml
```

### `funding_arb_preflight`

Private safety command. It reads account state but does not submit orders unless
a future explicit `--submit-test-order` flag is added.

Responsibilities per enabled exchange:

- verify API keys can be loaded from environment;
- verify futures balance read;
- verify position read;
- verify open-order read;
- verify trade-fee read for sample selected symbols;
- verify server time read;
- verify symbol metadata, quantity step, tick size, minimum quantity, and minimum
  notional;
- verify no existing position or open order exists for selected candidates;
- report whether the exchange is live-order capable in this repository.

Current repository status:

- Binance has a private `Exchange` implementation wired in existing preflight
  patterns.
- Bitget and Gate have public market adapters, but their private futures
  trading, position, order, fee, and funding-ledger paths must be implemented
  and verified before live mode.
- OKX is intentionally outside the initial scope for this strategy.

Example command:

```bash
cargo run --bin funding_arb_preflight -- --config config/funding_rate_arbitrage_usdt.yml --private
```

### `funding_arb_live`

Timed runner. It may submit live orders only when all safety gates allow it.

Responsibilities:

- synchronize against exchange server time;
- wake up before each configured funding settlement;
- run a fresh scan at minute 55 or a configurable scan offset;
- select one symbol per exchange;
- place all selected market open orders concurrently before settlement;
- verify fill and actual opened quantity;
- wait until after settlement;
- place all selected close orders concurrently after settlement using actual
  position quantity;
- reconcile fills, funding ledger, fees, and final position;
- calculate PnL;
- send WeCom notification;
- persist execution records.

Example command:

```bash
cargo run --bin funding_arb_live -- --config config/funding_rate_arbitrage_usdt.yml --mode live-small
```

## Configuration Contract

Proposed config shape:

```yaml
mode: observe

universe:
  enabled_exchanges:
    - binance
    - bitget
    - gate
  quote_asset: USDT
  contract_type: perpetual
  symbol_allowlist: []
  symbol_blocklist: []

selection:
  per_exchange_limit: 1
  max_candidates_per_exchange: 500
  min_funding_rate: -0.005
  require_next_funding_time: true
  max_funding_snapshot_age_ms: 5000
  min_seconds_to_settlement_at_scan: 180
  max_seconds_to_settlement_at_scan: 420
  min_24h_quote_volume_usdt: 10000000

timing:
  scan_minute: 55
  open_before_settlement_ms: 1000
  close_after_settlement_ms: 1000
  order_timeout_ms: 3000
  clock_skew_warn_ms: 250

sizing:
  target_notional_usdt: 10.0
  max_notional_usdt_per_exchange: 10.0
  leverage: 1

execution:
  dry_run: true
  order_type: market
  position_mode: hedge
  open_side_when_negative_funding: buy
  open_position_side_when_negative_funding: long
  close_side_when_negative_funding: sell
  close_position_side_when_negative_funding: long
  close_reduce_only_policy: venue_supported
  submit_open_orders_concurrently: true
  submit_close_orders_concurrently: true
  reject_if_existing_position: true
  reject_if_open_orders_exist: true
  max_slippage_pct: 0.003
  max_retries: 1

accounting:
  pnl_quote_asset: USDT
  wait_for_funding_ledger_secs: 90
  fallback_to_balance_delta: true
  persist_dir: logs/funding_rate_arbitrage

notifications:
  enabled: true
  wecom_webhook_env: FUNDING_ARB_WECOM_WEBHOOK_URL
  notify_on_open: true
  notify_on_close: true
  notify_on_error: true
```

Do not store webhook secrets in committed config files. Load the WeCom webhook
from `FUNDING_ARB_WECOM_WEBHOOK_URL`.

Runtime setup:

```bash
export FUNDING_ARB_WECOM_WEBHOOK_URL='https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=...'
```

## Exchange Selection Rules

Selection is per exchange, not global.

For each enabled exchange:

1. Load all tradeable USDT perpetual instruments.
2. Load funding snapshots for all eligible symbols.
3. Discard snapshots with missing `next_funding_time` when
   `require_next_funding_time = true`.
4. Discard stale funding snapshots.
5. Discard symbols on `symbol_blocklist`.
6. If `symbol_allowlist` is non-empty, keep only allowlisted symbols.
7. Discard symbols that fail volume, status, precision, or minimum notional
   checks.
8. Sort by `funding_rate` ascending.
9. Select the first candidate only.
10. Execute only if `funding_rate <= min_funding_rate`.

If an exchange has no qualifying candidate, it should produce a skip record with
the top rejected reason instead of failing the whole run.

## Timing Model

The initial requirement says to scan at each hour's minute 55. Implement this as
configuration, but execution should be based on each candidate's
`next_funding_time`, not hardcoded wall-clock assumptions.

Recommended timeline for an hourly funding event:

```text
T - 5m       scan exchange funding snapshots
T - 60s      refresh selected candidates and reject changed rates
T - 10s      verify account state, open orders, positions, and server time
T - 1s       submit selected market open orders concurrently
T + 1s       submit selected close orders concurrently
T + 1s..90s  reconcile fills, fees, funding ledger, and position flatness
T + 90s      send final notification, including any unsettled ledger caveat
```

`open_before_settlement_ms = 1000` and `close_after_settlement_ms = 1000` should
be configurable. For first live tests, consider using 3000-5000 ms before
settlement if exchange latency or API rate limits are not yet measured.

## Execution State Machine

Use an explicit state machine per exchange-selected candidate.

```text
Selected
  -> OpenSubmitted
  -> OpenFilled
  -> FundingWindowPassed
  -> CloseSubmitted
  -> Closed
  -> AccountingComplete
  -> Notified
```

Failure states:

```text
RejectedBeforeOpen
OpenSubmitFailed
OpenNotFilled
CloseSubmitFailed
CloseNotFilled
PositionNotFlat
AccountingTimedOut
NotificationFailed
```

State records should include:

- run id;
- exchange;
- canonical symbol;
- exchange symbol;
- selected funding rate;
- predicted funding rate if available;
- next funding time;
- open target notional;
- open order id;
- open client order id;
- open side;
- open requested quantity;
- open filled quantity;
- open average price;
- close order id;
- close client order id;
- close filled quantity;
- close average price;
- fees;
- funding ledger amount;
- realized trading PnL;
- total PnL;
- final position quantity;
- timestamps for every transition.

The runner should execute candidates as a batch:

```text
BatchSelected
  -> OpenBatchSubmitted
  -> OpenBatchSettled
  -> CloseBatchSubmitted
  -> CloseBatchSettled
  -> BatchAccountingComplete
  -> BatchSummaryNotified
```

Within `OpenBatchSubmitted`, submit one async task per selected exchange. Within
`CloseBatchSubmitted`, submit one async task per exchange that has a confirmed
open fill. One exchange failure must not block the close path for another
exchange.

## Order Construction

All enabled accounts must be in hedge/dual-position mode before live orders.
The strategy trades the long side when funding is negative.

Open order when funding is negative:

```text
side = Buy
type = Market
position_side = Long
reduce_only = false
quantity = round_down(target_notional_usdt / mark_price, quantity_step)
```

Close order:

```text
side = Sell
type = Market
position_side = Long
reduce_only = true where the venue supports it in hedge mode
quantity = actual_open_filled_quantity or reconciled position quantity
```

Venue-specific close rules:

- Binance USD-M hedge mode should use `positionSide=LONG` for the close order;
  do not blindly send `reduceOnly=true` if the exchange rejects that combination.
- Bitget and Gate adapters must expose the correct hedge-mode long close
  parameters before live mode is enabled.

Important rules:

- never close by theoretical target quantity if the open order partially filled;
- never open if an existing long or short position exists for that
  exchange-symbol unless config explicitly allows it;
- never open if there are existing open orders for the symbol;
- reject if rounded quantity is below exchange minimum quantity or minimum
  notional;
- set leverage before live order only if the exchange implementation supports
  reliable confirmation;
- reject live mode if hedge/dual-position mode cannot be confirmed for that
  exchange.

## PnL Accounting

The final notification must report this run's arbitrage result after close.

Preferred PnL components:

```text
open_notional = open_avg_price * open_filled_qty
close_notional = close_avg_price * close_filled_qty

trading_pnl_long = close_notional - open_notional
fee_pnl = -(open_fee + close_fee)
funding_pnl = funding ledger amount for this symbol/run

total_pnl = trading_pnl_long + fee_pnl + funding_pnl
```

If the exchange funding ledger is delayed:

1. wait up to `wait_for_funding_ledger_secs`;
2. if ledger is still missing and `fallback_to_balance_delta = true`, report a
   provisional balance-delta estimate;
3. mark the notification as `provisional`;
4. write a follow-up record when the final ledger arrives.

The implementation should attempt to correlate funding ledger rows by:

- exchange;
- symbol;
- funding settlement time;
- account;
- amount asset;
- timestamp window around `next_funding_time`.

If the core `Exchange` trait does not expose funding income history for a venue,
add a venue-specific accounting adapter rather than guessing from price PnL.

## WeCom Notification

Use the existing WeCom markdown JSON style:

```json
{
  "msgtype": "markdown",
  "markdown": {
    "content": "..."
  }
}
```

The webhook URL must be read from environment variable
`FUNDING_ARB_WECOM_WEBHOOK_URL`.

Send one final notification per exchange execution after close and accounting.
Also send error notifications when a live run opens a position but fails to
close or fails reconciliation.

Recommended final markdown:

```text
### 资金费率套利完成
> 状态：成功/失败/临时估算
> Run ID：...
> 交易所：binance
> 交易对：BTC/USDT
> 持仓模式：双向持仓 / LONG
> 资金费率：-0.5300%
> 结算时间：2026-05-30 08:00:00 UTC
> 开仓：Buy Market LONG qty=... avg=...
> 平仓：Sell Market LONG close qty=... avg=...
> 资金费收入：+0.0512 USDT
> 交易价差盈亏：-0.0123 USDT
> 手续费：-0.0100 USDT
> 总盈亏：+0.0289 USDT
> 最终仓位：0
```

For aggregate visibility, optionally send one summary message after all
exchange candidates finish:

```text
### 资金费率套利批次汇总
> Run ID：...
> 成功：3
> 跳过：0
> 失败：0
> 合计盈亏：+0.1034 USDT
```

Notification failure must not hide trading risk. If notification fails, log the
error and persist the final accounting record locally.

## Persistence

Write JSONL records under:

```text
logs/funding_rate_arbitrage/
```

Recommended files:

```text
selection_YYYY-MM-DD.jsonl
orders_YYYY-MM-DD.jsonl
accounting_YYYY-MM-DD.jsonl
errors_YYYY-MM-DD.jsonl
```

Every live order should have a deterministic client order id:

```text
funding_arb_{exchange}_{symbol}_{settlement_ts}_{open|close}
```

This supports idempotency after process restart.

## Risk Controls

Hard blocks before open:

- mode is not live-capable;
- `execution.dry_run = true`;
- webhook env missing when notification is required;
- exchange private adapter is not verified;
- hedge/dual-position mode is not confirmed;
- server time cannot be read;
- clock skew is above `clock_skew_warn_ms` in live mode;
- selected funding rate is no longer below threshold on final refresh;
- `next_funding_time` changed unexpectedly;
- current time is outside the configured open window;
- existing position exists;
- open orders exist;
- available balance is insufficient;
- rounded order quantity violates symbol rules;
- orderbook top levels imply slippage above `max_slippage_pct`;
- funding snapshot is stale.

Emergency behavior:

- if open fills but close fails, retry once;
- if retry fails, send critical WeCom notification and keep reconciling;
- never submit additional opens while any exchange has unresolved state;
- if final position is non-zero, persist `PositionNotFlat` and alert.

## Public Adapter Requirements

The existing `MarketDataAdapter::load_funding` should be reused where possible.
For each enabled exchange, the funding adapter must provide:

- `exchange`;
- `canonical_symbol`;
- `exchange_symbol`;
- current or predicted funding rate;
- mark price when available;
- index price when available;
- `next_funding_time`;
- receive timestamp.

If an exchange API provides both current and predicted funding, selection should
prefer the value that applies to the upcoming settlement. Store the other value
for reporting.

## Private Adapter Requirements

The live runner needs private functionality per exchange:

- futures balance;
- positions;
- open orders;
- market order submit;
- hedge/dual-position mode readback;
- hedge-mode long open and long close order parameters;
- reduce-only or close-position semantics when supported by the venue;
- order status;
- my trades or fills;
- trade fee;
- server time;
- funding income history or account ledger.

Initial implementation targets are Binance, Bitget, and Gate. Binance can reuse
the existing private `Exchange` path as the first live target. Bitget and Gate
must add verified private futures implementations before they can move beyond
observe/preflight mode.

## Test Plan

Unit tests:

- selection chooses exactly one lowest-rate candidate per exchange;
- candidates above threshold are skipped;
- stale funding snapshots are skipped;
- missing `next_funding_time` is skipped when required;
- blocklist and allowlist precedence;
- quantity rounding respects minimum quantity and minimum notional;
- hedge-mode open and close parameters are built per venue;
- async batch execution submits one open task per exchange and one close task
  per opened exchange;
- accounting handles full fill, partial open fill, missing funding ledger, and
  negative trading PnL;
- schedule calculates scan/open/close times from `next_funding_time`.

Integration tests with mock exchange:

- dry-run scan produces selected candidates and no orders;
- live-small happy path: open fill, close fill, ledger found, notification sent;
- open filled but close initially fails then retry succeeds;
- open partial fill closes only filled quantity;
- funding ledger timeout produces provisional PnL;
- existing position blocks open.

Manual validation:

```bash
cargo fmt
cargo check --bin funding_arb_observe --bin funding_arb_preflight --bin funding_arb_live
cargo test funding_rate_arbitrage
cargo run --bin funding_arb_observe -- --config config/funding_rate_arbitrage_usdt.yml
cargo run --bin funding_arb_preflight -- --config config/funding_rate_arbitrage_usdt.yml --private
```

Do not run live mode until public observe and private preflight pass for the
target exchange.

## Rollout Plan

1. Implement config, scanner, selection, schedule, and observer.
2. Add mock tests for per-exchange one-symbol selection across Binance, Bitget,
   and Gate.
3. Add preflight with Binance private reads and explicit Bitget/Gate private
   capability failures until those adapters are implemented.
4. Add dry-run live runner that writes state records and sends test
   notifications without orders.
5. Add async batch submission for open and close phases.
6. Add Binance minimum-size live execution in hedge/dual-position mode.
7. Add Bitget private futures trading, position, fee, and ledger support.
8. Add Gate private futures trading, position, fee, and ledger support.
9. Add funding-ledger reconciliation for Binance, Bitget, and Gate.
10. Run observe mode for several funding cycles.
11. Run live-small on Binance only.
12. Enable Bitget and Gate one at a time after private preflight and minimum
    live tests pass.
13. Enable one symbol per exchange after close/accounting/notification records
    are verified.

## Open Implementation Questions

- Whether the first live version should open exactly one second before
  settlement or use a wider configurable lead time after latency measurement.
- Whether funding ledger APIs should be added to the core `Exchange` trait or
  implemented as separate strategy accounting adapters.
- The exact hedge-mode close parameters for Bitget and Gate after their private
  adapters are implemented.
