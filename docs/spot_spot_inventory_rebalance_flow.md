# Spot-Spot Inventory Rebalance Flow

## Purpose

This document defines how spot-to-spot arbitrage should handle inventory after
normal arbitrage trades and after one-sided execution failures.

The key rule is that inventory rebalance is not the same as forced liquidation.
A rebalance order may run only when it is one of these cases:

- it preserves profit or at least does not lose money after fees and slippage
- the loss is explicitly covered by the symbol's realized profit pool
- emergency exposure control is triggered and the loss is accounted separately

## Problem Seen In Logs

Example OBOL timeline:

```text
16:42:24 CoinEx sell filled 2496.01 @ 0.004145, paired buy did not fill
16:42:25 CoinEx inventory balance bought 656.5159436 @ 0.004146
16:42:35 GateIO inventory balance bought 1839 @ 0.004185
```

This is a short one-sided exposure: the strategy sold base first, but the buy
leg did not complete. It then bought base back in later balance orders. The
second buy at `0.004185` is above the sell price `0.004145`, so it is a loss
before fees. This may be acceptable only as emergency exposure control, not as a
normal profit-preserving rebalance.

Example POND timeline:

```text
buy leg filled, sell leg did not fill
later inventory balance sold the base at lower prices
```

This is a long one-sided exposure: the strategy bought base, but the sell leg
did not complete. Selling lower than the buy cost realizes a loss. This should
not be the default balance behavior.

## Definitions

| Term | Meaning |
| --- | --- |
| Base | The coin being arbitraged, such as VSN or LAB |
| Quote | USDT |
| Low venue | Exchange with cheaper ask price |
| High venue | Exchange with higher bid price |
| Long one-sided exposure | Buy leg filled, sell leg missing; strategy holds extra base |
| Short one-sided exposure | Sell leg filled, buy leg missing; strategy owes base inventory |
| Inventory drift | Normal result of successful arbitrage: low venue gains base and loses USDT; high venue loses base and gains USDT |
| Profit pool | Realized net profit for one symbol after fees, excluding emergency rebalance losses |
| Cycle size | The planned notional for one arbitrage cycle |

## State Model

Normal inventory state:

```text
Ready
  -> Arbitraging
  -> InventoryDrift
  -> WaitingReverseArb
  -> TransferRebalance
  -> MarketRebalance
  -> PausedNeedInventory
```

One-sided failure state:

```text
OneSidedExposure
  -> ProfitPreservingRecovery
  -> WaitingRecovery
  -> EmergencyRecovery
  -> Resolved
```

These two state machines must stay separate. Normal inventory drift should not
be handled by the same unconditional close logic as one-sided execution failure.

## Intervention Timing

Inventory logic should intervene at five points.

### Before Order Submission

The strategy checks whether both sides can complete the next arbitrage cycle.
If the high-price venue lacks base or the low-price venue lacks USDT, the
strategy should not submit a partial order. It should move the symbol to
`PausedNeedInventory` or `InventoryDrift`.

Stopped-cleaning or manually stopped pairs must not be taken over by automatic
initial-entry logic. If the pair is not explicitly active, inventory logic may
record the shortage, but it must not create new position-building orders.

### During Execution

The sell leg runs first. If the sell leg fails, the strategy stops and does not
buy. If the sell leg succeeds but the buy leg fails, the strategy records a
short one-sided exposure and starts the recovery state machine.

If a future mode ever uses buy-first execution, then a filled buy with failed
sell must record a long one-sided exposure. It must not be handled as normal
inventory drift.

### Immediately After A Successful Arbitrage

After both legs fill, the strategy records realized profit and updates venue
inventory. If the remaining inventory cannot support the configured cycle
buffer, it should mark `InventoryDrift` and stop same-direction trading until a
valid rebalance path exists.

### Periodic Scanner

A timer evaluates open one-sided exposures and inventory drift. It can run
profit-preserving recovery, reverse arbitrage, transfer tasks, or profit-covered
market rebalance. It should not run unconditional market balance orders.

### Manual Stop Or Stop-Cleaning State

If the symbol is manually stopped or in stop-cleaning state, automatic
position-building and takeover are disabled. The only allowed automatic action
is an explicitly configured close/recovery task for existing risk, and that task
must still obey the no-loss, profit-covered, or emergency rules.

## Pre-Trade Gate

Before submitting any arbitrage order, calculate the executable quantity:

```text
sell_capacity = high_base_available - high_base_reserve
buy_capacity = low_quote_available / effective_buy_price
depth_capacity = min(low_ask_depth_qty, high_bid_depth_qty)

trade_qty = min(sell_capacity, buy_capacity, depth_capacity, configured_max_qty)
trade_notional = trade_qty * low_ask_price
```

The trade is allowed only if:

- `trade_notional >= max(min_notional_buy_venue, min_notional_sell_venue)`
- high venue has enough base to sell
- low venue has enough USDT to buy
- order book timestamps are fresh
- expected net profit after fees and slippage is positive
- the trade leaves enough inventory for the configured reserve, or the strategy
  accepts that this is the last cycle before rebalancing

Execution order should prefer sell-first:

```text
1. sell base on the high venue
2. if sell fails, stop; do not buy
3. if sell succeeds, buy base on the low venue
4. if buy fails, create short one-sided exposure
```

Sell-first avoids creating extra long base inventory when the sell side was not
available.

## One-Sided Failure Recovery

### Short Exposure

Short exposure means:

```text
sell leg filled, buy leg missing
```

The strategy has received USDT and reduced base inventory. It must buy base back
only when one of these rules is true:

```text
effective_buy_price <= sold_price - safety_buffer_price
```

or:

```text
buy_cost_after_fee <= sold_proceeds_after_fee - safety_buffer_usdt
```

If neither is true, the strategy should wait unless emergency control is
triggered.

### Long Exposure

Long exposure means:

```text
buy leg filled, sell leg missing
```

The strategy has spent USDT and holds extra base. It should sell base only when:

```text
effective_sell_price >= cost_price + safety_buffer_price
```

or:

```text
sell_proceeds_after_fee >= cost_basis_after_fee + safety_buffer_usdt
```

If neither is true, the strategy should wait unless emergency control is
triggered.

### Price Formulas

```text
effective_sell_price = bid_price * (1 - sell_fee_rate - slippage_buffer_rate)
effective_buy_price = ask_price * (1 + buy_fee_rate + slippage_buffer_rate)
```

For partial fills, use weighted average prices and track the remaining exposure
quantity separately.

### Profit-Covered Recovery

If no-loss recovery is not available, the strategy may use realized profit:

```text
rebalance_loss <= max(0, symbol_profit_pool - profit_floor_usdt)
```

Recommended stricter rule:

```text
rebalance_loss <= (symbol_profit_pool - profit_floor_usdt) * max_profit_use_ratio
```

This prevents one rebalance from consuming all realized profit.

### Emergency Recovery

Emergency recovery is allowed only when exposure risk is more important than
profit preservation. Suggested triggers:

- one-sided exposure age exceeds `emergency_after_seconds`
- exposure value exceeds `emergency_max_exposure_usdt`
- adverse price movement exceeds `emergency_adverse_bps`
- exchange/account risk check requires flattening

Emergency recovery loss must be recorded separately:

```text
arbitrage_pnl
rebalance_pnl
emergency_rebalance_pnl
```

Emergency loss should not be hidden inside normal arbitrage profit.

## Normal Inventory Rebalance

Successful arbitrage naturally moves inventory:

```text
low venue:  USDT decreases, base increases
high venue: USDT increases, base decreases
```

After enough cycles, the low venue may lack USDT and the high venue may lack
base. This is not a failed trade. It is inventory drift.

The rebalance priority should be:

1. Reverse arbitrage
2. Transfer between exchanges
3. Profit-covered market rebalance
4. Pause symbol until manual inventory is added

### Reverse Arbitrage

If the price direction reverses and the trade is profitable, run the opposite
arbitrage:

```text
sell venue becomes buy venue
buy venue becomes sell venue
```

This is the best rebalance because it also earns spread.

### Transfer Rebalance

If transfers are operationally safe, move assets instead of trading:

```text
send base from low venue to high venue
send USDT from high venue to low venue
```

Transfer is preferred when withdrawal fees and delay are lower than market
rebalance cost.

### Market Rebalance

Market rebalance is allowed only when no-loss or profit-covered conditions are
met. It should be capped by:

```text
rebalance_qty <= missing_inventory_qty
rebalance_notional <= max_rebalance_notional_usdt
rebalance_loss <= available_profit_budget
```

### Pause

If none of the above is available, pause the symbol with a clear reason:

```text
PausedNeedInventory: high venue lacks base or low venue lacks USDT
```

Do not continue creating orders that are likely to fail or force a losing
rebalance.

## Sizing Policy

For user-configured target capital:

```text
target_total_notional = 20 USDT
```

This target is split across the two exchanges by role and inventory need, not as
`20 USDT per exchange`.

Per arbitrage order:

```text
min_order_notional = max(exchange_a_min_notional, exchange_b_min_notional)
```

The planned order must satisfy both venues. If one venue requires at least `3
USDT` and another requires `5 USDT`, the arbitrage order should use at least `5
USDT`.

Keep cycle buffers:

```text
required_high_base = cycle_qty * min_cycles_buffer
required_low_quote = cycle_notional * min_cycles_buffer
target_high_base = cycle_qty * target_cycles_buffer
target_low_quote = cycle_notional * target_cycles_buffer
```

If inventory falls below the required buffer, move the symbol to
`InventoryDrift` or `PausedNeedInventory` instead of forcing a market rebalance.

## LAB And VSN Application

### VSN

Current pattern:

```text
GateIO lower price, Bitget higher price
buy VSN on GateIO, sell VSN on Bitget
```

After successful cycles:

```text
GateIO USDT decreases
GateIO VSN increases
Bitget VSN decreases
Bitget USDT increases
```

If GateIO USDT is depleted, the strategy should not keep trying the same
direction. Valid choices:

- wait for reverse arbitrage
- transfer USDT from Bitget to GateIO
- transfer VSN from GateIO to Bitget
- use market rebalance only if VSN profit pool covers the cost
- pause VSN with `PausedNeedInventory`

### LAB

If Bitget LAB is nearly zero and GateIO USDT is also insufficient, LAB should
not trade even if the spread looks positive. The missing high-side base and
low-side quote must be prepared first.

Valid choices:

- manual initial inventory build
- transfer LAB to the high-price venue
- transfer USDT to the low-price venue
- wait until reverse arbitrage can repair the inventory

## Suggested Config

```yaml
inventory_rebalance:
  enabled: true
  min_cycles_buffer: 3
  target_cycles_buffer: 5
  profit_floor_usdt: 0.02
  max_profit_use_ratio: 0.5
  max_rebalance_notional_usdt: 5
  no_loss_safety_bps: 8
  slippage_buffer_bps: 5
  emergency_after_seconds: 20
  emergency_max_exposure_usdt: 15
  emergency_adverse_bps: 30
  allow_market_rebalance: true
  allow_transfer_rebalance: false
```

## Decision Pseudocode

```text
on_arbitrage_opportunity(symbol, low_venue, high_venue):
    plan = build_trade_plan(symbol, low_venue, high_venue)

    if not has_required_inventory(plan):
        mark PausedNeedInventory or InventoryDrift
        return

    if not expected_profit_positive(plan):
        return

    sell_result = sell_high_venue(plan)
    if sell_result.failed:
        return

    buy_result = buy_low_venue(plan)
    if buy_result.failed:
        create_short_exposure(sell_result)
        return

    record_successful_arbitrage(sell_result, buy_result)
    evaluate_inventory_drift(symbol)

on_one_sided_exposure(exposure):
    if exposure.kind == short:
        if buy_back_is_no_loss(exposure):
            buy_back()
            return
        if profit_pool_covers_loss(exposure):
            buy_back()
            return

    if exposure.kind == long:
        if sell_out_is_no_loss(exposure):
            sell_out()
            return
        if profit_pool_covers_loss(exposure):
            sell_out()
            return

    if emergency_triggered(exposure):
        emergency_recover_and_record_loss()
        return

    wait_for_better_price()

on_inventory_drift(symbol):
    if reverse_arbitrage_available(symbol):
        run_reverse_arbitrage()
        return

    if transfer_rebalance_available(symbol):
        create_transfer_task()
        return

    if market_rebalance_is_no_loss_or_profit_covered(symbol):
        run_market_rebalance()
        return

    pause_symbol_need_inventory()
```

## Why This Can Avoid Losing Money

The strategy can avoid losing money because it does not rebalance at any price.
For each rebalance order, it compares the effective exit or entry price after
fees and slippage with the original one-sided cost or proceeds.

For long exposure:

```text
only sell when sell proceeds >= original buy cost + buffer
```

For short exposure:

```text
only buy when buy cost <= original sell proceeds - buffer
```

For normal inventory drift, the preferred repairs are reverse arbitrage or
transfer, not market orders. If a market rebalance is needed, it must be paid by
already realized profit and recorded separately. This prevents a profitable
arbitrage cycle from being immediately erased by an unconditional balance order.
