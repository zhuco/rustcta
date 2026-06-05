# MakerTaker Expected Value

MakerTaker analytics support two non-live modes:

- Maker buy plus taker sell.
- Taker buy plus maker sell.

MakerMaker is intentionally not represented as an executable mode.

A maker buy price must be passive and must not cross the ask. A maker sell price must be passive and must not cross the bid. The theoretical maker/taker spread is useful for ranking, but it is not executable profit because the maker order may not fill and the hedge price can move before fill.

The expected-value model is:

```text
fill_probability
* (
  expected_spread_at_fill_bps
  - maker_fee_bps
  - hedge_taker_fee_bps
  - expected_hedge_slippage_bps
  - expected_adverse_selection_bps
  - expected_residual_loss_bps
)
- expected_cancel_replace_cost_bps
- expected_inventory_risk_bps
```

Initial values come from config or historical statistics. When history is insufficient, conservative defaults are used and confidence remains low. These estimates must not be used to enable live maker/taker orders.
