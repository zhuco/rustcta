#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SpreadEstimate {
    pub raw_spread: f64,
    pub raw_spread_bps: f64,
    pub estimated_cost_bps: f64,
    pub net_spread_bps: f64,
}

pub fn calculate_spread(
    buy_price: f64,
    sell_price: f64,
    buy_taker_fee_bps: f64,
    sell_taker_fee_bps: f64,
    slippage_bps: f64,
    safety_buffer_bps: f64,
) -> SpreadEstimate {
    let raw_spread = sell_price - buy_price;
    let raw_spread_bps = if buy_price > 0.0 {
        raw_spread / buy_price * 10_000.0
    } else {
        0.0
    };
    let estimated_cost_bps =
        buy_taker_fee_bps + sell_taker_fee_bps + slippage_bps + safety_buffer_bps;
    SpreadEstimate {
        raw_spread,
        raw_spread_bps,
        estimated_cost_bps,
        net_spread_bps: raw_spread_bps - estimated_cost_bps,
    }
}
