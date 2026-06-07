use super::config::RiskLimits;

#[derive(Debug, Clone)]
pub struct RiskState {
    pub net_notional: f64,
    pub total_notional: f64,
    pub margin_ratio: f64,
    pub funding_rate: f64,
    pub expected_funding_cost: f64,
    pub flags: RiskFlags,
}

#[derive(Debug, Clone, Default)]
pub struct RiskFlags {
    pub block_open_long: bool,
    pub block_open_short: bool,
    pub block_risk_opens: bool,
    pub only_close: bool,
}

impl RiskState {
    pub fn evaluate(
        limits: &RiskLimits,
        long_qty: f64,
        short_qty: f64,
        mark_price: f64,
        equity: f64,
        maintenance_margin: f64,
        funding_rate: f64,
    ) -> Self {
        let long_notional = long_qty * mark_price;
        let short_notional = short_qty * mark_price;
        let net_notional = long_notional - short_notional;
        let total_notional = long_notional.abs() + short_notional.abs();
        let margin_ratio = if equity > 0.0 {
            maintenance_margin / equity
        } else {
            1.0
        };
        let expected_funding_cost = funding_rate * total_notional;
        let max_net_notional = limits.max_net_notional;

        let mut flags = RiskFlags::default();
        if max_net_notional > 0.0 && net_notional > max_net_notional {
            flags.block_open_long = true;
        }
        if max_net_notional > 0.0 && net_notional < -max_net_notional {
            flags.block_open_short = true;
        }
        if limits.max_total_notional > 0.0 && total_notional > limits.max_total_notional {
            flags.block_risk_opens = true;
            flags.only_close = true;
        }
        if margin_ratio > limits.margin_ratio_limit {
            flags.block_risk_opens = true;
            flags.only_close = true;
        }
        if funding_rate.abs() > limits.funding_rate_limit
            || expected_funding_cost.abs() > limits.funding_cost_limit
        {
            flags.block_risk_opens = true;
        }

        RiskState {
            net_notional,
            total_notional,
            margin_ratio,
            funding_rate,
            expected_funding_cost,
            flags,
        }
    }

    pub fn allow_open_long(&self) -> bool {
        !self.flags.block_risk_opens && !self.flags.block_open_long
    }

    pub fn allow_open_short(&self) -> bool {
        !self.flags.block_risk_opens && !self.flags.block_open_short
    }
}
