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
    pub block_all_opens: bool,
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
        // 净敞口阈值使用“配置阈值 vs 20x权益”中的较大者，
        // 以满足资金规模放大时的对冲滚动需求。
        let dynamic_net_limit = (equity * 20.0).max(0.0);
        let max_net_notional = limits.max_net_notional.max(dynamic_net_limit);

        let mut flags = RiskFlags::default();
        if net_notional > max_net_notional {
            flags.block_open_long = true;
        }
        if net_notional < -max_net_notional {
            flags.block_open_short = true;
        }
        if limits.max_total_notional > 0.0 && total_notional > limits.max_total_notional {
            // 对冲网格的 total_notional 可能长期高于配置值，因为双边仓位本身会放大总名义。
            // 不能因此禁止任一侧开仓，否则启动和滚动时会缺少开仓网格。风险扩张由净敞口
            // max_net_notional 控制；极端保证金风险仍由 margin_ratio 进入 only_close。
        }
        if margin_ratio > limits.margin_ratio_limit {
            flags.block_all_opens = true;
            flags.only_close = true;
        }
        if funding_rate.abs() > limits.funding_rate_limit
            || expected_funding_cost.abs() > limits.funding_cost_limit
        {
            flags.block_all_opens = true;
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
        !self.flags.block_all_opens && !self.flags.block_open_long
    }

    pub fn allow_open_short(&self) -> bool {
        !self.flags.block_all_opens && !self.flags.block_open_short
    }
}
