use chrono::Utc;

use rustcta::strategies::solusdc_hedged_grid::{
    FeeConfig, FollowConfig, GridConfig, PrecisionConfig, RiskLimits, SimulationEngine,
    SimulationSeries, StrategyConfig,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = StrategyConfig {
        symbol: "SOLUSDC".to_string(),
        require_hedge_mode: true,
        price_reference: rustcta::strategies::solusdc_hedged_grid::PriceReference::Mid,
        risk_reference: rustcta::strategies::solusdc_hedged_grid::RiskReference::Mark,
        grid: GridConfig {
            levels_per_side: 3,
            grid_spacing_pct: 0.001,
            grid_spacing_abs: None,
            order_notional: 10.0,
            order_qty: None,
            strict_pairing: false,
            fill_remaining_slots_with_opens: true,
        },
        follow: FollowConfig {
            max_gap_steps: 1.0,
            follow_cooldown_ms: 800,
            max_follow_actions_per_minute: 30,
        },
        execution: rustcta::strategies::solusdc_hedged_grid::ExecutionConfig {
            cooldown_ms: 500,
            post_only: true,
            post_only_retries: 3,
        },
        precision: PrecisionConfig {
            tick_size: 0.01,
            step_size: 0.001,
            min_qty: Some(0.001),
            min_notional: Some(5.0),
            price_digits: Some(2),
            qty_digits: Some(3),
        },
        fees: FeeConfig {
            maker_fee: 0.0,
            taker_fee: 0.0004,
        },
        risk: RiskLimits {
            max_net_notional: 500.0,
            max_total_notional: 1000.0,
            margin_ratio_limit: 0.7,
            funding_rate_limit: 0.002,
            funding_cost_limit: 3.0,
        },
    };

    let mut prices = Vec::new();
    for i in 0..600 {
        let base = 100.0;
        let swing = ((i as f64) * 0.03).sin() * 1.5;
        prices.push(base + swing);
    }
    let funding_rates = vec![0.0; prices.len()];
    let series = SimulationSeries {
        prices,
        funding_rates,
        start_time: Utc::now(),
        interval_secs: 60,
    };

    let simulator = SimulationEngine::new(config, 1000.0, 5.0)?;
    let result = simulator.run(series);
    println!(
        "final_equity={:.2}, max_drawdown={:.2}%, funding={:.4}, trades={}",
        result.final_equity,
        result.max_drawdown * 100.0,
        result.total_funding,
        result.trade_count
    );
    Ok(())
}
