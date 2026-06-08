use std::collections::BTreeMap;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{OpportunityRecord, SpotSpotTakerArbitrageConfig};
use rustcta_strategy_sdk::{ExecutionOrderCommand, OrderSide, OrderType, TimeInForce};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpotExecutionMode {
    Paper,
    LiveDryRun,
    Live,
}

impl SpotExecutionMode {
    pub fn from_config(config: &SpotSpotTakerArbitrageConfig) -> Self {
        match config.trading_mode.trim().to_ascii_lowercase().as_str() {
            "live" => Self::Live,
            "live_dry_run" => Self::LiveDryRun,
            _ => Self::Paper,
        }
    }

    pub fn builds_live_order_plans(self) -> bool {
        matches!(self, Self::LiveDryRun | Self::Live)
    }

    pub fn may_submit_live_orders(self) -> bool {
        matches!(self, Self::Live)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LiveOrderSafetyDecision {
    pub allowed: bool,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpotLiveOrderPlan {
    pub safety: LiveOrderSafetyDecision,
    pub command: Option<ExecutionOrderCommand>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpotPairedLiveOrderPlan {
    pub intent: String,
    pub buy: SpotLiveOrderPlan,
    pub sell: SpotLiveOrderPlan,
}

impl LiveOrderSafetyDecision {
    pub fn allow() -> Self {
        Self {
            allowed: true,
            reasons: Vec::new(),
        }
    }

    pub fn block(reasons: Vec<String>) -> Self {
        Self {
            allowed: false,
            reasons,
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn build_validated_live_order_command(
    config: &SpotSpotTakerArbitrageConfig,
    tenant_id: &str,
    account_id: &str,
    strategy_id: &str,
    run_id: &str,
    risk_profile_id: &str,
    exchange: &str,
    symbol: &str,
    side: OrderSide,
    quantity: f64,
    order_notional: f64,
    open_live_notional: f64,
) -> SpotLiveOrderPlan {
    let safety =
        validate_live_order_safety(config, symbol, exchange, order_notional, open_live_notional);
    if !safety.allowed {
        return SpotLiveOrderPlan {
            safety,
            command: None,
        };
    }

    let normalized_exchange = normalize_exchange(exchange);
    let normalized_symbol = normalize_symbol(symbol);
    let side_tag = match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    };
    let client_order_id = format!(
        "spot-spot-{}-{}-{}-{}",
        normalized_exchange,
        normalized_symbol,
        side_tag,
        Utc::now().timestamp_millis()
    );
    let idempotency_key = format!("{strategy_id}:{run_id}:{client_order_id}");
    let mut metadata = BTreeMap::new();
    metadata.insert("strategy_kind".to_string(), json!(crate::STRATEGY_KIND));
    metadata.insert(
        "execution_mode".to_string(),
        json!("validated_live_order_plan"),
    );
    metadata.insert("dry_run".to_string(), json!(config.dry_run));
    metadata.insert(
        "live_trading_enabled".to_string(),
        json!(config.live_trading_enabled),
    );
    metadata.insert("order_notional".to_string(), json!(order_notional));

    SpotLiveOrderPlan {
        safety,
        command: Some(ExecutionOrderCommand {
            schema_version: 1,
            tenant_id: tenant_id.to_string(),
            account_id: account_id.to_string(),
            strategy_id: strategy_id.to_string(),
            run_id: run_id.to_string(),
            client_order_id,
            idempotency_key,
            risk_profile_id: risk_profile_id.to_string(),
            requested_at: Utc::now(),
            exchange_id: normalized_exchange,
            symbol: normalized_symbol,
            side,
            order_type: OrderType::Market,
            quantity: format_quantity(quantity),
            price: None,
            time_in_force: Some(TimeInForce::ImmediateOrCancel),
            reduce_only: false,
            metadata,
        }),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn build_dual_taker_arbitrage_live_order_plan(
    config: &SpotSpotTakerArbitrageConfig,
    tenant_id: &str,
    account_id: &str,
    strategy_id: &str,
    run_id: &str,
    risk_profile_id: &str,
    opportunity: &OpportunityRecord,
    open_live_notional: f64,
) -> SpotPairedLiveOrderPlan {
    let order_notional = opportunity.executable_notional;
    let quantity = opportunity.quantity;
    let mut buy = build_validated_live_order_command(
        config,
        tenant_id,
        account_id,
        strategy_id,
        run_id,
        risk_profile_id,
        &opportunity.buy_exchange,
        &opportunity.symbol,
        OrderSide::Buy,
        quantity,
        order_notional,
        open_live_notional,
    );
    tag_live_order_plan(&mut buy, "dual_taker_arbitrage", "buy");

    let mut sell = build_validated_live_order_command(
        config,
        tenant_id,
        account_id,
        strategy_id,
        run_id,
        risk_profile_id,
        &opportunity.sell_exchange,
        &opportunity.symbol,
        OrderSide::Sell,
        quantity,
        order_notional,
        open_live_notional + order_notional,
    );
    tag_live_order_plan(&mut sell, "dual_taker_arbitrage", "sell");

    SpotPairedLiveOrderPlan {
        intent: "dual_taker_arbitrage".to_string(),
        buy,
        sell,
    }
}

pub fn validate_live_order_safety(
    config: &SpotSpotTakerArbitrageConfig,
    symbol: &str,
    exchange: &str,
    order_notional: f64,
    open_live_notional: f64,
) -> LiveOrderSafetyDecision {
    let mut reasons = Vec::new();
    if SpotExecutionMode::from_config(config) != SpotExecutionMode::Live {
        reasons.push("trading_mode must be live for live spot orders".to_string());
    }
    if config.dry_run {
        reasons.push("dry_run must be false for live spot orders".to_string());
    }
    if !config.live_trading_enabled {
        reasons.push("live_trading_enabled must be true for live spot orders".to_string());
    }
    if !config.live_dry_run.enabled || !config.live_dry_run.build_order_requests {
        reasons.push("live_dry_run must build validated order requests first".to_string());
    }
    if config.live_dry_run.submit_orders {
        reasons.push("live_dry_run.submit_orders must remain false".to_string());
    }
    if !config.kill_switch.allow_live_orders
        || config
            .kill_switch
            .initial_state
            .eq_ignore_ascii_case("triggered")
    {
        reasons.push("kill switch must explicitly allow live orders".to_string());
    }
    if !config.small_live_gate.enabled || !config.small_live_gate.explicit_live_confirmation {
        reasons.push("small_live_gate must be explicitly confirmed".to_string());
    }
    if order_notional <= 0.0
        || order_notional > config.small_live_gate.max_notional_per_order + 1e-12
    {
        reasons.push(format!(
            "order notional {:.8} exceeds small_live_gate.max_notional_per_order {:.8}",
            order_notional, config.small_live_gate.max_notional_per_order
        ));
    }
    if open_live_notional + order_notional > config.small_live_gate.max_total_notional + 1e-12 {
        reasons.push(format!(
            "open live notional {:.8} + order {:.8} exceeds small_live_gate.max_total_notional {:.8}",
            open_live_notional, order_notional, config.small_live_gate.max_total_notional
        ));
    }
    if !spot_live_execution_symbol_allowed(config, symbol) {
        reasons.push(format!(
            "symbol {} is not listed in small_live_gate.enabled_symbols",
            symbol
        ));
    }
    if !spot_live_execution_exchange_allowed(config, exchange) {
        reasons.push(format!(
            "exchange {} is not listed in small_live_gate.enabled_exchanges",
            exchange
        ));
    }
    if reasons.is_empty() {
        LiveOrderSafetyDecision::allow()
    } else {
        LiveOrderSafetyDecision::block(reasons)
    }
}

pub fn spot_live_execution_symbols(config: &SpotSpotTakerArbitrageConfig) -> Vec<String> {
    if config.small_live_gate.enabled && !config.small_live_gate.enabled_symbols.is_empty() {
        config.small_live_gate.enabled_symbols.clone()
    } else {
        config.symbols.clone()
    }
}

pub fn spot_live_execution_symbol_allowed(
    config: &SpotSpotTakerArbitrageConfig,
    symbol: &str,
) -> bool {
    let symbol = normalize_symbol(symbol);
    config.small_live_gate.enabled_symbols.is_empty()
        || config
            .small_live_gate
            .enabled_symbols
            .iter()
            .any(|allowed| normalize_symbol(allowed) == symbol)
}

pub fn spot_live_execution_exchange_allowed(
    config: &SpotSpotTakerArbitrageConfig,
    exchange: &str,
) -> bool {
    let exchange = normalize_exchange(exchange);
    config.small_live_gate.enabled_exchanges.is_empty()
        || config
            .small_live_gate
            .enabled_exchanges
            .iter()
            .any(|allowed| normalize_exchange(allowed) == exchange)
}

fn normalize_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .replace(['-', '_', '/'], "")
        .to_ascii_uppercase()
}

fn format_quantity(quantity: f64) -> String {
    format!("{quantity:.12}")
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}

fn normalize_exchange(exchange: &str) -> String {
    match exchange.trim().to_ascii_lowercase().as_str() {
        "gate" | "gate.io" => "gateio".to_string(),
        other => other.to_string(),
    }
}

fn tag_live_order_plan(plan: &mut SpotLiveOrderPlan, intent: &str, leg: &str) {
    if let Some(command) = plan.command.as_mut() {
        command.metadata.insert("intent".to_string(), json!(intent));
        command.metadata.insert("leg".to_string(), json!(leg));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn live_safety_should_keep_checked_in_small_live_flags() {
        let config: SpotSpotTakerArbitrageConfig = serde_yaml::from_str(include_str!(
            "../../../config/spot_spot_arbitrage_live_dry_run_2ex_5symbols.yml"
        ))
        .expect("checked-in config");

        let decision = validate_live_order_safety(&config, "VSN-USDT", "gate.io", 3.6, 0.0);

        assert!(decision.allowed, "{:?}", decision.reasons);
        assert_eq!(
            SpotExecutionMode::from_config(&config),
            SpotExecutionMode::Live
        );
        assert!(SpotExecutionMode::from_config(&config).builds_live_order_plans());
        assert!(SpotExecutionMode::from_config(&config).may_submit_live_orders());
    }

    #[test]
    fn live_safety_should_block_unlisted_symbol_and_notional_overrun() {
        let config: SpotSpotTakerArbitrageConfig = serde_yaml::from_str(include_str!(
            "../../../config/spot_spot_arbitrage_live_dry_run_2ex_5symbols.yml"
        ))
        .expect("checked-in config");

        let decision = validate_live_order_safety(&config, "BTCUSDT", "gateio", 20.0, 40.0);

        assert!(!decision.allowed);
        assert!(decision
            .reasons
            .iter()
            .any(|reason| reason.contains("enabled_symbols")));
        assert!(decision
            .reasons
            .iter()
            .any(|reason| reason.contains("max_notional_per_order")));
        assert!(decision
            .reasons
            .iter()
            .any(|reason| reason.contains("max_total_notional")));
    }

    #[test]
    fn live_order_plan_should_build_sdk_command_without_submitting() {
        let config: SpotSpotTakerArbitrageConfig = serde_yaml::from_str(include_str!(
            "../../../config/spot_spot_arbitrage_live_dry_run_2ex_5symbols.yml"
        ))
        .expect("checked-in config");

        let plan = build_validated_live_order_command(
            &config,
            "tenant-1",
            "account-1",
            "strategy-1",
            "run-1",
            "spot-small-live",
            "gate.io",
            "VSN-USDT",
            OrderSide::Buy,
            1.25,
            3.6,
            0.0,
        );

        assert!(plan.safety.allowed, "{:?}", plan.safety.reasons);
        let command = plan.command.expect("validated command");
        assert_eq!(command.exchange_id, "gateio");
        assert_eq!(command.symbol, "VSNUSDT");
        assert_eq!(command.side, OrderSide::Buy);
        assert_eq!(command.order_type, OrderType::Market);
        assert_eq!(command.quantity, "1.25");
        assert_eq!(
            command.metadata.get("execution_mode"),
            Some(&json!("validated_live_order_plan"))
        );
    }

    #[test]
    fn dual_taker_live_plan_should_build_paired_sdk_commands_without_submitting() {
        let config = checked_in_config();
        let opportunity = dual_taker_opportunity();

        let plan = build_dual_taker_arbitrage_live_order_plan(
            &config,
            "tenant-1",
            "account-1",
            "strategy-1",
            "run-1",
            "spot-small-live",
            &opportunity,
            0.0,
        );

        assert_eq!(plan.intent, "dual_taker_arbitrage");
        assert!(plan.buy.safety.allowed, "{:?}", plan.buy.safety.reasons);
        assert!(plan.sell.safety.allowed, "{:?}", plan.sell.safety.reasons);
        let buy = plan.buy.command.expect("buy command");
        let sell = plan.sell.command.expect("sell command");
        assert_eq!(
            buy.metadata.get("intent"),
            Some(&json!("dual_taker_arbitrage"))
        );
        assert_eq!(
            sell.metadata.get("intent"),
            Some(&json!("dual_taker_arbitrage"))
        );
        assert_eq!(buy.metadata.get("leg"), Some(&json!("buy")));
        assert_eq!(sell.metadata.get("leg"), Some(&json!("sell")));
    }

    #[test]
    fn dual_taker_live_plan_should_preserve_buy_sell_direction() {
        let config = checked_in_config();
        let opportunity = dual_taker_opportunity();

        let plan = build_dual_taker_arbitrage_live_order_plan(
            &config,
            "tenant-1",
            "account-1",
            "strategy-1",
            "run-1",
            "spot-small-live",
            &opportunity,
            0.0,
        );

        let buy = plan.buy.command.expect("buy command");
        let sell = plan.sell.command.expect("sell command");
        assert_eq!(buy.exchange_id, "gateio");
        assert_eq!(buy.side, OrderSide::Buy);
        assert_eq!(sell.exchange_id, "bitget");
        assert_eq!(sell.side, OrderSide::Sell);
        assert_eq!(buy.symbol, "VSNUSDT");
        assert_eq!(sell.symbol, "VSNUSDT");
        assert_eq!(buy.quantity, sell.quantity);
    }

    fn checked_in_config() -> SpotSpotTakerArbitrageConfig {
        serde_yaml::from_str(include_str!(
            "../../../config/spot_spot_arbitrage_live_dry_run_2ex_5symbols.yml"
        ))
        .expect("checked-in config")
    }

    fn dual_taker_opportunity() -> OpportunityRecord {
        OpportunityRecord {
            timestamp: Utc::now(),
            symbol: "VSNUSDT".to_string(),
            buy_exchange: "gate.io".to_string(),
            sell_exchange: "bitget".to_string(),
            buy_price: 2.88,
            sell_price: 2.91,
            raw_spread_bps: 104.1666666667,
            buy_fee_bps: 10.0,
            sell_fee_bps: 10.0,
            fee_source_buy: crate::SpotFeeSource::Config,
            fee_source_sell: crate::SpotFeeSource::Config,
            platform_discount_applied: false,
            estimated_fee_bps: 20.0,
            estimated_slippage_bps: 0.0,
            safety_buffer_bps: 0.0,
            estimated_net_spread_bps: 84.1666666667,
            estimated_total_fee: 0.072,
            estimated_gross_pnl: 0.0375,
            estimated_net_pnl: 0.0303,
            capital_cost_bps: 0.0,
            transfer_cost_bps: 0.0,
            transfer_delay_penalty_bps: 0.0,
            inventory_rebalance_cost_bps: 0.0,
            latency_penalty_bps: 0.0,
            effective_min_net_spread_bps: 0.0,
            estimated_slippage_cost: 0.0,
            estimated_capital_cost: 0.0,
            estimated_transfer_cost: 0.0,
            estimated_inventory_rebalance_cost: 0.0,
            estimated_latency_penalty_cost: 0.0,
            estimated_total_cost: 0.072,
            executable_notional: 3.6,
            quantity: 1.25,
            accepted: true,
            rejection_reason: None,
            rejection_detail: None,
            buy_book_age_ms: 10,
            sell_book_age_ms: 10,
            buy_book_source: crate::BookSource::Websocket,
            sell_book_source: crate::BookSource::Websocket,
            buy_latency_ms: Some(5),
            sell_latency_ms: Some(5),
        }
    }
}
