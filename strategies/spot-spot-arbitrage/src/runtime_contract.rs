use chrono::{DateTime, Utc};
use rustcta_strategy_sdk::{
    AccountEvent, ExecutionEvent, ExecutionOrderCommand, MarketDataEvent, OrderSide,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    calculate_spread, depth_notional, fee_model_from_strategy_config, market_data_subscriptions,
    EventDrivenSpreadEngine, EventDrivenSpreadEngineConfig, SpotBookEvent, SpotExecutionMode,
    SpotFeeLookup, SpotFeeModel, SpotLiveOrderPlan, SpotSpotTakerArbitrageConfig,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpotRuntimeContractSnapshot {
    pub readiness: SpotRuntimeReadinessSnapshot,
    pub market_data: SpotRuntimeMarketDataSnapshot,
    pub opportunity: Option<SpotOpportunityRuntimeSnapshot>,
    pub order_plans: Vec<SpotOrderPlanSnapshot>,
    pub inventory: SpotInventorySnapshot,
    pub fees: SpotFeeSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpotRuntimeReadinessSnapshot {
    pub config_valid: bool,
    pub market_data_ready: bool,
    pub subscriptions_ready: bool,
    pub inventory_ready: bool,
    pub fees_ready: bool,
    pub execution_plan_ready: bool,
    pub live_submission_allowed: bool,
    pub blocking_reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpotRuntimeMarketDataSnapshot {
    pub mode: String,
    pub subscriptions: Vec<rustcta_strategy_sdk::MarketDataSubscription>,
    pub latest_update: Option<SpotBookRuntimeSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpotBookRuntimeSnapshot {
    pub exchange: String,
    pub symbol: String,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpotOpportunityRuntimeSnapshot {
    pub symbol: String,
    pub observed_at: DateTime<Utc>,
    pub updated_exchange: String,
    pub candidate_pairs: Vec<SpotOpportunityPairSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpotOpportunityPairSnapshot {
    pub buy_exchange: String,
    pub sell_exchange: String,
    pub buy_price: Option<f64>,
    pub sell_price: Option<f64>,
    pub raw_spread_bps: Option<f64>,
    pub buy_fee_bps: Option<f64>,
    pub sell_fee_bps: Option<f64>,
    pub estimated_cost_bps: Option<f64>,
    pub estimated_net_spread_bps: Option<f64>,
    pub executable_notional: Option<f64>,
    pub quantity: Option<f64>,
    pub accepted: bool,
    pub rejection_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpotOrderPlanSnapshot {
    pub exchange: String,
    pub symbol: String,
    pub side: String,
    pub quantity: String,
    pub notional: Option<f64>,
    pub intent: String,
    pub client_order_id: Option<String>,
    pub idempotency_key: Option<String>,
    pub live_submission_allowed: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SpotInventorySnapshot {
    pub balances: Vec<SpotInventoryBalanceSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpotInventoryBalanceSnapshot {
    pub exchange: String,
    pub asset: String,
    pub total: f64,
    pub available: f64,
    pub locked: f64,
    pub locally_reserved: f64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SpotFeeSnapshot {
    pub venue_rates: Vec<SpotFeeLookup>,
}

#[derive(Debug, Clone)]
pub struct SpotRuntimeContractState {
    config: Option<SpotSpotTakerArbitrageConfig>,
    spread_engine: EventDrivenSpreadEngine,
    market_data: SpotRuntimeMarketDataSnapshot,
    opportunity: Option<SpotOpportunityRuntimeSnapshot>,
    order_plans: Vec<SpotOrderPlanSnapshot>,
    inventory: SpotInventorySnapshot,
    fees: SpotFeeSnapshot,
}

impl Default for SpotRuntimeContractState {
    fn default() -> Self {
        Self {
            config: None,
            spread_engine: EventDrivenSpreadEngine::default(),
            market_data: SpotRuntimeMarketDataSnapshot {
                mode: "unconfigured".to_string(),
                subscriptions: Vec::new(),
                latest_update: None,
            },
            opportunity: None,
            order_plans: Vec::new(),
            inventory: SpotInventorySnapshot::default(),
            fees: SpotFeeSnapshot::default(),
        }
    }
}

impl SpotRuntimeContractState {
    pub fn start(&mut self, config: &SpotSpotTakerArbitrageConfig) {
        self.config = Some(config.clone());
        self.spread_engine = EventDrivenSpreadEngine::new(EventDrivenSpreadEngineConfig {
            exchanges: config.exchanges.clone(),
            symbols: config.symbols.clone(),
        });
        self.market_data = SpotRuntimeMarketDataSnapshot {
            mode: format!("{:?}", config.market_data_mode),
            subscriptions: market_data_subscriptions(config),
            latest_update: None,
        };
        self.opportunity = None;
        self.order_plans.clear();
        self.inventory = initial_inventory_snapshot(config);
        self.fees = fee_snapshot(config);
    }

    pub fn apply_market_data(&mut self, event: &MarketDataEvent) {
        let Some(book_event) = book_event_from_payload(event) else {
            return;
        };
        self.market_data.latest_update = Some(SpotBookRuntimeSnapshot {
            exchange: book_event.exchange.clone(),
            symbol: book_event.symbol.clone(),
            best_bid: book_event.best_bid,
            best_ask: book_event.best_ask,
            observed_at: book_event.local_timestamp,
        });
        let result = self.spread_engine.on_book_event(book_event);
        if !result.recomputed_pairs.is_empty() {
            let fee_model = self.config.as_ref().map(fee_model_from_strategy_config);
            self.opportunity = Some(SpotOpportunityRuntimeSnapshot {
                symbol: result.symbol.clone(),
                observed_at: event.received_at,
                updated_exchange: result.updated_exchange.clone(),
                candidate_pairs: result
                    .recomputed_pairs
                    .into_iter()
                    .map(|pair| {
                        self.opportunity_pair_snapshot(&result.symbol, pair, fee_model.as_ref())
                    })
                    .collect(),
            });
        }
    }

    pub fn apply_account(&mut self, event: &AccountEvent) {
        if let Some(balances) = balances_from_payload(&event.payload) {
            self.inventory.balances = balances;
        }
    }

    pub fn apply_execution(&mut self, event: &ExecutionEvent) {
        if let Some(plans) = order_plans_from_payload(&event.payload) {
            self.order_plans = plans;
        }
    }

    pub fn snapshot(&self) -> SpotRuntimeContractSnapshot {
        SpotRuntimeContractSnapshot {
            readiness: self.readiness(),
            market_data: self.market_data.clone(),
            opportunity: self.opportunity.clone(),
            order_plans: self.order_plans.clone(),
            inventory: self.inventory.clone(),
            fees: self.fees.clone(),
        }
    }

    fn readiness(&self) -> SpotRuntimeReadinessSnapshot {
        let mut blocking_reasons = Vec::new();
        let Some(config) = self.config.as_ref() else {
            return SpotRuntimeReadinessSnapshot {
                config_valid: false,
                market_data_ready: false,
                subscriptions_ready: false,
                inventory_ready: false,
                fees_ready: false,
                execution_plan_ready: false,
                live_submission_allowed: false,
                blocking_reasons: vec!["runtime has not been started".to_string()],
            };
        };

        let config_valid = config.validate_safe_mode().is_ok();
        if !config_valid {
            blocking_reasons.push("config failed safe-mode validation".to_string());
        }
        let subscriptions_ready = !self.market_data.subscriptions.is_empty()
            || config.market_data_mode != crate::MarketDataMode::WebsocketCache;
        if !subscriptions_ready {
            blocking_reasons.push("market data subscriptions are empty".to_string());
        }
        let market_data_ready = self.market_data.latest_update.is_some()
            || config.market_data_mode != crate::MarketDataMode::WebsocketCache;
        if !market_data_ready {
            blocking_reasons.push("no market data update has been observed".to_string());
        }
        let inventory_ready = !self.inventory.balances.is_empty();
        if !inventory_ready {
            blocking_reasons.push("inventory snapshot is empty".to_string());
        }
        let fees_ready = !self.fees.venue_rates.is_empty();
        let execution_plan_ready = !self.order_plans.is_empty()
            || !SpotExecutionMode::from_config(config).builds_live_order_plans();
        if !execution_plan_ready {
            blocking_reasons.push("no live/live-dry-run order plan has been observed".to_string());
        }
        let live_submission_allowed = SpotExecutionMode::from_config(config)
            .may_submit_live_orders()
            && config.live_trading_enabled
            && !config.dry_run
            && config.kill_switch.allow_live_orders
            && config.small_live_gate.enabled
            && config.small_live_gate.explicit_live_confirmation;

        SpotRuntimeReadinessSnapshot {
            config_valid,
            market_data_ready,
            subscriptions_ready,
            inventory_ready,
            fees_ready,
            execution_plan_ready,
            live_submission_allowed,
            blocking_reasons,
        }
    }

    fn opportunity_pair_snapshot(
        &self,
        symbol: &str,
        pair: crate::DirectedVenuePair,
        fee_model: Option<&SpotFeeModel>,
    ) -> SpotOpportunityPairSnapshot {
        let Some(config) = self.config.as_ref() else {
            return missing_opportunity_pair(pair, "runtime config is not available");
        };
        let Some(fee_model) = fee_model else {
            return missing_opportunity_pair(pair, "fee model is not available");
        };
        let Some(buy_book) = self.spread_engine.book(&pair.buy_exchange, symbol) else {
            return missing_opportunity_pair(pair, "buy book is not available");
        };
        let Some(sell_book) = self.spread_engine.book(&pair.sell_exchange, symbol) else {
            return missing_opportunity_pair(pair, "sell book is not available");
        };
        let Some(buy_price) = buy_book.best_ask else {
            return missing_opportunity_pair(pair, "buy best ask is not available");
        };
        let Some(sell_price) = sell_book.best_bid else {
            return missing_opportunity_pair(pair, "sell best bid is not available");
        };

        let buy_depth = depth_notional(&buy_book.asks, config.max_notional_per_trade);
        let sell_depth = depth_notional(&sell_book.bids, config.max_notional_per_trade);
        let executable_notional = buy_depth.min(sell_depth).min(config.max_notional_per_trade);
        let fees = fee_model.estimate_pair_fees(&pair.buy_exchange, &pair.sell_exchange);
        let spread = calculate_spread(
            buy_price,
            sell_price,
            fees.buy_taker_bps,
            fees.sell_taker_bps,
            config.slippage_bps,
            config.safety_buffer_bps,
        );
        let rejection_reason = opportunity_rejection_reason(
            executable_notional,
            config.min_depth_notional,
            config.min_notional_per_trade,
            spread.net_spread_bps,
            config.min_net_spread_bps,
        );

        SpotOpportunityPairSnapshot {
            buy_exchange: pair.buy_exchange,
            sell_exchange: pair.sell_exchange,
            buy_price: Some(buy_price),
            sell_price: Some(sell_price),
            raw_spread_bps: Some(spread.raw_spread_bps),
            buy_fee_bps: Some(fees.buy_taker_bps),
            sell_fee_bps: Some(fees.sell_taker_bps),
            estimated_cost_bps: Some(spread.estimated_cost_bps),
            estimated_net_spread_bps: Some(spread.net_spread_bps),
            executable_notional: Some(executable_notional),
            quantity: (buy_price > 0.0).then_some(executable_notional / buy_price),
            accepted: rejection_reason.is_none(),
            rejection_reason,
        }
    }
}

fn missing_opportunity_pair(
    pair: crate::DirectedVenuePair,
    reason: impl Into<String>,
) -> SpotOpportunityPairSnapshot {
    SpotOpportunityPairSnapshot {
        buy_exchange: pair.buy_exchange,
        sell_exchange: pair.sell_exchange,
        buy_price: None,
        sell_price: None,
        raw_spread_bps: None,
        buy_fee_bps: None,
        sell_fee_bps: None,
        estimated_cost_bps: None,
        estimated_net_spread_bps: None,
        executable_notional: None,
        quantity: None,
        accepted: false,
        rejection_reason: Some(reason.into()),
    }
}

fn opportunity_rejection_reason(
    executable_notional: f64,
    min_depth_notional: f64,
    min_trade_notional: f64,
    estimated_net_spread_bps: f64,
    min_net_spread_bps: f64,
) -> Option<String> {
    if executable_notional < min_depth_notional {
        return Some("insufficient_depth".to_string());
    }
    if executable_notional < min_trade_notional {
        return Some("min_notional".to_string());
    }
    if estimated_net_spread_bps < min_net_spread_bps {
        return Some("net_spread_below_threshold".to_string());
    }
    None
}

fn initial_inventory_snapshot(config: &SpotSpotTakerArbitrageConfig) -> SpotInventorySnapshot {
    let mut balances = config
        .initial_balances
        .iter()
        .flat_map(|(exchange, assets)| {
            assets
                .iter()
                .map(|(asset, amount)| SpotInventoryBalanceSnapshot {
                    exchange: normalize_exchange(exchange),
                    asset: asset.trim().to_ascii_uppercase(),
                    total: *amount,
                    available: *amount,
                    locked: 0.0,
                    locally_reserved: 0.0,
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    balances.sort_by(|left, right| {
        (left.exchange.as_str(), left.asset.as_str())
            .cmp(&(right.exchange.as_str(), right.asset.as_str()))
    });
    SpotInventorySnapshot { balances }
}

fn fee_snapshot(config: &SpotSpotTakerArbitrageConfig) -> SpotFeeSnapshot {
    let model = fee_model_from_strategy_config(config);
    let mut venue_rates = config
        .exchanges
        .iter()
        .map(|exchange| model.lookup(exchange))
        .collect::<Vec<_>>();
    venue_rates.sort_by(|left, right| left.exchange.cmp(&right.exchange));
    venue_rates.dedup_by(|left, right| left.exchange == right.exchange);
    SpotFeeSnapshot { venue_rates }
}

fn book_event_from_payload(event: &MarketDataEvent) -> Option<SpotBookEvent> {
    let best_bid = number_field(&event.payload, "best_bid")
        .or_else(|| number_field(&event.payload, "bid"))
        .or_else(|| number_field(&event.payload, "bid_price"))?;
    let best_ask = number_field(&event.payload, "best_ask")
        .or_else(|| number_field(&event.payload, "ask"))
        .or_else(|| number_field(&event.payload, "ask_price"))?;
    let quantity = number_field(&event.payload, "quantity")
        .or_else(|| number_field(&event.payload, "size"))
        .or_else(|| number_field(&event.payload, "depth_quantity"))
        .unwrap_or(1.0);
    Some(SpotBookEvent::snapshot(
        &event.exchange_id,
        &event.symbol,
        best_bid,
        best_ask,
        quantity,
        event.received_at,
    ))
}

fn balances_from_payload(payload: &Value) -> Option<Vec<SpotInventoryBalanceSnapshot>> {
    let balances = payload.get("balances")?.as_array()?;
    let mut parsed = balances
        .iter()
        .filter_map(|balance| {
            let exchange = string_field(balance, "exchange")
                .or_else(|| string_field(balance, "exchange_id"))?;
            let asset =
                string_field(balance, "asset").or_else(|| string_field(balance, "currency"))?;
            let total = number_field(balance, "total").unwrap_or_else(|| {
                number_field(balance, "available").unwrap_or_default()
                    + number_field(balance, "locked").unwrap_or_default()
            });
            Some(SpotInventoryBalanceSnapshot {
                exchange: normalize_exchange(&exchange),
                asset: asset.trim().to_ascii_uppercase(),
                total,
                available: number_field(balance, "available").unwrap_or(total),
                locked: number_field(balance, "locked").unwrap_or_default(),
                locally_reserved: number_field(balance, "locally_reserved").unwrap_or_default(),
            })
        })
        .collect::<Vec<_>>();
    parsed.sort_by(|left, right| {
        (left.exchange.as_str(), left.asset.as_str())
            .cmp(&(right.exchange.as_str(), right.asset.as_str()))
    });
    Some(parsed)
}

fn order_plans_from_payload(payload: &Value) -> Option<Vec<SpotOrderPlanSnapshot>> {
    if let Some(plans) = payload.get("order_plans").and_then(Value::as_array) {
        return Some(
            plans
                .iter()
                .filter_map(order_plan_from_value)
                .collect::<Vec<_>>(),
        );
    }
    if let Some(plan) = payload.get("order_plan").and_then(order_plan_from_value) {
        return Some(vec![plan]);
    }
    if let Ok(command) = serde_json::from_value::<ExecutionOrderCommand>(payload.clone()) {
        return Some(vec![order_plan_from_command(&command)]);
    }
    None
}

fn order_plan_from_value(value: &Value) -> Option<SpotOrderPlanSnapshot> {
    if let Ok(plan) = serde_json::from_value::<SpotLiveOrderPlan>(value.clone()) {
        return plan.command.as_ref().map(order_plan_from_command);
    }
    if let Ok(command) = serde_json::from_value::<ExecutionOrderCommand>(value.clone()) {
        return Some(order_plan_from_command(&command));
    }
    let exchange =
        string_field(value, "exchange").or_else(|| string_field(value, "exchange_id"))?;
    let symbol = string_field(value, "symbol")?;
    Some(SpotOrderPlanSnapshot {
        exchange: normalize_exchange(&exchange),
        symbol: normalize_symbol(&symbol),
        side: string_field(value, "side").unwrap_or_else(|| "unknown".to_string()),
        quantity: string_or_number_field(value, "quantity").unwrap_or_else(|| "0".to_string()),
        notional: number_field(value, "notional").or_else(|| number_field(value, "order_notional")),
        intent: string_field(value, "intent").unwrap_or_else(|| "unspecified".to_string()),
        client_order_id: string_field(value, "client_order_id"),
        idempotency_key: string_field(value, "idempotency_key"),
        live_submission_allowed: value
            .get("live_submission_allowed")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
}

fn order_plan_from_command(command: &ExecutionOrderCommand) -> SpotOrderPlanSnapshot {
    SpotOrderPlanSnapshot {
        exchange: normalize_exchange(&command.exchange_id),
        symbol: normalize_symbol(&command.symbol),
        side: match command.side {
            OrderSide::Buy => "buy".to_string(),
            OrderSide::Sell => "sell".to_string(),
        },
        quantity: command.quantity.clone(),
        notional: command
            .metadata
            .get("order_notional")
            .and_then(Value::as_f64),
        intent: command
            .metadata
            .get("intent")
            .and_then(Value::as_str)
            .unwrap_or("execution_order_command")
            .to_string(),
        client_order_id: Some(command.client_order_id.clone()),
        idempotency_key: Some(command.idempotency_key.clone()),
        live_submission_allowed: true,
    }
}

fn number_field(value: &Value, key: &str) -> Option<f64> {
    value.get(key).and_then(|value| {
        value
            .as_f64()
            .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
    })
}

fn string_field(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn string_or_number_field(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(|value| {
        value.as_str().map(ToString::to_string).or_else(|| {
            value.as_f64().map(|number| {
                format!("{number:.12}")
                    .trim_end_matches('0')
                    .trim_end_matches('.')
                    .to_string()
            })
        })
    })
}

fn normalize_exchange(exchange: &str) -> String {
    match exchange.trim().to_ascii_lowercase().as_str() {
        "gate" | "gate.io" => "gateio".to_string(),
        other => other.to_string(),
    }
}

fn normalize_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .replace(['-', '_', '/'], "")
        .to_ascii_uppercase()
}
