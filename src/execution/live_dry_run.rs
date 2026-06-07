use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::exchanges::client_order_id::{generate_client_order_id, validate_client_order_id};
use crate::exchanges::spot_reservation::BalanceReservationManager;
use crate::exchanges::unified::{
    round_price_to_tick, round_quantity_to_step, validate_order_against_symbol_rule, AssetBalance,
    MarketType, OrderBookSnapshot, OrderRequest, OrderSide, OrderType, PositionSide, SymbolRule,
    TimeInForce,
};
use crate::execution::{FeeAssetMode, FeeCalculation, FeeLookupKey, FeeModel, FeeRole};
use crate::risk::DisabledRegistry;
use crate::utils::money;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveDryRunConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_true")]
    pub build_order_requests: bool,
    #[serde(default)]
    pub submit_orders: bool,
    #[serde(default = "default_max_notional_per_order")]
    pub max_notional_per_order: f64,
    #[serde(default = "default_max_total_notional")]
    pub max_total_notional: f64,
    #[serde(default = "default_true")]
    pub require_preflight_pass: bool,
    #[serde(default = "default_true")]
    pub require_fresh_books: bool,
    #[serde(default = "default_max_book_age_ms")]
    pub max_book_age_ms: u64,
    #[serde(default = "default_output_path")]
    pub output_path: String,
}

impl Default for LiveDryRunConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            build_order_requests: true,
            submit_orders: false,
            max_notional_per_order: default_max_notional_per_order(),
            max_total_notional: default_max_total_notional(),
            require_preflight_pass: true,
            require_fresh_books: true,
            max_book_age_ms: default_max_book_age_ms(),
            output_path: default_output_path(),
        }
    }
}

impl LiveDryRunConfig {
    pub fn validate(&self) -> Result<()> {
        if self.submit_orders {
            return Err(anyhow!(
                "live_dry_run.submit_orders=true is forbidden; live dry-run never submits orders"
            ));
        }
        if self.max_notional_per_order <= 0.0 || self.max_total_notional <= 0.0 {
            return Err(anyhow!("live dry-run notional limits must be positive"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveDryRunOrderPlan {
    pub plan_id: String,
    pub timestamp: DateTime<Utc>,
    #[serde(default = "default_plan_intent")]
    pub intent: String,
    pub exchange: String,
    pub market_type: MarketType,
    pub symbol: String,
    pub exchange_symbol: String,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub time_in_force: Option<TimeInForce>,
    pub price: Option<f64>,
    pub quantity: f64,
    pub notional: f64,
    pub client_order_id: Option<String>,
    pub fee_estimate: FeeCalculation,
    pub required_balance_asset: String,
    pub required_balance_amount: f64,
    pub symbol_rule_snapshot: SymbolRule,
    pub validation_result: LiveDryRunValidationResult,
    pub would_submit: bool,
    pub rejection_reason: Option<String>,
    pub order_request: OrderRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveDryRunValidationResult {
    pub passed: bool,
    pub checks: Vec<LiveDryRunCheck>,
}

impl LiveDryRunValidationResult {
    pub fn pass() -> Self {
        Self {
            passed: true,
            checks: Vec::new(),
        }
    }

    pub fn fail(reason: impl Into<String>) -> Self {
        Self {
            passed: false,
            checks: vec![LiveDryRunCheck {
                name: "validation".to_string(),
                passed: false,
                message: reason.into(),
            }],
        }
    }

    fn push(&mut self, name: &str, passed: bool, message: impl Into<String>) {
        if !passed {
            self.passed = false;
        }
        self.checks.push(LiveDryRunCheck {
            name: name.to_string(),
            passed,
            message: message.into(),
        });
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveDryRunCheck {
    pub name: String,
    pub passed: bool,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct LiveDryRunOrderInput<'a> {
    pub exchange: &'a str,
    pub market_type: MarketType,
    pub internal_symbol: &'a str,
    pub side: OrderSide,
    pub desired_notional: f64,
    pub book: &'a OrderBookSnapshot,
    pub symbol_rule: &'a SymbolRule,
    pub balances: &'a [AssetBalance],
    pub reservations: &'a BalanceReservationManager,
    pub disabled_registry: &'a DisabledRegistry,
    pub fee_model: &'a FeeModel,
}

#[derive(Debug, Clone, Copy)]
pub struct LiveDryRunOrderStyle {
    pub order_type: OrderType,
    pub time_in_force: Option<TimeInForce>,
    pub price: Option<f64>,
    pub request_price: Option<f64>,
    pub fee_role: FeeRole,
}

impl LiveDryRunOrderStyle {
    pub fn taker_limit(price: f64) -> Self {
        Self {
            order_type: OrderType::IOC,
            time_in_force: Some(TimeInForce::IOC),
            price: Some(price),
            request_price: Some(price),
            fee_role: FeeRole::Taker,
        }
    }

    pub fn maker_post_only(price: f64) -> Self {
        Self {
            order_type: OrderType::PostOnly,
            time_in_force: Some(TimeInForce::GTC),
            price: Some(price),
            request_price: Some(price),
            fee_role: FeeRole::Maker,
        }
    }

    pub fn market_taker(reference_price: f64) -> Self {
        Self {
            order_type: OrderType::Market,
            time_in_force: None,
            price: Some(reference_price),
            request_price: None,
            fee_role: FeeRole::Taker,
        }
    }
}

pub fn build_live_dry_run_order_plan(
    config: &LiveDryRunConfig,
    input: LiveDryRunOrderInput<'_>,
) -> Result<LiveDryRunOrderPlan> {
    build_live_dry_run_order_plan_with_style(config, input, None)
}

pub fn build_live_dry_run_order_plan_with_style(
    config: &LiveDryRunConfig,
    input: LiveDryRunOrderInput<'_>,
    style: Option<LiveDryRunOrderStyle>,
) -> Result<LiveDryRunOrderPlan> {
    config.validate()?;
    let timestamp = Utc::now();
    let exchange = normalize_exchange(input.exchange);
    let symbol = normalize_symbol(input.internal_symbol);
    let mut validation = LiveDryRunValidationResult::pass();

    let book_age_ms = timestamp
        .signed_duration_since(input.book.received_at)
        .num_milliseconds()
        .max(0);
    validation.push(
        "fresh_book",
        !config.require_fresh_books
            || (!input.book.is_stale && book_age_ms <= config.max_book_age_ms as i64),
        format!("book_age_ms={book_age_ms}"),
    );

    let disabled =
        input
            .disabled_registry
            .check_symbol(&exchange, input.market_type, &symbol, timestamp);
    validation.push(
        "disabled_registry",
        disabled.is_none(),
        disabled
            .as_ref()
            .map(|decision| decision.reason.clone())
            .unwrap_or_else(|| "symbol is enabled".to_string()),
    );

    let executable_price = match input.side {
        OrderSide::Buy => input
            .book
            .best_ask
            .or_else(|| input.book.asks.first().map(|l| l.price)),
        OrderSide::Sell => input
            .book
            .best_bid
            .or_else(|| input.book.bids.first().map(|l| l.price)),
    };
    let raw_price = executable_price.unwrap_or(0.0);
    validation.push(
        "executable_price",
        raw_price > 0.0,
        format!("price={raw_price}"),
    );
    let style = style.unwrap_or_else(|| LiveDryRunOrderStyle::taker_limit(raw_price));
    let raw_price = style.price.unwrap_or(raw_price);

    let capped_notional = input
        .desired_notional
        .min(config.max_notional_per_order)
        .min(config.max_total_notional);
    validation.push(
        "notional_limit",
        capped_notional > 0.0 && input.desired_notional <= config.max_notional_per_order + 1e-12,
        format!(
            "desired_notional={} max_notional_per_order={}",
            input.desired_notional, config.max_notional_per_order
        ),
    );
    let rounded_price = round_price_to_tick(
        raw_price,
        input.symbol_rule.tick_size,
        input.side == OrderSide::Buy,
    );
    let raw_quantity = if rounded_price > 0.0 {
        money::divide_f64(
            capped_notional,
            rounded_price,
            "capped_notional",
            "rounded_price",
        )
        .unwrap_or(capped_notional / rounded_price)
    } else {
        0.0
    };
    let rounded_quantity = round_quantity_to_step(raw_quantity, input.symbol_rule.step_size, false);
    let notional = money::notional_f64(rounded_price, rounded_quantity)
        .unwrap_or(rounded_price * rounded_quantity);

    let client_order_id =
        generate_client_order_id(&exchange, input.market_type, "ldry").into_string();
    validation.push(
        "client_order_id",
        validate_client_order_id(&exchange, input.market_type, &client_order_id).is_ok(),
        client_order_id.clone(),
    );

    let request = OrderRequest {
        market_type: input.market_type,
        symbol: input.symbol_rule.exchange_symbol.clone(),
        side: input.side,
        position_side: PositionSide::None,
        order_type: style.order_type,
        time_in_force: style.time_in_force,
        quantity: rounded_quantity,
        price: style.request_price,
        client_order_id: Some(client_order_id.clone()),
        reduce_only: false,
    };
    let order_validation = validate_order_against_symbol_rule(&request, input.symbol_rule);
    validation.push(
        "symbol_rule_validation",
        order_validation.is_ok(),
        order_validation
            .as_ref()
            .map(|_| "order satisfies symbol rules".to_string())
            .unwrap_or_else(|error| error.to_string()),
    );

    let fee_estimate = input.fee_model.calculate_fee_for_side(
        &FeeLookupKey {
            exchange: exchange.clone(),
            market_type: input.market_type,
            symbol: Some(symbol.clone()),
            liquidity_role: style.fee_role,
        },
        Some(input.side),
        notional,
    );

    let (required_balance_asset, required_balance_amount) = match input.side {
        OrderSide::Buy => {
            let quote_fee = if fee_estimate.fee_asset_mode == FeeAssetMode::Quote {
                fee_estimate.fee_amount
            } else {
                0.0
            };
            (
                input.symbol_rule.quote_asset.clone(),
                money::add_f64(notional, quote_fee, "notional", "quote_fee")
                    .unwrap_or(notional + quote_fee),
            )
        }
        OrderSide::Sell => (input.symbol_rule.base_asset.clone(), rounded_quantity),
    };
    let available = input
        .balances
        .iter()
        .find(|balance| balance.asset.eq_ignore_ascii_case(&required_balance_asset))
        .map(|balance| {
            balance.available
                - input
                    .reservations
                    .locally_reserved(&exchange, &required_balance_asset)
        })
        .unwrap_or(0.0);
    validation.push(
        "balance_available",
        available + 1e-12 >= required_balance_amount,
        format!(
            "{} available={} required={}",
            required_balance_asset, available, required_balance_amount
        ),
    );

    let rejection_reason = (!validation.passed).then(|| {
        validation
            .checks
            .iter()
            .filter(|check| !check.passed)
            .map(|check| format!("{}: {}", check.name, check.message))
            .collect::<Vec<_>>()
            .join("; ")
    });

    Ok(LiveDryRunOrderPlan {
        plan_id: format!("ldry-{}-{}", exchange, timestamp.timestamp_micros()),
        timestamp,
        intent: default_plan_intent(),
        exchange,
        market_type: input.market_type,
        symbol,
        exchange_symbol: input.symbol_rule.exchange_symbol.clone(),
        side: input.side,
        order_type: request.order_type,
        time_in_force: request.time_in_force,
        price: request.price,
        quantity: request.quantity,
        notional,
        client_order_id: Some(client_order_id),
        fee_estimate,
        required_balance_asset,
        required_balance_amount,
        symbol_rule_snapshot: input.symbol_rule.clone(),
        validation_result: validation,
        would_submit: false,
        rejection_reason,
        order_request: request,
    })
}

fn default_plan_intent() -> String {
    "arbitrage".to_string()
}

pub fn append_live_dry_run_plan(path: impl AsRef<Path>, plan: &LiveDryRunOrderPlan) -> Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open {}", path.display()))?;
    serde_json::to_writer(&mut file, plan)?;
    file.write_all(b"\n")?;
    Ok(())
}

fn normalize_exchange(exchange: &str) -> String {
    exchange.trim().to_ascii_lowercase()
}

fn normalize_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .replace(['-', '_', '/'], "")
        .to_ascii_uppercase()
}

fn default_true() -> bool {
    true
}

fn default_max_notional_per_order() -> f64 {
    5.0
}

fn default_max_total_notional() -> f64 {
    20.0
}

fn default_max_book_age_ms() -> u64 {
    1_000
}

fn default_output_path() -> String {
    "data/live_dry_run_orders.jsonl".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchanges::unified::{OrderBookLevel, SymbolStatus};

    fn rule() -> SymbolRule {
        SymbolRule {
            exchange: "mexc".to_string(),
            market_type: MarketType::Spot,
            internal_symbol: "BTCUSDT".to_string(),
            exchange_symbol: "BTCUSDT".to_string(),
            base_asset: "BTC".to_string(),
            quote_asset: "USDT".to_string(),
            price_precision: 2,
            quantity_precision: 4,
            tick_size: 0.01,
            step_size: 0.0001,
            min_quantity: 0.0001,
            min_notional: 5.0,
            max_quantity: None,
            supported_order_types: vec![OrderType::IOC],
            supported_time_in_force: vec![TimeInForce::IOC],
            status: SymbolStatus::Trading,
            raw_metadata: None,
        }
    }

    fn book() -> OrderBookSnapshot {
        OrderBookSnapshot {
            exchange: "mexc".to_string(),
            market_type: MarketType::Spot,
            symbol: "BTCUSDT".to_string(),
            bids: vec![OrderBookLevel {
                price: 100.0,
                quantity: 1.0,
            }],
            asks: vec![OrderBookLevel {
                price: 100.0,
                quantity: 1.0,
            }],
            best_bid: Some(100.0),
            best_ask: Some(100.0),
            exchange_timestamp: Some(Utc::now()),
            received_at: Utc::now(),
            latency_ms: Some(1),
            sequence: Some(1),
            is_stale: false,
        }
    }

    fn input<'a>(
        side: OrderSide,
        rule: &'a SymbolRule,
        book: &'a OrderBookSnapshot,
        balances: &'a [AssetBalance],
        reservations: &'a BalanceReservationManager,
        disabled_registry: &'a DisabledRegistry,
        fee_model: &'a FeeModel,
    ) -> LiveDryRunOrderInput<'a> {
        LiveDryRunOrderInput {
            exchange: "mexc",
            market_type: MarketType::Spot,
            internal_symbol: "BTCUSDT",
            side,
            desired_notional: 5.0,
            book,
            symbol_rule: rule,
            balances,
            reservations,
            disabled_registry,
            fee_model,
        }
    }

    #[test]
    fn submit_orders_true_fails() {
        let config = LiveDryRunConfig {
            submit_orders: true,
            ..LiveDryRunConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn builds_valid_buy_without_submitting() {
        let rule = rule();
        let book = book();
        let balances = vec![AssetBalance::new("USDT", 20.0, 20.0, 0.0)];
        let reservations = BalanceReservationManager::default();
        let plan = build_live_dry_run_order_plan(
            &LiveDryRunConfig::default(),
            input(
                OrderSide::Buy,
                &rule,
                &book,
                &balances,
                &reservations,
                &DisabledRegistry::new(),
                &FeeModel::default(),
            ),
        )
        .unwrap();
        assert!(plan.validation_result.passed);
        assert!(!plan.would_submit);
        assert_eq!(plan.order_request.side, OrderSide::Buy);
    }

    #[test]
    fn builds_valid_sell_without_submitting() {
        let rule = rule();
        let book = book();
        let balances = vec![AssetBalance::new("BTC", 1.0, 1.0, 0.0)];
        let reservations = BalanceReservationManager::default();
        let plan = build_live_dry_run_order_plan(
            &LiveDryRunConfig::default(),
            input(
                OrderSide::Sell,
                &rule,
                &book,
                &balances,
                &reservations,
                &DisabledRegistry::new(),
                &FeeModel::default(),
            ),
        )
        .unwrap();
        assert!(plan.validation_result.passed);
        assert!(!plan.would_submit);
        assert_eq!(plan.order_request.side, OrderSide::Sell);
    }

    #[test]
    fn rejects_stale_book() {
        let rule = rule();
        let mut book = book();
        book.received_at = Utc::now() - chrono::Duration::seconds(10);
        let balances = vec![AssetBalance::new("USDT", 20.0, 20.0, 0.0)];
        let reservations = BalanceReservationManager::default();
        let plan = build_live_dry_run_order_plan(
            &LiveDryRunConfig::default(),
            input(
                OrderSide::Buy,
                &rule,
                &book,
                &balances,
                &reservations,
                &DisabledRegistry::new(),
                &FeeModel::default(),
            ),
        )
        .unwrap();
        assert!(!plan.validation_result.passed);
        assert!(plan.rejection_reason.unwrap().contains("fresh_book"));
    }

    #[test]
    fn rejects_insufficient_balance() {
        let rule = rule();
        let book = book();
        let balances = vec![AssetBalance::new("USDT", 1.0, 1.0, 0.0)];
        let reservations = BalanceReservationManager::default();
        let plan = build_live_dry_run_order_plan(
            &LiveDryRunConfig::default(),
            input(
                OrderSide::Buy,
                &rule,
                &book,
                &balances,
                &reservations,
                &DisabledRegistry::new(),
                &FeeModel::default(),
            ),
        )
        .unwrap();
        assert!(!plan.validation_result.passed);
        assert!(plan.rejection_reason.unwrap().contains("balance_available"));
    }

    #[test]
    fn rejects_below_min_notional() {
        let rule = rule();
        let book = book();
        let balances = vec![AssetBalance::new("USDT", 20.0, 20.0, 0.0)];
        let reservations = BalanceReservationManager::default();
        let disabled_registry = DisabledRegistry::new();
        let fee_model = FeeModel::default();
        let config = LiveDryRunConfig {
            max_notional_per_order: 1.0,
            ..LiveDryRunConfig::default()
        };
        let mut order_input = input(
            OrderSide::Buy,
            &rule,
            &book,
            &balances,
            &reservations,
            &disabled_registry,
            &fee_model,
        );
        order_input.desired_notional = 1.0;
        let plan = build_live_dry_run_order_plan(&config, order_input).unwrap();
        assert!(!plan.validation_result.passed);
        assert!(plan
            .rejection_reason
            .unwrap()
            .contains("symbol_rule_validation"));
    }

    #[test]
    fn writes_jsonl_plan() {
        let path = std::env::temp_dir().join(format!(
            "rustcta-live-dry-run-{}.jsonl",
            Utc::now().timestamp_nanos_opt().unwrap()
        ));
        let rule = rule();
        let book = book();
        let balances = vec![AssetBalance::new("USDT", 20.0, 20.0, 0.0)];
        let reservations = BalanceReservationManager::default();
        let plan = build_live_dry_run_order_plan(
            &LiveDryRunConfig::default(),
            input(
                OrderSide::Buy,
                &rule,
                &book,
                &balances,
                &reservations,
                &DisabledRegistry::new(),
                &FeeModel::default(),
            ),
        )
        .unwrap();
        append_live_dry_run_plan(&path, &plan).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains("\"would_submit\":false"));
        let _ = std::fs::remove_file(path);
    }
}
