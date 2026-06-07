use chrono::Utc;

use super::{
    book_age_ms, buy_depth_notional, calculate_spread, configured_spot_pair, is_book_fresh,
    sell_depth_notional, BookSource, CommonSymbolRules, OpportunityRecord, PaperInventory,
    RejectionReason, RiskState, SpotSpotTakerArbitrageConfig, SpotVenue,
};
use crate::exchanges::unified::validate_quantity_step;
use crate::exchanges::unified::{MarketType, OrderBookSnapshot, OrderSide, SymbolStatus};
use crate::execution::{FeeLookupKey, FeeModel, FeeRole};
use crate::risk::{DisabledDecision, DisabledRegistry, DisabledScope};

pub fn detect_opportunities_for_pair(
    config: &SpotSpotTakerArbitrageConfig,
    rules: &CommonSymbolRules,
    mexc_book: &OrderBookSnapshot,
    coinex_book: &OrderBookSnapshot,
    inventory: &PaperInventory,
    risk: &RiskState,
) -> Vec<OpportunityRecord> {
    let fee_model = fee_model_from_strategy_config(config);
    let disabled_registry = DisabledRegistry::new();
    detect_opportunities_for_pair_with_source(
        config,
        rules,
        mexc_book,
        coinex_book,
        inventory,
        risk,
        &fee_model,
        &disabled_registry,
        BookSource::Rest,
        BookSource::Rest,
    )
}

pub fn detect_opportunities_for_pair_with_source(
    config: &SpotSpotTakerArbitrageConfig,
    rules: &CommonSymbolRules,
    mexc_book: &OrderBookSnapshot,
    coinex_book: &OrderBookSnapshot,
    inventory: &PaperInventory,
    risk: &RiskState,
    fee_model: &FeeModel,
    disabled_registry: &DisabledRegistry,
    mexc_source: BookSource,
    coinex_source: BookSource,
) -> Vec<OpportunityRecord> {
    let (left_exchange, right_exchange) = configured_spot_pair(&config.exchanges);
    vec![
        build_opportunity_with_source(
            config,
            rules,
            inventory,
            risk,
            left_exchange,
            right_exchange,
            mexc_book,
            coinex_book,
            fee_model,
            disabled_registry,
            mexc_source,
            coinex_source,
        ),
        build_opportunity_with_source(
            config,
            rules,
            inventory,
            risk,
            right_exchange,
            left_exchange,
            coinex_book,
            mexc_book,
            fee_model,
            disabled_registry,
            coinex_source,
            mexc_source,
        ),
    ]
}

pub fn build_opportunity(
    config: &SpotSpotTakerArbitrageConfig,
    rules: &CommonSymbolRules,
    inventory: &PaperInventory,
    risk: &RiskState,
    buy_exchange: SpotVenue,
    sell_exchange: SpotVenue,
    buy_book: &OrderBookSnapshot,
    sell_book: &OrderBookSnapshot,
) -> OpportunityRecord {
    let fee_model = fee_model_from_strategy_config(config);
    let disabled_registry = DisabledRegistry::new();
    build_opportunity_with_source(
        config,
        rules,
        inventory,
        risk,
        buy_exchange,
        sell_exchange,
        buy_book,
        sell_book,
        &fee_model,
        &disabled_registry,
        BookSource::Rest,
        BookSource::Rest,
    )
}

pub fn build_opportunity_with_source(
    config: &SpotSpotTakerArbitrageConfig,
    rules: &CommonSymbolRules,
    inventory: &PaperInventory,
    risk: &RiskState,
    buy_exchange: SpotVenue,
    sell_exchange: SpotVenue,
    buy_book: &OrderBookSnapshot,
    sell_book: &OrderBookSnapshot,
    fee_model: &FeeModel,
    disabled_registry: &DisabledRegistry,
    buy_source: BookSource,
    sell_source: BookSource,
) -> OpportunityRecord {
    let symbol = buy_book.symbol.clone();
    let buy_price = buy_book.best_ask.unwrap_or(0.0);
    let sell_price = sell_book.best_bid.unwrap_or(0.0);
    let buy_fee_key = FeeLookupKey {
        exchange: buy_exchange.as_str().to_string(),
        market_type: MarketType::Spot,
        symbol: Some(symbol.clone()),
        liquidity_role: FeeRole::Taker,
    };
    let sell_fee_key = FeeLookupKey {
        exchange: sell_exchange.as_str().to_string(),
        market_type: MarketType::Spot,
        symbol: Some(symbol.clone()),
        liquidity_role: FeeRole::Taker,
    };
    let buy_fee_lookup = fee_model.lookup_for_side(&buy_fee_key, Some(OrderSide::Buy));
    let sell_fee_lookup = fee_model.lookup_for_side(&sell_fee_key, Some(OrderSide::Sell));
    let buy_fee_bps = buy_fee_lookup.fee_bps;
    let sell_fee_bps = sell_fee_lookup.fee_bps;
    let spread = calculate_spread(
        buy_price,
        sell_price,
        buy_fee_bps,
        sell_fee_bps,
        config.slippage_bps,
        config.safety_buffer_bps,
    );
    let buy_rule = rules.for_exchange(buy_exchange);
    let sell_rule = rules.for_exchange(sell_exchange);
    let target_notional =
        active_trade_target_notional(config, buy_rule, sell_rule, buy_price, sell_price);
    let executable_notional = executable_notional(config, buy_book, sell_book, target_notional);
    let raw_quantity = if buy_price > 0.0 {
        executable_notional / buy_price
    } else {
        0.0
    };
    let quantity = round_down_to_common_step(raw_quantity, buy_rule.step_size, sell_rule.step_size);
    let executable_notional = quantity * buy_price;
    let buy_notional = quantity * buy_price;
    let sell_notional = quantity * sell_price;
    let fees =
        fee_model.calculate_buy_sell(&buy_fee_key, &sell_fee_key, buy_notional, sell_notional);
    let rejection = rejection_reason(
        config,
        rules,
        inventory,
        risk,
        disabled_registry,
        buy_exchange,
        sell_exchange,
        buy_book,
        sell_book,
        executable_notional,
        quantity,
        spread.raw_spread_bps,
        spread.net_spread_bps,
        buy_fee_bps,
    );
    let rejection_reason = rejection.as_ref().map(|item| item.0);
    let rejection_detail = rejection.and_then(|item| item.1);

    OpportunityRecord {
        timestamp: Utc::now(),
        symbol,
        buy_exchange: buy_exchange.as_str().to_string(),
        sell_exchange: sell_exchange.as_str().to_string(),
        buy_price,
        sell_price,
        raw_spread_bps: spread.raw_spread_bps,
        buy_fee_bps,
        sell_fee_bps,
        fee_source_buy: buy_fee_lookup.effective_rate.source,
        fee_source_sell: sell_fee_lookup.effective_rate.source,
        platform_discount_applied: buy_fee_lookup.platform_discount_applied
            || sell_fee_lookup.platform_discount_applied,
        estimated_fee_bps: buy_fee_bps + sell_fee_bps,
        estimated_slippage_bps: config.slippage_bps,
        safety_buffer_bps: config.safety_buffer_bps,
        estimated_net_spread_bps: spread.net_spread_bps,
        estimated_total_fee: fees.total_fee,
        estimated_gross_pnl: fees.gross_pnl,
        estimated_net_pnl: fees.net_pnl,
        executable_notional,
        quantity,
        accepted: rejection_reason.is_none(),
        rejection_reason,
        rejection_detail,
        buy_book_age_ms: book_age_ms(buy_book),
        sell_book_age_ms: book_age_ms(sell_book),
        buy_book_source: buy_source,
        sell_book_source: sell_source,
        buy_latency_ms: buy_book.latency_ms,
        sell_latency_ms: sell_book.latency_ms,
    }
}

fn round_down_to_common_step(quantity: f64, buy_step: f64, sell_step: f64) -> f64 {
    let step = buy_step.max(sell_step);
    if step <= 0.0 || quantity <= 0.0 {
        return quantity;
    }
    let rounded = (quantity / step).floor() * step;
    let precision = decimal_places(step);
    let factor = 10_f64.powi(precision as i32);
    (rounded * factor).floor() / factor
}

fn decimal_places(value: f64) -> u32 {
    let text = format!("{value:.12}");
    text.trim_end_matches('0')
        .split_once('.')
        .map(|(_, decimals)| decimals.len() as u32)
        .unwrap_or(0)
}

fn rejection_reason(
    config: &SpotSpotTakerArbitrageConfig,
    rules: &CommonSymbolRules,
    inventory: &PaperInventory,
    risk: &RiskState,
    disabled_registry: &DisabledRegistry,
    buy_exchange: SpotVenue,
    sell_exchange: SpotVenue,
    buy_book: &OrderBookSnapshot,
    sell_book: &OrderBookSnapshot,
    executable_notional: f64,
    quantity: f64,
    raw_spread_bps: f64,
    net_spread_bps: f64,
    buy_fee_bps: f64,
) -> Option<(RejectionReason, Option<String>)> {
    if !is_book_fresh(buy_book, config.stale_book_ms, config.max_book_latency_ms)
        || !is_book_fresh(sell_book, config.stale_book_ms, config.max_book_latency_ms)
    {
        return Some((RejectionReason::StaleBook, None));
    }
    if risk.is_symbol_blacklisted(&buy_book.symbol) {
        return Some((RejectionReason::SymbolBlacklisted, None));
    }
    if let Some(decision) = disabled_registry.check_symbol(
        buy_exchange.as_str(),
        MarketType::Spot,
        &buy_book.symbol,
        Utc::now(),
    ) {
        return Some(disabled_rejection(decision));
    }
    if let Some(decision) = disabled_registry.check_symbol(
        sell_exchange.as_str(),
        MarketType::Spot,
        &sell_book.symbol,
        Utc::now(),
    ) {
        return Some(disabled_rejection(decision));
    }
    if risk.is_in_cooldown(&buy_book.symbol) {
        return Some((RejectionReason::Cooldown, None));
    }
    if risk.daily_loss_limit_hit(config) {
        return Some((RejectionReason::DailyLossLimit, None));
    }
    if risk.notional_limit_hit(config, &buy_book.symbol, executable_notional) {
        return Some((RejectionReason::NotionalLimit, None));
    }
    let buy_rule = rules.for_exchange(buy_exchange);
    let sell_rule = rules.for_exchange(sell_exchange);
    if buy_rule.status != SymbolStatus::Trading || sell_rule.status != SymbolStatus::Trading {
        return Some((RejectionReason::SymbolRule, None));
    }
    let required_notional = active_trade_required_notional(
        config,
        buy_rule,
        sell_rule,
        buy_book.best_ask.unwrap_or(0.0),
        sell_book.best_bid.unwrap_or(0.0),
    );
    let required_depth_notional = config.min_depth_notional.max(required_notional);
    let target_notional = active_trade_target_notional(
        config,
        buy_rule,
        sell_rule,
        buy_book.best_ask.unwrap_or(0.0),
        sell_book.best_bid.unwrap_or(0.0),
    );
    let capped_target = target_notional.min(config.max_notional_per_trade);
    let buy_depth = buy_depth_notional(&buy_book.asks, capped_target);
    let sell_depth = sell_depth_notional(&sell_book.bids, capped_target);
    let depth_available = buy_depth.min(sell_depth).min(capped_target);
    if depth_available + 1e-12 < required_depth_notional {
        return Some((
            RejectionReason::InsufficientDepth,
            Some(format!(
                "executable_notional {:.8} below required_depth_notional {:.8}; buy_depth {:.8} sell_depth {:.8} capped_target {:.8} buy_levels={} sell_levels={} buy_min_notional {:.8} sell_min_notional {:.8} buy_step {:.8} sell_step {:.8}",
                executable_notional,
                required_depth_notional,
                buy_depth,
                sell_depth,
                capped_target,
                buy_book.asks.len(),
                sell_book.bids.len(),
                buy_rule.min_notional,
                sell_rule.min_notional,
                buy_rule.step_size,
                sell_rule.step_size
            )),
        ));
    }
    if executable_notional + 1e-12 < config.min_notional_per_trade {
        return Some((RejectionReason::MinNotional, None));
    }
    if executable_notional > config.max_notional_per_trade + 1e-12 {
        return Some((RejectionReason::NotionalLimit, None));
    }
    if executable_notional < buy_rule.min_notional || executable_notional < sell_rule.min_notional {
        return Some((RejectionReason::MinNotional, None));
    }
    if quantity + 1e-12 < buy_rule.min_quantity || quantity + 1e-12 < sell_rule.min_quantity {
        return Some((RejectionReason::MinNotional, None));
    }
    if validate_quantity_step(quantity, buy_rule.step_size).is_err()
        || validate_quantity_step(quantity, sell_rule.step_size).is_err()
    {
        return Some((RejectionReason::SymbolRule, None));
    }
    if config.max_raw_spread_bps > 0.0 && raw_spread_bps > config.max_raw_spread_bps + 1e-12 {
        return Some((
            RejectionReason::AbnormalSpread,
            Some(format!(
                "raw_spread_bps={raw_spread_bps:.4} exceeds max_raw_spread_bps={:.4}; raw spread is sell_exchange best_bid minus buy_exchange best_ask",
                config.max_raw_spread_bps
            )),
        ));
    }
    if raw_spread_bps + 1e-12 < config.min_raw_spread_bps {
        return Some((
            RejectionReason::NetSpreadBelowThreshold,
            Some(format!(
                "raw_spread_bps={raw_spread_bps:.4} below min_raw_spread_bps={:.4}; raw spread is sell_exchange best_bid minus buy_exchange best_ask",
                config.min_raw_spread_bps
            )),
        ));
    }
    if net_spread_bps + 1e-12 < config.min_net_spread_bps {
        return Some((
            RejectionReason::NetSpreadBelowThreshold,
            Some(format!(
                "net_spread_bps={net_spread_bps:.4} below min_net_spread_bps={:.4}",
                config.min_net_spread_bps
            )),
        ));
    }
    let quote_needed = executable_notional * (1.0 + buy_fee_bps / 10_000.0);
    if inventory
        .balance(buy_exchange, &config.quote_asset)
        .effective_available()
        + 1e-12
        < quote_needed
    {
        return Some((RejectionReason::InsufficientQuoteBalance, None));
    }
    if !spot_lifecycle_order_mode(config) {
        let base_asset = &buy_rule.base_asset;
        let base_available = inventory
            .balance(sell_exchange, base_asset)
            .effective_available();
        let managed_base = disabled_registry.effective_inventory_quantity(
            sell_exchange.as_str(),
            MarketType::Spot,
            &buy_book.symbol,
            base_asset,
            base_available,
        );
        if managed_base + 1e-12 < quantity {
            return Some((RejectionReason::InsufficientBaseBalance, None));
        }
    }
    None
}

fn spot_lifecycle_order_mode(config: &SpotSpotTakerArbitrageConfig) -> bool {
    config.trading_mode.eq_ignore_ascii_case("live_dry_run")
        || config.trading_mode.eq_ignore_ascii_case("live")
}

fn disabled_rejection(decision: DisabledDecision) -> (RejectionReason, Option<String>) {
    let reason = match decision.scope {
        DisabledScope::Symbol => RejectionReason::DisabledSymbol,
        DisabledScope::Exchange => RejectionReason::DisabledExchange,
        DisabledScope::ExchangeSymbol => RejectionReason::DisabledExchangeSymbol,
        DisabledScope::UnmanagedPosition => RejectionReason::InsufficientBaseBalance,
    };
    (reason, Some(decision.reason))
}

pub fn fee_model_from_strategy_config(config: &SpotSpotTakerArbitrageConfig) -> FeeModel {
    fee_model_with_strategy_overrides(FeeModel::default(), config)
}

pub fn fee_model_with_strategy_overrides(
    mut model: FeeModel,
    config: &SpotSpotTakerArbitrageConfig,
) -> FeeModel {
    if let Some(value) = config.taker_fee_bps_override {
        model = model
            .with_spot_taker_override("mexc", None, value)
            .with_spot_taker_override("coinex", None, value)
            .with_spot_taker_override("gateio", None, value)
            .with_spot_taker_override("bitget", None, value);
    } else {
        if let Some(fee) = config.mexc.fee_override {
            model = model.with_spot_taker_override("mexc", None, fee.taker_fee_rate * 10_000.0);
        }
        if let Some(fee) = config.coinex.fee_override {
            model = model.with_spot_taker_override("coinex", None, fee.taker_fee_rate * 10_000.0);
        }
        if let Some(fee) = config.gateio.fee_override {
            model = model.with_spot_taker_override("gateio", None, fee.taker_fee_rate * 10_000.0);
        }
        if let Some(fee) = config.bitget.fee_override {
            model = model.with_spot_taker_override("bitget", None, fee.taker_fee_rate * 10_000.0);
        }
    }
    model
}

fn executable_notional(
    config: &SpotSpotTakerArbitrageConfig,
    buy_book: &OrderBookSnapshot,
    sell_book: &OrderBookSnapshot,
    target_notional: f64,
) -> f64 {
    let target = target_notional.min(config.max_notional_per_trade);
    buy_depth_notional(&buy_book.asks, target)
        .min(sell_depth_notional(&sell_book.bids, target))
        .min(target)
}

pub fn active_trade_target_notional(
    config: &SpotSpotTakerArbitrageConfig,
    buy_rule: &crate::exchanges::unified::SymbolRule,
    sell_rule: &crate::exchanges::unified::SymbolRule,
    buy_reference_price: f64,
    sell_reference_price: f64,
) -> f64 {
    let buy_min = rule_min_executable_notional(buy_rule, buy_reference_price);
    let sell_min = rule_min_executable_notional(sell_rule, sell_reference_price);
    config
        .active_taker_notional_usdt
        .max(config.min_notional_per_trade)
        .max(buy_min)
        .max(sell_min)
}

pub fn active_trade_required_notional(
    config: &SpotSpotTakerArbitrageConfig,
    buy_rule: &crate::exchanges::unified::SymbolRule,
    sell_rule: &crate::exchanges::unified::SymbolRule,
    buy_reference_price: f64,
    sell_reference_price: f64,
) -> f64 {
    config
        .min_notional_per_trade
        .max(rule_min_notional_floor(buy_rule, buy_reference_price))
        .max(rule_min_notional_floor(sell_rule, sell_reference_price))
}

fn rule_min_executable_notional(
    rule: &crate::exchanges::unified::SymbolRule,
    reference_price: f64,
) -> f64 {
    let floor = rule_min_notional_floor(rule, reference_price);
    let step_buffer = if reference_price > 0.0 {
        reference_price * rule.step_size.max(0.0)
    } else {
        0.0
    };
    floor + step_buffer
}

fn rule_min_notional_floor(
    rule: &crate::exchanges::unified::SymbolRule,
    reference_price: f64,
) -> f64 {
    let quantity_floor = if reference_price > 0.0 {
        reference_price * rule.min_quantity.max(0.0)
    } else {
        0.0
    };
    rule.min_notional.max(quantity_floor)
}
