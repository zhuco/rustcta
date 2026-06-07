use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::{anyhow, Result};
use chrono::{DateTime, Duration, Utc};

use super::{
    CachedBook, CommonSymbolRules, OpportunityRecord, SpotSpotTakerArbitrageConfig, SpotVenue,
};
use crate::exchanges::spot_reservation::BalanceReservationManager;
use crate::exchanges::unified::{
    AssetBalance, MarketType, OrderBookSnapshot, OrderSide, SymbolRule,
};
use crate::execution::{
    build_live_dry_run_order_plan_with_style, FeeLookupKey, FeeModel, FeeRole,
    LiveDryRunOrderInput, LiveDryRunOrderPlan, LiveDryRunOrderStyle,
};
use crate::risk::DisabledRegistry;
use crate::strategies::spot_spot_taker_arbitrage::opportunity::active_trade_target_notional;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArbitragePairStatus {
    Opening,
    Arbitraging,
    InventoryDrift,
    OneSidedExposure,
    Exiting,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OneSidedExposureKind {
    LongBase,
    ShortBase,
}

#[derive(Debug, Clone)]
pub struct OneSidedExposure {
    pub symbol: String,
    pub kind: OneSidedExposureKind,
    pub exchange: SpotVenue,
    pub quantity: f64,
    pub reference_price: f64,
    pub notional: f64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ArbitragePairRuntime {
    pub symbol: String,
    pub status: ArbitragePairStatus,
    pub created_at: DateTime<Utc>,
    pub last_opportunity_at: DateTime<Utc>,
    pub last_entry_attempt_at: Option<DateTime<Utc>>,
    pub entry_attempts: u32,
    pub exit_attempts: u32,
    pub pause_reason: Option<String>,
    pub one_sided_exposure: Option<OneSidedExposure>,
}

impl ArbitragePairRuntime {
    pub fn opening(symbol: &str, now: DateTime<Utc>) -> Self {
        Self {
            symbol: normalize_symbol(symbol),
            status: ArbitragePairStatus::Opening,
            created_at: now,
            last_opportunity_at: now,
            last_entry_attempt_at: None,
            entry_attempts: 0,
            exit_attempts: 0,
            pause_reason: None,
            one_sided_exposure: None,
        }
    }

    pub fn should_attempt_entry(&self, now: DateTime<Utc>, cooldown_seconds: i64) -> bool {
        self.last_entry_attempt_at
            .is_none_or(|last| now.signed_duration_since(last).num_seconds() >= cooldown_seconds)
    }

    pub fn mark_entry_attempt(&mut self, now: DateTime<Utc>) {
        self.last_entry_attempt_at = Some(now);
        self.entry_attempts = self.entry_attempts.saturating_add(1);
    }

    pub fn mark_arbitraging(&mut self, now: DateTime<Utc>) {
        self.status = ArbitragePairStatus::Arbitraging;
        self.last_opportunity_at = now;
        self.pause_reason = None;
        self.one_sided_exposure = None;
    }

    pub fn mark_opportunity(&mut self, now: DateTime<Utc>) {
        self.last_opportunity_at = now;
    }

    pub fn should_exit_for_inactivity(
        &self,
        now: DateTime<Utc>,
        inactivity_exit_seconds: u64,
    ) -> bool {
        self.status == ArbitragePairStatus::Arbitraging
            && now
                .signed_duration_since(self.last_opportunity_at)
                .num_seconds()
                >= inactivity_exit_seconds as i64
    }

    pub fn mark_inventory_drift(&mut self, reason: impl Into<String>) {
        self.status = ArbitragePairStatus::InventoryDrift;
        self.pause_reason = Some(reason.into());
    }

    pub fn mark_one_sided_exposure(&mut self, exposure: OneSidedExposure) {
        self.status = ArbitragePairStatus::OneSidedExposure;
        self.pause_reason = Some(match exposure.kind {
            OneSidedExposureKind::LongBase => {
                "buy leg filled but sell leg failed; waiting for no-loss sell recovery".to_string()
            }
            OneSidedExposureKind::ShortBase => {
                "sell leg filled but buy leg failed; waiting for no-loss buy recovery".to_string()
            }
        });
        self.one_sided_exposure = Some(exposure);
    }
}

#[derive(Debug, Default)]
pub struct SpreadDurationTracker {
    active: BTreeMap<String, DateTime<Utc>>,
}

impl SpreadDurationTracker {
    pub fn observe(
        &mut self,
        opportunity: &OpportunityRecord,
        now: DateTime<Utc>,
        threshold_seconds: u64,
    ) -> bool {
        let symbol = normalize_symbol(&opportunity.symbol);
        if !opportunity.accepted {
            self.active.remove(&symbol);
            return false;
        }
        let first_seen = self.active.entry(symbol).or_insert(now);
        now.signed_duration_since(*first_seen).num_seconds() >= threshold_seconds as i64
    }

    pub fn clear(&mut self, symbol: &str) {
        self.active.remove(&normalize_symbol(symbol));
    }
}

#[derive(Debug, Clone)]
pub struct EntryAllocation {
    pub exchange: SpotVenue,
    pub best_bid: f64,
    pub best_ask: f64,
    pub weight: f64,
    pub notional: f64,
}

pub fn configured_spot_venues(exchanges: &[String]) -> Vec<SpotVenue> {
    let mut venues = Vec::new();
    let mut seen = BTreeSet::new();
    for exchange in exchanges {
        let venue = match exchange.trim().to_ascii_lowercase().as_str() {
            "mexc" => Some(SpotVenue::Mexc),
            "coinex" => Some(SpotVenue::CoinEx),
            "gate" | "gateio" | "gate.io" => Some(SpotVenue::GateIo),
            "bitget" => Some(SpotVenue::Bitget),
            _ => None,
        };
        if let Some(venue) = venue {
            if seen.insert(venue.as_str()) {
                venues.push(venue);
            }
        }
    }
    venues.truncate(4);
    venues
}

pub fn allocation_weights(exchange_count: usize) -> &'static [f64] {
    match exchange_count {
        4.. => &[0.40, 0.30, 0.20, 0.10],
        3 => &[0.50, 0.30, 0.20],
        2 => &[0.65, 0.35],
        1 => &[1.0],
        _ => &[],
    }
}

pub fn build_entry_allocations(
    config: &SpotSpotTakerArbitrageConfig,
    books: &[(SpotVenue, OrderBookSnapshot)],
) -> Vec<EntryAllocation> {
    let mut candidates = books
        .iter()
        .filter_map(|(exchange, book)| {
            Some(EntryAllocation {
                exchange: *exchange,
                best_bid: book
                    .best_bid
                    .or_else(|| book.bids.first().map(|l| l.price))?,
                best_ask: book
                    .best_ask
                    .or_else(|| book.asks.first().map(|l| l.price))?,
                weight: 0.0,
                notional: 0.0,
            })
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        left.best_bid
            .partial_cmp(&right.best_bid)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    candidates.truncate(4);
    let weights = allocation_weights(candidates.len());
    for (candidate, weight) in candidates.iter_mut().zip(weights.iter().copied()) {
        candidate.weight = weight;
        candidate.notional = config.initial_entry_notional_usdt * weight;
    }
    candidates
}

#[allow(clippy::too_many_arguments)]
pub fn build_entry_live_dry_run_plans(
    config: &SpotSpotTakerArbitrageConfig,
    symbol: &str,
    rules: &CommonSymbolRules,
    books: &[(SpotVenue, OrderBookSnapshot)],
    balances_by_exchange: &HashMap<SpotVenue, Vec<AssetBalance>>,
    attempts_so_far: u32,
    disabled_registry: &DisabledRegistry,
    fee_model: &FeeModel,
) -> Vec<LiveDryRunOrderPlan> {
    let reservations = BalanceReservationManager::default();
    let mut plans = Vec::new();
    for allocation in build_entry_allocations(config, books) {
        let Some((_, book)) = books
            .iter()
            .find(|(venue, _)| *venue == allocation.exchange)
        else {
            continue;
        };
        let style = if attempts_so_far < config.entry_maker_retries {
            LiveDryRunOrderStyle::maker_post_only(allocation.best_bid)
        } else {
            LiveDryRunOrderStyle::taker_limit(allocation.best_ask)
        };
        let balances = balances_by_exchange
            .get(&allocation.exchange)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let Ok(mut plan) = build_live_dry_run_order_plan_with_style(
            &config.live_dry_run,
            LiveDryRunOrderInput {
                exchange: allocation.exchange.as_str(),
                market_type: MarketType::Spot,
                internal_symbol: symbol,
                side: OrderSide::Buy,
                desired_notional: allocation.notional,
                book,
                symbol_rule: rules.for_exchange(allocation.exchange),
                balances,
                reservations: &reservations,
                disabled_registry,
                fee_model,
            },
            Some(style),
        ) else {
            continue;
        };
        plan.intent = if attempts_so_far < config.entry_maker_retries {
            "initial_entry_maker".to_string()
        } else {
            "initial_entry_taker_fallback".to_string()
        };
        plans.push(plan);
    }
    plans
}

#[allow(clippy::too_many_arguments)]
pub fn build_dual_taker_arbitrage_plans(
    config: &SpotSpotTakerArbitrageConfig,
    opportunity: &OpportunityRecord,
    rules: &CommonSymbolRules,
    buy_book: &OrderBookSnapshot,
    sell_book: &OrderBookSnapshot,
    buy_balances: &[AssetBalance],
    sell_balances: &[AssetBalance],
    disabled_registry: &DisabledRegistry,
    fee_model: &FeeModel,
) -> Result<Vec<LiveDryRunOrderPlan>> {
    if !opportunity.accepted || opportunity.estimated_net_pnl <= 0.0 {
        return Err(anyhow!("dual taker requires positive taker/taker pnl"));
    }
    let buy_exchange = parse_spot_venue(&opportunity.buy_exchange)?;
    let sell_exchange = parse_spot_venue(&opportunity.sell_exchange)?;
    let buy_rule = rules.for_exchange(buy_exchange);
    let sell_rule = rules.for_exchange(sell_exchange);
    let notional = active_trade_target_notional(
        config,
        buy_rule,
        sell_rule,
        opportunity.buy_price,
        opportunity.sell_price,
    )
    .min(opportunity.executable_notional);
    let reservations = BalanceReservationManager::default();
    let mut buy_plan = build_live_dry_run_order_plan_with_style(
        &config.live_dry_run,
        LiveDryRunOrderInput {
            exchange: buy_exchange.as_str(),
            market_type: MarketType::Spot,
            internal_symbol: &opportunity.symbol,
            side: OrderSide::Buy,
            desired_notional: notional,
            book: buy_book,
            symbol_rule: buy_rule,
            balances: buy_balances,
            reservations: &reservations,
            disabled_registry,
            fee_model,
        },
        Some(LiveDryRunOrderStyle::taker_limit(opportunity.buy_price)),
    )?;
    let mut sell_plan = build_live_dry_run_order_plan_with_style(
        &config.live_dry_run,
        LiveDryRunOrderInput {
            exchange: sell_exchange.as_str(),
            market_type: MarketType::Spot,
            internal_symbol: &opportunity.symbol,
            side: OrderSide::Sell,
            desired_notional: notional,
            book: sell_book,
            symbol_rule: sell_rule,
            balances: sell_balances,
            reservations: &reservations,
            disabled_registry,
            fee_model,
        },
        Some(LiveDryRunOrderStyle::taker_limit(opportunity.sell_price)),
    )?;
    buy_plan.intent = "dual_taker_arbitrage".to_string();
    sell_plan.intent = "dual_taker_arbitrage".to_string();
    Ok(vec![buy_plan, sell_plan])
}

#[allow(clippy::too_many_arguments)]
pub fn build_maker_taker_arbitrage_plans(
    config: &SpotSpotTakerArbitrageConfig,
    opportunity: &OpportunityRecord,
    rules: &CommonSymbolRules,
    buy_book: &OrderBookSnapshot,
    sell_book: &OrderBookSnapshot,
    buy_balances: &[AssetBalance],
    sell_balances: &[AssetBalance],
    disabled_registry: &DisabledRegistry,
    fee_model: &FeeModel,
) -> Result<Vec<LiveDryRunOrderPlan>> {
    let buy_exchange = parse_spot_venue(&opportunity.buy_exchange)?;
    let sell_exchange = parse_spot_venue(&opportunity.sell_exchange)?;
    let maker_price = buy_book
        .best_bid
        .or_else(|| buy_book.bids.first().map(|level| level.price))
        .ok_or_else(|| anyhow!("missing maker bid"))?;
    let taker_price = sell_book
        .best_bid
        .or_else(|| sell_book.bids.first().map(|level| level.price))
        .ok_or_else(|| anyhow!("missing taker bid"))?;
    let maker_fee = fee_model
        .lookup(&FeeLookupKey {
            exchange: buy_exchange.as_str().to_string(),
            market_type: MarketType::Spot,
            symbol: Some(opportunity.symbol.clone()),
            liquidity_role: FeeRole::Maker,
        })
        .fee_bps;
    let taker_fee = fee_model
        .lookup(&FeeLookupKey {
            exchange: sell_exchange.as_str().to_string(),
            market_type: MarketType::Spot,
            symbol: Some(opportunity.symbol.clone()),
            liquidity_role: FeeRole::Taker,
        })
        .fee_bps;
    let maker_taker_net_bps =
        (taker_price - maker_price) / maker_price * 10_000.0 - maker_fee - taker_fee;
    if maker_taker_net_bps <= 0.0 {
        return Err(anyhow!("maker+taker has no positive expected edge"));
    }
    let buy_rule = rules.for_exchange(buy_exchange);
    let sell_rule = rules.for_exchange(sell_exchange);
    let notional =
        active_trade_target_notional(config, buy_rule, sell_rule, maker_price, taker_price)
            .min(opportunity.executable_notional);
    let reservations = BalanceReservationManager::default();
    let mut maker_plan = build_live_dry_run_order_plan_with_style(
        &config.live_dry_run,
        LiveDryRunOrderInput {
            exchange: buy_exchange.as_str(),
            market_type: MarketType::Spot,
            internal_symbol: &opportunity.symbol,
            side: OrderSide::Buy,
            desired_notional: notional,
            book: buy_book,
            symbol_rule: buy_rule,
            balances: buy_balances,
            reservations: &reservations,
            disabled_registry,
            fee_model,
        },
        Some(LiveDryRunOrderStyle::maker_post_only(maker_price)),
    )?;
    let mut taker_plan = build_live_dry_run_order_plan_with_style(
        &config.live_dry_run,
        LiveDryRunOrderInput {
            exchange: sell_exchange.as_str(),
            market_type: MarketType::Spot,
            internal_symbol: &opportunity.symbol,
            side: OrderSide::Sell,
            desired_notional: notional,
            book: sell_book,
            symbol_rule: sell_rule,
            balances: sell_balances,
            reservations: &reservations,
            disabled_registry,
            fee_model,
        },
        Some(LiveDryRunOrderStyle::taker_limit(taker_price)),
    )?;
    maker_plan.intent = "maker_taker_arbitrage".to_string();
    taker_plan.intent = "maker_taker_arbitrage".to_string();
    Ok(vec![maker_plan, taker_plan])
}

#[allow(clippy::too_many_arguments)]
pub fn build_taker_failure_recovery_sell_plans(
    config: &SpotSpotTakerArbitrageConfig,
    symbol: &str,
    failed_exchange: SpotVenue,
    rules: &CommonSymbolRules,
    books: &[(SpotVenue, OrderBookSnapshot)],
    balances_by_exchange: &HashMap<SpotVenue, Vec<AssetBalance>>,
    disabled_registry: &DisabledRegistry,
    fee_model: &FeeModel,
) -> Vec<LiveDryRunOrderPlan> {
    if !config.inventory_rebalance.enabled || !config.inventory_rebalance.allow_emergency_recovery {
        return Vec::new();
    }
    let reservations = BalanceReservationManager::default();
    let mut candidates = books
        .iter()
        .filter_map(|(exchange, book)| {
            if *exchange == failed_exchange {
                return None;
            }
            let best_bid = book
                .best_bid
                .or_else(|| book.bids.first().map(|l| l.price))?;
            Some((*exchange, book, best_bid))
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        right
            .2
            .partial_cmp(&left.2)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut plans = Vec::new();
    for (exchange, book, best_bid) in candidates {
        let balances = balances_by_exchange
            .get(&exchange)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let Ok(mut plan) = build_live_dry_run_order_plan_with_style(
            &config.live_dry_run,
            LiveDryRunOrderInput {
                exchange: exchange.as_str(),
                market_type: MarketType::Spot,
                internal_symbol: symbol,
                side: OrderSide::Sell,
                desired_notional: config.active_taker_notional_usdt,
                book,
                symbol_rule: rules.for_exchange(exchange),
                balances,
                reservations: &reservations,
                disabled_registry,
                fee_model,
            },
            Some(LiveDryRunOrderStyle::taker_limit(best_bid)),
        ) else {
            continue;
        };
        plan.intent = "taker_failure_recovery_sell".to_string();
        plans.push(plan);
    }
    plans
}

#[allow(clippy::too_many_arguments)]
pub fn build_one_sided_exposure_recovery_plans(
    config: &SpotSpotTakerArbitrageConfig,
    exposure: &OneSidedExposure,
    rules: &CommonSymbolRules,
    books: &[(SpotVenue, OrderBookSnapshot)],
    balances_by_exchange: &HashMap<SpotVenue, Vec<AssetBalance>>,
    disabled_registry: &DisabledRegistry,
    fee_model: &FeeModel,
    symbol_realized_pnl: f64,
) -> Vec<LiveDryRunOrderPlan> {
    if !config.inventory_rebalance.enabled || exposure.quantity <= 0.0 {
        return Vec::new();
    }
    match exposure.kind {
        OneSidedExposureKind::LongBase => build_long_exposure_sell_recovery_plans(
            config,
            exposure,
            rules,
            books,
            balances_by_exchange,
            disabled_registry,
            fee_model,
            symbol_realized_pnl,
        ),
        OneSidedExposureKind::ShortBase => build_short_exposure_buy_recovery_plans(
            config,
            exposure,
            rules,
            books,
            balances_by_exchange,
            disabled_registry,
            fee_model,
            symbol_realized_pnl,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn build_long_exposure_sell_recovery_plans(
    config: &SpotSpotTakerArbitrageConfig,
    exposure: &OneSidedExposure,
    rules: &CommonSymbolRules,
    books: &[(SpotVenue, OrderBookSnapshot)],
    balances_by_exchange: &HashMap<SpotVenue, Vec<AssetBalance>>,
    disabled_registry: &DisabledRegistry,
    fee_model: &FeeModel,
    symbol_realized_pnl: f64,
) -> Vec<LiveDryRunOrderPlan> {
    let reservations = BalanceReservationManager::default();
    let mut candidates = books
        .iter()
        .filter_map(|(exchange, book)| {
            let bid = book
                .best_bid
                .or_else(|| book.bids.first().map(|l| l.price))?;
            let net_bps = recovery_sell_net_bps(
                config,
                fee_model,
                &exposure.symbol,
                *exchange,
                bid,
                exposure.reference_price,
            );
            let expected_loss = expected_long_recovery_loss(
                fee_model,
                &exposure.symbol,
                *exchange,
                bid,
                exposure.reference_price,
                exposure.quantity,
            );
            recovery_allowed(config, net_bps, expected_loss, symbol_realized_pnl)
                .then_some((*exchange, book, bid, net_bps))
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        right
            .3
            .partial_cmp(&left.3)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let mut plans = Vec::new();
    for (exchange, book, bid, _) in candidates {
        let balances = balances_by_exchange
            .get(&exchange)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let notional = (exposure.quantity * bid)
            .min(config.inventory_rebalance.max_rebalance_notional_usdt)
            .min(config.live_dry_run.max_notional_per_order)
            .max(0.0);
        if notional < config.min_notional_per_trade {
            continue;
        }
        let Ok(mut plan) = build_live_dry_run_order_plan_with_style(
            &config.live_dry_run,
            LiveDryRunOrderInput {
                exchange: exchange.as_str(),
                market_type: MarketType::Spot,
                internal_symbol: &exposure.symbol,
                side: OrderSide::Sell,
                desired_notional: notional,
                book,
                symbol_rule: rules.for_exchange(exchange),
                balances,
                reservations: &reservations,
                disabled_registry,
                fee_model,
            },
            Some(LiveDryRunOrderStyle::taker_limit(bid)),
        ) else {
            continue;
        };
        plan.intent = "one_sided_recovery_sell_no_loss".to_string();
        plans.push(plan);
    }
    plans
}

#[allow(clippy::too_many_arguments)]
fn build_short_exposure_buy_recovery_plans(
    config: &SpotSpotTakerArbitrageConfig,
    exposure: &OneSidedExposure,
    rules: &CommonSymbolRules,
    books: &[(SpotVenue, OrderBookSnapshot)],
    balances_by_exchange: &HashMap<SpotVenue, Vec<AssetBalance>>,
    disabled_registry: &DisabledRegistry,
    fee_model: &FeeModel,
    symbol_realized_pnl: f64,
) -> Vec<LiveDryRunOrderPlan> {
    let reservations = BalanceReservationManager::default();
    let mut candidates = books
        .iter()
        .filter_map(|(exchange, book)| {
            let ask = book
                .best_ask
                .or_else(|| book.asks.first().map(|l| l.price))?;
            let net_bps = recovery_buy_net_bps(
                config,
                fee_model,
                &exposure.symbol,
                *exchange,
                ask,
                exposure.reference_price,
            );
            let expected_loss = expected_short_recovery_loss(
                fee_model,
                &exposure.symbol,
                *exchange,
                ask,
                exposure.reference_price,
                exposure.quantity,
            );
            recovery_allowed(config, net_bps, expected_loss, symbol_realized_pnl)
                .then_some((*exchange, book, ask, net_bps))
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        right
            .3
            .partial_cmp(&left.3)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let mut plans = Vec::new();
    for (exchange, book, ask, _) in candidates {
        let balances = balances_by_exchange
            .get(&exchange)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let notional = (exposure.quantity * ask)
            .min(config.inventory_rebalance.max_rebalance_notional_usdt)
            .min(config.live_dry_run.max_notional_per_order)
            .max(0.0);
        if notional < config.min_notional_per_trade {
            continue;
        }
        let Ok(mut plan) = build_live_dry_run_order_plan_with_style(
            &config.live_dry_run,
            LiveDryRunOrderInput {
                exchange: exchange.as_str(),
                market_type: MarketType::Spot,
                internal_symbol: &exposure.symbol,
                side: OrderSide::Buy,
                desired_notional: notional,
                book,
                symbol_rule: rules.for_exchange(exchange),
                balances,
                reservations: &reservations,
                disabled_registry,
                fee_model,
            },
            Some(LiveDryRunOrderStyle::taker_limit(ask)),
        ) else {
            continue;
        };
        plan.intent = "one_sided_recovery_buy_no_loss".to_string();
        plans.push(plan);
    }
    plans
}

#[allow(clippy::too_many_arguments)]
pub fn build_inventory_rebalance_plans(
    config: &SpotSpotTakerArbitrageConfig,
    symbol: &str,
    rules: &CommonSymbolRules,
    books: &[(SpotVenue, OrderBookSnapshot)],
    balances_by_exchange: &HashMap<SpotVenue, Vec<AssetBalance>>,
    target_value_usdt: f64,
    disabled_registry: &DisabledRegistry,
    fee_model: &FeeModel,
) -> Vec<LiveDryRunOrderPlan> {
    build_inventory_rebalance_plans_with_mode(
        config,
        symbol,
        rules,
        books,
        balances_by_exchange,
        target_value_usdt,
        disabled_registry,
        fee_model,
        false,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn build_inventory_trim_excess_plans(
    config: &SpotSpotTakerArbitrageConfig,
    symbol: &str,
    rules: &CommonSymbolRules,
    books: &[(SpotVenue, OrderBookSnapshot)],
    balances_by_exchange: &HashMap<SpotVenue, Vec<AssetBalance>>,
    target_total_value_usdt: f64,
    disabled_registry: &DisabledRegistry,
    fee_model: &FeeModel,
) -> Vec<LiveDryRunOrderPlan> {
    if !config.inventory_rebalance.enabled
        || !config.inventory_rebalance.allow_market_rebalance
        || target_total_value_usdt <= 0.0
    {
        return Vec::new();
    }

    let venue_count = books.len().max(1) as f64;
    let per_venue_target = target_total_value_usdt / venue_count;
    let mut sell_candidates = Vec::new();
    let mut total_base_value = 0.0;
    for (exchange, book) in books {
        let rule = rules.for_exchange(*exchange);
        let Some(best_bid) = book
            .best_bid
            .or_else(|| book.bids.first().map(|level| level.price))
        else {
            continue;
        };
        if best_bid <= 0.0 {
            continue;
        }
        let balances = balances_by_exchange
            .get(exchange)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let base_available = available_asset(balances, &rule.base_asset);
        let base_value = base_available * best_bid;
        total_base_value += base_value;
        let venue_excess = (base_value - per_venue_target).max(0.0);
        if venue_excess >= config.min_notional_per_trade.max(rule.min_notional) {
            sell_candidates.push((*exchange, book, best_bid, base_value, venue_excess));
        }
    }

    let excess_total = total_base_value - target_total_value_usdt;
    if excess_total < config.min_notional_per_trade {
        return Vec::new();
    }

    sell_candidates.sort_by(|left, right| {
        right
            .4
            .partial_cmp(&left.4)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                right
                    .2
                    .partial_cmp(&left.2)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    });

    let reservations = BalanceReservationManager::default();
    let mut plans = Vec::new();
    for (exchange, book, sell_bid, venue_base_value, venue_excess) in sell_candidates {
        if excess_total < config.min_notional_per_trade {
            break;
        }
        let rule = rules.for_exchange(exchange);
        let notional = config
            .active_taker_notional_usdt
            .min(config.inventory_rebalance.max_rebalance_notional_usdt)
            .min(excess_total)
            .min(venue_excess)
            .min(venue_base_value)
            .max(0.0);
        if notional < config.min_notional_per_trade {
            continue;
        }
        let balances = balances_by_exchange
            .get(&exchange)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let Ok(mut sell_plan) = build_live_dry_run_order_plan_with_style(
            &config.live_dry_run,
            LiveDryRunOrderInput {
                exchange: exchange.as_str(),
                market_type: MarketType::Spot,
                internal_symbol: symbol,
                side: OrderSide::Sell,
                desired_notional: notional,
                book,
                symbol_rule: rule,
                balances,
                reservations: &reservations,
                disabled_registry,
                fee_model,
            },
            Some(LiveDryRunOrderStyle::taker_limit(sell_bid)),
        ) else {
            continue;
        };
        sell_plan.intent = "inventory_trim_excess".to_string();
        plans.push(sell_plan);
        break;
    }
    plans
}

#[allow(clippy::too_many_arguments)]
pub fn build_inventory_top_up_plans(
    config: &SpotSpotTakerArbitrageConfig,
    symbol: &str,
    rules: &CommonSymbolRules,
    books: &[(SpotVenue, OrderBookSnapshot)],
    balances_by_exchange: &HashMap<SpotVenue, Vec<AssetBalance>>,
    target_total_value_usdt: f64,
    disabled_registry: &DisabledRegistry,
    fee_model: &FeeModel,
) -> Vec<LiveDryRunOrderPlan> {
    if !config.inventory_rebalance.enabled
        || !config.inventory_rebalance.allow_market_rebalance
        || !config.inventory_rebalance.allow_auto_initial_entry
        || target_total_value_usdt <= 0.0
    {
        return Vec::new();
    }

    let venue_count = books.len().max(1) as f64;
    let per_venue_target = target_total_value_usdt / venue_count;
    let mut buy_candidates = Vec::new();
    let mut total_base_value = 0.0;
    for (exchange, book) in books {
        let rule = rules.for_exchange(*exchange);
        let Some(best_ask) = book
            .best_ask
            .or_else(|| book.asks.first().map(|level| level.price))
        else {
            continue;
        };
        if best_ask <= 0.0 {
            continue;
        }
        let balances = balances_by_exchange
            .get(exchange)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let base_available = available_asset(balances, &rule.base_asset);
        let base_value = base_available * best_ask;
        total_base_value += base_value;
        let venue_deficit = (per_venue_target - base_value).max(0.0);
        if venue_deficit >= config.min_notional_per_trade.max(rule.min_notional) {
            buy_candidates.push((*exchange, book, best_ask, venue_deficit));
        }
    }

    let total_deficit = target_total_value_usdt - total_base_value;
    if total_deficit < config.min_notional_per_trade {
        return Vec::new();
    }

    buy_candidates.sort_by(|left, right| {
        left.2
            .partial_cmp(&right.2)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                right
                    .3
                    .partial_cmp(&left.3)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    });

    let reservations = BalanceReservationManager::default();
    let mut plans = Vec::new();
    for (exchange, book, buy_ask, venue_deficit) in buy_candidates {
        let rule = rules.for_exchange(exchange);
        let notional = config
            .active_taker_notional_usdt
            .min(config.inventory_rebalance.max_rebalance_notional_usdt)
            .min(total_deficit)
            .min(venue_deficit)
            .max(0.0);
        if notional < config.min_notional_per_trade {
            continue;
        }
        let balances = balances_by_exchange
            .get(&exchange)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let Ok(mut buy_plan) = build_live_dry_run_order_plan_with_style(
            &config.live_dry_run,
            LiveDryRunOrderInput {
                exchange: exchange.as_str(),
                market_type: MarketType::Spot,
                internal_symbol: symbol,
                side: OrderSide::Buy,
                desired_notional: notional,
                book,
                symbol_rule: rule,
                balances,
                reservations: &reservations,
                disabled_registry,
                fee_model,
            },
            Some(LiveDryRunOrderStyle::taker_limit(buy_ask)),
        ) else {
            continue;
        };
        buy_plan.intent = "inventory_top_up".to_string();
        plans.push(buy_plan);
        break;
    }
    plans
}

#[allow(clippy::too_many_arguments)]
pub fn build_inventory_rebalance_plans_with_mode(
    config: &SpotSpotTakerArbitrageConfig,
    symbol: &str,
    rules: &CommonSymbolRules,
    books: &[(SpotVenue, OrderBookSnapshot)],
    balances_by_exchange: &HashMap<SpotVenue, Vec<AssetBalance>>,
    target_value_usdt: f64,
    disabled_registry: &DisabledRegistry,
    fee_model: &FeeModel,
    allow_blocked_loss_budget: bool,
) -> Vec<LiveDryRunOrderPlan> {
    if !config.inventory_rebalance.enabled
        || !config.inventory_rebalance.allow_market_rebalance
        || target_value_usdt <= 0.0
    {
        return Vec::new();
    }
    let reservations = BalanceReservationManager::default();
    let mut over = Vec::new();
    let mut under = Vec::new();
    for (exchange, book) in books {
        let rule = rules.for_exchange(*exchange);
        let best_bid = match book.best_bid.or_else(|| book.bids.first().map(|l| l.price)) {
            Some(price) if price > 0.0 => price,
            _ => continue,
        };
        let best_ask = match book.best_ask.or_else(|| book.asks.first().map(|l| l.price)) {
            Some(price) if price > 0.0 => price,
            _ => continue,
        };
        let balances = balances_by_exchange
            .get(exchange)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let base_available = available_asset(balances, &rule.base_asset);
        let base_value = base_available * best_bid;
        if base_value > target_value_usdt + config.min_notional_per_trade {
            over.push((*exchange, book, best_bid, base_value - target_value_usdt));
        } else if base_value + config.min_notional_per_trade < target_value_usdt {
            under.push((*exchange, book, best_ask, target_value_usdt - base_value));
        }
    }
    over.sort_by(|left, right| {
        right
            .2
            .partial_cmp(&left.2)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    under.sort_by(|left, right| {
        left.2
            .partial_cmp(&right.2)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut plans = Vec::new();
    for (sell_exchange, sell_book, sell_bid, excess) in over {
        let Some((buy_exchange, buy_book, buy_ask, deficit)) =
            under
                .iter()
                .copied()
                .find(|(buy_exchange, _, buy_ask, candidate_deficit)| {
                    *buy_exchange != sell_exchange
                        && rebalance_allowed_by_budget(
                            config,
                            fee_model,
                            symbol,
                            sell_exchange,
                            sell_bid,
                            *buy_exchange,
                            *buy_ask,
                            config
                                .active_taker_notional_usdt
                                .min(excess)
                                .min(*candidate_deficit)
                                .max(0.0),
                            allow_blocked_loss_budget,
                        )
                })
        else {
            continue;
        };
        let notional = config
            .active_taker_notional_usdt
            .min(excess)
            .min(deficit)
            .max(0.0);
        if notional < config.min_notional_per_trade {
            continue;
        }
        let sell_balances = balances_by_exchange
            .get(&sell_exchange)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let buy_balances = balances_by_exchange
            .get(&buy_exchange)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let Ok(mut sell_plan) = build_live_dry_run_order_plan_with_style(
            &config.live_dry_run,
            LiveDryRunOrderInput {
                exchange: sell_exchange.as_str(),
                market_type: MarketType::Spot,
                internal_symbol: symbol,
                side: OrderSide::Sell,
                desired_notional: notional,
                book: sell_book,
                symbol_rule: rules.for_exchange(sell_exchange),
                balances: sell_balances,
                reservations: &reservations,
                disabled_registry,
                fee_model,
            },
            Some(LiveDryRunOrderStyle::taker_limit(sell_bid)),
        ) else {
            continue;
        };
        let Ok(mut buy_plan) = build_live_dry_run_order_plan_with_style(
            &config.live_dry_run,
            LiveDryRunOrderInput {
                exchange: buy_exchange.as_str(),
                market_type: MarketType::Spot,
                internal_symbol: symbol,
                side: OrderSide::Buy,
                desired_notional: notional,
                book: buy_book,
                symbol_rule: rules.for_exchange(buy_exchange),
                balances: buy_balances,
                reservations: &reservations,
                disabled_registry,
                fee_model,
            },
            Some(LiveDryRunOrderStyle::taker_limit(buy_ask)),
        ) else {
            continue;
        };
        let intent = if rebalance_net_bps(
            config,
            fee_model,
            symbol,
            sell_exchange,
            sell_bid,
            buy_exchange,
            buy_ask,
        ) >= 0.0
        {
            "inventory_rebalance"
        } else {
            "blocked_inventory_rebalance"
        };
        sell_plan.intent = intent.to_string();
        buy_plan.intent = intent.to_string();
        plans.push(sell_plan);
        plans.push(buy_plan);
    }
    plans
}

#[allow(clippy::too_many_arguments)]
pub fn build_blocked_arbitrage_rebalance_plans(
    config: &SpotSpotTakerArbitrageConfig,
    opportunity: &OpportunityRecord,
    buy_exchange: SpotVenue,
    sell_exchange: SpotVenue,
    rules: &CommonSymbolRules,
    books: &[(SpotVenue, OrderBookSnapshot)],
    balances_by_exchange: &HashMap<SpotVenue, Vec<AssetBalance>>,
    disabled_registry: &DisabledRegistry,
    fee_model: &FeeModel,
) -> Vec<LiveDryRunOrderPlan> {
    if !config.inventory_rebalance.enabled
        || !config.inventory_rebalance.allow_market_rebalance
        || !config
            .inventory_rebalance
            .allow_lossy_rebalance_when_blocked
        || buy_exchange == sell_exchange
    {
        return Vec::new();
    }
    let Some((_, sell_book)) = books.iter().find(|(venue, _)| *venue == buy_exchange) else {
        return Vec::new();
    };
    let Some((_, buy_book)) = books.iter().find(|(venue, _)| *venue == sell_exchange) else {
        return Vec::new();
    };
    let Some(sell_bid) = sell_book
        .best_bid
        .or_else(|| sell_book.bids.first().map(|l| l.price))
    else {
        return Vec::new();
    };
    let Some(buy_ask) = buy_book
        .best_ask
        .or_else(|| buy_book.asks.first().map(|l| l.price))
    else {
        return Vec::new();
    };
    if sell_bid <= 0.0 || buy_ask <= 0.0 {
        return Vec::new();
    }
    let notional = config
        .active_taker_notional_usdt
        .min(config.inventory_rebalance.max_rebalance_notional_usdt)
        .min(opportunity.executable_notional)
        .max(0.0);
    if notional < config.min_notional_per_trade
        || !rebalance_allowed_by_budget(
            config,
            fee_model,
            &opportunity.symbol,
            buy_exchange,
            sell_bid,
            sell_exchange,
            buy_ask,
            notional,
            true,
        )
    {
        return Vec::new();
    }
    let reservations = BalanceReservationManager::default();
    let sell_balances = balances_by_exchange
        .get(&buy_exchange)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let buy_balances = balances_by_exchange
        .get(&sell_exchange)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let Ok(mut sell_plan) = build_live_dry_run_order_plan_with_style(
        &config.live_dry_run,
        LiveDryRunOrderInput {
            exchange: buy_exchange.as_str(),
            market_type: MarketType::Spot,
            internal_symbol: &opportunity.symbol,
            side: OrderSide::Sell,
            desired_notional: notional,
            book: sell_book,
            symbol_rule: rules.for_exchange(buy_exchange),
            balances: sell_balances,
            reservations: &reservations,
            disabled_registry,
            fee_model,
        },
        Some(LiveDryRunOrderStyle::taker_limit(sell_bid)),
    ) else {
        return Vec::new();
    };
    let Ok(mut buy_plan) = build_live_dry_run_order_plan_with_style(
        &config.live_dry_run,
        LiveDryRunOrderInput {
            exchange: sell_exchange.as_str(),
            market_type: MarketType::Spot,
            internal_symbol: &opportunity.symbol,
            side: OrderSide::Buy,
            desired_notional: notional,
            book: buy_book,
            symbol_rule: rules.for_exchange(sell_exchange),
            balances: buy_balances,
            reservations: &reservations,
            disabled_registry,
            fee_model,
        },
        Some(LiveDryRunOrderStyle::taker_limit(buy_ask)),
    ) else {
        return Vec::new();
    };
    sell_plan.intent = "blocked_inventory_rebalance".to_string();
    buy_plan.intent = "blocked_inventory_rebalance".to_string();
    vec![sell_plan, buy_plan]
}

fn rebalance_allowed_by_budget(
    config: &SpotSpotTakerArbitrageConfig,
    fee_model: &FeeModel,
    symbol: &str,
    sell_exchange: SpotVenue,
    sell_bid: f64,
    buy_exchange: SpotVenue,
    buy_ask: f64,
    notional: f64,
    allow_blocked_loss_budget: bool,
) -> bool {
    let net_bps = rebalance_net_bps(
        config,
        fee_model,
        symbol,
        sell_exchange,
        sell_bid,
        buy_exchange,
        buy_ask,
    );
    if net_bps >= 0.0 {
        return true;
    }
    if !allow_blocked_loss_budget
        || !config
            .inventory_rebalance
            .allow_lossy_rebalance_when_blocked
        || notional <= 0.0
    {
        return false;
    }
    expected_loss_from_net_bps(net_bps, notional)
        <= config
            .inventory_rebalance
            .max_blocked_rebalance_loss_usdt
            .max(0.0)
}

fn expected_loss_from_net_bps(net_bps: f64, notional: f64) -> f64 {
    if net_bps >= 0.0 || notional <= 0.0 {
        0.0
    } else {
        (-net_bps / 10_000.0) * notional
    }
}

fn rebalance_net_bps(
    config: &SpotSpotTakerArbitrageConfig,
    fee_model: &FeeModel,
    symbol: &str,
    sell_exchange: SpotVenue,
    sell_bid: f64,
    buy_exchange: SpotVenue,
    buy_ask: f64,
) -> f64 {
    if buy_ask <= 0.0 {
        return f64::NEG_INFINITY;
    }
    let sell_fee = fee_model
        .lookup_for_side(
            &FeeLookupKey {
                exchange: sell_exchange.as_str().to_string(),
                market_type: MarketType::Spot,
                symbol: Some(symbol.to_string()),
                liquidity_role: FeeRole::Taker,
            },
            Some(OrderSide::Sell),
        )
        .fee_bps;
    let buy_fee = fee_model
        .lookup_for_side(
            &FeeLookupKey {
                exchange: buy_exchange.as_str().to_string(),
                market_type: MarketType::Spot,
                symbol: Some(symbol.to_string()),
                liquidity_role: FeeRole::Taker,
            },
            Some(OrderSide::Buy),
        )
        .fee_bps;
    (sell_bid - buy_ask) / buy_ask * 10_000.0
        - sell_fee
        - buy_fee
        - config.inventory_rebalance.required_no_loss_bps()
}

fn recovery_sell_net_bps(
    config: &SpotSpotTakerArbitrageConfig,
    fee_model: &FeeModel,
    symbol: &str,
    sell_exchange: SpotVenue,
    sell_bid: f64,
    cost_price: f64,
) -> f64 {
    if cost_price <= 0.0 {
        return f64::NEG_INFINITY;
    }
    let sell_fee = fee_model
        .lookup_for_side(
            &FeeLookupKey {
                exchange: sell_exchange.as_str().to_string(),
                market_type: MarketType::Spot,
                symbol: Some(symbol.to_string()),
                liquidity_role: FeeRole::Taker,
            },
            Some(OrderSide::Sell),
        )
        .fee_bps;
    (sell_bid - cost_price) / cost_price * 10_000.0
        - sell_fee
        - config.inventory_rebalance.required_no_loss_bps()
}

fn recovery_buy_net_bps(
    config: &SpotSpotTakerArbitrageConfig,
    fee_model: &FeeModel,
    symbol: &str,
    buy_exchange: SpotVenue,
    buy_ask: f64,
    sold_price: f64,
) -> f64 {
    if buy_ask <= 0.0 {
        return f64::NEG_INFINITY;
    }
    let buy_fee = fee_model
        .lookup_for_side(
            &FeeLookupKey {
                exchange: buy_exchange.as_str().to_string(),
                market_type: MarketType::Spot,
                symbol: Some(symbol.to_string()),
                liquidity_role: FeeRole::Taker,
            },
            Some(OrderSide::Buy),
        )
        .fee_bps;
    (sold_price - buy_ask) / buy_ask * 10_000.0
        - buy_fee
        - config.inventory_rebalance.required_no_loss_bps()
}

fn expected_long_recovery_loss(
    fee_model: &FeeModel,
    symbol: &str,
    sell_exchange: SpotVenue,
    sell_bid: f64,
    cost_price: f64,
    quantity: f64,
) -> f64 {
    let proceeds = sell_bid * quantity;
    let fee = fee_model
        .calculate_fee_for_side(
            &FeeLookupKey {
                exchange: sell_exchange.as_str().to_string(),
                market_type: MarketType::Spot,
                symbol: Some(symbol.to_string()),
                liquidity_role: FeeRole::Taker,
            },
            Some(OrderSide::Sell),
            proceeds,
        )
        .fee_amount;
    (cost_price * quantity - (proceeds - fee)).max(0.0)
}

fn expected_short_recovery_loss(
    fee_model: &FeeModel,
    symbol: &str,
    buy_exchange: SpotVenue,
    buy_ask: f64,
    sold_price: f64,
    quantity: f64,
) -> f64 {
    let cost = buy_ask * quantity;
    let fee = fee_model
        .calculate_fee_for_side(
            &FeeLookupKey {
                exchange: buy_exchange.as_str().to_string(),
                market_type: MarketType::Spot,
                symbol: Some(symbol.to_string()),
                liquidity_role: FeeRole::Taker,
            },
            Some(OrderSide::Buy),
            cost,
        )
        .fee_amount;
    (cost + fee - sold_price * quantity).max(0.0)
}

fn recovery_allowed(
    config: &SpotSpotTakerArbitrageConfig,
    net_bps: f64,
    expected_loss: f64,
    symbol_realized_pnl: f64,
) -> bool {
    if net_bps >= 0.0 {
        return true;
    }
    expected_loss > 0.0
        && expected_loss
            <= config
                .inventory_rebalance
                .available_profit_budget(symbol_realized_pnl)
}

fn available_asset(balances: &[AssetBalance], asset: &str) -> f64 {
    balances
        .iter()
        .find(|balance| balance.asset.eq_ignore_ascii_case(asset))
        .map(|balance| balance.available)
        .unwrap_or(0.0)
}

#[allow(clippy::too_many_arguments)]
pub fn build_exit_live_dry_run_plans(
    config: &SpotSpotTakerArbitrageConfig,
    symbol: &str,
    rules: &CommonSymbolRules,
    books: &[(SpotVenue, OrderBookSnapshot)],
    balances_by_exchange: &HashMap<SpotVenue, Vec<AssetBalance>>,
    attempts_so_far: u32,
    disabled_registry: &DisabledRegistry,
    fee_model: &FeeModel,
) -> Vec<LiveDryRunOrderPlan> {
    let reservations = BalanceReservationManager::default();
    let mut plans = Vec::new();
    for (exchange, book) in books.iter().take(4) {
        let sell_price = if attempts_so_far < config.exit_maker_retries {
            book.best_ask
                .or_else(|| book.asks.first().map(|level| level.price))
        } else {
            book.best_bid
                .or_else(|| book.bids.first().map(|level| level.price))
        };
        let Some(sell_price) = sell_price else {
            continue;
        };
        let style = if attempts_so_far < config.exit_maker_retries {
            LiveDryRunOrderStyle::maker_post_only(sell_price)
        } else {
            LiveDryRunOrderStyle::market_taker(sell_price)
        };
        let balances = balances_by_exchange
            .get(exchange)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let symbol_rule = rules.for_exchange(*exchange);
        let Some(desired_notional) = exit_desired_notional(
            config,
            symbol_rule,
            balances,
            sell_price,
            books.len().max(1),
        ) else {
            continue;
        };
        let Ok(mut plan) = build_live_dry_run_order_plan_with_style(
            &config.live_dry_run,
            LiveDryRunOrderInput {
                exchange: exchange.as_str(),
                market_type: MarketType::Spot,
                internal_symbol: symbol,
                side: OrderSide::Sell,
                desired_notional,
                book,
                symbol_rule,
                balances,
                reservations: &reservations,
                disabled_registry,
                fee_model,
            },
            Some(style),
        ) else {
            continue;
        };
        plan.intent = if attempts_so_far < config.exit_maker_retries {
            "inactive_exit_maker".to_string()
        } else {
            "inactive_exit_market_fallback".to_string()
        };
        plans.push(plan);
    }
    plans
}

fn exit_desired_notional(
    config: &SpotSpotTakerArbitrageConfig,
    symbol_rule: &SymbolRule,
    balances: &[AssetBalance],
    reference_price: f64,
    venue_count: usize,
) -> Option<f64> {
    if reference_price <= 0.0 {
        return None;
    }
    let available_base = available_asset(balances, &symbol_rule.base_asset);
    let available_notional = available_base * reference_price;
    if available_notional + 1e-12 < symbol_rule.min_notional {
        return None;
    }
    let per_venue_target = config.initial_entry_notional_usdt / venue_count.max(1) as f64;
    let min_valid_notional =
        symbol_rule.min_notional + reference_price * symbol_rule.step_size.max(0.0);
    let desired = per_venue_target
        .max(min_valid_notional)
        .min(config.live_dry_run.max_notional_per_order)
        .min(config.live_dry_run.max_total_notional)
        .min(available_notional);
    (desired + 1e-12 >= symbol_rule.min_notional).then_some(desired)
}

pub fn collect_cached_books_for_venues(
    venues: &[SpotVenue],
    cached: &HashMap<SpotVenue, CachedBook>,
) -> Vec<(SpotVenue, OrderBookSnapshot)> {
    venues
        .iter()
        .filter_map(|venue| {
            cached
                .get(venue)
                .cloned()
                .map(|book| (*venue, book.into_snapshot()))
        })
        .collect()
}

fn parse_spot_venue(value: &str) -> Result<SpotVenue> {
    match value.trim().to_ascii_lowercase().as_str() {
        "mexc" => Ok(SpotVenue::Mexc),
        "coinex" => Ok(SpotVenue::CoinEx),
        "gate" | "gateio" | "gate.io" => Ok(SpotVenue::GateIo),
        "bitget" => Ok(SpotVenue::Bitget),
        other => Err(anyhow!("unsupported spot venue: {other}")),
    }
}

pub fn duration_from_seconds(seconds: u64) -> Duration {
    Duration::seconds(seconds as i64)
}

fn normalize_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .replace(['-', '_', '/'], "")
        .to_ascii_uppercase()
}
