//! Cross-exchange opportunity scanning with executable 5-level VWAP.

use super::config::CrossExchangeArbitrageConfig;
use super::config::OpenExecutionStyle;
use super::fees::FeeModel;
use super::funding::FundingModel;
use super::risk::{book_age_ms, RejectReason, RiskGate};
use super::simulation::{best_level_covers_base_quantity, calculate_taker_vwap};
use super::sizing::{
    size_dual_taker_pair_for_notional, size_hedge_pair_for_notional, ExchangeLegSize,
};
use super::state::{MakerLegKind, OrderSide, StrategyRoute};
use crate::market::{
    CanonicalSymbol, ExchangeId, InstrumentMeta, OrderBook5, PublicBookProfileKind, RoundingMode,
    RouteStatus,
};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::{Mutex, OnceLock};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketSnapshot {
    pub book: OrderBook5,
    pub instrument: Option<InstrumentMeta>,
    pub route_status: RouteStatus,
    pub funding_rate: f64,
    pub next_funding_time: Option<DateTime<Utc>>,
    pub trigger_book_profile: Option<PublicBookProfileKind>,
    pub validation_book_profile: Option<PublicBookProfileKind>,
}

impl MarketSnapshot {
    pub fn healthy(book: OrderBook5) -> Self {
        Self {
            book,
            instrument: None,
            route_status: RouteStatus::Healthy,
            funding_rate: 0.0,
            next_funding_time: None,
            trigger_book_profile: None,
            validation_book_profile: None,
        }
    }

    pub fn with_instrument(mut self, instrument: InstrumentMeta) -> Self {
        self.instrument = Some(instrument);
        self
    }

    pub fn with_book_profiles(
        mut self,
        trigger_book_profile: PublicBookProfileKind,
        validation_book_profile: PublicBookProfileKind,
    ) -> Self {
        self.trigger_book_profile = Some(trigger_book_profile);
        self.validation_book_profile = Some(validation_book_profile);
        let profile_marker = format!(
            "trigger_book_profile={};validation_book_profile={}",
            trigger_book_profile, validation_book_profile
        );
        self.book.source_route = Some(match self.book.source_route {
            Some(source_route) if !source_route.is_empty() => {
                format!("{source_route};{profile_marker}")
            }
            _ => profile_marker,
        });
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Opportunity {
    pub opportunity_id: String,
    #[serde(default)]
    pub bundle_id: String,
    pub canonical_symbol: CanonicalSymbol,
    pub long_exchange: ExchangeId,
    pub short_exchange: ExchangeId,
    #[serde(default)]
    pub long_exchange_symbol: String,
    #[serde(default)]
    pub short_exchange_symbol: String,
    pub maker_exchange: ExchangeId,
    pub taker_exchange: ExchangeId,
    pub maker_side: OrderSide,
    pub taker_side: OrderSide,
    pub maker_leg_kind: MakerLegKind,
    pub maker_price: f64,
    #[serde(default)]
    pub maker_price_tick: Option<f64>,
    #[serde(default)]
    pub maker_best_opposite_price: Option<f64>,
    pub maker_book_spread_pct: f64,
    pub taker_vwap: Option<f64>,
    pub target_notional_usdt: f64,
    #[serde(default)]
    pub long_limit_price: Option<f64>,
    #[serde(default)]
    pub short_limit_price: Option<f64>,
    #[serde(default)]
    pub normalized_base_qty: Option<f64>,
    #[serde(default)]
    pub expected_net_edge: f64,
    #[serde(default)]
    pub max_slippage_pct: Option<f64>,
    #[serde(default)]
    pub trigger_book_profile: Option<PublicBookProfileKind>,
    #[serde(default)]
    pub validation_book_profile: Option<PublicBookProfileKind>,
    pub maker_quantity: Option<f64>,
    pub taker_quantity: Option<f64>,
    pub maker_notional_usdt: Option<f64>,
    pub taker_notional_usdt: Option<f64>,
    pub executable_notional_usdt: f64,
    pub maker_leg_size: Option<ExchangeLegSize>,
    pub taker_leg_size: Option<ExchangeLegSize>,
    pub raw_open_spread: f64,
    pub maker_taker_net_edge: f64,
    pub open_fee_est_usdt: f64,
    pub close_fee_est_usdt: f64,
    pub expected_funding_usdt: f64,
    pub slippage_pct: f64,
    pub depth_notional_usdt: f64,
    pub book_age_ms: i64,
    pub route_status: RouteStatus,
    pub can_open: bool,
    pub reject_reasons: Vec<RejectReason>,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl Opportunity {
    pub fn route(&self) -> StrategyRoute {
        StrategyRoute {
            long_exchange: self.long_exchange.clone(),
            short_exchange: self.short_exchange.clone(),
            maker_exchange: self.maker_exchange.clone(),
            taker_exchange: self.taker_exchange.clone(),
            maker_side: self.maker_side,
            taker_side: self.taker_side,
            maker_leg_kind: self.maker_leg_kind,
        }
    }
}

pub fn scan_opportunities(
    canonical_symbol: &CanonicalSymbol,
    snapshots: &[MarketSnapshot],
    config: &CrossExchangeArbitrageConfig,
    now: DateTime<Utc>,
) -> Vec<Opportunity> {
    let fee_model = FeeModel::from_config(&config.fees);
    let funding_model = FundingModel::new(config.funding.max_adverse_funding_rate);
    let mut opportunities = Vec::new();

    for long_snapshot in snapshots {
        for short_snapshot in snapshots {
            if long_snapshot.book.exchange == short_snapshot.book.exchange {
                continue;
            }
            if long_snapshot.book.canonical_symbol != *canonical_symbol
                || short_snapshot.book.canonical_symbol != *canonical_symbol
            {
                continue;
            }

            opportunities.push(build_opportunity(
                canonical_symbol,
                long_snapshot,
                short_snapshot,
                MakerLegKind::LongMakerBuy,
                config,
                &fee_model,
                &funding_model,
                now,
            ));
        }
    }

    opportunities.sort_by(|a, b| {
        b.maker_taker_net_edge
            .partial_cmp(&a.maker_taker_net_edge)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                b.maker_book_spread_pct
                    .partial_cmp(&a.maker_book_spread_pct)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    });
    opportunities
}

#[allow(clippy::too_many_arguments)]
fn build_opportunity(
    canonical_symbol: &CanonicalSymbol,
    long_snapshot: &MarketSnapshot,
    short_snapshot: &MarketSnapshot,
    maker_leg_kind: MakerLegKind,
    config: &CrossExchangeArbitrageConfig,
    fee_model: &FeeModel,
    funding_model: &FundingModel,
    now: DateTime<Utc>,
) -> Opportunity {
    let target_notional = config.sizing.target_notional_usdt.clamp(
        config.sizing.min_notional_usdt,
        config.sizing.max_notional_usdt,
    );
    let (
        maker_snapshot,
        taker_snapshot,
        maker_book,
        taker_book,
        maker_side,
        taker_side,
        long_entry_price,
        short_entry_price,
    ) = match maker_leg_kind {
        MakerLegKind::ShortMakerSell => {
            let maker_price = short_snapshot.book.best_ask().map(|level| level.price);
            let taker_vwap =
                calculate_taker_vwap(&long_snapshot.book, OrderSide::Buy, target_notional);
            (
                short_snapshot,
                long_snapshot,
                &short_snapshot.book,
                &long_snapshot.book,
                OrderSide::Sell,
                OrderSide::Buy,
                taker_vwap.vwap_price,
                maker_price,
            )
        }
        MakerLegKind::LongMakerBuy => {
            let maker_price = long_snapshot.book.best_bid().map(|level| level.price);
            let taker_vwap =
                calculate_taker_vwap(&short_snapshot.book, OrderSide::Sell, target_notional);
            (
                long_snapshot,
                short_snapshot,
                &long_snapshot.book,
                &short_snapshot.book,
                OrderSide::Buy,
                OrderSide::Sell,
                maker_price,
                taker_vwap.vwap_price,
            )
        }
    };
    let taker_vwap = calculate_taker_vwap(taker_book, taker_side, target_notional);
    let long_taker_vwap =
        calculate_taker_vwap(&long_snapshot.book, OrderSide::Buy, target_notional);
    let short_taker_vwap =
        calculate_taker_vwap(&short_snapshot.book, OrderSide::Sell, target_notional);
    let long_limit_price = long_snapshot.book.best_ask().map(|level| level.price);
    let short_limit_price = short_snapshot.book.best_bid().map(|level| level.price);
    let maker_price = maker_limit_price(
        maker_book,
        maker_snapshot.instrument.as_ref(),
        maker_side,
        config.execution.maker_price_offset_ticks,
        config.execution.open_execution_style == OpenExecutionStyle::DualTaker,
    )
    .unwrap_or(0.0);
    let maker_price_tick = maker_snapshot
        .instrument
        .as_ref()
        .map(|instrument| instrument.price_tick)
        .filter(|tick| tick.is_finite() && *tick > 0.0);
    let maker_best_opposite_price = match maker_side {
        OrderSide::Buy => maker_book.best_ask().map(|level| level.price),
        OrderSide::Sell => maker_book.best_bid().map(|level| level.price),
    };
    let maker_book_spread_pct = book_spread_pct(maker_book);
    let sized_pair = if config.execution.open_execution_style == OpenExecutionStyle::DualTaker {
        match (&long_snapshot.instrument, &short_snapshot.instrument) {
            (Some(long_instrument), Some(short_instrument)) => {
                Some(size_dual_taker_pair_for_notional(
                    long_instrument,
                    long_taker_vwap
                        .vwap_price
                        .or(long_limit_price)
                        .unwrap_or(0.0),
                    long_limit_price,
                    short_instrument,
                    short_taker_vwap
                        .vwap_price
                        .or(short_limit_price)
                        .unwrap_or(0.0),
                    short_limit_price,
                    target_notional,
                ))
            }
            _ => None,
        }
    } else {
        match (&maker_snapshot.instrument, &taker_snapshot.instrument) {
            (Some(maker_instrument), Some(taker_instrument)) => Some(size_hedge_pair_for_notional(
                maker_instrument,
                maker_price,
                Some(maker_price),
                taker_instrument,
                taker_vwap.vwap_price.unwrap_or(0.0),
                target_notional,
            )),
            _ => None,
        }
    };
    let maker_leg_size = sized_pair.as_ref().map(|pair| pair.maker.clone());
    let taker_leg_size = sized_pair.as_ref().map(|pair| pair.taker.clone());
    let maker_notional_usdt = maker_leg_size
        .as_ref()
        .and_then(|leg| leg.normalized_notional_usdt);
    let taker_notional_usdt = taker_leg_size
        .as_ref()
        .and_then(|leg| leg.normalized_notional_usdt);
    let executable_notional =
        executable_notional_usdt(target_notional, maker_notional_usdt, taker_notional_usdt);
    let normalized_base_qty = sized_pair.as_ref().and_then(|pair| {
        let qty = pair
            .maker
            .normalized_base_quantity
            .min(pair.taker.normalized_base_quantity);
        (qty.is_finite() && qty > 0.0).then_some(qty)
    });
    let maker_entry_price = (maker_price > 0.0).then_some(maker_price);
    let (effective_long_entry_price, effective_short_entry_price, effective_slippage_pct) =
        if config.execution.open_execution_style == OpenExecutionStyle::DualTaker {
            (
                long_taker_vwap.vwap_price,
                short_taker_vwap.vwap_price,
                long_taker_vwap
                    .slippage_pct
                    .max(short_taker_vwap.slippage_pct),
            )
        } else {
            match maker_leg_kind {
                MakerLegKind::ShortMakerSell => {
                    (long_entry_price, maker_entry_price, taker_vwap.slippage_pct)
                }
                MakerLegKind::LongMakerBuy => (
                    maker_entry_price,
                    short_entry_price,
                    taker_vwap.slippage_pct,
                ),
            }
        };
    let raw_open_spread = match (effective_long_entry_price, effective_short_entry_price) {
        (Some(long_price), Some(short_price)) if long_price > 0.0 => short_price / long_price - 1.0,
        _ => -1.0,
    };
    let funding = funding_model.estimate_pair_with_timing(
        executable_notional,
        long_snapshot.funding_rate,
        long_snapshot.next_funding_time,
        executable_notional,
        short_snapshot.funding_rate,
        short_snapshot.next_funding_time,
        now,
        config.funding.no_open_before_funding_mins,
    );
    let fees = if config.execution.open_execution_style == OpenExecutionStyle::DualTaker {
        fee_model.estimate_dual_taker_round_trip(
            &long_snapshot.book.exchange,
            &short_snapshot.book.exchange,
            executable_notional,
        )
    } else {
        fee_model.estimate_maker_taker_round_trip(
            &maker_book.exchange,
            &taker_book.exchange,
            executable_notional,
        )
    };
    let maker_non_fill_penalty =
        if config.execution.open_execution_style == OpenExecutionStyle::DualTaker {
            0.0
        } else {
            config.risk.maker_non_fill_penalty
        };
    let maker_taker_net_edge = raw_open_spread
        - fees.total_normal_fee() / executable_notional.max(1.0)
        + funding.net_funding_rate
        - config.risk.taker_slippage_buffer
        - maker_non_fill_penalty
        - config.risk.safety_buffer;
    let route_status = min_route_status(maker_snapshot.route_status, taker_snapshot.route_status);
    let risk_decision = RiskGate::evaluate_open(
        maker_book,
        taker_book,
        route_status,
        route_status,
        &taker_vwap,
        &funding,
        executable_notional,
        config,
        now,
    );
    let mut reject_reasons = risk_decision.reject_reasons;
    if maker_snapshot.instrument.is_none() || taker_snapshot.instrument.is_none() {
        reject_reasons.push(RejectReason::PrecisionInvalid);
    }
    if config.execution.open_execution_style == OpenExecutionStyle::DualTaker {
        if !long_taker_vwap.depth_enough || !short_taker_vwap.depth_enough {
            reject_reasons.push(RejectReason::DepthInsufficient);
        }
        if effective_slippage_pct > config.risk.max_taker_slippage_pct {
            reject_reasons.push(RejectReason::SlippageTooHigh);
        }
        if let Some(base_quantity) = normalized_base_qty {
            if requires_l1_base_quantity_check(long_snapshot, short_snapshot)
                && (!best_level_covers_base_quantity(
                    &long_snapshot.book,
                    OrderSide::Buy,
                    base_quantity,
                ) || !best_level_covers_base_quantity(
                    &short_snapshot.book,
                    OrderSide::Sell,
                    base_quantity,
                ))
            {
                reject_reasons.push(RejectReason::DepthInsufficient);
            }
        }
    }
    if sized_pair
        .as_ref()
        .map(|pair| !pair.executable)
        .unwrap_or(false)
    {
        reject_reasons.push(RejectReason::PrecisionInvalid);
    }
    if maker_notional_usdt
        .map(|notional| notional > config.sizing.max_notional_usdt)
        .unwrap_or(false)
        || taker_notional_usdt
            .map(|notional| notional > config.sizing.max_notional_usdt)
            .unwrap_or(false)
    {
        reject_reasons.push(RejectReason::NotionalOverLimit);
    }
    warn_precision_notional_roundup(
        canonical_symbol,
        &maker_book.exchange,
        maker_notional_usdt,
        target_notional,
        config.sizing.max_notional_usdt,
    );
    warn_precision_notional_roundup(
        canonical_symbol,
        &taker_book.exchange,
        taker_notional_usdt,
        target_notional,
        config.sizing.max_notional_usdt,
    );
    let route_thresholds = config
        .thresholds
        .route_thresholds(&long_snapshot.book.exchange, &short_snapshot.book.exchange);
    if raw_open_spread < config.thresholds.min_display_raw_spread
        || raw_open_spread < route_thresholds.min_open_raw_spread
    {
        reject_reasons.push(RejectReason::RawSpreadTooSmall);
    }
    if raw_open_spread > route_thresholds.max_open_raw_spread {
        reject_reasons.push(RejectReason::AbnormalCrossExchangeSpread);
    }
    if maker_taker_net_edge < route_thresholds.min_open_maker_taker_net_edge {
        reject_reasons.push(RejectReason::NetEdgeTooSmall);
    }
    dedup_reject_reasons(&mut reject_reasons);
    let opportunity_id = opportunity_id(
        canonical_symbol,
        &long_snapshot.book.exchange,
        &short_snapshot.book.exchange,
        maker_leg_kind,
        now,
    );
    let long_trigger_book_profile = trigger_book_profile(long_snapshot);
    let short_trigger_book_profile = trigger_book_profile(short_snapshot);
    let long_validation_book_profile = validation_book_profile(long_snapshot);
    let short_validation_book_profile = validation_book_profile(short_snapshot);
    let trigger_book_profile =
        common_book_profile(long_trigger_book_profile, short_trigger_book_profile);
    let validation_book_profile =
        common_book_profile(long_validation_book_profile, short_validation_book_profile);
    let dual_taker = config.execution.open_execution_style == OpenExecutionStyle::DualTaker;

    Opportunity {
        opportunity_id: opportunity_id.clone(),
        bundle_id: opportunity_id,
        canonical_symbol: canonical_symbol.clone(),
        long_exchange: long_snapshot.book.exchange.clone(),
        short_exchange: short_snapshot.book.exchange.clone(),
        long_exchange_symbol: long_snapshot.book.exchange_symbol.symbol.clone(),
        short_exchange_symbol: short_snapshot.book.exchange_symbol.symbol.clone(),
        maker_exchange: maker_book.exchange.clone(),
        taker_exchange: taker_book.exchange.clone(),
        maker_side,
        taker_side,
        maker_leg_kind,
        maker_price,
        maker_price_tick,
        maker_best_opposite_price,
        maker_book_spread_pct,
        taker_vwap: taker_vwap.vwap_price,
        target_notional_usdt: target_notional,
        long_limit_price: dual_taker.then_some(long_limit_price).flatten(),
        short_limit_price: dual_taker.then_some(short_limit_price).flatten(),
        normalized_base_qty,
        expected_net_edge: maker_taker_net_edge,
        max_slippage_pct: Some(config.execution.taker_ioc_slippage_limit_pct),
        trigger_book_profile,
        validation_book_profile,
        maker_quantity: maker_leg_size
            .as_ref()
            .map(|leg| leg.normalized_base_quantity),
        taker_quantity: taker_leg_size
            .as_ref()
            .map(|leg| leg.normalized_base_quantity),
        maker_notional_usdt,
        taker_notional_usdt,
        executable_notional_usdt: executable_notional,
        maker_leg_size,
        taker_leg_size,
        raw_open_spread,
        maker_taker_net_edge,
        open_fee_est_usdt: fees.open_fee(),
        close_fee_est_usdt: fees.normal_close_fee(),
        expected_funding_usdt: funding.net_funding,
        slippage_pct: effective_slippage_pct,
        depth_notional_usdt: if dual_taker {
            long_taker_vwap
                .filled_notional_usdt
                .min(short_taker_vwap.filled_notional_usdt)
        } else {
            taker_vwap.filled_notional_usdt
        },
        book_age_ms: book_age_ms(maker_book, now).max(book_age_ms(taker_book, now)),
        route_status,
        can_open: reject_reasons.is_empty(),
        reject_reasons,
        created_at: now,
        expires_at: now + Duration::milliseconds(config.risk.stale_quote_ms),
    }
}

fn common_book_profile(
    a: Option<PublicBookProfileKind>,
    b: Option<PublicBookProfileKind>,
) -> Option<PublicBookProfileKind> {
    match (a, b) {
        (Some(a), Some(b)) if a == b => Some(a),
        (Some(profile), None) | (None, Some(profile)) => Some(profile),
        _ => None,
    }
}

fn requires_l1_base_quantity_check(
    long_snapshot: &MarketSnapshot,
    short_snapshot: &MarketSnapshot,
) -> bool {
    matches!(
        (
            validation_book_profile(long_snapshot),
            validation_book_profile(short_snapshot)
        ),
        (Some(PublicBookProfileKind::FastestL1), _) | (_, Some(PublicBookProfileKind::FastestL1))
    ) || (long_snapshot.book.asks.len() <= 1 && short_snapshot.book.bids.len() <= 1)
}

fn trigger_book_profile(snapshot: &MarketSnapshot) -> Option<PublicBookProfileKind> {
    snapshot
        .trigger_book_profile
        .or_else(|| book_profile_marker(&snapshot.book, "trigger_book_profile"))
        .or_else(|| book_profile_marker(&snapshot.book, "profile_kind"))
}

fn validation_book_profile(snapshot: &MarketSnapshot) -> Option<PublicBookProfileKind> {
    snapshot
        .validation_book_profile
        .or_else(|| book_profile_marker(&snapshot.book, "validation_book_profile"))
        .or_else(|| book_profile_marker(&snapshot.book, "profile_kind"))
}

fn book_profile_marker(book: &OrderBook5, key: &str) -> Option<PublicBookProfileKind> {
    book.source_route
        .as_deref()
        .and_then(|source_route| profile_marker_value(source_route, key))
        .and_then(parse_book_profile_kind)
}

fn profile_marker_value<'a>(source_route: &'a str, key: &str) -> Option<&'a str> {
    source_route
        .split([';', ',', '|', ' '])
        .filter_map(|part| part.split_once('='))
        .find_map(|(candidate_key, value)| (candidate_key == key).then_some(value))
}

fn parse_book_profile_kind(value: &str) -> Option<PublicBookProfileKind> {
    match value {
        "fastest_l1" => Some(PublicBookProfileKind::FastestL1),
        "fastest_depth" => Some(PublicBookProfileKind::FastestDepth),
        "conservative_depth" => Some(PublicBookProfileKind::ConservativeDepth),
        _ => None,
    }
}

fn warn_precision_notional_roundup(
    canonical_symbol: &CanonicalSymbol,
    exchange: &ExchangeId,
    notional: Option<f64>,
    target_notional: f64,
    max_notional: f64,
) {
    let Some(notional) = notional else {
        return;
    };
    let warning_floor = (target_notional + 0.2).min(max_notional);
    if notional <= warning_floor || notional > max_notional {
        return;
    }
    static WARNED: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    let key = format!("{exchange}:{canonical_symbol}");
    let mut warned = WARNED
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if !warned.insert(key) {
        return;
    }
    log::warn!(
        "cross-arb sizing rounded notional above target but within max exchange={} symbol={} target_notional_usdt={} rounded_notional_usdt={} max_notional_usdt={}",
        exchange,
        canonical_symbol,
        target_notional,
        notional,
        max_notional
    );
}

fn maker_limit_price(
    book: &OrderBook5,
    instrument: Option<&InstrumentMeta>,
    side: OrderSide,
    offset_ticks: u32,
    dual_taker: bool,
) -> Option<f64> {
    let price = match side {
        OrderSide::Sell => book.best_ask()?.price,
        OrderSide::Buy => book.best_bid()?.price,
    };
    if dual_taker || offset_ticks == 0 {
        return Some(price);
    }
    let Some(instrument) = instrument else {
        return Some(price);
    };
    let tick = instrument.price_tick;
    if !tick.is_finite() || tick <= 0.0 {
        return Some(price);
    }
    let base_price = match side {
        OrderSide::Sell => instrument.quantize_price(price, RoundingMode::Ceil),
        OrderSide::Buy => instrument.quantize_price(price, RoundingMode::Floor),
    };
    let offset = tick * f64::from(offset_ticks);
    let adjusted = match side {
        OrderSide::Sell => base_price + offset,
        OrderSide::Buy => base_price - offset,
    };
    if adjusted <= 0.0 || !adjusted.is_finite() {
        return Some(price);
    }
    Some(
        instrument
            .quantize_price(adjusted, RoundingMode::Nearest)
            .max(tick),
    )
}

fn book_spread_pct(book: &OrderBook5) -> f64 {
    match (book.best_bid(), book.best_ask()) {
        (Some(bid), Some(ask)) if bid.price > 0.0 && ask.price >= bid.price => {
            ask.price / bid.price - 1.0
        }
        _ => 0.0,
    }
}

fn executable_notional_usdt(
    target_notional: f64,
    maker_notional: Option<f64>,
    taker_notional: Option<f64>,
) -> f64 {
    match (maker_notional, taker_notional) {
        (Some(maker), Some(taker)) if maker.is_finite() && taker.is_finite() => maker.min(taker),
        (Some(maker), None) if maker.is_finite() => maker,
        (None, Some(taker)) if taker.is_finite() => taker,
        _ => target_notional,
    }
    .max(0.0)
}

fn min_route_status(a: RouteStatus, b: RouteStatus) -> RouteStatus {
    use RouteStatus::*;
    match (a, b) {
        (Offline, _) | (_, Offline) => Offline,
        (CloseOnly, _) | (_, CloseOnly) => CloseOnly,
        (Degraded, _) | (_, Degraded) => Degraded,
        _ => Healthy,
    }
}

fn dedup_reject_reasons(reasons: &mut Vec<RejectReason>) {
    let mut seen = std::collections::HashSet::new();
    reasons.retain(|reason| seen.insert(reason.clone()));
}

fn opportunity_id(
    symbol: &CanonicalSymbol,
    long_exchange: &ExchangeId,
    short_exchange: &ExchangeId,
    maker_leg_kind: MakerLegKind,
    now: DateTime<Utc>,
) -> String {
    format!(
        "crossarb-{}-{}-{}-{:?}-{}",
        symbol.to_string().replace('/', ""),
        long_exchange.as_str(),
        short_exchange.as_str(),
        maker_leg_kind,
        now.timestamp_millis()
    )
    .to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::{BookLevel, ContractType, ExchangeSymbol, InstrumentStatus};
    use crate::strategies::cross_exchange_arbitrage::RouteThresholdOverride;

    fn book(exchange: ExchangeId, bid: f64, ask: f64, qty: f64) -> OrderBook5 {
        OrderBook5::new(
            exchange.clone(),
            CanonicalSymbol::new("BTC", "USDT"),
            ExchangeSymbol::new(exchange.clone(), format!("{}BTCUSDT", exchange.as_str())),
            vec![BookLevel::new(bid, qty), BookLevel::new(bid - 1.0, qty)],
            vec![BookLevel::new(ask, qty), BookLevel::new(ask + 1.0, qty)],
            Utc::now(),
            Utc::now(),
            Some(1),
            Some("test".to_string()),
        )
    }

    fn depth_book(exchange: ExchangeId, bids: &[(f64, f64)], asks: &[(f64, f64)]) -> OrderBook5 {
        OrderBook5::new(
            exchange.clone(),
            CanonicalSymbol::new("BTC", "USDT"),
            ExchangeSymbol::new(exchange.clone(), format!("{}BTCUSDT", exchange.as_str())),
            bids.iter()
                .map(|(price, quantity)| BookLevel::new(*price, *quantity))
                .collect(),
            asks.iter()
                .map(|(price, quantity)| BookLevel::new(*price, *quantity))
                .collect(),
            Utc::now(),
            Utc::now(),
            Some(1),
            Some("test".to_string()),
        )
    }

    fn instrument(
        exchange: ExchangeId,
        symbol: &CanonicalSymbol,
        contract_size: f64,
        quantity_step: f64,
        min_notional: f64,
    ) -> InstrumentMeta {
        InstrumentMeta::new(
            exchange.clone(),
            symbol.clone(),
            ExchangeSymbol::new(exchange.clone(), format!("{}BTCUSDT", exchange.as_str())),
            symbol.base(),
            symbol.quote(),
            "USDT",
            ContractType::LinearPerpetual,
            contract_size,
            0.0001,
            quantity_step,
            quantity_step,
            min_notional,
            4,
            4,
            InstrumentStatus::Trading,
        )
    }

    fn config() -> CrossExchangeArbitrageConfig {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.sizing.target_notional_usdt = 100.0;
        config.thresholds.min_open_maker_taker_net_edge = 0.001;
        config.risk.max_book_age_ms = 10_000;
        config
    }

    fn dual_taker_config() -> CrossExchangeArbitrageConfig {
        let mut config = config();
        config.execution.open_execution_style = OpenExecutionStyle::DualTaker;
        config.thresholds.min_display_raw_spread = -1.0;
        config.thresholds.min_open_raw_spread = -1.0;
        config.thresholds.max_open_raw_spread = 1.0;
        config.thresholds.min_open_maker_taker_net_edge = -1.0;
        config.risk.max_taker_slippage_pct = 1.0;
        config.risk.taker_slippage_buffer = 0.0;
        config.risk.maker_non_fill_penalty = 0.0;
        config.risk.safety_buffer = 0.0;
        config
    }

    #[test]
    fn cross_exchange_arbitrage_opportunity_should_find_a_low_b_high() {
        let now = Utc::now();
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let snapshots = vec![
            MarketSnapshot::healthy(book(ExchangeId::Binance, 99.0, 100.0, 10.0)),
            MarketSnapshot::healthy(book(ExchangeId::Okx, 104.0, 105.0, 10.0)),
        ];

        let opportunities = scan_opportunities(&symbol, &snapshots, &config(), now);
        let best = opportunities.first().expect("opportunity");

        assert_eq!(best.long_exchange, ExchangeId::Binance);
        assert_eq!(best.short_exchange, ExchangeId::Okx);
        assert!(best.raw_open_spread > 0.03);
    }

    #[test]
    fn cross_exchange_arbitrage_opportunity_should_find_b_low_a_high() {
        let now = Utc::now();
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let snapshots = vec![
            MarketSnapshot::healthy(book(ExchangeId::Binance, 110.0, 111.0, 10.0)),
            MarketSnapshot::healthy(book(ExchangeId::Okx, 100.0, 101.0, 10.0)),
        ];

        let opportunities = scan_opportunities(&symbol, &snapshots, &config(), now);
        let best = opportunities.first().expect("opportunity");

        assert_eq!(best.long_exchange, ExchangeId::Okx);
        assert_eq!(best.short_exchange, ExchangeId::Binance);
        assert!(best.raw_open_spread > 0.08);
    }

    #[test]
    fn cross_exchange_arbitrage_opportunity_should_reject_after_fees_when_edge_is_low() {
        let now = Utc::now();
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let snapshots = vec![
            MarketSnapshot::healthy(book(ExchangeId::Binance, 99.9, 100.0, 10.0)),
            MarketSnapshot::healthy(book(ExchangeId::Okx, 100.1, 100.2, 10.0)),
        ];

        let opportunities = scan_opportunities(
            &symbol,
            &snapshots,
            &CrossExchangeArbitrageConfig::default(),
            now,
        );

        assert!(opportunities.iter().any(|opportunity| opportunity
            .reject_reasons
            .contains(&RejectReason::NetEdgeTooSmall)));
    }

    #[test]
    fn cross_exchange_arbitrage_opportunity_should_reject_depth_insufficient() {
        let now = Utc::now();
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let snapshots = vec![
            MarketSnapshot::healthy(book(ExchangeId::Binance, 99.0, 100.0, 0.01)),
            MarketSnapshot::healthy(book(ExchangeId::Okx, 104.0, 105.0, 0.01)),
        ];

        let opportunities = scan_opportunities(&symbol, &snapshots, &config(), now);

        assert!(opportunities.iter().any(|opportunity| opportunity
            .reject_reasons
            .contains(&RejectReason::DepthInsufficient)));
    }

    #[test]
    fn cross_exchange_arbitrage_opportunity_should_reject_abnormal_cross_exchange_spread() {
        let now = Utc::now();
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let snapshots = vec![
            MarketSnapshot::healthy(book(ExchangeId::Binance, 1.0, 1.01, 1_000.0)),
            MarketSnapshot::healthy(book(ExchangeId::Okx, 15.0, 15.1, 1_000.0)),
        ];

        let opportunities = scan_opportunities(&symbol, &snapshots, &config(), now);

        assert!(opportunities.iter().any(|opportunity| opportunity
            .reject_reasons
            .contains(&RejectReason::AbnormalCrossExchangeSpread)));
        assert!(opportunities
            .iter()
            .all(|opportunity| !opportunity.can_open));
    }

    #[test]
    fn cross_exchange_arbitrage_opportunity_should_reject_near_negative_funding_settlement() {
        let now = Utc::now();
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let mut config = config();
        config.funding.no_open_before_funding_mins = 5;

        let mut long_snapshot =
            MarketSnapshot::healthy(book(ExchangeId::Binance, 99.0, 100.0, 10.0));
        long_snapshot.funding_rate = 0.001;
        long_snapshot.next_funding_time = Some(now + Duration::minutes(3));

        let mut short_snapshot = MarketSnapshot::healthy(book(ExchangeId::Okx, 104.0, 105.0, 10.0));
        short_snapshot.funding_rate = -0.001;
        short_snapshot.next_funding_time = Some(now + Duration::minutes(10));

        let opportunities =
            scan_opportunities(&symbol, &[long_snapshot, short_snapshot], &config, now);

        assert!(opportunities.iter().any(|opportunity| opportunity
            .reject_reasons
            .contains(&RejectReason::FundingWindowTooClose)));
    }

    #[test]
    fn cross_exchange_arbitrage_opportunity_should_reject_close_only_route() {
        let now = Utc::now();
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let mut close_only = MarketSnapshot::healthy(book(ExchangeId::Binance, 99.0, 100.0, 10.0));
        close_only.route_status = RouteStatus::CloseOnly;
        let healthy = MarketSnapshot::healthy(book(ExchangeId::Okx, 104.0, 105.0, 10.0));

        let opportunities = scan_opportunities(&symbol, &[close_only, healthy], &config(), now);

        assert!(opportunities
            .iter()
            .all(|opportunity| opportunity.route_status == RouteStatus::CloseOnly));
        assert!(opportunities
            .iter()
            .all(|opportunity| !opportunity.can_open));
        assert!(opportunities.iter().all(|opportunity| opportunity
            .reject_reasons
            .contains(&RejectReason::RouteUnhealthy)));
    }

    #[test]
    fn cross_exchange_arbitrage_opportunity_should_apply_exchange_precision_sizing() {
        let now = Utc::now();
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let long_snapshot = MarketSnapshot::healthy(book(ExchangeId::Binance, 99.0, 100.0, 10.0))
            .with_instrument(instrument(ExchangeId::Binance, &symbol, 1.0, 0.1, 5.0));
        let short_snapshot = MarketSnapshot::healthy(book(ExchangeId::Okx, 104.0, 105.0, 10.0))
            .with_instrument(instrument(ExchangeId::Okx, &symbol, 10.0, 0.1, 5.0));

        let opportunities =
            scan_opportunities(&symbol, &[long_snapshot, short_snapshot], &config(), now);
        let best = opportunities.first().expect("opportunity");

        assert_eq!(best.target_notional_usdt, 100.0);
        assert!(best.maker_quantity.is_some());
        assert!(best.taker_quantity.is_some());
        assert!(best.maker_notional_usdt.unwrap() >= 99.0);
        assert!(best.maker_notional_usdt.unwrap() <= 101.0);
        assert!(best.taker_notional_usdt.unwrap() >= 100.0);
        assert!(best.taker_notional_usdt.unwrap() <= 110.0);
        assert!(best.executable_notional_usdt >= 99.0);
        assert!(best.executable_notional_usdt <= 101.0);
        assert!(!best
            .reject_reasons
            .contains(&RejectReason::PrecisionInvalid));
    }

    #[test]
    fn cross_exchange_arbitrage_opportunity_should_require_instrument_metadata_before_open() {
        let now = Utc::now();
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let long_snapshot = MarketSnapshot::healthy(book(ExchangeId::Binance, 99.0, 100.0, 10.0));
        let short_snapshot = MarketSnapshot::healthy(book(ExchangeId::Okx, 104.0, 105.0, 10.0))
            .with_instrument(instrument(ExchangeId::Okx, &symbol, 10.0, 0.1, 5.0));
        let mut config = config();
        config.thresholds.min_open_raw_spread = 0.0;
        config.thresholds.min_open_maker_taker_net_edge = -1.0;

        let opportunities =
            scan_opportunities(&symbol, &[long_snapshot, short_snapshot], &config, now);

        assert!(!opportunities.is_empty());
        assert!(opportunities
            .iter()
            .all(|opportunity| !opportunity.can_open));
        assert!(opportunities.iter().all(|opportunity| opportunity
            .reject_reasons
            .contains(&RejectReason::PrecisionInvalid)));
    }

    #[test]
    fn cross_exchange_arbitrage_opportunity_should_reject_when_sized_leg_below_min_notional() {
        let now = Utc::now();
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let long_snapshot = MarketSnapshot::healthy(book(ExchangeId::Binance, 99.0, 100.0, 10.0))
            .with_instrument(instrument(ExchangeId::Binance, &symbol, 1.0, 0.1, 150.0));
        let short_snapshot = MarketSnapshot::healthy(book(ExchangeId::Okx, 104.0, 105.0, 10.0))
            .with_instrument(instrument(ExchangeId::Okx, &symbol, 1.0, 0.1, 5.0));

        let opportunities =
            scan_opportunities(&symbol, &[long_snapshot, short_snapshot], &config(), now);

        assert!(opportunities.iter().any(|opportunity| opportunity
            .reject_reasons
            .contains(&RejectReason::PrecisionInvalid)));
    }

    #[test]
    fn cross_exchange_arbitrage_opportunity_dual_taker_should_price_buy_ask_and_sell_bid_once() {
        let now = Utc::now();
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let mut config = config();
        config.execution.open_execution_style = OpenExecutionStyle::DualTaker;
        config.thresholds.min_open_maker_taker_net_edge = -1.0;
        config.risk.taker_slippage_buffer = 0.0;
        config.risk.maker_non_fill_penalty = 0.0;
        config.risk.safety_buffer = 0.0;

        let long_snapshot = MarketSnapshot::healthy(book(ExchangeId::Binance, 99.0, 100.0, 10.0));
        let short_snapshot = MarketSnapshot::healthy(book(ExchangeId::Okx, 104.0, 105.0, 10.0));

        let opportunities =
            scan_opportunities(&symbol, &[long_snapshot, short_snapshot], &config, now);
        let route = opportunities
            .iter()
            .find(|opportunity| {
                opportunity.long_exchange == ExchangeId::Binance
                    && opportunity.short_exchange == ExchangeId::Okx
            })
            .expect("dual-taker route");

        assert_eq!(route.raw_open_spread, 104.0 / 100.0 - 1.0);
        assert_eq!(route.maker_side, OrderSide::Buy);
        assert_eq!(route.taker_side, OrderSide::Sell);
        assert_eq!(
            opportunities
                .iter()
                .filter(|opportunity| {
                    opportunity.long_exchange == ExchangeId::Binance
                        && opportunity.short_exchange == ExchangeId::Okx
                })
                .count(),
            1
        );
    }

    #[test]
    fn cross_exchange_arbitrage_opportunity_dual_taker_should_emit_candidate_fields_for_both_directions(
    ) {
        let now = Utc::now();
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let config = dual_taker_config();
        let binance = MarketSnapshot::healthy(book(ExchangeId::Binance, 99.0, 100.0, 10.0))
            .with_instrument(instrument(ExchangeId::Binance, &symbol, 1.0, 0.001, 5.0))
            .with_book_profiles(
                PublicBookProfileKind::FastestL1,
                PublicBookProfileKind::FastestDepth,
            );
        let okx = MarketSnapshot::healthy(book(ExchangeId::Okx, 104.0, 105.0, 10.0))
            .with_instrument(instrument(ExchangeId::Okx, &symbol, 1.0, 0.001, 5.0))
            .with_book_profiles(
                PublicBookProfileKind::FastestL1,
                PublicBookProfileKind::FastestDepth,
            );

        let opportunities = scan_opportunities(&symbol, &[binance, okx], &config, now);

        assert!(opportunities.iter().any(|opportunity| {
            opportunity.long_exchange == ExchangeId::Binance
                && opportunity.short_exchange == ExchangeId::Okx
        }));
        assert!(opportunities.iter().any(|opportunity| {
            opportunity.long_exchange == ExchangeId::Okx
                && opportunity.short_exchange == ExchangeId::Binance
        }));
        let route = opportunities
            .iter()
            .find(|opportunity| {
                opportunity.long_exchange == ExchangeId::Binance
                    && opportunity.short_exchange == ExchangeId::Okx
            })
            .expect("binance long / okx short route");

        assert_eq!(route.bundle_id, route.opportunity_id);
        assert_eq!(route.long_exchange_symbol, "binanceBTCUSDT");
        assert_eq!(route.short_exchange_symbol, "okxBTCUSDT");
        assert_eq!(route.long_limit_price, Some(100.0));
        assert_eq!(route.short_limit_price, Some(104.0));
        assert!(route.normalized_base_qty.unwrap_or(0.0) > 0.0);
        assert_eq!(route.expected_net_edge, route.maker_taker_net_edge);
        assert_eq!(
            route.max_slippage_pct,
            Some(config.execution.taker_ioc_slippage_limit_pct)
        );
        assert_eq!(
            route.trigger_book_profile,
            Some(PublicBookProfileKind::FastestL1)
        );
        assert_eq!(
            route.validation_book_profile,
            Some(PublicBookProfileKind::FastestDepth)
        );
    }

    #[test]
    fn cross_exchange_arbitrage_opportunity_dual_taker_l1_should_reject_when_best_qty_short() {
        let now = Utc::now();
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let config = dual_taker_config();
        let long_snapshot = MarketSnapshot::healthy(depth_book(
            ExchangeId::Binance,
            &[(99.0, 10.0)],
            &[(100.0, 0.4), (100.1, 10.0)],
        ))
        .with_instrument(instrument(ExchangeId::Binance, &symbol, 1.0, 0.001, 5.0))
        .with_book_profiles(
            PublicBookProfileKind::FastestL1,
            PublicBookProfileKind::FastestL1,
        );
        let short_snapshot = MarketSnapshot::healthy(depth_book(
            ExchangeId::Okx,
            &[(104.0, 0.4), (103.9, 10.0)],
            &[(105.0, 10.0)],
        ))
        .with_instrument(instrument(ExchangeId::Okx, &symbol, 1.0, 0.001, 5.0))
        .with_book_profiles(
            PublicBookProfileKind::FastestL1,
            PublicBookProfileKind::FastestL1,
        );

        let route = scan_opportunities(&symbol, &[long_snapshot, short_snapshot], &config, now)
            .into_iter()
            .find(|opportunity| {
                opportunity.long_exchange == ExchangeId::Binance
                    && opportunity.short_exchange == ExchangeId::Okx
            })
            .expect("dual taker route");

        assert!(route.normalized_base_qty.unwrap_or(0.0) > 0.4);
        assert!(!route.can_open);
        assert!(route
            .reject_reasons
            .contains(&RejectReason::DepthInsufficient));
    }

    #[test]
    fn cross_exchange_arbitrage_opportunity_dual_taker_depth_should_accept_when_multilevel_depth_enough(
    ) {
        let now = Utc::now();
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let config = dual_taker_config();
        let long_snapshot = MarketSnapshot::healthy(depth_book(
            ExchangeId::Binance,
            &[(99.0, 10.0)],
            &[(100.0, 0.4), (100.1, 10.0)],
        ))
        .with_instrument(instrument(ExchangeId::Binance, &symbol, 1.0, 0.001, 5.0))
        .with_book_profiles(
            PublicBookProfileKind::FastestL1,
            PublicBookProfileKind::FastestDepth,
        );
        let short_snapshot = MarketSnapshot::healthy(depth_book(
            ExchangeId::Okx,
            &[(104.0, 0.4), (103.9, 10.0)],
            &[(105.0, 10.0)],
        ))
        .with_instrument(instrument(ExchangeId::Okx, &symbol, 1.0, 0.001, 5.0))
        .with_book_profiles(
            PublicBookProfileKind::FastestL1,
            PublicBookProfileKind::FastestDepth,
        );

        let route = scan_opportunities(&symbol, &[long_snapshot, short_snapshot], &config, now)
            .into_iter()
            .find(|opportunity| {
                opportunity.long_exchange == ExchangeId::Binance
                    && opportunity.short_exchange == ExchangeId::Okx
            })
            .expect("dual taker route");

        assert!(route.can_open, "{:?}", route.reject_reasons);
        assert!(route.depth_notional_usdt >= 100.0);
        assert!(route.slippage_pct > 0.0);
    }

    #[test]
    fn cross_exchange_arbitrage_opportunity_dual_taker_depth_should_reject_when_either_leg_short() {
        let now = Utc::now();
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let config = dual_taker_config();
        let long_snapshot = MarketSnapshot::healthy(depth_book(
            ExchangeId::Binance,
            &[(99.0, 10.0)],
            &[(100.0, 0.2), (100.1, 0.2)],
        ))
        .with_instrument(instrument(ExchangeId::Binance, &symbol, 1.0, 0.001, 5.0))
        .with_book_profiles(
            PublicBookProfileKind::FastestL1,
            PublicBookProfileKind::FastestDepth,
        );
        let short_snapshot = MarketSnapshot::healthy(depth_book(
            ExchangeId::Okx,
            &[(104.0, 10.0), (103.9, 10.0)],
            &[(105.0, 10.0)],
        ))
        .with_instrument(instrument(ExchangeId::Okx, &symbol, 1.0, 0.001, 5.0))
        .with_book_profiles(
            PublicBookProfileKind::FastestL1,
            PublicBookProfileKind::FastestDepth,
        );

        let route = scan_opportunities(&symbol, &[long_snapshot, short_snapshot], &config, now)
            .into_iter()
            .find(|opportunity| {
                opportunity.long_exchange == ExchangeId::Binance
                    && opportunity.short_exchange == ExchangeId::Okx
            })
            .expect("dual taker route");

        assert!(!route.can_open);
        assert!(route.depth_notional_usdt < 100.0);
        assert!(route
            .reject_reasons
            .contains(&RejectReason::DepthInsufficient));
    }

    #[test]
    fn cross_exchange_arbitrage_opportunity_dual_taker_should_not_subtract_maker_non_fill_penalty()
    {
        let now = Utc::now();
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let long_snapshot = MarketSnapshot::healthy(book(ExchangeId::Binance, 99.0, 100.0, 10.0));
        let short_snapshot = MarketSnapshot::healthy(book(ExchangeId::Okx, 104.0, 105.0, 10.0));

        let mut baseline = config();
        baseline.execution.open_execution_style = OpenExecutionStyle::DualTaker;
        baseline.thresholds.min_open_maker_taker_net_edge = -1.0;
        baseline.risk.taker_slippage_buffer = 0.0;
        baseline.risk.maker_non_fill_penalty = 0.0;
        baseline.risk.safety_buffer = 0.0;

        let mut penalized = baseline.clone();
        penalized.risk.maker_non_fill_penalty = 0.01;

        let baseline_opportunity = scan_opportunities(
            &symbol,
            &[long_snapshot.clone(), short_snapshot.clone()],
            &baseline,
            now,
        )
        .into_iter()
        .find(|opportunity| {
            opportunity.long_exchange == ExchangeId::Binance
                && opportunity.short_exchange == ExchangeId::Okx
        })
        .expect("baseline dual-taker route");
        let penalized_opportunity =
            scan_opportunities(&symbol, &[long_snapshot, short_snapshot], &penalized, now)
                .into_iter()
                .find(|opportunity| {
                    opportunity.long_exchange == ExchangeId::Binance
                        && opportunity.short_exchange == ExchangeId::Okx
                })
                .expect("penalized dual-taker route");

        assert_eq!(
            baseline_opportunity.maker_taker_net_edge,
            penalized_opportunity.maker_taker_net_edge
        );
    }

    #[test]
    fn cross_exchange_arbitrage_maker_taker_should_only_emit_long_maker_buy_routes() {
        let now = Utc::now();
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let snapshots = vec![
            MarketSnapshot::healthy(book(ExchangeId::Binance, 99.0, 100.0, 10.0)),
            MarketSnapshot::healthy(book(ExchangeId::Okx, 104.0, 105.0, 10.0)),
        ];

        let opportunities = scan_opportunities(&symbol, &snapshots, &config(), now);

        assert!(!opportunities.is_empty());
        assert!(opportunities
            .iter()
            .all(|opportunity| opportunity.maker_leg_kind == MakerLegKind::LongMakerBuy));
        assert!(opportunities
            .iter()
            .all(|opportunity| opportunity.maker_side == OrderSide::Buy));
    }

    #[test]
    fn cross_exchange_arbitrage_maker_price_offset_should_move_buy_down_one_tick() {
        let now = Utc::now();
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let mut config = config();
        config.execution.maker_price_offset_ticks = 1;
        config.thresholds.min_open_maker_taker_net_edge = -1.0;
        config.risk.maker_non_fill_penalty = 0.0;
        config.risk.taker_slippage_buffer = 0.0;
        config.risk.safety_buffer = 0.0;

        let long_snapshot = MarketSnapshot::healthy(book(ExchangeId::Binance, 99.0, 100.0, 10.0))
            .with_instrument(instrument(ExchangeId::Binance, &symbol, 1.0, 0.1, 5.0));
        let short_snapshot = MarketSnapshot::healthy(book(ExchangeId::Okx, 104.0, 105.0, 10.0))
            .with_instrument(instrument(ExchangeId::Okx, &symbol, 1.0, 0.1, 5.0));

        let best = scan_opportunities(&symbol, &[long_snapshot, short_snapshot], &config, now)
            .into_iter()
            .find(|opportunity| {
                opportunity.long_exchange == ExchangeId::Binance
                    && opportunity.short_exchange == ExchangeId::Okx
            })
            .expect("maker route");

        assert_eq!(best.maker_price, 98.9999);
        assert_eq!(best.raw_open_spread, 104.0 / 98.9999 - 1.0);
        assert!(best.maker_book_spread_pct > 0.0);
    }

    #[test]
    fn cross_exchange_arbitrage_route_override_should_require_higher_raw_spread() {
        let now = Utc::now();
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let long_snapshot = MarketSnapshot::healthy(book(ExchangeId::Binance, 100.0, 101.0, 10.0));
        let short_snapshot = MarketSnapshot::healthy(book(ExchangeId::Gate, 101.5, 102.0, 10.0));
        let mut config = config();
        config.execution.open_execution_style = OpenExecutionStyle::DualTaker;
        config.thresholds.min_open_raw_spread = 0.001;
        config.thresholds.min_open_maker_taker_net_edge = -1.0;
        config.thresholds.route_overrides = vec![RouteThresholdOverride {
            long_exchange: ExchangeId::Binance,
            short_exchange: ExchangeId::Gate,
            min_open_raw_spread: Some(0.01),
            min_open_maker_taker_net_edge: None,
            max_open_raw_spread: None,
        }];

        let opportunities =
            scan_opportunities(&symbol, &[long_snapshot, short_snapshot], &config, now);
        let route = opportunities
            .iter()
            .find(|opportunity| {
                opportunity.long_exchange == ExchangeId::Binance
                    && opportunity.short_exchange == ExchangeId::Gate
            })
            .expect("route");

        assert!(route.raw_open_spread < 0.01);
        assert!(route
            .reject_reasons
            .contains(&RejectReason::RawSpreadTooSmall));
    }
}
