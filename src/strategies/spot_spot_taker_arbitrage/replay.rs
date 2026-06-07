use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use anyhow::{anyhow, Result};
use chrono::{DateTime, Duration, Utc};
use rand::{rngs::StdRng, Rng, SeedableRng};

use super::{
    configured_spot_pair, detect_opportunities_for_pair_with_source,
    fee_model_with_strategy_overrides, BookRecord, BookSource, CachedBook, CommonSymbolRules,
    OpportunityRecord, PaperInventory, RejectionReason, ReplayReport, ReplayReportBuilder,
    RiskState, SimulatedTradeRecord, SpotSpotTakerArbitrageConfig, SpotVenue,
};
use crate::exchanges::unified::{
    validate_min_notional, validate_quantity_step, MarketType, OrderBookLevel, OrderBookSnapshot,
    OrderSide, OrderType, SymbolRule, SymbolStatus, TimeInForce,
};
use crate::execution::FeeModel;
use crate::risk::DisabledRegistry;
use crate::utils::money;

pub async fn run_replay_mode(config: SpotSpotTakerArbitrageConfig) -> Result<ReplayReport> {
    config.validate_safe_mode()?;
    let model = ReplayExecutionModel::from_speed(&config)?;
    let mut rng = StdRng::seed_from_u64(model.seed);
    let mut events = load_book_records(&config.replay.input_path)?
        .into_iter()
        .map(|record| ReplayScheduledEvent::from_record(record, &model, &mut rng))
        .collect::<Vec<_>>();
    events.sort_by_key(|event| event.source_at);
    let mut scheduled_events = events.clone();
    scheduled_events.sort_by_key(|event| (event.visible_at, event.source_at));
    let mut visible_books = ReplayBookState::default();
    let mut report = ReplayReportBuilder::default();
    let mut inventory = PaperInventory::from_config(&config)?;
    let fee_model = fee_model_with_strategy_overrides(
        FeeModel::load_or_default(&config.fee_config_path),
        &config,
    );
    let disabled_registry = DisabledRegistry::load_or_empty(&config.disabled_registry_path);
    inventory.exclude_unmanaged_positions(&disabled_registry);
    let mut risk = RiskState::new(&config);
    let rules = replay_symbol_rules(&config);

    for event in &scheduled_events {
        if !within_replay_window(&config, event.source_at) {
            continue;
        }
        report.record_book_event(event.visible_at);
        visible_books.apply_visible_event(event);
        let exchange = event.cached.exchange.clone();
        let symbol = event.cached.symbol.clone();

        let Some(rules) = rules.get(&symbol) else {
            continue;
        };
        let Some((left_book, right_book)) = visible_books.configured_pair_books(&config, &symbol)
        else {
            continue;
        };
        let left_snapshot = replay_detection_snapshot(&left_book);
        let right_snapshot = replay_detection_snapshot(&right_book);
        let opportunities = detect_opportunities_for_pair_with_source(
            &config,
            rules,
            &left_snapshot,
            &right_snapshot,
            &inventory,
            &risk,
            &fee_model,
            &disabled_registry,
            BookSource::Replay,
            BookSource::Replay,
        );
        for mut opportunity in opportunities {
            stamp_opportunity_replay_time(
                &mut opportunity,
                event.visible_at,
                &left_book,
                &right_book,
            );
            report.record_opportunity(opportunity.clone());
            if opportunity.accepted {
                let timing =
                    ReplayExecutionTiming::sample(event.visible_at, &opportunity, &model, &mut rng);
                let latency_adjusted = latency_adjusted_opportunity(
                    &events,
                    &config,
                    &opportunity,
                    rules,
                    &inventory,
                    &risk,
                    &fee_model,
                    &disabled_registry,
                    &timing,
                );
                report.record_latency_adjusted_opportunity(latency_adjusted.clone());
                let outcome = simulate_replay_ioc_taker_taker(
                    &config,
                    &mut inventory,
                    &opportunity,
                    rules,
                    &events,
                    &timing,
                    &model,
                    &mut rng,
                );
                report.record_execution_result(
                    outcome.trade.is_some(),
                    outcome.rejected,
                    outcome.timed_out,
                    outcome.partial_fill,
                    outcome.one_sided_risk,
                );
                if let Some(trade) = outcome.trade {
                    risk.record_trade(&trade);
                    risk.apply_trade_cooldown(&trade.symbol, config.cooldown_ms_after_trade);
                    report.record_trade(trade);
                } else if let Some(reason) = outcome.rejection_reason {
                    risk.record_rejection(&opportunity.symbol, reason);
                }
            } else if let Some(reason) = opportunity.rejection_reason {
                risk.record_rejection(&opportunity.symbol, reason);
            }
        }
        log::trace!(
            "spot_spot_taker_arbitrage replay applied book exchange={} symbol={}",
            exchange,
            symbol
        );
    }

    let final_report = report.build(config.symbols.len());
    write_replay_report(&config.replay.output_path, &final_report)?;
    Ok(final_report)
}

#[derive(Debug, Clone)]
struct ReplayScheduledEvent {
    source_at: DateTime<Utc>,
    visible_at: DateTime<Utc>,
    cached: CachedBook,
}

impl ReplayScheduledEvent {
    fn from_record(record: BookRecord, model: &ReplayExecutionModel, rng: &mut StdRng) -> Self {
        let cached = record.into_cached();
        let source_at = cached.local_timestamp;
        let market_latency_ms = model.market_latency_ms(&cached.exchange, cached.latency_ms, rng);
        Self {
            source_at,
            visible_at: source_at + Duration::milliseconds(market_latency_ms),
            cached,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct ReplayBookState {
    books: HashMap<(String, String), CachedBook>,
}

impl ReplayBookState {
    fn apply_visible_event(&mut self, event: &ReplayScheduledEvent) {
        self.books.insert(
            book_key(&event.cached.exchange, &event.cached.symbol),
            event.cached.clone(),
        );
    }

    fn configured_pair_books(
        &self,
        config: &SpotSpotTakerArbitrageConfig,
        symbol: &str,
    ) -> Option<(CachedBook, CachedBook)> {
        let (left, right) = configured_spot_pair(&config.exchanges);
        Some((
            self.books.get(&book_key(left.as_str(), symbol))?.clone(),
            self.books.get(&book_key(right.as_str(), symbol))?.clone(),
        ))
    }
}

#[derive(Debug, Clone)]
struct ReplayExecutionModel {
    seed: u64,
    market_latency: ReplayLatencySampler,
    decision_latency: ReplayLatencySampler,
    submit_latency: ReplayLatencySampler,
    use_recorded_market_latency: bool,
    timeout_ms: i64,
    max_slippage_bps: f64,
    fill_probability: f64,
    partial_fill_probability: f64,
    partial_min_ratio: f64,
    partial_max_ratio: f64,
    queue_position: ReplayQueuePositionModel,
}

impl ReplayExecutionModel {
    fn from_speed(config: &SpotSpotTakerArbitrageConfig) -> Result<Self> {
        let raw = config.replay.speed.trim();
        let mut model = Self::default_for_config(config);
        if raw.is_empty() || raw.eq_ignore_ascii_case("max") || raw.eq_ignore_ascii_case("realtime")
        {
            return Ok(model);
        }

        let (mode, body) = raw
            .split_once(':')
            .map(|(mode, body)| (mode.trim().to_ascii_lowercase(), body))
            .unwrap_or_else(|| ("fixed".to_string(), raw));
        let params = parse_replay_model_params(body);

        if let Some(seed) = u64_param(&params, "seed") {
            model.seed = seed;
        }
        if let Some(value) =
            i64_param(&params, "timeout").or_else(|| i64_param(&params, "timeout_ms"))
        {
            model.timeout_ms = value.max(0);
        }
        if let Some(value) =
            f64_param(&params, "slippage").or_else(|| f64_param(&params, "max_slippage_bps"))
        {
            model.max_slippage_bps = value.max(0.0);
        }
        if let Some(value) =
            f64_param(&params, "fill_probability").or_else(|| f64_param(&params, "fill_prob"))
        {
            model.fill_probability = value.clamp(0.0, 1.0);
        }
        if let Some(value) = f64_param(&params, "partial_fill_probability")
            .or_else(|| f64_param(&params, "partial_prob"))
        {
            model.partial_fill_probability = value.clamp(0.0, 1.0);
        }
        if let Some(value) =
            f64_param(&params, "partial_min").or_else(|| f64_param(&params, "partial_min_ratio"))
        {
            model.partial_min_ratio = value.clamp(0.0, 1.0);
        }
        if let Some(value) =
            f64_param(&params, "partial_max").or_else(|| f64_param(&params, "partial_max_ratio"))
        {
            model.partial_max_ratio = value.clamp(0.0, 1.0);
        }
        if model.partial_max_ratio < model.partial_min_ratio {
            std::mem::swap(&mut model.partial_min_ratio, &mut model.partial_max_ratio);
        }
        if let Some(value) = f64_param(&params, "queue_ahead_ratio") {
            model.queue_position.queue_ahead_ratio = value.max(0.0);
        }
        if param_enabled(&params, "recorded_latency")
            || params
                .get("market")
                .is_some_and(|value| value.eq_ignore_ascii_case("recorded"))
        {
            model.use_recorded_market_latency = true;
        }

        match mode.as_str() {
            "fixed" => {
                if let Some(value) =
                    i64_param(&params, "market").or_else(|| i64_param(&params, "latency"))
                {
                    model.market_latency = ReplayLatencySampler::Fixed(value.max(0));
                }
                if let Some(value) = i64_param(&params, "decision") {
                    model.decision_latency = ReplayLatencySampler::Fixed(value.max(0));
                }
                if let Some(value) =
                    i64_param(&params, "submit").or_else(|| i64_param(&params, "order"))
                {
                    model.submit_latency = ReplayLatencySampler::Fixed(value.max(0));
                }
            }
            "uniform" => {
                model.market_latency = uniform_sampler(&params, "market", &model.market_latency);
                model.decision_latency =
                    uniform_sampler(&params, "decision", &model.decision_latency);
                model.submit_latency = uniform_sampler(&params, "submit", &model.submit_latency);
            }
            "normal" => {
                model.market_latency = normal_sampler(&params, "market", &model.market_latency);
                model.decision_latency =
                    normal_sampler(&params, "decision", &model.decision_latency);
                model.submit_latency = normal_sampler(&params, "submit", &model.submit_latency);
            }
            "profile" | "p99" => {
                model.market_latency = ReplayLatencySampler::ExchangeProfile(
                    exchange_latency_profile(&params),
                    i64_param(&params, "fallback").unwrap_or(50).max(0),
                );
                if let Some(value) = i64_param(&params, "decision") {
                    model.decision_latency = ReplayLatencySampler::Fixed(value.max(0));
                }
                if let Some(value) =
                    i64_param(&params, "submit").or_else(|| i64_param(&params, "order"))
                {
                    model.submit_latency = ReplayLatencySampler::Fixed(value.max(0));
                }
            }
            "recorded" => {
                model.use_recorded_market_latency = true;
            }
            other => {
                return Err(anyhow!(
                    "unsupported spot replay speed/model '{}'; use max, fixed:, uniform:, normal:, profile:, p99:, or recorded:",
                    other
                ));
            }
        }

        Ok(model)
    }

    fn default_for_config(config: &SpotSpotTakerArbitrageConfig) -> Self {
        Self {
            seed: 0,
            market_latency: ReplayLatencySampler::Fixed(0),
            decision_latency: ReplayLatencySampler::Fixed(0),
            submit_latency: ReplayLatencySampler::Fixed(0),
            use_recorded_market_latency: false,
            timeout_ms: config.request_timeout_ms as i64,
            max_slippage_bps: config.slippage_bps.max(0.0),
            fill_probability: 1.0,
            partial_fill_probability: 0.0,
            partial_min_ratio: 1.0,
            partial_max_ratio: 1.0,
            queue_position: ReplayQueuePositionModel::default(),
        }
    }

    fn market_latency_ms(
        &self,
        exchange: &str,
        recorded_latency_ms: Option<i64>,
        rng: &mut StdRng,
    ) -> i64 {
        if self.use_recorded_market_latency {
            return recorded_latency_ms.unwrap_or_default().max(0);
        }
        self.market_latency.sample(exchange, rng)
    }

    fn decision_latency_ms(&self, rng: &mut StdRng) -> i64 {
        self.decision_latency.sample("", rng)
    }

    fn submit_latency_ms(&self, exchange: &str, rng: &mut StdRng) -> i64 {
        self.submit_latency.sample(exchange, rng)
    }
}

#[derive(Debug, Clone)]
enum ReplayLatencySampler {
    Fixed(i64),
    Uniform { min_ms: i64, max_ms: i64 },
    Normal { mean_ms: f64, stddev_ms: f64 },
    ExchangeProfile(HashMap<String, i64>, i64),
}

impl ReplayLatencySampler {
    fn sample(&self, exchange: &str, rng: &mut StdRng) -> i64 {
        match self {
            Self::Fixed(value) => (*value).max(0),
            Self::Uniform { min_ms, max_ms } => {
                if max_ms <= min_ms {
                    (*min_ms).max(0)
                } else {
                    rng.gen_range(*min_ms..=*max_ms).max(0)
                }
            }
            Self::Normal { mean_ms, stddev_ms } => {
                let u1 = rng.gen::<f64>().clamp(f64::MIN_POSITIVE, 1.0);
                let u2 = rng.gen::<f64>();
                let z0 = (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos();
                (mean_ms + stddev_ms.max(0.0) * z0).round().max(0.0) as i64
            }
            Self::ExchangeProfile(profile, fallback) => profile
                .get(&normalize_exchange(exchange))
                .copied()
                .unwrap_or(*fallback)
                .max(0),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct ReplayQueuePositionModel {
    queue_ahead_ratio: f64,
}

impl ReplayQueuePositionModel {
    fn maker_queue_ahead_qty(&self, visible_level_qty: f64) -> f64 {
        (visible_level_qty * self.queue_ahead_ratio).max(0.0)
    }
}

#[derive(Debug, Clone)]
struct ReplayExecutionTiming {
    observed_at: DateTime<Utc>,
    decision_at: DateTime<Utc>,
    buy_arrival_at: DateTime<Utc>,
    sell_arrival_at: DateTime<Utc>,
    timeout_at: DateTime<Utc>,
}

impl ReplayExecutionTiming {
    fn sample(
        observed_at: DateTime<Utc>,
        opportunity: &OpportunityRecord,
        model: &ReplayExecutionModel,
        rng: &mut StdRng,
    ) -> Self {
        let decision_at = observed_at + Duration::milliseconds(model.decision_latency_ms(rng));
        let buy_arrival_at = decision_at
            + Duration::milliseconds(model.submit_latency_ms(&opportunity.buy_exchange, rng));
        let sell_arrival_at = decision_at
            + Duration::milliseconds(model.submit_latency_ms(&opportunity.sell_exchange, rng));
        Self {
            observed_at,
            decision_at,
            buy_arrival_at,
            sell_arrival_at,
            timeout_at: decision_at + Duration::milliseconds(model.timeout_ms),
        }
    }

    fn max_arrival_at(&self) -> DateTime<Utc> {
        self.buy_arrival_at.max(self.sell_arrival_at)
    }

    fn total_latency_ms(&self) -> i64 {
        self.max_arrival_at()
            .signed_duration_since(self.observed_at)
            .num_milliseconds()
            .max(0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplayLegStatus {
    Filled,
    Partial,
    Rejected,
    Timeout,
}

#[derive(Debug, Clone)]
struct ReplayLegFill {
    status: ReplayLegStatus,
    requested_quantity: f64,
    filled_quantity: f64,
    average_price: f64,
    notional: f64,
    fee: f64,
    limit_price: f64,
}

impl ReplayLegFill {
    fn rejected(status: ReplayLegStatus, requested_quantity: f64, limit_price: f64) -> Self {
        Self {
            status,
            requested_quantity,
            filled_quantity: 0.0,
            average_price: 0.0,
            notional: 0.0,
            fee: 0.0,
            limit_price,
        }
    }

    fn is_partial(&self) -> bool {
        self.filled_quantity > 1e-12 && self.filled_quantity + 1e-12 < self.requested_quantity
    }
}

#[derive(Debug, Clone, Default)]
struct ReplayExecutionOutcome {
    trade: Option<SimulatedTradeRecord>,
    rejected: bool,
    timed_out: bool,
    partial_fill: bool,
    one_sided_risk: bool,
    rejection_reason: Option<RejectionReason>,
}

fn latency_adjusted_opportunity(
    events: &[ReplayScheduledEvent],
    config: &SpotSpotTakerArbitrageConfig,
    opportunity: &OpportunityRecord,
    rules: &CommonSymbolRules,
    inventory: &PaperInventory,
    risk: &RiskState,
    fee_model: &FeeModel,
    disabled_registry: &DisabledRegistry,
    timing: &ReplayExecutionTiming,
) -> OpportunityRecord {
    let Some(buy_book) = latest_book_at(
        events,
        &opportunity.buy_exchange,
        &opportunity.symbol,
        timing.max_arrival_at(),
    ) else {
        return rejected_latency_opportunity(
            opportunity,
            timing.max_arrival_at(),
            RejectionReason::StaleBook,
        );
    };
    let Some(sell_book) = latest_book_at(
        events,
        &opportunity.sell_exchange,
        &opportunity.symbol,
        timing.max_arrival_at(),
    ) else {
        return rejected_latency_opportunity(
            opportunity,
            timing.max_arrival_at(),
            RejectionReason::StaleBook,
        );
    };

    let buy_snapshot = replay_detection_snapshot(&buy_book);
    let sell_snapshot = replay_detection_snapshot(&sell_book);
    let buy_exchange = parse_exchange(&opportunity.buy_exchange)
        .unwrap_or_else(|_| configured_spot_pair(&config.exchanges).0);
    let sell_exchange = parse_exchange(&opportunity.sell_exchange)
        .unwrap_or_else(|_| configured_spot_pair(&config.exchanges).1);
    let (left_exchange, _) = configured_spot_pair(&config.exchanges);
    let (left_snapshot, right_snapshot, left_book, right_book) = if buy_exchange == left_exchange {
        (&buy_snapshot, &sell_snapshot, &buy_book, &sell_book)
    } else {
        (&sell_snapshot, &buy_snapshot, &sell_book, &buy_book)
    };
    let mut adjusted = detect_opportunities_for_pair_with_source(
        config,
        rules,
        left_snapshot,
        right_snapshot,
        inventory,
        risk,
        fee_model,
        disabled_registry,
        BookSource::Replay,
        BookSource::Replay,
    )
    .into_iter()
    .find(|candidate| {
        candidate.buy_exchange == opportunity.buy_exchange
            && candidate.sell_exchange == opportunity.sell_exchange
    })
    .unwrap_or_else(|| {
        rejected_latency_opportunity(
            opportunity,
            timing.max_arrival_at(),
            RejectionReason::PaperExecutionRejected,
        )
    });
    stamp_opportunity_replay_time(
        &mut adjusted,
        timing.max_arrival_at(),
        left_book,
        right_book,
    );
    adjusted
}

fn rejected_latency_opportunity(
    opportunity: &OpportunityRecord,
    timestamp: DateTime<Utc>,
    reason: RejectionReason,
) -> OpportunityRecord {
    let mut rejected = opportunity.clone();
    rejected.timestamp = timestamp;
    rejected.accepted = false;
    rejected.rejection_reason = Some(reason);
    rejected.rejection_detail = Some("latency-adjusted replay book unavailable".to_string());
    rejected
}

fn simulate_replay_ioc_taker_taker(
    config: &SpotSpotTakerArbitrageConfig,
    inventory: &mut PaperInventory,
    opportunity: &OpportunityRecord,
    rules: &CommonSymbolRules,
    events: &[ReplayScheduledEvent],
    timing: &ReplayExecutionTiming,
    model: &ReplayExecutionModel,
    rng: &mut StdRng,
) -> ReplayExecutionOutcome {
    let buy_exchange = match parse_exchange(&opportunity.buy_exchange) {
        Ok(exchange) => exchange,
        Err(reason) => {
            return ReplayExecutionOutcome {
                rejected: true,
                rejection_reason: Some(reason),
                ..ReplayExecutionOutcome::default()
            }
        }
    };
    let sell_exchange = match parse_exchange(&opportunity.sell_exchange) {
        Ok(exchange) => exchange,
        Err(reason) => {
            return ReplayExecutionOutcome {
                rejected: true,
                rejection_reason: Some(reason),
                ..ReplayExecutionOutcome::default()
            }
        }
    };
    let buy_rule = rules.for_exchange(buy_exchange);
    let sell_rule = rules.for_exchange(sell_exchange);
    if let Err(reason) = validate_replay_order(
        config,
        opportunity,
        buy_rule,
        sell_rule,
        buy_exchange,
        sell_exchange,
        inventory,
    ) {
        return ReplayExecutionOutcome {
            rejected: true,
            rejection_reason: Some(reason),
            ..ReplayExecutionOutcome::default()
        };
    }

    let buy_limit_price = opportunity.buy_price * (1.0 + model.max_slippage_bps / 10_000.0);
    let sell_limit_price = opportunity.sell_price * (1.0 - model.max_slippage_bps / 10_000.0);
    let buy_book = latest_book_at(
        events,
        &opportunity.buy_exchange,
        &opportunity.symbol,
        timing.buy_arrival_at,
    );
    let sell_book = latest_book_at(
        events,
        &opportunity.sell_exchange,
        &opportunity.symbol,
        timing.sell_arrival_at,
    );
    let buy_leg = if timing.buy_arrival_at > timing.timeout_at {
        ReplayLegFill::rejected(
            ReplayLegStatus::Timeout,
            opportunity.quantity,
            buy_limit_price,
        )
    } else if let Some(book) = buy_book.as_ref().filter(|book| !book.is_stale) {
        let _queue_ahead = model
            .queue_position
            .maker_queue_ahead_qty(book.asks.first().map(|level| level.quantity).unwrap_or(0.0));
        simulate_ioc_leg(
            OrderSide::Buy,
            &book.asks,
            opportunity.quantity,
            buy_limit_price,
            opportunity.buy_fee_bps,
            model,
            rng,
        )
    } else {
        ReplayLegFill::rejected(
            ReplayLegStatus::Rejected,
            opportunity.quantity,
            buy_limit_price,
        )
    };
    let sell_leg = if timing.sell_arrival_at > timing.timeout_at {
        ReplayLegFill::rejected(
            ReplayLegStatus::Timeout,
            opportunity.quantity,
            sell_limit_price,
        )
    } else if let Some(book) = sell_book.as_ref().filter(|book| !book.is_stale) {
        let _queue_ahead = model
            .queue_position
            .maker_queue_ahead_qty(book.bids.first().map(|level| level.quantity).unwrap_or(0.0));
        simulate_ioc_leg(
            OrderSide::Sell,
            &book.bids,
            opportunity.quantity,
            sell_limit_price,
            opportunity.sell_fee_bps,
            model,
            rng,
        )
    } else {
        ReplayLegFill::rejected(
            ReplayLegStatus::Rejected,
            opportunity.quantity,
            sell_limit_price,
        )
    };

    settle_replay_fills(
        config,
        inventory,
        opportunity,
        rules,
        buy_exchange,
        sell_exchange,
        &buy_leg,
        &sell_leg,
        timing,
    )
}

fn validate_replay_order(
    config: &SpotSpotTakerArbitrageConfig,
    opportunity: &OpportunityRecord,
    buy_rule: &SymbolRule,
    sell_rule: &SymbolRule,
    buy_exchange: SpotVenue,
    sell_exchange: SpotVenue,
    inventory: &PaperInventory,
) -> Result<(), RejectionReason> {
    validate_quantity_step(opportunity.quantity, buy_rule.step_size)
        .map_err(|_| RejectionReason::SymbolRule)?;
    validate_quantity_step(opportunity.quantity, sell_rule.step_size)
        .map_err(|_| RejectionReason::SymbolRule)?;
    validate_min_notional(
        opportunity.quantity,
        Some(opportunity.buy_price),
        buy_rule.min_notional,
    )
    .map_err(|_| RejectionReason::MinNotional)?;
    validate_min_notional(
        opportunity.quantity,
        Some(opportunity.sell_price),
        sell_rule.min_notional,
    )
    .map_err(|_| RejectionReason::MinNotional)?;
    let quote_needed =
        opportunity.quantity * opportunity.buy_price * (1.0 + opportunity.buy_fee_bps / 10_000.0);
    if inventory
        .balance(buy_exchange, &config.quote_asset)
        .effective_available()
        + 1e-12
        < quote_needed
    {
        return Err(RejectionReason::InsufficientQuoteBalance);
    }
    if inventory
        .balance(sell_exchange, &buy_rule.base_asset)
        .effective_available()
        + 1e-12
        < opportunity.quantity
    {
        return Err(RejectionReason::InsufficientBaseBalance);
    }
    Ok(())
}

fn simulate_ioc_leg(
    side: OrderSide,
    levels: &[OrderBookLevel],
    requested_quantity: f64,
    limit_price: f64,
    taker_fee_bps: f64,
    model: &ReplayExecutionModel,
    rng: &mut StdRng,
) -> ReplayLegFill {
    if requested_quantity <= 0.0 || rng.gen::<f64>() > model.fill_probability {
        return ReplayLegFill::rejected(ReplayLegStatus::Rejected, requested_quantity, limit_price);
    }
    let mut target_quantity = requested_quantity;
    if rng.gen::<f64>() < model.partial_fill_probability {
        let ratio = if (model.partial_max_ratio - model.partial_min_ratio).abs() <= f64::EPSILON {
            model.partial_min_ratio
        } else {
            rng.gen_range(model.partial_min_ratio..=model.partial_max_ratio)
        }
        .clamp(0.0, 1.0);
        target_quantity *= ratio;
    }

    let mut remaining = target_quantity;
    let mut notional = 0.0;
    let mut filled_quantity = 0.0;
    for level in levels {
        if remaining <= 1e-12 {
            break;
        }
        let price_crosses = match side {
            OrderSide::Buy => level.price <= limit_price + 1e-12,
            OrderSide::Sell => level.price + 1e-12 >= limit_price,
        };
        if !price_crosses {
            break;
        }
        let fill_qty = remaining.min(level.quantity);
        let fill_notional =
            money::notional_f64(level.price, fill_qty).unwrap_or(level.price * fill_qty);
        filled_quantity += fill_qty;
        notional = money::add_f64(notional, fill_notional, "notional", "fill_notional")
            .unwrap_or(notional + fill_notional);
        remaining -= fill_qty;
    }

    if filled_quantity <= 1e-12 {
        return ReplayLegFill::rejected(ReplayLegStatus::Rejected, requested_quantity, limit_price);
    }
    let average_price = money::divide_f64(notional, filled_quantity, "notional", "quantity")
        .unwrap_or(notional / filled_quantity);
    let fee = money::fee_amount_f64(notional, taker_fee_bps)
        .unwrap_or(notional * taker_fee_bps / 10_000.0);
    let status = if filled_quantity + 1e-12 >= requested_quantity {
        ReplayLegStatus::Filled
    } else {
        ReplayLegStatus::Partial
    };
    ReplayLegFill {
        status,
        requested_quantity,
        filled_quantity,
        average_price,
        notional,
        fee,
        limit_price,
    }
}

fn settle_replay_fills(
    config: &SpotSpotTakerArbitrageConfig,
    inventory: &mut PaperInventory,
    opportunity: &OpportunityRecord,
    rules: &CommonSymbolRules,
    buy_exchange: SpotVenue,
    sell_exchange: SpotVenue,
    buy_leg: &ReplayLegFill,
    sell_leg: &ReplayLegFill,
    timing: &ReplayExecutionTiming,
) -> ReplayExecutionOutcome {
    let buy_rule = rules.for_exchange(buy_exchange);
    let sell_rule = rules.for_exchange(sell_exchange);
    let timed_out =
        buy_leg.status == ReplayLegStatus::Timeout || sell_leg.status == ReplayLegStatus::Timeout;
    let partial_fill = buy_leg.is_partial() || sell_leg.is_partial();
    let one_sided_risk = (buy_leg.filled_quantity - sell_leg.filled_quantity).abs() > 1e-12;

    if buy_leg.filled_quantity > 1e-12 {
        inventory.settle_buy(
            buy_exchange,
            &buy_rule.base_asset,
            &config.quote_asset,
            buy_leg.filled_quantity,
            buy_leg.notional,
            buy_leg.fee,
        );
    }
    if sell_leg.filled_quantity > 1e-12 {
        inventory.settle_sell(
            sell_exchange,
            &sell_rule.base_asset,
            &config.quote_asset,
            sell_leg.filled_quantity,
            sell_leg.notional,
            sell_leg.fee,
        );
    }

    let matched_quantity = buy_leg.filled_quantity.min(sell_leg.filled_quantity);
    if matched_quantity <= 1e-12 {
        return ReplayExecutionOutcome {
            rejected: !timed_out,
            timed_out,
            partial_fill,
            one_sided_risk,
            rejection_reason: Some(if timed_out {
                RejectionReason::ExchangeHealth
            } else {
                RejectionReason::PaperExecutionRejected
            }),
            ..ReplayExecutionOutcome::default()
        };
    }

    let buy_ratio = matched_quantity / buy_leg.filled_quantity;
    let sell_ratio = matched_quantity / sell_leg.filled_quantity;
    let matched_buy_notional = buy_leg.notional * buy_ratio;
    let matched_sell_notional = sell_leg.notional * sell_ratio;
    let matched_buy_fee = buy_leg.fee * buy_ratio;
    let matched_sell_fee = sell_leg.fee * sell_ratio;
    let gross_pnl = matched_sell_notional - matched_buy_notional;
    let fill_ratio = if opportunity.quantity > 1e-12 {
        matched_quantity / opportunity.quantity
    } else {
        0.0
    };
    let slippage_cost = opportunity.estimated_slippage_cost * fill_ratio;
    let capital_cost = opportunity.estimated_capital_cost * fill_ratio;
    let transfer_cost = opportunity.estimated_transfer_cost * fill_ratio;
    let inventory_rebalance_cost = opportunity.estimated_inventory_rebalance_cost * fill_ratio;
    let latency_penalty_cost = opportunity.estimated_latency_penalty_cost * fill_ratio;
    let non_fee_cost = slippage_cost
        + capital_cost
        + transfer_cost
        + inventory_rebalance_cost
        + latency_penalty_cost;
    let net_pnl = gross_pnl - matched_buy_fee - matched_sell_fee - non_fee_cost;
    inventory.add_pnl(gross_pnl, net_pnl);

    ReplayExecutionOutcome {
        trade: Some(SimulatedTradeRecord {
            timestamp: timing.max_arrival_at(),
            symbol: opportunity.symbol.clone(),
            buy_exchange: opportunity.buy_exchange.clone(),
            sell_exchange: opportunity.sell_exchange.clone(),
            buy_avg_price: buy_leg.average_price,
            sell_avg_price: sell_leg.average_price,
            quantity: matched_quantity,
            notional: matched_buy_notional,
            buy_fee: matched_buy_fee,
            sell_fee: matched_sell_fee,
            gross_pnl,
            net_pnl,
            pnl_category: if one_sided_risk {
                super::TradePnlCategory::OneSidedExposure
            } else {
                super::TradePnlCategory::Arbitrage
            },
            slippage_cost,
            capital_cost,
            transfer_cost,
            inventory_rebalance_cost,
            latency_penalty_cost,
            latency_ms: timing.total_latency_ms(),
            order_book_age_ms: timing
                .max_arrival_at()
                .signed_duration_since(timing.observed_at)
                .num_milliseconds()
                .max(0),
            execution_mode: if partial_fill || one_sided_risk {
                format!(
                    "replay_ioc_partial limit_buy={:.8} limit_sell={:.8}",
                    buy_leg.limit_price, sell_leg.limit_price
                )
            } else {
                format!(
                    "replay_ioc limit_buy={:.8} limit_sell={:.8}",
                    buy_leg.limit_price, sell_leg.limit_price
                )
            },
        }),
        timed_out,
        partial_fill,
        one_sided_risk,
        ..ReplayExecutionOutcome::default()
    }
}

fn latest_book_at(
    events: &[ReplayScheduledEvent],
    exchange: &str,
    symbol: &str,
    at: DateTime<Utc>,
) -> Option<CachedBook> {
    let key = book_key(exchange, symbol);
    events
        .iter()
        .filter(|event| {
            event.source_at <= at && book_key(&event.cached.exchange, &event.cached.symbol) == key
        })
        .max_by_key(|event| event.source_at)
        .map(|event| event.cached.clone())
}

fn replay_detection_snapshot(book: &CachedBook) -> OrderBookSnapshot {
    let mut snapshot = book.clone().into_snapshot();
    snapshot.received_at = Utc::now();
    snapshot
}

fn stamp_opportunity_replay_time(
    opportunity: &mut OpportunityRecord,
    timestamp: DateTime<Utc>,
    left_book: &CachedBook,
    right_book: &CachedBook,
) {
    opportunity.timestamp = timestamp;
    let buy_age = replay_book_age_ms(
        timestamp,
        if opportunity.buy_exchange == left_book.exchange {
            left_book
        } else {
            right_book
        },
    );
    let sell_age = replay_book_age_ms(
        timestamp,
        if opportunity.sell_exchange == left_book.exchange {
            left_book
        } else {
            right_book
        },
    );
    opportunity.buy_book_age_ms = buy_age;
    opportunity.sell_book_age_ms = sell_age;
}

fn replay_book_age_ms(now: DateTime<Utc>, book: &CachedBook) -> i64 {
    now.signed_duration_since(book.local_timestamp)
        .num_milliseconds()
        .max(0)
}

fn within_replay_window(config: &SpotSpotTakerArbitrageConfig, timestamp: DateTime<Utc>) -> bool {
    !config
        .replay
        .start_time
        .is_some_and(|start| timestamp < start)
        && !config.replay.end_time.is_some_and(|end| timestamp > end)
}

fn book_key(exchange: &str, symbol: &str) -> (String, String) {
    (
        normalize_exchange(exchange),
        symbol
            .trim()
            .replace(['/', '-', '_'], "")
            .to_ascii_uppercase(),
    )
}

fn normalize_exchange(exchange: &str) -> String {
    match exchange.trim().to_ascii_lowercase().as_str() {
        "gate" | "gate.io" => "gateio".to_string(),
        other => other.to_string(),
    }
}

fn parse_exchange(value: &str) -> Result<SpotVenue, RejectionReason> {
    match normalize_exchange(value).as_str() {
        "mexc" => Ok(SpotVenue::Mexc),
        "coinex" => Ok(SpotVenue::CoinEx),
        "gateio" => Ok(SpotVenue::GateIo),
        "bitget" => Ok(SpotVenue::Bitget),
        _ => Err(RejectionReason::PaperExecutionRejected),
    }
}

fn parse_replay_model_params(body: &str) -> HashMap<String, String> {
    body.split(',')
        .filter_map(|item| {
            let item = item.trim();
            if item.is_empty() {
                return None;
            }
            let (key, value) = item.split_once('=').unwrap_or((item, "true"));
            Some((key.trim().to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect()
}

fn i64_param(params: &HashMap<String, String>, key: &str) -> Option<i64> {
    params.get(key)?.parse::<i64>().ok()
}

fn u64_param(params: &HashMap<String, String>, key: &str) -> Option<u64> {
    params.get(key)?.parse::<u64>().ok()
}

fn f64_param(params: &HashMap<String, String>, key: &str) -> Option<f64> {
    params.get(key)?.parse::<f64>().ok()
}

fn param_enabled(params: &HashMap<String, String>, key: &str) -> bool {
    params.get(key).is_some_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

fn uniform_sampler(
    params: &HashMap<String, String>,
    prefix: &str,
    fallback: &ReplayLatencySampler,
) -> ReplayLatencySampler {
    let min = i64_param(params, &format!("{prefix}_min"));
    let max = i64_param(params, &format!("{prefix}_max"));
    match min.zip(max) {
        Some((min_ms, max_ms)) => ReplayLatencySampler::Uniform {
            min_ms: min_ms.min(max_ms).max(0),
            max_ms: min_ms.max(max_ms).max(0),
        },
        None => fallback.clone(),
    }
}

fn normal_sampler(
    params: &HashMap<String, String>,
    prefix: &str,
    fallback: &ReplayLatencySampler,
) -> ReplayLatencySampler {
    let mean = f64_param(params, &format!("{prefix}_mean"));
    let stddev = f64_param(params, &format!("{prefix}_std"))
        .or_else(|| f64_param(params, &format!("{prefix}_stddev")));
    match mean.zip(stddev) {
        Some((mean_ms, stddev_ms)) => ReplayLatencySampler::Normal {
            mean_ms: mean_ms.max(0.0),
            stddev_ms: stddev_ms.max(0.0),
        },
        None => fallback.clone(),
    }
}

fn exchange_latency_profile(params: &HashMap<String, String>) -> HashMap<String, i64> {
    let region = params
        .get("region")
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_else(|| "singapore".to_string());
    let mut profile = match region.as_str() {
        "singapore" | "sg" => HashMap::from([
            ("binance".to_string(), 40),
            ("bybit".to_string(), 15),
            ("bitget".to_string(), 45),
            ("gateio".to_string(), 150),
            ("gate".to_string(), 150),
            ("mexc".to_string(), 50),
            ("coinex".to_string(), 50),
        ]),
        "tokyo" | "jp" => HashMap::from([
            ("binance".to_string(), 35),
            ("bybit".to_string(), 20),
            ("bitget".to_string(), 45),
            ("gateio".to_string(), 120),
            ("gate".to_string(), 120),
            ("mexc".to_string(), 50),
            ("coinex".to_string(), 50),
        ]),
        _ => HashMap::new(),
    };
    let reserved = HashSet::from([
        "region",
        "fallback",
        "seed",
        "decision",
        "submit",
        "order",
        "timeout",
        "timeout_ms",
        "fill_probability",
        "fill_prob",
        "partial_fill_probability",
        "partial_prob",
        "partial_min",
        "partial_min_ratio",
        "partial_max",
        "partial_max_ratio",
        "slippage",
        "max_slippage_bps",
        "queue_ahead_ratio",
        "recorded_latency",
    ]);
    for (key, value) in params {
        if reserved.contains(key.as_str()) {
            continue;
        }
        if let Ok(latency_ms) = value.parse::<i64>() {
            profile.insert(normalize_exchange(key), latency_ms.max(0));
        }
    }
    profile
}

pub fn load_book_records(path: &str) -> Result<Vec<BookRecord>> {
    let file = fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut records = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<BookRecord>(&line) {
            Ok(record) => records.push(record),
            Err(strategy_error) => match serde_json::from_str::<crate::data::BookRecord>(&line) {
                Ok(shared) => records.push(BookRecord::from(shared)),
                Err(_) => return Err(strategy_error.into()),
            },
        }
    }
    Ok(records)
}

pub fn write_replay_report(path: &str, report: &ReplayReport) -> Result<()> {
    if let Some(parent) = Path::new(path).parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    serde_json::to_writer(&mut file, report)?;
    writeln!(file)?;
    Ok(())
}

pub fn replay_symbol_rules(
    config: &SpotSpotTakerArbitrageConfig,
) -> HashMap<String, CommonSymbolRules> {
    config
        .symbols
        .iter()
        .map(|symbol| {
            let normalized = symbol.trim().to_ascii_uppercase();
            (
                normalized.clone(),
                CommonSymbolRules {
                    mexc: fallback_rule("mexc", &normalized, &config.quote_asset),
                    coinex: fallback_rule("coinex", &normalized, &config.quote_asset),
                    gateio: None,
                    bitget: None,
                    kucoin: None,
                },
            )
        })
        .collect()
}

fn fallback_rule(exchange: &str, symbol: &str, quote_asset: &str) -> SymbolRule {
    let base = symbol
        .strip_suffix(quote_asset)
        .filter(|base| !base.is_empty())
        .unwrap_or("BASE");
    SymbolRule {
        exchange: exchange.to_string(),
        market_type: MarketType::Spot,
        internal_symbol: symbol.to_string(),
        exchange_symbol: symbol.to_string(),
        base_asset: base.to_string(),
        quote_asset: quote_asset.to_string(),
        price_precision: 8,
        quantity_precision: 8,
        tick_size: 0.00000001,
        step_size: 0.00000001,
        min_quantity: 0.0,
        min_notional: 0.0,
        max_quantity: None,
        supported_order_types: vec![OrderType::Market, OrderType::IOC],
        supported_time_in_force: vec![TimeInForce::IOC],
        status: SymbolStatus::Trading,
        raw_metadata: None,
    }
}

pub fn ensure_replay_is_network_free(config: &SpotSpotTakerArbitrageConfig) -> Result<()> {
    if config.market_data_mode != super::MarketDataMode::Replay {
        return Err(anyhow!("not replay mode"));
    }
    Ok(())
}
