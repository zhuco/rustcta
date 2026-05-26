//! Cross-exchange opportunity scanning with executable 5-level VWAP.

use super::config::CrossExchangeArbitrageConfig;
use super::fees::FeeModel;
use super::funding::FundingModel;
use super::risk::{book_age_ms, RejectReason, RiskGate};
use super::simulation::calculate_taker_vwap;
use super::state::{MakerLegKind, OrderSide, StrategyRoute};
use crate::market::{CanonicalSymbol, ExchangeId, OrderBook5, RouteStatus};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketSnapshot {
    pub book: OrderBook5,
    pub route_status: RouteStatus,
    pub funding_rate: f64,
    pub next_funding_time: Option<DateTime<Utc>>,
}

impl MarketSnapshot {
    pub fn healthy(book: OrderBook5) -> Self {
        Self {
            book,
            route_status: RouteStatus::Healthy,
            funding_rate: 0.0,
            next_funding_time: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Opportunity {
    pub opportunity_id: String,
    pub canonical_symbol: CanonicalSymbol,
    pub long_exchange: ExchangeId,
    pub short_exchange: ExchangeId,
    pub maker_exchange: ExchangeId,
    pub taker_exchange: ExchangeId,
    pub maker_side: OrderSide,
    pub taker_side: OrderSide,
    pub maker_leg_kind: MakerLegKind,
    pub maker_price: f64,
    pub taker_vwap: Option<f64>,
    pub target_notional_usdt: f64,
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
                MakerLegKind::ShortMakerSell,
                config,
                &fee_model,
                &funding_model,
                now,
            ));
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
    let (maker_book, taker_book, maker_side, taker_side, long_entry_price, short_entry_price) =
        match maker_leg_kind {
            MakerLegKind::ShortMakerSell => {
                let maker_price = short_snapshot.book.best_ask().map(|level| level.price);
                let taker_vwap =
                    calculate_taker_vwap(&long_snapshot.book, OrderSide::Buy, target_notional);
                (
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
    let maker_price = match maker_side {
        OrderSide::Sell => maker_book.best_ask().map(|level| level.price),
        OrderSide::Buy => maker_book.best_bid().map(|level| level.price),
    }
    .unwrap_or(0.0);
    let raw_open_spread = match (long_entry_price, short_entry_price) {
        (Some(long_price), Some(short_price)) if long_price > 0.0 => short_price / long_price - 1.0,
        _ => -1.0,
    };
    let funding = funding_model.estimate_pair_with_timing(
        target_notional,
        long_snapshot.funding_rate,
        long_snapshot.next_funding_time,
        target_notional,
        short_snapshot.funding_rate,
        short_snapshot.next_funding_time,
        now,
        config.funding.no_open_before_funding_mins,
    );
    let fees = fee_model.estimate_maker_taker_round_trip(
        &maker_book.exchange,
        &taker_book.exchange,
        target_notional,
    );
    let maker_taker_net_edge = raw_open_spread - fees.total_normal_fee() / target_notional.max(1.0)
        + funding.net_funding_rate
        - config.risk.taker_slippage_buffer
        - config.risk.maker_non_fill_penalty
        - config.risk.safety_buffer;
    let route_status = min_route_status(
        if maker_book.exchange == long_snapshot.book.exchange {
            long_snapshot.route_status
        } else {
            short_snapshot.route_status
        },
        if taker_book.exchange == long_snapshot.book.exchange {
            long_snapshot.route_status
        } else {
            short_snapshot.route_status
        },
    );
    let risk_decision = RiskGate::evaluate_open(
        maker_book,
        taker_book,
        route_status,
        route_status,
        &taker_vwap,
        &funding,
        target_notional,
        config,
        now,
    );
    let mut reject_reasons = risk_decision.reject_reasons;
    if raw_open_spread < config.thresholds.min_display_raw_spread {
        reject_reasons.push(RejectReason::RawSpreadTooSmall);
    }
    if maker_taker_net_edge < config.thresholds.min_open_maker_taker_net_edge {
        reject_reasons.push(RejectReason::NetEdgeTooSmall);
    }

    Opportunity {
        opportunity_id: opportunity_id(
            canonical_symbol,
            &long_snapshot.book.exchange,
            &short_snapshot.book.exchange,
            maker_leg_kind,
            now,
        ),
        canonical_symbol: canonical_symbol.clone(),
        long_exchange: long_snapshot.book.exchange.clone(),
        short_exchange: short_snapshot.book.exchange.clone(),
        maker_exchange: maker_book.exchange.clone(),
        taker_exchange: taker_book.exchange.clone(),
        maker_side,
        taker_side,
        maker_leg_kind,
        maker_price,
        taker_vwap: taker_vwap.vwap_price,
        target_notional_usdt: target_notional,
        raw_open_spread,
        maker_taker_net_edge,
        open_fee_est_usdt: fees.open_fee(),
        close_fee_est_usdt: fees.normal_close_fee(),
        expected_funding_usdt: funding.net_funding,
        slippage_pct: taker_vwap.slippage_pct,
        depth_notional_usdt: taker_vwap.filled_notional_usdt,
        book_age_ms: book_age_ms(maker_book, now).max(book_age_ms(taker_book, now)),
        route_status,
        can_open: reject_reasons.is_empty(),
        reject_reasons,
        created_at: now,
        expires_at: now + Duration::milliseconds(config.risk.stale_quote_ms),
    }
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
    use crate::market::{BookLevel, ExchangeSymbol};

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

    fn config() -> CrossExchangeArbitrageConfig {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.sizing.target_notional_usdt = 100.0;
        config.thresholds.min_open_maker_taker_net_edge = 0.001;
        config.risk.max_book_age_ms = 10_000;
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
}
