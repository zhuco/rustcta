use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ExchangeId(pub String);

impl ExchangeId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into().trim().to_ascii_lowercase())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ExchangeId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CanonicalSymbol {
    pub base: String,
    pub quote: String,
}

impl CanonicalSymbol {
    pub fn new(base: impl Into<String>, quote: impl Into<String>) -> Self {
        Self {
            base: base.into().trim().to_ascii_uppercase(),
            quote: quote.into().trim().to_ascii_uppercase(),
        }
    }

    pub fn as_pair(&self) -> String {
        format!("{}/{}", self.base, self.quote)
    }
}

impl std::fmt::Display for CanonicalSymbol {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.as_pair())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderSide {
    Buy,
    Sell,
}

impl OrderSide {
    pub fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PositionSide {
    Long,
    Short,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MakerLegKind {
    LongMakerBuy,
    ShortMakerSell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FillInferenceType {
    RealTrade,
    BookInferredFill,
    NotFilled,
    TimedOut,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StrategyRoute {
    pub long_exchange: ExchangeId,
    pub short_exchange: ExchangeId,
    pub maker_exchange: ExchangeId,
    pub taker_exchange: ExchangeId,
    pub maker_side: OrderSide,
    pub taker_side: OrderSide,
    pub maker_leg_kind: MakerLegKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SimulatedBundleStatus {
    Observing,
    MakerPending,
    MakerFilled,
    Hedging,
    OpenSimulated,
    ClosingSimulated,
    Closed,
    OrphanLeg,
    RiskStopped,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SimulatedBundleState {
    pub bundle_id: String,
    pub opportunity_id: String,
    pub status: SimulatedBundleStatus,
    pub route: StrategyRoute,
    pub target_notional_usdt: f64,
    pub opened_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FeeRole {
    Maker,
    Taker,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExchangeFeeRates {
    pub maker: f64,
    pub taker: f64,
}

impl ExchangeFeeRates {
    pub fn rate(self, role: FeeRole) -> f64 {
        match role {
            FeeRole::Maker => self.maker,
            FeeRole::Taker => self.taker,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeeBreakdown {
    pub maker_entry_fee: f64,
    pub taker_hedge_fee: f64,
    pub maker_close_fee: f64,
    pub taker_close_fee: f64,
    pub emergency_close_fee: f64,
}

impl FeeBreakdown {
    pub fn open_fee(&self) -> f64 {
        self.maker_entry_fee + self.taker_hedge_fee
    }

    pub fn normal_close_fee(&self) -> f64 {
        self.maker_close_fee + self.taker_close_fee
    }

    pub fn total_normal_fee(&self) -> f64 {
        self.open_fee() + self.normal_close_fee()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeeModel {
    default: ExchangeFeeRates,
    per_exchange: HashMap<ExchangeId, ExchangeFeeRates>,
}

impl FeeModel {
    pub fn new(
        default: ExchangeFeeRates,
        per_exchange: HashMap<ExchangeId, ExchangeFeeRates>,
    ) -> Self {
        Self {
            default,
            per_exchange,
        }
    }

    pub fn rates_for(&self, exchange: &ExchangeId) -> ExchangeFeeRates {
        self.per_exchange
            .get(exchange)
            .copied()
            .unwrap_or(self.default)
    }

    pub fn rate(&self, exchange: &ExchangeId, role: FeeRole) -> f64 {
        self.rates_for(exchange).rate(role)
    }

    pub fn fee_amount(&self, exchange: &ExchangeId, role: FeeRole, notional_usdt: f64) -> f64 {
        notional_usdt * self.rate(exchange, role)
    }

    pub fn estimate_maker_taker_round_trip(
        &self,
        maker_exchange: &ExchangeId,
        taker_exchange: &ExchangeId,
        notional_usdt: f64,
    ) -> FeeBreakdown {
        FeeBreakdown {
            maker_entry_fee: self.fee_amount(maker_exchange, FeeRole::Maker, notional_usdt),
            taker_hedge_fee: self.fee_amount(taker_exchange, FeeRole::Taker, notional_usdt),
            maker_close_fee: self.fee_amount(maker_exchange, FeeRole::Maker, notional_usdt),
            taker_close_fee: self.fee_amount(taker_exchange, FeeRole::Taker, notional_usdt),
            emergency_close_fee: self.fee_amount(maker_exchange, FeeRole::Taker, notional_usdt)
                + self.fee_amount(taker_exchange, FeeRole::Taker, notional_usdt),
        }
    }

    pub fn estimate_dual_taker_round_trip(
        &self,
        long_exchange: &ExchangeId,
        short_exchange: &ExchangeId,
        notional_usdt: f64,
    ) -> FeeBreakdown {
        FeeBreakdown {
            maker_entry_fee: 0.0,
            taker_hedge_fee: self.fee_amount(long_exchange, FeeRole::Taker, notional_usdt)
                + self.fee_amount(short_exchange, FeeRole::Taker, notional_usdt),
            maker_close_fee: 0.0,
            taker_close_fee: self.fee_amount(long_exchange, FeeRole::Taker, notional_usdt)
                + self.fee_amount(short_exchange, FeeRole::Taker, notional_usdt),
            emergency_close_fee: self.fee_amount(long_exchange, FeeRole::Taker, notional_usdt)
                + self.fee_amount(short_exchange, FeeRole::Taker, notional_usdt),
        }
    }
}

impl Default for FeeModel {
    fn default() -> Self {
        Self::new(
            ExchangeFeeRates {
                maker: 0.0002,
                taker: 0.0005,
            },
            HashMap::new(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundingSnapshot {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub funding_rate: f64,
    pub next_funding_time: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundingEstimate {
    pub long_leg_funding: f64,
    pub short_leg_funding: f64,
    pub net_funding: f64,
    pub net_funding_rate: f64,
    pub next_funding_time: Option<DateTime<Utc>>,
    pub minutes_to_funding: Option<i64>,
    pub dangerous: bool,
    pub near_negative_settlement: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundingModel {
    pub max_adverse_funding_rate: f64,
}

impl FundingModel {
    pub fn new(max_adverse_funding_rate: f64) -> Self {
        Self {
            max_adverse_funding_rate,
        }
    }

    pub fn leg_funding(position_side: PositionSide, notional_usdt: f64, funding_rate: f64) -> f64 {
        let funding = notional_usdt.abs() * funding_rate;
        match position_side {
            PositionSide::Long => -funding,
            PositionSide::Short => funding,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn estimate_pair_with_timing(
        &self,
        long_notional_usdt: f64,
        long_funding_rate: f64,
        long_next_funding_time: Option<DateTime<Utc>>,
        short_notional_usdt: f64,
        short_funding_rate: f64,
        short_next_funding_time: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
        no_open_before_funding_mins: i64,
    ) -> FundingEstimate {
        let long_leg_funding =
            Self::leg_funding(PositionSide::Long, long_notional_usdt, long_funding_rate);
        let short_leg_funding =
            Self::leg_funding(PositionSide::Short, short_notional_usdt, short_funding_rate);
        let net_funding = long_leg_funding + short_leg_funding;
        let base_notional = long_notional_usdt
            .abs()
            .max(short_notional_usdt.abs())
            .max(1.0);
        let net_funding_rate = net_funding / base_notional;
        let next_funding_time =
            earliest_future(long_next_funding_time, short_next_funding_time, now);
        let minutes_to_funding =
            next_funding_time.map(|time| time.signed_duration_since(now).num_minutes().max(0));
        let near_negative_settlement = net_funding < 0.0
            && minutes_to_funding
                .map(|mins| mins <= no_open_before_funding_mins.max(0))
                .unwrap_or(false);

        FundingEstimate {
            long_leg_funding,
            short_leg_funding,
            net_funding,
            net_funding_rate,
            next_funding_time,
            minutes_to_funding,
            dangerous: net_funding_rate < -self.max_adverse_funding_rate,
            near_negative_settlement,
        }
    }

    pub fn estimate_pair(
        &self,
        long_notional_usdt: f64,
        long_funding_rate: f64,
        short_notional_usdt: f64,
        short_funding_rate: f64,
    ) -> FundingEstimate {
        self.estimate_pair_with_timing(
            long_notional_usdt,
            long_funding_rate,
            None,
            short_notional_usdt,
            short_funding_rate,
            None,
            Utc::now(),
            0,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn settle_leg(
        bundle_id: impl Into<String>,
        exchange: ExchangeId,
        canonical_symbol: CanonicalSymbol,
        position_side: PositionSide,
        notional_usdt: f64,
        funding_rate: f64,
        mark_price: Option<f64>,
        settled_at: DateTime<Utc>,
    ) -> FundingSettlement {
        FundingSettlement {
            bundle_id: bundle_id.into(),
            exchange,
            canonical_symbol,
            position_side,
            notional_usdt: notional_usdt.abs(),
            funding_rate,
            funding_pnl_usdt: Self::leg_funding(position_side, notional_usdt, funding_rate),
            mark_price,
            settled_at,
        }
    }
}

impl Default for FundingModel {
    fn default() -> Self {
        Self::new(0.001)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundingSettlement {
    pub bundle_id: String,
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub position_side: PositionSide,
    pub notional_usdt: f64,
    pub funding_rate: f64,
    pub funding_pnl_usdt: f64,
    pub mark_price: Option<f64>,
    pub settled_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FundingSettlementLedger {
    settlements: Vec<FundingSettlement>,
}

impl FundingSettlementLedger {
    pub fn record(&mut self, settlement: FundingSettlement) {
        self.settlements.push(settlement);
    }

    pub fn settlements(&self) -> &[FundingSettlement] {
        &self.settlements
    }

    pub fn total_pnl_usdt(&self) -> f64 {
        self.settlements
            .iter()
            .map(|settlement| settlement.funding_pnl_usdt)
            .sum()
    }

    pub fn total_pnl_for_bundle(&self, bundle_id: &str) -> f64 {
        self.settlements
            .iter()
            .filter(|settlement| settlement.bundle_id == bundle_id)
            .map(|settlement| settlement.funding_pnl_usdt)
            .sum()
    }
}

fn earliest_future(
    left: Option<DateTime<Utc>>,
    right: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    [left, right]
        .into_iter()
        .flatten()
        .filter(|time| *time >= now)
        .min()
}
