//! Stable, dependency-light domain types shared across RustCTA platform crates.
//!
//! This crate intentionally contains no adapter, strategy, web, or supervisor
//! dependencies. Types here are suitable for persisted records and local
//! process/network contracts.

use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const CURRENT_SCHEMA_VERSION_MAJOR: u16 = 1;
pub const CURRENT_SCHEMA_VERSION_MINOR: u16 = 0;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ValidationError {
    #[error("{field} must not be empty")]
    Empty { field: &'static str },
    #[error("{field} contains invalid character {ch:?}")]
    InvalidCharacter { field: &'static str, ch: char },
    #[error("{field} must be greater than 0")]
    NotPositive { field: &'static str },
    #[error("{field} must be greater than or equal to 0")]
    Negative { field: &'static str },
    #[error("{field} must be finite")]
    NonFinite { field: &'static str },
    #[error("{field} must contain exactly one '/' separator")]
    InvalidCanonicalSymbol { field: &'static str },
    #[error("{field} base and quote assets must differ")]
    SameBaseQuote { field: &'static str },
    #[error("best bid must be lower than or equal to best ask")]
    CrossedOrderBook,
    #[error("{field} is required")]
    Required { field: &'static str },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SchemaVersion {
    pub major: u16,
    pub minor: u16,
}

impl SchemaVersion {
    pub const fn new(major: u16, minor: u16) -> Self {
        Self { major, minor }
    }

    pub const fn current() -> Self {
        Self {
            major: CURRENT_SCHEMA_VERSION_MAJOR,
            minor: CURRENT_SCHEMA_VERSION_MINOR,
        }
    }

    pub const fn is_compatible_with(self, other: Self) -> bool {
        self.major == other.major && self.minor <= other.minor
    }
}

impl Default for SchemaVersion {
    fn default() -> Self {
        Self::current()
    }
}

impl fmt::Display for SchemaVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid schema version: expected MAJOR.MINOR")]
pub struct ParseSchemaVersionError;

impl FromStr for SchemaVersion {
    type Err = ParseSchemaVersionError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (major, minor) = value.split_once('.').ok_or(ParseSchemaVersionError)?;
        if minor.contains('.') {
            return Err(ParseSchemaVersionError);
        }
        Ok(Self {
            major: major.parse().map_err(|_| ParseSchemaVersionError)?,
            minor: minor.parse().map_err(|_| ParseSchemaVersionError)?,
        })
    }
}

macro_rules! define_id {
    ($name:ident, $field:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
                let normalized = normalize_id(value.into(), $field)?;
                Ok(Self(normalized))
            }

            pub fn unchecked(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn into_inner(self) -> String {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::new(value).map_err(serde::de::Error::custom)
            }
        }

        impl FromStr for $name {
            type Err = ValidationError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }
    };
}

define_id!(TenantId, "tenant_id");
define_id!(AccountId, "account_id");
define_id!(StrategyId, "strategy_id");
define_id!(RunId, "run_id");
define_id!(CommandId, "command_id");
define_id!(CorrelationId, "correlation_id");

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExchangeId(String);

impl ExchangeId {
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = normalize_id(value.into(), "exchange_id")?.to_ascii_lowercase();
        Ok(Self(value))
    }

    pub fn unchecked(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl fmt::Display for ExchangeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Serialize for ExchangeId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for ExchangeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

impl FromStr for ExchangeId {
    type Err = ValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum MarketType {
    #[default]
    Spot,
    Margin,
    Futures,
    Perpetual,
    Option,
}

/// Backtest market type DTO that preserves the legacy `core::types::MarketType`
/// serde shape used by historical replay fixtures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BacktestMarketType {
    Spot,
    Futures,
}

/// Backtest trade side DTO that preserves the legacy `Buy`/`Sell` serde shape
/// used by historical replay fixtures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BacktestOrderSide {
    Buy,
    Sell,
}

impl fmt::Display for BacktestOrderSide {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

/// Backtest order type DTO matching the legacy backtest matching ledger shape.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum BacktestOrderType {
    Market,
    Limit,
    StopLimit,
    StopMarket,
    TakeProfitLimit,
    TakeProfitMarket,
    TrailingStop,
}

/// Backtest trade fee DTO matching the legacy persisted trade schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BacktestFee {
    pub currency: String,
    pub cost: f64,
    pub rate: Option<f64>,
}

/// Backtest trade DTO matching the legacy persisted trade schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BacktestTrade {
    pub id: String,
    pub symbol: String,
    pub side: BacktestOrderSide,
    pub amount: f64,
    pub price: f64,
    pub timestamp: DateTime<Utc>,
    pub order_id: Option<String>,
    pub fee: Option<BacktestFee>,
}

/// Backtest kline DTO matching the legacy persisted kline schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BacktestKline {
    pub symbol: String,
    pub interval: String,
    pub open_time: DateTime<Utc>,
    pub close_time: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub quote_volume: f64,
    pub trade_count: u64,
}

/// Backtest trading-pair metadata DTO matching the legacy exchange metadata
/// snapshot schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BacktestTradingPair {
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub status: String,
    pub min_order_size: f64,
    pub max_order_size: f64,
    pub tick_size: f64,
    pub step_size: f64,
    pub min_notional: Option<f64>,
    pub is_trading: bool,
    pub market_type: BacktestMarketType,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CanonicalSymbol(String);

impl CanonicalSymbol {
    pub fn new(
        base_asset: impl AsRef<str>,
        quote_asset: impl AsRef<str>,
    ) -> Result<Self, ValidationError> {
        let base = normalize_asset(base_asset.as_ref(), "canonical_symbol")?;
        let quote = normalize_asset(quote_asset.as_ref(), "canonical_symbol")?;
        if base == quote {
            return Err(ValidationError::SameBaseQuote {
                field: "canonical_symbol",
            });
        }
        Ok(Self(format!("{base}/{quote}")))
    }

    pub fn parse(value: impl AsRef<str>) -> Result<Self, ValidationError> {
        let raw = value.as_ref().trim();
        let mut parts = raw.split('/');
        let base = parts.next().unwrap_or_default();
        let quote = parts.next().unwrap_or_default();
        if parts.next().is_some() || base.is_empty() || quote.is_empty() {
            return Err(ValidationError::InvalidCanonicalSymbol {
                field: "canonical_symbol",
            });
        }
        Self::new(base, quote)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn base_asset(&self) -> &str {
        self.0
            .split_once('/')
            .map(|(base, _)| base)
            .expect("CanonicalSymbol invariant violated")
    }

    pub fn quote_asset(&self) -> &str {
        self.0
            .split_once('/')
            .map(|(_, quote)| quote)
            .expect("CanonicalSymbol invariant violated")
    }
}

impl fmt::Display for CanonicalSymbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Serialize for CanonicalSymbol {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for CanonicalSymbol {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}

impl FromStr for CanonicalSymbol {
    type Err = ValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExchangeSymbol {
    pub schema_version: SchemaVersion,
    pub exchange_id: ExchangeId,
    pub market_type: MarketType,
    pub symbol: String,
}

impl ExchangeSymbol {
    pub fn new(
        exchange_id: ExchangeId,
        market_type: MarketType,
        symbol: impl Into<String>,
    ) -> Result<Self, ValidationError> {
        let symbol = symbol.into().trim().to_string();
        if symbol.is_empty() {
            return Err(ValidationError::Empty { field: "symbol" });
        }
        Ok(Self {
            schema_version: SchemaVersion::current(),
            exchange_id,
            market_type,
            symbol,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionSide {
    Long,
    Short,
    Net,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[allow(clippy::upper_case_acronyms)]
pub enum OrderType {
    Market,
    Limit,
    PostOnly,
    #[serde(rename = "ioc")]
    IOC,
    #[serde(rename = "fok")]
    FOK,
    StopMarket,
    StopLimit,
}

impl OrderType {
    pub fn requires_limit_price(self) -> bool {
        matches!(
            self,
            Self::Limit | Self::PostOnly | Self::IOC | Self::FOK | Self::StopLimit
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[allow(clippy::upper_case_acronyms)]
pub enum TimeInForce {
    #[serde(rename = "gtc")]
    GTC,
    #[serde(rename = "ioc")]
    IOC,
    #[serde(rename = "fok")]
    FOK,
    #[serde(rename = "gtx")]
    GTX,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    New,
    Open,
    PartiallyFilled,
    Filled,
    PendingCancel,
    Cancelled,
    Rejected,
    Expired,
    Unknown,
}

impl OrderStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Filled | Self::Cancelled | Self::Rejected | Self::Expired | Self::Unknown
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FillStatus {
    Pending,
    Confirmed,
    Reversed,
    Corrected,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiquidityRole {
    Maker,
    Taker,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SymbolStatus {
    Trading,
    Halted,
    Suspended,
    ReduceOnly,
    PreOpen,
    Settling,
    Delisted,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeeRate {
    pub schema_version: SchemaVersion,
    pub exchange_id: ExchangeId,
    pub account_id: Option<AccountId>,
    pub market_type: MarketType,
    pub canonical_symbol: Option<CanonicalSymbol>,
    pub maker_rate: f64,
    pub taker_rate: f64,
    pub effective_at: Option<DateTime<Utc>>,
}

impl FeeRate {
    pub fn new(
        exchange_id: ExchangeId,
        market_type: MarketType,
        maker_rate: f64,
        taker_rate: f64,
    ) -> Result<Self, ValidationError> {
        validate_finite("maker_rate", maker_rate)?;
        validate_finite("taker_rate", taker_rate)?;
        Ok(Self {
            schema_version: SchemaVersion::current(),
            exchange_id,
            account_id: None,
            market_type,
            canonical_symbol: None,
            maker_rate,
            taker_rate,
            effective_at: None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AssetBalance {
    pub schema_version: SchemaVersion,
    pub asset: String,
    pub total: f64,
    pub available: f64,
    pub locked: f64,
    pub locally_reserved: f64,
    pub effective_available: f64,
}

impl AssetBalance {
    pub fn new(
        asset: impl AsRef<str>,
        total: f64,
        available: f64,
        locked: f64,
    ) -> Result<Self, ValidationError> {
        let asset = normalize_asset(asset.as_ref(), "asset")?;
        validate_non_negative("total", total)?;
        validate_non_negative("available", available)?;
        validate_non_negative("locked", locked)?;
        Ok(Self {
            schema_version: SchemaVersion::current(),
            asset,
            total,
            available,
            locked,
            locally_reserved: 0.0,
            effective_available: available,
        })
    }

    pub fn with_reservation(mut self, locally_reserved: f64) -> Result<Self, ValidationError> {
        validate_non_negative("locally_reserved", locally_reserved)?;
        self.locally_reserved = locally_reserved;
        self.effective_available = (self.available - self.locally_reserved).max(0.0);
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExchangeBalance {
    pub schema_version: SchemaVersion,
    pub tenant_id: TenantId,
    pub account_id: AccountId,
    pub exchange_id: ExchangeId,
    pub market_type: MarketType,
    pub balances: Vec<AssetBalance>,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExchangePosition {
    pub schema_version: SchemaVersion,
    pub tenant_id: TenantId,
    pub account_id: AccountId,
    pub exchange_id: ExchangeId,
    pub market_type: MarketType,
    pub canonical_symbol: CanonicalSymbol,
    pub exchange_symbol: Option<ExchangeSymbol>,
    pub side: PositionSide,
    pub quantity: f64,
    pub entry_price: Option<f64>,
    pub mark_price: Option<f64>,
    pub liquidation_price: Option<f64>,
    pub unrealized_pnl: Option<f64>,
    pub leverage: Option<f64>,
    pub observed_at: DateTime<Utc>,
}

impl ExchangePosition {
    pub fn validate(&self) -> Result<(), ValidationError> {
        validate_non_negative("quantity", self.quantity)?;
        validate_optional_positive("entry_price", self.entry_price)?;
        validate_optional_positive("mark_price", self.mark_price)?;
        validate_optional_positive("liquidation_price", self.liquidation_price)?;
        validate_optional_positive("leverage", self.leverage)?;
        if self.market_type == MarketType::Spot
            && !matches!(self.side, PositionSide::None | PositionSide::Net)
        {
            return Err(ValidationError::Required {
                field: "spot position side must be none or net",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct OrderBookLevel {
    pub price: f64,
    pub quantity: f64,
}

impl OrderBookLevel {
    pub fn new(price: f64, quantity: f64) -> Result<Self, ValidationError> {
        validate_positive("price", price)?;
        validate_positive("quantity", quantity)?;
        Ok(Self { price, quantity })
    }

    pub fn notional(&self) -> f64 {
        self.price * self.quantity
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderBookSnapshot {
    pub schema_version: SchemaVersion,
    pub exchange_id: ExchangeId,
    pub market_type: MarketType,
    pub canonical_symbol: CanonicalSymbol,
    pub exchange_symbol: Option<ExchangeSymbol>,
    pub bids: Vec<OrderBookLevel>,
    pub asks: Vec<OrderBookLevel>,
    pub sequence: Option<u64>,
    pub exchange_timestamp: Option<DateTime<Utc>>,
    pub received_at: DateTime<Utc>,
    pub is_stale: bool,
}

impl OrderBookSnapshot {
    pub fn new(
        exchange_id: ExchangeId,
        market_type: MarketType,
        canonical_symbol: CanonicalSymbol,
        bids: Vec<OrderBookLevel>,
        asks: Vec<OrderBookLevel>,
        received_at: DateTime<Utc>,
    ) -> Result<Self, ValidationError> {
        let snapshot = Self {
            schema_version: SchemaVersion::current(),
            exchange_id,
            market_type,
            canonical_symbol,
            exchange_symbol: None,
            bids,
            asks,
            sequence: None,
            exchange_timestamp: None,
            received_at,
            is_stale: false,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    pub fn best_bid(&self) -> Option<&OrderBookLevel> {
        self.bids.first()
    }

    pub fn best_ask(&self) -> Option<&OrderBookLevel> {
        self.asks.first()
    }

    pub fn mid_price(&self) -> Option<f64> {
        Some((self.best_bid()?.price + self.best_ask()?.price) / 2.0)
    }

    pub fn validate(&self) -> Result<(), ValidationError> {
        for level in self.bids.iter().chain(self.asks.iter()) {
            validate_positive("price", level.price)?;
            validate_positive("quantity", level.quantity)?;
        }
        if let (Some(bid), Some(ask)) = (self.best_bid(), self.best_ask()) {
            if bid.price > ask.price {
                return Err(ValidationError::CrossedOrderBook);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Fill {
    pub schema_version: SchemaVersion,
    pub tenant_id: TenantId,
    pub account_id: AccountId,
    pub exchange_id: ExchangeId,
    pub market_type: MarketType,
    pub canonical_symbol: CanonicalSymbol,
    pub exchange_symbol: Option<ExchangeSymbol>,
    pub order_id: Option<String>,
    pub client_order_id: Option<String>,
    pub fill_id: Option<String>,
    pub side: OrderSide,
    pub position_side: PositionSide,
    pub status: FillStatus,
    pub liquidity_role: LiquidityRole,
    pub price: f64,
    pub quantity: f64,
    pub quote_quantity: Option<f64>,
    pub fee_asset: Option<String>,
    pub fee_amount: Option<f64>,
    pub fee_rate: Option<f64>,
    pub realized_pnl: Option<f64>,
    pub filled_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
}

impl Fill {
    pub fn notional(&self) -> f64 {
        self.quote_quantity.unwrap_or(self.price * self.quantity)
    }

    pub fn validate(&self) -> Result<(), ValidationError> {
        validate_positive("price", self.price)?;
        validate_positive("quantity", self.quantity)?;
        validate_optional_non_negative("quote_quantity", self.quote_quantity)?;
        validate_optional_non_negative("fee_amount", self.fee_amount)?;
        validate_optional_finite("fee_rate", self.fee_rate)?;
        validate_optional_finite("realized_pnl", self.realized_pnl)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExchangeErrorClass {
    Unsupported,
    UnsupportedCapability,
    Authentication,
    Permission,
    RateLimited,
    Network,
    Timeout,
    ExchangeUnavailable,
    Maintenance,
    InvalidRequest,
    InvalidSymbol,
    InvalidPrecision,
    MinNotionalViolation,
    InsufficientBalance,
    InsufficientPosition,
    DuplicateClientOrderId,
    InvalidClientOrderId,
    OrderNotFound,
    OrderRejected,
    RiskRejected,
    StaleMarketData,
    UnknownOrderState,
    Decode,
    Internal,
    Unknown,
}

impl ExchangeErrorClass {
    pub fn is_retryable(self) -> bool {
        matches!(
            self,
            Self::RateLimited
                | Self::Network
                | Self::Timeout
                | Self::ExchangeUnavailable
                | Self::Maintenance
                | Self::UnknownOrderState
        )
    }

    pub fn requires_reconciliation(self) -> bool {
        matches!(
            self,
            Self::UnknownOrderState
                | Self::DuplicateClientOrderId
                | Self::OrderNotFound
                | Self::OrderRejected
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExchangeError {
    pub schema_version: SchemaVersion,
    pub exchange_id: ExchangeId,
    pub class: ExchangeErrorClass,
    pub code: Option<String>,
    pub message: String,
    pub retry_after_ms: Option<u64>,
    pub order_id: Option<String>,
    pub client_order_id: Option<String>,
    pub raw: Option<serde_json::Value>,
    pub occurred_at: DateTime<Utc>,
}

impl ExchangeError {
    pub fn new(
        exchange_id: ExchangeId,
        class: ExchangeErrorClass,
        message: impl Into<String>,
        occurred_at: DateTime<Utc>,
    ) -> Self {
        Self {
            schema_version: SchemaVersion::current(),
            exchange_id,
            class,
            code: None,
            message: message.into(),
            retry_after_ms: None,
            order_id: None,
            client_order_id: None,
            raw: None,
            occurred_at,
        }
    }

    pub fn is_retryable(&self) -> bool {
        self.class.is_retryable()
    }

    pub fn requires_reconciliation(&self) -> bool {
        self.class.requires_reconciliation()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RateLimitScope {
    Rest,
    WebSocket,
    Orders,
    Cancels,
    UserStream,
    Global,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RateLimitCapability {
    pub scope: RateLimitScope,
    pub limit: u32,
    pub window_ms: u64,
}

impl RateLimitCapability {
    pub fn new(scope: RateLimitScope, limit: u32, window_ms: u64) -> Result<Self, ValidationError> {
        if limit == 0 {
            return Err(ValidationError::NotPositive { field: "limit" });
        }
        if window_ms == 0 {
            return Err(ValidationError::NotPositive { field: "window_ms" });
        }
        Ok(Self {
            scope,
            limit,
            window_ms,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SymbolCapability {
    pub schema_version: SchemaVersion,
    pub exchange_id: ExchangeId,
    pub market_type: MarketType,
    pub canonical_symbol: CanonicalSymbol,
    pub exchange_symbol: ExchangeSymbol,
    pub status: SymbolStatus,
    pub base_asset: String,
    pub quote_asset: String,
    pub price_precision: Option<u32>,
    pub quantity_precision: Option<u32>,
    pub tick_size: Option<f64>,
    pub step_size: Option<f64>,
    pub min_quantity: Option<f64>,
    pub max_quantity: Option<f64>,
    pub min_notional: Option<f64>,
    pub max_notional: Option<f64>,
    pub supported_order_types: Vec<OrderType>,
    pub supported_time_in_force: Vec<TimeInForce>,
    pub supports_post_only: bool,
    pub supports_reduce_only: bool,
    pub supports_iceberg: bool,
    pub supports_client_order_id: bool,
    pub raw: Option<serde_json::Value>,
}

impl SymbolCapability {
    pub fn new(
        exchange_id: ExchangeId,
        market_type: MarketType,
        canonical_symbol: CanonicalSymbol,
        exchange_symbol: ExchangeSymbol,
    ) -> Self {
        let base_asset = canonical_symbol.base_asset().to_string();
        let quote_asset = canonical_symbol.quote_asset().to_string();
        Self {
            schema_version: SchemaVersion::current(),
            exchange_id,
            market_type,
            canonical_symbol,
            exchange_symbol,
            status: SymbolStatus::Unknown,
            base_asset,
            quote_asset,
            price_precision: None,
            quantity_precision: None,
            tick_size: None,
            step_size: None,
            min_quantity: None,
            max_quantity: None,
            min_notional: None,
            max_notional: None,
            supported_order_types: Vec::new(),
            supported_time_in_force: Vec::new(),
            supports_post_only: false,
            supports_reduce_only: false,
            supports_iceberg: false,
            supports_client_order_id: true,
            raw: None,
        }
    }

    pub fn supports_order_type(&self, order_type: OrderType) -> bool {
        self.supported_order_types.contains(&order_type)
    }

    pub fn validate(&self) -> Result<(), ValidationError> {
        validate_optional_positive("tick_size", self.tick_size)?;
        validate_optional_positive("step_size", self.step_size)?;
        validate_optional_positive("min_quantity", self.min_quantity)?;
        validate_optional_positive("max_quantity", self.max_quantity)?;
        validate_optional_positive("min_notional", self.min_notional)?;
        validate_optional_positive("max_notional", self.max_notional)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataCapability {
    pub supports_rest_order_book: bool,
    pub supports_ws_order_book: bool,
    pub supports_trades: bool,
    pub supports_tickers: bool,
    pub max_order_book_depth: Option<u32>,
}

impl Default for MarketDataCapability {
    fn default() -> Self {
        Self {
            supports_rest_order_book: true,
            supports_ws_order_book: false,
            supports_trades: false,
            supports_tickers: false,
            max_order_book_depth: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountCapability {
    pub supports_balances: bool,
    pub supports_positions: bool,
    pub supports_fee_rates: bool,
    pub supports_private_stream: bool,
}

impl Default for AccountCapability {
    fn default() -> Self {
        Self {
            supports_balances: true,
            supports_positions: false,
            supports_fee_rates: false,
            supports_private_stream: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderCapability {
    pub supports_place_order: bool,
    pub supports_cancel_order: bool,
    pub supports_query_order: bool,
    pub supports_query_open_orders: bool,
    pub supports_batch_orders: bool,
    pub supports_idempotent_client_order_id: bool,
    pub max_client_order_id_len: Option<u16>,
}

impl Default for OrderCapability {
    fn default() -> Self {
        Self {
            supports_place_order: true,
            supports_cancel_order: true,
            supports_query_order: true,
            supports_query_open_orders: true,
            supports_batch_orders: false,
            supports_idempotent_client_order_id: true,
            max_client_order_id_len: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExchangeCapability {
    pub schema_version: SchemaVersion,
    pub exchange_id: ExchangeId,
    pub market_types: Vec<MarketType>,
    pub market_data: MarketDataCapability,
    pub account: AccountCapability,
    pub orders: OrderCapability,
    pub rate_limits: Vec<RateLimitCapability>,
    pub symbols: Vec<SymbolCapability>,
    pub observed_at: DateTime<Utc>,
}

impl ExchangeCapability {
    pub fn new(exchange_id: ExchangeId, observed_at: DateTime<Utc>) -> Self {
        Self {
            schema_version: SchemaVersion::current(),
            exchange_id,
            market_types: vec![MarketType::Spot],
            market_data: MarketDataCapability::default(),
            account: AccountCapability::default(),
            orders: OrderCapability::default(),
            rate_limits: Vec::new(),
            symbols: Vec::new(),
            observed_at,
        }
    }

    pub fn supports_market_type(&self, market_type: MarketType) -> bool {
        self.market_types.contains(&market_type)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventEnvelope<T> {
    pub schema_version: SchemaVersion,
    pub event_id: String,
    pub tenant_id: TenantId,
    pub account_id: Option<AccountId>,
    pub strategy_id: Option<StrategyId>,
    pub run_id: Option<RunId>,
    pub command_id: Option<CommandId>,
    pub correlation_id: Option<CorrelationId>,
    pub event_type: String,
    pub occurred_at: DateTime<Utc>,
    pub payload: T,
}

impl<T> EventEnvelope<T> {
    pub fn new(
        tenant_id: TenantId,
        event_id: impl Into<String>,
        event_type: impl Into<String>,
        occurred_at: DateTime<Utc>,
        payload: T,
    ) -> Result<Self, ValidationError> {
        let event_id = event_id.into().trim().to_string();
        if event_id.is_empty() {
            return Err(ValidationError::Empty { field: "event_id" });
        }
        let event_type = event_type.into().trim().to_string();
        if event_type.is_empty() {
            return Err(ValidationError::Empty {
                field: "event_type",
            });
        }
        Ok(Self {
            schema_version: SchemaVersion::current(),
            event_id,
            tenant_id,
            account_id: None,
            strategy_id: None,
            run_id: None,
            command_id: None,
            correlation_id: None,
            event_type,
            occurred_at,
            payload,
        })
    }

    pub fn with_account_id(mut self, account_id: AccountId) -> Self {
        self.account_id = Some(account_id);
        self
    }

    pub fn with_strategy_id(mut self, strategy_id: StrategyId) -> Self {
        self.strategy_id = Some(strategy_id);
        self
    }

    pub fn with_run_id(mut self, run_id: RunId) -> Self {
        self.run_id = Some(run_id);
        self
    }

    pub fn with_command_id(mut self, command_id: CommandId) -> Self {
        self.command_id = Some(command_id);
        self
    }

    pub fn with_correlation_id(mut self, correlation_id: CorrelationId) -> Self {
        self.correlation_id = Some(correlation_id);
        self
    }
}

fn normalize_id(value: String, field: &'static str) -> Result<String, ValidationError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ValidationError::Empty { field });
    }
    for ch in value.chars() {
        if !(ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | ':' | '.')) {
            return Err(ValidationError::InvalidCharacter { field, ch });
        }
    }
    Ok(value.to_string())
}

fn normalize_asset(value: &str, field: &'static str) -> Result<String, ValidationError> {
    let value = value.trim().to_ascii_uppercase();
    if value.is_empty() {
        return Err(ValidationError::Empty { field });
    }
    for ch in value.chars() {
        if !(ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-')) {
            return Err(ValidationError::InvalidCharacter { field, ch });
        }
    }
    Ok(value)
}

fn validate_finite(field: &'static str, value: f64) -> Result<(), ValidationError> {
    if value.is_finite() {
        Ok(())
    } else {
        Err(ValidationError::NonFinite { field })
    }
}

fn validate_positive(field: &'static str, value: f64) -> Result<(), ValidationError> {
    validate_finite(field, value)?;
    if value > 0.0 {
        Ok(())
    } else {
        Err(ValidationError::NotPositive { field })
    }
}

fn validate_non_negative(field: &'static str, value: f64) -> Result<(), ValidationError> {
    validate_finite(field, value)?;
    if value >= 0.0 {
        Ok(())
    } else {
        Err(ValidationError::Negative { field })
    }
}

fn validate_optional_finite(
    field: &'static str,
    value: Option<f64>,
) -> Result<(), ValidationError> {
    if let Some(value) = value {
        validate_finite(field, value)?;
    }
    Ok(())
}

fn validate_optional_positive(
    field: &'static str,
    value: Option<f64>,
) -> Result<(), ValidationError> {
    if let Some(value) = value {
        validate_positive(field, value)?;
    }
    Ok(())
}

fn validate_optional_non_negative(
    field: &'static str,
    value: Option<f64>,
) -> Result<(), ValidationError> {
    if let Some(value) = value {
        validate_non_negative(field, value)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exchange_id() -> ExchangeId {
        ExchangeId::new("Binance").expect("valid exchange id")
    }

    fn tenant_id() -> TenantId {
        TenantId::new("tenant-a").expect("valid tenant id")
    }

    #[test]
    fn schema_version_should_parse_and_display() {
        let version = "1.7".parse::<SchemaVersion>().expect("valid version");

        assert_eq!(version.major, 1);
        assert_eq!(version.minor, 7);
        assert_eq!(version.to_string(), "1.7");
        assert!(SchemaVersion::new(1, 0).is_compatible_with(version));
        assert!(!SchemaVersion::new(2, 0).is_compatible_with(version));
    }

    #[test]
    fn ids_should_trim_and_reject_invalid_characters() {
        let id = StrategyId::new(" trend-alpha.1 ").expect("valid id");

        assert_eq!(id.as_str(), "trend-alpha.1");
        assert!(TenantId::new("bad tenant").is_err());
        assert!(RunId::new("").is_err());
    }

    #[test]
    fn exchange_id_should_normalize_to_lowercase() {
        let id = ExchangeId::new("OKX").expect("valid exchange id");

        assert_eq!(id.as_str(), "okx");
    }

    #[test]
    fn canonical_symbol_should_normalize_and_validate() {
        let symbol = CanonicalSymbol::parse(" btc / usdt ").expect("valid symbol");

        assert_eq!(symbol.as_str(), "BTC/USDT");
        assert_eq!(symbol.base_asset(), "BTC");
        assert_eq!(symbol.quote_asset(), "USDT");
        assert!(CanonicalSymbol::parse("BTCUSDT").is_err());
        assert!(CanonicalSymbol::new("BTC", "BTC").is_err());
    }

    #[test]
    fn serializable_types_should_include_schema_version() {
        let balance = AssetBalance::new("usdt", 10.0, 7.0, 3.0).expect("valid balance");
        let json = serde_json::to_value(balance).expect("serializes");

        assert_eq!(
            json["schema_version"]["major"],
            CURRENT_SCHEMA_VERSION_MAJOR
        );
        assert_eq!(json["asset"], "USDT");
    }

    #[test]
    fn asset_balance_should_calculate_effective_available_after_reservation() {
        let balance = AssetBalance::new("BTC", 2.0, 1.5, 0.5)
            .expect("valid balance")
            .with_reservation(0.75)
            .expect("valid reservation");

        assert_eq!(balance.effective_available, 0.75);
        assert!(balance.with_reservation(-1.0).is_err());
    }

    #[test]
    fn order_book_snapshot_should_reject_crossed_books() {
        let symbol = CanonicalSymbol::new("BTC", "USDT").expect("valid symbol");
        let snapshot = OrderBookSnapshot::new(
            exchange_id(),
            MarketType::Spot,
            symbol,
            vec![OrderBookLevel::new(101.0, 1.0).expect("valid level")],
            vec![OrderBookLevel::new(100.0, 1.0).expect("valid level")],
            Utc::now(),
        );

        assert!(matches!(snapshot, Err(ValidationError::CrossedOrderBook)));
    }

    #[test]
    fn order_book_snapshot_should_return_mid_price() {
        let symbol = CanonicalSymbol::new("ETH", "USDT").expect("valid symbol");
        let snapshot = OrderBookSnapshot::new(
            exchange_id(),
            MarketType::Spot,
            symbol,
            vec![OrderBookLevel::new(99.0, 1.0).expect("valid level")],
            vec![OrderBookLevel::new(101.0, 1.0).expect("valid level")],
            Utc::now(),
        )
        .expect("valid snapshot");

        assert_eq!(snapshot.mid_price(), Some(100.0));
    }

    #[test]
    fn fill_should_use_quote_quantity_for_notional_when_available() {
        let fill = Fill {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id(),
            account_id: AccountId::new("acct-a").expect("valid account id"),
            exchange_id: exchange_id(),
            market_type: MarketType::Perpetual,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT").expect("valid symbol"),
            exchange_symbol: None,
            order_id: Some("order-1".to_string()),
            client_order_id: Some("client-1".to_string()),
            fill_id: Some("fill-1".to_string()),
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            status: FillStatus::Confirmed,
            liquidity_role: LiquidityRole::Taker,
            price: 100.0,
            quantity: 2.0,
            quote_quantity: Some(205.0),
            fee_asset: Some("USDT".to_string()),
            fee_amount: Some(0.2),
            fee_rate: Some(0.001),
            realized_pnl: None,
            filled_at: Utc::now(),
            received_at: Utc::now(),
        };

        assert_eq!(fill.notional(), 205.0);
        fill.validate().expect("valid fill");
    }

    #[test]
    fn exchange_error_class_should_expose_operational_traits() {
        assert!(ExchangeErrorClass::RateLimited.is_retryable());
        assert!(ExchangeErrorClass::UnknownOrderState.requires_reconciliation());
        assert!(!ExchangeErrorClass::InvalidSymbol.is_retryable());
    }

    #[test]
    fn capability_model_should_validate_positive_limits() {
        let rate_limit =
            RateLimitCapability::new(RateLimitScope::Orders, 10, 1_000).expect("valid rate limit");
        let mut capability = ExchangeCapability::new(exchange_id(), Utc::now());
        capability.rate_limits.push(rate_limit);
        capability.market_types.push(MarketType::Perpetual);

        assert!(capability.supports_market_type(MarketType::Perpetual));
        assert!(RateLimitCapability::new(RateLimitScope::Orders, 0, 1_000).is_err());
    }

    #[test]
    fn event_envelope_should_carry_identity_and_payload() {
        let envelope = EventEnvelope::new(
            tenant_id(),
            "evt-1",
            "balance.updated",
            Utc::now(),
            serde_json::json!({ "asset": "USDT" }),
        )
        .expect("valid envelope")
        .with_account_id(AccountId::new("acct-a").expect("valid account id"))
        .with_correlation_id(CorrelationId::new("corr-a").expect("valid correlation id"));

        let json = serde_json::to_value(&envelope).expect("serializes");

        assert_eq!(
            json["schema_version"]["major"],
            CURRENT_SCHEMA_VERSION_MAJOR
        );
        assert_eq!(json["event_type"], "balance.updated");
        assert_eq!(json["account_id"], "acct-a");
        assert_eq!(envelope.payload["asset"], "USDT");
    }
}
