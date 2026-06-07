use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::mpsc;

use crate::core::exchange::Exchange as LegacyExchange;
use crate::exchanges::client_order_id::{generate_client_order_id, validate_client_order_id};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExchangeName {
    Binance,
    Okx,
    Mexc,
    CoinEx,
    Paper,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum MarketType {
    #[default]
    Spot,
    Perpetual,
}

impl MarketType {
    pub fn to_legacy(self) -> crate::core::types::MarketType {
        match self {
            Self::Spot => crate::core::types::MarketType::Spot,
            Self::Perpetual => crate::core::types::MarketType::Futures,
        }
    }

    pub fn from_legacy(value: crate::core::types::MarketType) -> Self {
        match value {
            crate::core::types::MarketType::Spot => Self::Spot,
            crate::core::types::MarketType::Futures => Self::Perpetual,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderSide {
    Buy,
    Sell,
}

impl From<OrderSide> for crate::core::types::OrderSide {
    fn from(value: OrderSide) -> Self {
        match value {
            OrderSide::Buy => Self::Buy,
            OrderSide::Sell => Self::Sell,
        }
    }
}

impl From<crate::core::types::OrderSide> for OrderSide {
    fn from(value: crate::core::types::OrderSide) -> Self {
        match value {
            crate::core::types::OrderSide::Buy => Self::Buy,
            crate::core::types::OrderSide::Sell => Self::Sell,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PositionSide {
    Long,
    Short,
    Net,
    None,
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderType {
    Market,
    Limit,
    PostOnly,
    IOC,
    FOK,
}

impl OrderType {
    pub fn is_limit_price_required(self) -> bool {
        matches!(self, Self::Limit | Self::PostOnly | Self::IOC | Self::FOK)
    }
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TimeInForce {
    GTC,
    IOC,
    FOK,
    GTX,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
    Expired,
    Unknown,
}

impl OrderStatus {
    pub fn from_legacy(value: &crate::core::types::OrderStatus) -> Self {
        match value {
            crate::core::types::OrderStatus::Open | crate::core::types::OrderStatus::Pending => {
                Self::New
            }
            crate::core::types::OrderStatus::PartiallyFilled => Self::PartiallyFilled,
            crate::core::types::OrderStatus::Closed => Self::Filled,
            crate::core::types::OrderStatus::Canceled => Self::Cancelled,
            crate::core::types::OrderStatus::Rejected => Self::Rejected,
            crate::core::types::OrderStatus::Expired => Self::Expired,
            crate::core::types::OrderStatus::Triggered => Self::New,
        }
    }

    pub fn from_exchange_status(value: &str) -> Self {
        match value.trim().to_ascii_uppercase().as_str() {
            "NEW" | "OPEN" | "PENDING" | "LIVE" => Self::New,
            "PARTIALLY_FILLED" | "PARTIAL" | "PARTIAL_FILLED" => Self::PartiallyFilled,
            "FILLED" | "CLOSED" | "FULLY_FILLED" => Self::Filled,
            "CANCELED" | "CANCELLED" => Self::Cancelled,
            "REJECTED" => Self::Rejected,
            "EXPIRED" => Self::Expired,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LiquidityRole {
    Maker,
    Taker,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SymbolStatus {
    Trading,
    Suspended,
    Delisted,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FeeRateSource {
    ExchangeApi,
    ConfigOverride,
    DefaultFallback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExchangeErrorClass {
    InsufficientBalance,
    Oversold,
    MinNotionalViolation,
    InvalidSymbol,
    InvalidPrecision,
    InvalidClientOrderId,
    RateLimited,
    AuthenticationFailed,
    PermissionDenied,
    NetworkError,
    Timeout,
    ExchangeUnavailable,
    OrderNotFound,
    OrderRejected,
    DuplicateClientOrderId,
    UnsupportedOrderType,
    UnsupportedTimeInForce,
    UnsupportedCapability,
    StaleMarketData,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExchangeError {
    pub exchange: String,
    pub class: ExchangeErrorClass,
    pub code: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SymbolRule {
    pub exchange: String,
    pub market_type: MarketType,
    pub internal_symbol: String,
    pub exchange_symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub price_precision: u32,
    pub quantity_precision: u32,
    pub tick_size: f64,
    pub step_size: f64,
    pub min_quantity: f64,
    pub min_notional: f64,
    pub max_quantity: Option<f64>,
    pub supported_order_types: Vec<OrderType>,
    pub supported_time_in_force: Vec<TimeInForce>,
    pub status: SymbolStatus,
    pub raw_metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AssetBalance {
    pub asset: String,
    pub total: f64,
    pub available: f64,
    pub locked: f64,
    #[serde(default)]
    pub locked_by_exchange: f64,
    #[serde(default)]
    pub locally_reserved: f64,
    #[serde(default)]
    pub effective_available: f64,
}

impl AssetBalance {
    pub fn new(
        asset: impl Into<String>,
        total: f64,
        available: f64,
        locked_by_exchange: f64,
    ) -> Self {
        let locally_reserved = 0.0;
        Self {
            asset: asset.into(),
            total,
            available,
            locked: locked_by_exchange,
            locked_by_exchange,
            locally_reserved,
            effective_available: (available - locally_reserved).max(0.0),
        }
    }

    pub fn with_reservation(mut self, locally_reserved: f64) -> Self {
        self.locally_reserved = locally_reserved.max(0.0);
        self.effective_available = (self.available - self.locally_reserved).max(0.0);
        self
    }

    pub fn from_legacy(balance: crate::core::types::Balance) -> Self {
        Self::new(balance.currency, balance.total, balance.free, balance.used)
    }

    pub fn validate(&self) -> Result<(), ExchangeClientError> {
        validate_positive_or_zero("total", self.total)?;
        validate_positive_or_zero("available", self.available)?;
        validate_positive_or_zero("locked", self.locked)?;
        validate_positive_or_zero("locked_by_exchange", self.locked_by_exchange)?;
        validate_positive_or_zero("locally_reserved", self.locally_reserved)?;
        validate_positive_or_zero("effective_available", self.effective_available)?;
        if self.asset.trim().is_empty() {
            return Err(ExchangeClientError::Validation {
                field: "asset",
                reason: "asset must not be empty".to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BalanceSnapshot {
    pub exchange: String,
    pub market_type: MarketType,
    pub balances: Vec<AssetBalance>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderBookLevel {
    pub price: f64,
    pub quantity: f64,
}

impl OrderBookLevel {
    pub fn validate(&self) -> Result<(), ExchangeClientError> {
        validate_positive("price", self.price)?;
        validate_positive("quantity", self.quantity)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderBookSnapshot {
    pub exchange: String,
    pub market_type: MarketType,
    pub symbol: String,
    pub bids: Vec<OrderBookLevel>,
    pub asks: Vec<OrderBookLevel>,
    #[serde(default)]
    pub best_bid: Option<f64>,
    #[serde(default)]
    pub best_ask: Option<f64>,
    pub exchange_timestamp: Option<DateTime<Utc>>,
    pub received_at: DateTime<Utc>,
    #[serde(default)]
    pub latency_ms: Option<i64>,
    pub sequence: Option<u64>,
    #[serde(default)]
    pub is_stale: bool,
}

impl OrderBookSnapshot {
    pub fn from_legacy(
        exchange: impl Into<String>,
        market_type: MarketType,
        book: crate::core::types::OrderBook,
    ) -> Self {
        let bids = book
            .bids
            .into_iter()
            .map(|level| OrderBookLevel {
                price: level[0],
                quantity: level[1],
            })
            .collect::<Vec<_>>();
        let asks = book
            .asks
            .into_iter()
            .map(|level| OrderBookLevel {
                price: level[0],
                quantity: level[1],
            })
            .collect::<Vec<_>>();
        Self {
            exchange: exchange.into(),
            market_type,
            symbol: book.symbol,
            best_bid: bids.first().map(|level| level.price),
            best_ask: asks.first().map(|level| level.price),
            bids,
            asks,
            exchange_timestamp: Some(book.timestamp),
            received_at: Utc::now(),
            latency_ms: None,
            sequence: None,
            is_stale: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradeFill {
    pub exchange: String,
    pub market_type: MarketType,
    pub symbol: String,
    pub trade_id: Option<String>,
    pub order_id: Option<String>,
    pub client_order_id: Option<String>,
    pub side: OrderSide,
    pub price: f64,
    pub quantity: f64,
    pub fee_asset: Option<String>,
    pub fee_amount: Option<f64>,
    pub liquidity: LiquidityRole,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderRequest {
    pub market_type: MarketType,
    pub symbol: String,
    pub side: OrderSide,
    pub position_side: PositionSide,
    pub order_type: OrderType,
    pub time_in_force: Option<TimeInForce>,
    pub quantity: f64,
    pub price: Option<f64>,
    pub client_order_id: Option<String>,
    pub reduce_only: bool,
}

impl OrderRequest {
    pub fn spot_market_buy(symbol: impl Into<String>, quantity: f64) -> Self {
        Self {
            market_type: MarketType::Spot,
            symbol: symbol.into(),
            side: OrderSide::Buy,
            position_side: PositionSide::None,
            order_type: OrderType::Market,
            time_in_force: None,
            quantity,
            price: None,
            client_order_id: None,
            reduce_only: false,
        }
    }

    pub fn validate(&self) -> Result<(), ExchangeClientError> {
        if self.symbol.trim().is_empty() {
            return Err(ExchangeClientError::Validation {
                field: "symbol",
                reason: "symbol must not be empty".to_string(),
            });
        }
        validate_positive("quantity", self.quantity)?;
        if self.order_type.is_limit_price_required() {
            match self.price {
                Some(price) => validate_positive("price", price)?,
                None => {
                    return Err(ExchangeClientError::Validation {
                        field: "price",
                        reason: "price is required for limit-style orders".to_string(),
                    });
                }
            }
        }
        if self.market_type == MarketType::Spot {
            if self.position_side != PositionSide::None && self.position_side != PositionSide::Net {
                return Err(ExchangeClientError::Validation {
                    field: "position_side",
                    reason: "spot orders must use PositionSide::None or PositionSide::Net"
                        .to_string(),
                });
            }
            if self.reduce_only {
                return Err(ExchangeClientError::Validation {
                    field: "reduce_only",
                    reason: "spot orders do not support reduce_only".to_string(),
                });
            }
        }
        Ok(())
    }

    pub fn to_legacy(&self) -> Result<crate::core::types::OrderRequest, ExchangeClientError> {
        self.validate()?;
        let (order_type, time_in_force, post_only) = match self.order_type {
            OrderType::Market => (crate::core::types::OrderType::Market, None, false),
            OrderType::Limit => (
                crate::core::types::OrderType::Limit,
                self.time_in_force.map(|value| value.as_legacy_string()),
                false,
            ),
            OrderType::PostOnly => (
                crate::core::types::OrderType::Limit,
                Some(TimeInForce::GTX.as_legacy_string()),
                true,
            ),
            OrderType::IOC => (
                crate::core::types::OrderType::Limit,
                Some(TimeInForce::IOC.as_legacy_string()),
                false,
            ),
            OrderType::FOK => (
                crate::core::types::OrderType::Limit,
                Some(TimeInForce::FOK.as_legacy_string()),
                false,
            ),
        };
        Ok(crate::core::types::OrderRequest {
            symbol: self.symbol.clone(),
            side: self.side.into(),
            order_type,
            amount: self.quantity,
            price: self.price,
            market_type: self.market_type.to_legacy(),
            params: None,
            client_order_id: self.client_order_id.clone(),
            time_in_force,
            reduce_only: Some(self.reduce_only),
            post_only: Some(post_only),
        })
    }
}

impl TimeInForce {
    fn as_legacy_string(self) -> String {
        match self {
            Self::GTC => "GTC",
            Self::IOC => "IOC",
            Self::FOK => "FOK",
            Self::GTX => "GTX",
        }
        .to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuoteMarketOrderRequest {
    pub market_type: MarketType,
    pub symbol: String,
    pub side: OrderSide,
    pub quote_quantity: f64,
    pub client_order_id: Option<String>,
}

impl QuoteMarketOrderRequest {
    pub fn spot_buy(symbol: impl Into<String>, quote_quantity: f64) -> Self {
        Self {
            market_type: MarketType::Spot,
            symbol: symbol.into(),
            side: OrderSide::Buy,
            quote_quantity,
            client_order_id: None,
        }
    }

    pub fn validate(&self) -> Result<(), ExchangeClientError> {
        if self.market_type != MarketType::Spot {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: "quote-sized market orders are currently a spot-only request model"
                    .to_string(),
            });
        }
        if self.symbol.trim().is_empty() {
            return Err(ExchangeClientError::Validation {
                field: "symbol",
                reason: "symbol must not be empty".to_string(),
            });
        }
        validate_positive("quote_quantity", self.quote_quantity)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AmendOrderRequest {
    pub market_type: MarketType,
    pub symbol: String,
    pub order_id: Option<String>,
    pub client_order_id: Option<String>,
    pub new_client_order_id: Option<String>,
    pub new_quantity: f64,
}

impl AmendOrderRequest {
    pub fn reduce_quantity_by_order_id(
        market_type: MarketType,
        symbol: impl Into<String>,
        order_id: impl Into<String>,
        new_quantity: f64,
    ) -> Self {
        Self {
            market_type,
            symbol: symbol.into(),
            order_id: Some(order_id.into()),
            client_order_id: None,
            new_client_order_id: None,
            new_quantity,
        }
    }

    pub fn validate(&self) -> Result<(), ExchangeClientError> {
        if self.market_type != MarketType::Spot {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: "spot amend requests are currently spot-only".to_string(),
            });
        }
        if self.symbol.trim().is_empty() {
            return Err(ExchangeClientError::Validation {
                field: "symbol",
                reason: "symbol must not be empty".to_string(),
            });
        }
        if self
            .order_id
            .as_deref()
            .unwrap_or_default()
            .trim()
            .is_empty()
            && self
                .client_order_id
                .as_deref()
                .unwrap_or_default()
                .trim()
                .is_empty()
        {
            return Err(ExchangeClientError::Validation {
                field: "order_id",
                reason: "order_id or client_order_id is required".to_string(),
            });
        }
        validate_positive("new_quantity", self.new_quantity)
    }

    pub fn order_id(&self) -> Option<&str> {
        self.order_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
    }

    pub fn client_order_id(&self) -> Option<&str> {
        self.client_order_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
    }

    pub fn new_client_order_id(&self) -> Option<&str> {
        self.new_client_order_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderListKind {
    Oco,
    Oto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderListLegType {
    Market,
    Limit,
    LimitMaker,
    StopLoss,
    StopLossLimit,
    TakeProfit,
    TakeProfitLimit,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderListConditionalLeg {
    pub order_type: OrderListLegType,
    pub price: Option<f64>,
    pub stop_price: Option<f64>,
    pub time_in_force: Option<TimeInForce>,
    pub client_order_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderListOrderLeg {
    pub side: OrderSide,
    pub order_type: OrderListLegType,
    pub quantity: f64,
    pub price: Option<f64>,
    pub stop_price: Option<f64>,
    pub time_in_force: Option<TimeInForce>,
    pub client_order_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OrderListRequest {
    Oco {
        market_type: MarketType,
        symbol: String,
        list_client_order_id: Option<String>,
        side: OrderSide,
        quantity: f64,
        above: OrderListConditionalLeg,
        below: OrderListConditionalLeg,
    },
    Oto {
        market_type: MarketType,
        symbol: String,
        list_client_order_id: Option<String>,
        working: OrderListOrderLeg,
        pending: OrderListOrderLeg,
    },
}

impl OrderListRequest {
    pub fn validate(&self) -> Result<(), ExchangeClientError> {
        let (market_type, symbol) = match self {
            Self::Oco {
                market_type,
                symbol,
                quantity,
                above,
                below,
                ..
            } => {
                validate_positive("quantity", *quantity)?;
                validate_order_list_conditional_leg("above", above)?;
                validate_order_list_conditional_leg("below", below)?;
                (*market_type, symbol)
            }
            Self::Oto {
                market_type,
                symbol,
                working,
                pending,
                ..
            } => {
                validate_positive("working.quantity", working.quantity)?;
                validate_positive("pending.quantity", pending.quantity)?;
                validate_order_list_order_leg("working", working)?;
                validate_order_list_order_leg("pending", pending)?;
                (*market_type, symbol)
            }
        };
        if market_type != MarketType::Spot {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: "order lists are currently a spot-only request model".to_string(),
            });
        }
        if symbol.trim().is_empty() {
            return Err(ExchangeClientError::Validation {
                field: "symbol",
                reason: "symbol must not be empty".to_string(),
            });
        }
        Ok(())
    }

    pub fn symbol(&self) -> &str {
        match self {
            Self::Oco { symbol, .. } | Self::Oto { symbol, .. } => symbol,
        }
    }

    pub fn kind(&self) -> OrderListKind {
        match self {
            Self::Oco { .. } => OrderListKind::Oco,
            Self::Oto { .. } => OrderListKind::Oto,
        }
    }
}

fn validate_order_list_conditional_leg(
    label: &'static str,
    leg: &OrderListConditionalLeg,
) -> Result<(), ExchangeClientError> {
    validate_order_list_leg_prices(label, leg.order_type, leg.price, leg.stop_price)
}

fn validate_order_list_order_leg(
    label: &'static str,
    leg: &OrderListOrderLeg,
) -> Result<(), ExchangeClientError> {
    validate_order_list_leg_prices(label, leg.order_type, leg.price, leg.stop_price)
}

fn validate_order_list_leg_prices(
    label: &'static str,
    order_type: OrderListLegType,
    price: Option<f64>,
    stop_price: Option<f64>,
) -> Result<(), ExchangeClientError> {
    if let Some(price) = price {
        validate_positive("price", price)?;
    }
    if let Some(stop_price) = stop_price {
        validate_positive("stop_price", stop_price)?;
    }

    if matches!(
        order_type,
        OrderListLegType::Limit
            | OrderListLegType::LimitMaker
            | OrderListLegType::StopLossLimit
            | OrderListLegType::TakeProfitLimit
    ) && price.is_none()
    {
        return Err(ExchangeClientError::Validation {
            field: "price",
            reason: format!("{label} order-list leg requires price for {order_type:?}"),
        });
    }

    if matches!(
        order_type,
        OrderListLegType::StopLoss
            | OrderListLegType::StopLossLimit
            | OrderListLegType::TakeProfit
            | OrderListLegType::TakeProfitLimit
    ) && stop_price.is_none()
    {
        return Err(ExchangeClientError::Validation {
            field: "stop_price",
            reason: format!("{label} order-list leg requires stop_price for {order_type:?}"),
        });
    }

    Ok(())
}

fn validate_optional_request_client_order_id(
    exchange: &str,
    market_type: MarketType,
    client_order_id: Option<&str>,
) -> ExchangeClientResult<()> {
    if let Some(client_order_id) = client_order_id {
        validate_client_order_id(exchange, market_type, client_order_id).map_err(|error| {
            ExchangeClientError::Validation {
                field: "client_order_id",
                reason: error.to_string(),
            }
        })?;
    }
    Ok(())
}

fn validate_order_list_request_client_ids(
    exchange: &str,
    request: &OrderListRequest,
) -> ExchangeClientResult<()> {
    match request {
        OrderListRequest::Oco {
            list_client_order_id,
            above,
            below,
            ..
        } => {
            validate_optional_request_client_order_id(
                exchange,
                MarketType::Spot,
                list_client_order_id.as_deref(),
            )?;
            validate_optional_request_client_order_id(
                exchange,
                MarketType::Spot,
                above.client_order_id.as_deref(),
            )?;
            validate_optional_request_client_order_id(
                exchange,
                MarketType::Spot,
                below.client_order_id.as_deref(),
            )?;
        }
        OrderListRequest::Oto {
            list_client_order_id,
            working,
            pending,
            ..
        } => {
            validate_optional_request_client_order_id(
                exchange,
                MarketType::Spot,
                list_client_order_id.as_deref(),
            )?;
            validate_optional_request_client_order_id(
                exchange,
                MarketType::Spot,
                working.client_order_id.as_deref(),
            )?;
            validate_optional_request_client_order_id(
                exchange,
                MarketType::Spot,
                pending.client_order_id.as_deref(),
            )?;
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderListResponse {
    pub exchange: String,
    pub market_type: MarketType,
    pub symbol: String,
    pub order_list_id: Option<String>,
    pub list_client_order_id: Option<String>,
    pub kind: OrderListKind,
    pub list_status_type: Option<String>,
    pub list_order_status: Option<String>,
    pub orders: Vec<OrderResponse>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderResponse {
    pub exchange: String,
    pub market_type: MarketType,
    pub symbol: String,
    pub order_id: String,
    pub client_order_id: Option<String>,
    pub side: OrderSide,
    pub position_side: PositionSide,
    pub order_type: OrderType,
    pub status: OrderStatus,
    pub price: Option<f64>,
    pub quantity: f64,
    pub filled_quantity: f64,
    pub average_price: Option<f64>,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
}

impl OrderResponse {
    pub fn from_legacy(exchange: impl Into<String>, order: crate::core::types::Order) -> Self {
        let order_type = match order.order_type {
            crate::core::types::OrderType::Market => OrderType::Market,
            crate::core::types::OrderType::Limit => OrderType::Limit,
            _ => OrderType::Limit,
        };
        Self {
            exchange: exchange.into(),
            market_type: MarketType::from_legacy(order.market_type),
            symbol: order.symbol,
            order_id: order.id,
            client_order_id: None,
            side: order.side.into(),
            position_side: PositionSide::None,
            order_type,
            status: OrderStatus::from_legacy(&order.status),
            price: order.price,
            quantity: order.amount,
            filled_quantity: order.filled,
            average_price: None,
            created_at: order.timestamp,
            updated_at: order.last_trade_timestamp,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelOrderRequest {
    pub market_type: MarketType,
    pub symbol: String,
    pub order_id: Option<String>,
    pub client_order_id: Option<String>,
}

impl CancelOrderRequest {
    pub fn validate(&self) -> Result<(), ExchangeClientError> {
        if self.symbol.trim().is_empty() {
            return Err(ExchangeClientError::Validation {
                field: "symbol",
                reason: "symbol must not be empty".to_string(),
            });
        }
        if self
            .order_id
            .as_deref()
            .unwrap_or_default()
            .trim()
            .is_empty()
            && self
                .client_order_id
                .as_deref()
                .unwrap_or_default()
                .trim()
                .is_empty()
        {
            return Err(ExchangeClientError::Validation {
                field: "order_id",
                reason: "order_id or client_order_id is required".to_string(),
            });
        }
        Ok(())
    }

    pub fn order_id(&self) -> Option<&str> {
        self.order_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
    }

    pub fn client_order_id(&self) -> Option<&str> {
        self.client_order_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
    }

    fn order_lookup_id(&self) -> &str {
        self.order_id()
            .or_else(|| self.client_order_id())
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelOrderResponse {
    pub exchange: String,
    pub market_type: MarketType,
    pub symbol: String,
    pub order_id: Option<String>,
    pub client_order_id: Option<String>,
    pub status: OrderStatus,
    pub cancelled_at: DateTime<Utc>,
}

impl CancelOrderResponse {
    pub fn from_legacy(exchange: impl Into<String>, order: crate::core::types::Order) -> Self {
        Self {
            exchange: exchange.into(),
            market_type: MarketType::from_legacy(order.market_type),
            symbol: order.symbol,
            order_id: Some(order.id),
            client_order_id: None,
            status: OrderStatus::from_legacy(&order.status),
            cancelled_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelAllOrdersRequest {
    pub market_type: MarketType,
    pub symbol: Option<String>,
}

impl CancelAllOrdersRequest {
    pub fn for_symbol(market_type: MarketType, symbol: impl Into<String>) -> Self {
        Self {
            market_type,
            symbol: Some(symbol.into()),
        }
    }

    pub fn validate_for_market_type(
        &self,
        expected_market_type: MarketType,
    ) -> Result<(), ExchangeClientError> {
        if self.market_type != expected_market_type {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: format!(
                    "cancel-all request market type {:?} does not match client market type {:?}",
                    self.market_type, expected_market_type
                ),
            });
        }
        if self
            .symbol
            .as_deref()
            .is_some_and(|symbol| symbol.trim().is_empty())
        {
            return Err(ExchangeClientError::Validation {
                field: "symbol",
                reason: "symbol must not be empty when provided".to_string(),
            });
        }
        Ok(())
    }

    pub fn validate_symbol_required(&self) -> Result<&str, ExchangeClientError> {
        self.symbol
            .as_deref()
            .map(str::trim)
            .filter(|symbol| !symbol.is_empty())
            .ok_or_else(|| ExchangeClientError::Validation {
                field: "symbol",
                reason: "symbol is required for this exchange cancel-all endpoint".to_string(),
            })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelAllOrdersResponse {
    pub exchange: String,
    pub market_type: MarketType,
    pub symbol: Option<String>,
    pub cancelled_orders: usize,
    pub cancelled_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FeeRate {
    pub maker: f64,
    pub taker: f64,
    #[serde(default)]
    pub maker_fee_rate: f64,
    #[serde(default)]
    pub taker_fee_rate: f64,
    #[serde(default = "default_fee_rate_source")]
    pub source: FeeRateSource,
    #[serde(default = "Utc::now")]
    pub timestamp: DateTime<Utc>,
}

impl FeeRate {
    pub fn new(maker: f64, taker: f64, source: FeeRateSource) -> Self {
        Self {
            maker,
            taker,
            maker_fee_rate: maker,
            taker_fee_rate: taker,
            source,
            timestamp: Utc::now(),
        }
    }
}

fn default_fee_rate_source() -> FeeRateSource {
    FeeRateSource::DefaultFallback
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenOrder {
    pub exchange: String,
    pub market_type: MarketType,
    pub symbol: String,
    pub order_id: String,
    pub client_order_id: Option<String>,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub time_in_force: Option<TimeInForce>,
    pub price: Option<f64>,
    pub quantity: f64,
    pub filled_quantity: f64,
    pub status: OrderStatus,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExchangeHealthStatus {
    pub exchange: String,
    pub market_type: MarketType,
    pub connected: bool,
    pub public_ws_healthy: bool,
    pub private_ws_healthy: bool,
    pub rest_healthy: bool,
    pub stale_books: Vec<String>,
    pub last_error: Option<ExchangeError>,
    pub checked_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExchangeClientCapabilities {
    pub exchange: String,
    pub market_type: MarketType,
    pub supports_spot: bool,
    pub supports_perpetual: bool,
    pub supports_market_order: bool,
    pub supports_limit_order: bool,
    pub supports_post_only: bool,
    pub supports_ioc: bool,
    pub supports_fok: bool,
    pub supports_cancel_order: bool,
    pub supports_cancel_all_orders: bool,
    #[serde(default)]
    pub supports_quote_market_order: bool,
    #[serde(default)]
    pub supports_amend_order: bool,
    #[serde(default)]
    pub supports_order_list: bool,
    pub supports_query_order: bool,
    pub supports_open_orders: bool,
    pub supports_balances: bool,
    pub supports_positions: bool,
    pub supports_leverage: bool,
    pub supports_funding_rate: bool,
    pub supports_public_ws: bool,
    pub supports_private_user_stream: bool,
    pub supports_fee_api: bool,
    #[serde(default)]
    pub order_book: crate::data::L2BookCapability,
}

impl ExchangeClientCapabilities {
    pub fn spot(exchange: impl Into<String>) -> Self {
        Self {
            exchange: exchange.into(),
            market_type: MarketType::Spot,
            supports_spot: true,
            supports_perpetual: false,
            supports_market_order: true,
            supports_limit_order: true,
            supports_post_only: true,
            supports_ioc: true,
            supports_fok: true,
            supports_cancel_order: true,
            supports_cancel_all_orders: false,
            supports_quote_market_order: false,
            supports_amend_order: false,
            supports_order_list: false,
            supports_query_order: true,
            supports_open_orders: true,
            supports_balances: true,
            supports_positions: false,
            supports_leverage: false,
            supports_funding_rate: false,
            supports_public_ws: true,
            supports_private_user_stream: false,
            supports_fee_api: true,
            order_book: crate::data::L2BookCapability::best_effort_delta(None),
        }
    }

    pub fn legacy(exchange: impl Into<String>, market_type: MarketType) -> Self {
        Self {
            exchange: exchange.into(),
            market_type,
            supports_spot: market_type == MarketType::Spot,
            supports_perpetual: market_type == MarketType::Perpetual,
            supports_market_order: true,
            supports_limit_order: true,
            supports_post_only: false,
            supports_ioc: false,
            supports_fok: false,
            supports_cancel_order: true,
            supports_cancel_all_orders: false,
            supports_quote_market_order: false,
            supports_amend_order: false,
            supports_order_list: false,
            supports_query_order: true,
            supports_open_orders: true,
            supports_balances: true,
            supports_positions: market_type == MarketType::Perpetual,
            supports_leverage: market_type == MarketType::Perpetual,
            supports_funding_rate: market_type == MarketType::Perpetual,
            supports_public_ws: false,
            supports_private_user_stream: false,
            supports_fee_api: true,
            order_book: crate::data::L2BookCapability::snapshot_only(None),
        }
    }

    pub fn unsupported_reason(&self, capability: ExchangeCapability) -> Option<String> {
        let supported = match capability {
            ExchangeCapability::Spot => self.supports_spot,
            ExchangeCapability::Perpetual => self.supports_perpetual,
            ExchangeCapability::MarketOrder => self.supports_market_order,
            ExchangeCapability::LimitOrder => self.supports_limit_order,
            ExchangeCapability::PostOnly => self.supports_post_only,
            ExchangeCapability::Ioc => self.supports_ioc,
            ExchangeCapability::Fok => self.supports_fok,
            ExchangeCapability::CancelOrder => self.supports_cancel_order,
            ExchangeCapability::CancelAllOrders => self.supports_cancel_all_orders,
            ExchangeCapability::QuoteMarketOrder => self.supports_quote_market_order,
            ExchangeCapability::AmendOrder => self.supports_amend_order,
            ExchangeCapability::OrderList => self.supports_order_list,
            ExchangeCapability::QueryOrder => self.supports_query_order,
            ExchangeCapability::OpenOrders => self.supports_open_orders,
            ExchangeCapability::Balances => self.supports_balances,
            ExchangeCapability::Positions => self.supports_positions,
            ExchangeCapability::Leverage => self.supports_leverage,
            ExchangeCapability::FundingRate => self.supports_funding_rate,
            ExchangeCapability::PublicWebSocket => self.supports_public_ws,
            ExchangeCapability::PrivateUserStream => self.supports_private_user_stream,
            ExchangeCapability::FeeApi => self.supports_fee_api,
        };
        (!supported).then(|| {
            format!(
                "{} {:?} does not support {:?}",
                self.exchange, self.market_type, capability
            )
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExchangeCapability {
    Spot,
    Perpetual,
    MarketOrder,
    LimitOrder,
    PostOnly,
    Ioc,
    Fok,
    CancelOrder,
    CancelAllOrders,
    QuoteMarketOrder,
    AmendOrder,
    OrderList,
    QueryOrder,
    OpenOrders,
    Balances,
    Positions,
    Leverage,
    FundingRate,
    PublicWebSocket,
    PrivateUserStream,
    FeeApi,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UserStreamEvent {
    Order(OrderResponse),
    Fill(TradeFill),
    Balance(BalanceSnapshot),
    Disconnected { reason: Option<String> },
}

#[derive(Debug, Error)]
pub enum ExchangeClientError {
    #[error("exchange operation failed: {0}")]
    Exchange(#[from] crate::core::error::ExchangeError),
    #[error("{0:?}")]
    Classified(ExchangeError),
    #[error("validation failed for {field}: {reason}")]
    Validation { field: &'static str, reason: String },
    #[error("unsupported feature: {0}")]
    Unsupported(String),
}

pub type ExchangeClientResult<T> = Result<T, ExchangeClientError>;

#[async_trait]
pub trait ExchangeClient: Send + Sync {
    fn market_type(&self) -> MarketType;
    fn exchange_name(&self) -> &str;

    fn capabilities(&self) -> ExchangeClientCapabilities {
        match self.market_type() {
            MarketType::Spot => ExchangeClientCapabilities::spot(self.exchange_name()),
            MarketType::Perpetual => {
                ExchangeClientCapabilities::legacy(self.exchange_name(), MarketType::Perpetual)
            }
        }
    }

    fn normalize_symbol(&self, symbol: &str) -> ExchangeClientResult<String> {
        let normalized = symbol.trim().to_ascii_uppercase();
        if normalized.is_empty() {
            return Err(ExchangeClientError::Validation {
                field: "symbol",
                reason: "symbol must not be empty".to_string(),
            });
        }
        Ok(normalized)
    }

    async fn get_balances(&self) -> ExchangeClientResult<BalanceSnapshot>;

    async fn get_orderbook(
        &self,
        symbol: &str,
        depth: u16,
    ) -> ExchangeClientResult<OrderBookSnapshot>;

    async fn place_order(&self, request: OrderRequest) -> ExchangeClientResult<OrderResponse>;

    async fn place_quote_market_order(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeClientResult<OrderResponse> {
        request.validate()?;
        validate_optional_request_client_order_id(
            self.exchange_name(),
            request.market_type,
            request.client_order_id.as_deref(),
        )?;
        Err(ExchangeClientError::Unsupported(
            "quote-sized market orders are not implemented for this client".to_string(),
        ))
    }

    async fn amend_order(&self, request: AmendOrderRequest) -> ExchangeClientResult<OrderResponse> {
        request.validate()?;
        validate_optional_request_client_order_id(
            self.exchange_name(),
            request.market_type,
            request.client_order_id(),
        )?;
        validate_optional_request_client_order_id(
            self.exchange_name(),
            request.market_type,
            request.new_client_order_id(),
        )?;
        Err(ExchangeClientError::Unsupported(
            "amend order is not implemented for this client".to_string(),
        ))
    }

    async fn place_order_list(
        &self,
        request: OrderListRequest,
    ) -> ExchangeClientResult<OrderListResponse> {
        request.validate()?;
        validate_order_list_request_client_ids(self.exchange_name(), &request)?;
        Err(ExchangeClientError::Unsupported(
            "order lists are not implemented for this client".to_string(),
        ))
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeClientResult<CancelOrderResponse>;

    async fn cancel_all_orders(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeClientResult<CancelAllOrdersResponse> {
        request.validate_for_market_type(self.market_type())?;
        Err(ExchangeClientError::Unsupported(
            "cancel-all orders is not implemented for this client".to_string(),
        ))
    }

    async fn get_order(&self, symbol: &str, order_id: &str) -> ExchangeClientResult<OrderResponse>;

    async fn get_open_orders(
        &self,
        symbol: Option<&str>,
    ) -> ExchangeClientResult<Vec<OrderResponse>>;

    async fn get_fee_rate(&self, symbol: &str) -> ExchangeClientResult<FeeRate>;

    async fn load_symbol_rules(&self) -> ExchangeClientResult<Vec<SymbolRule>> {
        Err(ExchangeClientError::Unsupported(
            "symbol rule loading is not implemented for this client".to_string(),
        ))
    }

    async fn get_symbol_rule(&self, symbol: &str) -> ExchangeClientResult<Option<SymbolRule>> {
        let normalized = self.normalize_symbol(symbol)?;
        Ok(self
            .load_symbol_rules()
            .await?
            .into_iter()
            .find(|rule| rule.internal_symbol == normalized || rule.exchange_symbol == normalized))
    }

    fn denormalize_symbol(&self, symbol: &str) -> ExchangeClientResult<String> {
        self.normalize_symbol(symbol)
    }

    async fn get_recent_fills(&self, symbol: &str) -> ExchangeClientResult<Vec<TradeFill>> {
        self.normalize_symbol(symbol)?;
        Err(ExchangeClientError::Unsupported(
            "recent fills are not implemented for this client".to_string(),
        ))
    }

    async fn health_check(&self) -> ExchangeClientResult<ExchangeHealthStatus> {
        Ok(ExchangeHealthStatus {
            exchange: self.exchange_name().to_string(),
            market_type: self.market_type(),
            connected: true,
            public_ws_healthy: true,
            private_ws_healthy: false,
            rest_healthy: true,
            stale_books: Vec::new(),
            last_error: None,
            checked_at: Utc::now(),
        })
    }

    async fn subscribe_orderbook(
        &self,
        symbols: Vec<String>,
    ) -> ExchangeClientResult<mpsc::Receiver<OrderBookSnapshot>> {
        validate_orderbook_subscription_symbols(&symbols)?;
        Err(ExchangeClientError::Unsupported(
            "orderbook subscription is not implemented for this client".to_string(),
        ))
    }

    async fn subscribe_user_stream(&self) -> ExchangeClientResult<mpsc::Receiver<UserStreamEvent>> {
        Err(ExchangeClientError::Unsupported(
            "user stream subscription is not implemented for this client".to_string(),
        ))
    }
}

/// Compatibility wrapper over the legacy `core::exchange::Exchange` trait.
///
/// New code can depend on `ExchangeClient`, while existing exchange adapters
/// continue to implement the older, wider trait until each venue is migrated.
pub struct LegacyExchangeClient {
    exchange: Arc<Box<dyn LegacyExchange>>,
    market_type: MarketType,
    exchange_name: String,
    strategy_prefix: String,
}

impl LegacyExchangeClient {
    pub fn new(exchange: Arc<Box<dyn LegacyExchange>>, market_type: MarketType) -> Self {
        let exchange_name = exchange.name().to_string();
        Self {
            exchange,
            market_type,
            exchange_name,
            strategy_prefix: "legacy".to_string(),
        }
    }

    pub fn with_strategy_prefix(mut self, strategy_prefix: impl Into<String>) -> Self {
        self.strategy_prefix = strategy_prefix.into();
        self
    }
}

#[async_trait]
impl ExchangeClient for LegacyExchangeClient {
    fn market_type(&self) -> MarketType {
        self.market_type
    }

    fn exchange_name(&self) -> &str {
        &self.exchange_name
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        ExchangeClientCapabilities::legacy(self.exchange_name.clone(), self.market_type)
    }

    async fn get_balances(&self) -> ExchangeClientResult<BalanceSnapshot> {
        let balances = self
            .exchange
            .get_balance(self.market_type.to_legacy())
            .await?;
        Ok(BalanceSnapshot {
            exchange: self.exchange_name.clone(),
            market_type: self.market_type,
            balances: balances
                .into_iter()
                .map(AssetBalance::from_legacy)
                .collect(),
            timestamp: Utc::now(),
        })
    }

    async fn get_orderbook(
        &self,
        symbol: &str,
        depth: u16,
    ) -> ExchangeClientResult<OrderBookSnapshot> {
        let symbol = self.normalize_symbol(symbol)?;
        validate_orderbook_depth(depth)?;
        let book = self
            .exchange
            .get_orderbook(&symbol, self.market_type.to_legacy(), Some(depth as u32))
            .await?;
        Ok(OrderBookSnapshot::from_legacy(
            self.exchange_name.clone(),
            self.market_type,
            book,
        ))
    }

    async fn place_order(&self, mut request: OrderRequest) -> ExchangeClientResult<OrderResponse> {
        request.validate()?;
        if request.market_type != self.market_type {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: format!(
                    "request market {:?} does not match client market {:?}",
                    request.market_type, self.market_type
                ),
            });
        }
        if request.client_order_id.is_none() {
            request.client_order_id = Some(
                generate_client_order_id(
                    &self.exchange_name,
                    self.market_type,
                    &self.strategy_prefix,
                )
                .into_string(),
            );
        }
        if let Some(client_order_id) = &request.client_order_id {
            validate_client_order_id(&self.exchange_name, self.market_type, client_order_id)
                .map_err(|error| {
                    ExchangeClientError::Classified(ExchangeError {
                        exchange: self.exchange_name.clone(),
                        class: ExchangeErrorClass::InvalidClientOrderId,
                        code: None,
                        message: error.to_string(),
                    })
                })?;
        }
        log::info!(
            "legacy unified order submit exchange={} market={:?} symbol={} client_order_id={}",
            self.exchange_name,
            self.market_type,
            request.symbol,
            request.client_order_id.as_deref().unwrap_or("")
        );
        let order = self.exchange.create_order(request.to_legacy()?).await?;
        Ok(OrderResponse::from_legacy(
            self.exchange_name.clone(),
            order,
        ))
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeClientResult<CancelOrderResponse> {
        request.validate()?;
        if request.market_type != self.market_type {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: format!(
                    "request market {:?} does not match client market {:?}",
                    request.market_type, self.market_type
                ),
            });
        }
        if let Some(client_order_id) = &request.client_order_id {
            validate_client_order_id(&self.exchange_name, self.market_type, client_order_id)
                .map_err(|error| {
                    ExchangeClientError::Classified(ExchangeError {
                        exchange: self.exchange_name.clone(),
                        class: ExchangeErrorClass::InvalidClientOrderId,
                        code: None,
                        message: error.to_string(),
                    })
                })?;
        }
        let order = self
            .exchange
            .cancel_order(
                request.order_lookup_id(),
                &request.symbol,
                self.market_type.to_legacy(),
            )
            .await?;
        Ok(CancelOrderResponse::from_legacy(
            self.exchange_name.clone(),
            order,
        ))
    }

    async fn get_order(&self, symbol: &str, order_id: &str) -> ExchangeClientResult<OrderResponse> {
        let symbol = self.normalize_symbol(symbol)?;
        let order_id = validate_order_lookup_id(order_id)?;
        let order = self
            .exchange
            .get_order(order_id, &symbol, self.market_type.to_legacy())
            .await?;
        Ok(OrderResponse::from_legacy(
            self.exchange_name.clone(),
            order,
        ))
    }

    async fn get_open_orders(
        &self,
        symbol: Option<&str>,
    ) -> ExchangeClientResult<Vec<OrderResponse>> {
        let normalized = symbol
            .map(|value| self.normalize_symbol(value))
            .transpose()?;
        let orders = self
            .exchange
            .get_open_orders(normalized.as_deref(), self.market_type.to_legacy())
            .await?;
        Ok(orders
            .into_iter()
            .map(|order| OrderResponse::from_legacy(self.exchange_name.clone(), order))
            .collect())
    }

    async fn get_fee_rate(&self, symbol: &str) -> ExchangeClientResult<FeeRate> {
        let symbol = self.normalize_symbol(symbol)?;
        let fee = self
            .exchange
            .get_trade_fee(&symbol, self.market_type.to_legacy())
            .await?;
        Ok(FeeRate::new(
            fee.maker_fee.unwrap_or(fee.maker),
            fee.taker_fee.unwrap_or(fee.taker),
            FeeRateSource::ExchangeApi,
        ))
    }
}

pub fn round_price_to_tick(price: f64, tick_size: f64, round_up: bool) -> f64 {
    round_to_step(price, tick_size, round_up)
}

pub fn round_quantity_to_step(quantity: f64, step_size: f64, round_up: bool) -> f64 {
    round_to_step(quantity, step_size, round_up)
}

fn round_to_step(value: f64, step: f64, round_up: bool) -> f64 {
    if !value.is_finite() || !step.is_finite() || step <= 0.0 {
        return value;
    }
    let units = value / step;
    let rounded = if round_up {
        units.ceil()
    } else {
        units.floor()
    };
    let precision = decimal_places(step);
    round_decimal(rounded * step, precision)
}

pub fn validate_price_tick(price: f64, tick_size: f64) -> ExchangeClientResult<()> {
    validate_step("price", price, tick_size)
}

pub fn validate_orderbook_subscription_symbols(symbols: &[String]) -> ExchangeClientResult<()> {
    if symbols.is_empty() {
        return Err(ExchangeClientError::Validation {
            field: "symbols",
            reason: "at least one symbol is required".to_string(),
        });
    }
    if symbols.iter().any(|symbol| symbol.trim().is_empty()) {
        return Err(ExchangeClientError::Validation {
            field: "symbols",
            reason: "symbols must not contain empty values".to_string(),
        });
    }
    Ok(())
}

pub fn validate_orderbook_depth(depth: u16) -> ExchangeClientResult<()> {
    if depth == 0 {
        return Err(ExchangeClientError::Validation {
            field: "depth",
            reason: "depth must be greater than zero".to_string(),
        });
    }
    Ok(())
}

pub fn validate_order_lookup_id(order_id: &str) -> ExchangeClientResult<&str> {
    let order_id = order_id.trim();
    if order_id.is_empty() {
        return Err(ExchangeClientError::Validation {
            field: "order_id",
            reason: "order_id must not be empty".to_string(),
        });
    }
    Ok(order_id)
}

pub fn validate_quantity_step(quantity: f64, step_size: f64) -> ExchangeClientResult<()> {
    validate_step("quantity", quantity, step_size)
}

pub fn validate_min_notional(
    quantity: f64,
    price: Option<f64>,
    min_notional: f64,
) -> ExchangeClientResult<()> {
    if min_notional <= 0.0 {
        return Ok(());
    }
    let notional = price.unwrap_or(0.0) * quantity;
    if notional + 1e-12 >= min_notional {
        Ok(())
    } else {
        Err(ExchangeClientError::Validation {
            field: "notional",
            reason: format!("notional {notional} is below min_notional {min_notional}"),
        })
    }
}

pub fn validate_order_against_symbol_rule(
    request: &OrderRequest,
    rule: &SymbolRule,
) -> ExchangeClientResult<()> {
    request.validate()?;
    if request.market_type != rule.market_type {
        return Err(ExchangeClientError::Validation {
            field: "market_type",
            reason: format!(
                "request market {:?} does not match symbol rule market {:?}",
                request.market_type, rule.market_type
            ),
        });
    }
    if rule.status != SymbolStatus::Trading {
        return Err(ExchangeClientError::Validation {
            field: "symbol",
            reason: format!("symbol {} is not tradable", rule.internal_symbol),
        });
    }
    if !rule.supported_order_types.contains(&request.order_type) {
        return Err(ExchangeClientError::Unsupported(format!(
            "{:?} is not supported for {}",
            request.order_type, rule.internal_symbol
        )));
    }
    if let Some(tif) = request.time_in_force {
        if !rule.supported_time_in_force.contains(&tif) {
            return Err(ExchangeClientError::Unsupported(format!(
                "{:?} is not supported for {}",
                tif, rule.internal_symbol
            )));
        }
    }
    if request.quantity + 1e-12 < rule.min_quantity {
        return Err(ExchangeClientError::Validation {
            field: "quantity",
            reason: format!(
                "quantity {} is below min_quantity {}",
                request.quantity, rule.min_quantity
            ),
        });
    }
    validate_quantity_step(request.quantity, rule.step_size)?;
    if let Some(price) = request.price {
        validate_price_tick(price, rule.tick_size)?;
        validate_min_notional(request.quantity, Some(price), rule.min_notional)?;
    }
    if let Some(client_order_id) = &request.client_order_id {
        validate_client_order_id(&rule.exchange, request.market_type, client_order_id).map_err(
            |error| {
                ExchangeClientError::Classified(ExchangeError {
                    exchange: rule.exchange.clone(),
                    class: ExchangeErrorClass::InvalidClientOrderId,
                    code: None,
                    message: error.to_string(),
                })
            },
        )?;
    }
    Ok(())
}

fn validate_step(field: &'static str, value: f64, step: f64) -> ExchangeClientResult<()> {
    if step <= 0.0 || !step.is_finite() {
        return Ok(());
    }
    let units = value / step;
    let nearest = units.round() * step;
    let tolerance = (step.abs() * 1e-6).max(value.abs() * 1e-12).max(1e-12);
    if (value - nearest).abs() <= tolerance {
        Ok(())
    } else {
        Err(ExchangeClientError::Validation {
            field,
            reason: format!("{value} does not conform to step {step}"),
        })
    }
}

fn decimal_places(value: f64) -> u32 {
    let text = format!("{value:.16}");
    text.trim_end_matches('0')
        .split('.')
        .nth(1)
        .map(|fraction| fraction.len() as u32)
        .unwrap_or(0)
}

fn round_decimal(value: f64, places: u32) -> f64 {
    let scale = 10_f64.powi(places as i32);
    (value * scale).round() / scale
}

fn validate_positive(field: &'static str, value: f64) -> Result<(), ExchangeClientError> {
    if value.is_finite() && value > 0.0 {
        Ok(())
    } else {
        Err(ExchangeClientError::Validation {
            field,
            reason: "value must be finite and > 0".to_string(),
        })
    }
}

fn validate_positive_or_zero(field: &'static str, value: f64) -> Result<(), ExchangeClientError> {
    if value.is_finite() && value >= 0.0 {
        Ok(())
    } else {
        Err(ExchangeClientError::Validation {
            field,
            reason: "value must be finite and >= 0".to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::sync::Arc;

    use crate::core::exchange::Exchange;

    #[test]
    fn unified_enums_should_serialize_and_deserialize() {
        assert_eq!(
            serde_json::to_string(&MarketType::Perpetual).unwrap(),
            "\"Perpetual\""
        );
        assert_eq!(
            serde_json::from_str::<OrderType>("\"PostOnly\"").unwrap(),
            OrderType::PostOnly
        );
        assert_eq!(
            serde_json::from_str::<TimeInForce>("\"IOC\"").unwrap(),
            TimeInForce::IOC
        );
        assert_eq!(
            serde_json::from_str::<LiquidityRole>("\"Maker\"").unwrap(),
            LiquidityRole::Maker
        );
    }

    #[test]
    fn order_request_validation_should_require_price_for_limit_style_orders() {
        let request = OrderRequest {
            market_type: MarketType::Spot,
            symbol: "BTC/USDT".to_string(),
            side: OrderSide::Buy,
            position_side: PositionSide::None,
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: 0.01,
            price: None,
            client_order_id: None,
            reduce_only: false,
        };

        assert!(matches!(
            request.validate(),
            Err(ExchangeClientError::Validation { field: "price", .. })
        ));
    }

    #[test]
    fn step_validation_should_accept_large_prices_with_small_ticks() {
        assert!(validate_price_tick(1_000.0, 0.00001).is_ok());
        assert!(validate_price_tick(10_000.0, 0.00001).is_ok());
        assert!(validate_price_tick(10_000.000003, 0.00001).is_err());
    }

    #[test]
    fn order_lookup_id_should_reject_blank_values() {
        assert_eq!(validate_order_lookup_id(" 1001 ").unwrap(), "1001");

        let err = validate_order_lookup_id("   ").unwrap_err();
        assert!(matches!(
            err,
            ExchangeClientError::Validation {
                field: "order_id",
                ..
            }
        ));
    }

    #[test]
    fn cancel_order_request_should_ignore_blank_identifiers_for_lookup() {
        let request = CancelOrderRequest {
            market_type: MarketType::Spot,
            symbol: "BTCUSDT".to_string(),
            order_id: Some("   ".to_string()),
            client_order_id: Some("CANCELCLIENT1".to_string()),
        };

        request.validate().unwrap();
        assert_eq!(request.order_id(), None);
        assert_eq!(request.client_order_id(), Some("CANCELCLIENT1"));
        assert_eq!(request.order_lookup_id(), "CANCELCLIENT1");
    }

    #[test]
    fn amend_order_request_should_ignore_blank_identifiers() {
        let request = AmendOrderRequest {
            market_type: MarketType::Spot,
            symbol: "BTCUSDT".to_string(),
            order_id: Some("   ".to_string()),
            client_order_id: Some("AMENDCLIENT1".to_string()),
            new_client_order_id: Some("   ".to_string()),
            new_quantity: 0.5,
        };

        request.validate().unwrap();
        assert_eq!(request.order_id(), None);
        assert_eq!(request.client_order_id(), Some("AMENDCLIENT1"));
        assert_eq!(request.new_client_order_id(), None);
    }

    #[test]
    fn order_request_validation_should_separate_spot_and_perpetual_rules() {
        let spot_reduce_only = OrderRequest {
            market_type: MarketType::Spot,
            symbol: "ETH/USDT".to_string(),
            side: OrderSide::Sell,
            position_side: PositionSide::Short,
            order_type: OrderType::Market,
            time_in_force: None,
            quantity: 1.0,
            price: None,
            client_order_id: None,
            reduce_only: true,
        };
        assert!(spot_reduce_only.validate().is_err());

        let perp_reduce_only = OrderRequest {
            market_type: MarketType::Perpetual,
            symbol: "ETH/USDT".to_string(),
            side: OrderSide::Sell,
            position_side: PositionSide::Long,
            order_type: OrderType::Market,
            time_in_force: None,
            quantity: 1.0,
            price: None,
            client_order_id: None,
            reduce_only: true,
        };
        assert!(perp_reduce_only.validate().is_ok());
    }

    #[test]
    fn balance_snapshot_should_validate_balance_model() {
        let balance = AssetBalance::new("USDT", 100.0, 80.0, 20.0);
        balance.validate().unwrap();

        let snapshot = BalanceSnapshot {
            exchange: "binance".to_string(),
            market_type: MarketType::Spot,
            balances: vec![balance],
            timestamp: Utc.with_ymd_and_hms(2026, 6, 4, 0, 0, 0).unwrap(),
        };
        assert_eq!(snapshot.balances[0].available, 80.0);
        assert_eq!(snapshot.market_type, MarketType::Spot);
    }

    #[tokio::test]
    async fn legacy_exchange_client_should_generate_client_order_id_before_submit() {
        let legacy: Box<dyn Exchange> = Box::new(crate::exchanges::MockExchange::new("binance"));
        let client = LegacyExchangeClient::new(Arc::new(legacy), MarketType::Spot)
            .with_strategy_prefix("arb");
        let response = client
            .place_order(OrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                side: OrderSide::Buy,
                position_side: PositionSide::None,
                order_type: OrderType::Limit,
                time_in_force: Some(TimeInForce::GTC),
                quantity: 0.01,
                price: Some(50_000.0),
                client_order_id: None,
                reduce_only: false,
            })
            .await
            .unwrap();

        assert!(response.order_id.starts_with("BN_SPT_ARB_"));
    }

    #[tokio::test]
    async fn legacy_exchange_client_should_reject_invalid_client_order_id() {
        let legacy: Box<dyn Exchange> = Box::new(crate::exchanges::MockExchange::new("mexc"));
        let client = LegacyExchangeClient::new(Arc::new(legacy), MarketType::Spot);
        let error = client
            .place_order(OrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                side: OrderSide::Buy,
                position_side: PositionSide::None,
                order_type: OrderType::Limit,
                time_in_force: Some(TimeInForce::GTC),
                quantity: 0.01,
                price: Some(50_000.0),
                client_order_id: Some("bad/id".to_string()),
                reduce_only: false,
            })
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            ExchangeClientError::Classified(ExchangeError {
                class: ExchangeErrorClass::InvalidClientOrderId,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn legacy_exchange_client_should_validate_get_order_id_before_lookup() {
        let legacy: Box<dyn Exchange> = Box::new(crate::exchanges::MockExchange::new("binance"));
        let client = LegacyExchangeClient::new(Arc::new(legacy), MarketType::Spot);

        let error = client.get_order("BTCUSDT", "   ").await.unwrap_err();
        assert!(matches!(
            error,
            ExchangeClientError::Validation {
                field: "order_id",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn legacy_exchange_client_should_validate_orderbook_depth_before_lookup() {
        let legacy: Box<dyn Exchange> = Box::new(crate::exchanges::MockExchange::new("binance"));
        let client = LegacyExchangeClient::new(Arc::new(legacy), MarketType::Spot);

        let error = client.get_orderbook("BTCUSDT", 0).await.unwrap_err();
        assert!(matches!(
            error,
            ExchangeClientError::Validation { field: "depth", .. }
        ));
    }

    #[tokio::test]
    async fn legacy_exchange_client_should_validate_cancel_client_order_id_before_lookup() {
        let legacy: Box<dyn Exchange> = Box::new(crate::exchanges::MockExchange::new("mexc"));
        let client = LegacyExchangeClient::new(Arc::new(legacy), MarketType::Spot);

        let error = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                order_id: None,
                client_order_id: Some("bad/id".to_string()),
            })
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            ExchangeClientError::Classified(ExchangeError {
                class: ExchangeErrorClass::InvalidClientOrderId,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn default_order_list_path_should_validate_before_unsupported() {
        let legacy: Box<dyn Exchange> = Box::new(crate::exchanges::MockExchange::new("coinex"));
        let client = LegacyExchangeClient::new(Arc::new(legacy), MarketType::Spot);

        fn oco_request(market_type: MarketType) -> OrderListRequest {
            OrderListRequest::Oco {
                market_type,
                symbol: "BTCUSDT".to_string(),
                list_client_order_id: None,
                side: OrderSide::Sell,
                quantity: 0.01,
                above: OrderListConditionalLeg {
                    order_type: OrderListLegType::LimitMaker,
                    price: Some(70_000.0),
                    stop_price: None,
                    time_in_force: None,
                    client_order_id: None,
                },
                below: OrderListConditionalLeg {
                    order_type: OrderListLegType::StopLossLimit,
                    price: Some(59_500.0),
                    stop_price: Some(60_000.0),
                    time_in_force: Some(TimeInForce::GTC),
                    client_order_id: None,
                },
            }
        }

        let valid = oco_request(MarketType::Spot);
        assert!(matches!(
            client.place_order_list(valid).await,
            Err(ExchangeClientError::Unsupported(message))
                if message.contains("order lists are not implemented")
        ));

        let invalid_market_type = oco_request(MarketType::Perpetual);
        assert!(matches!(
            client.place_order_list(invalid_market_type).await,
            Err(ExchangeClientError::Validation {
                field: "market_type",
                ..
            })
        ));

        let mut missing_limit_price = oco_request(MarketType::Spot);
        if let OrderListRequest::Oco { above, .. } = &mut missing_limit_price {
            above.price = None;
        }
        assert!(matches!(
            client.place_order_list(missing_limit_price).await,
            Err(ExchangeClientError::Validation { field: "price", .. })
        ));

        let mut missing_stop_price = oco_request(MarketType::Spot);
        if let OrderListRequest::Oco { below, .. } = &mut missing_stop_price {
            below.stop_price = None;
        }
        assert!(matches!(
            client.place_order_list(missing_stop_price).await,
            Err(ExchangeClientError::Validation {
                field: "stop_price",
                ..
            })
        ));

        let mut invalid_client_id = oco_request(MarketType::Spot);
        if let OrderListRequest::Oco {
            list_client_order_id,
            ..
        } = &mut invalid_client_id
        {
            *list_client_order_id = Some("bad/id".to_string());
        }
        assert!(matches!(
            client.place_order_list(invalid_client_id).await,
            Err(ExchangeClientError::Validation {
                field: "client_order_id",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn default_advanced_order_paths_should_validate_before_unsupported() {
        let legacy: Box<dyn Exchange> = Box::new(crate::exchanges::MockExchange::new("paper"));
        let client = LegacyExchangeClient::new(Arc::new(legacy), MarketType::Spot);

        assert!(matches!(
            client
                .place_quote_market_order(QuoteMarketOrderRequest::spot_buy("BTCUSDT", 25.0))
                .await,
            Err(ExchangeClientError::Unsupported(message))
                if message.contains("quote-sized market orders are not implemented")
        ));

        let invalid_quote = QuoteMarketOrderRequest {
            market_type: MarketType::Perpetual,
            symbol: "BTCUSDT".to_string(),
            side: OrderSide::Buy,
            quote_quantity: 25.0,
            client_order_id: None,
        };
        assert!(matches!(
            client.place_quote_market_order(invalid_quote).await,
            Err(ExchangeClientError::Validation {
                field: "market_type",
                ..
            })
        ));

        let invalid_quote_client_id = QuoteMarketOrderRequest {
            market_type: MarketType::Spot,
            symbol: "BTCUSDT".to_string(),
            side: OrderSide::Buy,
            quote_quantity: 25.0,
            client_order_id: Some("bad/id".to_string()),
        };
        assert!(matches!(
            client
                .place_quote_market_order(invalid_quote_client_id)
                .await,
            Err(ExchangeClientError::Validation {
                field: "client_order_id",
                ..
            })
        ));

        assert!(matches!(
            client
                .amend_order(AmendOrderRequest::reduce_quantity_by_order_id(
                    MarketType::Spot,
                    "BTCUSDT",
                    "order-1",
                    0.01,
                ))
                .await,
            Err(ExchangeClientError::Unsupported(message))
                if message.contains("amend order is not implemented")
        ));

        let blank_optional_amend_ids = AmendOrderRequest {
            market_type: MarketType::Spot,
            symbol: "BTCUSDT".to_string(),
            order_id: Some("order-1".to_string()),
            client_order_id: Some("   ".to_string()),
            new_client_order_id: Some("   ".to_string()),
            new_quantity: 0.01,
        };
        assert!(matches!(
            client.amend_order(blank_optional_amend_ids).await,
            Err(ExchangeClientError::Unsupported(message))
                if message.contains("amend order is not implemented")
        ));

        let invalid_amend = AmendOrderRequest {
            market_type: MarketType::Spot,
            symbol: "BTCUSDT".to_string(),
            order_id: None,
            client_order_id: None,
            new_client_order_id: None,
            new_quantity: 0.01,
        };
        assert!(matches!(
            client.amend_order(invalid_amend).await,
            Err(ExchangeClientError::Validation {
                field: "order_id",
                ..
            })
        ));

        let invalid_amend_client_id = AmendOrderRequest {
            market_type: MarketType::Spot,
            symbol: "BTCUSDT".to_string(),
            order_id: Some("order-1".to_string()),
            client_order_id: Some("bad/id".to_string()),
            new_client_order_id: None,
            new_quantity: 0.01,
        };
        assert!(matches!(
            client.amend_order(invalid_amend_client_id).await,
            Err(ExchangeClientError::Validation {
                field: "client_order_id",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn default_cancel_all_path_should_validate_before_unsupported() {
        let legacy: Box<dyn Exchange> = Box::new(crate::exchanges::MockExchange::new("paper"));
        let client = LegacyExchangeClient::new(Arc::new(legacy), MarketType::Spot);

        assert!(matches!(
            client
                .cancel_all_orders(CancelAllOrdersRequest::for_symbol(
                    MarketType::Spot,
                    "BTCUSDT"
                ))
                .await,
            Err(ExchangeClientError::Unsupported(message))
                if message.contains("cancel-all orders is not implemented")
        ));

        assert!(matches!(
            client
                .cancel_all_orders(CancelAllOrdersRequest::for_symbol(
                    MarketType::Perpetual,
                    "BTCUSDT",
                ))
                .await,
            Err(ExchangeClientError::Validation {
                field: "market_type",
                ..
            })
        ));

        assert!(matches!(
            client
                .cancel_all_orders(CancelAllOrdersRequest::for_symbol(MarketType::Spot, "   "))
                .await,
            Err(ExchangeClientError::Validation {
                field: "symbol",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn default_orderbook_subscription_should_validate_before_unsupported() {
        let legacy: Box<dyn Exchange> = Box::new(crate::exchanges::MockExchange::new("paper"));
        let client = LegacyExchangeClient::new(Arc::new(legacy), MarketType::Spot);

        assert!(matches!(
            client
                .subscribe_orderbook(vec!["BTCUSDT".to_string()])
                .await,
            Err(ExchangeClientError::Unsupported(message))
                if message.contains("orderbook subscription is not implemented")
        ));

        assert!(matches!(
            client.subscribe_orderbook(Vec::new()).await,
            Err(ExchangeClientError::Validation {
                field: "symbols",
                ..
            })
        ));

        assert!(matches!(
            client.subscribe_orderbook(vec!["   ".to_string()]).await,
            Err(ExchangeClientError::Validation {
                field: "symbols",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn default_recent_fills_path_should_validate_symbol_before_unsupported() {
        let legacy: Box<dyn Exchange> = Box::new(crate::exchanges::MockExchange::new("paper"));
        let client = LegacyExchangeClient::new(Arc::new(legacy), MarketType::Spot);

        assert!(matches!(
            client.get_recent_fills("BTCUSDT").await,
            Err(ExchangeClientError::Unsupported(message))
                if message.contains("recent fills are not implemented")
        ));

        assert!(matches!(
            client.get_recent_fills("   ").await,
            Err(ExchangeClientError::Validation {
                field: "symbol",
                ..
            })
        ));
    }

    #[test]
    fn order_status_mapping_should_cover_common_exchange_statuses() {
        assert_eq!(OrderStatus::from_exchange_status("NEW"), OrderStatus::New);
        assert_eq!(
            OrderStatus::from_exchange_status("PARTIALLY_FILLED"),
            OrderStatus::PartiallyFilled
        );
        assert_eq!(
            OrderStatus::from_exchange_status("FILLED"),
            OrderStatus::Filled
        );
        assert_eq!(
            OrderStatus::from_exchange_status("CANCELED"),
            OrderStatus::Cancelled
        );
        assert_eq!(
            OrderStatus::from_exchange_status("something-new"),
            OrderStatus::Unknown
        );
    }
    #[test]
    fn legacy_market_type_mapping_should_preserve_existing_futures_name() {
        assert_eq!(
            MarketType::Perpetual.to_legacy(),
            crate::core::types::MarketType::Futures
        );
        assert_eq!(
            MarketType::from_legacy(crate::core::types::MarketType::Spot),
            MarketType::Spot
        );
    }

    #[test]
    fn legacy_capabilities_should_report_unsupported_features_without_panicking() {
        let capabilities = ExchangeClientCapabilities::legacy("binance", MarketType::Perpetual);
        assert!(capabilities.supports_perpetual);
        assert!(capabilities.supports_positions);
        assert!(!capabilities.supports_private_user_stream);
        assert!(capabilities
            .unsupported_reason(ExchangeCapability::PrivateUserStream)
            .unwrap()
            .contains("does not support"));
    }

    #[test]
    fn spot_capabilities_should_expose_public_ws_and_disable_positions() {
        let capabilities = ExchangeClientCapabilities::spot("mexc");
        assert!(capabilities.supports_spot);
        assert!(capabilities.supports_public_ws);
        assert!(!capabilities.supports_positions);
        assert!(!capabilities.supports_quote_market_order);
        assert!(!capabilities.supports_amend_order);
        assert!(!capabilities.supports_order_list);
        assert!(capabilities
            .unsupported_reason(ExchangeCapability::Positions)
            .is_some());
        assert!(capabilities
            .unsupported_reason(ExchangeCapability::QuoteMarketOrder)
            .unwrap()
            .contains("does not support"));
    }

    #[test]
    fn spot_capability_matrix_should_match_binance_parity_baseline() {
        let clients: Vec<(&str, Box<dyn ExchangeClient>, bool, bool, bool, bool)> = vec![
            (
                "binance",
                Box::new(crate::exchanges::binance::spot::BinanceSpotClient::new(
                    crate::exchanges::binance::spot::BinanceSpotConfig::default(),
                )),
                true,
                true,
                true,
                true,
            ),
            (
                "okx",
                Box::new(crate::exchanges::okx::spot::OkxSpotClient::new(
                    crate::exchanges::okx::spot::OkxSpotConfig::default(),
                )),
                true,
                true,
                false,
                true,
            ),
            (
                "bitget",
                Box::new(crate::exchanges::bitget::BitgetSpotClient::new(
                    crate::exchanges::bitget::BitgetSpotConfig::default(),
                )),
                true,
                true,
                false,
                false,
            ),
            (
                "gateio",
                Box::new(crate::exchanges::gateio::GateIoSpotClient::new(
                    crate::exchanges::gateio::GateIoSpotConfig::default(),
                )),
                true,
                true,
                false,
                false,
            ),
            (
                "mexc",
                Box::new(crate::exchanges::mexc::MexcSpotClient::new(
                    crate::exchanges::mexc::MexcSpotConfig::default(),
                )),
                true,
                false,
                false,
                false,
            ),
            (
                "coinex",
                Box::new(crate::exchanges::coinex::CoinExSpotClient::new(
                    crate::exchanges::coinex::CoinExSpotConfig::default(),
                )),
                true,
                true,
                false,
                false,
            ),
            (
                "kucoin",
                Box::new(crate::exchanges::kucoin::KuCoinSpotClient::new(
                    crate::exchanges::kucoin::KuCoinSpotConfig::default(),
                )),
                true,
                true,
                false,
                true,
            ),
        ];

        for (
            exchange,
            client,
            supports_quote_market,
            supports_amend,
            supports_order_list,
            supports_private_stream,
        ) in clients
        {
            let capabilities = client.capabilities();

            assert_eq!(capabilities.exchange, exchange);
            assert_eq!(capabilities.market_type, MarketType::Spot, "{exchange}");
            assert!(capabilities.supports_spot, "{exchange}");
            assert!(!capabilities.supports_perpetual, "{exchange}");
            assert!(capabilities.supports_market_order, "{exchange}");
            assert!(capabilities.supports_limit_order, "{exchange}");
            assert!(capabilities.supports_post_only, "{exchange}");
            assert!(capabilities.supports_ioc, "{exchange}");
            assert!(capabilities.supports_fok, "{exchange}");
            assert!(capabilities.supports_cancel_order, "{exchange}");
            assert!(capabilities.supports_cancel_all_orders, "{exchange}");
            assert!(capabilities.supports_query_order, "{exchange}");
            assert!(capabilities.supports_open_orders, "{exchange}");
            assert!(capabilities.supports_balances, "{exchange}");
            assert!(!capabilities.supports_positions, "{exchange}");
            assert!(!capabilities.supports_leverage, "{exchange}");
            assert!(!capabilities.supports_funding_rate, "{exchange}");
            assert!(capabilities.supports_public_ws, "{exchange}");
            assert_eq!(
                capabilities.supports_private_user_stream, supports_private_stream,
                "{exchange}"
            );
            assert!(capabilities.supports_fee_api, "{exchange}");
            assert_eq!(
                capabilities.supports_quote_market_order, supports_quote_market,
                "{exchange}"
            );
            assert_eq!(
                capabilities.supports_amend_order, supports_amend,
                "{exchange}"
            );
            assert_eq!(
                capabilities.supports_order_list, supports_order_list,
                "{exchange}"
            );
        }
    }
}
