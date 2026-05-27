use crate::execution::{
    OrderCommand, OrderCommandStatus, OrderSide, OrderType, PositionSide, TimeInForce,
};
use crate::market::{CanonicalSymbol, ExchangeId, ExchangeSymbol, InstrumentMeta};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradingCapabilities {
    pub supports_market_orders: bool,
    pub supports_limit_orders: bool,
    pub supports_post_only: bool,
    pub supports_ioc: bool,
    pub supports_fok: bool,
    pub supports_reduce_only: bool,
    pub supports_hedge_mode: bool,
    pub supports_client_order_id: bool,
    pub supports_leverage: bool,
    pub supports_position_mode_change: bool,
    pub supports_close_position: bool,
}

impl Default for TradingCapabilities {
    fn default() -> Self {
        Self {
            supports_market_orders: true,
            supports_limit_orders: true,
            supports_post_only: true,
            supports_ioc: true,
            supports_fok: false,
            supports_reduce_only: true,
            supports_hedge_mode: true,
            supports_client_order_id: true,
            supports_leverage: true,
            supports_position_mode_change: true,
            supports_close_position: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderAck {
    pub exchange: ExchangeId,
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub accepted: bool,
    pub status: OrderCommandStatus,
    pub message: Option<String>,
    pub acknowledged_at: DateTime<Utc>,
}

impl OrderAck {
    pub fn dry_run(command: &OrderCommand, acknowledged_at: DateTime<Utc>) -> Self {
        Self {
            exchange: command.exchange.clone(),
            client_order_id: command.client_order_id.clone(),
            exchange_order_id: None,
            accepted: false,
            status: OrderCommandStatus::Planned,
            message: Some("dry-run: order was not sent to adapter".to_string()),
            acknowledged_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelCommand {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub exchange_symbol: ExchangeSymbol,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub reason: Option<String>,
    pub requested_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelAck {
    pub exchange: ExchangeId,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub accepted: bool,
    pub status: OrderCommandStatus,
    pub message: Option<String>,
    pub acknowledged_at: DateTime<Utc>,
}

impl CancelAck {
    pub fn dry_run(command: &CancelCommand, acknowledged_at: DateTime<Utc>) -> Self {
        Self {
            exchange: command.exchange.clone(),
            client_order_id: command.client_order_id.clone(),
            exchange_order_id: command.exchange_order_id.clone(),
            accepted: false,
            status: OrderCommandStatus::CancelRequested,
            message: Some("dry-run: cancel was not sent to adapter".to_string()),
            acknowledged_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderQuery {
    pub exchange: ExchangeId,
    pub exchange_symbol: ExchangeSymbol,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderState {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub exchange_symbol: ExchangeSymbol,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub side: OrderSide,
    pub position_side: PositionSide,
    pub order_type: OrderType,
    pub quantity: f64,
    pub price: Option<f64>,
    pub filled_quantity: f64,
    pub average_fill_price: Option<f64>,
    pub time_in_force: TimeInForce,
    pub reduce_only: bool,
    pub status: OrderCommandStatus,
    pub updated_at: DateTime<Utc>,
}

impl OrderState {
    pub fn is_open(&self) -> bool {
        matches!(
            self.status,
            OrderCommandStatus::Submitted
                | OrderCommandStatus::Accepted
                | OrderCommandStatus::PartiallyFilled
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExchangePosition {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub exchange_symbol: ExchangeSymbol,
    pub position_side: PositionSide,
    pub quantity: f64,
    pub entry_price: Option<f64>,
    pub mark_price: Option<f64>,
    pub unrealized_pnl: Option<f64>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExchangeBalance {
    pub exchange: ExchangeId,
    pub asset: String,
    pub total: f64,
    pub available: f64,
    pub locked: f64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FillQuery {
    pub exchange: ExchangeId,
    pub canonical_symbol: Option<CanonicalSymbol>,
    pub exchange_symbol: Option<ExchangeSymbol>,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub from_trade_id: Option<String>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub limit: Option<u32>,
}

impl FillQuery {
    pub fn new(exchange: ExchangeId) -> Self {
        Self {
            exchange,
            canonical_symbol: None,
            exchange_symbol: None,
            client_order_id: None,
            exchange_order_id: None,
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: None,
        }
    }

    pub fn for_symbol(
        exchange: ExchangeId,
        canonical_symbol: CanonicalSymbol,
        exchange_symbol: ExchangeSymbol,
    ) -> Self {
        Self {
            canonical_symbol: Some(canonical_symbol),
            exchange_symbol: Some(exchange_symbol),
            ..Self::new(exchange)
        }
    }

    pub fn for_order(
        exchange: ExchangeId,
        exchange_symbol: ExchangeSymbol,
        client_order_id: Option<String>,
        exchange_order_id: Option<String>,
    ) -> Self {
        Self {
            exchange,
            exchange_symbol: Some(exchange_symbol),
            client_order_id,
            exchange_order_id,
            canonical_symbol: None,
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FillLiquidity {
    Maker,
    Taker,
    Unknown,
}

impl FillLiquidity {
    pub fn is_maker(self) -> Option<bool> {
        match self {
            Self::Maker => Some(true),
            Self::Taker => Some(false),
            Self::Unknown => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FillEvent {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub exchange_symbol: ExchangeSymbol,
    pub trade_id: String,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub side: OrderSide,
    pub position_side: PositionSide,
    pub liquidity: FillLiquidity,
    pub price: f64,
    pub quantity: f64,
    pub quote_quantity: f64,
    pub fee: Option<f64>,
    pub fee_asset: Option<String>,
    pub fee_rate: Option<f64>,
    pub realized_pnl: Option<f64>,
    pub reduce_only: Option<bool>,
    pub filled_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
}

impl FillEvent {
    pub fn notional(&self) -> f64 {
        if self.quote_quantity > 0.0 {
            self.quote_quantity
        } else {
            self.price * self.quantity
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExchangeErrorClass {
    Unsupported,
    Authentication,
    Permission,
    RateLimited,
    Network,
    Timeout,
    ExchangeUnavailable,
    Maintenance,
    InvalidRequest,
    Precision,
    InsufficientBalance,
    InsufficientPosition,
    DuplicateClientOrderId,
    OrderNotFound,
    OrderRejected,
    RiskRejected,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RateLimitScope {
    Rest,
    WebSocket,
    Orders,
    Cancels,
    UserStream,
    Global,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RateLimitState {
    pub exchange: ExchangeId,
    pub scope: RateLimitScope,
    pub endpoint: Option<String>,
    pub limit: Option<u32>,
    pub used: Option<u32>,
    pub remaining: Option<u32>,
    pub window_ms: Option<u64>,
    pub reset_at: Option<DateTime<Utc>>,
    pub retry_after_ms: Option<u64>,
    pub banned_until: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

impl RateLimitState {
    pub fn is_limited(&self, now: DateTime<Utc>) -> bool {
        self.remaining == Some(0)
            || self
                .retry_after_ms
                .is_some_and(|retry_after| retry_after > 0)
            || self
                .banned_until
                .is_some_and(|banned_until| banned_until > now)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PositionMode {
    OneWay,
    Hedge,
}

impl PositionMode {
    pub fn is_hedge(self) -> bool {
        matches!(self, Self::Hedge)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LeverageCommand {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub exchange_symbol: ExchangeSymbol,
    pub leverage: u32,
    pub requested_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LeverageAck {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub exchange_symbol: ExchangeSymbol,
    pub leverage: u32,
    pub accepted: bool,
    pub message: Option<String>,
    pub acknowledged_at: DateTime<Utc>,
}

impl LeverageAck {
    pub fn dry_run(command: &LeverageCommand, acknowledged_at: DateTime<Utc>) -> Self {
        Self {
            exchange: command.exchange.clone(),
            canonical_symbol: command.canonical_symbol.clone(),
            exchange_symbol: command.exchange_symbol.clone(),
            leverage: command.leverage,
            accepted: false,
            message: Some("dry-run: leverage was not sent to adapter".to_string()),
            acknowledged_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PositionModeCommand {
    pub exchange: ExchangeId,
    pub mode: PositionMode,
    pub requested_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PositionModeAck {
    pub exchange: ExchangeId,
    pub mode: PositionMode,
    pub accepted: bool,
    pub message: Option<String>,
    pub acknowledged_at: DateTime<Utc>,
}

impl PositionModeAck {
    pub fn dry_run(command: &PositionModeCommand, acknowledged_at: DateTime<Utc>) -> Self {
        Self {
            exchange: command.exchange.clone(),
            mode: command.mode,
            accepted: false,
            message: Some("dry-run: position mode was not sent to adapter".to_string()),
            acknowledged_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClosePositionCommand {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub exchange_symbol: ExchangeSymbol,
    pub position_side: PositionSide,
    pub quantity: f64,
    pub price: Option<f64>,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    pub client_order_id: String,
    pub max_slippage_pct: Option<f64>,
    pub requested_at: DateTime<Utc>,
}

impl ClosePositionCommand {
    #[allow(clippy::too_many_arguments)]
    pub fn market(
        exchange: ExchangeId,
        canonical_symbol: CanonicalSymbol,
        exchange_symbol: ExchangeSymbol,
        position_side: PositionSide,
        quantity: f64,
        client_order_id: impl Into<String>,
        requested_at: DateTime<Utc>,
    ) -> Self {
        Self {
            exchange,
            canonical_symbol,
            exchange_symbol,
            position_side,
            quantity,
            price: None,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::Ioc,
            client_order_id: client_order_id.into(),
            max_slippage_pct: None,
            requested_at,
        }
    }

    pub fn order_side(&self) -> OrderSide {
        match self.position_side {
            PositionSide::Long => OrderSide::Sell,
            PositionSide::Short => OrderSide::Buy,
            PositionSide::Net => OrderSide::Sell,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClosePositionAck {
    pub exchange: ExchangeId,
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub accepted: bool,
    pub status: OrderCommandStatus,
    pub message: Option<String>,
    pub acknowledged_at: DateTime<Utc>,
}

impl ClosePositionAck {
    pub fn dry_run(command: &ClosePositionCommand, acknowledged_at: DateTime<Utc>) -> Self {
        Self {
            exchange: command.exchange.clone(),
            client_order_id: command.client_order_id.clone(),
            exchange_order_id: None,
            accepted: false,
            status: OrderCommandStatus::Planned,
            message: Some("dry-run: close position was not sent to adapter".to_string()),
            acknowledged_at,
        }
    }
}

#[async_trait]
pub trait TradingAdapter: Send + Sync {
    fn exchange(&self) -> ExchangeId;
    fn capabilities(&self) -> TradingCapabilities;

    async fn place_order(&self, command: OrderCommand) -> anyhow::Result<OrderAck>;
    async fn cancel_order(&self, command: CancelCommand) -> anyhow::Result<CancelAck>;
    async fn get_order(&self, query: OrderQuery) -> anyhow::Result<OrderState>;
    async fn get_open_orders(
        &self,
        symbol: Option<&ExchangeSymbol>,
    ) -> anyhow::Result<Vec<OrderState>>;
    async fn get_positions(
        &self,
        symbol: Option<&ExchangeSymbol>,
    ) -> anyhow::Result<Vec<ExchangePosition>>;
    async fn get_balances(&self) -> anyhow::Result<Vec<ExchangeBalance>>;
    async fn get_fills(&self, _query: FillQuery) -> anyhow::Result<Vec<FillEvent>> {
        anyhow::bail!(
            "unsupported: trading adapter {} does not support fill queries",
            self.exchange()
        )
    }

    async fn set_leverage(&self, _command: LeverageCommand) -> anyhow::Result<LeverageAck> {
        anyhow::bail!(
            "trading adapter {} does not support leverage",
            self.exchange()
        )
    }

    async fn set_position_mode(
        &self,
        _command: PositionModeCommand,
    ) -> anyhow::Result<PositionModeAck> {
        anyhow::bail!(
            "trading adapter {} does not support position mode changes",
            self.exchange()
        )
    }

    async fn close_position(
        &self,
        _command: ClosePositionCommand,
    ) -> anyhow::Result<ClosePositionAck> {
        anyhow::bail!(
            "trading adapter {} does not support close position",
            self.exchange()
        )
    }

    async fn load_symbol_rules(
        &self,
        _symbol: &ExchangeSymbol,
    ) -> anyhow::Result<Option<InstrumentMeta>> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    struct NoFillTradingAdapter;

    #[async_trait]
    impl TradingAdapter for NoFillTradingAdapter {
        fn exchange(&self) -> ExchangeId {
            ExchangeId::Binance
        }

        fn capabilities(&self) -> TradingCapabilities {
            TradingCapabilities::default()
        }

        async fn place_order(&self, _command: OrderCommand) -> anyhow::Result<OrderAck> {
            anyhow::bail!("not used")
        }

        async fn cancel_order(&self, _command: CancelCommand) -> anyhow::Result<CancelAck> {
            anyhow::bail!("not used")
        }

        async fn get_order(&self, _query: OrderQuery) -> anyhow::Result<OrderState> {
            anyhow::bail!("not used")
        }

        async fn get_open_orders(
            &self,
            _symbol: Option<&ExchangeSymbol>,
        ) -> anyhow::Result<Vec<OrderState>> {
            Ok(Vec::new())
        }

        async fn get_positions(
            &self,
            _symbol: Option<&ExchangeSymbol>,
        ) -> anyhow::Result<Vec<ExchangePosition>> {
            Ok(Vec::new())
        }

        async fn get_balances(&self) -> anyhow::Result<Vec<ExchangeBalance>> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn default_get_fills_should_report_unsupported() {
        let adapter = NoFillTradingAdapter;
        let err = adapter
            .get_fills(FillQuery::for_symbol(
                ExchangeId::Binance,
                CanonicalSymbol::new("BTC", "USDT"),
                ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT"),
            ))
            .await
            .expect_err("default get_fills should be unsupported");

        let message = err.to_string();
        assert!(message.contains("unsupported"));
        assert!(message.contains("binance"));
    }
}
