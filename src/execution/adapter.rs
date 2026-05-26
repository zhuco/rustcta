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
