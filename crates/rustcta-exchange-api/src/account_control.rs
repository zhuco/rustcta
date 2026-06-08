use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    ExchangeApiError, ExchangeApiResult, ExchangeId, OrderStatus, OrderType, PositionSide,
    RequestContext, ResponseMetadata, SymbolScope, TimeInForce,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountControlCapabilities {
    pub exchange: ExchangeId,
    pub supports_symbol_account_config: bool,
    pub supports_leverage: bool,
    pub supports_position_mode_change: bool,
    pub supports_close_position: bool,
    pub supports_countdown_cancel_all: bool,
}

impl AccountControlCapabilities {
    pub fn unsupported(exchange: ExchangeId) -> Self {
        Self {
            exchange,
            supports_symbol_account_config: false,
            supports_leverage: false,
            supports_position_mode_change: false,
            supports_close_position: false,
            supports_countdown_cancel_all: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionMode {
    OneWay,
    Hedge,
}

impl PositionMode {
    pub fn is_hedge(self) -> bool {
        matches!(self, Self::Hedge)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarginMode {
    Cross,
    Isolated,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SymbolAccountConfigRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub symbol: SymbolScope,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SymbolAccountConfig {
    pub symbol: SymbolScope,
    pub position_mode: Option<PositionMode>,
    pub margin_mode: Option<MarginMode>,
    pub leverage: Option<u32>,
    pub max_leverage: Option<u32>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SymbolAccountConfigResponse {
    pub schema_version: u16,
    pub metadata: ResponseMetadata,
    pub config: SymbolAccountConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SetLeverageRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub symbol: SymbolScope,
    pub leverage: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SetLeverageResponse {
    pub schema_version: u16,
    pub metadata: ResponseMetadata,
    pub symbol: SymbolScope,
    pub leverage: u32,
    pub accepted: bool,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SetPositionModeRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub exchange: ExchangeId,
    pub mode: PositionMode,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SetPositionModeResponse {
    pub schema_version: u16,
    pub metadata: ResponseMetadata,
    pub mode: PositionMode,
    pub accepted: bool,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClosePositionRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub symbol: SymbolScope,
    pub position_side: PositionSide,
    pub quantity: String,
    pub price: Option<String>,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    pub client_order_id: String,
    pub max_slippage_pct: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClosePositionResponse {
    pub schema_version: u16,
    pub metadata: ResponseMetadata,
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub accepted: bool,
    pub status: Option<OrderStatus>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CountdownCancelAllRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub exchange: ExchangeId,
    pub symbol: Option<SymbolScope>,
    pub timeout_secs: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CountdownCancelAllResponse {
    pub schema_version: u16,
    pub metadata: ResponseMetadata,
    pub symbol: Option<SymbolScope>,
    pub timeout_secs: u32,
    pub trigger_time: Option<DateTime<Utc>>,
    pub accepted: bool,
    pub message: Option<String>,
}

#[async_trait]
pub trait PerpAccountControlProvider: Send + Sync {
    fn exchange(&self) -> ExchangeId;

    fn account_control_capabilities(&self) -> AccountControlCapabilities {
        AccountControlCapabilities::unsupported(self.exchange())
    }

    async fn get_symbol_account_config(
        &self,
        _request: SymbolAccountConfigRequest,
    ) -> ExchangeApiResult<SymbolAccountConfigResponse> {
        Err(unsupported("get_symbol_account_config"))
    }

    async fn set_leverage(
        &self,
        _request: SetLeverageRequest,
    ) -> ExchangeApiResult<SetLeverageResponse> {
        Err(unsupported("set_leverage"))
    }

    async fn set_position_mode(
        &self,
        _request: SetPositionModeRequest,
    ) -> ExchangeApiResult<SetPositionModeResponse> {
        Err(unsupported("set_position_mode"))
    }

    async fn close_position(
        &self,
        _request: ClosePositionRequest,
    ) -> ExchangeApiResult<ClosePositionResponse> {
        Err(unsupported("close_position"))
    }

    async fn set_countdown_cancel_all(
        &self,
        _request: CountdownCancelAllRequest,
    ) -> ExchangeApiResult<CountdownCancelAllResponse> {
        Err(unsupported("set_countdown_cancel_all"))
    }
}

fn unsupported(operation: &'static str) -> ExchangeApiError {
    ExchangeApiError::Unsupported { operation }
}
