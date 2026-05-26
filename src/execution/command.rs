use crate::execution::{deterministic_client_order_id, BundleLeg};
use crate::market::{CanonicalSymbol, ExchangeId, ExchangeSymbol, RuntimeMode};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderIntent {
    OpenLongMaker,
    OpenShortMaker,
    HedgeLongTaker,
    HedgeShortTaker,
    CloseLongMaker,
    CloseShortMaker,
    CloseLongTaker,
    CloseShortTaker,
    EmergencyCloseLongTaker,
    EmergencyCloseShortTaker,
    CancelMaker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PositionSide {
    Long,
    Short,
    Net,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderType {
    Limit,
    Market,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TimeInForce {
    Gtc,
    Ioc,
    Fok,
    PostOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderCommandStatus {
    Planned,
    Submitted,
    Accepted,
    PartiallyFilled,
    Filled,
    CancelRequested,
    Cancelled,
    Rejected,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderCommand {
    pub command_id: String,
    pub bundle_id: String,
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub exchange_symbol: ExchangeSymbol,
    pub intent: OrderIntent,
    pub side: OrderSide,
    pub position_side: PositionSide,
    pub order_type: OrderType,
    pub quantity: f64,
    pub price: Option<f64>,
    pub time_in_force: TimeInForce,
    pub post_only: bool,
    pub reduce_only: bool,
    pub client_order_id: String,
    pub max_slippage_pct: Option<f64>,
    pub status: OrderCommandStatus,
    pub created_at: DateTime<Utc>,
}

impl OrderCommand {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        mode: RuntimeMode,
        bundle_id: impl Into<String>,
        leg: BundleLeg,
        attempt: u32,
        exchange: ExchangeId,
        canonical_symbol: CanonicalSymbol,
        exchange_symbol: ExchangeSymbol,
        intent: OrderIntent,
        side: OrderSide,
        position_side: PositionSide,
        order_type: OrderType,
        quantity: f64,
        price: Option<f64>,
        time_in_force: TimeInForce,
        post_only: bool,
        reduce_only: bool,
        max_slippage_pct: Option<f64>,
        created_at: DateTime<Utc>,
    ) -> Self {
        let bundle_id = bundle_id.into();
        let client_order_id = deterministic_client_order_id(mode, &bundle_id, leg, attempt);
        let command_id = format!("cmd-{client_order_id}");

        Self {
            command_id,
            bundle_id,
            exchange,
            canonical_symbol,
            exchange_symbol,
            intent,
            side,
            position_side,
            order_type,
            quantity,
            price,
            time_in_force,
            post_only,
            reduce_only,
            client_order_id,
            max_slippage_pct,
            status: OrderCommandStatus::Planned,
            created_at,
        }
    }
}
