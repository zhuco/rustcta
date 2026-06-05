//! Strategy-facing exchange gateway contracts.
//!
//! This module is the stable boundary for turning exchange implementations into
//! reusable gateway plugins. It deliberately composes the existing market-data
//! and trading traits instead of replacing them, so current strategies keep
//! running while exchange internals are split into smaller modules.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::exchanges::private_perp::{PrivatePerpExchange, PrivateRestAuth, PrivateWsAuth};
use crate::execution::{PositionMode, TradingAdapter, TradingCapabilities};
use crate::market::{ExchangeId, InstrumentMeta, MarketCapabilities, MarketDataAdapter};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GatewayKind {
    Core,
    PrivatePerp,
    MarketDataOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExchangeGatewayCapabilities {
    pub exchange: ExchangeId,
    pub kind: GatewayKind,
    pub market: Option<MarketCapabilities>,
    pub trading: Option<TradingCapabilities>,
    pub supports_private_rest: bool,
    pub supports_private_ws: bool,
    pub supports_dual_side_position: bool,
}

impl ExchangeGatewayCapabilities {
    pub fn market_data_only(exchange: ExchangeId, market: MarketCapabilities) -> Self {
        Self {
            exchange,
            kind: GatewayKind::MarketDataOnly,
            market: Some(market),
            trading: None,
            supports_private_rest: false,
            supports_private_ws: false,
            supports_dual_side_position: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrivateGatewayAuth {
    pub rest: PrivateRestAuth,
    pub ws: Option<PrivateWsAuth>,
}

#[async_trait]
pub trait ExchangeGateway: Send + Sync {
    fn exchange(&self) -> ExchangeId;
    fn capabilities(&self) -> ExchangeGatewayCapabilities;

    fn market_data(&self) -> Option<Box<dyn MarketDataAdapter + Send + Sync>>;

    fn trading(
        &self,
        _auth: PrivateRestAuth,
        _position_mode: PositionMode,
        _instruments: Vec<InstrumentMeta>,
        _private_rest_base_url: Option<&str>,
    ) -> Result<Arc<dyn TradingAdapter>> {
        anyhow::bail!(
            "{} gateway does not expose a trading adapter",
            self.exchange()
        )
    }

    fn private_perp_exchange(&self) -> Option<PrivatePerpExchange> {
        None
    }
}
