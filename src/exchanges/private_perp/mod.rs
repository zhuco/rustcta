//! Private USDT perpetual REST/WebSocket protocol contracts for Bitget and Gate.
//!
//! This module is intentionally side-effect free: it builds signed request
//! payloads, private WebSocket login/subscription messages, and parses private
//! WebSocket payloads into execution-layer events. Network clients should call
//! this layer instead of encoding exchange-specific fields in strategy code.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use base64::{engine::general_purpose, Engine as _};
use chrono::{DateTime, SecondsFormat, Utc};
use flate2::read::GzDecoder;
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256, Sha512};
use std::collections::{BTreeMap, HashMap};
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Duration as TokioDuration};
use tokio_tungstenite::tungstenite::Message;

use crate::core::types::{Fee, MarketType, OrderStatus as CoreOrderStatus};
use crate::core::ws_connect::connect_async;
use crate::execution::{
    AmendOrderAck, AmendOrderCommand, BatchPlaceAck, BatchPlaceCommand, BatchPlaceError, CancelAck,
    CancelAllAck, CancelAllCommand, CancelBatchAck, CancelBatchCommand, CancelCommand,
    ClosePositionAck, ClosePositionCommand, CountdownCancelAllAck, CountdownCancelAllCommand,
    ExchangeBalance, ExchangeErrorClass, ExchangePosition, FillEvent, FillLiquidity, FillQuery,
    LeverageAck, LeverageCommand, MarginMode, OrderAck, OrderCommand, OrderCommandStatus,
    OrderQuery, OrderSide, OrderState, OrderType, PositionMode, PositionModeAck,
    PositionModeCommand, PositionSide, PrivateErrorEvent, PrivateEvent, PrivateEventKind,
    SymbolAccountConfig, TimeInForce, TradeFeeSnapshot, TradingAdapter, TradingCapabilities,
};
use crate::market::{
    canonical_from_exchange_symbol, exchange_symbol_for, CanonicalSymbol, ExchangeId,
    ExchangeSymbol, InstrumentMeta, RoundingMode,
};

type HmacSha256 = Hmac<Sha256>;
type HmacSha512 = Hmac<Sha512>;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("{exchange} private REST {endpoint:?}: {class:?} code={code:?} message={message}")]
pub struct PrivateRestError {
    pub exchange: ExchangeId,
    pub class: ExchangeErrorClass,
    pub endpoint: Option<String>,
    pub code: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PrivatePerpExchange {
    Binance,
    Okx,
    CoinEx,
    Bitget,
    Gate,
    Bybit,
    Mexc,
    Htx,
    Toobit,
}

impl PrivatePerpExchange {
    pub fn exchange_id(self) -> ExchangeId {
        match self {
            Self::Binance => ExchangeId::Binance,
            Self::Okx => ExchangeId::Okx,
            Self::CoinEx => ExchangeId::CoinEx,
            Self::Bitget => ExchangeId::Bitget,
            Self::Gate => ExchangeId::Gate,
            Self::Bybit => ExchangeId::Bybit,
            Self::Mexc => ExchangeId::Mexc,
            Self::Htx => ExchangeId::Htx,
            Self::Toobit => ExchangeId::Toobit,
        }
    }

    pub fn rest_base_url(self) -> &'static str {
        match self {
            Self::Binance => "https://fapi.binance.com",
            Self::Okx => "https://www.okx.com",
            Self::CoinEx => "https://api.coinex.com/v2",
            Self::Bitget => "https://api.bitget.com",
            Self::Gate => "https://api.gateio.ws/api/v4",
            Self::Bybit => "https://api.bybit.com",
            Self::Mexc => "https://contract.mexc.com",
            Self::Htx => "https://api.hbdm.com",
            Self::Toobit => "https://api.toobit.com",
        }
    }

    pub fn private_ws_url(self) -> &'static str {
        match self {
            Self::Binance => "wss://fstream.binance.com/private",
            Self::Okx => "wss://ws.okx.com:8443/ws/v5/private",
            Self::CoinEx => "wss://socket.coinex.com/v2/futures",
            Self::Bitget => "wss://ws.bitget.com/v2/ws/private",
            Self::Gate => "wss://fx-ws.gateio.ws/v4/ws/usdt",
            Self::Bybit => "wss://stream.bybit.com/v5/private",
            Self::Mexc => "wss://contract.mexc.com/edge",
            Self::Htx => "wss://api.hbdm.com/linear-swap-notification",
            Self::Toobit => "wss://stream.toobit.com",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PrivateRestMethod {
    Get,
    Post,
    Put,
    Patch,
    Delete,
}

impl PrivateRestMethod {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Patch => "PATCH",
            Self::Delete => "DELETE",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrivateRestRequestSpec {
    pub exchange: ExchangeId,
    pub method: PrivateRestMethod,
    pub path: String,
    pub query: BTreeMap<String, String>,
    pub body: Option<Value>,
    pub requires_auth: bool,
}

impl PrivateRestRequestSpec {
    pub fn new(exchange: ExchangeId, method: PrivateRestMethod, path: impl Into<String>) -> Self {
        Self {
            exchange,
            method,
            path: path.into(),
            query: BTreeMap::new(),
            body: None,
            requires_auth: true,
        }
    }

    pub fn with_query(mut self, key: impl Into<String>, value: impl ToString) -> Self {
        self.query.insert(key.into(), value.to_string());
        self
    }

    pub fn with_body(mut self, body: Value) -> Self {
        self.body = Some(body);
        self
    }

    pub fn query_string(&self) -> String {
        self.query
            .iter()
            .map(|(key, value)| {
                format!(
                    "{}={}",
                    urlencoding::encode(key),
                    urlencoding::encode(value)
                )
            })
            .collect::<Vec<_>>()
            .join("&")
    }

    pub fn raw_query_string(&self) -> String {
        self.query
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join("&")
    }

    pub fn body_string(&self) -> String {
        self.body.as_ref().map(Value::to_string).unwrap_or_default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrivateWsRequest {
    pub exchange: ExchangeId,
    pub url: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrivateWsAuth {
    pub api_key: String,
    pub api_secret: String,
    pub passphrase: Option<String>,
    #[serde(default)]
    pub account_id: Option<String>,
    #[serde(default)]
    pub demo_trading: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrivateWsSubscription {
    pub channel: String,
    pub symbols: Vec<ExchangeSymbol>,
    #[serde(default)]
    pub auth: Option<PrivateWsAuth>,
}

pub trait PrivatePerpProtocol {
    fn exchange(&self) -> PrivatePerpExchange;

    fn place_order(
        &self,
        command: &OrderCommand,
        position_mode: PositionMode,
    ) -> Result<PrivateRestRequestSpec>;
    fn place_batch_orders(
        &self,
        command: &BatchPlaceCommand,
        position_mode: PositionMode,
    ) -> Result<PrivateRestRequestSpec> {
        let exchange = self.exchange().exchange_id();
        if command.exchange != exchange {
            anyhow::bail!(
                "{exchange} private perp batch-place requires {exchange} exchange symbols"
            );
        }
        if command.orders.is_empty() {
            anyhow::bail!("{exchange} private perp batch-place requires at least one order");
        }
        if command
            .orders
            .iter()
            .any(|order| order.exchange != exchange || order.exchange_symbol.exchange != exchange)
        {
            anyhow::bail!(
                "{exchange} private perp batch-place requires {exchange} exchange symbols"
            );
        }
        anyhow::bail!(
            "{} private perp batch-place is not implemented for {} orders in {:?}",
            exchange,
            command.orders.len(),
            position_mode
        )
    }
    fn cancel_order(&self, command: &CancelCommand) -> Result<PrivateRestRequestSpec>;
    fn cancel_all_orders(&self, command: &CancelAllCommand) -> Result<PrivateRestRequestSpec>;
    fn cancel_batch_orders(&self, command: &CancelBatchCommand) -> Result<PrivateRestRequestSpec>;
    fn get_order(&self, query: &OrderQuery) -> Result<PrivateRestRequestSpec>;
    fn get_open_orders(&self, symbol: Option<&ExchangeSymbol>) -> Result<PrivateRestRequestSpec>;
    fn get_all_orders(&self, query: &OrderHistoryQuery) -> Result<PrivateRestRequestSpec> {
        let exchange = self.exchange().exchange_id();
        if query.exchange != exchange || query.exchange_symbol.exchange != exchange {
            anyhow::bail!(
                "{exchange} private perp all-orders history requires {exchange} exchange symbols"
            );
        }
        anyhow::bail!(
            "{} private perp all-orders history is not implemented",
            exchange
        )
    }
    fn get_order_amendments(
        &self,
        query: &OrderAmendmentHistoryQuery,
    ) -> Result<PrivateRestRequestSpec> {
        let exchange = self.exchange().exchange_id();
        if query.exchange != exchange || query.exchange_symbol.exchange != exchange {
            anyhow::bail!(
                "{exchange} private perp order-amendment history requires {exchange} exchange symbols"
            );
        }
        if query.exchange_order_id.is_none() && query.client_order_id.is_none() {
            anyhow::bail!(
                "{exchange} private perp order-amendment history requires exchange or client order id"
            );
        }
        anyhow::bail!(
            "{} private perp order-amendment history is not implemented",
            exchange
        )
    }
    fn get_fills(&self, query: &FillQuery) -> Result<PrivateRestRequestSpec>;
    fn get_positions(&self, symbol: Option<&ExchangeSymbol>) -> Result<PrivateRestRequestSpec>;
    fn get_balances(&self) -> Result<PrivateRestRequestSpec>;
    fn get_trade_fee(&self, symbol: &ExchangeSymbol) -> Result<PrivateRestRequestSpec>;
    fn get_symbol_account_config(&self, symbol: &ExchangeSymbol) -> Result<PrivateRestRequestSpec>;
    fn amend_order(&self, command: &AmendOrderCommand) -> Result<PrivateRestRequestSpec>;
    fn set_leverage(&self, command: &LeverageCommand) -> Result<PrivateRestRequestSpec>;
    fn set_position_mode(&self, command: &PositionModeCommand) -> Result<PrivateRestRequestSpec>;
    fn set_countdown_cancel_all(
        &self,
        command: &CountdownCancelAllCommand,
    ) -> Result<PrivateRestRequestSpec>;

    fn ws_login(&self, auth: &PrivateWsAuth, timestamp: i64) -> Result<PrivateWsRequest>;
    fn ws_subscribe(
        &self,
        subscription: &PrivateWsSubscription,
        timestamp: i64,
    ) -> Result<PrivateWsRequest>;
    fn parse_private_ws_message(
        &self,
        raw: &str,
        received_at: DateTime<Utc>,
    ) -> Result<Vec<PrivateEvent>>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderHistoryQuery {
    pub exchange: ExchangeId,
    pub exchange_symbol: ExchangeSymbol,
    pub exchange_order_id: Option<String>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub limit: Option<u32>,
}

impl OrderHistoryQuery {
    pub fn for_symbol(exchange: ExchangeId, exchange_symbol: ExchangeSymbol) -> Self {
        Self {
            exchange,
            exchange_symbol,
            exchange_order_id: None,
            start_time: None,
            end_time: None,
            limit: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderAmendmentHistoryQuery {
    pub exchange: ExchangeId,
    pub exchange_symbol: ExchangeSymbol,
    pub exchange_order_id: Option<String>,
    pub client_order_id: Option<String>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub limit: Option<u32>,
}

impl OrderAmendmentHistoryQuery {
    pub fn for_order(
        exchange: ExchangeId,
        exchange_symbol: ExchangeSymbol,
        client_order_id: Option<String>,
        exchange_order_id: Option<String>,
    ) -> Self {
        Self {
            exchange,
            exchange_symbol,
            client_order_id,
            exchange_order_id,
            start_time: None,
            end_time: None,
            limit: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderAmendmentEvent {
    pub exchange: ExchangeId,
    pub exchange_symbol: ExchangeSymbol,
    pub amendment_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub client_order_id: Option<String>,
    pub price_before: Option<f64>,
    pub price_after: Option<f64>,
    pub quantity_before: Option<f64>,
    pub quantity_after: Option<f64>,
    pub amendment_count: Option<u32>,
    pub amended_at: DateTime<Utc>,
}

#[async_trait]
pub trait PrivateRestTransport: Send + Sync {
    async fn execute(&self, request: PrivateRestRequestSpec) -> Result<Value>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrivateRestAuth {
    pub api_key: String,
    pub api_secret: String,
    pub passphrase: Option<String>,
    #[serde(default)]
    pub demo_trading: bool,
}

impl From<PrivateWsAuth> for PrivateRestAuth {
    fn from(value: PrivateWsAuth) -> Self {
        Self {
            api_key: value.api_key,
            api_secret: value.api_secret,
            passphrase: value.passphrase,
            demo_trading: value.demo_trading,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReqwestPrivateRestTransport {
    exchange: PrivatePerpExchange,
    auth: PrivateRestAuth,
    base_url: String,
    client: reqwest::Client,
}

impl ReqwestPrivateRestTransport {
    pub fn new(exchange: PrivatePerpExchange, auth: PrivateRestAuth) -> Result<Self> {
        Self::with_base_url(exchange, auth, exchange.rest_base_url())
    }

    pub fn with_base_url(
        exchange: PrivatePerpExchange,
        auth: PrivateRestAuth,
        base_url: impl Into<String>,
    ) -> Result<Self> {
        let client = crate::core::http2_fix::shared_http_client();
        Ok(Self {
            exchange,
            auth,
            base_url: base_url.into().trim_end_matches('/').to_string(),
            client,
        })
    }

    pub fn signed_headers(
        &self,
        request: &PrivateRestRequestSpec,
        timestamp: i64,
    ) -> Result<BTreeMap<String, String>> {
        match self.exchange {
            PrivatePerpExchange::Binance => binance_rest_headers(&self.auth, request, timestamp),
            PrivatePerpExchange::Okx => okx_rest_headers(&self.auth, request, timestamp),
            PrivatePerpExchange::CoinEx => coinex_rest_headers(&self.auth, request, timestamp),
            PrivatePerpExchange::Bitget => bitget_rest_headers(&self.auth, request, timestamp),
            PrivatePerpExchange::Gate => {
                let mut headers = gate_rest_headers(&self.auth, request, timestamp)?;
                headers.insert("X-Gate-Size-Decimal".to_string(), "1".to_string());
                Ok(headers)
            }
            PrivatePerpExchange::Bybit => bybit_rest_headers(&self.auth, request, timestamp),
            PrivatePerpExchange::Mexc => mexc_rest_headers(&self.auth, request, timestamp),
            PrivatePerpExchange::Htx => htx_rest_headers(&self.auth, request, timestamp),
            PrivatePerpExchange::Toobit => toobit_rest_headers(&self.auth, request, timestamp),
        }
    }

    fn signed_request(
        &self,
        request: PrivateRestRequestSpec,
        timestamp: i64,
    ) -> Result<PrivateRestRequestSpec> {
        match self.exchange {
            PrivatePerpExchange::Binance => binance_signed_request(&self.auth, request, timestamp),
            PrivatePerpExchange::Toobit => toobit_signed_request(&self.auth, request, timestamp),
            _ => Ok(request),
        }
    }

    fn url(&self, request: &PrivateRestRequestSpec) -> String {
        let query = request.query_string();
        if query.is_empty() {
            format!("{}{}", self.base_url, request.path)
        } else {
            format!("{}{}?{}", self.base_url, request.path, query)
        }
    }
}

#[async_trait]
impl PrivateRestTransport for ReqwestPrivateRestTransport {
    async fn execute(&self, request: PrivateRestRequestSpec) -> Result<Value> {
        if request.exchange != self.exchange.exchange_id() {
            anyhow::bail!(
                "private transport for {} received {} request",
                self.exchange.exchange_id(),
                request.exchange
            );
        }

        let timestamp = match self.exchange {
            PrivatePerpExchange::Binance
            | PrivatePerpExchange::Okx
            | PrivatePerpExchange::CoinEx
            | PrivatePerpExchange::Bitget
            | PrivatePerpExchange::Bybit
            | PrivatePerpExchange::Mexc
            | PrivatePerpExchange::Toobit => Utc::now().timestamp_millis(),
            PrivatePerpExchange::Gate | PrivatePerpExchange::Htx => Utc::now().timestamp(),
        };
        let request = if request.requires_auth {
            self.signed_request(request, timestamp)?
        } else {
            request
        };
        let mut builder = match request.method {
            PrivateRestMethod::Get => self.client.get(self.url(&request)),
            PrivateRestMethod::Post => self.client.post(self.url(&request)),
            PrivateRestMethod::Put => self.client.put(self.url(&request)),
            PrivateRestMethod::Patch => self.client.patch(self.url(&request)),
            PrivateRestMethod::Delete => self.client.delete(self.url(&request)),
        };

        if request.requires_auth {
            for (key, value) in self.signed_headers(&request, timestamp)? {
                builder = builder.header(key, value);
            }
        }
        if let Some(body) = &request.body {
            builder = builder.json(body);
        }

        let response = builder.send().await?;
        let status = response.status();
        let raw = response.text().await?;
        let value = if raw.trim().is_empty() {
            Value::Null
        } else {
            serde_json::from_str::<Value>(&raw).map_err(|err| {
                anyhow!(
                    "{} private REST JSON decode failed: {err}; raw={raw}",
                    request.exchange
                )
            })?
        };
        if !status.is_success() {
            return Err(private_rest_http_error(&request, status.as_u16(), &value).into());
        }
        normalize_private_rest_response(request.exchange, value)
    }
}

pub struct PrivatePerpTradingAdapter<P, T> {
    exchange: ExchangeId,
    protocol: P,
    transport: T,
    position_mode: Mutex<PositionMode>,
    instruments: HashMap<CanonicalSymbol, InstrumentMeta>,
}

impl<P, T> PrivatePerpTradingAdapter<P, T>
where
    P: PrivatePerpProtocol + Send + Sync + Clone,
    T: PrivateRestTransport + Send + Sync + Clone,
{
    pub fn new(protocol: P, transport: T) -> Self {
        Self {
            exchange: protocol.exchange().exchange_id(),
            protocol,
            transport,
            position_mode: Mutex::new(PositionMode::OneWay),
            instruments: HashMap::new(),
        }
    }

    pub fn with_position_mode(mut self, position_mode: PositionMode) -> Self {
        *self
            .position_mode
            .get_mut()
            .expect("position mode mutex poisoned") = position_mode;
        self
    }

    pub fn with_instruments(
        mut self,
        instruments: impl IntoIterator<Item = InstrumentMeta>,
    ) -> Self {
        for instrument in instruments {
            self.register_instrument(instrument);
        }
        self
    }

    pub fn register_instrument(&mut self, instrument: InstrumentMeta) {
        if instrument.exchange == self.exchange {
            self.instruments
                .insert(instrument.canonical_symbol.clone(), instrument);
        }
    }

    fn instrument_for(&self, canonical_symbol: &CanonicalSymbol) -> Option<&InstrumentMeta> {
        self.instruments.get(canonical_symbol)
    }

    fn instrument_for_exchange_symbol(&self, symbol: &ExchangeSymbol) -> Option<&InstrumentMeta> {
        canonical_from_exchange_symbol(&symbol.exchange, &symbol.symbol)
            .and_then(|canonical| self.instruments.get(&canonical))
            .or_else(|| {
                self.instruments
                    .values()
                    .find(|instrument| instrument.exchange_symbol == *symbol)
            })
    }

    fn gate_contract_size_for_symbol(&self, symbol: &ExchangeSymbol) -> Option<f64> {
        if self.exchange != ExchangeId::Gate || symbol.exchange != ExchangeId::Gate {
            return None;
        }
        self.instrument_for_exchange_symbol(symbol)
            .map(instrument_contract_size)
    }

    fn gate_contract_size_for_item(
        &self,
        item: &Value,
        fallback_symbol: Option<&ExchangeSymbol>,
    ) -> Option<f64> {
        if self.exchange != ExchangeId::Gate {
            return None;
        }
        str_field(item, &["contract"])
            .and_then(|symbol| {
                self.gate_contract_size_for_symbol(&ExchangeSymbol::new(ExchangeId::Gate, symbol))
            })
            .or_else(|| {
                fallback_symbol.and_then(|symbol| self.gate_contract_size_for_symbol(symbol))
            })
    }

    fn order_command_for_protocol(&self, command: &OrderCommand) -> OrderCommand {
        let mut command = command.clone();
        if self.exchange == ExchangeId::Gate {
            if let Some(instrument) = self.instrument_for(&command.canonical_symbol) {
                command.quantity = gate_base_to_contract_quantity(command.quantity, instrument);
            }
        }
        command
    }

    fn close_command_for_protocol(&self, command: &ClosePositionCommand) -> ClosePositionCommand {
        let mut command = command.clone();
        if self.exchange == ExchangeId::Gate {
            if let Some(instrument) = self.instrument_for(&command.canonical_symbol) {
                command.quantity = gate_base_to_contract_quantity(command.quantity, instrument);
            }
        }
        command
    }

    fn normalize_order_command(&self, mut command: OrderCommand) -> Result<OrderCommand> {
        if !command.quantity.is_finite() || command.quantity <= 0.0 {
            anyhow::bail!(
                "{} order quantity must be positive and finite",
                self.exchange
            );
        }
        if command
            .price
            .is_some_and(|price| !price.is_finite() || price <= 0.0)
        {
            anyhow::bail!("{} order price must be positive and finite", self.exchange);
        }
        if let Some(instrument) = self.instrument_for(&command.canonical_symbol) {
            let quantity = if self.exchange == ExchangeId::Gate {
                gate_base_to_contract_quantity(command.quantity, instrument)
            } else {
                command.quantity
            };
            let normalized = instrument.normalize_order_input(
                quantity,
                command.price,
                RoundingMode::Floor,
                RoundingMode::Nearest,
            );
            if !normalized.is_valid() {
                anyhow::bail!(
                    "{} {} order violates precision rules for {}: {:?}",
                    self.exchange,
                    command.client_order_id,
                    instrument.exchange_symbol.symbol,
                    normalized.violations
                );
            }
            command.quantity = if self.exchange == ExchangeId::Gate {
                gate_contract_to_base_quantity(
                    normalized.quantity,
                    instrument_contract_size(instrument),
                )
            } else {
                normalized.quantity
            };
            command.price = normalized.price;
        }
        if matches!(command.order_type, OrderType::Limit) && command.price.is_none() {
            anyhow::bail!("{} limit order requires price", self.exchange);
        }
        Ok(command)
    }

    fn normalize_close_command(
        &self,
        mut command: ClosePositionCommand,
    ) -> Result<ClosePositionCommand> {
        if !command.quantity.is_finite() || command.quantity <= 0.0 {
            anyhow::bail!(
                "{} close quantity must be positive and finite",
                self.exchange
            );
        }
        if command
            .price
            .is_some_and(|price| !price.is_finite() || price <= 0.0)
        {
            anyhow::bail!("{} close price must be positive and finite", self.exchange);
        }
        if let Some(instrument) = self.instrument_for(&command.canonical_symbol) {
            let quantity = if self.exchange == ExchangeId::Gate {
                gate_base_to_contract_quantity(command.quantity, instrument)
            } else {
                command.quantity
            };
            let normalized = instrument.normalize_order_input(
                quantity,
                command.price,
                RoundingMode::Floor,
                RoundingMode::Nearest,
            );
            if !normalized.is_valid() {
                anyhow::bail!(
                    "{} {} close violates precision rules for {}: {:?}",
                    self.exchange,
                    command.client_order_id,
                    instrument.exchange_symbol.symbol,
                    normalized.violations
                );
            }
            command.quantity = if self.exchange == ExchangeId::Gate {
                gate_contract_to_base_quantity(
                    normalized.quantity,
                    instrument_contract_size(instrument),
                )
            } else {
                normalized.quantity
            };
            command.price = normalized.price;
        }
        if matches!(command.order_type, OrderType::Limit) && command.price.is_none() {
            anyhow::bail!("{} limit close requires price", self.exchange);
        }
        Ok(command)
    }

    fn normalize_amend_command(&self, mut command: AmendOrderCommand) -> Result<AmendOrderCommand> {
        if command
            .new_quantity
            .is_some_and(|quantity| !quantity.is_finite() || quantity <= 0.0)
        {
            anyhow::bail!(
                "{} amend quantity must be positive and finite",
                self.exchange
            );
        }
        if command
            .new_price
            .is_some_and(|price| !price.is_finite() || price <= 0.0)
        {
            anyhow::bail!("{} amend price must be positive and finite", self.exchange);
        }
        if let Some(instrument) = self.instrument_for(&command.canonical_symbol) {
            let quantity = command.new_quantity.map(|quantity| {
                if self.exchange == ExchangeId::Gate {
                    gate_base_to_contract_quantity(quantity, instrument)
                } else {
                    quantity
                }
            });
            let normalized = instrument.normalize_order_input(
                quantity.unwrap_or(instrument.min_qty.max(instrument.quantity_step)),
                command.new_price,
                RoundingMode::Floor,
                RoundingMode::Nearest,
            );
            if command.new_quantity.is_some() && !normalized.is_valid() {
                anyhow::bail!(
                    "{} amend violates precision rules for {}: {:?}",
                    self.exchange,
                    instrument.exchange_symbol.symbol,
                    normalized.violations
                );
            }
            if command.new_quantity.is_some() {
                command.new_quantity = Some(if self.exchange == ExchangeId::Gate {
                    gate_contract_to_base_quantity(
                        normalized.quantity,
                        instrument_contract_size(instrument),
                    )
                } else {
                    normalized.quantity
                });
            }
            if command.new_price.is_some() {
                command.new_price = normalized.price;
            }
        }
        Ok(command)
    }

    fn current_position_mode(&self) -> Result<PositionMode> {
        self.position_mode
            .lock()
            .map(|guard| *guard)
            .map_err(|_| anyhow!("position mode mutex poisoned"))
    }

    fn set_local_position_mode(&self, mode: PositionMode) -> Result<()> {
        *self
            .position_mode
            .lock()
            .map_err(|_| anyhow!("position mode mutex poisoned"))? = mode;
        Ok(())
    }

    pub async fn get_order_history(&self, query: OrderHistoryQuery) -> Result<Vec<OrderState>> {
        if query.exchange != self.exchange() || query.exchange_symbol.exchange != self.exchange() {
            anyhow::bail!(
                "{} all-orders history requires {} exchange symbols",
                self.exchange(),
                self.exchange()
            );
        }

        let value = self
            .transport
            .execute(self.protocol.get_all_orders(&query)?)
            .await?;
        let exchange = self.exchange.clone();
        let fallback_gate_contract_size =
            self.gate_contract_size_for_symbol(&query.exchange_symbol);
        Ok(response_items(&value)
            .into_iter()
            .filter_map(|item| {
                let gate_contract_size = self
                    .gate_contract_size_for_item(&item, Some(&query.exchange_symbol))
                    .or(fallback_gate_contract_size);
                parse_rest_order_with_gate_contract_size(
                    exchange.clone(),
                    &query.exchange_symbol,
                    &item,
                    Utc::now(),
                    gate_contract_size,
                )
            })
            .collect())
    }

    pub async fn get_order_amendment_history(
        &self,
        query: OrderAmendmentHistoryQuery,
    ) -> Result<Vec<OrderAmendmentEvent>> {
        if query.exchange != self.exchange() || query.exchange_symbol.exchange != self.exchange() {
            anyhow::bail!(
                "{} order-amendment history requires {} exchange symbols",
                self.exchange(),
                self.exchange()
            );
        }

        let value = self
            .transport
            .execute(self.protocol.get_order_amendments(&query)?)
            .await?;
        Ok(response_items(&value)
            .into_iter()
            .filter_map(|item| parse_order_amendment(self.exchange.clone(), &query, &item))
            .collect())
    }
}

#[async_trait]
impl<P, T> TradingAdapter for PrivatePerpTradingAdapter<P, T>
where
    P: PrivatePerpProtocol + Send + Sync + Clone,
    T: PrivateRestTransport + Send + Sync + Clone,
{
    fn exchange(&self) -> ExchangeId {
        self.exchange.clone()
    }

    fn capabilities(&self) -> TradingCapabilities {
        private_perp_trading_capabilities(self.exchange())
    }

    async fn place_order(&self, command: OrderCommand) -> Result<OrderAck> {
        ensure_order_supported(self.exchange(), &command, self.capabilities())?;
        let command = self.normalize_order_command(command)?;
        let protocol_command = self.order_command_for_protocol(&command);
        let position_mode = self.current_position_mode()?;
        let value = self
            .transport
            .execute(
                self.protocol
                    .place_order(&protocol_command, position_mode)?,
            )
            .await?;
        let order_id = response_string(
            &value,
            &["order_id_str", "order_id", "orderId", "ordId", "id"],
        )
        .or_else(|| response_string(&value, &["clientOid", "clOrdId", "text"]))
        .unwrap_or_else(|| command.client_order_id.clone());
        Ok(OrderAck {
            exchange: command.exchange,
            client_order_id: command.client_order_id,
            exchange_order_id: Some(order_id),
            accepted: true,
            status: OrderCommandStatus::Accepted,
            message: response_string(&value, &["msg", "message"]),
            acknowledged_at: Utc::now(),
        })
    }

    async fn place_batch_orders(&self, command: BatchPlaceCommand) -> Result<BatchPlaceAck> {
        if command.exchange != self.exchange() {
            anyhow::bail!(
                "{} batch-place requires {} exchange symbols",
                self.exchange(),
                self.exchange()
            );
        }
        ensure_capability(
            self.exchange(),
            self.capabilities().supports_batch_place_orders,
            "batch-place orders",
        )?;
        if command.orders.is_empty() {
            return Ok(BatchPlaceAck {
                exchange: command.exchange,
                accepted: true,
                placed_orders: 0,
                order_acks: Vec::new(),
                failed_orders: Vec::new(),
                message: Some("empty batch-place".to_string()),
                acknowledged_at: Utc::now(),
            });
        }

        let mut normalized_orders = Vec::with_capacity(command.orders.len());
        for order in command.orders {
            ensure_order_supported(self.exchange(), &order, self.capabilities())?;
            normalized_orders.push(self.normalize_order_command(order)?);
        }
        let protocol_orders = normalized_orders
            .iter()
            .map(|order| self.order_command_for_protocol(order))
            .collect::<Vec<_>>();
        let protocol_command = BatchPlaceCommand {
            exchange: command.exchange.clone(),
            orders: protocol_orders,
            requested_at: command.requested_at,
        };
        let position_mode = self.current_position_mode()?;
        let value = self
            .transport
            .execute(
                self.protocol
                    .place_batch_orders(&protocol_command, position_mode)?,
            )
            .await?;
        Ok(parse_batch_place_ack(
            command.exchange,
            &normalized_orders,
            &value,
            Utc::now(),
        ))
    }

    async fn cancel_order(&self, command: CancelCommand) -> Result<CancelAck> {
        if command.exchange != self.exchange()
            || command.exchange_symbol.exchange != self.exchange()
        {
            anyhow::bail!(
                "{} cancel requires {} exchange symbols",
                self.exchange(),
                self.exchange()
            );
        }

        let value = self
            .transport
            .execute(self.protocol.cancel_order(&command)?)
            .await?;
        Ok(CancelAck {
            exchange: command.exchange,
            client_order_id: command.client_order_id,
            exchange_order_id: command
                .exchange_order_id
                .or_else(|| response_string(&value, &["orderId", "ordId", "id"])),
            accepted: true,
            status: OrderCommandStatus::Cancelled,
            message: response_string(&value, &["msg", "message"]),
            acknowledged_at: Utc::now(),
        })
    }

    async fn cancel_all_orders(&self, command: CancelAllCommand) -> Result<CancelAllAck> {
        if command.exchange != self.exchange()
            || command
                .exchange_symbol
                .as_ref()
                .is_some_and(|symbol| symbol.exchange != self.exchange())
        {
            anyhow::bail!(
                "{} cancel-all requires {} exchange symbols",
                self.exchange(),
                self.exchange()
            );
        }
        if self.exchange() == ExchangeId::Okx {
            let open_orders = self
                .get_open_orders(command.exchange_symbol.as_ref())
                .await?;
            if open_orders.is_empty() {
                return Ok(CancelAllAck {
                    exchange: command.exchange,
                    canonical_symbol: command.canonical_symbol,
                    exchange_symbol: command.exchange_symbol,
                    accepted: true,
                    cancelled_orders: 0,
                    message: Some("no open orders to cancel".to_string()),
                    acknowledged_at: Utc::now(),
                });
            }
            let cancel_commands = open_orders
                .into_iter()
                .filter_map(|order| {
                    if order.exchange_order_id.is_none() && order.client_order_id.is_none() {
                        return None;
                    }
                    Some(CancelCommand {
                        exchange: self.exchange(),
                        canonical_symbol: order.canonical_symbol,
                        exchange_symbol: order.exchange_symbol,
                        client_order_id: order.client_order_id,
                        exchange_order_id: order.exchange_order_id,
                        reason: Some("cancel-all".to_string()),
                        requested_at: command.requested_at,
                    })
                })
                .collect::<Vec<_>>();
            if cancel_commands.is_empty() {
                return Ok(CancelAllAck {
                    exchange: command.exchange,
                    canonical_symbol: command.canonical_symbol,
                    exchange_symbol: command.exchange_symbol,
                    accepted: true,
                    cancelled_orders: 0,
                    message: Some("open orders did not include cancellable ids".to_string()),
                    acknowledged_at: Utc::now(),
                });
            }

            let batch =
                CancelBatchCommand::new(self.exchange(), cancel_commands, command.requested_at);
            let value = self
                .transport
                .execute(self.protocol.cancel_batch_orders(&batch)?)
                .await?;
            let order_acks = parse_cancel_batch_acks(self.exchange(), &value, Utc::now());
            let cancelled_orders = if order_acks.is_empty() {
                cancelled_count(&value)
            } else {
                order_acks.iter().filter(|ack| ack.accepted).count()
            };
            return Ok(CancelAllAck {
                exchange: command.exchange,
                canonical_symbol: command.canonical_symbol,
                exchange_symbol: command.exchange_symbol,
                accepted: order_acks.iter().all(|ack| ack.accepted),
                cancelled_orders,
                message: response_string(&value, &["msg", "message"]),
                acknowledged_at: Utc::now(),
            });
        }

        let value = self
            .transport
            .execute(self.protocol.cancel_all_orders(&command)?)
            .await?;
        Ok(CancelAllAck {
            exchange: command.exchange,
            canonical_symbol: command.canonical_symbol,
            exchange_symbol: command.exchange_symbol,
            accepted: true,
            cancelled_orders: cancelled_count(&value),
            message: response_string(&value, &["msg", "message"]),
            acknowledged_at: Utc::now(),
        })
    }

    async fn cancel_batch_orders(&self, command: CancelBatchCommand) -> Result<CancelBatchAck> {
        if command.exchange != self.exchange() {
            anyhow::bail!(
                "{} batch cancel requires {} exchange symbols",
                self.exchange(),
                self.exchange()
            );
        }
        if command.orders.is_empty() {
            return Ok(CancelBatchAck {
                exchange: command.exchange,
                accepted: true,
                cancelled_orders: 0,
                order_acks: Vec::new(),
                message: Some("empty cancel batch".to_string()),
                acknowledged_at: Utc::now(),
            });
        }
        if command.orders.iter().any(|order| {
            order.exchange != self.exchange() || order.exchange_symbol.exchange != self.exchange()
        }) {
            anyhow::bail!(
                "{} batch cancel requires {} exchange symbols",
                self.exchange(),
                self.exchange()
            );
        }
        if self.exchange() == ExchangeId::Mexc
            && command
                .orders
                .iter()
                .any(|order| order.exchange_order_id.is_none())
        {
            if command
                .orders
                .iter()
                .any(|order| order.exchange_order_id.is_none() && order.client_order_id.is_none())
            {
                anyhow::bail!(
                    "mexc composed batch cancel requires orderId or externalOid for every order"
                );
            }
            let mut order_acks = Vec::new();
            for order in &command.orders {
                let value = self
                    .transport
                    .execute(self.protocol.cancel_order(order)?)
                    .await?;
                order_acks.push(CancelAck {
                    exchange: order.exchange.clone(),
                    client_order_id: order
                        .client_order_id
                        .clone()
                        .or_else(|| response_string(&value, &["externalOid", "clientOrderId"])),
                    exchange_order_id: order
                        .exchange_order_id
                        .clone()
                        .or_else(|| response_string(&value, &["orderId", "id"])),
                    accepted: true,
                    status: OrderCommandStatus::Cancelled,
                    message: response_string(&value, &["msg", "message"]),
                    acknowledged_at: Utc::now(),
                });
            }
            let cancelled_orders = order_acks.iter().filter(|ack| ack.accepted).count();
            return Ok(CancelBatchAck {
                exchange: command.exchange,
                accepted: order_acks.iter().all(|ack| ack.accepted),
                cancelled_orders,
                order_acks,
                message: Some("composed MEXC batch cancel via externalOid".to_string()),
                acknowledged_at: Utc::now(),
            });
        }
        let value = self
            .transport
            .execute(self.protocol.cancel_batch_orders(&command)?)
            .await?;
        let order_acks = parse_cancel_batch_acks(self.exchange(), &value, Utc::now());
        let cancelled_orders = if order_acks.is_empty() {
            cancelled_count(&value)
        } else {
            order_acks.iter().filter(|ack| ack.accepted).count()
        };
        Ok(CancelBatchAck {
            exchange: command.exchange,
            accepted: order_acks.iter().all(|ack| ack.accepted),
            cancelled_orders,
            order_acks,
            message: response_string(&value, &["msg", "message"]),
            acknowledged_at: Utc::now(),
        })
    }

    async fn get_order(&self, query: OrderQuery) -> Result<OrderState> {
        if query.exchange != self.exchange() || query.exchange_symbol.exchange != self.exchange() {
            anyhow::bail!(
                "{} query order requires {} exchange symbols",
                self.exchange(),
                self.exchange()
            );
        }

        let value = self
            .transport
            .execute(self.protocol.get_order(&query)?)
            .await?;
        let gate_contract_size = self.gate_contract_size_for_symbol(&query.exchange_symbol);
        parse_rest_order_with_gate_contract_size(
            self.exchange(),
            &query.exchange_symbol,
            &value,
            Utc::now(),
            gate_contract_size,
        )
        .ok_or_else(|| anyhow!("{} order response could not be normalized", self.exchange()))
    }

    async fn get_open_orders(&self, symbol: Option<&ExchangeSymbol>) -> Result<Vec<OrderState>> {
        if symbol.is_some_and(|symbol| symbol.exchange != self.exchange()) {
            anyhow::bail!(
                "{} open orders requires {} exchange symbols",
                self.exchange(),
                self.exchange()
            );
        }

        let value = self
            .transport
            .execute(self.protocol.get_open_orders(symbol)?)
            .await?;
        let exchange = self.exchange();
        let fallback_symbol = symbol.cloned();
        Ok(response_items(&value)
            .into_iter()
            .filter_map(|item| {
                let symbol = fallback_symbol.clone().unwrap_or_else(|| {
                    ExchangeSymbol::new(
                        exchange.clone(),
                        str_field(&item, &["symbol", "instId", "contract"]).unwrap_or_default(),
                    )
                });
                let gate_contract_size =
                    self.gate_contract_size_for_item(&item, fallback_symbol.as_ref());
                parse_rest_order_with_gate_contract_size(
                    exchange.clone(),
                    &symbol,
                    &item,
                    Utc::now(),
                    gate_contract_size,
                )
            })
            .collect())
    }

    async fn get_positions(
        &self,
        symbol: Option<&ExchangeSymbol>,
    ) -> Result<Vec<ExchangePosition>> {
        if symbol.is_some_and(|symbol| symbol.exchange != self.exchange()) {
            anyhow::bail!(
                "{} positions requires {} exchange symbols",
                self.exchange(),
                self.exchange()
            );
        }

        let value = match self
            .transport
            .execute(self.protocol.get_positions(symbol)?)
            .await
        {
            Ok(value) => value,
            Err(err) if is_gate_position_not_found(self.exchange(), &err) => {
                return Ok(Vec::new());
            }
            Err(err) => return Err(err),
        };
        let exchange = self.exchange();
        Ok(response_items(&value)
            .into_iter()
            .filter_map(|item| match exchange {
                ExchangeId::Binance => {
                    parse_binance_position(&item, Utc::now()).and_then(event_position)
                }
                ExchangeId::Okx => parse_okx_position(&item, Utc::now()).and_then(event_position),
                ExchangeId::Bitget => {
                    parse_bitget_position(&item, Utc::now()).and_then(event_position)
                }
                ExchangeId::Bybit => {
                    parse_bybit_position(&item, Utc::now()).and_then(event_position)
                }
                ExchangeId::Mexc => parse_mexc_position(&item, Utc::now()).and_then(event_position),
                ExchangeId::Htx => parse_htx_position(&item, Utc::now()).and_then(event_position),
                ExchangeId::Toobit => {
                    parse_toobit_position(&item, Utc::now()).and_then(event_position)
                }
                ExchangeId::Gate => {
                    let contract_size = self
                        .gate_contract_size_for_item(&item, symbol)
                        .unwrap_or_else(|| gate_contract_size_from_item(&item));
                    parse_gate_position_with_contract_size(&item, Utc::now(), contract_size)
                        .and_then(event_position)
                }
                _ => None,
            })
            .collect())
    }

    async fn get_balances(&self) -> Result<Vec<ExchangeBalance>> {
        let value = self
            .transport
            .execute(self.protocol.get_balances()?)
            .await?;
        let exchange = self.exchange();
        Ok(response_items(&value)
            .into_iter()
            .filter_map(|item| match exchange {
                ExchangeId::Binance => {
                    parse_binance_balance(&item, Utc::now()).and_then(event_balance)
                }
                ExchangeId::Okx => parse_okx_balance(&item, Utc::now()).and_then(event_balance),
                ExchangeId::Bitget => {
                    parse_bitget_balance(&item, Utc::now()).and_then(event_balance)
                }
                ExchangeId::Bybit => parse_bybit_balance(&item, Utc::now()).and_then(event_balance),
                ExchangeId::Mexc => parse_mexc_balance(&item, Utc::now()).and_then(event_balance),
                ExchangeId::Htx => parse_htx_balance(&item, Utc::now()).and_then(event_balance),
                ExchangeId::Toobit => {
                    parse_toobit_balance(&item, Utc::now()).and_then(event_balance)
                }
                ExchangeId::Gate => parse_gate_balance(&item, Utc::now()).and_then(event_balance),
                _ => None,
            })
            .collect())
    }

    async fn get_trade_fee(&self, symbol: &ExchangeSymbol) -> Result<TradeFeeSnapshot> {
        if symbol.exchange != self.exchange() {
            anyhow::bail!(
                "{} trade fee requires {} exchange symbols",
                self.exchange(),
                self.exchange()
            );
        }

        let value = self
            .transport
            .execute(self.protocol.get_trade_fee(symbol)?)
            .await?;
        parse_trade_fee_snapshot(self.exchange(), symbol, &value, Utc::now()).ok_or_else(|| {
            anyhow!(
                "{} trade fee response could not be normalized",
                self.exchange()
            )
        })
    }

    async fn get_symbol_account_config(
        &self,
        symbol: &ExchangeSymbol,
    ) -> Result<SymbolAccountConfig> {
        if symbol.exchange != self.exchange() {
            anyhow::bail!(
                "{} symbol account config requires {} exchange symbols",
                self.exchange(),
                self.exchange()
            );
        }

        let value = match self
            .transport
            .execute(self.protocol.get_symbol_account_config(symbol)?)
            .await
        {
            Ok(value) => value,
            Err(err) if is_gate_position_not_found(self.exchange(), &err) => {
                return Ok(default_symbol_account_config(
                    self.exchange(),
                    symbol,
                    Utc::now(),
                    self.current_position_mode()?,
                ));
            }
            Err(err) => return Err(err),
        };
        parse_symbol_account_config(
            self.exchange(),
            symbol,
            &value,
            Utc::now(),
            self.current_position_mode()?,
        )
        .ok_or_else(|| {
            anyhow!(
                "{} symbol account config could not be normalized",
                self.exchange()
            )
        })
    }

    async fn get_fills(&self, query: FillQuery) -> Result<Vec<FillEvent>> {
        if query.exchange != self.exchange()
            || query
                .exchange_symbol
                .as_ref()
                .is_some_and(|symbol| symbol.exchange != self.exchange())
        {
            anyhow::bail!(
                "{} fills requires {} exchange symbols",
                self.exchange(),
                self.exchange()
            );
        }

        let value = self
            .transport
            .execute(self.protocol.get_fills(&query)?)
            .await?;
        let exchange = self.exchange();
        Ok(response_items(&value)
            .into_iter()
            .filter_map(|item| match exchange {
                ExchangeId::Binance => parse_binance_fill(&item, Utc::now()).and_then(event_fill),
                ExchangeId::Okx => parse_okx_fill(&item, Utc::now()).and_then(event_fill),
                ExchangeId::Bitget => parse_bitget_fill(&item, Utc::now()).and_then(event_fill),
                ExchangeId::Bybit => parse_bybit_fill(&item, Utc::now()).and_then(event_fill),
                ExchangeId::Mexc => parse_mexc_fill(&item, Utc::now()).and_then(event_fill),
                ExchangeId::Htx => parse_htx_fill(&item, Utc::now()).and_then(event_fill),
                ExchangeId::Toobit => parse_toobit_fill(&item, Utc::now()).and_then(event_fill),
                ExchangeId::Gate => {
                    let contract_size = self
                        .gate_contract_size_for_item(&item, query.exchange_symbol.as_ref())
                        .unwrap_or_else(|| gate_contract_size_from_item(&item));
                    parse_gate_fill_with_contract_size(&item, Utc::now(), contract_size)
                        .and_then(event_fill)
                }
                _ => None,
            })
            .filter(|fill| {
                query.client_order_id.as_ref().is_none_or(|id| {
                    fill.client_order_id.as_deref() == Some(id.as_str())
                        || (self.exchange() == ExchangeId::Gate
                            && fill.client_order_id.as_deref()
                                == Some(gate_client_text(id).as_str()))
                })
            })
            .collect())
    }

    async fn amend_order(&self, command: AmendOrderCommand) -> Result<AmendOrderAck> {
        if command.exchange != self.exchange()
            || command.exchange_symbol.exchange != self.exchange()
        {
            anyhow::bail!(
                "{} amend requires {} exchange symbols",
                self.exchange(),
                self.exchange()
            );
        }
        let command = self.normalize_amend_command(command)?;
        let value = self
            .transport
            .execute(self.protocol.amend_order(&command)?)
            .await?;
        Ok(AmendOrderAck {
            exchange: command.exchange,
            client_order_id: command
                .new_client_order_id
                .or(command.client_order_id)
                .or_else(|| response_string(&value, &["clientOid", "clOrdId", "text"])),
            exchange_order_id: command
                .exchange_order_id
                .or_else(|| response_string(&value, &["orderId", "ordId", "id"])),
            accepted: true,
            status: OrderCommandStatus::Accepted,
            message: response_string(&value, &["msg", "message"]),
            acknowledged_at: Utc::now(),
        })
    }

    async fn set_leverage(&self, command: LeverageCommand) -> Result<LeverageAck> {
        if command.exchange != self.exchange()
            || command.exchange_symbol.exchange != self.exchange()
        {
            anyhow::bail!(
                "{} set leverage requires {} exchange symbols",
                self.exchange(),
                self.exchange()
            );
        }
        ensure_capability(
            self.exchange(),
            self.capabilities().supports_leverage,
            "set leverage",
        )?;
        if command.leverage == 0 {
            anyhow::bail!("{} leverage must be positive", self.exchange);
        }
        let value = self
            .transport
            .execute(self.protocol.set_leverage(&command)?)
            .await?;
        Ok(LeverageAck {
            exchange: command.exchange,
            canonical_symbol: command.canonical_symbol,
            exchange_symbol: command.exchange_symbol,
            leverage: command.leverage,
            accepted: true,
            message: response_string(&value, &["msg", "message"]),
            acknowledged_at: Utc::now(),
        })
    }

    async fn set_position_mode(&self, command: PositionModeCommand) -> Result<PositionModeAck> {
        if command.exchange != self.exchange() {
            anyhow::bail!(
                "{} position mode requires {} exchange",
                self.exchange(),
                self.exchange()
            );
        }
        ensure_capability(
            self.exchange(),
            self.capabilities().supports_position_mode_change,
            "set position mode",
        )?;
        let value = self
            .transport
            .execute(self.protocol.set_position_mode(&command)?)
            .await?;
        self.set_local_position_mode(command.mode)?;
        Ok(PositionModeAck {
            exchange: command.exchange,
            mode: command.mode,
            accepted: true,
            message: response_string(&value, &["msg", "message"]),
            acknowledged_at: Utc::now(),
        })
    }

    async fn set_countdown_cancel_all(
        &self,
        command: CountdownCancelAllCommand,
    ) -> Result<CountdownCancelAllAck> {
        ensure_countdown_cancel_all_scope(&self.exchange(), &command)?;
        ensure_capability(
            self.exchange(),
            self.capabilities().supports_countdown_cancel_all,
            "set countdown cancel-all",
        )?;
        let value = self
            .transport
            .execute(self.protocol.set_countdown_cancel_all(&command)?)
            .await?;
        Ok(CountdownCancelAllAck {
            exchange: command.exchange,
            exchange_symbol: command.exchange_symbol,
            timeout_secs: command.timeout_secs,
            trigger_time: parse_countdown_trigger_time(&value),
            accepted: true,
            message: response_string(&value, &["retMsg", "msg", "message"]),
            acknowledged_at: Utc::now(),
        })
    }

    async fn close_position(&self, command: ClosePositionCommand) -> Result<ClosePositionAck> {
        ensure_close_position_exchange_scope(&self.exchange(), &command)?;
        ensure_capability(
            self.exchange(),
            self.capabilities().supports_close_position,
            "close position",
        )?;
        let command = self.normalize_close_command(command)?;
        let protocol_command = self.close_command_for_protocol(&command);
        let position_mode = self.current_position_mode()?;
        let value = self
            .transport
            .execute(close_position_spec(
                self.exchange(),
                &protocol_command,
                position_mode,
            )?)
            .await?;
        let order_id = response_string(
            &value,
            &["order_id_str", "order_id", "orderId", "ordId", "id"],
        )
        .or_else(|| response_string(&value, &["clientOid", "clOrdId", "text"]))
        .unwrap_or_else(|| command.client_order_id.clone());
        Ok(ClosePositionAck {
            exchange: command.exchange,
            client_order_id: command.client_order_id,
            exchange_order_id: Some(order_id),
            accepted: true,
            status: OrderCommandStatus::Accepted,
            message: response_string(&value, &["msg", "message"]),
            acknowledged_at: Utc::now(),
        })
    }

    async fn load_symbol_rules(&self, symbol: &ExchangeSymbol) -> Result<Option<InstrumentMeta>> {
        if symbol.exchange != self.exchange {
            return Ok(None);
        }
        Ok(
            canonical_from_exchange_symbol(&symbol.exchange, &symbol.symbol)
                .and_then(|canonical| self.instruments.get(&canonical).cloned()),
        )
    }
}

pub fn private_perp_trading_capabilities(exchange: ExchangeId) -> TradingCapabilities {
    TradingCapabilities {
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: true,
        supports_ioc: true,
        supports_fok: true,
        supports_reduce_only: true,
        supports_hedge_mode: matches!(
            exchange,
            ExchangeId::Binance
                | ExchangeId::Okx
                | ExchangeId::Bitget
                | ExchangeId::Gate
                | ExchangeId::Bybit
                | ExchangeId::Mexc
                | ExchangeId::Htx
                | ExchangeId::Toobit
        ),
        supports_client_order_id: true,
        supports_leverage: true,
        supports_position_mode_change: matches!(
            exchange,
            ExchangeId::Binance
                | ExchangeId::Okx
                | ExchangeId::Bitget
                | ExchangeId::Bybit
                | ExchangeId::Mexc
                | ExchangeId::Htx
        ),
        supports_close_position: true,
        supports_countdown_cancel_all: matches!(
            exchange,
            ExchangeId::Binance
                | ExchangeId::Okx
                | ExchangeId::Bitget
                | ExchangeId::Gate
                | ExchangeId::Bybit
        ),
        supports_batch_place_orders: matches!(
            exchange,
            ExchangeId::Binance
                | ExchangeId::Okx
                | ExchangeId::Bitget
                | ExchangeId::Gate
                | ExchangeId::Bybit
                | ExchangeId::Mexc
                | ExchangeId::Htx
                | ExchangeId::Toobit
        ),
    }
}

fn ensure_capability(exchange: ExchangeId, supported: bool, action: &str) -> Result<()> {
    if supported {
        Ok(())
    } else {
        anyhow::bail!("{exchange} private perp adapter does not support {action}")
    }
}

fn ensure_order_supported(
    exchange: ExchangeId,
    command: &OrderCommand,
    capabilities: TradingCapabilities,
) -> Result<()> {
    if command.exchange != exchange || command.exchange_symbol.exchange != exchange {
        anyhow::bail!("{exchange} place order requires {exchange} exchange symbols");
    }
    match command.order_type {
        OrderType::Market => ensure_capability(
            exchange.clone(),
            capabilities.supports_market_orders,
            "market orders",
        )?,
        OrderType::Limit => ensure_capability(
            exchange.clone(),
            capabilities.supports_limit_orders,
            "limit orders",
        )?,
    }
    if command.post_only {
        ensure_capability(
            exchange.clone(),
            capabilities.supports_post_only,
            "post-only orders",
        )?;
    }
    match command.time_in_force {
        TimeInForce::Ioc => {
            ensure_capability(exchange.clone(), capabilities.supports_ioc, "IOC orders")?;
        }
        TimeInForce::Fok => {
            ensure_capability(exchange.clone(), capabilities.supports_fok, "FOK orders")?;
        }
        TimeInForce::Gtc | TimeInForce::PostOnly => {}
    }
    if command.reduce_only {
        ensure_capability(
            exchange.clone(),
            capabilities.supports_reduce_only,
            "reduce-only orders",
        )?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrivateWsRunConfig {
    pub connect_timeout_ms: u64,
    pub reconnect_delay_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub subscribe_interval_ms: u64,
    pub stale_after_ms: u64,
}

impl Default for PrivateWsRunConfig {
    fn default() -> Self {
        Self {
            connect_timeout_ms: 10_000,
            reconnect_delay_ms: 2_000,
            heartbeat_interval_ms: 15_000,
            subscribe_interval_ms: 50,
            stale_after_ms: 45_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrivateWsEndpoint {
    pub exchange: ExchangeId,
    pub url: String,
    pub login_message: Option<String>,
    pub subscribe_messages: Vec<String>,
    pub send_interval_ms: u64,
}

const BINANCE_FUTURES_LISTEN_KEY_KEEPALIVE_MS: u64 = 25 * 60 * 1000;

pub fn bitget_private_trading_adapter(auth: PrivateRestAuth) -> Result<Arc<dyn TradingAdapter>> {
    private_perp_trading_adapter_for(PrivatePerpExchange::Bitget, auth, PositionMode::OneWay)
}

pub fn gate_private_trading_adapter(auth: PrivateRestAuth) -> Result<Arc<dyn TradingAdapter>> {
    private_perp_trading_adapter_for(PrivatePerpExchange::Gate, auth, PositionMode::OneWay)
}

pub fn private_perp_trading_adapter_for(
    exchange: PrivatePerpExchange,
    auth: PrivateRestAuth,
    position_mode: PositionMode,
) -> Result<Arc<dyn TradingAdapter>> {
    private_perp_trading_adapter_for_with_instruments(exchange, auth, position_mode, Vec::new())
}

pub fn private_perp_trading_adapter_for_with_instruments(
    exchange: PrivatePerpExchange,
    auth: PrivateRestAuth,
    position_mode: PositionMode,
    instruments: impl IntoIterator<Item = InstrumentMeta>,
) -> Result<Arc<dyn TradingAdapter>> {
    match exchange {
        PrivatePerpExchange::Binance => Ok(Arc::new(
            PrivatePerpTradingAdapter::new(
                BinancePrivatePerpProtocol,
                ReqwestPrivateRestTransport::new(exchange, auth)?,
            )
            .with_position_mode(position_mode)
            .with_instruments(instruments),
        )),
        PrivatePerpExchange::Okx => Ok(Arc::new(
            PrivatePerpTradingAdapter::new(
                OkxPrivatePerpProtocol,
                ReqwestPrivateRestTransport::new(exchange, auth)?,
            )
            .with_position_mode(position_mode)
            .with_instruments(instruments),
        )),
        PrivatePerpExchange::CoinEx => Ok(Arc::new(
            PrivatePerpTradingAdapter::new(
                CoinExPrivatePerpProtocol,
                ReqwestPrivateRestTransport::new(exchange, auth)?,
            )
            .with_position_mode(position_mode)
            .with_instruments(instruments),
        )),
        PrivatePerpExchange::Bitget => Ok(Arc::new(
            PrivatePerpTradingAdapter::new(
                BitgetPrivatePerpProtocol,
                ReqwestPrivateRestTransport::new(exchange, auth)?,
            )
            .with_position_mode(position_mode)
            .with_instruments(instruments),
        )),
        PrivatePerpExchange::Gate => Ok(Arc::new(
            PrivatePerpTradingAdapter::new(
                GatePrivatePerpProtocol,
                ReqwestPrivateRestTransport::new(exchange, auth)?,
            )
            .with_position_mode(position_mode)
            .with_instruments(instruments),
        )),
        PrivatePerpExchange::Bybit => Ok(Arc::new(
            PrivatePerpTradingAdapter::new(
                BybitPrivatePerpProtocol,
                ReqwestPrivateRestTransport::new(exchange, auth)?,
            )
            .with_position_mode(position_mode)
            .with_instruments(instruments),
        )),
        PrivatePerpExchange::Mexc => Ok(Arc::new(
            PrivatePerpTradingAdapter::new(
                MexcPrivatePerpProtocol,
                ReqwestPrivateRestTransport::new(exchange, auth)?,
            )
            .with_position_mode(position_mode)
            .with_instruments(instruments),
        )),
        PrivatePerpExchange::Htx => Ok(Arc::new(
            PrivatePerpTradingAdapter::new(
                HtxPrivatePerpProtocol,
                ReqwestPrivateRestTransport::new(exchange, auth)?,
            )
            .with_position_mode(position_mode)
            .with_instruments(instruments),
        )),
        PrivatePerpExchange::Toobit => Ok(Arc::new(
            PrivatePerpTradingAdapter::new(
                ToobitPrivatePerpProtocol,
                ReqwestPrivateRestTransport::new(exchange, auth)?,
            )
            .with_position_mode(position_mode)
            .with_instruments(instruments),
        )),
    }
}

pub fn private_perp_trading_adapter_for_with_base_url_and_instruments(
    exchange: PrivatePerpExchange,
    auth: PrivateRestAuth,
    position_mode: PositionMode,
    base_url: Option<&str>,
    instruments: impl IntoIterator<Item = InstrumentMeta>,
) -> Result<Arc<dyn TradingAdapter>> {
    let base_url = base_url
        .map(str::to_string)
        .unwrap_or_else(|| exchange.rest_base_url().to_string());
    match exchange {
        PrivatePerpExchange::Binance => Ok(Arc::new(
            PrivatePerpTradingAdapter::new(
                BinancePrivatePerpProtocol,
                ReqwestPrivateRestTransport::with_base_url(exchange, auth, base_url)?,
            )
            .with_position_mode(position_mode)
            .with_instruments(instruments),
        )),
        PrivatePerpExchange::Okx => Ok(Arc::new(
            PrivatePerpTradingAdapter::new(
                OkxPrivatePerpProtocol,
                ReqwestPrivateRestTransport::with_base_url(exchange, auth, base_url)?,
            )
            .with_position_mode(position_mode)
            .with_instruments(instruments),
        )),
        PrivatePerpExchange::CoinEx => Ok(Arc::new(
            PrivatePerpTradingAdapter::new(
                CoinExPrivatePerpProtocol,
                ReqwestPrivateRestTransport::with_base_url(exchange, auth, base_url)?,
            )
            .with_position_mode(position_mode)
            .with_instruments(instruments),
        )),
        PrivatePerpExchange::Bitget => Ok(Arc::new(
            PrivatePerpTradingAdapter::new(
                BitgetPrivatePerpProtocol,
                ReqwestPrivateRestTransport::with_base_url(exchange, auth, base_url)?,
            )
            .with_position_mode(position_mode)
            .with_instruments(instruments),
        )),
        PrivatePerpExchange::Gate => Ok(Arc::new(
            PrivatePerpTradingAdapter::new(
                GatePrivatePerpProtocol,
                ReqwestPrivateRestTransport::with_base_url(exchange, auth, base_url)?,
            )
            .with_position_mode(position_mode)
            .with_instruments(instruments),
        )),
        PrivatePerpExchange::Bybit => Ok(Arc::new(
            PrivatePerpTradingAdapter::new(
                BybitPrivatePerpProtocol,
                ReqwestPrivateRestTransport::with_base_url(exchange, auth, base_url)?,
            )
            .with_position_mode(position_mode)
            .with_instruments(instruments),
        )),
        PrivatePerpExchange::Mexc => Ok(Arc::new(
            PrivatePerpTradingAdapter::new(
                MexcPrivatePerpProtocol,
                ReqwestPrivateRestTransport::with_base_url(exchange, auth, base_url)?,
            )
            .with_position_mode(position_mode)
            .with_instruments(instruments),
        )),
        PrivatePerpExchange::Htx => Ok(Arc::new(
            PrivatePerpTradingAdapter::new(
                HtxPrivatePerpProtocol,
                ReqwestPrivateRestTransport::with_base_url(exchange, auth, base_url)?,
            )
            .with_position_mode(position_mode)
            .with_instruments(instruments),
        )),
        PrivatePerpExchange::Toobit => Ok(Arc::new(
            PrivatePerpTradingAdapter::new(
                ToobitPrivatePerpProtocol,
                ReqwestPrivateRestTransport::with_base_url(exchange, auth, base_url)?,
            )
            .with_position_mode(position_mode)
            .with_instruments(instruments),
        )),
    }
}

pub fn build_private_ws_endpoint(
    exchange: PrivatePerpExchange,
    auth: PrivateWsAuth,
    symbols: &[ExchangeSymbol],
    timestamp: i64,
) -> Result<PrivateWsEndpoint> {
    build_private_ws_endpoint_with_url(
        exchange,
        auth,
        symbols,
        timestamp,
        exchange.private_ws_url(),
    )
}

pub fn build_private_ws_endpoint_with_url(
    exchange: PrivatePerpExchange,
    auth: PrivateWsAuth,
    symbols: &[ExchangeSymbol],
    timestamp: i64,
    url: impl Into<String>,
) -> Result<PrivateWsEndpoint> {
    let url = url.into();
    if url.trim().is_empty() {
        anyhow::bail!(
            "{} private websocket URL cannot be empty",
            exchange.exchange_id()
        );
    }
    match exchange {
        PrivatePerpExchange::Binance => build_binance_private_ws_endpoint(auth, url.trim()),
        PrivatePerpExchange::Okx => build_private_ws_endpoint_for(
            OkxPrivatePerpProtocol,
            auth,
            symbols,
            timestamp,
            url.trim(),
        ),
        PrivatePerpExchange::CoinEx => build_private_ws_endpoint_for(
            CoinExPrivatePerpProtocol,
            auth,
            symbols,
            timestamp,
            url.trim(),
        ),
        PrivatePerpExchange::Bitget => build_private_ws_endpoint_for(
            BitgetPrivatePerpProtocol,
            auth,
            symbols,
            timestamp,
            url.trim(),
        ),
        PrivatePerpExchange::Gate => build_private_ws_endpoint_for(
            GatePrivatePerpProtocol,
            auth,
            symbols,
            timestamp,
            url.trim(),
        ),
        PrivatePerpExchange::Bybit => build_private_ws_endpoint_for(
            BybitPrivatePerpProtocol,
            auth,
            symbols,
            timestamp,
            url.trim(),
        ),
        PrivatePerpExchange::Mexc => build_private_ws_endpoint_for(
            MexcPrivatePerpProtocol,
            auth,
            symbols,
            timestamp,
            url.trim(),
        ),
        PrivatePerpExchange::Htx => build_private_ws_endpoint_for(
            HtxPrivatePerpProtocol,
            auth,
            symbols,
            timestamp,
            url.trim(),
        ),
        PrivatePerpExchange::Toobit => build_toobit_private_ws_endpoint(auth, url.trim()),
    }
}

fn build_private_ws_endpoint_for<P>(
    protocol: P,
    auth: PrivateWsAuth,
    symbols: &[ExchangeSymbol],
    timestamp: i64,
    url: &str,
) -> Result<PrivateWsEndpoint>
where
    P: PrivatePerpProtocol,
{
    let exchange = protocol.exchange();
    let subscribe_messages = match exchange {
        PrivatePerpExchange::Binance => Vec::new(),
        PrivatePerpExchange::Okx => ["orders", "positions", "account"]
            .into_iter()
            .map(|channel| {
                let symbols = if matches!(channel, "positions" | "account") {
                    Vec::new()
                } else {
                    symbols.to_vec()
                };
                protocol
                    .ws_subscribe(
                        &PrivateWsSubscription {
                            channel: channel.to_string(),
                            symbols,
                            auth: None,
                        },
                        timestamp,
                    )
                    .map(|request| request.message)
            })
            .collect::<Result<Vec<_>>>()?,
        PrivatePerpExchange::CoinEx => [
            "order.subscribe",
            "user_deals.subscribe",
            "position.subscribe",
        ]
        .into_iter()
        .map(|channel| {
            protocol
                .ws_subscribe(
                    &PrivateWsSubscription {
                        channel: channel.to_string(),
                        symbols: symbols.to_vec(),
                        auth: None,
                    },
                    timestamp,
                )
                .map(|request| request.message)
        })
        .collect::<Result<Vec<_>>>()?,
        PrivatePerpExchange::Bitget => ["orders", "positions", "account"]
            .into_iter()
            .map(|channel| {
                let symbols = if channel == "account" {
                    Vec::new()
                } else {
                    vec![ExchangeSymbol::new(exchange.exchange_id(), "default")]
                };
                protocol
                    .ws_subscribe(
                        &PrivateWsSubscription {
                            channel: channel.to_string(),
                            symbols,
                            auth: None,
                        },
                        timestamp,
                    )
                    .map(|request| request.message)
            })
            .collect::<Result<Vec<_>>>()?,
        PrivatePerpExchange::Gate => {
            let mut messages = Vec::new();
            for channel in [
                "futures.orders",
                "futures.usertrades",
                "futures.positions",
                "futures.balances",
            ] {
                for symbols in gate_private_ws_symbol_groups(channel, symbols) {
                    messages.push(
                        protocol
                            .ws_subscribe(
                                &PrivateWsSubscription {
                                    channel: channel.to_string(),
                                    symbols,
                                    auth: Some(auth.clone()),
                                },
                                timestamp,
                            )?
                            .message,
                    );
                }
            }
            messages
        }
        PrivatePerpExchange::Bybit => ["order", "execution", "position", "wallet"]
            .into_iter()
            .map(|channel| {
                protocol
                    .ws_subscribe(
                        &PrivateWsSubscription {
                            channel: channel.to_string(),
                            symbols: Vec::new(),
                            auth: None,
                        },
                        timestamp,
                    )
                    .map(|request| request.message)
            })
            .collect::<Result<Vec<_>>>()?,
        PrivatePerpExchange::Mexc => ["order", "deal", "position", "asset"]
            .into_iter()
            .map(|channel| {
                protocol
                    .ws_subscribe(
                        &PrivateWsSubscription {
                            channel: channel.to_string(),
                            symbols: symbols.to_vec(),
                            auth: None,
                        },
                        timestamp,
                    )
                    .map(|request| request.message)
            })
            .collect::<Result<Vec<_>>>()?,
        PrivatePerpExchange::Htx => ["orders", "match_orders", "positions", "accounts"]
            .into_iter()
            .map(|channel| {
                protocol
                    .ws_subscribe(
                        &PrivateWsSubscription {
                            channel: channel.to_string(),
                            symbols: symbols.to_vec(),
                            auth: None,
                        },
                        timestamp,
                    )
                    .map(|request| request.message)
            })
            .collect::<Result<Vec<_>>>()?,
        PrivatePerpExchange::Toobit => Vec::new(),
    };

    let login_message = match exchange {
        PrivatePerpExchange::Binance => None,
        PrivatePerpExchange::Okx | PrivatePerpExchange::CoinEx => {
            Some(protocol.ws_login(&auth, timestamp)?.message)
        }
        PrivatePerpExchange::Bitget
        | PrivatePerpExchange::Bybit
        | PrivatePerpExchange::Mexc
        | PrivatePerpExchange::Htx => Some(protocol.ws_login(&auth, timestamp)?.message),
        PrivatePerpExchange::Toobit => None,
        PrivatePerpExchange::Gate => None,
    };

    Ok(PrivateWsEndpoint {
        exchange: exchange.exchange_id(),
        url: url.to_string(),
        login_message,
        subscribe_messages,
        send_interval_ms: PrivateWsRunConfig::default().subscribe_interval_ms,
    })
}

fn build_binance_private_ws_endpoint(
    auth: PrivateWsAuth,
    base_url: &str,
) -> Result<PrivateWsEndpoint> {
    let listen_key = auth.account_id.as_deref().ok_or_else(|| {
        anyhow!(
            "binance private websocket endpoint builder requires a listenKey in PrivateWsAuth.account_id; live runtime should use the Binance listenKey manager"
        )
    })?;
    let base_url = base_url.trim_end_matches('/');
    let url = if base_url.ends_with("/ws") || base_url.ends_with("/private/ws") {
        format!("{base_url}/{listen_key}")
    } else {
        format!("{base_url}/ws/{listen_key}")
    };
    Ok(PrivateWsEndpoint {
        exchange: ExchangeId::Binance,
        url,
        login_message: None,
        subscribe_messages: Vec::new(),
        send_interval_ms: PrivateWsRunConfig::default().subscribe_interval_ms,
    })
}

fn build_toobit_private_ws_endpoint(
    auth: PrivateWsAuth,
    base_url: &str,
) -> Result<PrivateWsEndpoint> {
    let listen_key = auth.account_id.as_deref().ok_or_else(|| {
        anyhow!(
            "toobit private websocket endpoint builder requires a listenKey in PrivateWsAuth.account_id"
        )
    })?;
    let base_url = base_url
        .trim_end_matches('/')
        .strip_suffix("/quote/ws/v1")
        .unwrap_or_else(|| base_url.trim_end_matches('/'));
    Ok(PrivateWsEndpoint {
        exchange: ExchangeId::Toobit,
        url: format!("{base_url}/api/v1/ws/{listen_key}"),
        login_message: None,
        subscribe_messages: Vec::new(),
        send_interval_ms: PrivateWsRunConfig::default().subscribe_interval_ms,
    })
}

pub async fn run_private_ws(
    exchange: PrivatePerpExchange,
    auth: PrivateWsAuth,
    symbols: Vec<ExchangeSymbol>,
    config: PrivateWsRunConfig,
    tx: mpsc::Sender<PrivateEvent>,
) -> Result<()> {
    run_private_ws_with_url(
        exchange,
        auth,
        symbols,
        config,
        exchange.private_ws_url().to_string(),
        tx,
    )
    .await
}

pub async fn run_private_ws_with_url(
    exchange: PrivatePerpExchange,
    auth: PrivateWsAuth,
    symbols: Vec<ExchangeSymbol>,
    config: PrivateWsRunConfig,
    url: impl Into<String>,
    tx: mpsc::Sender<PrivateEvent>,
) -> Result<()> {
    run_private_ws_with_url_and_gate_contract_sizes(
        exchange,
        auth,
        symbols,
        config,
        url,
        tx,
        HashMap::new(),
    )
    .await
}

pub async fn run_private_ws_with_url_and_instruments(
    exchange: PrivatePerpExchange,
    auth: PrivateWsAuth,
    symbols: Vec<ExchangeSymbol>,
    config: PrivateWsRunConfig,
    url: impl Into<String>,
    tx: mpsc::Sender<PrivateEvent>,
    instruments: Vec<InstrumentMeta>,
) -> Result<()> {
    let gate_contract_sizes = if exchange == PrivatePerpExchange::Gate {
        gate_contract_sizes_from_instruments(instruments)
    } else {
        HashMap::new()
    };
    run_private_ws_with_url_and_gate_contract_sizes(
        exchange,
        auth,
        symbols,
        config,
        url,
        tx,
        gate_contract_sizes,
    )
    .await
}

async fn run_private_ws_with_url_and_gate_contract_sizes(
    exchange: PrivatePerpExchange,
    auth: PrivateWsAuth,
    symbols: Vec<ExchangeSymbol>,
    config: PrivateWsRunConfig,
    url: impl Into<String>,
    tx: mpsc::Sender<PrivateEvent>,
    gate_contract_sizes: HashMap<String, f64>,
) -> Result<()> {
    let url = url.into();
    if url.trim().is_empty() {
        anyhow::bail!(
            "{} private websocket URL cannot be empty",
            exchange.exchange_id()
        );
    }
    let url = url.trim().to_string();
    loop {
        if tx.is_closed() {
            return Ok(());
        }

        let result = match exchange {
            PrivatePerpExchange::Binance => {
                run_binance_private_ws_with_url(
                    auth.clone(),
                    symbols.clone(),
                    config,
                    url.clone(),
                    tx.clone(),
                )
                .await
            }
            PrivatePerpExchange::Bitget => {
                run_private_ws_protocol_with_url(
                    BitgetPrivatePerpProtocol,
                    auth.clone(),
                    symbols.clone(),
                    config,
                    url.clone(),
                    tx.clone(),
                    HashMap::new(),
                )
                .await
            }
            PrivatePerpExchange::Okx => {
                run_private_ws_protocol_with_url(
                    OkxPrivatePerpProtocol,
                    auth.clone(),
                    symbols.clone(),
                    config,
                    url.clone(),
                    tx.clone(),
                    HashMap::new(),
                )
                .await
            }
            PrivatePerpExchange::CoinEx => {
                run_private_ws_protocol_with_url(
                    CoinExPrivatePerpProtocol,
                    auth.clone(),
                    symbols.clone(),
                    config,
                    url.clone(),
                    tx.clone(),
                    HashMap::new(),
                )
                .await
            }
            PrivatePerpExchange::Gate => {
                run_private_ws_protocol_with_url(
                    GatePrivatePerpProtocol,
                    auth.clone(),
                    symbols.clone(),
                    config,
                    url.clone(),
                    tx.clone(),
                    gate_contract_sizes.clone(),
                )
                .await
            }
            PrivatePerpExchange::Bybit => {
                run_private_ws_protocol_with_url(
                    BybitPrivatePerpProtocol,
                    auth.clone(),
                    symbols.clone(),
                    config,
                    url.clone(),
                    tx.clone(),
                    HashMap::new(),
                )
                .await
            }
            PrivatePerpExchange::Mexc => {
                run_private_ws_protocol_with_url(
                    MexcPrivatePerpProtocol,
                    auth.clone(),
                    symbols.clone(),
                    config,
                    url.clone(),
                    tx.clone(),
                    HashMap::new(),
                )
                .await
            }
            PrivatePerpExchange::Htx => {
                run_private_ws_protocol_with_url(
                    HtxPrivatePerpProtocol,
                    auth.clone(),
                    symbols.clone(),
                    config,
                    url.clone(),
                    tx.clone(),
                    HashMap::new(),
                )
                .await
            }
            PrivatePerpExchange::Toobit => {
                run_private_ws_protocol_with_url(
                    ToobitPrivatePerpProtocol,
                    auth.clone(),
                    symbols.clone(),
                    config,
                    url.clone(),
                    tx.clone(),
                    HashMap::new(),
                )
                .await
            }
        };

        if tx.is_closed() {
            return Ok(());
        }
        if let Err(err) = result {
            emit_private_ws_error(
                &tx,
                exchange.exchange_id(),
                ExchangeErrorClass::Network,
                Some(url.clone()),
                err.to_string(),
            )
            .await;
            emit_private_ws_disconnected(&tx, exchange.exchange_id(), Some(err.to_string())).await;
        }
        sleep(TokioDuration::from_millis(config.reconnect_delay_ms)).await;
    }
}

pub async fn run_private_ws_protocol<P>(
    protocol: P,
    auth: PrivateWsAuth,
    symbols: Vec<ExchangeSymbol>,
    config: PrivateWsRunConfig,
    tx: mpsc::Sender<PrivateEvent>,
) -> Result<()>
where
    P: PrivatePerpProtocol + Send + Sync + Copy + 'static,
{
    run_private_ws_protocol_with_url(
        protocol,
        auth,
        symbols,
        config,
        protocol.exchange().private_ws_url().to_string(),
        tx,
        HashMap::new(),
    )
    .await
}

async fn run_binance_private_ws_with_url(
    mut auth: PrivateWsAuth,
    symbols: Vec<ExchangeSymbol>,
    config: PrivateWsRunConfig,
    url: impl Into<String>,
    tx: mpsc::Sender<PrivateEvent>,
) -> Result<()> {
    let listen_key = match auth.account_id.clone() {
        Some(listen_key) if !listen_key.trim().is_empty() => listen_key,
        _ => binance_create_futures_listen_key(&auth).await?,
    };
    auth.account_id = Some(listen_key.clone());
    let keepalive = spawn_binance_listen_key_keepalive(auth.clone(), listen_key, tx.clone());
    let result = run_private_ws_protocol_with_url(
        BinancePrivatePerpProtocol,
        auth,
        symbols,
        config,
        url,
        tx,
        HashMap::new(),
    )
    .await;
    keepalive.abort();
    result
}

pub async fn run_private_ws_protocol_with_url<P>(
    protocol: P,
    auth: PrivateWsAuth,
    symbols: Vec<ExchangeSymbol>,
    config: PrivateWsRunConfig,
    url: impl Into<String>,
    tx: mpsc::Sender<PrivateEvent>,
    gate_contract_sizes_override: HashMap<String, f64>,
) -> Result<()>
where
    P: PrivatePerpProtocol + Send + Sync + Copy + 'static,
{
    let exchange = protocol.exchange();
    let gate_contract_sizes = if !gate_contract_sizes_override.is_empty() {
        gate_contract_sizes_override
    } else if exchange == PrivatePerpExchange::Gate {
        gate_contract_sizes_from_symbols(&symbols)
    } else {
        HashMap::new()
    };
    let timestamp = Utc::now().timestamp();
    let mut endpoint =
        build_private_ws_endpoint_with_url(exchange, auth, &symbols, timestamp, url)?;
    endpoint.send_interval_ms = config.subscribe_interval_ms;

    let connect = timeout(
        TokioDuration::from_millis(config.connect_timeout_ms),
        connect_async(&endpoint.url),
    )
    .await
    .map_err(|_| anyhow!("{} private websocket connect timed out", endpoint.exchange))??;
    let (ws, _) = connect;
    let (mut write, mut read) = ws.split();

    if let Some(login_message) = endpoint.login_message {
        write.send(Message::Text(login_message)).await?;
        sleep(TokioDuration::from_millis(endpoint.send_interval_ms)).await;
    }
    for message in endpoint.subscribe_messages {
        write.send(Message::Text(message)).await?;
        sleep(TokioDuration::from_millis(endpoint.send_interval_ms)).await;
    }

    let mut heartbeat = tokio::time::interval(TokioDuration::from_millis(
        config.heartbeat_interval_ms.max(1_000),
    ));
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                match exchange {
                    PrivatePerpExchange::Binance => write.send(Message::Ping(Vec::new())).await?,
                    PrivatePerpExchange::Bitget => write.send(Message::Text("ping".to_string())).await?,
                    PrivatePerpExchange::Okx | PrivatePerpExchange::CoinEx | PrivatePerpExchange::Gate | PrivatePerpExchange::Bybit | PrivatePerpExchange::Mexc | PrivatePerpExchange::Toobit => write.send(Message::Ping(Vec::new())).await?,
                    PrivatePerpExchange::Htx => write.send(Message::Text(json!({"ping": Utc::now().timestamp_millis()}).to_string())).await?,
                }
                if !publish_private_event(
                    &tx,
                    PrivateEvent::new(endpoint.exchange.clone(), PrivateEventKind::Heartbeat, Utc::now()),
                ).await {
                    return Ok(());
                }
            }
            message = read.next() => {
                match message {
                    Some(Ok(Message::Text(raw))) => {
                        let received_at = Utc::now();
                        match parse_private_ws_message_with_symbol_rules(
                            protocol,
                            &gate_contract_sizes,
                            &raw,
                            received_at,
                        ) {
                            Ok(events) => {
                                for event in events {
                                    if !publish_private_event(&tx, event).await {
                                        return Ok(());
                                    }
                                }
                            }
                            Err(err) => {
                                emit_private_ws_error(
                                    &tx,
                                    endpoint.exchange.clone(),
                                    ExchangeErrorClass::Decode,
                                    Some(endpoint.url.clone()),
                                    err.to_string(),
                                ).await;
                            }
                        }
                    }
                    Some(Ok(Message::Binary(raw))) => {
                        match decode_private_ws_binary(exchange, &raw) {
                            Ok(text) => {
                                let received_at = Utc::now();
                                match parse_private_ws_message_with_symbol_rules(
                                    protocol,
                                    &gate_contract_sizes,
                                    &text,
                                    received_at,
                                ) {
                                    Ok(events) => {
                                        for event in events {
                                            if !publish_private_event(&tx, event).await {
                                                return Ok(());
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        emit_private_ws_error(
                                            &tx,
                                            endpoint.exchange.clone(),
                                            ExchangeErrorClass::Decode,
                                            Some(endpoint.url.clone()),
                                            err.to_string(),
                                        ).await;
                                    }
                                }
                            }
                            Err(err) => {
                                emit_private_ws_error(
                                    &tx,
                                    endpoint.exchange.clone(),
                                    ExchangeErrorClass::Decode,
                                    Some(endpoint.url.clone()),
                                    err.to_string(),
                                ).await;
                            }
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        write.send(Message::Pong(payload)).await?;
                    }
                    Some(Ok(Message::Pong(_))) => {
                        if !publish_private_event(
                            &tx,
                            PrivateEvent::new(endpoint.exchange.clone(), PrivateEventKind::Heartbeat, Utc::now()),
                        ).await {
                            return Ok(());
                        }
                    }
                    Some(Ok(Message::Close(frame))) => {
                        let reason = frame.map(|frame| frame.reason.to_string());
                        emit_private_ws_disconnected(&tx, endpoint.exchange.clone(), reason).await;
                        return Ok(());
                    }
                    Some(Err(err)) => {
                        emit_private_ws_disconnected(&tx, endpoint.exchange.clone(), Some(err.to_string())).await;
                        return Err(err.into());
                    }
                    None => {
                        emit_private_ws_disconnected(
                            &tx,
                            endpoint.exchange.clone(),
                            Some("private websocket stream ended".to_string()),
                        ).await;
                        return Ok(());
                    }
                    _ => {}
                }
            }
        }
    }
}

async fn publish_private_event(tx: &mpsc::Sender<PrivateEvent>, event: PrivateEvent) -> bool {
    tx.send(event).await.is_ok()
}

fn spawn_binance_listen_key_keepalive(
    auth: PrivateWsAuth,
    listen_key: String,
    tx: mpsc::Sender<PrivateEvent>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            sleep(TokioDuration::from_millis(
                BINANCE_FUTURES_LISTEN_KEY_KEEPALIVE_MS,
            ))
            .await;
            if tx.is_closed() {
                return;
            }
            if let Err(err) = binance_keepalive_futures_listen_key(&auth, &listen_key).await {
                emit_private_ws_error(
                    &tx,
                    ExchangeId::Binance,
                    ExchangeErrorClass::Network,
                    Some("/fapi/v1/listenKey".to_string()),
                    format!("binance futures listenKey keepalive failed: {err}"),
                )
                .await;
            }
        }
    })
}

async fn binance_create_futures_listen_key(auth: &PrivateWsAuth) -> Result<String> {
    let value = binance_futures_listen_key_request(reqwest::Method::POST, auth, None).await?;
    str_field(&value, &["listenKey"])
        .ok_or_else(|| anyhow!("binance futures listenKey response missing listenKey: {value}"))
}

async fn binance_keepalive_futures_listen_key(
    auth: &PrivateWsAuth,
    listen_key: &str,
) -> Result<()> {
    let _ =
        binance_futures_listen_key_request(reqwest::Method::PUT, auth, Some(listen_key)).await?;
    Ok(())
}

async fn binance_futures_listen_key_request(
    method: reqwest::Method,
    auth: &PrivateWsAuth,
    listen_key: Option<&str>,
) -> Result<Value> {
    let client = crate::core::http2_fix::shared_http_client();
    let url =
        binance_futures_listen_key_url(PrivatePerpExchange::Binance.rest_base_url(), listen_key);
    let response = client
        .request(method, &url)
        .header("X-MBX-APIKEY", &auth.api_key)
        .send()
        .await?;
    let status = response.status();
    let raw = response.text().await?;
    let value = if raw.trim().is_empty() {
        Value::Null
    } else {
        serde_json::from_str::<Value>(&raw).map_err(|err| {
            anyhow!("binance futures listenKey JSON decode failed: {err}; raw={raw}")
        })?
    };
    if !status.is_success() {
        return Err(PrivateRestError {
            exchange: ExchangeId::Binance,
            class: classify_generic_rest_error(
                str_field(&value, &["code"]).as_deref().unwrap_or_default(),
                str_field(&value, &["msg", "message"])
                    .as_deref()
                    .unwrap_or("binance futures listenKey request failed"),
            ),
            endpoint: Some("/fapi/v1/listenKey".to_string()),
            code: str_field(&value, &["code"]),
            message: str_field(&value, &["msg", "message"])
                .unwrap_or_else(|| format!("HTTP {}", status.as_u16())),
        }
        .into());
    }
    normalize_private_rest_response(ExchangeId::Binance, value)
}

fn binance_futures_listen_key_url(base_url: &str, listen_key: Option<&str>) -> String {
    let url = format!("{}/fapi/v1/listenKey", base_url.trim_end_matches('/'));
    match listen_key {
        Some(listen_key) if !listen_key.is_empty() => {
            format!("{url}?listenKey={}", urlencoding::encode(listen_key))
        }
        _ => url,
    }
}

async fn emit_private_ws_error(
    tx: &mpsc::Sender<PrivateEvent>,
    exchange: ExchangeId,
    class: ExchangeErrorClass,
    endpoint: Option<String>,
    message: String,
) {
    let now = Utc::now();
    let _ = tx
        .send(PrivateEvent::new(
            exchange,
            PrivateEventKind::Error(PrivateErrorEvent {
                class,
                endpoint,
                code: None,
                message,
                client_order_id: None,
                exchange_order_id: None,
                retry_after_ms: None,
                occurred_at: now,
            }),
            now,
        ))
        .await;
}

async fn emit_private_ws_disconnected(
    tx: &mpsc::Sender<PrivateEvent>,
    exchange: ExchangeId,
    reason: Option<String>,
) {
    let now = Utc::now();
    let _ = tx
        .send(PrivateEvent::new(
            exchange,
            PrivateEventKind::StreamDisconnected {
                reason,
                disconnected_at: now,
            },
            now,
        ))
        .await;
}

fn decode_private_ws_binary(exchange: PrivatePerpExchange, raw: &[u8]) -> Result<String> {
    if exchange == PrivatePerpExchange::Htx {
        let mut decoder = GzDecoder::new(raw);
        let mut text = String::new();
        decoder.read_to_string(&mut text)?;
        return Ok(text);
    }
    String::from_utf8(raw.to_vec()).map_err(Into::into)
}

#[derive(Debug, Clone, Copy, Default)]
pub struct BinancePrivatePerpProtocol;

#[derive(Debug, Clone, Copy, Default)]
pub struct OkxPrivatePerpProtocol;

#[derive(Debug, Clone, Copy, Default)]
pub struct CoinExPrivatePerpProtocol;

#[derive(Debug, Clone, Copy, Default)]
pub struct BitgetPrivatePerpProtocol;

#[derive(Debug, Clone, Copy, Default)]
pub struct GatePrivatePerpProtocol;

#[derive(Debug, Clone, Copy, Default)]
pub struct BybitPrivatePerpProtocol;

#[derive(Debug, Clone, Copy, Default)]
pub struct MexcPrivatePerpProtocol;

#[derive(Debug, Clone, Copy, Default)]
pub struct HtxPrivatePerpProtocol;

#[derive(Debug, Clone, Copy, Default)]
pub struct ToobitPrivatePerpProtocol;

impl PrivatePerpProtocol for OkxPrivatePerpProtocol {
    fn exchange(&self) -> PrivatePerpExchange {
        PrivatePerpExchange::Okx
    }

    fn place_order(
        &self,
        command: &OrderCommand,
        position_mode: PositionMode,
    ) -> Result<PrivateRestRequestSpec> {
        ensure_okx_command_symbol(&command.exchange, &command.exchange_symbol, "place order")?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Okx,
            PrivateRestMethod::Post,
            "/api/v5/trade/order",
        )
        .with_body(okx_order_body(command, position_mode)))
    }

    fn place_batch_orders(
        &self,
        command: &BatchPlaceCommand,
        position_mode: PositionMode,
    ) -> Result<PrivateRestRequestSpec> {
        if command.orders.is_empty() {
            anyhow::bail!("okx batch-place requires at least one order");
        }
        if command.orders.len() > 20 {
            anyhow::bail!("okx batch-place supports at most 20 orders");
        }
        if command.exchange != ExchangeId::Okx {
            anyhow::bail!("okx batch-place requires okx exchange symbols");
        }
        if command.orders.iter().any(|order| {
            order.exchange != ExchangeId::Okx || order.exchange_symbol.exchange != ExchangeId::Okx
        }) {
            anyhow::bail!("okx batch-place requires okx exchange symbols");
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Okx,
            PrivateRestMethod::Post,
            "/api/v5/trade/batch-orders",
        )
        .with_body(Value::Array(
            command
                .orders
                .iter()
                .map(|order| okx_order_body(order, position_mode))
                .collect(),
        )))
    }

    fn cancel_order(&self, command: &CancelCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Okx
            || command.exchange_symbol.exchange != ExchangeId::Okx
        {
            anyhow::bail!("okx cancel requires okx exchange symbols");
        }
        let mut body = json!({
            "instId": command.exchange_symbol.symbol,
        });
        if let Some(order_id) = &command.exchange_order_id {
            set_str(&mut body, "ordId", order_id);
        }
        if let Some(client_order_id) = &command.client_order_id {
            set_str(&mut body, "clOrdId", client_order_id);
        }
        if command.exchange_order_id.is_none() && command.client_order_id.is_none() {
            anyhow::bail!("okx cancel requires ordId or clOrdId");
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Okx,
            PrivateRestMethod::Post,
            "/api/v5/trade/cancel-order",
        )
        .with_body(body))
    }

    fn cancel_all_orders(&self, _command: &CancelAllCommand) -> Result<PrivateRestRequestSpec> {
        anyhow::bail!(
            "okx private perp immediate cancel-all is not supported without first reading open order ids; use batch cancel or countdown cancel-all"
        )
    }

    fn cancel_batch_orders(&self, command: &CancelBatchCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Okx {
            anyhow::bail!("okx batch cancel requires okx exchange symbols");
        }
        if command.orders.is_empty() {
            anyhow::bail!("okx batch cancel requires at least one order");
        }
        if command.orders.len() > 20 {
            anyhow::bail!("okx batch cancel supports at most 20 orders");
        }
        for order in &command.orders {
            if order.exchange != ExchangeId::Okx
                || order.exchange_symbol.exchange != ExchangeId::Okx
            {
                anyhow::bail!("okx batch cancel requires okx exchange symbols");
            }
            if order.exchange_order_id.is_none() && order.client_order_id.is_none() {
                anyhow::bail!("okx batch cancel requires ordId or clOrdId for every order");
            }
        }
        let orders = command
            .orders
            .iter()
            .map(|order| {
                let mut item = json!({
                    "instId": order.exchange_symbol.symbol,
                });
                if let Some(order_id) = &order.exchange_order_id {
                    set_str(&mut item, "ordId", order_id);
                }
                if let Some(client_order_id) = &order.client_order_id {
                    set_str(&mut item, "clOrdId", client_order_id);
                }
                item
            })
            .collect::<Vec<_>>();
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Okx,
            PrivateRestMethod::Post,
            "/api/v5/trade/cancel-batch-orders",
        )
        .with_body(Value::Array(orders)))
    }

    fn get_order(&self, query: &OrderQuery) -> Result<PrivateRestRequestSpec> {
        ensure_okx_symbol(&query.exchange_symbol, "query order")?;
        if query.exchange != ExchangeId::Okx {
            anyhow::bail!("okx query order requires okx exchange symbols");
        }
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Okx,
            PrivateRestMethod::Get,
            "/api/v5/trade/order",
        )
        .with_query("instId", &query.exchange_symbol.symbol);
        if let Some(order_id) = &query.exchange_order_id {
            spec = spec.with_query("ordId", order_id);
        }
        if let Some(client_order_id) = &query.client_order_id {
            spec = spec.with_query("clOrdId", client_order_id);
        }
        if query.exchange_order_id.is_none() && query.client_order_id.is_none() {
            anyhow::bail!("okx query order requires ordId or clOrdId");
        }
        Ok(spec)
    }

    fn get_open_orders(&self, symbol: Option<&ExchangeSymbol>) -> Result<PrivateRestRequestSpec> {
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Okx,
            PrivateRestMethod::Get,
            "/api/v5/trade/orders-pending",
        )
        .with_query("instType", "SWAP");
        if let Some(symbol) = symbol {
            ensure_okx_symbol(symbol, "open orders")?;
            spec = spec.with_query("instId", &symbol.symbol);
        }
        Ok(spec)
    }

    fn get_all_orders(&self, query: &OrderHistoryQuery) -> Result<PrivateRestRequestSpec> {
        if query.exchange != ExchangeId::Okx || query.exchange_symbol.exchange != ExchangeId::Okx {
            anyhow::bail!("okx all-orders history requires okx exchange symbols");
        }
        if let Some(limit) = query.limit {
            if limit > 100 {
                anyhow::bail!("okx all-orders history limit must be <= 100");
            }
        }
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Okx,
            PrivateRestMethod::Get,
            "/api/v5/trade/orders-history",
        )
        .with_query("instType", "SWAP")
        .with_query("instId", &query.exchange_symbol.symbol);
        if let Some(order_id) = &query.exchange_order_id {
            spec = spec.with_query("ordId", order_id);
        }
        if let Some(start_time) = query.start_time {
            spec = spec.with_query("begin", start_time.timestamp_millis());
        }
        if let Some(end_time) = query.end_time {
            spec = spec.with_query("end", end_time.timestamp_millis());
        }
        if let Some(limit) = query.limit {
            spec = spec.with_query("limit", limit);
        }
        Ok(spec)
    }

    fn get_fills(&self, query: &FillQuery) -> Result<PrivateRestRequestSpec> {
        if query.exchange != ExchangeId::Okx {
            anyhow::bail!("okx fills-history requires okx exchange symbols");
        }
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Okx,
            PrivateRestMethod::Get,
            "/api/v5/trade/fills-history",
        )
        .with_query("instType", "SWAP");
        if let Some(symbol) = &query.exchange_symbol {
            ensure_okx_symbol(symbol, "fills-history")?;
            spec = spec.with_query("instId", &symbol.symbol);
        }
        if let Some(order_id) = &query.exchange_order_id {
            spec = spec.with_query("ordId", order_id);
        }
        if let Some(start_time) = query.start_time {
            spec = spec.with_query("begin", start_time.timestamp_millis());
        }
        if let Some(end_time) = query.end_time {
            spec = spec.with_query("end", end_time.timestamp_millis());
        }
        if let Some(limit) = query.limit {
            spec = spec.with_query("limit", limit);
        }
        Ok(spec)
    }

    fn get_positions(&self, symbol: Option<&ExchangeSymbol>) -> Result<PrivateRestRequestSpec> {
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Okx,
            PrivateRestMethod::Get,
            "/api/v5/account/positions",
        )
        .with_query("instType", "SWAP");
        if let Some(symbol) = symbol {
            ensure_okx_symbol(symbol, "positions")?;
            spec = spec.with_query("instId", &symbol.symbol);
        }
        Ok(spec)
    }

    fn get_balances(&self) -> Result<PrivateRestRequestSpec> {
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Okx,
            PrivateRestMethod::Get,
            "/api/v5/account/balance",
        ))
    }

    fn get_trade_fee(&self, symbol: &ExchangeSymbol) -> Result<PrivateRestRequestSpec> {
        ensure_okx_symbol(symbol, "trade fee")?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Okx,
            PrivateRestMethod::Get,
            "/api/v5/account/trade-fee",
        )
        .with_query("instType", "SWAP")
        .with_query("instId", &symbol.symbol))
    }

    fn get_symbol_account_config(&self, symbol: &ExchangeSymbol) -> Result<PrivateRestRequestSpec> {
        ensure_okx_symbol(symbol, "symbol account config")?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Okx,
            PrivateRestMethod::Get,
            "/api/v5/account/leverage-info",
        )
        .with_query("instId", &symbol.symbol)
        .with_query("mgnMode", "cross"))
    }

    fn amend_order(&self, command: &AmendOrderCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Okx
            || command.exchange_symbol.exchange != ExchangeId::Okx
        {
            anyhow::bail!("okx amend requires okx exchange symbols");
        }
        let mut body = json!({
            "instId": command.exchange_symbol.symbol,
        });
        if let Some(order_id) = &command.exchange_order_id {
            set_str(&mut body, "ordId", order_id);
        }
        if let Some(client_order_id) = &command.client_order_id {
            set_str(&mut body, "clOrdId", client_order_id);
        }
        if let Some(quantity) = command.new_quantity {
            set_str(&mut body, "newSz", number_string(quantity));
        }
        if let Some(price) = command.new_price {
            set_str(&mut body, "newPx", number_string(price));
        }
        if command.exchange_order_id.is_none() && command.client_order_id.is_none() {
            anyhow::bail!("okx amend requires ordId or clOrdId");
        }
        if command.new_quantity.is_none() && command.new_price.is_none() {
            anyhow::bail!("okx amend requires new quantity or new price");
        }
        if command.new_client_order_id.is_some() {
            anyhow::bail!("okx amend does not replace client order id");
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Okx,
            PrivateRestMethod::Post,
            "/api/v5/trade/amend-order",
        )
        .with_body(body))
    }

    fn set_leverage(&self, command: &LeverageCommand) -> Result<PrivateRestRequestSpec> {
        ensure_okx_command_symbol(&command.exchange, &command.exchange_symbol, "set leverage")?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Okx,
            PrivateRestMethod::Post,
            "/api/v5/account/set-leverage",
        )
        .with_body(json!({
            "instId": command.exchange_symbol.symbol,
            "lever": command.leverage.to_string(),
            "mgnMode": "cross",
        })))
    }

    fn set_position_mode(&self, command: &PositionModeCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Okx {
            anyhow::bail!("okx position mode requires okx exchange");
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Okx,
            PrivateRestMethod::Post,
            "/api/v5/account/set-position-mode",
        )
        .with_body(json!({
            "posMode": if command.mode.is_hedge() { "long_short_mode" } else { "net_mode" },
        })))
    }

    fn set_countdown_cancel_all(
        &self,
        command: &CountdownCancelAllCommand,
    ) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Okx {
            anyhow::bail!("okx cancel-all-after requires okx exchange symbols");
        }
        if let Some(symbol) = &command.exchange_symbol {
            ensure_okx_symbol(symbol, "cancel-all-after")?;
        }
        if command.timeout_secs != 0 && !(10..=120).contains(&command.timeout_secs) {
            anyhow::bail!("okx cancel-all-after timeout must be 0 or between 10 and 120 seconds");
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Okx,
            PrivateRestMethod::Post,
            "/api/v5/trade/cancel-all-after",
        )
        .with_body(json!({
            "timeOut": command.timeout_secs.to_string(),
        })))
    }

    fn ws_login(&self, auth: &PrivateWsAuth, timestamp: i64) -> Result<PrivateWsRequest> {
        let passphrase = auth
            .passphrase
            .as_ref()
            .ok_or_else(|| anyhow!("okx private websocket requires passphrase"))?;
        let timestamp_text = timestamp_secs_from_any(timestamp).to_string();
        let sign = okx_signature(
            &auth.api_secret,
            &timestamp_text,
            "GET",
            "/users/self/verify",
            "",
        );
        Ok(PrivateWsRequest {
            exchange: ExchangeId::Okx,
            url: PrivatePerpExchange::Okx.private_ws_url().to_string(),
            message: json!({
                "op": "login",
                "args": [{
                    "apiKey": auth.api_key,
                    "passphrase": passphrase,
                    "timestamp": timestamp_text,
                    "sign": sign,
                }]
            })
            .to_string(),
        })
    }

    fn ws_subscribe(
        &self,
        subscription: &PrivateWsSubscription,
        _timestamp: i64,
    ) -> Result<PrivateWsRequest> {
        let args = if matches!(subscription.channel.as_str(), "account" | "positions") {
            vec![json!({
                "channel": subscription.channel,
                "instType": "SWAP",
            })]
        } else {
            let symbols = if subscription.symbols.is_empty() {
                vec![ExchangeSymbol::new(ExchangeId::Okx, "default")]
            } else {
                subscription.symbols.clone()
            };
            symbols
                .iter()
                .map(|symbol| {
                    json!({
                        "channel": subscription.channel,
                        "instType": "SWAP",
                        "instId": symbol.symbol,
                    })
                })
                .collect::<Vec<_>>()
        };
        Ok(PrivateWsRequest {
            exchange: ExchangeId::Okx,
            url: PrivatePerpExchange::Okx.private_ws_url().to_string(),
            message: json!({"op": "subscribe", "args": args}).to_string(),
        })
    }

    fn parse_private_ws_message(
        &self,
        raw: &str,
        received_at: DateTime<Utc>,
    ) -> Result<Vec<PrivateEvent>> {
        parse_okx_private_message(raw, received_at)
    }
}

impl PrivatePerpProtocol for CoinExPrivatePerpProtocol {
    fn exchange(&self) -> PrivatePerpExchange {
        PrivatePerpExchange::CoinEx
    }

    fn place_order(
        &self,
        command: &OrderCommand,
        _position_mode: PositionMode,
    ) -> Result<PrivateRestRequestSpec> {
        ensure_coinex_command_symbol(&command.exchange, &command.exchange_symbol, "place order")?;
        let mut body = coinex_order_body(command);
        if command.post_only {
            set_bool(&mut body, "is_hide", true);
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::CoinEx,
            PrivateRestMethod::Post,
            "/futures/order",
        )
        .with_body(body))
    }

    fn place_batch_orders(
        &self,
        command: &BatchPlaceCommand,
        _position_mode: PositionMode,
    ) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::CoinEx {
            anyhow::bail!("coinex batch-place requires coinex exchange symbols");
        }
        if command.orders.is_empty() {
            anyhow::bail!("coinex batch-place requires at least one order");
        }
        let mut orders = Vec::with_capacity(command.orders.len());
        for order in &command.orders {
            ensure_coinex_command_symbol(&order.exchange, &order.exchange_symbol, "batch-place")?;
            orders.push(coinex_order_body(order));
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::CoinEx,
            PrivateRestMethod::Post,
            "/futures/batch-order",
        )
        .with_body(Value::Array(orders)))
    }

    fn cancel_order(&self, command: &CancelCommand) -> Result<PrivateRestRequestSpec> {
        ensure_coinex_command_symbol(&command.exchange, &command.exchange_symbol, "cancel")?;
        if command.exchange_order_id.is_none() && command.client_order_id.is_none() {
            anyhow::bail!("coinex cancel requires order_id or client_id");
        }
        let mut body = coinex_market_body(&command.exchange_symbol);
        if let Some(order_id) = &command.exchange_order_id {
            set_str(&mut body, "order_id", order_id);
        }
        if let Some(client_id) = &command.client_order_id {
            set_str(&mut body, "client_id", client_id);
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::CoinEx,
            PrivateRestMethod::Delete,
            "/futures/order",
        )
        .with_body(body))
    }

    fn cancel_all_orders(&self, command: &CancelAllCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::CoinEx {
            anyhow::bail!("coinex cancel-all requires coinex exchange symbols");
        }
        let mut body = json!({"market_type": "FUTURES"});
        if let Some(symbol) = &command.exchange_symbol {
            ensure_coinex_symbol(symbol, "cancel-all")?;
            set_str(&mut body, "market", &symbol.symbol);
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::CoinEx,
            PrivateRestMethod::Post,
            "/futures/cancel-all-order",
        )
        .with_body(body))
    }

    fn cancel_batch_orders(&self, command: &CancelBatchCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::CoinEx {
            anyhow::bail!("coinex batch cancel requires coinex exchange symbols");
        }
        if command.orders.is_empty() {
            anyhow::bail!("coinex batch cancel requires at least one order");
        }
        let mut orders = Vec::with_capacity(command.orders.len());
        for order in &command.orders {
            ensure_coinex_command_symbol(&order.exchange, &order.exchange_symbol, "batch cancel")?;
            let mut item = coinex_market_body(&order.exchange_symbol);
            if let Some(order_id) = &order.exchange_order_id {
                set_str(&mut item, "order_id", order_id);
            }
            if let Some(client_id) = &order.client_order_id {
                set_str(&mut item, "client_id", client_id);
            }
            if order.exchange_order_id.is_none() && order.client_order_id.is_none() {
                anyhow::bail!("coinex batch cancel requires order_id or client_id");
            }
            orders.push(item);
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::CoinEx,
            PrivateRestMethod::Post,
            "/futures/batch-cancel-order",
        )
        .with_body(Value::Array(orders)))
    }

    fn get_order(&self, query: &OrderQuery) -> Result<PrivateRestRequestSpec> {
        if query.exchange != ExchangeId::CoinEx {
            anyhow::bail!("coinex query order requires coinex exchange symbols");
        }
        ensure_coinex_symbol(&query.exchange_symbol, "query order")?;
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::CoinEx,
            PrivateRestMethod::Get,
            "/futures/order-status",
        )
        .with_query("market", &query.exchange_symbol.symbol)
        .with_query("market_type", "FUTURES");
        if let Some(order_id) = &query.exchange_order_id {
            spec = spec.with_query("order_id", order_id);
        }
        if let Some(client_id) = &query.client_order_id {
            spec = spec.with_query("client_id", client_id);
        }
        if query.exchange_order_id.is_none() && query.client_order_id.is_none() {
            anyhow::bail!("coinex order query requires order_id or client_id");
        }
        Ok(spec)
    }

    fn get_open_orders(&self, symbol: Option<&ExchangeSymbol>) -> Result<PrivateRestRequestSpec> {
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::CoinEx,
            PrivateRestMethod::Get,
            "/futures/pending-order",
        )
        .with_query("market_type", "FUTURES");
        if let Some(symbol) = symbol {
            ensure_coinex_symbol(symbol, "open orders")?;
            spec = spec.with_query("market", &symbol.symbol);
        }
        Ok(spec)
    }

    fn get_all_orders(&self, query: &OrderHistoryQuery) -> Result<PrivateRestRequestSpec> {
        if query.exchange != ExchangeId::CoinEx {
            anyhow::bail!("coinex all-orders history requires coinex exchange symbols");
        }
        ensure_coinex_symbol(&query.exchange_symbol, "all-orders history")?;
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::CoinEx,
            PrivateRestMethod::Get,
            "/futures/finished-order",
        )
        .with_query("market", &query.exchange_symbol.symbol)
        .with_query("market_type", "FUTURES");
        if let Some(limit) = query.limit {
            spec = spec.with_query("limit", limit);
        }
        Ok(spec)
    }

    fn get_fills(&self, query: &FillQuery) -> Result<PrivateRestRequestSpec> {
        if query.exchange != ExchangeId::CoinEx {
            anyhow::bail!("coinex fills require coinex exchange");
        }
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::CoinEx,
            PrivateRestMethod::Get,
            "/futures/user-deals",
        )
        .with_query("market_type", "FUTURES");
        if let Some(symbol) = &query.exchange_symbol {
            ensure_coinex_symbol(symbol, "fills")?;
            spec = spec.with_query("market", &symbol.symbol);
        }
        if let Some(limit) = query.limit {
            spec = spec.with_query("limit", limit);
        }
        Ok(spec)
    }

    fn get_positions(&self, symbol: Option<&ExchangeSymbol>) -> Result<PrivateRestRequestSpec> {
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::CoinEx,
            PrivateRestMethod::Get,
            "/futures/pending-position",
        )
        .with_query("market_type", "FUTURES");
        if let Some(symbol) = symbol {
            ensure_coinex_symbol(symbol, "positions")?;
            spec = spec.with_query("market", &symbol.symbol);
        }
        Ok(spec)
    }

    fn get_balances(&self) -> Result<PrivateRestRequestSpec> {
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::CoinEx,
            PrivateRestMethod::Get,
            "/assets/futures/balance",
        ))
    }

    fn get_trade_fee(&self, symbol: &ExchangeSymbol) -> Result<PrivateRestRequestSpec> {
        ensure_coinex_symbol(symbol, "trade fee")?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::CoinEx,
            PrivateRestMethod::Get,
            "/futures/market",
        )
        .with_query("market", &symbol.symbol))
    }

    fn get_symbol_account_config(&self, symbol: &ExchangeSymbol) -> Result<PrivateRestRequestSpec> {
        ensure_coinex_symbol(symbol, "symbol account config")?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::CoinEx,
            PrivateRestMethod::Get,
            "/futures/pending-position",
        )
        .with_query("market", &symbol.symbol)
        .with_query("market_type", "FUTURES"))
    }

    fn amend_order(&self, command: &AmendOrderCommand) -> Result<PrivateRestRequestSpec> {
        ensure_coinex_command_symbol(&command.exchange, &command.exchange_symbol, "amend")?;
        if command.exchange_order_id.is_none() && command.client_order_id.is_none() {
            anyhow::bail!("coinex amend requires order_id or client_id");
        }
        if command.new_price.is_none() && command.new_quantity.is_none() {
            anyhow::bail!("coinex amend requires a new price or quantity");
        }
        let mut body = coinex_market_body(&command.exchange_symbol);
        if let Some(order_id) = &command.exchange_order_id {
            set_str(&mut body, "order_id", order_id);
        }
        if let Some(client_id) = &command.client_order_id {
            set_str(&mut body, "client_id", client_id);
        }
        if let Some(price) = command.new_price {
            set_str(&mut body, "price", number_string(price));
        }
        if let Some(quantity) = command.new_quantity {
            set_str(&mut body, "amount", number_string(quantity));
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::CoinEx,
            PrivateRestMethod::Post,
            "/futures/modify-order",
        )
        .with_body(body))
    }

    fn set_leverage(&self, command: &LeverageCommand) -> Result<PrivateRestRequestSpec> {
        ensure_coinex_command_symbol(&command.exchange, &command.exchange_symbol, "leverage")?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::CoinEx,
            PrivateRestMethod::Post,
            "/futures/adjust-position-leverage",
        )
        .with_body(json!({
            "market": command.exchange_symbol.symbol,
            "market_type": "FUTURES",
            "leverage": command.leverage,
        })))
    }

    fn set_position_mode(&self, command: &PositionModeCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::CoinEx {
            anyhow::bail!("coinex position mode requires coinex exchange");
        }
        anyhow::bail!("coinex private perp position mode change is not implemented")
    }

    fn set_countdown_cancel_all(
        &self,
        command: &CountdownCancelAllCommand,
    ) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::CoinEx {
            anyhow::bail!("coinex countdown cancel-all requires coinex exchange");
        }
        if let Some(symbol) = &command.exchange_symbol {
            ensure_coinex_symbol(symbol, "countdown cancel-all")?;
        }
        anyhow::bail!("coinex private perp countdown cancel-all is not implemented")
    }

    fn ws_login(&self, auth: &PrivateWsAuth, timestamp: i64) -> Result<PrivateWsRequest> {
        Ok(PrivateWsRequest {
            exchange: ExchangeId::CoinEx,
            url: PrivatePerpExchange::CoinEx.private_ws_url().to_string(),
            message: json!({
                "method": "server.sign",
                "params": {
                    "access_id": auth.api_key,
                    "signed_str": coinex_ws_signature(&auth.api_secret, timestamp),
                    "timestamp": timestamp,
                },
                "id": timestamp
            })
            .to_string(),
        })
    }

    fn ws_subscribe(
        &self,
        subscription: &PrivateWsSubscription,
        timestamp: i64,
    ) -> Result<PrivateWsRequest> {
        Ok(PrivateWsRequest {
            exchange: ExchangeId::CoinEx,
            url: PrivatePerpExchange::CoinEx.private_ws_url().to_string(),
            message: json!({
                "method": subscription.channel,
                "params": subscription.symbols.iter().map(|symbol| symbol.symbol.clone()).collect::<Vec<_>>(),
                "id": timestamp
            })
            .to_string(),
        })
    }

    fn parse_private_ws_message(
        &self,
        _raw: &str,
        _received_at: DateTime<Utc>,
    ) -> Result<Vec<PrivateEvent>> {
        Ok(Vec::new())
    }
}

impl PrivatePerpProtocol for BinancePrivatePerpProtocol {
    fn exchange(&self) -> PrivatePerpExchange {
        PrivatePerpExchange::Binance
    }

    fn place_order(
        &self,
        command: &OrderCommand,
        position_mode: PositionMode,
    ) -> Result<PrivateRestRequestSpec> {
        ensure_binance_command_symbol(&command.exchange, &command.exchange_symbol, "place order")?;
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Binance,
            PrivateRestMethod::Post,
            "/fapi/v1/order",
        )
        .with_query("symbol", &command.exchange_symbol.symbol)
        .with_query("side", binance_side(command.side))
        .with_query("type", binance_order_type(command.order_type))
        .with_query("quantity", number_string(command.quantity))
        .with_query("newClientOrderId", &command.client_order_id);
        if let Some(position_side) =
            binance_position_side(command.position_side, command.side, position_mode)
        {
            spec = spec.with_query("positionSide", position_side);
        }
        if command.order_type == OrderType::Limit {
            spec = spec.with_query(
                "timeInForce",
                binance_time_in_force(command.time_in_force, command.post_only),
            );
        }
        if let Some(price) = command.price {
            spec = spec.with_query("price", number_string(price));
        }
        if command.reduce_only && !position_mode.is_hedge() {
            spec = spec.with_query("reduceOnly", "true");
        }
        Ok(spec)
    }

    fn place_batch_orders(
        &self,
        command: &BatchPlaceCommand,
        position_mode: PositionMode,
    ) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Binance {
            anyhow::bail!("binance batch-place requires binance exchange symbols");
        }
        if command.orders.is_empty() {
            anyhow::bail!("binance batch-place requires at least one order");
        }
        if command.orders.len() > 5 {
            anyhow::bail!("binance batch-place supports at most 5 orders");
        }
        if command.orders.iter().any(|order| {
            order.exchange != ExchangeId::Binance
                || order.exchange_symbol.exchange != ExchangeId::Binance
        }) {
            anyhow::bail!("binance batch-place requires binance exchange symbols");
        }
        let orders = command
            .orders
            .iter()
            .map(|order| binance_batch_order_item(order, position_mode))
            .collect::<Vec<_>>();
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Binance,
            PrivateRestMethod::Post,
            "/fapi/v1/batchOrders",
        )
        .with_query("batchOrders", serde_json::to_string(&orders)?))
    }

    fn cancel_order(&self, command: &CancelCommand) -> Result<PrivateRestRequestSpec> {
        ensure_binance_command_symbol(&command.exchange, &command.exchange_symbol, "cancel")?;
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Binance,
            PrivateRestMethod::Delete,
            "/fapi/v1/order",
        )
        .with_query("symbol", &command.exchange_symbol.symbol);
        if let Some(order_id) = &command.exchange_order_id {
            spec = spec.with_query("orderId", order_id);
        }
        if let Some(client_order_id) = &command.client_order_id {
            spec = spec.with_query("origClientOrderId", client_order_id);
        }
        if command.exchange_order_id.is_none() && command.client_order_id.is_none() {
            anyhow::bail!("binance cancel requires orderId or origClientOrderId");
        }
        Ok(spec)
    }

    fn cancel_all_orders(&self, command: &CancelAllCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Binance {
            anyhow::bail!("binance cancel-all requires binance exchange symbols");
        }
        let symbol = command
            .exchange_symbol
            .as_ref()
            .ok_or_else(|| anyhow!("binance cancel-all requires symbol"))?;
        ensure_binance_symbol(symbol, "cancel-all")?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Binance,
            PrivateRestMethod::Delete,
            "/fapi/v1/allOpenOrders",
        )
        .with_query("symbol", &symbol.symbol))
    }

    fn cancel_batch_orders(&self, command: &CancelBatchCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Binance {
            anyhow::bail!("binance batch cancel requires binance exchange symbols");
        }
        let first = command
            .orders
            .first()
            .ok_or_else(|| anyhow!("binance batch cancel requires at least one order"))?;
        if command.orders.len() > 10 {
            anyhow::bail!("binance batch cancel supports at most 10 orders");
        }
        if command.orders.iter().any(|order| {
            order.exchange != ExchangeId::Binance
                || order.exchange_symbol.exchange != ExchangeId::Binance
        }) {
            anyhow::bail!("binance batch cancel requires binance exchange symbols");
        }
        if command
            .orders
            .iter()
            .any(|order| order.exchange_symbol.symbol != first.exchange_symbol.symbol)
        {
            anyhow::bail!("binance batch cancel requires one symbol per request");
        }
        let exchange_order_ids = command
            .orders
            .iter()
            .filter_map(|order| order.exchange_order_id.as_ref())
            .collect::<Vec<_>>();
        let client_order_ids = command
            .orders
            .iter()
            .filter_map(|order| order.client_order_id.as_ref())
            .collect::<Vec<_>>();
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Binance,
            PrivateRestMethod::Delete,
            "/fapi/v1/batchOrders",
        )
        .with_query("symbol", &first.exchange_symbol.symbol);
        if exchange_order_ids.len() == command.orders.len() {
            let ids = exchange_order_ids
                .into_iter()
                .map(|order_id| {
                    order_id
                        .parse::<i64>()
                        .map(Value::from)
                        .unwrap_or_else(|_| Value::String(order_id.clone()))
                })
                .collect::<Vec<_>>();
            spec = spec.with_query("orderIdList", serde_json::to_string(&ids)?);
        } else if client_order_ids.len() == command.orders.len() {
            spec = spec.with_query(
                "origClientOrderIdList",
                serde_json::to_string(&client_order_ids)?,
            );
        } else {
            anyhow::bail!(
                "binance batch cancel requires every order to have exchange_order_id or every order to have client_order_id"
            );
        }
        Ok(spec)
    }

    fn get_order(&self, query: &OrderQuery) -> Result<PrivateRestRequestSpec> {
        ensure_binance_symbol(&query.exchange_symbol, "query order")?;
        if query.exchange != ExchangeId::Binance {
            anyhow::bail!("binance query order requires binance exchange symbols");
        }
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Binance,
            PrivateRestMethod::Get,
            "/fapi/v1/order",
        )
        .with_query("symbol", &query.exchange_symbol.symbol);
        if let Some(order_id) = &query.exchange_order_id {
            spec = spec.with_query("orderId", order_id);
        }
        if let Some(client_order_id) = &query.client_order_id {
            spec = spec.with_query("origClientOrderId", client_order_id);
        }
        if query.exchange_order_id.is_none() && query.client_order_id.is_none() {
            anyhow::bail!("binance query order requires orderId or origClientOrderId");
        }
        Ok(spec)
    }

    fn get_open_orders(&self, symbol: Option<&ExchangeSymbol>) -> Result<PrivateRestRequestSpec> {
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Binance,
            PrivateRestMethod::Get,
            "/fapi/v1/openOrders",
        );
        if let Some(symbol) = symbol {
            ensure_binance_symbol(symbol, "open orders")?;
            spec = spec.with_query("symbol", &symbol.symbol);
        }
        Ok(spec)
    }

    fn get_all_orders(&self, query: &OrderHistoryQuery) -> Result<PrivateRestRequestSpec> {
        if query.exchange != ExchangeId::Binance
            || query.exchange_symbol.exchange != ExchangeId::Binance
        {
            anyhow::bail!("binance all-orders history requires binance exchange symbols");
        }
        if let Some(limit) = query.limit {
            if limit > 1_000 {
                anyhow::bail!("binance all-orders history limit must be <= 1000");
            }
        }
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Binance,
            PrivateRestMethod::Get,
            "/fapi/v1/allOrders",
        )
        .with_query("symbol", &query.exchange_symbol.symbol);
        if let Some(order_id) = &query.exchange_order_id {
            spec = spec.with_query("orderId", order_id);
        }
        if let Some(start_time) = query.start_time {
            spec = spec.with_query("startTime", start_time.timestamp_millis());
        }
        if let Some(end_time) = query.end_time {
            spec = spec.with_query("endTime", end_time.timestamp_millis());
        }
        if let Some(limit) = query.limit {
            spec = spec.with_query("limit", limit);
        }
        Ok(spec)
    }

    fn get_order_amendments(
        &self,
        query: &OrderAmendmentHistoryQuery,
    ) -> Result<PrivateRestRequestSpec> {
        if query.exchange != ExchangeId::Binance
            || query.exchange_symbol.exchange != ExchangeId::Binance
        {
            anyhow::bail!("binance order-amendment history requires binance exchange symbols");
        }
        if query.exchange_order_id.is_none() && query.client_order_id.is_none() {
            anyhow::bail!("binance order-amendment history requires orderId or origClientOrderId");
        }
        if let Some(limit) = query.limit {
            if limit > 100 {
                anyhow::bail!("binance order-amendment history limit must be <= 100");
            }
        }
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Binance,
            PrivateRestMethod::Get,
            "/fapi/v1/orderAmendment",
        )
        .with_query("symbol", &query.exchange_symbol.symbol);
        if let Some(order_id) = &query.exchange_order_id {
            spec = spec.with_query("orderId", order_id);
        } else if let Some(client_order_id) = &query.client_order_id {
            spec = spec.with_query("origClientOrderId", client_order_id);
        }
        if let Some(start_time) = query.start_time {
            spec = spec.with_query("startTime", start_time.timestamp_millis());
        }
        if let Some(end_time) = query.end_time {
            spec = spec.with_query("endTime", end_time.timestamp_millis());
        }
        if let Some(limit) = query.limit {
            spec = spec.with_query("limit", limit);
        }
        Ok(spec)
    }

    fn get_fills(&self, query: &FillQuery) -> Result<PrivateRestRequestSpec> {
        let symbol = query
            .exchange_symbol
            .as_ref()
            .ok_or_else(|| anyhow!("binance userTrades requires symbol"))?;
        if query.exchange != ExchangeId::Binance {
            anyhow::bail!("binance userTrades requires binance exchange symbols");
        }
        ensure_binance_symbol(symbol, "userTrades")?;
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Binance,
            PrivateRestMethod::Get,
            "/fapi/v1/userTrades",
        )
        .with_query("symbol", &symbol.symbol);
        if let Some(order_id) = &query.exchange_order_id {
            spec = spec.with_query("orderId", order_id);
        }
        if let Some(from_trade_id) = &query.from_trade_id {
            spec = spec.with_query("fromId", from_trade_id);
        }
        if let Some(start_time) = query.start_time {
            spec = spec.with_query("startTime", start_time.timestamp_millis());
        }
        if let Some(end_time) = query.end_time {
            spec = spec.with_query("endTime", end_time.timestamp_millis());
        }
        if let Some(limit) = query.limit {
            spec = spec.with_query("limit", limit);
        }
        Ok(spec)
    }

    fn get_positions(&self, symbol: Option<&ExchangeSymbol>) -> Result<PrivateRestRequestSpec> {
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Binance,
            PrivateRestMethod::Get,
            "/fapi/v2/positionRisk",
        );
        if let Some(symbol) = symbol {
            ensure_binance_symbol(symbol, "positions")?;
            spec = spec.with_query("symbol", &symbol.symbol);
        }
        Ok(spec)
    }

    fn get_balances(&self) -> Result<PrivateRestRequestSpec> {
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Binance,
            PrivateRestMethod::Get,
            "/fapi/v2/balance",
        ))
    }

    fn get_trade_fee(&self, symbol: &ExchangeSymbol) -> Result<PrivateRestRequestSpec> {
        ensure_binance_symbol(symbol, "trade fee")?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Binance,
            PrivateRestMethod::Get,
            "/fapi/v1/commissionRate",
        )
        .with_query("symbol", &symbol.symbol))
    }

    fn get_symbol_account_config(&self, symbol: &ExchangeSymbol) -> Result<PrivateRestRequestSpec> {
        ensure_binance_symbol(symbol, "symbol account config")?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Binance,
            PrivateRestMethod::Get,
            "/fapi/v1/symbolConfig",
        )
        .with_query("symbol", &symbol.symbol))
    }

    fn amend_order(&self, command: &AmendOrderCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Binance
            || command.exchange_symbol.exchange != ExchangeId::Binance
        {
            anyhow::bail!("binance amend requires binance exchange symbols");
        }
        let side = command
            .original_side
            .ok_or_else(|| anyhow!("binance amend requires original_side"))?;
        let quantity = command
            .new_quantity
            .ok_or_else(|| anyhow!("binance amend requires new_quantity"))?;
        let price = command
            .new_price
            .ok_or_else(|| anyhow!("binance amend requires new_price"))?;
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Binance,
            PrivateRestMethod::Put,
            "/fapi/v1/order",
        )
        .with_query("symbol", &command.exchange_symbol.symbol)
        .with_query("side", binance_side(side))
        .with_query("quantity", number_string(quantity))
        .with_query("price", number_string(price));
        if let Some(order_id) = &command.exchange_order_id {
            spec = spec.with_query("orderId", order_id);
        }
        if let Some(client_order_id) = &command.client_order_id {
            spec = spec.with_query("origClientOrderId", client_order_id);
        }
        if command.exchange_order_id.is_none() && command.client_order_id.is_none() {
            anyhow::bail!("binance amend requires orderId or origClientOrderId");
        }
        Ok(spec)
    }

    fn set_leverage(&self, command: &LeverageCommand) -> Result<PrivateRestRequestSpec> {
        ensure_binance_command_symbol(&command.exchange, &command.exchange_symbol, "set leverage")?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Binance,
            PrivateRestMethod::Post,
            "/fapi/v1/leverage",
        )
        .with_query("symbol", &command.exchange_symbol.symbol)
        .with_query("leverage", command.leverage))
    }

    fn set_position_mode(&self, command: &PositionModeCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Binance {
            anyhow::bail!("binance position mode requires binance exchange");
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Binance,
            PrivateRestMethod::Post,
            "/fapi/v1/positionSide/dual",
        )
        .with_query(
            "dualSidePosition",
            if command.mode.is_hedge() {
                "true"
            } else {
                "false"
            },
        ))
    }

    fn set_countdown_cancel_all(
        &self,
        command: &CountdownCancelAllCommand,
    ) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Binance {
            anyhow::bail!("binance countdown cancel-all requires binance exchange symbols");
        }
        let symbol = command
            .exchange_symbol
            .as_ref()
            .ok_or_else(|| anyhow!("binance countdown cancel-all requires symbol"))?;
        ensure_binance_symbol(symbol, "countdown cancel-all")?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Binance,
            PrivateRestMethod::Post,
            "/fapi/v1/countdownCancelAll",
        )
        .with_query("symbol", &symbol.symbol)
        .with_query("countdownTime", u64::from(command.timeout_secs) * 1_000))
    }

    fn ws_login(&self, _auth: &PrivateWsAuth, _timestamp: i64) -> Result<PrivateWsRequest> {
        anyhow::bail!(
            "binance futures private websocket uses REST-created listenKey instead of ws login"
        )
    }

    fn ws_subscribe(
        &self,
        _subscription: &PrivateWsSubscription,
        _timestamp: i64,
    ) -> Result<PrivateWsRequest> {
        anyhow::bail!("binance futures listenKey stream does not use subscribe messages")
    }

    fn parse_private_ws_message(
        &self,
        raw: &str,
        received_at: DateTime<Utc>,
    ) -> Result<Vec<PrivateEvent>> {
        crate::execution::parse_binance_futures_user_data_event(raw, received_at)
    }
}

impl PrivatePerpProtocol for BitgetPrivatePerpProtocol {
    fn exchange(&self) -> PrivatePerpExchange {
        PrivatePerpExchange::Bitget
    }

    fn place_order(
        &self,
        command: &OrderCommand,
        position_mode: PositionMode,
    ) -> Result<PrivateRestRequestSpec> {
        ensure_bitget_command_symbol(&command.exchange, &command.exchange_symbol, "place order")?;
        let side = if position_mode.is_hedge() {
            bitget_position_side(command.position_side, command.side)
        } else {
            bitget_side(command.side)
        };
        let mut body = json!({
            "productType": "USDT-FUTURES",
            "symbol": command.exchange_symbol.symbol,
            "marginMode": "crossed",
            "marginCoin": "USDT",
            "size": number_string(command.quantity),
            "side": side,
            "orderType": bitget_order_type(command.order_type),
            "clientOid": command.client_order_id,
        });
        set_optional_str(
            &mut body,
            "force",
            bitget_force(command.time_in_force, command.post_only),
        );
        if let Some(price) = command.price {
            set_str(&mut body, "price", number_string(price));
        } else if command.order_type == OrderType::Market {
            set_str(&mut body, "price", "0");
        }
        if position_mode.is_hedge() {
            set_str(
                &mut body,
                "tradeSide",
                bitget_trade_side(command.reduce_only),
            );
        } else if command.reduce_only {
            set_str(&mut body, "reduceOnly", "yes");
        }

        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Bitget,
            PrivateRestMethod::Post,
            "/api/v2/mix/order/place-order",
        )
        .with_body(body))
    }

    fn place_batch_orders(
        &self,
        command: &BatchPlaceCommand,
        position_mode: PositionMode,
    ) -> Result<PrivateRestRequestSpec> {
        let first = command
            .orders
            .first()
            .ok_or_else(|| anyhow!("bitget batch-place requires at least one order"))?;
        if command.exchange != ExchangeId::Bitget {
            anyhow::bail!("bitget batch-place requires bitget exchange symbols");
        }
        if command.orders.len() > 50 {
            anyhow::bail!("bitget batch-place supports at most 50 orders");
        }
        if command.orders.iter().any(|order| {
            order.exchange != ExchangeId::Bitget
                || order.exchange_symbol.exchange != ExchangeId::Bitget
                || order.exchange_symbol.symbol != first.exchange_symbol.symbol
        }) {
            anyhow::bail!("bitget batch-place requires one bitget symbol per request");
        }
        let order_list = command
            .orders
            .iter()
            .map(|order| bitget_batch_order_item(order, position_mode))
            .collect::<Vec<_>>();
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Bitget,
            PrivateRestMethod::Post,
            "/api/v2/mix/order/batch-place-order",
        )
        .with_body(json!({
            "symbol": first.exchange_symbol.symbol,
            "productType": "USDT-FUTURES",
            "marginMode": "crossed",
            "marginCoin": "USDT",
            "orderList": order_list,
        })))
    }

    fn cancel_order(&self, command: &CancelCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Bitget
            || command.exchange_symbol.exchange != ExchangeId::Bitget
        {
            anyhow::bail!("bitget cancel requires bitget exchange symbols");
        }
        if command.exchange_order_id.is_none() && command.client_order_id.is_none() {
            anyhow::bail!("bitget cancel requires orderId or clientOid");
        }
        let mut body = json!({
            "productType": "USDT-FUTURES",
            "symbol": command.exchange_symbol.symbol,
            "marginCoin": "USDT",
        });
        if let Some(order_id) = &command.exchange_order_id {
            set_str(&mut body, "orderId", order_id);
        }
        if let Some(client_oid) = &command.client_order_id {
            set_str(&mut body, "clientOid", client_oid);
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Bitget,
            PrivateRestMethod::Post,
            "/api/v2/mix/order/cancel-order",
        )
        .with_body(body))
    }

    fn cancel_all_orders(&self, command: &CancelAllCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Bitget {
            anyhow::bail!("bitget cancel-all requires bitget exchange symbols");
        }
        let mut body = json!({
            "productType": "USDT-FUTURES",
            "marginCoin": "USDT",
        });
        if let Some(symbol) = &command.exchange_symbol {
            ensure_bitget_symbol(symbol, "cancel-all")?;
            set_str(&mut body, "symbol", &symbol.symbol);
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Bitget,
            PrivateRestMethod::Post,
            "/api/v2/mix/order/cancel-all-orders",
        )
        .with_body(body))
    }

    fn cancel_batch_orders(&self, command: &CancelBatchCommand) -> Result<PrivateRestRequestSpec> {
        let first = command
            .orders
            .first()
            .ok_or_else(|| anyhow!("bitget batch cancel requires at least one order"))?;
        if command.exchange != ExchangeId::Bitget {
            anyhow::bail!("bitget batch cancel requires bitget exchange symbols");
        }
        if command.orders.len() > 50 {
            anyhow::bail!("bitget batch cancel supports at most 50 orders");
        }
        for order in &command.orders {
            if order.exchange != ExchangeId::Bitget
                || order.exchange_symbol.exchange != ExchangeId::Bitget
            {
                anyhow::bail!("bitget batch cancel requires bitget exchange symbols");
            }
            if order.exchange_order_id.is_none() && order.client_order_id.is_none() {
                anyhow::bail!("bitget batch cancel requires orderId or clientOid for every order");
            }
        }
        let mut seen_symbol = first.exchange_symbol.symbol.clone();
        let order_id_list = command
            .orders
            .iter()
            .map(|order| {
                if order.exchange_symbol.symbol != seen_symbol {
                    seen_symbol.clear();
                }
                json!({
                    "orderId": order.exchange_order_id,
                    "clientOid": order.client_order_id,
                })
            })
            .collect::<Vec<_>>();
        let mut body = json!({
            "productType": "USDT-FUTURES",
            "marginCoin": "USDT",
            "orderIdList": order_id_list,
        });
        if !seen_symbol.is_empty() {
            set_str(&mut body, "symbol", seen_symbol);
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Bitget,
            PrivateRestMethod::Post,
            "/api/v2/mix/order/batch-cancel-orders",
        )
        .with_body(body))
    }

    fn get_order(&self, query: &OrderQuery) -> Result<PrivateRestRequestSpec> {
        ensure_bitget_symbol(&query.exchange_symbol, "order detail")?;
        if query.exchange != ExchangeId::Bitget {
            anyhow::bail!("bitget order detail requires bitget exchange symbols");
        }
        if query.exchange_order_id.is_none() && query.client_order_id.is_none() {
            anyhow::bail!("bitget order detail requires orderId or clientOid");
        }
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Bitget,
            PrivateRestMethod::Get,
            "/api/v2/mix/order/detail",
        )
        .with_query("productType", "USDT-FUTURES")
        .with_query("symbol", &query.exchange_symbol.symbol);
        if let Some(order_id) = &query.exchange_order_id {
            spec = spec.with_query("orderId", order_id);
        }
        if let Some(client_oid) = &query.client_order_id {
            spec = spec.with_query("clientOid", client_oid);
        }
        Ok(spec)
    }

    fn get_open_orders(&self, symbol: Option<&ExchangeSymbol>) -> Result<PrivateRestRequestSpec> {
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Bitget,
            PrivateRestMethod::Get,
            "/api/v2/mix/order/orders-pending",
        )
        .with_query("productType", "USDT-FUTURES");
        if let Some(symbol) = symbol {
            ensure_bitget_symbol(symbol, "open orders")?;
            spec = spec.with_query("symbol", &symbol.symbol);
        }
        Ok(spec)
    }

    fn get_all_orders(&self, query: &OrderHistoryQuery) -> Result<PrivateRestRequestSpec> {
        if query.exchange != ExchangeId::Bitget
            || query.exchange_symbol.exchange != ExchangeId::Bitget
        {
            anyhow::bail!("bitget all-orders history requires bitget exchange symbols");
        }
        if let Some(limit) = query.limit {
            if limit > 100 {
                anyhow::bail!("bitget all-orders history limit must be <= 100");
            }
        }
        validate_bitget_time_window(
            query.start_time,
            query.end_time,
            "bitget all-orders history",
        )?;
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Bitget,
            PrivateRestMethod::Get,
            "/api/v2/mix/order/orders-history",
        )
        .with_query("productType", "USDT-FUTURES")
        .with_query("symbol", &query.exchange_symbol.symbol);
        if let Some(order_id) = &query.exchange_order_id {
            spec = spec.with_query("orderId", order_id);
        }
        if let Some(start_time) = query.start_time {
            spec = spec.with_query("startTime", start_time.timestamp_millis());
        }
        if let Some(end_time) = query.end_time {
            spec = spec.with_query("endTime", end_time.timestamp_millis());
        }
        if let Some(limit) = query.limit {
            spec = spec.with_query("limit", limit);
        }
        Ok(spec)
    }

    fn get_fills(&self, query: &FillQuery) -> Result<PrivateRestRequestSpec> {
        if query.exchange != ExchangeId::Bitget {
            anyhow::bail!("bitget fills require bitget exchange symbols");
        }
        validate_bitget_time_window(query.start_time, query.end_time, "bitget fills")?;
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Bitget,
            PrivateRestMethod::Get,
            "/api/v2/mix/order/fills",
        )
        .with_query("productType", "USDT-FUTURES");
        if let Some(symbol) = &query.exchange_symbol {
            ensure_bitget_symbol(symbol, "fills")?;
            spec = spec.with_query("symbol", &symbol.symbol);
        }
        if let Some(order_id) = &query.exchange_order_id {
            spec = spec.with_query("orderId", order_id);
        }
        if let Some(start_time) = query.start_time {
            spec = spec.with_query("startTime", start_time.timestamp_millis());
        }
        if let Some(end_time) = query.end_time {
            spec = spec.with_query("endTime", end_time.timestamp_millis());
        }
        if let Some(limit) = query.limit {
            spec = spec.with_query("limit", limit);
        }
        Ok(spec)
    }

    fn get_positions(&self, symbol: Option<&ExchangeSymbol>) -> Result<PrivateRestRequestSpec> {
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Bitget,
            PrivateRestMethod::Get,
            "/api/v2/mix/position/all-position",
        )
        .with_query("productType", "USDT-FUTURES")
        .with_query("marginCoin", "USDT");
        if let Some(symbol) = symbol {
            ensure_bitget_symbol(symbol, "positions")?;
            spec = spec.with_query("symbol", &symbol.symbol);
        }
        Ok(spec)
    }

    fn get_balances(&self) -> Result<PrivateRestRequestSpec> {
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Bitget,
            PrivateRestMethod::Get,
            "/api/v2/mix/account/accounts",
        )
        .with_query("productType", "USDT-FUTURES"))
    }

    fn get_trade_fee(&self, symbol: &ExchangeSymbol) -> Result<PrivateRestRequestSpec> {
        ensure_bitget_symbol(symbol, "trade fee")?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Bitget,
            PrivateRestMethod::Get,
            "/api/v2/mix/market/contracts",
        )
        .with_query("productType", "USDT-FUTURES")
        .with_query("symbol", &symbol.symbol))
    }

    fn get_symbol_account_config(&self, symbol: &ExchangeSymbol) -> Result<PrivateRestRequestSpec> {
        ensure_bitget_symbol(symbol, "symbol account config")?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Bitget,
            PrivateRestMethod::Get,
            "/api/v2/mix/account/account",
        )
        .with_query("productType", "USDT-FUTURES")
        .with_query("marginCoin", "USDT")
        .with_query("symbol", &symbol.symbol))
    }

    fn amend_order(&self, command: &AmendOrderCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Bitget
            || command.exchange_symbol.exchange != ExchangeId::Bitget
        {
            anyhow::bail!("bitget amend requires bitget exchange symbols");
        }
        let mut body = json!({
            "productType": "USDT-FUTURES",
            "symbol": command.exchange_symbol.symbol,
            "marginCoin": "USDT",
        });
        if let Some(order_id) = &command.exchange_order_id {
            set_str(&mut body, "orderId", order_id);
        }
        if let Some(client_oid) = &command.client_order_id {
            set_str(&mut body, "clientOid", client_oid);
        }
        if let Some(new_client_oid) = &command.new_client_order_id {
            set_str(&mut body, "newClientOid", new_client_oid);
        }
        if let Some(quantity) = command.new_quantity {
            set_str(&mut body, "newSize", number_string(quantity));
        }
        if let Some(price) = command.new_price {
            set_str(&mut body, "newPrice", number_string(price));
        }
        if command.exchange_order_id.is_none() && command.client_order_id.is_none() {
            anyhow::bail!("bitget amend requires orderId or clientOid");
        }
        if command.new_quantity.is_none() && command.new_price.is_none() {
            anyhow::bail!("bitget amend requires new quantity or new price");
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Bitget,
            PrivateRestMethod::Post,
            "/api/v2/mix/order/modify-order",
        )
        .with_body(body))
    }

    fn set_leverage(&self, command: &LeverageCommand) -> Result<PrivateRestRequestSpec> {
        ensure_bitget_command_symbol(&command.exchange, &command.exchange_symbol, "set leverage")?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Bitget,
            PrivateRestMethod::Post,
            "/api/v2/mix/account/set-leverage",
        )
        .with_body(json!({
            "productType": "USDT-FUTURES",
            "symbol": command.exchange_symbol.symbol,
            "marginCoin": "USDT",
            "leverage": command.leverage.to_string(),
        })))
    }

    fn set_position_mode(&self, command: &PositionModeCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Bitget {
            anyhow::bail!("bitget position mode requires bitget exchange");
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Bitget,
            PrivateRestMethod::Post,
            "/api/v2/mix/account/set-position-mode",
        )
        .with_body(json!({
            "productType": "USDT-FUTURES",
            "posMode": if command.mode.is_hedge() { "hedge_mode" } else { "one_way_mode" },
        })))
    }

    fn set_countdown_cancel_all(
        &self,
        command: &CountdownCancelAllCommand,
    ) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Bitget {
            anyhow::bail!("bitget countdown cancel-all requires bitget exchange symbols");
        }
        if let Some(symbol) = &command.exchange_symbol {
            ensure_bitget_symbol(symbol, "countdown cancel-all")?;
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Bitget,
            PrivateRestMethod::Post,
            "/api/v3/trade/countdown-cancel-all",
        )
        .with_body(json!({
            "countdown": command.timeout_secs.to_string(),
        })))
    }

    fn ws_login(&self, auth: &PrivateWsAuth, timestamp: i64) -> Result<PrivateWsRequest> {
        let passphrase = auth
            .passphrase
            .as_ref()
            .ok_or_else(|| anyhow!("bitget private websocket requires passphrase"))?;
        let timestamp_text = timestamp.to_string();
        let sign = bitget_signature(&auth.api_secret, &timestamp_text, "GET", "/user/verify", "");
        Ok(PrivateWsRequest {
            exchange: ExchangeId::Bitget,
            url: PrivatePerpExchange::Bitget.private_ws_url().to_string(),
            message: json!({
                "op": "login",
                "args": [{
                    "apiKey": auth.api_key,
                    "passphrase": passphrase,
                    "timestamp": timestamp_text,
                    "sign": sign,
                }]
            })
            .to_string(),
        })
    }

    fn ws_subscribe(
        &self,
        subscription: &PrivateWsSubscription,
        _timestamp: i64,
    ) -> Result<PrivateWsRequest> {
        let args = if subscription.channel == "account" {
            vec![json!({
                "instType": "USDT-FUTURES",
                "channel": subscription.channel,
                "coin": "default",
            })]
        } else {
            let symbols = if subscription.symbols.is_empty() {
                vec![ExchangeSymbol::new(ExchangeId::Bitget, "default")]
            } else {
                subscription.symbols.clone()
            };
            symbols
                .iter()
                .map(|symbol| {
                    json!({
                        "instType": "USDT-FUTURES",
                        "channel": subscription.channel,
                        "instId": symbol.symbol,
                    })
                })
                .collect::<Vec<_>>()
        };
        Ok(PrivateWsRequest {
            exchange: ExchangeId::Bitget,
            url: PrivatePerpExchange::Bitget.private_ws_url().to_string(),
            message: json!({"op": "subscribe", "args": args}).to_string(),
        })
    }

    fn parse_private_ws_message(
        &self,
        raw: &str,
        received_at: DateTime<Utc>,
    ) -> Result<Vec<PrivateEvent>> {
        parse_bitget_private_message(raw, received_at)
    }
}

impl PrivatePerpProtocol for GatePrivatePerpProtocol {
    fn exchange(&self) -> PrivatePerpExchange {
        PrivatePerpExchange::Gate
    }

    fn place_order(
        &self,
        command: &OrderCommand,
        _position_mode: PositionMode,
    ) -> Result<PrivateRestRequestSpec> {
        ensure_gate_command_symbol(&command.exchange, &command.exchange_symbol, "place order")?;
        let mut body = json!({
            "contract": command.exchange_symbol.symbol,
            "size": gate_signed_size(command.side, command.quantity)?,
            "tif": gate_tif(command.time_in_force, command.post_only),
            "text": gate_client_text(&command.client_order_id),
            "reduce_only": command.reduce_only,
        });
        if let Some(price) = command.price {
            set_str(&mut body, "price", number_string(price));
        } else if command.order_type == OrderType::Market {
            set_str(&mut body, "price", "0");
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Gate,
            PrivateRestMethod::Post,
            "/futures/usdt/orders",
        )
        .with_body(body))
    }

    fn place_batch_orders(
        &self,
        command: &BatchPlaceCommand,
        _position_mode: PositionMode,
    ) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Gate {
            anyhow::bail!("gate batch-place requires gate exchange symbols");
        }
        if command.orders.is_empty() {
            anyhow::bail!("gate batch-place requires at least one order");
        }
        if command.orders.len() > 10 {
            anyhow::bail!("gate batch-place supports at most 10 orders");
        }
        let mut orders = Vec::with_capacity(command.orders.len());
        for order in &command.orders {
            if order.exchange != ExchangeId::Gate
                || order.exchange_symbol.exchange != ExchangeId::Gate
            {
                anyhow::bail!("gate batch-place requires gate exchange symbols");
            }
            let mut body = json!({
                "contract": order.exchange_symbol.symbol,
                "size": gate_signed_size(order.side, order.quantity)?,
                "tif": gate_tif(order.time_in_force, order.post_only),
                "text": gate_client_text(&order.client_order_id),
                "reduce_only": order.reduce_only,
            });
            if let Some(price) = order.price {
                set_str(&mut body, "price", number_string(price));
            } else if order.order_type == OrderType::Market {
                set_str(&mut body, "price", "0");
            }
            orders.push(body);
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Gate,
            PrivateRestMethod::Post,
            "/futures/usdt/batch_orders",
        )
        .with_body(Value::Array(orders)))
    }

    fn cancel_order(&self, command: &CancelCommand) -> Result<PrivateRestRequestSpec> {
        ensure_gate_command_symbol(&command.exchange, &command.exchange_symbol, "cancel")?;
        let order_id = gate_order_lookup_id(
            command.exchange_order_id.as_deref(),
            command.client_order_id.as_deref(),
        )?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Gate,
            PrivateRestMethod::Delete,
            gate_order_path(order_id),
        )
        .with_query("contract", &command.exchange_symbol.symbol))
    }

    fn cancel_all_orders(&self, command: &CancelAllCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Gate {
            anyhow::bail!("gate cancel-all requires gate exchange symbols");
        }
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Gate,
            PrivateRestMethod::Delete,
            "/futures/usdt/orders",
        );
        if let Some(symbol) = &command.exchange_symbol {
            ensure_gate_symbol(symbol, "cancel-all")?;
            spec = spec.with_query("contract", &symbol.symbol);
        }
        Ok(spec)
    }

    fn cancel_batch_orders(&self, command: &CancelBatchCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Gate {
            anyhow::bail!("gate batch cancel requires gate exchange symbols");
        }
        if command.orders.is_empty() {
            anyhow::bail!("gate batch cancel requires at least one order");
        }
        if command.orders.len() > 20 {
            anyhow::bail!("gate batch cancel supports at most 20 orders");
        }
        let mut order_ids = Vec::with_capacity(command.orders.len());
        for order in &command.orders {
            if order.exchange != ExchangeId::Gate
                || order.exchange_symbol.exchange != ExchangeId::Gate
            {
                anyhow::bail!("gate batch cancel requires gate exchange symbols");
            }
            let order_id = order
                .exchange_order_id
                .as_ref()
                .ok_or_else(|| anyhow!("gate batch cancel requires exchange order ids"))?;
            order_ids.push(Value::String(order_id.clone()));
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Gate,
            PrivateRestMethod::Post,
            "/futures/usdt/batch_cancel_orders",
        )
        .with_body(Value::Array(order_ids)))
    }

    fn get_order(&self, query: &OrderQuery) -> Result<PrivateRestRequestSpec> {
        if query.exchange != ExchangeId::Gate {
            anyhow::bail!("gate query order requires gate exchange symbols");
        }
        ensure_gate_symbol(&query.exchange_symbol, "query order")?;
        let order_id = gate_order_lookup_id(
            query.exchange_order_id.as_deref(),
            query.client_order_id.as_deref(),
        )?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Gate,
            PrivateRestMethod::Get,
            gate_order_path(order_id),
        )
        .with_query("contract", &query.exchange_symbol.symbol))
    }

    fn get_open_orders(&self, symbol: Option<&ExchangeSymbol>) -> Result<PrivateRestRequestSpec> {
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Gate,
            PrivateRestMethod::Get,
            "/futures/usdt/orders",
        )
        .with_query("status", "open");
        if let Some(symbol) = symbol {
            ensure_gate_symbol(symbol, "open orders")?;
            spec = spec.with_query("contract", &symbol.symbol);
        }
        Ok(spec)
    }

    fn get_all_orders(&self, query: &OrderHistoryQuery) -> Result<PrivateRestRequestSpec> {
        if query.exchange != ExchangeId::Gate || query.exchange_symbol.exchange != ExchangeId::Gate
        {
            anyhow::bail!("gate all-orders history requires gate exchange symbols");
        }
        if query.exchange_order_id.is_some() {
            anyhow::bail!("gate all-orders history uses last_id pagination, not exchange order id");
        }
        if query.start_time.is_some() || query.end_time.is_some() {
            anyhow::bail!("gate all-orders history does not expose start/end time parameters");
        }
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Gate,
            PrivateRestMethod::Get,
            "/futures/usdt/orders",
        )
        .with_query("contract", &query.exchange_symbol.symbol)
        .with_query("status", "finished");
        if let Some(limit) = query.limit {
            spec = spec.with_query("limit", limit);
        }
        Ok(spec)
    }

    fn get_fills(&self, query: &FillQuery) -> Result<PrivateRestRequestSpec> {
        if query.exchange != ExchangeId::Gate {
            anyhow::bail!("gate fills requires gate exchange symbols");
        }
        if let Some(symbol) = &query.exchange_symbol {
            ensure_gate_symbol(symbol, "fills")?;
        }
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Gate,
            PrivateRestMethod::Get,
            "/futures/usdt/my_trades",
        );
        if let Some(symbol) = &query.exchange_symbol {
            spec = spec.with_query("contract", &symbol.symbol);
        }
        if let Some(order_id) = &query.exchange_order_id {
            spec = spec.with_query("order", order_id);
        }
        if let Some(last_id) = &query.from_trade_id {
            spec = spec.with_query("last_id", last_id);
        }
        if let Some(limit) = query.limit {
            spec = spec.with_query("limit", limit);
        }
        Ok(spec)
    }

    fn get_positions(&self, symbol: Option<&ExchangeSymbol>) -> Result<PrivateRestRequestSpec> {
        if let Some(symbol) = symbol {
            ensure_gate_symbol(symbol, "positions")?;
        }
        let path = symbol
            .map(|symbol| format!("/futures/usdt/positions/{}", symbol.symbol))
            .unwrap_or_else(|| "/futures/usdt/positions".to_string());
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Gate,
            PrivateRestMethod::Get,
            path,
        ))
    }

    fn get_balances(&self) -> Result<PrivateRestRequestSpec> {
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Gate,
            PrivateRestMethod::Get,
            "/futures/usdt/accounts",
        ))
    }

    fn get_trade_fee(&self, symbol: &ExchangeSymbol) -> Result<PrivateRestRequestSpec> {
        ensure_gate_symbol(symbol, "trade fee")?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Gate,
            PrivateRestMethod::Get,
            format!("/futures/usdt/contracts/{}", symbol.symbol),
        ))
    }

    fn get_symbol_account_config(&self, symbol: &ExchangeSymbol) -> Result<PrivateRestRequestSpec> {
        ensure_gate_symbol(symbol, "symbol account config")?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Gate,
            PrivateRestMethod::Get,
            format!("/futures/usdt/positions/{}", symbol.symbol),
        ))
    }

    fn amend_order(&self, command: &AmendOrderCommand) -> Result<PrivateRestRequestSpec> {
        ensure_gate_command_symbol(&command.exchange, &command.exchange_symbol, "amend")?;
        let order_id = gate_order_lookup_id(
            command.exchange_order_id.as_deref(),
            command.client_order_id.as_deref(),
        )?;
        if command.new_quantity.is_some() {
            anyhow::bail!(
                "gate amend size requires original order side and is intentionally not wired"
            );
        }
        if command.new_price.is_none() && command.new_client_order_id.is_none() {
            anyhow::bail!("gate amend requires new price or new client order id");
        }
        let mut body = json!({});
        if let Some(price) = command.new_price {
            set_str(&mut body, "price", number_string(price));
        }
        if let Some(client_order_id) = &command.new_client_order_id {
            set_str(&mut body, "amend_text", gate_client_text(client_order_id));
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Gate,
            PrivateRestMethod::Patch,
            gate_order_path(order_id),
        )
        .with_query("contract", &command.exchange_symbol.symbol)
        .with_body(body))
    }

    fn set_leverage(&self, command: &LeverageCommand) -> Result<PrivateRestRequestSpec> {
        ensure_gate_command_symbol(&command.exchange, &command.exchange_symbol, "set leverage")?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Gate,
            PrivateRestMethod::Post,
            format!(
                "/futures/usdt/positions/{}/leverage",
                command.exchange_symbol.symbol
            ),
        )
        .with_query("leverage", command.leverage))
    }

    fn set_position_mode(&self, command: &PositionModeCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Gate {
            anyhow::bail!("gate position mode requires gate exchange");
        }
        anyhow::bail!(
            "gate futures adapter does not support position mode changes; requested {:?}",
            command.mode
        )
    }

    fn set_countdown_cancel_all(
        &self,
        command: &CountdownCancelAllCommand,
    ) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Gate {
            anyhow::bail!("gate countdown cancel-all requires gate exchange symbols");
        }
        let mut body = json!({
            "timeout": command.timeout_secs,
        });
        if let Some(symbol) = &command.exchange_symbol {
            ensure_gate_symbol(symbol, "countdown cancel-all")?;
            set_str(&mut body, "contract", &symbol.symbol);
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Gate,
            PrivateRestMethod::Post,
            "/futures/usdt/countdown_cancel_all",
        )
        .with_body(body))
    }

    fn ws_login(&self, auth: &PrivateWsAuth, timestamp: i64) -> Result<PrivateWsRequest> {
        let channel = "futures.orders";
        let sign = gate_ws_signature(&auth.api_secret, channel, "subscribe", timestamp);
        Ok(PrivateWsRequest {
            exchange: ExchangeId::Gate,
            url: PrivatePerpExchange::Gate.private_ws_url().to_string(),
            message: json!({
                "time": timestamp,
                "channel": channel,
                "event": "subscribe",
                "payload": [],
                "auth": {"method": "api_key", "KEY": auth.api_key, "SIGN": sign}
            })
            .to_string(),
        })
    }

    fn ws_subscribe(
        &self,
        subscription: &PrivateWsSubscription,
        timestamp: i64,
    ) -> Result<PrivateWsRequest> {
        let auth = subscription
            .auth
            .as_ref()
            .ok_or_else(|| anyhow!("gate private websocket subscriptions require auth"))?;
        let account_id = auth
            .account_id
            .as_ref()
            .ok_or_else(|| anyhow!("gate private websocket subscriptions require account_id"))?;
        let sign = gate_ws_signature(
            &auth.api_secret,
            &subscription.channel,
            "subscribe",
            timestamp,
        );
        let payload =
            gate_private_ws_payload(&subscription.channel, account_id, &subscription.symbols);
        Ok(PrivateWsRequest {
            exchange: ExchangeId::Gate,
            url: PrivatePerpExchange::Gate.private_ws_url().to_string(),
            message: json!({
                "time": timestamp,
                "channel": subscription.channel,
                "event": "subscribe",
                "payload": payload,
                "auth": {"method": "api_key", "KEY": auth.api_key, "SIGN": sign}
            })
            .to_string(),
        })
    }

    fn parse_private_ws_message(
        &self,
        raw: &str,
        received_at: DateTime<Utc>,
    ) -> Result<Vec<PrivateEvent>> {
        parse_gate_private_message(raw, received_at)
    }
}

impl PrivatePerpProtocol for BybitPrivatePerpProtocol {
    fn exchange(&self) -> PrivatePerpExchange {
        PrivatePerpExchange::Bybit
    }

    fn place_order(
        &self,
        command: &OrderCommand,
        position_mode: PositionMode,
    ) -> Result<PrivateRestRequestSpec> {
        ensure_bybit_command_symbol(&command.exchange, &command.exchange_symbol, "place order")?;
        let mut body = json!({
            "category": "linear",
            "symbol": command.exchange_symbol.symbol,
            "side": bybit_side(command.side),
            "orderType": bybit_order_type(command.order_type),
            "qty": number_string(command.quantity),
            "orderLinkId": command.client_order_id,
            "timeInForce": bybit_time_in_force(command.time_in_force, command.post_only),
        });
        if let Some(price) = command.price {
            set_str(&mut body, "price", number_string(price));
        }
        if command.reduce_only {
            set_bool(&mut body, "reduceOnly", true);
        }
        if position_mode.is_hedge() {
            set_i64(
                &mut body,
                "positionIdx",
                bybit_position_idx(command.position_side, command.side),
            );
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Bybit,
            PrivateRestMethod::Post,
            "/v5/order/create",
        )
        .with_body(body))
    }

    fn place_batch_orders(
        &self,
        command: &BatchPlaceCommand,
        position_mode: PositionMode,
    ) -> Result<PrivateRestRequestSpec> {
        if command.orders.is_empty() {
            anyhow::bail!("bybit batch-place requires at least one order");
        }
        if command.orders.len() > 20 {
            anyhow::bail!("bybit linear batch-place supports at most 20 orders");
        }
        if command.exchange != ExchangeId::Bybit {
            anyhow::bail!("bybit batch-place requires bybit exchange symbols");
        }
        if command.orders.iter().any(|order| {
            order.exchange != ExchangeId::Bybit
                || order.exchange_symbol.exchange != ExchangeId::Bybit
        }) {
            anyhow::bail!("bybit batch-place requires bybit exchange symbols");
        }
        let requests = command
            .orders
            .iter()
            .map(|order| bybit_batch_order_item(order, position_mode))
            .collect::<Vec<_>>();
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Bybit,
            PrivateRestMethod::Post,
            "/v5/order/create-batch",
        )
        .with_body(json!({"category": "linear", "request": requests})))
    }

    fn cancel_order(&self, command: &CancelCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Bybit
            || command.exchange_symbol.exchange != ExchangeId::Bybit
        {
            anyhow::bail!("bybit cancel requires bybit exchange symbols");
        }
        if command.exchange_order_id.is_none() && command.client_order_id.is_none() {
            anyhow::bail!("bybit cancel requires orderId or orderLinkId");
        }
        let mut body = json!({
            "category": "linear",
            "symbol": command.exchange_symbol.symbol,
        });
        if let Some(order_id) = &command.exchange_order_id {
            set_str(&mut body, "orderId", order_id);
        }
        if let Some(client_order_id) = &command.client_order_id {
            set_str(&mut body, "orderLinkId", client_order_id);
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Bybit,
            PrivateRestMethod::Post,
            "/v5/order/cancel",
        )
        .with_body(body))
    }

    fn cancel_all_orders(&self, command: &CancelAllCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Bybit {
            anyhow::bail!("bybit cancel-all requires bybit exchange symbols");
        }
        let mut body = json!({"category": "linear"});
        if let Some(symbol) = &command.exchange_symbol {
            ensure_bybit_symbol(symbol, "cancel-all")?;
            set_str(&mut body, "symbol", &symbol.symbol);
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Bybit,
            PrivateRestMethod::Post,
            "/v5/order/cancel-all",
        )
        .with_body(body))
    }

    fn cancel_batch_orders(&self, command: &CancelBatchCommand) -> Result<PrivateRestRequestSpec> {
        if command.orders.is_empty() {
            anyhow::bail!("bybit batch cancel requires at least one order");
        }
        if command.orders.len() > 20 {
            anyhow::bail!("bybit linear batch cancel supports at most 20 orders");
        }
        if command.exchange != ExchangeId::Bybit {
            anyhow::bail!("bybit batch cancel requires bybit exchange symbols");
        }
        for order in &command.orders {
            if order.exchange != ExchangeId::Bybit
                || order.exchange_symbol.exchange != ExchangeId::Bybit
            {
                anyhow::bail!("bybit batch cancel requires bybit exchange symbols");
            }
            if order.exchange_order_id.is_none() && order.client_order_id.is_none() {
                anyhow::bail!("bybit batch cancel requires orderId or orderLinkId for every order");
            }
        }
        let list = command
            .orders
            .iter()
            .map(|order| {
                let mut item = json!({"symbol": order.exchange_symbol.symbol});
                if let Some(order_id) = &order.exchange_order_id {
                    set_str(&mut item, "orderId", order_id);
                }
                if let Some(client_order_id) = &order.client_order_id {
                    set_str(&mut item, "orderLinkId", client_order_id);
                }
                item
            })
            .collect::<Vec<_>>();
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Bybit,
            PrivateRestMethod::Post,
            "/v5/order/cancel-batch",
        )
        .with_body(json!({"category": "linear", "request": list})))
    }

    fn get_order(&self, query: &OrderQuery) -> Result<PrivateRestRequestSpec> {
        ensure_bybit_symbol(&query.exchange_symbol, "query order")?;
        if query.exchange != ExchangeId::Bybit {
            anyhow::bail!("bybit query order requires bybit exchange symbols");
        }
        if query.exchange_order_id.is_none() && query.client_order_id.is_none() {
            anyhow::bail!("bybit query order requires orderId or orderLinkId");
        }
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Bybit,
            PrivateRestMethod::Get,
            "/v5/order/realtime",
        )
        .with_query("category", "linear")
        .with_query("symbol", &query.exchange_symbol.symbol);
        if let Some(order_id) = &query.exchange_order_id {
            spec = spec.with_query("orderId", order_id);
        }
        if let Some(client_order_id) = &query.client_order_id {
            spec = spec.with_query("orderLinkId", client_order_id);
        }
        Ok(spec)
    }

    fn get_open_orders(&self, symbol: Option<&ExchangeSymbol>) -> Result<PrivateRestRequestSpec> {
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Bybit,
            PrivateRestMethod::Get,
            "/v5/order/realtime",
        )
        .with_query("category", "linear")
        .with_query("openOnly", "0");
        if let Some(symbol) = symbol {
            ensure_bybit_symbol(symbol, "open orders")?;
            spec = spec.with_query("symbol", &symbol.symbol);
        }
        Ok(spec)
    }

    fn get_all_orders(&self, query: &OrderHistoryQuery) -> Result<PrivateRestRequestSpec> {
        if query.exchange != ExchangeId::Bybit
            || query.exchange_symbol.exchange != ExchangeId::Bybit
        {
            anyhow::bail!("bybit all-orders history requires bybit exchange symbols");
        }
        if let Some(limit) = query.limit {
            if limit > 50 {
                anyhow::bail!("bybit all-orders history limit must be <= 50");
            }
        }
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Bybit,
            PrivateRestMethod::Get,
            "/v5/order/history",
        )
        .with_query("category", "linear")
        .with_query("symbol", &query.exchange_symbol.symbol);
        if let Some(order_id) = &query.exchange_order_id {
            spec = spec.with_query("orderId", order_id);
        }
        if let Some(start_time) = query.start_time {
            spec = spec.with_query("startTime", start_time.timestamp_millis());
        }
        if let Some(end_time) = query.end_time {
            spec = spec.with_query("endTime", end_time.timestamp_millis());
        }
        if let Some(limit) = query.limit {
            spec = spec.with_query("limit", limit);
        }
        Ok(spec)
    }

    fn get_fills(&self, query: &FillQuery) -> Result<PrivateRestRequestSpec> {
        if query.exchange != ExchangeId::Bybit {
            anyhow::bail!("bybit fills require bybit exchange symbols");
        }
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Bybit,
            PrivateRestMethod::Get,
            "/v5/execution/list",
        )
        .with_query("category", "linear");
        if let Some(symbol) = &query.exchange_symbol {
            ensure_bybit_symbol(symbol, "fills")?;
            spec = spec.with_query("symbol", &symbol.symbol);
        }
        if let Some(order_id) = &query.exchange_order_id {
            spec = spec.with_query("orderId", order_id);
        }
        if let Some(client_order_id) = &query.client_order_id {
            spec = spec.with_query("orderLinkId", client_order_id);
        }
        if let Some(start_time) = query.start_time {
            spec = spec.with_query("startTime", start_time.timestamp_millis());
        }
        if let Some(end_time) = query.end_time {
            spec = spec.with_query("endTime", end_time.timestamp_millis());
        }
        if let Some(limit) = query.limit {
            spec = spec.with_query("limit", limit);
        }
        Ok(spec)
    }

    fn get_positions(&self, symbol: Option<&ExchangeSymbol>) -> Result<PrivateRestRequestSpec> {
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Bybit,
            PrivateRestMethod::Get,
            "/v5/position/list",
        )
        .with_query("category", "linear")
        .with_query("settleCoin", "USDT");
        if let Some(symbol) = symbol {
            ensure_bybit_symbol(symbol, "positions")?;
            spec = spec.with_query("symbol", &symbol.symbol);
        }
        Ok(spec)
    }

    fn get_balances(&self) -> Result<PrivateRestRequestSpec> {
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Bybit,
            PrivateRestMethod::Get,
            "/v5/account/wallet-balance",
        )
        .with_query("accountType", "UNIFIED")
        .with_query("coin", "USDT"))
    }

    fn get_trade_fee(&self, symbol: &ExchangeSymbol) -> Result<PrivateRestRequestSpec> {
        ensure_bybit_symbol(symbol, "trade fee")?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Bybit,
            PrivateRestMethod::Get,
            "/v5/account/fee-rate",
        )
        .with_query("category", "linear")
        .with_query("symbol", &symbol.symbol))
    }

    fn get_symbol_account_config(&self, symbol: &ExchangeSymbol) -> Result<PrivateRestRequestSpec> {
        self.get_positions(Some(symbol))
    }

    fn amend_order(&self, command: &AmendOrderCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Bybit
            || command.exchange_symbol.exchange != ExchangeId::Bybit
        {
            anyhow::bail!("bybit amend requires bybit exchange symbols");
        }
        if command.new_client_order_id.is_some() {
            anyhow::bail!("bybit amend does not replace orderLinkId");
        }
        if command.exchange_order_id.is_none() && command.client_order_id.is_none() {
            anyhow::bail!("bybit amend requires orderId or orderLinkId");
        }
        if command.new_quantity.is_none() && command.new_price.is_none() {
            anyhow::bail!("bybit amend requires new quantity or new price");
        }
        let mut body = json!({
            "category": "linear",
            "symbol": command.exchange_symbol.symbol,
        });
        if let Some(order_id) = &command.exchange_order_id {
            set_str(&mut body, "orderId", order_id);
        }
        if let Some(client_order_id) = &command.client_order_id {
            set_str(&mut body, "orderLinkId", client_order_id);
        }
        if let Some(quantity) = command.new_quantity {
            set_str(&mut body, "qty", number_string(quantity));
        }
        if let Some(price) = command.new_price {
            set_str(&mut body, "price", number_string(price));
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Bybit,
            PrivateRestMethod::Post,
            "/v5/order/amend",
        )
        .with_body(body))
    }

    fn set_leverage(&self, command: &LeverageCommand) -> Result<PrivateRestRequestSpec> {
        ensure_bybit_command_symbol(&command.exchange, &command.exchange_symbol, "set leverage")?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Bybit,
            PrivateRestMethod::Post,
            "/v5/position/set-leverage",
        )
        .with_body(json!({
            "category": "linear",
            "symbol": command.exchange_symbol.symbol,
            "buyLeverage": command.leverage.to_string(),
            "sellLeverage": command.leverage.to_string(),
        })))
    }

    fn set_position_mode(&self, command: &PositionModeCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Bybit {
            anyhow::bail!("bybit position mode requires bybit exchange");
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Bybit,
            PrivateRestMethod::Post,
            "/v5/position/switch-mode",
        )
        .with_body(json!({
            "category": "linear",
            "coin": "USDT",
            "mode": if command.mode.is_hedge() { 3 } else { 0 },
        })))
    }

    fn set_countdown_cancel_all(
        &self,
        command: &CountdownCancelAllCommand,
    ) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Bybit {
            anyhow::bail!("bybit disconnected cancel-all requires bybit exchange symbols");
        }
        let mut body = json!({
            "timeWindow": command.timeout_secs,
        });
        if let Some(symbol) = &command.exchange_symbol {
            ensure_bybit_symbol(symbol, "disconnected cancel-all")?;
            set_str(&mut body, "symbol", &symbol.symbol);
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Bybit,
            PrivateRestMethod::Post,
            "/v5/order/disconnected-cancel-all",
        )
        .with_body(body))
    }

    fn ws_login(&self, auth: &PrivateWsAuth, timestamp: i64) -> Result<PrivateWsRequest> {
        let expires = timestamp_ms_from_any(timestamp) + 10_000;
        let sign = bybit_ws_signature(&auth.api_secret, expires);
        Ok(PrivateWsRequest {
            exchange: ExchangeId::Bybit,
            url: PrivatePerpExchange::Bybit.private_ws_url().to_string(),
            message: json!({
                "op": "auth",
                "args": [auth.api_key, expires, sign]
            })
            .to_string(),
        })
    }

    fn ws_subscribe(
        &self,
        subscription: &PrivateWsSubscription,
        _timestamp: i64,
    ) -> Result<PrivateWsRequest> {
        Ok(PrivateWsRequest {
            exchange: ExchangeId::Bybit,
            url: PrivatePerpExchange::Bybit.private_ws_url().to_string(),
            message: json!({"op": "subscribe", "args": [subscription.channel]}).to_string(),
        })
    }

    fn parse_private_ws_message(
        &self,
        raw: &str,
        received_at: DateTime<Utc>,
    ) -> Result<Vec<PrivateEvent>> {
        parse_bybit_private_message(raw, received_at)
    }
}

impl PrivatePerpProtocol for MexcPrivatePerpProtocol {
    fn exchange(&self) -> PrivatePerpExchange {
        PrivatePerpExchange::Mexc
    }

    fn place_order(
        &self,
        command: &OrderCommand,
        position_mode: PositionMode,
    ) -> Result<PrivateRestRequestSpec> {
        ensure_mexc_command_symbol(&command.exchange, &command.exchange_symbol, "place order")?;
        let mut body = json!({
            "symbol": command.exchange_symbol.symbol,
            "side": mexc_order_side(command.side, command.reduce_only),
            "type": mexc_order_type(command.order_type, command.time_in_force, command.post_only),
            "vol": number_string(command.quantity),
            "openType": 2,
            "externalOid": command.client_order_id,
        });
        if let Some(price) = command.price {
            set_str(&mut body, "price", number_string(price));
        }
        if position_mode.is_hedge() {
            set_str(&mut body, "positionMode", "hedge_mode");
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Mexc,
            PrivateRestMethod::Post,
            "/api/v1/private/order/submit",
        )
        .with_body(body))
    }

    fn place_batch_orders(
        &self,
        command: &BatchPlaceCommand,
        position_mode: PositionMode,
    ) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Mexc {
            anyhow::bail!("mexc batch-place requires mexc exchange symbols");
        }
        if command.orders.is_empty() {
            anyhow::bail!("mexc batch-place requires at least one order");
        }
        if command.orders.len() > 50 {
            anyhow::bail!("mexc batch-place supports at most 50 orders");
        }
        let mut orders = Vec::with_capacity(command.orders.len());
        for order in &command.orders {
            if order.exchange != ExchangeId::Mexc
                || order.exchange_symbol.exchange != ExchangeId::Mexc
            {
                anyhow::bail!("mexc batch-place requires mexc exchange symbols");
            }
            let mut body = json!({
                "symbol": order.exchange_symbol.symbol,
                "side": mexc_order_side(order.side, order.reduce_only),
                "type": mexc_order_type(order.order_type, order.time_in_force, order.post_only),
                "vol": number_string(order.quantity),
                "openType": 2,
                "externalOid": order.client_order_id,
            });
            if let Some(price) = order.price {
                set_str(&mut body, "price", number_string(price));
            }
            if position_mode.is_hedge() {
                set_str(&mut body, "positionMode", "hedge_mode");
            }
            orders.push(body);
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Mexc,
            PrivateRestMethod::Post,
            "/api/v1/private/order/submit_batch",
        )
        .with_body(Value::Array(orders)))
    }

    fn cancel_order(&self, command: &CancelCommand) -> Result<PrivateRestRequestSpec> {
        ensure_mexc_command_symbol(&command.exchange, &command.exchange_symbol, "cancel")?;
        let mut body = json!({});
        if let Some(order_id) = &command.exchange_order_id {
            set_str(&mut body, "orderId", order_id);
        }
        if let Some(client_order_id) = &command.client_order_id {
            set_str(&mut body, "externalOid", client_order_id);
            set_str(&mut body, "symbol", &command.exchange_symbol.symbol);
        }
        if command.exchange_order_id.is_none() && command.client_order_id.is_none() {
            anyhow::bail!("mexc cancel requires orderId or externalOid");
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Mexc,
            PrivateRestMethod::Post,
            "/api/v1/private/order/cancel",
        )
        .with_body(body))
    }

    fn cancel_all_orders(&self, command: &CancelAllCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Mexc {
            anyhow::bail!("mexc cancel-all requires mexc exchange symbols");
        }
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Mexc,
            PrivateRestMethod::Post,
            "/api/v1/private/order/cancel_all",
        );
        if let Some(symbol) = &command.exchange_symbol {
            ensure_mexc_symbol(symbol, "cancel-all")?;
            spec = spec.with_query("symbol", &symbol.symbol);
        }
        Ok(spec)
    }

    fn cancel_batch_orders(&self, command: &CancelBatchCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Mexc {
            anyhow::bail!("mexc batch cancel requires mexc exchange symbols");
        }
        if command.orders.is_empty() {
            anyhow::bail!("mexc batch cancel requires at least one order");
        }
        for order in &command.orders {
            ensure_mexc_command_symbol(&order.exchange, &order.exchange_symbol, "batch cancel")?;
            if order.exchange_order_id.is_none() {
                anyhow::bail!("mexc batch cancel requires exchange order ids for every order");
            }
        }
        let order_ids = command
            .orders
            .iter()
            .filter_map(|order| order.exchange_order_id.clone())
            .collect::<Vec<_>>();
        if order_ids.is_empty() {
            anyhow::bail!("mexc batch cancel requires exchange order ids");
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Mexc,
            PrivateRestMethod::Post,
            "/api/v1/private/order/cancel",
        )
        .with_body(json!(order_ids)))
    }

    fn get_order(&self, query: &OrderQuery) -> Result<PrivateRestRequestSpec> {
        if query.exchange != ExchangeId::Mexc {
            anyhow::bail!("mexc query order requires mexc exchange symbols");
        }
        ensure_mexc_symbol(&query.exchange_symbol, "query order")?;
        if let Some(order_id) = &query.exchange_order_id {
            return Ok(PrivateRestRequestSpec::new(
                ExchangeId::Mexc,
                PrivateRestMethod::Get,
                format!("/api/v1/private/order/get/{order_id}"),
            ));
        }
        let client_order_id = query
            .client_order_id
            .as_ref()
            .ok_or_else(|| anyhow!("mexc order query requires orderId or externalOid"))?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Mexc,
            PrivateRestMethod::Get,
            format!(
                "/api/v1/private/order/external/{}/{}",
                query.exchange_symbol.symbol, client_order_id
            ),
        ))
    }

    fn get_open_orders(&self, symbol: Option<&ExchangeSymbol>) -> Result<PrivateRestRequestSpec> {
        let symbol = symbol.ok_or_else(|| anyhow!("mexc open orders require symbol"))?;
        ensure_mexc_symbol(symbol, "open orders")?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Mexc,
            PrivateRestMethod::Get,
            format!("/api/v1/private/order/list/open_orders/{}", symbol.symbol),
        ))
    }

    fn get_all_orders(&self, query: &OrderHistoryQuery) -> Result<PrivateRestRequestSpec> {
        if query.exchange != ExchangeId::Mexc || query.exchange_symbol.exchange != ExchangeId::Mexc
        {
            anyhow::bail!("mexc all-orders history requires mexc exchange symbols");
        }
        if let Some(order_id) = &query.exchange_order_id {
            if query.start_time.is_some() || query.end_time.is_some() || query.limit.is_some() {
                anyhow::bail!(
                    "mexc all-orders history by order id cannot combine time or limit filters"
                );
            }
            return Ok(PrivateRestRequestSpec::new(
                ExchangeId::Mexc,
                PrivateRestMethod::Get,
                format!("/api/v1/private/order/get/{order_id}"),
            ));
        }
        if let Some(limit) = query.limit {
            if limit > 100 {
                anyhow::bail!("mexc all-orders history limit must be <= 100");
            }
        }
        if let (Some(start_time), Some(end_time)) = (query.start_time, query.end_time) {
            if end_time < start_time {
                anyhow::bail!("mexc all-orders history end_time must be after start_time");
            }
            if end_time
                .signed_duration_since(start_time)
                .num_milliseconds()
                > 90 * 24 * 60 * 60 * 1_000
            {
                anyhow::bail!("mexc all-orders history time span must be <= 90 days");
            }
        }
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Mexc,
            PrivateRestMethod::Get,
            "/api/v1/private/order/list/history_orders",
        )
        .with_query("symbol", &query.exchange_symbol.symbol)
        .with_query("page_num", 1);
        if let Some(start_time) = query.start_time {
            spec = spec.with_query("start_time", start_time.timestamp_millis());
        }
        if let Some(end_time) = query.end_time {
            spec = spec.with_query("end_time", end_time.timestamp_millis());
        }
        if let Some(limit) = query.limit {
            spec = spec.with_query("page_size", limit);
        }
        Ok(spec)
    }

    fn get_fills(&self, query: &FillQuery) -> Result<PrivateRestRequestSpec> {
        if query.exchange != ExchangeId::Mexc {
            anyhow::bail!("mexc fills require mexc exchange symbols");
        }
        let symbol = query
            .exchange_symbol
            .as_ref()
            .ok_or_else(|| anyhow!("mexc fills require symbol"))?;
        ensure_mexc_symbol(symbol, "fills")?;
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Mexc,
            PrivateRestMethod::Get,
            format!("/api/v1/private/order/list/order_deals/{}", symbol.symbol),
        );
        if let Some(order_id) = &query.exchange_order_id {
            spec = spec.with_query("order_id", order_id);
        }
        if let Some(limit) = query.limit {
            spec = spec.with_query("page_size", limit);
        }
        Ok(spec)
    }

    fn get_positions(&self, symbol: Option<&ExchangeSymbol>) -> Result<PrivateRestRequestSpec> {
        if let Some(symbol) = symbol {
            ensure_mexc_symbol(symbol, "positions")?;
        }
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Mexc,
            PrivateRestMethod::Get,
            "/api/v1/private/position/open_positions",
        );
        if let Some(symbol) = symbol {
            spec = spec.with_query("symbol", &symbol.symbol);
        }
        Ok(spec)
    }

    fn get_balances(&self) -> Result<PrivateRestRequestSpec> {
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Mexc,
            PrivateRestMethod::Get,
            "/api/v1/private/account/assets",
        ))
    }

    fn get_trade_fee(&self, symbol: &ExchangeSymbol) -> Result<PrivateRestRequestSpec> {
        ensure_mexc_symbol(symbol, "trade fee")?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Mexc,
            PrivateRestMethod::Get,
            "/api/v1/private/account/tiered_fee_rate",
        )
        .with_query("symbol", &symbol.symbol))
    }

    fn get_symbol_account_config(&self, symbol: &ExchangeSymbol) -> Result<PrivateRestRequestSpec> {
        ensure_mexc_symbol(symbol, "symbol account config")?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Mexc,
            PrivateRestMethod::Get,
            "/api/v1/private/position/leverage",
        )
        .with_query("symbol", &symbol.symbol))
    }

    fn amend_order(&self, command: &AmendOrderCommand) -> Result<PrivateRestRequestSpec> {
        ensure_mexc_command_symbol(&command.exchange, &command.exchange_symbol, "amend")?;
        let order_id = command
            .exchange_order_id
            .as_ref()
            .ok_or_else(|| anyhow!("mexc amend requires exchange order id"))?;
        if command.new_price.is_none() && command.new_quantity.is_none() {
            anyhow::bail!("mexc amend requires new price or new quantity");
        }
        let mut body = json!({"orderId": order_id});
        if let Some(price) = command.new_price {
            set_str(&mut body, "price", number_string(price));
        }
        if let Some(quantity) = command.new_quantity {
            set_str(&mut body, "vol", number_string(quantity));
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Mexc,
            PrivateRestMethod::Post,
            "/api/v1/private/order/change_order_price",
        )
        .with_body(body))
    }

    fn set_leverage(&self, command: &LeverageCommand) -> Result<PrivateRestRequestSpec> {
        ensure_mexc_command_symbol(&command.exchange, &command.exchange_symbol, "set leverage")?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Mexc,
            PrivateRestMethod::Post,
            "/api/v1/private/position/change_leverage",
        )
        .with_body(json!({
            "symbol": command.exchange_symbol.symbol,
            "leverage": command.leverage,
            "openType": 2,
        })))
    }

    fn set_position_mode(&self, command: &PositionModeCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Mexc {
            anyhow::bail!("mexc position mode requires mexc exchange");
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Mexc,
            PrivateRestMethod::Post,
            "/api/v1/private/position/change_position_mode",
        )
        .with_body(json!({
            "positionMode": if command.mode.is_hedge() { 1 } else { 2 },
        })))
    }

    fn set_countdown_cancel_all(
        &self,
        command: &CountdownCancelAllCommand,
    ) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Mexc {
            anyhow::bail!("mexc countdown cancel-all requires mexc exchange symbols");
        }
        if let Some(symbol) = &command.exchange_symbol {
            ensure_mexc_symbol(symbol, "countdown cancel-all")?;
        }
        anyhow::bail!("mexc countdown cancel-all is not supported by this adapter")
    }

    fn ws_login(&self, auth: &PrivateWsAuth, timestamp: i64) -> Result<PrivateWsRequest> {
        let timestamp = timestamp_ms_from_any(timestamp);
        let sign = mexc_ws_signature(&auth.api_key, &auth.api_secret, timestamp);
        Ok(PrivateWsRequest {
            exchange: ExchangeId::Mexc,
            url: PrivatePerpExchange::Mexc.private_ws_url().to_string(),
            message: json!({
                "method": "login",
                "param": {
                    "apiKey": auth.api_key,
                    "reqTime": timestamp,
                    "signature": sign
                }
            })
            .to_string(),
        })
    }

    fn ws_subscribe(
        &self,
        subscription: &PrivateWsSubscription,
        _timestamp: i64,
    ) -> Result<PrivateWsRequest> {
        Ok(PrivateWsRequest {
            exchange: ExchangeId::Mexc,
            url: PrivatePerpExchange::Mexc.private_ws_url().to_string(),
            message: json!({"method": format!("sub.personal.{}", subscription.channel)})
                .to_string(),
        })
    }

    fn parse_private_ws_message(
        &self,
        raw: &str,
        received_at: DateTime<Utc>,
    ) -> Result<Vec<PrivateEvent>> {
        parse_mexc_private_message(raw, received_at)
    }
}

impl PrivatePerpProtocol for HtxPrivatePerpProtocol {
    fn exchange(&self) -> PrivatePerpExchange {
        PrivatePerpExchange::Htx
    }

    fn place_order(
        &self,
        command: &OrderCommand,
        _position_mode: PositionMode,
    ) -> Result<PrivateRestRequestSpec> {
        ensure_htx_command_symbol(&command.exchange, &command.exchange_symbol, "place order")?;
        let body = json!({
            "contract_code": command.exchange_symbol.symbol,
            "client_order_id": htx_client_order_id(&command.client_order_id),
            "price": command.price.map(number_string),
            "volume": number_string(command.quantity),
            "direction": htx_direction(command.side),
            "offset": if command.reduce_only { "close" } else { "open" },
            "lever_rate": 1,
            "order_price_type": htx_order_price_type(command.order_type, command.time_in_force, command.post_only),
        });
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Htx,
            PrivateRestMethod::Post,
            "/linear-swap-api/v1/swap_cross_order",
        )
        .with_body(body))
    }

    fn place_batch_orders(
        &self,
        command: &BatchPlaceCommand,
        _position_mode: PositionMode,
    ) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Htx {
            anyhow::bail!("htx batch-place requires htx exchange symbols");
        }
        if command.orders.is_empty() {
            anyhow::bail!("htx batch-place requires at least one order");
        }
        if command.orders.len() > 25 {
            anyhow::bail!("htx cross batch-place supports at most 25 orders");
        }
        if command.orders.iter().any(|order| {
            order.exchange != ExchangeId::Htx || order.exchange_symbol.exchange != ExchangeId::Htx
        }) {
            anyhow::bail!("htx batch-place requires htx exchange symbols");
        }
        let orders_data = command
            .orders
            .iter()
            .map(htx_batch_order_item)
            .collect::<Vec<_>>();
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Htx,
            PrivateRestMethod::Post,
            "/linear-swap-api/v1/swap_cross_batchorder",
        )
        .with_body(json!({"orders_data": orders_data})))
    }

    fn cancel_order(&self, command: &CancelCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Htx
            || command.exchange_symbol.exchange != ExchangeId::Htx
        {
            anyhow::bail!("htx cancel requires htx exchange symbols");
        }
        if command.exchange_order_id.is_none() && command.client_order_id.is_none() {
            anyhow::bail!("htx cancel requires order_id or client_order_id");
        }
        let mut body = json!({"contract_code": command.exchange_symbol.symbol});
        if let Some(order_id) = &command.exchange_order_id {
            set_str(&mut body, "order_id", order_id);
        }
        if let Some(client_order_id) = &command.client_order_id {
            set_str(
                &mut body,
                "client_order_id",
                htx_client_order_id(client_order_id),
            );
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Htx,
            PrivateRestMethod::Post,
            "/linear-swap-api/v1/swap_cross_cancel",
        )
        .with_body(body))
    }

    fn cancel_all_orders(&self, command: &CancelAllCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Htx {
            anyhow::bail!("htx cancel-all requires htx exchange symbols");
        }
        let mut body = json!({});
        if let Some(symbol) = &command.exchange_symbol {
            ensure_htx_symbol(symbol, "cancel-all")?;
            set_str(&mut body, "contract_code", &symbol.symbol);
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Htx,
            PrivateRestMethod::Post,
            "/linear-swap-api/v1/swap_cross_cancelall",
        )
        .with_body(body))
    }

    fn cancel_batch_orders(&self, command: &CancelBatchCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Htx {
            anyhow::bail!("htx batch cancel requires htx exchange symbols");
        }
        let first = command
            .orders
            .first()
            .ok_or_else(|| anyhow!("htx batch cancel requires at least one order"))?;
        if first.exchange != ExchangeId::Htx || first.exchange_symbol.exchange != ExchangeId::Htx {
            anyhow::bail!("htx batch cancel requires htx exchange symbols");
        }
        for order in &command.orders {
            if order.exchange != ExchangeId::Htx
                || order.exchange_symbol.exchange != ExchangeId::Htx
            {
                anyhow::bail!("htx batch cancel requires htx exchange symbols");
            }
            if order.exchange_symbol.symbol != first.exchange_symbol.symbol {
                anyhow::bail!("htx batch cancel requires one contract_code per request");
            }
            if order.exchange_order_id.is_none() && order.client_order_id.is_none() {
                anyhow::bail!(
                    "htx batch cancel requires order_id or client_order_id for every order"
                );
            }
        }
        let order_ids = command
            .orders
            .iter()
            .filter_map(|order| order.exchange_order_id.clone())
            .collect::<Vec<_>>()
            .join(",");
        let client_order_ids = command
            .orders
            .iter()
            .filter_map(|order| {
                order
                    .client_order_id
                    .as_ref()
                    .map(|id| htx_client_order_id(id))
            })
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let mut body = json!({"contract_code": first.exchange_symbol.symbol});
        if !order_ids.is_empty() {
            set_str(&mut body, "order_id", order_ids);
        }
        if !client_order_ids.is_empty() {
            set_str(&mut body, "client_order_id", client_order_ids);
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Htx,
            PrivateRestMethod::Post,
            "/linear-swap-api/v1/swap_cross_cancel",
        )
        .with_body(body))
    }

    fn get_order(&self, query: &OrderQuery) -> Result<PrivateRestRequestSpec> {
        if query.exchange != ExchangeId::Htx {
            anyhow::bail!("htx query order requires htx exchange symbols");
        }
        ensure_htx_symbol(&query.exchange_symbol, "query order")?;
        if query.exchange_order_id.is_none() && query.client_order_id.is_none() {
            anyhow::bail!("htx query order requires order_id or client_order_id");
        }
        let mut body = json!({"contract_code": query.exchange_symbol.symbol});
        if let Some(order_id) = &query.exchange_order_id {
            set_str(&mut body, "order_id", order_id);
        }
        if let Some(client_order_id) = &query.client_order_id {
            set_str(
                &mut body,
                "client_order_id",
                htx_client_order_id(client_order_id),
            );
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Htx,
            PrivateRestMethod::Post,
            "/linear-swap-api/v1/swap_cross_order_info",
        )
        .with_body(body))
    }

    fn get_open_orders(&self, symbol: Option<&ExchangeSymbol>) -> Result<PrivateRestRequestSpec> {
        let mut body = json!({"page_index": 1, "page_size": 50});
        if let Some(symbol) = symbol {
            ensure_htx_symbol(symbol, "open orders")?;
            set_str(&mut body, "contract_code", &symbol.symbol);
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Htx,
            PrivateRestMethod::Post,
            "/linear-swap-api/v1/swap_cross_openorders",
        )
        .with_body(body))
    }

    fn get_all_orders(&self, query: &OrderHistoryQuery) -> Result<PrivateRestRequestSpec> {
        if query.exchange != ExchangeId::Htx || query.exchange_symbol.exchange != ExchangeId::Htx {
            anyhow::bail!("htx all-orders history requires htx exchange symbols");
        }
        if query.exchange_order_id.is_some() {
            anyhow::bail!("htx v3 all-orders history uses from_id, not exchange order id");
        }
        if query.limit.is_some() {
            anyhow::bail!("htx v3 all-orders history does not expose a limit/page-size parameter");
        }
        let mut body = json!({
            "contract": query.exchange_symbol.symbol,
            "trade_type": 0,
            "status": "0",
            "type": 1,
            "direct": "prev",
        });
        if let Some(start_time) = query.start_time {
            set_i64(&mut body, "start_time", start_time.timestamp_millis());
        }
        if let Some(end_time) = query.end_time {
            set_i64(&mut body, "end_time", end_time.timestamp_millis());
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Htx,
            PrivateRestMethod::Post,
            "/linear-swap-api/v3/swap_cross_hisorders",
        )
        .with_body(body))
    }

    fn get_fills(&self, query: &FillQuery) -> Result<PrivateRestRequestSpec> {
        if query.exchange != ExchangeId::Htx {
            anyhow::bail!("htx fills require htx exchange symbols");
        }
        let mut body = json!({"page_index": 1, "page_size": query.limit.unwrap_or(50)});
        if let Some(symbol) = &query.exchange_symbol {
            ensure_htx_symbol(symbol, "fills")?;
            set_str(&mut body, "contract_code", &symbol.symbol);
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Htx,
            PrivateRestMethod::Post,
            "/linear-swap-api/v1/swap_cross_matchresults",
        )
        .with_body(body))
    }

    fn get_positions(&self, symbol: Option<&ExchangeSymbol>) -> Result<PrivateRestRequestSpec> {
        let mut body = json!({});
        if let Some(symbol) = symbol {
            ensure_htx_symbol(symbol, "positions")?;
            set_str(&mut body, "contract_code", &symbol.symbol);
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Htx,
            PrivateRestMethod::Post,
            "/linear-swap-api/v1/swap_cross_position_info",
        )
        .with_body(body))
    }

    fn get_balances(&self) -> Result<PrivateRestRequestSpec> {
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Htx,
            PrivateRestMethod::Post,
            "/linear-swap-api/v1/swap_cross_account_info",
        )
        .with_body(json!({})))
    }

    fn get_trade_fee(&self, symbol: &ExchangeSymbol) -> Result<PrivateRestRequestSpec> {
        ensure_htx_symbol(symbol, "trade fee")?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Htx,
            PrivateRestMethod::Post,
            "/linear-swap-api/v1/swap_fee",
        )
        .with_body(json!({"contract_code": symbol.symbol})))
    }

    fn get_symbol_account_config(&self, symbol: &ExchangeSymbol) -> Result<PrivateRestRequestSpec> {
        ensure_htx_symbol(symbol, "symbol account config")?;
        self.get_positions(Some(symbol))
    }

    fn amend_order(&self, command: &AmendOrderCommand) -> Result<PrivateRestRequestSpec> {
        ensure_htx_command_symbol(&command.exchange, &command.exchange_symbol, "amend")?;
        if command.exchange_order_id.is_none() && command.client_order_id.is_none() {
            anyhow::bail!("htx amend requires order_id or client_order_id");
        }
        if command.new_quantity.is_none() && command.new_price.is_none() {
            anyhow::bail!("htx amend requires new quantity or new price");
        }
        anyhow::bail!("htx linear swap does not expose a verified amend-order endpoint")
    }

    fn set_leverage(&self, command: &LeverageCommand) -> Result<PrivateRestRequestSpec> {
        ensure_htx_command_symbol(&command.exchange, &command.exchange_symbol, "set leverage")?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Htx,
            PrivateRestMethod::Post,
            "/linear-swap-api/v1/swap_cross_switch_lever_rate",
        )
        .with_body(json!({
            "contract_code": command.exchange_symbol.symbol,
            "lever_rate": command.leverage,
        })))
    }

    fn set_position_mode(&self, command: &PositionModeCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Htx {
            anyhow::bail!("htx position mode requires htx exchange");
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Htx,
            PrivateRestMethod::Post,
            "/linear-swap-api/v1/swap_cross_switch_position_mode",
        )
        .with_body(json!({
            "margin_account": "USDT",
            "position_mode": if command.mode.is_hedge() { "dual_side" } else { "single_side" },
        })))
    }

    fn set_countdown_cancel_all(
        &self,
        command: &CountdownCancelAllCommand,
    ) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Htx {
            anyhow::bail!("htx countdown cancel-all requires htx exchange symbols");
        }
        if let Some(symbol) = &command.exchange_symbol {
            ensure_htx_symbol(symbol, "countdown cancel-all")?;
        }
        anyhow::bail!("htx countdown cancel-all is not supported by this adapter")
    }

    fn ws_login(&self, auth: &PrivateWsAuth, timestamp: i64) -> Result<PrivateWsRequest> {
        let timestamp = DateTime::<Utc>::from_timestamp(timestamp_secs_from_any(timestamp), 0)
            .unwrap_or_else(Utc::now)
            .format("%Y-%m-%dT%H:%M:%S")
            .to_string();
        let host = "api.hbdm.com";
        let path = "/linear-swap-notification";
        let signature = htx_signature(&auth.api_secret, "GET", host, path, &timestamp);
        Ok(PrivateWsRequest {
            exchange: ExchangeId::Htx,
            url: PrivatePerpExchange::Htx.private_ws_url().to_string(),
            message: json!({
                "op": "auth",
                "type": "api",
                "AccessKeyId": auth.api_key,
                "SignatureMethod": "HmacSHA256",
                "SignatureVersion": "2",
                "Timestamp": timestamp,
                "Signature": signature,
            })
            .to_string(),
        })
    }

    fn ws_subscribe(
        &self,
        subscription: &PrivateWsSubscription,
        _timestamp: i64,
    ) -> Result<PrivateWsRequest> {
        let topic = match subscription.channel.as_str() {
            "orders" => "orders_cross.*",
            "match_orders" => "match_orders_cross.*",
            "positions" => "positions_cross.*",
            "accounts" => "accounts_cross.*",
            other => other,
        };
        Ok(PrivateWsRequest {
            exchange: ExchangeId::Htx,
            url: PrivatePerpExchange::Htx.private_ws_url().to_string(),
            message: json!({"op": "sub", "topic": topic}).to_string(),
        })
    }

    fn parse_private_ws_message(
        &self,
        raw: &str,
        received_at: DateTime<Utc>,
    ) -> Result<Vec<PrivateEvent>> {
        parse_htx_private_message(raw, received_at)
    }
}

impl PrivatePerpProtocol for ToobitPrivatePerpProtocol {
    fn exchange(&self) -> PrivatePerpExchange {
        PrivatePerpExchange::Toobit
    }

    fn place_order(
        &self,
        command: &OrderCommand,
        position_mode: PositionMode,
    ) -> Result<PrivateRestRequestSpec> {
        ensure_toobit_command_symbol(&command.exchange, &command.exchange_symbol, "place order")?;
        let mut body = json!({
            "symbol": command.exchange_symbol.symbol,
            "side": toobit_side(command.side),
            "positionSide": toobit_position_side(position_mode, command.position_side, command.side),
            "type": toobit_order_type(command.order_type, command.post_only, command.time_in_force),
            "quantity": number_string(command.quantity),
            "newClientOrderId": command.client_order_id,
        });
        if let Some(price) = command.price {
            set_str(&mut body, "price", number_string(price));
        }
        if let Some(tif) = toobit_time_in_force(command.time_in_force, command.post_only) {
            set_str(&mut body, "timeInForce", tif);
        }
        if command.reduce_only {
            set_bool(&mut body, "reduceOnly", true);
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Toobit,
            PrivateRestMethod::Post,
            "/api/v2/futures/order",
        )
        .with_body(body))
    }

    fn place_batch_orders(
        &self,
        command: &BatchPlaceCommand,
        position_mode: PositionMode,
    ) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Toobit {
            anyhow::bail!("toobit batch-place requires toobit exchange symbols");
        }
        if command.orders.is_empty() {
            anyhow::bail!("toobit batch-place requires at least one order");
        }
        let orders = command
            .orders
            .iter()
            .map(|order| {
                ensure_toobit_command_symbol(
                    &order.exchange,
                    &order.exchange_symbol,
                    "batch-place",
                )?;
                let mut body = json!({
                    "symbol": order.exchange_symbol.symbol,
                    "side": toobit_side(order.side),
                    "positionSide": toobit_position_side(position_mode, order.position_side, order.side),
                    "type": toobit_order_type(order.order_type, order.post_only, order.time_in_force),
                    "quantity": number_string(order.quantity),
                    "newClientOrderId": order.client_order_id,
                });
                if let Some(price) = order.price {
                    set_str(&mut body, "price", number_string(price));
                }
                if let Some(tif) = toobit_time_in_force(order.time_in_force, order.post_only) {
                    set_str(&mut body, "timeInForce", tif);
                }
                if order.reduce_only {
                    set_bool(&mut body, "reduceOnly", true);
                }
                Ok(body)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Toobit,
            PrivateRestMethod::Post,
            "/api/v2/futures/batch-orders",
        )
        .with_body(Value::Array(orders)))
    }

    fn cancel_order(&self, command: &CancelCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Toobit
            || command.exchange_symbol.exchange != ExchangeId::Toobit
        {
            anyhow::bail!("toobit cancel requires toobit exchange symbols");
        }
        if command.exchange_order_id.is_none() && command.client_order_id.is_none() {
            anyhow::bail!("toobit cancel requires orderId or origClientOrderId");
        }
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Toobit,
            PrivateRestMethod::Delete,
            "/api/v2/futures/order",
        )
        .with_query("symbol", &command.exchange_symbol.symbol);
        if let Some(order_id) = &command.exchange_order_id {
            spec = spec.with_query("orderId", order_id);
        }
        if let Some(client_order_id) = &command.client_order_id {
            spec = spec.with_query("origClientOrderId", client_order_id);
        }
        Ok(spec)
    }

    fn cancel_all_orders(&self, command: &CancelAllCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Toobit
            || command
                .exchange_symbol
                .as_ref()
                .is_some_and(|symbol| symbol.exchange != ExchangeId::Toobit)
        {
            anyhow::bail!("toobit cancel-all requires toobit exchange symbols");
        }
        let symbol = command
            .exchange_symbol
            .as_ref()
            .ok_or_else(|| anyhow!("toobit cancel-all requires a symbol"))?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Toobit,
            PrivateRestMethod::Delete,
            "/api/v2/futures/batch-orders",
        )
        .with_query("symbol", &symbol.symbol))
    }

    fn cancel_batch_orders(&self, command: &CancelBatchCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Toobit {
            anyhow::bail!("toobit batch cancel requires toobit exchange symbols");
        }
        for order in &command.orders {
            if order.exchange != ExchangeId::Toobit
                || order.exchange_symbol.exchange != ExchangeId::Toobit
            {
                anyhow::bail!("toobit batch cancel requires toobit exchange symbols");
            }
            if order.exchange_order_id.is_none() {
                anyhow::bail!("toobit batch cancel by ids requires orderId for every order");
            }
        }
        let ids = command
            .orders
            .iter()
            .filter_map(|order| order.exchange_order_id.as_deref())
            .collect::<Vec<_>>();
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Toobit,
            PrivateRestMethod::Delete,
            "/api/v1/futures/cancelOrderByIds",
        )
        .with_query("ids", ids.join(",")))
    }

    fn get_order(&self, query: &OrderQuery) -> Result<PrivateRestRequestSpec> {
        if query.exchange != ExchangeId::Toobit
            || query.exchange_symbol.exchange != ExchangeId::Toobit
        {
            anyhow::bail!("toobit query order requires toobit exchange symbols");
        }
        if query.exchange_order_id.is_none() && query.client_order_id.is_none() {
            anyhow::bail!("toobit query order requires orderId or origClientOrderId");
        }
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Toobit,
            PrivateRestMethod::Get,
            "/api/v2/futures/order",
        )
        .with_query("symbol", &query.exchange_symbol.symbol);
        if let Some(order_id) = &query.exchange_order_id {
            spec = spec.with_query("orderId", order_id);
        }
        if let Some(client_order_id) = &query.client_order_id {
            spec = spec.with_query("origClientOrderId", client_order_id);
        }
        Ok(spec)
    }

    fn get_open_orders(&self, symbol: Option<&ExchangeSymbol>) -> Result<PrivateRestRequestSpec> {
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Toobit,
            PrivateRestMethod::Get,
            "/api/v2/futures/open-orders",
        );
        if let Some(symbol) = symbol {
            ensure_toobit_symbol(symbol, "open orders")?;
            spec = spec.with_query("symbol", &symbol.symbol);
        }
        Ok(spec)
    }

    fn get_all_orders(&self, query: &OrderHistoryQuery) -> Result<PrivateRestRequestSpec> {
        if query.exchange != ExchangeId::Toobit
            || query.exchange_symbol.exchange != ExchangeId::Toobit
        {
            anyhow::bail!("toobit all-orders history requires toobit exchange symbols");
        }
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Toobit,
            PrivateRestMethod::Get,
            "/api/v2/futures/history-orders",
        )
        .with_query("symbol", &query.exchange_symbol.symbol);
        if let Some(order_id) = &query.exchange_order_id {
            spec = spec.with_query("orderId", order_id);
        }
        if let Some(start_time) = query.start_time {
            spec = spec.with_query("startTime", start_time.timestamp_millis());
        }
        if let Some(end_time) = query.end_time {
            spec = spec.with_query("endTime", end_time.timestamp_millis());
        }
        if let Some(limit) = query.limit {
            spec = spec.with_query("limit", limit);
        }
        Ok(spec)
    }

    fn get_fills(&self, query: &FillQuery) -> Result<PrivateRestRequestSpec> {
        if query.exchange != ExchangeId::Toobit {
            anyhow::bail!("toobit fills require toobit exchange symbols");
        }
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Toobit,
            PrivateRestMethod::Get,
            "/api/v2/futures/user-trades",
        );
        if let Some(symbol) = &query.exchange_symbol {
            ensure_toobit_symbol(symbol, "fills")?;
            spec = spec.with_query("symbol", &symbol.symbol);
        }
        if let Some(order_id) = &query.exchange_order_id {
            spec = spec.with_query("orderId", order_id);
        }
        if let Some(client_order_id) = &query.client_order_id {
            spec = spec.with_query("clientOrderId", client_order_id);
        }
        if let Some(start_time) = query.start_time {
            spec = spec.with_query("startTime", start_time.timestamp_millis());
        }
        if let Some(end_time) = query.end_time {
            spec = spec.with_query("endTime", end_time.timestamp_millis());
        }
        if let Some(limit) = query.limit {
            spec = spec.with_query("limit", limit);
        }
        Ok(spec)
    }

    fn get_positions(&self, symbol: Option<&ExchangeSymbol>) -> Result<PrivateRestRequestSpec> {
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Toobit,
            PrivateRestMethod::Get,
            "/api/v1/futures/positions",
        );
        if let Some(symbol) = symbol {
            ensure_toobit_symbol(symbol, "positions")?;
            spec = spec.with_query("symbol", &symbol.symbol);
        }
        Ok(spec)
    }

    fn get_balances(&self) -> Result<PrivateRestRequestSpec> {
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Toobit,
            PrivateRestMethod::Get,
            "/api/v1/futures/balance",
        ))
    }

    fn get_trade_fee(&self, symbol: &ExchangeSymbol) -> Result<PrivateRestRequestSpec> {
        ensure_toobit_symbol(symbol, "trade fee")?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Toobit,
            PrivateRestMethod::Get,
            "/api/v1/futures/commissionRate",
        )
        .with_query("symbol", &symbol.symbol))
    }

    fn get_symbol_account_config(&self, symbol: &ExchangeSymbol) -> Result<PrivateRestRequestSpec> {
        ensure_toobit_symbol(symbol, "symbol account config")?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Toobit,
            PrivateRestMethod::Get,
            "/api/v1/futures/accountLeverage",
        )
        .with_query("symbol", &symbol.symbol))
    }

    fn amend_order(&self, command: &AmendOrderCommand) -> Result<PrivateRestRequestSpec> {
        ensure_toobit_command_symbol(&command.exchange, &command.exchange_symbol, "amend")?;
        if command.exchange_order_id.is_none() && command.client_order_id.is_none() {
            anyhow::bail!("toobit amend requires orderId or origClientOrderId");
        }
        if command.new_client_order_id.is_some() {
            anyhow::bail!("toobit amend does not replace client order id");
        }
        if command.new_quantity.is_none() && command.new_price.is_none() {
            anyhow::bail!("toobit amend requires new quantity or new price");
        }
        let mut body = json!({"symbol": command.exchange_symbol.symbol});
        if let Some(order_id) = &command.exchange_order_id {
            set_str(&mut body, "orderId", order_id);
        }
        if let Some(client_order_id) = &command.client_order_id {
            set_str(&mut body, "origClientOrderId", client_order_id);
        }
        if let Some(quantity) = command.new_quantity {
            set_str(&mut body, "quantity", number_string(quantity));
        }
        if let Some(price) = command.new_price {
            set_str(&mut body, "price", number_string(price));
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Toobit,
            PrivateRestMethod::Post,
            "/api/v2/futures/order/update",
        )
        .with_body(body))
    }

    fn set_leverage(&self, command: &LeverageCommand) -> Result<PrivateRestRequestSpec> {
        ensure_toobit_command_symbol(&command.exchange, &command.exchange_symbol, "set leverage")?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Toobit,
            PrivateRestMethod::Post,
            "/api/v2/futures/leverage",
        )
        .with_body(json!({
            "symbol": command.exchange_symbol.symbol,
            "leverage": command.leverage,
        })))
    }

    fn set_position_mode(&self, command: &PositionModeCommand) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Toobit {
            anyhow::bail!("toobit position mode requires toobit exchange");
        }
        anyhow::bail!("toobit position mode switching is not exposed as a verified REST endpoint")
    }

    fn set_countdown_cancel_all(
        &self,
        command: &CountdownCancelAllCommand,
    ) -> Result<PrivateRestRequestSpec> {
        if command.exchange != ExchangeId::Toobit {
            anyhow::bail!("toobit countdown cancel-all requires toobit exchange");
        }
        anyhow::bail!("toobit countdown cancel-all is not supported by this adapter")
    }

    fn ws_login(&self, _auth: &PrivateWsAuth, _timestamp: i64) -> Result<PrivateWsRequest> {
        anyhow::bail!("toobit private websocket uses listenKey URL authentication")
    }

    fn ws_subscribe(
        &self,
        _subscription: &PrivateWsSubscription,
        _timestamp: i64,
    ) -> Result<PrivateWsRequest> {
        Ok(PrivateWsRequest {
            exchange: ExchangeId::Toobit,
            url: PrivatePerpExchange::Toobit.private_ws_url().to_string(),
            message: String::new(),
        })
    }

    fn parse_private_ws_message(
        &self,
        raw: &str,
        received_at: DateTime<Utc>,
    ) -> Result<Vec<PrivateEvent>> {
        parse_toobit_private_message(raw, received_at)
    }
}

pub fn binance_signature(secret: &str, query: &str) -> String {
    hmac_sha256_hex(secret, query)
}

fn binance_signed_request(
    auth: &PrivateRestAuth,
    mut request: PrivateRestRequestSpec,
    timestamp_ms: i64,
) -> Result<PrivateRestRequestSpec> {
    request
        .query
        .entry("timestamp".to_string())
        .or_insert_with(|| timestamp_ms.to_string());
    request
        .query
        .entry("recvWindow".to_string())
        .or_insert_with(|| "5000".to_string());
    request.query.remove("signature");
    let signature = binance_signature(&auth.api_secret, &request.raw_query_string());
    request.query.insert("signature".to_string(), signature);
    Ok(request)
}

fn binance_rest_headers(
    auth: &PrivateRestAuth,
    _request: &PrivateRestRequestSpec,
    _timestamp_ms: i64,
) -> Result<BTreeMap<String, String>> {
    Ok(BTreeMap::from([
        ("X-MBX-APIKEY".to_string(), auth.api_key.clone()),
        (
            "Content-Type".to_string(),
            "application/x-www-form-urlencoded".to_string(),
        ),
    ]))
}

pub fn coinex_signature(secret: &str, payload: &str) -> String {
    hmac_sha256_hex(secret, payload)
}

fn coinex_rest_headers(
    auth: &PrivateRestAuth,
    request: &PrivateRestRequestSpec,
    timestamp_ms: i64,
) -> Result<BTreeMap<String, String>> {
    let request_path = if request.raw_query_string().is_empty() {
        request.path.clone()
    } else {
        format!("{}?{}", request.path, request.raw_query_string())
    };
    let payload = format!(
        "{}{}{}{}",
        request.method.as_str(),
        request_path,
        request.body_string(),
        timestamp_ms
    );
    Ok(BTreeMap::from([
        ("X-COINEX-KEY".to_string(), auth.api_key.clone()),
        (
            "X-COINEX-SIGN".to_string(),
            coinex_signature(&auth.api_secret, &payload),
        ),
        ("X-COINEX-TIMESTAMP".to_string(), timestamp_ms.to_string()),
        ("Content-Type".to_string(), "application/json".to_string()),
    ]))
}

fn validate_bitget_time_window(
    start_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
    context: &str,
) -> Result<()> {
    if let (Some(start_time), Some(end_time)) = (start_time, end_time) {
        if end_time < start_time {
            anyhow::bail!("{context} end_time must be after start_time");
        }
    }
    Ok(())
}

pub fn bitget_signature(
    secret: &str,
    timestamp: &str,
    method: &str,
    path: &str,
    body: &str,
) -> String {
    let prehash = format!("{timestamp}{}{path}{body}", method.to_ascii_uppercase());
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts arbitrary key length");
    mac.update(prehash.as_bytes());
    general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}

fn bitget_rest_headers(
    auth: &PrivateRestAuth,
    request: &PrivateRestRequestSpec,
    timestamp_ms: i64,
) -> Result<BTreeMap<String, String>> {
    let passphrase = auth
        .passphrase
        .as_ref()
        .ok_or_else(|| anyhow!("bitget private REST requires passphrase"))?;
    let query = request.query_string();
    let path = if query.is_empty() {
        request.path.clone()
    } else {
        format!("{}?{}", request.path, query)
    };
    let timestamp = timestamp_ms.to_string();
    let sign = bitget_signature(
        &auth.api_secret,
        &timestamp,
        request.method.as_str(),
        &path,
        &request.body_string(),
    );
    Ok(BTreeMap::from([
        ("ACCESS-KEY".to_string(), auth.api_key.clone()),
        ("ACCESS-SIGN".to_string(), sign),
        ("ACCESS-TIMESTAMP".to_string(), timestamp),
        ("ACCESS-PASSPHRASE".to_string(), passphrase.clone()),
        ("Content-Type".to_string(), "application/json".to_string()),
        ("locale".to_string(), "en-US".to_string()),
    ])
    .into_iter()
    .chain(
        auth.demo_trading
            .then(|| ("paptrading".to_string(), "1".to_string())),
    )
    .collect())
}

pub fn gate_rest_signature(
    secret: &str,
    method: &str,
    path: &str,
    query: &str,
    body: &str,
    timestamp: i64,
) -> String {
    let body_hash = hex::encode(Sha512::digest(body.as_bytes()));
    let prehash = format!(
        "{}\n{}\n{}\n{}\n{}",
        method.to_ascii_uppercase(),
        path,
        query,
        body_hash,
        timestamp
    );
    let mut mac =
        HmacSha512::new_from_slice(secret.as_bytes()).expect("HMAC accepts arbitrary key length");
    mac.update(prehash.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn gate_rest_headers(
    auth: &PrivateRestAuth,
    request: &PrivateRestRequestSpec,
    timestamp_secs: i64,
) -> Result<BTreeMap<String, String>> {
    let query = request.raw_query_string();
    let sign_path = if request.path.starts_with("/api/v4/") {
        request.path.clone()
    } else {
        format!("/api/v4{}", request.path)
    };
    let sign = gate_rest_signature(
        &auth.api_secret,
        request.method.as_str(),
        &sign_path,
        &query,
        &request.body_string(),
        timestamp_secs,
    );
    Ok(BTreeMap::from([
        ("KEY".to_string(), auth.api_key.clone()),
        ("Timestamp".to_string(), timestamp_secs.to_string()),
        ("SIGN".to_string(), sign),
        ("Content-Type".to_string(), "application/json".to_string()),
    ]))
}

pub fn gate_ws_signature(secret: &str, channel: &str, event: &str, timestamp: i64) -> String {
    let prehash = format!("channel={channel}&event={event}&time={timestamp}");
    let mut mac =
        HmacSha512::new_from_slice(secret.as_bytes()).expect("HMAC accepts arbitrary key length");
    mac.update(prehash.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

pub fn bybit_signature(
    secret: &str,
    timestamp_ms: i64,
    api_key: &str,
    recv_window: u64,
    payload: &str,
) -> String {
    let prehash = format!("{timestamp_ms}{api_key}{recv_window}{payload}");
    hmac_sha256_hex(secret, &prehash)
}

pub fn bybit_ws_signature(secret: &str, expires_ms: i64) -> String {
    hmac_sha256_hex(secret, &format!("GET/realtime{expires_ms}"))
}

fn bybit_rest_headers(
    auth: &PrivateRestAuth,
    request: &PrivateRestRequestSpec,
    timestamp_ms: i64,
) -> Result<BTreeMap<String, String>> {
    let recv_window = 5_000_u64;
    let payload = if request.method == PrivateRestMethod::Get {
        request.query_string()
    } else {
        request.body_string()
    };
    let sign = bybit_signature(
        &auth.api_secret,
        timestamp_ms,
        &auth.api_key,
        recv_window,
        &payload,
    );
    Ok(BTreeMap::from([
        ("X-BAPI-API-KEY".to_string(), auth.api_key.clone()),
        ("X-BAPI-SIGN".to_string(), sign),
        ("X-BAPI-SIGN-TYPE".to_string(), "2".to_string()),
        ("X-BAPI-TIMESTAMP".to_string(), timestamp_ms.to_string()),
        ("X-BAPI-RECV-WINDOW".to_string(), recv_window.to_string()),
        ("Content-Type".to_string(), "application/json".to_string()),
    ]))
}

pub fn toobit_signature(secret: &str, payload: &str) -> String {
    hmac_sha256_hex(secret, payload)
}

fn toobit_signed_request(
    auth: &PrivateRestAuth,
    mut request: PrivateRestRequestSpec,
    timestamp_ms: i64,
) -> Result<PrivateRestRequestSpec> {
    request
        .query
        .entry("recvWindow".to_string())
        .or_insert_with(|| "5000".to_string());
    request
        .query
        .insert("timestamp".to_string(), timestamp_ms.to_string());
    request.query.remove("signature");
    let payload = format!("{}{}", request.raw_query_string(), request.body_string());
    let signature = toobit_signature(&auth.api_secret, &payload);
    request.query.insert("signature".to_string(), signature);
    Ok(request)
}

fn toobit_rest_headers(
    auth: &PrivateRestAuth,
    _request: &PrivateRestRequestSpec,
    _timestamp_ms: i64,
) -> Result<BTreeMap<String, String>> {
    Ok(BTreeMap::from([
        ("X-BB-APIKEY".to_string(), auth.api_key.clone()),
        ("Content-Type".to_string(), "application/json".to_string()),
    ]))
}

pub fn okx_signature(
    secret: &str,
    timestamp: &str,
    method: &str,
    request_path: &str,
    body: &str,
) -> String {
    hmac_sha256_base64(
        secret,
        &format!(
            "{}{}{}{}",
            timestamp,
            method.to_ascii_uppercase(),
            request_path,
            body
        ),
    )
}

fn okx_rest_headers(
    auth: &PrivateRestAuth,
    request: &PrivateRestRequestSpec,
    timestamp_ms: i64,
) -> Result<BTreeMap<String, String>> {
    let passphrase = auth
        .passphrase
        .as_ref()
        .ok_or_else(|| anyhow!("okx private REST requires passphrase"))?;
    let timestamp = DateTime::<Utc>::from_timestamp_millis(timestamp_ms)
        .unwrap_or_else(Utc::now)
        .to_rfc3339_opts(SecondsFormat::Millis, true);
    let query = request.query_string();
    let request_path = if query.is_empty() {
        request.path.clone()
    } else {
        format!("{}?{}", request.path, query)
    };
    let body = if request.method == PrivateRestMethod::Get {
        String::new()
    } else {
        request.body_string()
    };
    let sign = okx_signature(
        &auth.api_secret,
        &timestamp,
        request.method.as_str(),
        &request_path,
        &body,
    );
    let mut headers = BTreeMap::from([
        ("OK-ACCESS-KEY".to_string(), auth.api_key.clone()),
        ("OK-ACCESS-SIGN".to_string(), sign),
        ("OK-ACCESS-TIMESTAMP".to_string(), timestamp),
        ("OK-ACCESS-PASSPHRASE".to_string(), passphrase.clone()),
        ("Content-Type".to_string(), "application/json".to_string()),
    ]);
    if auth.demo_trading {
        headers.insert("x-simulated-trading".to_string(), "1".to_string());
    }
    Ok(headers)
}

pub fn mexc_signature(secret: &str, payload: &str) -> String {
    hmac_sha256_hex(secret, payload)
}

fn mexc_rest_headers(
    auth: &PrivateRestAuth,
    request: &PrivateRestRequestSpec,
    timestamp_ms: i64,
) -> Result<BTreeMap<String, String>> {
    let body = request.body_string();
    let query = request.query_string();
    let payload = format!(
        "{}{}{}",
        auth.api_key,
        timestamp_ms,
        if body.is_empty() { &query } else { &body }
    );
    let sign = mexc_signature(&auth.api_secret, &payload);
    Ok(BTreeMap::from([
        ("ApiKey".to_string(), auth.api_key.clone()),
        ("Request-Time".to_string(), timestamp_ms.to_string()),
        ("Signature".to_string(), sign),
        ("Content-Type".to_string(), "application/json".to_string()),
    ]))
}

fn mexc_ws_signature(api_key: &str, secret: &str, timestamp_ms: i64) -> String {
    mexc_signature(secret, &format!("{api_key}{timestamp_ms}"))
}

pub fn htx_signature(
    secret: &str,
    method: &str,
    host: &str,
    path: &str,
    timestamp: &str,
) -> String {
    let query = BTreeMap::from([
        ("SignatureMethod", "HmacSHA256"),
        ("SignatureVersion", "2"),
        ("Timestamp", timestamp),
    ]);
    let query = query
        .iter()
        .map(|(key, value)| {
            format!(
                "{}={}",
                urlencoding::encode(key),
                urlencoding::encode(value)
            )
        })
        .collect::<Vec<_>>()
        .join("&");
    let payload = format!(
        "{}\n{}\n{}\n{}",
        method.to_ascii_uppercase(),
        host,
        path,
        query
    );
    hmac_sha256_base64(secret, &payload)
}

fn htx_rest_headers(
    auth: &PrivateRestAuth,
    request: &PrivateRestRequestSpec,
    timestamp_secs: i64,
) -> Result<BTreeMap<String, String>> {
    let timestamp = DateTime::<Utc>::from_timestamp(timestamp_secs, 0)
        .unwrap_or_else(Utc::now)
        .format("%Y-%m-%dT%H:%M:%S")
        .to_string();
    let sign = htx_signature(
        &auth.api_secret,
        request.method.as_str(),
        "api.hbdm.com",
        &request.path,
        &timestamp,
    );
    Ok(BTreeMap::from([
        ("AccessKeyId".to_string(), auth.api_key.clone()),
        ("SignatureMethod".to_string(), "HmacSHA256".to_string()),
        ("SignatureVersion".to_string(), "2".to_string()),
        ("Timestamp".to_string(), timestamp),
        ("Signature".to_string(), sign),
        ("Content-Type".to_string(), "application/json".to_string()),
    ]))
}

fn hmac_sha256_hex(secret: &str, payload: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts arbitrary key length");
    mac.update(payload.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn hmac_sha256_base64(secret: &str, payload: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts arbitrary key length");
    mac.update(payload.as_bytes());
    general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}

fn normalize_private_rest_response(exchange: ExchangeId, value: Value) -> Result<Value> {
    match exchange {
        ExchangeId::Binance => {
            if let Some(code) = str_field(&value, &["code"]) {
                if code != "0" && code != "200" {
                    let message = str_field(&value, &["msg", "message"])
                        .unwrap_or_else(|| "binance private REST error".to_string());
                    return Err(PrivateRestError {
                        exchange,
                        class: classify_generic_rest_error(&code, &message),
                        endpoint: None,
                        code: Some(code),
                        message,
                    }
                    .into());
                }
            }
            Ok(value)
        }
        ExchangeId::Bitget => {
            let code = str_field(&value, &["code"]).unwrap_or_else(|| "00000".to_string());
            if code != "00000" {
                let msg = value
                    .get("msg")
                    .or_else(|| value.get("message"))
                    .and_then(Value::as_str)
                    .unwrap_or("bitget private REST error");
                return Err(PrivateRestError {
                    exchange,
                    class: classify_bitget_rest_error(&code, msg),
                    endpoint: None,
                    code: Some(code),
                    message: msg.to_string(),
                }
                .into());
            }
            Ok(value)
        }
        ExchangeId::Okx => {
            let code = str_field(&value, &["code"]).unwrap_or_else(|| "0".to_string());
            if code != "0" {
                let message = str_field(&value, &["msg", "message"])
                    .unwrap_or_else(|| "okx private REST error".to_string());
                return Err(PrivateRestError {
                    exchange,
                    class: classify_generic_rest_error(&code, &message),
                    endpoint: None,
                    code: Some(code),
                    message,
                }
                .into());
            }
            Ok(value)
        }
        ExchangeId::Gate => {
            if let Some(label) = str_field(&value, &["label"]) {
                let message = value
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("gate private REST error");
                return Err(PrivateRestError {
                    exchange,
                    class: classify_gate_rest_error(&label, message),
                    endpoint: None,
                    code: Some(label),
                    message: message.to_string(),
                }
                .into());
            }
            Ok(value)
        }
        ExchangeId::Bybit => {
            let code = str_field(&value, &["retCode"]).unwrap_or_else(|| "0".to_string());
            if code != "0" {
                let message = str_field(&value, &["retMsg", "msg", "message"])
                    .unwrap_or_else(|| "bybit private REST error".to_string());
                return Err(PrivateRestError {
                    exchange,
                    class: classify_generic_rest_error(&code, &message),
                    endpoint: None,
                    code: Some(code),
                    message,
                }
                .into());
            }
            Ok(value)
        }
        ExchangeId::Mexc => {
            let success = value
                .get("success")
                .and_then(Value::as_bool)
                .unwrap_or(true);
            let code = str_field(&value, &["code"]).unwrap_or_else(|| "0".to_string());
            if !success || code != "0" {
                let message = str_field(&value, &["message", "msg"])
                    .unwrap_or_else(|| "mexc private REST error".to_string());
                return Err(PrivateRestError {
                    exchange,
                    class: classify_generic_rest_error(&code, &message),
                    endpoint: None,
                    code: Some(code),
                    message,
                }
                .into());
            }
            Ok(value)
        }
        ExchangeId::Htx => {
            let status = str_field(&value, &["status"]).unwrap_or_else(|| "ok".to_string());
            if !status.eq_ignore_ascii_case("ok") {
                let code = str_field(&value, &["err_code", "err-code", "code"]);
                let message = str_field(&value, &["err_msg", "err-msg", "message"])
                    .unwrap_or_else(|| "htx private REST error".to_string());
                return Err(PrivateRestError {
                    exchange,
                    class: classify_generic_rest_error(
                        code.as_deref().unwrap_or_default(),
                        &message,
                    ),
                    endpoint: None,
                    code,
                    message,
                }
                .into());
            }
            Ok(value)
        }
        ExchangeId::Toobit => {
            let code = str_field(&value, &["code"]).unwrap_or_else(|| "0".to_string());
            if code != "0" && code != "200" {
                let message = str_field(&value, &["msg", "message"])
                    .unwrap_or_else(|| "toobit private REST error".to_string());
                return Err(PrivateRestError {
                    exchange,
                    class: classify_generic_rest_error(&code, &message),
                    endpoint: None,
                    code: Some(code),
                    message,
                }
                .into());
            }
            Ok(value)
        }
        _ => Ok(value),
    }
}

fn private_rest_http_error(
    request: &PrivateRestRequestSpec,
    http_status: u16,
    value: &Value,
) -> PrivateRestError {
    let code = str_field(value, &["code", "label"]).or_else(|| Some(http_status.to_string()));
    let message = str_field(value, &["msg", "message"])
        .unwrap_or_else(|| format!("http status {http_status} body={value}"));
    let class = match http_status {
        401 => ExchangeErrorClass::Authentication,
        403 => ExchangeErrorClass::Permission,
        404 => ExchangeErrorClass::OrderNotFound,
        408 => ExchangeErrorClass::Timeout,
        418 | 429 => ExchangeErrorClass::RateLimited,
        500..=599 => ExchangeErrorClass::ExchangeUnavailable,
        _ => match request.exchange {
            ExchangeId::Binance => {
                classify_generic_rest_error(code.as_deref().unwrap_or_default(), &message)
            }
            ExchangeId::Bitget => {
                classify_bitget_rest_error(code.as_deref().unwrap_or_default(), &message)
            }
            ExchangeId::Gate => {
                classify_gate_rest_error(code.as_deref().unwrap_or_default(), &message)
            }
            ExchangeId::Okx
            | ExchangeId::Bybit
            | ExchangeId::Mexc
            | ExchangeId::Htx
            | ExchangeId::Toobit => {
                classify_generic_rest_error(code.as_deref().unwrap_or_default(), &message)
            }
            _ => ExchangeErrorClass::Unknown,
        },
    };
    PrivateRestError {
        exchange: request.exchange.clone(),
        class,
        endpoint: Some(format!("{} {}", request.method.as_str(), request.path)),
        code,
        message,
    }
}

fn classify_bitget_rest_error(code: &str, message: &str) -> ExchangeErrorClass {
    let text = format!("{code} {message}").to_ascii_lowercase();
    if text.contains("sign")
        || text.contains("api key")
        || text.contains("apikey")
        || text.contains("auth")
        || text.contains("login")
    {
        ExchangeErrorClass::Authentication
    } else if text.contains("permission") || text.contains("forbidden") {
        ExchangeErrorClass::Permission
    } else if text.contains("rate") || text.contains("too many") || text.contains("limit") {
        ExchangeErrorClass::RateLimited
    } else if text.contains("balance")
        || text.contains("margin")
        || text.contains("insufficient")
        || text.contains("not enough")
    {
        ExchangeErrorClass::InsufficientBalance
    } else if text.contains("position")
        && (text.contains("insufficient") || text.contains("not enough"))
    {
        ExchangeErrorClass::InsufficientPosition
    } else if text.contains("duplicate") || text.contains("clientoid") && text.contains("exist") {
        ExchangeErrorClass::DuplicateClientOrderId
    } else if text.contains("not found")
        || text.contains("not exist")
        || text.contains("order does not exist")
    {
        ExchangeErrorClass::OrderNotFound
    } else if text.contains("precision")
        || text.contains("minimum")
        || text.contains("min")
        || text.contains("size")
        || text.contains("price")
    {
        ExchangeErrorClass::Precision
    } else if text.contains("risk") || text.contains("reject") {
        ExchangeErrorClass::RiskRejected
    } else if text.contains("maintenance") {
        ExchangeErrorClass::Maintenance
    } else {
        ExchangeErrorClass::Unknown
    }
}

fn classify_gate_rest_error(label: &str, message: &str) -> ExchangeErrorClass {
    let text = format!("{label} {message}").to_ascii_lowercase();
    if text.contains("invalid_key")
        || text.contains("invalid_signature")
        || text.contains("auth")
        || text.contains("signature")
    {
        ExchangeErrorClass::Authentication
    } else if text.contains("permission") || text.contains("forbidden") {
        ExchangeErrorClass::Permission
    } else if text.contains("too_fast") || text.contains("rate") || text.contains("too many") {
        ExchangeErrorClass::RateLimited
    } else if text.contains("balance")
        || text.contains("margin")
        || text.contains("insufficient")
        || text.contains("not enough")
    {
        ExchangeErrorClass::InsufficientBalance
    } else if text.contains("position")
        && (text.contains("insufficient") || text.contains("not enough"))
    {
        ExchangeErrorClass::InsufficientPosition
    } else if text.contains("duplicated")
        || text.contains("duplicate")
        || text.contains("client") && (text.contains("exist") || text.contains("repeated"))
    {
        ExchangeErrorClass::DuplicateClientOrderId
    } else if text.contains("order_not_found")
        || text.contains("not_found")
        || text.contains("not found")
        || text.contains("not exist")
    {
        ExchangeErrorClass::OrderNotFound
    } else if text.contains("invalid_param")
        || text.contains("invalid_price")
        || text.contains("invalid_size")
        || text.contains("precision")
        || text.contains("minimum")
    {
        ExchangeErrorClass::Precision
    } else if text.contains("risk") || text.contains("reject") {
        ExchangeErrorClass::RiskRejected
    } else if text.contains("maintenance") {
        ExchangeErrorClass::Maintenance
    } else {
        ExchangeErrorClass::Unknown
    }
}

fn classify_generic_rest_error(code: &str, message: &str) -> ExchangeErrorClass {
    let text = format!("{code} {message}").to_ascii_lowercase();
    if text.contains("sign")
        || text.contains("signature")
        || text.contains("api key")
        || text.contains("apikey")
        || text.contains("auth")
        || text.contains("login")
    {
        ExchangeErrorClass::Authentication
    } else if text.contains("permission") || text.contains("forbidden") || text.contains("ip") {
        ExchangeErrorClass::Permission
    } else if text.contains("rate") || text.contains("too many") || text.contains("frequency") {
        ExchangeErrorClass::RateLimited
    } else if text.contains("balance")
        || text.contains("margin")
        || text.contains("insufficient")
        || text.contains("not enough")
    {
        ExchangeErrorClass::InsufficientBalance
    } else if text.contains("duplicate") || text.contains("client") && text.contains("exist") {
        ExchangeErrorClass::DuplicateClientOrderId
    } else if text.contains("not found")
        || text.contains("not exist")
        || text.contains("order does not exist")
    {
        ExchangeErrorClass::OrderNotFound
    } else if text.contains("precision")
        || text.contains("minimum")
        || text.contains("min")
        || text.contains("qty")
        || text.contains("size")
        || text.contains("price")
        || text.contains("param")
    {
        ExchangeErrorClass::Precision
    } else if text.contains("risk") || text.contains("reject") {
        ExchangeErrorClass::RiskRejected
    } else if text.contains("maintenance") || text.contains("under maintenance") {
        ExchangeErrorClass::Maintenance
    } else {
        ExchangeErrorClass::Unknown
    }
}

fn response_data(value: &Value) -> Value {
    value
        .get("data")
        .or_else(|| value.get("result"))
        .cloned()
        .unwrap_or_else(|| value.clone())
}

fn close_position_spec(
    exchange: ExchangeId,
    command: &ClosePositionCommand,
    position_mode: PositionMode,
) -> Result<PrivateRestRequestSpec> {
    ensure_close_position_exchange_scope(&exchange, command)?;

    match exchange {
        ExchangeId::Binance => BinancePrivatePerpProtocol.place_order(
            &OrderCommand {
                command_id: command.client_order_id.clone(),
                bundle_id: command.client_order_id.clone(),
                exchange: ExchangeId::Binance,
                canonical_symbol: command.canonical_symbol.clone(),
                exchange_symbol: command.exchange_symbol.clone(),
                intent: crate::execution::OrderIntent::CloseLongTaker,
                side: command.order_side(),
                position_side: command.position_side,
                order_type: command.order_type,
                quantity: command.quantity,
                price: command.price,
                time_in_force: command.time_in_force,
                post_only: false,
                reduce_only: true,
                client_order_id: command.client_order_id.clone(),
                max_slippage_pct: command.max_slippage_pct,
                status: OrderCommandStatus::Planned,
                created_at: command.requested_at,
            },
            position_mode,
        ),
        ExchangeId::Okx => Ok(PrivateRestRequestSpec::new(
            ExchangeId::Okx,
            PrivateRestMethod::Post,
            "/api/v5/trade/order",
        )
        .with_body(okx_order_body(
            &OrderCommand {
                command_id: command.client_order_id.clone(),
                bundle_id: command.client_order_id.clone(),
                exchange: ExchangeId::Okx,
                canonical_symbol: command.canonical_symbol.clone(),
                exchange_symbol: command.exchange_symbol.clone(),
                intent: crate::execution::OrderIntent::CloseLongTaker,
                side: command.order_side(),
                position_side: command.position_side,
                order_type: command.order_type,
                quantity: command.quantity,
                price: command.price,
                time_in_force: command.time_in_force,
                post_only: false,
                reduce_only: true,
                client_order_id: command.client_order_id.clone(),
                max_slippage_pct: command.max_slippage_pct,
                status: OrderCommandStatus::Planned,
                created_at: command.requested_at,
            },
            position_mode,
        ))),
        ExchangeId::Bitget => {
            let side = if position_mode.is_hedge() {
                bitget_position_side(command.position_side, command.order_side())
            } else {
                bitget_side(command.order_side())
            };
            let mut body = json!({
                "productType": "USDT-FUTURES",
                "symbol": command.exchange_symbol.symbol,
                "marginMode": "crossed",
                "marginCoin": "USDT",
                "size": number_string(command.quantity),
                "side": side,
                "orderType": bitget_order_type(command.order_type),
                "clientOid": command.client_order_id,
            });
            set_optional_str(
                &mut body,
                "force",
                bitget_force(command.time_in_force, false),
            );
            if let Some(price) = command.price {
                set_str(&mut body, "price", number_string(price));
            } else if command.order_type == OrderType::Market {
                set_str(&mut body, "price", "0");
            }
            if position_mode.is_hedge() {
                set_str(&mut body, "tradeSide", "close");
            } else {
                set_str(&mut body, "reduceOnly", "yes");
            }
            Ok(PrivateRestRequestSpec::new(
                ExchangeId::Bitget,
                PrivateRestMethod::Post,
                "/api/v2/mix/order/place-order",
            )
            .with_body(body))
        }
        ExchangeId::Gate => {
            let mut body = json!({
                "contract": command.exchange_symbol.symbol,
                "size": gate_signed_size(command.order_side(), command.quantity)?,
                "tif": gate_tif(command.time_in_force, false),
                "text": gate_client_text(&command.client_order_id),
                "reduce_only": true,
            });
            if let Some(price) = command.price {
                set_str(&mut body, "price", number_string(price));
            } else if command.order_type == OrderType::Market {
                set_str(&mut body, "price", "0");
            }
            Ok(PrivateRestRequestSpec::new(
                ExchangeId::Gate,
                PrivateRestMethod::Post,
                "/futures/usdt/orders",
            )
            .with_body(body))
        }
        ExchangeId::Bybit => BybitPrivatePerpProtocol.place_order(
            &OrderCommand {
                command_id: command.client_order_id.clone(),
                bundle_id: command.client_order_id.clone(),
                exchange: ExchangeId::Bybit,
                canonical_symbol: command.canonical_symbol.clone(),
                exchange_symbol: command.exchange_symbol.clone(),
                intent: crate::execution::OrderIntent::CloseLongTaker,
                side: command.order_side(),
                position_side: command.position_side,
                order_type: command.order_type,
                quantity: command.quantity,
                price: command.price,
                time_in_force: command.time_in_force,
                post_only: false,
                reduce_only: true,
                client_order_id: command.client_order_id.clone(),
                max_slippage_pct: command.max_slippage_pct,
                status: OrderCommandStatus::Planned,
                created_at: command.requested_at,
            },
            position_mode,
        ),
        ExchangeId::Mexc => MexcPrivatePerpProtocol.place_order(
            &OrderCommand {
                command_id: command.client_order_id.clone(),
                bundle_id: command.client_order_id.clone(),
                exchange: ExchangeId::Mexc,
                canonical_symbol: command.canonical_symbol.clone(),
                exchange_symbol: command.exchange_symbol.clone(),
                intent: crate::execution::OrderIntent::CloseLongTaker,
                side: command.order_side(),
                position_side: command.position_side,
                order_type: command.order_type,
                quantity: command.quantity,
                price: command.price,
                time_in_force: command.time_in_force,
                post_only: false,
                reduce_only: true,
                client_order_id: command.client_order_id.clone(),
                max_slippage_pct: command.max_slippage_pct,
                status: OrderCommandStatus::Planned,
                created_at: command.requested_at,
            },
            position_mode,
        ),
        ExchangeId::Htx if command.order_type == OrderType::Market && command.price.is_none() => {
            Ok(PrivateRestRequestSpec::new(
                ExchangeId::Htx,
                PrivateRestMethod::Post,
                "/linear-swap-api/v1/swap_cross_lightning_close_position",
            )
            .with_body(json!({
                "contract_code": command.exchange_symbol.symbol,
                "volume": number_string(command.quantity),
                "direction": htx_direction(command.order_side()),
                "client_order_id": htx_client_order_id(&command.client_order_id),
                "order_price_type": htx_lightning_order_price_type(command.time_in_force),
            })))
        }
        ExchangeId::Htx => HtxPrivatePerpProtocol.place_order(
            &OrderCommand {
                command_id: command.client_order_id.clone(),
                bundle_id: command.client_order_id.clone(),
                exchange: ExchangeId::Htx,
                canonical_symbol: command.canonical_symbol.clone(),
                exchange_symbol: command.exchange_symbol.clone(),
                intent: crate::execution::OrderIntent::CloseLongTaker,
                side: command.order_side(),
                position_side: command.position_side,
                order_type: command.order_type,
                quantity: command.quantity,
                price: command.price,
                time_in_force: command.time_in_force,
                post_only: false,
                reduce_only: true,
                client_order_id: command.client_order_id.clone(),
                max_slippage_pct: command.max_slippage_pct,
                status: OrderCommandStatus::Planned,
                created_at: command.requested_at,
            },
            position_mode,
        ),
        ExchangeId::Toobit => ToobitPrivatePerpProtocol.place_order(
            &OrderCommand {
                command_id: command.client_order_id.clone(),
                bundle_id: command.client_order_id.clone(),
                exchange: ExchangeId::Toobit,
                canonical_symbol: command.canonical_symbol.clone(),
                exchange_symbol: command.exchange_symbol.clone(),
                intent: crate::execution::OrderIntent::CloseLongTaker,
                side: command.order_side(),
                position_side: command.position_side,
                order_type: command.order_type,
                quantity: command.quantity,
                price: command.price,
                time_in_force: command.time_in_force,
                post_only: false,
                reduce_only: true,
                client_order_id: command.client_order_id.clone(),
                max_slippage_pct: command.max_slippage_pct,
                status: OrderCommandStatus::Planned,
                created_at: command.requested_at,
            },
            position_mode,
        ),
        other => anyhow::bail!("{other} private perp close_position is not supported"),
    }
}

fn ensure_close_position_exchange_scope(
    exchange: &ExchangeId,
    command: &ClosePositionCommand,
) -> Result<()> {
    if &command.exchange != exchange || &command.exchange_symbol.exchange != exchange {
        anyhow::bail!("{exchange} close position requires {exchange} exchange symbols");
    }
    Ok(())
}

fn ensure_countdown_cancel_all_scope(
    exchange: &ExchangeId,
    command: &CountdownCancelAllCommand,
) -> Result<()> {
    if &command.exchange != exchange
        || command
            .exchange_symbol
            .as_ref()
            .is_some_and(|symbol| &symbol.exchange != exchange)
    {
        anyhow::bail!("{exchange} countdown cancel-all requires {exchange} exchange symbols");
    }
    Ok(())
}

fn response_items(value: &Value) -> Vec<Value> {
    match response_data(value) {
        Value::Array(items) => items,
        Value::Object(map) => {
            if let Some(Value::Array(items)) = map
                .get("list")
                .or_else(|| map.get("orders"))
                .or_else(|| map.get("fillList"))
                .or_else(|| map.get("entrustedList"))
                .or_else(|| map.get("orderList"))
                .or_else(|| map.get("successList"))
                .or_else(|| map.get("failureList"))
                .or_else(|| map.get("result"))
                .or_else(|| map.get("orders"))
                .or_else(|| map.get("positions"))
                .or_else(|| map.get("assets"))
                .or_else(|| map.get("balances"))
                .or_else(|| map.get("trades"))
                .or_else(|| map.get("ticks"))
            {
                items.clone()
            } else {
                vec![Value::Object(map)]
            }
        }
        _ => Vec::new(),
    }
}

fn response_readback_item(value: &Value, fallback_symbol: &ExchangeSymbol) -> Option<Value> {
    let items = response_items(value);
    if !items.is_empty() {
        return items
            .iter()
            .find(|item| symbol_matches(item, fallback_symbol))
            .cloned()
            .or_else(|| items.into_iter().next());
    }
    match response_data(value) {
        Value::Object(_) => Some(response_data(value)),
        _ => None,
    }
}

fn response_string(value: &Value, keys: &[&str]) -> Option<String> {
    let data = response_data(value);
    str_field(&data, keys)
        .or_else(|| {
            data.as_array()
                .and_then(|items| items.first())
                .and_then(|item| str_field(item, keys))
        })
        .or_else(|| str_field(value, keys))
}

fn parse_countdown_trigger_time(value: &Value) -> Option<DateTime<Utc>> {
    let data = response_data(value);
    i64_field(
        &data,
        &["trigger_time", "triggerTime", "cancel_time", "cancelTime"],
    )
    .and_then(|timestamp| {
        if timestamp > 1_000_000_000_000 {
            DateTime::<Utc>::from_timestamp_millis(timestamp)
        } else {
            DateTime::<Utc>::from_timestamp(timestamp, 0)
        }
    })
}

fn cancelled_count(value: &Value) -> usize {
    let data = response_data(value);
    match data {
        Value::Array(items) => items.len(),
        Value::Object(map) => map
            .get("successList")
            .or_else(|| map.get("success"))
            .or_else(|| map.get("cancelled"))
            .and_then(Value::as_array)
            .map(Vec::len)
            .or_else(|| {
                map.get("count")
                    .or_else(|| map.get("cancelled_count"))
                    .and_then(Value::as_u64)
                    .map(|count| count as usize)
            })
            .unwrap_or(0),
        _ => 0,
    }
}

fn parse_batch_place_ack(
    exchange: ExchangeId,
    orders: &[OrderCommand],
    value: &Value,
    acknowledged_at: DateTime<Utc>,
) -> BatchPlaceAck {
    if let Some(ret_ext_list) = value
        .pointer("/retExtInfo/list")
        .and_then(Value::as_array)
        .or_else(|| value.pointer("/retExt/list").and_then(Value::as_array))
    {
        let result_items = value
            .pointer("/result/list")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut order_acks = Vec::new();
        let mut failed_orders = Vec::new();
        for (index, ext) in ret_ext_list.iter().enumerate() {
            let item = result_items.get(index).unwrap_or(ext);
            let code = response_string(ext, &["code", "retCode", "sCode"]);
            let message = response_string(ext, &["msg", "retMsg", "message", "sMsg"]);
            let ok = code
                .as_deref()
                .and_then(|code| code.parse::<i64>().ok())
                .is_none_or(|code| code == 0);
            let order = batch_place_order_for_item(orders, item, index);
            if !ok {
                failed_orders.push(BatchPlaceError {
                    order,
                    code,
                    message: message.unwrap_or_else(|| "batch-place order failed".to_string()),
                });
                continue;
            }
            let exchange_order_id = response_string(item, &["orderId", "ordId", "id"])
                .or_else(|| Some(order.client_order_id.clone()));
            order_acks.push(OrderAck {
                exchange: order.exchange.clone(),
                client_order_id: response_string(
                    item,
                    &[
                        "clientOrderId",
                        "clientOid",
                        "newClientOrderId",
                        "orderLinkId",
                        "clOrdId",
                        "text",
                    ],
                )
                .unwrap_or_else(|| order.client_order_id.clone()),
                exchange_order_id,
                accepted: true,
                status: OrderCommandStatus::Accepted,
                message,
                acknowledged_at,
            });
        }
        return BatchPlaceAck {
            exchange,
            accepted: failed_orders.is_empty(),
            placed_orders: order_acks.len(),
            order_acks,
            failed_orders,
            message: response_string(value, &["retMsg", "msg", "message"]),
            acknowledged_at,
        };
    }

    let data = response_data(value);
    if let Value::Object(map) = &data {
        if map.get("success").is_some_and(Value::is_array)
            || map.get("errors").is_some_and(Value::is_array)
        {
            let mut order_acks = Vec::new();
            let mut failed_orders = Vec::new();
            if let Some(successes) = map.get("success").and_then(Value::as_array) {
                for (index, item) in successes.iter().enumerate() {
                    let order = batch_place_order_for_htx_index(orders, item, index);
                    let exchange_order_id = response_string(
                        item,
                        &["order_id_str", "order_id", "orderId", "ordId", "id"],
                    )
                    .or_else(|| Some(order.client_order_id.clone()));
                    order_acks.push(OrderAck {
                        exchange: order.exchange.clone(),
                        client_order_id: order.client_order_id.clone(),
                        exchange_order_id,
                        accepted: true,
                        status: OrderCommandStatus::Accepted,
                        message: response_string(item, &["msg", "message"]),
                        acknowledged_at,
                    });
                }
            }
            if let Some(errors) = map.get("errors").and_then(Value::as_array) {
                for (index, item) in errors.iter().enumerate() {
                    let order = batch_place_order_for_htx_index(orders, item, index);
                    failed_orders.push(BatchPlaceError {
                        order,
                        code: response_string(item, &["err_code", "code", "errorCode"]),
                        message: response_string(item, &["err_msg", "msg", "message", "errorMsg"])
                            .unwrap_or_else(|| "batch-place order failed".to_string()),
                    });
                }
            }
            return BatchPlaceAck {
                exchange,
                accepted: failed_orders.is_empty(),
                placed_orders: order_acks.len(),
                order_acks,
                failed_orders,
                message: response_string(value, &["err_msg", "msg", "message"]),
                acknowledged_at,
            };
        }
        if map.contains_key("successList") || map.contains_key("failureList") {
            let mut order_acks = Vec::new();
            let mut failed_orders = Vec::new();
            if let Some(successes) = map.get("successList").and_then(Value::as_array) {
                for (index, item) in successes.iter().enumerate() {
                    let order = batch_place_order_for_item(orders, item, index);
                    let exchange_order_id = response_string(item, &["orderId", "ordId", "id"])
                        .or_else(|| Some(order.client_order_id.clone()));
                    order_acks.push(OrderAck {
                        exchange: order.exchange.clone(),
                        client_order_id: batch_place_client_order_id(item)
                            .unwrap_or_else(|| order.client_order_id.clone()),
                        exchange_order_id,
                        accepted: true,
                        status: OrderCommandStatus::Accepted,
                        message: response_string(item, &["msg", "message"]),
                        acknowledged_at,
                    });
                }
            }
            if let Some(failures) = map.get("failureList").and_then(Value::as_array) {
                for (index, item) in failures.iter().enumerate() {
                    let order = batch_place_order_for_item(orders, item, index + order_acks.len());
                    failed_orders.push(BatchPlaceError {
                        order,
                        code: response_string(item, &["code", "errorCode", "sCode"]),
                        message: response_string(item, &["msg", "message", "errorMsg", "sMsg"])
                            .unwrap_or_else(|| "batch-place order failed".to_string()),
                    });
                }
            }
            let result_ok = map
                .get("result")
                .and_then(Value::as_bool)
                .unwrap_or_else(|| failed_orders.is_empty());
            return BatchPlaceAck {
                exchange,
                accepted: result_ok && failed_orders.is_empty(),
                placed_orders: order_acks.len(),
                order_acks,
                failed_orders,
                message: response_string(value, &["msg", "message"]),
                acknowledged_at,
            };
        }
    }

    let items = response_items(value);
    let mut order_acks = Vec::new();
    let mut failed_orders = Vec::new();
    for (index, order) in orders.iter().enumerate() {
        let item = items.get(index).unwrap_or(value);
        let code = response_string(item, &["code", "label", "errorCode", "sCode"]);
        let message = response_string(item, &["msg", "message", "detail", "errorMsg", "sMsg"]);
        if batch_place_code_is_error(code.as_deref())
            || item
                .get("succeeded")
                .and_then(Value::as_bool)
                .is_some_and(|succeeded| !succeeded)
            || (code.is_some() && response_string(item, &["orderId", "ordId", "id"]).is_none())
        {
            failed_orders.push(BatchPlaceError {
                order: order.clone(),
                code,
                message: message.unwrap_or_else(|| "batch-place order failed".to_string()),
            });
            continue;
        }
        let exchange_order_id = response_string(item, &["orderId", "ordId", "id"])
            .or_else(|| Some(order.client_order_id.clone()));
        order_acks.push(OrderAck {
            exchange: order.exchange.clone(),
            client_order_id: batch_place_client_order_id(item)
                .unwrap_or_else(|| order.client_order_id.clone()),
            exchange_order_id,
            accepted: true,
            status: OrderCommandStatus::Accepted,
            message,
            acknowledged_at,
        });
    }
    BatchPlaceAck {
        exchange,
        accepted: failed_orders.is_empty(),
        placed_orders: order_acks.len(),
        order_acks,
        failed_orders,
        message: response_string(value, &["msg", "message"]),
        acknowledged_at,
    }
}

fn batch_place_code_is_error(code: Option<&str>) -> bool {
    code.is_some_and(|code| {
        code.parse::<i64>()
            .map(|parsed| parsed != 0)
            .unwrap_or_else(|_| {
                !matches!(code.to_ascii_lowercase().as_str(), "0" | "ok" | "success")
            })
    })
}

fn batch_place_order_for_htx_index(
    orders: &[OrderCommand],
    item: &Value,
    fallback_index: usize,
) -> OrderCommand {
    if let Some(index) = i64_field(item, &["index"]) {
        let index = if index > 0 { index - 1 } else { 0 };
        if let Some(order) = orders.get(index as usize) {
            return order.clone();
        }
    }
    batch_place_order_for_item(orders, item, fallback_index)
}

fn batch_place_order_for_item(
    orders: &[OrderCommand],
    item: &Value,
    fallback_index: usize,
) -> OrderCommand {
    if let Some(client_order_id) = batch_place_client_order_id(item) {
        if let Some(order) = orders
            .iter()
            .find(|order| order.client_order_id == client_order_id)
        {
            return order.clone();
        }
    }
    orders
        .get(fallback_index)
        .or_else(|| orders.first())
        .cloned()
        .unwrap_or_else(|| OrderCommand {
            command_id: "unknown-batch-place-order".to_string(),
            bundle_id: "unknown".to_string(),
            exchange: ExchangeId::Other("unknown".to_string()),
            canonical_symbol: CanonicalSymbol::new("UNKNOWN", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Other("unknown".to_string()), ""),
            intent: crate::execution::OrderIntent::OpenLongMaker,
            side: OrderSide::Buy,
            position_side: PositionSide::Net,
            order_type: OrderType::Limit,
            quantity: 0.0,
            price: None,
            time_in_force: TimeInForce::Gtc,
            post_only: false,
            reduce_only: false,
            client_order_id: "unknown".to_string(),
            max_slippage_pct: None,
            status: OrderCommandStatus::Failed,
            created_at: Utc::now(),
        })
}

fn batch_place_client_order_id(item: &Value) -> Option<String> {
    if let Some(text) = str_field(item, &["text"]) {
        return Some(gate_client_text_to_local(&text));
    }
    response_string(
        item,
        &[
            "clientOrderId",
            "clientOid",
            "externalOid",
            "newClientOrderId",
            "orderLinkId",
            "clOrdId",
        ],
    )
}

fn parse_cancel_batch_acks(
    exchange: ExchangeId,
    value: &Value,
    acknowledged_at: DateTime<Utc>,
) -> Vec<CancelAck> {
    if let Some(ret_ext_list) = value
        .pointer("/retExtInfo/list")
        .and_then(Value::as_array)
        .or_else(|| value.pointer("/retExt/list").and_then(Value::as_array))
    {
        let result_items = value
            .pointer("/result/list")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        return ret_ext_list
            .iter()
            .enumerate()
            .filter_map(|(index, ext)| {
                let mut item = result_items.get(index).cloned().unwrap_or_default();
                if let Value::Object(item_map) = &mut item {
                    if let Some(ext_map) = ext.as_object() {
                        for key in ["code", "retCode", "msg", "retMsg", "message"] {
                            if let Some(value) = ext_map.get(key) {
                                item_map.entry(key.to_string()).or_insert(value.clone());
                            }
                        }
                    }
                } else {
                    item = ext.clone();
                }
                parse_cancel_batch_item_ack(exchange.clone(), &item, true, acknowledged_at)
            })
            .collect();
    }

    let data = response_data(value);
    if let Value::Object(map) = &data {
        if map.get("success").is_some_and(Value::is_array)
            || map.get("errors").is_some_and(Value::is_array)
        {
            let mut acks = Vec::new();
            if let Some(successes) = map.get("success").and_then(Value::as_array) {
                acks.extend(successes.iter().filter_map(|item| {
                    parse_cancel_batch_item_ack(exchange.clone(), item, true, acknowledged_at)
                }));
            }
            if let Some(errors) = map.get("errors").and_then(Value::as_array) {
                acks.extend(errors.iter().filter_map(|item| {
                    parse_cancel_batch_item_ack(exchange.clone(), item, false, acknowledged_at)
                }));
            }
            return acks;
        }
        if map.contains_key("successList") || map.contains_key("failureList") {
            let mut acks = Vec::new();
            if let Some(successes) = map.get("successList").and_then(Value::as_array) {
                acks.extend(successes.iter().filter_map(|item| {
                    parse_cancel_batch_item_ack(exchange.clone(), item, true, acknowledged_at)
                }));
            }
            if let Some(failures) = map.get("failureList").and_then(Value::as_array) {
                acks.extend(failures.iter().filter_map(|item| {
                    parse_cancel_batch_item_ack(exchange.clone(), item, false, acknowledged_at)
                }));
            }
            return acks;
        }
    }

    response_items(value)
        .into_iter()
        .filter_map(|item| {
            parse_cancel_batch_item_ack(exchange.clone(), &item, true, acknowledged_at)
        })
        .collect()
}

fn parse_cancel_batch_item_ack(
    exchange: ExchangeId,
    item: &Value,
    default_accepted: bool,
    acknowledged_at: DateTime<Utc>,
) -> Option<CancelAck> {
    let exchange_order_id = str_field(
        item,
        &["order_id_str", "order_id", "orderId", "ordId", "id"],
    );
    let client_order_id = str_field(
        item,
        &[
            "client_order_id",
            "clientOid",
            "clientOrderId",
            "externalOid",
            "clOrdId",
            "orderLinkId",
            "text",
        ],
    )
    .map(|client_id| gate_client_text_to_local(&client_id));
    if exchange_order_id.is_none() && client_order_id.is_none() {
        return None;
    }
    let message = response_string(item, &["msg", "message", "sMsg", "errorMsg", "err_msg"]);
    let item_code = response_string(item, &["sCode", "code", "retCode", "errorCode", "err_code"]);
    let accepted = match item.get("succeeded").and_then(Value::as_bool) {
        Some(succeeded) => succeeded,
        None if item_code.is_some() => {
            item_code.and_then(|code| code.parse::<i64>().ok()) == Some(0)
        }
        None if !default_accepted => false,
        None if exchange == ExchangeId::Gate && message.is_some() => false,
        None => default_accepted,
    };
    Some(CancelAck {
        exchange,
        client_order_id,
        exchange_order_id,
        accepted,
        status: if accepted {
            OrderCommandStatus::Cancelled
        } else {
            OrderCommandStatus::Rejected
        },
        message,
        acknowledged_at,
    })
}

fn private_error_event(
    exchange: ExchangeId,
    class: ExchangeErrorClass,
    code: Option<String>,
    message: String,
    occurred_at: DateTime<Utc>,
) -> PrivateEvent {
    PrivateEvent::new(
        exchange,
        PrivateEventKind::Error(PrivateErrorEvent {
            class,
            endpoint: None,
            code,
            message,
            client_order_id: None,
            exchange_order_id: None,
            retry_after_ms: None,
            occurred_at,
        }),
        occurred_at,
    )
}

fn parse_trade_fee_snapshot(
    exchange: ExchangeId,
    fallback_symbol: &ExchangeSymbol,
    value: &Value,
    received_at: DateTime<Utc>,
) -> Option<TradeFeeSnapshot> {
    let item = response_readback_item(value, fallback_symbol)?;
    let symbol = str_field(
        &item,
        &["symbol", "instId", "contract", "contract_code", "name"],
    )
    .unwrap_or_else(|| fallback_symbol.symbol.clone());
    let maker = f64_field(
        &item,
        &[
            "makerFeeRate",
            "makerCommissionRate",
            "maker_fee_rate",
            "makerFee",
            "maker",
            "makerFee",
            "feeRateMaker",
            "openMakerFeeRate",
            "makerFeeRateE4",
            "open_maker_fee",
        ],
    )?;
    let taker = f64_field(
        &item,
        &[
            "takerFeeRate",
            "takerCommissionRate",
            "taker_fee_rate",
            "takerFee",
            "taker",
            "takerFee",
            "feeRateTaker",
            "openTakerFeeRate",
            "takerFeeRateE4",
            "open_taker_fee",
        ],
    )?;
    Some(TradeFeeSnapshot {
        exchange: exchange.clone(),
        canonical_symbol: canonical(&exchange, &symbol),
        exchange_symbol: ExchangeSymbol::new(exchange, symbol),
        maker,
        taker,
        source: "private_perp_readback".to_string(),
        updated_at: received_at,
    })
}

fn parse_symbol_account_config(
    exchange: ExchangeId,
    fallback_symbol: &ExchangeSymbol,
    value: &Value,
    received_at: DateTime<Utc>,
    configured_position_mode: PositionMode,
) -> Option<SymbolAccountConfig> {
    let item = response_readback_item(value, fallback_symbol)?;
    let symbol = str_field(
        &item,
        &["symbol", "instId", "contract", "contract_code", "name"],
    )
    .unwrap_or_else(|| fallback_symbol.symbol.clone());
    let position_mode = str_field(
        &item,
        &["posMode", "pos_mode", "positionMode", "position_mode"],
    )
    .as_deref()
    .map(parse_position_mode_text)
    .or(Some(configured_position_mode));
    Some(SymbolAccountConfig {
        exchange: exchange.clone(),
        canonical_symbol: canonical(&exchange, &symbol),
        exchange_symbol: ExchangeSymbol::new(exchange, symbol),
        position_mode,
        margin_mode: str_field(
            &item,
            &[
                "marginType",
                "marginMode",
                "margin_mode",
                "marginModeName",
                "mgnMode",
            ],
        )
        .as_deref()
        .map(parse_margin_mode_text),
        leverage: f64_field(
            &item,
            &[
                "leverage",
                "lever",
                "lever_rate",
                "crossLeverage",
                "crossedMarginLeverage",
                "crossedLeverage",
                "isolatedLeverage",
                "longLeverage",
                "shortLeverage",
                "holdModeLeverage",
            ],
        )
        .map(|leverage| leverage.round().max(0.0) as u32),
        max_leverage: f64_field(&item, &["maxLever", "maxLeverage", "max_leverage"])
            .map(|leverage| leverage.round().max(0.0) as u32),
        updated_at: received_at,
    })
}

fn symbol_matches(item: &Value, symbol: &ExchangeSymbol) -> bool {
    str_field(
        item,
        &["symbol", "instId", "contract", "contract_code", "name"],
    )
    .is_some_and(|value| value.eq_ignore_ascii_case(&symbol.symbol))
}

fn parse_rest_order_with_gate_contract_size(
    exchange: ExchangeId,
    fallback_symbol: &ExchangeSymbol,
    value: &Value,
    received_at: DateTime<Utc>,
    gate_contract_size: Option<f64>,
) -> Option<OrderState> {
    let item = response_readback_item(value, fallback_symbol)?;
    let event = match exchange {
        ExchangeId::Binance => parse_binance_order_or_fill(&item, received_at),
        ExchangeId::Okx => parse_okx_order(&item, received_at),
        ExchangeId::Bitget => parse_bitget_order_or_fill(&item, received_at),
        ExchangeId::Bybit => parse_bybit_order_or_fill(&item, received_at),
        ExchangeId::Mexc => parse_mexc_order_or_fill(&item, received_at),
        ExchangeId::Htx => parse_htx_order_or_fill(&item, received_at),
        ExchangeId::Toobit => parse_toobit_order_or_fill(&item, received_at),
        ExchangeId::Gate => parse_gate_order_or_fill_with_contract_size(
            &item,
            received_at,
            gate_contract_size.unwrap_or_else(|| gate_contract_size_from_item(&item)),
        ),
        _ => None,
    }?;
    match event.kind {
        PrivateEventKind::Order(mut order) => {
            if order.exchange_symbol.symbol.is_empty() {
                order.exchange_symbol = fallback_symbol.clone();
            }
            Some(order)
        }
        _ => None,
    }
}

fn parse_order_amendment(
    exchange: ExchangeId,
    query: &OrderAmendmentHistoryQuery,
    item: &Value,
) -> Option<OrderAmendmentEvent> {
    if exchange != ExchangeId::Binance {
        return None;
    }
    let exchange_symbol = str_field(item, &["symbol"])
        .map(|symbol| ExchangeSymbol::new(exchange.clone(), symbol))
        .unwrap_or_else(|| query.exchange_symbol.clone());
    Some(OrderAmendmentEvent {
        exchange,
        exchange_symbol,
        amendment_id: str_field(item, &["amendmentId"]),
        exchange_order_id: str_field(item, &["orderId"])
            .or_else(|| query.exchange_order_id.clone()),
        client_order_id: str_field(item, &["clientOrderId"])
            .or_else(|| query.client_order_id.clone()),
        price_before: nested_f64_field(item, &["amendment", "price", "before"]),
        price_after: nested_f64_field(item, &["amendment", "price", "after"]),
        quantity_before: nested_f64_field(item, &["amendment", "origQty", "before"]),
        quantity_after: nested_f64_field(item, &["amendment", "origQty", "after"]),
        amendment_count: nested_u32_field(item, &["amendment", "count"]),
        amended_at: millis_field(item, &["time"], Utc::now()),
    })
}

fn nested_f64_field(value: &Value, path: &[&str]) -> Option<f64> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    match current {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}

fn nested_u32_field(value: &Value, path: &[&str]) -> Option<u32> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    match current {
        Value::Number(number) => number.as_u64().map(|value| value as u32),
        Value::String(text) => text.parse::<u32>().ok(),
        _ => None,
    }
}

fn event_position(event: PrivateEvent) -> Option<ExchangePosition> {
    match event.kind {
        PrivateEventKind::Position(position) => Some(position),
        _ => None,
    }
}

fn event_balance(event: PrivateEvent) -> Option<ExchangeBalance> {
    match event.kind {
        PrivateEventKind::Balance(balance) => Some(balance),
        _ => None,
    }
}

fn event_fill(event: PrivateEvent) -> Option<FillEvent> {
    match event.kind {
        PrivateEventKind::Fill(fill) => Some(fill),
        _ => None,
    }
}

fn is_gate_position_not_found(exchange: ExchangeId, err: &anyhow::Error) -> bool {
    exchange == ExchangeId::Gate
        && err.downcast_ref::<PrivateRestError>().is_some_and(|error| {
            error.class == ExchangeErrorClass::OrderNotFound
                && error.code.as_deref() == Some("POSITION_NOT_FOUND")
        })
}

fn default_symbol_account_config(
    exchange: ExchangeId,
    symbol: &ExchangeSymbol,
    received_at: DateTime<Utc>,
    configured_position_mode: PositionMode,
) -> SymbolAccountConfig {
    SymbolAccountConfig {
        exchange: exchange.clone(),
        canonical_symbol: canonical(&exchange, &symbol.symbol),
        exchange_symbol: symbol.clone(),
        position_mode: Some(configured_position_mode),
        margin_mode: None,
        leverage: None,
        max_leverage: None,
        updated_at: received_at,
    }
}

fn parse_bitget_private_message(
    raw: &str,
    received_at: DateTime<Utc>,
) -> Result<Vec<PrivateEvent>> {
    if raw == "pong" {
        return Ok(vec![PrivateEvent::new(
            ExchangeId::Bitget,
            PrivateEventKind::Heartbeat,
            received_at,
        )]);
    }
    let value: Value = serde_json::from_str(raw)?;
    if value.get("event").and_then(Value::as_str) == Some("error") {
        return Ok(vec![private_error_event(
            ExchangeId::Bitget,
            ExchangeErrorClass::InvalidRequest,
            str_field(&value, &["code"]),
            str_field(&value, &["msg", "message"])
                .unwrap_or_else(|| "bitget websocket error".to_string()),
            received_at,
        )
        .with_raw(value)]);
    }
    if value.get("event").and_then(Value::as_str) == Some("login")
        || value.get("event").and_then(Value::as_str) == Some("subscribe")
    {
        return Ok(vec![PrivateEvent::new(
            ExchangeId::Bitget,
            PrivateEventKind::Heartbeat,
            received_at,
        )
        .with_raw(value)]);
    }
    let channel = value
        .pointer("/arg/channel")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let data = value
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut events = Vec::new();
    for item in data {
        let event = match channel {
            "orders" => parse_bitget_order_or_fill(&item, received_at),
            "positions" => parse_bitget_position(&item, received_at),
            "account" => parse_bitget_balance(&item, received_at),
            _ => None,
        };
        if let Some(event) = event {
            events.push(event.with_raw(item));
        }
    }
    Ok(events)
}

fn parse_gate_private_message(raw: &str, received_at: DateTime<Utc>) -> Result<Vec<PrivateEvent>> {
    let value: Value = serde_json::from_str(raw)?;
    if value.get("event").and_then(Value::as_str) == Some("subscribe") {
        if let Some(status) = value.pointer("/result/status").and_then(Value::as_str) {
            if status != "success" {
                return Ok(vec![private_error_event(
                    ExchangeId::Gate,
                    ExchangeErrorClass::InvalidRequest,
                    None,
                    str_field(value.get("result").unwrap_or(&value), &["message", "error"])
                        .unwrap_or_else(|| format!("gate websocket subscribe status={status}")),
                    received_at,
                )
                .with_raw(value)]);
            }
        }
        return Ok(vec![PrivateEvent::new(
            ExchangeId::Gate,
            PrivateEventKind::Heartbeat,
            received_at,
        )
        .with_raw(value)]);
    }
    let channel = value
        .get("channel")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let result = value.get("result").cloned().unwrap_or(Value::Null);
    let items = match result {
        Value::Array(items) => items,
        Value::Object(_) => vec![result],
        _ => Vec::new(),
    };
    let mut events = Vec::new();
    for item in items {
        let event = match channel {
            "futures.orders" => parse_gate_order_or_fill(&item, received_at),
            "futures.usertrades" => parse_gate_fill(&item, received_at),
            "futures.positions" => parse_gate_position(&item, received_at),
            "futures.balances" => parse_gate_balance(&item, received_at),
            _ => None,
        };
        if let Some(event) = event {
            events.push(event.with_raw(item));
        }
    }
    Ok(events)
}

fn parse_private_ws_message_with_symbol_rules<P>(
    protocol: P,
    gate_contract_sizes: &HashMap<String, f64>,
    raw: &str,
    received_at: DateTime<Utc>,
) -> Result<Vec<PrivateEvent>>
where
    P: PrivatePerpProtocol + Send + Sync + Copy + 'static,
{
    if protocol.exchange() == PrivatePerpExchange::Gate {
        return parse_gate_private_message_with_contract_sizes(
            raw,
            received_at,
            gate_contract_sizes,
        );
    }
    protocol.parse_private_ws_message(raw, received_at)
}

fn parse_gate_private_message_with_contract_sizes(
    raw: &str,
    received_at: DateTime<Utc>,
    contract_sizes: &HashMap<String, f64>,
) -> Result<Vec<PrivateEvent>> {
    let value: Value = serde_json::from_str(raw)?;
    if value.get("event").and_then(Value::as_str) == Some("subscribe") {
        return parse_gate_private_message(raw, received_at);
    }
    let channel = value
        .get("channel")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let result = value.get("result").cloned().unwrap_or(Value::Null);
    let items = match result {
        Value::Array(items) => items,
        Value::Object(_) => vec![result],
        _ => Vec::new(),
    };
    let mut events = Vec::new();
    for item in items {
        let contract_size = gate_contract_size_for_ws_item(&item, contract_sizes);
        let event = match channel {
            "futures.orders" => {
                parse_gate_order_or_fill_with_contract_size(&item, received_at, contract_size)
            }
            "futures.usertrades" => {
                parse_gate_fill_with_contract_size(&item, received_at, contract_size)
            }
            "futures.positions" => {
                parse_gate_position_with_contract_size(&item, received_at, contract_size)
            }
            "futures.balances" => parse_gate_balance(&item, received_at),
            _ => None,
        };
        if let Some(event) = event {
            events.push(event.with_raw(item));
        }
    }
    Ok(events)
}

fn gate_contract_size_for_ws_item(item: &Value, contract_sizes: &HashMap<String, f64>) -> f64 {
    str_field(item, &["contract"])
        .and_then(|symbol| contract_sizes.get(&symbol).copied())
        .unwrap_or_else(|| gate_contract_size_from_item(item))
}

fn gate_contract_sizes_from_symbols(symbols: &[ExchangeSymbol]) -> HashMap<String, f64> {
    symbols
        .iter()
        .filter(|symbol| symbol.exchange == ExchangeId::Gate)
        .map(|symbol| (symbol.symbol.clone(), 1.0))
        .collect()
}

fn gate_contract_sizes_from_instruments(
    instruments: impl IntoIterator<Item = InstrumentMeta>,
) -> HashMap<String, f64> {
    instruments
        .into_iter()
        .filter(|instrument| instrument.exchange == ExchangeId::Gate)
        .map(|instrument| {
            (
                instrument.exchange_symbol.symbol.clone(),
                instrument_contract_size(&instrument),
            )
        })
        .collect()
}

fn parse_okx_private_message(raw: &str, received_at: DateTime<Utc>) -> Result<Vec<PrivateEvent>> {
    let value: Value = serde_json::from_str(raw)?;
    if value.get("event").and_then(Value::as_str) == Some("login")
        || value.get("event").and_then(Value::as_str) == Some("subscribe")
    {
        let code = str_field(&value, &["code"]).unwrap_or_else(|| "0".to_string());
        if code != "0" {
            return Ok(vec![private_error_event(
                ExchangeId::Okx,
                ExchangeErrorClass::InvalidRequest,
                Some(code),
                str_field(&value, &["msg", "message"])
                    .unwrap_or_else(|| "okx websocket error".to_string()),
                received_at,
            )
            .with_raw(value)]);
        }
        return Ok(vec![PrivateEvent::new(
            ExchangeId::Okx,
            PrivateEventKind::Heartbeat,
            received_at,
        )
        .with_raw(value)]);
    }
    if value.get("event").and_then(Value::as_str) == Some("error") {
        return Ok(vec![private_error_event(
            ExchangeId::Okx,
            ExchangeErrorClass::InvalidRequest,
            str_field(&value, &["code"]),
            str_field(&value, &["msg", "message"])
                .unwrap_or_else(|| "okx websocket error".to_string()),
            received_at,
        )
        .with_raw(value)]);
    }

    let channel = value
        .pointer("/arg/channel")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let items = value
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut events = Vec::new();
    for item in items {
        match channel {
            "orders" => {
                if let Some(event) = parse_okx_order(&item, received_at) {
                    events.push(event.with_raw(item.clone()));
                }
                if let Some(event) = parse_okx_fill(&item, received_at) {
                    events.push(event.with_raw(item));
                }
            }
            "positions" => {
                if let Some(event) = parse_okx_position(&item, received_at) {
                    events.push(event.with_raw(item));
                }
            }
            "account" => {
                if let Some(event) = parse_okx_balance(&item, received_at) {
                    events.push(event.with_raw(item));
                }
            }
            _ => {}
        }
    }
    Ok(events)
}

fn parse_bybit_private_message(raw: &str, received_at: DateTime<Utc>) -> Result<Vec<PrivateEvent>> {
    let value: Value = serde_json::from_str(raw)?;
    if value.get("op").and_then(Value::as_str) == Some("pong")
        || value.get("success").and_then(Value::as_bool) == Some(true)
    {
        return Ok(vec![PrivateEvent::new(
            ExchangeId::Bybit,
            PrivateEventKind::Heartbeat,
            received_at,
        )
        .with_raw(value)]);
    }
    if value.get("success").and_then(Value::as_bool) == Some(false) {
        return Ok(vec![private_error_event(
            ExchangeId::Bybit,
            ExchangeErrorClass::InvalidRequest,
            str_field(&value, &["retCode", "code"]),
            str_field(&value, &["retMsg", "msg", "message"])
                .unwrap_or_else(|| "bybit websocket error".to_string()),
            received_at,
        )
        .with_raw(value)]);
    }
    let topic = value
        .get("topic")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let data = value
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut events = Vec::new();
    for item in data {
        let event = match topic {
            "order" => parse_bybit_order_or_fill(&item, received_at),
            "execution" => parse_bybit_fill(&item, received_at),
            "position" => parse_bybit_position(&item, received_at),
            "wallet" => parse_bybit_balance(&item, received_at),
            _ => None,
        };
        if let Some(event) = event {
            events.push(event.with_raw(item));
        }
    }
    Ok(events)
}

fn parse_mexc_private_message(raw: &str, received_at: DateTime<Utc>) -> Result<Vec<PrivateEvent>> {
    let value: Value = serde_json::from_str(raw)?;
    if value.get("channel").and_then(Value::as_str) == Some("pong")
        || value.get("method").and_then(Value::as_str) == Some("login")
    {
        return Ok(vec![PrivateEvent::new(
            ExchangeId::Mexc,
            PrivateEventKind::Heartbeat,
            received_at,
        )
        .with_raw(value)]);
    }
    if value.get("success").and_then(Value::as_bool) == Some(false) {
        return Ok(vec![private_error_event(
            ExchangeId::Mexc,
            ExchangeErrorClass::InvalidRequest,
            str_field(&value, &["code"]),
            str_field(&value, &["message", "msg"])
                .unwrap_or_else(|| "mexc websocket error".to_string()),
            received_at,
        )
        .with_raw(value)]);
    }
    let channel = value
        .get("channel")
        .or_else(|| value.get("method"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let data = value.get("data").cloned().unwrap_or(Value::Null);
    let items = match data {
        Value::Array(items) => items,
        Value::Object(_) => vec![data],
        _ => Vec::new(),
    };
    let mut events = Vec::new();
    for item in items {
        let event = if channel.contains("order") {
            parse_mexc_order_or_fill(&item, received_at)
        } else if channel.contains("deal") {
            parse_mexc_fill(&item, received_at)
        } else if channel.contains("position") {
            parse_mexc_position(&item, received_at)
        } else if channel.contains("asset") || channel.contains("account") {
            parse_mexc_balance(&item, received_at)
        } else {
            None
        };
        if let Some(event) = event {
            events.push(event.with_raw(item));
        }
    }
    Ok(events)
}

fn parse_htx_private_message(raw: &str, received_at: DateTime<Utc>) -> Result<Vec<PrivateEvent>> {
    let value: Value = serde_json::from_str(raw)?;
    if value.get("op").and_then(Value::as_str) == Some("ping") {
        return Ok(vec![PrivateEvent::new(
            ExchangeId::Htx,
            PrivateEventKind::Heartbeat,
            received_at,
        )
        .with_raw(value)]);
    }
    if value.get("op").and_then(Value::as_str) == Some("pong")
        || value.get("pong").is_some()
        || value.get("ping").is_some()
    {
        return Ok(vec![PrivateEvent::new(
            ExchangeId::Htx,
            PrivateEventKind::Heartbeat,
            received_at,
        )
        .with_raw(value)]);
    }
    if value.get("op").and_then(Value::as_str) == Some("auth")
        || value.get("op").and_then(Value::as_str) == Some("sub")
    {
        if value
            .get("err-code")
            .or_else(|| value.get("err_code"))
            .is_some()
        {
            return Ok(vec![private_error_event(
                ExchangeId::Htx,
                ExchangeErrorClass::InvalidRequest,
                str_field(&value, &["err-code", "err_code"]),
                str_field(&value, &["err-msg", "err_msg"])
                    .unwrap_or_else(|| "htx websocket error".to_string()),
                received_at,
            )
            .with_raw(value)]);
        }
        return Ok(vec![PrivateEvent::new(
            ExchangeId::Htx,
            PrivateEventKind::Heartbeat,
            received_at,
        )
        .with_raw(value)]);
    }
    let topic = value
        .get("topic")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let data = value.get("data").cloned().unwrap_or(Value::Null);
    let items = match data {
        Value::Array(items) => items,
        Value::Object(_) => vec![data],
        _ => Vec::new(),
    };
    let mut events = Vec::new();
    for item in items {
        let event = if topic.contains("orders") && !topic.contains("match_orders") {
            parse_htx_order_or_fill(&item, received_at)
        } else if topic.contains("match_orders") {
            parse_htx_fill(&item, received_at)
        } else if topic.contains("positions") {
            parse_htx_position(&item, received_at)
        } else if topic.contains("accounts") {
            parse_htx_balance(&item, received_at)
        } else {
            None
        };
        if let Some(event) = event {
            events.push(event.with_raw(item));
        }
    }
    Ok(events)
}

fn parse_toobit_private_message(
    raw: &str,
    received_at: DateTime<Utc>,
) -> Result<Vec<PrivateEvent>> {
    let value: Value = serde_json::from_str(raw)?;
    if value.get("pong").is_some()
        || value.get("ping").is_some()
        || value.get("event").and_then(Value::as_str) == Some("sub")
        || value.get("event").and_then(Value::as_str) == Some("subscribe")
        || value.get("e").and_then(Value::as_str) == Some("listenKeyExpired")
    {
        return Ok(vec![PrivateEvent::new(
            ExchangeId::Toobit,
            PrivateEventKind::Heartbeat,
            received_at,
        )
        .with_raw(value)]);
    }
    if value.get("code").is_some()
        && value
            .get("code")
            .and_then(Value::as_i64)
            .is_some_and(|code| code != 0)
    {
        return Ok(vec![private_error_event(
            ExchangeId::Toobit,
            ExchangeErrorClass::InvalidRequest,
            str_field(&value, &["code"]),
            str_field(&value, &["msg", "message"])
                .unwrap_or_else(|| "toobit websocket error".to_string()),
            received_at,
        )
        .with_raw(value)]);
    }

    let event_type = value
        .get("e")
        .or_else(|| value.get("eventType"))
        .or_else(|| value.get("event"))
        .or_else(|| value.get("topic"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let data = value
        .get("data")
        .or_else(|| value.get("o"))
        .cloned()
        .unwrap_or_else(|| value.clone());
    let items = match data {
        Value::Array(items) => items,
        Value::Object(_) => vec![data],
        _ => Vec::new(),
    };

    let mut events = Vec::new();
    for item in items {
        let event = if matches!(event_type, "executionReport" | "ORDER_TRADE_UPDATE")
            || event_type.contains("order")
        {
            parse_toobit_order_or_fill(&item, received_at)
        } else if event_type == "ticketInfo" || event_type.contains("trade") {
            parse_toobit_fill(&item, received_at)
        } else if event_type.contains("position") {
            parse_toobit_position(&item, received_at)
        } else if event_type == "outboundAccountInfo"
            || event_type.contains("account")
            || event_type.contains("balance")
        {
            parse_toobit_balance(&item, received_at)
        } else if item
            .get("positionAmt")
            .or_else(|| item.get("positionQty"))
            .is_some()
        {
            parse_toobit_position(&item, received_at)
        } else if item
            .get("asset")
            .or_else(|| item.get("availableBalance"))
            .or_else(|| item.get("walletBalance"))
            .is_some()
        {
            parse_toobit_balance(&item, received_at)
        } else if item.get("orderId").is_some() {
            parse_toobit_order_or_fill(&item, received_at)
        } else {
            None
        };
        if let Some(event) = event {
            events.push(event.with_raw(item));
        }
    }
    Ok(events)
}

fn parse_binance_order_or_fill(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    if item.get("commission").is_some()
        || item.get("tradeId").is_some()
        || item.get("buyer").is_some()
    {
        return parse_binance_fill(item, received_at);
    }
    let symbol = str_field(item, &["symbol"])?;
    let side = parse_order_side(str_field(item, &["side"]).as_deref());
    Some(PrivateEvent::order(
        OrderState {
            exchange: ExchangeId::Binance,
            canonical_symbol: canonical(&ExchangeId::Binance, &symbol),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Binance, symbol),
            client_order_id: str_field(item, &["clientOrderId", "origClientOrderId"]),
            exchange_order_id: str_field(item, &["orderId"]),
            side,
            position_side: binance_position_side_from_text(
                str_field(item, &["positionSide"]).as_deref(),
                f64_field(item, &["positionAmt"]),
                side,
            ),
            order_type: parse_order_type(str_field(item, &["type", "origType"]).as_deref()),
            quantity: f64_field(item, &["origQty", "quantity", "qty"]).unwrap_or_default(),
            price: f64_field(item, &["price"]),
            filled_quantity: f64_field(item, &["executedQty", "cumQty"]).unwrap_or_default(),
            average_fill_price: f64_field(item, &["avgPrice", "averagePrice"]),
            time_in_force: parse_time_in_force(str_field(item, &["timeInForce"]).as_deref()),
            reduce_only: bool_field(item, &["reduceOnly"]),
            status: binance_status(str_field(item, &["status"]).as_deref()),
            updated_at: millis_field(item, &["updateTime", "time"], received_at),
        },
        received_at,
    ))
}

fn parse_binance_fill(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    let symbol = str_field(item, &["symbol"])?;
    let side = str_field(item, &["side"])
        .as_deref()
        .map(|side| parse_order_side(Some(side)))
        .unwrap_or_else(|| {
            if bool_field(item, &["buyer"]) {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            }
        });
    let price = f64_field(item, &["price", "lastPrice"]).unwrap_or_default();
    let quantity = f64_field(item, &["qty", "quantity", "lastQty"]).unwrap_or_default();
    Some(PrivateEvent::fill(
        FillEvent {
            exchange: ExchangeId::Binance,
            canonical_symbol: canonical(&ExchangeId::Binance, &symbol),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Binance, symbol),
            trade_id: str_field(item, &["id", "tradeId"])
                .unwrap_or_else(|| format!("binance-fill-{}", received_at.timestamp_millis())),
            client_order_id: str_field(item, &["clientOrderId", "origClientOrderId"]),
            exchange_order_id: str_field(item, &["orderId"]),
            side,
            position_side: binance_position_side_from_text(
                str_field(item, &["positionSide"]).as_deref(),
                None,
                side,
            ),
            liquidity: if bool_field(item, &["maker", "isMaker"]) {
                FillLiquidity::Maker
            } else {
                FillLiquidity::Taker
            },
            price,
            quantity,
            quote_quantity: f64_field(item, &["quoteQty", "quoteQuantity"])
                .unwrap_or(price * quantity),
            fee: f64_field(item, &["commission", "fee"]),
            fee_asset: str_field(item, &["commissionAsset", "feeAsset"]),
            fee_rate: None,
            realized_pnl: f64_field(item, &["realizedPnl"]),
            reduce_only: None,
            filled_at: millis_field(item, &["time", "tradeTime"], received_at),
            received_at,
        },
        received_at,
    ))
}

fn parse_binance_position(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    let symbol = str_field(item, &["symbol"])?;
    let signed_qty = f64_field(item, &["positionAmt", "positionAmount"]).unwrap_or_default();
    Some(PrivateEvent::position(
        ExchangePosition {
            exchange: ExchangeId::Binance,
            canonical_symbol: canonical(&ExchangeId::Binance, &symbol),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Binance, symbol),
            position_side: binance_position_side_from_text(
                str_field(item, &["positionSide"]).as_deref(),
                Some(signed_qty),
                if signed_qty < 0.0 {
                    OrderSide::Sell
                } else {
                    OrderSide::Buy
                },
            ),
            quantity: signed_qty.abs(),
            entry_price: f64_field(item, &["entryPrice"]),
            mark_price: f64_field(item, &["markPrice"]),
            unrealized_pnl: f64_field(item, &["unRealizedProfit", "unrealizedProfit"]),
            updated_at: millis_field(item, &["updateTime"], received_at),
        },
        received_at,
    ))
}

fn parse_binance_balance(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    let asset = str_field(item, &["asset"])?;
    let total = f64_field(item, &["balance", "walletBalance"]).unwrap_or_default();
    let available = f64_field(item, &["availableBalance", "maxWithdrawAmount"])
        .or_else(|| f64_field(item, &["crossWalletBalance"]))
        .unwrap_or(total);
    Some(PrivateEvent::balance(
        ExchangeBalance {
            exchange: ExchangeId::Binance,
            asset,
            total,
            available,
            locked: (total - available).max(0.0),
            updated_at: millis_field(item, &["updateTime"], received_at),
        },
        received_at,
    ))
}

fn parse_bitget_order_or_fill(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    if item.get("tradeId").or_else(|| item.get("fillId")).is_some() {
        return parse_bitget_fill(item, received_at);
    }
    let symbol = str_field(item, &["symbol", "instId"])?;
    let exchange_symbol = ExchangeSymbol::new(ExchangeId::Bitget, symbol.clone());
    let status = bitget_order_status(item);
    let side = parse_order_side(str_field(item, &["side"]).as_deref());
    let trade_side = str_field(item, &["tradeSide"]).unwrap_or_default();
    Some(PrivateEvent::order(
        OrderState {
            exchange: ExchangeId::Bitget,
            canonical_symbol: canonical(&ExchangeId::Bitget, &symbol),
            exchange_symbol,
            client_order_id: str_field(item, &["clientOid", "clientOrderId"]),
            exchange_order_id: str_field(item, &["orderId", "id"]),
            side,
            position_side: position_side_from_trade_side(&trade_side, side),
            order_type: parse_order_type(str_field(item, &["orderType"]).as_deref()),
            quantity: f64_field(item, &["size", "qty"]).unwrap_or_default(),
            price: f64_field(item, &["price"]),
            filled_quantity: f64_field(item, &["accBaseVolume", "filledQty", "fillSize"])
                .unwrap_or_default(),
            average_fill_price: f64_field(item, &["priceAvg", "avgPrice", "fillPrice"]),
            time_in_force: TimeInForce::Gtc,
            reduce_only: trade_side.eq_ignore_ascii_case("close")
                || str_field(item, &["reduceOnly"]).is_some_and(|v| v.eq_ignore_ascii_case("yes")),
            status,
            updated_at: millis_field(item, &["uTime", "updatedTime", "cTime"], received_at),
        },
        received_at,
    ))
}

fn parse_bitget_fill(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    let symbol = str_field(item, &["symbol", "instId"])?;
    let side = parse_order_side(str_field(item, &["side"]).as_deref());
    let trade_side = str_field(item, &["tradeSide"]).unwrap_or_default();
    let price = f64_field(item, &["fillPrice", "price"]).unwrap_or_default();
    let quantity = f64_field(item, &["fillSize", "size", "baseVolume"]).unwrap_or_default();
    let fee_detail = bitget_fee_detail(item);
    Some(PrivateEvent::fill(
        FillEvent {
            exchange: ExchangeId::Bitget,
            canonical_symbol: canonical(&ExchangeId::Bitget, &symbol),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Bitget, symbol),
            trade_id: str_field(item, &["tradeId", "fillId"])
                .unwrap_or_else(|| format!("bitget-fill-{}", received_at.timestamp_millis())),
            client_order_id: str_field(item, &["clientOid", "clientOrderId"]),
            exchange_order_id: str_field(item, &["orderId"]),
            side,
            position_side: position_side_from_trade_side(&trade_side, side),
            liquidity: liquidity(str_field(item, &["execType", "role"]).as_deref()),
            price,
            quantity,
            quote_quantity: f64_field(item, &["quoteVolume"]).unwrap_or(price * quantity),
            fee: f64_field(item, &["fee"]).or(fee_detail.as_ref().and_then(|detail| detail.0)),
            fee_asset: str_field(item, &["feeCcy", "feeCoin"])
                .or_else(|| fee_detail.and_then(|detail| detail.1)),
            fee_rate: None,
            realized_pnl: f64_field(item, &["profit", "realizedPnl"]),
            reduce_only: Some(trade_side.eq_ignore_ascii_case("close")),
            filled_at: millis_field(item, &["tradeTime", "cTime", "uTime"], received_at),
            received_at,
        },
        received_at,
    ))
}

fn bitget_fee_detail(item: &Value) -> Option<(Option<f64>, Option<String>)> {
    let detail = item.get("feeDetail")?.as_array()?.first()?;
    Some((
        f64_field(detail, &["totalFee", "fee", "totalDeductionFee"]),
        str_field(detail, &["feeCoin", "feeCcy"]),
    ))
}

fn parse_bitget_position(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    let symbol = str_field(item, &["symbol", "instId"])?;
    let side_text = str_field(item, &["holdSide", "posSide"]).unwrap_or_default();
    Some(PrivateEvent::position(
        ExchangePosition {
            exchange: ExchangeId::Bitget,
            canonical_symbol: canonical(&ExchangeId::Bitget, &symbol),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Bitget, symbol),
            position_side: parse_position_side(&side_text),
            quantity: f64_field(item, &["total", "size", "available"])
                .unwrap_or_default()
                .abs(),
            entry_price: f64_field(item, &["openPriceAvg", "entryPrice"]),
            mark_price: f64_field(item, &["markPrice"]),
            unrealized_pnl: f64_field(item, &["unrealizedPL", "unrealizedPnl"]),
            updated_at: millis_field(item, &["uTime", "cTime"], received_at),
        },
        received_at,
    ))
}

fn parse_bitget_balance(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    let asset = str_field(item, &["marginCoin", "coin"])?;
    Some(PrivateEvent::balance(
        ExchangeBalance {
            exchange: ExchangeId::Bitget,
            asset,
            total: f64_field(item, &["equity", "accountEquity"]).unwrap_or_default(),
            available: f64_field(item, &["available", "availableBalance"]).unwrap_or_default(),
            locked: f64_field(item, &["locked", "frozen"]).unwrap_or_default(),
            updated_at: received_at,
        },
        received_at,
    ))
}

fn parse_okx_order(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    let symbol = str_field(item, &["instId"])?;
    let side = parse_order_side(str_field(item, &["side"]).as_deref());
    let filled_quantity = f64_field(item, &["accFillSz", "fillSz"]).unwrap_or_default();
    let quantity = f64_field(item, &["sz"]).unwrap_or(filled_quantity);
    Some(PrivateEvent::order(
        OrderState {
            exchange: ExchangeId::Okx,
            canonical_symbol: canonical(&ExchangeId::Okx, &symbol),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Okx, symbol),
            client_order_id: str_field(item, &["clOrdId"]),
            exchange_order_id: str_field(item, &["ordId"]),
            side,
            position_side: parse_position_side(
                str_field(item, &["posSide"]).as_deref().unwrap_or_default(),
            ),
            order_type: parse_order_type(str_field(item, &["ordType"]).as_deref()),
            quantity,
            price: f64_field(item, &["px"]),
            filled_quantity,
            average_fill_price: f64_field(item, &["avgPx", "fillPx"]),
            time_in_force: parse_time_in_force(str_field(item, &["ordType"]).as_deref()),
            reduce_only: bool_field(item, &["reduceOnly"]),
            status: okx_status(str_field(item, &["state"]).as_deref()),
            updated_at: millis_field(item, &["uTime", "cTime"], received_at),
        },
        received_at,
    ))
}

fn parse_okx_fill(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    let quantity = f64_field(item, &["fillSz"])?;
    if quantity <= 0.0 {
        return None;
    }
    let symbol = str_field(item, &["instId"])?;
    let side = parse_order_side(str_field(item, &["side"]).as_deref());
    let price = f64_field(item, &["fillPx", "avgPx", "px"]).unwrap_or_default();
    Some(PrivateEvent::fill(
        FillEvent {
            exchange: ExchangeId::Okx,
            canonical_symbol: canonical(&ExchangeId::Okx, &symbol),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Okx, symbol),
            trade_id: str_field(item, &["tradeId", "fillId"])
                .unwrap_or_else(|| format!("okx-fill-{}", received_at.timestamp_millis())),
            client_order_id: str_field(item, &["clOrdId"]),
            exchange_order_id: str_field(item, &["ordId"]),
            side,
            position_side: parse_position_side(
                str_field(item, &["posSide"]).as_deref().unwrap_or_default(),
            ),
            liquidity: liquidity(str_field(item, &["execType", "fillRole"]).as_deref()),
            price,
            quantity,
            quote_quantity: f64_field(item, &["fillNotionalUsd"]).unwrap_or(price * quantity),
            fee: f64_field(item, &["fee"]).map(f64::abs),
            fee_asset: str_field(item, &["feeCcy"]),
            fee_rate: None,
            realized_pnl: f64_field(item, &["pnl", "fillPnl"]),
            reduce_only: bool_field(item, &["reduceOnly"]).then_some(true),
            filled_at: millis_field(item, &["fillTime", "uTime", "cTime"], received_at),
            received_at,
        },
        received_at,
    ))
}

fn parse_okx_position(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    let symbol = str_field(item, &["instId"])?;
    Some(PrivateEvent::position(
        ExchangePosition {
            exchange: ExchangeId::Okx,
            canonical_symbol: canonical(&ExchangeId::Okx, &symbol),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Okx, symbol),
            position_side: parse_position_side(
                str_field(item, &["posSide"]).as_deref().unwrap_or_default(),
            ),
            quantity: f64_field(item, &["pos"]).unwrap_or_default().abs(),
            entry_price: f64_field(item, &["avgPx"]),
            mark_price: f64_field(item, &["markPx", "last"]),
            unrealized_pnl: f64_field(item, &["upl", "uplRatio"]),
            updated_at: millis_field(item, &["uTime", "cTime"], received_at),
        },
        received_at,
    ))
}

fn parse_okx_balance(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    if let Some(details) = item.get("details").and_then(Value::as_array) {
        return details
            .first()
            .and_then(|detail| parse_okx_balance(detail, received_at));
    }
    let asset = str_field(item, &["ccy"]).unwrap_or_else(|| "USDT".to_string());
    let total = f64_field(item, &["eq", "cashBal", "bal"]).unwrap_or_default();
    let available = f64_field(item, &["availEq", "availBal", "available"]).unwrap_or(total);
    Some(PrivateEvent::balance(
        ExchangeBalance {
            exchange: ExchangeId::Okx,
            asset,
            total,
            available,
            locked: (total - available).max(0.0),
            updated_at: millis_field(item, &["uTime"], received_at),
        },
        received_at,
    ))
}

fn parse_bybit_order_or_fill(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    if item
        .get("execId")
        .or_else(|| item.get("execPrice"))
        .is_some()
    {
        return parse_bybit_fill(item, received_at);
    }
    let symbol = str_field(item, &["symbol"])?;
    let side = parse_order_side(str_field(item, &["side"]).as_deref());
    Some(PrivateEvent::order(
        OrderState {
            exchange: ExchangeId::Bybit,
            canonical_symbol: canonical(&ExchangeId::Bybit, &symbol),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Bybit, symbol),
            client_order_id: str_field(item, &["orderLinkId"]),
            exchange_order_id: str_field(item, &["orderId"]),
            side,
            position_side: bybit_position_side_from_idx(i64_field(item, &["positionIdx"]), side),
            order_type: parse_order_type(str_field(item, &["orderType"]).as_deref()),
            quantity: f64_field(item, &["qty", "cumExecQty"]).unwrap_or_default(),
            price: f64_field(item, &["price"]),
            filled_quantity: f64_field(item, &["cumExecQty"]).unwrap_or_default(),
            average_fill_price: f64_field(item, &["avgPrice"]),
            time_in_force: parse_time_in_force(str_field(item, &["timeInForce"]).as_deref()),
            reduce_only: bool_field(item, &["reduceOnly"]),
            status: bybit_status(str_field(item, &["orderStatus"]).as_deref()),
            updated_at: millis_field(item, &["updatedTime", "createdTime"], received_at),
        },
        received_at,
    ))
}

fn parse_bybit_fill(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    let symbol = str_field(item, &["symbol"])?;
    let side = parse_order_side(str_field(item, &["side"]).as_deref());
    let price = f64_field(item, &["execPrice", "price"]).unwrap_or_default();
    let quantity = f64_field(item, &["execQty", "qty"]).unwrap_or_default();
    Some(PrivateEvent::fill(
        FillEvent {
            exchange: ExchangeId::Bybit,
            canonical_symbol: canonical(&ExchangeId::Bybit, &symbol),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Bybit, symbol),
            trade_id: str_field(item, &["execId"])
                .unwrap_or_else(|| format!("bybit-fill-{}", received_at.timestamp_millis())),
            client_order_id: str_field(item, &["orderLinkId"]),
            exchange_order_id: str_field(item, &["orderId"]),
            side,
            position_side: bybit_position_side_from_idx(i64_field(item, &["positionIdx"]), side),
            liquidity: liquidity(str_field(item, &["execType", "isMaker"]).as_deref()),
            price,
            quantity,
            quote_quantity: f64_field(item, &["execValue"]).unwrap_or(price * quantity),
            fee: f64_field(item, &["execFee"]),
            fee_asset: str_field(item, &["feeCurrency"]).or_else(|| Some("USDT".to_string())),
            fee_rate: f64_field(item, &["feeRate"]),
            realized_pnl: f64_field(item, &["execPnl"]),
            reduce_only: None,
            filled_at: millis_field(item, &["execTime", "createdTime"], received_at),
            received_at,
        },
        received_at,
    ))
}

fn parse_bybit_position(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    let symbol = str_field(item, &["symbol"])?;
    Some(PrivateEvent::position(
        ExchangePosition {
            exchange: ExchangeId::Bybit,
            canonical_symbol: canonical(&ExchangeId::Bybit, &symbol),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Bybit, symbol),
            position_side: parse_position_side(
                str_field(item, &["side"]).as_deref().unwrap_or_default(),
            ),
            quantity: f64_field(item, &["size"]).unwrap_or_default().abs(),
            entry_price: f64_field(item, &["avgPrice", "entryPrice"]),
            mark_price: f64_field(item, &["markPrice"]),
            unrealized_pnl: f64_field(item, &["unrealisedPnl", "unrealizedPnl"]),
            updated_at: millis_field(item, &["updatedTime", "createdTime"], received_at),
        },
        received_at,
    ))
}

fn parse_bybit_balance(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    if let Some(coins) = item.get("coin").and_then(Value::as_array) {
        return coins
            .first()
            .and_then(|coin| parse_bybit_balance(coin, received_at));
    }
    let asset = str_field(item, &["coin"]).unwrap_or_else(|| "USDT".to_string());
    Some(PrivateEvent::balance(
        ExchangeBalance {
            exchange: ExchangeId::Bybit,
            asset,
            total: f64_field(item, &["walletBalance", "equity", "totalEquity"]).unwrap_or_default(),
            available: f64_field(
                item,
                &["availableToWithdraw", "availableBalance", "walletBalance"],
            )
            .unwrap_or_default(),
            locked: f64_field(item, &["locked", "orderIM", "totalOrderIM"]).unwrap_or_default(),
            updated_at: received_at,
        },
        received_at,
    ))
}

fn parse_mexc_order_or_fill(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    if item.get("tradeId").or_else(|| item.get("dealId")).is_some() {
        return parse_mexc_fill(item, received_at);
    }
    let symbol = str_field(item, &["symbol"])?;
    let side = mexc_side_to_order_side(i64_field(item, &["side"]).unwrap_or_default());
    Some(PrivateEvent::order(
        OrderState {
            exchange: ExchangeId::Mexc,
            canonical_symbol: canonical(&ExchangeId::Mexc, &symbol),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Mexc, symbol),
            client_order_id: str_field(item, &["externalOid", "clientOrderId"]),
            exchange_order_id: str_field(item, &["orderId", "id"]),
            side,
            position_side: mexc_side_to_position_side(
                i64_field(item, &["side"]).unwrap_or_default(),
                side,
            ),
            order_type: mexc_parse_order_type(i64_field(item, &["type", "orderType"])),
            quantity: f64_field(item, &["vol", "qty"]).unwrap_or_default(),
            price: f64_field(item, &["price"]),
            filled_quantity: f64_field(item, &["dealVol", "filledQty"]).unwrap_or_default(),
            average_fill_price: f64_field(item, &["dealAvgPrice", "avgPrice"]),
            time_in_force: TimeInForce::Gtc,
            reduce_only: mexc_side_is_close(i64_field(item, &["side"]).unwrap_or_default()),
            status: mexc_order_status(item),
            updated_at: millis_field(item, &["updateTime", "createTime", "time"], received_at),
        },
        received_at,
    ))
}

fn parse_mexc_fill(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    let symbol = str_field(item, &["symbol"])?;
    let side = mexc_side_to_order_side(i64_field(item, &["side"]).unwrap_or_default());
    let price = f64_field(item, &["price", "dealPrice"]).unwrap_or_default();
    let quantity = f64_field(item, &["vol", "qty", "dealVol"]).unwrap_or_default();
    Some(PrivateEvent::fill(
        FillEvent {
            exchange: ExchangeId::Mexc,
            canonical_symbol: canonical(&ExchangeId::Mexc, &symbol),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Mexc, symbol),
            trade_id: str_field(item, &["tradeId", "dealId", "id"])
                .unwrap_or_else(|| format!("mexc-fill-{}", received_at.timestamp_millis())),
            client_order_id: str_field(item, &["externalOid", "clientOrderId"]),
            exchange_order_id: str_field(item, &["orderId"]),
            side,
            position_side: mexc_side_to_position_side(
                i64_field(item, &["side"]).unwrap_or_default(),
                side,
            ),
            liquidity: liquidity(str_field(item, &["role", "takerOrMaker"]).as_deref()),
            price,
            quantity,
            quote_quantity: f64_field(item, &["amount"]).unwrap_or(price * quantity),
            fee: f64_field(item, &["fee"]),
            fee_asset: str_field(item, &["feeCurrency"]).or_else(|| Some("USDT".to_string())),
            fee_rate: None,
            realized_pnl: f64_field(item, &["profit", "realisedPnl", "realizedPnl"]),
            reduce_only: Some(mexc_side_is_close(
                i64_field(item, &["side"]).unwrap_or_default(),
            )),
            filled_at: millis_field(item, &["time", "createTime"], received_at),
            received_at,
        },
        received_at,
    ))
}

fn parse_mexc_position(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    let symbol = str_field(item, &["symbol"])?;
    let side_text = str_field(item, &["positionType", "holdSide", "side"]).unwrap_or_default();
    Some(PrivateEvent::position(
        ExchangePosition {
            exchange: ExchangeId::Mexc,
            canonical_symbol: canonical(&ExchangeId::Mexc, &symbol),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Mexc, symbol),
            position_side: mexc_position_side(&side_text),
            quantity: f64_field(item, &["holdVol", "vol", "position"])
                .unwrap_or_default()
                .abs(),
            entry_price: f64_field(item, &["holdAvgPrice", "avgPrice", "openAvgPrice"]),
            mark_price: f64_field(item, &["markPrice", "fairPrice"]),
            unrealized_pnl: f64_field(item, &["unrealised", "unrealizedPnl", "profit"]),
            updated_at: millis_field(item, &["updateTime", "createTime"], received_at),
        },
        received_at,
    ))
}

fn parse_mexc_balance(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    let asset =
        str_field(item, &["currency", "asset", "coin"]).unwrap_or_else(|| "USDT".to_string());
    Some(PrivateEvent::balance(
        ExchangeBalance {
            exchange: ExchangeId::Mexc,
            asset,
            total: f64_field(item, &["equity", "total", "balance"]).unwrap_or_default(),
            available: f64_field(item, &["availableBalance", "available", "cashBalance"])
                .unwrap_or_default(),
            locked: f64_field(item, &["frozenBalance", "frozen", "positionMargin"])
                .unwrap_or_default(),
            updated_at: received_at,
        },
        received_at,
    ))
}

fn parse_htx_order_or_fill(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    if item.get("trade_id").is_some() {
        return parse_htx_fill(item, received_at);
    }
    let symbol = str_field(item, &["contract_code"])?;
    let side = parse_order_side(str_field(item, &["direction"]).as_deref());
    Some(PrivateEvent::order(
        OrderState {
            exchange: ExchangeId::Htx,
            canonical_symbol: canonical(&ExchangeId::Htx, &symbol),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Htx, symbol),
            client_order_id: str_field(item, &["client_order_id"]).map(|id| id.to_string()),
            exchange_order_id: str_field(item, &["order_id_str", "order_id"]),
            side,
            position_side: htx_position_side(
                str_field(item, &["direction"]).as_deref(),
                str_field(item, &["offset"]).as_deref(),
            ),
            order_type: if str_field(item, &["order_price_type"])
                .as_deref()
                .unwrap_or_default()
                .contains("market")
            {
                OrderType::Market
            } else {
                OrderType::Limit
            },
            quantity: f64_field(item, &["volume"]).unwrap_or_default(),
            price: f64_field(item, &["price"]),
            filled_quantity: f64_field(item, &["trade_volume"]).unwrap_or_default(),
            average_fill_price: f64_field(item, &["trade_avg_price"]),
            time_in_force: TimeInForce::Gtc,
            reduce_only: str_field(item, &["offset"])
                .is_some_and(|offset| offset.eq_ignore_ascii_case("close")),
            status: htx_status(i64_field(item, &["status"])),
            updated_at: millis_field(
                item,
                &["update_time", "created_at", "create_date"],
                received_at,
            ),
        },
        received_at,
    ))
}

fn parse_htx_fill(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    let symbol = str_field(item, &["contract_code"])?;
    let side = parse_order_side(str_field(item, &["direction"]).as_deref());
    let price = f64_field(item, &["trade_price", "price"]).unwrap_or_default();
    let quantity = f64_field(item, &["trade_volume", "volume"]).unwrap_or_default();
    Some(PrivateEvent::fill(
        FillEvent {
            exchange: ExchangeId::Htx,
            canonical_symbol: canonical(&ExchangeId::Htx, &symbol),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Htx, symbol),
            trade_id: str_field(item, &["trade_id", "id"])
                .unwrap_or_else(|| format!("htx-fill-{}", received_at.timestamp_millis())),
            client_order_id: str_field(item, &["client_order_id"]),
            exchange_order_id: str_field(item, &["order_id"]),
            side,
            position_side: htx_position_side(
                str_field(item, &["direction"]).as_deref(),
                str_field(item, &["offset"]).as_deref(),
            ),
            liquidity: liquidity(str_field(item, &["role"]).as_deref()),
            price,
            quantity,
            quote_quantity: price * quantity,
            fee: f64_field(item, &["trade_fee", "fee"]),
            fee_asset: str_field(item, &["fee_asset"]).or_else(|| Some("USDT".to_string())),
            fee_rate: None,
            realized_pnl: f64_field(item, &["profit", "real_profit"]),
            reduce_only: str_field(item, &["offset"])
                .map(|offset| offset.eq_ignore_ascii_case("close")),
            filled_at: millis_field(item, &["created_at", "trade_time"], received_at),
            received_at,
        },
        received_at,
    ))
}

fn parse_htx_position(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    let symbol = str_field(item, &["contract_code"])?;
    Some(PrivateEvent::position(
        ExchangePosition {
            exchange: ExchangeId::Htx,
            canonical_symbol: canonical(&ExchangeId::Htx, &symbol),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Htx, symbol),
            position_side: parse_position_side(
                str_field(item, &["direction"])
                    .as_deref()
                    .unwrap_or_default(),
            ),
            quantity: f64_field(item, &["volume", "available"])
                .unwrap_or_default()
                .abs(),
            entry_price: f64_field(item, &["cost_open", "cost_hold", "entry_price"]),
            mark_price: f64_field(item, &["last_price", "mark_price"]),
            unrealized_pnl: f64_field(item, &["profit_unreal", "unrealized_pnl"]),
            updated_at: received_at,
        },
        received_at,
    ))
}

fn parse_htx_balance(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    let asset = str_field(item, &["margin_asset", "symbol", "currency"])
        .unwrap_or_else(|| "USDT".to_string());
    Some(PrivateEvent::balance(
        ExchangeBalance {
            exchange: ExchangeId::Htx,
            asset,
            total: f64_field(item, &["margin_balance", "total", "equity"]).unwrap_or_default(),
            available: f64_field(
                item,
                &["withdraw_available", "available_margin", "available"],
            )
            .unwrap_or_default(),
            locked: f64_field(item, &["margin_frozen", "frozen"]).unwrap_or_default(),
            updated_at: received_at,
        },
        received_at,
    ))
}

fn parse_toobit_order_or_fill(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    if item
        .get("tradeId")
        .or_else(|| item.get("ticketId"))
        .or_else(|| item.get("id"))
        .is_some()
        && item.get("orderId").is_some()
        && item
            .get("status")
            .or_else(|| item.get("type"))
            .or_else(|| item.get("origQty"))
            .is_none()
    {
        return parse_toobit_fill(item, received_at);
    }
    let symbol = str_field(item, &["symbol", "symbolName"])?;
    let side = parse_order_side(str_field(item, &["side"]).as_deref());
    Some(PrivateEvent::order(
        OrderState {
            exchange: ExchangeId::Toobit,
            canonical_symbol: canonical(&ExchangeId::Toobit, &symbol),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Toobit, symbol),
            client_order_id: str_field(item, &["clientOrderId", "newClientOrderId"]),
            exchange_order_id: str_field(item, &["orderId", "id"]),
            side,
            position_side: str_field(item, &["positionSide"])
                .as_deref()
                .map(parse_position_side)
                .unwrap_or(PositionSide::Net),
            order_type: parse_order_type(str_field(item, &["type", "orderType"]).as_deref()),
            quantity: f64_field(item, &["origQty", "quantity", "qty"]).unwrap_or_default(),
            price: f64_field(item, &["price"]),
            filled_quantity: f64_field(item, &["executedQty", "filledQty", "cumQty"])
                .unwrap_or_default(),
            average_fill_price: f64_field(item, &["avgPrice", "priceAvg"]),
            time_in_force: parse_time_in_force(str_field(item, &["timeInForce"]).as_deref()),
            reduce_only: bool_field(item, &["reduceOnly"]),
            status: binance_status(str_field(item, &["status", "state"]).as_deref()),
            updated_at: millis_field(
                item,
                &["updateTime", "updatedTime", "time", "transactTime"],
                received_at,
            ),
        },
        received_at,
    ))
}

fn parse_toobit_fill(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    let symbol = str_field(item, &["symbol", "symbolName"])?;
    let side = str_field(item, &["side"])
        .as_deref()
        .map(|side| parse_order_side(Some(side)))
        .unwrap_or_else(|| {
            if bool_field(item, &["isBuyer"]) {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            }
        });
    let price = f64_field(item, &["price"]).unwrap_or_default();
    let quantity = f64_field(item, &["qty", "quantity", "executedQty"]).unwrap_or_default();
    Some(PrivateEvent::fill(
        FillEvent {
            exchange: ExchangeId::Toobit,
            canonical_symbol: canonical(&ExchangeId::Toobit, &symbol),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Toobit, symbol),
            trade_id: str_field(item, &["tradeId", "ticketId", "id"])
                .unwrap_or_else(|| format!("toobit-fill-{}", received_at.timestamp_millis())),
            client_order_id: str_field(item, &["clientOrderId", "newClientOrderId"]),
            exchange_order_id: str_field(item, &["orderId"]),
            side,
            position_side: str_field(item, &["positionSide"])
                .as_deref()
                .map(parse_position_side)
                .unwrap_or(PositionSide::Net),
            liquidity: if bool_field(item, &["isMaker"]) {
                FillLiquidity::Maker
            } else {
                liquidity(str_field(item, &["role", "liquidity"]).as_deref())
            },
            price,
            quantity,
            quote_quantity: f64_field(item, &["quoteQty", "quoteQuantity"])
                .unwrap_or(price * quantity),
            fee: f64_field(item, &["commission", "fee", "feeAmount"]),
            fee_asset: str_field(
                item,
                &["commissionAsset", "feeAsset", "feeCoinId", "feeCoin"],
            ),
            fee_rate: f64_field(item, &["feeRate"]),
            realized_pnl: f64_field(item, &["realizedPnl", "realizedProfit"]),
            reduce_only: Some(bool_field(item, &["reduceOnly"])),
            filled_at: millis_field(item, &["time", "tradeTime", "createdTime"], received_at),
            received_at,
        },
        received_at,
    ))
}

fn parse_toobit_position(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    let symbol = str_field(item, &["symbol", "symbolName"])?;
    Some(PrivateEvent::position(
        ExchangePosition {
            exchange: ExchangeId::Toobit,
            canonical_symbol: canonical(&ExchangeId::Toobit, &symbol),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Toobit, symbol),
            position_side: str_field(item, &["positionSide", "side"])
                .as_deref()
                .map(parse_position_side)
                .unwrap_or(PositionSide::Net),
            quantity: f64_field(item, &["positionAmt", "positionQty", "quantity", "size"])
                .unwrap_or_default()
                .abs(),
            entry_price: f64_field(item, &["entryPrice", "avgPrice", "openPrice"]),
            mark_price: f64_field(item, &["markPrice", "lastPrice"]),
            unrealized_pnl: f64_field(item, &["unRealizedProfit", "unrealizedPnl", "pnl"]),
            updated_at: millis_field(item, &["updateTime", "updatedTime"], received_at),
        },
        received_at,
    ))
}

fn parse_toobit_balance(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    let asset = str_field(item, &["asset", "coin", "marginCoin", "currency"])
        .unwrap_or_else(|| "USDT".to_string());
    let total = f64_field(item, &["balance", "walletBalance", "equity", "total"])
        .or_else(|| {
            f64_field(item, &["availableBalance", "available", "free"]).map(|available| {
                available + f64_field(item, &["locked", "frozen"]).unwrap_or_default()
            })
        })
        .unwrap_or_default();
    let available = f64_field(item, &["availableBalance", "available", "free"]).unwrap_or(total);
    let locked =
        f64_field(item, &["locked", "frozen"]).unwrap_or_else(|| (total - available).max(0.0));
    Some(PrivateEvent::balance(
        ExchangeBalance {
            exchange: ExchangeId::Toobit,
            asset,
            total,
            available,
            locked,
            updated_at: received_at,
        },
        received_at,
    ))
}

fn parse_gate_order_or_fill(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    parse_gate_order_or_fill_with_contract_size(
        item,
        received_at,
        gate_contract_size_from_item(item),
    )
}

fn parse_gate_order_or_fill_with_contract_size(
    item: &Value,
    received_at: DateTime<Utc>,
    contract_size: f64,
) -> Option<PrivateEvent> {
    if item.get("trade_id").or_else(|| item.get("id")).is_some() && item.get("order_id").is_some() {
        return parse_gate_fill_with_contract_size(item, received_at, contract_size);
    }
    let symbol = str_field(item, &["contract"])?;
    let signed_size = gate_signed_contracts(item);
    let left = gate_contract_to_base_quantity(
        f64_field(item, &["left"]).unwrap_or_default().abs(),
        contract_size,
    );
    let quantity = gate_contract_to_base_quantity(signed_size.abs(), contract_size);
    let side = gate_side_from_contracts(item, signed_size);
    Some(PrivateEvent::order(
        OrderState {
            exchange: ExchangeId::Gate,
            canonical_symbol: canonical(&ExchangeId::Gate, &symbol),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, symbol),
            client_order_id: str_field(item, &["text"])
                .map(|text| gate_client_text_to_local(&text)),
            exchange_order_id: str_field(item, &["id"]),
            side,
            position_side: gate_position_side_from_contracts(item, signed_size, side),
            order_type: if item.get("price").is_some() {
                OrderType::Limit
            } else {
                OrderType::Market
            },
            quantity,
            price: f64_field(item, &["price"]),
            filled_quantity: (quantity - left).max(0.0),
            average_fill_price: f64_field(item, &["fill_price", "avg_deal_price"]),
            time_in_force: TimeInForce::Gtc,
            reduce_only: bool_field(item, &["is_reduce_only", "reduce_only"]),
            status: gate_status(str_field(item, &["finish_as", "status"]).as_deref(), left),
            updated_at: millis_field(item, &["update_time_ms", "create_time_ms"], received_at),
        },
        received_at,
    ))
}

fn parse_gate_fill(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    parse_gate_fill_with_contract_size(item, received_at, gate_contract_size_from_item(item))
}

fn parse_gate_fill_with_contract_size(
    item: &Value,
    received_at: DateTime<Utc>,
    contract_size: f64,
) -> Option<PrivateEvent> {
    let symbol = str_field(item, &["contract"])?;
    let signed_size = gate_signed_contracts(item);
    let price = f64_field(item, &["price"]).unwrap_or_default();
    let quantity = gate_contract_to_base_quantity(signed_size.abs(), contract_size);
    let side = gate_side_from_contracts(item, signed_size);
    Some(PrivateEvent::fill(
        FillEvent {
            exchange: ExchangeId::Gate,
            canonical_symbol: canonical(&ExchangeId::Gate, &symbol),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, symbol),
            trade_id: str_field(item, &["trade_id", "id"])
                .unwrap_or_else(|| format!("gate-fill-{}", received_at.timestamp_millis())),
            client_order_id: str_field(item, &["text"])
                .map(|text| gate_client_text_to_local(&text)),
            exchange_order_id: str_field(item, &["order_id", "order"]),
            side,
            position_side: gate_position_side_from_contracts(item, signed_size, side),
            liquidity: liquidity(str_field(item, &["role"]).as_deref()),
            price,
            quantity,
            quote_quantity: price * quantity,
            fee: f64_field(item, &["fee"]),
            fee_asset: str_field(item, &["fee_currency"]),
            fee_rate: None,
            realized_pnl: f64_field(item, &["realised_pnl", "realized_pnl"]),
            reduce_only: None,
            filled_at: millis_field(item, &["create_time_ms"], received_at),
            received_at,
        },
        received_at,
    ))
}

fn parse_gate_position(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    parse_gate_position_with_contract_size(item, received_at, gate_contract_size_from_item(item))
}

fn parse_gate_position_with_contract_size(
    item: &Value,
    received_at: DateTime<Utc>,
    contract_size: f64,
) -> Option<PrivateEvent> {
    let symbol = str_field(item, &["contract"])?;
    let signed_size = gate_signed_contracts(item);
    let side = gate_side_from_contracts(item, signed_size);
    Some(PrivateEvent::position(
        ExchangePosition {
            exchange: ExchangeId::Gate,
            canonical_symbol: canonical(&ExchangeId::Gate, &symbol),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, symbol),
            position_side: gate_position_side_from_contracts(item, signed_size, side),
            quantity: gate_contract_to_base_quantity(signed_size.abs(), contract_size),
            entry_price: f64_field(item, &["entry_price"]),
            mark_price: f64_field(item, &["mark_price"]),
            unrealized_pnl: f64_field(item, &["unrealised_pnl", "unrealized_pnl"]),
            updated_at: millis_field(item, &["update_time_ms"], received_at),
        },
        received_at,
    ))
}

fn parse_gate_balance(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    Some(PrivateEvent::balance(
        ExchangeBalance {
            exchange: ExchangeId::Gate,
            asset: str_field(item, &["currency"]).unwrap_or_else(|| "USDT".to_string()),
            total: f64_field(item, &["total", "total_balance"]).unwrap_or_default(),
            available: f64_field(item, &["available", "available_balance"]).unwrap_or_default(),
            locked: f64_field(item, &["unrealised_pnl", "order_margin"]).unwrap_or_default(),
            updated_at: received_at,
        },
        received_at,
    ))
}

fn set_str(body: &mut Value, key: &str, value: impl ToString) {
    if let Some(map) = body.as_object_mut() {
        map.insert(key.to_string(), Value::String(value.to_string()));
    }
}

fn set_bool(body: &mut Value, key: &str, value: bool) {
    if let Some(map) = body.as_object_mut() {
        map.insert(key.to_string(), Value::Bool(value));
    }
}

fn set_i64(body: &mut Value, key: &str, value: i64) {
    if let Some(map) = body.as_object_mut() {
        map.insert(key.to_string(), Value::Number(value.into()));
    }
}

fn set_optional_str(body: &mut Value, key: &str, value: Option<&'static str>) {
    if let Some(value) = value {
        set_str(body, key, value);
    }
}

fn number_string(value: f64) -> String {
    format!("{value:.12}")
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}

fn ensure_toobit_symbol(symbol: &ExchangeSymbol, action: &str) -> Result<()> {
    if symbol.exchange != ExchangeId::Toobit {
        anyhow::bail!("toobit {action} requires toobit exchange symbols");
    }
    if symbol.symbol.trim().is_empty() {
        anyhow::bail!("toobit {action} requires non-empty symbol");
    }
    Ok(())
}

fn ensure_toobit_command_symbol(
    exchange: &ExchangeId,
    symbol: &ExchangeSymbol,
    action: &str,
) -> Result<()> {
    if exchange != &ExchangeId::Toobit || symbol.exchange != ExchangeId::Toobit {
        anyhow::bail!("toobit {action} requires toobit exchange symbols");
    }
    ensure_toobit_symbol(symbol, action)
}

fn toobit_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    }
}

fn toobit_position_side(
    position_mode: PositionMode,
    position_side: PositionSide,
    fallback_side: OrderSide,
) -> &'static str {
    match (position_mode, position_side) {
        (PositionMode::Hedge, PositionSide::Long) => "LONG",
        (PositionMode::Hedge, PositionSide::Short) => "SHORT",
        _ => match fallback_side {
            OrderSide::Buy => "LONG",
            OrderSide::Sell => "SHORT",
        },
    }
}

fn toobit_order_type(
    order_type: OrderType,
    post_only: bool,
    time_in_force: TimeInForce,
) -> &'static str {
    match order_type {
        OrderType::Market => "MARKET",
        OrderType::Limit if post_only || matches!(time_in_force, TimeInForce::PostOnly) => {
            "LIMIT_MAKER"
        }
        OrderType::Limit => "LIMIT",
    }
}

fn toobit_time_in_force(tif: TimeInForce, post_only: bool) -> Option<&'static str> {
    if post_only || matches!(tif, TimeInForce::PostOnly) {
        return None;
    }
    Some(match tif {
        TimeInForce::Ioc => "IOC",
        TimeInForce::Fok => "FOK",
        TimeInForce::Gtc | TimeInForce::PostOnly => "GTC",
    })
}

fn ensure_coinex_symbol(symbol: &ExchangeSymbol, action: &str) -> Result<()> {
    if symbol.exchange != ExchangeId::CoinEx {
        anyhow::bail!("coinex {action} requires coinex exchange symbols");
    }
    if symbol.symbol.trim().is_empty() {
        anyhow::bail!("coinex {action} requires non-empty market");
    }
    Ok(())
}

fn ensure_coinex_command_symbol(
    exchange: &ExchangeId,
    symbol: &ExchangeSymbol,
    action: &str,
) -> Result<()> {
    if *exchange != ExchangeId::CoinEx {
        anyhow::bail!("coinex {action} requires coinex exchange symbols");
    }
    ensure_coinex_symbol(symbol, action)
}

fn coinex_market_body(symbol: &ExchangeSymbol) -> Value {
    json!({
        "market": symbol.symbol,
        "market_type": "FUTURES",
    })
}

fn coinex_order_body(command: &OrderCommand) -> Value {
    let mut body = coinex_market_body(&command.exchange_symbol);
    set_str(&mut body, "side", coinex_side(command.side));
    set_str(
        &mut body,
        "type",
        coinex_order_type(command.order_type, command.time_in_force, command.post_only),
    );
    set_str(&mut body, "amount", number_string(command.quantity));
    set_str(&mut body, "client_id", &command.client_order_id);
    if let Some(price) = command.price {
        set_str(&mut body, "price", number_string(price));
    }
    body
}

fn coinex_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn coinex_order_type(order_type: OrderType, tif: TimeInForce, post_only: bool) -> &'static str {
    if post_only {
        return "maker_only";
    }
    match (order_type, tif) {
        (OrderType::Market, _) => "market",
        (_, TimeInForce::Ioc) => "ioc",
        (_, TimeInForce::Fok) => "fok",
        _ => "limit",
    }
}

fn coinex_ws_signature(secret: &str, timestamp_ms: i64) -> String {
    hmac_sha256_hex(secret, &timestamp_ms.to_string())
}

fn binance_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    }
}

fn binance_order_type(order_type: OrderType) -> &'static str {
    match order_type {
        OrderType::Limit => "LIMIT",
        OrderType::Market => "MARKET",
    }
}

fn binance_batch_order_item(command: &OrderCommand, position_mode: PositionMode) -> Value {
    let mut item = json!({
        "symbol": command.exchange_symbol.symbol,
        "side": binance_side(command.side),
        "type": binance_order_type(command.order_type),
        "quantity": number_string(command.quantity),
        "newClientOrderId": command.client_order_id,
    });
    if let Some(position_side) =
        binance_position_side(command.position_side, command.side, position_mode)
    {
        set_str(&mut item, "positionSide", position_side);
    }
    if command.order_type == OrderType::Limit {
        set_str(
            &mut item,
            "timeInForce",
            binance_time_in_force(command.time_in_force, command.post_only),
        );
    }
    if let Some(price) = command.price {
        set_str(&mut item, "price", number_string(price));
    }
    if command.reduce_only && !position_mode.is_hedge() {
        set_str(&mut item, "reduceOnly", "true");
    }
    item
}

fn binance_position_side(
    position_side: PositionSide,
    fallback_side: OrderSide,
    position_mode: PositionMode,
) -> Option<&'static str> {
    if !position_mode.is_hedge() {
        return None;
    }
    Some(match position_side {
        PositionSide::Long => "LONG",
        PositionSide::Short => "SHORT",
        PositionSide::Net => match fallback_side {
            OrderSide::Buy => "LONG",
            OrderSide::Sell => "SHORT",
        },
    })
}

fn binance_time_in_force(tif: TimeInForce, post_only: bool) -> &'static str {
    match (tif, post_only) {
        (_, true) | (TimeInForce::PostOnly, _) => "GTX",
        (TimeInForce::Ioc, _) => "IOC",
        (TimeInForce::Fok, _) => "FOK",
        (TimeInForce::Gtc, _) => "GTC",
    }
}

fn binance_position_side_from_text(
    value: Option<&str>,
    signed_quantity: Option<f64>,
    fallback_side: OrderSide,
) -> PositionSide {
    match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "LONG" => PositionSide::Long,
        "SHORT" => PositionSide::Short,
        "BOTH" => match signed_quantity {
            Some(quantity) if quantity > 0.0 => PositionSide::Long,
            Some(quantity) if quantity < 0.0 => PositionSide::Short,
            _ => PositionSide::Net,
        },
        _ => match signed_quantity {
            Some(quantity) if quantity > 0.0 => PositionSide::Long,
            Some(quantity) if quantity < 0.0 => PositionSide::Short,
            _ => match fallback_side {
                OrderSide::Buy => PositionSide::Long,
                OrderSide::Sell => PositionSide::Short,
            },
        },
    }
}

fn ensure_binance_symbol(symbol: &ExchangeSymbol, action: &str) -> Result<()> {
    if symbol.exchange != ExchangeId::Binance {
        anyhow::bail!("binance {action} requires binance exchange symbols");
    }
    Ok(())
}

fn ensure_binance_command_symbol(
    exchange: &ExchangeId,
    symbol: &ExchangeSymbol,
    action: &str,
) -> Result<()> {
    if exchange != &ExchangeId::Binance || symbol.exchange != ExchangeId::Binance {
        anyhow::bail!("binance {action} requires binance exchange symbols");
    }
    Ok(())
}

fn okx_order_body(command: &OrderCommand, position_mode: PositionMode) -> Value {
    let mut body = json!({
        "instId": command.exchange_symbol.symbol,
        "tdMode": "cross",
        "side": okx_side(command.side),
        "ordType": okx_order_type(command.order_type, command.time_in_force, command.post_only),
        "sz": number_string(command.quantity),
        "clOrdId": command.client_order_id,
    });
    if let Some(price) = command.price {
        set_str(&mut body, "px", number_string(price));
    }
    if let Some(position_side) =
        okx_position_side(command.position_side, command.side, position_mode)
    {
        set_str(&mut body, "posSide", position_side);
    }
    if command.reduce_only {
        set_bool(&mut body, "reduceOnly", true);
    }
    body
}

fn ensure_okx_symbol(symbol: &ExchangeSymbol, action: &str) -> Result<()> {
    if symbol.exchange != ExchangeId::Okx {
        anyhow::bail!("okx {action} requires okx exchange symbols");
    }
    Ok(())
}

fn ensure_okx_command_symbol(
    exchange: &ExchangeId,
    symbol: &ExchangeSymbol,
    action: &str,
) -> Result<()> {
    if exchange != &ExchangeId::Okx || symbol.exchange != ExchangeId::Okx {
        anyhow::bail!("okx {action} requires okx exchange symbols");
    }
    Ok(())
}

fn okx_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn okx_order_type(order_type: OrderType, tif: TimeInForce, post_only: bool) -> &'static str {
    match (order_type, tif, post_only) {
        (OrderType::Market, _, _) => "market",
        (_, _, true) | (_, TimeInForce::PostOnly, _) => "post_only",
        (_, TimeInForce::Ioc, _) => "ioc",
        (_, TimeInForce::Fok, _) => "fok",
        _ => "limit",
    }
}

fn okx_position_side(
    position_side: PositionSide,
    fallback_side: OrderSide,
    position_mode: PositionMode,
) -> Option<&'static str> {
    if !position_mode.is_hedge() {
        return None;
    }
    Some(match position_side {
        PositionSide::Long => "long",
        PositionSide::Short => "short",
        PositionSide::Net => match fallback_side {
            OrderSide::Buy => "long",
            OrderSide::Sell => "short",
        },
    })
}

fn okx_status(status: Option<&str>) -> OrderCommandStatus {
    match status.unwrap_or_default().to_ascii_lowercase().as_str() {
        "live" | "effective" => OrderCommandStatus::Accepted,
        "partially_filled" | "partially-filled" => OrderCommandStatus::PartiallyFilled,
        "filled" => OrderCommandStatus::Filled,
        "canceled" | "cancelled" => OrderCommandStatus::Cancelled,
        "order_failed" | "failed" | "rejected" => OrderCommandStatus::Rejected,
        _ => OrderCommandStatus::Submitted,
    }
}

fn bitget_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn bitget_position_side(position_side: PositionSide, fallback_side: OrderSide) -> &'static str {
    match position_side {
        PositionSide::Long => "buy",
        PositionSide::Short => "sell",
        PositionSide::Net => bitget_side(fallback_side),
    }
}

fn bitget_order_type(order_type: OrderType) -> &'static str {
    match order_type {
        OrderType::Limit => "limit",
        OrderType::Market => "market",
    }
}

fn bitget_batch_order_item(command: &OrderCommand, position_mode: PositionMode) -> Value {
    let side = if position_mode.is_hedge() {
        bitget_position_side(command.position_side, command.side)
    } else {
        bitget_side(command.side)
    };
    let mut item = json!({
        "size": number_string(command.quantity),
        "side": side,
        "orderType": bitget_order_type(command.order_type),
        "clientOid": command.client_order_id,
    });
    set_optional_str(
        &mut item,
        "force",
        bitget_force(command.time_in_force, command.post_only),
    );
    if let Some(price) = command.price {
        set_str(&mut item, "price", number_string(price));
    }
    if position_mode.is_hedge() {
        set_str(
            &mut item,
            "tradeSide",
            bitget_trade_side(command.reduce_only),
        );
    } else if command.reduce_only {
        set_str(&mut item, "reduceOnly", "YES");
    }
    item
}

fn bitget_force(tif: TimeInForce, post_only: bool) -> Option<&'static str> {
    match (tif, post_only) {
        (_, true) | (TimeInForce::PostOnly, _) => Some("post_only"),
        (TimeInForce::Ioc, _) => Some("ioc"),
        (TimeInForce::Fok, _) => Some("fok"),
        (TimeInForce::Gtc, _) => Some("gtc"),
    }
}

fn bitget_trade_side(reduce_only: bool) -> &'static str {
    if reduce_only {
        "close"
    } else {
        "open"
    }
}

fn ensure_bitget_symbol(symbol: &ExchangeSymbol, action: &str) -> Result<()> {
    if symbol.exchange != ExchangeId::Bitget {
        anyhow::bail!("bitget {action} requires bitget exchange symbols");
    }
    Ok(())
}

fn ensure_bitget_command_symbol(
    exchange: &ExchangeId,
    symbol: &ExchangeSymbol,
    action: &str,
) -> Result<()> {
    if exchange != &ExchangeId::Bitget || symbol.exchange != ExchangeId::Bitget {
        anyhow::bail!("bitget {action} requires bitget exchange symbols");
    }
    Ok(())
}

fn bybit_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "Buy",
        OrderSide::Sell => "Sell",
    }
}

fn bybit_order_type(order_type: OrderType) -> &'static str {
    match order_type {
        OrderType::Limit => "Limit",
        OrderType::Market => "Market",
    }
}

fn bybit_batch_order_item(command: &OrderCommand, position_mode: PositionMode) -> Value {
    let mut item = json!({
        "symbol": command.exchange_symbol.symbol,
        "side": bybit_side(command.side),
        "orderType": bybit_order_type(command.order_type),
        "qty": number_string(command.quantity),
        "orderLinkId": command.client_order_id,
        "timeInForce": bybit_time_in_force(command.time_in_force, command.post_only),
    });
    if let Some(price) = command.price {
        set_str(&mut item, "price", number_string(price));
    }
    if command.reduce_only {
        set_bool(&mut item, "reduceOnly", true);
    }
    if position_mode.is_hedge() {
        set_i64(
            &mut item,
            "positionIdx",
            bybit_position_idx(command.position_side, command.side),
        );
    }
    item
}

fn bybit_time_in_force(tif: TimeInForce, post_only: bool) -> &'static str {
    match (tif, post_only) {
        (_, true) | (TimeInForce::PostOnly, _) => "PostOnly",
        (TimeInForce::Ioc, _) => "IOC",
        (TimeInForce::Fok, _) => "FOK",
        (TimeInForce::Gtc, _) => "GTC",
    }
}

fn bybit_position_idx(position_side: PositionSide, fallback_side: OrderSide) -> i64 {
    match position_side {
        PositionSide::Long => 1,
        PositionSide::Short => 2,
        PositionSide::Net => match fallback_side {
            OrderSide::Buy => 1,
            OrderSide::Sell => 2,
        },
    }
}

fn ensure_bybit_symbol(symbol: &ExchangeSymbol, action: &str) -> Result<()> {
    if symbol.exchange != ExchangeId::Bybit {
        anyhow::bail!("bybit {action} requires bybit exchange symbols");
    }
    Ok(())
}

fn ensure_bybit_command_symbol(
    exchange: &ExchangeId,
    symbol: &ExchangeSymbol,
    action: &str,
) -> Result<()> {
    if exchange != &ExchangeId::Bybit || symbol.exchange != ExchangeId::Bybit {
        anyhow::bail!("bybit {action} requires bybit exchange symbols");
    }
    Ok(())
}

fn bybit_position_side_from_idx(idx: Option<i64>, fallback_side: OrderSide) -> PositionSide {
    match idx {
        Some(1) => PositionSide::Long,
        Some(2) => PositionSide::Short,
        _ => match fallback_side {
            OrderSide::Buy => PositionSide::Long,
            OrderSide::Sell => PositionSide::Short,
        },
    }
}

fn mexc_order_side(side: OrderSide, reduce_only: bool) -> i64 {
    match (side, reduce_only) {
        (OrderSide::Buy, false) => 1,
        (OrderSide::Sell, false) => 3,
        (OrderSide::Buy, true) => 2,
        (OrderSide::Sell, true) => 4,
    }
}

fn mexc_order_type(order_type: OrderType, tif: TimeInForce, post_only: bool) -> i64 {
    match (order_type, tif, post_only) {
        (OrderType::Market, _, _) => 5,
        (_, _, true) | (_, TimeInForce::PostOnly, _) => 2,
        (_, TimeInForce::Ioc, _) => 3,
        (_, TimeInForce::Fok, _) => 4,
        _ => 1,
    }
}

fn ensure_mexc_symbol(symbol: &ExchangeSymbol, action: &str) -> Result<()> {
    if symbol.exchange != ExchangeId::Mexc {
        anyhow::bail!("mexc {action} requires mexc exchange symbols");
    }
    Ok(())
}

fn ensure_mexc_command_symbol(
    exchange: &ExchangeId,
    symbol: &ExchangeSymbol,
    action: &str,
) -> Result<()> {
    if exchange != &ExchangeId::Mexc || symbol.exchange != ExchangeId::Mexc {
        anyhow::bail!("mexc {action} requires mexc exchange symbols");
    }
    Ok(())
}

fn mexc_side_to_order_side(side: i64) -> OrderSide {
    match side {
        3 | 4 => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn mexc_side_is_close(side: i64) -> bool {
    matches!(side, 2 | 4)
}

fn mexc_side_to_position_side(side: i64, fallback: OrderSide) -> PositionSide {
    match side {
        1 | 4 => PositionSide::Long,
        2 | 3 => PositionSide::Short,
        _ => match fallback {
            OrderSide::Buy => PositionSide::Long,
            OrderSide::Sell => PositionSide::Short,
        },
    }
}

fn mexc_parse_order_type(order_type: Option<i64>) -> OrderType {
    match order_type {
        Some(1 | 2 | 3 | 4) => OrderType::Limit,
        _ => OrderType::Market,
    }
}

fn mexc_position_side(value: &str) -> PositionSide {
    match value.to_ascii_lowercase().as_str() {
        "1" | "long" => PositionSide::Long,
        "2" | "short" => PositionSide::Short,
        _ => PositionSide::Net,
    }
}

fn htx_direction(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn htx_order_price_type(order_type: OrderType, tif: TimeInForce, post_only: bool) -> &'static str {
    match (order_type, tif, post_only) {
        (OrderType::Market, _, _) => "optimal_5",
        (_, _, true) | (_, TimeInForce::PostOnly, _) => "post_only",
        (_, TimeInForce::Ioc, _) => "ioc",
        (_, TimeInForce::Fok, _) => "fok",
        _ => "limit",
    }
}

fn htx_lightning_order_price_type(tif: TimeInForce) -> &'static str {
    match tif {
        TimeInForce::Fok => "lightning_fok",
        TimeInForce::Ioc => "lightning_ioc",
        _ => "market",
    }
}

fn ensure_htx_symbol(symbol: &ExchangeSymbol, action: &str) -> Result<()> {
    if symbol.exchange != ExchangeId::Htx {
        anyhow::bail!("htx {action} requires htx exchange symbols");
    }
    Ok(())
}

fn ensure_htx_command_symbol(
    exchange: &ExchangeId,
    symbol: &ExchangeSymbol,
    action: &str,
) -> Result<()> {
    if exchange != &ExchangeId::Htx || symbol.exchange != ExchangeId::Htx {
        anyhow::bail!("htx {action} requires htx exchange symbols");
    }
    Ok(())
}

fn htx_batch_order_item(command: &OrderCommand) -> Value {
    let mut body = json!({
        "contract_code": command.exchange_symbol.symbol,
        "client_order_id": htx_client_order_id(&command.client_order_id),
        "volume": number_string(command.quantity),
        "direction": htx_direction(command.side),
        "offset": if command.reduce_only { "close" } else { "open" },
        "lever_rate": 1,
        "order_price_type": htx_order_price_type(command.order_type, command.time_in_force, command.post_only),
    });
    if let Some(price) = command.price {
        set_str(&mut body, "price", number_string(price));
    }
    body
}

fn htx_client_order_id(client_order_id: &str) -> i64 {
    let digest = Sha256::digest(client_order_id.as_bytes());
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    (u64::from_be_bytes(bytes) % 9_000_000_000_000_000_000) as i64
}

fn htx_position_side(direction: Option<&str>, offset: Option<&str>) -> PositionSide {
    match (direction.unwrap_or_default(), offset.unwrap_or_default()) {
        ("buy", "open") | ("sell", "close") => PositionSide::Long,
        ("sell", "open") | ("buy", "close") => PositionSide::Short,
        _ => PositionSide::Net,
    }
}

fn instrument_contract_size(instrument: &InstrumentMeta) -> f64 {
    valid_contract_size(instrument.contract_size)
}

fn valid_contract_size(contract_size: f64) -> f64 {
    if contract_size.is_finite() && contract_size > 0.0 {
        contract_size
    } else {
        1.0
    }
}

fn gate_base_to_contract_quantity(quantity: f64, instrument: &InstrumentMeta) -> f64 {
    normalize_number(quantity / instrument_contract_size(instrument))
}

fn gate_contract_to_base_quantity(quantity: f64, contract_size: f64) -> f64 {
    normalize_number(quantity * valid_contract_size(contract_size))
}

fn gate_contract_size_from_item(item: &Value) -> f64 {
    valid_contract_size(
        f64_field(item, &["contract_size", "quanto_multiplier", "multiplier"]).unwrap_or(1.0),
    )
}

fn gate_signed_contracts(item: &Value) -> f64 {
    let quantity = f64_field(item, &["size", "contracts"]).unwrap_or_default();
    if quantity < 0.0 {
        return quantity;
    }
    match str_field(item, &["side"]).as_deref() {
        Some(side) if parse_order_side(Some(side)) == OrderSide::Sell => -quantity.abs(),
        _ => quantity,
    }
}

fn gate_side_from_contracts(item: &Value, signed_contracts: f64) -> OrderSide {
    str_field(item, &["side"])
        .as_deref()
        .map(|side| parse_order_side(Some(side)))
        .unwrap_or_else(|| {
            if signed_contracts < 0.0 {
                OrderSide::Sell
            } else {
                OrderSide::Buy
            }
        })
}

fn gate_position_side_from_contracts(
    item: &Value,
    signed_contracts: f64,
    side: OrderSide,
) -> PositionSide {
    str_field(item, &["position_side", "pos_side", "mode"])
        .as_deref()
        .map(parse_position_side)
        .filter(|position_side| *position_side != PositionSide::Net)
        .unwrap_or_else(|| {
            if signed_contracts < 0.0 || side == OrderSide::Sell {
                PositionSide::Short
            } else {
                PositionSide::Long
            }
        })
}

fn normalize_number(value: f64) -> f64 {
    if !value.is_finite() {
        return value;
    }
    number_string(value).parse().unwrap_or(value)
}

fn gate_signed_size(side: OrderSide, quantity: f64) -> Result<i64> {
    let contracts = quantity.abs();
    if !contracts.is_finite() || contracts <= 0.0 {
        anyhow::bail!("gate order size must be a positive finite contract count");
    }
    let rounded = contracts.round();
    if (contracts - rounded).abs() > 1e-9 {
        anyhow::bail!("gate order size must be an integer contract count, got {contracts}");
    }
    if rounded > i64::MAX as f64 {
        anyhow::bail!("gate order size exceeds i64 range, got {contracts}");
    }
    let signed = match side {
        OrderSide::Buy => rounded as i64,
        OrderSide::Sell => -(rounded as i64),
    };
    Ok(signed)
}

fn gate_tif(tif: TimeInForce, post_only: bool) -> &'static str {
    match (tif, post_only) {
        (_, true) | (TimeInForce::PostOnly, _) => "poc",
        (TimeInForce::Ioc, _) => "ioc",
        (TimeInForce::Fok, _) => "fok",
        (TimeInForce::Gtc, _) => "gtc",
    }
}

fn gate_client_text(client_order_id: &str) -> String {
    const MAX_BODY_LEN: usize = 28;
    let raw = client_order_id
        .strip_prefix("t-")
        .unwrap_or(client_order_id);
    let mut sanitized = raw
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' || ch == '.' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if sanitized.is_empty() {
        sanitized = "order".to_string();
    }
    if sanitized.len() > MAX_BODY_LEN {
        let digest = hex::encode(Sha256::digest(raw.as_bytes()));
        let suffix = &digest[..8];
        let prefix_len = MAX_BODY_LEN - 1 - suffix.len();
        sanitized = format!("{}-{suffix}", &sanitized[..prefix_len]);
    }
    format!("t-{sanitized}")
}

fn gate_client_text_to_local(text: &str) -> String {
    text.strip_prefix("t-").unwrap_or(text).to_string()
}

fn ensure_gate_symbol(symbol: &ExchangeSymbol, action: &str) -> Result<()> {
    if symbol.exchange != ExchangeId::Gate {
        anyhow::bail!("gate {action} requires gate exchange symbols");
    }
    Ok(())
}

fn ensure_gate_command_symbol(
    exchange: &ExchangeId,
    symbol: &ExchangeSymbol,
    action: &str,
) -> Result<()> {
    if exchange != &ExchangeId::Gate || symbol.exchange != ExchangeId::Gate {
        anyhow::bail!("gate {action} requires gate exchange symbols");
    }
    Ok(())
}

fn gate_order_lookup_id<'a>(
    exchange_order_id: Option<&'a str>,
    client_order_id: Option<&'a str>,
) -> Result<String> {
    exchange_order_id
        .map(ToString::to_string)
        .or_else(|| client_order_id.map(gate_client_text))
        .ok_or_else(|| anyhow!("gate order lookup requires exchange_order_id or client_order_id"))
}

fn gate_order_path(order_id: String) -> String {
    format!("/futures/usdt/orders/{}", urlencoding::encode(&order_id))
}

fn gate_private_ws_payload(
    channel: &str,
    account_id: &str,
    symbols: &[ExchangeSymbol],
) -> Vec<String> {
    if channel == "futures.balances" {
        return vec![account_id.to_string()];
    }
    if symbols.is_empty() {
        return vec![account_id.to_string(), "!all".to_string()];
    }
    let mut payload = Vec::with_capacity(symbols.len() + 1);
    payload.push(account_id.to_string());
    payload.extend(symbols.iter().map(|symbol| symbol.symbol.clone()));
    payload
}

fn gate_private_ws_symbol_groups(
    _channel: &str,
    _symbols: &[ExchangeSymbol],
) -> Vec<Vec<ExchangeSymbol>> {
    vec![Vec::new()]
}

fn str_field(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        value.get(*key).and_then(|v| match v {
            Value::String(text) => Some(text.clone()),
            Value::Number(number) => Some(number.to_string()),
            Value::Bool(flag) => Some(flag.to_string()),
            _ => None,
        })
    })
}

fn f64_field(value: &Value, keys: &[&str]) -> Option<f64> {
    keys.iter().find_map(|key| {
        value.get(*key).and_then(|v| match v {
            Value::Number(number) => number.as_f64(),
            Value::String(text) => text.parse::<f64>().ok(),
            _ => None,
        })
    })
}

fn i64_field(value: &Value, keys: &[&str]) -> Option<i64> {
    keys.iter().find_map(|key| {
        value.get(*key).and_then(|v| match v {
            Value::Number(number) => number.as_i64(),
            Value::String(text) => text.parse::<i64>().ok(),
            _ => None,
        })
    })
}

fn bool_field(value: &Value, keys: &[&str]) -> bool {
    keys.iter().any(|key| {
        value.get(*key).is_some_and(|v| match v {
            Value::Bool(flag) => *flag,
            Value::String(text) => {
                matches!(text.to_ascii_lowercase().as_str(), "true" | "yes" | "1")
            }
            Value::Number(number) => number.as_i64() == Some(1),
            _ => false,
        })
    })
}

fn millis_field(value: &Value, keys: &[&str], fallback: DateTime<Utc>) -> DateTime<Utc> {
    keys.iter()
        .find_map(|key| {
            value.get(*key).and_then(|v| match v {
                Value::Number(number) => number.as_i64(),
                Value::String(text) => text.parse::<i64>().ok(),
                _ => None,
            })
        })
        .and_then(DateTime::<Utc>::from_timestamp_millis)
        .unwrap_or(fallback)
}

fn canonical(exchange: &ExchangeId, symbol: &str) -> CanonicalSymbol {
    canonical_from_exchange_symbol(exchange, symbol).unwrap_or_else(|| {
        CanonicalSymbol::parse(symbol).unwrap_or_else(|| CanonicalSymbol::new(symbol, "USDT"))
    })
}

fn parse_order_side(side: Option<&str>) -> OrderSide {
    match side.unwrap_or_default().to_ascii_lowercase().as_str() {
        "sell" | "ask" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn parse_position_side(side: &str) -> PositionSide {
    match side.to_ascii_lowercase().as_str() {
        "short" | "sell" => PositionSide::Short,
        "long" | "buy" => PositionSide::Long,
        _ => PositionSide::Net,
    }
}

fn parse_position_mode_text(value: &str) -> PositionMode {
    if matches!(
        value.to_ascii_lowercase().as_str(),
        "hedge" | "hedge_mode" | "long_short" | "long_short_mode" | "dual" | "dual_side"
    ) {
        PositionMode::Hedge
    } else {
        PositionMode::OneWay
    }
}

fn parse_margin_mode_text(value: &str) -> MarginMode {
    match value.to_ascii_lowercase().as_str() {
        "cross" | "crossed" => MarginMode::Cross,
        "isolated" | "fixed" => MarginMode::Isolated,
        _ => MarginMode::Unknown,
    }
}

fn position_side_from_trade_side(trade_side: &str, side: OrderSide) -> PositionSide {
    match trade_side.to_ascii_lowercase().as_str() {
        "close" => match side {
            OrderSide::Buy => PositionSide::Short,
            OrderSide::Sell => PositionSide::Long,
        },
        _ => match side {
            OrderSide::Buy => PositionSide::Long,
            OrderSide::Sell => PositionSide::Short,
        },
    }
}

fn parse_order_type(order_type: Option<&str>) -> OrderType {
    match order_type.unwrap_or_default().to_ascii_lowercase().as_str() {
        "limit" | "post_only" | "postonly" | "ioc" | "fok" => OrderType::Limit,
        _ => OrderType::Market,
    }
}

fn parse_time_in_force(tif: Option<&str>) -> TimeInForce {
    match tif.unwrap_or_default().to_ascii_lowercase().as_str() {
        "ioc" | "immediateorcancel" => TimeInForce::Ioc,
        "fok" | "fillorkill" => TimeInForce::Fok,
        "gtx" | "postonly" | "post_only" => TimeInForce::PostOnly,
        _ => TimeInForce::Gtc,
    }
}

fn liquidity(role: Option<&str>) -> FillLiquidity {
    match role.unwrap_or_default().to_ascii_lowercase().as_str() {
        "maker" | "m" => FillLiquidity::Maker,
        "true" => FillLiquidity::Maker,
        "taker" | "t" => FillLiquidity::Taker,
        "false" => FillLiquidity::Taker,
        _ => FillLiquidity::Unknown,
    }
}

fn bitget_status(status: Option<&str>) -> OrderCommandStatus {
    match status.unwrap_or_default().to_ascii_lowercase().as_str() {
        "live" | "new" | "init" => OrderCommandStatus::Accepted,
        "partially_filled" | "partial-fill" | "partial_filled" => {
            OrderCommandStatus::PartiallyFilled
        }
        "filled" | "full-fill" | "full_filled" => OrderCommandStatus::Filled,
        "canceled" | "cancelled" => OrderCommandStatus::Cancelled,
        "rejected" => OrderCommandStatus::Rejected,
        _ => OrderCommandStatus::Submitted,
    }
}

fn bitget_order_status(item: &Value) -> OrderCommandStatus {
    let status = bitget_status(str_field(item, &["status", "state"]).as_deref());
    let filled = f64_field(item, &["accBaseVolume", "filledQty", "fillSize"]).unwrap_or_default();
    let quantity = f64_field(item, &["size", "qty"]).unwrap_or_default();
    if matches!(
        status,
        OrderCommandStatus::Submitted | OrderCommandStatus::Accepted
    ) && filled > 0.0
        && (quantity <= 0.0 || filled < quantity)
    {
        return OrderCommandStatus::PartiallyFilled;
    }
    status
}

fn binance_status(status: Option<&str>) -> OrderCommandStatus {
    match status.unwrap_or_default().to_ascii_uppercase().as_str() {
        "NEW" => OrderCommandStatus::Accepted,
        "PARTIALLY_FILLED" => OrderCommandStatus::PartiallyFilled,
        "FILLED" => OrderCommandStatus::Filled,
        "CANCELED" | "CANCELLED" | "EXPIRED" => OrderCommandStatus::Cancelled,
        "REJECTED" => OrderCommandStatus::Rejected,
        _ => OrderCommandStatus::Submitted,
    }
}

fn gate_status(status: Option<&str>, left: f64) -> OrderCommandStatus {
    match status.unwrap_or_default().to_ascii_lowercase().as_str() {
        "open" if left > 0.0 => OrderCommandStatus::Accepted,
        "finished" | "filled" => OrderCommandStatus::Filled,
        "cancelled" | "canceled" | "cancelled_by_user" => OrderCommandStatus::Cancelled,
        "rejected" | "failed" => OrderCommandStatus::Rejected,
        _ if left > 0.0 => OrderCommandStatus::PartiallyFilled,
        _ => OrderCommandStatus::Filled,
    }
}

fn bybit_status(status: Option<&str>) -> OrderCommandStatus {
    match status.unwrap_or_default().to_ascii_lowercase().as_str() {
        "new" | "created" | "untriggered" => OrderCommandStatus::Accepted,
        "partiallyfilled" => OrderCommandStatus::PartiallyFilled,
        "filled" => OrderCommandStatus::Filled,
        "cancelled" | "canceled" | "partiallyfilledcanceled" => OrderCommandStatus::Cancelled,
        "rejected" | "deactivated" => OrderCommandStatus::Rejected,
        _ => OrderCommandStatus::Submitted,
    }
}

fn mexc_order_status(item: &Value) -> OrderCommandStatus {
    let status = mexc_status(i64_field(item, &["state"]).or_else(|| i64_field(item, &["status"])));
    let filled = f64_field(item, &["dealVol", "filledQty"]).unwrap_or_default();
    let quantity = f64_field(item, &["vol", "qty"]).unwrap_or_default();
    if matches!(
        status,
        OrderCommandStatus::Submitted | OrderCommandStatus::Accepted
    ) && filled > 0.0
        && (quantity <= 0.0 || filled < quantity)
    {
        return OrderCommandStatus::PartiallyFilled;
    }
    status
}

fn mexc_status(status: Option<i64>) -> OrderCommandStatus {
    match status {
        Some(1 | 2) => OrderCommandStatus::Accepted,
        Some(3) => OrderCommandStatus::Filled,
        Some(4) => OrderCommandStatus::Cancelled,
        Some(5) => OrderCommandStatus::Rejected,
        _ => OrderCommandStatus::Submitted,
    }
}

fn htx_status(status: Option<i64>) -> OrderCommandStatus {
    match status {
        Some(1 | 2 | 3) => OrderCommandStatus::Accepted,
        Some(4) => OrderCommandStatus::PartiallyFilled,
        Some(5 | 6) => OrderCommandStatus::Filled,
        Some(7) => OrderCommandStatus::Cancelled,
        Some(10 | 11) => OrderCommandStatus::Rejected,
        _ => OrderCommandStatus::Submitted,
    }
}

fn timestamp_ms_from_any(timestamp: i64) -> i64 {
    if timestamp > 1_000_000_000_000 {
        timestamp
    } else {
        timestamp * 1000
    }
}

fn timestamp_secs_from_any(timestamp: i64) -> i64 {
    if timestamp > 1_000_000_000_000 {
        timestamp / 1000
    } else {
        timestamp
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::{BundleLeg, OrderIntent};
    use crate::market::RuntimeMode;
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct MockTransport {
        response: Value,
        error: Option<PrivateRestError>,
        seen: Arc<Mutex<Vec<PrivateRestRequestSpec>>>,
    }

    impl MockTransport {
        fn new(response: Value) -> Self {
            Self {
                response,
                error: None,
                seen: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn with_error(error: PrivateRestError) -> Self {
            Self {
                response: Value::Null,
                error: Some(error),
                seen: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl PrivateRestTransport for MockTransport {
        async fn execute(&self, request: PrivateRestRequestSpec) -> Result<Value> {
            self.seen.lock().unwrap().push(request);
            if let Some(error) = &self.error {
                return Err(error.clone().into());
            }
            Ok(self.response.clone())
        }
    }

    fn assert_open_order_fields(
        order: &OrderState,
        client_order_id: &str,
        quantity: f64,
        price: f64,
    ) {
        assert_eq!(order.client_order_id.as_deref(), Some(client_order_id));
        assert_eq!(order.side, OrderSide::Buy);
        assert_eq!(order.position_side, PositionSide::Long);
        assert_eq!(order.order_type, OrderType::Limit);
        assert_eq!(order.status, OrderCommandStatus::Accepted);
        assert_eq!(order.quantity, quantity);
        assert_eq!(order.filled_quantity, 0.0);
        assert_eq!(order.price, Some(price));
    }

    fn assert_close_ack(
        ack: &ClosePositionAck,
        exchange: ExchangeId,
        client_order_id: &str,
        exchange_order_id: &str,
        message: Option<&str>,
    ) {
        assert!(ack.accepted);
        assert_eq!(ack.exchange, exchange);
        assert_eq!(ack.client_order_id, client_order_id);
        assert_eq!(ack.exchange_order_id.as_deref(), Some(exchange_order_id));
        assert_eq!(ack.status, OrderCommandStatus::Accepted);
        assert_eq!(ack.message.as_deref(), message);
    }

    fn assert_cancel_all_ack(
        ack: &CancelAllAck,
        exchange: ExchangeId,
        exchange_symbol: Option<&str>,
        cancelled_orders: usize,
        message: Option<&str>,
    ) {
        assert!(ack.accepted);
        assert_eq!(ack.exchange, exchange);
        assert_eq!(
            ack.exchange_symbol
                .as_ref()
                .map(|symbol| symbol.symbol.as_str()),
            exchange_symbol
        );
        assert_eq!(ack.cancelled_orders, cancelled_orders);
        assert_eq!(ack.message.as_deref(), message);
    }

    fn assert_countdown_ack(
        ack: &CountdownCancelAllAck,
        exchange: ExchangeId,
        exchange_symbol: Option<&str>,
        timeout_secs: u32,
        trigger_time: Option<i64>,
        message: Option<&str>,
    ) {
        assert!(ack.accepted);
        assert_eq!(ack.exchange, exchange);
        assert_eq!(
            ack.exchange_symbol
                .as_ref()
                .map(|symbol| symbol.symbol.as_str()),
            exchange_symbol
        );
        assert_eq!(ack.timeout_secs, timeout_secs);
        assert_eq!(
            ack.trigger_time
                .map(|trigger_time| trigger_time.timestamp()),
            trigger_time
        );
        assert_eq!(ack.message.as_deref(), message);
    }

    fn assert_batch_place_ack(
        ack: &BatchPlaceAck,
        exchange: ExchangeId,
        accepted: bool,
        placed_orders: usize,
        failed_orders: usize,
        message: Option<&str>,
    ) {
        assert_eq!(ack.accepted, accepted);
        assert_eq!(ack.exchange, exchange);
        assert_eq!(ack.placed_orders, placed_orders);
        assert_eq!(ack.order_acks.len(), placed_orders);
        assert_eq!(ack.failed_orders.len(), failed_orders);
        assert_eq!(ack.message.as_deref(), message);
    }

    fn assert_cancel_batch_ack(
        ack: &CancelBatchAck,
        exchange: ExchangeId,
        accepted: bool,
        cancelled_orders: usize,
        order_acks: usize,
        message: Option<&str>,
    ) {
        assert_eq!(ack.accepted, accepted);
        assert_eq!(ack.exchange, exchange);
        assert_eq!(ack.cancelled_orders, cancelled_orders);
        assert_eq!(ack.order_acks.len(), order_acks);
        assert_eq!(ack.message.as_deref(), message);
    }

    fn assert_amend_ack(
        ack: &AmendOrderAck,
        exchange: ExchangeId,
        client_order_id: Option<&str>,
        exchange_order_id: Option<&str>,
        message: Option<&str>,
    ) {
        assert!(ack.accepted);
        assert_eq!(ack.exchange, exchange);
        assert_eq!(ack.client_order_id.as_deref(), client_order_id);
        assert_eq!(ack.exchange_order_id.as_deref(), exchange_order_id);
        assert_eq!(ack.status, OrderCommandStatus::Accepted);
        assert_eq!(ack.message.as_deref(), message);
    }

    fn assert_leverage_ack(
        ack: &LeverageAck,
        exchange: ExchangeId,
        exchange_symbol: &str,
        leverage: u32,
        message: Option<&str>,
    ) {
        assert!(ack.accepted);
        assert_eq!(ack.exchange, exchange);
        assert_eq!(ack.exchange_symbol.symbol, exchange_symbol);
        assert_eq!(ack.leverage, leverage);
        assert_eq!(ack.message.as_deref(), message);
    }

    fn assert_position_mode_ack(
        ack: &PositionModeAck,
        exchange: ExchangeId,
        mode: PositionMode,
        message: Option<&str>,
    ) {
        assert!(ack.accepted);
        assert_eq!(ack.exchange, exchange);
        assert_eq!(ack.mode, mode);
        assert_eq!(ack.message.as_deref(), message);
    }

    fn assert_single_order_fields(
        order: &OrderState,
        expected: (OrderSide, PositionSide, OrderType, OrderCommandStatus),
        quantity: f64,
        filled_quantity: f64,
        price: f64,
        average_fill_price: Option<f64>,
    ) {
        assert_eq!(order.side, expected.0);
        assert_eq!(order.position_side, expected.1);
        assert_eq!(order.order_type, expected.2);
        assert_eq!(order.status, expected.3);
        assert_eq!(order.quantity, quantity);
        assert_eq!(order.filled_quantity, filled_quantity);
        assert_eq!(order.price, Some(price));
        assert_eq!(order.average_fill_price, average_fill_price);
    }

    fn parse_json_message(message: &str) -> Value {
        serde_json::from_str(message).unwrap()
    }

    #[derive(Clone)]
    struct SequentialMockTransport {
        responses: Arc<Mutex<Vec<Value>>>,
        seen: Arc<Mutex<Vec<PrivateRestRequestSpec>>>,
    }

    impl SequentialMockTransport {
        fn new(responses: impl IntoIterator<Item = Value>) -> Self {
            Self {
                responses: Arc::new(Mutex::new(responses.into_iter().collect())),
                seen: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl PrivateRestTransport for SequentialMockTransport {
        async fn execute(&self, request: PrivateRestRequestSpec) -> Result<Value> {
            self.seen.lock().unwrap().push(request);
            let mut responses = self.responses.lock().unwrap();
            if responses.is_empty() {
                anyhow::bail!("sequential mock transport has no response left");
            }
            Ok(responses.remove(0))
        }
    }

    fn command(exchange: ExchangeId, symbol: &str) -> OrderCommand {
        OrderCommand::new(
            RuntimeMode::LiveSmall,
            "bundle-abcdef",
            BundleLeg::Maker,
            1,
            exchange.clone(),
            CanonicalSymbol::new("BTC", "USDT"),
            ExchangeSymbol::new(exchange, symbol),
            OrderIntent::OpenLongMaker,
            OrderSide::Buy,
            PositionSide::Long,
            OrderType::Limit,
            0.01,
            Some(65_000.0),
            TimeInForce::PostOnly,
            true,
            false,
            None,
            Utc::now(),
        )
    }

    fn instrument(exchange: ExchangeId, symbol: &str) -> InstrumentMeta {
        InstrumentMeta::new(
            exchange.clone(),
            CanonicalSymbol::new("BTC", "USDT"),
            ExchangeSymbol::new(exchange, symbol),
            "BTC",
            "USDT",
            "USDT",
            crate::market::ContractType::LinearPerpetual,
            1.0,
            0.5,
            0.001,
            0.001,
            10.0,
            1,
            3,
            crate::market::InstrumentStatus::Trading,
        )
    }

    #[derive(Clone)]
    struct DefaultHistoryFallbackProtocol;

    impl PrivatePerpProtocol for DefaultHistoryFallbackProtocol {
        fn exchange(&self) -> PrivatePerpExchange {
            PrivatePerpExchange::Okx
        }

        fn place_order(
            &self,
            _command: &OrderCommand,
            _position_mode: PositionMode,
        ) -> Result<PrivateRestRequestSpec> {
            anyhow::bail!("unused")
        }

        fn cancel_order(&self, _command: &CancelCommand) -> Result<PrivateRestRequestSpec> {
            anyhow::bail!("unused")
        }

        fn cancel_all_orders(&self, _command: &CancelAllCommand) -> Result<PrivateRestRequestSpec> {
            anyhow::bail!("unused")
        }

        fn cancel_batch_orders(
            &self,
            _command: &CancelBatchCommand,
        ) -> Result<PrivateRestRequestSpec> {
            anyhow::bail!("unused")
        }

        fn get_order(&self, _query: &OrderQuery) -> Result<PrivateRestRequestSpec> {
            anyhow::bail!("unused")
        }

        fn get_open_orders(
            &self,
            _symbol: Option<&ExchangeSymbol>,
        ) -> Result<PrivateRestRequestSpec> {
            anyhow::bail!("unused")
        }

        fn get_fills(&self, _query: &FillQuery) -> Result<PrivateRestRequestSpec> {
            anyhow::bail!("unused")
        }

        fn get_positions(
            &self,
            _symbol: Option<&ExchangeSymbol>,
        ) -> Result<PrivateRestRequestSpec> {
            anyhow::bail!("unused")
        }

        fn get_balances(&self) -> Result<PrivateRestRequestSpec> {
            anyhow::bail!("unused")
        }

        fn get_trade_fee(&self, _symbol: &ExchangeSymbol) -> Result<PrivateRestRequestSpec> {
            anyhow::bail!("unused")
        }

        fn get_symbol_account_config(
            &self,
            _symbol: &ExchangeSymbol,
        ) -> Result<PrivateRestRequestSpec> {
            anyhow::bail!("unused")
        }

        fn amend_order(&self, _command: &AmendOrderCommand) -> Result<PrivateRestRequestSpec> {
            anyhow::bail!("unused")
        }

        fn set_leverage(&self, _command: &LeverageCommand) -> Result<PrivateRestRequestSpec> {
            anyhow::bail!("unused")
        }

        fn set_position_mode(
            &self,
            _command: &PositionModeCommand,
        ) -> Result<PrivateRestRequestSpec> {
            anyhow::bail!("unused")
        }

        fn set_countdown_cancel_all(
            &self,
            _command: &CountdownCancelAllCommand,
        ) -> Result<PrivateRestRequestSpec> {
            anyhow::bail!("unused")
        }

        fn ws_login(&self, _auth: &PrivateWsAuth, _timestamp: i64) -> Result<PrivateWsRequest> {
            anyhow::bail!("unused")
        }

        fn ws_subscribe(
            &self,
            _subscription: &PrivateWsSubscription,
            _timestamp: i64,
        ) -> Result<PrivateWsRequest> {
            anyhow::bail!("unused")
        }

        fn parse_private_ws_message(
            &self,
            _raw: &str,
            _received_at: DateTime<Utc>,
        ) -> Result<Vec<PrivateEvent>> {
            anyhow::bail!("unused")
        }
    }

    #[test]
    fn default_all_orders_history_should_validate_scope_before_unsupported() {
        let wrong_symbol =
            DefaultHistoryFallbackProtocol.get_all_orders(&OrderHistoryQuery::for_symbol(
                ExchangeId::Okx,
                ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
            ));
        assert!(wrong_symbol
            .unwrap_err()
            .to_string()
            .contains("okx private perp all-orders history requires okx exchange symbols"));

        let wrong_exchange =
            DefaultHistoryFallbackProtocol.get_all_orders(&OrderHistoryQuery::for_symbol(
                ExchangeId::Mexc,
                ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
            ));
        assert!(wrong_exchange
            .unwrap_err()
            .to_string()
            .contains("okx private perp all-orders history requires okx exchange symbols"));

        let unsupported =
            DefaultHistoryFallbackProtocol.get_all_orders(&OrderHistoryQuery::for_symbol(
                ExchangeId::Okx,
                ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
            ));
        assert!(unsupported
            .unwrap_err()
            .to_string()
            .contains("okx private perp all-orders history is not implemented"));
    }

    #[test]
    fn default_batch_place_should_validate_scope_before_unsupported() {
        let valid_order = command(ExchangeId::Okx, "BTC-USDT-SWAP");

        let wrong_exchange = DefaultHistoryFallbackProtocol.place_batch_orders(
            &BatchPlaceCommand::new(ExchangeId::Mexc, [valid_order.clone()], Utc::now()),
            PositionMode::Hedge,
        );
        assert!(wrong_exchange
            .unwrap_err()
            .to_string()
            .contains("okx private perp batch-place requires okx exchange symbols"));

        let mut wrong_order_symbol = valid_order.clone();
        wrong_order_symbol.exchange_symbol = ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT");
        let wrong_order = DefaultHistoryFallbackProtocol.place_batch_orders(
            &BatchPlaceCommand::new(ExchangeId::Okx, [wrong_order_symbol], Utc::now()),
            PositionMode::Hedge,
        );
        assert!(wrong_order
            .unwrap_err()
            .to_string()
            .contains("okx private perp batch-place requires okx exchange symbols"));

        let empty = DefaultHistoryFallbackProtocol.place_batch_orders(
            &BatchPlaceCommand::new(ExchangeId::Okx, [], Utc::now()),
            PositionMode::Hedge,
        );
        assert!(empty
            .unwrap_err()
            .to_string()
            .contains("okx private perp batch-place requires at least one order"));

        let unsupported = DefaultHistoryFallbackProtocol.place_batch_orders(
            &BatchPlaceCommand::new(ExchangeId::Okx, [valid_order], Utc::now()),
            PositionMode::Hedge,
        );
        assert!(unsupported
            .unwrap_err()
            .to_string()
            .contains("okx private perp batch-place is not implemented"));
    }

    fn gate_contract_instrument(contract_size: f64) -> InstrumentMeta {
        InstrumentMeta::new(
            ExchangeId::Gate,
            CanonicalSymbol::new("BTC", "USDT"),
            ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
            "BTC",
            "USDT",
            "USDT",
            crate::market::ContractType::LinearPerpetual,
            contract_size,
            0.5,
            1.0,
            1.0,
            10.0,
            1,
            0,
            crate::market::InstrumentStatus::Trading,
        )
    }

    fn cancel_all_command(exchange: ExchangeId, symbol: &str) -> CancelAllCommand {
        CancelAllCommand::for_symbol(
            exchange.clone(),
            CanonicalSymbol::new("BTC", "USDT"),
            ExchangeSymbol::new(exchange, symbol),
            Utc::now(),
        )
    }

    #[test]
    fn coinex_private_perp_should_build_common_private_rest_specs() {
        let cmd = command(ExchangeId::CoinEx, "BTCUSDT");
        let place = CoinExPrivatePerpProtocol
            .place_order(&cmd, PositionMode::OneWay)
            .unwrap();
        assert_eq!(place.exchange, ExchangeId::CoinEx);
        assert_eq!(place.method, PrivateRestMethod::Post);
        assert_eq!(place.path, "/futures/order");
        let body = place.body.as_ref().unwrap();
        assert_eq!(body["market"], "BTCUSDT");
        assert_eq!(body["market_type"], "FUTURES");
        assert_eq!(body["side"], "buy");
        assert_eq!(body["type"], "maker_only");
        assert_eq!(body["amount"], "0.01");
        assert_eq!(body["price"], "65000");
        assert_eq!(body["client_id"], "crossarb-ls-mk-1-c0b4e763");

        let batch = CoinExPrivatePerpProtocol
            .place_batch_orders(
                &BatchPlaceCommand::new(ExchangeId::CoinEx, [cmd.clone()], Utc::now()),
                PositionMode::OneWay,
            )
            .unwrap();
        assert_eq!(batch.path, "/futures/batch-order");
        assert_eq!(batch.body.as_ref().unwrap().as_array().unwrap().len(), 1);

        let cancel_all = CoinExPrivatePerpProtocol
            .cancel_all_orders(&cancel_all_command(ExchangeId::CoinEx, "BTCUSDT"))
            .unwrap();
        assert_eq!(cancel_all.path, "/futures/cancel-all-order");
        assert_eq!(cancel_all.body.as_ref().unwrap()["market"], "BTCUSDT");

        let query = OrderQuery {
            exchange: ExchangeId::CoinEx,
            exchange_symbol: ExchangeSymbol::new(ExchangeId::CoinEx, "BTCUSDT"),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: None,
        };
        let order = CoinExPrivatePerpProtocol.get_order(&query).unwrap();
        assert_eq!(order.path, "/futures/order-status");
        assert_eq!(
            order.query.get("client_id").map(String::as_str),
            Some("client-1")
        );
    }

    #[test]
    fn coinex_private_perp_should_build_account_and_control_specs() {
        let symbol = ExchangeSymbol::new(ExchangeId::CoinEx, "BTCUSDT");

        assert_eq!(
            CoinExPrivatePerpProtocol
                .get_open_orders(Some(&symbol))
                .unwrap()
                .path,
            "/futures/pending-order"
        );
        assert_eq!(
            CoinExPrivatePerpProtocol
                .get_all_orders(&OrderHistoryQuery::for_symbol(
                    ExchangeId::CoinEx,
                    symbol.clone(),
                ))
                .unwrap()
                .path,
            "/futures/finished-order"
        );
        assert_eq!(
            CoinExPrivatePerpProtocol.get_balances().unwrap().path,
            "/assets/futures/balance"
        );
        assert_eq!(
            CoinExPrivatePerpProtocol
                .get_positions(Some(&symbol))
                .unwrap()
                .path,
            "/futures/pending-position"
        );

        let leverage = CoinExPrivatePerpProtocol
            .set_leverage(&LeverageCommand {
                exchange: ExchangeId::CoinEx,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: symbol.clone(),
                leverage: 7,
                requested_at: Utc::now(),
            })
            .unwrap();
        assert_eq!(leverage.path, "/futures/adjust-position-leverage");
        assert_eq!(leverage.body.as_ref().unwrap()["leverage"], 7);

        let amend = CoinExPrivatePerpProtocol
            .amend_order(&AmendOrderCommand {
                exchange: ExchangeId::CoinEx,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: symbol,
                exchange_order_id: Some("12345".to_string()),
                client_order_id: None,
                original_side: None,
                new_client_order_id: None,
                new_price: Some(64_900.0),
                new_quantity: Some(0.02),
                requested_at: Utc::now(),
            })
            .unwrap();
        assert_eq!(amend.path, "/futures/modify-order");
        assert_eq!(amend.body.as_ref().unwrap()["amount"], "0.02");
    }

    #[test]
    fn coinex_private_perp_should_keep_unverified_capabilities_unsupported() {
        let position_mode = CoinExPrivatePerpProtocol
            .set_position_mode(&PositionModeCommand {
                exchange: ExchangeId::CoinEx,
                mode: PositionMode::Hedge,
                requested_at: Utc::now(),
            })
            .unwrap_err();
        assert!(position_mode
            .to_string()
            .contains("position mode change is not implemented"));

        let countdown = CoinExPrivatePerpProtocol
            .set_countdown_cancel_all(&CountdownCancelAllCommand::set_for_symbol(
                ExchangeId::CoinEx,
                ExchangeSymbol::new(ExchangeId::CoinEx, "BTCUSDT"),
                30,
                Utc::now(),
            ))
            .unwrap_err();
        assert!(countdown
            .to_string()
            .contains("countdown cancel-all is not implemented"));
    }

    #[test]
    fn coinex_private_perp_headers_should_match_v2_hmac_shape() {
        let auth = PrivateRestAuth {
            api_key: "key".to_string(),
            api_secret: "secret".to_string(),
            passphrase: None,
            demo_trading: false,
        };
        let spec = PrivateRestRequestSpec::new(
            ExchangeId::CoinEx,
            PrivateRestMethod::Post,
            "/futures/order",
        )
        .with_body(json!({"market":"BTCUSDT","market_type":"FUTURES"}));

        let headers = coinex_rest_headers(&auth, &spec, 1_700_000_000_000).unwrap();
        let expected = coinex_signature(
            "secret",
            r#"POST/futures/order{"market":"BTCUSDT","market_type":"FUTURES"}1700000000000"#,
        );
        assert_eq!(headers.get("X-COINEX-KEY").map(String::as_str), Some("key"));
        assert_eq!(
            headers.get("X-COINEX-SIGN").map(String::as_str),
            Some(expected.as_str())
        );
        assert_eq!(
            headers.get("X-COINEX-TIMESTAMP").map(String::as_str),
            Some("1700000000000")
        );
    }

    #[test]
    fn binance_should_build_common_private_rest_specs() {
        let cmd = command(ExchangeId::Binance, "BTCUSDT");
        let place = BinancePrivatePerpProtocol
            .place_order(&cmd, PositionMode::Hedge)
            .unwrap();
        assert_eq!(place.method, PrivateRestMethod::Post);
        assert_eq!(place.path, "/fapi/v1/order");
        assert_eq!(
            place.query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
        assert_eq!(place.query.get("side").map(String::as_str), Some("BUY"));
        assert_eq!(place.query.get("type").map(String::as_str), Some("LIMIT"));
        assert_eq!(
            place.query.get("timeInForce").map(String::as_str),
            Some("GTX")
        );
        assert_eq!(
            place.query.get("positionSide").map(String::as_str),
            Some("LONG")
        );
        assert_eq!(
            place.query.get("newClientOrderId").map(String::as_str),
            Some("crossarb-ls-mk-1-c0b4e763")
        );

        let cancel_all = BinancePrivatePerpProtocol
            .cancel_all_orders(&CancelAllCommand::for_symbol(
                ExchangeId::Binance,
                CanonicalSymbol::new("BTC", "USDT"),
                ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT"),
                Utc::now(),
            ))
            .unwrap();
        assert_eq!(cancel_all.method, PrivateRestMethod::Delete);
        assert_eq!(cancel_all.path, "/fapi/v1/allOpenOrders");

        let cancel = CancelCommand {
            exchange: ExchangeId::Binance,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT"),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: Some("12345".to_string()),
            reason: None,
            requested_at: Utc::now(),
        };
        let batch = BinancePrivatePerpProtocol
            .cancel_batch_orders(&CancelBatchCommand::new(
                ExchangeId::Binance,
                [cancel],
                Utc::now(),
            ))
            .unwrap();
        assert_eq!(batch.path, "/fapi/v1/batchOrders");
        assert_eq!(
            batch.query.get("orderIdList").map(String::as_str),
            Some("[12345]")
        );

        let amend = BinancePrivatePerpProtocol
            .amend_order(&AmendOrderCommand {
                exchange: ExchangeId::Binance,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT"),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: None,
                original_side: Some(OrderSide::Buy),
                new_client_order_id: None,
                new_quantity: Some(0.02),
                new_price: Some(64_900.0),
                requested_at: Utc::now(),
            })
            .unwrap();
        assert_eq!(amend.method, PrivateRestMethod::Put);
        assert_eq!(amend.path, "/fapi/v1/order");
        assert_eq!(
            amend.query.get("origClientOrderId").map(String::as_str),
            Some("client-1")
        );
        assert_eq!(amend.query.get("side").map(String::as_str), Some("BUY"));
        assert_eq!(
            amend.query.get("quantity").map(String::as_str),
            Some("0.02")
        );

        let wrong_amend_symbol = BinancePrivatePerpProtocol.amend_order(&AmendOrderCommand {
            exchange: ExchangeId::Binance,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: None,
            original_side: Some(OrderSide::Buy),
            new_client_order_id: None,
            new_quantity: Some(0.02),
            new_price: Some(64_900.0),
            requested_at: Utc::now(),
        });
        assert!(wrong_amend_symbol
            .unwrap_err()
            .to_string()
            .contains("binance amend requires binance exchange symbols"));

        let fee = BinancePrivatePerpProtocol
            .get_trade_fee(&ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT"))
            .unwrap();
        assert_eq!(fee.path, "/fapi/v1/commissionRate");
        let config = BinancePrivatePerpProtocol
            .get_symbol_account_config(&ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT"))
            .unwrap();
        assert_eq!(config.path, "/fapi/v1/symbolConfig");
        let countdown = BinancePrivatePerpProtocol
            .set_countdown_cancel_all(&CountdownCancelAllCommand::set_for_symbol(
                ExchangeId::Binance,
                ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT"),
                30,
                Utc::now(),
            ))
            .unwrap();
        assert_eq!(countdown.path, "/fapi/v1/countdownCancelAll");
        assert_eq!(
            countdown.query.get("countdownTime").map(String::as_str),
            Some("30000")
        );
    }

    #[test]
    fn binance_symbol_scoped_specs_should_validate_exchange_scope() {
        let wrong_symbol = ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT");
        let mut place = command(ExchangeId::Binance, "BTCUSDT");
        place.exchange_symbol = wrong_symbol.clone();
        assert!(BinancePrivatePerpProtocol
            .place_order(&place, PositionMode::Hedge)
            .unwrap_err()
            .to_string()
            .contains("place order requires binance exchange symbols"));

        let cancel = CancelCommand {
            exchange: ExchangeId::Binance,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: wrong_symbol.clone(),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: None,
            reason: None,
            requested_at: Utc::now(),
        };
        assert!(BinancePrivatePerpProtocol
            .cancel_order(&cancel)
            .unwrap_err()
            .to_string()
            .contains("cancel requires binance exchange symbols"));
        assert!(BinancePrivatePerpProtocol
            .cancel_all_orders(&CancelAllCommand::for_symbol(
                ExchangeId::Binance,
                CanonicalSymbol::new("BTC", "USDT"),
                wrong_symbol.clone(),
                Utc::now(),
            ))
            .unwrap_err()
            .to_string()
            .contains("cancel-all requires binance exchange symbols"));

        let binance_symbol = ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT");
        assert!(BinancePrivatePerpProtocol
            .cancel_all_orders(&CancelAllCommand::for_symbol(
                ExchangeId::Mexc,
                CanonicalSymbol::new("BTC", "USDT"),
                binance_symbol.clone(),
                Utc::now(),
            ))
            .unwrap_err()
            .to_string()
            .contains("cancel-all requires binance exchange symbols"));

        let batch_place_wrong_exchange = BatchPlaceCommand::new(
            ExchangeId::Mexc,
            [command(ExchangeId::Binance, "BTCUSDT")],
            Utc::now(),
        );
        assert!(BinancePrivatePerpProtocol
            .place_batch_orders(&batch_place_wrong_exchange, PositionMode::Hedge)
            .unwrap_err()
            .to_string()
            .contains("batch-place requires binance exchange symbols"));

        assert!(BinancePrivatePerpProtocol
            .cancel_batch_orders(&CancelBatchCommand::new(
                ExchangeId::Binance,
                [cancel],
                Utc::now(),
            ))
            .unwrap_err()
            .to_string()
            .contains("batch cancel requires binance exchange symbols"));

        let batch_cancel_wrong_exchange = CancelBatchCommand::new(
            ExchangeId::Mexc,
            [CancelCommand {
                exchange: ExchangeId::Binance,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: binance_symbol,
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: None,
                reason: None,
                requested_at: Utc::now(),
            }],
            Utc::now(),
        );
        assert!(BinancePrivatePerpProtocol
            .cancel_batch_orders(&batch_cancel_wrong_exchange)
            .unwrap_err()
            .to_string()
            .contains("batch cancel requires binance exchange symbols"));

        assert!(BinancePrivatePerpProtocol
            .get_order(&OrderQuery {
                exchange: ExchangeId::Binance,
                exchange_symbol: wrong_symbol.clone(),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: None,
            })
            .unwrap_err()
            .to_string()
            .contains("query order requires binance exchange symbols"));
        assert!(BinancePrivatePerpProtocol
            .get_open_orders(Some(&wrong_symbol))
            .unwrap_err()
            .to_string()
            .contains("open orders requires binance exchange symbols"));
        assert!(BinancePrivatePerpProtocol
            .get_fills(&FillQuery::for_symbol(
                ExchangeId::Binance,
                CanonicalSymbol::new("BTC", "USDT"),
                wrong_symbol.clone(),
            ))
            .unwrap_err()
            .to_string()
            .contains("userTrades requires binance exchange symbols"));
        assert!(BinancePrivatePerpProtocol
            .get_positions(Some(&wrong_symbol))
            .unwrap_err()
            .to_string()
            .contains("positions requires binance exchange symbols"));
        assert!(BinancePrivatePerpProtocol
            .get_trade_fee(&wrong_symbol)
            .unwrap_err()
            .to_string()
            .contains("trade fee requires binance exchange symbols"));
        assert!(BinancePrivatePerpProtocol
            .get_symbol_account_config(&wrong_symbol)
            .unwrap_err()
            .to_string()
            .contains("symbol account config requires binance exchange symbols"));
        assert!(BinancePrivatePerpProtocol
            .set_leverage(&LeverageCommand {
                exchange: ExchangeId::Binance,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: wrong_symbol.clone(),
                leverage: 3,
                requested_at: Utc::now(),
            })
            .unwrap_err()
            .to_string()
            .contains("set leverage requires binance exchange symbols"));
        assert!(BinancePrivatePerpProtocol
            .set_countdown_cancel_all(&CountdownCancelAllCommand::set_for_symbol(
                ExchangeId::Binance,
                wrong_symbol,
                30,
                Utc::now(),
            ))
            .unwrap_err()
            .to_string()
            .contains("countdown cancel-all requires binance exchange symbols"));
    }

    #[test]
    fn binance_should_build_order_history_specs() {
        let start_time = DateTime::<Utc>::from_timestamp_millis(1_700_000_000_000).unwrap();
        let end_time = DateTime::<Utc>::from_timestamp_millis(1_700_000_600_000).unwrap();
        let mut history = OrderHistoryQuery::for_symbol(
            ExchangeId::Binance,
            ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT"),
        );
        history.exchange_order_id = Some("1917641".to_string());
        history.start_time = Some(start_time);
        history.end_time = Some(end_time);
        history.limit = Some(1000);

        let spec = BinancePrivatePerpProtocol.get_all_orders(&history).unwrap();
        assert_eq!(spec.method, PrivateRestMethod::Get);
        assert_eq!(spec.path, "/fapi/v1/allOrders");
        assert_eq!(
            spec.query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
        assert_eq!(
            spec.query.get("orderId").map(String::as_str),
            Some("1917641")
        );
        assert_eq!(
            spec.query.get("startTime").map(String::as_str),
            Some("1700000000000")
        );
        assert_eq!(
            spec.query.get("endTime").map(String::as_str),
            Some("1700000600000")
        );
        assert_eq!(spec.query.get("limit").map(String::as_str), Some("1000"));

        let mut amendments = OrderAmendmentHistoryQuery::for_order(
            ExchangeId::Binance,
            ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT"),
            Some("client-1".to_string()),
            None,
        );
        amendments.limit = Some(100);

        let spec = BinancePrivatePerpProtocol
            .get_order_amendments(&amendments)
            .unwrap();
        assert_eq!(spec.method, PrivateRestMethod::Get);
        assert_eq!(spec.path, "/fapi/v1/orderAmendment");
        assert_eq!(
            spec.query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
        assert_eq!(
            spec.query.get("origClientOrderId").map(String::as_str),
            Some("client-1")
        );
        assert_eq!(spec.query.get("limit").map(String::as_str), Some("100"));
    }

    #[test]
    fn binance_should_build_batch_place_spec() {
        let mut first = command(ExchangeId::Binance, "BTCUSDT");
        first.client_order_id = "client-1".to_string();
        let mut second = command(ExchangeId::Binance, "BTCUSDT");
        second.client_order_id = "client-2".to_string();
        second.side = OrderSide::Sell;
        second.position_side = PositionSide::Short;
        second.quantity = 0.02;
        second.price = Some(66_000.0);

        let spec = BinancePrivatePerpProtocol
            .place_batch_orders(
                &BatchPlaceCommand::new(ExchangeId::Binance, [first, second], Utc::now()),
                PositionMode::Hedge,
            )
            .unwrap();
        assert_eq!(spec.method, PrivateRestMethod::Post);
        assert_eq!(spec.path, "/fapi/v1/batchOrders");
        let orders: Value = serde_json::from_str(spec.query.get("batchOrders").unwrap()).unwrap();
        assert_eq!(orders[0]["symbol"], "BTCUSDT");
        assert_eq!(orders[0]["side"], "BUY");
        assert_eq!(orders[0]["type"], "LIMIT");
        assert_eq!(orders[0]["timeInForce"], "GTX");
        assert_eq!(orders[0]["positionSide"], "LONG");
        assert_eq!(orders[0]["newClientOrderId"], "client-1");
        assert_eq!(orders[1]["side"], "SELL");
        assert_eq!(orders[1]["positionSide"], "SHORT");
        assert_eq!(orders[1]["quantity"], "0.02");
    }

    #[tokio::test]
    async fn binance_adapter_should_place_batch_orders_and_split_failures() {
        let transport = MockTransport::new(json!([
            {
                "symbol": "BTCUSDT",
                "orderId": 1917641,
                "clientOrderId": "client-1",
                "status": "NEW"
            },
            {
                "code": -2022,
                "msg": "ReduceOnly Order is rejected."
            }
        ]));
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(BinancePrivatePerpProtocol, transport)
            .with_position_mode(PositionMode::Hedge);
        let mut first = command(ExchangeId::Binance, "BTCUSDT");
        first.client_order_id = "client-1".to_string();
        let mut second = command(ExchangeId::Binance, "BTCUSDT");
        second.client_order_id = "client-2".to_string();
        second.reduce_only = true;

        let ack = adapter
            .place_batch_orders(BatchPlaceCommand::new(
                ExchangeId::Binance,
                [first, second],
                Utc::now(),
            ))
            .await
            .unwrap();

        assert_batch_place_ack(&ack, ExchangeId::Binance, false, 1, 1, None);
        assert_eq!(ack.order_acks[0].exchange, ExchangeId::Binance);
        assert_eq!(ack.order_acks[0].client_order_id, "client-1");
        assert_eq!(
            ack.order_acks[0].exchange_order_id.as_deref(),
            Some("1917641")
        );
        assert_eq!(ack.order_acks[0].status, OrderCommandStatus::Accepted);
        assert!(ack.order_acks[0].accepted);
        assert_eq!(ack.failed_orders[0].code.as_deref(), Some("-2022"));
        assert_eq!(
            ack.failed_orders[0].message,
            "ReduceOnly Order is rejected."
        );
        assert_eq!(ack.failed_orders[0].order.client_order_id, "client-2");
        assert_eq!(seen.lock().unwrap()[0].path, "/fapi/v1/batchOrders");
    }

    #[tokio::test]
    async fn binance_adapter_should_batch_cancel_by_order_ids() {
        let transport = MockTransport::new(json!([
            {
                "symbol": "BTCUSDT",
                "orderId": 1917641,
                "clientOrderId": "client-1",
                "status": "CANCELED"
            },
            {
                "symbol": "BTCUSDT",
                "orderId": 1917642,
                "clientOrderId": "client-2",
                "status": "CANCELED"
            }
        ]));
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(BinancePrivatePerpProtocol, transport);
        let first = CancelCommand {
            exchange: ExchangeId::Binance,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT"),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: Some("1917641".to_string()),
            reason: None,
            requested_at: Utc::now(),
        };
        let second = CancelCommand {
            client_order_id: Some("client-2".to_string()),
            exchange_order_id: Some("1917642".to_string()),
            ..first.clone()
        };

        let ack = adapter
            .cancel_batch_orders(CancelBatchCommand::new(
                ExchangeId::Binance,
                [first, second],
                Utc::now(),
            ))
            .await
            .unwrap();

        assert_cancel_batch_ack(&ack, ExchangeId::Binance, true, 2, 2, None);
        assert!(ack.order_acks[0].accepted);
        assert_eq!(ack.order_acks[0].exchange, ExchangeId::Binance);
        assert_eq!(
            ack.order_acks[0].client_order_id.as_deref(),
            Some("client-1")
        );
        assert_eq!(
            ack.order_acks[0].exchange_order_id.as_deref(),
            Some("1917641")
        );
        assert_eq!(ack.order_acks[0].status, OrderCommandStatus::Cancelled);
        assert!(ack.order_acks[1].accepted);
        assert_eq!(ack.order_acks[1].exchange, ExchangeId::Binance);
        assert_eq!(
            ack.order_acks[1].client_order_id.as_deref(),
            Some("client-2")
        );
        assert_eq!(
            ack.order_acks[1].exchange_order_id.as_deref(),
            Some("1917642")
        );
        assert_eq!(ack.order_acks[1].status, OrderCommandStatus::Cancelled);
        let seen = seen.lock().unwrap();
        assert_eq!(seen[0].method, PrivateRestMethod::Delete);
        assert_eq!(seen[0].path, "/fapi/v1/batchOrders");
        assert_eq!(
            seen[0].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
        assert_eq!(
            seen[0].query.get("orderIdList").map(String::as_str),
            Some("[1917641,1917642]")
        );
    }

    #[tokio::test]
    async fn binance_adapter_should_route_single_cancel_and_order_readbacks() {
        let transport = SequentialMockTransport::new([
            json!({
                "symbol": "BTCUSDT",
                "orderId": 1917641,
                "clientOrderId": "client-1",
                "status": "CANCELED"
            }),
            json!({
                "symbol": "BTCUSDT",
                "orderId": 1917641,
                "clientOrderId": "client-1",
                "side": "BUY",
                "positionSide": "LONG",
                "type": "LIMIT",
                "origQty": "0.40",
                "price": "30004",
                "executedQty": "0.10",
                "avgPrice": "30003.5",
                "timeInForce": "GTC",
                "reduceOnly": false,
                "status": "PARTIALLY_FILLED",
                "updateTime": 1700000000000_i64
            }),
            json!([
                {
                    "symbol": "BTCUSDT",
                    "orderId": 1917642,
                    "clientOrderId": "client-2",
                    "side": "SELL",
                    "positionSide": "SHORT",
                    "type": "LIMIT",
                    "origQty": "0.20",
                    "price": "31000",
                    "executedQty": "0",
                    "avgPrice": "0",
                    "timeInForce": "GTX",
                    "reduceOnly": false,
                    "status": "NEW",
                    "updateTime": 1700000001000_i64
                }
            ]),
        ]);
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(BinancePrivatePerpProtocol, transport);
        let symbol = ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT");

        let cancel_ack = adapter
            .cancel_order(CancelCommand {
                exchange: ExchangeId::Binance,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: symbol.clone(),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: Some("1917641".to_string()),
                reason: None,
                requested_at: Utc::now(),
            })
            .await
            .unwrap();
        assert!(cancel_ack.accepted);
        assert_eq!(cancel_ack.status, OrderCommandStatus::Cancelled);
        assert_eq!(cancel_ack.exchange_order_id.as_deref(), Some("1917641"));

        let order = adapter
            .get_order(OrderQuery {
                exchange: ExchangeId::Binance,
                exchange_symbol: symbol.clone(),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: Some("1917641".to_string()),
            })
            .await
            .unwrap();
        assert_eq!(order.exchange, ExchangeId::Binance);
        assert_eq!(order.exchange_order_id.as_deref(), Some("1917641"));
        assert_eq!(order.client_order_id.as_deref(), Some("client-1"));
        assert_eq!(order.status, OrderCommandStatus::PartiallyFilled);
        assert_eq!(order.position_side, PositionSide::Long);
        assert_eq!(order.quantity, 0.40);
        assert_eq!(order.filled_quantity, 0.10);
        assert_eq!(order.average_fill_price, Some(30003.5));

        let open_orders = adapter.get_open_orders(Some(&symbol)).await.unwrap();
        assert_eq!(open_orders.len(), 1);
        assert_eq!(open_orders[0].exchange_order_id.as_deref(), Some("1917642"));
        assert_eq!(open_orders[0].client_order_id.as_deref(), Some("client-2"));
        assert_eq!(open_orders[0].status, OrderCommandStatus::Accepted);
        assert_eq!(open_orders[0].position_side, PositionSide::Short);
        assert_eq!(open_orders[0].quantity, 0.20);

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 3);
        assert_eq!(seen[0].method, PrivateRestMethod::Delete);
        assert_eq!(seen[0].path, "/fapi/v1/order");
        assert_eq!(
            seen[0].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
        assert_eq!(
            seen[0].query.get("orderId").map(String::as_str),
            Some("1917641")
        );
        assert_eq!(
            seen[0].query.get("origClientOrderId").map(String::as_str),
            Some("client-1")
        );
        assert_eq!(seen[1].method, PrivateRestMethod::Get);
        assert_eq!(seen[1].path, "/fapi/v1/order");
        assert_eq!(
            seen[1].query.get("orderId").map(String::as_str),
            Some("1917641")
        );
        assert_eq!(
            seen[1].query.get("origClientOrderId").map(String::as_str),
            Some("client-1")
        );
        assert_eq!(seen[2].method, PrivateRestMethod::Get);
        assert_eq!(seen[2].path, "/fapi/v1/openOrders");
        assert_eq!(
            seen[2].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
    }

    #[tokio::test]
    async fn binance_adapter_should_parse_order_and_amendment_history() {
        let order_transport = MockTransport::new(json!([
            {
                "symbol": "BTCUSDT",
                "orderId": 1917641,
                "clientOrderId": "client-1",
                "side": "BUY",
                "positionSide": "LONG",
                "type": "LIMIT",
                "origQty": "0.40",
                "price": "30004",
                "executedQty": "0.10",
                "avgPrice": "30003.5",
                "timeInForce": "GTC",
                "reduceOnly": false,
                "status": "PARTIALLY_FILLED",
                "updateTime": 1700000000000_i64
            }
        ]));
        let order_seen = order_transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(BinancePrivatePerpProtocol, order_transport);
        let orders = adapter
            .get_order_history(OrderHistoryQuery::for_symbol(
                ExchangeId::Binance,
                ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT"),
            ))
            .await
            .unwrap();
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].exchange_order_id.as_deref(), Some("1917641"));
        assert_eq!(orders[0].status, OrderCommandStatus::PartiallyFilled);
        assert_eq!(orders[0].filled_quantity, 0.10);
        assert_eq!(order_seen.lock().unwrap()[0].path, "/fapi/v1/allOrders");

        let amendment_transport = MockTransport::new(json!([
            {
                "amendmentId": 5363,
                "symbol": "BTCUSDT",
                "orderId": 1917641,
                "clientOrderId": "client-1",
                "time": 1700000000000_i64,
                "amendment": {
                    "price": {"before": "30004", "after": "30003.2"},
                    "origQty": {"before": "1", "after": "0.8"},
                    "count": 3
                }
            }
        ]));
        let amendment_seen = amendment_transport.seen.clone();
        let adapter =
            PrivatePerpTradingAdapter::new(BinancePrivatePerpProtocol, amendment_transport);
        let amendments = adapter
            .get_order_amendment_history(OrderAmendmentHistoryQuery::for_order(
                ExchangeId::Binance,
                ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT"),
                Some("client-1".to_string()),
                None,
            ))
            .await
            .unwrap();
        assert_eq!(amendments.len(), 1);
        assert_eq!(amendments[0].amendment_id.as_deref(), Some("5363"));
        assert_eq!(amendments[0].price_before, Some(30004.0));
        assert_eq!(amendments[0].price_after, Some(30003.2));
        assert_eq!(amendments[0].quantity_after, Some(0.8));
        assert_eq!(amendments[0].amendment_count, Some(3));
        assert_eq!(
            amendment_seen.lock().unwrap()[0].path,
            "/fapi/v1/orderAmendment"
        );
    }

    #[tokio::test]
    async fn binance_adapter_should_route_account_and_fill_readbacks() {
        let transport = SequentialMockTransport::new([
            json!([
                {
                    "symbol": "BTCUSDT",
                    "id": 698759,
                    "orderId": 1917641,
                    "clientOrderId": "client-1",
                    "side": "SELL",
                    "positionSide": "SHORT",
                    "price": "65000",
                    "qty": "0.02",
                    "quoteQty": "1300",
                    "commission": "0.65",
                    "commissionAsset": "USDT",
                    "realizedPnl": "12.5",
                    "maker": false,
                    "time": 1700000000000_i64
                }
            ]),
            json!([
                {
                    "symbol": "BTCUSDT",
                    "positionAmt": "-0.02",
                    "entryPrice": "64000",
                    "markPrice": "65000",
                    "unRealizedProfit": "20",
                    "positionSide": "SHORT",
                    "updateTime": 1700000000000_i64
                }
            ]),
            json!([
                {
                    "asset": "USDT",
                    "balance": "1000",
                    "availableBalance": "850",
                    "updateTime": 1700000000000_i64
                }
            ]),
            json!({
                "symbol": "BTCUSDT",
                "makerCommissionRate": "0.0002",
                "takerCommissionRate": "0.0005"
            }),
            json!({
                "symbol": "BTCUSDT",
                "marginType": "cross",
                "positionMode": "hedge_mode",
                "leverage": "10",
                "maxLeverage": "125"
            }),
        ]);
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(BinancePrivatePerpProtocol, transport)
            .with_position_mode(PositionMode::OneWay);
        let symbol = ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT");
        let mut fill_query = FillQuery::for_symbol(
            ExchangeId::Binance,
            CanonicalSymbol::new("BTC", "USDT"),
            symbol.clone(),
        );
        fill_query.exchange_order_id = Some("1917641".to_string());
        fill_query.limit = Some(25);

        let fills = adapter.get_fills(fill_query).await.unwrap();
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].exchange, ExchangeId::Binance);
        assert_eq!(fills[0].trade_id, "698759");
        assert_eq!(fills[0].exchange_order_id.as_deref(), Some("1917641"));
        assert_eq!(fills[0].client_order_id.as_deref(), Some("client-1"));
        assert_eq!(fills[0].side, OrderSide::Sell);
        assert_eq!(fills[0].position_side, PositionSide::Short);
        assert_eq!(fills[0].liquidity, FillLiquidity::Taker);
        assert_eq!(fills[0].price, 65_000.0);
        assert_eq!(fills[0].quantity, 0.02);
        assert_eq!(fills[0].quote_quantity, 1300.0);
        assert_eq!(fills[0].fee, Some(0.65));
        assert_eq!(fills[0].fee_asset.as_deref(), Some("USDT"));
        assert_eq!(fills[0].realized_pnl, Some(12.5));

        let positions = adapter.get_positions(Some(&symbol)).await.unwrap();
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].exchange, ExchangeId::Binance);
        assert_eq!(positions[0].exchange_symbol, symbol);
        assert_eq!(positions[0].position_side, PositionSide::Short);
        assert_eq!(positions[0].quantity, 0.02);
        assert_eq!(positions[0].entry_price, Some(64_000.0));
        assert_eq!(positions[0].mark_price, Some(65_000.0));
        assert_eq!(positions[0].unrealized_pnl, Some(20.0));

        let balances = adapter.get_balances().await.unwrap();
        assert_eq!(balances.len(), 1);
        assert_eq!(balances[0].exchange, ExchangeId::Binance);
        assert_eq!(balances[0].asset, "USDT");
        assert_eq!(balances[0].total, 1000.0);
        assert_eq!(balances[0].available, 850.0);
        assert_eq!(balances[0].locked, 150.0);

        let fee = adapter.get_trade_fee(&symbol).await.unwrap();
        assert_eq!(fee.exchange, ExchangeId::Binance);
        assert_eq!(fee.exchange_symbol, symbol);
        assert_eq!(fee.maker, 0.0002);
        assert_eq!(fee.taker, 0.0005);

        let config = adapter.get_symbol_account_config(&symbol).await.unwrap();
        assert_eq!(config.exchange, ExchangeId::Binance);
        assert_eq!(config.exchange_symbol, symbol);
        assert_eq!(config.position_mode, Some(PositionMode::Hedge));
        assert_eq!(config.margin_mode, Some(MarginMode::Cross));
        assert_eq!(config.leverage, Some(10));
        assert_eq!(config.max_leverage, Some(125));

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 5);
        assert_eq!(seen[0].method, PrivateRestMethod::Get);
        assert_eq!(seen[0].path, "/fapi/v1/userTrades");
        assert_eq!(
            seen[0].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
        assert_eq!(
            seen[0].query.get("orderId").map(String::as_str),
            Some("1917641")
        );
        assert_eq!(seen[0].query.get("limit").map(String::as_str), Some("25"));
        assert_eq!(seen[1].method, PrivateRestMethod::Get);
        assert_eq!(seen[1].path, "/fapi/v2/positionRisk");
        assert_eq!(
            seen[1].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
        assert_eq!(seen[2].method, PrivateRestMethod::Get);
        assert_eq!(seen[2].path, "/fapi/v2/balance");
        assert_eq!(seen[3].method, PrivateRestMethod::Get);
        assert_eq!(seen[3].path, "/fapi/v1/commissionRate");
        assert_eq!(
            seen[3].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
        assert_eq!(seen[4].method, PrivateRestMethod::Get);
        assert_eq!(seen[4].path, "/fapi/v1/symbolConfig");
        assert_eq!(
            seen[4].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
    }

    async fn assert_private_perp_account_and_fill_readbacks<P>(
        adapter: &PrivatePerpTradingAdapter<P, SequentialMockTransport>,
        symbol: &ExchangeSymbol,
        exchange_order_id: Option<&str>,
        client_order_id: Option<&str>,
        expected: AccountFillReadbackExpectations,
    ) where
        P: PrivatePerpProtocol + Send + Sync + Clone,
    {
        let mut fill_query = FillQuery::for_symbol(
            symbol.exchange.clone(),
            CanonicalSymbol::new("BTC", "USDT"),
            symbol.clone(),
        );
        fill_query.exchange_order_id = exchange_order_id.map(str::to_string);
        fill_query.client_order_id = client_order_id.map(str::to_string);
        fill_query.limit = Some(25);

        let fills = adapter.get_fills(fill_query).await.unwrap();
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].exchange, symbol.exchange);
        assert_eq!(fills[0].trade_id, expected.trade_id);
        assert_eq!(fills[0].exchange_symbol, *symbol);
        assert_eq!(
            fills[0].exchange_order_id.as_deref(),
            expected.exchange_order_id
        );
        assert_eq!(fills[0].client_order_id.as_deref(), client_order_id);
        assert_eq!(fills[0].side, expected.fill_side);
        assert_eq!(
            fills[0].position_side, expected.fill_position_side,
            "fill position side mismatch for {:?}",
            symbol.exchange
        );
        assert_eq!(fills[0].liquidity, expected.fill_liquidity);
        assert_eq!(fills[0].price, expected.fill_price);
        assert_eq!(fills[0].quantity, expected.fill_quantity);
        assert_eq!(fills[0].quote_quantity, expected.fill_quote_quantity);
        assert_eq!(fills[0].fee, expected.fill_fee);
        assert_eq!(fills[0].fee_asset.as_deref(), Some("USDT"));
        assert_eq!(fills[0].realized_pnl, expected.fill_realized_pnl);

        let positions = adapter.get_positions(Some(symbol)).await.unwrap();
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].exchange, symbol.exchange);
        assert_eq!(positions[0].exchange_symbol, *symbol);
        assert_eq!(positions[0].position_side, expected.position_side);
        assert_eq!(positions[0].quantity, expected.position_quantity);
        assert_eq!(positions[0].entry_price, expected.position_entry_price);
        assert_eq!(positions[0].mark_price, expected.position_mark_price);
        assert_eq!(
            positions[0].unrealized_pnl,
            expected.position_unrealized_pnl
        );

        let balances = adapter.get_balances().await.unwrap();
        assert_eq!(balances.len(), 1);
        assert_eq!(balances[0].exchange, symbol.exchange);
        assert_eq!(balances[0].asset, "USDT");
        assert_eq!(balances[0].total, expected.balance_total);
        assert_eq!(balances[0].available, expected.balance_available);
        assert_eq!(balances[0].locked, expected.balance_locked);

        let fee = adapter.get_trade_fee(symbol).await.unwrap();
        assert_eq!(fee.exchange, symbol.exchange);
        assert_eq!(fee.exchange_symbol, *symbol);
        assert_eq!(fee.maker, expected.maker_fee);
        assert_eq!(fee.taker, expected.taker_fee);

        let config = adapter.get_symbol_account_config(symbol).await.unwrap();
        assert_eq!(config.exchange, symbol.exchange);
        assert_eq!(config.exchange_symbol, *symbol);
        assert_eq!(config.position_mode, expected.position_mode);
        assert_eq!(
            config.margin_mode, expected.margin_mode,
            "margin mode mismatch for {:?}",
            symbol.exchange
        );
        assert_eq!(config.leverage, expected.leverage);
        assert_eq!(config.max_leverage, expected.max_leverage);
    }

    #[derive(Debug, Clone, Copy)]
    struct AccountFillReadbackExpectations {
        trade_id: &'static str,
        exchange_order_id: Option<&'static str>,
        fill_side: OrderSide,
        fill_position_side: PositionSide,
        fill_liquidity: FillLiquidity,
        fill_price: f64,
        fill_quantity: f64,
        fill_quote_quantity: f64,
        fill_fee: Option<f64>,
        fill_realized_pnl: Option<f64>,
        position_side: PositionSide,
        position_quantity: f64,
        position_entry_price: Option<f64>,
        position_mark_price: Option<f64>,
        position_unrealized_pnl: Option<f64>,
        balance_total: f64,
        balance_available: f64,
        balance_locked: f64,
        maker_fee: f64,
        taker_fee: f64,
        position_mode: Option<PositionMode>,
        margin_mode: Option<MarginMode>,
        leverage: Option<u32>,
        max_leverage: Option<u32>,
    }

    fn assert_seen_paths(seen: &Arc<Mutex<Vec<PrivateRestRequestSpec>>>, expected_paths: &[&str]) {
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), expected_paths.len());
        for (request, expected_path) in seen.iter().zip(expected_paths) {
            assert_eq!(request.path, *expected_path);
        }
    }

    async fn assert_private_perp_order_history<P>(
        adapter: &PrivatePerpTradingAdapter<P, MockTransport>,
        exchange: ExchangeId,
        symbol: &str,
        expected_exchange_order_id: &str,
        expected_client_order_id: Option<&str>,
        expected: (OrderSide, PositionSide, OrderType, OrderCommandStatus),
        quantity: f64,
        filled_quantity: f64,
        price: f64,
        average_fill_price: Option<f64>,
    ) where
        P: PrivatePerpProtocol + Send + Sync + Clone,
    {
        let orders = adapter
            .get_order_history(OrderHistoryQuery::for_symbol(
                exchange.clone(),
                ExchangeSymbol::new(exchange.clone(), symbol),
            ))
            .await
            .unwrap();

        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].exchange, exchange);
        assert_eq!(
            orders[0].exchange_order_id.as_deref(),
            Some(expected_exchange_order_id)
        );
        assert_eq!(
            orders[0].client_order_id.as_deref(),
            expected_client_order_id
        );
        assert_single_order_fields(
            &orders[0],
            expected,
            quantity,
            filled_quantity,
            price,
            average_fill_price,
        );
    }

    fn okx_order_history_response() -> Value {
        json!({
            "code": "0",
            "msg": "",
            "data": [
                {
                    "instId": "BTC-USDT-SWAP",
                    "ordId": "order-1",
                    "clOrdId": "client-1",
                    "side": "buy",
                    "posSide": "long",
                    "ordType": "limit",
                    "state": "filled",
                    "px": "65000",
                    "sz": "0.02",
                    "accFillSz": "0.02",
                    "avgPx": "64950",
                    "uTime": "1700000000000"
                }
            ]
        })
    }

    fn bitget_order_history_response() -> Value {
        json!({
            "code": "00000",
            "msg": "success",
            "data": {
                "entrustedList": [
                    {
                        "symbol": "BTCUSDT",
                        "orderId": "order-1",
                        "clientOid": "client-1",
                        "side": "buy",
                        "tradeSide": "open",
                        "orderType": "limit",
                        "size": "0.01",
                        "price": "65000",
                        "accBaseVolume": "0.004",
                        "priceAvg": "64950",
                        "status": "partial_filled",
                        "reduceOnly": "NO",
                        "uTime": "1700000000123"
                    }
                ],
                "endId": "order-1"
            }
        })
    }

    fn gate_order_history_response() -> Value {
        json!([
            {
                "id": "gate-order-1",
                "contract": "BTC_USDT",
                "size": "25",
                "left": "5",
                "price": "65000",
                "fill_price": "64950",
                "status": "finished",
                "finish_as": "cancelled",
                "text": "t-client-1",
                "is_reduce_only": false,
                "create_time_ms": "1700000000000"
            }
        ])
    }

    fn bybit_order_history_response() -> Value {
        json!({
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": [
                    {
                        "orderId": "order-1",
                        "orderLinkId": "client-1",
                        "symbol": "BTCUSDT",
                        "price": "26864.40",
                        "qty": "0.003",
                        "side": "Buy",
                        "positionIdx": 1,
                        "orderStatus": "PartiallyFilled",
                        "avgPrice": "26864.4",
                        "cumExecQty": "0.001",
                        "timeInForce": "PostOnly",
                        "orderType": "Limit",
                        "reduceOnly": false,
                        "createdTime": "1684476068369",
                        "updatedTime": "1684476068372"
                    }
                ],
                "category": "linear"
            },
            "retExtInfo": {},
            "time": 1684766282976_i64
        })
    }

    fn mexc_order_history_response() -> Value {
        json!({
            "success": true,
            "code": 0,
            "data": [
                {
                    "orderId": "102015012431820288",
                    "symbol": "BTC_USDT",
                    "price": "65000",
                    "vol": "2",
                    "side": 1,
                    "category": 1,
                    "orderType": 1,
                    "dealAvgPrice": "64990",
                    "dealVol": "1",
                    "state": 2,
                    "externalOid": "client-1",
                    "createTime": 1700000000000_i64,
                    "updateTime": 1700000001000_i64
                }
            ]
        })
    }

    fn htx_order_history_response() -> Value {
        json!({
            "code": 200,
            "msg": "",
            "data": [
                {
                    "query_id": 13580806498_i64,
                    "order_id": 918800256249405440_i64,
                    "order_id_str": "918800256249405440",
                    "contract_code": "BTC-USDT",
                    "symbol": "BTC",
                    "lever_rate": 5,
                    "direction": "buy",
                    "offset": "open",
                    "volume": "100",
                    "price": "48555.6",
                    "create_date": 1639100651569_i64,
                    "update_time": 1639100651000_i64,
                    "order_price_type": "limit",
                    "order_type": 1,
                    "trade_volume": "25",
                    "trade_avg_price": "48550",
                    "status": 4,
                    "margin_mode": "cross",
                    "margin_account": "USDT",
                    "reduce_only": 0
                }
            ],
            "ts": 1604312615051_i64
        })
    }

    fn okx_account_and_fill_readback_responses() -> [Value; 5] {
        [
            json!({
                "code": "0",
                "msg": "",
                "data": [
                    {
                        "instId": "BTC-USDT-SWAP",
                        "tradeId": "trade-1",
                        "ordId": "order-1",
                        "clOrdId": "client-1",
                        "side": "sell",
                        "posSide": "short",
                        "fillPx": "65000",
                        "fillSz": "0.02",
                        "fillNotionalUsd": "1300",
                        "fee": "-0.65",
                        "feeCcy": "USDT",
                        "fillPnl": "12.5",
                        "fillRole": "taker",
                        "fillTime": "1700000000000"
                    }
                ]
            }),
            json!({
                "code": "0",
                "msg": "",
                "data": [
                    {
                        "instId": "BTC-USDT-SWAP",
                        "posSide": "long",
                        "pos": "0.5",
                        "avgPx": "65000",
                        "markPx": "65100",
                        "upl": "50",
                        "uTime": "1700000000000"
                    }
                ]
            }),
            json!({
                "code": "0",
                "msg": "",
                "data": [
                    {
                        "details": [
                            {
                                "ccy": "USDT",
                                "eq": "1000",
                                "availEq": "850",
                                "uTime": "1700000000000"
                            }
                        ]
                    }
                ]
            }),
            json!({
                "code": "0",
                "msg": "",
                "data": [
                    {
                        "instId": "BTC-USDT-SWAP",
                        "maker": "-0.0001",
                        "taker": "0.0005"
                    }
                ]
            }),
            json!({
                "code": "0",
                "msg": "",
                "data": [
                    {
                        "instId": "BTC-USDT-SWAP",
                        "posMode": "long_short_mode",
                        "mgnMode": "cross",
                        "lever": "10",
                        "maxLever": "125"
                    }
                ]
            }),
        ]
    }

    fn bitget_account_and_fill_readback_responses() -> [Value; 5] {
        [
            json!({
                "code": "00000",
                "msg": "success",
                "data": {
                    "fillList": [
                        {
                            "symbol": "BTCUSDT",
                            "tradeId": "trade-1",
                            "orderId": "order-1",
                            "clientOid": "client-1",
                            "side": "sell",
                            "tradeSide": "close",
                            "fillPrice": "65000",
                            "fillSize": "0.02",
                            "quoteVolume": "1300",
                            "fee": "0.65",
                            "feeCcy": "USDT",
                            "profit": "12.5",
                            "role": "taker",
                            "tradeTime": "1700000000000"
                        }
                    ],
                    "endId": "trade-1"
                }
            }),
            json!({
                "code": "00000",
                "msg": "success",
                "data": [
                    {
                        "symbol": "BTCUSDT",
                        "holdSide": "long",
                        "total": "0.5",
                        "openPriceAvg": "65000",
                        "markPrice": "65100",
                        "unrealizedPL": "50",
                        "uTime": "1700000000000"
                    }
                ]
            }),
            json!({
                "code": "00000",
                "msg": "success",
                "data": [
                    {
                        "marginCoin": "USDT",
                        "equity": "1000",
                        "available": "850",
                        "locked": "150"
                    }
                ]
            }),
            json!({
                "code": "00000",
                "msg": "success",
                "data": [
                    {
                        "symbol": "BTCUSDT",
                        "makerFeeRate": "0.0002",
                        "takerFeeRate": "0.0006"
                    }
                ]
            }),
            json!({
                "code": "00000",
                "msg": "success",
                "data": {
                    "symbol": "BTCUSDT",
                    "posMode": "hedge_mode",
                    "marginMode": "crossed",
                    "crossedMarginLeverage": "3",
                    "maxLever": "125"
                }
            }),
        ]
    }

    fn gate_account_and_fill_readback_responses() -> [Value; 5] {
        [
            json!({
                "result": [
                    {
                        "id": "trade-1",
                        "order_id": "gate-order-1",
                        "contract": "BTC_USDT",
                        "size": "25",
                        "price": "65000",
                        "fee": "0.1",
                        "fee_currency": "USDT",
                        "role": "maker",
                        "create_time_ms": "1700000000000"
                    }
                ]
            }),
            json!({
                "result": [
                    {
                        "contract": "BTC_USDT",
                        "size": "-25",
                        "entry_price": "64000",
                        "mark_price": "65000",
                        "unrealised_pnl": "2.5",
                        "update_time_ms": "1700000000000"
                    }
                ]
            }),
            json!({
                "currency": "USDT",
                "total": "1000",
                "available": "850",
                "order_margin": "150"
            }),
            json!({
                "name": "BTC_USDT",
                "maker_fee_rate": "-0.00025",
                "taker_fee_rate": "0.00075"
            }),
            json!({
                "contract": "BTC_USDT",
                "marginMode": "cross",
                "leverage": "10",
                "max_leverage": "100"
            }),
        ]
    }

    fn bybit_account_and_fill_readback_responses() -> [Value; 5] {
        [
            json!({
                "retCode": 0,
                "retMsg": "OK",
                "result": {
                    "list": [
                        {
                            "symbol": "BTCUSDT",
                            "execId": "exec-1",
                            "orderId": "order-1",
                            "orderLinkId": "client-1",
                            "side": "Sell",
                            "positionIdx": 2,
                            "execPrice": "65000",
                            "execQty": "0.02",
                            "execValue": "1300",
                            "execFee": "0.65",
                            "feeCurrency": "USDT",
                            "feeRate": "0.0005",
                            "execPnl": "12.5",
                            "isMaker": false,
                            "execTime": "1700000000000"
                        }
                    ]
                },
                "retExtInfo": {},
                "time": 1700000000001_i64
            }),
            json!({
                "retCode": 0,
                "retMsg": "OK",
                "result": {
                    "list": [
                        {
                            "symbol": "BTCUSDT",
                            "side": "Buy",
                            "size": "0.5",
                            "avgPrice": "65000",
                            "markPrice": "65100",
                            "unrealisedPnl": "50",
                            "updatedTime": "1700000000000"
                        }
                    ]
                }
            }),
            json!({
                "retCode": 0,
                "retMsg": "OK",
                "result": {
                    "list": [
                        {
                            "accountType": "UNIFIED",
                            "coin": [
                                {
                                    "coin": "USDT",
                                    "walletBalance": "1000",
                                    "availableToWithdraw": "850",
                                    "locked": "150"
                                }
                            ]
                        }
                    ]
                }
            }),
            json!({
                "retCode": 0,
                "retMsg": "OK",
                "result": {
                    "list": [
                        {
                            "symbol": "BTCUSDT",
                            "makerFeeRate": "0.0001",
                            "takerFeeRate": "0.00055"
                        }
                    ]
                }
            }),
            json!({
                "retCode": 0,
                "retMsg": "OK",
                "result": {
                    "list": [
                        {
                            "symbol": "BTCUSDT",
                            "tradeMode": 0,
                            "leverage": "10",
                            "maxLeverage": "100"
                        }
                    ]
                }
            }),
        ]
    }

    fn mexc_account_and_fill_readback_responses() -> [Value; 5] {
        [
            json!({
                "success": true,
                "code": 0,
                "data": [
                    {
                        "symbol":"BTC_USDT",
                        "tradeId":"trade-1",
                        "orderId":"order-1",
                        "externalOid":"client-1",
                        "side": 1,
                        "price":"65000",
                        "vol":"0.02",
                        "amount":"1300",
                        "fee":"0.65",
                        "feeCurrency":"USDT",
                        "role":"maker",
                        "profit":"1.5",
                        "time":1700000000000_i64
                    }
                ]
            }),
            json!({
                "success": true,
                "code": 0,
                "data": [
                    {
                        "symbol":"BTC_USDT",
                        "positionType":"short",
                        "holdVol":"2",
                        "holdAvgPrice":"65000",
                        "markPrice":"64900",
                        "unrealised":"12.5"
                    }
                ]
            }),
            json!({
                "success": true,
                "code": 0,
                "data": [
                    {
                        "currency":"USDT",
                        "equity":"1000",
                        "availableBalance":"850",
                        "frozenBalance":"150"
                    }
                ]
            }),
            json!({
                "success": true,
                "code": 0,
                "data": [
                    {"symbol":"BTC_USDT","makerFee":"0.0001","takerFee":"0.0005"}
                ]
            }),
            json!({
                "success": true,
                "code": 0,
                "data": [
                    {
                        "symbol":"BTC_USDT",
                        "positionMode":"long_short_mode",
                        "marginMode":"isolated",
                        "longLeverage":10,
                        "maxLeverage":"200"
                    }
                ]
            }),
        ]
    }

    fn htx_account_and_fill_readback_responses() -> [Value; 5] {
        [
            json!({
                "status": "ok",
                "data": [
                    {
                        "contract_code":"BTC-USDT",
                        "trade_id":"trade-1",
                        "order_id":"order-1",
                        "client_order_id":"client-1",
                        "direction":"sell",
                        "offset":"close",
                        "trade_price":"64800",
                        "trade_volume":"3",
                        "trade_fee":"1.2",
                        "fee_asset":"USDT",
                        "role":"taker",
                        "profit":"21.5",
                        "created_at":1700000000000_i64
                    }
                ]
            }),
            json!({
                "status": "ok",
                "data": [
                    {
                        "contract_code":"BTC-USDT",
                        "direction":"sell",
                        "volume":"3",
                        "cost_open":"65000",
                        "last_price":"64800",
                        "profit_unreal":"21.5"
                    }
                ]
            }),
            json!({
                "status": "ok",
                "data": [
                    {
                        "margin_asset":"USDT",
                        "margin_balance":"1200",
                        "withdraw_available":"900",
                        "margin_frozen":"300"
                    }
                ]
            }),
            json!({
                "status": "ok",
                "data": [
                    {"contract_code":"BTC-USDT","open_maker_fee":"0.0002","open_taker_fee":"0.0006"}
                ]
            }),
            json!({
                "status": "ok",
                "data": [
                    {
                        "contract_code":"BTC-USDT",
                        "position_mode":"dual",
                        "margin_mode":"cross",
                        "lever_rate":5
                    }
                ]
            }),
        ]
    }

    #[tokio::test]
    async fn non_binance_adapters_should_route_account_and_fill_readbacks() {
        let transport = SequentialMockTransport::new(okx_account_and_fill_readback_responses());
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(OkxPrivatePerpProtocol, transport)
            .with_position_mode(PositionMode::OneWay);
        let symbol = ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP");
        assert_private_perp_account_and_fill_readbacks(
            &adapter,
            &symbol,
            Some("order-1"),
            Some("client-1"),
            AccountFillReadbackExpectations {
                trade_id: "trade-1",
                exchange_order_id: Some("order-1"),
                fill_side: OrderSide::Sell,
                fill_position_side: PositionSide::Short,
                fill_liquidity: FillLiquidity::Taker,
                fill_price: 65_000.0,
                fill_quantity: 0.02,
                fill_quote_quantity: 1300.0,
                fill_fee: Some(0.65),
                fill_realized_pnl: Some(12.5),
                position_side: PositionSide::Long,
                position_quantity: 0.5,
                position_entry_price: Some(65_000.0),
                position_mark_price: Some(65_100.0),
                position_unrealized_pnl: Some(50.0),
                balance_total: 1000.0,
                balance_available: 850.0,
                balance_locked: 150.0,
                maker_fee: -0.0001,
                taker_fee: 0.0005,
                position_mode: Some(PositionMode::Hedge),
                margin_mode: Some(MarginMode::Cross),
                leverage: Some(10),
                max_leverage: Some(125),
            },
        )
        .await;
        assert_seen_paths(
            &seen,
            &[
                "/api/v5/trade/fills-history",
                "/api/v5/account/positions",
                "/api/v5/account/balance",
                "/api/v5/account/trade-fee",
                "/api/v5/account/leverage-info",
            ],
        );

        let transport = SequentialMockTransport::new(bitget_account_and_fill_readback_responses());
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport)
            .with_position_mode(PositionMode::OneWay);
        let symbol = ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT");
        assert_private_perp_account_and_fill_readbacks(
            &adapter,
            &symbol,
            Some("order-1"),
            Some("client-1"),
            AccountFillReadbackExpectations {
                trade_id: "trade-1",
                exchange_order_id: Some("order-1"),
                fill_side: OrderSide::Sell,
                fill_position_side: PositionSide::Long,
                fill_liquidity: FillLiquidity::Taker,
                fill_price: 65_000.0,
                fill_quantity: 0.02,
                fill_quote_quantity: 1300.0,
                fill_fee: Some(0.65),
                fill_realized_pnl: Some(12.5),
                position_side: PositionSide::Long,
                position_quantity: 0.5,
                position_entry_price: Some(65_000.0),
                position_mark_price: Some(65_100.0),
                position_unrealized_pnl: Some(50.0),
                balance_total: 1000.0,
                balance_available: 850.0,
                balance_locked: 150.0,
                maker_fee: 0.0002,
                taker_fee: 0.0006,
                position_mode: Some(PositionMode::Hedge),
                margin_mode: Some(MarginMode::Cross),
                leverage: Some(3),
                max_leverage: Some(125),
            },
        )
        .await;
        assert_seen_paths(
            &seen,
            &[
                "/api/v2/mix/order/fills",
                "/api/v2/mix/position/all-position",
                "/api/v2/mix/account/accounts",
                "/api/v2/mix/market/contracts",
                "/api/v2/mix/account/account",
            ],
        );

        let transport = SequentialMockTransport::new(gate_account_and_fill_readback_responses());
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(GatePrivatePerpProtocol, transport)
            .with_position_mode(PositionMode::OneWay)
            .with_instruments([gate_contract_instrument(0.0001)]);
        let symbol = ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT");
        assert_private_perp_account_and_fill_readbacks(
            &adapter,
            &symbol,
            Some("gate-order-1"),
            None,
            AccountFillReadbackExpectations {
                trade_id: "trade-1",
                exchange_order_id: Some("gate-order-1"),
                fill_side: OrderSide::Buy,
                fill_position_side: PositionSide::Long,
                fill_liquidity: FillLiquidity::Maker,
                fill_price: 65_000.0,
                fill_quantity: 0.0025,
                fill_quote_quantity: 162.5,
                fill_fee: Some(0.1),
                fill_realized_pnl: None,
                position_side: PositionSide::Short,
                position_quantity: 0.0025,
                position_entry_price: Some(64_000.0),
                position_mark_price: Some(65_000.0),
                position_unrealized_pnl: Some(2.5),
                balance_total: 1000.0,
                balance_available: 850.0,
                balance_locked: 150.0,
                maker_fee: -0.00025,
                taker_fee: 0.00075,
                position_mode: Some(PositionMode::OneWay),
                margin_mode: Some(MarginMode::Cross),
                leverage: Some(10),
                max_leverage: Some(100),
            },
        )
        .await;
        assert_seen_paths(
            &seen,
            &[
                "/futures/usdt/my_trades",
                "/futures/usdt/positions/BTC_USDT",
                "/futures/usdt/accounts",
                "/futures/usdt/contracts/BTC_USDT",
                "/futures/usdt/positions/BTC_USDT",
            ],
        );

        let transport = SequentialMockTransport::new(bybit_account_and_fill_readback_responses());
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(BybitPrivatePerpProtocol, transport)
            .with_position_mode(PositionMode::OneWay);
        let symbol = ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT");
        assert_private_perp_account_and_fill_readbacks(
            &adapter,
            &symbol,
            Some("order-1"),
            Some("client-1"),
            AccountFillReadbackExpectations {
                trade_id: "exec-1",
                exchange_order_id: Some("order-1"),
                fill_side: OrderSide::Sell,
                fill_position_side: PositionSide::Short,
                fill_liquidity: FillLiquidity::Taker,
                fill_price: 65_000.0,
                fill_quantity: 0.02,
                fill_quote_quantity: 1300.0,
                fill_fee: Some(0.65),
                fill_realized_pnl: Some(12.5),
                position_side: PositionSide::Long,
                position_quantity: 0.5,
                position_entry_price: Some(65_000.0),
                position_mark_price: Some(65_100.0),
                position_unrealized_pnl: Some(50.0),
                balance_total: 1000.0,
                balance_available: 850.0,
                balance_locked: 150.0,
                maker_fee: 0.0001,
                taker_fee: 0.00055,
                position_mode: Some(PositionMode::OneWay),
                margin_mode: None,
                leverage: Some(10),
                max_leverage: Some(100),
            },
        )
        .await;
        assert_seen_paths(
            &seen,
            &[
                "/v5/execution/list",
                "/v5/position/list",
                "/v5/account/wallet-balance",
                "/v5/account/fee-rate",
                "/v5/position/list",
            ],
        );

        let transport = SequentialMockTransport::new(mexc_account_and_fill_readback_responses());
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(MexcPrivatePerpProtocol, transport)
            .with_position_mode(PositionMode::OneWay);
        let symbol = ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT");
        assert_private_perp_account_and_fill_readbacks(
            &adapter,
            &symbol,
            Some("order-1"),
            Some("client-1"),
            AccountFillReadbackExpectations {
                trade_id: "trade-1",
                exchange_order_id: Some("order-1"),
                fill_side: OrderSide::Buy,
                fill_position_side: PositionSide::Long,
                fill_liquidity: FillLiquidity::Maker,
                fill_price: 65_000.0,
                fill_quantity: 0.02,
                fill_quote_quantity: 1300.0,
                fill_fee: Some(0.65),
                fill_realized_pnl: Some(1.5),
                position_side: PositionSide::Short,
                position_quantity: 2.0,
                position_entry_price: Some(65_000.0),
                position_mark_price: Some(64_900.0),
                position_unrealized_pnl: Some(12.5),
                balance_total: 1000.0,
                balance_available: 850.0,
                balance_locked: 150.0,
                maker_fee: 0.0001,
                taker_fee: 0.0005,
                position_mode: Some(PositionMode::Hedge),
                margin_mode: Some(MarginMode::Isolated),
                leverage: Some(10),
                max_leverage: Some(200),
            },
        )
        .await;
        assert_seen_paths(
            &seen,
            &[
                "/api/v1/private/order/list/order_deals/BTC_USDT",
                "/api/v1/private/position/open_positions",
                "/api/v1/private/account/assets",
                "/api/v1/private/account/tiered_fee_rate",
                "/api/v1/private/position/leverage",
            ],
        );

        let transport = SequentialMockTransport::new(htx_account_and_fill_readback_responses());
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(HtxPrivatePerpProtocol, transport)
            .with_position_mode(PositionMode::OneWay);
        let symbol = ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT");
        assert_private_perp_account_and_fill_readbacks(
            &adapter,
            &symbol,
            None,
            Some("client-1"),
            AccountFillReadbackExpectations {
                trade_id: "trade-1",
                exchange_order_id: Some("order-1"),
                fill_side: OrderSide::Sell,
                fill_position_side: PositionSide::Long,
                fill_liquidity: FillLiquidity::Taker,
                fill_price: 64_800.0,
                fill_quantity: 3.0,
                fill_quote_quantity: 194_400.0,
                fill_fee: Some(1.2),
                fill_realized_pnl: Some(21.5),
                position_side: PositionSide::Short,
                position_quantity: 3.0,
                position_entry_price: Some(65_000.0),
                position_mark_price: Some(64_800.0),
                position_unrealized_pnl: Some(21.5),
                balance_total: 1200.0,
                balance_available: 900.0,
                balance_locked: 300.0,
                maker_fee: 0.0002,
                taker_fee: 0.0006,
                position_mode: Some(PositionMode::Hedge),
                margin_mode: Some(MarginMode::Cross),
                leverage: Some(5),
                max_leverage: None,
            },
        )
        .await;
        assert_seen_paths(
            &seen,
            &[
                "/linear-swap-api/v1/swap_cross_matchresults",
                "/linear-swap-api/v1/swap_cross_position_info",
                "/linear-swap-api/v1/swap_cross_account_info",
                "/linear-swap-api/v1/swap_fee",
                "/linear-swap-api/v1/swap_cross_position_info",
            ],
        );
    }

    #[tokio::test]
    async fn non_binance_adapters_should_parse_order_history() {
        let transport = MockTransport::new(okx_order_history_response());
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(OkxPrivatePerpProtocol, transport);
        assert_private_perp_order_history(
            &adapter,
            ExchangeId::Okx,
            "BTC-USDT-SWAP",
            "order-1",
            Some("client-1"),
            (
                OrderSide::Buy,
                PositionSide::Long,
                OrderType::Limit,
                OrderCommandStatus::Filled,
            ),
            0.02,
            0.02,
            65_000.0,
            Some(64_950.0),
        )
        .await;
        assert_seen_paths(&seen, &["/api/v5/trade/orders-history"]);

        let transport = MockTransport::new(bitget_order_history_response());
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport);
        assert_private_perp_order_history(
            &adapter,
            ExchangeId::Bitget,
            "BTCUSDT",
            "order-1",
            Some("client-1"),
            (
                OrderSide::Buy,
                PositionSide::Long,
                OrderType::Limit,
                OrderCommandStatus::PartiallyFilled,
            ),
            0.01,
            0.004,
            65_000.0,
            Some(64_950.0),
        )
        .await;
        assert_seen_paths(&seen, &["/api/v2/mix/order/orders-history"]);

        let transport = MockTransport::new(gate_order_history_response());
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(GatePrivatePerpProtocol, transport)
            .with_instruments([gate_contract_instrument(0.0001)]);
        assert_private_perp_order_history(
            &adapter,
            ExchangeId::Gate,
            "BTC_USDT",
            "gate-order-1",
            Some("client-1"),
            (
                OrderSide::Buy,
                PositionSide::Long,
                OrderType::Limit,
                OrderCommandStatus::Cancelled,
            ),
            0.0025,
            0.002,
            65_000.0,
            Some(64_950.0),
        )
        .await;
        assert_seen_paths(&seen, &["/futures/usdt/orders"]);

        let transport = MockTransport::new(bybit_order_history_response());
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(BybitPrivatePerpProtocol, transport);
        assert_private_perp_order_history(
            &adapter,
            ExchangeId::Bybit,
            "BTCUSDT",
            "order-1",
            Some("client-1"),
            (
                OrderSide::Buy,
                PositionSide::Long,
                OrderType::Limit,
                OrderCommandStatus::PartiallyFilled,
            ),
            0.003,
            0.001,
            26_864.4,
            Some(26_864.4),
        )
        .await;
        assert_seen_paths(&seen, &["/v5/order/history"]);

        let transport = MockTransport::new(mexc_order_history_response());
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(MexcPrivatePerpProtocol, transport);
        assert_private_perp_order_history(
            &adapter,
            ExchangeId::Mexc,
            "BTC_USDT",
            "102015012431820288",
            Some("client-1"),
            (
                OrderSide::Buy,
                PositionSide::Long,
                OrderType::Limit,
                OrderCommandStatus::PartiallyFilled,
            ),
            2.0,
            1.0,
            65_000.0,
            Some(64_990.0),
        )
        .await;
        assert_seen_paths(&seen, &["/api/v1/private/order/list/history_orders"]);

        let transport = MockTransport::new(htx_order_history_response());
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(HtxPrivatePerpProtocol, transport);
        assert_private_perp_order_history(
            &adapter,
            ExchangeId::Htx,
            "BTC-USDT",
            "918800256249405440",
            None,
            (
                OrderSide::Buy,
                PositionSide::Long,
                OrderType::Limit,
                OrderCommandStatus::PartiallyFilled,
            ),
            100.0,
            25.0,
            48_555.6,
            Some(48_550.0),
        )
        .await;
        assert_seen_paths(&seen, &["/linear-swap-api/v3/swap_cross_hisorders"]);
    }

    #[test]
    fn binance_signed_request_should_append_timestamp_recv_window_and_signature() {
        let transport = ReqwestPrivateRestTransport::new(
            PrivatePerpExchange::Binance,
            PrivateRestAuth {
                api_key: "key".to_string(),
                api_secret: "secret".to_string(),
                passphrase: None,
                demo_trading: false,
            },
        )
        .unwrap();
        let spec = BinancePrivatePerpProtocol
            .get_open_orders(Some(&ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT")))
            .unwrap();

        let signed = transport.signed_request(spec, 1_700_000_000_000).unwrap();
        let headers = transport
            .signed_headers(&signed, 1_700_000_000_000)
            .unwrap();

        assert_eq!(
            signed.query.get("timestamp").map(String::as_str),
            Some("1700000000000")
        );
        assert_eq!(
            signed.query.get("recvWindow").map(String::as_str),
            Some("5000")
        );
        assert!(signed
            .query
            .get("signature")
            .is_some_and(|value| !value.is_empty()));
        assert_eq!(headers.get("X-MBX-APIKEY").map(String::as_str), Some("key"));
    }

    #[test]
    fn reqwest_transport_should_build_okx_headers_with_passphrase_and_demo_flag() {
        let auth = PrivateRestAuth {
            api_key: "key".to_string(),
            api_secret: "secret".to_string(),
            passphrase: Some("pass".to_string()),
            demo_trading: true,
        };
        let transport = ReqwestPrivateRestTransport::new(PrivatePerpExchange::Okx, auth).unwrap();
        let spec = OkxPrivatePerpProtocol
            .get_open_orders(Some(&ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP")))
            .unwrap();

        let headers = transport.signed_headers(&spec, 1_700_000_000_000).unwrap();

        assert_eq!(
            headers.get("OK-ACCESS-KEY").map(String::as_str),
            Some("key")
        );
        assert_eq!(
            headers.get("OK-ACCESS-PASSPHRASE").map(String::as_str),
            Some("pass")
        );
        assert_eq!(
            headers.get("OK-ACCESS-TIMESTAMP").map(String::as_str),
            Some("2023-11-14T22:13:20.000Z")
        );
        assert_eq!(
            headers.get("x-simulated-trading").map(String::as_str),
            Some("1")
        );
        assert!(headers
            .get("OK-ACCESS-SIGN")
            .is_some_and(|value| !value.is_empty()));
    }

    #[test]
    fn reqwest_transport_should_reject_okx_headers_without_passphrase() {
        let transport = ReqwestPrivateRestTransport::new(
            PrivatePerpExchange::Okx,
            PrivateRestAuth {
                api_key: "key".to_string(),
                api_secret: "secret".to_string(),
                passphrase: None,
                demo_trading: false,
            },
        )
        .unwrap();
        let spec = OkxPrivatePerpProtocol
            .get_open_orders(Some(&ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP")))
            .unwrap();

        let err = transport
            .signed_headers(&spec, 1_700_000_000_000)
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("okx private REST requires passphrase"));
    }

    #[test]
    fn binance_should_parse_rest_order_fill_position_and_balance() {
        let now = Utc::now();
        let symbol = ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT");
        let order = parse_rest_order_with_gate_contract_size(
            ExchangeId::Binance,
            &symbol,
            &json!({
                "symbol":"BTCUSDT","orderId":12345,"clientOrderId":"client-1",
                "side":"BUY","positionSide":"LONG","type":"LIMIT","origQty":"0.01",
                "price":"65000","executedQty":"0.005","avgPrice":"65001",
                "timeInForce":"GTX","status":"PARTIALLY_FILLED","updateTime":1700000000000_i64
            }),
            now,
            None,
        )
        .unwrap();
        assert_eq!(order.status, OrderCommandStatus::PartiallyFilled);
        assert_eq!(order.time_in_force, TimeInForce::PostOnly);
        assert_eq!(order.position_side, PositionSide::Long);

        let fill = parse_binance_fill(
            &json!({
                "symbol":"BTCUSDT","id":698759,"orderId":12345,"side":"SELL",
                "price":"7819.01","qty":"0.002","quoteQty":"15.63802",
                "commission":"0.07819010","commissionAsset":"USDT",
                "realizedPnl":"-0.91539999","positionSide":"SHORT",
                "maker":false,"time":1569514978020_i64
            }),
            now,
        )
        .and_then(event_fill)
        .unwrap();
        assert_eq!(fill.exchange, ExchangeId::Binance);
        assert_eq!(fill.side, OrderSide::Sell);
        assert_eq!(fill.position_side, PositionSide::Short);
        assert_eq!(fill.liquidity, FillLiquidity::Taker);

        let position = parse_binance_position(
            &json!({
                "symbol":"BTCUSDT","positionAmt":"-0.003",
                "entryPrice":"64000","markPrice":"65000",
                "unRealizedProfit":"3","positionSide":"BOTH",
                "updateTime":1700000000000_i64
            }),
            now,
        )
        .and_then(event_position)
        .unwrap();
        assert_eq!(position.position_side, PositionSide::Short);
        assert_eq!(position.quantity, 0.003);

        let balance = parse_binance_balance(
            &json!({
                "asset":"USDT","balance":"100.5","availableBalance":"80.0",
                "updateTime":1700000000000_i64
            }),
            now,
        )
        .and_then(event_balance)
        .unwrap();
        assert_eq!(balance.total, 100.5);
        assert_eq!(balance.available, 80.0);
    }

    #[test]
    fn okx_should_build_full_private_rest_specs() {
        let cmd = command(ExchangeId::Okx, "BTC-USDT-SWAP");
        let place = OkxPrivatePerpProtocol
            .place_order(&cmd, PositionMode::Hedge)
            .unwrap();
        assert_eq!(place.method, PrivateRestMethod::Post);
        assert_eq!(place.path, "/api/v5/trade/order");
        let body = place.body.unwrap();
        assert_eq!(body["instId"], "BTC-USDT-SWAP");
        assert_eq!(body["tdMode"], "cross");
        assert_eq!(body["side"], "buy");
        assert_eq!(body["ordType"], "post_only");
        assert_eq!(body["posSide"], "long");
        assert_eq!(body["clOrdId"], "crossarb-ls-mk-1-c0b4e763");

        let mut batch_first = command(ExchangeId::Okx, "BTC-USDT-SWAP");
        batch_first.client_order_id = "client-1".to_string();
        let mut batch_second = command(ExchangeId::Okx, "ETH-USDT-SWAP");
        batch_second.client_order_id = "client-2".to_string();
        batch_second.side = OrderSide::Sell;
        batch_second.position_side = PositionSide::Short;
        let batch = OkxPrivatePerpProtocol
            .place_batch_orders(
                &BatchPlaceCommand::new(
                    ExchangeId::Okx,
                    [batch_first.clone(), batch_second],
                    Utc::now(),
                ),
                PositionMode::Hedge,
            )
            .unwrap();
        assert_eq!(batch.path, "/api/v5/trade/batch-orders");
        let batch_body = batch.body.unwrap().as_array().unwrap().clone();
        assert_eq!(batch_body.len(), 2);
        assert_eq!(batch_body[0]["clOrdId"], "client-1");
        assert_eq!(batch_body[1]["side"], "sell");
        assert_eq!(batch_body[1]["posSide"], "short");

        let cancel = OkxPrivatePerpProtocol
            .cancel_order(&CancelCommand {
                exchange: ExchangeId::Okx,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: Some("order-1".to_string()),
                reason: None,
                requested_at: Utc::now(),
            })
            .unwrap();
        assert_eq!(cancel.path, "/api/v5/trade/cancel-order");
        let cancel_body = cancel.body.unwrap();
        assert_eq!(cancel_body["ordId"], "order-1");
        assert_eq!(cancel_body["clOrdId"], "client-1");

        let history = OrderHistoryQuery::for_symbol(
            ExchangeId::Okx,
            ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
        );
        let history_spec = OkxPrivatePerpProtocol.get_all_orders(&history).unwrap();
        assert_eq!(history_spec.path, "/api/v5/trade/orders-history");
        assert_eq!(
            history_spec.query.get("instType").map(String::as_str),
            Some("SWAP")
        );

        let mut fills = FillQuery::for_symbol(
            ExchangeId::Okx,
            CanonicalSymbol::new("BTC", "USDT"),
            ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
        );
        fills.exchange_order_id = Some("order-1".to_string());
        fills.limit = Some(100);
        let fills_spec = OkxPrivatePerpProtocol.get_fills(&fills).unwrap();
        assert_eq!(fills_spec.path, "/api/v5/trade/fills-history");
        assert_eq!(
            fills_spec.query.get("ordId").map(String::as_str),
            Some("order-1")
        );

        let amend = OkxPrivatePerpProtocol
            .amend_order(&AmendOrderCommand {
                exchange: ExchangeId::Okx,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: None,
                original_side: Some(OrderSide::Buy),
                new_client_order_id: None,
                new_quantity: Some(0.02),
                new_price: Some(64_900.0),
                requested_at: Utc::now(),
            })
            .unwrap();
        assert_eq!(amend.path, "/api/v5/trade/amend-order");
        let amend_body = amend.body.unwrap();
        assert_eq!(amend_body["clOrdId"], "client-1");
        assert_eq!(amend_body["newSz"], "0.02");
        assert_eq!(amend_body["newPx"], "64900");

        assert_eq!(
            OkxPrivatePerpProtocol
                .get_trade_fee(&ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"))
                .unwrap()
                .path,
            "/api/v5/account/trade-fee"
        );
        assert_eq!(
            OkxPrivatePerpProtocol
                .get_symbol_account_config(&ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"))
                .unwrap()
                .path,
            "/api/v5/account/leverage-info"
        );
        assert_eq!(
            OkxPrivatePerpProtocol
                .set_leverage(&LeverageCommand {
                    exchange: ExchangeId::Okx,
                    canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                    exchange_symbol: ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
                    leverage: 5,
                    requested_at: Utc::now(),
                })
                .unwrap()
                .body
                .unwrap()["lever"],
            "5"
        );
        let countdown = OkxPrivatePerpProtocol
            .set_countdown_cancel_all(&CountdownCancelAllCommand::set_for_symbol(
                ExchangeId::Okx,
                ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
                10,
                Utc::now(),
            ))
            .unwrap();
        assert_eq!(countdown.path, "/api/v5/trade/cancel-all-after");
        assert_eq!(countdown.body.unwrap()["timeOut"], "10");
        assert!(OkxPrivatePerpProtocol
            .set_countdown_cancel_all(&CountdownCancelAllCommand::set_for_symbol(
                ExchangeId::Okx,
                ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
                5,
                Utc::now(),
            ))
            .is_err());
    }

    #[test]
    fn okx_private_ws_endpoint_should_login_and_subscribe_all_private_channels() {
        let auth = PrivateWsAuth {
            api_key: "key".to_string(),
            api_secret: "secret".to_string(),
            passphrase: Some("passphrase".to_string()),
            account_id: None,
            demo_trading: false,
        };
        let symbols = [ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP")];

        let endpoint = build_private_ws_endpoint_for(
            OkxPrivatePerpProtocol,
            auth,
            &symbols,
            1_700_000_000,
            "wss://ws.okx.com:8443/ws/v5/private",
        )
        .unwrap();

        assert_eq!(endpoint.exchange, ExchangeId::Okx);
        assert_eq!(endpoint.url, "wss://ws.okx.com:8443/ws/v5/private");

        let login: Value =
            serde_json::from_str(endpoint.login_message.as_deref().unwrap()).unwrap();
        assert_eq!(login["op"], "login");
        assert_eq!(login["args"][0]["apiKey"], "key");
        assert_eq!(login["args"][0]["passphrase"], "passphrase");
        assert_eq!(login["args"][0]["timestamp"], "1700000000");
        assert!(login["args"][0]["sign"]
            .as_str()
            .is_some_and(|sign| !sign.is_empty()));

        assert_eq!(endpoint.subscribe_messages.len(), 3);
        let orders: Value = serde_json::from_str(&endpoint.subscribe_messages[0]).unwrap();
        assert_eq!(orders["op"], "subscribe");
        assert_eq!(orders["args"][0]["channel"], "orders");
        assert_eq!(orders["args"][0]["instType"], "SWAP");
        assert_eq!(orders["args"][0]["instId"], "BTC-USDT-SWAP");

        let positions: Value = serde_json::from_str(&endpoint.subscribe_messages[1]).unwrap();
        assert_eq!(positions["args"][0]["channel"], "positions");
        assert_eq!(positions["args"][0]["instType"], "SWAP");
        assert!(positions["args"][0].get("instId").is_none());

        let account: Value = serde_json::from_str(&endpoint.subscribe_messages[2]).unwrap();
        assert_eq!(account["args"][0]["channel"], "account");
        assert_eq!(account["args"][0]["instType"], "SWAP");
        assert!(account["args"][0].get("instId").is_none());
    }

    #[tokio::test]
    async fn okx_adapter_should_place_batch_orders_and_split_results() {
        let transport = MockTransport::new(json!({
            "code": "0",
            "msg": "",
            "data": [
                {
                    "ordId": "okx-order-1",
                    "clOrdId": "client-1",
                    "sCode": "0",
                    "sMsg": ""
                },
                {
                    "ordId": "",
                    "clOrdId": "client-2",
                    "sCode": "51008",
                    "sMsg": "Insufficient balance"
                }
            ]
        }));
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(OkxPrivatePerpProtocol, transport)
            .with_position_mode(PositionMode::Hedge);
        let mut first = command(ExchangeId::Okx, "BTC-USDT-SWAP");
        first.client_order_id = "client-1".to_string();
        let mut second = command(ExchangeId::Okx, "ETH-USDT-SWAP");
        second.client_order_id = "client-2".to_string();
        second.side = OrderSide::Sell;
        second.position_side = PositionSide::Short;

        let ack = adapter
            .place_batch_orders(BatchPlaceCommand::new(
                ExchangeId::Okx,
                [first, second],
                Utc::now(),
            ))
            .await
            .unwrap();

        assert_batch_place_ack(&ack, ExchangeId::Okx, false, 1, 1, Some(""));
        assert_eq!(ack.order_acks[0].exchange, ExchangeId::Okx);
        assert_eq!(ack.order_acks[0].client_order_id, "client-1");
        assert_eq!(
            ack.order_acks[0].exchange_order_id.as_deref(),
            Some("okx-order-1")
        );
        assert!(ack.order_acks[0].accepted);
        assert_eq!(ack.order_acks[0].status, OrderCommandStatus::Accepted);
        assert_eq!(ack.order_acks[0].message.as_deref(), Some(""));
        assert_eq!(ack.failed_orders[0].code.as_deref(), Some("51008"));
        assert_eq!(
            ack.failed_orders[0].message,
            "Insufficient balance".to_string()
        );
        assert_eq!(ack.failed_orders[0].order.client_order_id, "client-2");

        let seen = seen.lock().unwrap();
        assert_eq!(seen[0].path, "/api/v5/trade/batch-orders");
        let body = seen[0].body.as_ref().unwrap().as_array().unwrap();
        assert_eq!(body[0]["instId"], "BTC-USDT-SWAP");
        assert_eq!(body[0]["posSide"], "long");
        assert_eq!(body[1]["instId"], "ETH-USDT-SWAP");
        assert_eq!(body[1]["side"], "sell");
        assert_eq!(body[1]["posSide"], "short");
    }

    #[test]
    fn okx_cancel_should_validate_required_identifiers_and_exchange_scope() {
        let missing_id = OkxPrivatePerpProtocol.cancel_order(&CancelCommand {
            exchange: ExchangeId::Okx,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
            client_order_id: None,
            exchange_order_id: None,
            reason: None,
            requested_at: Utc::now(),
        });
        assert!(missing_id
            .unwrap_err()
            .to_string()
            .contains("requires ordId or clOrdId"));

        let mut wrong_single_symbol = CancelCommand {
            exchange: ExchangeId::Okx,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: None,
            reason: None,
            requested_at: Utc::now(),
        };
        let wrong_single = OkxPrivatePerpProtocol.cancel_order(&wrong_single_symbol);
        assert!(wrong_single
            .unwrap_err()
            .to_string()
            .contains("okx exchange symbols"));

        let wrong_symbol = ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT");
        let mut wrong_place_symbol = command(ExchangeId::Okx, "BTC-USDT-SWAP");
        wrong_place_symbol.exchange_symbol = wrong_symbol.clone();
        assert!(OkxPrivatePerpProtocol
            .place_order(&wrong_place_symbol, PositionMode::Hedge)
            .unwrap_err()
            .to_string()
            .contains("place order requires okx exchange symbols"));

        let batch_wrong_exchange = BatchPlaceCommand::new(
            ExchangeId::Mexc,
            [command(ExchangeId::Okx, "BTC-USDT-SWAP")],
            Utc::now(),
        );
        assert!(OkxPrivatePerpProtocol
            .place_batch_orders(&batch_wrong_exchange, PositionMode::Hedge)
            .unwrap_err()
            .to_string()
            .contains("batch-place requires okx exchange symbols"));

        wrong_single_symbol.exchange_symbol = ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP");
        let mut missing_batch_id = wrong_single_symbol.clone();
        missing_batch_id.client_order_id = None;
        let batch_missing_id =
            OkxPrivatePerpProtocol.cancel_batch_orders(&CancelBatchCommand::new(
                ExchangeId::Okx,
                [wrong_single_symbol.clone(), missing_batch_id],
                Utc::now(),
            ));
        assert!(batch_missing_id
            .unwrap_err()
            .to_string()
            .contains("for every order"));

        let mut wrong_batch_symbol = wrong_single_symbol.clone();
        wrong_batch_symbol.exchange_symbol = ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT");
        let wrong_batch = OkxPrivatePerpProtocol.cancel_batch_orders(&CancelBatchCommand::new(
            ExchangeId::Okx,
            [wrong_batch_symbol],
            Utc::now(),
        ));
        assert!(wrong_batch
            .unwrap_err()
            .to_string()
            .contains("okx exchange symbols"));

        let wrong_batch_command_exchange = OkxPrivatePerpProtocol.cancel_batch_orders(
            &CancelBatchCommand::new(ExchangeId::Mexc, [wrong_single_symbol.clone()], Utc::now()),
        );
        assert!(wrong_batch_command_exchange
            .unwrap_err()
            .to_string()
            .contains("okx batch cancel requires okx exchange symbols"));

        assert!(OkxPrivatePerpProtocol
            .get_order(&OrderQuery {
                exchange: ExchangeId::Okx,
                exchange_symbol: wrong_symbol.clone(),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: None,
            })
            .unwrap_err()
            .to_string()
            .contains("query order requires okx exchange symbols"));
        assert!(OkxPrivatePerpProtocol
            .get_open_orders(Some(&wrong_symbol))
            .unwrap_err()
            .to_string()
            .contains("open orders requires okx exchange symbols"));
        assert!(OkxPrivatePerpProtocol
            .get_fills(&FillQuery::for_symbol(
                ExchangeId::Okx,
                CanonicalSymbol::new("BTC", "USDT"),
                wrong_symbol.clone(),
            ))
            .unwrap_err()
            .to_string()
            .contains("fills-history requires okx exchange symbols"));
        assert!(OkxPrivatePerpProtocol
            .get_positions(Some(&wrong_symbol))
            .unwrap_err()
            .to_string()
            .contains("positions requires okx exchange symbols"));
        assert!(OkxPrivatePerpProtocol
            .get_trade_fee(&wrong_symbol)
            .unwrap_err()
            .to_string()
            .contains("trade fee requires okx exchange symbols"));
        assert!(OkxPrivatePerpProtocol
            .get_symbol_account_config(&wrong_symbol)
            .unwrap_err()
            .to_string()
            .contains("symbol account config requires okx exchange symbols"));
        assert!(OkxPrivatePerpProtocol
            .get_order_amendments(&OrderAmendmentHistoryQuery::for_order(
                ExchangeId::Okx,
                wrong_symbol.clone(),
                Some("client-1".to_string()),
                None,
            ))
            .unwrap_err()
            .to_string()
            .contains("order-amendment history requires okx exchange symbols"));
        assert!(OkxPrivatePerpProtocol
            .get_order_amendments(&OrderAmendmentHistoryQuery::for_order(
                ExchangeId::Okx,
                ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
                None,
                None,
            ))
            .unwrap_err()
            .to_string()
            .contains("order-amendment history requires exchange or client order id"));
        assert!(OkxPrivatePerpProtocol
            .get_order_amendments(&OrderAmendmentHistoryQuery::for_order(
                ExchangeId::Okx,
                ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
                Some("client-1".to_string()),
                None,
            ))
            .unwrap_err()
            .to_string()
            .contains("order-amendment history is not implemented"));

        let wrong_amend_symbol = OkxPrivatePerpProtocol.amend_order(&AmendOrderCommand {
            exchange: ExchangeId::Okx,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: wrong_symbol.clone(),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: None,
            original_side: None,
            new_client_order_id: None,
            new_quantity: Some(0.02),
            new_price: None,
            requested_at: Utc::now(),
        });
        assert!(wrong_amend_symbol
            .unwrap_err()
            .to_string()
            .contains("okx amend requires okx exchange symbols"));

        let mut okx_amend = AmendOrderCommand {
            exchange: ExchangeId::Okx,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: None,
            original_side: None,
            new_client_order_id: None,
            new_quantity: Some(0.02),
            new_price: None,
            requested_at: Utc::now(),
        };
        okx_amend.client_order_id = None;
        okx_amend.new_client_order_id = Some("client-2".to_string());
        assert!(OkxPrivatePerpProtocol
            .amend_order(&okx_amend)
            .unwrap_err()
            .to_string()
            .contains("okx amend requires ordId or clOrdId"));

        okx_amend.client_order_id = Some("client-1".to_string());
        okx_amend.new_quantity = None;
        assert!(OkxPrivatePerpProtocol
            .amend_order(&okx_amend)
            .unwrap_err()
            .to_string()
            .contains("okx amend requires new quantity or new price"));

        okx_amend.new_quantity = Some(0.02);
        assert!(OkxPrivatePerpProtocol
            .amend_order(&okx_amend)
            .unwrap_err()
            .to_string()
            .contains("okx amend does not replace client order id"));

        assert!(OkxPrivatePerpProtocol
            .set_leverage(&LeverageCommand {
                exchange: ExchangeId::Okx,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: wrong_symbol.clone(),
                leverage: 3,
                requested_at: Utc::now(),
            })
            .unwrap_err()
            .to_string()
            .contains("set leverage requires okx exchange symbols"));
        assert!(OkxPrivatePerpProtocol
            .set_countdown_cancel_all(&CountdownCancelAllCommand::set_for_symbol(
                ExchangeId::Okx,
                wrong_symbol,
                10,
                Utc::now(),
            ))
            .unwrap_err()
            .to_string()
            .contains("cancel-all-after requires okx exchange symbols"));
        assert!(OkxPrivatePerpProtocol
            .set_countdown_cancel_all(&CountdownCancelAllCommand::cancel(
                ExchangeId::Mexc,
                Utc::now(),
            ))
            .unwrap_err()
            .to_string()
            .contains("cancel-all-after requires okx exchange symbols"));

        let too_many = (0..21)
            .map(|index| {
                let mut order = wrong_single_symbol.clone();
                order.client_order_id = Some(format!("client-{index}"));
                order
            })
            .collect::<Vec<_>>();
        let too_many_batch = OkxPrivatePerpProtocol.cancel_batch_orders(&CancelBatchCommand::new(
            ExchangeId::Okx,
            too_many,
            Utc::now(),
        ));
        assert!(too_many_batch
            .unwrap_err()
            .to_string()
            .contains("at most 20"));
    }

    #[test]
    fn okx_should_parse_private_events() {
        let now = Utc::now();
        let order_events = OkxPrivatePerpProtocol
            .parse_private_ws_message(
                r#"{
                    "arg":{"channel":"orders","instType":"SWAP"},
                    "data":[{
                        "instId":"BTC-USDT-SWAP",
                        "ordId":"12345",
                        "clOrdId":"client-1",
                        "side":"buy",
                        "posSide":"long",
                        "ordType":"limit",
                        "state":"partially_filled",
                        "px":"65000",
                        "sz":"0.02",
                        "accFillSz":"0.01",
                        "avgPx":"65005",
                        "fillSz":"0.01",
                        "fillPx":"65010",
                        "tradeId":"trade-1",
                        "fee":"-0.13",
                        "feeCcy":"USDT",
                        "fillTime":"1700000000000",
                        "uTime":"1700000000100"
                    }]
                }"#,
                now,
            )
            .unwrap();
        assert_eq!(order_events.len(), 2);
        match &order_events[0].kind {
            PrivateEventKind::Order(order) => {
                assert_eq!(order.exchange, ExchangeId::Okx);
                assert_eq!(order.exchange_order_id.as_deref(), Some("12345"));
                assert_eq!(order.client_order_id.as_deref(), Some("client-1"));
                assert_eq!(order.position_side, PositionSide::Long);
                assert_eq!(order.status, OrderCommandStatus::PartiallyFilled);
                assert_eq!(order.filled_quantity, 0.01);
            }
            other => panic!("expected okx order event, got {other:?}"),
        }
        match &order_events[1].kind {
            PrivateEventKind::Fill(fill) => {
                assert_eq!(fill.trade_id, "trade-1");
                assert_eq!(fill.fee, Some(0.13));
                assert_eq!(fill.fee_asset.as_deref(), Some("USDT"));
                assert_eq!(fill.quantity, 0.01);
            }
            other => panic!("expected okx fill event, got {other:?}"),
        }

        let position_events = OkxPrivatePerpProtocol
            .parse_private_ws_message(
                r#"{
                    "arg":{"channel":"positions","instType":"SWAP"},
                    "data":[{
                        "instId":"BTC-USDT-SWAP",
                        "posSide":"short",
                        "pos":"0.03",
                        "avgPx":"64000",
                        "markPx":"65000",
                        "upl":"-30",
                        "uTime":"1700000000200"
                    }]
                }"#,
                now,
            )
            .unwrap();
        match &position_events[0].kind {
            PrivateEventKind::Position(position) => {
                assert_eq!(position.exchange, ExchangeId::Okx);
                assert_eq!(position.position_side, PositionSide::Short);
                assert_eq!(position.quantity, 0.03);
                assert_eq!(position.unrealized_pnl, Some(-30.0));
            }
            other => panic!("expected okx position event, got {other:?}"),
        }

        let balance_events = OkxPrivatePerpProtocol
            .parse_private_ws_message(
                r#"{
                    "arg":{"channel":"account"},
                    "data":[{
                        "details":[{
                            "ccy":"USDT",
                            "eq":"100.5",
                            "availEq":"80.0",
                            "uTime":"1700000000300"
                        }]
                    }]
                }"#,
                now,
            )
            .unwrap();
        match &balance_events[0].kind {
            PrivateEventKind::Balance(balance) => {
                assert_eq!(balance.exchange, ExchangeId::Okx);
                assert_eq!(balance.asset, "USDT");
                assert_eq!(balance.total, 100.5);
                assert_eq!(balance.available, 80.0);
            }
            other => panic!("expected okx balance event, got {other:?}"),
        }

        let subscribe_events = OkxPrivatePerpProtocol
            .parse_private_ws_message(
                r#"{"event":"subscribe","arg":{"channel":"orders","instType":"SWAP"},"code":"0"}"#,
                now,
            )
            .unwrap();
        assert!(matches!(
            subscribe_events[0].kind,
            PrivateEventKind::Heartbeat
        ));

        let login_error_events = OkxPrivatePerpProtocol
            .parse_private_ws_message(
                r#"{"event":"login","code":"60009","msg":"login failed"}"#,
                now,
            )
            .unwrap();
        match &login_error_events[0].kind {
            PrivateEventKind::Error(error) => {
                assert_eq!(error.code.as_deref(), Some("60009"));
                assert!(error.message.contains("login failed"));
            }
            other => panic!("expected okx login error event, got {other:?}"),
        }

        let error_events = OkxPrivatePerpProtocol
            .parse_private_ws_message(
                r#"{"event":"error","code":"60012","msg":"bad subscription"}"#,
                now,
            )
            .unwrap();
        match &error_events[0].kind {
            PrivateEventKind::Error(error) => {
                assert_eq!(error.code.as_deref(), Some("60012"));
                assert!(error.message.contains("bad subscription"));
            }
            other => panic!("expected okx error event, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn okx_adapter_should_compose_cancel_all_from_open_orders_and_batch_cancel() {
        let transport = SequentialMockTransport::new([
            json!({
                "code": "0",
                "msg": "",
                "data": [
                    {
                        "instId": "BTC-USDT-SWAP",
                        "ordId": "order-1",
                        "clOrdId": "client-1",
                        "side": "buy",
                        "posSide": "long",
                        "ordType": "post_only",
                        "state": "live",
                        "px": "65000",
                        "sz": "0.02",
                        "accFillSz": "0",
                        "uTime": "1700000000000"
                    },
                    {
                        "instId": "BTC-USDT-SWAP",
                        "ordId": "order-2",
                        "clOrdId": "client-2",
                        "side": "sell",
                        "posSide": "short",
                        "ordType": "limit",
                        "state": "partially_filled",
                        "px": "66000",
                        "sz": "0.03",
                        "accFillSz": "0.01",
                        "uTime": "1700000000100"
                    }
                ]
            }),
            json!({
                "code": "0",
                "msg": "",
                "data": [
                    {"instId":"BTC-USDT-SWAP","ordId":"order-1","clOrdId":"client-1","sCode":"0","sMsg":""},
                    {"instId":"BTC-USDT-SWAP","ordId":"order-2","clOrdId":"client-2","sCode":"0","sMsg":""}
                ]
            }),
        ]);
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(OkxPrivatePerpProtocol, transport);

        let ack = adapter
            .cancel_all_orders(CancelAllCommand::for_symbol(
                ExchangeId::Okx,
                CanonicalSymbol::new("BTC", "USDT"),
                ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
                Utc::now(),
            ))
            .await
            .unwrap();

        assert!(ack.accepted);
        assert_eq!(ack.cancelled_orders, 2);
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].path, "/api/v5/trade/orders-pending");
        assert_eq!(
            seen[0].query.get("instId").map(String::as_str),
            Some("BTC-USDT-SWAP")
        );
        assert_eq!(seen[1].path, "/api/v5/trade/cancel-batch-orders");
        let body = seen[1].body.as_ref().unwrap().as_array().unwrap();
        assert_eq!(body.len(), 2);
        assert_eq!(body[0]["ordId"], "order-1");
        assert_eq!(body[0]["clOrdId"], "client-1");
        assert_eq!(body[1]["ordId"], "order-2");
        assert_eq!(body[1]["clOrdId"], "client-2");
    }

    #[tokio::test]
    async fn okx_adapter_cancel_all_should_skip_batch_cancel_when_no_open_orders() {
        let transport = MockTransport::new(json!({
            "code": "0",
            "msg": "",
            "data": []
        }));
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(OkxPrivatePerpProtocol, transport);

        let ack = adapter
            .cancel_all_orders(CancelAllCommand::for_symbol(
                ExchangeId::Okx,
                CanonicalSymbol::new("BTC", "USDT"),
                ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
                Utc::now(),
            ))
            .await
            .unwrap();

        assert!(ack.accepted);
        assert_eq!(ack.exchange, ExchangeId::Okx);
        assert_eq!(ack.cancelled_orders, 0);
        assert_eq!(ack.message.as_deref(), Some("no open orders to cancel"));
        assert_eq!(
            ack.exchange_symbol
                .as_ref()
                .map(|symbol| symbol.symbol.as_str()),
            Some("BTC-USDT-SWAP")
        );
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].path, "/api/v5/trade/orders-pending");
        assert_eq!(
            seen[0].query.get("instId").map(String::as_str),
            Some("BTC-USDT-SWAP")
        );
    }

    #[tokio::test]
    async fn okx_adapter_cancel_all_should_skip_batch_cancel_when_open_orders_lack_ids() {
        let transport = MockTransport::new(json!({
            "code": "0",
            "msg": "",
            "data": [
                {
                    "instId": "BTC-USDT-SWAP",
                    "side": "buy",
                    "posSide": "long",
                    "ordType": "limit",
                    "state": "live",
                    "px": "65000",
                    "sz": "0.02",
                    "accFillSz": "0",
                    "uTime": "1700000000000"
                }
            ]
        }));
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(OkxPrivatePerpProtocol, transport);

        let ack = adapter
            .cancel_all_orders(CancelAllCommand::for_symbol(
                ExchangeId::Okx,
                CanonicalSymbol::new("BTC", "USDT"),
                ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
                Utc::now(),
            ))
            .await
            .unwrap();

        assert!(ack.accepted);
        assert_eq!(ack.exchange, ExchangeId::Okx);
        assert_eq!(ack.cancelled_orders, 0);
        assert_eq!(
            ack.message.as_deref(),
            Some("open orders did not include cancellable ids")
        );
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].path, "/api/v5/trade/orders-pending");
        assert_eq!(
            seen[0].query.get("instId").map(String::as_str),
            Some("BTC-USDT-SWAP")
        );
    }

    #[tokio::test]
    async fn okx_adapter_should_batch_cancel_and_split_item_results() {
        let transport = MockTransport::new(json!({
            "code": "0",
            "msg": "",
            "data": [
                {
                    "instId": "BTC-USDT-SWAP",
                    "ordId": "order-1",
                    "clOrdId": "client-1",
                    "sCode": "0",
                    "sMsg": ""
                },
                {
                    "instId": "BTC-USDT-SWAP",
                    "ordId": "order-2",
                    "clOrdId": "client-2",
                    "sCode": "51400",
                    "sMsg": "Order cancellation failed"
                }
            ]
        }));
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(OkxPrivatePerpProtocol, transport);
        let first = CancelCommand {
            exchange: ExchangeId::Okx,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: Some("order-1".to_string()),
            reason: None,
            requested_at: Utc::now(),
        };
        let second = CancelCommand {
            client_order_id: Some("client-2".to_string()),
            exchange_order_id: Some("order-2".to_string()),
            ..first.clone()
        };

        let ack = adapter
            .cancel_batch_orders(CancelBatchCommand::new(
                ExchangeId::Okx,
                [first, second],
                Utc::now(),
            ))
            .await
            .unwrap();

        assert_cancel_batch_ack(&ack, ExchangeId::Okx, false, 1, 2, Some(""));
        assert!(ack.order_acks[0].accepted);
        assert_eq!(ack.order_acks[0].exchange, ExchangeId::Okx);
        assert_eq!(
            ack.order_acks[0].client_order_id.as_deref(),
            Some("client-1")
        );
        assert_eq!(ack.order_acks[0].status, OrderCommandStatus::Cancelled);
        assert_eq!(
            ack.order_acks[0].exchange_order_id.as_deref(),
            Some("order-1")
        );
        assert_eq!(ack.order_acks[0].message.as_deref(), Some(""));
        assert!(!ack.order_acks[1].accepted);
        assert_eq!(ack.order_acks[1].exchange, ExchangeId::Okx);
        assert_eq!(
            ack.order_acks[1].client_order_id.as_deref(),
            Some("client-2")
        );
        assert_eq!(
            ack.order_acks[1].exchange_order_id.as_deref(),
            Some("order-2")
        );
        assert_eq!(ack.order_acks[1].status, OrderCommandStatus::Rejected);
        assert_eq!(
            ack.order_acks[1].message.as_deref(),
            Some("Order cancellation failed")
        );

        let seen = seen.lock().unwrap();
        assert_eq!(seen[0].path, "/api/v5/trade/cancel-batch-orders");
        let body = seen[0].body.as_ref().unwrap().as_array().unwrap();
        assert_eq!(body[0]["instId"], "BTC-USDT-SWAP");
        assert_eq!(body[0]["ordId"], "order-1");
        assert_eq!(body[0]["clOrdId"], "client-1");
        assert_eq!(body[1]["ordId"], "order-2");
        assert_eq!(body[1]["clOrdId"], "client-2");
    }

    #[tokio::test]
    async fn okx_adapter_should_parse_order_history() {
        let transport = MockTransport::new(json!({
            "code": "0",
            "msg": "",
            "data": [
                {
                    "instId": "BTC-USDT-SWAP",
                    "ordId": "order-1",
                    "clOrdId": "client-1",
                    "side": "buy",
                    "posSide": "long",
                    "ordType": "limit",
                    "state": "filled",
                    "px": "65000",
                    "sz": "0.02",
                    "accFillSz": "0.02",
                    "avgPx": "64950",
                    "uTime": "1700000000000"
                }
            ]
        }));
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(OkxPrivatePerpProtocol, transport);
        let mut query = OrderHistoryQuery::for_symbol(
            ExchangeId::Okx,
            ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
        );
        query.exchange_order_id = Some("order-1".to_string());
        query.limit = Some(20);

        let orders = adapter.get_order_history(query).await.unwrap();

        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].exchange, ExchangeId::Okx);
        assert_eq!(orders[0].exchange_order_id.as_deref(), Some("order-1"));
        assert_eq!(orders[0].client_order_id.as_deref(), Some("client-1"));
        assert_eq!(orders[0].side, OrderSide::Buy);
        assert_eq!(orders[0].position_side, PositionSide::Long);
        assert_eq!(orders[0].order_type, OrderType::Limit);
        assert_eq!(orders[0].status, OrderCommandStatus::Filled);
        assert_eq!(orders[0].quantity, 0.02);
        assert_eq!(orders[0].filled_quantity, 0.02);
        assert_eq!(orders[0].average_fill_price, Some(64950.0));

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].path, "/api/v5/trade/orders-history");
        assert_eq!(
            seen[0].query.get("instType").map(String::as_str),
            Some("SWAP")
        );
        assert_eq!(
            seen[0].query.get("instId").map(String::as_str),
            Some("BTC-USDT-SWAP")
        );
        assert_eq!(
            seen[0].query.get("ordId").map(String::as_str),
            Some("order-1")
        );
        assert_eq!(seen[0].query.get("limit").map(String::as_str), Some("20"));
    }

    #[tokio::test]
    async fn okx_adapter_order_amendment_history_should_fail_before_rest() {
        let transport = MockTransport::new(json!({"code": "0", "data": []}));
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(OkxPrivatePerpProtocol, transport);

        let unsupported = adapter
            .get_order_amendment_history(OrderAmendmentHistoryQuery::for_order(
                ExchangeId::Okx,
                ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
                Some("client-1".to_string()),
                None,
            ))
            .await
            .unwrap_err();
        assert!(unsupported
            .to_string()
            .contains("order-amendment history is not implemented"));

        let wrong_symbol = adapter
            .get_order_amendment_history(OrderAmendmentHistoryQuery::for_order(
                ExchangeId::Okx,
                ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT"),
                Some("client-1".to_string()),
                None,
            ))
            .await
            .unwrap_err();
        assert!(wrong_symbol
            .to_string()
            .contains("order-amendment history requires okx exchange symbols"));
        assert!(seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn okx_adapter_should_route_fills_readback() {
        let transport = MockTransport::new(json!({
            "code": "0",
            "msg": "",
            "data": [
                {
                    "instId": "BTC-USDT-SWAP",
                    "tradeId": "trade-1",
                    "ordId": "order-1",
                    "clOrdId": "client-1",
                    "side": "sell",
                    "posSide": "short",
                    "fillPx": "65000",
                    "fillSz": "0.02",
                    "fillNotionalUsd": "1300",
                    "fee": "-0.65",
                    "feeCcy": "USDT",
                    "fillPnl": "12.5",
                    "fillRole": "taker",
                    "fillTime": "1700000000000"
                }
            ]
        }));
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(OkxPrivatePerpProtocol, transport);
        let mut query = FillQuery::for_symbol(
            ExchangeId::Okx,
            CanonicalSymbol::new("BTC", "USDT"),
            ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
        );
        query.exchange_order_id = Some("order-1".to_string());
        query.limit = Some(25);

        let fills = adapter.get_fills(query).await.unwrap();
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].exchange, ExchangeId::Okx);
        assert_eq!(fills[0].exchange_symbol.symbol, "BTC-USDT-SWAP");
        assert_eq!(fills[0].trade_id, "trade-1");
        assert_eq!(fills[0].exchange_order_id.as_deref(), Some("order-1"));
        assert_eq!(fills[0].client_order_id.as_deref(), Some("client-1"));
        assert_eq!(fills[0].side, OrderSide::Sell);
        assert_eq!(fills[0].position_side, PositionSide::Short);
        assert_eq!(fills[0].liquidity, FillLiquidity::Taker);
        assert_eq!(fills[0].price, 65_000.0);
        assert_eq!(fills[0].quantity, 0.02);
        assert_eq!(fills[0].quote_quantity, 1300.0);
        assert_eq!(fills[0].fee, Some(0.65));
        assert_eq!(fills[0].fee_asset.as_deref(), Some("USDT"));
        assert_eq!(fills[0].realized_pnl, Some(12.5));

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].path, "/api/v5/trade/fills-history");
        assert_eq!(
            seen[0].query.get("instType").map(String::as_str),
            Some("SWAP")
        );
        assert_eq!(
            seen[0].query.get("instId").map(String::as_str),
            Some("BTC-USDT-SWAP")
        );
        assert_eq!(
            seen[0].query.get("ordId").map(String::as_str),
            Some("order-1")
        );
        assert_eq!(seen[0].query.get("limit").map(String::as_str), Some("25"));
    }

    #[tokio::test]
    async fn okx_adapter_should_route_positions_and_balances_readbacks() {
        let transport = SequentialMockTransport::new([
            json!({
                "code": "0",
                "msg": "",
                "data": [
                    {
                        "instId": "BTC-USDT-SWAP",
                        "posSide": "long",
                        "pos": "0.5",
                        "avgPx": "65000",
                        "markPx": "65100",
                        "upl": "50",
                        "uTime": "1700000000000"
                    }
                ]
            }),
            json!({
                "code": "0",
                "msg": "",
                "data": [
                    {
                        "details": [
                            {
                                "ccy": "USDT",
                                "eq": "1000",
                                "availEq": "850",
                                "uTime": "1700000000000"
                            }
                        ]
                    }
                ]
            }),
        ]);
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(OkxPrivatePerpProtocol, transport);
        let symbol = ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP");

        let positions = adapter.get_positions(Some(&symbol)).await.unwrap();
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].exchange, ExchangeId::Okx);
        assert_eq!(positions[0].exchange_symbol, symbol);
        assert_eq!(positions[0].position_side, PositionSide::Long);
        assert_eq!(positions[0].quantity, 0.5);
        assert_eq!(positions[0].entry_price, Some(65_000.0));
        assert_eq!(positions[0].mark_price, Some(65_100.0));
        assert_eq!(positions[0].unrealized_pnl, Some(50.0));

        let balances = adapter.get_balances().await.unwrap();
        assert_eq!(balances.len(), 1);
        assert_eq!(balances[0].exchange, ExchangeId::Okx);
        assert_eq!(balances[0].asset, "USDT");
        assert_eq!(balances[0].total, 1000.0);
        assert_eq!(balances[0].available, 850.0);
        assert_eq!(balances[0].locked, 150.0);

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].path, "/api/v5/account/positions");
        assert_eq!(
            seen[0].query.get("instType").map(String::as_str),
            Some("SWAP")
        );
        assert_eq!(
            seen[0].query.get("instId").map(String::as_str),
            Some("BTC-USDT-SWAP")
        );
        assert_eq!(seen[1].path, "/api/v5/account/balance");
    }

    #[tokio::test]
    async fn okx_adapter_should_route_fee_and_symbol_account_config_readbacks() {
        let transport = SequentialMockTransport::new([
            json!({
                "code": "0",
                "msg": "",
                "data": [
                    {
                        "instId": "BTC-USDT-SWAP",
                        "maker": "-0.0001",
                        "taker": "0.0005"
                    }
                ]
            }),
            json!({
                "code": "0",
                "msg": "",
                "data": [
                    {
                        "instId": "BTC-USDT-SWAP",
                        "posMode": "long_short_mode",
                        "mgnMode": "cross",
                        "lever": "10",
                        "maxLever": "125"
                    }
                ]
            }),
        ]);
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(OkxPrivatePerpProtocol, transport)
            .with_position_mode(PositionMode::OneWay);
        let symbol = ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP");

        let fee = adapter.get_trade_fee(&symbol).await.unwrap();
        assert_eq!(fee.exchange, ExchangeId::Okx);
        assert_eq!(fee.exchange_symbol, symbol);
        assert_eq!(fee.maker, -0.0001);
        assert_eq!(fee.taker, 0.0005);

        let config = adapter.get_symbol_account_config(&symbol).await.unwrap();
        assert_eq!(config.exchange, ExchangeId::Okx);
        assert_eq!(config.exchange_symbol, symbol);
        assert_eq!(config.position_mode, Some(PositionMode::Hedge));
        assert_eq!(config.margin_mode, Some(MarginMode::Cross));
        assert_eq!(config.leverage, Some(10));
        assert_eq!(config.max_leverage, Some(125));

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].path, "/api/v5/account/trade-fee");
        assert_eq!(
            seen[0].query.get("instType").map(String::as_str),
            Some("SWAP")
        );
        assert_eq!(
            seen[0].query.get("instId").map(String::as_str),
            Some("BTC-USDT-SWAP")
        );
        assert_eq!(seen[1].path, "/api/v5/account/leverage-info");
        assert_eq!(
            seen[1].query.get("mgnMode").map(String::as_str),
            Some("cross")
        );
        assert_eq!(
            seen[1].query.get("instId").map(String::as_str),
            Some("BTC-USDT-SWAP")
        );
    }

    #[test]
    fn bitget_should_build_place_order_spec() {
        let spec = BitgetPrivatePerpProtocol
            .place_order(&command(ExchangeId::Bitget, "BTCUSDT"), PositionMode::Hedge)
            .unwrap();

        assert_eq!(spec.method, PrivateRestMethod::Post);
        assert_eq!(spec.path, "/api/v2/mix/order/place-order");
        let body = spec.body.unwrap();
        assert_eq!(body["productType"], "USDT-FUTURES");
        assert_eq!(body["marginMode"], "crossed");
        assert_eq!(body["force"], "post_only");
        assert_eq!(body["tradeSide"], "open");
        assert_eq!(body["clientOid"], "crossarb-ls-mk-1-c0b4e763");
    }

    #[test]
    fn bitget_hedge_close_should_use_position_side_without_reduce_only() {
        let cmd = ClosePositionCommand::market(
            ExchangeId::Bitget,
            CanonicalSymbol::new("BTC", "USDT"),
            ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
            PositionSide::Long,
            0.01,
            "close-long-1",
            Utc::now(),
        );

        let spec = close_position_spec(ExchangeId::Bitget, &cmd, PositionMode::Hedge).unwrap();
        let body = spec.body.unwrap();

        assert_eq!(body["side"], "buy");
        assert_eq!(body["marginMode"], "crossed");
        assert_eq!(body["tradeSide"], "close");
        assert_eq!(body["price"], "0");
        assert!(body.get("reduceOnly").is_none());

        let mut wrong_command_exchange = cmd.clone();
        wrong_command_exchange.exchange = ExchangeId::Mexc;
        let err = close_position_spec(
            ExchangeId::Bitget,
            &wrong_command_exchange,
            PositionMode::Hedge,
        )
        .unwrap_err();
        assert!(err
            .to_string()
            .contains("bitget close position requires bitget exchange symbols"));

        let mut wrong_symbol_exchange = cmd.clone();
        wrong_symbol_exchange.exchange_symbol = ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT");
        let err = close_position_spec(
            ExchangeId::Bitget,
            &wrong_symbol_exchange,
            PositionMode::Hedge,
        )
        .unwrap_err();
        assert!(err
            .to_string()
            .contains("bitget close position requires bitget exchange symbols"));
    }

    #[test]
    fn htx_market_close_should_use_cross_lightning_close() {
        let cmd = ClosePositionCommand::market(
            ExchangeId::Htx,
            CanonicalSymbol::new("BTC", "USDT"),
            ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"),
            PositionSide::Long,
            3.0,
            "close-long-1",
            Utc::now(),
        );

        let spec = close_position_spec(ExchangeId::Htx, &cmd, PositionMode::Hedge).unwrap();
        assert_eq!(spec.method, PrivateRestMethod::Post);
        assert_eq!(
            spec.path,
            "/linear-swap-api/v1/swap_cross_lightning_close_position"
        );
        let body = spec.body.unwrap();
        assert_eq!(body["contract_code"], "BTC-USDT");
        assert_eq!(body["volume"], "3");
        assert_eq!(body["direction"], "sell");
        assert_eq!(body["order_price_type"], "lightning_ioc");
        assert!(body["client_order_id"].as_i64().unwrap() > 0);
    }

    #[test]
    fn gate_should_build_signed_size_place_order_spec() {
        let mut cmd = command(ExchangeId::Gate, "BTC_USDT");
        cmd.side = OrderSide::Sell;
        cmd.reduce_only = true;
        cmd.quantity = 25.0;

        let spec = GatePrivatePerpProtocol
            .place_order(&cmd, PositionMode::OneWay)
            .unwrap();
        let body = spec.body.unwrap();

        assert_eq!(spec.path, "/futures/usdt/orders");
        assert_eq!(body["contract"], "BTC_USDT");
        assert_eq!(body["size"], -25);
        assert_eq!(body["tif"], "poc");
        assert_eq!(body["price"], "65000");
        assert_eq!(body["reduce_only"], true);
        assert_eq!(
            body["text"].as_str().unwrap(),
            "t-crossarb-ls-mk-1-c0b4e763"
        );
        assert!(body["text"].as_str().unwrap().len() <= 30);
    }

    #[test]
    fn gate_should_build_batch_place_spec() {
        let mut first = command(ExchangeId::Gate, "BTC_USDT");
        first.client_order_id = "client-1".to_string();
        first.quantity = 25.0;
        first.time_in_force = TimeInForce::Gtc;
        first.post_only = false;

        let mut second = first.clone();
        second.client_order_id = "client-2".to_string();
        second.side = OrderSide::Sell;
        second.order_type = OrderType::Market;
        second.price = None;
        second.time_in_force = TimeInForce::Ioc;
        second.quantity = 4.0;
        second.reduce_only = true;

        let spec = GatePrivatePerpProtocol
            .place_batch_orders(
                &BatchPlaceCommand::new(ExchangeId::Gate, [first.clone(), second], Utc::now()),
                PositionMode::OneWay,
            )
            .unwrap();
        let body = spec.body.unwrap();

        assert_eq!(spec.path, "/futures/usdt/batch_orders");
        assert_eq!(body[0]["contract"], "BTC_USDT");
        assert_eq!(body[0]["size"], 25);
        assert_eq!(body[0]["tif"], "gtc");
        assert_eq!(body[0]["text"], "t-client-1");
        assert_eq!(body[1]["size"], -4);
        assert_eq!(body[1]["tif"], "ioc");
        assert_eq!(body[1]["price"], "0");
        assert_eq!(body[1]["reduce_only"], true);

        assert!(GatePrivatePerpProtocol
            .place_batch_orders(
                &BatchPlaceCommand::new(ExchangeId::Bitget, [first.clone()], Utc::now()),
                PositionMode::OneWay,
            )
            .unwrap_err()
            .to_string()
            .contains("gate batch-place requires gate exchange symbols"));

        let mut wrong_item = first.clone();
        wrong_item.exchange_symbol = ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT");
        assert!(GatePrivatePerpProtocol
            .place_batch_orders(
                &BatchPlaceCommand::new(ExchangeId::Gate, [wrong_item], Utc::now()),
                PositionMode::OneWay,
            )
            .unwrap_err()
            .to_string()
            .contains("gate batch-place requires gate exchange symbols"));

        assert!(GatePrivatePerpProtocol
            .place_batch_orders(
                &BatchPlaceCommand::new(ExchangeId::Gate, Vec::<OrderCommand>::new(), Utc::now()),
                PositionMode::OneWay,
            )
            .unwrap_err()
            .to_string()
            .contains("gate batch-place requires at least one order"));

        assert!(GatePrivatePerpProtocol
            .place_batch_orders(
                &BatchPlaceCommand::new(ExchangeId::Gate, vec![first; 11], Utc::now()),
                PositionMode::OneWay,
            )
            .unwrap_err()
            .to_string()
            .contains("gate batch-place supports at most 10 orders"));
    }

    #[test]
    fn gate_market_order_should_send_zero_price() {
        let mut cmd = command(ExchangeId::Gate, "BTC_USDT");
        cmd.order_type = OrderType::Market;
        cmd.price = None;
        cmd.time_in_force = TimeInForce::Ioc;
        cmd.post_only = false;
        cmd.quantity = 1.0;

        let spec = GatePrivatePerpProtocol
            .place_order(&cmd, PositionMode::OneWay)
            .unwrap();
        let body = spec.body.unwrap();

        assert_eq!(body["price"], "0");
        assert_eq!(body["tif"], "ioc");
    }

    #[test]
    fn gate_client_order_id_should_be_stable_for_lookup_paths() {
        let client_id = "crossarb-ls-mk-1-c0b4e763";
        let cancel = GatePrivatePerpProtocol
            .cancel_order(&CancelCommand {
                exchange: ExchangeId::Gate,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
                client_order_id: Some(client_id.to_string()),
                exchange_order_id: None,
                reason: None,
                requested_at: Utc::now(),
            })
            .unwrap();

        assert_eq!(
            cancel.path,
            "/futures/usdt/orders/t-crossarb-ls-mk-1-c0b4e763"
        );
        assert_eq!(gate_client_text(client_id), "t-crossarb-ls-mk-1-c0b4e763");
        assert_eq!(
            gate_client_text_to_local("t-crossarb-ls-mk-1-c0b4e763"),
            "crossarb-ls-mk-1-c0b4e763"
        );
    }

    #[test]
    fn gate_symbol_scoped_specs_should_validate_exchange_scope() {
        let wrong_symbol = ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT");
        let mut place = command(ExchangeId::Gate, "BTC_USDT");
        place.exchange_symbol = wrong_symbol.clone();
        let place_err = GatePrivatePerpProtocol
            .place_order(&place, PositionMode::OneWay)
            .unwrap_err();
        assert!(place_err
            .to_string()
            .contains("place order requires gate exchange symbols"));

        let cancel = CancelCommand {
            exchange: ExchangeId::Gate,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: wrong_symbol.clone(),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: None,
            reason: None,
            requested_at: Utc::now(),
        };
        assert!(GatePrivatePerpProtocol
            .cancel_order(&cancel)
            .unwrap_err()
            .to_string()
            .contains("cancel requires gate exchange symbols"));

        assert!(GatePrivatePerpProtocol
            .cancel_all_orders(&CancelAllCommand {
                exchange: ExchangeId::Mexc,
                canonical_symbol: None,
                exchange_symbol: None,
                requested_at: Utc::now(),
            })
            .unwrap_err()
            .to_string()
            .contains("cancel-all requires gate exchange symbols"));

        assert!(GatePrivatePerpProtocol
            .get_order(&OrderQuery {
                exchange: ExchangeId::Gate,
                exchange_symbol: wrong_symbol.clone(),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: None,
            })
            .unwrap_err()
            .to_string()
            .contains("query order requires gate exchange symbols"));

        assert!(GatePrivatePerpProtocol
            .get_order(&OrderQuery {
                exchange: ExchangeId::Bybit,
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: None,
            })
            .unwrap_err()
            .to_string()
            .contains("query order requires gate exchange symbols"));

        assert!(GatePrivatePerpProtocol
            .get_open_orders(Some(&wrong_symbol))
            .unwrap_err()
            .to_string()
            .contains("open orders requires gate exchange symbols"));

        assert!(GatePrivatePerpProtocol
            .get_fills(&FillQuery::for_symbol(
                ExchangeId::Gate,
                CanonicalSymbol::new("BTC", "USDT"),
                wrong_symbol.clone(),
            ))
            .unwrap_err()
            .to_string()
            .contains("fills requires gate exchange symbols"));

        assert!(GatePrivatePerpProtocol
            .get_fills(&FillQuery::new(ExchangeId::Bybit))
            .unwrap_err()
            .to_string()
            .contains("fills requires gate exchange symbols"));

        assert!(GatePrivatePerpProtocol
            .get_positions(Some(&wrong_symbol))
            .unwrap_err()
            .to_string()
            .contains("positions requires gate exchange symbols"));
        assert!(GatePrivatePerpProtocol
            .get_trade_fee(&wrong_symbol)
            .unwrap_err()
            .to_string()
            .contains("trade fee requires gate exchange symbols"));
        assert!(GatePrivatePerpProtocol
            .get_symbol_account_config(&wrong_symbol)
            .unwrap_err()
            .to_string()
            .contains("symbol account config requires gate exchange symbols"));

        assert!(GatePrivatePerpProtocol
            .amend_order(&AmendOrderCommand {
                exchange: ExchangeId::Gate,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: wrong_symbol.clone(),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: None,
                original_side: None,
                new_client_order_id: None,
                new_quantity: None,
                new_price: Some(64_900.0),
                requested_at: Utc::now(),
            })
            .unwrap_err()
            .to_string()
            .contains("amend requires gate exchange symbols"));

        assert!(GatePrivatePerpProtocol
            .set_leverage(&LeverageCommand {
                exchange: ExchangeId::Gate,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: wrong_symbol,
                leverage: 3,
                requested_at: Utc::now(),
            })
            .unwrap_err()
            .to_string()
            .contains("set leverage requires gate exchange symbols"));

        assert!(GatePrivatePerpProtocol
            .set_countdown_cancel_all(&CountdownCancelAllCommand::cancel(
                ExchangeId::Bybit,
                Utc::now(),
            ))
            .unwrap_err()
            .to_string()
            .contains("countdown cancel-all requires gate exchange symbols"));

        assert!(GatePrivatePerpProtocol
            .set_countdown_cancel_all(&CountdownCancelAllCommand::set_for_symbol(
                ExchangeId::Gate,
                ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
                30,
                Utc::now(),
            ))
            .unwrap_err()
            .to_string()
            .contains("countdown cancel-all requires gate exchange symbols"));
    }

    #[test]
    fn gate_signed_size_should_reject_fractional_contracts() {
        let err = gate_signed_size(OrderSide::Buy, 12.5).unwrap_err();
        assert!(err.to_string().contains("integer contract count"));
    }

    #[test]
    fn private_ws_login_shapes_should_be_stable() {
        let auth = PrivateWsAuth {
            api_key: "key".to_string(),
            api_secret: "secret".to_string(),
            passphrase: Some("pass".to_string()),
            account_id: Some("20011".to_string()),
            demo_trading: false,
        };

        let bitget = BitgetPrivatePerpProtocol
            .ws_login(&auth, 1_700_000_000)
            .unwrap();
        assert_eq!(bitget.exchange, ExchangeId::Bitget);
        assert!(bitget.message.contains("\"op\":\"login\""));
        assert!(bitget.message.contains("\"/user/verify\"").not());

        let gate = GatePrivatePerpProtocol
            .ws_login(&auth, 1_700_000_000)
            .unwrap();
        assert_eq!(gate.exchange, ExchangeId::Gate);
        assert!(gate.message.contains("\"channel\":\"futures.orders\""));
        assert!(gate.message.contains("\"auth\""));
    }

    #[test]
    fn private_trading_adapter_factories_should_return_registered_exchanges() {
        let bitget = bitget_private_trading_adapter(PrivateRestAuth {
            api_key: "key".to_string(),
            api_secret: "secret".to_string(),
            passphrase: Some("pass".to_string()),
            demo_trading: false,
        })
        .unwrap();
        assert_eq!(bitget.exchange(), ExchangeId::Bitget);
        assert!(bitget.capabilities().supports_close_position);

        let gate = private_perp_trading_adapter_for(
            PrivatePerpExchange::Gate,
            PrivateRestAuth {
                api_key: "key".to_string(),
                api_secret: "secret".to_string(),
                passphrase: None,
                demo_trading: false,
            },
            PositionMode::OneWay,
        )
        .unwrap();
        assert_eq!(gate.exchange(), ExchangeId::Gate);
        assert!(gate.capabilities().supports_leverage);

        for exchange in [
            PrivatePerpExchange::Bybit,
            PrivatePerpExchange::Mexc,
            PrivatePerpExchange::Htx,
        ] {
            let adapter = private_perp_trading_adapter_for(
                exchange,
                PrivateRestAuth {
                    api_key: "key".to_string(),
                    api_secret: "secret".to_string(),
                    passphrase: None,
                    demo_trading: false,
                },
                PositionMode::OneWay,
            )
            .unwrap();
            assert_eq!(adapter.exchange(), exchange.exchange_id());
            assert!(adapter.capabilities().supports_leverage);
        }
    }

    #[test]
    fn private_perp_capability_matrix_should_match_verified_common_surface() {
        for (exchange, supports_position_mode_change, supports_countdown_cancel_all) in [
            (ExchangeId::Binance, true, true),
            (ExchangeId::Okx, true, true),
            (ExchangeId::Bitget, true, true),
            (ExchangeId::Gate, false, true),
            (ExchangeId::Bybit, true, true),
            (ExchangeId::Mexc, true, false),
            (ExchangeId::Htx, true, false),
        ] {
            let capabilities = private_perp_trading_capabilities(exchange.clone());

            assert!(capabilities.supports_market_orders, "{exchange}");
            assert!(capabilities.supports_limit_orders, "{exchange}");
            assert!(capabilities.supports_post_only, "{exchange}");
            assert!(capabilities.supports_ioc, "{exchange}");
            assert!(capabilities.supports_fok, "{exchange}");
            assert!(capabilities.supports_reduce_only, "{exchange}");
            assert!(capabilities.supports_hedge_mode, "{exchange}");
            assert!(capabilities.supports_client_order_id, "{exchange}");
            assert!(capabilities.supports_leverage, "{exchange}");
            assert!(capabilities.supports_close_position, "{exchange}");
            assert!(capabilities.supports_batch_place_orders, "{exchange}");
            assert_eq!(
                capabilities.supports_position_mode_change, supports_position_mode_change,
                "{exchange}"
            );
            assert_eq!(
                capabilities.supports_countdown_cancel_all, supports_countdown_cancel_all,
                "{exchange}"
            );
        }
    }

    #[test]
    fn gate_position_mode_change_should_be_unsupported() {
        let err = GatePrivatePerpProtocol
            .set_position_mode(&PositionModeCommand {
                exchange: ExchangeId::Gate,
                mode: PositionMode::OneWay,
                requested_at: Utc::now(),
            })
            .unwrap_err();

        assert!(err.to_string().contains("does not support position mode"));

        let wrong_exchange = GatePrivatePerpProtocol
            .set_position_mode(&PositionModeCommand {
                exchange: ExchangeId::Mexc,
                mode: PositionMode::OneWay,
                requested_at: Utc::now(),
            })
            .unwrap_err();
        assert!(wrong_exchange
            .to_string()
            .contains("gate position mode requires gate exchange"));
    }

    #[test]
    fn position_mode_specs_should_validate_command_exchange() {
        let command = PositionModeCommand {
            exchange: ExchangeId::Gate,
            mode: PositionMode::Hedge,
            requested_at: Utc::now(),
        };

        assert!(BinancePrivatePerpProtocol
            .set_position_mode(&command)
            .unwrap_err()
            .to_string()
            .contains("binance position mode requires binance exchange"));
        assert!(OkxPrivatePerpProtocol
            .set_position_mode(&command)
            .unwrap_err()
            .to_string()
            .contains("okx position mode requires okx exchange"));
        assert!(BitgetPrivatePerpProtocol
            .set_position_mode(&command)
            .unwrap_err()
            .to_string()
            .contains("bitget position mode requires bitget exchange"));
        assert!(BybitPrivatePerpProtocol
            .set_position_mode(&command)
            .unwrap_err()
            .to_string()
            .contains("bybit position mode requires bybit exchange"));
        assert!(MexcPrivatePerpProtocol
            .set_position_mode(&command)
            .unwrap_err()
            .to_string()
            .contains("mexc position mode requires mexc exchange"));
        assert!(HtxPrivatePerpProtocol
            .set_position_mode(&command)
            .unwrap_err()
            .to_string()
            .contains("htx position mode requires htx exchange"));
    }

    #[test]
    fn bitget_should_build_cancel_all_and_fills_query_specs() {
        let command = CancelAllCommand::for_symbol(
            ExchangeId::Bitget,
            CanonicalSymbol::new("BTC", "USDT"),
            ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
            Utc::now(),
        );
        let spec = BitgetPrivatePerpProtocol
            .cancel_all_orders(&command)
            .unwrap();

        assert_eq!(spec.method, PrivateRestMethod::Post);
        assert_eq!(spec.path, "/api/v2/mix/order/cancel-all-orders");
        let body = spec.body.unwrap();
        assert_eq!(body["symbol"], "BTCUSDT");
        assert_eq!(body["productType"], "USDT-FUTURES");

        let mut query = FillQuery::for_symbol(
            ExchangeId::Bitget,
            CanonicalSymbol::new("BTC", "USDT"),
            ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
        );
        query.exchange_order_id = Some("order-1".to_string());
        query.limit = Some(20);
        let fills = BitgetPrivatePerpProtocol.get_fills(&query).unwrap();

        assert_eq!(fills.path, "/api/v2/mix/order/fills");
        assert_eq!(
            fills.query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
        assert_eq!(
            fills.query.get("orderId").map(String::as_str),
            Some("order-1")
        );
        assert_eq!(fills.query.get("limit").map(String::as_str), Some("20"));

        let mut history = OrderHistoryQuery::for_symbol(
            ExchangeId::Bitget,
            ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
        );
        history.exchange_order_id = Some("order-1".to_string());
        history.start_time = DateTime::<Utc>::from_timestamp_millis(1_700_000_000_000);
        history.end_time = DateTime::<Utc>::from_timestamp_millis(1_700_000_600_000);
        history.limit = Some(100);
        let history_spec = BitgetPrivatePerpProtocol.get_all_orders(&history).unwrap();
        assert_eq!(history_spec.method, PrivateRestMethod::Get);
        assert_eq!(history_spec.path, "/api/v2/mix/order/orders-history");
        assert_eq!(
            history_spec.query.get("productType").map(String::as_str),
            Some("USDT-FUTURES")
        );
        assert_eq!(
            history_spec.query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
        assert_eq!(
            history_spec.query.get("orderId").map(String::as_str),
            Some("order-1")
        );
        assert_eq!(
            history_spec.query.get("startTime").map(String::as_str),
            Some("1700000000000")
        );
        assert_eq!(
            history_spec.query.get("endTime").map(String::as_str),
            Some("1700000600000")
        );
        assert_eq!(
            history_spec.query.get("limit").map(String::as_str),
            Some("100")
        );

        let mut reversed_history = history.clone();
        reversed_history.start_time = history.end_time;
        reversed_history.end_time = history.start_time;
        assert!(BitgetPrivatePerpProtocol
            .get_all_orders(&reversed_history)
            .unwrap_err()
            .to_string()
            .contains("bitget all-orders history end_time must be after start_time"));

        let mut reversed_fills = query;
        reversed_fills.start_time = history.end_time;
        reversed_fills.end_time = history.start_time;
        assert!(BitgetPrivatePerpProtocol
            .get_fills(&reversed_fills)
            .unwrap_err()
            .to_string()
            .contains("bitget fills end_time must be after start_time"));
    }

    #[test]
    fn bybit_should_build_full_private_rest_specs() {
        let cmd = command(ExchangeId::Bybit, "BTCUSDT");
        let place = BybitPrivatePerpProtocol
            .place_order(&cmd, PositionMode::Hedge)
            .unwrap();
        assert_eq!(place.path, "/v5/order/create");
        let body = place.body.unwrap();
        assert_eq!(body["category"], "linear");
        assert_eq!(body["symbol"], "BTCUSDT");
        assert_eq!(body["orderLinkId"], cmd.client_order_id);
        assert_eq!(body["positionIdx"], 1);

        let cancel = BybitPrivatePerpProtocol
            .cancel_order(&CancelCommand {
                exchange: ExchangeId::Bybit,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT"),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: None,
                reason: None,
                requested_at: Utc::now(),
            })
            .unwrap();
        assert_eq!(cancel.path, "/v5/order/cancel");
        assert_eq!(cancel.body.unwrap()["orderLinkId"], "client-1");

        let cancel_all = BybitPrivatePerpProtocol
            .cancel_all_orders(&CancelAllCommand::for_symbol(
                ExchangeId::Bybit,
                CanonicalSymbol::new("BTC", "USDT"),
                ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT"),
                Utc::now(),
            ))
            .unwrap();
        assert_eq!(cancel_all.path, "/v5/order/cancel-all");
        let cancel_all_body = cancel_all.body.unwrap();
        assert_eq!(cancel_all_body["category"], "linear");
        assert_eq!(cancel_all_body["symbol"], "BTCUSDT");

        let batch = CancelBatchCommand::new(
            ExchangeId::Bybit,
            [CancelCommand {
                exchange: ExchangeId::Bybit,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT"),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: Some("order-1".to_string()),
                reason: None,
                requested_at: Utc::now(),
            }],
            Utc::now(),
        );
        let batch_spec = BybitPrivatePerpProtocol
            .cancel_batch_orders(&batch)
            .unwrap();
        assert_eq!(batch_spec.path, "/v5/order/cancel-batch");
        let batch_body = batch_spec.body.unwrap();
        assert_eq!(batch_body["category"], "linear");
        assert_eq!(batch_body["request"][0]["symbol"], "BTCUSDT");
        assert_eq!(batch_body["request"][0]["orderId"], "order-1");
        assert_eq!(batch_body["request"][0]["orderLinkId"], "client-1");

        let mut second = cmd.clone();
        second.client_order_id = "client-2".to_string();
        second.side = OrderSide::Sell;
        second.position_side = PositionSide::Short;
        second.quantity = 0.02;
        second.price = Some(66_000.0);
        let batch_place = BybitPrivatePerpProtocol
            .place_batch_orders(
                &BatchPlaceCommand::new(ExchangeId::Bybit, [cmd.clone(), second], Utc::now()),
                PositionMode::Hedge,
            )
            .unwrap();
        assert_eq!(batch_place.path, "/v5/order/create-batch");
        let batch_place_body = batch_place.body.unwrap();
        assert_eq!(batch_place_body["category"], "linear");
        assert_eq!(batch_place_body["request"][0]["symbol"], "BTCUSDT");
        assert_eq!(batch_place_body["request"][0]["side"], "Buy");
        assert_eq!(batch_place_body["request"][0]["timeInForce"], "PostOnly");
        assert_eq!(batch_place_body["request"][0]["positionIdx"], 1);
        assert_eq!(batch_place_body["request"][1]["side"], "Sell");
        assert_eq!(batch_place_body["request"][1]["positionIdx"], 2);
        assert_eq!(batch_place_body["request"][1]["qty"], "0.02");

        let order = BybitPrivatePerpProtocol
            .get_order(&OrderQuery {
                exchange: ExchangeId::Bybit,
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT"),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: Some("order-1".to_string()),
            })
            .unwrap();
        assert_eq!(order.method, PrivateRestMethod::Get);
        assert_eq!(order.path, "/v5/order/realtime");
        assert_eq!(
            order.query.get("category").map(String::as_str),
            Some("linear")
        );
        assert_eq!(
            order.query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
        assert_eq!(
            order.query.get("orderId").map(String::as_str),
            Some("order-1")
        );
        assert_eq!(
            order.query.get("orderLinkId").map(String::as_str),
            Some("client-1")
        );

        let open_orders = BybitPrivatePerpProtocol
            .get_open_orders(Some(&ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT")))
            .unwrap();
        assert_eq!(open_orders.path, "/v5/order/realtime");
        assert_eq!(
            open_orders.query.get("openOnly").map(String::as_str),
            Some("0")
        );

        let mut history = OrderHistoryQuery::for_symbol(
            ExchangeId::Bybit,
            ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT"),
        );
        history.exchange_order_id = Some("order-1".to_string());
        history.start_time = DateTime::<Utc>::from_timestamp_millis(1_700_000_000_000);
        history.end_time = DateTime::<Utc>::from_timestamp_millis(1_700_000_600_000);
        history.limit = Some(50);
        let history_spec = BybitPrivatePerpProtocol.get_all_orders(&history).unwrap();
        assert_eq!(history_spec.method, PrivateRestMethod::Get);
        assert_eq!(history_spec.path, "/v5/order/history");
        assert_eq!(
            history_spec.query.get("category").map(String::as_str),
            Some("linear")
        );
        assert_eq!(
            history_spec.query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
        assert_eq!(
            history_spec.query.get("orderId").map(String::as_str),
            Some("order-1")
        );
        assert_eq!(
            history_spec.query.get("startTime").map(String::as_str),
            Some("1700000000000")
        );
        assert_eq!(
            history_spec.query.get("endTime").map(String::as_str),
            Some("1700000600000")
        );
        assert_eq!(
            history_spec.query.get("limit").map(String::as_str),
            Some("50")
        );

        let mut fills = FillQuery::for_symbol(
            ExchangeId::Bybit,
            CanonicalSymbol::new("BTC", "USDT"),
            ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT"),
        );
        fills.exchange_order_id = Some("order-1".to_string());
        fills.client_order_id = Some("client-1".to_string());
        fills.limit = Some(20);
        let fills_spec = BybitPrivatePerpProtocol.get_fills(&fills).unwrap();
        assert_eq!(fills_spec.path, "/v5/execution/list");
        assert_eq!(
            fills_spec.query.get("orderId").map(String::as_str),
            Some("order-1")
        );
        assert_eq!(
            fills_spec.query.get("orderLinkId").map(String::as_str),
            Some("client-1")
        );
        assert_eq!(
            fills_spec.query.get("limit").map(String::as_str),
            Some("20")
        );

        let positions = BybitPrivatePerpProtocol
            .get_positions(Some(&ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT")))
            .unwrap();
        assert_eq!(positions.path, "/v5/position/list");
        assert_eq!(
            positions.query.get("settleCoin").map(String::as_str),
            Some("USDT")
        );

        assert_eq!(
            BybitPrivatePerpProtocol.get_balances().unwrap().path,
            "/v5/account/wallet-balance"
        );
        assert_eq!(
            BybitPrivatePerpProtocol
                .get_trade_fee(&ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT"))
                .unwrap()
                .path,
            "/v5/account/fee-rate"
        );
        assert_eq!(
            BybitPrivatePerpProtocol
                .get_symbol_account_config(&ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT"))
                .unwrap()
                .path,
            "/v5/position/list"
        );

        let amend = BybitPrivatePerpProtocol
            .amend_order(&AmendOrderCommand {
                exchange: ExchangeId::Bybit,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT"),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: None,
                original_side: None,
                new_client_order_id: None,
                new_quantity: Some(0.02),
                new_price: Some(64_900.0),
                requested_at: Utc::now(),
            })
            .unwrap();
        assert_eq!(amend.path, "/v5/order/amend");
        let amend_body = amend.body.unwrap();
        assert_eq!(amend_body["orderLinkId"], "client-1");
        assert_eq!(amend_body["qty"], "0.02");
        assert_eq!(amend_body["price"], "64900");

        let leverage = BybitPrivatePerpProtocol
            .set_leverage(&LeverageCommand {
                exchange: ExchangeId::Bybit,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT"),
                leverage: 3,
                requested_at: Utc::now(),
            })
            .unwrap();
        assert_eq!(leverage.path, "/v5/position/set-leverage");
        assert_eq!(leverage.body.unwrap()["buyLeverage"], "3");

        let mode = BybitPrivatePerpProtocol
            .set_position_mode(&PositionModeCommand {
                exchange: ExchangeId::Bybit,
                mode: PositionMode::Hedge,
                requested_at: Utc::now(),
            })
            .unwrap();
        assert_eq!(mode.path, "/v5/position/switch-mode");
        assert_eq!(mode.body.unwrap()["mode"], 3);

        let countdown = BybitPrivatePerpProtocol
            .set_countdown_cancel_all(&CountdownCancelAllCommand::set_for_symbol(
                ExchangeId::Bybit,
                ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT"),
                30,
                Utc::now(),
            ))
            .unwrap();
        assert_eq!(countdown.path, "/v5/order/disconnected-cancel-all");
        let countdown_body = countdown.body.unwrap();
        assert_eq!(countdown_body["symbol"], "BTCUSDT");
        assert_eq!(countdown_body["timeWindow"], 30);
    }

    #[test]
    fn bybit_cancel_should_validate_required_identifiers_and_exchange_scope() {
        let missing_id = BybitPrivatePerpProtocol.cancel_order(&CancelCommand {
            exchange: ExchangeId::Bybit,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT"),
            client_order_id: None,
            exchange_order_id: None,
            reason: None,
            requested_at: Utc::now(),
        });
        assert!(missing_id
            .unwrap_err()
            .to_string()
            .contains("requires orderId or orderLinkId"));

        let valid = CancelCommand {
            exchange: ExchangeId::Bybit,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT"),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: None,
            reason: None,
            requested_at: Utc::now(),
        };
        let wrong_symbol = ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT");
        let mut wrong_place_symbol = command(ExchangeId::Bybit, "BTCUSDT");
        wrong_place_symbol.exchange_symbol = wrong_symbol.clone();
        assert!(BybitPrivatePerpProtocol
            .place_order(&wrong_place_symbol, PositionMode::Hedge)
            .unwrap_err()
            .to_string()
            .contains("place order requires bybit exchange symbols"));

        let batch_wrong_exchange = BatchPlaceCommand::new(
            ExchangeId::Mexc,
            [command(ExchangeId::Bybit, "BTCUSDT")],
            Utc::now(),
        );
        assert!(BybitPrivatePerpProtocol
            .place_batch_orders(&batch_wrong_exchange, PositionMode::Hedge)
            .unwrap_err()
            .to_string()
            .contains("batch-place requires bybit exchange symbols"));

        assert!(BybitPrivatePerpProtocol
            .cancel_all_orders(&CancelAllCommand::for_symbol(
                ExchangeId::Bybit,
                CanonicalSymbol::new("BTC", "USDT"),
                wrong_symbol.clone(),
                Utc::now(),
            ))
            .unwrap_err()
            .to_string()
            .contains("cancel-all requires bybit exchange symbols"));
        assert!(BybitPrivatePerpProtocol
            .cancel_all_orders(&CancelAllCommand::for_symbol(
                ExchangeId::Mexc,
                CanonicalSymbol::new("BTC", "USDT"),
                ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT"),
                Utc::now(),
            ))
            .unwrap_err()
            .to_string()
            .contains("cancel-all requires bybit exchange symbols"));

        let mut missing_batch_id = valid.clone();
        missing_batch_id.client_order_id = None;
        let batch_missing_id =
            BybitPrivatePerpProtocol.cancel_batch_orders(&CancelBatchCommand::new(
                ExchangeId::Bybit,
                [valid.clone(), missing_batch_id],
                Utc::now(),
            ));
        assert!(batch_missing_id
            .unwrap_err()
            .to_string()
            .contains("for every order"));

        let batch_wrong_command_exchange = BybitPrivatePerpProtocol.cancel_batch_orders(
            &CancelBatchCommand::new(ExchangeId::Mexc, [valid.clone()], Utc::now()),
        );
        assert!(batch_wrong_command_exchange
            .unwrap_err()
            .to_string()
            .contains("batch cancel requires bybit exchange symbols"));

        let mut wrong_exchange_symbol = valid.clone();
        wrong_exchange_symbol.exchange_symbol = wrong_symbol.clone();
        let wrong_exchange = BybitPrivatePerpProtocol.cancel_batch_orders(
            &CancelBatchCommand::new(ExchangeId::Bybit, [wrong_exchange_symbol], Utc::now()),
        );
        assert!(wrong_exchange
            .unwrap_err()
            .to_string()
            .contains("bybit exchange symbols"));

        assert!(BybitPrivatePerpProtocol
            .get_order(&OrderQuery {
                exchange: ExchangeId::Bybit,
                exchange_symbol: wrong_symbol.clone(),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: None,
            })
            .unwrap_err()
            .to_string()
            .contains("query order requires bybit exchange symbols"));
        assert!(BybitPrivatePerpProtocol
            .get_order(&OrderQuery {
                exchange: ExchangeId::Bybit,
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT"),
                client_order_id: None,
                exchange_order_id: None,
            })
            .unwrap_err()
            .to_string()
            .contains("query order requires orderId or orderLinkId"));
        assert!(BybitPrivatePerpProtocol
            .get_open_orders(Some(&wrong_symbol))
            .unwrap_err()
            .to_string()
            .contains("open orders requires bybit exchange symbols"));
        assert!(BybitPrivatePerpProtocol
            .get_fills(&FillQuery::for_symbol(
                ExchangeId::Bybit,
                CanonicalSymbol::new("BTC", "USDT"),
                wrong_symbol.clone(),
            ))
            .unwrap_err()
            .to_string()
            .contains("fills requires bybit exchange symbols"));
        assert!(BybitPrivatePerpProtocol
            .get_positions(Some(&wrong_symbol))
            .unwrap_err()
            .to_string()
            .contains("positions requires bybit exchange symbols"));
        assert!(BybitPrivatePerpProtocol
            .get_trade_fee(&wrong_symbol)
            .unwrap_err()
            .to_string()
            .contains("trade fee requires bybit exchange symbols"));
        assert!(BybitPrivatePerpProtocol
            .get_symbol_account_config(&wrong_symbol)
            .unwrap_err()
            .to_string()
            .contains("positions requires bybit exchange symbols"));
        assert!(BybitPrivatePerpProtocol
            .set_leverage(&LeverageCommand {
                exchange: ExchangeId::Bybit,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: wrong_symbol.clone(),
                leverage: 3,
                requested_at: Utc::now(),
            })
            .unwrap_err()
            .to_string()
            .contains("set leverage requires bybit exchange symbols"));
        assert!(BybitPrivatePerpProtocol
            .set_countdown_cancel_all(&CountdownCancelAllCommand::set_for_symbol(
                ExchangeId::Bybit,
                wrong_symbol,
                30,
                Utc::now(),
            ))
            .unwrap_err()
            .to_string()
            .contains("disconnected cancel-all requires bybit exchange symbols"));
        assert!(BybitPrivatePerpProtocol
            .set_countdown_cancel_all(&CountdownCancelAllCommand::cancel(
                ExchangeId::Mexc,
                Utc::now(),
            ))
            .unwrap_err()
            .to_string()
            .contains("disconnected cancel-all requires bybit exchange symbols"));

        let too_many = (0..21)
            .map(|index| {
                let mut order = valid.clone();
                order.client_order_id = Some(format!("client-{index}"));
                order
            })
            .collect::<Vec<_>>();
        let too_many_batch = BybitPrivatePerpProtocol.cancel_batch_orders(
            &CancelBatchCommand::new(ExchangeId::Bybit, too_many, Utc::now()),
        );
        assert!(too_many_batch
            .unwrap_err()
            .to_string()
            .contains("at most 20"));
    }

    #[test]
    fn bybit_amend_should_validate_identifiers_changes_and_exchange_scope() {
        let base = AmendOrderCommand {
            exchange: ExchangeId::Bybit,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT"),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: None,
            original_side: None,
            new_client_order_id: None,
            new_quantity: Some(0.02),
            new_price: None,
            requested_at: Utc::now(),
        };

        let mut wrong_symbol = base.clone();
        wrong_symbol.exchange_symbol = ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT");
        assert!(BybitPrivatePerpProtocol
            .amend_order(&wrong_symbol)
            .unwrap_err()
            .to_string()
            .contains("bybit amend requires bybit exchange symbols"));

        let mut missing_id = base.clone();
        missing_id.client_order_id = None;
        assert!(BybitPrivatePerpProtocol
            .amend_order(&missing_id)
            .unwrap_err()
            .to_string()
            .contains("bybit amend requires orderId or orderLinkId"));

        let mut missing_change = base.clone();
        missing_change.new_quantity = None;
        assert!(BybitPrivatePerpProtocol
            .amend_order(&missing_change)
            .unwrap_err()
            .to_string()
            .contains("bybit amend requires new quantity or new price"));

        let mut new_link_id = base;
        new_link_id.new_client_order_id = Some("client-2".to_string());
        assert!(BybitPrivatePerpProtocol
            .amend_order(&new_link_id)
            .unwrap_err()
            .to_string()
            .contains("does not replace orderLinkId"));
    }

    #[tokio::test]
    async fn bybit_adapter_should_parse_order_history() {
        let transport = MockTransport::new(json!({
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": [
                    {
                        "orderId": "order-1",
                        "orderLinkId": "client-1",
                        "symbol": "BTCUSDT",
                        "price": "26864.40",
                        "qty": "0.003",
                        "side": "Buy",
                        "positionIdx": 1,
                        "orderStatus": "PartiallyFilled",
                        "avgPrice": "26864.4",
                        "cumExecQty": "0.001",
                        "timeInForce": "PostOnly",
                        "orderType": "Limit",
                        "reduceOnly": false,
                        "createdTime": "1684476068369",
                        "updatedTime": "1684476068372"
                    }
                ],
                "category": "linear"
            },
            "retExtInfo": {},
            "time": 1684766282976_i64
        }));
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(BybitPrivatePerpProtocol, transport);

        let orders = adapter
            .get_order_history(OrderHistoryQuery::for_symbol(
                ExchangeId::Bybit,
                ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT"),
            ))
            .await
            .unwrap();

        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].exchange, ExchangeId::Bybit);
        assert_eq!(orders[0].exchange_order_id.as_deref(), Some("order-1"));
        assert_eq!(orders[0].client_order_id.as_deref(), Some("client-1"));
        assert_eq!(orders[0].status, OrderCommandStatus::PartiallyFilled);
        assert_eq!(orders[0].quantity, 0.003);
        assert_eq!(orders[0].filled_quantity, 0.001);
        assert_eq!(orders[0].average_fill_price, Some(26864.4));
        assert_eq!(seen.lock().unwrap()[0].path, "/v5/order/history");
    }

    #[tokio::test]
    async fn bybit_adapter_should_route_fills_readback() {
        let transport = MockTransport::new(json!({
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": [
                    {
                        "symbol": "BTCUSDT",
                        "execId": "exec-1",
                        "orderId": "order-1",
                        "orderLinkId": "client-1",
                        "side": "Sell",
                        "positionIdx": 2,
                        "execPrice": "65000",
                        "execQty": "0.02",
                        "execValue": "1300",
                        "execFee": "0.65",
                        "feeCurrency": "USDT",
                        "feeRate": "0.0005",
                        "execPnl": "12.5",
                        "isMaker": false,
                        "execTime": "1700000000000"
                    }
                ]
            },
            "retExtInfo": {},
            "time": 1700000000001_i64
        }));
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(BybitPrivatePerpProtocol, transport);
        let mut query = FillQuery::for_symbol(
            ExchangeId::Bybit,
            CanonicalSymbol::new("BTC", "USDT"),
            ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT"),
        );
        query.exchange_order_id = Some("order-1".to_string());
        query.client_order_id = Some("client-1".to_string());
        query.limit = Some(25);

        let fills = adapter.get_fills(query).await.unwrap();
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].exchange, ExchangeId::Bybit);
        assert_eq!(fills[0].exchange_symbol.symbol, "BTCUSDT");
        assert_eq!(fills[0].trade_id, "exec-1");
        assert_eq!(fills[0].exchange_order_id.as_deref(), Some("order-1"));
        assert_eq!(fills[0].client_order_id.as_deref(), Some("client-1"));
        assert_eq!(fills[0].side, OrderSide::Sell);
        assert_eq!(fills[0].position_side, PositionSide::Short);
        assert_eq!(fills[0].liquidity, FillLiquidity::Taker);
        assert_eq!(fills[0].price, 65_000.0);
        assert_eq!(fills[0].quantity, 0.02);
        assert_eq!(fills[0].quote_quantity, 1300.0);
        assert_eq!(fills[0].fee, Some(0.65));
        assert_eq!(fills[0].fee_asset.as_deref(), Some("USDT"));
        assert_eq!(fills[0].fee_rate, Some(0.0005));
        assert_eq!(fills[0].realized_pnl, Some(12.5));

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].path, "/v5/execution/list");
        assert_eq!(
            seen[0].query.get("category").map(String::as_str),
            Some("linear")
        );
        assert_eq!(
            seen[0].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
        assert_eq!(
            seen[0].query.get("orderId").map(String::as_str),
            Some("order-1")
        );
        assert_eq!(
            seen[0].query.get("orderLinkId").map(String::as_str),
            Some("client-1")
        );
        assert_eq!(seen[0].query.get("limit").map(String::as_str), Some("25"));
    }

    #[tokio::test]
    async fn bybit_adapter_should_batch_cancel_and_split_ret_ext_info() {
        let transport = MockTransport::new(json!({
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": [
                    {"category": "linear", "symbol": "BTCUSDT", "orderId": "order-1", "orderLinkId": "client-1"},
                    {"category": "linear", "symbol": "ETHUSDT", "orderId": "order-2", "orderLinkId": "client-2"}
                ]
            },
            "retExtInfo": {
                "list": [
                    {"code": 0, "msg": "OK"},
                    {"code": 110001, "msg": "Order does not exist"}
                ]
            },
            "time": 1713434102753_i64
        }));
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(BybitPrivatePerpProtocol, transport);
        let first = CancelCommand {
            exchange: ExchangeId::Bybit,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT"),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: Some("order-1".to_string()),
            reason: None,
            requested_at: Utc::now(),
        };
        let second = CancelCommand {
            canonical_symbol: CanonicalSymbol::new("ETH", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Bybit, "ETHUSDT"),
            client_order_id: Some("client-2".to_string()),
            exchange_order_id: Some("order-2".to_string()),
            ..first.clone()
        };

        let ack = adapter
            .cancel_batch_orders(CancelBatchCommand::new(
                ExchangeId::Bybit,
                [first, second],
                Utc::now(),
            ))
            .await
            .unwrap();

        assert_cancel_batch_ack(&ack, ExchangeId::Bybit, false, 1, 2, None);
        assert!(ack.order_acks[0].accepted);
        assert_eq!(ack.order_acks[0].exchange, ExchangeId::Bybit);
        assert_eq!(
            ack.order_acks[0].client_order_id.as_deref(),
            Some("client-1")
        );
        assert_eq!(ack.order_acks[0].status, OrderCommandStatus::Cancelled);
        assert_eq!(
            ack.order_acks[0].exchange_order_id.as_deref(),
            Some("order-1")
        );
        assert_eq!(ack.order_acks[0].message.as_deref(), Some("OK"));
        assert!(!ack.order_acks[1].accepted);
        assert_eq!(ack.order_acks[1].exchange, ExchangeId::Bybit);
        assert_eq!(
            ack.order_acks[1].exchange_order_id.as_deref(),
            Some("order-2")
        );
        assert_eq!(ack.order_acks[1].status, OrderCommandStatus::Rejected);
        assert_eq!(
            ack.order_acks[1].client_order_id.as_deref(),
            Some("client-2")
        );
        assert_eq!(
            ack.order_acks[1].message.as_deref(),
            Some("Order does not exist")
        );

        let seen = seen.lock().unwrap();
        assert_eq!(seen[0].path, "/v5/order/cancel-batch");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["category"], "linear");
        assert_eq!(body["request"][0]["symbol"], "BTCUSDT");
        assert_eq!(body["request"][0]["orderId"], "order-1");
        assert_eq!(body["request"][0]["orderLinkId"], "client-1");
        assert_eq!(body["request"][1]["symbol"], "ETHUSDT");
        assert_eq!(body["request"][1]["orderId"], "order-2");
        assert_eq!(body["request"][1]["orderLinkId"], "client-2");
    }

    #[tokio::test]
    async fn bybit_adapter_should_route_positions_and_balances_readbacks() {
        let transport = SequentialMockTransport::new([
            json!({
                "retCode": 0,
                "retMsg": "OK",
                "result": {
                    "list": [
                        {
                            "symbol": "BTCUSDT",
                            "side": "Buy",
                            "size": "0.5",
                            "avgPrice": "65000",
                            "markPrice": "65100",
                            "unrealisedPnl": "50",
                            "updatedTime": "1700000000000"
                        }
                    ]
                }
            }),
            json!({
                "retCode": 0,
                "retMsg": "OK",
                "result": {
                    "list": [
                        {
                            "accountType": "UNIFIED",
                            "coin": [
                                {
                                    "coin": "USDT",
                                    "walletBalance": "1000",
                                    "availableToWithdraw": "850",
                                    "locked": "150"
                                }
                            ]
                        }
                    ]
                }
            }),
        ]);
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(BybitPrivatePerpProtocol, transport);
        let symbol = ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT");

        let positions = adapter.get_positions(Some(&symbol)).await.unwrap();
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].exchange, ExchangeId::Bybit);
        assert_eq!(positions[0].exchange_symbol, symbol);
        assert_eq!(positions[0].position_side, PositionSide::Long);
        assert_eq!(positions[0].quantity, 0.5);
        assert_eq!(positions[0].entry_price, Some(65_000.0));
        assert_eq!(positions[0].mark_price, Some(65_100.0));
        assert_eq!(positions[0].unrealized_pnl, Some(50.0));

        let balances = adapter.get_balances().await.unwrap();
        assert_eq!(balances.len(), 1);
        assert_eq!(balances[0].exchange, ExchangeId::Bybit);
        assert_eq!(balances[0].asset, "USDT");
        assert_eq!(balances[0].total, 1000.0);
        assert_eq!(balances[0].available, 850.0);
        assert_eq!(balances[0].locked, 150.0);

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].path, "/v5/position/list");
        assert_eq!(
            seen[0].query.get("category").map(String::as_str),
            Some("linear")
        );
        assert_eq!(
            seen[0].query.get("settleCoin").map(String::as_str),
            Some("USDT")
        );
        assert_eq!(
            seen[0].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
        assert_eq!(seen[1].path, "/v5/account/wallet-balance");
        assert_eq!(
            seen[1].query.get("accountType").map(String::as_str),
            Some("UNIFIED")
        );
        assert_eq!(seen[1].query.get("coin").map(String::as_str), Some("USDT"));
    }

    #[tokio::test]
    async fn bybit_adapter_should_route_fee_and_symbol_account_config_readbacks() {
        let transport = SequentialMockTransport::new([
            json!({
                "retCode": 0,
                "retMsg": "OK",
                "result": {
                    "list": [
                        {
                            "symbol": "BTCUSDT",
                            "makerFeeRate": "0.0001",
                            "takerFeeRate": "0.00055"
                        }
                    ]
                }
            }),
            json!({
                "retCode": 0,
                "retMsg": "OK",
                "result": {
                    "list": [
                        {
                            "symbol": "BTCUSDT",
                            "tradeMode": 0,
                            "leverage": "10",
                            "maxLeverage": "100"
                        }
                    ]
                }
            }),
        ]);
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(BybitPrivatePerpProtocol, transport)
            .with_position_mode(PositionMode::OneWay);
        let symbol = ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT");

        let fee = adapter.get_trade_fee(&symbol).await.unwrap();
        assert_eq!(fee.exchange, ExchangeId::Bybit);
        assert_eq!(fee.exchange_symbol, symbol);
        assert_eq!(fee.maker, 0.0001);
        assert_eq!(fee.taker, 0.00055);

        let config = adapter.get_symbol_account_config(&symbol).await.unwrap();
        assert_eq!(config.exchange, ExchangeId::Bybit);
        assert_eq!(config.exchange_symbol, symbol);
        assert_eq!(config.position_mode, Some(PositionMode::OneWay));
        assert_eq!(config.leverage, Some(10));
        assert_eq!(config.max_leverage, Some(100));

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].path, "/v5/account/fee-rate");
        assert_eq!(
            seen[0].query.get("category").map(String::as_str),
            Some("linear")
        );
        assert_eq!(
            seen[0].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
        assert_eq!(seen[1].path, "/v5/position/list");
        assert_eq!(
            seen[1].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
    }

    #[tokio::test]
    async fn bybit_adapter_should_place_batch_orders_and_split_ret_ext_info() {
        let transport = MockTransport::new(json!({
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": [
                    {"category": "linear", "symbol": "BTCUSDT", "orderId": "order-1", "orderLinkId": "client-1"},
                    {"category": "linear", "symbol": "ETHUSDT", "orderId": "", "orderLinkId": "client-2"}
                ]
            },
            "retExtInfo": {
                "list": [
                    {"code": 0, "msg": "OK"},
                    {"code": 110007, "msg": "Insufficient available balance"}
                ]
            },
            "time": 1713434102753_i64
        }));
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(BybitPrivatePerpProtocol, transport)
            .with_position_mode(PositionMode::Hedge);
        let mut first = command(ExchangeId::Bybit, "BTCUSDT");
        first.client_order_id = "client-1".to_string();
        let mut second = command(ExchangeId::Bybit, "ETHUSDT");
        second.client_order_id = "client-2".to_string();
        second.side = OrderSide::Sell;
        second.position_side = PositionSide::Short;

        let ack = adapter
            .place_batch_orders(BatchPlaceCommand::new(
                ExchangeId::Bybit,
                [first, second],
                Utc::now(),
            ))
            .await
            .unwrap();

        assert_batch_place_ack(&ack, ExchangeId::Bybit, false, 1, 1, Some("OK"));
        assert_eq!(ack.order_acks[0].exchange, ExchangeId::Bybit);
        assert_eq!(ack.order_acks[0].client_order_id, "client-1");
        assert_eq!(
            ack.order_acks[0].exchange_order_id.as_deref(),
            Some("order-1")
        );
        assert!(ack.order_acks[0].accepted);
        assert_eq!(ack.order_acks[0].status, OrderCommandStatus::Accepted);
        assert_eq!(ack.order_acks[0].message.as_deref(), Some("OK"));
        assert_eq!(ack.failed_orders[0].code.as_deref(), Some("110007"));
        assert_eq!(
            ack.failed_orders[0].message,
            "Insufficient available balance"
        );
        assert_eq!(ack.failed_orders[0].order.client_order_id, "client-2");
        assert_eq!(seen.lock().unwrap()[0].path, "/v5/order/create-batch");
    }

    #[test]
    fn mexc_should_build_full_private_rest_specs() {
        let cmd = command(ExchangeId::Mexc, "BTC_USDT");
        let place = MexcPrivatePerpProtocol
            .place_order(&cmd, PositionMode::Hedge)
            .unwrap();
        assert_eq!(place.path, "/api/v1/private/order/submit");
        let body = place.body.unwrap();
        assert_eq!(body["symbol"], "BTC_USDT");
        assert_eq!(body["side"], 1);
        assert_eq!(body["externalOid"], cmd.client_order_id);

        let mut second = cmd.clone();
        second.client_order_id = "client-2".to_string();
        second.side = OrderSide::Sell;
        second.position_side = PositionSide::Short;
        second.quantity = 2.0;
        second.price = Some(66_000.0);
        let batch_place = MexcPrivatePerpProtocol
            .place_batch_orders(
                &BatchPlaceCommand::new(ExchangeId::Mexc, [cmd.clone(), second], Utc::now()),
                PositionMode::Hedge,
            )
            .unwrap();
        assert_eq!(batch_place.path, "/api/v1/private/order/submit_batch");
        let batch_body = batch_place.body.unwrap();
        assert_eq!(batch_body[0]["symbol"], "BTC_USDT");
        assert_eq!(batch_body[0]["side"], 1);
        assert_eq!(batch_body[0]["type"], 2);
        assert_eq!(batch_body[0]["positionMode"], "hedge_mode");
        assert_eq!(batch_body[1]["side"], 3);
        assert_eq!(batch_body[1]["price"], "66000");
        assert_eq!(batch_body[1]["externalOid"], "client-2");

        let cancel = MexcPrivatePerpProtocol
            .cancel_order(&CancelCommand {
                exchange: ExchangeId::Mexc,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: Some("order-1".to_string()),
                reason: None,
                requested_at: Utc::now(),
            })
            .unwrap();
        assert_eq!(cancel.path, "/api/v1/private/order/cancel");
        let cancel_body = cancel.body.unwrap();
        assert_eq!(cancel_body["orderId"], "order-1");
        assert_eq!(cancel_body["externalOid"], "client-1");
        assert_eq!(cancel_body["symbol"], "BTC_USDT");

        let cancel_all = MexcPrivatePerpProtocol
            .cancel_all_orders(&CancelAllCommand::for_symbol(
                ExchangeId::Mexc,
                CanonicalSymbol::new("BTC", "USDT"),
                ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
                Utc::now(),
            ))
            .unwrap();
        assert_eq!(cancel_all.path, "/api/v1/private/order/cancel_all");
        assert_eq!(
            cancel_all.query.get("symbol").map(String::as_str),
            Some("BTC_USDT")
        );

        let batch = CancelBatchCommand::new(
            ExchangeId::Mexc,
            [CancelCommand {
                exchange: ExchangeId::Mexc,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
                client_order_id: None,
                exchange_order_id: Some("order-1".to_string()),
                reason: None,
                requested_at: Utc::now(),
            }],
            Utc::now(),
        );
        let batch_spec = MexcPrivatePerpProtocol.cancel_batch_orders(&batch).unwrap();
        assert_eq!(batch_spec.path, "/api/v1/private/order/cancel");
        assert_eq!(batch_spec.body.unwrap()[0], "order-1");

        let by_order_id = MexcPrivatePerpProtocol
            .get_order(&OrderQuery {
                exchange: ExchangeId::Mexc,
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
                client_order_id: None,
                exchange_order_id: Some("order-1".to_string()),
            })
            .unwrap();
        assert_eq!(by_order_id.path, "/api/v1/private/order/get/order-1");

        let by_client_id = MexcPrivatePerpProtocol
            .get_order(&OrderQuery {
                exchange: ExchangeId::Mexc,
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: None,
            })
            .unwrap();
        assert_eq!(
            by_client_id.path,
            "/api/v1/private/order/external/BTC_USDT/client-1"
        );

        let open_orders = MexcPrivatePerpProtocol
            .get_open_orders(Some(&ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT")))
            .unwrap();
        assert_eq!(
            open_orders.path,
            "/api/v1/private/order/list/open_orders/BTC_USDT"
        );

        let mut history = OrderHistoryQuery::for_symbol(
            ExchangeId::Mexc,
            ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
        );
        history.start_time = DateTime::<Utc>::from_timestamp_millis(1_700_000_000_000);
        history.end_time = DateTime::<Utc>::from_timestamp_millis(1_700_000_600_000);
        history.limit = Some(100);
        let history_spec = MexcPrivatePerpProtocol.get_all_orders(&history).unwrap();
        assert_eq!(history_spec.method, PrivateRestMethod::Get);
        assert_eq!(
            history_spec.path,
            "/api/v1/private/order/list/history_orders"
        );
        assert_eq!(
            history_spec.query.get("symbol").map(String::as_str),
            Some("BTC_USDT")
        );
        assert_eq!(
            history_spec.query.get("start_time").map(String::as_str),
            Some("1700000000000")
        );
        assert_eq!(
            history_spec.query.get("end_time").map(String::as_str),
            Some("1700000600000")
        );
        assert_eq!(
            history_spec.query.get("page_num").map(String::as_str),
            Some("1")
        );
        assert_eq!(
            history_spec.query.get("page_size").map(String::as_str),
            Some("100")
        );

        let mut fills = FillQuery::for_symbol(
            ExchangeId::Mexc,
            CanonicalSymbol::new("BTC", "USDT"),
            ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
        );
        fills.exchange_order_id = Some("order-1".to_string());
        fills.limit = Some(25);
        let fills_spec = MexcPrivatePerpProtocol.get_fills(&fills).unwrap();
        assert_eq!(
            fills_spec.path,
            "/api/v1/private/order/list/order_deals/BTC_USDT"
        );
        assert_eq!(
            fills_spec.query.get("order_id").map(String::as_str),
            Some("order-1")
        );
        assert_eq!(
            fills_spec.query.get("page_size").map(String::as_str),
            Some("25")
        );

        let positions = MexcPrivatePerpProtocol
            .get_positions(Some(&ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT")))
            .unwrap();
        assert_eq!(positions.path, "/api/v1/private/position/open_positions");
        assert_eq!(
            positions.query.get("symbol").map(String::as_str),
            Some("BTC_USDT")
        );

        assert_eq!(
            MexcPrivatePerpProtocol.get_balances().unwrap().path,
            "/api/v1/private/account/assets"
        );
        assert_eq!(
            MexcPrivatePerpProtocol
                .get_trade_fee(&ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"))
                .unwrap()
                .path,
            "/api/v1/private/account/tiered_fee_rate"
        );
        assert_eq!(
            MexcPrivatePerpProtocol
                .get_symbol_account_config(&ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"))
                .unwrap()
                .path,
            "/api/v1/private/position/leverage"
        );

        let amend = MexcPrivatePerpProtocol
            .amend_order(&AmendOrderCommand {
                exchange: ExchangeId::Mexc,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
                client_order_id: None,
                exchange_order_id: Some("order-1".to_string()),
                original_side: None,
                new_client_order_id: None,
                new_quantity: Some(0.02),
                new_price: Some(64_900.0),
                requested_at: Utc::now(),
            })
            .unwrap();
        assert_eq!(amend.path, "/api/v1/private/order/change_order_price");
        let amend_body = amend.body.unwrap();
        assert_eq!(amend_body["orderId"], "order-1");
        assert_eq!(amend_body["price"], "64900");
        assert_eq!(amend_body["vol"], "0.02");

        let leverage = MexcPrivatePerpProtocol
            .set_leverage(&LeverageCommand {
                exchange: ExchangeId::Mexc,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
                leverage: 3,
                requested_at: Utc::now(),
            })
            .unwrap();
        assert_eq!(leverage.path, "/api/v1/private/position/change_leverage");
        assert_eq!(leverage.body.unwrap()["leverage"], 3);

        let mode = MexcPrivatePerpProtocol
            .set_position_mode(&PositionModeCommand {
                exchange: ExchangeId::Mexc,
                mode: PositionMode::Hedge,
                requested_at: Utc::now(),
            })
            .unwrap();
        assert_eq!(mode.body.unwrap()["positionMode"], 1);
    }

    #[test]
    fn mexc_symbol_scoped_specs_should_validate_exchange_scope_and_batch_ids() {
        let wrong_symbol = ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT");
        let mut place = command(ExchangeId::Mexc, "BTC_USDT");
        place.exchange_symbol = wrong_symbol.clone();
        assert!(MexcPrivatePerpProtocol
            .place_order(&place, PositionMode::Hedge)
            .unwrap_err()
            .to_string()
            .contains("place order requires mexc exchange symbols"));

        let valid_place = command(ExchangeId::Mexc, "BTC_USDT");
        assert!(MexcPrivatePerpProtocol
            .place_batch_orders(
                &BatchPlaceCommand::new(ExchangeId::Gate, [valid_place.clone()], Utc::now()),
                PositionMode::Hedge,
            )
            .unwrap_err()
            .to_string()
            .contains("mexc batch-place requires mexc exchange symbols"));

        let mut wrong_batch_item = valid_place.clone();
        wrong_batch_item.exchange_symbol = wrong_symbol.clone();
        assert!(MexcPrivatePerpProtocol
            .place_batch_orders(
                &BatchPlaceCommand::new(ExchangeId::Mexc, [wrong_batch_item], Utc::now()),
                PositionMode::Hedge,
            )
            .unwrap_err()
            .to_string()
            .contains("mexc batch-place requires mexc exchange symbols"));

        assert!(MexcPrivatePerpProtocol
            .place_batch_orders(
                &BatchPlaceCommand::new(ExchangeId::Mexc, Vec::<OrderCommand>::new(), Utc::now()),
                PositionMode::Hedge,
            )
            .unwrap_err()
            .to_string()
            .contains("mexc batch-place requires at least one order"));

        assert!(MexcPrivatePerpProtocol
            .place_batch_orders(
                &BatchPlaceCommand::new(ExchangeId::Mexc, vec![valid_place; 51], Utc::now()),
                PositionMode::Hedge,
            )
            .unwrap_err()
            .to_string()
            .contains("mexc batch-place supports at most 50 orders"));

        let valid_cancel = CancelCommand {
            exchange: ExchangeId::Mexc,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
            client_order_id: None,
            exchange_order_id: Some("order-1".to_string()),
            reason: None,
            requested_at: Utc::now(),
        };
        let mut wrong_cancel = valid_cancel.clone();
        wrong_cancel.exchange_symbol = wrong_symbol.clone();
        assert!(MexcPrivatePerpProtocol
            .cancel_order(&wrong_cancel)
            .unwrap_err()
            .to_string()
            .contains("cancel requires mexc exchange symbols"));

        assert!(MexcPrivatePerpProtocol
            .cancel_all_orders(&CancelAllCommand {
                exchange: ExchangeId::Gate,
                canonical_symbol: None,
                exchange_symbol: None,
                requested_at: Utc::now(),
            })
            .unwrap_err()
            .to_string()
            .contains("cancel-all requires mexc exchange symbols"));

        let mut missing_batch_id = valid_cancel.clone();
        missing_batch_id.exchange_order_id = None;
        missing_batch_id.client_order_id = Some("client-1".to_string());
        assert!(MexcPrivatePerpProtocol
            .cancel_batch_orders(&CancelBatchCommand::new(
                ExchangeId::Mexc,
                [valid_cancel.clone(), missing_batch_id],
                Utc::now(),
            ))
            .unwrap_err()
            .to_string()
            .contains("exchange order ids for every order"));

        assert!(MexcPrivatePerpProtocol
            .cancel_batch_orders(&CancelBatchCommand::new(
                ExchangeId::Gate,
                [valid_cancel.clone()],
                Utc::now(),
            ))
            .unwrap_err()
            .to_string()
            .contains("mexc batch cancel requires mexc exchange symbols"));

        assert!(MexcPrivatePerpProtocol
            .get_order(&OrderQuery {
                exchange: ExchangeId::Mexc,
                exchange_symbol: wrong_symbol.clone(),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: None,
            })
            .unwrap_err()
            .to_string()
            .contains("query order requires mexc exchange symbols"));

        assert!(MexcPrivatePerpProtocol
            .get_open_orders(Some(&wrong_symbol))
            .unwrap_err()
            .to_string()
            .contains("open orders requires mexc exchange symbols"));

        assert!(MexcPrivatePerpProtocol
            .get_fills(&FillQuery::for_symbol(
                ExchangeId::Mexc,
                CanonicalSymbol::new("BTC", "USDT"),
                wrong_symbol.clone(),
            ))
            .unwrap_err()
            .to_string()
            .contains("fills requires mexc exchange symbols"));

        assert!(MexcPrivatePerpProtocol
            .get_positions(Some(&wrong_symbol))
            .unwrap_err()
            .to_string()
            .contains("positions requires mexc exchange symbols"));
        assert!(MexcPrivatePerpProtocol
            .get_trade_fee(&wrong_symbol)
            .unwrap_err()
            .to_string()
            .contains("trade fee requires mexc exchange symbols"));
        assert!(MexcPrivatePerpProtocol
            .get_symbol_account_config(&wrong_symbol)
            .unwrap_err()
            .to_string()
            .contains("symbol account config requires mexc exchange symbols"));

        assert!(MexcPrivatePerpProtocol
            .amend_order(&AmendOrderCommand {
                exchange: ExchangeId::Mexc,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: wrong_symbol.clone(),
                client_order_id: None,
                exchange_order_id: Some("order-1".to_string()),
                original_side: None,
                new_client_order_id: None,
                new_quantity: Some(0.02),
                new_price: Some(64_900.0),
                requested_at: Utc::now(),
            })
            .unwrap_err()
            .to_string()
            .contains("amend requires mexc exchange symbols"));

        assert!(MexcPrivatePerpProtocol
            .amend_order(&AmendOrderCommand {
                exchange: ExchangeId::Mexc,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
                client_order_id: None,
                exchange_order_id: Some("order-1".to_string()),
                original_side: None,
                new_client_order_id: None,
                new_quantity: None,
                new_price: None,
                requested_at: Utc::now(),
            })
            .unwrap_err()
            .to_string()
            .contains("amend requires new price or new quantity"));

        assert!(MexcPrivatePerpProtocol
            .set_leverage(&LeverageCommand {
                exchange: ExchangeId::Mexc,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: wrong_symbol,
                leverage: 3,
                requested_at: Utc::now(),
            })
            .unwrap_err()
            .to_string()
            .contains("set leverage requires mexc exchange symbols"));
    }

    #[test]
    fn mexc_countdown_cancel_should_be_explicitly_unsupported() {
        let countdown = MexcPrivatePerpProtocol.set_countdown_cancel_all(
            &CountdownCancelAllCommand::set_for_symbol(
                ExchangeId::Mexc,
                ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
                30,
                Utc::now(),
            ),
        );

        assert!(countdown
            .unwrap_err()
            .to_string()
            .contains("mexc countdown cancel-all is not supported"));

        let wrong_symbol = MexcPrivatePerpProtocol.set_countdown_cancel_all(
            &CountdownCancelAllCommand::set_for_symbol(
                ExchangeId::Mexc,
                ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"),
                30,
                Utc::now(),
            ),
        );
        assert!(wrong_symbol
            .unwrap_err()
            .to_string()
            .contains("countdown cancel-all requires mexc exchange symbols"));

        let wrong_exchange = MexcPrivatePerpProtocol.set_countdown_cancel_all(
            &CountdownCancelAllCommand::cancel(ExchangeId::Htx, Utc::now()),
        );
        assert!(wrong_exchange
            .unwrap_err()
            .to_string()
            .contains("mexc countdown cancel-all requires mexc exchange symbols"));
    }

    #[tokio::test]
    async fn mexc_adapter_should_parse_order_history() {
        let transport = MockTransport::new(json!({
            "success": true,
            "code": 0,
            "data": [
                {
                    "orderId": "102015012431820288",
                    "symbol": "BTC_USDT",
                    "price": "65000",
                    "vol": "2",
                    "side": 1,
                    "category": 1,
                    "orderType": 1,
                    "dealAvgPrice": "64990",
                    "dealVol": "1",
                    "state": 2,
                    "externalOid": "client-1",
                    "createTime": 1700000000000_i64,
                    "updateTime": 1700000001000_i64
                }
            ]
        }));
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(MexcPrivatePerpProtocol, transport);

        let orders = adapter
            .get_order_history(OrderHistoryQuery::for_symbol(
                ExchangeId::Mexc,
                ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
            ))
            .await
            .unwrap();

        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].exchange, ExchangeId::Mexc);
        assert_eq!(
            orders[0].exchange_order_id.as_deref(),
            Some("102015012431820288")
        );
        assert_eq!(orders[0].client_order_id.as_deref(), Some("client-1"));
        assert_eq!(orders[0].order_type, OrderType::Limit);
        assert_eq!(orders[0].status, OrderCommandStatus::PartiallyFilled);
        assert_eq!(orders[0].quantity, 2.0);
        assert_eq!(orders[0].filled_quantity, 1.0);
        assert_eq!(orders[0].average_fill_price, Some(64990.0));
        assert_eq!(
            seen.lock().unwrap()[0].path,
            "/api/v1/private/order/list/history_orders"
        );
    }

    #[tokio::test]
    async fn mexc_adapter_should_route_fee_and_symbol_account_config_readbacks() {
        let transport = SequentialMockTransport::new([
            json!({
                "success": true,
                "code": 0,
                "data": [
                    {"symbol":"BTC_USDT","makerFee":"0.0001","takerFee":"0.0005"}
                ]
            }),
            json!({
                "success": true,
                "code": 0,
                "data": [
                    {
                        "symbol":"BTC_USDT",
                        "positionMode":"long_short_mode",
                        "marginMode":"isolated",
                        "longLeverage":10,
                        "maxLeverage":"200"
                    }
                ]
            }),
        ]);
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(MexcPrivatePerpProtocol, transport)
            .with_position_mode(PositionMode::OneWay);
        let symbol = ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT");

        let fee = adapter.get_trade_fee(&symbol).await.unwrap();
        assert_eq!(fee.exchange, ExchangeId::Mexc);
        assert_eq!(fee.exchange_symbol, symbol);
        assert_eq!(fee.maker, 0.0001);
        assert_eq!(fee.taker, 0.0005);

        let config = adapter.get_symbol_account_config(&symbol).await.unwrap();
        assert_eq!(config.exchange, ExchangeId::Mexc);
        assert_eq!(config.exchange_symbol, symbol);
        assert_eq!(config.position_mode, Some(PositionMode::Hedge));
        assert_eq!(config.margin_mode, Some(MarginMode::Isolated));
        assert_eq!(config.leverage, Some(10));
        assert_eq!(config.max_leverage, Some(200));

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].path, "/api/v1/private/account/tiered_fee_rate");
        assert_eq!(
            seen[0].query.get("symbol").map(String::as_str),
            Some("BTC_USDT")
        );
        assert_eq!(seen[1].path, "/api/v1/private/position/leverage");
        assert_eq!(
            seen[1].query.get("symbol").map(String::as_str),
            Some("BTC_USDT")
        );
    }

    #[tokio::test]
    async fn mexc_adapter_should_route_positions_and_balances_readbacks() {
        let transport = SequentialMockTransport::new([
            json!({
                "success": true,
                "code": 0,
                "data": [
                    {
                        "symbol":"BTC_USDT",
                        "positionType":"short",
                        "holdVol":"2",
                        "holdAvgPrice":"65000",
                        "markPrice":"64900",
                        "unrealised":"12.5"
                    }
                ]
            }),
            json!({
                "success": true,
                "code": 0,
                "data": [
                    {
                        "currency":"USDT",
                        "equity":"1000",
                        "availableBalance":"850",
                        "frozenBalance":"150"
                    }
                ]
            }),
        ]);
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(MexcPrivatePerpProtocol, transport);
        let symbol = ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT");

        let positions = adapter.get_positions(Some(&symbol)).await.unwrap();
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].exchange, ExchangeId::Mexc);
        assert_eq!(positions[0].exchange_symbol, symbol);
        assert_eq!(positions[0].position_side, PositionSide::Short);
        assert_eq!(positions[0].quantity, 2.0);
        assert_eq!(positions[0].entry_price, Some(65_000.0));
        assert_eq!(positions[0].mark_price, Some(64_900.0));
        assert_eq!(positions[0].unrealized_pnl, Some(12.5));

        let balances = adapter.get_balances().await.unwrap();
        assert_eq!(balances.len(), 1);
        assert_eq!(balances[0].exchange, ExchangeId::Mexc);
        assert_eq!(balances[0].asset, "USDT");
        assert_eq!(balances[0].total, 1000.0);
        assert_eq!(balances[0].available, 850.0);
        assert_eq!(balances[0].locked, 150.0);

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].path, "/api/v1/private/position/open_positions");
        assert_eq!(
            seen[0].query.get("symbol").map(String::as_str),
            Some("BTC_USDT")
        );
        assert_eq!(seen[1].path, "/api/v1/private/account/assets");
    }

    #[tokio::test]
    async fn mexc_adapter_should_route_fills_readback() {
        let transport = MockTransport::new(json!({
            "success": true,
            "code": 0,
            "data": [
                {
                    "symbol":"BTC_USDT",
                    "tradeId":"trade-1",
                    "orderId":"order-1",
                    "externalOid":"client-1",
                    "side": 1,
                    "price":"65000",
                    "vol":"0.02",
                    "amount":"1300",
                    "fee":"0.65",
                    "feeCurrency":"USDT",
                    "role":"maker",
                    "profit":"1.5",
                    "time":1700000000000_i64
                }
            ]
        }));
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(MexcPrivatePerpProtocol, transport);
        let mut query = FillQuery::for_symbol(
            ExchangeId::Mexc,
            CanonicalSymbol::new("BTC", "USDT"),
            ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
        );
        query.exchange_order_id = Some("order-1".to_string());
        query.limit = Some(25);

        let fills = adapter.get_fills(query).await.unwrap();
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].exchange, ExchangeId::Mexc);
        assert_eq!(fills[0].exchange_symbol.symbol, "BTC_USDT");
        assert_eq!(fills[0].trade_id, "trade-1");
        assert_eq!(fills[0].client_order_id.as_deref(), Some("client-1"));
        assert_eq!(fills[0].exchange_order_id.as_deref(), Some("order-1"));
        assert_eq!(fills[0].side, OrderSide::Buy);
        assert_eq!(fills[0].position_side, PositionSide::Long);
        assert_eq!(fills[0].liquidity, FillLiquidity::Maker);
        assert_eq!(fills[0].price, 65_000.0);
        assert_eq!(fills[0].quantity, 0.02);
        assert_eq!(fills[0].quote_quantity, 1300.0);
        assert_eq!(fills[0].fee, Some(0.65));
        assert_eq!(fills[0].fee_asset.as_deref(), Some("USDT"));
        assert_eq!(fills[0].realized_pnl, Some(1.5));
        assert_eq!(fills[0].reduce_only, Some(false));

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(
            seen[0].path,
            "/api/v1/private/order/list/order_deals/BTC_USDT"
        );
        assert_eq!(
            seen[0].query.get("order_id").map(String::as_str),
            Some("order-1")
        );
        assert_eq!(
            seen[0].query.get("page_size").map(String::as_str),
            Some("25")
        );
    }

    #[tokio::test]
    async fn mexc_adapter_should_place_batch_orders_and_split_errors() {
        let transport = MockTransport::new(json!({
            "success": true,
            "code": 0,
            "data": [
                {
                    "externalOid": "client-1",
                    "orderId": "102015012431820288",
                    "errorCode": 0,
                    "errorMsg": "success"
                },
                {
                    "externalOid": "client-2",
                    "orderId": null,
                    "errorCode": 2011,
                    "errorMsg": "Order quantity error"
                }
            ]
        }));
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(MexcPrivatePerpProtocol, transport)
            .with_position_mode(PositionMode::Hedge);
        let mut first = command(ExchangeId::Mexc, "BTC_USDT");
        first.client_order_id = "client-1".to_string();
        let mut second = first.clone();
        second.client_order_id = "client-2".to_string();
        second.side = OrderSide::Sell;
        second.position_side = PositionSide::Short;

        let ack = adapter
            .place_batch_orders(BatchPlaceCommand::new(
                ExchangeId::Mexc,
                [first, second],
                Utc::now(),
            ))
            .await
            .unwrap();

        assert_batch_place_ack(&ack, ExchangeId::Mexc, false, 1, 1, None);
        assert_eq!(ack.order_acks[0].exchange, ExchangeId::Mexc);
        assert_eq!(ack.order_acks[0].client_order_id, "client-1");
        assert_eq!(
            ack.order_acks[0].exchange_order_id.as_deref(),
            Some("102015012431820288")
        );
        assert!(ack.order_acks[0].accepted);
        assert_eq!(ack.order_acks[0].status, OrderCommandStatus::Accepted);
        assert_eq!(ack.order_acks[0].message.as_deref(), Some("success"));
        assert_eq!(ack.failed_orders[0].order.client_order_id, "client-2");
        assert_eq!(ack.failed_orders[0].code.as_deref(), Some("2011"));
        assert_eq!(ack.failed_orders[0].message, "Order quantity error");

        let seen = seen.lock().unwrap();
        assert_eq!(seen[0].path, "/api/v1/private/order/submit_batch");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body[0]["externalOid"], "client-1");
        assert_eq!(body[0]["positionMode"], "hedge_mode");
        assert_eq!(body[1]["side"], 3);
    }

    #[tokio::test]
    async fn mexc_adapter_should_batch_cancel_by_order_ids_and_split_errors() {
        let transport = MockTransport::new(json!({
            "success": true,
            "code": 0,
            "data": [
                {
                    "orderId": "exchange-1",
                    "externalOid": "client-1",
                    "errorCode": 0,
                    "errorMsg": "success"
                },
                {
                    "orderId": "exchange-2",
                    "externalOid": "client-2",
                    "errorCode": 2009,
                    "errorMsg": "Order does not exist"
                }
            ]
        }));
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(MexcPrivatePerpProtocol, transport);
        let first = CancelCommand {
            exchange: ExchangeId::Mexc,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: Some("exchange-1".to_string()),
            reason: None,
            requested_at: Utc::now(),
        };
        let mut second = first.clone();
        second.client_order_id = Some("client-2".to_string());
        second.exchange_order_id = Some("exchange-2".to_string());

        let ack = adapter
            .cancel_batch_orders(CancelBatchCommand::new(
                ExchangeId::Mexc,
                [first, second],
                Utc::now(),
            ))
            .await
            .unwrap();

        assert_cancel_batch_ack(&ack, ExchangeId::Mexc, false, 1, 2, None);
        assert_eq!(ack.order_acks[0].exchange, ExchangeId::Mexc);
        assert!(ack.order_acks[0].accepted);
        assert_eq!(ack.order_acks[0].status, OrderCommandStatus::Cancelled);
        assert_eq!(
            ack.order_acks[0].exchange_order_id.as_deref(),
            Some("exchange-1")
        );
        assert_eq!(
            ack.order_acks[0].client_order_id.as_deref(),
            Some("client-1")
        );
        assert_eq!(ack.order_acks[0].message.as_deref(), Some("success"));
        assert_eq!(ack.order_acks[1].exchange, ExchangeId::Mexc);
        assert!(!ack.order_acks[1].accepted);
        assert_eq!(ack.order_acks[1].status, OrderCommandStatus::Rejected);
        assert_eq!(
            ack.order_acks[1].exchange_order_id.as_deref(),
            Some("exchange-2")
        );
        assert_eq!(
            ack.order_acks[1].client_order_id.as_deref(),
            Some("client-2")
        );
        assert_eq!(
            ack.order_acks[1].message.as_deref(),
            Some("Order does not exist")
        );

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].path, "/api/v1/private/order/cancel");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body[0], "exchange-1");
        assert_eq!(body[1], "exchange-2");
    }

    #[tokio::test]
    async fn mexc_adapter_should_compose_client_id_batch_cancel() {
        let transport = SequentialMockTransport::new([
            json!({
                "success": true,
                "code": 0,
                "data": {
                    "orderId": "exchange-1",
                    "externalOid": "client-1"
                }
            }),
            json!({
                "success": true,
                "code": 0,
                "data": {
                    "orderId": "exchange-2",
                    "externalOid": "client-2"
                }
            }),
        ]);
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(MexcPrivatePerpProtocol, transport);
        let first = CancelCommand {
            exchange: ExchangeId::Mexc,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: None,
            reason: None,
            requested_at: Utc::now(),
        };
        let second = CancelCommand {
            exchange: ExchangeId::Mexc,
            canonical_symbol: CanonicalSymbol::new("ETH", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Mexc, "ETH_USDT"),
            client_order_id: Some("client-2".to_string()),
            exchange_order_id: None,
            reason: None,
            requested_at: Utc::now(),
        };

        let ack = adapter
            .cancel_batch_orders(CancelBatchCommand::new(
                ExchangeId::Mexc,
                [first, second],
                Utc::now(),
            ))
            .await
            .unwrap();

        assert!(ack.accepted);
        assert_eq!(ack.cancelled_orders, 2);
        assert_eq!(ack.order_acks.len(), 2);
        assert_eq!(
            ack.order_acks[0].client_order_id.as_deref(),
            Some("client-1")
        );
        assert_eq!(
            ack.order_acks[0].exchange_order_id.as_deref(),
            Some("exchange-1")
        );
        assert_eq!(
            ack.order_acks[1].client_order_id.as_deref(),
            Some("client-2")
        );
        assert_eq!(
            ack.order_acks[1].exchange_order_id.as_deref(),
            Some("exchange-2")
        );

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].path, "/api/v1/private/order/cancel");
        let first_body = seen[0].body.as_ref().unwrap();
        assert_eq!(first_body["externalOid"], "client-1");
        assert_eq!(first_body["symbol"], "BTC_USDT");
        assert_eq!(seen[1].path, "/api/v1/private/order/cancel");
        let second_body = seen[1].body.as_ref().unwrap();
        assert_eq!(second_body["externalOid"], "client-2");
        assert_eq!(second_body["symbol"], "ETH_USDT");
    }

    #[tokio::test]
    async fn mexc_adapter_should_reject_composed_batch_cancel_missing_identifiers_before_rest() {
        let transport = MockTransport::new(json!({
            "success": true,
            "code": 0,
            "data": {"orderId": "exchange-1", "externalOid": "client-1"}
        }));
        let adapter = PrivatePerpTradingAdapter::new(MexcPrivatePerpProtocol, transport.clone());
        let first = CancelCommand {
            exchange: ExchangeId::Mexc,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: None,
            reason: None,
            requested_at: Utc::now(),
        };
        let second = CancelCommand {
            exchange: ExchangeId::Mexc,
            canonical_symbol: CanonicalSymbol::new("ETH", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Mexc, "ETH_USDT"),
            client_order_id: None,
            exchange_order_id: None,
            reason: None,
            requested_at: Utc::now(),
        };

        let err = adapter
            .cancel_batch_orders(CancelBatchCommand::new(
                ExchangeId::Mexc,
                [first, second],
                Utc::now(),
            ))
            .await
            .unwrap_err();

        assert!(err.to_string().contains(
            "mexc composed batch cancel requires orderId or externalOid for every order"
        ));
        assert!(transport.seen.lock().unwrap().is_empty());
    }

    #[test]
    fn htx_should_build_full_private_rest_specs() {
        let cmd = command(ExchangeId::Htx, "BTC-USDT");
        let place = HtxPrivatePerpProtocol
            .place_order(&cmd, PositionMode::OneWay)
            .unwrap();
        assert_eq!(place.path, "/linear-swap-api/v1/swap_cross_order");
        let body = place.body.unwrap();
        assert_eq!(body["contract_code"], "BTC-USDT");
        assert_eq!(body["direction"], "buy");
        assert_eq!(body["offset"], "open");

        let mut second = cmd.clone();
        second.client_order_id = "client-2".to_string();
        second.side = OrderSide::Sell;
        second.position_side = PositionSide::Short;
        second.quantity = 0.02;
        second.price = Some(66_000.0);
        second.reduce_only = true;
        let batch_place = HtxPrivatePerpProtocol
            .place_batch_orders(
                &BatchPlaceCommand::new(ExchangeId::Htx, [cmd.clone(), second], Utc::now()),
                PositionMode::Hedge,
            )
            .unwrap();
        assert_eq!(
            batch_place.path,
            "/linear-swap-api/v1/swap_cross_batchorder"
        );
        let batch_body = batch_place.body.unwrap();
        assert_eq!(batch_body["orders_data"][0]["contract_code"], "BTC-USDT");
        assert_eq!(batch_body["orders_data"][0]["direction"], "buy");
        assert_eq!(batch_body["orders_data"][0]["offset"], "open");
        assert_eq!(
            batch_body["orders_data"][0]["order_price_type"],
            "post_only"
        );
        assert!(batch_body["orders_data"][0]["client_order_id"]
            .as_i64()
            .is_some());
        assert_eq!(batch_body["orders_data"][1]["direction"], "sell");
        assert_eq!(batch_body["orders_data"][1]["offset"], "close");
        assert_eq!(batch_body["orders_data"][1]["volume"], "0.02");

        let cancel = HtxPrivatePerpProtocol
            .cancel_order(&CancelCommand {
                exchange: ExchangeId::Htx,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: Some("order-1".to_string()),
                reason: None,
                requested_at: Utc::now(),
            })
            .unwrap();
        assert_eq!(cancel.path, "/linear-swap-api/v1/swap_cross_cancel");
        let cancel_body = cancel.body.unwrap();
        assert_eq!(cancel_body["contract_code"], "BTC-USDT");
        assert_eq!(cancel_body["order_id"], "order-1");
        assert!(cancel_body["client_order_id"]
            .as_str()
            .is_some_and(|id| !id.is_empty()));

        let cancel_all = HtxPrivatePerpProtocol
            .cancel_all_orders(&CancelAllCommand::for_symbol(
                ExchangeId::Htx,
                CanonicalSymbol::new("BTC", "USDT"),
                ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"),
                Utc::now(),
            ))
            .unwrap();
        assert_eq!(cancel_all.path, "/linear-swap-api/v1/swap_cross_cancelall");
        assert_eq!(cancel_all.body.unwrap()["contract_code"], "BTC-USDT");

        let batch = CancelBatchCommand::new(
            ExchangeId::Htx,
            [
                CancelCommand {
                    exchange: ExchangeId::Htx,
                    canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                    exchange_symbol: ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"),
                    client_order_id: None,
                    exchange_order_id: Some("order-1".to_string()),
                    reason: None,
                    requested_at: Utc::now(),
                },
                CancelCommand {
                    exchange: ExchangeId::Htx,
                    canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                    exchange_symbol: ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"),
                    client_order_id: None,
                    exchange_order_id: Some("order-2".to_string()),
                    reason: None,
                    requested_at: Utc::now(),
                },
            ],
            Utc::now(),
        );
        let batch_spec = HtxPrivatePerpProtocol.cancel_batch_orders(&batch).unwrap();
        assert_eq!(batch_spec.path, "/linear-swap-api/v1/swap_cross_cancel");
        let batch_body = batch_spec.body.unwrap();
        assert_eq!(batch_body["contract_code"], "BTC-USDT");
        assert_eq!(batch_body["order_id"], "order-1,order-2");

        let client_batch = CancelBatchCommand::new(
            ExchangeId::Htx,
            [
                CancelCommand {
                    exchange: ExchangeId::Htx,
                    canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                    exchange_symbol: ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"),
                    client_order_id: Some("client-1".to_string()),
                    exchange_order_id: None,
                    reason: None,
                    requested_at: Utc::now(),
                },
                CancelCommand {
                    exchange: ExchangeId::Htx,
                    canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                    exchange_symbol: ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"),
                    client_order_id: Some("client-2".to_string()),
                    exchange_order_id: None,
                    reason: None,
                    requested_at: Utc::now(),
                },
            ],
            Utc::now(),
        );
        let client_batch_spec = HtxPrivatePerpProtocol
            .cancel_batch_orders(&client_batch)
            .unwrap();
        let client_batch_body = client_batch_spec.body.unwrap();
        assert_eq!(
            client_batch_body["client_order_id"],
            format!(
                "{},{}",
                htx_client_order_id("client-1"),
                htx_client_order_id("client-2")
            )
        );
        assert!(client_batch_body.get("order_id").is_none());

        let order = HtxPrivatePerpProtocol
            .get_order(&OrderQuery {
                exchange: ExchangeId::Htx,
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"),
                client_order_id: None,
                exchange_order_id: Some("order-1".to_string()),
            })
            .unwrap();
        assert_eq!(order.path, "/linear-swap-api/v1/swap_cross_order_info");
        assert_eq!(order.body.unwrap()["order_id"], "order-1");

        let open_orders = HtxPrivatePerpProtocol
            .get_open_orders(Some(&ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT")))
            .unwrap();
        assert_eq!(
            open_orders.path,
            "/linear-swap-api/v1/swap_cross_openorders"
        );
        let open_body = open_orders.body.unwrap();
        assert_eq!(open_body["contract_code"], "BTC-USDT");
        assert_eq!(open_body["page_size"], 50);

        let mut history_query = OrderHistoryQuery::for_symbol(
            ExchangeId::Htx,
            ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"),
        );
        history_query.start_time = DateTime::<Utc>::from_timestamp_millis(1_700_000_000_000);
        history_query.end_time = DateTime::<Utc>::from_timestamp_millis(1_700_086_400_000);
        let history = HtxPrivatePerpProtocol
            .get_all_orders(&history_query)
            .unwrap();
        assert_eq!(history.path, "/linear-swap-api/v3/swap_cross_hisorders");
        let history_body = history.body.unwrap();
        assert_eq!(history_body["contract"], "BTC-USDT");
        assert_eq!(history_body["trade_type"], 0);
        assert_eq!(history_body["status"], "0");
        assert_eq!(history_body["type"], 1);
        assert_eq!(history_body["direct"], "prev");
        assert_eq!(history_body["start_time"], 1_700_000_000_000_i64);
        assert_eq!(history_body["end_time"], 1_700_086_400_000_i64);

        let fills = HtxPrivatePerpProtocol
            .get_fills(&FillQuery::for_symbol(
                ExchangeId::Htx,
                CanonicalSymbol::new("BTC", "USDT"),
                ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"),
            ))
            .unwrap();
        assert_eq!(fills.path, "/linear-swap-api/v1/swap_cross_matchresults");
        assert_eq!(fills.body.unwrap()["contract_code"], "BTC-USDT");

        let positions = HtxPrivatePerpProtocol
            .get_positions(Some(&ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT")))
            .unwrap();
        assert_eq!(
            positions.path,
            "/linear-swap-api/v1/swap_cross_position_info"
        );
        assert_eq!(positions.body.unwrap()["contract_code"], "BTC-USDT");

        assert_eq!(
            HtxPrivatePerpProtocol.get_balances().unwrap().path,
            "/linear-swap-api/v1/swap_cross_account_info"
        );
        assert_eq!(
            HtxPrivatePerpProtocol
                .get_trade_fee(&ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"))
                .unwrap()
                .path,
            "/linear-swap-api/v1/swap_fee"
        );
        assert_eq!(
            HtxPrivatePerpProtocol
                .get_symbol_account_config(&ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"))
                .unwrap()
                .path,
            "/linear-swap-api/v1/swap_cross_position_info"
        );

        let leverage = HtxPrivatePerpProtocol
            .set_leverage(&LeverageCommand {
                exchange: ExchangeId::Htx,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"),
                leverage: 3,
                requested_at: Utc::now(),
            })
            .unwrap();
        assert_eq!(
            leverage.path,
            "/linear-swap-api/v1/swap_cross_switch_lever_rate"
        );

        let mode = HtxPrivatePerpProtocol
            .set_position_mode(&PositionModeCommand {
                exchange: ExchangeId::Htx,
                mode: PositionMode::Hedge,
                requested_at: Utc::now(),
            })
            .unwrap();
        assert_eq!(
            mode.path,
            "/linear-swap-api/v1/swap_cross_switch_position_mode"
        );
        let mode_body = mode.body.unwrap();
        assert_eq!(mode_body["margin_account"], "USDT");
        assert_eq!(mode_body["position_mode"], "dual_side");
    }

    #[test]
    fn toobit_should_build_private_perp_rest_specs() {
        let cmd = command(ExchangeId::Toobit, "BTC-SWAP-USDT");
        let place = ToobitPrivatePerpProtocol
            .place_order(&cmd, PositionMode::Hedge)
            .unwrap();
        assert_eq!(place.exchange, ExchangeId::Toobit);
        assert_eq!(place.method, PrivateRestMethod::Post);
        assert_eq!(place.path, "/api/v2/futures/order");
        let body = place.body.unwrap();
        assert_eq!(body["symbol"], "BTC-SWAP-USDT");
        assert_eq!(body["side"], "BUY");
        assert_eq!(body["positionSide"], "LONG");
        assert_eq!(body["type"], "LIMIT_MAKER");
        assert_eq!(body["quantity"], "0.01");
        assert_eq!(body["price"], "65000");
        assert_eq!(body["newClientOrderId"], cmd.client_order_id);
        assert!(body.get("timeInForce").is_none());

        let batch_place = ToobitPrivatePerpProtocol
            .place_batch_orders(
                &BatchPlaceCommand::new(ExchangeId::Toobit, [cmd.clone()], Utc::now()),
                PositionMode::Hedge,
            )
            .unwrap();
        assert_eq!(batch_place.method, PrivateRestMethod::Post);
        assert_eq!(batch_place.path, "/api/v2/futures/batch-orders");
        let batch_place_orders = batch_place.body.as_ref().and_then(Value::as_array).unwrap();
        assert_eq!(batch_place_orders[0]["symbol"], "BTC-SWAP-USDT");
        assert_eq!(
            batch_place_orders[0]["newClientOrderId"],
            cmd.client_order_id
        );

        let cancel = ToobitPrivatePerpProtocol
            .cancel_order(&CancelCommand {
                exchange: ExchangeId::Toobit,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Toobit, "BTC-SWAP-USDT"),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: Some("order-1".to_string()),
                reason: None,
                requested_at: Utc::now(),
            })
            .unwrap();
        assert_eq!(cancel.method, PrivateRestMethod::Delete);
        assert_eq!(cancel.path, "/api/v2/futures/order");
        assert_eq!(cancel.query["symbol"], "BTC-SWAP-USDT");
        assert_eq!(cancel.query["orderId"], "order-1");
        assert_eq!(cancel.query["origClientOrderId"], "client-1");

        let batch = ToobitPrivatePerpProtocol
            .cancel_batch_orders(&CancelBatchCommand::new(
                ExchangeId::Toobit,
                [
                    CancelCommand {
                        exchange: ExchangeId::Toobit,
                        canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                        exchange_symbol: ExchangeSymbol::new(ExchangeId::Toobit, "BTC-SWAP-USDT"),
                        client_order_id: None,
                        exchange_order_id: Some("order-1".to_string()),
                        reason: None,
                        requested_at: Utc::now(),
                    },
                    CancelCommand {
                        exchange: ExchangeId::Toobit,
                        canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                        exchange_symbol: ExchangeSymbol::new(ExchangeId::Toobit, "BTC-SWAP-USDT"),
                        client_order_id: Some("client-2".to_string()),
                        exchange_order_id: Some("order-2".to_string()),
                        reason: None,
                        requested_at: Utc::now(),
                    },
                ],
                Utc::now(),
            ))
            .unwrap();
        assert_eq!(batch.method, PrivateRestMethod::Delete);
        assert_eq!(batch.path, "/api/v1/futures/cancelOrderByIds");
        assert_eq!(batch.query["ids"], "order-1,order-2");

        let cancel_all = ToobitPrivatePerpProtocol
            .cancel_all_orders(&CancelAllCommand::for_symbol(
                ExchangeId::Toobit,
                CanonicalSymbol::new("BTC", "USDT"),
                ExchangeSymbol::new(ExchangeId::Toobit, "BTC-SWAP-USDT"),
                Utc::now(),
            ))
            .unwrap();
        assert_eq!(cancel_all.path, "/api/v2/futures/batch-orders");
        assert_eq!(cancel_all.query["symbol"], "BTC-SWAP-USDT");

        let order = ToobitPrivatePerpProtocol
            .get_order(&OrderQuery {
                exchange: ExchangeId::Toobit,
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Toobit, "BTC-SWAP-USDT"),
                client_order_id: None,
                exchange_order_id: Some("order-1".to_string()),
            })
            .unwrap();
        assert_eq!(order.path, "/api/v2/futures/order");
        assert_eq!(order.query["orderId"], "order-1");

        let open = ToobitPrivatePerpProtocol
            .get_open_orders(Some(&ExchangeSymbol::new(
                ExchangeId::Toobit,
                "BTC-SWAP-USDT",
            )))
            .unwrap();
        assert_eq!(open.path, "/api/v2/futures/open-orders");
        assert_eq!(open.query["symbol"], "BTC-SWAP-USDT");

        let mut history = OrderHistoryQuery::for_symbol(
            ExchangeId::Toobit,
            ExchangeSymbol::new(ExchangeId::Toobit, "BTC-SWAP-USDT"),
        );
        history.limit = Some(100);
        history.start_time = DateTime::<Utc>::from_timestamp_millis(1_700_000_000_000);
        let history_spec = ToobitPrivatePerpProtocol.get_all_orders(&history).unwrap();
        assert_eq!(history_spec.path, "/api/v2/futures/history-orders");
        assert_eq!(history_spec.query["limit"], "100");
        assert_eq!(history_spec.query["startTime"], "1700000000000");

        let fills = ToobitPrivatePerpProtocol
            .get_fills(&FillQuery::for_symbol(
                ExchangeId::Toobit,
                CanonicalSymbol::new("BTC", "USDT"),
                ExchangeSymbol::new(ExchangeId::Toobit, "BTC-SWAP-USDT"),
            ))
            .unwrap();
        assert_eq!(fills.path, "/api/v2/futures/user-trades");
        assert_eq!(fills.query["symbol"], "BTC-SWAP-USDT");

        let positions = ToobitPrivatePerpProtocol
            .get_positions(Some(&ExchangeSymbol::new(
                ExchangeId::Toobit,
                "BTC-SWAP-USDT",
            )))
            .unwrap();
        assert_eq!(positions.path, "/api/v1/futures/positions");
        assert_eq!(positions.query["symbol"], "BTC-SWAP-USDT");

        assert_eq!(
            ToobitPrivatePerpProtocol.get_balances().unwrap().path,
            "/api/v1/futures/balance"
        );
        assert_eq!(
            ToobitPrivatePerpProtocol
                .get_trade_fee(&ExchangeSymbol::new(ExchangeId::Toobit, "BTC-SWAP-USDT"))
                .unwrap()
                .path,
            "/api/v1/futures/commissionRate"
        );
        assert_eq!(
            ToobitPrivatePerpProtocol
                .get_symbol_account_config(&ExchangeSymbol::new(
                    ExchangeId::Toobit,
                    "BTC-SWAP-USDT"
                ))
                .unwrap()
                .path,
            "/api/v1/futures/accountLeverage"
        );

        let amend = ToobitPrivatePerpProtocol
            .amend_order(&AmendOrderCommand {
                exchange: ExchangeId::Toobit,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Toobit, "BTC-SWAP-USDT"),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: None,
                original_side: Some(OrderSide::Buy),
                new_client_order_id: None,
                new_quantity: Some(0.02),
                new_price: Some(64_900.0),
                requested_at: Utc::now(),
            })
            .unwrap();
        assert_eq!(amend.path, "/api/v2/futures/order/update");
        let amend_body = amend.body.unwrap();
        assert_eq!(amend_body["origClientOrderId"], "client-1");
        assert_eq!(amend_body["quantity"], "0.02");
        assert_eq!(amend_body["price"], "64900");

        let leverage = ToobitPrivatePerpProtocol
            .set_leverage(&LeverageCommand {
                exchange: ExchangeId::Toobit,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Toobit, "BTC-SWAP-USDT"),
                leverage: 5,
                requested_at: Utc::now(),
            })
            .unwrap();
        assert_eq!(leverage.path, "/api/v2/futures/leverage");
        assert_eq!(leverage.body.unwrap()["leverage"], 5);
    }

    #[test]
    fn toobit_private_perp_should_sign_and_parse_readbacks() {
        let auth = PrivateRestAuth {
            api_key: "key".to_string(),
            api_secret: "secret".to_string(),
            passphrase: None,
            demo_trading: false,
        };
        let request = PrivateRestRequestSpec::new(
            ExchangeId::Toobit,
            PrivateRestMethod::Post,
            "/api/v2/futures/order",
        )
        .with_query("symbol", "BTC-SWAP-USDT")
        .with_body(json!({"symbol":"BTC-SWAP-USDT","side":"BUY"}));
        let signed = toobit_signed_request(&auth, request, 1_700_000_000_000).unwrap();
        assert_eq!(signed.query["recvWindow"], "5000");
        assert_eq!(signed.query["timestamp"], "1700000000000");
        let mut signed_without_sig = signed.clone();
        signed_without_sig.query.remove("signature");
        let expected_payload = format!(
            "{}{}",
            signed_without_sig.raw_query_string(),
            signed_without_sig.body_string()
        );
        assert_eq!(
            signed.query["signature"],
            toobit_signature("secret", &expected_payload)
        );
        let headers = toobit_rest_headers(&auth, &signed, 1_700_000_000_000).unwrap();
        assert_eq!(headers["X-BB-APIKEY"], "key");

        let now = Utc::now();
        let order = parse_rest_order_with_gate_contract_size(
            ExchangeId::Toobit,
            &ExchangeSymbol::new(ExchangeId::Toobit, "BTC-SWAP-USDT"),
            &json!({
                "code": 0,
                "data": {
                    "symbol": "BTC-SWAP-USDT",
                    "orderId": "order-1",
                    "clientOrderId": "client-1",
                    "side": "BUY",
                    "positionSide": "LONG",
                    "type": "LIMIT",
                    "origQty": "0.02",
                    "price": "65000",
                    "executedQty": "0.01",
                    "avgPrice": "65010",
                    "timeInForce": "GTC",
                    "reduceOnly": false,
                    "status": "PARTIALLY_FILLED",
                    "updateTime": 1700000000000_i64
                }
            }),
            now,
            None,
        )
        .unwrap();
        assert_eq!(order.exchange, ExchangeId::Toobit);
        assert_eq!(order.exchange_order_id.as_deref(), Some("order-1"));
        assert_eq!(order.position_side, PositionSide::Long);
        assert_eq!(order.status, OrderCommandStatus::PartiallyFilled);
        assert_eq!(order.filled_quantity, 0.01);

        let fill = parse_toobit_fill(
            &json!({
                "symbol": "BTC-SWAP-USDT",
                "id": "trade-1",
                "orderId": "order-1",
                "clientOrderId": "client-1",
                "isBuyer": true,
                "positionSide": "LONG",
                "price": "65000",
                "qty": "0.02",
                "quoteQty": "1300",
                "commission": "0.65",
                "commissionAsset": "USDT",
                "isMaker": true,
                "realizedPnl": "1.2",
                "time": 1700000001000_i64
            }),
            now,
        )
        .and_then(event_fill)
        .unwrap();
        assert_eq!(fill.exchange, ExchangeId::Toobit);
        assert_eq!(fill.liquidity, FillLiquidity::Maker);
        assert_eq!(fill.fee, Some(0.65));

        let position = parse_toobit_position(
            &json!({
                "symbol": "BTC-SWAP-USDT",
                "positionSide": "SHORT",
                "positionAmt": "-0.5",
                "entryPrice": "64000",
                "markPrice": "65000",
                "unrealizedPnl": "-5"
            }),
            now,
        )
        .and_then(event_position)
        .unwrap();
        assert_eq!(position.position_side, PositionSide::Short);
        assert_eq!(position.quantity, 0.5);

        let balance = parse_toobit_balance(
            &json!({
                "asset": "USDT",
                "balance": "1000",
                "availableBalance": "850",
                "locked": "150"
            }),
            now,
        )
        .and_then(event_balance)
        .unwrap();
        assert_eq!(balance.asset, "USDT");
        assert_eq!(balance.total, 1000.0);
        assert_eq!(balance.available, 850.0);
        assert_eq!(balance.locked, 150.0);
    }

    #[tokio::test]
    async fn toobit_adapter_cancel_all_should_route_native_symbol_cancel_all() {
        let transport = SequentialMockTransport::new([json!({
            "code": 0,
            "data": [
                {"orderId": "order-1", "clientOrderId": "client-1"},
                {"orderId": "order-2", "clientOrderId": "client-2"}
            ]
        })]);
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(ToobitPrivatePerpProtocol, transport);

        let ack = adapter
            .cancel_all_orders(CancelAllCommand::for_symbol(
                ExchangeId::Toobit,
                CanonicalSymbol::new("BTC", "USDT"),
                ExchangeSymbol::new(ExchangeId::Toobit, "BTC-SWAP-USDT"),
                Utc::now(),
            ))
            .await
            .unwrap();

        assert!(ack.accepted);
        assert_eq!(ack.exchange, ExchangeId::Toobit);
        assert_eq!(ack.cancelled_orders, 2);
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].path, "/api/v2/futures/batch-orders");
        assert_eq!(seen[0].method, PrivateRestMethod::Delete);
        assert_eq!(seen[0].query["symbol"], "BTC-SWAP-USDT");
    }

    #[test]
    fn toobit_private_perp_capabilities_should_match_supported_surface() {
        let capabilities = private_perp_trading_capabilities(ExchangeId::Toobit);
        assert!(capabilities.supports_market_orders);
        assert!(capabilities.supports_limit_orders);
        assert!(capabilities.supports_post_only);
        assert!(capabilities.supports_reduce_only);
        assert!(capabilities.supports_hedge_mode);
        assert!(capabilities.supports_leverage);
        assert!(capabilities.supports_close_position);
        assert!(!capabilities.supports_position_mode_change);
        assert!(!capabilities.supports_countdown_cancel_all);
        assert!(capabilities.supports_batch_place_orders);

        assert!(ToobitPrivatePerpProtocol
            .set_position_mode(&PositionModeCommand {
                exchange: ExchangeId::Toobit,
                mode: PositionMode::Hedge,
                requested_at: Utc::now(),
            })
            .unwrap_err()
            .to_string()
            .contains("position mode switching"));
        let endpoint = build_private_ws_endpoint(
            PrivatePerpExchange::Toobit,
            PrivateWsAuth {
                api_key: "key".to_string(),
                api_secret: "secret".to_string(),
                passphrase: None,
                account_id: Some("listen-key-1".to_string()),
                demo_trading: false,
            },
            &[ExchangeSymbol::new(ExchangeId::Toobit, "BTC-SWAP-USDT")],
            1_700_000_000,
        )
        .unwrap();
        assert_eq!(
            endpoint.url,
            "wss://stream.toobit.com/api/v1/ws/listen-key-1"
        );
    }

    #[test]
    fn htx_cancel_should_validate_required_identifiers_and_symbol_scope() {
        let missing_id = HtxPrivatePerpProtocol.cancel_order(&CancelCommand {
            exchange: ExchangeId::Htx,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"),
            client_order_id: None,
            exchange_order_id: None,
            reason: None,
            requested_at: Utc::now(),
        });
        assert!(missing_id
            .unwrap_err()
            .to_string()
            .contains("requires order_id or client_order_id"));

        let missing_query_id = HtxPrivatePerpProtocol.get_order(&OrderQuery {
            exchange: ExchangeId::Htx,
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"),
            client_order_id: None,
            exchange_order_id: None,
        });
        assert!(missing_query_id
            .unwrap_err()
            .to_string()
            .contains("query order requires order_id or client_order_id"));

        let first = CancelCommand {
            exchange: ExchangeId::Htx,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: None,
            reason: None,
            requested_at: Utc::now(),
        };
        let mut missing_batch_id = first.clone();
        missing_batch_id.client_order_id = None;
        let batch_missing_id =
            HtxPrivatePerpProtocol.cancel_batch_orders(&CancelBatchCommand::new(
                ExchangeId::Htx,
                [first.clone(), missing_batch_id],
                Utc::now(),
            ));
        assert!(batch_missing_id
            .unwrap_err()
            .to_string()
            .contains("for every order"));

        let wrong_command_batch = HtxPrivatePerpProtocol.cancel_batch_orders(
            &CancelBatchCommand::new(ExchangeId::Gate, [first.clone()], Utc::now()),
        );
        assert!(wrong_command_batch
            .unwrap_err()
            .to_string()
            .contains("htx batch cancel requires htx exchange symbols"));

        let mut mixed_symbol = first.clone();
        mixed_symbol.exchange_symbol = ExchangeSymbol::new(ExchangeId::Htx, "ETH-USDT");
        mixed_symbol.client_order_id = Some("client-2".to_string());
        let mixed_symbol_batch = HtxPrivatePerpProtocol.cancel_batch_orders(
            &CancelBatchCommand::new(ExchangeId::Htx, [first, mixed_symbol], Utc::now()),
        );
        assert!(mixed_symbol_batch
            .unwrap_err()
            .to_string()
            .contains("one contract_code"));
    }

    #[test]
    fn htx_symbol_scoped_specs_should_validate_exchange_scope() {
        let wrong_symbol = ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT");
        let mut place = command(ExchangeId::Htx, "BTC-USDT");
        place.exchange_symbol = wrong_symbol.clone();
        assert!(HtxPrivatePerpProtocol
            .place_order(&place, PositionMode::OneWay)
            .unwrap_err()
            .to_string()
            .contains("place order requires htx exchange symbols"));

        let valid_place = command(ExchangeId::Htx, "BTC-USDT");
        assert!(HtxPrivatePerpProtocol
            .place_batch_orders(
                &BatchPlaceCommand::new(ExchangeId::Gate, [valid_place.clone()], Utc::now()),
                PositionMode::OneWay,
            )
            .unwrap_err()
            .to_string()
            .contains("htx batch-place requires htx exchange symbols"));

        let mut wrong_batch_item = valid_place.clone();
        wrong_batch_item.exchange_symbol = wrong_symbol.clone();
        assert!(HtxPrivatePerpProtocol
            .place_batch_orders(
                &BatchPlaceCommand::new(ExchangeId::Htx, [wrong_batch_item], Utc::now()),
                PositionMode::OneWay,
            )
            .unwrap_err()
            .to_string()
            .contains("htx batch-place requires htx exchange symbols"));

        assert!(HtxPrivatePerpProtocol
            .place_batch_orders(
                &BatchPlaceCommand::new(ExchangeId::Htx, Vec::<OrderCommand>::new(), Utc::now()),
                PositionMode::OneWay,
            )
            .unwrap_err()
            .to_string()
            .contains("htx batch-place requires at least one order"));

        assert!(HtxPrivatePerpProtocol
            .place_batch_orders(
                &BatchPlaceCommand::new(ExchangeId::Htx, vec![valid_place; 26], Utc::now()),
                PositionMode::OneWay,
            )
            .unwrap_err()
            .to_string()
            .contains("htx cross batch-place supports at most 25 orders"));

        assert!(HtxPrivatePerpProtocol
            .cancel_all_orders(&CancelAllCommand::for_symbol(
                ExchangeId::Htx,
                CanonicalSymbol::new("BTC", "USDT"),
                wrong_symbol.clone(),
                Utc::now(),
            ))
            .unwrap_err()
            .to_string()
            .contains("cancel-all requires htx exchange symbols"));

        assert!(HtxPrivatePerpProtocol
            .cancel_all_orders(&CancelAllCommand {
                exchange: ExchangeId::Gate,
                canonical_symbol: None,
                exchange_symbol: None,
                requested_at: Utc::now(),
            })
            .unwrap_err()
            .to_string()
            .contains("cancel-all requires htx exchange symbols"));

        assert!(HtxPrivatePerpProtocol
            .get_order(&OrderQuery {
                exchange: ExchangeId::Htx,
                exchange_symbol: wrong_symbol.clone(),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: None,
            })
            .unwrap_err()
            .to_string()
            .contains("query order requires htx exchange symbols"));

        assert!(HtxPrivatePerpProtocol
            .get_open_orders(Some(&wrong_symbol))
            .unwrap_err()
            .to_string()
            .contains("open orders requires htx exchange symbols"));

        assert!(HtxPrivatePerpProtocol
            .get_fills(&FillQuery::for_symbol(
                ExchangeId::Htx,
                CanonicalSymbol::new("BTC", "USDT"),
                wrong_symbol.clone(),
            ))
            .unwrap_err()
            .to_string()
            .contains("fills requires htx exchange symbols"));

        assert!(HtxPrivatePerpProtocol
            .get_positions(Some(&wrong_symbol))
            .unwrap_err()
            .to_string()
            .contains("positions requires htx exchange symbols"));
        assert!(HtxPrivatePerpProtocol
            .get_trade_fee(&wrong_symbol)
            .unwrap_err()
            .to_string()
            .contains("trade fee requires htx exchange symbols"));
        assert!(HtxPrivatePerpProtocol
            .get_symbol_account_config(&wrong_symbol)
            .unwrap_err()
            .to_string()
            .contains("symbol account config requires htx exchange symbols"));

        assert!(HtxPrivatePerpProtocol
            .amend_order(&AmendOrderCommand {
                exchange: ExchangeId::Htx,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: wrong_symbol.clone(),
                client_order_id: None,
                exchange_order_id: Some("order-1".to_string()),
                original_side: None,
                new_client_order_id: None,
                new_quantity: Some(0.02),
                new_price: Some(64_900.0),
                requested_at: Utc::now(),
            })
            .unwrap_err()
            .to_string()
            .contains("amend requires htx exchange symbols"));

        assert!(HtxPrivatePerpProtocol
            .set_leverage(&LeverageCommand {
                exchange: ExchangeId::Htx,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: wrong_symbol,
                leverage: 3,
                requested_at: Utc::now(),
            })
            .unwrap_err()
            .to_string()
            .contains("set leverage requires htx exchange symbols"));
    }

    #[test]
    fn htx_amend_and_countdown_cancel_should_be_explicitly_unsupported() {
        let amend = HtxPrivatePerpProtocol.amend_order(&AmendOrderCommand {
            exchange: ExchangeId::Htx,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: Some("order-1".to_string()),
            original_side: Some(OrderSide::Buy),
            new_client_order_id: None,
            new_quantity: Some(0.02),
            new_price: Some(64_900.0),
            requested_at: Utc::now(),
        });
        assert!(amend
            .unwrap_err()
            .to_string()
            .contains("does not expose a verified amend-order endpoint"));

        let missing_identifier = HtxPrivatePerpProtocol.amend_order(&AmendOrderCommand {
            exchange: ExchangeId::Htx,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"),
            client_order_id: None,
            exchange_order_id: None,
            original_side: Some(OrderSide::Buy),
            new_client_order_id: None,
            new_quantity: Some(0.02),
            new_price: Some(64_900.0),
            requested_at: Utc::now(),
        });
        assert!(missing_identifier
            .unwrap_err()
            .to_string()
            .contains("htx amend requires order_id or client_order_id"));

        let missing_change = HtxPrivatePerpProtocol.amend_order(&AmendOrderCommand {
            exchange: ExchangeId::Htx,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: None,
            original_side: Some(OrderSide::Buy),
            new_client_order_id: None,
            new_quantity: None,
            new_price: None,
            requested_at: Utc::now(),
        });
        assert!(missing_change
            .unwrap_err()
            .to_string()
            .contains("htx amend requires new quantity or new price"));

        let countdown = HtxPrivatePerpProtocol.set_countdown_cancel_all(
            &CountdownCancelAllCommand::set_for_symbol(
                ExchangeId::Htx,
                ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"),
                30,
                Utc::now(),
            ),
        );
        assert!(countdown
            .unwrap_err()
            .to_string()
            .contains("countdown cancel-all is not supported"));

        let wrong_symbol = HtxPrivatePerpProtocol.set_countdown_cancel_all(
            &CountdownCancelAllCommand::set_for_symbol(
                ExchangeId::Htx,
                ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
                30,
                Utc::now(),
            ),
        );
        assert!(wrong_symbol
            .unwrap_err()
            .to_string()
            .contains("countdown cancel-all requires htx exchange symbols"));

        let wrong_exchange = HtxPrivatePerpProtocol.set_countdown_cancel_all(
            &CountdownCancelAllCommand::cancel(ExchangeId::Mexc, Utc::now()),
        );
        assert!(wrong_exchange
            .unwrap_err()
            .to_string()
            .contains("htx countdown cancel-all requires htx exchange symbols"));
    }

    #[tokio::test]
    async fn htx_adapter_should_parse_v3_order_history() {
        let transport = MockTransport::new(json!({
            "code": 200,
            "msg": "",
            "data": [
                {
                    "query_id": 13580806498_i64,
                    "order_id": 918800256249405440_i64,
                    "order_id_str": "918800256249405440",
                    "contract_code": "BTC-USDT",
                    "symbol": "BTC",
                    "lever_rate": 5,
                    "direction": "buy",
                    "offset": "open",
                    "volume": "100",
                    "price": "48555.6",
                    "create_date": 1639100651569_i64,
                    "update_time": 1639100651000_i64,
                    "order_price_type": "limit",
                    "order_type": 1,
                    "trade_volume": "25",
                    "trade_avg_price": "48550",
                    "status": 4,
                    "margin_mode": "cross",
                    "margin_account": "USDT",
                    "reduce_only": 0
                }
            ],
            "ts": 1604312615051_i64
        }));
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(HtxPrivatePerpProtocol, transport);

        let orders = adapter
            .get_order_history(OrderHistoryQuery::for_symbol(
                ExchangeId::Htx,
                ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"),
            ))
            .await
            .unwrap();

        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].exchange, ExchangeId::Htx);
        assert_eq!(
            orders[0].exchange_order_id.as_deref(),
            Some("918800256249405440")
        );
        assert_eq!(orders[0].status, OrderCommandStatus::PartiallyFilled);
        assert_eq!(orders[0].quantity, 100.0);
        assert_eq!(orders[0].filled_quantity, 25.0);
        assert_eq!(orders[0].average_fill_price, Some(48550.0));
        assert_eq!(
            seen.lock().unwrap()[0].path,
            "/linear-swap-api/v3/swap_cross_hisorders"
        );
    }

    #[tokio::test]
    async fn htx_adapter_should_route_fee_and_symbol_account_config_readbacks() {
        let transport = SequentialMockTransport::new([
            json!({
                "status": "ok",
                "data": [
                    {"contract_code":"BTC-USDT","open_maker_fee":"0.0002","open_taker_fee":"0.0006"}
                ]
            }),
            json!({
                "status": "ok",
                "data": [
                    {
                        "contract_code":"BTC-USDT",
                        "position_mode":"dual",
                        "margin_mode":"cross",
                        "lever_rate":5
                    }
                ]
            }),
        ]);
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(HtxPrivatePerpProtocol, transport)
            .with_position_mode(PositionMode::OneWay);
        let symbol = ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT");

        let fee = adapter.get_trade_fee(&symbol).await.unwrap();
        assert_eq!(fee.exchange, ExchangeId::Htx);
        assert_eq!(fee.exchange_symbol, symbol);
        assert_eq!(fee.maker, 0.0002);
        assert_eq!(fee.taker, 0.0006);

        let config = adapter.get_symbol_account_config(&symbol).await.unwrap();
        assert_eq!(config.exchange, ExchangeId::Htx);
        assert_eq!(config.exchange_symbol, symbol);
        assert_eq!(config.position_mode, Some(PositionMode::Hedge));
        assert_eq!(config.margin_mode, Some(MarginMode::Cross));
        assert_eq!(config.leverage, Some(5));

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].path, "/linear-swap-api/v1/swap_fee");
        assert_eq!(seen[0].body.as_ref().unwrap()["contract_code"], "BTC-USDT");
        assert_eq!(seen[1].path, "/linear-swap-api/v1/swap_cross_position_info");
        assert_eq!(seen[1].body.as_ref().unwrap()["contract_code"], "BTC-USDT");
    }

    #[tokio::test]
    async fn htx_adapter_should_route_positions_and_balances_readbacks() {
        let transport = SequentialMockTransport::new([
            json!({
                "status": "ok",
                "data": [
                    {
                        "contract_code":"BTC-USDT",
                        "direction":"sell",
                        "volume":"3",
                        "cost_open":"65000",
                        "last_price":"64800",
                        "profit_unreal":"21.5"
                    }
                ]
            }),
            json!({
                "status": "ok",
                "data": [
                    {
                        "margin_asset":"USDT",
                        "margin_balance":"1200",
                        "withdraw_available":"900",
                        "margin_frozen":"300"
                    }
                ]
            }),
        ]);
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(HtxPrivatePerpProtocol, transport);
        let symbol = ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT");

        let positions = adapter.get_positions(Some(&symbol)).await.unwrap();
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].exchange, ExchangeId::Htx);
        assert_eq!(positions[0].exchange_symbol, symbol);
        assert_eq!(positions[0].position_side, PositionSide::Short);
        assert_eq!(positions[0].quantity, 3.0);
        assert_eq!(positions[0].entry_price, Some(65_000.0));
        assert_eq!(positions[0].mark_price, Some(64_800.0));
        assert_eq!(positions[0].unrealized_pnl, Some(21.5));

        let balances = adapter.get_balances().await.unwrap();
        assert_eq!(balances.len(), 1);
        assert_eq!(balances[0].exchange, ExchangeId::Htx);
        assert_eq!(balances[0].asset, "USDT");
        assert_eq!(balances[0].total, 1200.0);
        assert_eq!(balances[0].available, 900.0);
        assert_eq!(balances[0].locked, 300.0);

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].path, "/linear-swap-api/v1/swap_cross_position_info");
        assert_eq!(seen[0].body.as_ref().unwrap()["contract_code"], "BTC-USDT");
        assert_eq!(seen[1].path, "/linear-swap-api/v1/swap_cross_account_info");
    }

    #[tokio::test]
    async fn htx_adapter_should_route_fills_readback() {
        let transport = MockTransport::new(json!({
            "status": "ok",
            "data": [
                {
                    "contract_code":"BTC-USDT",
                    "trade_id":"trade-1",
                    "order_id":"order-1",
                    "client_order_id":"client-1",
                    "direction":"sell",
                    "offset":"close",
                    "trade_price":"64800",
                    "trade_volume":"3",
                    "trade_fee":"1.2",
                    "fee_asset":"USDT",
                    "role":"taker",
                    "profit":"21.5",
                    "created_at":1700000000000_i64
                }
            ]
        }));
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(HtxPrivatePerpProtocol, transport);
        let mut query = FillQuery::for_symbol(
            ExchangeId::Htx,
            CanonicalSymbol::new("BTC", "USDT"),
            ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"),
        );
        query.limit = Some(20);

        let fills = adapter.get_fills(query).await.unwrap();
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].exchange, ExchangeId::Htx);
        assert_eq!(fills[0].exchange_symbol.symbol, "BTC-USDT");
        assert_eq!(fills[0].trade_id, "trade-1");
        assert_eq!(fills[0].client_order_id.as_deref(), Some("client-1"));
        assert_eq!(fills[0].exchange_order_id.as_deref(), Some("order-1"));
        assert_eq!(fills[0].side, OrderSide::Sell);
        assert_eq!(fills[0].position_side, PositionSide::Long);
        assert_eq!(fills[0].liquidity, FillLiquidity::Taker);
        assert_eq!(fills[0].price, 64_800.0);
        assert_eq!(fills[0].quantity, 3.0);
        assert_eq!(fills[0].quote_quantity, 194_400.0);
        assert_eq!(fills[0].fee, Some(1.2));
        assert_eq!(fills[0].fee_asset.as_deref(), Some("USDT"));
        assert_eq!(fills[0].realized_pnl, Some(21.5));
        assert_eq!(fills[0].reduce_only, Some(true));

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].path, "/linear-swap-api/v1/swap_cross_matchresults");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["contract_code"], "BTC-USDT");
        assert_eq!(body["page_index"], 1);
        assert_eq!(body["page_size"], 20);
    }

    #[tokio::test]
    async fn htx_adapter_should_place_batch_orders_and_split_indexed_results() {
        let transport = MockTransport::new(json!({
            "status": "ok",
            "data": {
                "errors": [
                    {"index": 2, "err_code": 1045, "err_msg": "Unable to switch leverage due to open orders."}
                ],
                "success": [
                    {"order_id": 784022175422087168_i64, "index": 1, "order_id_str": "784022175422087168"}
                ]
            },
            "ts": 1606967053089_i64
        }));
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(HtxPrivatePerpProtocol, transport)
            .with_position_mode(PositionMode::Hedge);
        let mut first = command(ExchangeId::Htx, "BTC-USDT");
        first.client_order_id = "client-1".to_string();
        let mut second = command(ExchangeId::Htx, "ETH-USDT");
        second.client_order_id = "client-2".to_string();
        second.side = OrderSide::Sell;
        second.position_side = PositionSide::Short;

        let ack = adapter
            .place_batch_orders(BatchPlaceCommand::new(
                ExchangeId::Htx,
                [first, second],
                Utc::now(),
            ))
            .await
            .unwrap();
        assert_batch_place_ack(&ack, ExchangeId::Htx, false, 1, 1, None);
        assert_eq!(ack.order_acks[0].exchange, ExchangeId::Htx);
        assert_eq!(
            ack.order_acks[0].exchange_order_id.as_deref(),
            Some("784022175422087168")
        );
        assert_eq!(ack.order_acks[0].client_order_id, "client-1");
        assert!(ack.order_acks[0].accepted);
        assert_eq!(ack.order_acks[0].status, OrderCommandStatus::Accepted);
        assert_eq!(ack.order_acks[0].message, None);
        assert_eq!(ack.failed_orders[0].code.as_deref(), Some("1045"));
        assert_eq!(
            ack.failed_orders[0].message,
            "Unable to switch leverage due to open orders."
        );
        assert_eq!(ack.failed_orders[0].order.client_order_id, "client-2");
        assert_eq!(
            seen.lock().unwrap()[0].path,
            "/linear-swap-api/v1/swap_cross_batchorder"
        );
    }

    #[tokio::test]
    async fn htx_adapter_should_batch_cancel_and_split_indexed_results() {
        let transport = MockTransport::new(json!({
            "status": "ok",
            "data": {
                "success": [
                    {"order_id": 784022175422087168_i64, "order_id_str": "784022175422087168"}
                ],
                "errors": [
                    {"order_id": "784022175422087169", "err_code": 1048, "err_msg": "Contract order does not exist."}
                ]
            },
            "ts": 1606967053089_i64
        }));
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(HtxPrivatePerpProtocol, transport);
        let first = CancelCommand {
            exchange: ExchangeId::Htx,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"),
            client_order_id: None,
            exchange_order_id: Some("784022175422087168".to_string()),
            reason: None,
            requested_at: Utc::now(),
        };
        let mut second = first.clone();
        second.exchange_order_id = Some("784022175422087169".to_string());

        let ack = adapter
            .cancel_batch_orders(CancelBatchCommand::new(
                ExchangeId::Htx,
                [first, second],
                Utc::now(),
            ))
            .await
            .unwrap();

        assert_cancel_batch_ack(&ack, ExchangeId::Htx, false, 1, 2, None);
        assert_eq!(ack.order_acks[0].exchange, ExchangeId::Htx);
        assert!(ack.order_acks[0].accepted);
        assert_eq!(ack.order_acks[0].status, OrderCommandStatus::Cancelled);
        assert_eq!(
            ack.order_acks[0].exchange_order_id.as_deref(),
            Some("784022175422087168")
        );
        assert_eq!(ack.order_acks[0].message, None);
        assert_eq!(ack.order_acks[1].exchange, ExchangeId::Htx);
        assert!(!ack.order_acks[1].accepted);
        assert_eq!(ack.order_acks[1].status, OrderCommandStatus::Rejected);
        assert_eq!(
            ack.order_acks[1].exchange_order_id.as_deref(),
            Some("784022175422087169")
        );
        assert_eq!(
            ack.order_acks[1].message.as_deref(),
            Some("Contract order does not exist.")
        );

        let seen = seen.lock().unwrap();
        assert_eq!(seen[0].path, "/linear-swap-api/v1/swap_cross_cancel");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["contract_code"], "BTC-USDT");
        assert_eq!(body["order_id"], "784022175422087168,784022175422087169");
    }

    #[test]
    fn bitget_should_build_batch_cancel_and_amend_specs() {
        let cancel = CancelCommand {
            exchange: ExchangeId::Bitget,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: Some("order-1".to_string()),
            reason: None,
            requested_at: Utc::now(),
        };
        let batch = CancelBatchCommand::new(ExchangeId::Bitget, [cancel], Utc::now());
        let spec = BitgetPrivatePerpProtocol
            .cancel_batch_orders(&batch)
            .unwrap();
        assert_eq!(spec.path, "/api/v2/mix/order/batch-cancel-orders");
        let body = spec.body.unwrap();
        assert_eq!(body["symbol"], "BTCUSDT");
        assert_eq!(body["orderIdList"][0]["orderId"], "order-1");

        let amend = BitgetPrivatePerpProtocol
            .amend_order(&AmendOrderCommand {
                exchange: ExchangeId::Bitget,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: None,
                original_side: None,
                new_client_order_id: Some("client-2".to_string()),
                new_quantity: Some(0.02),
                new_price: Some(64_900.0),
                requested_at: Utc::now(),
            })
            .unwrap();
        assert_eq!(amend.path, "/api/v2/mix/order/modify-order");
        let amend_body = amend.body.unwrap();
        assert_eq!(amend_body["clientOid"], "client-1");
        assert_eq!(amend_body["newClientOid"], "client-2");
        assert_eq!(amend_body["newSize"], "0.02");
        assert_eq!(amend_body["newPrice"], "64900");

        let countdown = BitgetPrivatePerpProtocol
            .set_countdown_cancel_all(&CountdownCancelAllCommand::set_for_symbol(
                ExchangeId::Bitget,
                ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
                30,
                Utc::now(),
            ))
            .unwrap();
        assert_eq!(countdown.method, PrivateRestMethod::Post);
        assert_eq!(countdown.path, "/api/v3/trade/countdown-cancel-all");
        let countdown_body = countdown.body.unwrap();
        assert_eq!(countdown_body["countdown"], "30");

        let disable = BitgetPrivatePerpProtocol
            .set_countdown_cancel_all(&CountdownCancelAllCommand::cancel(
                ExchangeId::Bitget,
                Utc::now(),
            ))
            .unwrap();
        assert_eq!(disable.path, "/api/v3/trade/countdown-cancel-all");
        assert_eq!(disable.body.unwrap()["countdown"], "0");
    }

    #[test]
    fn bitget_cancel_should_validate_required_identifiers_and_exchange_scope() {
        let missing_id = BitgetPrivatePerpProtocol.cancel_order(&CancelCommand {
            exchange: ExchangeId::Bitget,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
            client_order_id: None,
            exchange_order_id: None,
            reason: None,
            requested_at: Utc::now(),
        });
        assert!(missing_id
            .unwrap_err()
            .to_string()
            .contains("requires orderId or clientOid"));

        let valid = CancelCommand {
            exchange: ExchangeId::Bitget,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: None,
            reason: None,
            requested_at: Utc::now(),
        };
        let wrong_symbol = ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT");
        let mut wrong_place_symbol = command(ExchangeId::Bitget, "BTCUSDT");
        wrong_place_symbol.exchange_symbol = wrong_symbol.clone();
        assert!(BitgetPrivatePerpProtocol
            .place_order(&wrong_place_symbol, PositionMode::Hedge)
            .unwrap_err()
            .to_string()
            .contains("place order requires bitget exchange symbols"));

        let batch_wrong_exchange = BatchPlaceCommand::new(
            ExchangeId::Mexc,
            [command(ExchangeId::Bitget, "BTCUSDT")],
            Utc::now(),
        );
        assert!(BitgetPrivatePerpProtocol
            .place_batch_orders(&batch_wrong_exchange, PositionMode::Hedge)
            .unwrap_err()
            .to_string()
            .contains("batch-place requires bitget exchange symbols"));

        assert!(BitgetPrivatePerpProtocol
            .cancel_all_orders(&CancelAllCommand::for_symbol(
                ExchangeId::Bitget,
                CanonicalSymbol::new("BTC", "USDT"),
                wrong_symbol.clone(),
                Utc::now(),
            ))
            .unwrap_err()
            .to_string()
            .contains("cancel-all requires bitget exchange symbols"));
        assert!(BitgetPrivatePerpProtocol
            .cancel_all_orders(&CancelAllCommand::for_symbol(
                ExchangeId::Mexc,
                CanonicalSymbol::new("BTC", "USDT"),
                ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
                Utc::now(),
            ))
            .unwrap_err()
            .to_string()
            .contains("cancel-all requires bitget exchange symbols"));

        let mut missing_batch_id = valid.clone();
        missing_batch_id.client_order_id = None;
        let batch_missing_id =
            BitgetPrivatePerpProtocol.cancel_batch_orders(&CancelBatchCommand::new(
                ExchangeId::Bitget,
                [valid.clone(), missing_batch_id],
                Utc::now(),
            ));
        assert!(batch_missing_id
            .unwrap_err()
            .to_string()
            .contains("for every order"));

        let batch_wrong_command_exchange = BitgetPrivatePerpProtocol.cancel_batch_orders(
            &CancelBatchCommand::new(ExchangeId::Mexc, [valid.clone()], Utc::now()),
        );
        assert!(batch_wrong_command_exchange
            .unwrap_err()
            .to_string()
            .contains("batch cancel requires bitget exchange symbols"));

        let mut wrong_exchange_symbol = valid.clone();
        wrong_exchange_symbol.exchange_symbol = wrong_symbol.clone();
        let wrong_exchange = BitgetPrivatePerpProtocol.cancel_batch_orders(
            &CancelBatchCommand::new(ExchangeId::Bitget, [wrong_exchange_symbol], Utc::now()),
        );
        assert!(wrong_exchange
            .unwrap_err()
            .to_string()
            .contains("bitget exchange symbols"));

        assert!(BitgetPrivatePerpProtocol
            .get_order(&OrderQuery {
                exchange: ExchangeId::Bitget,
                exchange_symbol: wrong_symbol.clone(),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: None,
            })
            .unwrap_err()
            .to_string()
            .contains("order detail requires bitget exchange symbols"));
        assert!(BitgetPrivatePerpProtocol
            .get_open_orders(Some(&wrong_symbol))
            .unwrap_err()
            .to_string()
            .contains("open orders requires bitget exchange symbols"));
        assert!(BitgetPrivatePerpProtocol
            .get_fills(&FillQuery::for_symbol(
                ExchangeId::Bitget,
                CanonicalSymbol::new("BTC", "USDT"),
                wrong_symbol.clone(),
            ))
            .unwrap_err()
            .to_string()
            .contains("fills requires bitget exchange symbols"));
        assert!(BitgetPrivatePerpProtocol
            .get_positions(Some(&wrong_symbol))
            .unwrap_err()
            .to_string()
            .contains("positions requires bitget exchange symbols"));
        assert!(BitgetPrivatePerpProtocol
            .get_trade_fee(&wrong_symbol)
            .unwrap_err()
            .to_string()
            .contains("trade fee requires bitget exchange symbols"));
        assert!(BitgetPrivatePerpProtocol
            .get_symbol_account_config(&wrong_symbol)
            .unwrap_err()
            .to_string()
            .contains("symbol account config requires bitget exchange symbols"));

        let wrong_amend_symbol = AmendOrderCommand {
            exchange: ExchangeId::Bitget,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: wrong_symbol.clone(),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: None,
            original_side: None,
            new_client_order_id: None,
            new_quantity: Some(0.02),
            new_price: None,
            requested_at: Utc::now(),
        };
        assert!(BitgetPrivatePerpProtocol
            .amend_order(&wrong_amend_symbol)
            .unwrap_err()
            .to_string()
            .contains("bitget amend requires bitget exchange symbols"));

        assert!(BitgetPrivatePerpProtocol
            .set_leverage(&LeverageCommand {
                exchange: ExchangeId::Bitget,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: wrong_symbol.clone(),
                leverage: 3,
                requested_at: Utc::now(),
            })
            .unwrap_err()
            .to_string()
            .contains("set leverage requires bitget exchange symbols"));
        assert!(BitgetPrivatePerpProtocol
            .set_countdown_cancel_all(&CountdownCancelAllCommand::set_for_symbol(
                ExchangeId::Bitget,
                wrong_symbol,
                30,
                Utc::now(),
            ))
            .unwrap_err()
            .to_string()
            .contains("countdown cancel-all requires bitget exchange symbols"));
        assert!(BitgetPrivatePerpProtocol
            .set_countdown_cancel_all(&CountdownCancelAllCommand::cancel(
                ExchangeId::Mexc,
                Utc::now(),
            ))
            .unwrap_err()
            .to_string()
            .contains("countdown cancel-all requires bitget exchange symbols"));

        let too_many = (0..51)
            .map(|index| {
                let mut order = valid.clone();
                order.client_order_id = Some(format!("client-{index}"));
                order
            })
            .collect::<Vec<_>>();
        let too_many_batch = BitgetPrivatePerpProtocol.cancel_batch_orders(
            &CancelBatchCommand::new(ExchangeId::Bitget, too_many, Utc::now()),
        );
        assert!(too_many_batch
            .unwrap_err()
            .to_string()
            .contains("at most 50"));
    }

    #[tokio::test]
    async fn bitget_adapter_should_parse_order_history() {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "msg": "success",
            "data": {
                "entrustedList": [
                    {
                        "symbol": "BTCUSDT",
                        "orderId": "order-1",
                        "clientOid": "client-1",
                        "side": "buy",
                        "tradeSide": "open",
                        "orderType": "limit",
                        "size": "0.01",
                        "price": "65000",
                        "accBaseVolume": "0.004",
                        "priceAvg": "64950",
                        "status": "partial_filled",
                        "reduceOnly": "NO",
                        "uTime": "1700000000123"
                    }
                ],
                "endId": "order-1"
            }
        }));
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport);

        let orders = adapter
            .get_order_history(OrderHistoryQuery::for_symbol(
                ExchangeId::Bitget,
                ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
            ))
            .await
            .unwrap();

        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].exchange, ExchangeId::Bitget);
        assert_eq!(orders[0].exchange_order_id.as_deref(), Some("order-1"));
        assert_eq!(orders[0].client_order_id.as_deref(), Some("client-1"));
        assert_eq!(orders[0].status, OrderCommandStatus::PartiallyFilled);
        assert_eq!(orders[0].quantity, 0.01);
        assert_eq!(orders[0].filled_quantity, 0.004);
        assert_eq!(orders[0].average_fill_price, Some(64950.0));
        assert_eq!(
            seen.lock().unwrap()[0].path,
            "/api/v2/mix/order/orders-history"
        );
    }

    #[tokio::test]
    async fn bitget_adapter_should_route_fills_readback() {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "msg": "success",
            "data": {
                "fillList": [
                    {
                        "symbol": "BTCUSDT",
                        "tradeId": "trade-1",
                        "orderId": "order-1",
                        "clientOid": "client-1",
                        "side": "sell",
                        "tradeSide": "close",
                        "fillPrice": "65000",
                        "fillSize": "0.02",
                        "quoteVolume": "1300",
                        "fee": "0.65",
                        "feeCcy": "USDT",
                        "profit": "12.5",
                        "role": "taker",
                        "tradeTime": "1700000000000"
                    }
                ],
                "endId": "trade-1"
            }
        }));
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport);
        let mut query = FillQuery::for_symbol(
            ExchangeId::Bitget,
            CanonicalSymbol::new("BTC", "USDT"),
            ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
        );
        query.exchange_order_id = Some("order-1".to_string());
        query.limit = Some(25);

        let fills = adapter.get_fills(query).await.unwrap();
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].exchange, ExchangeId::Bitget);
        assert_eq!(fills[0].exchange_symbol.symbol, "BTCUSDT");
        assert_eq!(fills[0].trade_id, "trade-1");
        assert_eq!(fills[0].exchange_order_id.as_deref(), Some("order-1"));
        assert_eq!(fills[0].client_order_id.as_deref(), Some("client-1"));
        assert_eq!(fills[0].side, OrderSide::Sell);
        assert_eq!(fills[0].position_side, PositionSide::Long);
        assert_eq!(fills[0].liquidity, FillLiquidity::Taker);
        assert_eq!(fills[0].price, 65_000.0);
        assert_eq!(fills[0].quantity, 0.02);
        assert_eq!(fills[0].quote_quantity, 1300.0);
        assert_eq!(fills[0].fee, Some(0.65));
        assert_eq!(fills[0].fee_asset.as_deref(), Some("USDT"));
        assert_eq!(fills[0].realized_pnl, Some(12.5));
        assert_eq!(fills[0].reduce_only, Some(true));

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].path, "/api/v2/mix/order/fills");
        assert_eq!(
            seen[0].query.get("productType").map(String::as_str),
            Some("USDT-FUTURES")
        );
        assert_eq!(
            seen[0].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
        assert_eq!(
            seen[0].query.get("orderId").map(String::as_str),
            Some("order-1")
        );
        assert_eq!(seen[0].query.get("limit").map(String::as_str), Some("25"));
    }

    #[tokio::test]
    async fn bitget_adapter_should_route_positions_and_balances_readbacks() {
        let transport = SequentialMockTransport::new([
            json!({
                "code": "00000",
                "msg": "success",
                "data": [
                    {
                        "symbol": "BTCUSDT",
                        "holdSide": "long",
                        "total": "0.5",
                        "openPriceAvg": "65000",
                        "markPrice": "65100",
                        "unrealizedPL": "50",
                        "uTime": "1700000000000"
                    }
                ]
            }),
            json!({
                "code": "00000",
                "msg": "success",
                "data": [
                    {
                        "marginCoin": "USDT",
                        "equity": "1000",
                        "available": "850",
                        "locked": "150"
                    }
                ]
            }),
        ]);
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport);
        let symbol = ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT");

        let positions = adapter.get_positions(Some(&symbol)).await.unwrap();
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].exchange, ExchangeId::Bitget);
        assert_eq!(positions[0].exchange_symbol, symbol);
        assert_eq!(positions[0].position_side, PositionSide::Long);
        assert_eq!(positions[0].quantity, 0.5);
        assert_eq!(positions[0].entry_price, Some(65_000.0));
        assert_eq!(positions[0].mark_price, Some(65_100.0));
        assert_eq!(positions[0].unrealized_pnl, Some(50.0));

        let balances = adapter.get_balances().await.unwrap();
        assert_eq!(balances.len(), 1);
        assert_eq!(balances[0].exchange, ExchangeId::Bitget);
        assert_eq!(balances[0].asset, "USDT");
        assert_eq!(balances[0].total, 1000.0);
        assert_eq!(balances[0].available, 850.0);
        assert_eq!(balances[0].locked, 150.0);

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].path, "/api/v2/mix/position/all-position");
        assert_eq!(
            seen[0].query.get("productType").map(String::as_str),
            Some("USDT-FUTURES")
        );
        assert_eq!(
            seen[0].query.get("marginCoin").map(String::as_str),
            Some("USDT")
        );
        assert_eq!(
            seen[0].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
        assert_eq!(seen[1].path, "/api/v2/mix/account/accounts");
        assert_eq!(
            seen[1].query.get("productType").map(String::as_str),
            Some("USDT-FUTURES")
        );
    }

    #[tokio::test]
    async fn bitget_adapter_should_route_fee_and_symbol_account_config_readbacks() {
        let transport = SequentialMockTransport::new([
            json!({
                "code": "00000",
                "msg": "success",
                "data": [
                    {
                        "symbol": "BTCUSDT",
                        "makerFeeRate": "0.0002",
                        "takerFeeRate": "0.0006"
                    }
                ]
            }),
            json!({
                "code": "00000",
                "msg": "success",
                "data": {
                    "symbol": "BTCUSDT",
                    "posMode": "hedge_mode",
                    "marginMode": "crossed",
                    "crossedMarginLeverage": "3",
                    "maxLever": "125"
                }
            }),
        ]);
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport)
            .with_position_mode(PositionMode::OneWay);
        let symbol = ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT");

        let fee = adapter.get_trade_fee(&symbol).await.unwrap();
        assert_eq!(fee.exchange, ExchangeId::Bitget);
        assert_eq!(fee.exchange_symbol, symbol);
        assert_eq!(fee.maker, 0.0002);
        assert_eq!(fee.taker, 0.0006);

        let config = adapter.get_symbol_account_config(&symbol).await.unwrap();
        assert_eq!(config.exchange, ExchangeId::Bitget);
        assert_eq!(config.exchange_symbol, symbol);
        assert_eq!(config.position_mode, Some(PositionMode::Hedge));
        assert_eq!(config.margin_mode, Some(MarginMode::Cross));
        assert_eq!(config.leverage, Some(3));
        assert_eq!(config.max_leverage, Some(125));

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].path, "/api/v2/mix/market/contracts");
        assert_eq!(
            seen[0].query.get("productType").map(String::as_str),
            Some("USDT-FUTURES")
        );
        assert_eq!(
            seen[0].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
        assert_eq!(seen[1].path, "/api/v2/mix/account/account");
        assert_eq!(
            seen[1].query.get("marginCoin").map(String::as_str),
            Some("USDT")
        );
        assert_eq!(
            seen[1].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
    }

    #[tokio::test]
    async fn bitget_adapter_should_batch_cancel_and_split_item_results() {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "msg": "success",
            "data": {
                "successList": [
                    {"orderId": "order-1", "clientOid": "client-1"}
                ],
                "failureList": [
                    {"orderId": "order-2", "clientOid": "client-2", "errorCode": "40725", "errorMsg": "order not found"}
                ],
                "result": false
            }
        }));
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport);
        let first = CancelCommand {
            exchange: ExchangeId::Bitget,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: Some("order-1".to_string()),
            reason: None,
            requested_at: Utc::now(),
        };
        let second = CancelCommand {
            client_order_id: Some("client-2".to_string()),
            exchange_order_id: Some("order-2".to_string()),
            ..first.clone()
        };

        let ack = adapter
            .cancel_batch_orders(CancelBatchCommand::new(
                ExchangeId::Bitget,
                [first, second],
                Utc::now(),
            ))
            .await
            .unwrap();

        assert_cancel_batch_ack(&ack, ExchangeId::Bitget, false, 1, 2, Some("success"));
        assert!(ack.order_acks[0].accepted);
        assert_eq!(ack.order_acks[0].exchange, ExchangeId::Bitget);
        assert_eq!(
            ack.order_acks[0].client_order_id.as_deref(),
            Some("client-1")
        );
        assert_eq!(ack.order_acks[0].status, OrderCommandStatus::Cancelled);
        assert_eq!(
            ack.order_acks[0].exchange_order_id.as_deref(),
            Some("order-1")
        );
        assert!(ack.order_acks[0].message.is_none());
        assert!(!ack.order_acks[1].accepted);
        assert_eq!(ack.order_acks[1].exchange, ExchangeId::Bitget);
        assert_eq!(
            ack.order_acks[1].exchange_order_id.as_deref(),
            Some("order-2")
        );
        assert_eq!(ack.order_acks[1].status, OrderCommandStatus::Rejected);
        assert_eq!(
            ack.order_acks[1].client_order_id.as_deref(),
            Some("client-2")
        );
        assert_eq!(
            ack.order_acks[1].message.as_deref(),
            Some("order not found")
        );

        let seen = seen.lock().unwrap();
        assert_eq!(seen[0].path, "/api/v2/mix/order/batch-cancel-orders");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["productType"], "USDT-FUTURES");
        assert_eq!(body["marginCoin"], "USDT");
        assert_eq!(body["symbol"], "BTCUSDT");
        assert_eq!(body["orderIdList"][0]["orderId"], "order-1");
        assert_eq!(body["orderIdList"][0]["clientOid"], "client-1");
        assert_eq!(body["orderIdList"][1]["orderId"], "order-2");
        assert_eq!(body["orderIdList"][1]["clientOid"], "client-2");
    }

    #[tokio::test]
    async fn bitget_adapter_should_route_countdown_cancel() {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "msg": "success",
            "requestTime": 1728625799912_i64,
            "data": "success"
        }));
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport.clone());

        let ack = adapter
            .set_countdown_cancel_all(CountdownCancelAllCommand::cancel(
                ExchangeId::Bitget,
                Utc::now(),
            ))
            .await
            .unwrap();

        assert_countdown_ack(&ack, ExchangeId::Bitget, None, 0, None, Some("success"));
        let seen = transport.seen.lock().unwrap();
        assert_eq!(seen[0].path, "/api/v3/trade/countdown-cancel-all");
        assert_eq!(seen[0].body.as_ref().unwrap()["countdown"], "0");
    }

    #[tokio::test]
    async fn bitget_should_build_and_parse_batch_place_specs() {
        let mut first = command(ExchangeId::Bitget, "BTCUSDT");
        first.client_order_id = "client-1".to_string();
        let mut second = command(ExchangeId::Bitget, "BTCUSDT");
        second.client_order_id = "client-2".to_string();
        second.side = OrderSide::Sell;
        second.position_side = PositionSide::Short;
        second.quantity = 0.02;
        second.price = Some(66_000.0);

        let spec = BitgetPrivatePerpProtocol
            .place_batch_orders(
                &BatchPlaceCommand::new(
                    ExchangeId::Bitget,
                    [first.clone(), second.clone()],
                    Utc::now(),
                ),
                PositionMode::Hedge,
            )
            .unwrap();
        assert_eq!(spec.method, PrivateRestMethod::Post);
        assert_eq!(spec.path, "/api/v2/mix/order/batch-place-order");
        let body = spec.body.unwrap();
        assert_eq!(body["symbol"], "BTCUSDT");
        assert_eq!(body["productType"], "USDT-FUTURES");
        assert_eq!(body["orderList"][0]["side"], "buy");
        assert_eq!(body["orderList"][0]["tradeSide"], "open");
        assert_eq!(body["orderList"][0]["force"], "post_only");
        assert_eq!(body["orderList"][1]["side"], "sell");
        assert_eq!(body["orderList"][1]["tradeSide"], "open");
        assert_eq!(body["orderList"][1]["size"], "0.02");

        let transport = MockTransport::new(json!({
            "code": "00000",
            "msg": "success",
            "data": {
                "successList": [
                    {"orderId": "order-1", "clientOid": "client-1"}
                ],
                "failureList": [
                    {"clientOid": "client-2", "errorCode": "40762", "errorMsg": "The order size is greater than the max open size"}
                ],
                "result": false
            }
        }));
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport)
            .with_position_mode(PositionMode::Hedge);
        let ack = adapter
            .place_batch_orders(BatchPlaceCommand::new(
                ExchangeId::Bitget,
                [first, second],
                Utc::now(),
            ))
            .await
            .unwrap();
        assert_batch_place_ack(&ack, ExchangeId::Bitget, false, 1, 1, Some("success"));
        assert_eq!(ack.order_acks[0].exchange, ExchangeId::Bitget);
        assert_eq!(ack.order_acks[0].client_order_id, "client-1");
        assert_eq!(
            ack.order_acks[0].exchange_order_id.as_deref(),
            Some("order-1")
        );
        assert!(ack.order_acks[0].accepted);
        assert_eq!(ack.order_acks[0].status, OrderCommandStatus::Accepted);
        assert!(ack.order_acks[0].message.is_none());
        assert_eq!(ack.failed_orders[0].code.as_deref(), Some("40762"));
        assert_eq!(
            ack.failed_orders[0].message,
            "The order size is greater than the max open size"
        );
        assert_eq!(ack.failed_orders[0].order.client_order_id, "client-2");
        assert_eq!(
            seen.lock().unwrap()[0].path,
            "/api/v2/mix/order/batch-place-order"
        );
    }

    #[test]
    fn response_items_should_expand_bitget_list_envelopes() {
        let fills = response_items(&json!({
            "code": "00000",
            "data": {"fillList": [{"tradeId": "t1"}, {"tradeId": "t2"}]}
        }));
        assert_eq!(fills.len(), 2);

        let pending = response_items(&json!({
            "code": "00000",
            "data": {"entrustedList": [{"orderId": "o1"}]}
        }));
        assert_eq!(pending.len(), 1);
    }

    #[test]
    fn gate_should_build_cancel_all_and_readback_specs() {
        let command = CancelAllCommand::for_symbol(
            ExchangeId::Gate,
            CanonicalSymbol::new("BTC", "USDT"),
            ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
            Utc::now(),
        );
        let cancel = GatePrivatePerpProtocol.cancel_all_orders(&command).unwrap();

        assert_eq!(cancel.method, PrivateRestMethod::Delete);
        assert_eq!(cancel.path, "/futures/usdt/orders");
        assert_eq!(
            cancel.query.get("contract").map(String::as_str),
            Some("BTC_USDT")
        );

        let mut history = OrderHistoryQuery::for_symbol(
            ExchangeId::Gate,
            ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
        );
        history.limit = Some(100);
        let history_spec = GatePrivatePerpProtocol.get_all_orders(&history).unwrap();
        assert_eq!(history_spec.method, PrivateRestMethod::Get);
        assert_eq!(history_spec.path, "/futures/usdt/orders");
        assert_eq!(
            history_spec.query.get("contract").map(String::as_str),
            Some("BTC_USDT")
        );
        assert_eq!(
            history_spec.query.get("status").map(String::as_str),
            Some("finished")
        );
        assert_eq!(
            history_spec.query.get("limit").map(String::as_str),
            Some("100")
        );

        let batch_cancel = CancelBatchCommand::new(
            ExchangeId::Gate,
            [
                CancelCommand {
                    exchange: ExchangeId::Gate,
                    canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                    exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
                    client_order_id: None,
                    exchange_order_id: Some("order-1".to_string()),
                    reason: None,
                    requested_at: Utc::now(),
                },
                CancelCommand {
                    exchange: ExchangeId::Gate,
                    canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                    exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
                    client_order_id: None,
                    exchange_order_id: Some("order-2".to_string()),
                    reason: None,
                    requested_at: Utc::now(),
                },
            ],
            Utc::now(),
        );
        let batch_cancel_spec = GatePrivatePerpProtocol
            .cancel_batch_orders(&batch_cancel)
            .unwrap();
        assert_eq!(batch_cancel_spec.method, PrivateRestMethod::Post);
        assert_eq!(batch_cancel_spec.path, "/futures/usdt/batch_cancel_orders");
        let batch_cancel_body = batch_cancel_spec.body.unwrap();
        assert_eq!(batch_cancel_body[0], "order-1");
        assert_eq!(batch_cancel_body[1], "order-2");

        let mut wrong_command_batch = batch_cancel.clone();
        wrong_command_batch.exchange = ExchangeId::Mexc;
        assert!(GatePrivatePerpProtocol
            .cancel_batch_orders(&wrong_command_batch)
            .unwrap_err()
            .to_string()
            .contains("gate batch cancel requires gate exchange symbols"));

        let symbol = ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT");
        let fee = GatePrivatePerpProtocol.get_trade_fee(&symbol).unwrap();
        assert_eq!(fee.path, "/futures/usdt/contracts/BTC_USDT");
        let config = GatePrivatePerpProtocol
            .get_symbol_account_config(&symbol)
            .unwrap();
        assert_eq!(config.path, "/futures/usdt/positions/BTC_USDT");
    }

    #[test]
    fn gate_signature_should_use_unescaped_query_string() {
        let spec = GatePrivatePerpProtocol
            .get_open_orders(Some(&ExchangeSymbol::new(
                ExchangeId::Gate,
                "草根文化_USDT",
            )))
            .unwrap();

        assert_eq!(
            spec.query_string(),
            "contract=%E8%8D%89%E6%A0%B9%E6%96%87%E5%8C%96_USDT&status=open"
        );
        assert_eq!(
            spec.raw_query_string(),
            "contract=草根文化_USDT&status=open"
        );

        let transport = ReqwestPrivateRestTransport::new(
            PrivatePerpExchange::Gate,
            PrivateRestAuth {
                api_key: "key".to_string(),
                api_secret: "secret".to_string(),
                passphrase: None,
                demo_trading: false,
            },
        )
        .unwrap();
        let headers = gate_rest_headers(&transport.auth, &spec, 1_700_000_000).unwrap();
        let expected = gate_rest_signature(
            "secret",
            "GET",
            "/api/v4/futures/usdt/orders",
            "contract=草根文化_USDT&status=open",
            "",
            1_700_000_000,
        );

        assert_eq!(
            headers.get("SIGN").map(String::as_str),
            Some(expected.as_str())
        );
    }

    #[test]
    fn gate_should_build_amend_and_countdown_cancel_specs() {
        let amend = GatePrivatePerpProtocol
            .amend_order(&AmendOrderCommand {
                exchange: ExchangeId::Gate,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
                client_order_id: Some("crossarb-ls-mk-1-c0b4e763".to_string()),
                exchange_order_id: None,
                original_side: None,
                new_client_order_id: Some("crossarb-ls-mk-2-c0b4e763".to_string()),
                new_quantity: None,
                new_price: Some(65_001.5),
                requested_at: Utc::now(),
            })
            .unwrap();
        assert_eq!(amend.method, PrivateRestMethod::Patch);
        assert_eq!(
            amend.path,
            "/futures/usdt/orders/t-crossarb-ls-mk-1-c0b4e763"
        );
        assert_eq!(
            amend.query.get("contract").map(String::as_str),
            Some("BTC_USDT")
        );
        let body = amend.body.unwrap();
        assert_eq!(body["price"], "65001.5");
        assert_eq!(body["amend_text"], "t-crossarb-ls-mk-2-c0b4e763");

        let countdown = GatePrivatePerpProtocol
            .set_countdown_cancel_all(&CountdownCancelAllCommand::set_for_symbol(
                ExchangeId::Gate,
                ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
                30,
                Utc::now(),
            ))
            .unwrap();
        assert_eq!(countdown.method, PrivateRestMethod::Post);
        assert_eq!(countdown.path, "/futures/usdt/countdown_cancel_all");
        let body = countdown.body.unwrap();
        assert_eq!(body["timeout"], 30);
        assert_eq!(body["contract"], "BTC_USDT");
    }

    #[test]
    fn gate_amend_should_reject_size_without_original_side() {
        let err = GatePrivatePerpProtocol
            .amend_order(&AmendOrderCommand {
                exchange: ExchangeId::Gate,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: None,
                original_side: None,
                new_client_order_id: None,
                new_quantity: Some(2.0),
                new_price: None,
                requested_at: Utc::now(),
            })
            .unwrap_err();

        assert!(err.to_string().contains("original order side"));
    }

    #[test]
    fn private_perp_should_parse_trade_fee_and_account_config_readbacks() {
        let now = Utc::now();
        let bitget_fee = parse_trade_fee_snapshot(
            ExchangeId::Bitget,
            &ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
            &json!({"data":[{"symbol":"BTCUSDT","makerFeeRate":"0.0002","takerFeeRate":"0.0006"}]}),
            now,
        )
        .unwrap();
        assert_eq!(bitget_fee.maker, 0.0002);
        assert_eq!(bitget_fee.taker, 0.0006);

        let gate_fee = parse_trade_fee_snapshot(
            ExchangeId::Gate,
            &ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
            &json!({"name":"BTC_USDT","maker_fee_rate":"-0.00025","taker_fee_rate":"0.00075"}),
            now,
        )
        .unwrap();
        assert_eq!(gate_fee.exchange_symbol.symbol, "BTC_USDT");
        assert_eq!(gate_fee.maker, -0.00025);

        let mexc_fee = parse_trade_fee_snapshot(
            ExchangeId::Mexc,
            &ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
            &json!({"data":[{"symbol":"BTC_USDT","makerFee":"0.0001","takerFee":"0.0005"}]}),
            now,
        )
        .unwrap();
        assert_eq!(mexc_fee.exchange_symbol.symbol, "BTC_USDT");
        assert_eq!(mexc_fee.maker, 0.0001);
        assert_eq!(mexc_fee.taker, 0.0005);

        let htx_fee = parse_trade_fee_snapshot(
            ExchangeId::Htx,
            &ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"),
            &json!({"data":[{"contract_code":"BTC-USDT","open_maker_fee":"0.0002","open_taker_fee":"0.0006"}]}),
            now,
        )
        .unwrap();
        assert_eq!(htx_fee.exchange_symbol.symbol, "BTC-USDT");
        assert_eq!(htx_fee.maker, 0.0002);
        assert_eq!(htx_fee.taker, 0.0006);

        let config = parse_symbol_account_config(
            ExchangeId::Bitget,
            &ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
            &json!({"data":{"symbol":"BTCUSDT","posMode":"hedge_mode","marginMode":"crossed","crossedMarginLeverage":3,"maxLever":"125"}}),
            now,
            PositionMode::OneWay,
        )
        .unwrap();
        assert_eq!(config.position_mode, Some(PositionMode::Hedge));
        assert_eq!(config.margin_mode, Some(MarginMode::Cross));
        assert_eq!(config.leverage, Some(3));
        assert_eq!(config.max_leverage, Some(125));

        let mexc_config = parse_symbol_account_config(
            ExchangeId::Mexc,
            &ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
            &json!({"data":[{"symbol":"BTC_USDT","positionMode":"long_short_mode","marginMode":"isolated","longLeverage":10,"maxLeverage":"200"}]}),
            now,
            PositionMode::OneWay,
        )
        .unwrap();
        assert_eq!(mexc_config.position_mode, Some(PositionMode::Hedge));
        assert_eq!(mexc_config.margin_mode, Some(MarginMode::Isolated));
        assert_eq!(mexc_config.leverage, Some(10));
        assert_eq!(mexc_config.max_leverage, Some(200));

        let htx_config = parse_symbol_account_config(
            ExchangeId::Htx,
            &ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"),
            &json!({"data":[{"contract_code":"BTC-USDT","position_mode":"dual","margin_mode":"cross","lever_rate":5}]}),
            now,
            PositionMode::OneWay,
        )
        .unwrap();
        assert_eq!(htx_config.exchange_symbol.symbol, "BTC-USDT");
        assert_eq!(htx_config.position_mode, Some(PositionMode::Hedge));
        assert_eq!(htx_config.margin_mode, Some(MarginMode::Cross));
        assert_eq!(htx_config.leverage, Some(5));
    }

    #[test]
    fn bitget_private_ws_endpoint_should_login_and_subscribe_all_private_channels() {
        let endpoint = build_private_ws_endpoint(
            PrivatePerpExchange::Bitget,
            PrivateWsAuth {
                api_key: "key".to_string(),
                api_secret: "secret".to_string(),
                passphrase: Some("pass".to_string()),
                account_id: None,
                demo_trading: false,
            },
            &[ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT")],
            1_700_000_000,
        )
        .unwrap();

        assert_eq!(endpoint.exchange, ExchangeId::Bitget);
        assert!(endpoint.login_message.is_some());
        assert_eq!(endpoint.subscribe_messages.len(), 3);

        let orders: Value = serde_json::from_str(&endpoint.subscribe_messages[0]).unwrap();
        assert_eq!(orders["op"], "subscribe");
        assert_eq!(orders["args"][0]["channel"], "orders");
        assert_eq!(orders["args"][0]["instId"], "default");

        let account: Value = serde_json::from_str(&endpoint.subscribe_messages[2]).unwrap();
        assert_eq!(account["args"][0]["channel"], "account");
        assert_eq!(account["args"][0]["coin"], "default");
        assert!(account["args"][0].get("instId").is_none());
    }

    #[test]
    fn private_ws_endpoint_should_accept_configured_url_override() {
        let endpoint = build_private_ws_endpoint_with_url(
            PrivatePerpExchange::Gate,
            PrivateWsAuth {
                api_key: "key".to_string(),
                api_secret: "secret".to_string(),
                passphrase: None,
                account_id: Some("20011".to_string()),
                demo_trading: false,
            },
            &[],
            1_700_000_000,
            "wss://ws-testnet.gate.com/v4/ws/futures/usdt",
        )
        .unwrap();

        assert_eq!(endpoint.url, "wss://ws-testnet.gate.com/v4/ws/futures/usdt");
        assert_eq!(endpoint.exchange, ExchangeId::Gate);
    }

    #[test]
    fn binance_private_ws_endpoint_should_accept_precreated_listen_key() {
        let endpoint = build_private_ws_endpoint_with_url(
            PrivatePerpExchange::Binance,
            PrivateWsAuth {
                api_key: "key".to_string(),
                api_secret: "secret".to_string(),
                passphrase: None,
                account_id: Some("listen-key-1".to_string()),
                demo_trading: false,
            },
            &[],
            1_700_000_000,
            "wss://fstream.binance.com",
        )
        .unwrap();

        assert_eq!(endpoint.exchange, ExchangeId::Binance);
        assert_eq!(endpoint.url, "wss://fstream.binance.com/ws/listen-key-1");
        assert!(endpoint.login_message.is_none());
        assert!(endpoint.subscribe_messages.is_empty());
    }

    #[test]
    fn binance_listen_key_rest_url_should_match_futures_contract() {
        assert_eq!(
            binance_futures_listen_key_url("https://fapi.binance.com/", None),
            "https://fapi.binance.com/fapi/v1/listenKey"
        );
        assert_eq!(
            binance_futures_listen_key_url("https://fapi.binance.com", Some("abc/with space")),
            "https://fapi.binance.com/fapi/v1/listenKey?listenKey=abc%2Fwith%20space"
        );
    }

    #[test]
    fn private_ws_endpoint_matrix_should_cover_verified_common_surface() {
        for (exchange, symbol, account_id, expects_login, subscribe_count) in [
            (
                PrivatePerpExchange::Binance,
                "BTCUSDT",
                Some("listen-key-1"),
                false,
                0,
            ),
            (PrivatePerpExchange::Okx, "BTC-USDT-SWAP", None, true, 3),
            (PrivatePerpExchange::Bitget, "BTCUSDT", None, true, 3),
            (
                PrivatePerpExchange::Gate,
                "BTC_USDT",
                Some("20011"),
                false,
                4,
            ),
            (PrivatePerpExchange::Bybit, "BTCUSDT", None, true, 4),
            (PrivatePerpExchange::Mexc, "BTC_USDT", None, true, 4),
            (PrivatePerpExchange::Htx, "BTC-USDT", None, true, 4),
        ] {
            let endpoint = build_private_ws_endpoint(
                exchange,
                PrivateWsAuth {
                    api_key: "key".to_string(),
                    api_secret: "secret".to_string(),
                    passphrase: Some("passphrase".to_string()),
                    account_id: account_id.map(str::to_string),
                    demo_trading: false,
                },
                &[ExchangeSymbol::new(exchange.exchange_id(), symbol)],
                1_700_000_000,
            )
            .unwrap();

            assert_eq!(endpoint.exchange, exchange.exchange_id(), "{exchange:?}");
            assert!(!endpoint.url.is_empty(), "{exchange:?}");
            assert_eq!(
                endpoint.login_message.is_some(),
                expects_login,
                "{exchange:?}"
            );
            assert_eq!(
                endpoint.subscribe_messages.len(),
                subscribe_count,
                "{exchange:?}"
            );
        }
    }

    #[test]
    fn gate_private_ws_endpoint_should_subscribe_account_level_channels() {
        let endpoint = build_private_ws_endpoint(
            PrivatePerpExchange::Gate,
            PrivateWsAuth {
                api_key: "key".to_string(),
                api_secret: "secret".to_string(),
                passphrase: None,
                account_id: Some("20011".to_string()),
                demo_trading: false,
            },
            &[
                ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
                ExchangeSymbol::new(ExchangeId::Gate, "ETH_USDT"),
            ],
            1_700_000_000,
        )
        .unwrap();

        assert_eq!(endpoint.exchange, ExchangeId::Gate);
        assert!(endpoint.login_message.is_none());
        assert_eq!(endpoint.subscribe_messages.len(), 4);

        let orders: Value = serde_json::from_str(&endpoint.subscribe_messages[0]).unwrap();
        assert_eq!(orders["channel"], "futures.orders");
        assert_eq!(orders["payload"][0], "20011");
        assert_eq!(orders["payload"][1], "!all");
        assert_eq!(orders["auth"]["KEY"], "key");

        let usertrades: Value = serde_json::from_str(&endpoint.subscribe_messages[1]).unwrap();
        assert_eq!(usertrades["channel"], "futures.usertrades");
        assert_eq!(usertrades["payload"][1], "!all");

        let positions: Value = serde_json::from_str(&endpoint.subscribe_messages[2]).unwrap();
        assert_eq!(positions["channel"], "futures.positions");
        assert_eq!(positions["payload"][1], "!all");

        let balances: Value = serde_json::from_str(&endpoint.subscribe_messages[3]).unwrap();
        assert_eq!(balances["channel"], "futures.balances");
        assert_eq!(balances["payload"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn new_private_ws_endpoints_should_login_and_subscribe() {
        let auth = PrivateWsAuth {
            api_key: "key".to_string(),
            api_secret: "secret".to_string(),
            passphrase: None,
            account_id: None,
            demo_trading: false,
        };

        let bybit = build_private_ws_endpoint(
            PrivatePerpExchange::Bybit,
            auth.clone(),
            &[ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT")],
            1_700_000_000,
        )
        .unwrap();
        assert_eq!(bybit.exchange, ExchangeId::Bybit);
        let bybit_login = parse_json_message(bybit.login_message.as_deref().unwrap());
        assert_eq!(bybit_login["op"], "auth");
        assert_eq!(bybit_login["args"].as_array().unwrap().len(), 3);
        assert_eq!(bybit_login["args"][0], "key");
        assert!(bybit_login["args"][1].as_i64().unwrap() > 1_700_000_000_000);
        assert!(bybit_login["args"][2].as_str().unwrap().len() > 20);
        assert_eq!(bybit.subscribe_messages.len(), 4);
        let bybit_channels = bybit
            .subscribe_messages
            .iter()
            .map(|message| {
                let value = parse_json_message(message);
                assert_eq!(value["op"], "subscribe");
                value["args"][0].as_str().unwrap().to_string()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            bybit_channels,
            vec!["order", "execution", "position", "wallet"]
        );

        let mexc = build_private_ws_endpoint(
            PrivatePerpExchange::Mexc,
            auth.clone(),
            &[ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT")],
            1_700_000_000,
        )
        .unwrap();
        assert_eq!(mexc.exchange, ExchangeId::Mexc);
        let mexc_login = parse_json_message(mexc.login_message.as_deref().unwrap());
        assert_eq!(mexc_login["method"], "login");
        assert_eq!(mexc_login["param"]["apiKey"], "key");
        assert_eq!(mexc_login["param"]["reqTime"], 1_700_000_000_000_i64);
        assert!(mexc_login["param"]["signature"].as_str().unwrap().len() > 20);
        assert_eq!(mexc.subscribe_messages.len(), 4);
        let mexc_methods = mexc
            .subscribe_messages
            .iter()
            .map(|message| {
                parse_json_message(message)["method"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            mexc_methods,
            vec![
                "sub.personal.order",
                "sub.personal.deal",
                "sub.personal.position",
                "sub.personal.asset",
            ]
        );

        let htx = build_private_ws_endpoint(
            PrivatePerpExchange::Htx,
            auth,
            &[ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT")],
            1_700_000_000,
        )
        .unwrap();
        assert_eq!(htx.exchange, ExchangeId::Htx);
        let htx_login = parse_json_message(htx.login_message.as_deref().unwrap());
        assert_eq!(htx_login["op"], "auth");
        assert_eq!(htx_login["type"], "api");
        assert_eq!(htx_login["AccessKeyId"], "key");
        assert_eq!(htx_login["SignatureMethod"], "HmacSHA256");
        assert_eq!(htx_login["SignatureVersion"], "2");
        assert!(htx_login["Timestamp"].as_str().unwrap().contains('T'));
        assert!(htx_login["Signature"].as_str().unwrap().len() > 20);
        assert_eq!(htx.subscribe_messages.len(), 4);
        let htx_topics = htx
            .subscribe_messages
            .iter()
            .map(|message| {
                let value = parse_json_message(message);
                assert_eq!(value["op"], "sub");
                value["topic"].as_str().unwrap().to_string()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            htx_topics,
            vec![
                "orders_cross.*",
                "match_orders_cross.*",
                "positions_cross.*",
                "accounts_cross.*",
            ]
        );
    }

    #[test]
    fn gate_private_ws_subscription_should_require_account_id() {
        let err = build_private_ws_endpoint(
            PrivatePerpExchange::Gate,
            PrivateWsAuth {
                api_key: "key".to_string(),
                api_secret: "secret".to_string(),
                passphrase: None,
                account_id: None,
                demo_trading: false,
            },
            &[],
            1_700_000_000,
        )
        .unwrap_err();

        assert!(err.to_string().contains("account_id"));
    }

    #[test]
    fn bitget_should_parse_order_position_and_balance_events() {
        let now = Utc::now();
        let order_raw = r#"{
          "arg":{"channel":"orders","instType":"USDT-FUTURES","instId":"BTCUSDT"},
          "data":[{
            "symbol":"BTCUSDT","orderId":"1","clientOid":"c1","side":"buy",
            "tradeSide":"open","orderType":"limit","size":"0.01","price":"65000",
            "accBaseVolume":"0.005","status":"partially_filled","uTime":"1700000000000"
          }]
        }"#;

        let events = BitgetPrivatePerpProtocol
            .parse_private_ws_message(order_raw, now)
            .unwrap();

        assert_eq!(events.len(), 1);
        match &events[0].kind {
            PrivateEventKind::Order(order) => {
                assert_eq!(order.exchange, ExchangeId::Bitget);
                assert_eq!(order.status, OrderCommandStatus::PartiallyFilled);
                assert_eq!(order.position_side, PositionSide::Long);
            }
            other => panic!("expected order event, got {other:?}"),
        }

        let fill_raw = r#"{
          "arg":{"channel":"orders","instType":"USDT-FUTURES","instId":"BTCUSDT"},
          "data":[{
            "symbol":"BTCUSDT","orderId":"1","clientOid":"c1","tradeId":"t1",
            "side":"buy","tradeSide":"open","fillPrice":"65000","fillSize":"0.01",
            "quoteVolume":"650","fee":"0.1","feeCcy":"USDT","execType":"maker",
            "tradeTime":"1700000000100"
          }]
        }"#;
        let events = BitgetPrivatePerpProtocol
            .parse_private_ws_message(fill_raw, now)
            .unwrap();
        match &events[0].kind {
            PrivateEventKind::Fill(fill) => {
                assert_eq!(fill.exchange, ExchangeId::Bitget);
                assert_eq!(fill.trade_id, "t1");
                assert_eq!(fill.side, OrderSide::Buy);
                assert_eq!(fill.position_side, PositionSide::Long);
                assert_eq!(fill.liquidity, FillLiquidity::Maker);
                assert_eq!(fill.price, 65_000.0);
                assert_eq!(fill.quantity, 0.01);
                assert_eq!(fill.quote_quantity, 650.0);
                assert_eq!(fill.fee, Some(0.1));
                assert_eq!(fill.fee_asset.as_deref(), Some("USDT"));
            }
            other => panic!("expected fill event, got {other:?}"),
        }

        let position_raw = r#"{
          "arg":{"channel":"positions","instType":"USDT-FUTURES","instId":"BTCUSDT"},
          "data":[{
            "symbol":"BTCUSDT","holdSide":"long","total":"0.02",
            "openPriceAvg":"64000","markPrice":"65000","unrealizedPL":"5",
            "uTime":"1700000000200"
          }]
        }"#;
        let events = BitgetPrivatePerpProtocol
            .parse_private_ws_message(position_raw, now)
            .unwrap();
        match &events[0].kind {
            PrivateEventKind::Position(position) => {
                assert_eq!(position.exchange, ExchangeId::Bitget);
                assert_eq!(position.position_side, PositionSide::Long);
                assert_eq!(position.quantity, 0.02);
                assert_eq!(position.entry_price, Some(64_000.0));
                assert_eq!(position.mark_price, Some(65_000.0));
                assert_eq!(position.unrealized_pnl, Some(5.0));
            }
            other => panic!("expected position event, got {other:?}"),
        }

        let balance_raw = r#"{
          "arg":{"channel":"account","instType":"USDT-FUTURES"},
          "data":[{
            "marginCoin":"USDT","equity":"100","available":"80","locked":"20"
          }]
        }"#;
        let events = BitgetPrivatePerpProtocol
            .parse_private_ws_message(balance_raw, now)
            .unwrap();
        match &events[0].kind {
            PrivateEventKind::Balance(balance) => {
                assert_eq!(balance.exchange, ExchangeId::Bitget);
                assert_eq!(balance.asset, "USDT");
                assert_eq!(balance.total, 100.0);
                assert_eq!(balance.available, 80.0);
                assert_eq!(balance.locked, 20.0);
            }
            other => panic!("expected balance event, got {other:?}"),
        }

        let login_events = BitgetPrivatePerpProtocol
            .parse_private_ws_message(r#"{"event":"login","code":"0"}"#, now)
            .unwrap();
        assert!(matches!(login_events[0].kind, PrivateEventKind::Heartbeat));

        let subscribe_events = BitgetPrivatePerpProtocol
            .parse_private_ws_message(
                r#"{"event":"subscribe","arg":{"channel":"orders","instId":"BTCUSDT"}}"#,
                now,
            )
            .unwrap();
        assert!(matches!(
            subscribe_events[0].kind,
            PrivateEventKind::Heartbeat
        ));
    }

    #[test]
    fn gate_should_parse_fill_event() {
        let now = Utc::now();
        let order_raw = r#"{
          "channel":"futures.orders","event":"update",
          "result":[{"id":"o1","contract":"BTC_USDT","size":"2","left":"1","price":"65000","text":"t-client-1","status":"open","create_time_ms":"1700000000000"}]
        }"#;
        let events = GatePrivatePerpProtocol
            .parse_private_ws_message(order_raw, now)
            .unwrap();
        assert_eq!(events.len(), 1);
        match &events[0].kind {
            PrivateEventKind::Order(order) => {
                assert_eq!(order.exchange, ExchangeId::Gate);
                assert_eq!(order.exchange_order_id.as_deref(), Some("o1"));
                assert_eq!(order.client_order_id.as_deref(), Some("client-1"));
                assert_eq!(order.side, OrderSide::Buy);
                assert_eq!(order.position_side, PositionSide::Long);
                assert_eq!(order.quantity, 2.0);
                assert_eq!(order.filled_quantity, 1.0);
                assert_eq!(order.status, OrderCommandStatus::Accepted);
            }
            other => panic!("expected order event, got {other:?}"),
        }

        let fill_raw = r#"{
          "channel":"futures.usertrades","event":"update",
          "result":[{"id":"t1","order_id":"o1","contract":"BTC_USDT","size":"-2","price":"65000","fee":"0.1","fee_currency":"USDT","role":"maker","create_time_ms":"1700000000000"}]
        }"#;

        let events = GatePrivatePerpProtocol
            .parse_private_ws_message(fill_raw, now)
            .unwrap();

        assert_eq!(events.len(), 1);
        match &events[0].kind {
            PrivateEventKind::Fill(fill) => {
                assert_eq!(fill.exchange, ExchangeId::Gate);
                assert_eq!(fill.trade_id, "t1");
                assert_eq!(fill.side, OrderSide::Sell);
                assert_eq!(fill.position_side, PositionSide::Short);
                assert_eq!(fill.liquidity, FillLiquidity::Maker);
                assert_eq!(fill.quantity, 2.0);
                assert_eq!(fill.fee, Some(0.1));
                assert_eq!(fill.fee_asset.as_deref(), Some("USDT"));
            }
            other => panic!("expected fill event, got {other:?}"),
        }

        let position_raw = r#"{
          "channel":"futures.positions","event":"update",
          "result":[{"contract":"BTC_USDT","size":"-2","entry_price":"64000","mark_price":"65000","unrealised_pnl":"-3","update_time_ms":"1700000000000"}]
        }"#;
        let events = GatePrivatePerpProtocol
            .parse_private_ws_message(position_raw, now)
            .unwrap();
        match &events[0].kind {
            PrivateEventKind::Position(position) => {
                assert_eq!(position.exchange, ExchangeId::Gate);
                assert_eq!(position.position_side, PositionSide::Short);
                assert_eq!(position.quantity, 2.0);
                assert_eq!(position.entry_price, Some(64_000.0));
                assert_eq!(position.mark_price, Some(65_000.0));
                assert_eq!(position.unrealized_pnl, Some(-3.0));
            }
            other => panic!("expected position event, got {other:?}"),
        }

        let balance_raw = r#"{
          "channel":"futures.balances","event":"update",
          "result":[{"currency":"USDT","total":"100","available":"80","order_margin":"20"}]
        }"#;
        let events = GatePrivatePerpProtocol
            .parse_private_ws_message(balance_raw, now)
            .unwrap();
        match &events[0].kind {
            PrivateEventKind::Balance(balance) => {
                assert_eq!(balance.exchange, ExchangeId::Gate);
                assert_eq!(balance.asset, "USDT");
                assert_eq!(balance.total, 100.0);
                assert_eq!(balance.available, 80.0);
                assert_eq!(balance.locked, 20.0);
            }
            other => panic!("expected balance event, got {other:?}"),
        }

        let subscribe_raw = r#"{
          "channel":"futures.orders","event":"subscribe",
          "result":{"status":"success"}
        }"#;
        let events = GatePrivatePerpProtocol
            .parse_private_ws_message(subscribe_raw, now)
            .unwrap();
        assert!(matches!(events[0].kind, PrivateEventKind::Heartbeat));
    }

    #[test]
    fn bybit_should_parse_private_events() {
        let now = Utc::now();
        let order_raw = r#"{
          "topic":"order",
          "data":[{
            "symbol":"BTCUSDT","orderId":"o1","orderLinkId":"c1","side":"Buy",
            "orderType":"Limit","qty":"0.01","price":"65000","cumExecQty":"0.005",
            "orderStatus":"PartiallyFilled","positionIdx":1,"updatedTime":"1700000000000"
          }]
        }"#;

        let events = BybitPrivatePerpProtocol
            .parse_private_ws_message(order_raw, now)
            .unwrap();

        assert_eq!(events.len(), 1);
        match &events[0].kind {
            PrivateEventKind::Order(order) => {
                assert_eq!(order.exchange, ExchangeId::Bybit);
                assert_eq!(order.position_side, PositionSide::Long);
                assert_eq!(order.status, OrderCommandStatus::PartiallyFilled);
            }
            other => panic!("expected order event, got {other:?}"),
        }

        let fill_raw = r#"{
          "topic":"execution",
          "data":[{
            "symbol":"BTCUSDT","execId":"t1","orderId":"o1","orderLinkId":"c1",
            "side":"Sell","execPrice":"65010","execQty":"0.002","execValue":"130.02",
            "execFee":"0.078","feeCurrency":"USDT","execType":"Taker",
            "positionIdx":2,"execTime":"1700000000100"
          }]
        }"#;
        let events = BybitPrivatePerpProtocol
            .parse_private_ws_message(fill_raw, now)
            .unwrap();
        match &events[0].kind {
            PrivateEventKind::Fill(fill) => {
                assert_eq!(fill.exchange, ExchangeId::Bybit);
                assert_eq!(fill.trade_id, "t1");
                assert_eq!(fill.side, OrderSide::Sell);
                assert_eq!(fill.position_side, PositionSide::Short);
                assert_eq!(fill.liquidity, FillLiquidity::Taker);
                assert_eq!(fill.price, 65_010.0);
                assert_eq!(fill.quantity, 0.002);
                assert_eq!(fill.quote_quantity, 130.02);
                assert_eq!(fill.fee, Some(0.078));
            }
            other => panic!("expected fill event, got {other:?}"),
        }

        let position_raw = r#"{
          "topic":"position",
          "data":[{
            "symbol":"BTCUSDT","side":"Sell","size":"0.004","avgPrice":"64000",
            "markPrice":"65000","unrealisedPnl":"-4","updatedTime":"1700000000200"
          }]
        }"#;
        let events = BybitPrivatePerpProtocol
            .parse_private_ws_message(position_raw, now)
            .unwrap();
        match &events[0].kind {
            PrivateEventKind::Position(position) => {
                assert_eq!(position.exchange, ExchangeId::Bybit);
                assert_eq!(position.position_side, PositionSide::Short);
                assert_eq!(position.quantity, 0.004);
                assert_eq!(position.entry_price, Some(64_000.0));
                assert_eq!(position.mark_price, Some(65_000.0));
                assert_eq!(position.unrealized_pnl, Some(-4.0));
            }
            other => panic!("expected position event, got {other:?}"),
        }

        let wallet_raw = r#"{
          "topic":"wallet",
          "data":[{
            "coin":[{
              "coin":"USDT","walletBalance":"100.5","availableToWithdraw":"80",
              "totalOrderIM":"2.5"
            }]
          }]
        }"#;
        let events = BybitPrivatePerpProtocol
            .parse_private_ws_message(wallet_raw, now)
            .unwrap();
        match &events[0].kind {
            PrivateEventKind::Balance(balance) => {
                assert_eq!(balance.exchange, ExchangeId::Bybit);
                assert_eq!(balance.asset, "USDT");
                assert_eq!(balance.total, 100.5);
                assert_eq!(balance.available, 80.0);
                assert_eq!(balance.locked, 2.5);
            }
            other => panic!("expected balance event, got {other:?}"),
        }
    }

    #[test]
    fn mexc_should_parse_private_events() {
        let now = Utc::now();
        let order_raw = r#"{
          "channel":"push.personal.order",
          "data":{"symbol":"BTC_USDT","orderId":"o1","externalOid":"c1","side":3,
            "type":1,"vol":"0.01","price":"65000","dealVol":"0.005","state":1,
            "updateTime":1700000000000}
        }"#;

        let events = MexcPrivatePerpProtocol
            .parse_private_ws_message(order_raw, now)
            .unwrap();

        assert_eq!(events.len(), 1);
        match &events[0].kind {
            PrivateEventKind::Order(order) => {
                assert_eq!(order.exchange, ExchangeId::Mexc);
                assert_eq!(order.side, OrderSide::Sell);
                assert_eq!(order.position_side, PositionSide::Short);
                assert_eq!(order.order_type, OrderType::Limit);
                assert_eq!(order.quantity, 0.01);
                assert_eq!(order.filled_quantity, 0.005);
                assert_eq!(order.status, OrderCommandStatus::PartiallyFilled);
            }
            other => panic!("expected order event, got {other:?}"),
        }

        let fill_raw = r#"{
          "channel":"push.personal.deal",
          "data":{"symbol":"BTC_USDT","orderId":"o1","tradeId":"t1","externalOid":"c1",
            "side":1,"price":"65000","vol":"0.01","amount":"650","fee":"0.1",
            "feeCurrency":"USDT","role":"maker","time":1700000000000}
        }"#;
        let events = MexcPrivatePerpProtocol
            .parse_private_ws_message(fill_raw, now)
            .unwrap();
        match &events[0].kind {
            PrivateEventKind::Fill(fill) => {
                assert_eq!(fill.exchange, ExchangeId::Mexc);
                assert_eq!(fill.trade_id, "t1");
                assert_eq!(fill.side, OrderSide::Buy);
                assert_eq!(fill.position_side, PositionSide::Long);
                assert_eq!(fill.liquidity, FillLiquidity::Maker);
                assert_eq!(fill.price, 65_000.0);
                assert_eq!(fill.quantity, 0.01);
                assert_eq!(fill.quote_quantity, 650.0);
                assert_eq!(fill.fee, Some(0.1));
                assert_eq!(fill.fee_asset.as_deref(), Some("USDT"));
            }
            other => panic!("expected fill event, got {other:?}"),
        }

        let position_raw = r#"{
          "channel":"push.personal.position",
          "data":{"symbol":"BTC_USDT","positionType":"long","holdVol":"0.02",
            "holdAvgPrice":"64000","markPrice":"65000","unrealised":"5",
            "updateTime":1700000000000}
        }"#;
        let events = MexcPrivatePerpProtocol
            .parse_private_ws_message(position_raw, now)
            .unwrap();
        match &events[0].kind {
            PrivateEventKind::Position(position) => {
                assert_eq!(position.exchange, ExchangeId::Mexc);
                assert_eq!(position.position_side, PositionSide::Long);
                assert_eq!(position.quantity, 0.02);
                assert_eq!(position.entry_price, Some(64_000.0));
                assert_eq!(position.mark_price, Some(65_000.0));
                assert_eq!(position.unrealized_pnl, Some(5.0));
            }
            other => panic!("expected position event, got {other:?}"),
        }

        let balance_raw = r#"{
          "channel":"push.personal.asset",
          "data":{"currency":"USDT","equity":"100","availableBalance":"80","frozenBalance":"20"}
        }"#;
        let events = MexcPrivatePerpProtocol
            .parse_private_ws_message(balance_raw, now)
            .unwrap();
        match &events[0].kind {
            PrivateEventKind::Balance(balance) => {
                assert_eq!(balance.exchange, ExchangeId::Mexc);
                assert_eq!(balance.asset, "USDT");
                assert_eq!(balance.total, 100.0);
                assert_eq!(balance.available, 80.0);
                assert_eq!(balance.locked, 20.0);
            }
            other => panic!("expected balance event, got {other:?}"),
        }

        let error_events = MexcPrivatePerpProtocol
            .parse_private_ws_message(
                r#"{"success":false,"code":"401","message":"bad auth"}"#,
                now,
            )
            .unwrap();
        match &error_events[0].kind {
            PrivateEventKind::Error(error) => {
                assert_eq!(error.code.as_deref(), Some("401"));
                assert!(error.message.contains("bad auth"));
            }
            other => panic!("expected error event, got {other:?}"),
        }

        let pong_events = MexcPrivatePerpProtocol
            .parse_private_ws_message(r#"{"channel":"pong"}"#, now)
            .unwrap();
        assert!(matches!(pong_events[0].kind, PrivateEventKind::Heartbeat));

        let login_events = MexcPrivatePerpProtocol
            .parse_private_ws_message(r#"{"method":"login"}"#, now)
            .unwrap();
        assert!(matches!(login_events[0].kind, PrivateEventKind::Heartbeat));

        let subscribe_events = MexcPrivatePerpProtocol
            .parse_private_ws_message(r#"{"method":"sub.personal.order","success":true}"#, now)
            .unwrap();
        assert!(subscribe_events.is_empty());
    }

    #[test]
    fn htx_should_parse_private_events() {
        let now = Utc::now();
        let order_raw = r#"{
          "op":"notify",
          "topic":"orders_cross.*",
          "data":{"contract_code":"BTC-USDT","order_id":"o1","client_order_id":"12345",
            "direction":"buy","offset":"open","volume":"0.01","price":"65000",
            "trade_volume":"0.005","trade_avg_price":"65010","order_price_type":"limit",
            "status":4,"update_time":1700000000000}
        }"#;

        let events = HtxPrivatePerpProtocol
            .parse_private_ws_message(order_raw, now)
            .unwrap();

        assert_eq!(events.len(), 1);
        match &events[0].kind {
            PrivateEventKind::Order(order) => {
                assert_eq!(order.exchange, ExchangeId::Htx);
                assert_eq!(order.side, OrderSide::Buy);
                assert_eq!(order.position_side, PositionSide::Long);
                assert_eq!(order.quantity, 0.01);
                assert_eq!(order.filled_quantity, 0.005);
                assert_eq!(order.status, OrderCommandStatus::PartiallyFilled);
            }
            other => panic!("expected order event, got {other:?}"),
        }

        let fill_raw = r#"{
          "op":"notify",
          "topic":"match_orders_cross.*",
          "data":{"contract_code":"BTC-USDT","order_id":"o1","trade_id":"t1",
            "client_order_id":"12345","direction":"sell","offset":"open",
            "trade_price":"65000","trade_volume":"0.01","trade_fee":"0.1",
            "fee_asset":"USDT","role":"taker","trade_time":1700000000000}
        }"#;
        let events = HtxPrivatePerpProtocol
            .parse_private_ws_message(fill_raw, now)
            .unwrap();
        match &events[0].kind {
            PrivateEventKind::Fill(fill) => {
                assert_eq!(fill.exchange, ExchangeId::Htx);
                assert_eq!(fill.trade_id, "t1");
                assert_eq!(fill.side, OrderSide::Sell);
                assert_eq!(fill.position_side, PositionSide::Short);
                assert_eq!(fill.liquidity, FillLiquidity::Taker);
                assert_eq!(fill.price, 65_000.0);
                assert_eq!(fill.quantity, 0.01);
                assert_eq!(fill.fee, Some(0.1));
                assert_eq!(fill.fee_asset.as_deref(), Some("USDT"));
            }
            other => panic!("expected fill event, got {other:?}"),
        }

        let position_raw = r#"{
          "op":"notify",
          "topic":"positions_cross.*",
          "data":{"contract_code":"BTC-USDT","direction":"buy","volume":"0.02",
            "cost_open":"64000","last_price":"65000","profit_unreal":"5"}
        }"#;
        let events = HtxPrivatePerpProtocol
            .parse_private_ws_message(position_raw, now)
            .unwrap();
        match &events[0].kind {
            PrivateEventKind::Position(position) => {
                assert_eq!(position.exchange, ExchangeId::Htx);
                assert_eq!(position.position_side, PositionSide::Long);
                assert_eq!(position.quantity, 0.02);
                assert_eq!(position.entry_price, Some(64_000.0));
                assert_eq!(position.mark_price, Some(65_000.0));
                assert_eq!(position.unrealized_pnl, Some(5.0));
            }
            other => panic!("expected position event, got {other:?}"),
        }

        let balance_raw = r#"{
          "op":"notify",
          "topic":"accounts_cross.*",
          "data":{"margin_asset":"USDT","margin_balance":"100",
            "withdraw_available":"80","margin_frozen":"20"}
        }"#;
        let events = HtxPrivatePerpProtocol
            .parse_private_ws_message(balance_raw, now)
            .unwrap();
        match &events[0].kind {
            PrivateEventKind::Balance(balance) => {
                assert_eq!(balance.exchange, ExchangeId::Htx);
                assert_eq!(balance.asset, "USDT");
                assert_eq!(balance.total, 100.0);
                assert_eq!(balance.available, 80.0);
                assert_eq!(balance.locked, 20.0);
            }
            other => panic!("expected balance event, got {other:?}"),
        }

        let error_events = HtxPrivatePerpProtocol
            .parse_private_ws_message(r#"{"op":"sub","err-code":"401","err-msg":"bad auth"}"#, now)
            .unwrap();
        match &error_events[0].kind {
            PrivateEventKind::Error(error) => {
                assert_eq!(error.code.as_deref(), Some("401"));
                assert!(error.message.contains("bad auth"));
            }
            other => panic!("expected error event, got {other:?}"),
        }

        let sub_events = HtxPrivatePerpProtocol
            .parse_private_ws_message(r#"{"op":"sub","topic":"orders_cross.*"}"#, now)
            .unwrap();
        assert!(matches!(sub_events[0].kind, PrivateEventKind::Heartbeat));

        let auth_events = HtxPrivatePerpProtocol
            .parse_private_ws_message(r#"{"op":"auth","ts":1700000000000}"#, now)
            .unwrap();
        assert!(matches!(auth_events[0].kind, PrivateEventKind::Heartbeat));

        let ping_events = HtxPrivatePerpProtocol
            .parse_private_ws_message(r#"{"op":"ping","ts":1700000000000}"#, now)
            .unwrap();
        assert!(matches!(ping_events[0].kind, PrivateEventKind::Heartbeat));

        let pong_events = HtxPrivatePerpProtocol
            .parse_private_ws_message(r#"{"op":"pong","ts":1700000000000}"#, now)
            .unwrap();
        assert!(matches!(pong_events[0].kind, PrivateEventKind::Heartbeat));

        let top_level_ping_events = HtxPrivatePerpProtocol
            .parse_private_ws_message(r#"{"ping":1700000000000}"#, now)
            .unwrap();
        assert!(matches!(
            top_level_ping_events[0].kind,
            PrivateEventKind::Heartbeat
        ));

        let top_level_pong_events = HtxPrivatePerpProtocol
            .parse_private_ws_message(r#"{"pong":1700000000000}"#, now)
            .unwrap();
        assert!(matches!(
            top_level_pong_events[0].kind,
            PrivateEventKind::Heartbeat
        ));
    }

    #[tokio::test]
    async fn private_perp_trading_adapter_should_place_order_via_protocol() {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "data": {"orderId": "exchange-1", "clientOid": "client-1"}
        }));
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport.clone())
            .with_position_mode(PositionMode::Hedge);

        let ack = adapter
            .place_order(command(ExchangeId::Bitget, "BTCUSDT"))
            .await
            .unwrap();

        assert_eq!(ack.exchange, ExchangeId::Bitget);
        assert_eq!(ack.exchange_order_id.as_deref(), Some("exchange-1"));
        let seen = transport.seen.lock().unwrap();
        assert_eq!(seen[0].path, "/api/v2/mix/order/place-order");
    }

    #[tokio::test]
    async fn non_binance_adapters_should_route_place_order_requests() {
        let okx_transport = MockTransport::new(json!({
            "code": "0",
            "data": [{"ordId": "okx-order-1", "clOrdId": "crossarb-ls-mk-1-c0b4e763"}]
        }));
        let okx_adapter =
            PrivatePerpTradingAdapter::new(OkxPrivatePerpProtocol, okx_transport.clone())
                .with_position_mode(PositionMode::Hedge);
        let okx_ack = okx_adapter
            .place_order(command(ExchangeId::Okx, "BTC-USDT-SWAP"))
            .await
            .unwrap();
        assert_eq!(okx_ack.exchange, ExchangeId::Okx);
        assert_eq!(okx_ack.exchange_order_id.as_deref(), Some("okx-order-1"));
        let seen = okx_transport.seen.lock().unwrap();
        assert_eq!(seen[0].path, "/api/v5/trade/order");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["instId"], "BTC-USDT-SWAP");
        assert_eq!(body["tdMode"], "cross");
        assert_eq!(body["side"], "buy");
        assert_eq!(body["ordType"], "post_only");
        assert_eq!(body["posSide"], "long");
        drop(seen);

        let bitget_transport = MockTransport::new(json!({
            "code": "00000",
            "data": {"orderId": "bitget-order-1", "clientOid": "crossarb-ls-mk-1-c0b4e763"}
        }));
        let bitget_adapter =
            PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, bitget_transport.clone())
                .with_position_mode(PositionMode::Hedge);
        let bitget_ack = bitget_adapter
            .place_order(command(ExchangeId::Bitget, "BTCUSDT"))
            .await
            .unwrap();
        assert_eq!(bitget_ack.exchange, ExchangeId::Bitget);
        assert_eq!(
            bitget_ack.exchange_order_id.as_deref(),
            Some("bitget-order-1")
        );
        let seen = bitget_transport.seen.lock().unwrap();
        assert_eq!(seen[0].path, "/api/v2/mix/order/place-order");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["symbol"], "BTCUSDT");
        assert_eq!(body["productType"], "USDT-FUTURES");
        assert_eq!(body["side"], "buy");
        assert_eq!(body["tradeSide"], "open");
        assert_eq!(body["orderType"], "limit");
        assert_eq!(body["force"], "post_only");
        assert_eq!(body["size"], "0.01");
        drop(seen);

        let gate_transport = MockTransport::new(json!({
            "id": "gate-order-1",
            "text": "t-crossarb-ls-mk-1-c0b4e763",
            "status": "open"
        }));
        let gate_adapter =
            PrivatePerpTradingAdapter::new(GatePrivatePerpProtocol, gate_transport.clone());
        let mut gate_command = command(ExchangeId::Gate, "BTC_USDT");
        gate_command.quantity = 1.0;
        let gate_ack = gate_adapter.place_order(gate_command).await.unwrap();
        assert_eq!(gate_ack.exchange, ExchangeId::Gate);
        assert_eq!(gate_ack.exchange_order_id.as_deref(), Some("gate-order-1"));
        let seen = gate_transport.seen.lock().unwrap();
        assert_eq!(seen[0].path, "/futures/usdt/orders");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["contract"], "BTC_USDT");
        assert_eq!(body["size"], 1.0);
        assert_eq!(body["tif"], "poc");
        assert_eq!(body["text"], "t-crossarb-ls-mk-1-c0b4e763");
        assert_eq!(body["reduce_only"], false);
        drop(seen);

        let bybit_transport = MockTransport::new(json!({
            "retCode": 0,
            "retMsg": "OK",
            "result": {"orderId": "bybit-order-1", "orderLinkId": "crossarb-ls-mk-1-c0b4e763"}
        }));
        let bybit_adapter =
            PrivatePerpTradingAdapter::new(BybitPrivatePerpProtocol, bybit_transport.clone())
                .with_position_mode(PositionMode::Hedge);
        let bybit_ack = bybit_adapter
            .place_order(command(ExchangeId::Bybit, "BTCUSDT"))
            .await
            .unwrap();
        assert_eq!(bybit_ack.exchange, ExchangeId::Bybit);
        assert_eq!(
            bybit_ack.exchange_order_id.as_deref(),
            Some("bybit-order-1")
        );
        let seen = bybit_transport.seen.lock().unwrap();
        assert_eq!(seen[0].path, "/v5/order/create");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["category"], "linear");
        assert_eq!(body["symbol"], "BTCUSDT");
        assert_eq!(body["side"], "Buy");
        assert_eq!(body["orderType"], "Limit");
        assert_eq!(body["timeInForce"], "PostOnly");
        assert_eq!(body["positionIdx"], 1);
        drop(seen);

        let mexc_transport = MockTransport::new(json!({
            "success": true,
            "code": 0,
            "data": {"orderId": "mexc-order-1", "externalOid": "crossarb-ls-mk-1-c0b4e763"}
        }));
        let mexc_adapter =
            PrivatePerpTradingAdapter::new(MexcPrivatePerpProtocol, mexc_transport.clone())
                .with_position_mode(PositionMode::Hedge);
        let mexc_ack = mexc_adapter
            .place_order(command(ExchangeId::Mexc, "BTC_USDT"))
            .await
            .unwrap();
        assert_eq!(mexc_ack.exchange, ExchangeId::Mexc);
        assert_eq!(mexc_ack.exchange_order_id.as_deref(), Some("mexc-order-1"));
        let seen = mexc_transport.seen.lock().unwrap();
        assert_eq!(seen[0].path, "/api/v1/private/order/submit");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["symbol"], "BTC_USDT");
        assert_eq!(body["side"], 1);
        assert_eq!(body["type"], 2);
        assert_eq!(body["vol"], "0.01");
        assert_eq!(body["openType"], 2);
        assert_eq!(body["positionMode"], "hedge_mode");
        drop(seen);

        let htx_transport = MockTransport::new(json!({
            "status": "ok",
            "data": {"order_id_str": "htx-order-1", "client_order_id": 123456}
        }));
        let htx_adapter =
            PrivatePerpTradingAdapter::new(HtxPrivatePerpProtocol, htx_transport.clone())
                .with_position_mode(PositionMode::Hedge);
        let htx_ack = htx_adapter
            .place_order(command(ExchangeId::Htx, "BTC-USDT"))
            .await
            .unwrap();
        assert_eq!(htx_ack.exchange, ExchangeId::Htx);
        assert_eq!(htx_ack.exchange_order_id.as_deref(), Some("htx-order-1"));
        let seen = htx_transport.seen.lock().unwrap();
        assert_eq!(seen[0].path, "/linear-swap-api/v1/swap_cross_order");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["contract_code"], "BTC-USDT");
        assert_eq!(body["direction"], "buy");
        assert_eq!(body["offset"], "open");
        assert_eq!(body["volume"], "0.01");
        assert_eq!(body["order_price_type"], "post_only");
    }

    #[tokio::test]
    async fn private_perp_trading_adapter_should_apply_instrument_precision() {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "data": {"orderId": "exchange-1"}
        }));
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport.clone())
            .with_instruments([instrument(ExchangeId::Bitget, "BTCUSDT")]);
        let mut cmd = command(ExchangeId::Bitget, "BTCUSDT");
        cmd.quantity = 0.01234;
        cmd.price = Some(65_000.24);

        adapter.place_order(cmd).await.unwrap();

        let seen = transport.seen.lock().unwrap();
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["size"], "0.012");
        assert_eq!(body["price"], "65000");
    }

    #[tokio::test]
    async fn gate_adapter_should_send_contract_count_for_non_unit_contract_size() {
        let transport = MockTransport::new(json!({"id": "gate-order-1"}));
        let adapter = PrivatePerpTradingAdapter::new(GatePrivatePerpProtocol, transport.clone())
            .with_instruments([gate_contract_instrument(0.0001)]);
        let mut cmd = command(ExchangeId::Gate, "BTC_USDT");
        cmd.side = OrderSide::Sell;
        cmd.quantity = 0.01234;
        cmd.price = Some(65_000.24);

        adapter.place_order(cmd).await.unwrap();

        let seen = transport.seen.lock().unwrap();
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["size"], -123);
        assert_eq!(body["price"], "65000");
    }

    #[tokio::test]
    async fn gate_adapter_should_place_batch_orders_and_split_failures() {
        let transport = MockTransport::new(json!([
            {
                "succeeded": true,
                "id": 15675394,
                "contract": "BTC_USDT",
                "size": 123,
                "left": 123,
                "price": "65000",
                "tif": "gtc",
                "text": "t-client-1",
                "status": "open"
            },
            {
                "succeeded": false,
                "label": "INSUFFICIENT_AVAILABLE",
                "detail": "balance not enough",
                "text": "t-client-2"
            }
        ]));
        let adapter = PrivatePerpTradingAdapter::new(GatePrivatePerpProtocol, transport.clone())
            .with_instruments([gate_contract_instrument(0.0001)]);
        let mut first = command(ExchangeId::Gate, "BTC_USDT");
        first.client_order_id = "client-1".to_string();
        first.quantity = 0.01234;
        first.price = Some(65_000.24);
        first.time_in_force = TimeInForce::Gtc;
        first.post_only = false;

        let mut second = first.clone();
        second.client_order_id = "client-2".to_string();
        second.side = OrderSide::Sell;
        second.quantity = 0.02;

        let ack = adapter
            .place_batch_orders(BatchPlaceCommand::new(
                ExchangeId::Gate,
                [first, second],
                Utc::now(),
            ))
            .await
            .unwrap();

        assert_batch_place_ack(&ack, ExchangeId::Gate, false, 1, 1, None);
        assert_eq!(ack.order_acks[0].exchange, ExchangeId::Gate);
        assert_eq!(ack.order_acks[0].client_order_id, "client-1");
        assert_eq!(
            ack.order_acks[0].exchange_order_id.as_deref(),
            Some("15675394")
        );
        assert!(ack.order_acks[0].accepted);
        assert_eq!(ack.order_acks[0].status, OrderCommandStatus::Accepted);
        assert!(ack.order_acks[0].message.is_none());
        assert_eq!(ack.failed_orders[0].order.client_order_id, "client-2");
        assert_eq!(
            ack.failed_orders[0].code.as_deref(),
            Some("INSUFFICIENT_AVAILABLE")
        );
        assert_eq!(ack.failed_orders[0].message, "balance not enough");

        let seen = transport.seen.lock().unwrap();
        assert_eq!(seen[0].path, "/futures/usdt/batch_orders");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body[0]["size"], 123);
        assert_eq!(body[0]["price"], "65000");
        assert_eq!(body[1]["size"], -200);
    }

    #[tokio::test]
    async fn gate_adapter_should_batch_cancel_by_ids_and_count_failures() {
        let transport = MockTransport::new(json!([
            {"id": "order-1", "succeeded": true},
            {"id": "order-2", "message": "ORDER_NOT_FOUND"}
        ]));
        let adapter = PrivatePerpTradingAdapter::new(GatePrivatePerpProtocol, transport.clone());
        let cancel_one = CancelCommand {
            exchange: ExchangeId::Gate,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
            client_order_id: None,
            exchange_order_id: Some("order-1".to_string()),
            reason: None,
            requested_at: Utc::now(),
        };
        let mut cancel_two = cancel_one.clone();
        cancel_two.exchange_order_id = Some("order-2".to_string());

        let ack = adapter
            .cancel_batch_orders(CancelBatchCommand::new(
                ExchangeId::Gate,
                [cancel_one, cancel_two],
                Utc::now(),
            ))
            .await
            .unwrap();

        assert_cancel_batch_ack(&ack, ExchangeId::Gate, false, 1, 2, None);
        assert!(ack.order_acks[0].accepted);
        assert_eq!(ack.order_acks[0].exchange, ExchangeId::Gate);
        assert_eq!(
            ack.order_acks[0].exchange_order_id.as_deref(),
            Some("order-1")
        );
        assert_eq!(ack.order_acks[0].status, OrderCommandStatus::Cancelled);
        assert!(ack.order_acks[0].message.is_none());
        assert!(!ack.order_acks[1].accepted);
        assert_eq!(ack.order_acks[1].exchange, ExchangeId::Gate);
        assert_eq!(
            ack.order_acks[1].exchange_order_id.as_deref(),
            Some("order-2")
        );
        assert_eq!(ack.order_acks[1].status, OrderCommandStatus::Rejected);
        assert_eq!(
            ack.order_acks[1].message.as_deref(),
            Some("ORDER_NOT_FOUND")
        );

        let seen = transport.seen.lock().unwrap();
        assert_eq!(seen[0].path, "/futures/usdt/batch_cancel_orders");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body[0], "order-1");
        assert_eq!(body[1], "order-2");
    }

    #[tokio::test]
    async fn gate_adapter_should_not_double_convert_contract_quantity() {
        let transport = MockTransport::new(json!({"id": "gate-order-1"}));
        let adapter = PrivatePerpTradingAdapter::new(GatePrivatePerpProtocol, transport.clone())
            .with_instruments([InstrumentMeta::new(
                ExchangeId::Gate,
                CanonicalSymbol::new("BTC", "USDT"),
                ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
                "BTC",
                "USDT",
                "USDT",
                crate::market::ContractType::LinearPerpetual,
                0.01,
                0.01,
                1.0,
                1.0,
                1.0,
                2,
                0,
                crate::market::InstrumentStatus::Trading,
            )]);
        let mut cmd = command(ExchangeId::Gate, "BTC_USDT");
        cmd.quantity = 0.05;
        cmd.price = Some(192.15);

        adapter.place_order(cmd).await.unwrap();

        let seen = transport.seen.lock().unwrap();
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["size"], 5);
        assert_eq!(body["price"], "192.15");
    }

    #[tokio::test]
    async fn private_perp_trading_adapter_should_reject_below_min_notional() {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "data": {"orderId": "exchange-1"}
        }));
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport)
            .with_instruments([instrument(ExchangeId::Bitget, "BTCUSDT")]);
        let mut cmd = command(ExchangeId::Bitget, "BTCUSDT");
        cmd.quantity = 0.0001;
        cmd.price = Some(65_000.0);

        let err = adapter.place_order(cmd).await.unwrap_err();

        assert!(err.to_string().contains("precision rules"));
        assert!(err.to_string().contains("BelowMinQuantity"));
    }

    #[tokio::test]
    async fn private_perp_trading_adapter_should_reject_invalid_order_quantity() {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "data": {"orderId": "exchange-1"}
        }));
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport.clone());
        let mut cmd = command(ExchangeId::Bitget, "BTCUSDT");
        cmd.quantity = 0.0;

        let err = adapter.place_order(cmd.clone()).await.unwrap_err();
        assert!(err
            .to_string()
            .contains("bitget order quantity must be positive and finite"));

        cmd.quantity = 0.01;
        cmd.price = Some(-1.0);
        let err = adapter.place_order(cmd.clone()).await.unwrap_err();
        assert!(err
            .to_string()
            .contains("bitget order price must be positive and finite"));

        cmd.quantity = f64::NAN;
        cmd.price = Some(65_000.0);
        let err = adapter
            .place_batch_orders(BatchPlaceCommand::new(
                ExchangeId::Bitget,
                [cmd],
                Utc::now(),
            ))
            .await
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("bitget order quantity must be positive and finite"));
        assert!(transport.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn private_perp_trading_adapter_should_reject_order_scope_before_quantity() {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "data": {"orderId": "exchange-1"}
        }));
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport.clone());
        let mut cmd = command(ExchangeId::Gate, "BTC_USDT");
        cmd.quantity = 0.0;

        let err = adapter.place_order(cmd.clone()).await.unwrap_err();
        assert!(err
            .to_string()
            .contains("bitget place order requires bitget exchange symbols"));

        cmd.quantity = f64::NAN;
        let err = adapter
            .place_batch_orders(BatchPlaceCommand::new(
                ExchangeId::Bitget,
                [cmd],
                Utc::now(),
            ))
            .await
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("bitget place order requires bitget exchange symbols"));
        assert!(transport.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn private_perp_trading_adapter_should_reject_wrong_exchange_empty_batch_place() {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "data": []
        }));
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport.clone());

        let err = adapter
            .place_batch_orders(BatchPlaceCommand::new(
                ExchangeId::Gate,
                Vec::<OrderCommand>::new(),
                Utc::now(),
            ))
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("bitget batch-place requires bitget exchange symbols"));
        assert!(transport.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn private_perp_trading_adapter_should_reject_wrong_exchange_empty_batch_cancel() {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "data": []
        }));
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport.clone());

        let err = adapter
            .cancel_batch_orders(CancelBatchCommand::new(
                ExchangeId::Gate,
                Vec::<CancelCommand>::new(),
                Utc::now(),
            ))
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("bitget batch cancel requires bitget exchange symbols"));
        assert!(transport.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn private_perp_trading_adapter_should_ack_empty_batches_without_rest() {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "data": []
        }));
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport.clone());

        let place_ack = adapter
            .place_batch_orders(BatchPlaceCommand::new(
                ExchangeId::Bitget,
                Vec::<OrderCommand>::new(),
                Utc::now(),
            ))
            .await
            .unwrap();
        assert!(place_ack.accepted);
        assert_eq!(place_ack.exchange, ExchangeId::Bitget);
        assert_eq!(place_ack.placed_orders, 0);
        assert!(place_ack.order_acks.is_empty());
        assert!(place_ack.failed_orders.is_empty());
        assert_eq!(place_ack.message.as_deref(), Some("empty batch-place"));

        let cancel_ack = adapter
            .cancel_batch_orders(CancelBatchCommand::new(
                ExchangeId::Bitget,
                Vec::<CancelCommand>::new(),
                Utc::now(),
            ))
            .await
            .unwrap();
        assert!(cancel_ack.accepted);
        assert_eq!(cancel_ack.exchange, ExchangeId::Bitget);
        assert_eq!(cancel_ack.cancelled_orders, 0);
        assert!(cancel_ack.order_acks.is_empty());
        assert_eq!(cancel_ack.message.as_deref(), Some("empty cancel batch"));
        assert!(transport.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn private_perp_trading_adapter_should_reject_batch_cancel_order_scope_before_request() {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "data": []
        }));
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport.clone());

        let err = adapter
            .cancel_batch_orders(CancelBatchCommand::new(
                ExchangeId::Bitget,
                [CancelCommand {
                    exchange: ExchangeId::Gate,
                    canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                    exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
                    client_order_id: Some("client-1".to_string()),
                    exchange_order_id: None,
                    reason: Some("test".to_string()),
                    requested_at: Utc::now(),
                }],
                Utc::now(),
            ))
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("bitget batch cancel requires bitget exchange symbols"));
        assert!(transport.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn private_perp_trading_adapter_should_reject_cancel_scope_before_request() {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "data": {"orderId": "exchange-1"}
        }));
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport.clone());

        let err = adapter
            .cancel_order(CancelCommand {
                exchange: ExchangeId::Gate,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: None,
                reason: Some("test".to_string()),
                requested_at: Utc::now(),
            })
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("bitget cancel requires bitget exchange symbols"));
        assert!(transport.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn non_binance_adapters_should_route_single_cancel_requests() {
        let okx_transport = MockTransport::new(json!({
            "code": "0",
            "data": [{"ordId": "okx-order-1", "clOrdId": "client-1"}]
        }));
        let okx_adapter =
            PrivatePerpTradingAdapter::new(OkxPrivatePerpProtocol, okx_transport.clone());
        let okx_ack = okx_adapter
            .cancel_order(CancelCommand {
                exchange: ExchangeId::Okx,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: Some("okx-order-1".to_string()),
                reason: Some("test".to_string()),
                requested_at: Utc::now(),
            })
            .await
            .unwrap();
        assert!(okx_ack.accepted);
        assert_eq!(okx_ack.exchange_order_id.as_deref(), Some("okx-order-1"));
        let seen = okx_transport.seen.lock().unwrap();
        assert_eq!(seen[0].path, "/api/v5/trade/cancel-order");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["instId"], "BTC-USDT-SWAP");
        assert_eq!(body["ordId"], "okx-order-1");
        assert_eq!(body["clOrdId"], "client-1");
        drop(seen);

        let bitget_transport = MockTransport::new(json!({
            "code": "00000",
            "data": {"orderId": "bitget-order-1", "clientOid": "client-1"}
        }));
        let bitget_adapter =
            PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, bitget_transport.clone());
        let bitget_ack = bitget_adapter
            .cancel_order(CancelCommand {
                exchange: ExchangeId::Bitget,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: Some("bitget-order-1".to_string()),
                reason: Some("test".to_string()),
                requested_at: Utc::now(),
            })
            .await
            .unwrap();
        assert!(bitget_ack.accepted);
        assert_eq!(
            bitget_ack.exchange_order_id.as_deref(),
            Some("bitget-order-1")
        );
        let seen = bitget_transport.seen.lock().unwrap();
        assert_eq!(seen[0].path, "/api/v2/mix/order/cancel-order");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["symbol"], "BTCUSDT");
        assert_eq!(body["orderId"], "bitget-order-1");
        assert_eq!(body["clientOid"], "client-1");
        drop(seen);

        let gate_transport = MockTransport::new(json!({
            "id": "gate-order-1",
            "text": "t-client-1",
            "status": "cancelled"
        }));
        let gate_adapter =
            PrivatePerpTradingAdapter::new(GatePrivatePerpProtocol, gate_transport.clone());
        let gate_ack = gate_adapter
            .cancel_order(CancelCommand {
                exchange: ExchangeId::Gate,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: Some("gate-order-1".to_string()),
                reason: Some("test".to_string()),
                requested_at: Utc::now(),
            })
            .await
            .unwrap();
        assert!(gate_ack.accepted);
        assert_eq!(gate_ack.exchange_order_id.as_deref(), Some("gate-order-1"));
        let seen = gate_transport.seen.lock().unwrap();
        assert_eq!(seen[0].method, PrivateRestMethod::Delete);
        assert_eq!(seen[0].path, "/futures/usdt/orders/gate-order-1");
        assert_eq!(
            seen[0].query.get("contract").map(String::as_str),
            Some("BTC_USDT")
        );
        drop(seen);

        let bybit_transport = MockTransport::new(json!({
            "retCode": 0,
            "retMsg": "OK",
            "result": {"orderId": "bybit-order-1", "orderLinkId": "client-1"}
        }));
        let bybit_adapter =
            PrivatePerpTradingAdapter::new(BybitPrivatePerpProtocol, bybit_transport.clone());
        let bybit_ack = bybit_adapter
            .cancel_order(CancelCommand {
                exchange: ExchangeId::Bybit,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT"),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: Some("bybit-order-1".to_string()),
                reason: Some("test".to_string()),
                requested_at: Utc::now(),
            })
            .await
            .unwrap();
        assert!(bybit_ack.accepted);
        assert_eq!(
            bybit_ack.exchange_order_id.as_deref(),
            Some("bybit-order-1")
        );
        let seen = bybit_transport.seen.lock().unwrap();
        assert_eq!(seen[0].path, "/v5/order/cancel");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["category"], "linear");
        assert_eq!(body["symbol"], "BTCUSDT");
        assert_eq!(body["orderId"], "bybit-order-1");
        assert_eq!(body["orderLinkId"], "client-1");
        drop(seen);

        let mexc_transport = MockTransport::new(json!({
            "success": true,
            "code": 0,
            "data": {"orderId": "mexc-order-1", "externalOid": "client-1"}
        }));
        let mexc_adapter =
            PrivatePerpTradingAdapter::new(MexcPrivatePerpProtocol, mexc_transport.clone());
        let mexc_ack = mexc_adapter
            .cancel_order(CancelCommand {
                exchange: ExchangeId::Mexc,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: Some("mexc-order-1".to_string()),
                reason: Some("test".to_string()),
                requested_at: Utc::now(),
            })
            .await
            .unwrap();
        assert!(mexc_ack.accepted);
        assert_eq!(mexc_ack.exchange_order_id.as_deref(), Some("mexc-order-1"));
        let seen = mexc_transport.seen.lock().unwrap();
        assert_eq!(seen[0].path, "/api/v1/private/order/cancel");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["orderId"], "mexc-order-1");
        assert_eq!(body["externalOid"], "client-1");
        assert_eq!(body["symbol"], "BTC_USDT");
        drop(seen);

        let htx_transport = MockTransport::new(json!({
            "status": "ok",
            "data": {
                "successes": "htx-order-1",
                "errors": []
            }
        }));
        let htx_adapter =
            PrivatePerpTradingAdapter::new(HtxPrivatePerpProtocol, htx_transport.clone());
        let htx_ack = htx_adapter
            .cancel_order(CancelCommand {
                exchange: ExchangeId::Htx,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: Some("htx-order-1".to_string()),
                reason: Some("test".to_string()),
                requested_at: Utc::now(),
            })
            .await
            .unwrap();
        assert!(htx_ack.accepted);
        assert_eq!(htx_ack.exchange_order_id.as_deref(), Some("htx-order-1"));
        let seen = htx_transport.seen.lock().unwrap();
        assert_eq!(seen[0].path, "/linear-swap-api/v1/swap_cross_cancel");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["contract_code"], "BTC-USDT");
        assert_eq!(body["order_id"], "htx-order-1");
        assert!(body["client_order_id"].as_str().is_some());
    }

    #[tokio::test]
    async fn non_binance_adapters_should_route_single_order_readbacks() {
        let okx_transport = MockTransport::new(json!({
            "code": "0",
            "data": [{
                "instId": "BTC-USDT-SWAP",
                "ordId": "okx-order-1",
                "clOrdId": "client-1",
                "side": "buy",
                "posSide": "long",
                "ordType": "limit",
                "state": "live",
                "px": "65000",
                "sz": "0.02",
                "accFillSz": "0.01",
                "uTime": "1700000000000"
            }]
        }));
        let okx_adapter =
            PrivatePerpTradingAdapter::new(OkxPrivatePerpProtocol, okx_transport.clone());
        let okx_order = okx_adapter
            .get_order(OrderQuery {
                exchange: ExchangeId::Okx,
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: Some("okx-order-1".to_string()),
            })
            .await
            .unwrap();
        assert_eq!(okx_order.exchange, ExchangeId::Okx);
        assert_eq!(okx_order.exchange_order_id.as_deref(), Some("okx-order-1"));
        assert_eq!(okx_order.client_order_id.as_deref(), Some("client-1"));
        assert_single_order_fields(
            &okx_order,
            (
                OrderSide::Buy,
                PositionSide::Long,
                OrderType::Limit,
                OrderCommandStatus::Accepted,
            ),
            0.02,
            0.01,
            65_000.0,
            None,
        );
        let seen = okx_transport.seen.lock().unwrap();
        assert_eq!(seen[0].path, "/api/v5/trade/order");
        assert_eq!(
            seen[0].query.get("instId").map(String::as_str),
            Some("BTC-USDT-SWAP")
        );
        assert_eq!(
            seen[0].query.get("ordId").map(String::as_str),
            Some("okx-order-1")
        );
        assert_eq!(
            seen[0].query.get("clOrdId").map(String::as_str),
            Some("client-1")
        );
        drop(seen);

        let bitget_transport = MockTransport::new(json!({
            "code": "00000",
            "data": {
                "symbol": "BTCUSDT",
                "orderId": "bitget-order-1",
                "clientOid": "client-1",
                "side": "buy",
                "tradeSide": "open",
                "orderType": "limit",
                "status": "live",
                "price": "65000",
                "size": "0.02",
                "accBaseVolume": "0.01",
                "priceAvg": "64950",
                "uTime": "1700000000000"
            }
        }));
        let bitget_adapter =
            PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, bitget_transport.clone());
        let bitget_order = bitget_adapter
            .get_order(OrderQuery {
                exchange: ExchangeId::Bitget,
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: Some("bitget-order-1".to_string()),
            })
            .await
            .unwrap();
        assert_eq!(bitget_order.exchange, ExchangeId::Bitget);
        assert_eq!(
            bitget_order.exchange_order_id.as_deref(),
            Some("bitget-order-1")
        );
        assert_eq!(bitget_order.client_order_id.as_deref(), Some("client-1"));
        assert_single_order_fields(
            &bitget_order,
            (
                OrderSide::Buy,
                PositionSide::Long,
                OrderType::Limit,
                OrderCommandStatus::PartiallyFilled,
            ),
            0.02,
            0.01,
            65_000.0,
            Some(64_950.0),
        );
        let seen = bitget_transport.seen.lock().unwrap();
        assert_eq!(seen[0].path, "/api/v2/mix/order/detail");
        assert_eq!(
            seen[0].query.get("orderId").map(String::as_str),
            Some("bitget-order-1")
        );
        assert_eq!(
            seen[0].query.get("clientOid").map(String::as_str),
            Some("client-1")
        );
        drop(seen);

        let gate_transport = MockTransport::new(json!({
            "id": "gate-order-1",
            "contract": "BTC_USDT",
            "text": "t-client-1",
            "size": "25",
            "left": "5",
            "price": "65000",
            "fill_price": "64950",
            "status": "open",
            "create_time_ms": "1700000000000"
        }));
        let gate_adapter = PrivatePerpTradingAdapter::new(GatePrivatePerpProtocol, gate_transport)
            .with_instruments([gate_contract_instrument(0.0001)]);
        let gate_order = gate_adapter
            .get_order(OrderQuery {
                exchange: ExchangeId::Gate,
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: Some("gate-order-1".to_string()),
            })
            .await
            .unwrap();
        assert_eq!(gate_order.exchange, ExchangeId::Gate);
        assert_eq!(
            gate_order.exchange_order_id.as_deref(),
            Some("gate-order-1")
        );
        assert_eq!(gate_order.client_order_id.as_deref(), Some("client-1"));
        assert_single_order_fields(
            &gate_order,
            (
                OrderSide::Buy,
                PositionSide::Long,
                OrderType::Limit,
                OrderCommandStatus::Accepted,
            ),
            0.0025,
            0.002,
            65_000.0,
            Some(64_950.0),
        );

        let bybit_transport = MockTransport::new(json!({
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": [{
                    "symbol": "BTCUSDT",
                    "orderId": "bybit-order-1",
                    "orderLinkId": "client-1",
                    "side": "Buy",
                    "positionIdx": 1,
                    "orderType": "Limit",
                    "orderStatus": "New",
                    "price": "65000",
                    "qty": "0.02",
                    "cumExecQty": "0.01",
                    "avgPrice": "64950",
                    "updatedTime": "1700000000000"
                }]
            }
        }));
        let bybit_adapter =
            PrivatePerpTradingAdapter::new(BybitPrivatePerpProtocol, bybit_transport.clone());
        let bybit_order = bybit_adapter
            .get_order(OrderQuery {
                exchange: ExchangeId::Bybit,
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT"),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: Some("bybit-order-1".to_string()),
            })
            .await
            .unwrap();
        assert_eq!(bybit_order.exchange, ExchangeId::Bybit);
        assert_eq!(
            bybit_order.exchange_order_id.as_deref(),
            Some("bybit-order-1")
        );
        assert_eq!(bybit_order.client_order_id.as_deref(), Some("client-1"));
        assert_single_order_fields(
            &bybit_order,
            (
                OrderSide::Buy,
                PositionSide::Long,
                OrderType::Limit,
                OrderCommandStatus::Accepted,
            ),
            0.02,
            0.01,
            65_000.0,
            Some(64_950.0),
        );
        let seen = bybit_transport.seen.lock().unwrap();
        assert_eq!(seen[0].path, "/v5/order/realtime");
        assert_eq!(
            seen[0].query.get("orderId").map(String::as_str),
            Some("bybit-order-1")
        );
        drop(seen);

        let mexc_transport = MockTransport::new(json!({
            "success": true,
            "code": 0,
            "data": {
                "symbol": "BTC_USDT",
                "orderId": "mexc-order-1",
                "externalOid": "client-1",
                "side": 1,
                "type": 1,
                "state": 2,
                "price": "65000",
                "vol": "0.02",
                "dealVol": "0.01",
                "dealAvgPrice": "64950",
                "updateTime": "1700000000000"
            }
        }));
        let mexc_adapter =
            PrivatePerpTradingAdapter::new(MexcPrivatePerpProtocol, mexc_transport.clone());
        let mexc_order = mexc_adapter
            .get_order(OrderQuery {
                exchange: ExchangeId::Mexc,
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: Some("mexc-order-1".to_string()),
            })
            .await
            .unwrap();
        assert_eq!(mexc_order.exchange, ExchangeId::Mexc);
        assert_eq!(
            mexc_order.exchange_order_id.as_deref(),
            Some("mexc-order-1")
        );
        assert_eq!(mexc_order.client_order_id.as_deref(), Some("client-1"));
        assert_single_order_fields(
            &mexc_order,
            (
                OrderSide::Buy,
                PositionSide::Long,
                OrderType::Limit,
                OrderCommandStatus::PartiallyFilled,
            ),
            0.02,
            0.01,
            65_000.0,
            Some(64_950.0),
        );
        let seen = mexc_transport.seen.lock().unwrap();
        assert_eq!(seen[0].path, "/api/v1/private/order/get/mexc-order-1");
        drop(seen);

        let htx_transport = MockTransport::new(json!({
            "status": "ok",
            "data": {
                "contract_code": "BTC-USDT",
                "order_id_str": "htx-order-1",
                "client_order_id": "12345",
                "direction": "buy",
                "offset": "open",
                "order_price_type": "limit",
                "status": 3,
                "price": "65000",
                "volume": "2",
                "trade_volume": "1",
                "trade_avg_price": "64950",
                "update_time": "1700000000000"
            }
        }));
        let htx_adapter =
            PrivatePerpTradingAdapter::new(HtxPrivatePerpProtocol, htx_transport.clone());
        let htx_order = htx_adapter
            .get_order(OrderQuery {
                exchange: ExchangeId::Htx,
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: Some("htx-order-1".to_string()),
            })
            .await
            .unwrap();
        assert_eq!(htx_order.exchange, ExchangeId::Htx);
        assert_eq!(htx_order.exchange_order_id.as_deref(), Some("htx-order-1"));
        assert_eq!(htx_order.client_order_id.as_deref(), Some("12345"));
        assert_single_order_fields(
            &htx_order,
            (
                OrderSide::Buy,
                PositionSide::Long,
                OrderType::Limit,
                OrderCommandStatus::Accepted,
            ),
            2.0,
            1.0,
            65_000.0,
            Some(64_950.0),
        );
        let seen = htx_transport.seen.lock().unwrap();
        assert_eq!(seen[0].path, "/linear-swap-api/v1/swap_cross_order_info");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["contract_code"], "BTC-USDT");
        assert_eq!(body["order_id"], "htx-order-1");
        assert!(body["client_order_id"].as_str().is_some());
    }

    #[tokio::test]
    async fn non_binance_adapters_should_route_open_order_readbacks() {
        let okx_transport = MockTransport::new(json!({
            "code": "0",
            "data": [{
                "instId": "BTC-USDT-SWAP",
                "ordId": "okx-order-1",
                "clOrdId": "client-1",
                "side": "buy",
                "posSide": "long",
                "ordType": "limit",
                "state": "live",
                "px": "65000",
                "sz": "0.02",
                "accFillSz": "0",
                "uTime": "1700000000000"
            }]
        }));
        let okx_adapter =
            PrivatePerpTradingAdapter::new(OkxPrivatePerpProtocol, okx_transport.clone());
        let okx_orders = okx_adapter
            .get_open_orders(Some(&ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP")))
            .await
            .unwrap();
        assert_eq!(okx_orders.len(), 1);
        assert_eq!(
            okx_orders[0].exchange_order_id.as_deref(),
            Some("okx-order-1")
        );
        assert_open_order_fields(&okx_orders[0], "client-1", 0.02, 65_000.0);
        let seen = okx_transport.seen.lock().unwrap();
        assert_eq!(seen[0].path, "/api/v5/trade/orders-pending");
        assert_eq!(
            seen[0].query.get("instId").map(String::as_str),
            Some("BTC-USDT-SWAP")
        );
        drop(seen);

        let bitget_transport = MockTransport::new(json!({
            "code": "00000",
            "data": {
                "entrustedList": [{
                    "symbol": "BTCUSDT",
                    "orderId": "bitget-order-1",
                    "clientOid": "client-1",
                    "side": "buy",
                    "tradeSide": "open",
                    "orderType": "limit",
                    "status": "live",
                    "price": "65000",
                    "size": "0.02",
                    "accBaseVolume": "0",
                    "uTime": "1700000000000"
                }]
            }
        }));
        let bitget_adapter =
            PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, bitget_transport.clone());
        let bitget_orders = bitget_adapter
            .get_open_orders(Some(&ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT")))
            .await
            .unwrap();
        assert_eq!(bitget_orders.len(), 1);
        assert_eq!(
            bitget_orders[0].exchange_order_id.as_deref(),
            Some("bitget-order-1")
        );
        assert_open_order_fields(&bitget_orders[0], "client-1", 0.02, 65_000.0);
        let seen = bitget_transport.seen.lock().unwrap();
        assert_eq!(seen[0].path, "/api/v2/mix/order/orders-pending");
        assert_eq!(
            seen[0].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
        drop(seen);

        let gate_transport = MockTransport::new(json!([{
            "id": "gate-order-1",
            "contract": "BTC_USDT",
            "text": "t-client-1",
            "size": "25",
            "left": "25",
            "price": "65000",
            "status": "open",
            "create_time_ms": "1700000000000"
        }]));
        let gate_adapter =
            PrivatePerpTradingAdapter::new(GatePrivatePerpProtocol, gate_transport.clone())
                .with_instruments([gate_contract_instrument(0.0001)]);
        let gate_orders = gate_adapter
            .get_open_orders(Some(&ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT")))
            .await
            .unwrap();
        assert_eq!(gate_orders.len(), 1);
        assert_eq!(
            gate_orders[0].exchange_order_id.as_deref(),
            Some("gate-order-1")
        );
        assert_open_order_fields(&gate_orders[0], "client-1", 0.0025, 65_000.0);
        let seen = gate_transport.seen.lock().unwrap();
        assert_eq!(seen[0].path, "/futures/usdt/orders");
        assert_eq!(
            seen[0].query.get("status").map(String::as_str),
            Some("open")
        );
        drop(seen);

        let bybit_transport = MockTransport::new(json!({
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": [{
                    "symbol": "BTCUSDT",
                    "orderId": "bybit-order-1",
                    "orderLinkId": "client-1",
                    "side": "Buy",
                    "positionIdx": 1,
                    "orderType": "Limit",
                    "orderStatus": "New",
                    "price": "65000",
                    "qty": "0.02",
                    "cumExecQty": "0",
                    "updatedTime": "1700000000000"
                }]
            }
        }));
        let bybit_adapter =
            PrivatePerpTradingAdapter::new(BybitPrivatePerpProtocol, bybit_transport.clone());
        let bybit_orders = bybit_adapter
            .get_open_orders(Some(&ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT")))
            .await
            .unwrap();
        assert_eq!(bybit_orders.len(), 1);
        assert_eq!(
            bybit_orders[0].exchange_order_id.as_deref(),
            Some("bybit-order-1")
        );
        assert_open_order_fields(&bybit_orders[0], "client-1", 0.02, 65_000.0);
        let seen = bybit_transport.seen.lock().unwrap();
        assert_eq!(seen[0].path, "/v5/order/realtime");
        assert_eq!(seen[0].query.get("openOnly").map(String::as_str), Some("0"));
        drop(seen);

        let mexc_transport = MockTransport::new(json!({
            "success": true,
            "code": 0,
            "data": [{
                "symbol": "BTC_USDT",
                "orderId": "mexc-order-1",
                "externalOid": "client-1",
                "side": 1,
                "type": 1,
                "state": 2,
                "price": "65000",
                "vol": "0.02",
                "dealVol": "0",
                "updateTime": "1700000000000"
            }]
        }));
        let mexc_adapter =
            PrivatePerpTradingAdapter::new(MexcPrivatePerpProtocol, mexc_transport.clone());
        let mexc_orders = mexc_adapter
            .get_open_orders(Some(&ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT")))
            .await
            .unwrap();
        assert_eq!(mexc_orders.len(), 1);
        assert_eq!(
            mexc_orders[0].exchange_order_id.as_deref(),
            Some("mexc-order-1")
        );
        assert_open_order_fields(&mexc_orders[0], "client-1", 0.02, 65_000.0);
        let seen = mexc_transport.seen.lock().unwrap();
        assert_eq!(
            seen[0].path,
            "/api/v1/private/order/list/open_orders/BTC_USDT"
        );
        drop(seen);

        let htx_transport = MockTransport::new(json!({
            "status": "ok",
            "data": {
                "orders": [{
                    "contract_code": "BTC-USDT",
                    "order_id_str": "htx-order-1",
                    "client_order_id": "12345",
                    "direction": "buy",
                    "offset": "open",
                    "order_price_type": "limit",
                    "status": 3,
                    "price": "65000",
                    "volume": "2",
                    "trade_volume": "0",
                    "update_time": "1700000000000"
                }]
            }
        }));
        let htx_adapter =
            PrivatePerpTradingAdapter::new(HtxPrivatePerpProtocol, htx_transport.clone());
        let htx_orders = htx_adapter
            .get_open_orders(Some(&ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT")))
            .await
            .unwrap();
        assert_eq!(htx_orders.len(), 1);
        assert_eq!(
            htx_orders[0].exchange_order_id.as_deref(),
            Some("htx-order-1")
        );
        assert_open_order_fields(&htx_orders[0], "12345", 2.0, 65_000.0);
        let seen = htx_transport.seen.lock().unwrap();
        assert_eq!(seen[0].path, "/linear-swap-api/v1/swap_cross_openorders");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["contract_code"], "BTC-USDT");
        assert_eq!(body["page_size"], 50);
    }

    #[tokio::test]
    async fn private_perp_trading_adapter_should_reject_get_order_scope_before_request() {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "data": {"orderId": "exchange-1"}
        }));
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport.clone());

        let err = adapter
            .get_order(OrderQuery {
                exchange: ExchangeId::Gate,
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: None,
            })
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("bitget query order requires bitget exchange symbols"));
        assert!(transport.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn private_perp_trading_adapter_should_reject_open_orders_scope_before_request() {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "data": []
        }));
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport.clone());

        let err = adapter
            .get_open_orders(Some(&ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT")))
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("bitget open orders requires bitget exchange symbols"));
        assert!(transport.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn private_perp_trading_adapter_should_reject_positions_scope_before_request() {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "data": []
        }));
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport.clone());

        let err = adapter
            .get_positions(Some(&ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT")))
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("bitget positions requires bitget exchange symbols"));
        assert!(transport.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn private_perp_trading_adapter_should_reject_fills_scope_before_request() {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "data": []
        }));
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport.clone());

        let err = adapter
            .get_fills(FillQuery::for_symbol(
                ExchangeId::Gate,
                CanonicalSymbol::new("BTC", "USDT"),
                ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
            ))
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("bitget fills requires bitget exchange symbols"));
        assert!(transport.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn private_perp_trading_adapter_should_reject_trade_fee_scope_before_request() {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "data": {}
        }));
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport.clone());

        let err = adapter
            .get_trade_fee(&ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"))
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("bitget trade fee requires bitget exchange symbols"));
        assert!(transport.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn private_perp_trading_adapter_should_reject_symbol_account_config_scope_before_request()
    {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "data": {}
        }));
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport.clone());

        let err = adapter
            .get_symbol_account_config(&ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"))
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("bitget symbol account config requires bitget exchange symbols"));
        assert!(transport.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn private_perp_trading_adapter_should_reject_order_history_scope_before_request() {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "data": []
        }));
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport.clone());

        let err = adapter
            .get_order_history(OrderHistoryQuery::for_symbol(
                ExchangeId::Gate,
                ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
            ))
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("bitget all-orders history requires bitget exchange symbols"));
        assert!(transport.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn private_perp_trading_adapter_should_reject_order_amendment_history_scope_before_request(
    ) {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "data": []
        }));
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport.clone());

        let err = adapter
            .get_order_amendment_history(OrderAmendmentHistoryQuery::for_order(
                ExchangeId::Gate,
                ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
                Some("client-1".to_string()),
                None,
            ))
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("bitget order-amendment history requires bitget exchange symbols"));
        assert!(transport.seen.lock().unwrap().is_empty());
    }

    async fn assert_order_amendment_history_unsupported_before_rest<P>(
        protocol: P,
        exchange: ExchangeId,
        symbol: &str,
    ) where
        P: PrivatePerpProtocol + Send + Sync + Clone,
    {
        let transport = MockTransport::new(json!({"data": []}));
        let adapter = PrivatePerpTradingAdapter::new(protocol, transport.clone());

        let err = adapter
            .get_order_amendment_history(OrderAmendmentHistoryQuery::for_order(
                exchange.clone(),
                ExchangeSymbol::new(exchange, symbol),
                Some("client-1".to_string()),
                None,
            ))
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("order-amendment history is not implemented"));
        assert!(transport.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn non_binance_adapters_should_reject_order_amendment_history_before_rest() {
        assert_order_amendment_history_unsupported_before_rest(
            BitgetPrivatePerpProtocol,
            ExchangeId::Bitget,
            "BTCUSDT",
        )
        .await;
        assert_order_amendment_history_unsupported_before_rest(
            GatePrivatePerpProtocol,
            ExchangeId::Gate,
            "BTC_USDT",
        )
        .await;
        assert_order_amendment_history_unsupported_before_rest(
            BybitPrivatePerpProtocol,
            ExchangeId::Bybit,
            "BTCUSDT",
        )
        .await;
        assert_order_amendment_history_unsupported_before_rest(
            MexcPrivatePerpProtocol,
            ExchangeId::Mexc,
            "BTC_USDT",
        )
        .await;
        assert_order_amendment_history_unsupported_before_rest(
            HtxPrivatePerpProtocol,
            ExchangeId::Htx,
            "BTC-USDT",
        )
        .await;
    }

    #[tokio::test]
    async fn private_perp_trading_adapter_should_reject_wrong_exchange_cancel_all_before_composed_readback(
    ) {
        let transport = MockTransport::new(json!({
            "code": "0",
            "data": []
        }));
        let adapter = PrivatePerpTradingAdapter::new(OkxPrivatePerpProtocol, transport.clone());

        let err = adapter
            .cancel_all_orders(CancelAllCommand::for_symbol(
                ExchangeId::Gate,
                CanonicalSymbol::new("BTC", "USDT"),
                ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
                Utc::now(),
            ))
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("okx cancel-all requires okx exchange symbols"));
        assert!(transport.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn private_perp_trading_adapter_should_reject_cancel_all_symbol_scope_before_request() {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "data": {}
        }));
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport.clone());

        let err = adapter
            .cancel_all_orders(CancelAllCommand::for_symbol(
                ExchangeId::Bitget,
                CanonicalSymbol::new("BTC", "USDT"),
                ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
                Utc::now(),
            ))
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("bitget cancel-all requires bitget exchange symbols"));
        assert!(transport.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn native_cancel_all_adapters_should_route_requests() {
        let binance_transport = MockTransport::new(json!({
            "msg": "success",
            "count": 2
        }));
        let binance_adapter =
            PrivatePerpTradingAdapter::new(BinancePrivatePerpProtocol, binance_transport.clone());
        let binance_ack = binance_adapter
            .cancel_all_orders(cancel_all_command(ExchangeId::Binance, "BTCUSDT"))
            .await
            .unwrap();
        assert_cancel_all_ack(
            &binance_ack,
            ExchangeId::Binance,
            Some("BTCUSDT"),
            2,
            Some("success"),
        );
        let binance_seen = binance_transport.seen.lock().unwrap();
        assert_eq!(binance_seen.len(), 1);
        assert_eq!(binance_seen[0].method, PrivateRestMethod::Delete);
        assert_eq!(binance_seen[0].path, "/fapi/v1/allOpenOrders");
        assert_eq!(
            binance_seen[0].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
        drop(binance_seen);

        let bitget_transport = MockTransport::new(json!({
            "msg": "success",
            "data": {"successList": [{}, {}]}
        }));
        let bitget_adapter =
            PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, bitget_transport.clone());
        let bitget_ack = bitget_adapter
            .cancel_all_orders(cancel_all_command(ExchangeId::Bitget, "BTCUSDT"))
            .await
            .unwrap();
        assert_cancel_all_ack(
            &bitget_ack,
            ExchangeId::Bitget,
            Some("BTCUSDT"),
            2,
            Some("success"),
        );
        let bitget_seen = bitget_transport.seen.lock().unwrap();
        assert_eq!(bitget_seen.len(), 1);
        assert_eq!(bitget_seen[0].method, PrivateRestMethod::Post);
        assert_eq!(bitget_seen[0].path, "/api/v2/mix/order/cancel-all-orders");
        let bitget_body = bitget_seen[0].body.as_ref().unwrap();
        assert_eq!(bitget_body["productType"], "USDT-FUTURES");
        assert_eq!(bitget_body["marginCoin"], "USDT");
        assert_eq!(bitget_body["symbol"], "BTCUSDT");
        drop(bitget_seen);

        let gate_transport = MockTransport::new(json!({
            "message": "success",
            "data": [{}, {}]
        }));
        let gate_adapter =
            PrivatePerpTradingAdapter::new(GatePrivatePerpProtocol, gate_transport.clone());
        let gate_ack = gate_adapter
            .cancel_all_orders(cancel_all_command(ExchangeId::Gate, "BTC_USDT"))
            .await
            .unwrap();
        assert_cancel_all_ack(
            &gate_ack,
            ExchangeId::Gate,
            Some("BTC_USDT"),
            2,
            Some("success"),
        );
        let gate_seen = gate_transport.seen.lock().unwrap();
        assert_eq!(gate_seen.len(), 1);
        assert_eq!(gate_seen[0].method, PrivateRestMethod::Delete);
        assert_eq!(gate_seen[0].path, "/futures/usdt/orders");
        assert_eq!(
            gate_seen[0].query.get("contract").map(String::as_str),
            Some("BTC_USDT")
        );
        drop(gate_seen);

        let bybit_transport = MockTransport::new(json!({
            "retMsg": "OK",
            "message": "OK",
            "result": {"successList": [{}, {}]}
        }));
        let bybit_adapter =
            PrivatePerpTradingAdapter::new(BybitPrivatePerpProtocol, bybit_transport.clone());
        let bybit_ack = bybit_adapter
            .cancel_all_orders(cancel_all_command(ExchangeId::Bybit, "BTCUSDT"))
            .await
            .unwrap();
        assert_cancel_all_ack(
            &bybit_ack,
            ExchangeId::Bybit,
            Some("BTCUSDT"),
            2,
            Some("OK"),
        );
        let bybit_seen = bybit_transport.seen.lock().unwrap();
        assert_eq!(bybit_seen.len(), 1);
        assert_eq!(bybit_seen[0].method, PrivateRestMethod::Post);
        assert_eq!(bybit_seen[0].path, "/v5/order/cancel-all");
        let bybit_body = bybit_seen[0].body.as_ref().unwrap();
        assert_eq!(bybit_body["category"], "linear");
        assert_eq!(bybit_body["symbol"], "BTCUSDT");
        drop(bybit_seen);

        let mexc_transport = MockTransport::new(json!({
            "message": "success",
            "data": {"count": 2}
        }));
        let mexc_adapter =
            PrivatePerpTradingAdapter::new(MexcPrivatePerpProtocol, mexc_transport.clone());
        let mexc_ack = mexc_adapter
            .cancel_all_orders(cancel_all_command(ExchangeId::Mexc, "BTC_USDT"))
            .await
            .unwrap();
        assert_cancel_all_ack(
            &mexc_ack,
            ExchangeId::Mexc,
            Some("BTC_USDT"),
            2,
            Some("success"),
        );
        let mexc_seen = mexc_transport.seen.lock().unwrap();
        assert_eq!(mexc_seen.len(), 1);
        assert_eq!(mexc_seen[0].method, PrivateRestMethod::Post);
        assert_eq!(mexc_seen[0].path, "/api/v1/private/order/cancel_all");
        assert_eq!(
            mexc_seen[0].query.get("symbol").map(String::as_str),
            Some("BTC_USDT")
        );
        drop(mexc_seen);

        let htx_transport = MockTransport::new(json!({
            "message": "success",
            "data": {"success": [{}, {}]}
        }));
        let htx_adapter =
            PrivatePerpTradingAdapter::new(HtxPrivatePerpProtocol, htx_transport.clone());
        let htx_ack = htx_adapter
            .cancel_all_orders(cancel_all_command(ExchangeId::Htx, "BTC-USDT"))
            .await
            .unwrap();
        assert_cancel_all_ack(
            &htx_ack,
            ExchangeId::Htx,
            Some("BTC-USDT"),
            2,
            Some("success"),
        );
        let htx_seen = htx_transport.seen.lock().unwrap();
        assert_eq!(htx_seen.len(), 1);
        assert_eq!(htx_seen[0].method, PrivateRestMethod::Post);
        assert_eq!(htx_seen[0].path, "/linear-swap-api/v1/swap_cross_cancelall");
        assert_eq!(
            htx_seen[0].body.as_ref().unwrap()["contract_code"],
            "BTC-USDT"
        );
    }

    #[tokio::test]
    async fn private_perp_trading_adapter_should_reject_invalid_close_quantity() {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "data": {"orderId": "exchange-1"}
        }));
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport.clone());
        let mut cmd = ClosePositionCommand::market(
            ExchangeId::Bitget,
            CanonicalSymbol::new("BTC", "USDT"),
            ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
            PositionSide::Long,
            0.0,
            "close-long-1",
            Utc::now(),
        );

        let err = adapter.close_position(cmd.clone()).await.unwrap_err();
        assert!(err
            .to_string()
            .contains("bitget close quantity must be positive and finite"));

        cmd.quantity = 0.01;
        cmd.price = Some(f64::NAN);
        let err = adapter.close_position(cmd.clone()).await.unwrap_err();
        assert!(err
            .to_string()
            .contains("bitget close price must be positive and finite"));

        cmd.quantity = f64::NAN;
        cmd.price = None;
        let err = adapter.close_position(cmd).await.unwrap_err();
        assert!(err
            .to_string()
            .contains("bitget close quantity must be positive and finite"));
        assert!(transport.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn private_perp_trading_adapter_should_reject_close_scope_before_quantity() {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "data": {"orderId": "exchange-1"}
        }));
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport.clone());
        let cmd = ClosePositionCommand::market(
            ExchangeId::Gate,
            CanonicalSymbol::new("BTC", "USDT"),
            ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
            PositionSide::Long,
            0.0,
            "close-long-1",
            Utc::now(),
        );

        let err = adapter.close_position(cmd).await.unwrap_err();

        assert!(err
            .to_string()
            .contains("bitget close position requires bitget exchange symbols"));
        assert!(transport.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn okx_adapter_should_route_close_position_as_reduce_only_order() {
        let transport = MockTransport::new(json!({
            "code": "0",
            "msg": "",
            "data": [
                {
                    "ordId": "okx-close-1",
                    "clOrdId": "close-long-1",
                    "sCode": "0",
                    "sMsg": ""
                }
            ]
        }));
        let adapter = PrivatePerpTradingAdapter::new(OkxPrivatePerpProtocol, transport.clone())
            .with_position_mode(PositionMode::Hedge);

        let ack = adapter
            .close_position(ClosePositionCommand::market(
                ExchangeId::Okx,
                CanonicalSymbol::new("BTC", "USDT"),
                ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
                PositionSide::Long,
                0.02,
                "close-long-1",
                Utc::now(),
            ))
            .await
            .unwrap();

        assert_close_ack(
            &ack,
            ExchangeId::Okx,
            "close-long-1",
            "okx-close-1",
            Some(""),
        );
        let seen = transport.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].path, "/api/v5/trade/order");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["instId"], "BTC-USDT-SWAP");
        assert_eq!(body["tdMode"], "cross");
        assert_eq!(body["side"], "sell");
        assert_eq!(body["posSide"], "long");
        assert_eq!(body["ordType"], "market");
        assert_eq!(body["sz"], "0.02");
        assert_eq!(body["clOrdId"], "close-long-1");
        assert_eq!(body["reduceOnly"], true);
    }

    #[tokio::test]
    async fn binance_adapter_should_route_countdown_cancel_all() {
        let transport = MockTransport::new(json!({
            "msg": "success"
        }));
        let adapter = PrivatePerpTradingAdapter::new(BinancePrivatePerpProtocol, transport.clone());

        let ack = adapter
            .set_countdown_cancel_all(CountdownCancelAllCommand::set_for_symbol(
                ExchangeId::Binance,
                ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT"),
                30,
                Utc::now(),
            ))
            .await
            .unwrap();

        assert_countdown_ack(
            &ack,
            ExchangeId::Binance,
            Some("BTCUSDT"),
            30,
            None,
            Some("success"),
        );
        let seen = transport.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].method, PrivateRestMethod::Post);
        assert_eq!(seen[0].path, "/fapi/v1/countdownCancelAll");
        assert_eq!(
            seen[0].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
        assert_eq!(
            seen[0].query.get("countdownTime").map(String::as_str),
            Some("30000")
        );
    }

    #[tokio::test]
    async fn okx_adapter_should_route_countdown_cancel_all_after() {
        let transport = MockTransport::new(json!({
            "code": "0",
            "msg": "success",
            "data": []
        }));
        let adapter = PrivatePerpTradingAdapter::new(OkxPrivatePerpProtocol, transport.clone());

        let ack = adapter
            .set_countdown_cancel_all(CountdownCancelAllCommand::set_for_symbol(
                ExchangeId::Okx,
                ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
                10,
                Utc::now(),
            ))
            .await
            .unwrap();

        assert_countdown_ack(
            &ack,
            ExchangeId::Okx,
            Some("BTC-USDT-SWAP"),
            10,
            None,
            Some("success"),
        );
        let seen = transport.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].path, "/api/v5/trade/cancel-all-after");
        assert_eq!(seen[0].body.as_ref().unwrap()["timeOut"], "10");
    }

    #[tokio::test]
    async fn bitget_adapter_should_route_close_position_as_hedge_close_order() {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "msg": "success",
            "data": {
                "orderId": "exchange-1",
                "clientOid": "close-long-1"
            }
        }));
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport.clone())
            .with_position_mode(PositionMode::Hedge);

        let ack = adapter
            .close_position(ClosePositionCommand::market(
                ExchangeId::Bitget,
                CanonicalSymbol::new("BTC", "USDT"),
                ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
                PositionSide::Long,
                0.02,
                "close-long-1",
                Utc::now(),
            ))
            .await
            .unwrap();

        assert_close_ack(
            &ack,
            ExchangeId::Bitget,
            "close-long-1",
            "exchange-1",
            Some("success"),
        );
        let seen = transport.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].path, "/api/v2/mix/order/place-order");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["productType"], "USDT-FUTURES");
        assert_eq!(body["symbol"], "BTCUSDT");
        assert_eq!(body["side"], "buy");
        assert_eq!(body["tradeSide"], "close");
        assert_eq!(body["orderType"], "market");
        assert_eq!(body["price"], "0");
        assert_eq!(body["size"], "0.02");
        assert_eq!(body["clientOid"], "close-long-1");
    }

    #[tokio::test]
    async fn gate_adapter_should_route_close_position_as_reduce_only_order() {
        let transport = MockTransport::new(json!({
            "id": "gate-close-1",
            "text": "t-close-short-1"
        }));
        let adapter = PrivatePerpTradingAdapter::new(GatePrivatePerpProtocol, transport.clone());

        let ack = adapter
            .close_position(ClosePositionCommand::market(
                ExchangeId::Gate,
                CanonicalSymbol::new("BTC", "USDT"),
                ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
                PositionSide::Short,
                3.0,
                "close-short-1",
                Utc::now(),
            ))
            .await
            .unwrap();

        assert_close_ack(
            &ack,
            ExchangeId::Gate,
            "close-short-1",
            "gate-close-1",
            None,
        );
        let seen = transport.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].path, "/futures/usdt/orders");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["contract"], "BTC_USDT");
        assert_eq!(body["size"], 3);
        assert_eq!(body["tif"], "ioc");
        assert_eq!(body["price"], "0");
        assert_eq!(body["text"], "t-close-short-1");
        assert_eq!(body["reduce_only"], true);
    }

    #[tokio::test]
    async fn bybit_adapter_should_route_close_position_as_reduce_only_order() {
        let transport = MockTransport::new(json!({
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "orderId": "bybit-close-1",
                "orderLinkId": "close-short-1"
            }
        }));
        let adapter = PrivatePerpTradingAdapter::new(BybitPrivatePerpProtocol, transport.clone())
            .with_position_mode(PositionMode::Hedge);

        let ack = adapter
            .close_position(ClosePositionCommand::market(
                ExchangeId::Bybit,
                CanonicalSymbol::new("BTC", "USDT"),
                ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT"),
                PositionSide::Short,
                0.03,
                "close-short-1",
                Utc::now(),
            ))
            .await
            .unwrap();

        assert_close_ack(
            &ack,
            ExchangeId::Bybit,
            "close-short-1",
            "bybit-close-1",
            None,
        );
        let seen = transport.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].path, "/v5/order/create");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["category"], "linear");
        assert_eq!(body["symbol"], "BTCUSDT");
        assert_eq!(body["side"], "Buy");
        assert_eq!(body["orderType"], "Market");
        assert_eq!(body["qty"], "0.03");
        assert_eq!(body["orderLinkId"], "close-short-1");
        assert_eq!(body["reduceOnly"], true);
        assert_eq!(body["positionIdx"], 2);
    }

    #[tokio::test]
    async fn bybit_adapter_should_route_disconnected_countdown_cancel_all() {
        let transport = MockTransport::new(json!({
            "retCode": 0,
            "retMsg": "OK",
            "result": {}
        }));
        let adapter = PrivatePerpTradingAdapter::new(BybitPrivatePerpProtocol, transport.clone());

        let ack = adapter
            .set_countdown_cancel_all(CountdownCancelAllCommand::set_for_symbol(
                ExchangeId::Bybit,
                ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT"),
                30,
                Utc::now(),
            ))
            .await
            .unwrap();

        assert_countdown_ack(
            &ack,
            ExchangeId::Bybit,
            Some("BTCUSDT"),
            30,
            None,
            Some("OK"),
        );
        let seen = transport.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].path, "/v5/order/disconnected-cancel-all");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["symbol"], "BTCUSDT");
        assert_eq!(body["timeWindow"], 30);
    }

    #[tokio::test]
    async fn mexc_adapter_should_route_close_position_as_reduce_only_order() {
        let transport = MockTransport::new(json!({
            "success": true,
            "code": 0,
            "data": {"orderId": "exchange-1", "externalOid": "close-short-1"}
        }));
        let adapter = PrivatePerpTradingAdapter::new(MexcPrivatePerpProtocol, transport.clone())
            .with_position_mode(PositionMode::Hedge);

        let ack = adapter
            .close_position(ClosePositionCommand::market(
                ExchangeId::Mexc,
                CanonicalSymbol::new("BTC", "USDT"),
                ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
                PositionSide::Short,
                2.0,
                "close-short-1",
                Utc::now(),
            ))
            .await
            .unwrap();

        assert_close_ack(&ack, ExchangeId::Mexc, "close-short-1", "exchange-1", None);
        let seen = transport.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].path, "/api/v1/private/order/submit");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["symbol"], "BTC_USDT");
        assert_eq!(body["side"], 2);
        assert_eq!(body["type"], 5);
        assert_eq!(body["vol"], "2");
        assert_eq!(body["positionMode"], "hedge_mode");
        assert_eq!(body["externalOid"], "close-short-1");
    }

    #[tokio::test]
    async fn htx_adapter_should_route_market_close_to_lightning_close() {
        let transport = MockTransport::new(json!({
            "status": "ok",
            "data": {"order_id": 918800256249405440_i64, "order_id_str": "918800256249405440"}
        }));
        let adapter = PrivatePerpTradingAdapter::new(HtxPrivatePerpProtocol, transport.clone())
            .with_position_mode(PositionMode::Hedge);

        let ack = adapter
            .close_position(ClosePositionCommand::market(
                ExchangeId::Htx,
                CanonicalSymbol::new("BTC", "USDT"),
                ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"),
                PositionSide::Long,
                3.0,
                "close-long-1",
                Utc::now(),
            ))
            .await
            .unwrap();

        assert_close_ack(
            &ack,
            ExchangeId::Htx,
            "close-long-1",
            "918800256249405440",
            None,
        );
        let seen = transport.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(
            seen[0].path,
            "/linear-swap-api/v1/swap_cross_lightning_close_position"
        );
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["contract_code"], "BTC-USDT");
        assert_eq!(body["volume"], "3");
        assert_eq!(body["direction"], "sell");
        assert_eq!(body["order_price_type"], "lightning_ioc");
        assert!(body["client_order_id"].as_i64().is_some());
    }

    #[tokio::test]
    async fn mexc_and_htx_adapters_should_reject_close_scope_before_request() {
        let mexc_transport = MockTransport::new(json!({
            "success": true,
            "code": 0,
            "data": {"orderId": "exchange-1"}
        }));
        let mexc_adapter =
            PrivatePerpTradingAdapter::new(MexcPrivatePerpProtocol, mexc_transport.clone());
        let mexc_err = mexc_adapter
            .close_position(ClosePositionCommand::market(
                ExchangeId::Mexc,
                CanonicalSymbol::new("BTC", "USDT"),
                ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"),
                PositionSide::Long,
                0.01,
                "close-long-1",
                Utc::now(),
            ))
            .await
            .unwrap_err();
        assert!(mexc_err
            .to_string()
            .contains("mexc close position requires mexc exchange symbols"));
        assert!(mexc_transport.seen.lock().unwrap().is_empty());

        let htx_transport = MockTransport::new(json!({
            "status": "ok",
            "data": {"order_id": "exchange-1"}
        }));
        let htx_adapter =
            PrivatePerpTradingAdapter::new(HtxPrivatePerpProtocol, htx_transport.clone());
        let htx_err = htx_adapter
            .close_position(ClosePositionCommand::market(
                ExchangeId::Mexc,
                CanonicalSymbol::new("BTC", "USDT"),
                ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
                PositionSide::Long,
                0.01,
                "close-long-1",
                Utc::now(),
            ))
            .await
            .unwrap_err();
        assert!(htx_err
            .to_string()
            .contains("htx close position requires htx exchange symbols"));
        assert!(htx_transport.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn private_perp_trading_adapter_should_reject_invalid_amend_quantity_and_price() {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "data": {"orderId": "exchange-1"}
        }));
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport.clone());
        let mut cmd = AmendOrderCommand {
            exchange: ExchangeId::Bitget,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
            exchange_order_id: Some("order-1".to_string()),
            client_order_id: Some("client-1".to_string()),
            new_client_order_id: None,
            new_quantity: Some(0.0),
            new_price: Some(65_000.0),
            original_side: Some(OrderSide::Buy),
            requested_at: Utc::now(),
        };

        let err = adapter.amend_order(cmd.clone()).await.unwrap_err();
        assert!(err
            .to_string()
            .contains("bitget amend quantity must be positive and finite"));

        cmd.new_quantity = Some(0.01);
        cmd.new_price = Some(f64::NAN);
        let err = adapter.amend_order(cmd).await.unwrap_err();
        assert!(err
            .to_string()
            .contains("bitget amend price must be positive and finite"));
        assert!(transport.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn private_perp_trading_adapter_should_reject_amend_scope_before_quantity() {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "data": {"orderId": "exchange-1"}
        }));
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport.clone());

        let err = adapter
            .amend_order(AmendOrderCommand {
                exchange: ExchangeId::Gate,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
                exchange_order_id: Some("order-1".to_string()),
                client_order_id: Some("client-1".to_string()),
                new_client_order_id: None,
                new_quantity: Some(0.0),
                new_price: Some(65_000.0),
                original_side: Some(OrderSide::Buy),
                requested_at: Utc::now(),
            })
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("bitget amend requires bitget exchange symbols"));
        assert!(transport.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn binance_adapter_should_route_amend_order() {
        let transport = MockTransport::new(json!({
            "orderId": 12345,
            "clientOrderId": "client-1"
        }));
        let adapter = PrivatePerpTradingAdapter::new(BinancePrivatePerpProtocol, transport.clone());

        let ack = adapter
            .amend_order(AmendOrderCommand {
                exchange: ExchangeId::Binance,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT"),
                exchange_order_id: None,
                client_order_id: Some("client-1".to_string()),
                new_client_order_id: None,
                new_quantity: Some(0.02),
                new_price: Some(64_900.0),
                original_side: Some(OrderSide::Buy),
                requested_at: Utc::now(),
            })
            .await
            .unwrap();

        assert_amend_ack(
            &ack,
            ExchangeId::Binance,
            Some("client-1"),
            Some("12345"),
            None,
        );
        let seen = transport.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].method, PrivateRestMethod::Put);
        assert_eq!(seen[0].path, "/fapi/v1/order");
        assert_eq!(
            seen[0].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
        assert_eq!(
            seen[0].query.get("origClientOrderId").map(String::as_str),
            Some("client-1")
        );
        assert_eq!(seen[0].query.get("side").map(String::as_str), Some("BUY"));
        assert_eq!(
            seen[0].query.get("quantity").map(String::as_str),
            Some("0.02")
        );
        assert_eq!(
            seen[0].query.get("price").map(String::as_str),
            Some("64900")
        );
    }

    #[tokio::test]
    async fn okx_adapter_should_route_amend_order() {
        let transport = MockTransport::new(json!({
            "code": "0",
            "msg": "success",
            "data": [
                {
                    "ordId": "okx-order-1",
                    "clOrdId": "client-1",
                    "sCode": "0",
                    "sMsg": ""
                }
            ]
        }));
        let adapter = PrivatePerpTradingAdapter::new(OkxPrivatePerpProtocol, transport.clone());

        let ack = adapter
            .amend_order(AmendOrderCommand {
                exchange: ExchangeId::Okx,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
                exchange_order_id: None,
                client_order_id: Some("client-1".to_string()),
                new_client_order_id: None,
                new_quantity: Some(0.02),
                new_price: Some(64_900.0),
                original_side: Some(OrderSide::Buy),
                requested_at: Utc::now(),
            })
            .await
            .unwrap();

        assert_amend_ack(
            &ack,
            ExchangeId::Okx,
            Some("client-1"),
            Some("okx-order-1"),
            Some("success"),
        );
        let seen = transport.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].path, "/api/v5/trade/amend-order");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["instId"], "BTC-USDT-SWAP");
        assert_eq!(body["clOrdId"], "client-1");
        assert_eq!(body["newSz"], "0.02");
        assert_eq!(body["newPx"], "64900");
    }

    #[tokio::test]
    async fn bitget_adapter_should_route_amend_order() {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "msg": "success",
            "data": {
                "orderId": "bitget-order-1",
                "clientOid": "client-2"
            }
        }));
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport.clone());

        let ack = adapter
            .amend_order(AmendOrderCommand {
                exchange: ExchangeId::Bitget,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
                exchange_order_id: Some("bitget-order-1".to_string()),
                client_order_id: Some("client-1".to_string()),
                new_client_order_id: Some("client-2".to_string()),
                new_quantity: Some(0.02),
                new_price: Some(64_900.0),
                original_side: Some(OrderSide::Buy),
                requested_at: Utc::now(),
            })
            .await
            .unwrap();

        assert_amend_ack(
            &ack,
            ExchangeId::Bitget,
            Some("client-2"),
            Some("bitget-order-1"),
            Some("success"),
        );
        let seen = transport.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].path, "/api/v2/mix/order/modify-order");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["productType"], "USDT-FUTURES");
        assert_eq!(body["symbol"], "BTCUSDT");
        assert_eq!(body["orderId"], "bitget-order-1");
        assert_eq!(body["clientOid"], "client-1");
        assert_eq!(body["newClientOid"], "client-2");
        assert_eq!(body["newSize"], "0.02");
        assert_eq!(body["newPrice"], "64900");
    }

    #[tokio::test]
    async fn bybit_adapter_should_route_amend_order() {
        let transport = MockTransport::new(json!({
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "orderId": "bybit-order-1",
                "orderLinkId": "client-1"
            }
        }));
        let adapter = PrivatePerpTradingAdapter::new(BybitPrivatePerpProtocol, transport.clone());

        let ack = adapter
            .amend_order(AmendOrderCommand {
                exchange: ExchangeId::Bybit,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT"),
                exchange_order_id: None,
                client_order_id: Some("client-1".to_string()),
                new_client_order_id: None,
                new_quantity: Some(0.02),
                new_price: Some(64_900.0),
                original_side: Some(OrderSide::Buy),
                requested_at: Utc::now(),
            })
            .await
            .unwrap();

        assert_amend_ack(
            &ack,
            ExchangeId::Bybit,
            Some("client-1"),
            Some("bybit-order-1"),
            None,
        );
        let seen = transport.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].path, "/v5/order/amend");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["category"], "linear");
        assert_eq!(body["symbol"], "BTCUSDT");
        assert_eq!(body["orderLinkId"], "client-1");
        assert_eq!(body["qty"], "0.02");
        assert_eq!(body["price"], "64900");
    }

    #[tokio::test]
    async fn mexc_adapter_should_route_amend_order() {
        let transport = MockTransport::new(json!({
            "success": true,
            "code": 0,
            "message": "success",
            "data": {"orderId": "mexc-order-1"}
        }));
        let adapter = PrivatePerpTradingAdapter::new(MexcPrivatePerpProtocol, transport.clone());

        let ack = adapter
            .amend_order(AmendOrderCommand {
                exchange: ExchangeId::Mexc,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
                exchange_order_id: Some("mexc-order-1".to_string()),
                client_order_id: None,
                new_client_order_id: None,
                new_quantity: Some(0.02),
                new_price: Some(64_900.0),
                original_side: Some(OrderSide::Buy),
                requested_at: Utc::now(),
            })
            .await
            .unwrap();

        assert_amend_ack(
            &ack,
            ExchangeId::Mexc,
            None,
            Some("mexc-order-1"),
            Some("success"),
        );
        let seen = transport.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].path, "/api/v1/private/order/change_order_price");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["orderId"], "mexc-order-1");
        assert_eq!(body["price"], "64900");
        assert_eq!(body["vol"], "0.02");
    }

    #[tokio::test]
    async fn gate_adapter_should_route_amend_order() {
        let transport = MockTransport::new(json!({
            "id": "gate-order-1",
            "text": "t-client-2"
        }));
        let adapter = PrivatePerpTradingAdapter::new(GatePrivatePerpProtocol, transport.clone());

        let ack = adapter
            .amend_order(AmendOrderCommand {
                exchange: ExchangeId::Gate,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
                exchange_order_id: Some("gate-order-1".to_string()),
                client_order_id: None,
                new_client_order_id: Some("client-2".to_string()),
                new_quantity: None,
                new_price: Some(64_900.0),
                original_side: None,
                requested_at: Utc::now(),
            })
            .await
            .unwrap();

        assert_amend_ack(
            &ack,
            ExchangeId::Gate,
            Some("client-2"),
            Some("gate-order-1"),
            None,
        );
        let seen = transport.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].path, "/futures/usdt/orders/gate-order-1");
        assert_eq!(
            seen[0].query.get("contract").map(String::as_str),
            Some("BTC_USDT")
        );
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["price"], "64900");
        assert_eq!(body["amend_text"], "t-client-2");
    }

    #[tokio::test]
    async fn htx_adapter_should_reject_amend_unsupported_before_rest() {
        let transport = MockTransport::new(json!({"status": "ok"}));
        let adapter = PrivatePerpTradingAdapter::new(HtxPrivatePerpProtocol, transport.clone());

        let err = adapter
            .amend_order(AmendOrderCommand {
                exchange: ExchangeId::Htx,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"),
                exchange_order_id: Some("htx-order-1".to_string()),
                client_order_id: None,
                new_client_order_id: None,
                new_quantity: Some(0.02),
                new_price: Some(64_900.0),
                original_side: Some(OrderSide::Buy),
                requested_at: Utc::now(),
            })
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("does not expose a verified amend-order endpoint"));
        assert!(transport.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn private_perp_trading_adapter_should_reject_zero_leverage() {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "data": {"symbol": "BTCUSDT", "leverage": 0}
        }));
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport.clone());

        let err = adapter
            .set_leverage(LeverageCommand {
                exchange: ExchangeId::Bitget,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
                leverage: 0,
                requested_at: Utc::now(),
            })
            .await
            .unwrap_err();

        assert!(err.to_string().contains("bitget leverage must be positive"));
        assert!(transport.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn binance_adapter_should_route_set_leverage() {
        let transport = MockTransport::new(json!({
            "symbol": "BTCUSDT",
            "leverage": 7,
            "maxNotionalValue": "1000000"
        }));
        let adapter = PrivatePerpTradingAdapter::new(BinancePrivatePerpProtocol, transport.clone());

        let ack = adapter
            .set_leverage(LeverageCommand {
                exchange: ExchangeId::Binance,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT"),
                leverage: 7,
                requested_at: Utc::now(),
            })
            .await
            .unwrap();

        assert_leverage_ack(&ack, ExchangeId::Binance, "BTCUSDT", 7, None);
        let seen = transport.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].path, "/fapi/v1/leverage");
        assert_eq!(
            seen[0].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
        assert_eq!(seen[0].query.get("leverage").map(String::as_str), Some("7"));
    }

    #[tokio::test]
    async fn okx_adapter_should_route_set_leverage() {
        let transport = MockTransport::new(json!({
            "code": "0",
            "msg": "success",
            "data": []
        }));
        let adapter = PrivatePerpTradingAdapter::new(OkxPrivatePerpProtocol, transport.clone());

        let ack = adapter
            .set_leverage(LeverageCommand {
                exchange: ExchangeId::Okx,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
                leverage: 5,
                requested_at: Utc::now(),
            })
            .await
            .unwrap();

        assert_leverage_ack(&ack, ExchangeId::Okx, "BTC-USDT-SWAP", 5, Some("success"));
        let seen = transport.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].path, "/api/v5/account/set-leverage");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["instId"], "BTC-USDT-SWAP");
        assert_eq!(body["lever"], "5");
        assert_eq!(body["mgnMode"], "cross");
    }

    #[tokio::test]
    async fn bitget_adapter_should_route_set_leverage() {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "msg": "success",
            "data": {
                "symbol": "BTCUSDT",
                "leverage": "6"
            }
        }));
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport.clone());

        let ack = adapter
            .set_leverage(LeverageCommand {
                exchange: ExchangeId::Bitget,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
                leverage: 6,
                requested_at: Utc::now(),
            })
            .await
            .unwrap();

        assert_leverage_ack(&ack, ExchangeId::Bitget, "BTCUSDT", 6, Some("success"));
        let seen = transport.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].path, "/api/v2/mix/account/set-leverage");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["productType"], "USDT-FUTURES");
        assert_eq!(body["symbol"], "BTCUSDT");
        assert_eq!(body["marginCoin"], "USDT");
        assert_eq!(body["leverage"], "6");
    }

    #[tokio::test]
    async fn bybit_adapter_should_route_set_leverage() {
        let transport = MockTransport::new(json!({
            "retCode": 0,
            "retMsg": "OK",
            "result": {}
        }));
        let adapter = PrivatePerpTradingAdapter::new(BybitPrivatePerpProtocol, transport.clone());

        let ack = adapter
            .set_leverage(LeverageCommand {
                exchange: ExchangeId::Bybit,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT"),
                leverage: 8,
                requested_at: Utc::now(),
            })
            .await
            .unwrap();

        assert_leverage_ack(&ack, ExchangeId::Bybit, "BTCUSDT", 8, None);
        let seen = transport.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].path, "/v5/position/set-leverage");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["category"], "linear");
        assert_eq!(body["symbol"], "BTCUSDT");
        assert_eq!(body["buyLeverage"], "8");
        assert_eq!(body["sellLeverage"], "8");
    }

    #[tokio::test]
    async fn mexc_adapter_should_route_set_leverage() {
        let transport = MockTransport::new(json!({
            "success": true,
            "code": 0,
            "message": "success"
        }));
        let adapter = PrivatePerpTradingAdapter::new(MexcPrivatePerpProtocol, transport.clone());

        let ack = adapter
            .set_leverage(LeverageCommand {
                exchange: ExchangeId::Mexc,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
                leverage: 7,
                requested_at: Utc::now(),
            })
            .await
            .unwrap();

        assert_leverage_ack(&ack, ExchangeId::Mexc, "BTC_USDT", 7, Some("success"));
        let seen = transport.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].path, "/api/v1/private/position/change_leverage");
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["symbol"], "BTC_USDT");
        assert_eq!(body["leverage"], 7);
        assert_eq!(body["openType"], 2);
    }

    #[tokio::test]
    async fn htx_adapter_should_route_set_leverage() {
        let transport = MockTransport::new(json!({
            "status": "ok",
            "msg": "success"
        }));
        let adapter = PrivatePerpTradingAdapter::new(HtxPrivatePerpProtocol, transport.clone());

        let ack = adapter
            .set_leverage(LeverageCommand {
                exchange: ExchangeId::Htx,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"),
                leverage: 5,
                requested_at: Utc::now(),
            })
            .await
            .unwrap();

        assert_leverage_ack(&ack, ExchangeId::Htx, "BTC-USDT", 5, Some("success"));
        let seen = transport.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(
            seen[0].path,
            "/linear-swap-api/v1/swap_cross_switch_lever_rate"
        );
        let body = seen[0].body.as_ref().unwrap();
        assert_eq!(body["contract_code"], "BTC-USDT");
        assert_eq!(body["lever_rate"], 5);
    }

    #[tokio::test]
    async fn private_perp_trading_adapter_should_reject_leverage_scope_before_value() {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "data": {"symbol": "BTCUSDT", "leverage": 0}
        }));
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport.clone());

        let err = adapter
            .set_leverage(LeverageCommand {
                exchange: ExchangeId::Gate,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
                leverage: 0,
                requested_at: Utc::now(),
            })
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("bitget set leverage requires bitget exchange symbols"));
        assert!(transport.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn mexc_and_htx_adapters_should_reject_leverage_scope_before_request() {
        let mexc_transport = MockTransport::new(json!({
            "success": true,
            "code": 0
        }));
        let mexc_adapter =
            PrivatePerpTradingAdapter::new(MexcPrivatePerpProtocol, mexc_transport.clone());

        let mexc_err = mexc_adapter
            .set_leverage(LeverageCommand {
                exchange: ExchangeId::Mexc,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"),
                leverage: 0,
                requested_at: Utc::now(),
            })
            .await
            .unwrap_err();

        assert!(mexc_err
            .to_string()
            .contains("mexc set leverage requires mexc exchange symbols"));
        assert!(mexc_transport.seen.lock().unwrap().is_empty());

        let htx_transport = MockTransport::new(json!({"status": "ok"}));
        let htx_adapter =
            PrivatePerpTradingAdapter::new(HtxPrivatePerpProtocol, htx_transport.clone());

        let htx_err = htx_adapter
            .set_leverage(LeverageCommand {
                exchange: ExchangeId::Htx,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
                leverage: 0,
                requested_at: Utc::now(),
            })
            .await
            .unwrap_err();

        assert!(htx_err
            .to_string()
            .contains("htx set leverage requires htx exchange symbols"));
        assert!(htx_transport.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn private_perp_trading_adapter_should_reject_countdown_scope_before_request() {
        let transport = MockTransport::new(json!({
            "code": "00000",
            "data": {"triggerTime": "1700000000000"}
        }));
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport.clone());

        let err = adapter
            .set_countdown_cancel_all(CountdownCancelAllCommand::cancel(
                ExchangeId::Gate,
                Utc::now(),
            ))
            .await
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("bitget countdown cancel-all requires bitget exchange symbols"));

        let err = adapter
            .set_countdown_cancel_all(CountdownCancelAllCommand::set_for_symbol(
                ExchangeId::Bitget,
                ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
                30,
                Utc::now(),
            ))
            .await
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("bitget countdown cancel-all requires bitget exchange symbols"));
        assert!(transport.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn private_perp_trading_adapter_should_reject_countdown_scope_before_capability() {
        let transport = MockTransport::new(json!({
            "success": true
        }));
        let adapter = PrivatePerpTradingAdapter::new(MexcPrivatePerpProtocol, transport.clone());

        let err = adapter
            .set_countdown_cancel_all(CountdownCancelAllCommand::set_for_symbol(
                ExchangeId::Mexc,
                ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
                30,
                Utc::now(),
            ))
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("mexc countdown cancel-all requires mexc exchange symbols"));
        assert!(transport.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn mexc_and_htx_adapters_should_reject_countdown_capability_before_rest() {
        let mexc_transport = MockTransport::new(json!({"success": true}));
        let mexc_adapter =
            PrivatePerpTradingAdapter::new(MexcPrivatePerpProtocol, mexc_transport.clone());
        let mexc_err = mexc_adapter
            .set_countdown_cancel_all(CountdownCancelAllCommand::set_for_symbol(
                ExchangeId::Mexc,
                ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
                30,
                Utc::now(),
            ))
            .await
            .unwrap_err();
        assert!(mexc_err
            .to_string()
            .contains("mexc private perp adapter does not support set countdown cancel-all"));
        assert!(mexc_transport.seen.lock().unwrap().is_empty());

        let htx_transport = MockTransport::new(json!({"status": "ok"}));
        let htx_adapter =
            PrivatePerpTradingAdapter::new(HtxPrivatePerpProtocol, htx_transport.clone());
        let htx_err = htx_adapter
            .set_countdown_cancel_all(CountdownCancelAllCommand::set_for_symbol(
                ExchangeId::Htx,
                ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT"),
                30,
                Utc::now(),
            ))
            .await
            .unwrap_err();
        assert!(htx_err
            .to_string()
            .contains("htx private perp adapter does not support set countdown cancel-all"));
        assert!(htx_transport.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn private_perp_trading_adapter_should_load_registered_symbol_rules() {
        let adapter = PrivatePerpTradingAdapter::new(
            GatePrivatePerpProtocol,
            MockTransport::new(Value::Null),
        )
        .with_instruments([instrument(ExchangeId::Gate, "BTC_USDT")]);

        let rules = adapter
            .load_symbol_rules(&ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(rules.exchange, ExchangeId::Gate);
        assert_eq!(rules.quantity_step, 0.001);
    }

    #[tokio::test]
    async fn private_perp_trading_adapter_should_normalize_open_orders() {
        let transport = MockTransport::new(json!({
            "result": [{
                "id": "gate-order-1",
                "contract": "BTC_USDT",
                "size": "-2",
                "left": "1",
                "price": "65000",
                "status": "open",
                "text": "t-client-1",
                "create_time_ms": "1700000000000"
            }]
        }));
        let adapter = PrivatePerpTradingAdapter::new(GatePrivatePerpProtocol, transport);

        let orders = adapter
            .get_open_orders(Some(&ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT")))
            .await
            .unwrap();

        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].exchange, ExchangeId::Gate);
        assert_eq!(orders[0].side, OrderSide::Sell);
        assert_eq!(orders[0].position_side, PositionSide::Short);
        assert_eq!(orders[0].status, OrderCommandStatus::Accepted);
        assert_eq!(orders[0].filled_quantity, 1.0);
    }

    #[tokio::test]
    async fn gate_adapter_should_normalize_open_order_contracts_to_base_quantity() {
        let transport = MockTransport::new(json!({
            "result": [{
                "id": "gate-order-1",
                "contract": "BTC_USDT",
                "size": "-25",
                "left": "5",
                "price": "65000",
                "status": "open",
                "text": "t-client-1",
                "create_time_ms": "1700000000000"
            }]
        }));
        let adapter = PrivatePerpTradingAdapter::new(GatePrivatePerpProtocol, transport)
            .with_instruments([gate_contract_instrument(0.0001)]);

        let orders = adapter
            .get_open_orders(Some(&ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT")))
            .await
            .unwrap();

        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].quantity, 0.0025);
        assert_eq!(orders[0].filled_quantity, 0.002);
    }

    #[tokio::test]
    async fn gate_adapter_should_parse_finished_order_history() {
        let transport = MockTransport::new(json!([
            {
                "id": "gate-order-1",
                "contract": "BTC_USDT",
                "size": "25",
                "left": "5",
                "price": "65000",
                "fill_price": "64950",
                "status": "finished",
                "finish_as": "cancelled",
                "text": "t-client-1",
                "is_reduce_only": false,
                "create_time_ms": "1700000000000"
            }
        ]));
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(GatePrivatePerpProtocol, transport)
            .with_instruments([gate_contract_instrument(0.0001)]);

        let orders = adapter
            .get_order_history(OrderHistoryQuery::for_symbol(
                ExchangeId::Gate,
                ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
            ))
            .await
            .unwrap();

        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].exchange, ExchangeId::Gate);
        assert_eq!(orders[0].exchange_order_id.as_deref(), Some("gate-order-1"));
        assert_eq!(orders[0].client_order_id.as_deref(), Some("client-1"));
        assert_eq!(orders[0].status, OrderCommandStatus::Cancelled);
        assert_eq!(orders[0].quantity, 0.0025);
        assert_eq!(orders[0].filled_quantity, 0.002);
        assert_eq!(orders[0].average_fill_price, Some(64950.0));
        assert_eq!(seen.lock().unwrap()[0].path, "/futures/usdt/orders");
        assert_eq!(
            seen.lock().unwrap()[0]
                .query
                .get("status")
                .map(String::as_str),
            Some("finished")
        );
    }

    #[tokio::test]
    async fn gate_adapter_should_normalize_fill_contracts_to_base_quantity() {
        let transport = MockTransport::new(json!({
            "result": [{
                "id": "trade-1",
                "order_id": "gate-order-1",
                "contract": "BTC_USDT",
                "size": "25",
                "price": "65000",
                "fee": "0.1",
                "fee_currency": "USDT",
                "role": "maker",
                "create_time_ms": "1700000000000"
            }]
        }));
        let adapter = PrivatePerpTradingAdapter::new(GatePrivatePerpProtocol, transport)
            .with_instruments([gate_contract_instrument(0.0001)]);

        let mut query = FillQuery::new(ExchangeId::Gate);
        query.exchange_symbol = Some(ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"));
        let fills = adapter.get_fills(query).await.unwrap();

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].quantity, 0.0025);
        assert_eq!(fills[0].quote_quantity, 162.5);
    }

    #[tokio::test]
    async fn gate_adapter_should_normalize_position_contracts_to_base_quantity() {
        let transport = MockTransport::new(json!({
            "result": [{
                "contract": "BTC_USDT",
                "size": "-25",
                "entry_price": "64000",
                "mark_price": "65000",
                "unrealised_pnl": "2.5",
                "update_time_ms": "1700000000000"
            }]
        }));
        let adapter = PrivatePerpTradingAdapter::new(GatePrivatePerpProtocol, transport)
            .with_instruments([gate_contract_instrument(0.0001)]);

        let positions = adapter
            .get_positions(Some(&ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT")))
            .await
            .unwrap();

        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].position_side, PositionSide::Short);
        assert_eq!(positions[0].quantity, 0.0025);
    }

    #[tokio::test]
    async fn gate_adapter_should_treat_missing_position_as_flat() {
        let error = PrivateRestError {
            exchange: ExchangeId::Gate,
            class: ExchangeErrorClass::OrderNotFound,
            endpoint: Some("GET /futures/usdt/positions/DOGE_USDT".to_string()),
            code: Some("POSITION_NOT_FOUND".to_string()),
            message: "position not found".to_string(),
        };
        let adapter = PrivatePerpTradingAdapter::new(
            GatePrivatePerpProtocol,
            MockTransport::with_error(error),
        );
        let symbol = ExchangeSymbol::new(ExchangeId::Gate, "DOGE_USDT");

        let positions = adapter.get_positions(Some(&symbol)).await.unwrap();
        let config = adapter.get_symbol_account_config(&symbol).await.unwrap();

        assert!(positions.is_empty());
        assert_eq!(config.exchange, ExchangeId::Gate);
        assert_eq!(config.exchange_symbol, symbol);
        assert_eq!(config.position_mode, Some(PositionMode::OneWay));
        assert_eq!(config.leverage, None);
    }

    #[tokio::test]
    async fn gate_adapter_should_route_balances_and_fee_config_readbacks() {
        let transport = SequentialMockTransport::new([
            json!({
                "currency": "USDT",
                "total": "1000",
                "available": "850",
                "order_margin": "150"
            }),
            json!({
                "name": "BTC_USDT",
                "maker_fee_rate": "-0.00025",
                "taker_fee_rate": "0.00075"
            }),
            json!({
                "contract": "BTC_USDT",
                "marginMode": "cross",
                "leverage": "10",
                "max_leverage": "100"
            }),
        ]);
        let seen = transport.seen.clone();
        let adapter = PrivatePerpTradingAdapter::new(GatePrivatePerpProtocol, transport)
            .with_position_mode(PositionMode::OneWay);
        let symbol = ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT");

        let balances = adapter.get_balances().await.unwrap();
        assert_eq!(balances.len(), 1);
        assert_eq!(balances[0].exchange, ExchangeId::Gate);
        assert_eq!(balances[0].asset, "USDT");
        assert_eq!(balances[0].total, 1000.0);
        assert_eq!(balances[0].available, 850.0);
        assert_eq!(balances[0].locked, 150.0);

        let fee = adapter.get_trade_fee(&symbol).await.unwrap();
        assert_eq!(fee.exchange, ExchangeId::Gate);
        assert_eq!(fee.exchange_symbol, symbol);
        assert_eq!(fee.maker, -0.00025);
        assert_eq!(fee.taker, 0.00075);

        let config = adapter.get_symbol_account_config(&symbol).await.unwrap();
        assert_eq!(config.exchange, ExchangeId::Gate);
        assert_eq!(config.exchange_symbol, symbol);
        assert_eq!(config.position_mode, Some(PositionMode::OneWay));
        assert_eq!(config.margin_mode, Some(MarginMode::Cross));
        assert_eq!(config.leverage, Some(10));
        assert_eq!(config.max_leverage, Some(100));

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 3);
        assert_eq!(seen[0].path, "/futures/usdt/accounts");
        assert_eq!(seen[1].path, "/futures/usdt/contracts/BTC_USDT");
        assert_eq!(seen[2].path, "/futures/usdt/positions/BTC_USDT");
    }

    #[tokio::test]
    async fn gate_adapter_should_route_set_leverage() {
        let transport = MockTransport::new(json!({
            "leverage": "7"
        }));
        let adapter = PrivatePerpTradingAdapter::new(GatePrivatePerpProtocol, transport.clone());

        let ack = adapter
            .set_leverage(LeverageCommand {
                exchange: ExchangeId::Gate,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
                leverage: 7,
                requested_at: Utc::now(),
            })
            .await
            .unwrap();

        assert_leverage_ack(&ack, ExchangeId::Gate, "BTC_USDT", 7, None);
        let seen = transport.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].path, "/futures/usdt/positions/BTC_USDT/leverage");
        assert_eq!(seen[0].query.get("leverage").map(String::as_str), Some("7"));
    }

    #[tokio::test]
    async fn gate_adapter_should_route_countdown_cancel_and_parse_trigger_time() {
        let transport = MockTransport::new(json!({
            "trigger_time": 1_700_000_030
        }));
        let adapter = PrivatePerpTradingAdapter::new(GatePrivatePerpProtocol, transport.clone());

        let ack = adapter
            .set_countdown_cancel_all(CountdownCancelAllCommand::set_for_symbol(
                ExchangeId::Gate,
                ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
                30,
                Utc::now(),
            ))
            .await
            .unwrap();

        assert_countdown_ack(
            &ack,
            ExchangeId::Gate,
            Some("BTC_USDT"),
            30,
            Some(1_700_000_030),
            None,
        );
        let seen = transport.seen.lock().unwrap();
        assert_eq!(seen[0].path, "/futures/usdt/countdown_cancel_all");
    }

    #[test]
    fn gate_protocol_should_use_payload_contract_size_when_present() {
        let now = Utc::now();
        let raw = r#"{
          "channel":"futures.usertrades","event":"update",
          "result":[{
            "id":"t1","order_id":"o1","contract":"BTC_USDT","contracts":"25",
            "contract_size":"0.0001","price":"65000","fee":"0.1",
            "fee_currency":"USDT","role":"taker","create_time_ms":"1700000000000"
          }]
        }"#;

        let events = GatePrivatePerpProtocol
            .parse_private_ws_message(raw, now)
            .unwrap();

        match &events[0].kind {
            PrivateEventKind::Fill(fill) => {
                assert_eq!(fill.quantity, 0.0025);
                assert_eq!(fill.quote_quantity, 162.5);
            }
            other => panic!("expected fill event, got {other:?}"),
        }
    }

    #[test]
    fn reqwest_transport_should_build_bitget_headers() {
        let transport = ReqwestPrivateRestTransport::new(
            PrivatePerpExchange::Bitget,
            PrivateRestAuth {
                api_key: "key".to_string(),
                api_secret: "secret".to_string(),
                passphrase: Some("pass".to_string()),
                demo_trading: false,
            },
        )
        .unwrap();
        let spec = BitgetPrivatePerpProtocol
            .get_open_orders(Some(&ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT")))
            .unwrap();

        let headers = transport.signed_headers(&spec, 1_700_000_000_000).unwrap();

        assert_eq!(headers.get("ACCESS-KEY").map(String::as_str), Some("key"));
        assert_eq!(
            headers.get("ACCESS-PASSPHRASE").map(String::as_str),
            Some("pass")
        );
        assert_eq!(headers.get("paptrading").map(String::as_str), None);
        assert!(headers.get("ACCESS-SIGN").is_some_and(|v| !v.is_empty()));
        assert_eq!(
            headers.get("ACCESS-TIMESTAMP").map(String::as_str),
            Some("1700000000000")
        );
    }

    #[test]
    fn reqwest_transport_should_add_bitget_demo_header_when_enabled() {
        let transport = ReqwestPrivateRestTransport::new(
            PrivatePerpExchange::Bitget,
            PrivateRestAuth {
                api_key: "key".to_string(),
                api_secret: "secret".to_string(),
                passphrase: Some("pass".to_string()),
                demo_trading: true,
            },
        )
        .unwrap();
        let spec = BitgetPrivatePerpProtocol
            .get_open_orders(Some(&ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT")))
            .unwrap();

        let headers = transport.signed_headers(&spec, 1_700_000_000_000).unwrap();

        assert_eq!(headers.get("paptrading").map(String::as_str), Some("1"));
    }

    #[test]
    fn reqwest_transport_should_build_gate_headers_with_api_v4_sign_path() {
        let transport = ReqwestPrivateRestTransport::new(
            PrivatePerpExchange::Gate,
            PrivateRestAuth {
                api_key: "key".to_string(),
                api_secret: "secret".to_string(),
                passphrase: None,
                demo_trading: false,
            },
        )
        .unwrap();
        let spec = GatePrivatePerpProtocol
            .get_open_orders(Some(&ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT")))
            .unwrap();

        let headers = transport.signed_headers(&spec, 1_700_000_000).unwrap();

        assert_eq!(headers.get("KEY").map(String::as_str), Some("key"));
        assert_eq!(
            headers.get("X-Gate-Size-Decimal").map(String::as_str),
            Some("1")
        );
        assert_eq!(
            headers.get("Timestamp").map(String::as_str),
            Some("1700000000")
        );
        assert!(headers.get("SIGN").is_some_and(|v| !v.is_empty()));
    }

    #[test]
    fn reqwest_transport_should_build_new_exchange_headers() {
        let auth = PrivateRestAuth {
            api_key: "key".to_string(),
            api_secret: "secret".to_string(),
            passphrase: None,
            demo_trading: false,
        };

        let bybit_transport =
            ReqwestPrivateRestTransport::new(PrivatePerpExchange::Bybit, auth.clone()).unwrap();
        let bybit_spec = BybitPrivatePerpProtocol
            .get_open_orders(Some(&ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT")))
            .unwrap();
        let bybit_headers = bybit_transport
            .signed_headers(&bybit_spec, 1_700_000_000_000)
            .unwrap();
        assert_eq!(
            bybit_headers.get("X-BAPI-API-KEY").map(String::as_str),
            Some("key")
        );
        assert!(bybit_headers
            .get("X-BAPI-SIGN")
            .is_some_and(|value| !value.is_empty()));

        let mexc_transport =
            ReqwestPrivateRestTransport::new(PrivatePerpExchange::Mexc, auth.clone()).unwrap();
        let mexc_spec = MexcPrivatePerpProtocol
            .get_positions(Some(&ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT")))
            .unwrap();
        let mexc_headers = mexc_transport
            .signed_headers(&mexc_spec, 1_700_000_000_000)
            .unwrap();
        assert_eq!(mexc_headers.get("ApiKey").map(String::as_str), Some("key"));
        assert!(mexc_headers
            .get("Signature")
            .is_some_and(|value| !value.is_empty()));

        let htx_transport =
            ReqwestPrivateRestTransport::new(PrivatePerpExchange::Htx, auth).unwrap();
        let htx_spec = HtxPrivatePerpProtocol.get_balances().unwrap();
        let htx_headers = htx_transport
            .signed_headers(&htx_spec, 1_700_000_000)
            .unwrap();
        assert_eq!(
            htx_headers.get("AccessKeyId").map(String::as_str),
            Some("key")
        );
        assert!(htx_headers
            .get("Signature")
            .is_some_and(|value| !value.is_empty()));
    }

    #[test]
    fn htx_binary_decoder_should_inflate_gzip_payloads() {
        use flate2::{write::GzEncoder, Compression};
        use std::io::Write;

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder
            .write_all(br#"{"op":"ping","ts":1700000000000}"#)
            .unwrap();
        let compressed = encoder.finish().unwrap();

        let text = decode_private_ws_binary(PrivatePerpExchange::Htx, &compressed).unwrap();

        assert!(text.contains("\"op\":\"ping\""));
    }

    #[test]
    fn private_rest_response_should_reject_error_envelopes() {
        let bitget = normalize_private_rest_response(
            ExchangeId::Bitget,
            json!({"code":"40010","msg":"bad request"}),
        )
        .unwrap_err();
        assert!(bitget.to_string().contains("40010"));
        let bitget = bitget.downcast_ref::<PrivateRestError>().unwrap();
        assert_eq!(bitget.class, ExchangeErrorClass::Unknown);

        let gate = normalize_private_rest_response(
            ExchangeId::Gate,
            json!({"label":"INVALID_KEY","message":"bad key"}),
        )
        .unwrap_err();
        assert!(gate.to_string().contains("INVALID_KEY"));
        let gate = gate.downcast_ref::<PrivateRestError>().unwrap();
        assert_eq!(gate.class, ExchangeErrorClass::Authentication);
    }

    #[test]
    fn private_rest_error_classifier_should_cover_recovery_classes() {
        assert_eq!(
            classify_gate_rest_error("ORDER_NOT_FOUND", "order not found"),
            ExchangeErrorClass::OrderNotFound
        );
        assert_eq!(
            classify_gate_rest_error("INSUFFICIENT_AVAILABLE", "not enough balance"),
            ExchangeErrorClass::InsufficientBalance
        );
        assert_eq!(
            classify_bitget_rest_error("40762", "clientOid already exists"),
            ExchangeErrorClass::DuplicateClientOrderId
        );
        assert_eq!(
            classify_bitget_rest_error("429", "too many requests"),
            ExchangeErrorClass::RateLimited
        );
    }

    #[tokio::test]
    async fn private_perp_position_mode_should_update_local_state_after_set() {
        let transport = MockTransport::new(json!({"code": "00000", "msg": "success"}));
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport.clone());

        adapter
            .set_position_mode(PositionModeCommand {
                exchange: ExchangeId::Bitget,
                mode: PositionMode::Hedge,
                requested_at: Utc::now(),
            })
            .await
            .unwrap();

        let mut cmd = command(ExchangeId::Bitget, "BTCUSDT");
        cmd.position_side = PositionSide::Short;
        cmd.side = OrderSide::Sell;
        adapter.place_order(cmd).await.unwrap();
        let seen = transport.seen.lock().unwrap();
        let body = seen.last().unwrap().body.as_ref().unwrap();
        assert_eq!(body["side"], "sell");
        assert_eq!(body["tradeSide"], "open");
        assert!(body.get("reduceOnly").is_none());
    }

    #[tokio::test]
    async fn binance_adapter_position_mode_should_route_and_update_local_state() {
        let transport = SequentialMockTransport::new([
            json!({"code": 200, "msg": "success"}),
            json!({"orderId": 12345, "clientOrderId": "client-1"}),
        ]);
        let adapter = PrivatePerpTradingAdapter::new(BinancePrivatePerpProtocol, transport.clone());

        let ack = adapter
            .set_position_mode(PositionModeCommand {
                exchange: ExchangeId::Binance,
                mode: PositionMode::Hedge,
                requested_at: Utc::now(),
            })
            .await
            .unwrap();

        assert_position_mode_ack(
            &ack,
            ExchangeId::Binance,
            PositionMode::Hedge,
            Some("success"),
        );

        let mut cmd = command(ExchangeId::Binance, "BTCUSDT");
        cmd.position_side = PositionSide::Short;
        cmd.side = OrderSide::Sell;
        adapter.place_order(cmd).await.unwrap();

        let seen = transport.seen.lock().unwrap();
        assert_eq!(seen[0].path, "/fapi/v1/positionSide/dual");
        assert_eq!(
            seen[0].query.get("dualSidePosition").map(String::as_str),
            Some("true")
        );
        assert_eq!(seen[1].path, "/fapi/v1/order");
        assert_eq!(
            seen[1].query.get("positionSide").map(String::as_str),
            Some("SHORT")
        );
    }

    #[tokio::test]
    async fn okx_adapter_position_mode_should_route_and_update_local_state() {
        let transport = SequentialMockTransport::new([
            json!({"code": "0", "msg": "success", "data": []}),
            json!({"code": "0", "data": [{"ordId": "okx-order-1", "clOrdId": "client-1"}]}),
        ]);
        let adapter = PrivatePerpTradingAdapter::new(OkxPrivatePerpProtocol, transport.clone());

        let ack = adapter
            .set_position_mode(PositionModeCommand {
                exchange: ExchangeId::Okx,
                mode: PositionMode::Hedge,
                requested_at: Utc::now(),
            })
            .await
            .unwrap();

        assert_position_mode_ack(&ack, ExchangeId::Okx, PositionMode::Hedge, Some("success"));

        let mut cmd = command(ExchangeId::Okx, "BTC-USDT-SWAP");
        cmd.position_side = PositionSide::Short;
        cmd.side = OrderSide::Sell;
        adapter.place_order(cmd).await.unwrap();

        let seen = transport.seen.lock().unwrap();
        assert_eq!(seen[0].path, "/api/v5/account/set-position-mode");
        assert_eq!(seen[0].body.as_ref().unwrap()["posMode"], "long_short_mode");
        assert_eq!(seen[1].path, "/api/v5/trade/order");
        assert_eq!(seen[1].body.as_ref().unwrap()["posSide"], "short");
    }

    #[tokio::test]
    async fn bitget_adapter_position_mode_should_route_and_update_local_state() {
        let transport = SequentialMockTransport::new([
            json!({"code": "00000", "msg": "success"}),
            json!({"code": "00000", "data": {"orderId": "bitget-order-1", "clientOid": "client-1"}}),
        ]);
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport.clone());

        let ack = adapter
            .set_position_mode(PositionModeCommand {
                exchange: ExchangeId::Bitget,
                mode: PositionMode::Hedge,
                requested_at: Utc::now(),
            })
            .await
            .unwrap();

        assert_position_mode_ack(
            &ack,
            ExchangeId::Bitget,
            PositionMode::Hedge,
            Some("success"),
        );

        let mut cmd = command(ExchangeId::Bitget, "BTCUSDT");
        cmd.position_side = PositionSide::Short;
        cmd.side = OrderSide::Sell;
        adapter.place_order(cmd).await.unwrap();

        let seen = transport.seen.lock().unwrap();
        assert_eq!(seen[0].path, "/api/v2/mix/account/set-position-mode");
        let mode_body = seen[0].body.as_ref().unwrap();
        assert_eq!(mode_body["productType"], "USDT-FUTURES");
        assert_eq!(mode_body["posMode"], "hedge_mode");
        assert_eq!(seen[1].path, "/api/v2/mix/order/place-order");
        let order_body = seen[1].body.as_ref().unwrap();
        assert_eq!(order_body["side"], "sell");
        assert_eq!(order_body["tradeSide"], "open");
    }

    #[tokio::test]
    async fn bybit_adapter_position_mode_should_route_and_update_local_state() {
        let transport = SequentialMockTransport::new([
            json!({"retCode": 0, "retMsg": "OK", "result": {}}),
            json!({"retCode": 0, "retMsg": "OK", "result": {"orderId": "bybit-order-1", "orderLinkId": "client-1"}}),
        ]);
        let adapter = PrivatePerpTradingAdapter::new(BybitPrivatePerpProtocol, transport.clone());

        let ack = adapter
            .set_position_mode(PositionModeCommand {
                exchange: ExchangeId::Bybit,
                mode: PositionMode::Hedge,
                requested_at: Utc::now(),
            })
            .await
            .unwrap();

        assert_position_mode_ack(&ack, ExchangeId::Bybit, PositionMode::Hedge, None);

        let mut cmd = command(ExchangeId::Bybit, "BTCUSDT");
        cmd.position_side = PositionSide::Short;
        cmd.side = OrderSide::Sell;
        adapter.place_order(cmd).await.unwrap();

        let seen = transport.seen.lock().unwrap();
        assert_eq!(seen[0].path, "/v5/position/switch-mode");
        let mode_body = seen[0].body.as_ref().unwrap();
        assert_eq!(mode_body["category"], "linear");
        assert_eq!(mode_body["coin"], "USDT");
        assert_eq!(mode_body["mode"], 3);
        assert_eq!(seen[1].path, "/v5/order/create");
        assert_eq!(seen[1].body.as_ref().unwrap()["positionIdx"], 2);
    }

    #[tokio::test]
    async fn gate_adapter_should_reject_position_mode_capability_before_rest() {
        let transport = MockTransport::new(json!({"message": "success"}));
        let adapter = PrivatePerpTradingAdapter::new(GatePrivatePerpProtocol, transport.clone());

        let err = adapter
            .set_position_mode(PositionModeCommand {
                exchange: ExchangeId::Gate,
                mode: PositionMode::Hedge,
                requested_at: Utc::now(),
            })
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("gate private perp adapter does not support set position mode"));
        assert!(transport.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn mexc_adapter_position_mode_should_route_and_update_local_state() {
        let transport =
            MockTransport::new(json!({"success": true, "code": 0, "message": "success"}));
        let adapter = PrivatePerpTradingAdapter::new(MexcPrivatePerpProtocol, transport.clone());

        let ack = adapter
            .set_position_mode(PositionModeCommand {
                exchange: ExchangeId::Mexc,
                mode: PositionMode::Hedge,
                requested_at: Utc::now(),
            })
            .await
            .unwrap();

        assert_position_mode_ack(&ack, ExchangeId::Mexc, PositionMode::Hedge, Some("success"));

        let mut cmd = command(ExchangeId::Mexc, "BTC_USDT");
        cmd.position_side = PositionSide::Short;
        cmd.side = OrderSide::Sell;
        adapter.place_order(cmd).await.unwrap();

        let seen = transport.seen.lock().unwrap();
        assert_eq!(
            seen[0].path,
            "/api/v1/private/position/change_position_mode"
        );
        assert_eq!(seen[0].body.as_ref().unwrap()["positionMode"], 1);
        assert_eq!(seen[1].path, "/api/v1/private/order/submit");
        let order_body = seen[1].body.as_ref().unwrap();
        assert_eq!(order_body["positionMode"], "hedge_mode");
        assert_eq!(order_body["side"], 3);
    }

    #[tokio::test]
    async fn htx_adapter_position_mode_should_route_and_update_local_state() {
        let transport = MockTransport::new(json!({"status": "ok", "msg": "success"}));
        let adapter = PrivatePerpTradingAdapter::new(HtxPrivatePerpProtocol, transport.clone());

        let ack = adapter
            .set_position_mode(PositionModeCommand {
                exchange: ExchangeId::Htx,
                mode: PositionMode::Hedge,
                requested_at: Utc::now(),
            })
            .await
            .unwrap();

        assert_position_mode_ack(&ack, ExchangeId::Htx, PositionMode::Hedge, Some("success"));

        let mut cmd = command(ExchangeId::Htx, "BTC-USDT");
        cmd.position_side = PositionSide::Short;
        cmd.side = OrderSide::Sell;
        adapter.place_order(cmd).await.unwrap();

        let seen = transport.seen.lock().unwrap();
        assert_eq!(
            seen[0].path,
            "/linear-swap-api/v1/swap_cross_switch_position_mode"
        );
        let mode_body = seen[0].body.as_ref().unwrap();
        assert_eq!(mode_body["margin_account"], "USDT");
        assert_eq!(mode_body["position_mode"], "dual_side");
        assert_eq!(seen[1].path, "/linear-swap-api/v1/swap_cross_order");
        let order_body = seen[1].body.as_ref().unwrap();
        assert_eq!(order_body["direction"], "sell");
        assert_eq!(order_body["offset"], "open");
    }

    #[tokio::test]
    async fn private_perp_trading_adapter_should_reject_position_mode_scope_before_request() {
        let transport = MockTransport::new(json!({"code": "00000", "msg": "success"}));
        let adapter = PrivatePerpTradingAdapter::new(BitgetPrivatePerpProtocol, transport.clone());

        let err = adapter
            .set_position_mode(PositionModeCommand {
                exchange: ExchangeId::Gate,
                mode: PositionMode::Hedge,
                requested_at: Utc::now(),
            })
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("bitget position mode requires bitget exchange"));
        assert!(transport.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn mexc_and_htx_adapters_should_reject_position_mode_scope_before_request() {
        let mexc_transport = MockTransport::new(json!({"success": true, "code": 0}));
        let mexc_adapter =
            PrivatePerpTradingAdapter::new(MexcPrivatePerpProtocol, mexc_transport.clone());

        let mexc_err = mexc_adapter
            .set_position_mode(PositionModeCommand {
                exchange: ExchangeId::Htx,
                mode: PositionMode::Hedge,
                requested_at: Utc::now(),
            })
            .await
            .unwrap_err();

        assert!(mexc_err
            .to_string()
            .contains("mexc position mode requires mexc exchange"));
        assert!(mexc_transport.seen.lock().unwrap().is_empty());

        let htx_transport = MockTransport::new(json!({"status": "ok"}));
        let htx_adapter =
            PrivatePerpTradingAdapter::new(HtxPrivatePerpProtocol, htx_transport.clone());

        let htx_err = htx_adapter
            .set_position_mode(PositionModeCommand {
                exchange: ExchangeId::Mexc,
                mode: PositionMode::Hedge,
                requested_at: Utc::now(),
            })
            .await
            .unwrap_err();

        assert!(htx_err
            .to_string()
            .contains("htx position mode requires htx exchange"));
        assert!(htx_transport.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn private_perp_trading_adapter_should_reject_position_mode_scope_before_capability() {
        let transport = MockTransport::new(json!({"id": "gate-order-1"}));
        let adapter = PrivatePerpTradingAdapter::new(GatePrivatePerpProtocol, transport.clone());

        let err = adapter
            .set_position_mode(PositionModeCommand {
                exchange: ExchangeId::Mexc,
                mode: PositionMode::Hedge,
                requested_at: Utc::now(),
            })
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("gate position mode requires gate exchange"));
        assert!(transport.seen.lock().unwrap().is_empty());
    }

    #[test]
    fn private_ws_should_parse_heartbeat_and_subscribe_errors() {
        let now = Utc::now();
        let bitget_pong = BitgetPrivatePerpProtocol
            .parse_private_ws_message("pong", now)
            .unwrap();
        assert!(matches!(bitget_pong[0].kind, PrivateEventKind::Heartbeat));

        let bitget_error = BitgetPrivatePerpProtocol
            .parse_private_ws_message(
                r#"{"event":"error","code":"30001","msg":"invalid sign"}"#,
                now,
            )
            .unwrap();
        match &bitget_error[0].kind {
            PrivateEventKind::Error(error) => {
                assert_eq!(error.code.as_deref(), Some("30001"));
                assert!(error.message.contains("invalid sign"));
            }
            other => panic!("expected error event, got {other:?}"),
        }

        let bybit_pong = BybitPrivatePerpProtocol
            .parse_private_ws_message(r#"{"op":"pong"}"#, now)
            .unwrap();
        assert!(matches!(bybit_pong[0].kind, PrivateEventKind::Heartbeat));

        let bybit_success = BybitPrivatePerpProtocol
            .parse_private_ws_message(r#"{"op":"subscribe","success":true}"#, now)
            .unwrap();
        assert!(matches!(bybit_success[0].kind, PrivateEventKind::Heartbeat));

        let bybit_error = BybitPrivatePerpProtocol
            .parse_private_ws_message(
                r#"{"op":"subscribe","success":false,"retCode":"10001","retMsg":"invalid topic"}"#,
                now,
            )
            .unwrap();
        match &bybit_error[0].kind {
            PrivateEventKind::Error(error) => {
                assert_eq!(error.code.as_deref(), Some("10001"));
                assert!(error.message.contains("invalid topic"));
            }
            other => panic!("expected error event, got {other:?}"),
        }

        let gate_error = GatePrivatePerpProtocol
            .parse_private_ws_message(
                r#"{"time":1,"channel":"futures.orders","event":"subscribe","result":{"status":"fail","message":"bad auth"}}"#,
                now,
            )
            .unwrap();
        match &gate_error[0].kind {
            PrivateEventKind::Error(error) => assert!(error.message.contains("bad auth")),
            other => panic!("expected error event, got {other:?}"),
        }

        let gate_wrapper_success = parse_private_ws_message_with_symbol_rules(
            GatePrivatePerpProtocol,
            &HashMap::new(),
            r#"{"time":1,"channel":"futures.orders","event":"subscribe","result":{"status":"success"}}"#,
            now,
        )
        .unwrap();
        assert!(matches!(
            gate_wrapper_success[0].kind,
            PrivateEventKind::Heartbeat
        ));

        let gate_wrapper_error = parse_private_ws_message_with_symbol_rules(
            GatePrivatePerpProtocol,
            &HashMap::new(),
            r#"{"time":1,"channel":"futures.orders","event":"subscribe","result":{"status":"fail","message":"wrapper bad auth"}}"#,
            now,
        )
        .unwrap();
        match &gate_wrapper_error[0].kind {
            PrivateEventKind::Error(error) => assert!(error.message.contains("wrapper bad auth")),
            other => panic!("expected error event, got {other:?}"),
        }
    }

    trait BoolNot {
        fn not(self) -> bool;
    }

    impl BoolNot for bool {
        fn not(self) -> bool {
            !self
        }
    }
}
