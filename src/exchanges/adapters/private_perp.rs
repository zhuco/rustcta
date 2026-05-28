//! Private USDT perpetual REST/WebSocket protocol contracts for Bitget and Gate.
//!
//! This module is intentionally side-effect free: it builds signed request
//! payloads, private WebSocket login/subscription messages, and parses private
//! WebSocket payloads into execution-layer events. Network clients should call
//! this layer instead of encoding exchange-specific fields in strategy code.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use base64::{engine::general_purpose, Engine as _};
use chrono::{DateTime, Utc};
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
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::core::types::{Fee, MarketType, OrderStatus as CoreOrderStatus};
use crate::execution::{
    AmendOrderAck, AmendOrderCommand, CancelAck, CancelAllAck, CancelAllCommand, CancelBatchAck,
    CancelBatchCommand, CancelCommand, ClosePositionAck, ClosePositionCommand,
    CountdownCancelAllAck, CountdownCancelAllCommand, ExchangeBalance, ExchangeErrorClass,
    ExchangePosition, FillEvent, FillLiquidity, FillQuery, LeverageAck, LeverageCommand,
    MarginMode, OrderAck, OrderCommand, OrderCommandStatus, OrderQuery, OrderSide, OrderState,
    OrderType, PositionMode, PositionModeAck, PositionModeCommand, PositionSide, PrivateErrorEvent,
    PrivateEvent, PrivateEventKind, SymbolAccountConfig, TimeInForce, TradeFeeSnapshot,
    TradingAdapter, TradingCapabilities,
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
    Bitget,
    Gate,
    Bybit,
    Mexc,
    Htx,
}

impl PrivatePerpExchange {
    pub fn exchange_id(self) -> ExchangeId {
        match self {
            Self::Bitget => ExchangeId::Bitget,
            Self::Gate => ExchangeId::Gate,
            Self::Bybit => ExchangeId::Bybit,
            Self::Mexc => ExchangeId::Mexc,
            Self::Htx => ExchangeId::Htx,
        }
    }

    pub fn rest_base_url(self) -> &'static str {
        match self {
            Self::Bitget => "https://api.bitget.com",
            Self::Gate => "https://api.gateio.ws/api/v4",
            Self::Bybit => "https://api.bybit.com",
            Self::Mexc => "https://contract.mexc.com",
            Self::Htx => "https://api.hbdm.com",
        }
    }

    pub fn private_ws_url(self) -> &'static str {
        match self {
            Self::Bitget => "wss://ws.bitget.com/v2/ws/private",
            Self::Gate => "wss://fx-ws.gateio.ws/v4/ws/usdt",
            Self::Bybit => "wss://stream.bybit.com/v5/private",
            Self::Mexc => "wss://contract.mexc.com/edge",
            Self::Htx => "wss://api.hbdm.com/linear-swap-notification",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PrivateRestMethod {
    Get,
    Post,
    Patch,
    Delete,
}

impl PrivateRestMethod {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
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
    fn cancel_order(&self, command: &CancelCommand) -> Result<PrivateRestRequestSpec>;
    fn cancel_all_orders(&self, command: &CancelAllCommand) -> Result<PrivateRestRequestSpec>;
    fn cancel_batch_orders(&self, command: &CancelBatchCommand) -> Result<PrivateRestRequestSpec>;
    fn get_order(&self, query: &OrderQuery) -> Result<PrivateRestRequestSpec>;
    fn get_open_orders(&self, symbol: Option<&ExchangeSymbol>) -> Result<PrivateRestRequestSpec>;
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
        let client = reqwest::Client::builder()
            .user_agent("RustCTA/0.3 private-perp")
            .timeout(Duration::from_secs(15))
            .build()?;
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
            PrivatePerpExchange::Bitget => bitget_rest_headers(&self.auth, request, timestamp),
            PrivatePerpExchange::Gate => {
                let mut headers = gate_rest_headers(&self.auth, request, timestamp)?;
                headers.insert("X-Gate-Size-Decimal".to_string(), "1".to_string());
                Ok(headers)
            }
            PrivatePerpExchange::Bybit => bybit_rest_headers(&self.auth, request, timestamp),
            PrivatePerpExchange::Mexc => mexc_rest_headers(&self.auth, request, timestamp),
            PrivatePerpExchange::Htx => htx_rest_headers(&self.auth, request, timestamp),
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
            PrivatePerpExchange::Bitget
            | PrivatePerpExchange::Bybit
            | PrivatePerpExchange::Mexc => Utc::now().timestamp_millis(),
            PrivatePerpExchange::Gate | PrivatePerpExchange::Htx => Utc::now().timestamp(),
        };
        let mut builder = match request.method {
            PrivateRestMethod::Get => self.client.get(self.url(&request)),
            PrivateRestMethod::Post => self.client.post(self.url(&request)),
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
        let order_id = response_string(&value, &["orderId", "id"])
            .or_else(|| response_string(&value, &["clientOid", "text"]))
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

    async fn cancel_order(&self, command: CancelCommand) -> Result<CancelAck> {
        let value = self
            .transport
            .execute(self.protocol.cancel_order(&command)?)
            .await?;
        Ok(CancelAck {
            exchange: command.exchange,
            client_order_id: command.client_order_id,
            exchange_order_id: command
                .exchange_order_id
                .or_else(|| response_string(&value, &["orderId", "id"])),
            accepted: true,
            status: OrderCommandStatus::Cancelled,
            message: response_string(&value, &["msg", "message"]),
            acknowledged_at: Utc::now(),
        })
    }

    async fn cancel_all_orders(&self, command: CancelAllCommand) -> Result<CancelAllAck> {
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
        if self.exchange() == ExchangeId::Gate {
            let mut order_acks = Vec::new();
            for order in &command.orders {
                order_acks.push(self.cancel_order(order.clone()).await?);
            }
            return Ok(CancelBatchAck {
                exchange: command.exchange,
                accepted: true,
                cancelled_orders: order_acks.len(),
                order_acks,
                message: Some("gate cancel-batch executed as per-order cancels".to_string()),
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
            order_acks.len()
        };
        Ok(CancelBatchAck {
            exchange: command.exchange,
            accepted: true,
            cancelled_orders,
            order_acks,
            message: response_string(&value, &["msg", "message"]),
            acknowledged_at: Utc::now(),
        })
    }

    async fn get_order(&self, query: OrderQuery) -> Result<OrderState> {
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
        let value = self
            .transport
            .execute(self.protocol.get_positions(symbol)?)
            .await?;
        let exchange = self.exchange();
        Ok(response_items(&value)
            .into_iter()
            .filter_map(|item| match exchange {
                ExchangeId::Bitget => {
                    parse_bitget_position(&item, Utc::now()).and_then(event_position)
                }
                ExchangeId::Bybit => {
                    parse_bybit_position(&item, Utc::now()).and_then(event_position)
                }
                ExchangeId::Mexc => parse_mexc_position(&item, Utc::now()).and_then(event_position),
                ExchangeId::Htx => parse_htx_position(&item, Utc::now()).and_then(event_position),
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
                ExchangeId::Bitget => {
                    parse_bitget_balance(&item, Utc::now()).and_then(event_balance)
                }
                ExchangeId::Bybit => parse_bybit_balance(&item, Utc::now()).and_then(event_balance),
                ExchangeId::Mexc => parse_mexc_balance(&item, Utc::now()).and_then(event_balance),
                ExchangeId::Htx => parse_htx_balance(&item, Utc::now()).and_then(event_balance),
                ExchangeId::Gate => parse_gate_balance(&item, Utc::now()).and_then(event_balance),
                _ => None,
            })
            .collect())
    }

    async fn get_trade_fee(&self, symbol: &ExchangeSymbol) -> Result<TradeFeeSnapshot> {
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
        let value = self
            .transport
            .execute(self.protocol.get_symbol_account_config(symbol)?)
            .await?;
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
        let value = self
            .transport
            .execute(self.protocol.get_fills(&query)?)
            .await?;
        let exchange = self.exchange();
        Ok(response_items(&value)
            .into_iter()
            .filter_map(|item| match exchange {
                ExchangeId::Bitget => parse_bitget_fill(&item, Utc::now()).and_then(event_fill),
                ExchangeId::Bybit => parse_bybit_fill(&item, Utc::now()).and_then(event_fill),
                ExchangeId::Mexc => parse_mexc_fill(&item, Utc::now()).and_then(event_fill),
                ExchangeId::Htx => parse_htx_fill(&item, Utc::now()).and_then(event_fill),
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
                .or_else(|| response_string(&value, &["clientOid", "text"])),
            exchange_order_id: command
                .exchange_order_id
                .or_else(|| response_string(&value, &["orderId", "id"])),
            accepted: true,
            status: OrderCommandStatus::Accepted,
            message: response_string(&value, &["msg", "message"]),
            acknowledged_at: Utc::now(),
        })
    }

    async fn set_leverage(&self, command: LeverageCommand) -> Result<LeverageAck> {
        ensure_capability(
            self.exchange(),
            self.capabilities().supports_leverage,
            "set leverage",
        )?;
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
            message: response_string(&value, &["msg", "message"]),
            acknowledged_at: Utc::now(),
        })
    }

    async fn close_position(&self, command: ClosePositionCommand) -> Result<ClosePositionAck> {
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
        let order_id = response_string(&value, &["orderId", "id"])
            .or_else(|| response_string(&value, &["clientOid", "text"]))
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
            ExchangeId::Bitget | ExchangeId::Bybit | ExchangeId::Mexc
        ),
        supports_client_order_id: true,
        supports_leverage: true,
        supports_position_mode_change: matches!(
            exchange,
            ExchangeId::Bitget | ExchangeId::Bybit | ExchangeId::Mexc
        ),
        supports_close_position: true,
        supports_countdown_cancel_all: matches!(exchange, ExchangeId::Gate | ExchangeId::Bybit),
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
}

impl Default for PrivateWsRunConfig {
    fn default() -> Self {
        Self {
            connect_timeout_ms: 10_000,
            reconnect_delay_ms: 2_000,
            heartbeat_interval_ms: 15_000,
            subscribe_interval_ms: 50,
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
        PrivatePerpExchange::Bitget => ["orders", "positions", "account"]
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
        PrivatePerpExchange::Gate => [
            "futures.orders",
            "futures.usertrades",
            "futures.positions",
            "futures.balances",
        ]
        .into_iter()
        .map(|channel| {
            protocol
                .ws_subscribe(
                    &PrivateWsSubscription {
                        channel: channel.to_string(),
                        symbols: symbols.to_vec(),
                        auth: Some(auth.clone()),
                    },
                    timestamp,
                )
                .map(|request| request.message)
        })
        .collect::<Result<Vec<_>>>()?,
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
    };

    let login_message = match exchange {
        PrivatePerpExchange::Bitget
        | PrivatePerpExchange::Bybit
        | PrivatePerpExchange::Mexc
        | PrivatePerpExchange::Htx => Some(protocol.ws_login(&auth, timestamp)?.message),
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
            PrivatePerpExchange::Bitget => {
                run_private_ws_protocol_with_url(
                    BitgetPrivatePerpProtocol,
                    auth.clone(),
                    symbols.clone(),
                    config,
                    url.clone(),
                    tx.clone(),
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
    )
    .await
}

pub async fn run_private_ws_protocol_with_url<P>(
    protocol: P,
    auth: PrivateWsAuth,
    symbols: Vec<ExchangeSymbol>,
    config: PrivateWsRunConfig,
    url: impl Into<String>,
    tx: mpsc::Sender<PrivateEvent>,
) -> Result<()>
where
    P: PrivatePerpProtocol + Send + Sync + Copy + 'static,
{
    let exchange = protocol.exchange();
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
                    PrivatePerpExchange::Bitget => write.send(Message::Text("ping".to_string())).await?,
                    PrivatePerpExchange::Gate | PrivatePerpExchange::Bybit | PrivatePerpExchange::Mexc => write.send(Message::Ping(Vec::new())).await?,
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
                        match protocol.parse_private_ws_message(&raw, received_at) {
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
                                match protocol.parse_private_ws_message(&text, received_at) {
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
pub struct BitgetPrivatePerpProtocol;

#[derive(Debug, Clone, Copy, Default)]
pub struct GatePrivatePerpProtocol;

#[derive(Debug, Clone, Copy, Default)]
pub struct BybitPrivatePerpProtocol;

#[derive(Debug, Clone, Copy, Default)]
pub struct MexcPrivatePerpProtocol;

#[derive(Debug, Clone, Copy, Default)]
pub struct HtxPrivatePerpProtocol;

impl PrivatePerpProtocol for BitgetPrivatePerpProtocol {
    fn exchange(&self) -> PrivatePerpExchange {
        PrivatePerpExchange::Bitget
    }

    fn place_order(
        &self,
        command: &OrderCommand,
        position_mode: PositionMode,
    ) -> Result<PrivateRestRequestSpec> {
        let side = if position_mode.is_hedge() {
            bitget_position_side(command.position_side, command.side)
        } else {
            bitget_side(command.side)
        };
        let mut body = json!({
            "productType": "USDT-FUTURES",
            "symbol": command.exchange_symbol.symbol,
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

    fn cancel_order(&self, command: &CancelCommand) -> Result<PrivateRestRequestSpec> {
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
        let mut body = json!({
            "productType": "USDT-FUTURES",
            "marginCoin": "USDT",
        });
        if let Some(symbol) = &command.exchange_symbol {
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
            spec = spec.with_query("symbol", &symbol.symbol);
        }
        Ok(spec)
    }

    fn get_fills(&self, query: &FillQuery) -> Result<PrivateRestRequestSpec> {
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Bitget,
            PrivateRestMethod::Get,
            "/api/v2/mix/order/fills",
        )
        .with_query("productType", "USDT-FUTURES");
        if let Some(symbol) = &query.exchange_symbol {
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
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Bitget,
            PrivateRestMethod::Get,
            "/api/v2/mix/market/contracts",
        )
        .with_query("productType", "USDT-FUTURES")
        .with_query("symbol", &symbol.symbol))
    }

    fn get_symbol_account_config(&self, symbol: &ExchangeSymbol) -> Result<PrivateRestRequestSpec> {
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Bitget,
            PrivateRestMethod::Get,
            "/api/v2/mix/position/single-position",
        )
        .with_query("productType", "USDT-FUTURES")
        .with_query("marginCoin", "USDT")
        .with_query("symbol", &symbol.symbol))
    }

    fn amend_order(&self, command: &AmendOrderCommand) -> Result<PrivateRestRequestSpec> {
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
        _command: &CountdownCancelAllCommand,
    ) -> Result<PrivateRestRequestSpec> {
        anyhow::bail!("bitget mix futures countdown cancel-all is not supported by this adapter")
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
        let symbols = if subscription.symbols.is_empty() {
            vec![ExchangeSymbol::new(ExchangeId::Bitget, "default")]
        } else {
            subscription.symbols.clone()
        };
        let args = symbols
            .iter()
            .map(|symbol| {
                json!({
                    "instType": "USDT-FUTURES",
                    "channel": subscription.channel,
                    "instId": symbol.symbol,
                })
            })
            .collect::<Vec<_>>();
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
        let mut body = json!({
            "contract": command.exchange_symbol.symbol,
            "size": gate_signed_size(command.side, command.quantity)?,
            "tif": gate_tif(command.time_in_force, command.post_only),
            "text": gate_client_text(&command.client_order_id),
            "reduce_only": command.reduce_only,
        });
        if let Some(price) = command.price {
            set_str(&mut body, "price", number_string(price));
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Gate,
            PrivateRestMethod::Post,
            "/futures/usdt/orders",
        )
        .with_body(body))
    }

    fn cancel_order(&self, command: &CancelCommand) -> Result<PrivateRestRequestSpec> {
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
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Gate,
            PrivateRestMethod::Delete,
            "/futures/usdt/orders",
        );
        if let Some(symbol) = &command.exchange_symbol {
            spec = spec.with_query("contract", &symbol.symbol);
        }
        Ok(spec)
    }

    fn cancel_batch_orders(&self, _command: &CancelBatchCommand) -> Result<PrivateRestRequestSpec> {
        anyhow::bail!(
            "gate native cancel-batch is not wired; adapter falls back to per-order cancel"
        )
    }

    fn get_order(&self, query: &OrderQuery) -> Result<PrivateRestRequestSpec> {
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
            spec = spec.with_query("contract", &symbol.symbol);
        }
        Ok(spec)
    }

    fn get_fills(&self, query: &FillQuery) -> Result<PrivateRestRequestSpec> {
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
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Gate,
            PrivateRestMethod::Get,
            format!("/futures/usdt/contracts/{}", symbol.symbol),
        ))
    }

    fn get_symbol_account_config(&self, symbol: &ExchangeSymbol) -> Result<PrivateRestRequestSpec> {
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Gate,
            PrivateRestMethod::Get,
            format!("/futures/usdt/positions/{}", symbol.symbol),
        ))
    }

    fn amend_order(&self, command: &AmendOrderCommand) -> Result<PrivateRestRequestSpec> {
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
        anyhow::bail!(
            "gate futures adapter does not support position mode changes; requested {:?}",
            command.mode
        )
    }

    fn set_countdown_cancel_all(
        &self,
        command: &CountdownCancelAllCommand,
    ) -> Result<PrivateRestRequestSpec> {
        let mut body = json!({
            "timeout": command.timeout_secs,
        });
        if let Some(symbol) = &command.exchange_symbol {
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

    fn cancel_order(&self, command: &CancelCommand) -> Result<PrivateRestRequestSpec> {
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
        if command.exchange_order_id.is_none() && command.client_order_id.is_none() {
            anyhow::bail!("bybit cancel requires orderId or orderLinkId");
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Bybit,
            PrivateRestMethod::Post,
            "/v5/order/cancel",
        )
        .with_body(body))
    }

    fn cancel_all_orders(&self, command: &CancelAllCommand) -> Result<PrivateRestRequestSpec> {
        let mut body = json!({"category": "linear"});
        if let Some(symbol) = &command.exchange_symbol {
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
            spec = spec.with_query("symbol", &symbol.symbol);
        }
        Ok(spec)
    }

    fn get_fills(&self, query: &FillQuery) -> Result<PrivateRestRequestSpec> {
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Bybit,
            PrivateRestMethod::Get,
            "/v5/execution/list",
        )
        .with_query("category", "linear");
        if let Some(symbol) = &query.exchange_symbol {
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
        if command.exchange_order_id.is_none() && command.client_order_id.is_none() {
            anyhow::bail!("bybit amend requires orderId or orderLinkId");
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Bybit,
            PrivateRestMethod::Post,
            "/v5/order/amend",
        )
        .with_body(body))
    }

    fn set_leverage(&self, command: &LeverageCommand) -> Result<PrivateRestRequestSpec> {
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
        let mut body = json!({
            "timeWindow": command.timeout_secs,
        });
        if let Some(symbol) = &command.exchange_symbol {
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

    fn cancel_order(&self, command: &CancelCommand) -> Result<PrivateRestRequestSpec> {
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
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Mexc,
            PrivateRestMethod::Post,
            "/api/v1/private/order/cancel_all",
        );
        if let Some(symbol) = &command.exchange_symbol {
            spec = spec.with_query("symbol", &symbol.symbol);
        }
        Ok(spec)
    }

    fn cancel_batch_orders(&self, command: &CancelBatchCommand) -> Result<PrivateRestRequestSpec> {
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
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Mexc,
            PrivateRestMethod::Get,
            format!("/api/v1/private/order/list/open_orders/{}", symbol.symbol),
        ))
    }

    fn get_fills(&self, query: &FillQuery) -> Result<PrivateRestRequestSpec> {
        let symbol = query
            .exchange_symbol
            .as_ref()
            .ok_or_else(|| anyhow!("mexc fills require symbol"))?;
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
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Mexc,
            PrivateRestMethod::Get,
            "/api/v1/private/account/tiered_fee_rate",
        )
        .with_query("symbol", &symbol.symbol))
    }

    fn get_symbol_account_config(&self, symbol: &ExchangeSymbol) -> Result<PrivateRestRequestSpec> {
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Mexc,
            PrivateRestMethod::Get,
            "/api/v1/private/position/leverage",
        )
        .with_query("symbol", &symbol.symbol))
    }

    fn amend_order(&self, command: &AmendOrderCommand) -> Result<PrivateRestRequestSpec> {
        let order_id = command
            .exchange_order_id
            .as_ref()
            .ok_or_else(|| anyhow!("mexc amend requires exchange order id"))?;
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
        _command: &CountdownCancelAllCommand,
    ) -> Result<PrivateRestRequestSpec> {
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

    fn cancel_order(&self, command: &CancelCommand) -> Result<PrivateRestRequestSpec> {
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
        let mut body = json!({});
        if let Some(symbol) = &command.exchange_symbol {
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
        let first = command
            .orders
            .first()
            .ok_or_else(|| anyhow!("htx batch cancel requires at least one order"))?;
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
            set_str(&mut body, "contract_code", &symbol.symbol);
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Htx,
            PrivateRestMethod::Post,
            "/linear-swap-api/v1/swap_cross_openorders",
        )
        .with_body(body))
    }

    fn get_fills(&self, query: &FillQuery) -> Result<PrivateRestRequestSpec> {
        let mut body = json!({"page_index": 1, "page_size": query.limit.unwrap_or(50)});
        if let Some(symbol) = &query.exchange_symbol {
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
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Htx,
            PrivateRestMethod::Post,
            "/linear-swap-api/v1/swap_fee",
        )
        .with_body(json!({"contract_code": symbol.symbol})))
    }

    fn get_symbol_account_config(&self, symbol: &ExchangeSymbol) -> Result<PrivateRestRequestSpec> {
        self.get_positions(Some(symbol))
    }

    fn amend_order(&self, _command: &AmendOrderCommand) -> Result<PrivateRestRequestSpec> {
        anyhow::bail!("htx linear swap does not expose a verified amend-order endpoint")
    }

    fn set_leverage(&self, command: &LeverageCommand) -> Result<PrivateRestRequestSpec> {
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
        anyhow::bail!(
            "htx position mode is account/product-specific and is not changed by this adapter; requested {:?}",
            command.mode
        )
    }

    fn set_countdown_cancel_all(
        &self,
        _command: &CountdownCancelAllCommand,
    ) -> Result<PrivateRestRequestSpec> {
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
    let query = request.query_string();
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
            ExchangeId::Bitget => {
                classify_bitget_rest_error(code.as_deref().unwrap_or_default(), &message)
            }
            ExchangeId::Gate => {
                classify_gate_rest_error(code.as_deref().unwrap_or_default(), &message)
            }
            ExchangeId::Bybit | ExchangeId::Mexc | ExchangeId::Htx => {
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
    match exchange {
        ExchangeId::Bitget => {
            let side = if position_mode.is_hedge() {
                bitget_position_side(command.position_side, command.order_side())
            } else {
                bitget_side(command.order_side())
            };
            let mut body = json!({
                "productType": "USDT-FUTURES",
                "symbol": command.exchange_symbol.symbol,
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
        other => anyhow::bail!("{other} private perp close_position is not supported"),
    }
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
                .or_else(|| map.get("orders"))
                .or_else(|| map.get("positions"))
                .or_else(|| map.get("assets"))
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

fn response_string(value: &Value, keys: &[&str]) -> Option<String> {
    let data = response_data(value);
    str_field(&data, keys).or_else(|| str_field(value, keys))
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

fn parse_cancel_batch_acks(
    exchange: ExchangeId,
    value: &Value,
    acknowledged_at: DateTime<Utc>,
) -> Vec<CancelAck> {
    response_items(value)
        .into_iter()
        .filter_map(|item| {
            let exchange_order_id = str_field(&item, &["orderId", "id"]);
            let client_order_id = str_field(&item, &["clientOid", "clientOrderId", "text"]);
            if exchange_order_id.is_none() && client_order_id.is_none() {
                return None;
            }
            Some(CancelAck {
                exchange: exchange.clone(),
                client_order_id,
                exchange_order_id,
                accepted: true,
                status: OrderCommandStatus::Cancelled,
                message: response_string(&item, &["msg", "message"]),
                acknowledged_at,
            })
        })
        .collect()
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
    let data = response_data(value);
    let item = match data {
        Value::Array(items) => items
            .iter()
            .cloned()
            .find(|item| symbol_matches(item, fallback_symbol))
            .or_else(|| items.first().cloned())?,
        Value::Object(_) => data,
        _ => return None,
    };
    let symbol = str_field(&item, &["symbol", "instId", "contract", "name"])
        .unwrap_or_else(|| fallback_symbol.symbol.clone());
    let maker = f64_field(
        &item,
        &[
            "makerFeeRate",
            "maker_fee_rate",
            "maker",
            "makerFee",
            "makerFeeRateE4",
            "open_maker_fee",
        ],
    )?;
    let taker = f64_field(
        &item,
        &[
            "takerFeeRate",
            "taker_fee_rate",
            "taker",
            "takerFee",
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
    let data = response_data(value);
    let item = match data {
        Value::Array(items) => items
            .iter()
            .cloned()
            .find(|item| symbol_matches(item, fallback_symbol))
            .or_else(|| items.first().cloned())?,
        Value::Object(_) => data,
        _ => return None,
    };
    let symbol = str_field(&item, &["symbol", "instId", "contract", "name"])
        .unwrap_or_else(|| fallback_symbol.symbol.clone());
    let position_mode = str_field(&item, &["posMode", "pos_mode", "positionMode"])
        .as_deref()
        .map(parse_position_mode_text)
        .or(Some(configured_position_mode));
    Some(SymbolAccountConfig {
        exchange: exchange.clone(),
        canonical_symbol: canonical(&exchange, &symbol),
        exchange_symbol: ExchangeSymbol::new(exchange, symbol),
        position_mode,
        margin_mode: str_field(&item, &["marginMode", "margin_mode", "marginModeName"])
            .as_deref()
            .map(parse_margin_mode_text),
        leverage: f64_field(
            &item,
            &["leverage", "lever", "crossedLeverage", "isolatedLeverage"],
        )
        .map(|leverage| leverage.round().max(0.0) as u32),
        max_leverage: f64_field(&item, &["maxLever", "maxLeverage", "max_leverage"])
            .map(|leverage| leverage.round().max(0.0) as u32),
        updated_at: received_at,
    })
}

fn symbol_matches(item: &Value, symbol: &ExchangeSymbol) -> bool {
    str_field(item, &["symbol", "instId", "contract", "name"])
        .is_some_and(|value| value.eq_ignore_ascii_case(&symbol.symbol))
}

fn parse_rest_order_with_gate_contract_size(
    exchange: ExchangeId,
    fallback_symbol: &ExchangeSymbol,
    value: &Value,
    received_at: DateTime<Utc>,
    gate_contract_size: Option<f64>,
) -> Option<OrderState> {
    let data = response_data(value);
    let item = match data {
        Value::Array(items) => items.into_iter().next()?,
        Value::Object(_) => data,
        _ => return None,
    };
    let event = match exchange {
        ExchangeId::Bitget => parse_bitget_order_or_fill(&item, received_at),
        ExchangeId::Bybit => parse_bybit_order_or_fill(&item, received_at),
        ExchangeId::Mexc => parse_mexc_order_or_fill(&item, received_at),
        ExchangeId::Htx => parse_htx_order_or_fill(&item, received_at),
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

fn parse_bitget_order_or_fill(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    if item.get("tradeId").or_else(|| item.get("fillId")).is_some() {
        return parse_bitget_fill(item, received_at);
    }
    let symbol = str_field(item, &["symbol", "instId"])?;
    let exchange_symbol = ExchangeSymbol::new(ExchangeId::Bitget, symbol.clone());
    let status = bitget_status(str_field(item, &["status", "state"]).as_deref());
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
            fee: f64_field(item, &["fee", "feeDetail"]),
            fee_asset: str_field(item, &["feeCcy", "feeCoin"]),
            fee_rate: None,
            realized_pnl: f64_field(item, &["profit", "realizedPnl"]),
            reduce_only: Some(trade_side.eq_ignore_ascii_case("close")),
            filled_at: millis_field(item, &["tradeTime", "cTime", "uTime"], received_at),
            received_at,
        },
        received_at,
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
            order_type: mexc_parse_order_type(i64_field(item, &["type"])),
            quantity: f64_field(item, &["vol", "qty"]).unwrap_or_default(),
            price: f64_field(item, &["price"]),
            filled_quantity: f64_field(item, &["dealVol", "filledQty"]).unwrap_or_default(),
            average_fill_price: f64_field(item, &["dealAvgPrice", "avgPrice"]),
            time_in_force: TimeInForce::Gtc,
            reduce_only: mexc_side_is_close(i64_field(item, &["side"]).unwrap_or_default()),
            status: mexc_status(
                i64_field(item, &["state"]).or_else(|| i64_field(item, &["status"])),
            ),
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
            exchange_order_id: str_field(item, &["order_id"]),
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
            updated_at: millis_field(item, &["created_at", "update_time"], received_at),
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
            status: gate_status(str_field(item, &["status", "finish_as"]).as_deref(), left),
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
        "hedge" | "hedge_mode" | "long_short"
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
        "limit" => OrderType::Limit,
        _ => OrderType::Market,
    }
}

fn parse_time_in_force(tif: Option<&str>) -> TimeInForce {
    match tif.unwrap_or_default().to_ascii_lowercase().as_str() {
        "ioc" | "immediateorcancel" => TimeInForce::Ioc,
        "fok" | "fillorkill" => TimeInForce::Fok,
        "postonly" | "post_only" => TimeInForce::PostOnly,
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

fn mexc_status(status: Option<i64>) -> OrderCommandStatus {
    match status {
        Some(1 | 2) => OrderCommandStatus::Accepted,
        Some(3) => OrderCommandStatus::PartiallyFilled,
        Some(4) => OrderCommandStatus::Filled,
        Some(5) => OrderCommandStatus::Cancelled,
        Some(6) => OrderCommandStatus::Rejected,
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
        seen: Arc<Mutex<Vec<PrivateRestRequestSpec>>>,
    }

    impl MockTransport {
        fn new(response: Value) -> Self {
            Self {
                response,
                seen: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl PrivateRestTransport for MockTransport {
        async fn execute(&self, request: PrivateRestRequestSpec) -> Result<Value> {
            self.seen.lock().unwrap().push(request);
            Ok(self.response.clone())
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

    #[test]
    fn bitget_should_build_place_order_spec() {
        let spec = BitgetPrivatePerpProtocol
            .place_order(&command(ExchangeId::Bitget, "BTCUSDT"), PositionMode::Hedge)
            .unwrap();

        assert_eq!(spec.method, PrivateRestMethod::Post);
        assert_eq!(spec.path, "/api/v2/mix/order/place-order");
        let body = spec.body.unwrap();
        assert_eq!(body["productType"], "USDT-FUTURES");
        assert_eq!(body["force"], "post_only");
        assert_eq!(body["tradeSide"], "open");
        assert_eq!(
            body["clientOid"],
            "crossarb-live-small-bundleabcdef-maker-1"
        );
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
        assert_eq!(body["tradeSide"], "close");
        assert!(body.get("reduceOnly").is_none());
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
        assert_eq!(body["reduce_only"], true);
        assert_eq!(
            body["text"].as_str().unwrap(),
            "t-crossarb-live-small-dc1f310f"
        );
        assert!(body["text"].as_str().unwrap().len() <= 30);
    }

    #[test]
    fn gate_client_order_id_should_be_stable_for_lookup_paths() {
        let client_id = "crossarb-live-small-bundleabcdef-maker-1";
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
            "/futures/usdt/orders/t-crossarb-live-small-dc1f310f"
        );
        assert_eq!(
            gate_client_text(client_id),
            "t-crossarb-live-small-dc1f310f"
        );
        assert_eq!(
            gate_client_text_to_local("t-crossarb-live-small-dc1f310f"),
            "crossarb-live-small-dc1f310f"
        );
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
    fn gate_position_mode_change_should_be_unsupported() {
        let err = GatePrivatePerpProtocol
            .set_position_mode(&PositionModeCommand {
                exchange: ExchangeId::Gate,
                mode: PositionMode::OneWay,
                requested_at: Utc::now(),
            })
            .unwrap_err();

        assert!(err.to_string().contains("does not support position mode"));
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

        let open_orders = MexcPrivatePerpProtocol
            .get_open_orders(Some(&ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT")))
            .unwrap();
        assert_eq!(
            open_orders.path,
            "/api/v1/private/order/list/open_orders/BTC_USDT"
        );

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

        let symbol = ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT");
        let fee = GatePrivatePerpProtocol.get_trade_fee(&symbol).unwrap();
        assert_eq!(fee.path, "/futures/usdt/contracts/BTC_USDT");
        let config = GatePrivatePerpProtocol
            .get_symbol_account_config(&symbol)
            .unwrap();
        assert_eq!(config.path, "/futures/usdt/positions/BTC_USDT");
    }

    #[test]
    fn gate_should_build_amend_and_countdown_cancel_specs() {
        let amend = GatePrivatePerpProtocol
            .amend_order(&AmendOrderCommand {
                exchange: ExchangeId::Gate,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
                client_order_id: Some("crossarb-live-small-bundleabcdef-maker-1".to_string()),
                exchange_order_id: None,
                new_client_order_id: Some("crossarb-live-small-bundleabcdef-maker-2".to_string()),
                new_quantity: None,
                new_price: Some(65_001.5),
                requested_at: Utc::now(),
            })
            .unwrap();
        assert_eq!(amend.method, PrivateRestMethod::Patch);
        assert_eq!(
            amend.path,
            "/futures/usdt/orders/t-crossarb-live-small-dc1f310f"
        );
        assert_eq!(
            amend.query.get("contract").map(String::as_str),
            Some("BTC_USDT")
        );
        let body = amend.body.unwrap();
        assert_eq!(body["price"], "65001.5");
        assert_eq!(body["amend_text"], "t-crossarb-live-small-acafe971");

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

        let config = parse_symbol_account_config(
            ExchangeId::Bitget,
            &ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
            &json!({"data":[{"symbol":"BTCUSDT","posMode":"hedge_mode","marginMode":"crossed","leverage":"3","maxLever":"125"}]}),
            now,
            PositionMode::OneWay,
        )
        .unwrap();
        assert_eq!(config.position_mode, Some(PositionMode::Hedge));
        assert_eq!(config.margin_mode, Some(MarginMode::Cross));
        assert_eq!(config.leverage, Some(3));
        assert_eq!(config.max_leverage, Some(125));
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
    fn gate_private_ws_endpoint_should_authenticate_every_subscription() {
        let endpoint = build_private_ws_endpoint(
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
        assert!(bybit.login_message.unwrap().contains("\"op\":\"auth\""));
        assert_eq!(bybit.subscribe_messages.len(), 4);
        assert!(bybit.subscribe_messages[0].contains("\"order\""));

        let mexc = build_private_ws_endpoint(
            PrivatePerpExchange::Mexc,
            auth.clone(),
            &[ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT")],
            1_700_000_000,
        )
        .unwrap();
        assert_eq!(mexc.exchange, ExchangeId::Mexc);
        assert!(mexc.login_message.unwrap().contains("\"method\":\"login\""));
        assert!(mexc.subscribe_messages[0].contains("sub.personal.order"));

        let htx = build_private_ws_endpoint(
            PrivatePerpExchange::Htx,
            auth,
            &[ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT")],
            1_700_000_000,
        )
        .unwrap();
        assert_eq!(htx.exchange, ExchangeId::Htx);
        assert!(htx.login_message.unwrap().contains("\"op\":\"auth\""));
        assert!(htx.subscribe_messages[0].contains("orders_cross"));
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
        let raw = r#"{
          "arg":{"channel":"orders","instType":"USDT-FUTURES","instId":"BTCUSDT"},
          "data":[{
            "symbol":"BTCUSDT","orderId":"1","clientOid":"c1","side":"buy",
            "tradeSide":"open","orderType":"limit","size":"0.01","price":"65000",
            "accBaseVolume":"0.005","status":"partially_filled","uTime":"1700000000000"
          }]
        }"#;

        let events = BitgetPrivatePerpProtocol
            .parse_private_ws_message(raw, now)
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
    }

    #[test]
    fn gate_should_parse_fill_event() {
        let now = Utc::now();
        let raw = r#"{
          "channel":"futures.usertrades","event":"update",
          "result":[{"id":"t1","order_id":"o1","contract":"BTC_USDT","size":"-2","price":"65000","fee":"0.1","fee_currency":"USDT","role":"taker","create_time_ms":"1700000000000"}]
        }"#;

        let events = GatePrivatePerpProtocol
            .parse_private_ws_message(raw, now)
            .unwrap();

        assert_eq!(events.len(), 1);
        match &events[0].kind {
            PrivateEventKind::Fill(fill) => {
                assert_eq!(fill.exchange, ExchangeId::Gate);
                assert_eq!(fill.side, OrderSide::Sell);
                assert_eq!(fill.position_side, PositionSide::Short);
                assert_eq!(fill.liquidity, FillLiquidity::Taker);
                assert_eq!(fill.quantity, 2.0);
            }
            other => panic!("expected fill event, got {other:?}"),
        }
    }

    #[test]
    fn bybit_should_parse_private_events() {
        let now = Utc::now();
        let raw = r#"{
          "topic":"order",
          "data":[{
            "symbol":"BTCUSDT","orderId":"o1","orderLinkId":"c1","side":"Buy",
            "orderType":"Limit","qty":"0.01","price":"65000","cumExecQty":"0.005",
            "orderStatus":"PartiallyFilled","positionIdx":1,"updatedTime":"1700000000000"
          }]
        }"#;

        let events = BybitPrivatePerpProtocol
            .parse_private_ws_message(raw, now)
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
    }

    #[test]
    fn mexc_should_parse_private_events() {
        let now = Utc::now();
        let raw = r#"{
          "channel":"push.personal.order",
          "data":{"symbol":"BTC_USDT","orderId":"o1","externalOid":"c1","side":3,
            "type":1,"vol":"2","price":"65000","dealVol":"1","state":3,
            "updateTime":1700000000000}
        }"#;

        let events = MexcPrivatePerpProtocol
            .parse_private_ws_message(raw, now)
            .unwrap();

        assert_eq!(events.len(), 1);
        match &events[0].kind {
            PrivateEventKind::Order(order) => {
                assert_eq!(order.exchange, ExchangeId::Mexc);
                assert_eq!(order.side, OrderSide::Sell);
                assert_eq!(order.position_side, PositionSide::Short);
                assert_eq!(order.status, OrderCommandStatus::PartiallyFilled);
            }
            other => panic!("expected order event, got {other:?}"),
        }
    }

    #[test]
    fn htx_should_parse_private_events() {
        let now = Utc::now();
        let raw = r#"{
          "op":"notify",
          "topic":"orders_cross.*",
          "data":{"contract_code":"BTC-USDT","order_id":"o1","client_order_id":"123",
            "direction":"sell","offset":"open","volume":"2","price":"65000",
            "trade_volume":"1","status":4,"created_at":1700000000000}
        }"#;

        let events = HtxPrivatePerpProtocol
            .parse_private_ws_message(raw, now)
            .unwrap();

        assert_eq!(events.len(), 1);
        match &events[0].kind {
            PrivateEventKind::Order(order) => {
                assert_eq!(order.exchange, ExchangeId::Htx);
                assert_eq!(order.side, OrderSide::Sell);
                assert_eq!(order.position_side, PositionSide::Short);
                assert_eq!(order.status, OrderCommandStatus::PartiallyFilled);
            }
            other => panic!("expected order event, got {other:?}"),
        }
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
        assert_eq!(body["size"], -123.0);
        assert_eq!(body["price"], "65000");
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

        assert!(ack.accepted);
        assert_eq!(ack.timeout_secs, 30);
        assert_eq!(ack.trigger_time.unwrap().timestamp(), 1_700_000_030);
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
