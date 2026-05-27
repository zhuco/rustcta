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
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256, Sha512};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Duration as TokioDuration};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::core::types::{Fee, MarketType, OrderStatus as CoreOrderStatus};
use crate::execution::{
    CancelAck, CancelCommand, ClosePositionAck, ClosePositionCommand, ExchangeBalance,
    ExchangeErrorClass, ExchangePosition, FillEvent, FillLiquidity, FillQuery, LeverageAck,
    LeverageCommand, OrderAck, OrderCommand, OrderCommandStatus, OrderQuery, OrderSide, OrderState,
    OrderType, PositionMode, PositionModeAck, PositionModeCommand, PositionSide, PrivateErrorEvent,
    PrivateEvent, PrivateEventKind, TimeInForce, TradingAdapter, TradingCapabilities,
};
use crate::market::{canonical_from_exchange_symbol, CanonicalSymbol, ExchangeId, ExchangeSymbol};

type HmacSha256 = Hmac<Sha256>;
type HmacSha512 = Hmac<Sha512>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PrivatePerpExchange {
    Bitget,
    Gate,
}

impl PrivatePerpExchange {
    pub fn exchange_id(self) -> ExchangeId {
        match self {
            Self::Bitget => ExchangeId::Bitget,
            Self::Gate => ExchangeId::Gate,
        }
    }

    pub fn rest_base_url(self) -> &'static str {
        match self {
            Self::Bitget => "https://api.bitget.com",
            Self::Gate => "https://api.gateio.ws/api/v4",
        }
    }

    pub fn private_ws_url(self) -> &'static str {
        match self {
            Self::Bitget => "wss://ws.bitget.com/v2/ws/private",
            Self::Gate => "wss://fx-ws.gateio.ws/v4/ws/usdt",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PrivateRestMethod {
    Get,
    Post,
    Delete,
}

impl PrivateRestMethod {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
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
    fn get_order(&self, query: &OrderQuery) -> Result<PrivateRestRequestSpec>;
    fn get_open_orders(&self, symbol: Option<&ExchangeSymbol>) -> Result<PrivateRestRequestSpec>;
    fn get_fills(
        &self,
        symbol: Option<&ExchangeSymbol>,
        order_id: Option<&str>,
    ) -> Result<PrivateRestRequestSpec>;
    fn get_positions(&self, symbol: Option<&ExchangeSymbol>) -> Result<PrivateRestRequestSpec>;
    fn get_balances(&self) -> Result<PrivateRestRequestSpec>;
    fn set_leverage(&self, command: &LeverageCommand) -> Result<PrivateRestRequestSpec>;
    fn set_position_mode(&self, command: &PositionModeCommand) -> Result<PrivateRestRequestSpec>;

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
}

impl From<PrivateWsAuth> for PrivateRestAuth {
    fn from(value: PrivateWsAuth) -> Self {
        Self {
            api_key: value.api_key,
            api_secret: value.api_secret,
            passphrase: value.passphrase,
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
            PrivatePerpExchange::Gate => gate_rest_headers(&self.auth, request, timestamp),
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
            PrivatePerpExchange::Bitget => Utc::now().timestamp_millis(),
            PrivatePerpExchange::Gate => Utc::now().timestamp(),
        };
        let mut builder = match request.method {
            PrivateRestMethod::Get => self.client.get(self.url(&request)),
            PrivateRestMethod::Post => self.client.post(self.url(&request)),
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
            anyhow::bail!(
                "{} private REST {} {} failed: http={} body={}",
                request.exchange,
                request.method.as_str(),
                request.path,
                status.as_u16(),
                value
            );
        }
        normalize_private_rest_response(request.exchange, value)
    }
}

#[derive(Clone)]
pub struct PrivatePerpTradingAdapter<P, T> {
    exchange: ExchangeId,
    protocol: P,
    transport: T,
    position_mode: PositionMode,
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
            position_mode: PositionMode::OneWay,
        }
    }

    pub fn with_position_mode(mut self, position_mode: PositionMode) -> Self {
        self.position_mode = position_mode;
        self
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
        let value = self
            .transport
            .execute(self.protocol.place_order(&command, self.position_mode)?)
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

    async fn get_order(&self, query: OrderQuery) -> Result<OrderState> {
        let value = self
            .transport
            .execute(self.protocol.get_order(&query)?)
            .await?;
        parse_rest_order(self.exchange(), &query.exchange_symbol, &value, Utc::now())
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
                parse_rest_order(exchange.clone(), &symbol, &item, Utc::now())
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
                ExchangeId::Gate => parse_gate_position(&item, Utc::now()).and_then(event_position),
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
                ExchangeId::Gate => parse_gate_balance(&item, Utc::now()).and_then(event_balance),
                _ => None,
            })
            .collect())
    }

    async fn get_fills(&self, query: FillQuery) -> Result<Vec<FillEvent>> {
        let value = self
            .transport
            .execute(self.protocol.get_fills(
                query.exchange_symbol.as_ref(),
                query.exchange_order_id.as_deref(),
            )?)
            .await?;
        let exchange = self.exchange();
        Ok(response_items(&value)
            .into_iter()
            .filter_map(|item| match exchange {
                ExchangeId::Bitget => parse_bitget_fill(&item, Utc::now()).and_then(event_fill),
                ExchangeId::Gate => parse_gate_fill(&item, Utc::now()).and_then(event_fill),
                _ => None,
            })
            .filter(|fill| {
                query
                    .client_order_id
                    .as_ref()
                    .map(|id| fill.client_order_id.as_deref() == Some(id.as_str()))
                    .unwrap_or(true)
            })
            .collect())
    }

    async fn set_leverage(&self, command: LeverageCommand) -> Result<LeverageAck> {
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
        let value = self
            .transport
            .execute(self.protocol.set_position_mode(&command)?)
            .await?;
        Ok(PositionModeAck {
            exchange: command.exchange,
            mode: command.mode,
            accepted: true,
            message: response_string(&value, &["msg", "message"]),
            acknowledged_at: Utc::now(),
        })
    }

    async fn close_position(&self, command: ClosePositionCommand) -> Result<ClosePositionAck> {
        let value = self
            .transport
            .execute(close_position_spec(
                self.exchange(),
                &command,
                self.position_mode,
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
}

pub fn private_perp_trading_capabilities(exchange: ExchangeId) -> TradingCapabilities {
    TradingCapabilities {
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: true,
        supports_ioc: true,
        supports_fok: true,
        supports_reduce_only: true,
        supports_hedge_mode: matches!(exchange, ExchangeId::Bitget),
        supports_client_order_id: true,
        supports_leverage: true,
        supports_position_mode_change: matches!(exchange, ExchangeId::Bitget),
        supports_close_position: true,
    }
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
    match exchange {
        PrivatePerpExchange::Bitget => Ok(Arc::new(
            PrivatePerpTradingAdapter::new(
                BitgetPrivatePerpProtocol,
                ReqwestPrivateRestTransport::new(exchange, auth)?,
            )
            .with_position_mode(position_mode),
        )),
        PrivatePerpExchange::Gate => Ok(Arc::new(
            PrivatePerpTradingAdapter::new(
                GatePrivatePerpProtocol,
                ReqwestPrivateRestTransport::new(exchange, auth)?,
            )
            .with_position_mode(position_mode),
        )),
    }
}

pub fn build_private_ws_endpoint(
    exchange: PrivatePerpExchange,
    auth: PrivateWsAuth,
    symbols: &[ExchangeSymbol],
    timestamp: i64,
) -> Result<PrivateWsEndpoint> {
    match exchange {
        PrivatePerpExchange::Bitget => {
            build_private_ws_endpoint_for(BitgetPrivatePerpProtocol, auth, symbols, timestamp)
        }
        PrivatePerpExchange::Gate => {
            build_private_ws_endpoint_for(GatePrivatePerpProtocol, auth, symbols, timestamp)
        }
    }
}

fn build_private_ws_endpoint_for<P>(
    protocol: P,
    auth: PrivateWsAuth,
    symbols: &[ExchangeSymbol],
    timestamp: i64,
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
    };

    let login_message = match exchange {
        PrivatePerpExchange::Bitget => Some(protocol.ws_login(&auth, timestamp)?.message),
        PrivatePerpExchange::Gate => None,
    };

    Ok(PrivateWsEndpoint {
        exchange: exchange.exchange_id(),
        url: exchange.private_ws_url().to_string(),
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
    loop {
        if tx.is_closed() {
            return Ok(());
        }

        let result = match exchange {
            PrivatePerpExchange::Bitget => {
                run_private_ws_protocol(
                    BitgetPrivatePerpProtocol,
                    auth.clone(),
                    symbols.clone(),
                    config,
                    tx.clone(),
                )
                .await
            }
            PrivatePerpExchange::Gate => {
                run_private_ws_protocol(
                    GatePrivatePerpProtocol,
                    auth.clone(),
                    symbols.clone(),
                    config,
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
                Some(exchange.private_ws_url().to_string()),
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
    let exchange = protocol.exchange();
    let timestamp = Utc::now().timestamp();
    let mut endpoint = build_private_ws_endpoint(exchange, auth, &symbols, timestamp)?;
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
                write.send(Message::Ping(Vec::new())).await?;
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
                        match String::from_utf8(raw) {
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

#[derive(Debug, Clone, Copy, Default)]
pub struct BitgetPrivatePerpProtocol;

#[derive(Debug, Clone, Copy, Default)]
pub struct GatePrivatePerpProtocol;

impl PrivatePerpProtocol for BitgetPrivatePerpProtocol {
    fn exchange(&self) -> PrivatePerpExchange {
        PrivatePerpExchange::Bitget
    }

    fn place_order(
        &self,
        command: &OrderCommand,
        position_mode: PositionMode,
    ) -> Result<PrivateRestRequestSpec> {
        let mut body = json!({
            "productType": "USDT-FUTURES",
            "symbol": command.exchange_symbol.symbol,
            "marginCoin": "USDT",
            "size": number_string(command.quantity),
            "side": bitget_side(command.side),
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
        }
        if command.reduce_only {
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

    fn get_order(&self, query: &OrderQuery) -> Result<PrivateRestRequestSpec> {
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

    fn get_fills(
        &self,
        symbol: Option<&ExchangeSymbol>,
        order_id: Option<&str>,
    ) -> Result<PrivateRestRequestSpec> {
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Bitget,
            PrivateRestMethod::Get,
            "/api/v2/mix/order/fills",
        )
        .with_query("productType", "USDT-FUTURES");
        if let Some(symbol) = symbol {
            spec = spec.with_query("symbol", &symbol.symbol);
        }
        if let Some(order_id) = order_id {
            spec = spec.with_query("orderId", order_id);
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
            "size": gate_signed_size(command.side, command.quantity),
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
        let order_id = command
            .exchange_order_id
            .as_deref()
            .or(command.client_order_id.as_deref())
            .ok_or_else(|| anyhow!("gate cancel requires exchange_order_id or client_order_id"))?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Gate,
            PrivateRestMethod::Delete,
            format!("/futures/usdt/orders/{order_id}"),
        )
        .with_query("contract", &command.exchange_symbol.symbol))
    }

    fn get_order(&self, query: &OrderQuery) -> Result<PrivateRestRequestSpec> {
        let order_id = query
            .exchange_order_id
            .as_deref()
            .or(query.client_order_id.as_deref())
            .ok_or_else(|| {
                anyhow!("gate order query requires exchange_order_id or client_order_id")
            })?;
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Gate,
            PrivateRestMethod::Get,
            format!("/futures/usdt/orders/{order_id}"),
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

    fn get_fills(
        &self,
        symbol: Option<&ExchangeSymbol>,
        order_id: Option<&str>,
    ) -> Result<PrivateRestRequestSpec> {
        let mut spec = PrivateRestRequestSpec::new(
            ExchangeId::Gate,
            PrivateRestMethod::Get,
            "/futures/usdt/my_trades",
        );
        if let Some(symbol) = symbol {
            spec = spec.with_query("contract", &symbol.symbol);
        }
        if let Some(order_id) = order_id {
            spec = spec.with_query("order", order_id);
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
        if command.mode.is_hedge() {
            anyhow::bail!("gate futures adapter is net-position only in the current contract");
        }
        Ok(PrivateRestRequestSpec::new(
            ExchangeId::Gate,
            PrivateRestMethod::Get,
            "/futures/usdt/positions",
        ))
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
    ]))
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

fn normalize_private_rest_response(exchange: ExchangeId, value: Value) -> Result<Value> {
    match exchange {
        ExchangeId::Bitget => {
            let code = value.get("code").and_then(Value::as_str).unwrap_or("00000");
            if code != "00000" {
                let msg = value
                    .get("msg")
                    .or_else(|| value.get("message"))
                    .and_then(Value::as_str)
                    .unwrap_or("bitget private REST error");
                anyhow::bail!("bitget private REST error code={code} msg={msg}");
            }
            Ok(value)
        }
        ExchangeId::Gate => {
            if let Some(label) = value.get("label").and_then(Value::as_str) {
                let message = value
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("gate private REST error");
                anyhow::bail!("gate private REST error label={label} message={message}");
            }
            Ok(value)
        }
        _ => Ok(value),
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
            let mut body = json!({
                "productType": "USDT-FUTURES",
                "symbol": command.exchange_symbol.symbol,
                "marginCoin": "USDT",
                "size": number_string(command.quantity),
                "side": bitget_side(command.order_side()),
                "orderType": bitget_order_type(command.order_type),
                "clientOid": command.client_order_id,
                "reduceOnly": "yes",
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
                "size": gate_signed_size(command.order_side(), command.quantity),
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
        other => anyhow::bail!("{other} private perp close_position is not supported"),
    }
}

fn response_items(value: &Value) -> Vec<Value> {
    match response_data(value) {
        Value::Array(items) => items,
        Value::Object(map) => {
            if let Some(Value::Array(items)) = map.get("list").or_else(|| map.get("orders")) {
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

fn parse_rest_order(
    exchange: ExchangeId,
    fallback_symbol: &ExchangeSymbol,
    value: &Value,
    received_at: DateTime<Utc>,
) -> Option<OrderState> {
    let data = response_data(value);
    let item = match data {
        Value::Array(items) => items.into_iter().next()?,
        Value::Object(_) => data,
        _ => return None,
    };
    let event = match exchange {
        ExchangeId::Bitget => parse_bitget_order_or_fill(&item, received_at),
        ExchangeId::Gate => parse_gate_order_or_fill(&item, received_at),
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
    let value: Value = serde_json::from_str(raw)?;
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

fn parse_gate_order_or_fill(item: &Value, received_at: DateTime<Utc>) -> Option<PrivateEvent> {
    if item.get("trade_id").or_else(|| item.get("id")).is_some() && item.get("order_id").is_some() {
        return parse_gate_fill(item, received_at);
    }
    let symbol = str_field(item, &["contract"])?;
    let signed_size = f64_field(item, &["size"]).unwrap_or_default();
    let left = f64_field(item, &["left"]).unwrap_or_default().abs();
    let quantity = signed_size.abs();
    Some(PrivateEvent::order(
        OrderState {
            exchange: ExchangeId::Gate,
            canonical_symbol: canonical(&ExchangeId::Gate, &symbol),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, symbol),
            client_order_id: str_field(item, &["text"]),
            exchange_order_id: str_field(item, &["id"]),
            side: if signed_size < 0.0 {
                OrderSide::Sell
            } else {
                OrderSide::Buy
            },
            position_side: if signed_size < 0.0 {
                PositionSide::Short
            } else {
                PositionSide::Long
            },
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
    let symbol = str_field(item, &["contract"])?;
    let signed_size = f64_field(item, &["size"]).unwrap_or_default();
    let price = f64_field(item, &["price"]).unwrap_or_default();
    let quantity = signed_size.abs();
    Some(PrivateEvent::fill(
        FillEvent {
            exchange: ExchangeId::Gate,
            canonical_symbol: canonical(&ExchangeId::Gate, &symbol),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, symbol),
            trade_id: str_field(item, &["trade_id", "id"])
                .unwrap_or_else(|| format!("gate-fill-{}", received_at.timestamp_millis())),
            client_order_id: str_field(item, &["text"]),
            exchange_order_id: str_field(item, &["order_id", "order"]),
            side: if signed_size < 0.0 {
                OrderSide::Sell
            } else {
                OrderSide::Buy
            },
            position_side: if signed_size < 0.0 {
                PositionSide::Short
            } else {
                PositionSide::Long
            },
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
    let symbol = str_field(item, &["contract"])?;
    let signed_size = f64_field(item, &["size"]).unwrap_or_default();
    Some(PrivateEvent::position(
        ExchangePosition {
            exchange: ExchangeId::Gate,
            canonical_symbol: canonical(&ExchangeId::Gate, &symbol),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, symbol),
            position_side: if signed_size < 0.0 {
                PositionSide::Short
            } else {
                PositionSide::Long
            },
            quantity: signed_size.abs(),
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

fn gate_signed_size(side: OrderSide, quantity: f64) -> f64 {
    match side {
        OrderSide::Buy => quantity.abs(),
        OrderSide::Sell => -quantity.abs(),
    }
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
    if client_order_id.starts_with("t-") {
        client_order_id.to_string()
    } else {
        format!("t-{client_order_id}")
    }
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

fn liquidity(role: Option<&str>) -> FillLiquidity {
    match role.unwrap_or_default().to_ascii_lowercase().as_str() {
        "maker" | "m" => FillLiquidity::Maker,
        "taker" | "t" => FillLiquidity::Taker,
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
    fn gate_should_build_signed_size_place_order_spec() {
        let mut cmd = command(ExchangeId::Gate, "BTC_USDT");
        cmd.side = OrderSide::Sell;
        cmd.reduce_only = true;

        let spec = GatePrivatePerpProtocol
            .place_order(&cmd, PositionMode::OneWay)
            .unwrap();
        let body = spec.body.unwrap();

        assert_eq!(spec.path, "/futures/usdt/orders");
        assert_eq!(body["contract"], "BTC_USDT");
        assert_eq!(body["size"], -0.01);
        assert_eq!(body["tif"], "poc");
        assert_eq!(body["reduce_only"], true);
        assert!(body["text"].as_str().unwrap().starts_with("t-crossarb-"));
    }

    #[test]
    fn private_ws_login_shapes_should_be_stable() {
        let auth = PrivateWsAuth {
            api_key: "key".to_string(),
            api_secret: "secret".to_string(),
            passphrase: Some("pass".to_string()),
            account_id: Some("20011".to_string()),
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
            },
            PositionMode::OneWay,
        )
        .unwrap();
        assert_eq!(gate.exchange(), ExchangeId::Gate);
        assert!(gate.capabilities().supports_leverage);
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
    fn gate_private_ws_endpoint_should_authenticate_every_subscription() {
        let endpoint = build_private_ws_endpoint(
            PrivatePerpExchange::Gate,
            PrivateWsAuth {
                api_key: "key".to_string(),
                api_secret: "secret".to_string(),
                passphrase: None,
                account_id: Some("20011".to_string()),
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
    fn gate_private_ws_subscription_should_require_account_id() {
        let err = build_private_ws_endpoint(
            PrivatePerpExchange::Gate,
            PrivateWsAuth {
                api_key: "key".to_string(),
                api_secret: "secret".to_string(),
                passphrase: None,
                account_id: None,
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

    #[test]
    fn reqwest_transport_should_build_bitget_headers() {
        let transport = ReqwestPrivateRestTransport::new(
            PrivatePerpExchange::Bitget,
            PrivateRestAuth {
                api_key: "key".to_string(),
                api_secret: "secret".to_string(),
                passphrase: Some("pass".to_string()),
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
        assert!(headers.get("ACCESS-SIGN").is_some_and(|v| !v.is_empty()));
        assert_eq!(
            headers.get("ACCESS-TIMESTAMP").map(String::as_str),
            Some("1700000000000")
        );
    }

    #[test]
    fn reqwest_transport_should_build_gate_headers_with_api_v4_sign_path() {
        let transport = ReqwestPrivateRestTransport::new(
            PrivatePerpExchange::Gate,
            PrivateRestAuth {
                api_key: "key".to_string(),
                api_secret: "secret".to_string(),
                passphrase: None,
            },
        )
        .unwrap();
        let spec = GatePrivatePerpProtocol
            .get_open_orders(Some(&ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT")))
            .unwrap();

        let headers = transport.signed_headers(&spec, 1_700_000_000).unwrap();

        assert_eq!(headers.get("KEY").map(String::as_str), Some("key"));
        assert_eq!(
            headers.get("Timestamp").map(String::as_str),
            Some("1700000000")
        );
        assert!(headers.get("SIGN").is_some_and(|v| !v.is_empty()));
    }

    #[test]
    fn private_rest_response_should_reject_error_envelopes() {
        let bitget = normalize_private_rest_response(
            ExchangeId::Bitget,
            json!({"code":"40010","msg":"bad request"}),
        )
        .unwrap_err();
        assert!(bitget.to_string().contains("40010"));

        let gate = normalize_private_rest_response(
            ExchangeId::Gate,
            json!({"label":"INVALID_KEY","message":"bad key"}),
        )
        .unwrap_err();
        assert!(gate.to_string().contains("INVALID_KEY"));
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
