use std::collections::HashMap;
use std::time::Duration as StdDuration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::time::{interval, sleep, timeout};
use tokio_tungstenite::tungstenite::Message;

use crate::core::error::ExchangeError as CoreExchangeError;
use crate::core::ws_connect::connect_async;
use crate::exchanges::client_order_id::{generate_client_order_id, validate_client_order_id};
use crate::exchanges::unified::{
    validate_order_lookup_id, validate_orderbook_depth, AssetBalance, BalanceSnapshot,
    CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse,
    ExchangeClient, ExchangeClientCapabilities, ExchangeClientError, ExchangeClientResult,
    ExchangeError, ExchangeErrorClass, FeeRate, FeeRateSource, MarketType, OrderBookLevel,
    OrderBookSnapshot, OrderRequest, OrderResponse, OrderSide, OrderStatus, OrderType,
    PositionSide, SymbolRule, SymbolStatus, TimeInForce, TradeFill, UserStreamEvent,
};
use crate::utils::SignatureHelper;

const DEFAULT_REST_BASE_URL: &str = "https://api.toobit.com";
const DEFAULT_WS_URL: &str = "wss://stream.toobit.com/quote/ws/v1";
const DEFAULT_RECV_WINDOW_MS: u64 = 5_000;
const DEFAULT_STALE_BOOK_MS: u64 = 10_000;
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_DEPTH: u16 = 5;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToobitConfig {
    pub api_key: String,
    pub api_secret: String,
    #[serde(default = "default_rest_base_url")]
    pub base_url: String,
    #[serde(default = "default_ws_url")]
    pub websocket_url: String,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default = "default_recv_window_ms")]
    pub recv_window: u64,
    #[serde(default = "default_stale_book_ms")]
    pub stale_book_ms: u64,
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    #[serde(default = "default_depth")]
    pub orderbook_depth: u16,
    #[serde(default)]
    pub enabled_symbols: Vec<String>,
    #[serde(default)]
    pub fee_override: Option<FeeOverride>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct FeeOverride {
    pub maker_fee_rate: f64,
    pub taker_fee_rate: f64,
}

impl Default for ToobitConfig {
    fn default() -> Self {
        Self {
            api_key: std::env::var("TOOBIT_API_KEY").unwrap_or_default(),
            api_secret: std::env::var("TOOBIT_API_SECRET").unwrap_or_default(),
            base_url: default_rest_base_url(),
            websocket_url: default_ws_url(),
            dry_run: true,
            recv_window: DEFAULT_RECV_WINDOW_MS,
            stale_book_ms: DEFAULT_STALE_BOOK_MS,
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            orderbook_depth: DEFAULT_DEPTH,
            enabled_symbols: Vec::new(),
            fee_override: None,
        }
    }
}

pub type ToobitSpotConfig = ToobitConfig;
pub type ToobitPerpConfig = ToobitConfig;

#[derive(Clone)]
pub struct ToobitSpotClient {
    config: ToobitConfig,
    http: reqwest::Client,
}

#[derive(Clone)]
pub struct ToobitPerpClient {
    config: ToobitConfig,
    http: reqwest::Client,
}

impl ToobitSpotClient {
    pub fn new(config: ToobitConfig) -> Self {
        Self {
            config,
            http: crate::core::http2_fix::shared_http_client(),
        }
    }

    pub fn generate_client_order_id() -> String {
        generate_client_order_id("toobit", MarketType::Spot, "spot").into_string()
    }

    async fn send_public_request(
        &self,
        endpoint: &str,
        params: HashMap<String, String>,
    ) -> ExchangeClientResult<Value> {
        send_public_request(&self.http, &self.config, endpoint, params).await
    }

    async fn send_signed_request(
        &self,
        method: Method,
        endpoint: &str,
        params: HashMap<String, String>,
    ) -> ExchangeClientResult<Value> {
        send_signed_request(&self.http, &self.config, method, endpoint, params).await
    }

    fn dry_run_order_response(&self, request: &OrderRequest, symbol: &str) -> OrderResponse {
        dry_run_order_response("toobit", MarketType::Spot, request, symbol)
    }
}

impl ToobitPerpClient {
    pub fn new(config: ToobitConfig) -> Self {
        Self {
            config,
            http: crate::core::http2_fix::shared_http_client(),
        }
    }

    pub fn generate_client_order_id() -> String {
        generate_client_order_id("toobit", MarketType::Perpetual, "perp").into_string()
    }

    async fn send_public_request(
        &self,
        endpoint: &str,
        params: HashMap<String, String>,
    ) -> ExchangeClientResult<Value> {
        send_public_request(&self.http, &self.config, endpoint, params).await
    }

    async fn send_signed_request(
        &self,
        method: Method,
        endpoint: &str,
        params: HashMap<String, String>,
    ) -> ExchangeClientResult<Value> {
        send_signed_request(&self.http, &self.config, method, endpoint, params).await
    }

    fn dry_run_order_response(&self, request: &OrderRequest, symbol: &str) -> OrderResponse {
        dry_run_order_response("toobit", MarketType::Perpetual, request, symbol)
    }
}

#[async_trait]
impl ExchangeClient for ToobitSpotClient {
    fn market_type(&self) -> MarketType {
        MarketType::Spot
    }

    fn exchange_name(&self) -> &str {
        "toobit"
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let mut capabilities = ExchangeClientCapabilities::spot(self.exchange_name());
        capabilities.supports_quote_market_order = false;
        capabilities.supports_amend_order = false;
        capabilities.supports_order_list = false;
        capabilities.supports_private_user_stream = true;
        capabilities.supports_public_ws = true;
        capabilities.supports_fee_api = self.config.fee_override.is_some();
        capabilities
    }

    fn normalize_symbol(&self, symbol: &str) -> ExchangeClientResult<String> {
        normalize_spot_symbol(symbol)
    }

    fn denormalize_symbol(&self, symbol: &str) -> ExchangeClientResult<String> {
        normalize_spot_symbol(symbol)
    }

    async fn load_symbol_rules(&self) -> ExchangeClientResult<Vec<SymbolRule>> {
        let value = self
            .send_public_request("/api/v1/exchangeInfo", HashMap::new())
            .await?;
        parse_symbol_rules(&value, MarketType::Spot)
    }

    async fn get_balances(&self) -> ExchangeClientResult<BalanceSnapshot> {
        let value = self
            .send_signed_request(Method::GET, "/api/v1/account", HashMap::new())
            .await?;
        parse_balance_snapshot(&value, MarketType::Spot)
    }

    async fn get_orderbook(
        &self,
        symbol: &str,
        depth: u16,
    ) -> ExchangeClientResult<OrderBookSnapshot> {
        validate_orderbook_depth(depth)?;
        let symbol = self.normalize_symbol(symbol)?;
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.clone());
        params.insert("limit".to_string(), normalize_depth(depth).to_string());
        let value = self.send_public_request("/quote/v1/depth", params).await?;
        parse_orderbook_snapshot(&value, &symbol, MarketType::Spot, self.config.stale_book_ms)
    }

    async fn place_order(&self, mut request: OrderRequest) -> ExchangeClientResult<OrderResponse> {
        request.validate()?;
        if request.market_type != MarketType::Spot {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: "ToobitSpotClient only supports MarketType::Spot".to_string(),
            });
        }
        let symbol = self.normalize_symbol(&request.symbol)?;
        request.symbol = symbol.clone();
        ensure_order_client_id(&mut request, "toobit")?;
        let params = toobit_order_params(&request, &symbol)?;
        if self.config.dry_run {
            return Ok(self.dry_run_order_response(&request, &symbol));
        }
        let value = self
            .send_signed_request(Method::POST, "/api/v1/spot/order", params)
            .await?;
        parse_order_response(&value, MarketType::Spot)
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeClientResult<CancelOrderResponse> {
        request.validate()?;
        if request.market_type != MarketType::Spot {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: "ToobitSpotClient only supports MarketType::Spot".to_string(),
            });
        }
        ensure_cancel_client_order_id(&request, "toobit")?;
        let symbol = self.normalize_symbol(&request.symbol)?;
        if self.config.dry_run {
            return Ok(cancel_response(
                MarketType::Spot,
                &symbol,
                request.order_id(),
                request.client_order_id(),
            ));
        }
        let mut params = HashMap::new();
        if let Some(order_id) = request.order_id() {
            params.insert("orderId".to_string(), order_id.to_string());
        }
        if let Some(client_order_id) = request.client_order_id() {
            params.insert("clientOrderId".to_string(), client_order_id.to_string());
        }
        let value = self
            .send_signed_request(Method::DELETE, "/api/v1/spot/order", params)
            .await?;
        Ok(cancel_response(
            MarketType::Spot,
            &symbol,
            value_as_string(value.get("orderId"))
                .as_deref()
                .or(request.order_id()),
            value_as_string(value.get("clientOrderId"))
                .as_deref()
                .or(request.client_order_id()),
        ))
    }

    async fn cancel_all_orders(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeClientResult<CancelAllOrdersResponse> {
        request.validate_for_market_type(MarketType::Spot)?;
        let symbol = request
            .symbol
            .as_deref()
            .map(|symbol| self.normalize_symbol(symbol))
            .transpose()?;
        if self.config.dry_run {
            return Ok(CancelAllOrdersResponse {
                exchange: "toobit".to_string(),
                market_type: MarketType::Spot,
                symbol,
                cancelled_orders: 0,
                cancelled_at: Utc::now(),
            });
        }
        let mut params = HashMap::new();
        if let Some(symbol) = &symbol {
            params.insert("symbol".to_string(), symbol.clone());
        }
        let value = self
            .send_signed_request(Method::DELETE, "/api/v1/spot/openOrders", params)
            .await?;
        Ok(CancelAllOrdersResponse {
            exchange: "toobit".to_string(),
            market_type: MarketType::Spot,
            symbol,
            cancelled_orders: usize::from(
                value.get("success").and_then(Value::as_bool) == Some(true),
            ),
            cancelled_at: Utc::now(),
        })
    }

    async fn get_order(&self, symbol: &str, order_id: &str) -> ExchangeClientResult<OrderResponse> {
        let symbol = self.normalize_symbol(symbol)?;
        let order_id = validate_order_lookup_id(order_id)?;
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol);
        params.insert("orderId".to_string(), order_id.to_string());
        let value = self
            .send_signed_request(Method::GET, "/api/v1/spot/order", params)
            .await?;
        parse_order_response(&value, MarketType::Spot)
    }

    async fn get_open_orders(
        &self,
        symbol: Option<&str>,
    ) -> ExchangeClientResult<Vec<OrderResponse>> {
        let mut params = HashMap::new();
        if let Some(symbol) = symbol {
            params.insert("symbol".to_string(), self.normalize_symbol(symbol)?);
        }
        let value = self
            .send_signed_request(Method::GET, "/api/v1/spot/openOrders", params)
            .await?;
        parse_order_list(&value, MarketType::Spot)
    }

    async fn get_fee_rate(&self, symbol: &str) -> ExchangeClientResult<FeeRate> {
        self.normalize_symbol(symbol)?;
        if let Some(fee) = self.config.fee_override {
            return Ok(FeeRate::new(
                fee.maker_fee_rate,
                fee.taker_fee_rate,
                FeeRateSource::ConfigOverride,
            ));
        }
        Err(ExchangeClientError::Unsupported(
            "Toobit Spot fee API has not been mapped; configure fee_override for now".to_string(),
        ))
    }

    async fn get_recent_fills(&self, symbol: &str) -> ExchangeClientResult<Vec<TradeFill>> {
        let symbol = self.normalize_symbol(symbol)?;
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol);
        let value = self
            .send_signed_request(Method::GET, "/api/v1/account/trades", params)
            .await?;
        parse_fills(&value, MarketType::Spot)
    }

    async fn subscribe_orderbook(
        &self,
        symbols: Vec<String>,
    ) -> ExchangeClientResult<mpsc::Receiver<OrderBookSnapshot>> {
        if symbols.is_empty() {
            return Err(ExchangeClientError::Validation {
                field: "symbols",
                reason: "at least one symbol is required".to_string(),
            });
        }
        let symbols = symbols
            .into_iter()
            .map(|symbol| self.normalize_symbol(&symbol))
            .collect::<ExchangeClientResult<Vec<_>>>()?;
        let client = self.clone();
        let (tx, rx) = mpsc::channel(1024);
        tokio::spawn(async move {
            client.run_orderbook_stream(symbols, tx).await;
        });
        Ok(rx)
    }

    async fn subscribe_user_stream(&self) -> ExchangeClientResult<mpsc::Receiver<UserStreamEvent>> {
        ensure_credentials(&self.config)?;
        let client = self.clone();
        let (tx, rx) = mpsc::channel(1024);
        tokio::spawn(async move {
            client.run_user_stream(tx).await;
        });
        Ok(rx)
    }
}

impl ToobitSpotClient {
    async fn create_listen_key(&self) -> ExchangeClientResult<String> {
        let value = self
            .send_signed_request(Method::POST, "/api/v1/userDataStream", HashMap::new())
            .await?;
        value
            .get("listenKey")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| parser_error("Toobit listenKey response missing listenKey", &value))
    }

    async fn keepalive_listen_key(&self, listen_key: &str) -> ExchangeClientResult<()> {
        let mut params = HashMap::new();
        params.insert("listenKey".to_string(), listen_key.to_string());
        self.send_signed_request(Method::PUT, "/api/v1/userDataStream", params)
            .await
            .map(|_| ())
    }

    async fn run_orderbook_stream(self, symbols: Vec<String>, tx: mpsc::Sender<OrderBookSnapshot>) {
        let reconnect_delay = StdDuration::from_millis(self.config.request_timeout_ms.min(30_000));
        let stale_after = StdDuration::from_millis(self.config.stale_book_ms.max(1_000));
        let subscribe = json!({
            "symbol": symbols.join(","),
            "topic": "depth",
            "event": "sub",
            "params": {"binary": false}
        })
        .to_string();
        loop {
            match connect_async(self.config.websocket_url.as_str()).await {
                Ok((mut ws, _)) => {
                    if let Err(error) = ws.send(Message::Text(subscribe.clone())).await {
                        log::warn!("Toobit Spot orderbook subscribe failed: {}", error);
                        sleep(reconnect_delay).await;
                        continue;
                    }
                    let mut heartbeat = interval(StdDuration::from_secs(120));
                    loop {
                        tokio::select! {
                            _ = heartbeat.tick() => {
                                let ping = json!({"ping": Utc::now().timestamp_millis()}).to_string();
                                if let Err(error) = ws.send(Message::Text(ping)).await {
                                    log::warn!("Toobit Spot orderbook ping failed: {}", error);
                                    break;
                                }
                            }
                            message = timeout(stale_after, ws.next()) => {
                                match message {
                                    Err(_) => {
                                        log::warn!("Toobit Spot orderbook stream stale for {:?}", stale_after);
                                        break;
                                    }
                                    Ok(Some(Ok(Message::Text(text)))) => {
                                        match parse_ws_orderbook_message(&text, self.config.stale_book_ms) {
                                            Ok(Some(book)) => {
                                                if tx.send(book).await.is_err() {
                                                    return;
                                                }
                                            }
                                            Ok(None) => {}
                                            Err(error) => log::warn!("Toobit Spot orderbook parse error: {}", error),
                                        }
                                    }
                                    Ok(Some(Ok(Message::Ping(payload)))) => {
                                        let _ = ws.send(Message::Pong(payload)).await;
                                    }
                                    Ok(Some(Ok(Message::Close(frame)))) => {
                                        log::warn!("Toobit Spot orderbook stream closed: {:?}", frame);
                                        break;
                                    }
                                    Ok(Some(Ok(_))) => {}
                                    Ok(Some(Err(error))) => {
                                        log::warn!("Toobit Spot orderbook websocket error: {}", error);
                                        break;
                                    }
                                    Ok(None) => break,
                                }
                            }
                        }
                    }
                }
                Err(error) => log::warn!("Toobit Spot orderbook connect error: {}", error),
            }
            sleep(reconnect_delay).await;
        }
    }

    async fn run_user_stream(self, tx: mpsc::Sender<UserStreamEvent>) {
        let reconnect_delay = StdDuration::from_millis(self.config.request_timeout_ms.min(30_000));
        loop {
            let listen_key = match self.create_listen_key().await {
                Ok(key) => key,
                Err(error) => {
                    log::warn!("Toobit Spot listenKey creation failed: {}", error);
                    sleep(reconnect_delay).await;
                    continue;
                }
            };
            let url = toobit_user_stream_url(&self.config.websocket_url, &listen_key);
            match connect_async(url.as_str()).await {
                Ok((mut ws, _)) => {
                    let mut keepalive = interval(StdDuration::from_secs(25 * 60));
                    loop {
                        tokio::select! {
                            _ = keepalive.tick() => {
                                if let Err(error) = self.keepalive_listen_key(&listen_key).await {
                                    log::warn!("Toobit Spot listenKey keepalive failed: {}", error);
                                    break;
                                }
                            }
                            message = ws.next() => {
                                match message {
                                    Some(Ok(Message::Text(text))) => {
                                        match parse_user_stream_message(&text) {
                                            Ok(events) => {
                                                for event in events {
                                                    if tx.send(event).await.is_err() {
                                                        return;
                                                    }
                                                }
                                            }
                                            Err(error) => log::warn!("Toobit Spot user stream parse error: {}", error),
                                        }
                                    }
                                    Some(Ok(Message::Close(frame))) => {
                                        let _ = tx.send(UserStreamEvent::Disconnected {
                                            reason: Some(format!("websocket close: {frame:?}")),
                                        }).await;
                                        break;
                                    }
                                    Some(Ok(_)) => {}
                                    Some(Err(error)) => {
                                        let _ = tx.send(UserStreamEvent::Disconnected {
                                            reason: Some(error.to_string()),
                                        }).await;
                                        break;
                                    }
                                    None => {
                                        let _ = tx.send(UserStreamEvent::Disconnected {
                                            reason: Some("stream ended".to_string()),
                                        }).await;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                Err(error) => {
                    let _ = tx
                        .send(UserStreamEvent::Disconnected {
                            reason: Some(error.to_string()),
                        })
                        .await;
                }
            }
            sleep(reconnect_delay).await;
        }
    }
}

#[async_trait]
impl ExchangeClient for ToobitPerpClient {
    fn market_type(&self) -> MarketType {
        MarketType::Perpetual
    }

    fn exchange_name(&self) -> &str {
        "toobit"
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        ExchangeClientCapabilities {
            exchange: self.exchange_name().to_string(),
            market_type: MarketType::Perpetual,
            supports_spot: false,
            supports_perpetual: true,
            supports_market_order: true,
            supports_limit_order: true,
            supports_post_only: true,
            supports_ioc: true,
            supports_fok: true,
            supports_cancel_order: true,
            supports_cancel_all_orders: true,
            supports_quote_market_order: false,
            supports_amend_order: false,
            supports_order_list: false,
            supports_query_order: true,
            supports_open_orders: true,
            supports_balances: true,
            supports_positions: false,
            supports_leverage: true,
            supports_funding_rate: false,
            supports_public_ws: false,
            supports_private_user_stream: false,
            supports_fee_api: true,
            order_book: crate::data::L2BookCapability::snapshot_only(None),
        }
    }

    fn normalize_symbol(&self, symbol: &str) -> ExchangeClientResult<String> {
        normalize_perp_symbol(symbol)
    }

    fn denormalize_symbol(&self, symbol: &str) -> ExchangeClientResult<String> {
        normalize_perp_symbol(symbol)
    }

    async fn load_symbol_rules(&self) -> ExchangeClientResult<Vec<SymbolRule>> {
        let value = self
            .send_public_request("/api/v1/exchangeInfo", HashMap::new())
            .await?;
        parse_symbol_rules(&value, MarketType::Perpetual)
    }

    async fn get_balances(&self) -> ExchangeClientResult<BalanceSnapshot> {
        let value = self
            .send_signed_request(Method::GET, "/api/v1/futures/balance", HashMap::new())
            .await?;
        parse_balance_snapshot(&value, MarketType::Perpetual)
    }

    async fn get_orderbook(
        &self,
        symbol: &str,
        depth: u16,
    ) -> ExchangeClientResult<OrderBookSnapshot> {
        validate_orderbook_depth(depth)?;
        let symbol = self.normalize_symbol(symbol)?;
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.clone());
        params.insert("limit".to_string(), normalize_depth(depth).to_string());
        let value = self.send_public_request("/quote/v1/depth", params).await?;
        parse_orderbook_snapshot(
            &value,
            &symbol,
            MarketType::Perpetual,
            self.config.stale_book_ms,
        )
    }

    async fn place_order(&self, mut request: OrderRequest) -> ExchangeClientResult<OrderResponse> {
        request.validate()?;
        if request.market_type != MarketType::Perpetual {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: "ToobitPerpClient only supports MarketType::Perpetual".to_string(),
            });
        }
        let symbol = self.normalize_symbol(&request.symbol)?;
        request.symbol = symbol.clone();
        if request.client_order_id.is_none() {
            request.client_order_id = Some(Self::generate_client_order_id());
        }
        if let Some(client_order_id) = &request.client_order_id {
            validate_client_order_id("toobit", MarketType::Perpetual, client_order_id).map_err(
                |error| ExchangeClientError::Validation {
                    field: "client_order_id",
                    reason: error.to_string(),
                },
            )?;
        }
        let params = toobit_perp_order_params(&request, &symbol)?;
        if self.config.dry_run {
            return Ok(self.dry_run_order_response(&request, &symbol));
        }
        let value = self
            .send_signed_request(Method::POST, "/api/v2/futures/order", params)
            .await?;
        parse_order_response(&value, MarketType::Perpetual)
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeClientResult<CancelOrderResponse> {
        request.validate()?;
        if request.market_type != MarketType::Perpetual {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: "ToobitPerpClient only supports MarketType::Perpetual".to_string(),
            });
        }
        ensure_cancel_client_order_id(&request, "toobit")?;
        let symbol = self.normalize_symbol(&request.symbol)?;
        if self.config.dry_run {
            return Ok(cancel_response(
                MarketType::Perpetual,
                &symbol,
                request.order_id(),
                request.client_order_id(),
            ));
        }
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.clone());
        if let Some(order_id) = request.order_id() {
            params.insert("orderId".to_string(), order_id.to_string());
        }
        if let Some(client_order_id) = request.client_order_id() {
            params.insert("origClientOrderId".to_string(), client_order_id.to_string());
        }
        let value = self
            .send_signed_request(Method::DELETE, "/api/v2/futures/order", params)
            .await?;
        Ok(cancel_response(
            MarketType::Perpetual,
            &symbol,
            value_as_string(data_value(&value).get("orderId"))
                .as_deref()
                .or(request.order_id()),
            value_as_string(data_value(&value).get("clientOrderId"))
                .as_deref()
                .or(request.client_order_id()),
        ))
    }

    async fn cancel_all_orders(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeClientResult<CancelAllOrdersResponse> {
        request.validate_for_market_type(MarketType::Perpetual)?;
        let symbol = self.normalize_symbol(request.validate_symbol_required()?)?;
        if self.config.dry_run {
            return Ok(CancelAllOrdersResponse {
                exchange: "toobit".to_string(),
                market_type: MarketType::Perpetual,
                symbol: Some(symbol),
                cancelled_orders: 0,
                cancelled_at: Utc::now(),
            });
        }
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.clone());
        let value = self
            .send_signed_request(Method::DELETE, "/api/v2/futures/batch-orders", params)
            .await?;
        Ok(CancelAllOrdersResponse {
            exchange: "toobit".to_string(),
            market_type: MarketType::Perpetual,
            symbol: Some(symbol),
            cancelled_orders: count_response_items(&value),
            cancelled_at: Utc::now(),
        })
    }

    async fn get_order(&self, symbol: &str, order_id: &str) -> ExchangeClientResult<OrderResponse> {
        let symbol = self.normalize_symbol(symbol)?;
        let order_id = validate_order_lookup_id(order_id)?;
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol);
        params.insert("orderId".to_string(), order_id.to_string());
        let value = self
            .send_signed_request(Method::GET, "/api/v2/futures/order", params)
            .await?;
        parse_order_response(&value, MarketType::Perpetual)
    }

    async fn get_open_orders(
        &self,
        symbol: Option<&str>,
    ) -> ExchangeClientResult<Vec<OrderResponse>> {
        if let Some(symbol) = symbol {
            self.normalize_symbol(symbol)?;
        }
        let mut params = HashMap::new();
        if let Some(symbol) = symbol {
            params.insert("symbol".to_string(), self.normalize_symbol(symbol)?);
        }
        let value = self
            .send_signed_request(Method::GET, "/api/v2/futures/open-orders", params)
            .await?;
        parse_order_list(&value, MarketType::Perpetual)
    }

    async fn get_fee_rate(&self, symbol: &str) -> ExchangeClientResult<FeeRate> {
        if let Some(fee) = self.config.fee_override {
            return Ok(FeeRate::new(
                fee.maker_fee_rate,
                fee.taker_fee_rate,
                FeeRateSource::ConfigOverride,
            ));
        }
        let symbol = self.normalize_symbol(symbol)?;
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol);
        let value = self
            .send_signed_request(Method::GET, "/api/v1/futures/commissionRate", params)
            .await?;
        parse_fee_rate(&value)
    }

    async fn get_recent_fills(&self, symbol: &str) -> ExchangeClientResult<Vec<TradeFill>> {
        let symbol = self.normalize_symbol(symbol)?;
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol);
        params.insert("limit".to_string(), "1000".to_string());
        let value = self
            .send_signed_request(Method::GET, "/api/v2/futures/user-trades", params)
            .await?;
        parse_fills(&value, MarketType::Perpetual)
    }
}

async fn send_public_request(
    http: &reqwest::Client,
    config: &ToobitConfig,
    endpoint: &str,
    params: HashMap<String, String>,
) -> ExchangeClientResult<Value> {
    let response = http
        .get(build_url(&config.base_url, endpoint, &params, None))
        .timeout(StdDuration::from_millis(config.request_timeout_ms))
        .send()
        .await
        .map_err(CoreExchangeError::from)?;
    parse_response(response).await
}

async fn send_signed_request(
    http: &reqwest::Client,
    config: &ToobitConfig,
    method: Method,
    endpoint: &str,
    mut params: HashMap<String, String>,
) -> ExchangeClientResult<Value> {
    ensure_credentials(config)?;
    params.insert(
        "recvWindow".to_string(),
        config.recv_window.min(60_000).to_string(),
    );
    params.insert(
        "timestamp".to_string(),
        Utc::now().timestamp_millis().to_string(),
    );
    let query = SignatureHelper::build_query_string(&params);
    let signature = SignatureHelper::hmac_sha256(&config.api_secret, &query);
    let response = http
        .request(
            method,
            build_url(&config.base_url, endpoint, &params, Some(&signature)),
        )
        .header("X-BB-APIKEY", &config.api_key)
        .timeout(StdDuration::from_millis(config.request_timeout_ms))
        .send()
        .await
        .map_err(CoreExchangeError::from)?;
    parse_response(response).await
}

fn ensure_credentials(config: &ToobitConfig) -> ExchangeClientResult<()> {
    if config.api_key.trim().is_empty() || config.api_secret.trim().is_empty() {
        return Err(ExchangeClientError::Classified(ExchangeError {
            exchange: "toobit".to_string(),
            class: ExchangeErrorClass::AuthenticationFailed,
            code: None,
            message: "TOOBIT_API_KEY and TOOBIT_API_SECRET are required".to_string(),
        }));
    }
    Ok(())
}

async fn parse_response(response: reqwest::Response) -> ExchangeClientResult<Value> {
    let status = response.status();
    let value: Value = response.json().await.map_err(CoreExchangeError::from)?;
    let code = value.get("code").and_then(value_as_i64);
    if !status.is_success() || code.is_some_and(|code| code != 0 && code != 200) {
        let message = value
            .get("msg")
            .or_else(|| value.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("Toobit request failed");
        return Err(ExchangeClientError::Classified(ExchangeError {
            exchange: "toobit".to_string(),
            class: classify_toobit_error(code, message, status.as_u16()),
            code: code.map(|code| code.to_string()),
            message: message.to_string(),
        }));
    }
    Ok(value)
}

pub fn classify_toobit_error(
    code: Option<i64>,
    message: &str,
    http_status: u16,
) -> ExchangeErrorClass {
    let lower = message.to_ascii_lowercase();
    match (code, http_status) {
        (_, 401 | 403) => ExchangeErrorClass::AuthenticationFailed,
        (_, 429) => ExchangeErrorClass::RateLimited,
        (_, 500..=599) => ExchangeErrorClass::ExchangeUnavailable,
        (Some(-2013), _) => ExchangeErrorClass::OrderNotFound,
        (Some(-2010), _) => ExchangeErrorClass::InsufficientBalance,
        (Some(-1013), _) => ExchangeErrorClass::MinNotionalViolation,
        (Some(-1021) | Some(-1022), _) => ExchangeErrorClass::AuthenticationFailed,
        _ if lower.contains("insufficient") => ExchangeErrorClass::InsufficientBalance,
        _ if lower.contains("not found") || lower.contains("unknown order") => {
            ExchangeErrorClass::OrderNotFound
        }
        _ if lower.contains("too many") || lower.contains("rate") => {
            ExchangeErrorClass::RateLimited
        }
        _ if lower.contains("precision") => ExchangeErrorClass::InvalidPrecision,
        _ if lower.contains("symbol") => ExchangeErrorClass::InvalidSymbol,
        _ => ExchangeErrorClass::Unknown,
    }
}

pub fn parse_symbol_rules(
    value: &Value,
    market_type: MarketType,
) -> ExchangeClientResult<Vec<SymbolRule>> {
    let key = match market_type {
        MarketType::Spot => "symbols",
        MarketType::Perpetual => "contracts",
    };
    let symbols = value
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| parser_error(&format!("exchangeInfo missing {key}"), value))?;
    symbols
        .iter()
        .filter(|item| {
            market_type == MarketType::Spot
                || !item
                    .get("inverse")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
        })
        .map(|item| parse_symbol_rule(item, market_type))
        .collect()
}

pub fn parse_symbol_rule(
    value: &Value,
    market_type: MarketType,
) -> ExchangeClientResult<SymbolRule> {
    let exchange_symbol = required_str(value, "symbol")?.to_ascii_uppercase();
    let base_asset = value
        .get("underlying")
        .and_then(Value::as_str)
        .or_else(|| value.get("baseAsset").and_then(Value::as_str))
        .unwrap_or("UNKNOWN")
        .to_ascii_uppercase()
        .replace("-SWAP-USDT", "");
    let quote_asset = required_str(value, "quoteAsset")
        .or_else(|_| required_str(value, "marginToken"))?
        .to_ascii_uppercase();
    let filters = value
        .get("filters")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let price_filter = find_filter(&filters, "PRICE_FILTER");
    let lot_filter = find_filter(&filters, "LOT_SIZE");
    let notional_filter = find_filter(&filters, "MIN_NOTIONAL");
    let tick_size = number_from_str(price_filter.and_then(|f| f.get("tickSize"))).unwrap_or(0.0);
    let step_size = number_from_str(lot_filter.and_then(|f| f.get("stepSize"))).unwrap_or(0.0);
    let min_quantity = number_from_str(lot_filter.and_then(|f| f.get("minQty"))).unwrap_or(0.0);
    let min_notional =
        number_from_str(notional_filter.and_then(|f| f.get("minNotional"))).unwrap_or(0.0);
    Ok(SymbolRule {
        exchange: "toobit".to_string(),
        market_type,
        internal_symbol: exchange_symbol.clone(),
        exchange_symbol,
        base_asset,
        quote_asset,
        price_precision: precision_from_step(tick_size),
        quantity_precision: precision_from_step(step_size),
        tick_size,
        step_size,
        min_quantity,
        min_notional,
        max_quantity: number_from_str(lot_filter.and_then(|f| f.get("maxQty"))),
        supported_order_types: vec![
            OrderType::Market,
            OrderType::Limit,
            OrderType::PostOnly,
            OrderType::IOC,
            OrderType::FOK,
        ],
        supported_time_in_force: vec![
            TimeInForce::GTC,
            TimeInForce::IOC,
            TimeInForce::FOK,
            TimeInForce::GTX,
        ],
        status: match value
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("UNKNOWN")
        {
            "TRADING" => SymbolStatus::Trading,
            "SUSPENDED" => SymbolStatus::Suspended,
            "DELISTED" => SymbolStatus::Delisted,
            _ => SymbolStatus::Unknown,
        },
        raw_metadata: Some(value.clone()),
    })
}

pub fn parse_balance_snapshot(
    value: &Value,
    market_type: MarketType,
) -> ExchangeClientResult<BalanceSnapshot> {
    let balances = data_value(value)
        .get("balances")
        .and_then(Value::as_array)
        .or_else(|| data_value(value).as_array())
        .ok_or_else(|| parser_error("account response missing balances", value))?;
    let balances = balances
        .iter()
        .filter_map(|item| {
            let asset = item
                .get("coin")
                .or_else(|| item.get("asset"))
                .or_else(|| item.get("marginCoin"))
                .and_then(Value::as_str)?;
            let available = number_from_str(item.get("free"))
                .or_else(|| number_from_str(item.get("availableBalance")))
                .or_else(|| number_from_str(item.get("available")))
                .unwrap_or(0.0);
            let locked = number_from_str(item.get("locked"))
                .or_else(|| number_from_str(item.get("frozen")))
                .unwrap_or(0.0);
            let total = number_from_str(item.get("total"))
                .or_else(|| number_from_str(item.get("balance")))
                .or_else(|| number_from_str(item.get("walletBalance")))
                .unwrap_or(available + locked);
            (total > 0.0 || available > 0.0 || locked > 0.0)
                .then(|| AssetBalance::new(asset, total, available, locked))
        })
        .collect();
    Ok(BalanceSnapshot {
        exchange: "toobit".to_string(),
        market_type,
        balances,
        timestamp: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    value: &Value,
    symbol: &str,
    market_type: MarketType,
    stale_book_ms: u64,
) -> ExchangeClientResult<OrderBookSnapshot> {
    let bids = value
        .get("bids")
        .or_else(|| value.get("b"))
        .and_then(Value::as_array)
        .ok_or_else(|| parser_error("orderbook missing bids", value))?;
    let asks = value
        .get("asks")
        .or_else(|| value.get("a"))
        .and_then(Value::as_array)
        .ok_or_else(|| parser_error("orderbook missing asks", value))?;
    let bids = parse_levels(bids)?;
    let asks = parse_levels(asks)?;
    let exchange_timestamp = value
        .get("t")
        .or_else(|| value.get("timestamp"))
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    let received_at = Utc::now();
    let latency_ms =
        exchange_timestamp.map(|ts| received_at.signed_duration_since(ts).num_milliseconds());
    Ok(OrderBookSnapshot {
        exchange: "toobit".to_string(),
        market_type,
        symbol: symbol.to_ascii_uppercase(),
        bids,
        asks,
        best_bid: None,
        best_ask: None,
        exchange_timestamp,
        received_at,
        latency_ms,
        sequence: value
            .get("lastUpdateId")
            .or_else(|| value.get("u"))
            .and_then(Value::as_u64),
        is_stale: latency_ms.is_some_and(|latency| latency > stale_book_ms as i64),
    }
    .with_best_levels())
}

pub fn parse_order_response(
    value: &Value,
    market_type: MarketType,
) -> ExchangeClientResult<OrderResponse> {
    let value = data_value(value);
    let symbol = value
        .get("symbol")
        .or_else(|| value.get("symbolName"))
        .and_then(Value::as_str)
        .unwrap_or("UNKNOWN")
        .to_ascii_uppercase();
    let quantity =
        number_from_str(value.get("origQty").or_else(|| value.get("quantity"))).unwrap_or(0.0);
    let filled_quantity = number_from_str(value.get("executedQty")).unwrap_or(0.0);
    Ok(OrderResponse {
        exchange: "toobit".to_string(),
        market_type,
        symbol,
        order_id: value_as_string(value.get("orderId"))
            .or_else(|| value_as_string(value.get("id")))
            .unwrap_or_else(|| "unknown".to_string()),
        client_order_id: value_as_string(value.get("clientOrderId")),
        side: value
            .get("side")
            .and_then(Value::as_str)
            .map(parse_side)
            .transpose()?
            .unwrap_or(OrderSide::Buy),
        position_side: match market_type {
            MarketType::Spot => PositionSide::None,
            MarketType::Perpetual => value
                .get("positionSide")
                .and_then(Value::as_str)
                .map(parse_position_side)
                .unwrap_or(PositionSide::Net),
        },
        order_type: parse_order_type(
            value.get("type").and_then(Value::as_str).unwrap_or("LIMIT"),
            value.get("timeInForce").and_then(Value::as_str),
        ),
        status: value
            .get("status")
            .and_then(Value::as_str)
            .map(map_order_status)
            .unwrap_or(OrderStatus::Unknown),
        price: number_from_str(value.get("price")).filter(|price| *price > 0.0),
        quantity,
        filled_quantity,
        average_price: average_price(value, filled_quantity),
        created_at: first_timestamp(value, &["transactTime", "time"]).unwrap_or_else(Utc::now),
        updated_at: first_timestamp(value, &["updateTime"]),
    })
}

pub fn parse_order_list(
    value: &Value,
    market_type: MarketType,
) -> ExchangeClientResult<Vec<OrderResponse>> {
    data_value(value)
        .as_array()
        .ok_or_else(|| parser_error("order list response is not an array", value))?
        .iter()
        .map(|item| parse_order_response(item, market_type))
        .collect()
}

pub fn parse_fills(value: &Value, market_type: MarketType) -> ExchangeClientResult<Vec<TradeFill>> {
    data_value(value)
        .as_array()
        .ok_or_else(|| parser_error("fills response is not an array", value))?
        .iter()
        .map(|item| parse_fill(item, market_type))
        .collect()
}

pub fn parse_fill(value: &Value, market_type: MarketType) -> ExchangeClientResult<TradeFill> {
    Ok(TradeFill {
        exchange: "toobit".to_string(),
        market_type,
        symbol: required_str(value, "symbol")?.to_ascii_uppercase(),
        trade_id: value_as_string(value.get("id"))
            .or_else(|| value_as_string(value.get("ticketId"))),
        order_id: value_as_string(value.get("orderId")),
        client_order_id: value_as_string(value.get("clientOrderId")),
        side: if value
            .get("isBuyer")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        },
        price: number_from_str(value.get("price")).unwrap_or(0.0),
        quantity: number_from_str(value.get("qty"))
            .or_else(|| number_from_str(value.get("quantity")))
            .unwrap_or(0.0),
        fee_asset: value_as_string(value.get("commissionAsset"))
            .or_else(|| value_as_string(value.get("feeCoinId"))),
        fee_amount: number_from_str(value.get("commission"))
            .or_else(|| number_from_str(value.get("feeAmount"))),
        liquidity: if value
            .get("isMaker")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            crate::exchanges::unified::LiquidityRole::Maker
        } else {
            crate::exchanges::unified::LiquidityRole::Taker
        },
        timestamp: first_timestamp(value, &["time"]).unwrap_or_else(Utc::now),
    })
}

fn parse_fee_rate(value: &Value) -> ExchangeClientResult<FeeRate> {
    let value = data_value(value);
    let maker = number_from_str(value.get("makerCommission"))
        .or_else(|| number_from_str(value.get("makerFeeRate")))
        .or_else(|| number_from_str(value.get("makerFee")))
        .or_else(|| number_from_str(value.get("maker")))
        .ok_or_else(|| parser_error("fee response missing maker rate", value))?;
    let taker = number_from_str(value.get("takerCommission"))
        .or_else(|| number_from_str(value.get("takerFeeRate")))
        .or_else(|| number_from_str(value.get("takerFee")))
        .or_else(|| number_from_str(value.get("taker")))
        .ok_or_else(|| parser_error("fee response missing taker rate", value))?;
    Ok(FeeRate::new(maker, taker, FeeRateSource::ExchangeApi))
}

pub fn parse_ws_orderbook_message(
    raw: &str,
    stale_book_ms: u64,
) -> ExchangeClientResult<Option<OrderBookSnapshot>> {
    let value: Value = serde_json::from_str(raw).map_err(CoreExchangeError::from)?;
    if value.get("pong").is_some() || value.get("ping").is_some() {
        return Ok(None);
    }
    if value.get("topic").and_then(Value::as_str) != Some("depth") {
        return Ok(None);
    }
    let item = value
        .get("data")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .ok_or_else(|| parser_error("Toobit depth websocket payload missing data", &value))?;
    let symbol = item
        .get("s")
        .or_else(|| item.get("symbol"))
        .or_else(|| value.get("symbol"))
        .and_then(Value::as_str)
        .ok_or_else(|| parser_error("Toobit depth websocket payload missing symbol", &value))?;
    parse_orderbook_snapshot(item, symbol, MarketType::Spot, stale_book_ms).map(Some)
}

pub fn parse_user_stream_message(raw: &str) -> ExchangeClientResult<Vec<UserStreamEvent>> {
    let value: Value = serde_json::from_str(raw).map_err(CoreExchangeError::from)?;
    let items = value
        .as_array()
        .map(|items| items.iter().collect::<Vec<_>>())
        .unwrap_or_else(|| vec![&value]);
    items
        .into_iter()
        .filter_map(|item| match item.get("e").and_then(Value::as_str) {
            Some("outboundAccountInfo") => Some(parse_account_update_event(item)),
            Some("executionReport") => Some(parse_execution_report_event(item)),
            Some("ticketInfo") => Some(parse_ticket_info_event(item)),
            _ => None,
        })
        .collect()
}

fn parse_account_update_event(value: &Value) -> ExchangeClientResult<UserStreamEvent> {
    let balances = value
        .get("B")
        .and_then(Value::as_array)
        .ok_or_else(|| parser_error("Toobit account update missing balances", value))?
        .iter()
        .filter_map(|item| {
            let asset = item.get("a").and_then(Value::as_str)?;
            let available = number_from_str(item.get("f")).unwrap_or(0.0);
            let locked = number_from_str(item.get("l")).unwrap_or(0.0);
            Some(AssetBalance::new(
                asset,
                available + locked,
                available,
                locked,
            ))
        })
        .collect();
    Ok(UserStreamEvent::Balance(BalanceSnapshot {
        exchange: "toobit".to_string(),
        market_type: MarketType::Spot,
        balances,
        timestamp: value
            .get("E")
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .unwrap_or_else(Utc::now),
    }))
}

fn parse_execution_report_event(value: &Value) -> ExchangeClientResult<UserStreamEvent> {
    let order = json!({
        "symbol": value.get("s").cloned().unwrap_or(Value::Null),
        "clientOrderId": value.get("c").cloned().unwrap_or(Value::Null),
        "side": value.get("S").cloned().unwrap_or(Value::Null),
        "type": value.get("o").cloned().unwrap_or(Value::Null),
        "timeInForce": value.get("f").cloned().unwrap_or(Value::Null),
        "origQty": value.get("q").cloned().unwrap_or(Value::Null),
        "price": value.get("p").cloned().unwrap_or(Value::Null),
        "status": value.get("X").cloned().unwrap_or(Value::Null),
        "orderId": value.get("i").cloned().unwrap_or(Value::Null),
        "executedQty": value.get("z").cloned().unwrap_or(Value::Null),
        "cumulativeQuoteQty": value.get("Z").cloned().unwrap_or(Value::Null),
        "time": value.get("O").cloned().unwrap_or(Value::Null),
        "updateTime": value.get("E").cloned().unwrap_or(Value::Null),
    });
    Ok(UserStreamEvent::Order(parse_order_response(
        &order,
        MarketType::Spot,
    )?))
}

fn parse_ticket_info_event(value: &Value) -> ExchangeClientResult<UserStreamEvent> {
    Ok(UserStreamEvent::Fill(TradeFill {
        exchange: "toobit".to_string(),
        market_type: MarketType::Spot,
        symbol: required_str(value, "s")?.to_ascii_uppercase(),
        trade_id: value_as_string(value.get("T")),
        order_id: value_as_string(value.get("o")),
        client_order_id: value_as_string(value.get("c")),
        side: value
            .get("S")
            .and_then(Value::as_str)
            .map(parse_side)
            .transpose()?
            .unwrap_or(OrderSide::Buy),
        price: number_from_str(value.get("p")).unwrap_or(0.0),
        quantity: number_from_str(value.get("q")).unwrap_or(0.0),
        fee_asset: None,
        fee_amount: None,
        liquidity: if value.get("m").and_then(Value::as_bool).unwrap_or(false) {
            crate::exchanges::unified::LiquidityRole::Maker
        } else {
            crate::exchanges::unified::LiquidityRole::Taker
        },
        timestamp: value
            .get("t")
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .unwrap_or_else(Utc::now),
    }))
}

fn toobit_order_params(
    request: &OrderRequest,
    symbol: &str,
) -> ExchangeClientResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    params.insert("symbol".to_string(), symbol.to_string());
    params.insert(
        "side".to_string(),
        match request.side {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        }
        .to_string(),
    );
    params.insert("type".to_string(), toobit_order_type(request).to_string());
    params.insert("quantity".to_string(), request.quantity.to_string());
    if let Some(price) = request.price {
        params.insert("price".to_string(), price.to_string());
    }
    if let Some(client_order_id) = &request.client_order_id {
        params.insert("newClientOrderId".to_string(), client_order_id.clone());
    }
    if let Some(tif) = request.time_in_force {
        params.insert("timeInForce".to_string(), tif_to_toobit(tif).to_string());
    }
    Ok(params)
}

fn toobit_perp_order_params(
    request: &OrderRequest,
    symbol: &str,
) -> ExchangeClientResult<HashMap<String, String>> {
    let mut params = toobit_order_params(request, symbol)?;
    params.insert(
        "positionSide".to_string(),
        match request.position_side {
            PositionSide::Long => "LONG",
            PositionSide::Short => "SHORT",
            PositionSide::Net | PositionSide::None => match request.side {
                OrderSide::Buy => "LONG",
                OrderSide::Sell => "SHORT",
            },
        }
        .to_string(),
    );
    if request.reduce_only {
        params.insert("reduceOnly".to_string(), "true".to_string());
    }
    Ok(params)
}

fn toobit_order_type(request: &OrderRequest) -> &'static str {
    match request.order_type {
        OrderType::Market => "MARKET",
        OrderType::Limit | OrderType::IOC | OrderType::FOK => "LIMIT",
        OrderType::PostOnly => "LIMIT_MAKER",
    }
}

fn tif_to_toobit(tif: TimeInForce) -> &'static str {
    match tif {
        TimeInForce::GTC => "GTC",
        TimeInForce::IOC => "IOC",
        TimeInForce::FOK => "FOK",
        TimeInForce::GTX => "POST_ONLY",
    }
}

fn ensure_order_client_id(request: &mut OrderRequest, exchange: &str) -> ExchangeClientResult<()> {
    if request.client_order_id.is_none() {
        request.client_order_id = Some(ToobitSpotClient::generate_client_order_id());
    }
    if let Some(client_order_id) = &request.client_order_id {
        validate_client_order_id(exchange, request.market_type, client_order_id).map_err(
            |error| ExchangeClientError::Validation {
                field: "client_order_id",
                reason: error.to_string(),
            },
        )?;
    }
    Ok(())
}

fn ensure_cancel_client_order_id(
    request: &CancelOrderRequest,
    exchange: &str,
) -> ExchangeClientResult<()> {
    if let Some(client_order_id) = request.client_order_id() {
        validate_client_order_id(exchange, request.market_type, client_order_id).map_err(
            |error| ExchangeClientError::Validation {
                field: "client_order_id",
                reason: error.to_string(),
            },
        )?;
    }
    Ok(())
}

fn dry_run_order_response(
    exchange: &str,
    market_type: MarketType,
    request: &OrderRequest,
    symbol: &str,
) -> OrderResponse {
    OrderResponse {
        exchange: exchange.to_string(),
        market_type,
        symbol: symbol.to_string(),
        order_id: format!("dry-{exchange}-{}", Utc::now().timestamp_millis()),
        client_order_id: request.client_order_id.clone(),
        side: request.side,
        position_side: request.position_side,
        order_type: request.order_type,
        status: OrderStatus::New,
        price: request.price,
        quantity: request.quantity,
        filled_quantity: 0.0,
        average_price: None,
        created_at: Utc::now(),
        updated_at: None,
    }
}

fn cancel_response(
    market_type: MarketType,
    symbol: &str,
    order_id: Option<&str>,
    client_order_id: Option<&str>,
) -> CancelOrderResponse {
    CancelOrderResponse {
        exchange: "toobit".to_string(),
        market_type,
        symbol: symbol.to_string(),
        order_id: order_id.map(str::to_string),
        client_order_id: client_order_id.map(str::to_string),
        status: OrderStatus::Cancelled,
        cancelled_at: Utc::now(),
    }
}

trait WithBestLevels {
    fn with_best_levels(self) -> Self;
}

impl WithBestLevels for OrderBookSnapshot {
    fn with_best_levels(mut self) -> Self {
        self.best_bid = self.bids.first().map(|level| level.price);
        self.best_ask = self.asks.first().map(|level| level.price);
        self
    }
}

fn build_url(
    base: &str,
    endpoint: &str,
    params: &HashMap<String, String>,
    signature: Option<&str>,
) -> String {
    let mut url = format!("{}{}", base.trim_end_matches('/'), endpoint);
    let query = SignatureHelper::build_query_string(params);
    if !query.is_empty() {
        url.push('?');
        url.push_str(&query);
    }
    if let Some(signature) = signature {
        url.push(if query.is_empty() { '?' } else { '&' });
        url.push_str("signature=");
        url.push_str(signature);
    }
    url
}

fn parse_levels(levels: &[Value]) -> ExchangeClientResult<Vec<OrderBookLevel>> {
    levels
        .iter()
        .map(|level| {
            let array = level
                .as_array()
                .ok_or_else(|| parser_error("invalid orderbook level", level))?;
            Ok(OrderBookLevel {
                price: number_from_str(array.first()).unwrap_or(0.0),
                quantity: number_from_str(array.get(1)).unwrap_or(0.0),
            })
        })
        .collect()
}

fn parse_side(value: &str) -> ExchangeClientResult<OrderSide> {
    match value.to_ascii_uppercase().as_str() {
        "BUY" | "BUY_OPEN" | "BUY_CLOSE" => Ok(OrderSide::Buy),
        "SELL" | "SELL_OPEN" | "SELL_CLOSE" => Ok(OrderSide::Sell),
        _ => Err(parser_error(
            "invalid side",
            &Value::String(value.to_string()),
        )),
    }
}

fn parse_order_type(order_type: &str, tif: Option<&str>) -> OrderType {
    match (
        order_type.to_ascii_uppercase().as_str(),
        tif.map(str::to_ascii_uppercase),
    ) {
        ("MARKET", _) => OrderType::Market,
        ("LIMIT_MAKER", _) => OrderType::PostOnly,
        ("LIMIT", Some(tif)) if tif == "IOC" => OrderType::IOC,
        ("LIMIT", Some(tif)) if tif == "FOK" => OrderType::FOK,
        ("LIMIT", Some(tif)) if tif == "POST_ONLY" => OrderType::PostOnly,
        _ => OrderType::Limit,
    }
}

fn map_order_status(value: &str) -> OrderStatus {
    match value.to_ascii_uppercase().as_str() {
        "PENDING_NEW" | "NEW" | "ORDER_NEW" => OrderStatus::New,
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "FILLED" | "ORDER_FILLED" => OrderStatus::Filled,
        "CANCELED" | "CANCELLED" | "ORDER_CANCELED" => OrderStatus::Cancelled,
        "REJECTED" | "ORDER_REJECTED" | "ORDER_FAILED" => OrderStatus::Rejected,
        "EXPIRED" => OrderStatus::Expired,
        _ => OrderStatus::Unknown,
    }
}

fn parse_position_side(value: &str) -> PositionSide {
    match value.to_ascii_uppercase().as_str() {
        "LONG" => PositionSide::Long,
        "SHORT" => PositionSide::Short,
        "BOTH" | "NET" => PositionSide::Net,
        _ => PositionSide::Net,
    }
}

fn average_price(value: &Value, filled_quantity: f64) -> Option<f64> {
    number_from_str(value.get("cumulativeQuoteQty"))
        .or_else(|| number_from_str(value.get("cummulativeQuoteQty")))
        .filter(|quote| filled_quantity > 0.0 && *quote > 0.0)
        .map(|quote| quote / filled_quantity)
}

fn normalize_spot_symbol(symbol: &str) -> ExchangeClientResult<String> {
    let normalized = symbol
        .trim()
        .replace(['-', '_', '/'], "")
        .to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(ExchangeClientError::Validation {
            field: "symbol",
            reason: "symbol must not be empty".to_string(),
        });
    }
    Ok(normalized)
}

fn normalize_perp_symbol(symbol: &str) -> ExchangeClientResult<String> {
    let normalized = symbol
        .trim()
        .replace('_', "-")
        .replace('/', "-")
        .to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(ExchangeClientError::Validation {
            field: "symbol",
            reason: "symbol must not be empty".to_string(),
        });
    }
    if normalized.contains("-SWAP-") {
        return Ok(normalized);
    }
    if let Some(base) = normalized.strip_suffix("USDT") {
        if !base.is_empty() {
            return Ok(format!("{base}-SWAP-USDT"));
        }
    }
    Ok(normalized)
}

fn normalize_depth(depth: u16) -> u16 {
    match depth {
        0..=5 => 5,
        6..=20 => 20,
        21..=100 => 100,
        _ => 200,
    }
}

fn required_str<'a>(value: &'a Value, field: &str) -> ExchangeClientResult<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| parser_error(&format!("missing field {field}"), value))
}

fn value_as_string(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

fn data_value(value: &Value) -> &Value {
    value.get("data").unwrap_or(value)
}

fn count_response_items(value: &Value) -> usize {
    let value = data_value(value);
    value
        .as_array()
        .map(Vec::len)
        .or_else(|| value.get("success").and_then(Value::as_array).map(Vec::len))
        .or_else(|| {
            value
                .get("successList")
                .and_then(Value::as_array)
                .map(Vec::len)
        })
        .or_else(|| {
            value
                .get("count")
                .and_then(Value::as_u64)
                .map(|count| count as usize)
        })
        .or_else(|| (value.get("success").and_then(Value::as_bool) == Some(true)).then_some(1))
        .unwrap_or(0)
}

fn number_from_str(value: Option<&Value>) -> Option<f64> {
    value.and_then(|value| match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    })
}

fn first_timestamp(value: &Value, keys: &[&str]) -> Option<DateTime<Utc>> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(value_as_i64))
        .and_then(DateTime::<Utc>::from_timestamp_millis)
}

fn find_filter<'a>(filters: &'a [Value], filter_type: &str) -> Option<&'a Value> {
    filters.iter().find(|filter| {
        filter
            .get("filterType")
            .and_then(Value::as_str)
            .is_some_and(|value| value.eq_ignore_ascii_case(filter_type))
    })
}

fn precision_from_step(step: f64) -> u32 {
    if step <= 0.0 {
        return 0;
    }
    format!("{step:.16}")
        .trim_end_matches('0')
        .split('.')
        .nth(1)
        .map(|fraction| fraction.len() as u32)
        .unwrap_or(0)
}

fn parser_error(message: &str, value: &Value) -> ExchangeClientError {
    ExchangeClientError::Classified(ExchangeError {
        exchange: "toobit".to_string(),
        class: ExchangeErrorClass::Unknown,
        code: None,
        message: format!("{message}: {value}"),
    })
}

fn default_rest_base_url() -> String {
    DEFAULT_REST_BASE_URL.to_string()
}
fn default_ws_url() -> String {
    DEFAULT_WS_URL.to_string()
}

fn toobit_user_stream_url(websocket_url: &str, listen_key: &str) -> String {
    let base = websocket_url
        .trim_end_matches('/')
        .strip_suffix("/quote/ws/v1")
        .unwrap_or_else(|| websocket_url.trim_end_matches('/'));
    format!("{base}/api/v1/ws/{listen_key}")
}
fn default_recv_window_ms() -> u64 {
    DEFAULT_RECV_WINDOW_MS
}
fn default_stale_book_ms() -> u64 {
    DEFAULT_STALE_BOOK_MS
}
fn default_request_timeout_ms() -> u64 {
    DEFAULT_REQUEST_TIMEOUT_MS
}
fn default_depth() -> u16 {
    DEFAULT_DEPTH
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    #[derive(Debug, Clone)]
    struct SeenRequest {
        method: String,
        path: String,
        query: HashMap<String, String>,
        headers: HashMap<String, String>,
    }

    async fn spawn_rest_server(responses: Vec<Value>) -> (String, Arc<Mutex<Vec<SeenRequest>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_requests = Arc::clone(&seen);
        let responses = Arc::new(Mutex::new(responses.into_iter()));
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let mut buffer = vec![0_u8; 8192];
                let bytes_read = stream.read(&mut buffer).await.unwrap();
                let request_text = String::from_utf8_lossy(&buffer[..bytes_read]).to_string();
                seen_requests
                    .lock()
                    .unwrap()
                    .push(parse_seen_request(&request_text));
                let body = responses
                    .lock()
                    .unwrap()
                    .next()
                    .unwrap_or_else(|| json!({}));
                let body_text = body.to_string();
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body_text.len(),
                    body_text
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        (format!("http://{address}"), seen)
    }

    fn parse_seen_request(request_text: &str) -> SeenRequest {
        let mut lines = request_text.lines();
        let request_line = lines.next().unwrap_or_default();
        let mut request_parts = request_line.split_whitespace();
        let method = request_parts.next().unwrap_or_default().to_string();
        let target = request_parts.next().unwrap_or_default();
        let (path, query_text) = target.split_once('?').unwrap_or((target, ""));
        let query = query_text
            .split('&')
            .filter(|pair| !pair.is_empty())
            .filter_map(|pair| {
                let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
                Some((key.to_string(), value.to_string()))
            })
            .collect();
        let headers = lines
            .take_while(|line| !line.trim().is_empty())
            .filter_map(|line| {
                let (key, value) = line.split_once(':')?;
                Some((key.to_ascii_lowercase(), value.trim().to_string()))
            })
            .collect();
        SeenRequest {
            method,
            path: path.to_string(),
            query,
            headers,
        }
    }

    fn assert_signed_request(request: &SeenRequest, method: &str, path: &str) {
        assert_eq!(request.method, method);
        assert_eq!(request.path, path);
        assert_eq!(
            request.headers.get("x-bb-apikey").map(String::as_str),
            Some("key")
        );
        assert!(request.query.get("timestamp").is_some());
        assert_eq!(
            request.query.get("recvWindow").map(String::as_str),
            Some("5000")
        );
        assert!(request.query.get("signature").is_some());
    }

    #[test]
    fn toobit_signing_should_match_hmac_sha256_query() {
        let query = "price=400&quantity=1&recvWindow=100000&side=SELL&symbol=BTCUSDT&timeInForce=GTC&timestamp=1668481902307&type=LIMIT";
        let expected = SignatureHelper::hmac_sha256("secret", query);
        assert_eq!(SignatureHelper::hmac_sha256("secret", query), expected);
    }

    #[test]
    fn toobit_symbol_rule_parsing_should_cover_spot_and_linear_contracts() {
        let value: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/exchanges/toobit/exchange_info.json"
        ))
        .unwrap();
        let spot = parse_symbol_rules(&value, MarketType::Spot).unwrap();
        assert_eq!(spot[0].internal_symbol, "ETHUSDT");
        assert_eq!(spot[0].base_asset, "ETH");
        assert_eq!(spot[0].tick_size, 0.01);
        assert_eq!(spot[0].step_size, 0.0001);
        assert_eq!(spot[0].min_notional, 10.0);

        let perp = parse_symbol_rules(&value, MarketType::Perpetual).unwrap();
        assert_eq!(perp.len(), 1);
        assert_eq!(perp[0].internal_symbol, "BTC-SWAP-USDT");
        assert_eq!(perp[0].base_asset, "BTC");
        assert_eq!(perp[0].quote_asset, "USDT");
    }

    #[test]
    fn toobit_orderbook_parsing_should_capture_best_levels() {
        let value: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/exchanges/toobit/orderbook.json"
        ))
        .unwrap();
        let book = parse_orderbook_snapshot(&value, "BTCUSDT", MarketType::Spot, 10_000).unwrap();
        assert_eq!(book.best_bid, Some(3.9));
        assert_eq!(book.best_ask, Some(4.000002));
        assert_eq!(book.bids[0].quantity, 431.0);
    }

    #[test]
    fn toobit_balance_and_fill_parsers_should_normalize_docs_shape() {
        let balance: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/exchanges/toobit/balance.json"
        ))
        .unwrap();
        let snapshot = parse_balance_snapshot(&balance, MarketType::Spot).unwrap();
        assert_eq!(snapshot.balances[0].asset, "BTC");
        assert_eq!(snapshot.balances[0].available, 995.899);

        let fills: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/exchanges/toobit/fills.json"
        ))
        .unwrap();
        let fills = parse_fills(&fills, MarketType::Spot).unwrap();
        assert_eq!(fills[0].trade_id.as_deref(), Some("1291291745779199489"));
        assert_eq!(fills[0].side, OrderSide::Sell);
        assert_eq!(fills[0].fee_asset.as_deref(), Some("USDT"));
        assert_eq!(
            fills[0].liquidity,
            crate::exchanges::unified::LiquidityRole::Maker
        );
    }

    #[test]
    fn toobit_ws_parsers_should_normalize_public_and_private_events() {
        let book = parse_ws_orderbook_message(
            r#"{"topic":"depth","event":"sub","symbol":"BTCUSDT","data":[{"s":"BTCUSDT","t":1700000000000,"b":[["65000","0.1"]],"a":[["65001","0.2"]]}]}"#,
            10_000,
        )
        .unwrap()
        .unwrap();
        assert_eq!(book.symbol, "BTCUSDT");
        assert_eq!(book.best_bid, Some(65_000.0));
        assert_eq!(book.best_ask, Some(65_001.0));

        let events = parse_user_stream_message(
            r#"{"e":"executionReport","E":1700000000001,"s":"BTCUSDT","c":"client-1","S":"BUY","o":"LIMIT","f":"GTC","q":"0.02","p":"65000","X":"PARTIALLY_FILLED","i":"order-1","z":"0.01","Z":"650","O":1700000000000}"#,
        )
        .unwrap();
        match &events[0] {
            UserStreamEvent::Order(order) => {
                assert_eq!(order.order_id, "order-1");
                assert_eq!(order.status, OrderStatus::PartiallyFilled);
                assert_eq!(order.average_price, Some(65_000.0));
            }
            other => panic!("expected order event, got {other:?}"),
        }

        let events = parse_user_stream_message(
            r#"{"e":"outboundAccountInfo","E":1700000000002,"B":[{"a":"USDT","f":"8","l":"2"}]}"#,
        )
        .unwrap();
        match &events[0] {
            UserStreamEvent::Balance(snapshot) => {
                assert_eq!(snapshot.balances[0].asset, "USDT");
                assert_eq!(snapshot.balances[0].total, 10.0);
            }
            other => panic!("expected balance event, got {other:?}"),
        }

        assert_eq!(
            toobit_user_stream_url("wss://stream.toobit.com/quote/ws/v1", "listen-1"),
            "wss://stream.toobit.com/api/v1/ws/listen-1"
        );
    }

    #[tokio::test]
    async fn toobit_spot_client_should_route_private_readbacks() {
        let (base_url, seen) = spawn_rest_server(vec![
            json!({"balances":[{"coin":"USDT","total":"10","free":"8","locked":"2"}]}),
            json!({"symbol":"BTCUSDT","orderId":"1001","clientOrderId":"LDRY_TOOBIT_1","price":"65000","origQty":"0.01","executedQty":"0.004","cumulativeQuoteQty":"260","status":"PARTIALLY_FILLED","timeInForce":"GTC","type":"LIMIT","side":"BUY","time":"1700000000000","updateTime":"1700000001000"}),
            json!([{"symbol":"BTCUSDT","orderId":"1002","clientOrderId":"LDRY_TOOBIT_2","price":"70000","origQty":"0.02","executedQty":"0","status":"NEW","timeInForce":"GTC","type":"LIMIT","side":"SELL","time":"1700000002000"}]),
            json!([{"id":"2001","symbol":"BTCUSDT","orderId":"1001","price":"65000","qty":"0.004","commission":"0.26","commissionAsset":"USDT","time":"1700000001000","isBuyer":true,"isMaker":false}]),
        ])
        .await;
        let client = ToobitSpotClient::new(ToobitConfig {
            api_key: "key".to_string(),
            api_secret: "secret".to_string(),
            base_url,
            dry_run: false,
            ..ToobitConfig::default()
        });

        let balances = client.get_balances().await.unwrap();
        assert_eq!(balances.balances[0].effective_available, 8.0);
        let order = client.get_order("BTCUSDT", "1001").await.unwrap();
        assert_eq!(order.status, OrderStatus::PartiallyFilled);
        assert_eq!(order.average_price, Some(65_000.0));
        let open_orders = client.get_open_orders(Some("BTCUSDT")).await.unwrap();
        assert_eq!(open_orders[0].side, OrderSide::Sell);
        let fills = client.get_recent_fills("BTCUSDT").await.unwrap();
        assert_eq!(fills[0].side, OrderSide::Buy);

        let requests = seen.lock().unwrap().clone();
        assert_signed_request(&requests[0], "GET", "/api/v1/account");
        assert_signed_request(&requests[1], "GET", "/api/v1/spot/order");
        assert_eq!(
            requests[1].query.get("orderId").map(String::as_str),
            Some("1001")
        );
        assert_signed_request(&requests[2], "GET", "/api/v1/spot/openOrders");
        assert_signed_request(&requests[3], "GET", "/api/v1/account/trades");
    }

    #[tokio::test]
    async fn toobit_spot_client_should_route_order_mutations() {
        let (base_url, seen) = spawn_rest_server(vec![
            json!({"symbol":"BTCUSDT","orderId":"2001","clientOrderId":"LDRY_TOOBIT_LIMIT_1","price":"65000","origQty":"0.01","executedQty":"0","status":"NEW","timeInForce":"GTC","type":"LIMIT","side":"BUY","transactTime":"1700000000000"}),
            json!({"symbol":"BTCUSDT","orderId":"2001","clientOrderId":"LDRY_TOOBIT_LIMIT_1","status":"CANCELED"}),
            json!({"success":true}),
        ])
        .await;
        let client = ToobitSpotClient::new(ToobitConfig {
            api_key: "key".to_string(),
            api_secret: "secret".to_string(),
            base_url,
            dry_run: false,
            ..ToobitConfig::default()
        });

        let order = client
            .place_order(OrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                side: OrderSide::Buy,
                position_side: PositionSide::None,
                order_type: OrderType::Limit,
                time_in_force: Some(TimeInForce::GTC),
                quantity: 0.01,
                price: Some(65_000.0),
                client_order_id: Some("LDRY_TOOBIT_LIMIT_1".to_string()),
                reduce_only: false,
            })
            .await
            .unwrap();
        assert_eq!(order.order_id, "2001");

        let cancelled = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                order_id: Some("2001".to_string()),
                client_order_id: Some("LDRY_TOOBIT_LIMIT_1".to_string()),
            })
            .await
            .unwrap();
        assert_eq!(cancelled.status, OrderStatus::Cancelled);

        let cancelled_all = client
            .cancel_all_orders(CancelAllOrdersRequest::for_symbol(
                MarketType::Spot,
                "BTCUSDT",
            ))
            .await
            .unwrap();
        assert_eq!(cancelled_all.cancelled_orders, 1);

        let requests = seen.lock().unwrap().clone();
        assert_signed_request(&requests[0], "POST", "/api/v1/spot/order");
        assert_eq!(
            requests[0].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
        assert_eq!(
            requests[0].query.get("type").map(String::as_str),
            Some("LIMIT")
        );
        assert_eq!(
            requests[0]
                .query
                .get("newClientOrderId")
                .map(String::as_str),
            Some("LDRY_TOOBIT_LIMIT_1")
        );
        assert_signed_request(&requests[1], "DELETE", "/api/v1/spot/order");
        assert_signed_request(&requests[2], "DELETE", "/api/v1/spot/openOrders");
    }

    #[tokio::test]
    async fn toobit_perp_client_should_route_private_rest_surface() {
        let (base_url, seen) = spawn_rest_server(vec![
            json!({"code":0,"data":[{"asset":"USDT","balance":"100","availableBalance":"80","locked":"20"}]}),
            json!({"code":0,"data":{"symbol":"BTC-SWAP-USDT","orderId":"3001","clientOrderId":"LDRY_TOOBIT_PERP_1","price":"65000","origQty":"0.01","executedQty":"0","status":"NEW","timeInForce":"GTC","type":"LIMIT","side":"BUY","positionSide":"LONG","transactTime":"1700000000000"}}),
            json!({"code":0,"data":{"symbol":"BTC-SWAP-USDT","orderId":"3001","clientOrderId":"LDRY_TOOBIT_PERP_1","status":"CANCELED"}}),
            json!({"code":0,"data":[{"orderId":"3001"},{"orderId":"3002"}]}),
            json!({"code":0,"data":{"symbol":"BTC-SWAP-USDT","orderId":"3001","clientOrderId":"LDRY_TOOBIT_PERP_1","price":"65000","origQty":"0.01","executedQty":"0.004","cumulativeQuoteQty":"260","status":"PARTIALLY_FILLED","timeInForce":"GTC","type":"LIMIT","side":"BUY","positionSide":"LONG","time":"1700000000000"}}),
            json!({"code":0,"data":[{"symbol":"BTC-SWAP-USDT","orderId":"3002","clientOrderId":"LDRY_TOOBIT_PERP_2","price":"66000","origQty":"0.02","executedQty":"0","status":"NEW","timeInForce":"GTC","type":"LIMIT","side":"SELL","positionSide":"SHORT","time":"1700000001000"}]}),
            json!({"code":0,"data":{"makerCommission":"0.0002","takerCommission":"0.0006"}}),
            json!({"code":0,"data":[{"id":"4001","symbol":"BTC-SWAP-USDT","orderId":"3001","price":"65000","qty":"0.004","commission":"0.26","commissionAsset":"USDT","time":"1700000001000","isBuyer":true,"isMaker":false}]}),
        ])
        .await;
        let client = ToobitPerpClient::new(ToobitConfig {
            api_key: "key".to_string(),
            api_secret: "secret".to_string(),
            base_url,
            dry_run: false,
            ..ToobitConfig::default()
        });

        let balances = client.get_balances().await.unwrap();
        assert_eq!(balances.balances[0].total, 100.0);

        let order = client
            .place_order(OrderRequest {
                market_type: MarketType::Perpetual,
                symbol: "BTCUSDT".to_string(),
                side: OrderSide::Buy,
                position_side: PositionSide::Long,
                order_type: OrderType::Limit,
                time_in_force: Some(TimeInForce::GTC),
                quantity: 0.01,
                price: Some(65_000.0),
                client_order_id: Some("LDRY_TOOBIT_PERP_1".to_string()),
                reduce_only: false,
            })
            .await
            .unwrap();
        assert_eq!(order.order_id, "3001");
        assert_eq!(order.position_side, PositionSide::Long);

        let cancelled = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Perpetual,
                symbol: "BTCUSDT".to_string(),
                order_id: Some("3001".to_string()),
                client_order_id: Some("LDRY_TOOBIT_PERP_1".to_string()),
            })
            .await
            .unwrap();
        assert_eq!(cancelled.status, OrderStatus::Cancelled);

        let cancel_all = client
            .cancel_all_orders(CancelAllOrdersRequest::for_symbol(
                MarketType::Perpetual,
                "BTCUSDT",
            ))
            .await
            .unwrap();
        assert_eq!(cancel_all.cancelled_orders, 2);

        let order = client.get_order("BTCUSDT", "3001").await.unwrap();
        assert_eq!(order.status, OrderStatus::PartiallyFilled);
        let open_orders = client.get_open_orders(Some("BTCUSDT")).await.unwrap();
        assert_eq!(open_orders[0].side, OrderSide::Sell);
        let fee = client.get_fee_rate("BTCUSDT").await.unwrap();
        assert_eq!(fee.maker, 0.0002);
        assert_eq!(fee.taker, 0.0006);
        let fills = client.get_recent_fills("BTCUSDT").await.unwrap();
        assert_eq!(fills[0].fee_asset.as_deref(), Some("USDT"));

        let requests = seen.lock().unwrap().clone();
        assert_signed_request(&requests[0], "GET", "/api/v1/futures/balance");
        assert_signed_request(&requests[1], "POST", "/api/v2/futures/order");
        assert_eq!(
            requests[1].query.get("positionSide").map(String::as_str),
            Some("LONG")
        );
        assert_signed_request(&requests[2], "DELETE", "/api/v2/futures/order");
        assert_signed_request(&requests[3], "DELETE", "/api/v2/futures/batch-orders");
        assert_signed_request(&requests[4], "GET", "/api/v2/futures/order");
        assert_signed_request(&requests[5], "GET", "/api/v2/futures/open-orders");
        assert_signed_request(&requests[6], "GET", "/api/v1/futures/commissionRate");
        assert_signed_request(&requests[7], "GET", "/api/v2/futures/user-trades");
    }

    #[tokio::test]
    async fn toobit_perp_should_normalize_symbols_and_dry_run_orders() {
        let client = ToobitPerpClient::new(ToobitConfig::default());
        assert_eq!(client.normalize_symbol("btcusdt").unwrap(), "BTC-SWAP-USDT");

        let response = client
            .place_order(OrderRequest {
                market_type: MarketType::Perpetual,
                symbol: "btcusdt".to_string(),
                side: OrderSide::Buy,
                position_side: PositionSide::Net,
                order_type: OrderType::Market,
                time_in_force: None,
                quantity: 1.0,
                price: None,
                client_order_id: Some("LDRY_TOOBIT_PERP_1".to_string()),
                reduce_only: false,
            })
            .await
            .unwrap();
        assert_eq!(response.market_type, MarketType::Perpetual);
        assert_eq!(response.symbol, "BTC-SWAP-USDT");
    }
}
