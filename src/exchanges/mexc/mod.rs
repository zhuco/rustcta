use std::collections::HashMap;
use std::time::Duration as StdDuration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use prost::Message as ProstMessage;
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::Message;

use crate::core::error::ExchangeError as CoreExchangeError;
use crate::core::ws_connect::connect_async;
use crate::exchanges::client_order_id::{generate_client_order_id, validate_client_order_id};
use crate::exchanges::spot_reservation::{BalanceReservation, BalanceReservationManager};
use crate::exchanges::unified::{
    round_price_to_tick, round_quantity_to_step, validate_order_against_symbol_rule,
    validate_order_lookup_id, validate_orderbook_depth, AmendOrderRequest, AssetBalance,
    BalanceSnapshot, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeClient, ExchangeClientCapabilities, ExchangeClientError,
    ExchangeClientResult, ExchangeError, ExchangeErrorClass, ExchangeHealthStatus, FeeRate,
    FeeRateSource, MarketType, OrderBookLevel, OrderBookSnapshot, OrderRequest, OrderResponse,
    OrderSide, OrderStatus, OrderType, PositionSide, QuoteMarketOrderRequest, SymbolRule,
    SymbolStatus, TimeInForce, TradeFill, UserStreamEvent,
};
use crate::utils::SignatureHelper;

const DEFAULT_REST_BASE_URL: &str = "https://api.mexc.com";
const DEFAULT_WS_URL: &str = "wss://wbs-api.mexc.com/ws";
const DEFAULT_RECV_WINDOW_MS: u64 = 5_000;
const DEFAULT_STALE_BOOK_MS: u64 = 10_000;
const DEFAULT_RECONNECT_INTERVAL_MS: u64 = 1_000;
const DEFAULT_ORDER_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_DEPTH: u16 = 5;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MexcSpotConfig {
    pub api_key: String,
    pub api_secret: String,
    #[serde(default = "default_rest_base_url")]
    pub base_url: String,
    #[serde(default = "default_ws_url")]
    pub websocket_url: String,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub enable_private_stream: bool,
    #[serde(default = "default_stale_book_ms")]
    pub stale_book_ms: u64,
    #[serde(default = "default_reconnect_interval_ms")]
    pub reconnect_interval_ms: u64,
    #[serde(default = "default_max_reconnect_attempts")]
    pub max_reconnect_attempts: u32,
    #[serde(default = "default_order_timeout_ms")]
    pub order_timeout_ms: u64,
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    #[serde(default = "default_recv_window_ms")]
    pub recv_window: u64,
    #[serde(default = "default_depth")]
    pub orderbook_depth: u16,
    #[serde(default)]
    pub enabled_symbols: Vec<String>,
    #[serde(default)]
    pub fee_override: Option<FeeOverride>,
    #[serde(default)]
    pub log_raw_messages: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct FeeOverride {
    pub maker_fee_rate: f64,
    pub taker_fee_rate: f64,
}

impl Default for MexcSpotConfig {
    fn default() -> Self {
        Self {
            api_key: std::env::var("MEXC_API_KEY").unwrap_or_default(),
            api_secret: std::env::var("MEXC_API_SECRET").unwrap_or_default(),
            base_url: default_rest_base_url(),
            websocket_url: default_ws_url(),
            dry_run: true,
            enable_private_stream: false,
            stale_book_ms: DEFAULT_STALE_BOOK_MS,
            reconnect_interval_ms: DEFAULT_RECONNECT_INTERVAL_MS,
            max_reconnect_attempts: default_max_reconnect_attempts(),
            order_timeout_ms: DEFAULT_ORDER_TIMEOUT_MS,
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            recv_window: DEFAULT_RECV_WINDOW_MS,
            orderbook_depth: DEFAULT_DEPTH,
            enabled_symbols: Vec::new(),
            fee_override: None,
            log_raw_messages: false,
        }
    }
}

#[derive(Clone)]
pub struct MexcSpotClient {
    config: MexcSpotConfig,
    http: reqwest::Client,
    reservations: BalanceReservationManager,
}

impl MexcSpotClient {
    pub fn new(config: MexcSpotConfig) -> Self {
        Self {
            config,
            http: crate::core::http2_fix::shared_http_client(),
            reservations: BalanceReservationManager::default(),
        }
    }

    pub fn reservations(&self) -> BalanceReservationManager {
        self.reservations.clone()
    }

    pub fn generate_client_order_id() -> String {
        generate_client_order_id("mexc", MarketType::Spot, "spot").into_string()
    }

    async fn send_public_request(
        &self,
        endpoint: &str,
        params: HashMap<String, String>,
    ) -> ExchangeClientResult<Value> {
        let url = build_url(&self.config.base_url, endpoint, &params, None);
        let response = self
            .http
            .get(url)
            .timeout(StdDuration::from_millis(self.config.request_timeout_ms))
            .send()
            .await
            .map_err(CoreExchangeError::from)?;
        parse_response(response).await
    }

    async fn send_signed_request(
        &self,
        method: Method,
        endpoint: &str,
        mut params: HashMap<String, String>,
    ) -> ExchangeClientResult<Value> {
        self.ensure_credentials()?;
        params.insert(
            "timestamp".to_string(),
            Utc::now().timestamp_millis().to_string(),
        );
        params.insert(
            "recvWindow".to_string(),
            self.config.recv_window.to_string(),
        );
        let query = SignatureHelper::build_query_string(&params);
        let signature = sign_query(&self.config.api_secret, &query);
        let url = build_url(&self.config.base_url, endpoint, &params, Some(&signature));
        let response = self
            .http
            .request(method, url)
            .header("X-MEXC-APIKEY", &self.config.api_key)
            .timeout(StdDuration::from_millis(self.config.request_timeout_ms))
            .send()
            .await
            .map_err(CoreExchangeError::from)?;
        parse_response(response).await
    }

    fn ensure_credentials(&self) -> ExchangeClientResult<()> {
        if self.config.api_key.trim().is_empty() || self.config.api_secret.trim().is_empty() {
            return Err(ExchangeClientError::Classified(ExchangeError {
                exchange: "mexc".to_string(),
                class: ExchangeErrorClass::AuthenticationFailed,
                code: None,
                message: "MEXC_API_KEY and MEXC_API_SECRET are required".to_string(),
            }));
        }
        Ok(())
    }

    async fn reserve_for_order(
        &self,
        request: &OrderRequest,
        rule: &SymbolRule,
    ) -> ExchangeClientResult<Option<BalanceReservation>> {
        if self.config.dry_run {
            return Ok(None);
        }
        let snapshot = self.get_balances().await?;
        self.reservations
            .update_balances(self.exchange_name(), &snapshot.balances)?;
        match request.side {
            OrderSide::Sell => self
                .reservations
                .reserve(self.exchange_name(), &rule.base_asset, request.quantity)
                .map(Some),
            OrderSide::Buy => {
                let price = request
                    .price
                    .ok_or_else(|| ExchangeClientError::Validation {
                        field: "price",
                        reason: "MEXC buy reservation requires explicit price for cost estimate"
                            .to_string(),
                    })?;
                let fee_rate = self
                    .get_fee_rate(&request.symbol)
                    .await?
                    .taker_fee_rate
                    .max(0.0);
                let quote_amount = price * request.quantity * (1.0 + fee_rate + 0.002);
                self.reservations
                    .reserve(self.exchange_name(), &rule.quote_asset, quote_amount)
                    .map(Some)
            }
        }
    }

    async fn reserve_for_quote_market_order(
        &self,
        request: &QuoteMarketOrderRequest,
        rule: &SymbolRule,
    ) -> ExchangeClientResult<Option<BalanceReservation>> {
        if self.config.dry_run {
            return Ok(None);
        }
        let snapshot = self.get_balances().await?;
        self.reservations
            .update_balances(self.exchange_name(), &snapshot.balances)?;
        self.reservations
            .reserve(
                self.exchange_name(),
                &rule.quote_asset,
                request.quote_quantity,
            )
            .map(Some)
    }

    fn dry_run_order_response(&self, request: &OrderRequest, symbol: &str) -> OrderResponse {
        OrderResponse {
            exchange: "mexc".to_string(),
            market_type: MarketType::Spot,
            symbol: symbol.to_string(),
            order_id: format!("dry-mexc-{}", Utc::now().timestamp_millis()),
            client_order_id: request.client_order_id.clone(),
            side: request.side,
            position_side: PositionSide::None,
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

    fn dry_run_quote_market_order_response(
        &self,
        request: &QuoteMarketOrderRequest,
        symbol: &str,
    ) -> OrderResponse {
        OrderResponse {
            exchange: "mexc".to_string(),
            market_type: MarketType::Spot,
            symbol: symbol.to_string(),
            order_id: format!("dry-mexc-{}", Utc::now().timestamp_millis()),
            client_order_id: request.client_order_id.clone(),
            side: request.side,
            position_side: PositionSide::None,
            order_type: OrderType::Market,
            status: OrderStatus::New,
            price: None,
            quantity: request.quote_quantity,
            filled_quantity: 0.0,
            average_price: None,
            created_at: Utc::now(),
            updated_at: None,
        }
    }
}

#[async_trait]
impl ExchangeClient for MexcSpotClient {
    fn market_type(&self) -> MarketType {
        MarketType::Spot
    }

    fn exchange_name(&self) -> &str {
        "mexc"
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let mut capabilities = ExchangeClientCapabilities::spot(self.exchange_name());
        capabilities.supports_cancel_all_orders = true;
        capabilities.supports_quote_market_order = true;
        capabilities
    }

    fn normalize_symbol(&self, symbol: &str) -> ExchangeClientResult<String> {
        normalize_mexc_symbol(symbol)
    }

    fn denormalize_symbol(&self, symbol: &str) -> ExchangeClientResult<String> {
        normalize_mexc_symbol(symbol)
    }

    async fn load_symbol_rules(&self) -> ExchangeClientResult<Vec<SymbolRule>> {
        let response = self
            .send_public_request("/api/v3/exchangeInfo", HashMap::new())
            .await?;
        parse_symbol_rules(&response)
    }

    async fn get_balances(&self) -> ExchangeClientResult<BalanceSnapshot> {
        let response = self
            .send_signed_request(Method::GET, "/api/v3/account", HashMap::new())
            .await?;
        let snapshot = parse_balance_snapshot(&response)?;
        self.reservations
            .update_balances(self.exchange_name(), &snapshot.balances)?;
        Ok(BalanceSnapshot {
            balances: snapshot
                .balances
                .into_iter()
                .map(|balance| self.reservations.balance_with_reservation("mexc", balance))
                .collect(),
            ..snapshot
        })
    }

    async fn get_orderbook(
        &self,
        symbol: &str,
        depth: u16,
    ) -> ExchangeClientResult<OrderBookSnapshot> {
        let symbol = self.normalize_symbol(symbol)?;
        validate_orderbook_depth(depth)?;
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.clone());
        params.insert("limit".to_string(), normalize_depth(depth).to_string());
        let response = self.send_public_request("/api/v3/depth", params).await?;
        parse_orderbook_snapshot(&response, &symbol, self.config.stale_book_ms)
    }

    async fn place_order(&self, mut request: OrderRequest) -> ExchangeClientResult<OrderResponse> {
        request.validate()?;
        if request.market_type != MarketType::Spot {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: "MexcSpotClient only supports MarketType::Spot".to_string(),
            });
        }
        let symbol = self.normalize_symbol(&request.symbol)?;
        request.symbol = symbol.clone();
        if request.client_order_id.is_none() {
            request.client_order_id = Some(Self::generate_client_order_id());
        }
        if let Some(client_order_id) = &request.client_order_id {
            validate_client_order_id("mexc", MarketType::Spot, client_order_id).map_err(
                |error| ExchangeClientError::Validation {
                    field: "client_order_id",
                    reason: error.to_string(),
                },
            )?;
        }
        let params = mexc_order_params(&request, &symbol)?;
        if self.config.dry_run {
            return Ok(self.dry_run_order_response(&request, &symbol));
        }
        let rule = self
            .get_symbol_rule(&symbol)
            .await?
            .unwrap_or_else(|| fallback_rule(&symbol));
        validate_order_against_symbol_rule(&request, &rule)?;
        let mut reservation = self.reserve_for_order(&request, &rule).await?;

        let response = self
            .send_signed_request(Method::POST, "/api/v3/order", params)
            .await;
        match response {
            Ok(value) => parse_order_response(&value, "mexc"),
            Err(error) => {
                if let Some(reservation) = reservation.as_mut() {
                    let _ = self.reservations.release(reservation);
                }
                Err(error)
            }
        }
    }

    async fn place_quote_market_order(
        &self,
        mut request: QuoteMarketOrderRequest,
    ) -> ExchangeClientResult<OrderResponse> {
        request.validate()?;
        if request.side != OrderSide::Buy {
            return Err(ExchangeClientError::Unsupported(
                "MEXC Spot quote-sized market orders are only supported for market buys"
                    .to_string(),
            ));
        }
        let symbol = self.normalize_symbol(&request.symbol)?;
        request.symbol = symbol.clone();
        ensure_quote_market_client_order_id(&mut request)?;
        if self.config.dry_run {
            return Ok(self.dry_run_quote_market_order_response(&request, &symbol));
        }
        let rule = self
            .get_symbol_rule(&symbol)
            .await?
            .unwrap_or_else(|| fallback_rule(&symbol));
        if request.quote_quantity < rule.min_notional {
            return Err(ExchangeClientError::Validation {
                field: "quote_quantity",
                reason: format!(
                    "quote quantity {} below min notional {} for {}",
                    request.quote_quantity, rule.min_notional, symbol
                ),
            });
        }
        let mut reservation = self.reserve_for_quote_market_order(&request, &rule).await?;
        let params = mexc_quote_market_order_params(&request, &symbol)?;
        let response = self
            .send_signed_request(Method::POST, "/api/v3/order", params)
            .await;
        match response {
            Ok(value) => parse_order_response(&value, "mexc"),
            Err(error) => {
                if let Some(reservation) = reservation.as_mut() {
                    let _ = self.reservations.release(reservation);
                }
                Err(error)
            }
        }
    }

    async fn amend_order(&self, request: AmendOrderRequest) -> ExchangeClientResult<OrderResponse> {
        request.validate()?;
        if request.market_type != MarketType::Spot {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: "MexcSpotClient only supports MarketType::Spot".to_string(),
            });
        }
        ensure_amend_client_order_id(&request)?;
        Err(ExchangeClientError::Unsupported(
            "MEXC Spot v3 does not expose a native amend or cancel-replace endpoint; cancel and place a new order instead".to_string(),
        ))
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeClientResult<CancelOrderResponse> {
        request.validate()?;
        if request.market_type != MarketType::Spot {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: "MexcSpotClient only supports MarketType::Spot".to_string(),
            });
        }
        ensure_cancel_client_order_id(&request)?;
        let symbol = self.normalize_symbol(&request.symbol)?;
        if self.config.dry_run {
            return Ok(CancelOrderResponse {
                exchange: "mexc".to_string(),
                market_type: MarketType::Spot,
                symbol,
                order_id: request.order_id().map(str::to_string),
                client_order_id: request.client_order_id().map(str::to_string),
                status: OrderStatus::Cancelled,
                cancelled_at: Utc::now(),
            });
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
            .send_signed_request(Method::DELETE, "/api/v3/order", params)
            .await?;
        Ok(CancelOrderResponse {
            exchange: "mexc".to_string(),
            market_type: MarketType::Spot,
            symbol,
            order_id: value_as_string(value.get("orderId"))
                .or_else(|| request.order_id().map(str::to_string)),
            client_order_id: value_as_string(value.get("clientOrderId"))
                .or_else(|| request.client_order_id().map(str::to_string)),
            status: value
                .get("status")
                .and_then(Value::as_str)
                .map(map_mexc_order_status)
                .unwrap_or(OrderStatus::Cancelled),
            cancelled_at: Utc::now(),
        })
    }

    async fn cancel_all_orders(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeClientResult<CancelAllOrdersResponse> {
        if request.market_type != MarketType::Spot {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: "MexcSpotClient only supports MarketType::Spot".to_string(),
            });
        }
        let symbol = self.normalize_symbol(request.validate_symbol_required()?)?;
        if self.config.dry_run {
            return Ok(CancelAllOrdersResponse {
                exchange: "mexc".to_string(),
                market_type: MarketType::Spot,
                symbol: Some(symbol),
                cancelled_orders: 0,
                cancelled_at: Utc::now(),
            });
        }
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.clone());
        let value = self
            .send_signed_request(Method::DELETE, "/api/v3/openOrders", params)
            .await?;
        Ok(CancelAllOrdersResponse {
            exchange: "mexc".to_string(),
            market_type: MarketType::Spot,
            symbol: Some(symbol),
            cancelled_orders: mexc_cancel_all_cancelled_count(&value)?,
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
            .send_signed_request(Method::GET, "/api/v3/order", params)
            .await?;
        parse_order_response(&value, "mexc")
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
            .send_signed_request(Method::GET, "/api/v3/openOrders", params)
            .await?;
        value
            .as_array()
            .ok_or_else(|| parser_error("mexc", "open orders response is not an array", &value))?
            .iter()
            .map(|item| parse_order_response(item, "mexc"))
            .collect()
    }

    async fn get_fee_rate(&self, symbol: &str) -> ExchangeClientResult<FeeRate> {
        if let Some(fee) = self.config.fee_override {
            return Ok(FeeRate::new(
                fee.maker_fee_rate,
                fee.taker_fee_rate,
                FeeRateSource::ConfigOverride,
            ));
        }
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), self.normalize_symbol(symbol)?);
        let value = self
            .send_signed_request(Method::GET, "/api/v3/tradeFee", params)
            .await?;
        parse_fee_rate(&value)
    }

    async fn get_recent_fills(&self, symbol: &str) -> ExchangeClientResult<Vec<TradeFill>> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), self.normalize_symbol(symbol)?);
        let value = self
            .send_signed_request(Method::GET, "/api/v3/myTrades", params)
            .await?;
        value
            .as_array()
            .ok_or_else(|| parser_error("mexc", "fills response is not an array", &value))?
            .iter()
            .map(parse_fill)
            .collect()
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
            .iter()
            .map(|symbol| self.normalize_symbol(symbol))
            .collect::<ExchangeClientResult<Vec<_>>>()?;
        let client = self.clone();
        let (tx, rx) = mpsc::channel(1024);
        tokio::spawn(async move {
            client.run_orderbook_stream(symbols, tx).await;
        });
        Ok(rx)
    }

    async fn subscribe_user_stream(&self) -> ExchangeClientResult<mpsc::Receiver<UserStreamEvent>> {
        Err(ExchangeClientError::Unsupported(
            "MEXC Spot private user stream is not implemented; use REST polling fallback"
                .to_string(),
        ))
    }

    async fn health_check(&self) -> ExchangeClientResult<ExchangeHealthStatus> {
        let mut params = HashMap::new();
        params.insert(
            "timestamp".to_string(),
            Utc::now().timestamp_millis().to_string(),
        );
        Ok(ExchangeHealthStatus {
            exchange: "mexc".to_string(),
            market_type: MarketType::Spot,
            connected: self
                .send_public_request("/api/v3/time", HashMap::new())
                .await
                .is_ok(),
            public_ws_healthy: true,
            private_ws_healthy: false,
            rest_healthy: true,
            stale_books: Vec::new(),
            last_error: None,
            checked_at: Utc::now(),
        })
    }
}

impl MexcSpotClient {
    async fn run_orderbook_stream(self, symbols: Vec<String>, tx: mpsc::Sender<OrderBookSnapshot>) {
        let stale_after = StdDuration::from_millis(self.config.stale_book_ms);
        let reconnect_delay = StdDuration::from_millis(self.config.reconnect_interval_ms);
        let subscribe_messages = symbols
            .iter()
            .map(|symbol| {
                serde_json::json!({
                    "method": "SUBSCRIPTION",
                    "params": [format!("spot@public.limit.depth.v3.api.pb@{}@{}", symbol, normalize_depth(self.config.orderbook_depth))]
                })
                .to_string()
            })
            .collect::<Vec<_>>();
        loop {
            log::info!(
                "MEXC Spot websocket connecting symbols={}",
                symbols.join(",")
            );
            match connect_async(self.config.websocket_url.as_str()).await {
                Ok((mut ws, _)) => {
                    for message in &subscribe_messages {
                        if ws.send(Message::Text(message.clone())).await.is_err() {
                            break;
                        }
                    }
                    loop {
                        match timeout(stale_after, ws.next()).await {
                            Err(_) => {
                                log::warn!("MEXC Spot order book stream stale; reconnecting");
                                break;
                            }
                            Ok(Some(Ok(Message::Text(text)))) => {
                                if self.config.log_raw_messages {
                                    log::debug!("MEXC Spot WS raw={}", text);
                                }
                                match parse_ws_orderbook_message(&text, self.config.stale_book_ms) {
                                    Ok(Some(snapshot)) => {
                                        if tx.send(snapshot).await.is_err() {
                                            return;
                                        }
                                    }
                                    Ok(None) => {}
                                    Err(error) => log::warn!("MEXC Spot WS parse error: {}", error),
                                }
                            }
                            Ok(Some(Ok(Message::Binary(raw)))) => {
                                match parse_ws_orderbook_binary(&raw, self.config.stale_book_ms) {
                                    Ok(Some(snapshot)) => {
                                        if tx.send(snapshot).await.is_err() {
                                            return;
                                        }
                                    }
                                    Ok(None) => {}
                                    Err(error) => {
                                        log::warn!("MEXC Spot WS binary parse error: {}", error)
                                    }
                                }
                            }
                            Ok(Some(Ok(Message::Ping(payload)))) => {
                                let _ = ws.send(Message::Pong(payload)).await;
                            }
                            Ok(Some(Ok(Message::Close(frame)))) => {
                                log::warn!("MEXC Spot websocket closed: {:?}", frame);
                                break;
                            }
                            Ok(Some(Ok(_))) => {}
                            Ok(Some(Err(error))) => {
                                log::warn!("MEXC Spot websocket error: {}", error);
                                break;
                            }
                            Ok(None) => break,
                        }
                    }
                }
                Err(error) => log::warn!("MEXC Spot websocket connect error: {}", error),
            }
            sleep(reconnect_delay).await;
        }
    }
}

pub fn sign_query(secret: &str, query: &str) -> String {
    SignatureHelper::binance_signature(secret, query)
}

pub fn normalize_mexc_symbol(symbol: &str) -> ExchangeClientResult<String> {
    let normalized = symbol
        .trim()
        .replace(['/', '-', '_'], "")
        .to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(ExchangeClientError::Validation {
            field: "symbol",
            reason: "symbol must not be empty".to_string(),
        });
    }
    Ok(normalized)
}

pub fn map_mexc_order_status(status: &str) -> OrderStatus {
    match status.trim().to_ascii_uppercase().as_str() {
        "NEW" => OrderStatus::New,
        "PARTIALLY_FILLED" | "PARTIALLY_FILLED_CANCELED" => OrderStatus::PartiallyFilled,
        "FILLED" => OrderStatus::Filled,
        "CANCELED" | "CANCELLED" => OrderStatus::Cancelled,
        "REJECTED" => OrderStatus::Rejected,
        "EXPIRED" => OrderStatus::Expired,
        _ => OrderStatus::Unknown,
    }
}

pub fn classify_mexc_error(code: Option<i64>, message: &str) -> ExchangeErrorClass {
    match (code, message.to_ascii_lowercase()) {
        (Some(30005), _) => ExchangeErrorClass::Oversold,
        (Some(10101), _) => ExchangeErrorClass::InvalidSymbol,
        (Some(10072), _) => ExchangeErrorClass::InvalidPrecision,
        (Some(700003), _) => ExchangeErrorClass::AuthenticationFailed,
        (Some(10007), _) => ExchangeErrorClass::RateLimited,
        (_, msg) if msg.contains("oversold") => ExchangeErrorClass::Oversold,
        (_, msg) if msg.contains("insufficient") => ExchangeErrorClass::InsufficientBalance,
        (_, msg) if msg.contains("symbol") => ExchangeErrorClass::InvalidSymbol,
        (_, msg) if msg.contains("rate") || msg.contains("too many") => {
            ExchangeErrorClass::RateLimited
        }
        _ => ExchangeErrorClass::Unknown,
    }
}

pub fn parse_symbol_rules(value: &Value) -> ExchangeClientResult<Vec<SymbolRule>> {
    let symbols = value
        .get("symbols")
        .and_then(Value::as_array)
        .ok_or_else(|| parser_error("mexc", "exchangeInfo missing symbols", value))?;
    symbols.iter().map(parse_symbol_rule).collect()
}

pub fn parse_symbol_rule(value: &Value) -> ExchangeClientResult<SymbolRule> {
    let exchange_symbol = required_str(value, "symbol")?.to_ascii_uppercase();
    let base_asset = required_str(value, "baseAsset")?.to_ascii_uppercase();
    let quote_asset = required_str(value, "quoteAsset")?.to_ascii_uppercase();
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
        exchange: "mexc".to_string(),
        market_type: MarketType::Spot,
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
            "TRADING" | "ENABLED" => SymbolStatus::Trading,
            "SUSPENDED" => SymbolStatus::Suspended,
            "DELISTED" => SymbolStatus::Delisted,
            _ => SymbolStatus::Unknown,
        },
        raw_metadata: Some(value.clone()),
    })
}

pub fn parse_balance_snapshot(value: &Value) -> ExchangeClientResult<BalanceSnapshot> {
    let balances = value
        .get("balances")
        .and_then(Value::as_array)
        .ok_or_else(|| parser_error("mexc", "account response missing balances", value))?;
    let mut normalized = Vec::new();
    for item in balances {
        let asset = required_str(item, "asset")?;
        let available = number_from_str(item.get("free")).unwrap_or(0.0);
        let locked = number_from_str(item.get("locked")).unwrap_or(0.0);
        if available > 0.0 || locked > 0.0 {
            normalized.push(AssetBalance::new(
                asset,
                available + locked,
                available,
                locked,
            ));
        }
    }
    Ok(BalanceSnapshot {
        exchange: "mexc".to_string(),
        market_type: MarketType::Spot,
        balances: normalized,
        timestamp: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    value: &Value,
    symbol: &str,
    stale_book_ms: u64,
) -> ExchangeClientResult<OrderBookSnapshot> {
    let bids = value
        .get("bids")
        .or_else(|| value.get("b"))
        .and_then(Value::as_array)
        .ok_or_else(|| parser_error("mexc", "orderbook missing bids", value))?;
    let asks = value
        .get("asks")
        .or_else(|| value.get("a"))
        .and_then(Value::as_array)
        .ok_or_else(|| parser_error("mexc", "orderbook missing asks", value))?;
    let bids = parse_levels(bids)?;
    let asks = parse_levels(asks)?;
    let exchange_timestamp = value
        .get("timestamp")
        .or_else(|| value.get("E"))
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    let received_at = Utc::now();
    let latency_ms =
        exchange_timestamp.map(|ts| received_at.signed_duration_since(ts).num_milliseconds());
    Ok(OrderBookSnapshot {
        exchange: "mexc".to_string(),
        market_type: MarketType::Spot,
        symbol: symbol.to_ascii_uppercase(),
        best_bid: bids.first().map(|level| level.price),
        best_ask: asks.first().map(|level| level.price),
        bids,
        asks,
        exchange_timestamp,
        received_at,
        latency_ms,
        sequence: value
            .get("lastUpdateId")
            .or_else(|| value.get("u"))
            .and_then(Value::as_u64),
        is_stale: latency_ms.is_some_and(|latency| latency > stale_book_ms as i64),
    })
}

pub fn parse_ws_orderbook_message(
    text: &str,
    stale_book_ms: u64,
) -> ExchangeClientResult<Option<OrderBookSnapshot>> {
    let value: Value = serde_json::from_str(text).map_err(CoreExchangeError::from)?;
    if value.get("code").and_then(Value::as_i64) == Some(0) && value.get("d").is_none() {
        return Ok(None);
    }
    let symbol = value
        .get("s")
        .or_else(|| value.get("symbol"))
        .and_then(Value::as_str)
        .or_else(|| {
            value
                .get("c")
                .and_then(Value::as_str)
                .and_then(|channel| channel.split('@').nth(2))
        })
        .unwrap_or("UNKNOWN");
    let data = value.get("d").unwrap_or(&value);
    parse_orderbook_snapshot(data, symbol, stale_book_ms).map(Some)
}

pub fn parse_ws_orderbook_binary(
    raw: &[u8],
    stale_book_ms: u64,
) -> ExchangeClientResult<Option<OrderBookSnapshot>> {
    let wrapper = MexcPushDataV3ApiWrapper::decode(raw)
        .map_err(|error| CoreExchangeError::ParseError(error.to_string()))?;
    let Some(depth) = wrapper.public_limit_depths else {
        return Ok(None);
    };
    let bids = depth
        .bids
        .iter()
        .map(mexc_pb_level)
        .collect::<Result<Vec<_>, _>>()?;
    let asks = depth
        .asks
        .iter()
        .map(mexc_pb_level)
        .collect::<Result<Vec<_>, _>>()?;
    let exchange_timestamp = wrapper
        .send_time
        .or(wrapper.create_time)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    let received_at = Utc::now();
    let latency_ms =
        exchange_timestamp.map(|ts| received_at.signed_duration_since(ts).num_milliseconds());
    let sequence = depth
        .version
        .parse::<u64>()
        .ok()
        .or_else(|| depth.event_type.parse::<u64>().ok());
    Ok(Some(OrderBookSnapshot {
        exchange: "mexc".to_string(),
        market_type: MarketType::Spot,
        symbol: wrapper
            .symbol
            .unwrap_or_else(|| {
                symbol_from_mexc_channel(&wrapper.channel)
                    .unwrap_or("UNKNOWN")
                    .to_string()
            })
            .to_ascii_uppercase(),
        best_bid: bids.first().map(|level| level.price),
        best_ask: asks.first().map(|level| level.price),
        bids,
        asks,
        exchange_timestamp,
        received_at,
        latency_ms,
        sequence,
        is_stale: latency_ms.is_some_and(|latency| latency > stale_book_ms as i64),
    }))
}

fn mexc_pb_level(item: &MexcPublicLimitDepthV3ApiItem) -> ExchangeClientResult<OrderBookLevel> {
    Ok(OrderBookLevel {
        price: item
            .price
            .parse::<f64>()
            .map_err(|error| CoreExchangeError::ParseError(error.to_string()))?,
        quantity: item
            .quantity
            .parse::<f64>()
            .map_err(|error| CoreExchangeError::ParseError(error.to_string()))?,
    })
}

fn symbol_from_mexc_channel(channel: &str) -> Option<&str> {
    channel.split('@').nth(2)
}

pub fn parse_private_stream_message(text: &str) -> ExchangeClientResult<Vec<UserStreamEvent>> {
    let value: Value = serde_json::from_str(text).map_err(CoreExchangeError::from)?;
    if value.get("success").and_then(Value::as_bool) == Some(true)
        || value.get("code").and_then(Value::as_i64) == Some(0) && value.get("data").is_none()
        || value
            .get("method")
            .and_then(Value::as_str)
            .is_some_and(|method| method.contains("login") || method.contains("subscribe"))
    {
        return Ok(Vec::new());
    }
    let channel = value
        .get("channel")
        .or_else(|| value.get("method"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let payload = value
        .get("data")
        .or_else(|| value.get("d"))
        .or_else(|| value.get("params"))
        .cloned()
        .unwrap_or(Value::Null);
    let items = match payload {
        Value::Array(items) => items,
        Value::Object(_) => vec![payload],
        _ => Vec::new(),
    };
    let mut events = Vec::new();
    for item in items {
        if channel.contains("order") {
            events.push(UserStreamEvent::Order(parse_order_response(&item, "mexc")?));
        } else if channel.contains("deal") || channel.contains("trade") || channel.contains("fill")
        {
            events.push(UserStreamEvent::Fill(parse_fill(&item)?));
        } else if channel.contains("balance")
            || channel.contains("account")
            || channel.contains("asset")
        {
            events.push(UserStreamEvent::Balance(parse_balance_snapshot(&json!({
                "balances": [item],
            }))?));
        }
    }
    Ok(events)
}

#[derive(Clone, PartialEq, ProstMessage)]
struct MexcPushDataV3ApiWrapper {
    #[prost(string, tag = "1")]
    channel: String,
    #[prost(message, optional, tag = "303")]
    public_limit_depths: Option<MexcPublicLimitDepthsV3Api>,
    #[prost(string, optional, tag = "3")]
    symbol: Option<String>,
    #[prost(string, optional, tag = "4")]
    symbol_id: Option<String>,
    #[prost(int64, optional, tag = "5")]
    create_time: Option<i64>,
    #[prost(int64, optional, tag = "6")]
    send_time: Option<i64>,
}

#[derive(Clone, PartialEq, ProstMessage)]
struct MexcPublicLimitDepthsV3Api {
    #[prost(message, repeated, tag = "1")]
    asks: Vec<MexcPublicLimitDepthV3ApiItem>,
    #[prost(message, repeated, tag = "2")]
    bids: Vec<MexcPublicLimitDepthV3ApiItem>,
    #[prost(string, tag = "3")]
    event_type: String,
    #[prost(string, tag = "4")]
    version: String,
}

#[derive(Clone, PartialEq, ProstMessage)]
struct MexcPublicLimitDepthV3ApiItem {
    #[prost(string, tag = "1")]
    price: String,
    #[prost(string, tag = "2")]
    quantity: String,
}

pub fn parse_order_response(value: &Value, exchange: &str) -> ExchangeClientResult<OrderResponse> {
    let symbol = required_str(value, "symbol")
        .unwrap_or("UNKNOWN")
        .to_ascii_uppercase();
    let quantity =
        number_from_str(value.get("origQty").or_else(|| value.get("quantity"))).unwrap_or(0.0);
    let filled_quantity = number_from_str(value.get("executedQty")).unwrap_or(0.0);
    Ok(OrderResponse {
        exchange: exchange.to_string(),
        market_type: MarketType::Spot,
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
        position_side: PositionSide::None,
        order_type: parse_order_type(
            value.get("type").and_then(Value::as_str).unwrap_or("LIMIT"),
            value.get("timeInForce").and_then(Value::as_str),
        ),
        status: value
            .get("status")
            .and_then(Value::as_str)
            .map(map_mexc_order_status)
            .unwrap_or(OrderStatus::Unknown),
        price: number_from_str(value.get("price")).filter(|price| *price > 0.0),
        quantity,
        filled_quantity,
        average_price: average_price(value, filled_quantity),
        created_at: first_timestamp(value, &["transactTime", "time"]).unwrap_or_else(Utc::now),
        updated_at: first_timestamp(value, &["updateTime"]),
    })
}

pub fn parse_fill(value: &Value) -> ExchangeClientResult<TradeFill> {
    Ok(TradeFill {
        exchange: "mexc".to_string(),
        market_type: MarketType::Spot,
        symbol: required_str(value, "symbol")?.to_string(),
        trade_id: value_as_string(value.get("id")),
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
        quantity: number_from_str(value.get("qty")).unwrap_or(0.0),
        fee_asset: value_as_string(value.get("commissionAsset")),
        fee_amount: number_from_str(value.get("commission")),
        liquidity: crate::exchanges::unified::LiquidityRole::Unknown,
        timestamp: first_timestamp(value, &["time"]).unwrap_or_else(Utc::now),
    })
}

pub fn mexc_cancel_all_cancelled_count(value: &Value) -> ExchangeClientResult<usize> {
    let items = value
        .as_array()
        .ok_or_else(|| parser_error("mexc", "cancel-all response is not an array", value))?;
    Ok(items
        .iter()
        .map(|item| {
            item.get("orderReports")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or_else(|| usize::from(item.get("orderId").is_some()))
        })
        .sum())
}

fn mexc_order_params(
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
    params.insert("type".to_string(), mexc_order_type(request)?.to_string());
    params.insert("quantity".to_string(), request.quantity.to_string());
    if let Some(price) = request.price {
        params.insert("price".to_string(), price.to_string());
    }
    if let Some(client_order_id) = &request.client_order_id {
        params.insert("newClientOrderId".to_string(), client_order_id.clone());
    }
    if let Some(tif) = request.time_in_force {
        params.insert("timeInForce".to_string(), tif_to_mexc(tif).to_string());
    }
    Ok(params)
}

fn mexc_quote_market_order_params(
    request: &QuoteMarketOrderRequest,
    symbol: &str,
) -> ExchangeClientResult<HashMap<String, String>> {
    if request.side != OrderSide::Buy {
        return Err(ExchangeClientError::Unsupported(
            "MEXC Spot quote-sized market orders are only supported for market buys".to_string(),
        ));
    }
    let mut params = HashMap::new();
    params.insert("symbol".to_string(), symbol.to_string());
    params.insert("side".to_string(), "BUY".to_string());
    params.insert("type".to_string(), "MARKET".to_string());
    params.insert(
        "quoteOrderQty".to_string(),
        request.quote_quantity.to_string(),
    );
    if let Some(client_order_id) = &request.client_order_id {
        params.insert("newClientOrderId".to_string(), client_order_id.clone());
    }
    Ok(params)
}

fn ensure_quote_market_client_order_id(
    request: &mut QuoteMarketOrderRequest,
) -> ExchangeClientResult<()> {
    if request.client_order_id.is_none() {
        request.client_order_id = Some(MexcSpotClient::generate_client_order_id());
    }
    if let Some(client_order_id) = &request.client_order_id {
        validate_client_order_id("mexc", request.market_type, client_order_id).map_err(
            |error| ExchangeClientError::Validation {
                field: "client_order_id",
                reason: error.to_string(),
            },
        )?;
    }
    Ok(())
}

fn ensure_amend_client_order_id(request: &AmendOrderRequest) -> ExchangeClientResult<()> {
    for client_order_id in [request.client_order_id(), request.new_client_order_id()]
        .into_iter()
        .flatten()
    {
        validate_client_order_id("mexc", request.market_type, client_order_id).map_err(
            |error| ExchangeClientError::Validation {
                field: "client_order_id",
                reason: error.to_string(),
            },
        )?;
    }
    Ok(())
}

fn ensure_cancel_client_order_id(request: &CancelOrderRequest) -> ExchangeClientResult<()> {
    if let Some(client_order_id) = request.client_order_id() {
        validate_client_order_id("mexc", request.market_type, client_order_id).map_err(
            |error| ExchangeClientError::Validation {
                field: "client_order_id",
                reason: error.to_string(),
            },
        )?;
    }
    Ok(())
}

fn mexc_order_type(request: &OrderRequest) -> ExchangeClientResult<&'static str> {
    match request.order_type {
        OrderType::Market => Ok("MARKET"),
        OrderType::Limit => Ok("LIMIT"),
        OrderType::PostOnly => Ok("LIMIT_MAKER"),
        OrderType::IOC | OrderType::FOK => Ok("LIMIT"),
    }
}

fn tif_to_mexc(tif: TimeInForce) -> &'static str {
    match tif {
        TimeInForce::GTC => "GTC",
        TimeInForce::IOC => "IOC",
        TimeInForce::FOK => "FOK",
        TimeInForce::GTX => "GTX",
    }
}

fn fallback_rule(symbol: &str) -> SymbolRule {
    let (base, quote) =
        split_compact_symbol(symbol).unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
    SymbolRule {
        exchange: "mexc".to_string(),
        market_type: MarketType::Spot,
        internal_symbol: symbol.to_string(),
        exchange_symbol: symbol.to_string(),
        base_asset: base,
        quote_asset: quote,
        price_precision: 8,
        quantity_precision: 8,
        tick_size: 0.00000001,
        step_size: 0.00000001,
        min_quantity: 0.0,
        min_notional: 0.0,
        max_quantity: None,
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
        status: SymbolStatus::Trading,
        raw_metadata: None,
    }
}

fn parse_fee_rate(value: &Value) -> ExchangeClientResult<FeeRate> {
    let item = value
        .as_array()
        .and_then(|items| items.first())
        .or_else(|| {
            value
                .get("data")
                .and_then(Value::as_array)
                .and_then(|items| items.first())
        })
        .or_else(|| if value.is_object() { Some(value) } else { None })
        .ok_or_else(|| parser_error("mexc", "fee response missing data", value))?;
    Ok(FeeRate::new(
        number_from_str(item.get("makerCommission"))
            .or_else(|| number_from_str(item.get("maker")))
            .unwrap_or(0.0),
        number_from_str(item.get("takerCommission"))
            .or_else(|| number_from_str(item.get("taker")))
            .unwrap_or(0.0),
        FeeRateSource::ExchangeApi,
    ))
}

async fn parse_response(response: reqwest::Response) -> ExchangeClientResult<Value> {
    let status = response.status();
    let value: Value = response.json().await.map_err(CoreExchangeError::from)?;
    if !status.is_success() {
        let code = value.get("code").and_then(Value::as_i64);
        let message = value
            .get("msg")
            .or_else(|| value.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("MEXC request failed");
        return Err(ExchangeClientError::Classified(ExchangeError {
            exchange: "mexc".to_string(),
            class: classify_mexc_error(code, message),
            code: code.map(|code| code.to_string()),
            message: message.to_string(),
        }));
    }
    if value
        .get("code")
        .and_then(Value::as_i64)
        .is_some_and(|code| code != 0)
    {
        let code = value.get("code").and_then(Value::as_i64);
        let message = value
            .get("msg")
            .or_else(|| value.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("MEXC request failed");
        return Err(ExchangeClientError::Classified(ExchangeError {
            exchange: "mexc".to_string(),
            class: classify_mexc_error(code, message),
            code: code.map(|code| code.to_string()),
            message: message.to_string(),
        }));
    }
    Ok(value)
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
                .ok_or_else(|| parser_error("mexc", "invalid orderbook level", level))?;
            Ok(OrderBookLevel {
                price: number_from_str(array.first()).unwrap_or(0.0),
                quantity: number_from_str(array.get(1)).unwrap_or(0.0),
            })
        })
        .collect()
}

fn parse_side(value: &str) -> ExchangeClientResult<OrderSide> {
    match value.to_ascii_uppercase().as_str() {
        "BUY" => Ok(OrderSide::Buy),
        "SELL" => Ok(OrderSide::Sell),
        _ => Err(parser_error(
            "mexc",
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
        _ => OrderType::Limit,
    }
}

fn average_price(value: &Value, filled_quantity: f64) -> Option<f64> {
    number_from_str(value.get("cummulativeQuoteQty"))
        .filter(|quote| filled_quantity > 0.0 && *quote > 0.0)
        .map(|quote| quote / filled_quantity)
}

fn required_str<'a>(value: &'a Value, field: &str) -> ExchangeClientResult<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| parser_error("mexc", &format!("missing field {field}"), value))
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

fn split_compact_symbol(symbol: &str) -> Option<(String, String)> {
    for quote in ["USDT", "USDC", "BTC", "ETH", "USD"] {
        if let Some(base) = symbol.strip_suffix(quote) {
            if !base.is_empty() {
                return Some((base.to_string(), quote.to_string()));
            }
        }
    }
    None
}

fn normalize_depth(depth: u16) -> u16 {
    match depth {
        0..=5 => 5,
        6..=10 => 10,
        11..=20 => 20,
        _ => 50,
    }
}

fn parser_error(exchange: &str, message: &str, value: &Value) -> ExchangeClientError {
    ExchangeClientError::Classified(ExchangeError {
        exchange: exchange.to_string(),
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
fn default_recv_window_ms() -> u64 {
    DEFAULT_RECV_WINDOW_MS
}
fn default_stale_book_ms() -> u64 {
    DEFAULT_STALE_BOOK_MS
}
fn default_reconnect_interval_ms() -> u64 {
    DEFAULT_RECONNECT_INTERVAL_MS
}
fn default_max_reconnect_attempts() -> u32 {
    10
}
fn default_order_timeout_ms() -> u64 {
    DEFAULT_ORDER_TIMEOUT_MS
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

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    #[derive(Debug, Clone)]
    struct SeenMexcRequest {
        method: String,
        path: String,
        query: HashMap<String, String>,
        headers: HashMap<String, String>,
    }

    async fn spawn_mexc_rest_server(
        responses: Vec<Value>,
    ) -> (String, Arc<Mutex<Vec<SeenMexcRequest>>>) {
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
                let request = parse_seen_mexc_request(&request_text);
                seen_requests.lock().unwrap().push(request);
                let body = responses
                    .lock()
                    .unwrap()
                    .next()
                    .unwrap_or_else(|| serde_json::json!({}));
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

    fn parse_seen_mexc_request(request_text: &str) -> SeenMexcRequest {
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

        SeenMexcRequest {
            method,
            path: path.to_string(),
            query,
            headers,
        }
    }

    fn assert_signed_mexc_request(request: &SeenMexcRequest, method: &str, path: &str) {
        assert_eq!(request.method, method);
        assert_eq!(request.path, path);
        assert_eq!(
            request.headers.get("x-mexc-apikey").map(String::as_str),
            Some("key")
        );
        assert!(request
            .query
            .get("timestamp")
            .is_some_and(|value| !value.is_empty()));
        assert_eq!(
            request.query.get("recvWindow").map(String::as_str),
            Some("5000")
        );
        assert!(request
            .query
            .get("signature")
            .is_some_and(|value| !value.is_empty()));
    }

    #[test]
    fn mexc_request_signing_should_match_known_hmac() {
        let query = "symbol=BTCUSDT&side=BUY&type=LIMIT&quantity=1&price=11&recvWindow=5000&timestamp=1644489390087";
        let expected = SignatureHelper::hmac_sha256("secret", query);
        assert_eq!(sign_query("secret", query), expected);
    }

    #[test]
    fn mexc_capabilities_should_advertise_cancel_all() {
        let client = MexcSpotClient::new(MexcSpotConfig::default());
        let capabilities = client.capabilities();
        assert_eq!(capabilities.market_type, MarketType::Spot);
        assert!(capabilities.supports_cancel_all_orders);
        assert!(capabilities.supports_quote_market_order);
        assert!(!capabilities.supports_amend_order);
        assert!(!capabilities.supports_order_list);
        assert!(capabilities.supports_open_orders);
        assert!(capabilities.supports_fee_api);
    }

    #[test]
    fn mexc_symbol_rule_parsing_should_extract_filters() {
        let value = serde_json::json!({"symbols":[{"symbol":"DKAUSDT","status":"TRADING","baseAsset":"DKA","quoteAsset":"USDT","filters":[{"filterType":"PRICE_FILTER","tickSize":"0.0001"},{"filterType":"LOT_SIZE","minQty":"1","maxQty":"100000","stepSize":"1"},{"filterType":"MIN_NOTIONAL","minNotional":"5"}]}]});
        let rules = parse_symbol_rules(&value).unwrap();
        assert_eq!(rules[0].internal_symbol, "DKAUSDT");
        assert_eq!(rules[0].tick_size, 0.0001);
        assert_eq!(rules[0].step_size, 1.0);
        assert_eq!(rules[0].min_notional, 5.0);
    }

    #[test]
    fn mexc_balance_parsing_should_normalize_nonzero_assets() {
        let value = serde_json::json!({"balances":[{"asset":"USDT","free":"10","locked":"2"},{"asset":"BTC","free":"0","locked":"0"}]});
        let snapshot = parse_balance_snapshot(&value).unwrap();
        assert_eq!(snapshot.balances.len(), 1);
        assert_eq!(snapshot.balances[0].effective_available, 10.0);
        assert_eq!(snapshot.balances[0].locked_by_exchange, 2.0);
    }

    #[test]
    fn mexc_orderbook_parsing_should_capture_best_bid_ask_and_sequence() {
        let value =
            serde_json::json!({"lastUpdateId": 42, "bids":[["99","1"]], "asks":[["100","2"]]});
        let book = parse_orderbook_snapshot(&value, "BTCUSDT", 1_000).unwrap();
        assert_eq!(book.best_bid, Some(99.0));
        assert_eq!(book.best_ask, Some(100.0));
        assert_eq!(book.sequence, Some(42));
    }

    #[tokio::test]
    async fn mexc_orderbook_should_validate_depth_before_request() {
        let client = MexcSpotClient::new(MexcSpotConfig::default());

        let err = client.get_orderbook("BTCUSDT", 0).await.unwrap_err();

        assert!(matches!(
            err,
            ExchangeClientError::Validation { field: "depth", .. }
        ));
    }

    #[tokio::test]
    async fn mexc_get_order_should_validate_order_id_before_request() {
        let client = MexcSpotClient::new(MexcSpotConfig::default());

        let err = client.get_order("BTCUSDT", "   ").await.unwrap_err();

        assert!(matches!(
            err,
            ExchangeClientError::Validation {
                field: "order_id",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn mexc_spot_client_should_route_common_private_rest_readbacks() {
        let (base_url, seen) = spawn_mexc_rest_server(vec![
            serde_json::json!({
                "balances": [
                    {"asset": "BTC", "free": "0.10000000", "locked": "0.02000000"},
                    {"asset": "USDT", "free": "123.45", "locked": "1.55"}
                ]
            }),
            serde_json::json!({
                "symbol": "BTCUSDT",
                "orderId": 1001,
                "clientOrderId": "CLIENT1",
                "price": "65000",
                "origQty": "0.01",
                "executedQty": "0.004",
                "cummulativeQuoteQty": "259.8",
                "status": "PARTIALLY_FILLED",
                "timeInForce": "GTC",
                "type": "LIMIT",
                "side": "BUY",
                "time": 1700000000000i64,
                "updateTime": 1700000001000i64
            }),
            serde_json::json!([{
                "symbol": "BTCUSDT",
                "orderId": 1002,
                "clientOrderId": "CLIENT2",
                "price": "70000",
                "origQty": "0.02",
                "executedQty": "0",
                "status": "NEW",
                "timeInForce": "GTC",
                "type": "LIMIT",
                "side": "SELL",
                "time": 1700000002000i64,
                "updateTime": 1700000002000i64
            }]),
            serde_json::json!([{
                "symbol": "BTCUSDT",
                "makerCommission": "0.0008",
                "takerCommission": "0.001"
            }]),
            serde_json::json!([{
                "symbol": "BTCUSDT",
                "id": 2001,
                "orderId": 1001,
                "clientOrderId": "CLIENT1",
                "price": "64950",
                "qty": "0.004",
                "commission": "0.2598",
                "commissionAsset": "USDT",
                "isBuyer": true,
                "time": 1700000001000i64
            }]),
        ])
        .await;
        let client = MexcSpotClient::new(MexcSpotConfig {
            api_key: "key".to_string(),
            api_secret: "secret".to_string(),
            base_url,
            dry_run: false,
            ..MexcSpotConfig::default()
        });

        let balances = client.get_balances().await.unwrap();
        assert_eq!(balances.exchange, "mexc");
        assert_eq!(balances.market_type, MarketType::Spot);
        assert_eq!(balances.balances.len(), 2);
        assert_eq!(balances.balances[0].asset, "BTC");
        assert_eq!(balances.balances[0].available, 0.1);
        assert_eq!(balances.balances[0].locked_by_exchange, 0.02);

        let order = client.get_order("BTCUSDT", "1001").await.unwrap();
        assert_eq!(order.exchange, "mexc");
        assert_eq!(order.market_type, MarketType::Spot);
        assert_eq!(order.symbol, "BTCUSDT");
        assert_eq!(order.order_id, "1001");
        assert_eq!(order.client_order_id.as_deref(), Some("CLIENT1"));
        assert_eq!(order.side, OrderSide::Buy);
        assert_eq!(order.status, OrderStatus::PartiallyFilled);
        assert_eq!(order.price, Some(65_000.0));
        assert_eq!(order.quantity, 0.01);
        assert_eq!(order.filled_quantity, 0.004);
        assert_eq!(order.average_price, Some(64_950.0));

        let open_orders = client.get_open_orders(Some("BTCUSDT")).await.unwrap();
        assert_eq!(open_orders.len(), 1);
        assert_eq!(open_orders[0].symbol, "BTCUSDT");
        assert_eq!(open_orders[0].order_id, "1002");
        assert_eq!(open_orders[0].client_order_id.as_deref(), Some("CLIENT2"));
        assert_eq!(open_orders[0].side, OrderSide::Sell);
        assert_eq!(open_orders[0].status, OrderStatus::New);
        assert_eq!(open_orders[0].price, Some(70_000.0));
        assert_eq!(open_orders[0].quantity, 0.02);
        assert_eq!(open_orders[0].filled_quantity, 0.0);
        assert_eq!(open_orders[0].average_price, None);

        let fee_rate = client.get_fee_rate("BTCUSDT").await.unwrap();
        assert_eq!(fee_rate.maker, 0.0008);
        assert_eq!(fee_rate.taker, 0.001);

        let fills = client.get_recent_fills("BTCUSDT").await.unwrap();
        assert_eq!(fills.len(), 1);
        let fill = &fills[0];
        assert_eq!(fill.exchange, "mexc");
        assert_eq!(fill.market_type, MarketType::Spot);
        assert_eq!(fill.symbol, "BTCUSDT");
        assert_eq!(fill.trade_id.as_deref(), Some("2001"));
        assert_eq!(fill.order_id.as_deref(), Some("1001"));
        assert_eq!(fill.client_order_id.as_deref(), Some("CLIENT1"));
        assert_eq!(fill.side, OrderSide::Buy);
        assert_eq!(fill.price, 64_950.0);
        assert_eq!(fill.quantity, 0.004);
        assert_eq!(fill.fee_asset.as_deref(), Some("USDT"));
        assert_eq!(fill.fee_amount, Some(0.2598));

        let requests = seen.lock().unwrap().clone();
        assert_eq!(requests.len(), 5);
        assert_signed_mexc_request(&requests[0], "GET", "/api/v3/account");
        assert!(requests[0].query.get("symbol").is_none());

        assert_signed_mexc_request(&requests[1], "GET", "/api/v3/order");
        assert_eq!(
            requests[1].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
        assert_eq!(
            requests[1].query.get("orderId").map(String::as_str),
            Some("1001")
        );

        assert_signed_mexc_request(&requests[2], "GET", "/api/v3/openOrders");
        assert_eq!(
            requests[2].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );

        assert_signed_mexc_request(&requests[3], "GET", "/api/v3/tradeFee");
        assert_eq!(
            requests[3].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );

        assert_signed_mexc_request(&requests[4], "GET", "/api/v3/myTrades");
        assert_eq!(
            requests[4].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
    }

    #[tokio::test]
    async fn mexc_spot_client_should_route_order_mutations() {
        let symbol_rule = serde_json::json!({
            "symbols": [{
                "symbol": "DKAUSDT",
                "status": "TRADING",
                "baseAsset": "DKA",
                "quoteAsset": "USDT",
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.0001"},
                    {"filterType": "LOT_SIZE", "minQty": "1", "maxQty": "100000", "stepSize": "1"},
                    {"filterType": "MIN_NOTIONAL", "minNotional": "5"}
                ]
            }]
        });
        let balances = serde_json::json!({
            "balances": [
                {"asset": "USDT", "free": "1000", "locked": "0"},
                {"asset": "DKA", "free": "100", "locked": "0"}
            ]
        });
        let fee = serde_json::json!([{
            "symbol": "DKAUSDT",
            "makerCommission": "0.0008",
            "takerCommission": "0.001"
        }]);
        let (base_url, seen) = spawn_mexc_rest_server(vec![
            symbol_rule.clone(),
            balances.clone(),
            fee.clone(),
            serde_json::json!({
                "symbol": "DKAUSDT",
                "orderId": 2001,
                "clientOrderId": "MXSPTLIMIT1",
                "price": "1.23",
                "origQty": "10",
                "executedQty": "0",
                "status": "NEW",
                "timeInForce": "GTC",
                "type": "LIMIT",
                "side": "BUY",
                "transactTime": 1700000000000i64
            }),
            symbol_rule.clone(),
            balances,
            serde_json::json!({
                "symbol": "DKAUSDT",
                "orderId": 2002,
                "clientOrderId": "MXSPTQUOTE1",
                "price": "0",
                "origQty": "25.5",
                "executedQty": "0",
                "status": "NEW",
                "type": "MARKET",
                "side": "BUY",
                "transactTime": 1700000001000i64
            }),
            serde_json::json!({
                "symbol": "DKAUSDT",
                "orderId": 2001,
                "clientOrderId": "MXSPTLIMIT1",
                "status": "CANCELED"
            }),
            serde_json::json!([
                {"symbol": "DKAUSDT", "orderId": 2002, "clientOrderId": "MXSPTQUOTE1", "status": "CANCELED"},
                {"symbol": "DKAUSDT", "orderId": 2003, "clientOrderId": "MXSPTSELL1", "status": "CANCELED"}
            ]),
        ])
        .await;
        let client = MexcSpotClient::new(MexcSpotConfig {
            api_key: "key".to_string(),
            api_secret: "secret".to_string(),
            base_url,
            dry_run: false,
            ..MexcSpotConfig::default()
        });

        let limit_order = client
            .place_order(OrderRequest {
                market_type: MarketType::Spot,
                symbol: "DKAUSDT".to_string(),
                side: OrderSide::Buy,
                position_side: PositionSide::None,
                order_type: OrderType::Limit,
                time_in_force: Some(TimeInForce::GTC),
                quantity: 10.0,
                price: Some(1.23),
                client_order_id: Some("MXSPTLIMIT1".to_string()),
                reduce_only: false,
            })
            .await
            .unwrap();
        assert_eq!(limit_order.exchange, "mexc");
        assert_eq!(limit_order.market_type, MarketType::Spot);
        assert_eq!(limit_order.symbol, "DKAUSDT");
        assert_eq!(limit_order.order_id, "2001");
        assert_eq!(limit_order.client_order_id.as_deref(), Some("MXSPTLIMIT1"));
        assert_eq!(limit_order.side, OrderSide::Buy);
        assert_eq!(limit_order.order_type, OrderType::Limit);
        assert_eq!(limit_order.status, OrderStatus::New);
        assert_eq!(limit_order.price, Some(1.23));
        assert_eq!(limit_order.quantity, 10.0);

        let mut quote_request = QuoteMarketOrderRequest::spot_buy("DKAUSDT", 25.5);
        quote_request.client_order_id = Some("MXSPTQUOTE1".to_string());
        let quote_order = client
            .place_quote_market_order(quote_request)
            .await
            .unwrap();
        assert_eq!(quote_order.exchange, "mexc");
        assert_eq!(quote_order.market_type, MarketType::Spot);
        assert_eq!(quote_order.symbol, "DKAUSDT");
        assert_eq!(quote_order.order_id, "2002");
        assert_eq!(quote_order.client_order_id.as_deref(), Some("MXSPTQUOTE1"));
        assert_eq!(quote_order.side, OrderSide::Buy);
        assert_eq!(quote_order.order_type, OrderType::Market);
        assert_eq!(quote_order.status, OrderStatus::New);

        let cancelled = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Spot,
                symbol: "DKAUSDT".to_string(),
                order_id: Some("2001".to_string()),
                client_order_id: Some("MXSPTLIMIT1".to_string()),
            })
            .await
            .unwrap();
        assert_eq!(cancelled.exchange, "mexc");
        assert_eq!(cancelled.market_type, MarketType::Spot);
        assert_eq!(cancelled.symbol, "DKAUSDT");
        assert_eq!(cancelled.order_id.as_deref(), Some("2001"));
        assert_eq!(cancelled.client_order_id.as_deref(), Some("MXSPTLIMIT1"));
        assert_eq!(cancelled.status, OrderStatus::Cancelled);

        let cancelled_all = client
            .cancel_all_orders(CancelAllOrdersRequest::for_symbol(
                MarketType::Spot,
                "DKAUSDT",
            ))
            .await
            .unwrap();
        assert_eq!(cancelled_all.exchange, "mexc");
        assert_eq!(cancelled_all.market_type, MarketType::Spot);
        assert_eq!(cancelled_all.symbol.as_deref(), Some("DKAUSDT"));
        assert_eq!(cancelled_all.cancelled_orders, 2);

        let capabilities = client.capabilities();
        assert!(!capabilities.supports_amend_order);
        assert!(!capabilities.supports_order_list);
        let amend_error = client
            .amend_order(AmendOrderRequest::reduce_quantity_by_order_id(
                MarketType::Spot,
                "DKAUSDT",
                "2001",
                5.0,
            ))
            .await
            .unwrap_err();
        assert!(matches!(
            amend_error,
            ExchangeClientError::Unsupported(message)
                if message.contains("does not expose a native amend")
        ));

        let requests = seen.lock().unwrap().clone();
        assert_eq!(requests.len(), 9);

        assert_eq!(requests[0].method, "GET");
        assert_eq!(requests[0].path, "/api/v3/exchangeInfo");
        assert!(requests[0].query.is_empty());

        assert_signed_mexc_request(&requests[1], "GET", "/api/v3/account");
        assert!(requests[1].query.get("symbol").is_none());

        assert_signed_mexc_request(&requests[2], "GET", "/api/v3/tradeFee");
        assert_eq!(
            requests[2].query.get("symbol").map(String::as_str),
            Some("DKAUSDT")
        );

        assert_signed_mexc_request(&requests[3], "POST", "/api/v3/order");
        assert_eq!(
            requests[3].query.get("symbol").map(String::as_str),
            Some("DKAUSDT")
        );
        assert_eq!(
            requests[3].query.get("side").map(String::as_str),
            Some("BUY")
        );
        assert_eq!(
            requests[3].query.get("type").map(String::as_str),
            Some("LIMIT")
        );
        assert_eq!(
            requests[3].query.get("quantity").map(String::as_str),
            Some("10")
        );
        assert_eq!(
            requests[3].query.get("price").map(String::as_str),
            Some("1.23")
        );
        assert_eq!(
            requests[3].query.get("timeInForce").map(String::as_str),
            Some("GTC")
        );
        assert_eq!(
            requests[3]
                .query
                .get("newClientOrderId")
                .map(String::as_str),
            Some("MXSPTLIMIT1")
        );

        assert_eq!(requests[4].method, "GET");
        assert_eq!(requests[4].path, "/api/v3/exchangeInfo");
        assert!(requests[4].query.is_empty());

        assert_signed_mexc_request(&requests[5], "GET", "/api/v3/account");
        assert!(requests[5].query.get("symbol").is_none());

        assert_signed_mexc_request(&requests[6], "POST", "/api/v3/order");
        assert_eq!(
            requests[6].query.get("symbol").map(String::as_str),
            Some("DKAUSDT")
        );
        assert_eq!(
            requests[6].query.get("side").map(String::as_str),
            Some("BUY")
        );
        assert_eq!(
            requests[6].query.get("type").map(String::as_str),
            Some("MARKET")
        );
        assert_eq!(
            requests[6].query.get("quoteOrderQty").map(String::as_str),
            Some("25.5")
        );
        assert_eq!(
            requests[6]
                .query
                .get("newClientOrderId")
                .map(String::as_str),
            Some("MXSPTQUOTE1")
        );
        assert!(!requests[6].query.contains_key("quantity"));

        assert_signed_mexc_request(&requests[7], "DELETE", "/api/v3/order");
        assert_eq!(
            requests[7].query.get("symbol").map(String::as_str),
            Some("DKAUSDT")
        );
        assert_eq!(
            requests[7].query.get("orderId").map(String::as_str),
            Some("2001")
        );
        assert_eq!(
            requests[7]
                .query
                .get("origClientOrderId")
                .map(String::as_str),
            Some("MXSPTLIMIT1")
        );

        assert_signed_mexc_request(&requests[8], "DELETE", "/api/v3/openOrders");
        assert_eq!(
            requests[8].query.get("symbol").map(String::as_str),
            Some("DKAUSDT")
        );
    }

    #[test]
    fn mexc_order_status_mapping_should_cover_spot_statuses() {
        assert_eq!(map_mexc_order_status("NEW"), OrderStatus::New);
        assert_eq!(
            map_mexc_order_status("PARTIALLY_FILLED"),
            OrderStatus::PartiallyFilled
        );
        assert_eq!(map_mexc_order_status("FILLED"), OrderStatus::Filled);
        assert_eq!(map_mexc_order_status("CANCELED"), OrderStatus::Cancelled);
    }

    #[test]
    fn mexc_cancel_all_should_count_orders_and_order_lists() {
        let value = serde_json::json!([
            {"symbol":"BTCUSDT","orderId":1,"clientOrderId":"plain","status":"CANCELED"},
            {"orderListId":2,"symbol":"BTCUSDT","orderReports":[
                {"symbol":"BTCUSDT","orderId":3,"status":"CANCELED"},
                {"symbol":"BTCUSDT","orderId":4,"status":"CANCELED"}
            ]}
        ]);
        assert_eq!(mexc_cancel_all_cancelled_count(&value).unwrap(), 3);
    }

    #[test]
    fn mexc_quote_market_order_params_should_use_quote_order_qty_for_buy() {
        let mut request = QuoteMarketOrderRequest::spot_buy("DKAUSDT", 25.5);
        request.client_order_id = Some("ldry-mexc-quote-1".to_string());

        let params = mexc_quote_market_order_params(&request, "DKAUSDT").unwrap();

        assert_eq!(params.get("symbol").map(String::as_str), Some("DKAUSDT"));
        assert_eq!(params.get("side").map(String::as_str), Some("BUY"));
        assert_eq!(params.get("type").map(String::as_str), Some("MARKET"));
        assert_eq!(
            params.get("quoteOrderQty").map(String::as_str),
            Some("25.5")
        );
        assert!(!params.contains_key("quantity"));
        assert_eq!(
            params.get("newClientOrderId").map(String::as_str),
            Some("ldry-mexc-quote-1")
        );

        let sell = QuoteMarketOrderRequest {
            side: OrderSide::Sell,
            ..request
        };
        assert!(mexc_quote_market_order_params(&sell, "DKAUSDT").is_err());
    }

    #[tokio::test]
    async fn mexc_place_order_should_ack_in_dry_run_without_symbol_rule_readback() {
        let client = MexcSpotClient::new(MexcSpotConfig {
            dry_run: true,
            ..MexcSpotConfig::default()
        });

        let response = client
            .place_order(OrderRequest {
                market_type: MarketType::Spot,
                symbol: "DKAUSDT".to_string(),
                side: OrderSide::Buy,
                position_side: PositionSide::None,
                order_type: OrderType::Limit,
                time_in_force: Some(TimeInForce::GTC),
                quantity: 10.0,
                price: Some(1.23),
                client_order_id: None,
                reduce_only: false,
            })
            .await
            .unwrap();

        assert_eq!(response.exchange, "mexc");
        assert_eq!(response.market_type, MarketType::Spot);
        assert_eq!(response.symbol, "DKAUSDT");
        assert_eq!(response.side, OrderSide::Buy);
        assert_eq!(response.order_type, OrderType::Limit);
        assert_eq!(response.price, Some(1.23));
        assert_eq!(response.quantity, 10.0);
        assert_eq!(response.status, OrderStatus::New);
        assert!(response.client_order_id.is_some());
        assert!(response.order_id.starts_with("dry-mexc-"));
    }

    #[tokio::test]
    async fn mexc_quote_market_order_should_ack_in_dry_run() {
        let client = MexcSpotClient::new(MexcSpotConfig {
            dry_run: true,
            ..MexcSpotConfig::default()
        });

        let response = client
            .place_quote_market_order(QuoteMarketOrderRequest::spot_buy("DKAUSDT", 25.5))
            .await
            .unwrap();

        assert_eq!(response.exchange, "mexc");
        assert_eq!(response.symbol, "DKAUSDT");
        assert_eq!(response.side, OrderSide::Buy);
        assert_eq!(response.order_type, OrderType::Market);
        assert_eq!(response.quantity, 25.5);
        assert!(response.client_order_id.is_some());
    }

    #[tokio::test]
    async fn mexc_cancel_order_should_validate_market_type_in_dry_run() {
        let client = MexcSpotClient::new(MexcSpotConfig {
            dry_run: true,
            ..MexcSpotConfig::default()
        });

        let response = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Spot,
                symbol: "DKAUSDT".to_string(),
                order_id: Some("1001".to_string()),
                client_order_id: None,
            })
            .await
            .unwrap();
        assert_eq!(response.exchange, "mexc");
        assert_eq!(response.market_type, MarketType::Spot);
        assert_eq!(response.symbol, "DKAUSDT");
        assert_eq!(response.order_id.as_deref(), Some("1001"));

        let response = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Spot,
                symbol: "DKAUSDT".to_string(),
                order_id: Some("   ".to_string()),
                client_order_id: Some("CANCELCLIENT1".to_string()),
            })
            .await
            .unwrap();
        assert_eq!(response.order_id, None);
        assert_eq!(response.client_order_id.as_deref(), Some("CANCELCLIENT1"));

        let error = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Perpetual,
                symbol: "DKAUSDT".to_string(),
                order_id: Some("1001".to_string()),
                client_order_id: None,
            })
            .await
            .unwrap_err();
        assert!(error.to_string().contains("only supports MarketType::Spot"));

        let error = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Spot,
                symbol: "DKAUSDT".to_string(),
                order_id: None,
                client_order_id: Some("bad/id".to_string()),
            })
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ExchangeClientError::Validation {
                field: "client_order_id",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn mexc_cancel_all_orders_should_ack_and_validate_market_type_in_dry_run() {
        let client = MexcSpotClient::new(MexcSpotConfig {
            dry_run: true,
            ..MexcSpotConfig::default()
        });

        let response = client
            .cancel_all_orders(CancelAllOrdersRequest::for_symbol(
                MarketType::Spot,
                "DKAUSDT",
            ))
            .await
            .unwrap();
        assert_eq!(response.exchange, "mexc");
        assert_eq!(response.market_type, MarketType::Spot);
        assert_eq!(response.symbol.as_deref(), Some("DKAUSDT"));
        assert_eq!(response.cancelled_orders, 0);

        let error = client
            .cancel_all_orders(CancelAllOrdersRequest::for_symbol(
                MarketType::Perpetual,
                "DKAUSDT",
            ))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("only supports MarketType::Spot"));
    }

    #[tokio::test]
    async fn mexc_order_list_should_validate_before_unsupported() {
        use crate::exchanges::unified::{
            OrderListConditionalLeg, OrderListLegType, OrderListRequest,
        };

        fn oco_request() -> OrderListRequest {
            OrderListRequest::Oco {
                market_type: MarketType::Spot,
                symbol: "DKAUSDT".to_string(),
                list_client_order_id: Some("MEXCOCOLIST1".to_string()),
                side: OrderSide::Sell,
                quantity: 5.0,
                above: OrderListConditionalLeg {
                    order_type: OrderListLegType::LimitMaker,
                    price: Some(2.0),
                    stop_price: None,
                    time_in_force: None,
                    client_order_id: Some("MEXCABOVE1".to_string()),
                },
                below: OrderListConditionalLeg {
                    order_type: OrderListLegType::StopLossLimit,
                    price: Some(0.8),
                    stop_price: Some(0.9),
                    time_in_force: Some(TimeInForce::GTC),
                    client_order_id: Some("MEXCBELOW1".to_string()),
                },
            }
        }

        let client = MexcSpotClient::new(MexcSpotConfig {
            dry_run: false,
            base_url: "http://127.0.0.1:9".to_string(),
            ..MexcSpotConfig::default()
        });
        assert!(!client.capabilities().supports_order_list);

        assert!(matches!(
            client.place_order_list(oco_request()).await,
            Err(ExchangeClientError::Unsupported(message))
                if message.contains("order lists are not implemented")
        ));

        let mut invalid_market_type = oco_request();
        if let OrderListRequest::Oco { market_type, .. } = &mut invalid_market_type {
            *market_type = MarketType::Perpetual;
        }
        assert!(matches!(
            client.place_order_list(invalid_market_type).await,
            Err(ExchangeClientError::Validation {
                field: "market_type",
                ..
            })
        ));

        let mut empty_symbol = oco_request();
        if let OrderListRequest::Oco { symbol, .. } = &mut empty_symbol {
            symbol.clear();
        }
        assert!(matches!(
            client.place_order_list(empty_symbol).await,
            Err(ExchangeClientError::Validation {
                field: "symbol",
                ..
            })
        ));

        let mut missing_limit_price = oco_request();
        if let OrderListRequest::Oco { above, .. } = &mut missing_limit_price {
            above.price = None;
        }
        assert!(matches!(
            client.place_order_list(missing_limit_price).await,
            Err(ExchangeClientError::Validation { field: "price", .. })
        ));

        let mut missing_stop_price = oco_request();
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

        let mut invalid_list_client_id = oco_request();
        if let OrderListRequest::Oco {
            list_client_order_id,
            ..
        } = &mut invalid_list_client_id
        {
            *list_client_order_id = Some("bad/id".to_string());
        }
        assert!(matches!(
            client.place_order_list(invalid_list_client_id).await,
            Err(ExchangeClientError::Validation {
                field: "client_order_id",
                ..
            })
        ));

        let mut invalid_leg_client_id = oco_request();
        if let OrderListRequest::Oco { below, .. } = &mut invalid_leg_client_id {
            below.client_order_id = Some("bad/id".to_string());
        }
        assert!(matches!(
            client.place_order_list(invalid_leg_client_id).await,
            Err(ExchangeClientError::Validation {
                field: "client_order_id",
                ..
            })
        ));
    }

    #[test]
    fn mexc_spot_private_stream_parser_should_parse_order_fill_balance_and_ignore_control() {
        let order_events = parse_private_stream_message(
            r#"{
                "channel":"spot@private.orders.v3.api@BTCUSDT",
                "data":[{
                    "symbol":"BTCUSDT",
                    "orderId":"order-1",
                    "clientOrderId":"client-1",
                    "side":"BUY",
                    "type":"LIMIT",
                    "status":"PARTIALLY_FILLED",
                    "price":"65000",
                    "origQty":"0.02",
                    "executedQty":"0.01",
                    "cummulativeQuoteQty":"650.1",
                    "transactTime":1700000000000,
                    "updateTime":1700000000100
                }]
            }"#,
        )
        .unwrap();
        match &order_events[0] {
            UserStreamEvent::Order(order) => {
                assert_eq!(order.exchange, "mexc");
                assert_eq!(order.market_type, MarketType::Spot);
                assert_eq!(order.symbol, "BTCUSDT");
                assert_eq!(order.order_id, "order-1");
                assert_eq!(order.client_order_id.as_deref(), Some("client-1"));
                assert_eq!(order.side, OrderSide::Buy);
                assert_eq!(order.position_side, PositionSide::None);
                assert_eq!(order.order_type, OrderType::Limit);
                assert_eq!(order.status, OrderStatus::PartiallyFilled);
                assert_eq!(order.price, Some(65_000.0));
                assert_eq!(order.quantity, 0.02);
                assert_eq!(order.filled_quantity, 0.01);
                assert_eq!(order.average_price, Some(65_010.0));
            }
            other => panic!("expected MEXC order event, got {other:?}"),
        }

        let fill_events = parse_private_stream_message(
            r#"{
                "method":"spot.private.deals",
                "d":{
                    "symbol":"BTCUSDT",
                    "id":"trade-1",
                    "orderId":"order-1",
                    "clientOrderId":"client-1",
                    "isBuyer":false,
                    "price":"66000",
                    "qty":"0.03",
                    "commissionAsset":"BTC",
                    "commission":"0.00003",
                    "time":1700000000200
                }
            }"#,
        )
        .unwrap();
        match &fill_events[0] {
            UserStreamEvent::Fill(fill) => {
                assert_eq!(fill.exchange, "mexc");
                assert_eq!(fill.market_type, MarketType::Spot);
                assert_eq!(fill.symbol, "BTCUSDT");
                assert_eq!(fill.trade_id.as_deref(), Some("trade-1"));
                assert_eq!(fill.order_id.as_deref(), Some("order-1"));
                assert_eq!(fill.client_order_id.as_deref(), Some("client-1"));
                assert_eq!(fill.side, OrderSide::Sell);
                assert_eq!(fill.price, 66_000.0);
                assert_eq!(fill.quantity, 0.03);
                assert_eq!(fill.fee_asset.as_deref(), Some("BTC"));
                assert_eq!(fill.fee_amount, Some(0.00003));
            }
            other => panic!("expected MEXC fill event, got {other:?}"),
        }

        let balance_events = parse_private_stream_message(
            r#"{
                "channel":"spot.private.account",
                "params":{
                    "asset":"USDT",
                    "free":"80",
                    "locked":"20"
                }
            }"#,
        )
        .unwrap();
        match &balance_events[0] {
            UserStreamEvent::Balance(snapshot) => {
                assert_eq!(snapshot.exchange, "mexc");
                assert_eq!(snapshot.market_type, MarketType::Spot);
                assert_eq!(snapshot.balances.len(), 1);
                let balance = &snapshot.balances[0];
                assert_eq!(balance.asset, "USDT");
                assert_eq!(balance.total, 100.0);
                assert_eq!(balance.available, 80.0);
                assert_eq!(balance.locked, 20.0);
                assert_eq!(balance.locked_by_exchange, 20.0);
            }
            other => panic!("expected MEXC balance event, got {other:?}"),
        }

        assert!(
            parse_private_stream_message(r#"{"method":"login","success":true}"#)
                .unwrap()
                .is_empty()
        );
        assert!(
            parse_private_stream_message(r#"{"code":0,"msg":"subscribed"}"#)
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn mexc_amend_order_should_be_explicitly_unsupported() {
        let client = MexcSpotClient::new(MexcSpotConfig {
            dry_run: true,
            ..MexcSpotConfig::default()
        });

        let error = client
            .amend_order(AmendOrderRequest::reduce_quantity_by_order_id(
                MarketType::Spot,
                "DKAUSDT",
                "1001",
                5.0,
            ))
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            ExchangeClientError::Unsupported(message)
                if message.contains("does not expose a native amend")
        ));

        let error = client
            .amend_order(AmendOrderRequest {
                market_type: MarketType::Spot,
                symbol: "DKAUSDT".to_string(),
                order_id: Some("1001".to_string()),
                client_order_id: Some("   ".to_string()),
                new_client_order_id: Some("   ".to_string()),
                new_quantity: 4.0,
            })
            .await
            .unwrap_err();
        assert!(matches!(error, ExchangeClientError::Unsupported(_)));

        let error = client
            .amend_order(AmendOrderRequest {
                market_type: MarketType::Spot,
                symbol: "DKAUSDT".to_string(),
                order_id: Some("1001".to_string()),
                client_order_id: None,
                new_client_order_id: Some("LDRYMEXCNEW".to_string()),
                new_quantity: 4.0,
            })
            .await
            .unwrap_err();
        assert!(matches!(error, ExchangeClientError::Unsupported(_)));

        let error = client
            .amend_order(AmendOrderRequest {
                market_type: MarketType::Spot,
                symbol: "DKAUSDT".to_string(),
                order_id: None,
                client_order_id: Some("bad/id".to_string()),
                new_client_order_id: None,
                new_quantity: 4.0,
            })
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ExchangeClientError::Validation {
                field: "client_order_id",
                ..
            }
        ));

        let error = client
            .amend_order(AmendOrderRequest {
                market_type: MarketType::Spot,
                symbol: "DKAUSDT".to_string(),
                order_id: Some("1001".to_string()),
                client_order_id: None,
                new_client_order_id: Some("bad/id".to_string()),
                new_quantity: 4.0,
            })
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ExchangeClientError::Validation {
                field: "client_order_id",
                ..
            }
        ));

        let error = client
            .amend_order(AmendOrderRequest::reduce_quantity_by_order_id(
                MarketType::Perpetual,
                "DKAUSDT",
                "1001",
                5.0,
            ))
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ExchangeClientError::Validation {
                field: "market_type",
                ..
            }
        ));

        let error = client
            .amend_order(AmendOrderRequest {
                market_type: MarketType::Perpetual,
                symbol: "DKAUSDT".to_string(),
                order_id: Some("1001".to_string()),
                client_order_id: None,
                new_client_order_id: Some("LDRYMEXCNEW".to_string()),
                new_quantity: 4.0,
            })
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ExchangeClientError::Validation {
                field: "market_type",
                ..
            }
        ));
    }

    #[test]
    fn mexc_fill_parsing_should_normalize_trade() {
        let value = serde_json::json!({"symbol":"BTCUSDT","id":1,"orderId":2,"price":"100","qty":"0.01","commission":"0.1","commissionAsset":"USDT","isBuyer":true,"time":1700000000000i64});
        let fill = parse_fill(&value).unwrap();
        assert_eq!(fill.side, OrderSide::Buy);
        assert_eq!(fill.price, 100.0);
        assert_eq!(fill.quantity, 0.01);
    }

    #[test]
    fn mexc_min_notional_tick_and_step_validation_should_reject_invalid_orders() {
        let rule = SymbolRule {
            exchange: "mexc".to_string(),
            market_type: MarketType::Spot,
            internal_symbol: "DKAUSDT".to_string(),
            exchange_symbol: "DKAUSDT".to_string(),
            base_asset: "DKA".to_string(),
            quote_asset: "USDT".to_string(),
            price_precision: 4,
            quantity_precision: 0,
            tick_size: 0.0001,
            step_size: 1.0,
            min_quantity: 1.0,
            min_notional: 5.0,
            max_quantity: None,
            supported_order_types: vec![OrderType::Limit],
            supported_time_in_force: vec![TimeInForce::GTC],
            status: SymbolStatus::Trading,
            raw_metadata: None,
        };
        let mut order = OrderRequest {
            market_type: MarketType::Spot,
            symbol: "DKAUSDT".to_string(),
            side: OrderSide::Buy,
            position_side: PositionSide::None,
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: 1.5,
            price: Some(1.0),
            client_order_id: None,
            reduce_only: false,
        };
        assert!(validate_order_against_symbol_rule(&order, &rule).is_err());
        order.quantity = 1.0;
        order.price = Some(1.00005);
        assert!(validate_order_against_symbol_rule(&order, &rule).is_err());
        order.price = Some(1.0);
        assert!(validate_order_against_symbol_rule(&order, &rule).is_err());
        order.quantity = 5.0;
        assert!(validate_order_against_symbol_rule(&order, &rule).is_ok());
    }

    #[test]
    fn mexc_error_classification_should_map_oversold_rate_limit_and_invalid_symbol() {
        assert_eq!(
            classify_mexc_error(Some(30005), "Oversold"),
            ExchangeErrorClass::Oversold
        );
        assert_eq!(
            classify_mexc_error(Some(10007), "Too many requests"),
            ExchangeErrorClass::RateLimited
        );
        assert_eq!(
            classify_mexc_error(Some(10101), "Invalid symbol"),
            ExchangeErrorClass::InvalidSymbol
        );
    }

    #[test]
    fn mexc_rounding_helpers_should_be_deterministic() {
        assert_eq!(round_price_to_tick(1.23456, 0.01, false), 1.23);
        assert_eq!(round_quantity_to_step(1.23456, 0.001, false), 1.234);
    }

    #[tokio::test]
    #[ignore]
    async fn mexc_live_health_check_requires_credentials() {
        let api_key = std::env::var("MEXC_API_KEY")
            .expect("MEXC_API_KEY is required for ignored live MEXC tests");
        let api_secret = std::env::var("MEXC_API_SECRET")
            .expect("MEXC_API_SECRET is required for ignored live MEXC tests");
        let client = MexcSpotClient::new(MexcSpotConfig {
            api_key,
            api_secret,
            dry_run: true,
            ..MexcSpotConfig::default()
        });
        let health = client.health_check().await.unwrap();
        assert_eq!(health.exchange, "mexc");
    }

    #[test]
    #[ignore]
    fn mexc_live_order_tests_are_double_gated() {
        assert_eq!(
            std::env::var("ENABLE_LIVE_ORDER_TESTS").ok().as_deref(),
            Some("true"),
            "live order tests require ENABLE_LIVE_ORDER_TESTS=true and must be implemented with tiny dry-run-safe orders"
        );
    }
}
