use std::collections::HashMap;
use std::time::Duration as StdDuration;

use async_trait::async_trait;
use base64::{engine::general_purpose, Engine as _};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::Sha256;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::Message;

use crate::core::error::ExchangeError as CoreExchangeError;
use crate::core::ws_connect::connect_async;
use crate::exchanges::client_order_id::{generate_client_order_id, validate_client_order_id};
use crate::exchanges::spot_reservation::{BalanceReservation, BalanceReservationManager};
use crate::exchanges::unified::{
    validate_order_lookup_id, validate_orderbook_depth, AmendOrderRequest, AssetBalance,
    BalanceSnapshot, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeClient, ExchangeClientCapabilities, ExchangeClientError,
    ExchangeClientResult, ExchangeError, ExchangeErrorClass, ExchangeHealthStatus, FeeRate,
    FeeRateSource, LiquidityRole, MarketType, OrderBookLevel, OrderBookSnapshot, OrderRequest,
    OrderResponse, OrderSide, OrderStatus, OrderType, PositionSide, QuoteMarketOrderRequest,
    SymbolRule, SymbolStatus, TimeInForce, TradeFill, UserStreamEvent,
};

type HmacSha256 = Hmac<Sha256>;

const DEFAULT_REST_BASE_URL: &str = "https://api.kucoin.com";
const DEFAULT_WS_URL: &str = "wss://ws-api-spot.kucoin.com";
const DEFAULT_STALE_BOOK_MS: u64 = 10_000;
const DEFAULT_RECONNECT_INTERVAL_MS: u64 = 1_000;
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_DEPTH: u16 = 5;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KuCoinSpotConfig {
    #[serde(default)]
    pub api_key: String,
    #[serde(default)]
    pub api_secret: String,
    #[serde(default)]
    pub api_passphrase: String,
    #[serde(default = "default_rest_base_url")]
    pub base_url: String,
    #[serde(default = "default_ws_url")]
    pub websocket_url: String,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default = "default_stale_book_ms")]
    pub stale_book_ms: u64,
    #[serde(default = "default_reconnect_interval_ms")]
    pub reconnect_interval_ms: u64,
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    #[serde(default = "default_depth")]
    pub orderbook_depth: u16,
    #[serde(default)]
    pub enabled_symbols: Vec<String>,
    #[serde(default)]
    pub log_raw_messages: bool,
}

impl Default for KuCoinSpotConfig {
    fn default() -> Self {
        Self {
            api_key: std::env::var("KUCOIN_API_KEY").unwrap_or_default(),
            api_secret: std::env::var("KUCOIN_API_SECRET").unwrap_or_default(),
            api_passphrase: std::env::var("KUCOIN_API_PASSPHRASE").unwrap_or_default(),
            base_url: default_rest_base_url(),
            websocket_url: default_ws_url(),
            dry_run: false,
            stale_book_ms: DEFAULT_STALE_BOOK_MS,
            reconnect_interval_ms: DEFAULT_RECONNECT_INTERVAL_MS,
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            orderbook_depth: DEFAULT_DEPTH,
            enabled_symbols: Vec::new(),
            log_raw_messages: false,
        }
    }
}

#[derive(Clone)]
pub struct KuCoinSpotClient {
    config: KuCoinSpotConfig,
    http: reqwest::Client,
    reservations: BalanceReservationManager,
}

impl KuCoinSpotClient {
    pub fn new(config: KuCoinSpotConfig) -> Self {
        Self {
            config,
            http: crate::core::http2_fix::shared_http_client(),
            reservations: BalanceReservationManager::default(),
        }
    }

    async fn send_public_request(
        &self,
        endpoint: &str,
        params: HashMap<String, String>,
    ) -> ExchangeClientResult<Value> {
        let response = self
            .http
            .get(build_url(&self.config.base_url, endpoint, &params))
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
        params: HashMap<String, String>,
        body: Option<Value>,
    ) -> ExchangeClientResult<Value> {
        self.ensure_credentials()?;
        let path = build_path(endpoint, &params);
        let url = format!("{}{}", self.config.base_url.trim_end_matches('/'), path);
        let timestamp = Utc::now().timestamp_millis().to_string();
        let body_text = body.as_ref().map(Value::to_string).unwrap_or_default();
        let prehash = format!("{}{}{}{}", timestamp, method.as_str(), path, body_text);
        let signature = sign_base64(&self.config.api_secret, &prehash);
        let passphrase = sign_base64(&self.config.api_secret, &self.config.api_passphrase);
        let mut request = self
            .http
            .request(method, url)
            .header("KC-API-KEY", &self.config.api_key)
            .header("KC-API-SIGN", signature)
            .header("KC-API-TIMESTAMP", timestamp)
            .header("KC-API-PASSPHRASE", passphrase)
            .header("KC-API-KEY-VERSION", "2")
            .header("Content-Type", "application/json")
            .timeout(StdDuration::from_millis(self.config.request_timeout_ms));
        if !body_text.is_empty() {
            request = request.body(body_text);
        }
        let response = request.send().await.map_err(CoreExchangeError::from)?;
        parse_response(response).await
    }

    fn ensure_credentials(&self) -> ExchangeClientResult<()> {
        if self.config.api_key.trim().is_empty()
            || self.config.api_secret.trim().is_empty()
            || self.config.api_passphrase.trim().is_empty()
        {
            return Err(classified(
                ExchangeErrorClass::AuthenticationFailed,
                None,
                "KUCOIN_API_KEY, KUCOIN_API_SECRET and KUCOIN_API_PASSPHRASE are required",
            ));
        }
        Ok(())
    }

    async fn private_ws_endpoint(&self) -> ExchangeClientResult<String> {
        let value = self
            .send_signed_request(Method::POST, "/api/v1/bullet-private", HashMap::new(), None)
            .await?;
        parse_bullet_endpoint(&value)
    }

    fn dry_run_order_response(&self, request: &OrderRequest, symbol: &str) -> OrderResponse {
        OrderResponse {
            exchange: "kucoin".to_string(),
            market_type: MarketType::Spot,
            symbol: symbol.to_string(),
            order_id: format!("dry-kucoin-{}", Utc::now().timestamp_millis()),
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

    fn dry_run_quote_market_order_response(
        &self,
        request: &QuoteMarketOrderRequest,
        symbol: &str,
    ) -> OrderResponse {
        OrderResponse {
            exchange: "kucoin".to_string(),
            market_type: MarketType::Spot,
            symbol: symbol.to_string(),
            order_id: format!("dry-kucoin-{}", Utc::now().timestamp_millis()),
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

    fn dry_run_amend_order_response(
        &self,
        request: &AmendOrderRequest,
        symbol: &str,
    ) -> OrderResponse {
        OrderResponse {
            exchange: "kucoin".to_string(),
            market_type: MarketType::Spot,
            symbol: symbol.to_string(),
            order_id: request
                .order_id()
                .map(str::to_string)
                .unwrap_or_else(|| format!("dry-kucoin-{}", Utc::now().timestamp_millis())),
            client_order_id: request.client_order_id().map(str::to_string),
            side: OrderSide::Buy,
            position_side: PositionSide::None,
            order_type: OrderType::Limit,
            status: OrderStatus::New,
            price: None,
            quantity: request.new_quantity,
            filled_quantity: 0.0,
            average_price: None,
            created_at: Utc::now(),
            updated_at: Some(Utc::now()),
        }
    }
}

#[async_trait]
impl ExchangeClient for KuCoinSpotClient {
    fn market_type(&self) -> MarketType {
        MarketType::Spot
    }

    fn exchange_name(&self) -> &str {
        "kucoin"
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let mut capabilities = ExchangeClientCapabilities::spot(self.exchange_name());
        capabilities.supports_private_user_stream = true;
        capabilities.supports_cancel_all_orders = true;
        capabilities.supports_quote_market_order = true;
        capabilities.supports_amend_order = true;
        capabilities
    }

    fn normalize_symbol(&self, symbol: &str) -> ExchangeClientResult<String> {
        normalize_kucoin_symbol(symbol)
    }

    fn denormalize_symbol(&self, symbol: &str) -> ExchangeClientResult<String> {
        Ok(to_exchange_symbol(&self.normalize_symbol(symbol)?))
    }

    async fn load_symbol_rules(&self) -> ExchangeClientResult<Vec<SymbolRule>> {
        let value = self
            .send_public_request("/api/v2/symbols", HashMap::new())
            .await?;
        parse_symbol_rules(&value)
    }

    async fn get_balances(&self) -> ExchangeClientResult<BalanceSnapshot> {
        let value = self
            .send_signed_request(Method::GET, "/api/v1/accounts", HashMap::new(), None)
            .await?;
        let snapshot = parse_balance_snapshot(&value)?;
        self.reservations
            .update_balances(self.exchange_name(), &snapshot.balances)?;
        Ok(BalanceSnapshot {
            balances: snapshot
                .balances
                .into_iter()
                .map(|balance| {
                    self.reservations
                        .balance_with_reservation("kucoin", balance)
                })
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
        params.insert("symbol".to_string(), to_exchange_symbol(&symbol));
        let endpoint = if depth <= 20 {
            "/api/v1/market/orderbook/level2_20"
        } else {
            "/api/v1/market/orderbook/level2_100"
        };
        let value = self.send_public_request(endpoint, params).await?;
        parse_orderbook_snapshot(&value, &symbol, self.config.stale_book_ms)
    }

    async fn place_order(&self, mut request: OrderRequest) -> ExchangeClientResult<OrderResponse> {
        request.validate()?;
        if request.market_type != MarketType::Spot {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: "KuCoinSpotClient only supports MarketType::Spot".to_string(),
            });
        }
        let symbol = self.normalize_symbol(&request.symbol)?;
        request.symbol = symbol.clone();
        ensure_client_order_id(&mut request)?;
        let exchange_symbol = to_exchange_symbol(&symbol);
        let body = kucoin_order_body(&request, &exchange_symbol)?;
        if self.config.dry_run {
            return Ok(self.dry_run_order_response(&request, &symbol));
        }
        let value = self
            .send_signed_request(
                Method::POST,
                "/api/v1/hf/orders",
                HashMap::new(),
                Some(body),
            )
            .await?;
        parse_order_ack(&value, &request, &exchange_symbol)
    }

    async fn place_quote_market_order(
        &self,
        mut request: QuoteMarketOrderRequest,
    ) -> ExchangeClientResult<OrderResponse> {
        request.validate()?;
        if request.side != OrderSide::Buy {
            return Err(ExchangeClientError::Unsupported(
                "KuCoin Spot quote-sized market orders are only supported for market buys"
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
        let exchange_symbol = to_exchange_symbol(&symbol);
        let body = kucoin_quote_market_order_body(&request, &exchange_symbol)?;
        let response = self
            .send_signed_request(
                Method::POST,
                "/api/v1/hf/orders",
                HashMap::new(),
                Some(body),
            )
            .await;
        match response {
            Ok(value) => parse_quote_order_ack(&value, &request, &exchange_symbol),
            Err(error) => {
                if let Some(reservation) = reservation.as_mut() {
                    let _ = self.reservations.release(reservation);
                }
                Err(error)
            }
        }
    }

    async fn amend_order(
        &self,
        mut request: AmendOrderRequest,
    ) -> ExchangeClientResult<OrderResponse> {
        request.validate()?;
        if request.market_type != MarketType::Spot {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: "KuCoinSpotClient only supports MarketType::Spot".to_string(),
            });
        }
        ensure_amend_client_order_id(&request)?;
        if request.new_client_order_id().is_some() {
            return Err(ExchangeClientError::Unsupported(
                "KuCoin HF amend does not support assigning a new client order id".to_string(),
            ));
        }
        let symbol = self.normalize_symbol(&request.symbol)?;
        request.symbol = symbol.clone();
        if self.config.dry_run {
            return Ok(self.dry_run_amend_order_response(&request, &symbol));
        }
        let exchange_symbol = to_exchange_symbol(&symbol);
        let body = kucoin_amend_order_body(&request, &exchange_symbol)?;
        let value = self
            .send_signed_request(
                Method::POST,
                "/api/v1/hf/orders/alter",
                HashMap::new(),
                Some(body),
            )
            .await?;
        parse_amend_order_ack(&value, &request, &exchange_symbol)
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeClientResult<CancelOrderResponse> {
        request.validate()?;
        if request.market_type != MarketType::Spot {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: "KuCoinSpotClient only supports MarketType::Spot".to_string(),
            });
        }
        ensure_cancel_client_order_id(&request)?;
        let symbol = self.normalize_symbol(&request.symbol)?;
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), to_exchange_symbol(&symbol));
        let endpoint = if let Some(order_id) = request.order_id() {
            format!("/api/v1/hf/orders/{order_id}")
        } else if let Some(client_order_id) = request.client_order_id() {
            format!("/api/v1/hf/orders/client-order/{client_order_id}")
        } else {
            return Err(ExchangeClientError::Validation {
                field: "order_id",
                reason: "order_id or client_order_id is required".to_string(),
            });
        };
        if self.config.dry_run {
            return Ok(CancelOrderResponse {
                exchange: "kucoin".to_string(),
                market_type: MarketType::Spot,
                symbol,
                order_id: request.order_id().map(str::to_string),
                client_order_id: request.client_order_id().map(str::to_string),
                status: OrderStatus::Cancelled,
                cancelled_at: Utc::now(),
            });
        }
        let value = self
            .send_signed_request(Method::DELETE, &endpoint, params, None)
            .await?;
        Ok(CancelOrderResponse {
            exchange: "kucoin".to_string(),
            market_type: MarketType::Spot,
            symbol,
            order_id: kucoin_cancelled_order_id(&value)
                .or_else(|| request.order_id().map(str::to_string)),
            client_order_id: request.client_order_id().map(str::to_string),
            status: OrderStatus::Cancelled,
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
                reason: "KuCoinSpotClient only supports MarketType::Spot".to_string(),
            });
        }
        let mut params = HashMap::new();
        let symbol = if let Some(symbol) = request.symbol.as_deref() {
            let symbol = self.normalize_symbol(symbol)?;
            params.insert("symbol".to_string(), to_exchange_symbol(&symbol));
            Some(symbol)
        } else {
            None
        };
        let endpoint = if symbol.is_some() {
            "/api/v1/hf/orders"
        } else {
            "/api/v1/hf/orders/cancelAll"
        };
        if self.config.dry_run {
            return Ok(CancelAllOrdersResponse {
                exchange: "kucoin".to_string(),
                market_type: MarketType::Spot,
                symbol,
                cancelled_orders: 0,
                cancelled_at: Utc::now(),
            });
        }
        let value = self
            .send_signed_request(Method::DELETE, endpoint, params, None)
            .await?;
        Ok(CancelAllOrdersResponse {
            exchange: "kucoin".to_string(),
            market_type: MarketType::Spot,
            symbol,
            cancelled_orders: kucoin_cancel_all_cancelled_count(&value),
            cancelled_at: Utc::now(),
        })
    }

    async fn get_order(&self, symbol: &str, order_id: &str) -> ExchangeClientResult<OrderResponse> {
        let order_id = validate_order_lookup_id(order_id)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            to_exchange_symbol(&self.normalize_symbol(symbol)?),
        );
        let value = self
            .send_signed_request(
                Method::GET,
                &format!("/api/v1/hf/orders/{order_id}"),
                params,
                None,
            )
            .await?;
        let order = value
            .get("items")
            .and_then(Value::as_array)
            .and_then(|items| items.first())
            .unwrap_or(&value);
        parse_order_response(order)
    }

    async fn get_open_orders(
        &self,
        symbol: Option<&str>,
    ) -> ExchangeClientResult<Vec<OrderResponse>> {
        let mut params = HashMap::new();
        params.insert("status".to_string(), "active".to_string());
        if let Some(symbol) = symbol {
            params.insert(
                "symbol".to_string(),
                to_exchange_symbol(&self.normalize_symbol(symbol)?),
            );
        }
        let value = self
            .send_signed_request(Method::GET, "/api/v1/hf/orders/active", params, None)
            .await?;
        let items = value
            .as_array()
            .cloned()
            .or_else(|| value.get("items").and_then(Value::as_array).cloned())
            .or_else(|| {
                value
                    .get("data")
                    .and_then(|data| data.get("items").or(Some(data)))
                    .and_then(Value::as_array)
                    .cloned()
            })
            .unwrap_or_default();
        items.iter().map(parse_order_response).collect()
    }

    async fn get_fee_rate(&self, symbol: &str) -> ExchangeClientResult<FeeRate> {
        if self.ensure_credentials().is_err() {
            return Ok(FeeRate::new(0.001, 0.001, FeeRateSource::DefaultFallback));
        }
        let mut params = HashMap::new();
        params.insert(
            "symbols".to_string(),
            to_exchange_symbol(&self.normalize_symbol(symbol)?),
        );
        let value = self
            .send_signed_request(Method::GET, "/api/v1/trade-fees", params, None)
            .await?;
        parse_fee_rate(&value)
    }

    async fn get_recent_fills(&self, symbol: &str) -> ExchangeClientResult<Vec<TradeFill>> {
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            to_exchange_symbol(&self.normalize_symbol(symbol)?),
        );
        let value = self
            .send_signed_request(Method::GET, "/api/v1/hf/fills", params, None)
            .await?;
        let items = value
            .get("items")
            .or_else(|| value.get("data").and_then(|data| data.get("items")))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        items.iter().map(parse_fill).collect()
    }

    async fn health_check(&self) -> ExchangeClientResult<ExchangeHealthStatus> {
        let rest_healthy = self
            .send_public_request("/api/v1/timestamp", HashMap::new())
            .await
            .is_ok();
        Ok(ExchangeHealthStatus {
            exchange: "kucoin".to_string(),
            market_type: MarketType::Spot,
            connected: rest_healthy,
            public_ws_healthy: true,
            private_ws_healthy: false,
            rest_healthy,
            stale_books: Vec::new(),
            last_error: None,
            checked_at: Utc::now(),
        })
    }

    async fn subscribe_orderbook(
        &self,
        symbols: Vec<String>,
    ) -> ExchangeClientResult<mpsc::Receiver<OrderBookSnapshot>> {
        let symbols = if symbols.is_empty() {
            self.config.enabled_symbols.clone()
        } else {
            symbols
        };
        let symbols = symbols
            .into_iter()
            .map(|symbol| self.normalize_symbol(&symbol))
            .collect::<ExchangeClientResult<Vec<_>>>()?;
        let (tx, rx) = mpsc::channel(1024);
        let config = self.config.clone();
        tokio::spawn(async move {
            loop {
                if let Err(error) =
                    kucoin_ws_loop(config.clone(), symbols.clone(), tx.clone()).await
                {
                    log::warn!("KuCoin public WS disconnected: {}", error);
                    sleep(StdDuration::from_millis(config.reconnect_interval_ms)).await;
                }
            }
        });
        Ok(rx)
    }

    async fn subscribe_user_stream(&self) -> ExchangeClientResult<mpsc::Receiver<UserStreamEvent>> {
        self.ensure_credentials()?;
        let client = self.clone();
        let (tx, rx) = mpsc::channel(1024);
        tokio::spawn(async move {
            loop {
                if let Err(error) = kucoin_private_ws_loop(client.clone(), tx.clone()).await {
                    log::warn!("KuCoin private WS disconnected: {}", error);
                    let _ = tx
                        .send(UserStreamEvent::Disconnected {
                            reason: Some(error.to_string()),
                        })
                        .await;
                    sleep(StdDuration::from_millis(
                        client.config.reconnect_interval_ms,
                    ))
                    .await;
                }
            }
        });
        Ok(rx)
    }
}

async fn kucoin_ws_loop(
    config: KuCoinSpotConfig,
    symbols: Vec<String>,
    tx: mpsc::Sender<OrderBookSnapshot>,
) -> ExchangeClientResult<()> {
    let endpoint = ws_endpoint(&config)
        .await
        .unwrap_or_else(|_| config.websocket_url.clone());
    let (stream, _) = connect_async(endpoint).await.map_err(|error| {
        classified(
            ExchangeErrorClass::NetworkError,
            None,
            format!("KuCoin public WS connect failed: {error}"),
        )
    })?;
    let (mut write, mut read) = stream.split();
    for symbol in &symbols {
        let msg = json!({
            "id": Utc::now().timestamp_millis().to_string(),
            "type": "subscribe",
            "topic": format!("/spotMarket/level2Depth5:{}", to_exchange_symbol(symbol)),
            "privateChannel": false,
            "response": true
        });
        write
            .send(Message::Text(msg.to_string().into()))
            .await
            .map_err(|error| {
                classified(
                    ExchangeErrorClass::NetworkError,
                    None,
                    format!("KuCoin public WS subscribe failed: {error}"),
                )
            })?;
    }
    while let Some(message) = timeout(StdDuration::from_secs(30), read.next())
        .await
        .map_err(|_| {
            classified(
                ExchangeErrorClass::Timeout,
                None,
                "KuCoin public WS read timeout",
            )
        })?
    {
        let message = message.map_err(|error| {
            classified(
                ExchangeErrorClass::NetworkError,
                None,
                format!("KuCoin public WS read failed: {error}"),
            )
        })?;
        if let Message::Text(text) = message {
            if config.log_raw_messages {
                log::debug!("KuCoin WS: {}", text);
            }
            if let Some(snapshot) = parse_ws_orderbook_message(&text, config.stale_book_ms)? {
                let _ = tx.send(snapshot).await;
            }
        }
    }
    Ok(())
}

async fn ws_endpoint(config: &KuCoinSpotConfig) -> ExchangeClientResult<String> {
    let response = crate::core::http2_fix::shared_http_client()
        .post(format!(
            "{}/api/v1/bullet-public",
            config.base_url.trim_end_matches('/')
        ))
        .timeout(StdDuration::from_millis(config.request_timeout_ms))
        .send()
        .await
        .map_err(CoreExchangeError::from)?;
    let value = parse_response(response).await?;
    let server = value
        .get("instanceServers")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .and_then(|item| item.get("endpoint"))
        .and_then(Value::as_str)
        .ok_or_else(|| parser_error("bullet response missing endpoint", &value))?;
    let token = value
        .get("token")
        .and_then(Value::as_str)
        .ok_or_else(|| parser_error("bullet response missing token", &value))?;
    Ok(format!("{server}?token={token}"))
}

async fn kucoin_private_ws_loop(
    client: KuCoinSpotClient,
    tx: mpsc::Sender<UserStreamEvent>,
) -> ExchangeClientResult<()> {
    let endpoint = client.private_ws_endpoint().await?;
    let (stream, _) = connect_async(endpoint).await.map_err(|error| {
        classified(
            ExchangeErrorClass::NetworkError,
            None,
            format!("KuCoin private WS connect failed: {error}"),
        )
    })?;
    let (mut write, mut read) = stream.split();
    for topic in ["/spotMarket/tradeOrders", "/account/balance"] {
        let msg = json!({
            "id": Utc::now().timestamp_millis().to_string(),
            "type": "subscribe",
            "topic": topic,
            "privateChannel": true,
            "response": true
        });
        write
            .send(Message::Text(msg.to_string().into()))
            .await
            .map_err(|error| {
                classified(
                    ExchangeErrorClass::NetworkError,
                    None,
                    format!("KuCoin private WS subscribe failed: {error}"),
                )
            })?;
    }
    while let Some(message) = timeout(StdDuration::from_secs(60), read.next())
        .await
        .map_err(|_| {
            classified(
                ExchangeErrorClass::Timeout,
                None,
                "KuCoin private WS read timeout",
            )
        })?
    {
        let message = message.map_err(|error| {
            classified(
                ExchangeErrorClass::NetworkError,
                None,
                format!("KuCoin private WS read failed: {error}"),
            )
        })?;
        match message {
            Message::Text(text) => {
                if client.config.log_raw_messages {
                    log::debug!("KuCoin private WS: {}", text);
                }
                for event in parse_user_stream_message(&text)? {
                    if tx.send(event).await.is_err() {
                        return Ok(());
                    }
                }
            }
            Message::Ping(payload) => {
                let _ = write.send(Message::Pong(payload)).await;
            }
            Message::Close(_) => {
                return Err(classified(
                    ExchangeErrorClass::NetworkError,
                    None,
                    "KuCoin private WS closed",
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

fn parse_bullet_endpoint(value: &Value) -> ExchangeClientResult<String> {
    let server = value
        .get("instanceServers")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .and_then(|item| item.get("endpoint"))
        .and_then(Value::as_str)
        .ok_or_else(|| parser_error("bullet response missing endpoint", value))?;
    let token = value
        .get("token")
        .and_then(Value::as_str)
        .ok_or_else(|| parser_error("bullet response missing token", value))?;
    Ok(format!("{server}?token={token}"))
}

pub fn parse_symbol_rules(value: &Value) -> ExchangeClientResult<Vec<SymbolRule>> {
    let items = value
        .as_array()
        .or_else(|| value.get("data").and_then(Value::as_array))
        .ok_or_else(|| parser_error("symbols response missing data", value))?;
    items.iter().map(parse_symbol_rule).collect()
}

pub fn parse_symbol_rule(value: &Value) -> ExchangeClientResult<SymbolRule> {
    let exchange_symbol = required_str(value, "symbol")?.to_string();
    let base_asset = required_str(value, "baseCurrency")?.to_ascii_uppercase();
    let quote_asset = required_str(value, "quoteCurrency")?.to_ascii_uppercase();
    let tick_size = number_from_str(value.get("priceIncrement")).unwrap_or(0.0);
    let step_size = number_from_str(value.get("baseIncrement")).unwrap_or(0.0);
    let min_quantity = number_from_str(value.get("baseMinSize")).unwrap_or(0.0);
    let min_notional = number_from_str(value.get("quoteMinSize")).unwrap_or(0.0);
    Ok(SymbolRule {
        exchange: "kucoin".to_string(),
        market_type: MarketType::Spot,
        internal_symbol: normalize_kucoin_symbol(&exchange_symbol)?,
        exchange_symbol,
        base_asset,
        quote_asset,
        price_precision: precision_from_step(tick_size),
        quantity_precision: precision_from_step(step_size),
        tick_size,
        step_size,
        min_quantity,
        min_notional,
        max_quantity: number_from_str(value.get("baseMaxSize")),
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
        status: if value
            .get("enableTrading")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            SymbolStatus::Trading
        } else {
            SymbolStatus::Suspended
        },
        raw_metadata: Some(value.clone()),
    })
}

pub fn parse_orderbook_snapshot(
    value: &Value,
    symbol: &str,
    stale_book_ms: u64,
) -> ExchangeClientResult<OrderBookSnapshot> {
    let data = value.get("data").unwrap_or(value);
    let bids = data
        .get("bids")
        .and_then(Value::as_array)
        .ok_or_else(|| parser_error("orderbook missing bids", value))?;
    let asks = data
        .get("asks")
        .and_then(Value::as_array)
        .ok_or_else(|| parser_error("orderbook missing asks", value))?;
    let bids = parse_levels(bids)?;
    let asks = parse_levels(asks)?;
    let received_at = Utc::now();
    let exchange_timestamp = data
        .get("time")
        .or_else(|| data.get("timestamp"))
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    let latency_ms =
        exchange_timestamp.map(|ts| received_at.signed_duration_since(ts).num_milliseconds());
    Ok(OrderBookSnapshot {
        exchange: "kucoin".to_string(),
        market_type: MarketType::Spot,
        symbol: normalize_kucoin_symbol(symbol)?,
        best_bid: bids.first().map(|level| level.price),
        best_ask: asks.first().map(|level| level.price),
        bids,
        asks,
        exchange_timestamp,
        received_at,
        latency_ms,
        sequence: data
            .get("sequence")
            .or_else(|| data.get("sequenceStart"))
            .and_then(Value::as_u64),
        is_stale: latency_ms.is_some_and(|latency| latency > stale_book_ms as i64),
    })
}

pub fn parse_ws_orderbook_message(
    text: &str,
    stale_book_ms: u64,
) -> ExchangeClientResult<Option<OrderBookSnapshot>> {
    let value: Value = serde_json::from_str(text).map_err(CoreExchangeError::from)?;
    if value.get("type").and_then(Value::as_str) != Some("message") {
        return Ok(None);
    }
    let topic = value
        .get("topic")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let symbol = topic.split(':').nth(1).unwrap_or("UNKNOWN");
    parse_orderbook_snapshot(value.get("data").unwrap_or(&value), symbol, stale_book_ms).map(Some)
}

pub fn parse_user_stream_message(text: &str) -> ExchangeClientResult<Vec<UserStreamEvent>> {
    let value: Value = serde_json::from_str(text).map_err(CoreExchangeError::from)?;
    if value.get("type").and_then(Value::as_str) != Some("message") {
        return Ok(Vec::new());
    }
    let topic = value
        .get("topic")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let data = value.get("data").unwrap_or(&value);
    match topic {
        "/spotMarket/tradeOrders" => parse_private_order_update(data),
        "/account/balance" => parse_private_balance_update(data),
        _ => Ok(Vec::new()),
    }
}

fn parse_private_order_update(value: &Value) -> ExchangeClientResult<Vec<UserStreamEvent>> {
    let symbol = required_str(value, "symbol")?;
    let side = parse_side(value.get("side").and_then(Value::as_str).unwrap_or("buy"));
    let quantity = number_from_str(
        value
            .get("size")
            .or_else(|| value.get("orderSize"))
            .or_else(|| value.get("filledSize")),
    )
    .unwrap_or(0.0);
    let filled_quantity = number_from_str(
        value
            .get("filledSize")
            .or_else(|| value.get("matchSize"))
            .or_else(|| value.get("remainSize")),
    )
    .unwrap_or(0.0);
    let status_text = value
        .get("status")
        .or_else(|| value.get("type"))
        .and_then(Value::as_str);
    let order = OrderResponse {
        exchange: "kucoin".to_string(),
        market_type: MarketType::Spot,
        symbol: normalize_kucoin_symbol(symbol)?,
        order_id: value_as_string(value.get("orderId").or_else(|| value.get("id")))
            .unwrap_or_else(|| "unknown".to_string()),
        client_order_id: value_as_string(value.get("clientOid")),
        side,
        position_side: PositionSide::None,
        order_type: match value
            .get("orderType")
            .or_else(|| value.get("type"))
            .and_then(Value::as_str)
            .unwrap_or("limit")
        {
            "market" => OrderType::Market,
            _ => OrderType::Limit,
        },
        status: kucoin_ws_order_status(status_text, filled_quantity, quantity),
        price: number_from_str(value.get("price").or_else(|| value.get("orderPrice"))),
        quantity,
        filled_quantity,
        average_price: number_from_str(value.get("matchPrice")),
        created_at: first_kucoin_timestamp(value, &["orderTime", "createdAt", "ts"])
            .unwrap_or_else(Utc::now),
        updated_at: first_kucoin_timestamp(value, &["ts", "orderTime", "createdAt"]),
    };
    let mut events = vec![UserStreamEvent::Order(order.clone())];
    let match_size = number_from_str(value.get("matchSize")).unwrap_or(0.0);
    if match_size > 0.0 || value.get("tradeId").is_some() {
        let price = number_from_str(value.get("matchPrice").or_else(|| value.get("price")))
            .unwrap_or_default();
        let quantity = if match_size > 0.0 {
            match_size
        } else {
            filled_quantity
        };
        events.push(UserStreamEvent::Fill(TradeFill {
            exchange: "kucoin".to_string(),
            market_type: MarketType::Spot,
            symbol: order.symbol,
            trade_id: value_as_string(value.get("tradeId")).filter(|trade_id| !trade_id.is_empty()),
            order_id: Some(order.order_id),
            client_order_id: order.client_order_id,
            side,
            price,
            quantity,
            fee_asset: value_as_string(value.get("feeCurrency")),
            fee_amount: number_from_str(value.get("fee")),
            liquidity: match value
                .get("liquidity")
                .and_then(Value::as_str)
                .unwrap_or_default()
            {
                "maker" => LiquidityRole::Maker,
                "taker" => LiquidityRole::Taker,
                _ => LiquidityRole::Unknown,
            },
            timestamp: first_kucoin_timestamp(value, &["ts", "orderTime", "createdAt"])
                .unwrap_or_else(Utc::now),
        }));
    }
    Ok(events)
}

fn parse_private_balance_update(value: &Value) -> ExchangeClientResult<Vec<UserStreamEvent>> {
    let asset = required_str(value, "currency")?.to_ascii_uppercase();
    let available = number_from_str(value.get("available")).unwrap_or(0.0);
    let locked = number_from_str(value.get("hold").or_else(|| value.get("holds"))).unwrap_or(0.0);
    let total = number_from_str(value.get("total")).unwrap_or(available + locked);
    Ok(vec![UserStreamEvent::Balance(BalanceSnapshot {
        exchange: "kucoin".to_string(),
        market_type: MarketType::Spot,
        balances: vec![AssetBalance::new(asset, total, available, locked)],
        timestamp: first_kucoin_timestamp(value, &["time", "ts"]).unwrap_or_else(Utc::now),
    })])
}

pub fn parse_balance_snapshot(value: &Value) -> ExchangeClientResult<BalanceSnapshot> {
    let items = value
        .as_array()
        .or_else(|| value.get("data").and_then(Value::as_array))
        .ok_or_else(|| parser_error("accounts response missing data", value))?;
    let mut balances = Vec::new();
    for item in items {
        if item.get("type").and_then(Value::as_str) != Some("trade") {
            continue;
        }
        let asset = required_str(item, "currency")?.to_ascii_uppercase();
        let available = number_from_str(item.get("available")).unwrap_or(0.0);
        let locked = number_from_str(item.get("holds")).unwrap_or(0.0);
        if available > 0.0 || locked > 0.0 {
            balances.push(AssetBalance::new(
                asset,
                available + locked,
                available,
                locked,
            ));
        }
    }
    Ok(BalanceSnapshot {
        exchange: "kucoin".to_string(),
        market_type: MarketType::Spot,
        balances,
        timestamp: Utc::now(),
    })
}

pub fn parse_order_response(value: &Value) -> ExchangeClientResult<OrderResponse> {
    let symbol = value
        .get("symbol")
        .and_then(Value::as_str)
        .map(normalize_kucoin_symbol)
        .transpose()?
        .unwrap_or_else(|| "UNKNOWN".to_string());
    let quantity = number_from_str(value.get("size")).unwrap_or(0.0);
    let filled_quantity = number_from_str(value.get("dealSize")).unwrap_or(0.0);
    Ok(OrderResponse {
        exchange: "kucoin".to_string(),
        market_type: MarketType::Spot,
        symbol,
        order_id: value_as_string(value.get("id")).unwrap_or_else(|| "unknown".to_string()),
        client_order_id: value_as_string(value.get("clientOid")),
        side: parse_side(value.get("side").and_then(Value::as_str).unwrap_or("buy")),
        position_side: PositionSide::None,
        order_type: match value.get("type").and_then(Value::as_str).unwrap_or("limit") {
            "market" => OrderType::Market,
            _ => OrderType::Limit,
        },
        status: if value
            .get("isActive")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            OrderStatus::New
        } else if filled_quantity >= quantity && quantity > 0.0 {
            OrderStatus::Filled
        } else {
            OrderStatus::Unknown
        },
        price: number_from_str(value.get("price")),
        quantity,
        filled_quantity,
        average_price: number_from_str(value.get("dealFunds"))
            .and_then(|funds| (filled_quantity > 0.0).then_some(funds / filled_quantity)),
        created_at: value
            .get("createdAt")
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .unwrap_or_else(Utc::now),
        updated_at: None,
    })
}

pub fn parse_fill(value: &Value) -> ExchangeClientResult<TradeFill> {
    Ok(TradeFill {
        exchange: "kucoin".to_string(),
        market_type: MarketType::Spot,
        symbol: value
            .get("symbol")
            .and_then(Value::as_str)
            .map(normalize_kucoin_symbol)
            .transpose()?
            .unwrap_or_else(|| "UNKNOWN".to_string()),
        trade_id: value_as_string(value.get("tradeId")),
        order_id: value_as_string(value.get("orderId")),
        client_order_id: value_as_string(value.get("clientOid")),
        side: parse_side(value.get("side").and_then(Value::as_str).unwrap_or("buy")),
        price: number_from_str(value.get("price")).unwrap_or(0.0),
        quantity: number_from_str(value.get("size")).unwrap_or(0.0),
        fee_asset: value_as_string(value.get("feeCurrency")),
        fee_amount: number_from_str(value.get("fee")),
        liquidity: match value
            .get("liquidity")
            .and_then(Value::as_str)
            .unwrap_or_default()
        {
            "maker" => LiquidityRole::Maker,
            "taker" => LiquidityRole::Taker,
            _ => LiquidityRole::Unknown,
        },
        timestamp: value
            .get("createdAt")
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .unwrap_or_else(Utc::now),
    })
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
        .or_else(|| value.get("data"))
        .unwrap_or(value);
    Ok(FeeRate::new(
        number_from_str(item.get("makerFeeRate")).unwrap_or(0.001),
        number_from_str(item.get("takerFeeRate")).unwrap_or(0.001),
        FeeRateSource::ExchangeApi,
    ))
}

fn ensure_client_order_id(request: &mut OrderRequest) -> ExchangeClientResult<()> {
    if request.client_order_id.is_none() {
        request.client_order_id =
            Some(generate_client_order_id("kucoin", request.market_type, "spot").into_string());
    }
    if let Some(client_order_id) = &request.client_order_id {
        validate_client_order_id("kucoin", request.market_type, client_order_id).map_err(
            |error| ExchangeClientError::Validation {
                field: "client_order_id",
                reason: error.to_string(),
            },
        )?;
    }
    Ok(())
}

fn ensure_quote_market_client_order_id(
    request: &mut QuoteMarketOrderRequest,
) -> ExchangeClientResult<()> {
    if request.client_order_id.is_none() {
        request.client_order_id =
            Some(generate_client_order_id("kucoin", request.market_type, "spot").into_string());
    }
    if let Some(client_order_id) = &request.client_order_id {
        validate_client_order_id("kucoin", request.market_type, client_order_id).map_err(
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
        validate_client_order_id("kucoin", request.market_type, client_order_id).map_err(
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
        validate_client_order_id("kucoin", request.market_type, client_order_id).map_err(
            |error| ExchangeClientError::Validation {
                field: "client_order_id",
                reason: error.to_string(),
            },
        )?;
    }
    Ok(())
}

fn kucoin_order_body(request: &OrderRequest, exchange_symbol: &str) -> ExchangeClientResult<Value> {
    let mut body = serde_json::Map::new();
    body.insert(
        "clientOid".to_string(),
        Value::String(request.client_order_id.clone().ok_or_else(|| {
            ExchangeClientError::Validation {
                field: "client_order_id",
                reason: "client_order_id is required".to_string(),
            }
        })?),
    );
    body.insert(
        "symbol".to_string(),
        Value::String(exchange_symbol.to_string()),
    );
    body.insert(
        "side".to_string(),
        Value::String(
            match request.side {
                OrderSide::Buy => "buy",
                OrderSide::Sell => "sell",
            }
            .to_string(),
        ),
    );
    body.insert(
        "size".to_string(),
        Value::String(format_decimal(request.quantity)),
    );
    match request.order_type {
        OrderType::Market => {
            body.insert("type".to_string(), Value::String("market".to_string()));
        }
        OrderType::Limit | OrderType::PostOnly | OrderType::IOC | OrderType::FOK => {
            body.insert("type".to_string(), Value::String("limit".to_string()));
            body.insert(
                "price".to_string(),
                Value::String(format_decimal(request.price.ok_or_else(|| {
                    ExchangeClientError::Validation {
                        field: "price",
                        reason: "price is required for KuCoin limit-style orders".to_string(),
                    }
                })?)),
            );
            if request.order_type == OrderType::PostOnly
                || matches!(request.time_in_force, Some(TimeInForce::GTX))
            {
                body.insert("postOnly".to_string(), Value::Bool(true));
            }
            let tif = match request.order_type {
                OrderType::IOC => Some("IOC"),
                OrderType::FOK => Some("FOK"),
                _ => match request.time_in_force {
                    Some(TimeInForce::IOC) => Some("IOC"),
                    Some(TimeInForce::FOK) => Some("FOK"),
                    Some(TimeInForce::GTC) | Some(TimeInForce::GTX) | None => None,
                },
            };
            if let Some(tif) = tif {
                body.insert("timeInForce".to_string(), Value::String(tif.to_string()));
            }
        }
    }
    body.insert("tradeType".to_string(), Value::String("TRADE".to_string()));
    Ok(Value::Object(body))
}

fn kucoin_amend_order_body(
    request: &AmendOrderRequest,
    exchange_symbol: &str,
) -> ExchangeClientResult<Value> {
    if request.new_client_order_id().is_some() {
        return Err(ExchangeClientError::Unsupported(
            "KuCoin HF amend does not support assigning a new client order id".to_string(),
        ));
    }
    let mut body = serde_json::Map::new();
    body.insert(
        "symbol".to_string(),
        Value::String(exchange_symbol.to_string()),
    );
    body.insert(
        "newSize".to_string(),
        Value::String(format_decimal(request.new_quantity)),
    );
    if let Some(order_id) = request.order_id() {
        body.insert("orderId".to_string(), Value::String(order_id.to_string()));
    }
    if let Some(client_order_id) = request.client_order_id() {
        body.insert(
            "clientOid".to_string(),
            Value::String(client_order_id.to_string()),
        );
    }
    Ok(Value::Object(body))
}

fn kucoin_quote_market_order_body(
    request: &QuoteMarketOrderRequest,
    exchange_symbol: &str,
) -> ExchangeClientResult<Value> {
    if request.side != OrderSide::Buy {
        return Err(ExchangeClientError::Unsupported(
            "KuCoin Spot quote-sized market orders are only supported for market buys".to_string(),
        ));
    }
    let mut body = serde_json::Map::new();
    body.insert(
        "clientOid".to_string(),
        Value::String(request.client_order_id.clone().ok_or_else(|| {
            ExchangeClientError::Validation {
                field: "client_order_id",
                reason: "client_order_id is required".to_string(),
            }
        })?),
    );
    body.insert(
        "symbol".to_string(),
        Value::String(exchange_symbol.to_string()),
    );
    body.insert("side".to_string(), Value::String("buy".to_string()));
    body.insert("type".to_string(), Value::String("market".to_string()));
    body.insert(
        "funds".to_string(),
        Value::String(format_decimal(request.quote_quantity)),
    );
    body.insert("tradeType".to_string(), Value::String("TRADE".to_string()));
    Ok(Value::Object(body))
}

fn parse_order_ack(
    value: &Value,
    request: &OrderRequest,
    exchange_symbol: &str,
) -> ExchangeClientResult<OrderResponse> {
    let order_id = value_as_string(value.get("orderId"))
        .or_else(|| value_as_string(value.get("id")))
        .ok_or_else(|| parser_error("order ack missing orderId", value))?;
    Ok(OrderResponse {
        exchange: "kucoin".to_string(),
        market_type: MarketType::Spot,
        symbol: normalize_kucoin_symbol(exchange_symbol)?,
        order_id,
        client_order_id: value_as_string(value.get("clientOid"))
            .or_else(|| request.client_order_id.clone()),
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
    })
}

fn parse_amend_order_ack(
    value: &Value,
    request: &AmendOrderRequest,
    exchange_symbol: &str,
) -> ExchangeClientResult<OrderResponse> {
    let order_id = value_as_string(value.get("newOrderId"))
        .or_else(|| value_as_string(value.get("orderId")))
        .or_else(|| request.order_id.clone())
        .ok_or_else(|| parser_error("amend ack missing newOrderId", value))?;
    Ok(OrderResponse {
        exchange: "kucoin".to_string(),
        market_type: MarketType::Spot,
        symbol: normalize_kucoin_symbol(exchange_symbol)?,
        order_id,
        client_order_id: value_as_string(value.get("clientOid"))
            .or_else(|| request.client_order_id.clone()),
        side: OrderSide::Buy,
        position_side: PositionSide::None,
        order_type: OrderType::Limit,
        status: OrderStatus::New,
        price: None,
        quantity: request.new_quantity,
        filled_quantity: 0.0,
        average_price: None,
        created_at: Utc::now(),
        updated_at: Some(Utc::now()),
    })
}

fn parse_quote_order_ack(
    value: &Value,
    request: &QuoteMarketOrderRequest,
    exchange_symbol: &str,
) -> ExchangeClientResult<OrderResponse> {
    let order_id = value_as_string(value.get("orderId"))
        .or_else(|| value_as_string(value.get("id")))
        .ok_or_else(|| parser_error("order ack missing orderId", value))?;
    Ok(OrderResponse {
        exchange: "kucoin".to_string(),
        market_type: MarketType::Spot,
        symbol: normalize_kucoin_symbol(exchange_symbol)?,
        order_id,
        client_order_id: value_as_string(value.get("clientOid"))
            .or_else(|| request.client_order_id.clone()),
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
    })
}

fn kucoin_cancelled_order_id(value: &Value) -> Option<String> {
    value_as_string(value.get("orderId"))
        .or_else(|| value_as_string(value.get("cancelledOrderId")))
        .or_else(|| {
            value
                .get("cancelledOrderIds")
                .and_then(Value::as_array)
                .and_then(|items| items.first())
                .and_then(|item| value_as_string(Some(item)))
        })
}

fn kucoin_cancel_all_cancelled_count(value: &Value) -> usize {
    value
        .get("cancelledOrderIds")
        .and_then(Value::as_array)
        .map(Vec::len)
        .or_else(|| value.get("items").and_then(Value::as_array).map(Vec::len))
        .unwrap_or(0)
}

async fn parse_response(response: reqwest::Response) -> ExchangeClientResult<Value> {
    let status = response.status();
    let value: Value = response.json().await.map_err(CoreExchangeError::from)?;
    let data = value.get("data").cloned().unwrap_or_else(|| value.clone());
    if !status.is_success()
        || value
            .get("code")
            .and_then(Value::as_str)
            .is_some_and(|code| code != "200000")
    {
        let code = value
            .get("code")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        let message = value
            .get("msg")
            .or_else(|| value.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("KuCoin request failed");
        return Err(classified(
            classify_kucoin_error(code.as_deref(), message),
            code,
            message,
        ));
    }
    Ok(data)
}

fn classify_kucoin_error(code: Option<&str>, message: &str) -> ExchangeErrorClass {
    let msg = message.to_ascii_lowercase();
    match code {
        Some("400100") => ExchangeErrorClass::InvalidSymbol,
        Some("400001") | Some("400002") | Some("400003") | Some("400004") | Some("400005") => {
            ExchangeErrorClass::AuthenticationFailed
        }
        Some("429000") => ExchangeErrorClass::RateLimited,
        _ if msg.contains("rate") || msg.contains("too many") => ExchangeErrorClass::RateLimited,
        _ if msg.contains("permission") => ExchangeErrorClass::PermissionDenied,
        _ if msg.contains("symbol") => ExchangeErrorClass::InvalidSymbol,
        _ => ExchangeErrorClass::Unknown,
    }
}

fn classified(
    class: ExchangeErrorClass,
    code: Option<String>,
    message: impl Into<String>,
) -> ExchangeClientError {
    ExchangeClientError::Classified(ExchangeError {
        exchange: "kucoin".to_string(),
        class,
        code,
        message: message.into(),
    })
}

fn parser_error(message: &str, value: &Value) -> ExchangeClientError {
    classified(
        ExchangeErrorClass::Unknown,
        None,
        format!("{message}: {}", truncate_json(value)),
    )
}

fn required_str<'a>(value: &'a Value, field: &'static str) -> ExchangeClientResult<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| ExchangeClientError::Validation {
            field,
            reason: "field is required".to_string(),
        })
}

fn parse_levels(values: &[Value]) -> ExchangeClientResult<Vec<OrderBookLevel>> {
    values
        .iter()
        .filter_map(|level| {
            let pair = level.as_array()?;
            Some(OrderBookLevel {
                price: number_from_str(pair.first()).unwrap_or(0.0),
                quantity: number_from_str(pair.get(1)).unwrap_or(0.0),
            })
        })
        .filter(|level| level.price > 0.0 && level.quantity > 0.0)
        .collect::<Vec<_>>()
        .pipe(Ok)
}

trait Pipe: Sized {
    fn pipe<T>(self, f: impl FnOnce(Self) -> T) -> T {
        f(self)
    }
}
impl<T> Pipe for T {}

fn number_from_str(value: Option<&Value>) -> Option<f64> {
    value.and_then(|value| match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    })
}

fn value_as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn value_as_string(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

fn format_decimal(value: f64) -> String {
    let formatted = format!("{value:.16}");
    formatted
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}

fn parse_side(value: &str) -> OrderSide {
    if value.eq_ignore_ascii_case("sell") {
        OrderSide::Sell
    } else {
        OrderSide::Buy
    }
}

fn kucoin_ws_order_status(
    status: Option<&str>,
    filled_quantity: f64,
    quantity: f64,
) -> OrderStatus {
    match status.unwrap_or_default().to_ascii_lowercase().as_str() {
        "new" | "open" => OrderStatus::New,
        "match" => OrderStatus::PartiallyFilled,
        "done" if quantity > 0.0 && filled_quantity >= quantity => OrderStatus::Filled,
        "done" => OrderStatus::Cancelled,
        _ if quantity > 0.0 && filled_quantity >= quantity => OrderStatus::Filled,
        _ => OrderStatus::Unknown,
    }
}

fn first_kucoin_timestamp(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields
        .iter()
        .filter_map(|field| value.get(*field).and_then(value_as_i64))
        .find_map(kucoin_timestamp_to_datetime)
}

fn kucoin_timestamp_to_datetime(raw: i64) -> Option<DateTime<Utc>> {
    let millis = if raw > 10_000_000_000_000_000 {
        raw / 1_000_000
    } else if raw > 10_000_000_000 {
        raw
    } else {
        raw * 1_000
    };
    DateTime::<Utc>::from_timestamp_millis(millis)
}

fn normalize_kucoin_symbol(symbol: &str) -> ExchangeClientResult<String> {
    let normalized = symbol.replace(['-', '/', '_'], "").to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(ExchangeClientError::Validation {
            field: "symbol",
            reason: "symbol must not be empty".to_string(),
        });
    }
    Ok(normalized)
}

fn to_exchange_symbol(symbol: &str) -> String {
    if symbol.contains('-') {
        return symbol.to_ascii_uppercase();
    }
    for quote in ["USDT", "USDC", "BTC", "ETH", "KCS"] {
        if let Some(base) = symbol.strip_suffix(quote) {
            if !base.is_empty() {
                return format!("{}-{}", base, quote);
            }
        }
    }
    symbol.to_ascii_uppercase()
}

fn split_compact_symbol(symbol: &str) -> Option<(String, String)> {
    for quote in ["USDT", "USDC", "BTC", "ETH", "KCS", "USD"] {
        if let Some(base) = symbol.strip_suffix(quote) {
            if !base.is_empty() {
                return Some((base.to_string(), quote.to_string()));
            }
        }
    }
    None
}

fn fallback_rule(symbol: &str) -> SymbolRule {
    let (base, quote) =
        split_compact_symbol(symbol).unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
    SymbolRule {
        exchange: "kucoin".to_string(),
        market_type: MarketType::Spot,
        internal_symbol: symbol.to_string(),
        exchange_symbol: to_exchange_symbol(symbol),
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

fn precision_from_step(step: f64) -> u32 {
    if step <= 0.0 {
        return 8;
    }
    let text = format!("{step:.12}");
    text.trim_end_matches('0')
        .split('.')
        .nth(1)
        .map(|digits| digits.len() as u32)
        .unwrap_or(0)
}

fn sign_base64(secret: &str, payload: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts key");
    mac.update(payload.as_bytes());
    general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}

fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    let path = build_path(endpoint, params);
    format!("{}{}", base.trim_end_matches('/'), path)
}

fn build_path(endpoint: &str, params: &HashMap<String, String>) -> String {
    if params.is_empty() {
        return endpoint.to_string();
    }
    let mut pairs = params.iter().collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0.cmp(right.0));
    let query = pairs
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&");
    format!("{endpoint}?{query}")
}

fn default_rest_base_url() -> String {
    DEFAULT_REST_BASE_URL.to_string()
}
fn default_ws_url() -> String {
    DEFAULT_WS_URL.to_string()
}
fn default_stale_book_ms() -> u64 {
    DEFAULT_STALE_BOOK_MS
}
fn default_reconnect_interval_ms() -> u64 {
    DEFAULT_RECONNECT_INTERVAL_MS
}
fn default_request_timeout_ms() -> u64 {
    DEFAULT_REQUEST_TIMEOUT_MS
}
fn default_depth() -> u16 {
    DEFAULT_DEPTH
}

fn truncate_json(value: &Value) -> String {
    let text = value.to_string();
    if text.len() > 256 {
        format!("{}...", &text[..256])
    } else {
        text
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    #[derive(Debug, Clone)]
    struct SeenKuCoinRequest {
        method: String,
        path: String,
        query: HashMap<String, String>,
        headers: HashMap<String, String>,
        body: Option<Value>,
    }

    async fn spawn_kucoin_rest_server(
        responses: Vec<Value>,
    ) -> (String, Arc<Mutex<Vec<SeenKuCoinRequest>>>) {
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
                let request = parse_seen_kucoin_request(&request_text);
                seen_requests.lock().unwrap().push(request);
                let body = responses
                    .lock()
                    .unwrap()
                    .next()
                    .unwrap_or_else(|| json!({"code": "200000", "data": {}}));
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

    fn parse_seen_kucoin_request(request_text: &str) -> SeenKuCoinRequest {
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
        let body = request_text
            .split_once("\r\n\r\n")
            .map(|(_, body)| body.trim())
            .filter(|body| !body.is_empty())
            .and_then(|body| serde_json::from_str(body).ok());

        SeenKuCoinRequest {
            method,
            path: path.to_string(),
            query,
            headers,
            body,
        }
    }

    fn assert_signed_kucoin_request(request: &SeenKuCoinRequest, method: &str, path: &str) {
        assert_eq!(request.method, method);
        assert_eq!(request.path, path);
        assert_eq!(
            request.headers.get("kc-api-key").map(String::as_str),
            Some("key")
        );
        assert!(request
            .headers
            .get("kc-api-sign")
            .is_some_and(|value| !value.is_empty()));
        assert!(request
            .headers
            .get("kc-api-timestamp")
            .is_some_and(|value| !value.is_empty()));
        assert!(request
            .headers
            .get("kc-api-passphrase")
            .is_some_and(|value| !value.is_empty() && value != "passphrase"));
        assert_eq!(
            request
                .headers
                .get("kc-api-key-version")
                .map(String::as_str),
            Some("2")
        );
        assert_eq!(
            request.headers.get("content-type").map(String::as_str),
            Some("application/json")
        );
    }

    #[test]
    fn kucoin_capabilities_should_match_implemented_private_routes() {
        let client = KuCoinSpotClient::new(KuCoinSpotConfig::default());
        let capabilities = client.capabilities();
        assert_eq!(capabilities.market_type, MarketType::Spot);
        assert!(capabilities.supports_private_user_stream);
        assert!(capabilities.supports_cancel_all_orders);
        assert!(capabilities.supports_quote_market_order);
        assert!(capabilities.supports_amend_order);
        assert!(!capabilities.supports_order_list);
        assert!(capabilities.supports_open_orders);
        assert!(capabilities.supports_fee_api);
    }

    #[test]
    fn kucoin_symbol_rule_should_parse_spot_metadata() {
        let rule = parse_symbol_rule(&json!({
            "symbol": "BTC-USDT",
            "baseCurrency": "BTC",
            "quoteCurrency": "USDT",
            "priceIncrement": "0.1",
            "baseIncrement": "0.000001",
            "baseMinSize": "0.00001",
            "quoteMinSize": "1",
            "enableTrading": true
        }))
        .unwrap();
        assert_eq!(rule.internal_symbol, "BTCUSDT");
        assert_eq!(rule.exchange_symbol, "BTC-USDT");
        assert_eq!(rule.status, SymbolStatus::Trading);
        assert!(rule.supported_order_types.contains(&OrderType::PostOnly));
        assert!(rule.supported_time_in_force.contains(&TimeInForce::GTX));
    }

    #[test]
    fn kucoin_orderbook_should_parse_public_snapshot() {
        let book = parse_orderbook_snapshot(
            &json!({
                "bids": [["99", "1"]],
                "asks": [["100", "2"]],
                "time": Utc::now().timestamp_millis(),
                "sequence": "42"
            }),
            "BTC-USDT",
            10_000,
        )
        .unwrap();
        assert_eq!(book.symbol, "BTCUSDT");
        assert_eq!(book.best_bid, Some(99.0));
        assert_eq!(book.best_ask, Some(100.0));
    }

    #[tokio::test]
    async fn kucoin_orderbook_should_validate_depth_before_request() {
        let client = KuCoinSpotClient::new(KuCoinSpotConfig::default());

        let err = client.get_orderbook("BTCUSDT", 0).await.unwrap_err();

        assert!(matches!(
            err,
            ExchangeClientError::Validation { field: "depth", .. }
        ));
    }

    #[tokio::test]
    async fn kucoin_get_order_should_validate_order_id_before_request() {
        let client = KuCoinSpotClient::new(KuCoinSpotConfig::default());

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
    async fn kucoin_spot_client_should_route_common_private_rest_readbacks() {
        let (base_url, seen) = spawn_kucoin_rest_server(vec![
            json!({
                "code": "200000",
                "data": [
                    {"type": "trade", "currency": "BTC", "available": "0.5", "holds": "0.1"},
                    {"type": "trade", "currency": "USDT", "available": "123.45", "holds": "1.55"}
                ]
            }),
            json!({
                "code": "200000",
                "data": {
                    "id": "1001",
                    "symbol": "BTC-USDT",
                    "clientOid": "CID1001",
                    "side": "buy",
                    "type": "limit",
                    "isActive": true,
                    "price": "65000",
                    "size": "0.01",
                    "dealSize": "0.006",
                    "dealFunds": "390.06",
                    "createdAt": 1743054548123i64
                }
            }),
            json!({
                "code": "200000",
                "data": {
                    "items": [{
                        "id": "1002",
                        "symbol": "BTC-USDT",
                        "clientOid": "CID1002",
                        "side": "sell",
                        "type": "limit",
                        "isActive": true,
                        "price": "70000",
                        "size": "0.02",
                        "dealSize": "0",
                        "createdAt": 1743054549000i64
                    }]
                }
            }),
            json!({
                "code": "200000",
                "data": [{
                    "symbol": "BTC-USDT",
                    "makerFeeRate": "0.001",
                    "takerFeeRate": "0.0015"
                }]
            }),
            json!({
                "code": "200000",
                "data": {
                    "items": [{
                        "tradeId": "9001",
                        "orderId": "1001",
                        "clientOid": "CID1001",
                        "symbol": "BTC-USDT",
                        "side": "buy",
                        "price": "65010",
                        "size": "0.006",
                        "fee": "0.39",
                        "feeCurrency": "USDT",
                        "liquidity": "taker",
                        "createdAt": 1743054550000i64
                    }]
                }
            }),
        ])
        .await;
        let client = KuCoinSpotClient::new(KuCoinSpotConfig {
            api_key: "key".to_string(),
            api_secret: "secret".to_string(),
            api_passphrase: "passphrase".to_string(),
            base_url,
            dry_run: false,
            ..KuCoinSpotConfig::default()
        });

        let balances = client.get_balances().await.unwrap();
        assert_eq!(balances.exchange, "kucoin");
        assert_eq!(balances.market_type, MarketType::Spot);
        assert_eq!(balances.balances.len(), 2);
        assert_eq!(balances.balances[0].asset, "BTC");
        assert_eq!(balances.balances[0].available, 0.5);
        assert_eq!(balances.balances[0].locked_by_exchange, 0.1);

        let order = client.get_order("BTCUSDT", "1001").await.unwrap();
        assert_eq!(order.exchange, "kucoin");
        assert_eq!(order.market_type, MarketType::Spot);
        assert_eq!(order.symbol, "BTCUSDT");
        assert_eq!(order.order_id, "1001");
        assert_eq!(order.client_order_id.as_deref(), Some("CID1001"));
        assert_eq!(order.side, OrderSide::Buy);
        assert_eq!(order.status, OrderStatus::New);
        assert_eq!(order.price, Some(65_000.0));
        assert_eq!(order.quantity, 0.01);
        assert_eq!(order.filled_quantity, 0.006);
        assert_eq!(order.average_price, Some(65_010.0));

        let open_orders = client.get_open_orders(Some("BTCUSDT")).await.unwrap();
        assert_eq!(open_orders.len(), 1);
        assert_eq!(open_orders[0].symbol, "BTCUSDT");
        assert_eq!(open_orders[0].order_id, "1002");
        assert_eq!(open_orders[0].client_order_id.as_deref(), Some("CID1002"));
        assert_eq!(open_orders[0].side, OrderSide::Sell);
        assert_eq!(open_orders[0].status, OrderStatus::New);
        assert_eq!(open_orders[0].price, Some(70_000.0));
        assert_eq!(open_orders[0].quantity, 0.02);
        assert_eq!(open_orders[0].filled_quantity, 0.0);
        assert_eq!(open_orders[0].average_price, None);

        let fee_rate = client.get_fee_rate("BTCUSDT").await.unwrap();
        assert_eq!(fee_rate.maker, 0.001);
        assert_eq!(fee_rate.taker, 0.0015);

        let fills = client.get_recent_fills("BTCUSDT").await.unwrap();
        assert_eq!(fills.len(), 1);
        let fill = &fills[0];
        assert_eq!(fill.exchange, "kucoin");
        assert_eq!(fill.market_type, MarketType::Spot);
        assert_eq!(fill.symbol, "BTCUSDT");
        assert_eq!(fill.trade_id.as_deref(), Some("9001"));
        assert_eq!(fill.order_id.as_deref(), Some("1001"));
        assert_eq!(fill.client_order_id.as_deref(), Some("CID1001"));
        assert_eq!(fill.side, OrderSide::Buy);
        assert_eq!(fill.price, 65_010.0);
        assert_eq!(fill.quantity, 0.006);
        assert_eq!(fill.fee_asset.as_deref(), Some("USDT"));
        assert_eq!(fill.fee_amount, Some(0.39));
        assert_eq!(fill.liquidity, LiquidityRole::Taker);

        let requests = seen.lock().unwrap().clone();
        assert_eq!(requests.len(), 5);
        assert_signed_kucoin_request(&requests[0], "GET", "/api/v1/accounts");
        assert!(requests[0].query.is_empty());

        assert_signed_kucoin_request(&requests[1], "GET", "/api/v1/hf/orders/1001");
        assert_eq!(
            requests[1].query.get("symbol").map(String::as_str),
            Some("BTC-USDT")
        );

        assert_signed_kucoin_request(&requests[2], "GET", "/api/v1/hf/orders/active");
        assert_eq!(
            requests[2].query.get("status").map(String::as_str),
            Some("active")
        );
        assert_eq!(
            requests[2].query.get("symbol").map(String::as_str),
            Some("BTC-USDT")
        );

        assert_signed_kucoin_request(&requests[3], "GET", "/api/v1/trade-fees");
        assert_eq!(
            requests[3].query.get("symbols").map(String::as_str),
            Some("BTC-USDT")
        );

        assert_signed_kucoin_request(&requests[4], "GET", "/api/v1/hf/fills");
        assert_eq!(
            requests[4].query.get("symbol").map(String::as_str),
            Some("BTC-USDT")
        );
    }

    #[tokio::test]
    async fn kucoin_spot_client_should_route_order_mutations() {
        let (base_url, seen) = spawn_kucoin_rest_server(vec![
            json!({
                "code": "200000",
                "data": {"orderId": "2001", "clientOid": "LIMIT1"}
            }),
            json!({
                "code": "200000",
                "data": [{
                    "symbol": "BTC-USDT",
                    "baseCurrency": "BTC",
                    "quoteCurrency": "USDT",
                    "priceIncrement": "0.1",
                    "baseIncrement": "0.00000001",
                    "baseMinSize": "0.00001",
                    "quoteMinSize": "1",
                    "enableTrading": true
                }]
            }),
            json!({
                "code": "200000",
                "data": [
                    {"type": "trade", "currency": "BTC", "available": "0.5", "holds": "0"},
                    {"type": "trade", "currency": "USDT", "available": "1000", "holds": "0"}
                ]
            }),
            json!({
                "code": "200000",
                "data": {"orderId": "2002", "clientOid": "QUOTE1"}
            }),
            json!({
                "code": "200000",
                "data": {"orderId": "2001"}
            }),
            json!({
                "code": "200000",
                "data": {"cancelledOrderIds": ["2003", "2004"]}
            }),
            json!({
                "code": "200000",
                "data": {"newOrderId": "2005", "clientOid": "AMEND1"}
            }),
        ])
        .await;
        let client = KuCoinSpotClient::new(KuCoinSpotConfig {
            api_key: "key".to_string(),
            api_secret: "secret".to_string(),
            api_passphrase: "passphrase".to_string(),
            base_url,
            dry_run: false,
            ..KuCoinSpotConfig::default()
        });

        let placed = client
            .place_order(OrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                side: OrderSide::Buy,
                position_side: PositionSide::None,
                order_type: OrderType::Limit,
                time_in_force: Some(TimeInForce::GTC),
                quantity: 0.01,
                price: Some(65_000.0),
                client_order_id: Some("LIMIT1".to_string()),
                reduce_only: false,
            })
            .await
            .unwrap();
        assert_eq!(placed.exchange, "kucoin");
        assert_eq!(placed.market_type, MarketType::Spot);
        assert_eq!(placed.order_id, "2001");
        assert_eq!(placed.client_order_id.as_deref(), Some("LIMIT1"));
        assert_eq!(placed.status, OrderStatus::New);

        let quote = client
            .place_quote_market_order(QuoteMarketOrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                side: OrderSide::Buy,
                quote_quantity: 125.5,
                client_order_id: Some("QUOTE1".to_string()),
            })
            .await
            .unwrap();
        assert_eq!(quote.order_id, "2002");
        assert_eq!(quote.order_type, OrderType::Market);
        assert_eq!(quote.quantity, 125.5);

        let cancelled = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                order_id: Some("2001".to_string()),
                client_order_id: Some("LIMIT1".to_string()),
            })
            .await
            .unwrap();
        assert_eq!(cancelled.order_id.as_deref(), Some("2001"));
        assert_eq!(cancelled.client_order_id.as_deref(), Some("LIMIT1"));
        assert_eq!(cancelled.status, OrderStatus::Cancelled);

        let cancel_all = client
            .cancel_all_orders(CancelAllOrdersRequest::for_symbol(
                MarketType::Spot,
                "BTCUSDT",
            ))
            .await
            .unwrap();
        assert_eq!(cancel_all.exchange, "kucoin");
        assert_eq!(cancel_all.market_type, MarketType::Spot);
        assert_eq!(cancel_all.symbol.as_deref(), Some("BTCUSDT"));
        assert_eq!(cancel_all.cancelled_orders, 2);

        let amended = client
            .amend_order(AmendOrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                order_id: Some("2005".to_string()),
                client_order_id: Some("AMEND1".to_string()),
                new_client_order_id: None,
                new_quantity: 0.005,
            })
            .await
            .unwrap();
        assert_eq!(amended.order_id, "2005");
        assert_eq!(amended.client_order_id.as_deref(), Some("AMEND1"));
        assert_eq!(amended.quantity, 0.005);

        assert!(!client.capabilities().supports_order_list);

        let requests = seen.lock().unwrap().clone();
        assert_eq!(requests.len(), 7);

        assert_signed_kucoin_request(&requests[0], "POST", "/api/v1/hf/orders");
        let body = requests[0].body.as_ref().unwrap();
        assert_eq!(body["symbol"], "BTC-USDT");
        assert_eq!(body["side"], "buy");
        assert_eq!(body["type"], "limit");
        assert_eq!(body["size"], "0.01");
        assert_eq!(body["price"], "65000");
        assert_eq!(body["clientOid"], "LIMIT1");
        assert_eq!(body["tradeType"], "TRADE");

        assert_eq!(requests[1].method, "GET");
        assert_eq!(requests[1].path, "/api/v2/symbols");

        assert_signed_kucoin_request(&requests[2], "GET", "/api/v1/accounts");
        assert!(requests[2].query.is_empty());

        assert_signed_kucoin_request(&requests[3], "POST", "/api/v1/hf/orders");
        let body = requests[3].body.as_ref().unwrap();
        assert_eq!(body["symbol"], "BTC-USDT");
        assert_eq!(body["side"], "buy");
        assert_eq!(body["type"], "market");
        assert_eq!(body["funds"], "125.5");
        assert_eq!(body["clientOid"], "QUOTE1");
        assert_eq!(body["tradeType"], "TRADE");

        assert_signed_kucoin_request(&requests[4], "DELETE", "/api/v1/hf/orders/2001");
        assert_eq!(
            requests[4].query.get("symbol").map(String::as_str),
            Some("BTC-USDT")
        );

        assert_signed_kucoin_request(&requests[5], "DELETE", "/api/v1/hf/orders");
        assert_eq!(
            requests[5].query.get("symbol").map(String::as_str),
            Some("BTC-USDT")
        );

        assert_signed_kucoin_request(&requests[6], "POST", "/api/v1/hf/orders/alter");
        let body = requests[6].body.as_ref().unwrap();
        assert_eq!(body["symbol"], "BTC-USDT");
        assert_eq!(body["orderId"], "2005");
        assert_eq!(body["clientOid"], "AMEND1");
        assert_eq!(body["newSize"], "0.005");
    }

    #[test]
    fn kucoin_order_body_should_map_common_spot_order_types() {
        let limit = OrderRequest {
            market_type: MarketType::Spot,
            symbol: "BTCUSDT".to_string(),
            side: OrderSide::Buy,
            position_side: PositionSide::None,
            order_type: OrderType::PostOnly,
            time_in_force: None,
            quantity: 0.001,
            price: Some(65_000.0),
            client_order_id: Some("crossarb-s-mk-1-deadbeef".to_string()),
            reduce_only: false,
        };
        let body = kucoin_order_body(&limit, "BTC-USDT").unwrap();
        assert_eq!(body.get("symbol").and_then(Value::as_str), Some("BTC-USDT"));
        assert_eq!(body.get("type").and_then(Value::as_str), Some("limit"));
        assert_eq!(body.get("price").and_then(Value::as_str), Some("65000"));
        assert_eq!(body.get("postOnly").and_then(Value::as_bool), Some(true));

        let mut gtx = limit.clone();
        gtx.order_type = OrderType::Limit;
        gtx.time_in_force = Some(TimeInForce::GTX);
        let gtx_body = kucoin_order_body(&gtx, "BTC-USDT").unwrap();
        assert_eq!(
            gtx_body.get("postOnly").and_then(Value::as_bool),
            Some(true)
        );
        assert!(gtx_body.get("timeInForce").is_none());

        let mut ioc = limit.clone();
        ioc.order_type = OrderType::IOC;
        ioc.time_in_force = None;
        let ioc_body = kucoin_order_body(&ioc, "BTC-USDT").unwrap();
        assert_eq!(
            ioc_body.get("timeInForce").and_then(Value::as_str),
            Some("IOC")
        );
        assert!(ioc_body.get("postOnly").is_none());

        let market =
            kucoin_order_body(&OrderRequest::spot_market_buy("BTCUSDT", 0.002), "BTC-USDT");
        assert!(matches!(
            market,
            Err(ExchangeClientError::Validation {
                field: "client_order_id",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn kucoin_place_order_should_ack_in_dry_run() {
        let client = KuCoinSpotClient::new(KuCoinSpotConfig {
            dry_run: true,
            ..KuCoinSpotConfig::default()
        });

        let response = client
            .place_order(OrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                side: OrderSide::Buy,
                position_side: PositionSide::None,
                order_type: OrderType::Limit,
                time_in_force: Some(TimeInForce::GTC),
                quantity: 0.01,
                price: Some(65_000.0),
                client_order_id: None,
                reduce_only: false,
            })
            .await
            .unwrap();

        assert_eq!(response.exchange, "kucoin");
        assert_eq!(response.market_type, MarketType::Spot);
        assert_eq!(response.symbol, "BTCUSDT");
        assert_eq!(response.side, OrderSide::Buy);
        assert_eq!(response.order_type, OrderType::Limit);
        assert_eq!(response.price, Some(65_000.0));
        assert_eq!(response.quantity, 0.01);
        assert!(response.client_order_id.is_some());
        assert!(response.order_id.starts_with("dry-kucoin-"));
    }

    #[test]
    fn kucoin_quote_market_order_body_should_use_funds_for_buy() {
        let mut request = QuoteMarketOrderRequest::spot_buy("BTCUSDT", 25.5);
        request.client_order_id = Some("crossarb-s-mk-1-deadbeef".to_string());

        let body = kucoin_quote_market_order_body(&request, "BTC-USDT").unwrap();

        assert_eq!(body.get("symbol").and_then(Value::as_str), Some("BTC-USDT"));
        assert_eq!(body.get("side").and_then(Value::as_str), Some("buy"));
        assert_eq!(body.get("type").and_then(Value::as_str), Some("market"));
        assert_eq!(body.get("funds").and_then(Value::as_str), Some("25.5"));
        assert_eq!(body.get("tradeType").and_then(Value::as_str), Some("TRADE"));
        assert!(body.get("size").is_none());

        let sell = QuoteMarketOrderRequest {
            side: OrderSide::Sell,
            ..request
        };
        assert!(kucoin_quote_market_order_body(&sell, "BTC-USDT").is_err());
    }

    #[tokio::test]
    async fn kucoin_quote_market_order_should_ack_in_dry_run() {
        let client = KuCoinSpotClient::new(KuCoinSpotConfig {
            dry_run: true,
            ..KuCoinSpotConfig::default()
        });

        let response = client
            .place_quote_market_order(QuoteMarketOrderRequest::spot_buy("BTCUSDT", 25.5))
            .await
            .unwrap();

        assert_eq!(response.exchange, "kucoin");
        assert_eq!(response.symbol, "BTCUSDT");
        assert_eq!(response.side, OrderSide::Buy);
        assert_eq!(response.order_type, OrderType::Market);
        assert_eq!(response.quantity, 25.5);
        assert!(response.client_order_id.is_some());
    }

    #[tokio::test]
    async fn kucoin_cancel_order_should_ack_and_validate_market_type_in_dry_run() {
        let client = KuCoinSpotClient::new(KuCoinSpotConfig {
            dry_run: true,
            ..KuCoinSpotConfig::default()
        });

        let response = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                order_id: Some("66d1".to_string()),
                client_order_id: None,
            })
            .await
            .unwrap();
        assert_eq!(response.exchange, "kucoin");
        assert_eq!(response.market_type, MarketType::Spot);
        assert_eq!(response.symbol, "BTCUSDT");
        assert_eq!(response.order_id.as_deref(), Some("66d1"));

        let response = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
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
                symbol: "BTCUSDT".to_string(),
                order_id: Some("66d1".to_string()),
                client_order_id: None,
            })
            .await
            .unwrap_err();
        assert!(error.to_string().contains("only supports MarketType::Spot"));

        let error = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
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
    async fn kucoin_cancel_all_orders_should_ack_and_validate_market_type_in_dry_run() {
        let client = KuCoinSpotClient::new(KuCoinSpotConfig {
            dry_run: true,
            ..KuCoinSpotConfig::default()
        });

        let response = client
            .cancel_all_orders(CancelAllOrdersRequest::for_symbol(
                MarketType::Spot,
                "BTCUSDT",
            ))
            .await
            .unwrap();
        assert_eq!(response.exchange, "kucoin");
        assert_eq!(response.market_type, MarketType::Spot);
        assert_eq!(response.symbol.as_deref(), Some("BTCUSDT"));
        assert_eq!(response.cancelled_orders, 0);

        let error = client
            .cancel_all_orders(CancelAllOrdersRequest::for_symbol(
                MarketType::Perpetual,
                "BTCUSDT",
            ))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("only supports MarketType::Spot"));
    }

    #[test]
    fn kucoin_amend_order_body_should_use_new_size() {
        let request = AmendOrderRequest {
            market_type: MarketType::Spot,
            symbol: "BTCUSDT".to_string(),
            order_id: Some("66d1".to_string()),
            client_order_id: None,
            new_client_order_id: None,
            new_quantity: 0.005,
        };

        let body = kucoin_amend_order_body(&request, "BTC-USDT").unwrap();

        assert_eq!(body.get("symbol").and_then(Value::as_str), Some("BTC-USDT"));
        assert_eq!(body.get("orderId").and_then(Value::as_str), Some("66d1"));
        assert_eq!(body.get("newSize").and_then(Value::as_str), Some("0.005"));
        assert!(body.get("newPrice").is_none());

        let client_oid_request = AmendOrderRequest {
            order_id: None,
            client_order_id: Some("crossarb-s-mk-1-deadbeef".to_string()),
            ..request.clone()
        };
        let client_oid_body = kucoin_amend_order_body(&client_oid_request, "BTC-USDT").unwrap();
        assert_eq!(
            client_oid_body.get("clientOid").and_then(Value::as_str),
            Some("crossarb-s-mk-1-deadbeef")
        );

        let client_oid_body = kucoin_amend_order_body(
            &AmendOrderRequest {
                order_id: Some("   ".to_string()),
                client_order_id: Some("crossarb-s-mk-1-deadbeef".to_string()),
                ..request.clone()
            },
            "BTC-USDT",
        )
        .unwrap();
        assert!(client_oid_body.get("orderId").is_none());
        assert_eq!(
            client_oid_body.get("clientOid").and_then(Value::as_str),
            Some("crossarb-s-mk-1-deadbeef")
        );

        let unsupported = AmendOrderRequest {
            new_client_order_id: Some("crossarb-s-mk-1-newid".to_string()),
            ..request
        };
        assert!(kucoin_amend_order_body(&unsupported, "BTC-USDT").is_err());
    }

    #[test]
    fn kucoin_amend_order_ack_should_parse_new_order_id() {
        let request = AmendOrderRequest::reduce_quantity_by_order_id(
            MarketType::Spot,
            "BTCUSDT",
            "66d1",
            0.005,
        );

        let response = parse_amend_order_ack(
            &json!({"newOrderId":"66d2","clientOid":"crossarb-s-mk-1-deadbeef"}),
            &request,
            "BTC-USDT",
        )
        .unwrap();

        assert_eq!(response.exchange, "kucoin");
        assert_eq!(response.symbol, "BTCUSDT");
        assert_eq!(response.order_id, "66d2");
        assert_eq!(response.quantity, 0.005);
        assert_eq!(
            response.client_order_id.as_deref(),
            Some("crossarb-s-mk-1-deadbeef")
        );
    }

    #[tokio::test]
    async fn kucoin_amend_order_should_ack_in_dry_run() {
        let client = KuCoinSpotClient::new(KuCoinSpotConfig {
            dry_run: true,
            ..KuCoinSpotConfig::default()
        });

        let response = client
            .amend_order(AmendOrderRequest::reduce_quantity_by_order_id(
                MarketType::Spot,
                "BTCUSDT",
                "66d1",
                0.005,
            ))
            .await
            .unwrap();

        assert_eq!(response.exchange, "kucoin");
        assert_eq!(response.symbol, "BTCUSDT");
        assert_eq!(response.order_id, "66d1");
        assert_eq!(response.quantity, 0.005);

        let response = client
            .amend_order(AmendOrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                order_id: None,
                client_order_id: Some("KUCOINAMEND1".to_string()),
                new_client_order_id: None,
                new_quantity: 0.0045,
            })
            .await
            .unwrap();
        assert!(response.order_id.starts_with("dry-kucoin-"));
        assert_eq!(response.client_order_id.as_deref(), Some("KUCOINAMEND1"));
        assert_eq!(response.quantity, 0.0045);

        let response = client
            .amend_order(AmendOrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                order_id: Some("66d1".to_string()),
                client_order_id: Some("   ".to_string()),
                new_client_order_id: Some("   ".to_string()),
                new_quantity: 0.004,
            })
            .await
            .unwrap();
        assert_eq!(response.order_id, "66d1");
        assert_eq!(response.client_order_id, None);

        let unsupported = client
            .amend_order(AmendOrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                order_id: Some("66d1".to_string()),
                client_order_id: None,
                new_client_order_id: Some("LDRYKUCOINNEW".to_string()),
                new_quantity: 0.004,
            })
            .await
            .unwrap_err();
        assert!(matches!(unsupported, ExchangeClientError::Unsupported(_)));

        let invalid_new_client_id = client
            .amend_order(AmendOrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                order_id: Some("66d1".to_string()),
                client_order_id: None,
                new_client_order_id: Some("bad/id".to_string()),
                new_quantity: 0.004,
            })
            .await
            .unwrap_err();
        assert!(matches!(
            invalid_new_client_id,
            ExchangeClientError::Validation {
                field: "client_order_id",
                ..
            }
        ));
    }

    #[test]
    fn kucoin_order_ack_should_parse_submit_response() {
        let request = OrderRequest {
            market_type: MarketType::Spot,
            symbol: "BTCUSDT".to_string(),
            side: OrderSide::Sell,
            position_side: PositionSide::None,
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: 0.01,
            price: Some(70_000.0),
            client_order_id: Some("crossarb-s-mk-1-deadbeef".to_string()),
            reduce_only: false,
        };
        let ack = parse_order_ack(
            &json!({"orderId":"66d1","clientOid":"crossarb-s-mk-1-deadbeef"}),
            &request,
            "BTC-USDT",
        )
        .unwrap();
        assert_eq!(ack.exchange, "kucoin");
        assert_eq!(ack.symbol, "BTCUSDT");
        assert_eq!(ack.order_id, "66d1");
        assert_eq!(ack.status, OrderStatus::New);
    }

    #[test]
    fn kucoin_cancel_response_should_parse_cancelled_order_ids() {
        assert_eq!(
            kucoin_cancelled_order_id(&json!({"cancelledOrderIds":["66d1"]})).as_deref(),
            Some("66d1")
        );
        assert_eq!(
            kucoin_cancelled_order_id(&json!({"cancelledOrderId":"66d2"})).as_deref(),
            Some("66d2")
        );
    }

    #[test]
    fn kucoin_cancel_all_response_should_count_only_order_id_shapes() {
        assert_eq!(kucoin_cancel_all_cancelled_count(&json!("success")), 0);
        assert_eq!(
            kucoin_cancel_all_cancelled_count(
                &json!({"succeedSymbols":["BTC-USDT"],"failedSymbols":[]})
            ),
            0
        );
        assert_eq!(
            kucoin_cancel_all_cancelled_count(&json!({"cancelledOrderIds":["66d1","66d2"]})),
            2
        );
        assert_eq!(
            kucoin_cancel_all_cancelled_count(
                &json!({"tradeType":"SPOT","items":[{"orderId":"66d1"},{"orderId":"66d2"}]})
            ),
            2
        );
    }

    #[tokio::test]
    async fn kucoin_order_list_should_validate_before_unsupported() {
        use crate::exchanges::unified::{
            OrderListConditionalLeg, OrderListLegType, OrderListRequest,
        };

        fn oco_request() -> OrderListRequest {
            OrderListRequest::Oco {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                list_client_order_id: Some("KUCOINOCOLIST1".to_string()),
                side: OrderSide::Sell,
                quantity: 0.01,
                above: OrderListConditionalLeg {
                    order_type: OrderListLegType::LimitMaker,
                    price: Some(70_000.0),
                    stop_price: None,
                    time_in_force: None,
                    client_order_id: Some("KUCOINABOVE1".to_string()),
                },
                below: OrderListConditionalLeg {
                    order_type: OrderListLegType::StopLossLimit,
                    price: Some(59_500.0),
                    stop_price: Some(60_000.0),
                    time_in_force: Some(TimeInForce::GTC),
                    client_order_id: Some("KUCOINBELOW1".to_string()),
                },
            }
        }

        let client = KuCoinSpotClient::new(KuCoinSpotConfig {
            dry_run: false,
            base_url: "http://127.0.0.1:9".to_string(),
            ..KuCoinSpotConfig::default()
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
    fn kucoin_private_order_stream_should_parse_order_and_fill() {
        let raw = json!({
            "type": "message",
            "topic": "/spotMarket/tradeOrders",
            "subject": "orderChange",
            "data": {
                "symbol": "BTC-USDT",
                "orderId": "66d1",
                "clientOid": "crossarb-s-mk-1-deadbeef",
                "side": "buy",
                "orderType": "limit",
                "status": "match",
                "type": "match",
                "price": "65000",
                "size": "0.01",
                "filledSize": "0.004",
                "matchPrice": "65001",
                "matchSize": "0.004",
                "tradeId": "trade-1",
                "liquidity": "taker",
                "fee": "0.01",
                "feeCurrency": "USDT",
                "orderTime": 1700000000000000000_i64
            }
        })
        .to_string();
        let events = parse_user_stream_message(&raw).unwrap();
        assert_eq!(events.len(), 2);
        match &events[0] {
            UserStreamEvent::Order(order) => {
                assert_eq!(order.symbol, "BTCUSDT");
                assert_eq!(order.status, OrderStatus::PartiallyFilled);
                assert_eq!(order.filled_quantity, 0.004);
            }
            other => panic!("expected order event, got {other:?}"),
        }
        match &events[1] {
            UserStreamEvent::Fill(fill) => {
                assert_eq!(fill.trade_id.as_deref(), Some("trade-1"));
                assert_eq!(fill.liquidity, LiquidityRole::Taker);
                assert_eq!(fill.quantity, 0.004);
            }
            other => panic!("expected fill event, got {other:?}"),
        }
    }

    #[test]
    fn kucoin_private_balance_stream_should_parse_balance_snapshot() {
        let raw = json!({
            "type": "message",
            "topic": "/account/balance",
            "subject": "account.balance",
            "data": {
                "currency": "USDT",
                "available": "100.5",
                "hold": "2.5",
                "total": "103.0",
                "time": "1700000000000"
            }
        })
        .to_string();
        let events = parse_user_stream_message(&raw).unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            UserStreamEvent::Balance(snapshot) => {
                assert_eq!(snapshot.balances[0].asset, "USDT");
                assert_eq!(snapshot.balances[0].available, 100.5);
                assert_eq!(snapshot.balances[0].locked, 2.5);
            }
            other => panic!("expected balance event, got {other:?}"),
        }
    }

    #[test]
    fn kucoin_bullet_endpoint_should_append_token() {
        let endpoint = parse_bullet_endpoint(&json!({
            "instanceServers": [{"endpoint": "wss://ws-api-spot.kucoin.com"}],
            "token": "abc"
        }))
        .unwrap();
        assert_eq!(endpoint, "wss://ws-api-spot.kucoin.com?token=abc");
    }
}
