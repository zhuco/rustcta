use std::collections::HashMap;
use std::time::Duration as StdDuration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use flate2::read::GzDecoder;
use futures_util::{SinkExt, StreamExt};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::Read;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::Message;

use crate::core::error::ExchangeError as CoreExchangeError;
use crate::core::ws_connect::connect_async;
use crate::exchanges::client_order_id::{generate_client_order_id, validate_client_order_id};
use crate::exchanges::spot_reservation::{BalanceReservation, BalanceReservationManager};
use crate::exchanges::unified::{
    validate_order_against_symbol_rule, validate_order_lookup_id, validate_orderbook_depth,
    AmendOrderRequest, AssetBalance, BalanceSnapshot, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, ExchangeClient,
    ExchangeClientCapabilities, ExchangeClientError, ExchangeClientResult, ExchangeError,
    ExchangeErrorClass, ExchangeHealthStatus, FeeRate, FeeRateSource, MarketType, OrderBookLevel,
    OrderBookSnapshot, OrderRequest, OrderResponse, OrderSide, OrderStatus, OrderType,
    PositionSide, QuoteMarketOrderRequest, SymbolRule, SymbolStatus, TimeInForce, TradeFill,
    UserStreamEvent,
};
use crate::utils::SignatureHelper;

const DEFAULT_REST_BASE_URL: &str = "https://api.coinex.com/v2";
const DEFAULT_WS_URL: &str = "wss://socket.coinex.com/v2/spot";
const DEFAULT_STALE_BOOK_MS: u64 = 10_000;
const DEFAULT_RECONNECT_INTERVAL_MS: u64 = 1_000;
const DEFAULT_ORDER_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_DEPTH: u16 = 5;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoinExSpotConfig {
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
    #[serde(default = "default_depth")]
    pub orderbook_depth: u16,
    #[serde(default)]
    pub enabled_symbols: Vec<String>,
    #[serde(default)]
    pub symbol_mappings: HashMap<String, String>,
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

impl Default for CoinExSpotConfig {
    fn default() -> Self {
        Self {
            api_key: std::env::var("COINEX_API_KEY").unwrap_or_default(),
            api_secret: std::env::var("COINEX_API_SECRET").unwrap_or_default(),
            base_url: default_rest_base_url(),
            websocket_url: default_ws_url(),
            dry_run: true,
            enable_private_stream: false,
            stale_book_ms: DEFAULT_STALE_BOOK_MS,
            reconnect_interval_ms: DEFAULT_RECONNECT_INTERVAL_MS,
            max_reconnect_attempts: default_max_reconnect_attempts(),
            order_timeout_ms: DEFAULT_ORDER_TIMEOUT_MS,
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            orderbook_depth: DEFAULT_DEPTH,
            enabled_symbols: Vec::new(),
            symbol_mappings: HashMap::new(),
            fee_override: None,
            log_raw_messages: false,
        }
    }
}

#[derive(Clone)]
pub struct CoinExSpotClient {
    config: CoinExSpotConfig,
    http: reqwest::Client,
    reservations: BalanceReservationManager,
}

impl CoinExSpotClient {
    pub fn new(config: CoinExSpotConfig) -> Self {
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
        generate_client_order_id("coinex", MarketType::Spot, "spot").into_string()
    }

    async fn send_public_request(
        &self,
        endpoint: &str,
        params: HashMap<String, String>,
    ) -> ExchangeClientResult<Value> {
        let path = build_request_path(endpoint, &params);
        let response = self
            .http
            .get(format!(
                "{}{}",
                self.config.base_url.trim_end_matches('/'),
                path
            ))
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
        let request_path = if method == Method::GET {
            build_request_path(endpoint, &params)
        } else {
            endpoint.to_string()
        };
        let body_text = body
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(CoreExchangeError::from)?
            .unwrap_or_default();
        let timestamp = Utc::now().timestamp_millis().to_string();
        let signature = sign_request(
            &self.config.api_secret,
            method.as_str(),
            &request_path,
            &body_text,
            &timestamp,
        );
        let mut request = self
            .http
            .request(
                method,
                format!(
                    "{}{}",
                    self.config.base_url.trim_end_matches('/'),
                    request_path
                ),
            )
            .header("X-COINEX-KEY", &self.config.api_key)
            .header("X-COINEX-SIGN", signature)
            .header("X-COINEX-TIMESTAMP", timestamp)
            .header("Content-Type", "application/json")
            .timeout(StdDuration::from_millis(self.config.request_timeout_ms));
        if !body_text.is_empty() {
            request = request.body(body_text);
        }
        parse_response(request.send().await.map_err(CoreExchangeError::from)?).await
    }

    fn ensure_credentials(&self) -> ExchangeClientResult<()> {
        if self.config.api_key.trim().is_empty() || self.config.api_secret.trim().is_empty() {
            return Err(ExchangeClientError::Classified(ExchangeError {
                exchange: "coinex".to_string(),
                class: ExchangeErrorClass::AuthenticationFailed,
                code: None,
                message: "COINEX_API_KEY and COINEX_API_SECRET are required".to_string(),
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
                        reason: "CoinEx buy reservation requires explicit price".to_string(),
                    })?;
                let fee_rate = self
                    .get_fee_rate(&request.symbol)
                    .await?
                    .taker_fee_rate
                    .max(0.0);
                self.reservations
                    .reserve(
                        self.exchange_name(),
                        &rule.quote_asset,
                        price * request.quantity * (1.0 + fee_rate + 0.002),
                    )
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
            exchange: "coinex".to_string(),
            market_type: MarketType::Spot,
            symbol: symbol.to_string(),
            order_id: format!("dry-coinex-{}", Utc::now().timestamp_millis()),
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
            exchange: "coinex".to_string(),
            market_type: MarketType::Spot,
            symbol: symbol.to_string(),
            order_id: format!("dry-coinex-{}", Utc::now().timestamp_millis()),
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
            exchange: "coinex".to_string(),
            market_type: MarketType::Spot,
            symbol: symbol.to_string(),
            order_id: request
                .order_id()
                .map(str::to_string)
                .unwrap_or_else(|| format!("dry-coinex-{}", Utc::now().timestamp_millis())),
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
impl ExchangeClient for CoinExSpotClient {
    fn market_type(&self) -> MarketType {
        MarketType::Spot
    }

    fn exchange_name(&self) -> &str {
        "coinex"
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let mut capabilities = ExchangeClientCapabilities::spot(self.exchange_name());
        capabilities.supports_cancel_all_orders = true;
        capabilities.supports_quote_market_order = true;
        capabilities.supports_amend_order = true;
        capabilities
    }

    fn normalize_symbol(&self, symbol: &str) -> ExchangeClientResult<String> {
        normalize_coinex_symbol(symbol, &self.config.symbol_mappings)
    }

    fn denormalize_symbol(&self, symbol: &str) -> ExchangeClientResult<String> {
        normalize_coinex_symbol(symbol, &self.config.symbol_mappings)
    }

    async fn load_symbol_rules(&self) -> ExchangeClientResult<Vec<SymbolRule>> {
        let response = self
            .send_public_request("/spot/market", HashMap::new())
            .await?;
        parse_symbol_rules(&response)
    }

    async fn get_balances(&self) -> ExchangeClientResult<BalanceSnapshot> {
        let response = self
            .send_signed_request(Method::GET, "/assets/spot/balance", HashMap::new(), None)
            .await?;
        let snapshot = parse_balance_snapshot(&response)?;
        self.reservations
            .update_balances(self.exchange_name(), &snapshot.balances)?;
        Ok(BalanceSnapshot {
            balances: snapshot
                .balances
                .into_iter()
                .map(|balance| {
                    self.reservations
                        .balance_with_reservation("coinex", balance)
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
        params.insert("market".to_string(), symbol.clone());
        params.insert("limit".to_string(), normalize_depth(depth).to_string());
        params.insert("interval".to_string(), "0".to_string());
        let response = self.send_public_request("/spot/depth", params).await?;
        parse_orderbook_snapshot(&response, &symbol, self.config.stale_book_ms)
    }

    async fn place_order(&self, mut request: OrderRequest) -> ExchangeClientResult<OrderResponse> {
        request.validate()?;
        if request.market_type != MarketType::Spot {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: "CoinExSpotClient only supports MarketType::Spot".to_string(),
            });
        }
        let symbol = self.normalize_symbol(&request.symbol)?;
        request.symbol = symbol.clone();
        if request.client_order_id.is_none() {
            request.client_order_id = Some(Self::generate_client_order_id());
        }
        if let Some(client_order_id) = &request.client_order_id {
            validate_client_order_id("coinex", MarketType::Spot, client_order_id).map_err(
                |error| ExchangeClientError::Validation {
                    field: "client_order_id",
                    reason: error.to_string(),
                },
            )?;
        }
        let body = coinex_order_body(&request, &symbol)?;
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
            .send_signed_request(Method::POST, "/spot/order", HashMap::new(), Some(body))
            .await;
        match response {
            Ok(value) => parse_order_response(value.get("data").unwrap_or(&value), "coinex"),
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
                "CoinEx Spot quote-sized market orders are only supported for market buys"
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
        let body = coinex_quote_market_order_body(&request, &symbol, &rule.quote_asset)?;
        let response = self
            .send_signed_request(Method::POST, "/spot/order", HashMap::new(), Some(body))
            .await;
        match response {
            Ok(value) => parse_order_response(value.get("data").unwrap_or(&value), "coinex"),
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
                reason: "CoinExSpotClient only supports MarketType::Spot".to_string(),
            });
        }
        validate_amend_client_order_id(&request)?;
        if request.order_id().is_none() {
            return Err(ExchangeClientError::Unsupported(
                "CoinEx Spot modify-order requires exchange order_id; client_id lookup is not supported by the endpoint".to_string(),
            ));
        }
        if request.new_client_order_id().is_some() {
            return Err(ExchangeClientError::Unsupported(
                "CoinEx Spot modify-order does not support assigning a new client_id".to_string(),
            ));
        }
        let symbol = self.normalize_symbol(&request.symbol)?;
        request.symbol = symbol.clone();
        let body = coinex_amend_order_body(&request, &symbol)?;
        if self.config.dry_run {
            return Ok(self.dry_run_amend_order_response(&request, &symbol));
        }
        let value = self
            .send_signed_request(
                Method::POST,
                "/spot/modify-order",
                HashMap::new(),
                Some(body),
            )
            .await?;
        parse_order_response(value.get("data").unwrap_or(&value), "coinex")
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeClientResult<CancelOrderResponse> {
        request.validate()?;
        if request.market_type != MarketType::Spot {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: "CoinExSpotClient only supports MarketType::Spot".to_string(),
            });
        }
        validate_cancel_client_order_id(&request)?;
        let symbol = self.normalize_symbol(&request.symbol)?;
        if self.config.dry_run {
            return Ok(CancelOrderResponse {
                exchange: "coinex".to_string(),
                market_type: MarketType::Spot,
                symbol,
                order_id: request.order_id().map(str::to_string),
                client_order_id: request.client_order_id().map(str::to_string),
                status: OrderStatus::Cancelled,
                cancelled_at: Utc::now(),
            });
        }
        let mut body = serde_json::Map::new();
        body.insert("market".to_string(), Value::String(symbol.clone()));
        if let Some(order_id) = request.order_id() {
            body.insert("order_id".to_string(), Value::String(order_id.to_string()));
        }
        if let Some(client_order_id) = request.client_order_id() {
            body.insert(
                "client_id".to_string(),
                Value::String(client_order_id.to_string()),
            );
        }
        let value = self
            .send_signed_request(
                Method::DELETE,
                "/spot/order",
                HashMap::new(),
                Some(Value::Object(body)),
            )
            .await?;
        let data = value.get("data").unwrap_or(&value);
        Ok(CancelOrderResponse {
            exchange: "coinex".to_string(),
            market_type: MarketType::Spot,
            symbol,
            order_id: value_as_string(data.get("order_id"))
                .or_else(|| request.order_id().map(str::to_string)),
            client_order_id: value_as_string(data.get("client_id"))
                .or_else(|| request.client_order_id().map(str::to_string)),
            status: data
                .get("status")
                .and_then(Value::as_str)
                .map(map_coinex_order_status)
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
                reason: "CoinExSpotClient only supports MarketType::Spot".to_string(),
            });
        }
        let symbol = self.normalize_symbol(request.validate_symbol_required()?)?;
        if self.config.dry_run {
            return Ok(CancelAllOrdersResponse {
                exchange: "coinex".to_string(),
                market_type: MarketType::Spot,
                symbol: Some(symbol),
                cancelled_orders: 0,
                cancelled_at: Utc::now(),
            });
        }
        let value = self
            .send_signed_request(
                Method::POST,
                "/spot/cancel-all-order",
                HashMap::new(),
                Some(coinex_cancel_all_body(&symbol)),
            )
            .await?;
        let data = value.get("data").unwrap_or(&value);
        Ok(CancelAllOrdersResponse {
            exchange: "coinex".to_string(),
            market_type: MarketType::Spot,
            symbol: Some(symbol),
            cancelled_orders: coinex_cancel_all_cancelled_count(data),
            cancelled_at: Utc::now(),
        })
    }

    async fn get_order(&self, symbol: &str, order_id: &str) -> ExchangeClientResult<OrderResponse> {
        let order_id = validate_order_lookup_id(order_id)?;
        let mut params = HashMap::new();
        params.insert("market".to_string(), self.normalize_symbol(symbol)?);
        params.insert("order_id".to_string(), order_id.to_string());
        let value = self
            .send_signed_request(Method::GET, "/spot/order-status", params, None)
            .await?;
        parse_order_response(value.get("data").unwrap_or(&value), "coinex")
    }

    async fn get_open_orders(
        &self,
        symbol: Option<&str>,
    ) -> ExchangeClientResult<Vec<OrderResponse>> {
        let mut params = HashMap::new();
        if let Some(symbol) = symbol {
            params.insert("market".to_string(), self.normalize_symbol(symbol)?);
        }
        let value = self
            .send_signed_request(Method::GET, "/spot/pending-order", params, None)
            .await?;
        value
            .get("data")
            .and_then(Value::as_array)
            .or_else(|| value.as_array())
            .ok_or_else(|| parser_error("open orders response is not an array", &value))?
            .iter()
            .map(|item| parse_order_response(item, "coinex"))
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
        params.insert("market".to_string(), self.normalize_symbol(symbol)?);
        let value = self
            .send_signed_request(Method::GET, "/spot/market", params, None)
            .await?;
        parse_fee_rate(&value)
    }

    async fn get_recent_fills(&self, symbol: &str) -> ExchangeClientResult<Vec<TradeFill>> {
        let mut params = HashMap::new();
        params.insert("market".to_string(), self.normalize_symbol(symbol)?);
        let value = self
            .send_signed_request(Method::GET, "/spot/finished-order", params, None)
            .await?;
        value
            .get("data")
            .and_then(Value::as_array)
            .or_else(|| value.as_array())
            .ok_or_else(|| parser_error("fills response is not an array", &value))?
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
            "CoinEx Spot private user stream is not implemented; use REST polling fallback"
                .to_string(),
        ))
    }

    async fn health_check(&self) -> ExchangeClientResult<ExchangeHealthStatus> {
        Ok(ExchangeHealthStatus {
            exchange: "coinex".to_string(),
            market_type: MarketType::Spot,
            connected: self
                .send_public_request("/time", HashMap::new())
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

impl CoinExSpotClient {
    async fn run_orderbook_stream(self, symbols: Vec<String>, tx: mpsc::Sender<OrderBookSnapshot>) {
        let stale_after = StdDuration::from_millis(self.config.stale_book_ms);
        let reconnect_delay = StdDuration::from_millis(self.config.reconnect_interval_ms);
        let subscribe = serde_json::json!({
            "method": "depth.subscribe",
            "params": {
                "market_list": symbols.iter().map(|symbol| {
                    serde_json::json!([symbol, normalize_depth(self.config.orderbook_depth), "0", true])
                }).collect::<Vec<_>>()
            },
            "id": Utc::now().timestamp_millis()
        })
        .to_string();
        loop {
            log::info!(
                "CoinEx Spot websocket connecting symbols={}",
                symbols.join(",")
            );
            match connect_async(self.config.websocket_url.as_str()).await {
                Ok((mut ws, _)) => {
                    if ws.send(Message::Text(subscribe.clone())).await.is_err() {
                        sleep(reconnect_delay).await;
                        continue;
                    }
                    loop {
                        match timeout(stale_after, ws.next()).await {
                            Err(_) => {
                                log::warn!("CoinEx Spot order book stream stale; reconnecting");
                                break;
                            }
                            Ok(Some(Ok(Message::Text(text)))) => {
                                if self.config.log_raw_messages {
                                    log::debug!("CoinEx Spot WS raw={}", text);
                                }
                                match parse_ws_orderbook_message(&text, self.config.stale_book_ms) {
                                    Ok(Some(snapshot)) => {
                                        if tx.send(snapshot).await.is_err() {
                                            return;
                                        }
                                    }
                                    Ok(None) => {}
                                    Err(error) => {
                                        log::warn!("CoinEx Spot WS parse error: {}", error)
                                    }
                                }
                            }
                            Ok(Some(Ok(Message::Binary(raw)))) => {
                                match decode_coinex_binary_message(&raw).and_then(|text| {
                                    if self.config.log_raw_messages {
                                        log::debug!("CoinEx Spot WS raw={}", text);
                                    }
                                    parse_ws_orderbook_message(&text, self.config.stale_book_ms)
                                }) {
                                    Ok(Some(snapshot)) => {
                                        if tx.send(snapshot).await.is_err() {
                                            return;
                                        }
                                    }
                                    Ok(None) => {}
                                    Err(error) => {
                                        log::warn!("CoinEx Spot WS binary parse error: {}", error)
                                    }
                                }
                            }
                            Ok(Some(Ok(Message::Ping(payload)))) => {
                                let _ = ws.send(Message::Pong(payload)).await;
                            }
                            Ok(Some(Ok(Message::Close(frame)))) => {
                                log::warn!("CoinEx Spot websocket closed: {:?}", frame);
                                break;
                            }
                            Ok(Some(Ok(_))) => {}
                            Ok(Some(Err(error))) => {
                                log::warn!("CoinEx Spot websocket error: {}", error);
                                break;
                            }
                            Ok(None) => break,
                        }
                    }
                }
                Err(error) => log::warn!("CoinEx Spot websocket connect error: {}", error),
            }
            sleep(reconnect_delay).await;
        }
    }
}

pub fn sign_request(
    secret: &str,
    method: &str,
    request_path: &str,
    body: &str,
    timestamp: &str,
) -> String {
    SignatureHelper::hmac_sha256(secret, &format!("{method}{request_path}{body}{timestamp}"))
}

pub fn normalize_coinex_symbol(
    symbol: &str,
    mappings: &HashMap<String, String>,
) -> ExchangeClientResult<String> {
    let trimmed = symbol.trim();
    if trimmed.is_empty() {
        return Err(ExchangeClientError::Validation {
            field: "symbol",
            reason: "symbol must not be empty".to_string(),
        });
    }
    let normalized = trimmed.replace(['/', '-', '_'], "").to_ascii_uppercase();
    if let Some(mapped) = mappings
        .get(trimmed)
        .or_else(|| mappings.get(&trimmed.to_ascii_uppercase()))
        .or_else(|| mappings.get(&normalized))
    {
        return Ok(mapped.replace(['/', '-', '_'], "").to_ascii_uppercase());
    }
    Ok(normalized)
}

pub fn map_coinex_order_status(status: &str) -> OrderStatus {
    match status.trim().to_ascii_lowercase().as_str() {
        "open" | "not_deal" | "pending" => OrderStatus::New,
        "part_deal" | "partially_filled" => OrderStatus::PartiallyFilled,
        "done" | "filled" | "finished" => OrderStatus::Filled,
        "cancel" | "canceled" | "cancelled" => OrderStatus::Cancelled,
        "expired" => OrderStatus::Expired,
        "rejected" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}

pub fn classify_coinex_error(code: Option<i64>, message: &str) -> ExchangeErrorClass {
    let msg = message.to_ascii_lowercase();
    if code == Some(25) || msg.contains("insufficient") {
        ExchangeErrorClass::InsufficientBalance
    } else if code == Some(23) || msg.contains("market") || msg.contains("symbol") {
        ExchangeErrorClass::InvalidSymbol
    } else if code == Some(213) || msg.contains("rate") || msg.contains("too many") {
        ExchangeErrorClass::RateLimited
    } else if code == Some(3008) || msg.contains("precision") {
        ExchangeErrorClass::InvalidPrecision
    } else if code == Some(3109) || msg.contains("notional") {
        ExchangeErrorClass::MinNotionalViolation
    } else if code == Some(3610) || msg.contains("order not found") {
        ExchangeErrorClass::OrderNotFound
    } else if code == Some(401) {
        ExchangeErrorClass::AuthenticationFailed
    } else {
        ExchangeErrorClass::Unknown
    }
}

pub fn parse_symbol_rules(value: &Value) -> ExchangeClientResult<Vec<SymbolRule>> {
    let data = value.get("data").unwrap_or(value);
    let items = data
        .as_array()
        .ok_or_else(|| parser_error("market list missing array data", value))?;
    items.iter().map(parse_symbol_rule).collect()
}

pub fn parse_symbol_rule(value: &Value) -> ExchangeClientResult<SymbolRule> {
    let market = required_str(value, "market")
        .or_else(|_| required_str(value, "name"))?
        .replace(['-', '_', '/'], "")
        .to_ascii_uppercase();
    let base_asset = value
        .get("base_ccy")
        .or_else(|| value.get("base_currency"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .or_else(|| split_compact_symbol(&market).map(|(base, _)| base))
        .ok_or_else(|| parser_error("market missing base asset", value))?;
    let quote_asset = value
        .get("quote_ccy")
        .or_else(|| value.get("quote_currency"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .or_else(|| split_compact_symbol(&market).map(|(_, quote)| quote))
        .ok_or_else(|| parser_error("market missing quote asset", value))?;
    let tick_size = decimal_to_step(
        value
            .get("price_precision")
            .or_else(|| value.get("pricing_decimal")),
    )
    .unwrap_or_else(|| number_from_str(value.get("tick_size")).unwrap_or(0.0));
    let step_size = decimal_to_step(
        value
            .get("amount_precision")
            .or_else(|| value.get("trading_decimal")),
    )
    .unwrap_or_else(|| number_from_str(value.get("step_size")).unwrap_or(0.0));
    Ok(SymbolRule {
        exchange: "coinex".to_string(),
        market_type: MarketType::Spot,
        internal_symbol: market.clone(),
        exchange_symbol: market,
        base_asset,
        quote_asset,
        price_precision: precision_from_step(tick_size),
        quantity_precision: precision_from_step(step_size),
        tick_size,
        step_size,
        min_quantity: number_from_str(value.get("min_amount")).unwrap_or(0.0),
        min_notional: number_from_str(value.get("min_notional")).unwrap_or(0.0),
        max_quantity: number_from_str(value.get("max_amount")),
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
            .get("is_trading_available")
            .or_else(|| value.get("trading_enabled"))
            .and_then(Value::as_bool)
            .unwrap_or(true)
        {
            SymbolStatus::Trading
        } else {
            SymbolStatus::Suspended
        },
        raw_metadata: Some(value.clone()),
    })
}

pub fn parse_balance_snapshot(value: &Value) -> ExchangeClientResult<BalanceSnapshot> {
    let data = value.get("data").unwrap_or(value);
    let items = data
        .as_array()
        .ok_or_else(|| parser_error("balance response missing array data", value))?;
    let mut balances = Vec::new();
    for item in items {
        let asset = required_str(item, "ccy")
            .or_else(|_| required_str(item, "asset"))?
            .to_ascii_uppercase();
        let available = number_from_str(
            item.get("available")
                .or_else(|| item.get("available_balance")),
        )
        .unwrap_or(0.0);
        let locked =
            number_from_str(item.get("frozen").or_else(|| item.get("locked"))).unwrap_or(0.0);
        let total = number_from_str(item.get("total")).unwrap_or(available + locked);
        if total > 0.0 || available > 0.0 || locked > 0.0 {
            balances.push(AssetBalance::new(asset, total, available, locked));
        }
    }
    Ok(BalanceSnapshot {
        exchange: "coinex".to_string(),
        market_type: MarketType::Spot,
        balances,
        timestamp: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    value: &Value,
    symbol: &str,
    stale_book_ms: u64,
) -> ExchangeClientResult<OrderBookSnapshot> {
    let data = value.get("data").unwrap_or(value);
    let book = data.get("depth").unwrap_or(data);
    let bids = book
        .get("bids")
        .and_then(Value::as_array)
        .ok_or_else(|| parser_error("orderbook missing bids", value))?;
    let asks = book
        .get("asks")
        .and_then(Value::as_array)
        .ok_or_else(|| parser_error("orderbook missing asks", value))?;
    let bids = parse_levels(bids)?;
    let asks = parse_levels(asks)?;
    let exchange_timestamp = book
        .get("updated_at")
        .or_else(|| data.get("updated_at"))
        .or_else(|| data.get("timestamp"))
        .or_else(|| value.get("timestamp"))
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    let received_at = Utc::now();
    let latency_ms =
        exchange_timestamp.map(|ts| received_at.signed_duration_since(ts).num_milliseconds());
    Ok(OrderBookSnapshot {
        exchange: "coinex".to_string(),
        market_type: MarketType::Spot,
        symbol: symbol.to_ascii_uppercase(),
        best_bid: bids.first().map(|level| level.price),
        best_ask: asks.first().map(|level| level.price),
        bids,
        asks,
        exchange_timestamp,
        received_at,
        latency_ms,
        sequence: data
            .get("last")
            .or_else(|| book.get("last"))
            .or_else(|| data.get("sequence"))
            .or_else(|| book.get("sequence"))
            .and_then(Value::as_u64),
        is_stale: latency_ms.is_some_and(|latency| latency > stale_book_ms as i64),
    })
}

pub fn parse_ws_orderbook_message(
    text: &str,
    stale_book_ms: u64,
) -> ExchangeClientResult<Option<OrderBookSnapshot>> {
    let value: Value = serde_json::from_str(text).map_err(CoreExchangeError::from)?;
    if value
        .get("method")
        .and_then(Value::as_str)
        .is_some_and(|method| method.contains("subscribe"))
    {
        return Ok(None);
    }
    if value.get("code").and_then(Value::as_i64) == Some(0) && value.get("data").is_none() {
        return Ok(None);
    }
    let payload = value
        .get("data")
        .or_else(|| value.get("params"))
        .unwrap_or(&value);
    let symbol = payload
        .get("market")
        .and_then(Value::as_str)
        .or_else(|| value.get("market").and_then(Value::as_str))
        .unwrap_or("UNKNOWN");
    parse_orderbook_snapshot(payload, symbol, stale_book_ms).map(Some)
}

pub fn parse_private_stream_message(text: &str) -> ExchangeClientResult<Vec<UserStreamEvent>> {
    let value: Value = serde_json::from_str(text).map_err(CoreExchangeError::from)?;
    if value
        .get("method")
        .and_then(Value::as_str)
        .is_some_and(|method| method.contains("subscribe") || method.contains("login"))
        || value.get("code").and_then(Value::as_i64) == Some(0) && value.get("data").is_none()
    {
        return Ok(Vec::new());
    }
    let channel = value
        .get("method")
        .or_else(|| value.get("channel"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let payload = value
        .get("data")
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
            events.push(UserStreamEvent::Order(parse_order_response(
                &item, "coinex",
            )?));
        } else if channel.contains("deal") || channel.contains("fill") || channel.contains("trade")
        {
            events.push(UserStreamEvent::Fill(parse_fill(&item)?));
        } else if channel.contains("balance")
            || channel.contains("account")
            || channel.contains("asset")
        {
            events.push(UserStreamEvent::Balance(parse_balance_snapshot(
                &Value::Array(vec![item]),
            )?));
        }
    }
    Ok(events)
}

pub fn decode_coinex_binary_message(raw: &[u8]) -> ExchangeClientResult<String> {
    if let Ok(text) = std::str::from_utf8(raw) {
        return Ok(text.to_string());
    }

    let mut decoder = GzDecoder::new(raw);
    let mut text = String::new();
    decoder
        .read_to_string(&mut text)
        .map_err(|error| CoreExchangeError::ParseError(error.to_string()))?;
    Ok(text)
}

pub fn parse_order_response(value: &Value, exchange: &str) -> ExchangeClientResult<OrderResponse> {
    let symbol = required_str(value, "market")
        .unwrap_or("UNKNOWN")
        .replace(['-', '_', '/'], "")
        .to_ascii_uppercase();
    let quantity =
        number_from_str(value.get("amount").or_else(|| value.get("quantity"))).unwrap_or(0.0);
    let filled_quantity = number_from_str(
        value
            .get("filled_amount")
            .or_else(|| value.get("deal_amount")),
    )
    .unwrap_or(0.0);
    Ok(OrderResponse {
        exchange: exchange.to_string(),
        market_type: MarketType::Spot,
        symbol,
        order_id: value_as_string(value.get("order_id").or_else(|| value.get("id")))
            .unwrap_or_else(|| "unknown".to_string()),
        client_order_id: value_as_string(value.get("client_id")),
        side: value
            .get("side")
            .and_then(Value::as_str)
            .map(parse_side)
            .transpose()?
            .unwrap_or(OrderSide::Buy),
        position_side: PositionSide::None,
        order_type: parse_order_type(
            value.get("type").and_then(Value::as_str).unwrap_or("limit"),
            value
                .get("option")
                .or_else(|| value.get("time_in_force"))
                .and_then(Value::as_str),
        ),
        status: value
            .get("status")
            .and_then(Value::as_str)
            .map(map_coinex_order_status)
            .unwrap_or(OrderStatus::Unknown),
        price: number_from_str(value.get("price")).filter(|price| *price > 0.0),
        quantity,
        filled_quantity,
        average_price: number_from_str(value.get("avg_price")),
        created_at: first_timestamp(value, &["created_at", "create_time"]).unwrap_or_else(Utc::now),
        updated_at: first_timestamp(value, &["updated_at", "finished_at"]),
    })
}

pub fn parse_fill(value: &Value) -> ExchangeClientResult<TradeFill> {
    Ok(TradeFill {
        exchange: "coinex".to_string(),
        market_type: MarketType::Spot,
        symbol: required_str(value, "market")?
            .replace(['-', '_', '/'], "")
            .to_ascii_uppercase(),
        trade_id: value_as_string(value.get("deal_id").or_else(|| value.get("id"))),
        order_id: value_as_string(value.get("order_id")),
        client_order_id: value_as_string(value.get("client_id")),
        side: parse_side(required_str(value, "side")?)?,
        price: number_from_str(value.get("price")).unwrap_or(0.0),
        quantity: number_from_str(value.get("amount").or_else(|| value.get("quantity")))
            .unwrap_or(0.0),
        fee_asset: value_as_string(value.get("fee_ccy").or_else(|| value.get("fee_asset"))),
        fee_amount: number_from_str(value.get("fee")),
        liquidity: crate::exchanges::unified::LiquidityRole::Unknown,
        timestamp: first_timestamp(value, &["created_at", "time"]).unwrap_or_else(Utc::now),
    })
}

fn coinex_order_body(request: &OrderRequest, symbol: &str) -> ExchangeClientResult<Value> {
    let mut body = serde_json::Map::new();
    body.insert("market".to_string(), Value::String(symbol.to_string()));
    body.insert("market_type".to_string(), Value::String("SPOT".to_string()));
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
    let (order_type, option) = coinex_order_type(request)?;
    body.insert("type".to_string(), Value::String(order_type.to_string()));
    if let Some(option) = option {
        body.insert("option".to_string(), Value::String(option.to_string()));
    }
    body.insert(
        "amount".to_string(),
        Value::String(request.quantity.to_string()),
    );
    if let Some(price) = request.price {
        body.insert("price".to_string(), Value::String(price.to_string()));
    }
    if let Some(client_order_id) = &request.client_order_id {
        body.insert(
            "client_id".to_string(),
            Value::String(client_order_id.clone()),
        );
    }
    Ok(Value::Object(body))
}

fn coinex_quote_market_order_body(
    request: &QuoteMarketOrderRequest,
    symbol: &str,
    quote_asset: &str,
) -> ExchangeClientResult<Value> {
    if request.side != OrderSide::Buy {
        return Err(ExchangeClientError::Unsupported(
            "CoinEx Spot quote-sized market orders are only supported for market buys".to_string(),
        ));
    }
    let mut body = serde_json::Map::new();
    body.insert("market".to_string(), Value::String(symbol.to_string()));
    body.insert("market_type".to_string(), Value::String("SPOT".to_string()));
    body.insert("side".to_string(), Value::String("buy".to_string()));
    body.insert("type".to_string(), Value::String("market".to_string()));
    body.insert(
        "amount".to_string(),
        Value::String(request.quote_quantity.to_string()),
    );
    body.insert("ccy".to_string(), Value::String(quote_asset.to_string()));
    if let Some(client_order_id) = &request.client_order_id {
        body.insert(
            "client_id".to_string(),
            Value::String(client_order_id.clone()),
        );
    }
    Ok(Value::Object(body))
}

fn coinex_amend_order_body(
    request: &AmendOrderRequest,
    symbol: &str,
) -> ExchangeClientResult<Value> {
    let order_id = request.order_id().ok_or_else(|| {
        ExchangeClientError::Unsupported(
            "CoinEx Spot modify-order requires exchange order_id; client_id lookup is not supported by the endpoint".to_string(),
        )
    })?;
    if request.new_client_order_id().is_some() {
        return Err(ExchangeClientError::Unsupported(
            "CoinEx Spot modify-order does not support assigning a new client_id".to_string(),
        ));
    }
    let mut body = serde_json::Map::new();
    body.insert("market".to_string(), Value::String(symbol.to_string()));
    body.insert("market_type".to_string(), Value::String("SPOT".to_string()));
    body.insert(
        "order_id".to_string(),
        coinex_numeric_order_id_value(order_id)?,
    );
    body.insert(
        "amount".to_string(),
        Value::String(request.new_quantity.to_string()),
    );
    Ok(Value::Object(body))
}

fn ensure_quote_market_client_order_id(
    request: &mut QuoteMarketOrderRequest,
) -> ExchangeClientResult<()> {
    if request.client_order_id.is_none() {
        request.client_order_id = Some(CoinExSpotClient::generate_client_order_id());
    }
    if let Some(client_order_id) = &request.client_order_id {
        validate_client_order_id("coinex", request.market_type, client_order_id).map_err(
            |error| ExchangeClientError::Validation {
                field: "client_order_id",
                reason: error.to_string(),
            },
        )?;
    }
    Ok(())
}

fn validate_amend_client_order_id(request: &AmendOrderRequest) -> ExchangeClientResult<()> {
    for client_order_id in [request.client_order_id(), request.new_client_order_id()]
        .into_iter()
        .flatten()
    {
        validate_client_order_id("coinex", request.market_type, client_order_id).map_err(
            |error| ExchangeClientError::Validation {
                field: "client_order_id",
                reason: error.to_string(),
            },
        )?;
    }
    Ok(())
}

fn validate_cancel_client_order_id(request: &CancelOrderRequest) -> ExchangeClientResult<()> {
    if let Some(client_order_id) = request.client_order_id() {
        validate_client_order_id("coinex", request.market_type, client_order_id).map_err(
            |error| ExchangeClientError::Validation {
                field: "client_order_id",
                reason: error.to_string(),
            },
        )?;
    }
    Ok(())
}

fn coinex_numeric_order_id_value(order_id: &str) -> ExchangeClientResult<Value> {
    let order_id = order_id.trim();
    let numeric_id = order_id
        .parse::<u64>()
        .map_err(|_| ExchangeClientError::Validation {
            field: "order_id",
            reason: "CoinEx Spot modify-order requires numeric exchange order_id".to_string(),
        })?;
    Ok(Value::Number(numeric_id.into()))
}

fn coinex_cancel_all_body(symbol: &str) -> Value {
    let mut body = serde_json::Map::new();
    body.insert("market".to_string(), Value::String(symbol.to_string()));
    body.insert("market_type".to_string(), Value::String("SPOT".to_string()));
    Value::Object(body)
}

fn coinex_cancel_all_cancelled_count(value: &Value) -> usize {
    value
        .as_array()
        .map(Vec::len)
        .or_else(|| value.get("items").and_then(Value::as_array).map(Vec::len))
        .unwrap_or(0)
}

fn coinex_order_type(
    request: &OrderRequest,
) -> ExchangeClientResult<(&'static str, Option<&'static str>)> {
    match request.order_type {
        OrderType::Market => Ok(("market", None)),
        OrderType::Limit => Ok((
            match request.time_in_force {
                Some(TimeInForce::IOC) => "ioc",
                Some(TimeInForce::FOK) => "fok",
                Some(TimeInForce::GTX) => "maker_only",
                Some(TimeInForce::GTC) | None => "limit",
            },
            None,
        )),
        OrderType::IOC => Ok(("ioc", None)),
        OrderType::FOK => Ok(("fok", None)),
        OrderType::PostOnly => Ok(("maker_only", None)),
    }
}

fn parse_fee_rate(value: &Value) -> ExchangeClientResult<FeeRate> {
    let data = value.get("data").unwrap_or(value);
    let item = data
        .as_array()
        .and_then(|items| items.first())
        .unwrap_or(data);
    Ok(FeeRate::new(
        number_from_str(item.get("maker_fee_rate").or_else(|| item.get("maker_fee")))
            .unwrap_or(0.0),
        number_from_str(item.get("taker_fee_rate").or_else(|| item.get("taker_fee")))
            .unwrap_or(0.0),
        FeeRateSource::ExchangeApi,
    ))
}

async fn parse_response(response: reqwest::Response) -> ExchangeClientResult<Value> {
    let status = response.status();
    let value: Value = response.json().await.map_err(CoreExchangeError::from)?;
    let code = value.get("code").and_then(Value::as_i64);
    if !status.is_success() || code.is_some_and(|code| code != 0) {
        let message = value
            .get("message")
            .or_else(|| value.get("msg"))
            .and_then(Value::as_str)
            .unwrap_or("CoinEx request failed");
        return Err(ExchangeClientError::Classified(ExchangeError {
            exchange: "coinex".to_string(),
            class: classify_coinex_error(code, message),
            code: code.map(|code| code.to_string()),
            message: message.to_string(),
        }));
    }
    Ok(value)
}

fn build_request_path(endpoint: &str, params: &HashMap<String, String>) -> String {
    if params.is_empty() {
        endpoint.to_string()
    } else {
        format!(
            "{}?{}",
            endpoint,
            SignatureHelper::build_query_string(params)
        )
    }
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
    match value.to_ascii_lowercase().as_str() {
        "buy" => Ok(OrderSide::Buy),
        "sell" => Ok(OrderSide::Sell),
        _ => Err(parser_error(
            "invalid side",
            &Value::String(value.to_string()),
        )),
    }
}

fn parse_order_type(order_type: &str, tif: Option<&str>) -> OrderType {
    match (
        order_type.to_ascii_lowercase().as_str(),
        tif.map(str::to_ascii_uppercase),
    ) {
        ("market", _) => OrderType::Market,
        ("maker_only", _) => OrderType::PostOnly,
        ("ioc", _) => OrderType::IOC,
        ("fok", _) => OrderType::FOK,
        ("limit", Some(tif)) if tif == "IOC" => OrderType::IOC,
        ("limit", Some(tif)) if tif == "FOK" => OrderType::FOK,
        ("limit", Some(tif)) if tif == "MAKER_ONLY" || tif == "GTX" => OrderType::PostOnly,
        _ => OrderType::Limit,
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

fn decimal_to_step(value: Option<&Value>) -> Option<f64> {
    let decimals = value.and_then(|value| {
        value
            .as_u64()
            .or_else(|| value.as_str()?.parse::<u64>().ok())
    })?;
    Some(10_f64.powi(-(decimals as i32)))
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

fn fallback_rule(symbol: &str) -> SymbolRule {
    let (base, quote) =
        split_compact_symbol(symbol).unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
    SymbolRule {
        exchange: "coinex".to_string(),
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

fn normalize_depth(depth: u16) -> u16 {
    match depth {
        0..=5 => 5,
        6..=10 => 10,
        11..=20 => 20,
        _ => 50,
    }
}

fn parser_error(message: &str, value: &Value) -> ExchangeClientError {
    ExchangeClientError::Classified(ExchangeError {
        exchange: "coinex".to_string(),
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
    struct SeenCoinExRequest {
        method: String,
        path: String,
        query: HashMap<String, String>,
        headers: HashMap<String, String>,
        body: Option<Value>,
    }

    async fn spawn_coinex_rest_server(
        responses: Vec<Value>,
    ) -> (String, Arc<Mutex<Vec<SeenCoinExRequest>>>) {
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
                let request = parse_seen_coinex_request(&request_text);
                seen_requests.lock().unwrap().push(request);
                let body = responses
                    .lock()
                    .unwrap()
                    .next()
                    .unwrap_or_else(|| serde_json::json!({"code": 0, "data": []}));
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

    fn parse_seen_coinex_request(request_text: &str) -> SeenCoinExRequest {
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

        SeenCoinExRequest {
            method,
            path: path.to_string(),
            query,
            headers,
            body,
        }
    }

    fn assert_signed_coinex_request(request: &SeenCoinExRequest, method: &str, path: &str) {
        assert_eq!(request.method, method);
        assert_eq!(request.path, path);
        assert_eq!(
            request.headers.get("x-coinex-key").map(String::as_str),
            Some("key")
        );
        assert!(request
            .headers
            .get("x-coinex-sign")
            .is_some_and(|value| !value.is_empty()));
        assert!(request
            .headers
            .get("x-coinex-timestamp")
            .is_some_and(|value| !value.is_empty()));
        assert_eq!(
            request.headers.get("content-type").map(String::as_str),
            Some("application/json")
        );
    }

    #[test]
    fn coinex_request_signing_should_match_known_hmac() {
        let expected =
            SignatureHelper::hmac_sha256("secret", "GET/spot/balance?a=1{}1700000000000");
        assert_eq!(
            sign_request("secret", "GET", "/spot/balance?a=1", "{}", "1700000000000"),
            expected
        );
    }

    #[test]
    fn coinex_capabilities_should_advertise_cancel_all() {
        let client = CoinExSpotClient::new(CoinExSpotConfig::default());
        let capabilities = client.capabilities();
        assert_eq!(capabilities.market_type, MarketType::Spot);
        assert!(capabilities.supports_cancel_all_orders);
        assert!(capabilities.supports_quote_market_order);
        assert!(capabilities.supports_amend_order);
        assert!(!capabilities.supports_order_list);
        assert!(capabilities.supports_open_orders);
        assert!(capabilities.supports_fee_api);
    }

    #[test]
    fn coinex_symbol_rule_parsing_should_extract_precision_and_limits() {
        let value = serde_json::json!({"code":0,"data":[{"market":"CUDISUSDT","base_ccy":"CUDIS","quote_ccy":"USDT","price_precision":4,"amount_precision":2,"min_amount":"1","min_notional":"5","is_trading_available":true}]});
        let rules = parse_symbol_rules(&value).unwrap();
        assert_eq!(rules[0].internal_symbol, "CUDISUSDT");
        assert_eq!(rules[0].tick_size, 0.0001);
        assert_eq!(rules[0].step_size, 0.01);
        assert!(rules[0]
            .supported_order_types
            .contains(&OrderType::PostOnly));
        assert!(rules[0].supported_time_in_force.contains(&TimeInForce::GTX));
    }

    #[test]
    fn coinex_fallback_rule_should_advertise_v2_order_type_parity() {
        let rule = fallback_rule("CUDISUSDT");

        assert!(rule.supported_order_types.contains(&OrderType::PostOnly));
        assert!(rule.supported_order_types.contains(&OrderType::IOC));
        assert!(rule.supported_order_types.contains(&OrderType::FOK));
        assert!(rule.supported_time_in_force.contains(&TimeInForce::GTX));
    }

    #[test]
    fn coinex_balance_parsing_should_normalize_available_and_locked() {
        let value =
            serde_json::json!({"code":0,"data":[{"ccy":"USDT","available":"10","frozen":"2"}]});
        let snapshot = parse_balance_snapshot(&value).unwrap();
        assert_eq!(snapshot.balances[0].available, 10.0);
        assert_eq!(snapshot.balances[0].locked_by_exchange, 2.0);
    }

    #[test]
    fn coinex_orderbook_parsing_should_capture_best_bid_ask() {
        let value = serde_json::json!({"code":0,"data":{"bids":[["99","1"]],"asks":[["100","2"]],"last":7}});
        let book = parse_orderbook_snapshot(&value, "BTCUSDT", 1_000).unwrap();
        assert_eq!(book.best_bid, Some(99.0));
        assert_eq!(book.best_ask, Some(100.0));
        assert_eq!(book.sequence, Some(7));
    }

    #[tokio::test]
    async fn coinex_orderbook_should_validate_depth_before_request() {
        let client = CoinExSpotClient::new(CoinExSpotConfig::default());

        let err = client.get_orderbook("BTCUSDT", 0).await.unwrap_err();

        assert!(matches!(
            err,
            ExchangeClientError::Validation { field: "depth", .. }
        ));
    }

    #[tokio::test]
    async fn coinex_get_order_should_validate_order_id_before_request() {
        let client = CoinExSpotClient::new(CoinExSpotConfig::default());

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
    async fn coinex_spot_client_should_route_common_private_rest_readbacks() {
        let (base_url, seen) = spawn_coinex_rest_server(vec![
            serde_json::json!({
                "code": 0,
                "data": [
                    {"ccy": "BTC", "available": "0.5", "frozen": "0.1"},
                    {"ccy": "USDT", "available": "123.45", "frozen": "1.55"}
                ]
            }),
            serde_json::json!({
                "code": 0,
                "data": {
                    "order_id": "1001",
                    "market": "BTCUSDT",
                    "client_id": "CID1001",
                    "side": "buy",
                    "type": "limit",
                    "status": "part_deal",
                    "price": "65000",
                    "amount": "0.01",
                    "deal_amount": "0.006",
                    "avg_price": "65010",
                    "created_at": 1743054548123i64,
                    "updated_at": 1743054550000i64
                }
            }),
            serde_json::json!({
                "code": 0,
                "data": [{
                    "order_id": "1002",
                    "market": "BTCUSDT",
                    "client_id": "CID1002",
                    "side": "sell",
                    "type": "limit",
                    "status": "open",
                    "price": "70000",
                    "amount": "0.02",
                    "deal_amount": "0"
                }]
            }),
            serde_json::json!({
                "code": 0,
                "data": {
                    "market": "BTCUSDT",
                    "maker_fee_rate": "0.001",
                    "taker_fee_rate": "0.0015"
                }
            }),
            serde_json::json!({
                "code": 0,
                "data": [{
                    "deal_id": "9001",
                    "order_id": "1001",
                    "client_id": "CID1001",
                    "market": "BTCUSDT",
                    "side": "buy",
                    "price": "65010",
                    "amount": "0.006",
                    "fee": "0.39",
                    "fee_ccy": "USDT",
                    "created_at": 1743054550000i64
                }]
            }),
        ])
        .await;
        let client = CoinExSpotClient::new(CoinExSpotConfig {
            api_key: "key".to_string(),
            api_secret: "secret".to_string(),
            base_url: format!("{base_url}/v2"),
            dry_run: false,
            ..CoinExSpotConfig::default()
        });

        let balances = client.get_balances().await.unwrap();
        assert_eq!(balances.exchange, "coinex");
        assert_eq!(balances.market_type, MarketType::Spot);
        assert_eq!(balances.balances.len(), 2);
        assert_eq!(balances.balances[0].asset, "BTC");
        assert_eq!(balances.balances[0].available, 0.5);
        assert_eq!(balances.balances[0].locked_by_exchange, 0.1);

        let order = client.get_order("BTCUSDT", "1001").await.unwrap();
        assert_eq!(order.exchange, "coinex");
        assert_eq!(order.market_type, MarketType::Spot);
        assert_eq!(order.symbol, "BTCUSDT");
        assert_eq!(order.order_id, "1001");
        assert_eq!(order.client_order_id.as_deref(), Some("CID1001"));
        assert_eq!(order.side, OrderSide::Buy);
        assert_eq!(order.status, OrderStatus::PartiallyFilled);
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
        assert_eq!(fill.exchange, "coinex");
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

        let requests = seen.lock().unwrap().clone();
        assert_eq!(requests.len(), 5);
        assert_signed_coinex_request(&requests[0], "GET", "/v2/assets/spot/balance");
        assert!(requests[0].query.is_empty());

        assert_signed_coinex_request(&requests[1], "GET", "/v2/spot/order-status");
        assert_eq!(
            requests[1].query.get("market").map(String::as_str),
            Some("BTCUSDT")
        );
        assert_eq!(
            requests[1].query.get("order_id").map(String::as_str),
            Some("1001")
        );

        assert_signed_coinex_request(&requests[2], "GET", "/v2/spot/pending-order");
        assert_eq!(
            requests[2].query.get("market").map(String::as_str),
            Some("BTCUSDT")
        );

        assert_signed_coinex_request(&requests[3], "GET", "/v2/spot/market");
        assert_eq!(
            requests[3].query.get("market").map(String::as_str),
            Some("BTCUSDT")
        );

        assert_signed_coinex_request(&requests[4], "GET", "/v2/spot/finished-order");
        assert_eq!(
            requests[4].query.get("market").map(String::as_str),
            Some("BTCUSDT")
        );
    }

    #[tokio::test]
    async fn coinex_spot_client_should_route_order_mutations() {
        let symbol_rule = serde_json::json!({
            "code": 0,
            "data": [{
                "market": "CUDISUSDT",
                "base_ccy": "CUDIS",
                "quote_ccy": "USDT",
                "price_precision": 4,
                "amount_precision": 2,
                "min_amount": "1",
                "min_notional": "5",
                "is_trading_available": true
            }]
        });
        let balances = serde_json::json!({
            "code": 0,
            "data": [
                {"ccy": "CUDIS", "available": "100", "frozen": "0"},
                {"ccy": "USDT", "available": "1000", "frozen": "0"}
            ]
        });
        let fee = serde_json::json!({
            "code": 0,
            "data": {
                "market": "CUDISUSDT",
                "maker_fee_rate": "0.001",
                "taker_fee_rate": "0.0015"
            }
        });
        let (base_url, seen) = spawn_coinex_rest_server(vec![
            symbol_rule.clone(),
            balances.clone(),
            fee.clone(),
            serde_json::json!({
                "code": 0,
                "data": {
                    "order_id": "2001",
                    "market": "CUDISUSDT",
                    "client_id": "CLIENTLIMIT1",
                    "side": "buy",
                    "type": "limit",
                    "status": "open",
                    "price": "1.23",
                    "amount": "10",
                    "deal_amount": "0",
                    "created_at": 1743054548123i64
                }
            }),
            symbol_rule.clone(),
            balances,
            serde_json::json!({
                "code": 0,
                "data": {
                    "order_id": "2002",
                    "market": "CUDISUSDT",
                    "client_id": "CLIENTQUOTE1",
                    "side": "buy",
                    "type": "market",
                    "status": "open",
                    "amount": "25.5",
                    "deal_amount": "0",
                    "created_at": 1743054549000i64
                }
            }),
            serde_json::json!({
                "code": 0,
                "data": {
                    "order_id": "2001",
                    "market": "CUDISUSDT",
                    "client_id": "CLIENTLIMIT1",
                    "status": "cancel"
                }
            }),
            serde_json::json!({
                "code": 0,
                "data": [{"order_id": "2003"}, {"order_id": "2004"}]
            }),
            serde_json::json!({
                "code": 0,
                "data": {
                    "order_id": "2005",
                    "market": "CUDISUSDT",
                    "side": "buy",
                    "type": "limit",
                    "status": "open",
                    "amount": "5.5",
                    "deal_amount": "0",
                    "created_at": 1743054550000i64,
                    "updated_at": 1743054551000i64
                }
            }),
        ])
        .await;
        let client = CoinExSpotClient::new(CoinExSpotConfig {
            api_key: "key".to_string(),
            api_secret: "secret".to_string(),
            base_url: format!("{base_url}/v2"),
            dry_run: false,
            ..CoinExSpotConfig::default()
        });

        let limit_order = client
            .place_order(OrderRequest {
                market_type: MarketType::Spot,
                symbol: "CUDISUSDT".to_string(),
                side: OrderSide::Buy,
                position_side: PositionSide::None,
                order_type: OrderType::Limit,
                time_in_force: Some(TimeInForce::GTC),
                quantity: 10.0,
                price: Some(1.23),
                client_order_id: Some("CLIENTLIMIT1".to_string()),
                reduce_only: false,
            })
            .await
            .unwrap();
        assert_eq!(limit_order.order_id, "2001");
        assert_eq!(limit_order.client_order_id.as_deref(), Some("CLIENTLIMIT1"));

        let mut quote_request = QuoteMarketOrderRequest::spot_buy("CUDISUSDT", 25.5);
        quote_request.client_order_id = Some("CLIENTQUOTE1".to_string());
        let quote_order = client
            .place_quote_market_order(quote_request)
            .await
            .unwrap();
        assert_eq!(quote_order.order_id, "2002");
        assert_eq!(quote_order.client_order_id.as_deref(), Some("CLIENTQUOTE1"));

        let cancel = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Spot,
                symbol: "CUDISUSDT".to_string(),
                order_id: Some("2001".to_string()),
                client_order_id: Some("CLIENTLIMIT1".to_string()),
            })
            .await
            .unwrap();
        assert_eq!(cancel.order_id.as_deref(), Some("2001"));
        assert_eq!(cancel.client_order_id.as_deref(), Some("CLIENTLIMIT1"));
        assert_eq!(cancel.status, OrderStatus::Cancelled);

        let cancel_all = client
            .cancel_all_orders(CancelAllOrdersRequest::for_symbol(
                MarketType::Spot,
                "CUDISUSDT",
            ))
            .await
            .unwrap();
        assert_eq!(cancel_all.symbol.as_deref(), Some("CUDISUSDT"));
        assert_eq!(cancel_all.cancelled_orders, 2);

        let amend = client
            .amend_order(AmendOrderRequest::reduce_quantity_by_order_id(
                MarketType::Spot,
                "CUDISUSDT",
                "2005",
                5.5,
            ))
            .await
            .unwrap();
        assert_eq!(amend.order_id, "2005");
        assert_eq!(amend.quantity, 5.5);

        let capabilities = client.capabilities();
        assert!(capabilities.supports_quote_market_order);
        assert!(capabilities.supports_cancel_all_orders);
        assert!(capabilities.supports_amend_order);
        assert!(!capabilities.supports_order_list);

        let requests = seen.lock().unwrap().clone();
        assert_eq!(requests.len(), 10);
        assert_eq!(requests[0].method, "GET");
        assert_eq!(requests[0].path, "/v2/spot/market");
        assert!(requests[0].query.is_empty());

        assert_signed_coinex_request(&requests[1], "GET", "/v2/assets/spot/balance");
        assert_signed_coinex_request(&requests[2], "GET", "/v2/spot/market");
        assert_eq!(
            requests[2].query.get("market").map(String::as_str),
            Some("CUDISUSDT")
        );

        assert_signed_coinex_request(&requests[3], "POST", "/v2/spot/order");
        let body = requests[3].body.as_ref().unwrap();
        assert_eq!(body["market"], "CUDISUSDT");
        assert_eq!(body["market_type"], "SPOT");
        assert_eq!(body["side"], "buy");
        assert_eq!(body["type"], "limit");
        assert_eq!(body["amount"], "10");
        assert_eq!(body["price"], "1.23");
        assert_eq!(body["client_id"], "CLIENTLIMIT1");

        assert_eq!(requests[4].method, "GET");
        assert_eq!(requests[4].path, "/v2/spot/market");
        assert_signed_coinex_request(&requests[5], "GET", "/v2/assets/spot/balance");

        assert_signed_coinex_request(&requests[6], "POST", "/v2/spot/order");
        let body = requests[6].body.as_ref().unwrap();
        assert_eq!(body["market"], "CUDISUSDT");
        assert_eq!(body["market_type"], "SPOT");
        assert_eq!(body["side"], "buy");
        assert_eq!(body["type"], "market");
        assert_eq!(body["amount"], "25.5");
        assert_eq!(body["ccy"], "USDT");
        assert_eq!(body["client_id"], "CLIENTQUOTE1");

        assert_signed_coinex_request(&requests[7], "DELETE", "/v2/spot/order");
        let body = requests[7].body.as_ref().unwrap();
        assert_eq!(body["market"], "CUDISUSDT");
        assert_eq!(body["order_id"], "2001");
        assert_eq!(body["client_id"], "CLIENTLIMIT1");

        assert_signed_coinex_request(&requests[8], "POST", "/v2/spot/cancel-all-order");
        let body = requests[8].body.as_ref().unwrap();
        assert_eq!(body["market"], "CUDISUSDT");
        assert_eq!(body["market_type"], "SPOT");

        assert_signed_coinex_request(&requests[9], "POST", "/v2/spot/modify-order");
        let body = requests[9].body.as_ref().unwrap();
        assert_eq!(body["market"], "CUDISUSDT");
        assert_eq!(body["market_type"], "SPOT");
        assert_eq!(body["order_id"], 2005);
        assert_eq!(body["amount"], "5.5");
    }

    #[test]
    fn coinex_order_status_mapping_should_cover_common_states() {
        assert_eq!(map_coinex_order_status("open"), OrderStatus::New);
        assert_eq!(
            map_coinex_order_status("part_deal"),
            OrderStatus::PartiallyFilled
        );
        assert_eq!(map_coinex_order_status("done"), OrderStatus::Filled);
        assert_eq!(map_coinex_order_status("cancel"), OrderStatus::Cancelled);
    }

    #[test]
    fn coinex_fill_parsing_should_normalize_trade() {
        let value = serde_json::json!({"market":"PIPEUSDT","deal_id":"1","order_id":"2","side":"buy","price":"0.1","amount":"100","fee":"0.01","fee_ccy":"USDT","created_at":1700000000000i64});
        let fill = parse_fill(&value).unwrap();
        assert_eq!(fill.side, OrderSide::Buy);
        assert_eq!(fill.symbol, "PIPEUSDT");
    }

    #[test]
    fn coinex_order_body_should_map_v2_order_types() {
        let mut order = OrderRequest {
            market_type: MarketType::Spot,
            symbol: "CUDISUSDT".to_string(),
            side: OrderSide::Buy,
            position_side: PositionSide::None,
            order_type: OrderType::PostOnly,
            time_in_force: None,
            quantity: 10.0,
            price: Some(1.23),
            client_order_id: Some("client-1".to_string()),
            reduce_only: false,
        };

        let post_only = coinex_order_body(&order, "CUDISUSDT").unwrap();
        assert_eq!(post_only["type"], "maker_only");
        assert!(post_only.get("option").is_none());

        order.order_type = OrderType::Limit;
        order.time_in_force = Some(TimeInForce::GTX);
        let gtx = coinex_order_body(&order, "CUDISUSDT").unwrap();
        assert_eq!(gtx["type"], "maker_only");

        order.time_in_force = Some(TimeInForce::IOC);
        let ioc = coinex_order_body(&order, "CUDISUSDT").unwrap();
        assert_eq!(ioc["type"], "ioc");

        order.time_in_force = Some(TimeInForce::FOK);
        let fok = coinex_order_body(&order, "CUDISUSDT").unwrap();
        assert_eq!(fok["type"], "fok");

        assert_eq!(parse_order_type("maker_only", None), OrderType::PostOnly);
        assert_eq!(parse_order_type("ioc", None), OrderType::IOC);
        assert_eq!(parse_order_type("fok", None), OrderType::FOK);
    }

    #[tokio::test]
    async fn coinex_place_order_should_ack_in_dry_run_without_symbol_rule_readback() {
        let client = CoinExSpotClient::new(CoinExSpotConfig {
            dry_run: true,
            ..CoinExSpotConfig::default()
        });

        let response = client
            .place_order(OrderRequest {
                market_type: MarketType::Spot,
                symbol: "CUDISUSDT".to_string(),
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

        assert_eq!(response.exchange, "coinex");
        assert_eq!(response.market_type, MarketType::Spot);
        assert_eq!(response.symbol, "CUDISUSDT");
        assert_eq!(response.side, OrderSide::Buy);
        assert_eq!(response.order_type, OrderType::Limit);
        assert_eq!(response.price, Some(1.23));
        assert_eq!(response.quantity, 10.0);
        assert_eq!(response.status, OrderStatus::New);
        assert!(response.client_order_id.is_some());
        assert!(response.order_id.starts_with("dry-coinex-"));
    }

    #[test]
    fn coinex_quote_market_order_body_should_use_quote_currency_for_buy() {
        let mut request = QuoteMarketOrderRequest::spot_buy("CUDISUSDT", 25.5);
        request.client_order_id = Some("ldry-coinex-quote-1".to_string());

        let body = coinex_quote_market_order_body(&request, "CUDISUSDT", "USDT").unwrap();

        assert_eq!(body["market"], "CUDISUSDT");
        assert_eq!(body["market_type"], "SPOT");
        assert_eq!(body["side"], "buy");
        assert_eq!(body["type"], "market");
        assert_eq!(body["amount"], "25.5");
        assert_eq!(body["ccy"], "USDT");
        assert_eq!(body["client_id"], "ldry-coinex-quote-1");

        let sell = QuoteMarketOrderRequest {
            side: OrderSide::Sell,
            ..request
        };
        assert!(coinex_quote_market_order_body(&sell, "CUDISUSDT", "USDT").is_err());
    }

    #[tokio::test]
    async fn coinex_quote_market_order_should_ack_in_dry_run() {
        let client = CoinExSpotClient::new(CoinExSpotConfig {
            dry_run: true,
            ..CoinExSpotConfig::default()
        });

        let response = client
            .place_quote_market_order(QuoteMarketOrderRequest::spot_buy("CUDISUSDT", 25.5))
            .await
            .unwrap();

        assert_eq!(response.exchange, "coinex");
        assert_eq!(response.symbol, "CUDISUSDT");
        assert_eq!(response.side, OrderSide::Buy);
        assert_eq!(response.order_type, OrderType::Market);
        assert_eq!(response.quantity, 25.5);
        assert!(response.client_order_id.is_some());
    }

    #[tokio::test]
    async fn coinex_cancel_order_should_validate_market_type_in_dry_run() {
        let client = CoinExSpotClient::new(CoinExSpotConfig {
            dry_run: true,
            ..CoinExSpotConfig::default()
        });

        let response = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Spot,
                symbol: "CUDISUSDT".to_string(),
                order_id: Some("1001".to_string()),
                client_order_id: None,
            })
            .await
            .unwrap();
        assert_eq!(response.exchange, "coinex");
        assert_eq!(response.market_type, MarketType::Spot);
        assert_eq!(response.symbol, "CUDISUSDT");
        assert_eq!(response.order_id.as_deref(), Some("1001"));

        let response = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Spot,
                symbol: "CUDISUSDT".to_string(),
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
                symbol: "CUDISUSDT".to_string(),
                order_id: Some("1001".to_string()),
                client_order_id: None,
            })
            .await
            .unwrap_err();
        assert!(error.to_string().contains("only supports MarketType::Spot"));

        let error = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Spot,
                symbol: "CUDISUSDT".to_string(),
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

    #[test]
    fn coinex_amend_order_body_should_use_modify_order_contract() {
        let request = AmendOrderRequest::reduce_quantity_by_order_id(
            MarketType::Spot,
            "CUDISUSDT",
            "1001",
            5.5,
        );

        let body = coinex_amend_order_body(&request, "CUDISUSDT").unwrap();

        assert_eq!(body["market"], "CUDISUSDT");
        assert_eq!(body["market_type"], "SPOT");
        assert_eq!(body["order_id"], 1001);
        assert_eq!(body["amount"], "5.5");
        assert!(body.get("price").is_none());
        assert!(body.get("client_id").is_none());

        let client_id_only = AmendOrderRequest {
            order_id: None,
            client_order_id: Some("ldry-coinex-client-1".to_string()),
            ..request.clone()
        };
        assert!(coinex_amend_order_body(&client_id_only, "CUDISUSDT").is_err());

        let blank_order_id = AmendOrderRequest {
            order_id: Some("   ".to_string()),
            client_order_id: Some("ldry-coinex-client-1".to_string()),
            ..request.clone()
        };
        assert!(coinex_amend_order_body(&blank_order_id, "CUDISUSDT").is_err());

        let non_numeric_order_id = AmendOrderRequest {
            order_id: Some("not-numeric".to_string()),
            ..request
        };
        assert!(matches!(
            coinex_amend_order_body(&non_numeric_order_id, "CUDISUSDT"),
            Err(ExchangeClientError::Validation {
                field: "order_id",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn coinex_amend_order_should_ack_in_dry_run() {
        let client = CoinExSpotClient::new(CoinExSpotConfig {
            dry_run: true,
            ..CoinExSpotConfig::default()
        });

        let response = client
            .amend_order(AmendOrderRequest::reduce_quantity_by_order_id(
                MarketType::Spot,
                "CUDISUSDT",
                "1001",
                5.5,
            ))
            .await
            .unwrap();

        assert_eq!(response.exchange, "coinex");
        assert_eq!(response.symbol, "CUDISUSDT");
        assert_eq!(response.order_id, "1001");
        assert_eq!(response.quantity, 5.5);

        let response = client
            .amend_order(AmendOrderRequest {
                market_type: MarketType::Spot,
                symbol: "CUDISUSDT".to_string(),
                order_id: Some("1001".to_string()),
                client_order_id: Some("   ".to_string()),
                new_client_order_id: Some("   ".to_string()),
                new_quantity: 5.0,
            })
            .await
            .unwrap();
        assert_eq!(response.order_id, "1001");
        assert_eq!(response.client_order_id, None);

        let unsupported_new_client_id = client
            .amend_order(AmendOrderRequest {
                market_type: MarketType::Spot,
                symbol: "CUDISUSDT".to_string(),
                order_id: Some("1001".to_string()),
                client_order_id: None,
                new_client_order_id: Some("LDRYCOINEXNEW".to_string()),
                new_quantity: 5.0,
            })
            .await
            .unwrap_err();
        assert!(matches!(
            unsupported_new_client_id,
            ExchangeClientError::Unsupported(_)
        ));

        let invalid_client_id_only = client
            .amend_order(AmendOrderRequest {
                market_type: MarketType::Spot,
                symbol: "CUDISUSDT".to_string(),
                order_id: None,
                client_order_id: Some("bad/id".to_string()),
                new_client_order_id: None,
                new_quantity: 5.0,
            })
            .await
            .unwrap_err();
        assert!(matches!(
            invalid_client_id_only,
            ExchangeClientError::Validation {
                field: "client_order_id",
                ..
            }
        ));

        let invalid_new_client_id = client
            .amend_order(AmendOrderRequest {
                market_type: MarketType::Spot,
                symbol: "CUDISUSDT".to_string(),
                order_id: Some("1001".to_string()),
                client_order_id: None,
                new_client_order_id: Some("bad/id".to_string()),
                new_quantity: 5.0,
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

    #[tokio::test]
    async fn coinex_amend_order_should_validate_numeric_order_id_in_dry_run() {
        let client = CoinExSpotClient::new(CoinExSpotConfig {
            dry_run: true,
            ..CoinExSpotConfig::default()
        });

        let error = client
            .amend_order(AmendOrderRequest::reduce_quantity_by_order_id(
                MarketType::Spot,
                "CUDISUSDT",
                "not-numeric",
                5.5,
            ))
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            ExchangeClientError::Validation {
                field: "order_id",
                ..
            }
        ));
    }

    #[test]
    fn coinex_cancel_all_body_should_use_v2_spot_contract() {
        let body = coinex_cancel_all_body("CUDISUSDT");
        assert_eq!(body["market"], "CUDISUSDT");
        assert_eq!(body["market_type"], "SPOT");
        assert_eq!(coinex_cancel_all_cancelled_count(&serde_json::json!({})), 0);
        assert_eq!(
            coinex_cancel_all_cancelled_count(
                &serde_json::json!({"items":[{"order_id":"1"},{"order_id":"2"}]})
            ),
            2
        );
    }

    #[tokio::test]
    async fn coinex_cancel_all_orders_should_ack_and_validate_market_type_in_dry_run() {
        let client = CoinExSpotClient::new(CoinExSpotConfig {
            dry_run: true,
            ..CoinExSpotConfig::default()
        });

        let response = client
            .cancel_all_orders(CancelAllOrdersRequest::for_symbol(
                MarketType::Spot,
                "CUDISUSDT",
            ))
            .await
            .unwrap();
        assert_eq!(response.exchange, "coinex");
        assert_eq!(response.market_type, MarketType::Spot);
        assert_eq!(response.symbol.as_deref(), Some("CUDISUSDT"));
        assert_eq!(response.cancelled_orders, 0);

        let error = client
            .cancel_all_orders(CancelAllOrdersRequest::for_symbol(
                MarketType::Perpetual,
                "CUDISUSDT",
            ))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("only supports MarketType::Spot"));
    }

    #[tokio::test]
    async fn coinex_order_list_should_validate_before_unsupported() {
        use crate::exchanges::unified::{
            OrderListConditionalLeg, OrderListLegType, OrderListRequest,
        };

        fn oco_request() -> OrderListRequest {
            OrderListRequest::Oco {
                market_type: MarketType::Spot,
                symbol: "CUDISUSDT".to_string(),
                list_client_order_id: Some("COINEXOCOLIST1".to_string()),
                side: OrderSide::Sell,
                quantity: 5.0,
                above: OrderListConditionalLeg {
                    order_type: OrderListLegType::LimitMaker,
                    price: Some(2.0),
                    stop_price: None,
                    time_in_force: None,
                    client_order_id: Some("COINEXABOVE1".to_string()),
                },
                below: OrderListConditionalLeg {
                    order_type: OrderListLegType::StopLossLimit,
                    price: Some(0.8),
                    stop_price: Some(0.9),
                    time_in_force: Some(TimeInForce::GTC),
                    client_order_id: Some("COINEXBELOW1".to_string()),
                },
            }
        }

        let client = CoinExSpotClient::new(CoinExSpotConfig {
            dry_run: false,
            base_url: "http://127.0.0.1:9".to_string(),
            ..CoinExSpotConfig::default()
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
    fn coinex_spot_private_stream_parser_should_parse_order_fill_balance_and_ignore_control() {
        let order_events = parse_private_stream_message(
            r#"{
                "method":"order.update",
                "data":[{
                    "market":"BTCUSDT",
                    "order_id":"order-1",
                    "client_id":"client-1",
                    "side":"buy",
                    "type":"limit",
                    "option":"maker_only",
                    "status":"part_deal",
                    "price":"65000",
                    "amount":"0.02",
                    "filled_amount":"0.01",
                    "avg_price":"65010",
                    "created_at":"1700000000000",
                    "updated_at":"1700000000100"
                }]
            }"#,
        )
        .unwrap();
        match &order_events[0] {
            UserStreamEvent::Order(order) => {
                assert_eq!(order.exchange, "coinex");
                assert_eq!(order.market_type, MarketType::Spot);
                assert_eq!(order.symbol, "BTCUSDT");
                assert_eq!(order.order_id, "order-1");
                assert_eq!(order.client_order_id.as_deref(), Some("client-1"));
                assert_eq!(order.side, OrderSide::Buy);
                assert_eq!(order.order_type, OrderType::PostOnly);
                assert_eq!(order.status, OrderStatus::PartiallyFilled);
                assert_eq!(order.price, Some(65_000.0));
                assert_eq!(order.quantity, 0.02);
                assert_eq!(order.filled_quantity, 0.01);
                assert_eq!(order.average_price, Some(65_010.0));
            }
            other => panic!("expected CoinEx order event, got {other:?}"),
        }

        let fill_events = parse_private_stream_message(
            r#"{
                "channel":"deal.update",
                "data":{
                    "market":"BTCUSDT",
                    "deal_id":"trade-1",
                    "order_id":"order-1",
                    "client_id":"client-1",
                    "side":"sell",
                    "price":"66000",
                    "amount":"0.03",
                    "fee_ccy":"BTC",
                    "fee":"0.00003",
                    "created_at":"1700000000200"
                }
            }"#,
        )
        .unwrap();
        match &fill_events[0] {
            UserStreamEvent::Fill(fill) => {
                assert_eq!(fill.exchange, "coinex");
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
            other => panic!("expected CoinEx fill event, got {other:?}"),
        }

        let balance_events = parse_private_stream_message(
            r#"{
                "method":"balance.update",
                "data":[{
                    "ccy":"USDT",
                    "available":"80",
                    "frozen":"20",
                    "total":"100"
                }]
            }"#,
        )
        .unwrap();
        match &balance_events[0] {
            UserStreamEvent::Balance(snapshot) => {
                assert_eq!(snapshot.exchange, "coinex");
                assert_eq!(snapshot.market_type, MarketType::Spot);
                assert_eq!(snapshot.balances.len(), 1);
                let balance = &snapshot.balances[0];
                assert_eq!(balance.asset, "USDT");
                assert_eq!(balance.total, 100.0);
                assert_eq!(balance.available, 80.0);
                assert_eq!(balance.locked_by_exchange, 20.0);
            }
            other => panic!("expected CoinEx balance event, got {other:?}"),
        }

        assert!(
            parse_private_stream_message(r#"{"method":"order.subscribe","code":0}"#)
                .unwrap()
                .is_empty()
        );
        assert!(parse_private_stream_message(r#"{"code":0,"message":"OK"}"#)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn coinex_validation_should_reject_min_notional_tick_and_step() {
        let rule = parse_symbol_rule(&serde_json::json!({"market":"PIPEUSDT","base_ccy":"PIPE","quote_ccy":"USDT","price_precision":4,"amount_precision":1,"min_amount":"1","min_notional":"5"})).unwrap();
        let order = OrderRequest {
            market_type: MarketType::Spot,
            symbol: "PIPEUSDT".to_string(),
            side: OrderSide::Buy,
            position_side: PositionSide::None,
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: 1.05,
            price: Some(1.0),
            client_order_id: None,
            reduce_only: false,
        };
        assert!(validate_order_against_symbol_rule(&order, &rule).is_err());
    }

    #[test]
    fn coinex_error_classification_should_map_balance_rate_and_symbol_errors() {
        assert_eq!(
            classify_coinex_error(Some(25), "insufficient balance"),
            ExchangeErrorClass::InsufficientBalance
        );
        assert_eq!(
            classify_coinex_error(Some(213), "rate limited"),
            ExchangeErrorClass::RateLimited
        );
        assert_eq!(
            classify_coinex_error(Some(23), "invalid market"),
            ExchangeErrorClass::InvalidSymbol
        );
    }

    #[test]
    fn coinex_symbol_conversion_should_support_compact_and_mappings() {
        let mut mappings = HashMap::new();
        mappings.insert("CUDISUSDT".to_string(), "CUDIS_USDT".to_string());
        assert_eq!(
            normalize_coinex_symbol("BTC/USDT", &HashMap::new()).unwrap(),
            "BTCUSDT"
        );
        assert_eq!(
            normalize_coinex_symbol("CUDISUSDT", &mappings).unwrap(),
            "CUDISUSDT"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn coinex_live_health_check_requires_credentials() {
        let api_key = std::env::var("COINEX_API_KEY")
            .expect("COINEX_API_KEY is required for ignored live CoinEx tests");
        let api_secret = std::env::var("COINEX_API_SECRET")
            .expect("COINEX_API_SECRET is required for ignored live CoinEx tests");
        let client = CoinExSpotClient::new(CoinExSpotConfig {
            api_key,
            api_secret,
            dry_run: true,
            ..CoinExSpotConfig::default()
        });
        let health = client.health_check().await.unwrap();
        assert_eq!(health.exchange, "coinex");
    }

    #[test]
    #[ignore]
    fn coinex_live_order_tests_are_double_gated() {
        assert_eq!(
            std::env::var("ENABLE_LIVE_ORDER_TESTS").ok().as_deref(),
            Some("true"),
            "live order tests require ENABLE_LIVE_ORDER_TESTS=true and must be implemented with tiny dry-run-safe orders"
        );
    }
}
