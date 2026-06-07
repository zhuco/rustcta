use std::collections::HashMap;
use std::time::Duration as StdDuration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::Message;

use crate::core::error::ExchangeError;
use crate::core::ws_connect::connect_async;
use crate::exchanges::client_order_id::{generate_client_order_id, validate_client_order_id};
use crate::exchanges::unified::{
    validate_order_lookup_id, validate_orderbook_depth, AmendOrderRequest, AssetBalance,
    BalanceSnapshot, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeClient, ExchangeClientCapabilities, ExchangeClientError,
    ExchangeClientResult, FeeRate, LiquidityRole, MarketType, OrderBookLevel, OrderBookSnapshot,
    OrderRequest, OrderResponse, OrderSide, OrderStatus, OrderType, PositionSide,
    QuoteMarketOrderRequest, SymbolRule, SymbolStatus, TimeInForce, TradeFill, UserStreamEvent,
};
use crate::utils::SignatureHelper;

const OKX_SPOT_REST_BASE: &str = "https://www.okx.com";
const OKX_PUBLIC_WS_URL: &str = "wss://ws.okx.com:8443/ws/v5/public";
const OKX_PRIVATE_WS_URL: &str = "wss://ws.okx.com:8443/ws/v5/private";
const DEFAULT_ORDERBOOK_DEPTH: u16 = 5;
const DEFAULT_STALE_AFTER_MS: u64 = 10_000;
const DEFAULT_RECONNECT_DELAY_MS: u64 = 1_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OkxSpotConfig {
    pub api_key: String,
    pub api_secret: String,
    pub passphrase: String,
    #[serde(default = "default_rest_base_url")]
    pub rest_base_url: String,
    #[serde(default = "default_public_ws_url")]
    pub public_ws_url: String,
    #[serde(default = "default_private_ws_url")]
    pub private_ws_url: String,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default = "default_orderbook_depth")]
    pub orderbook_depth: u16,
    #[serde(default = "default_stale_after_ms")]
    pub stale_after_ms: u64,
    #[serde(default = "default_reconnect_delay_ms")]
    pub reconnect_delay_ms: u64,
    #[serde(default)]
    pub symbol_mappings: HashMap<String, String>,
}

impl OkxSpotConfig {
    pub fn from_env() -> Self {
        Self {
            api_key: std::env::var("OKX_SPOT_API_KEY").unwrap_or_default(),
            api_secret: std::env::var("OKX_SPOT_API_SECRET").unwrap_or_default(),
            passphrase: std::env::var("OKX_SPOT_PASSPHRASE").unwrap_or_default(),
            rest_base_url: default_rest_base_url(),
            public_ws_url: default_public_ws_url(),
            private_ws_url: default_private_ws_url(),
            dry_run: true,
            orderbook_depth: DEFAULT_ORDERBOOK_DEPTH,
            stale_after_ms: DEFAULT_STALE_AFTER_MS,
            reconnect_delay_ms: DEFAULT_RECONNECT_DELAY_MS,
            symbol_mappings: HashMap::new(),
        }
    }
}

impl Default for OkxSpotConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

#[derive(Clone)]
pub struct OkxSpotClient {
    config: OkxSpotConfig,
    http: reqwest::Client,
}

impl OkxSpotClient {
    pub fn new(config: OkxSpotConfig) -> Self {
        Self {
            config,
            http: crate::core::http2_fix::shared_http_client(),
        }
    }

    pub fn dry_run(&self) -> bool {
        self.config.dry_run
    }

    pub fn okx_symbol(&self, symbol: &str) -> ExchangeClientResult<String> {
        normalize_okx_symbol(symbol, &self.config.symbol_mappings)
    }

    async fn send_public_request(
        &self,
        endpoint: &str,
        params: HashMap<String, String>,
    ) -> ExchangeClientResult<Value> {
        let request_path = build_request_path(endpoint, &params);
        let url = format!("{}{}", self.config.rest_base_url, request_path);
        let response = self
            .http
            .get(url)
            .send()
            .await
            .map_err(ExchangeError::from)?;
        parse_okx_response(response).await
    }

    async fn send_signed_request(
        &self,
        method: Method,
        endpoint: &str,
        params: HashMap<String, String>,
        body: Option<Value>,
    ) -> ExchangeClientResult<Value> {
        self.ensure_credentials()?;
        let body_text = body
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(ExchangeError::from)?
            .unwrap_or_default();
        let request_path = if method == Method::GET {
            build_request_path(endpoint, &params)
        } else {
            endpoint.to_string()
        };
        let timestamp = okx_timestamp();
        let signature = SignatureHelper::okx_signature(
            &self.config.api_secret,
            &timestamp,
            method.as_str(),
            &request_path,
            &body_text,
        );
        let url = format!("{}{}", self.config.rest_base_url, request_path);
        let mut request = self
            .http
            .request(method, url)
            .header("OK-ACCESS-KEY", &self.config.api_key)
            .header("OK-ACCESS-SIGN", signature)
            .header("OK-ACCESS-TIMESTAMP", timestamp)
            .header("OK-ACCESS-PASSPHRASE", &self.config.passphrase)
            .header("Content-Type", "application/json");
        if !body_text.is_empty() {
            request = request.body(body_text);
        }
        let response = request.send().await.map_err(ExchangeError::from)?;
        parse_okx_response(response).await
    }

    fn ensure_credentials(&self) -> ExchangeClientResult<()> {
        if self.config.api_key.trim().is_empty()
            || self.config.api_secret.trim().is_empty()
            || self.config.passphrase.trim().is_empty()
        {
            return Err(ExchangeError::AuthError(
                "OKX_SPOT_API_KEY, OKX_SPOT_API_SECRET, and OKX_SPOT_PASSPHRASE are required for OKX Spot private calls"
                    .to_string(),
            )
            .into());
        }
        Ok(())
    }

    fn dry_run_order_response(&self, request: &OrderRequest, okx_symbol: &str) -> OrderResponse {
        OrderResponse {
            exchange: "okx".to_string(),
            market_type: MarketType::Spot,
            symbol: internal_symbol_from_okx(okx_symbol),
            order_id: format!("dry-okx-spot-{}", Utc::now().timestamp_millis()),
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

    fn dry_run_amend_order_response(
        &self,
        request: &AmendOrderRequest,
        okx_symbol: &str,
    ) -> OrderResponse {
        OrderResponse {
            exchange: "okx".to_string(),
            market_type: MarketType::Spot,
            symbol: internal_symbol_from_okx(okx_symbol),
            order_id: request
                .order_id()
                .map(str::to_string)
                .unwrap_or_else(|| format!("dry-okx-spot-{}", Utc::now().timestamp_millis())),
            client_order_id: request
                .new_client_order_id()
                .or_else(|| request.client_order_id())
                .map(str::to_string),
            side: OrderSide::Buy,
            position_side: PositionSide::None,
            order_type: OrderType::Limit,
            status: OrderStatus::New,
            price: None,
            quantity: request.new_quantity,
            filled_quantity: 0.0,
            average_price: None,
            created_at: Utc::now(),
            updated_at: None,
        }
    }

    fn dry_run_quote_market_order_response(
        &self,
        request: &QuoteMarketOrderRequest,
        okx_symbol: &str,
    ) -> OrderResponse {
        OrderResponse {
            exchange: "okx".to_string(),
            market_type: MarketType::Spot,
            symbol: internal_symbol_from_okx(okx_symbol),
            order_id: format!("dry-okx-spot-{}", Utc::now().timestamp_millis()),
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

    fn ws_login_message(&self) -> ExchangeClientResult<String> {
        self.ensure_credentials()?;
        let timestamp = Utc::now().timestamp().to_string();
        let sign = SignatureHelper::okx_signature(
            &self.config.api_secret,
            &timestamp,
            "GET",
            "/users/self/verify",
            "",
        );
        Ok(serde_json::json!({
            "op": "login",
            "args": [{
                "apiKey": self.config.api_key,
                "passphrase": self.config.passphrase,
                "timestamp": timestamp,
                "sign": sign,
            }]
        })
        .to_string())
    }
}

#[async_trait]
impl ExchangeClient for OkxSpotClient {
    fn market_type(&self) -> MarketType {
        MarketType::Spot
    }

    fn exchange_name(&self) -> &str {
        "okx"
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
        let okx_symbol = self.okx_symbol(symbol)?;
        Ok(internal_symbol_from_okx(&okx_symbol))
    }

    async fn load_symbol_rules(&self) -> ExchangeClientResult<Vec<SymbolRule>> {
        let mut params = HashMap::new();
        params.insert("instType".to_string(), "SPOT".to_string());
        let response = self
            .send_public_request("/api/v5/public/instruments", params)
            .await?;
        parse_symbol_rules(&response)
    }

    async fn get_balances(&self) -> ExchangeClientResult<BalanceSnapshot> {
        let response = self
            .send_signed_request(Method::GET, "/api/v5/account/balance", HashMap::new(), None)
            .await?;
        parse_balance_snapshot(&response, "okx")
    }

    async fn get_orderbook(
        &self,
        symbol: &str,
        depth: u16,
    ) -> ExchangeClientResult<OrderBookSnapshot> {
        let okx_symbol = self.okx_symbol(symbol)?;
        validate_orderbook_depth(depth)?;
        let mut params = HashMap::new();
        params.insert("instId".to_string(), okx_symbol.clone());
        params.insert("sz".to_string(), normalize_depth(depth).to_string());
        let response = self
            .send_public_request("/api/v5/market/books", params)
            .await?;
        parse_orderbook_snapshot(&response, "okx", Some(&okx_symbol))
    }

    async fn place_order(&self, mut request: OrderRequest) -> ExchangeClientResult<OrderResponse> {
        request.validate()?;
        if request.market_type != MarketType::Spot {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: "OkxSpotClient only supports MarketType::Spot".to_string(),
            });
        }
        let okx_symbol = self.okx_symbol(&request.symbol)?;
        request.symbol = internal_symbol_from_okx(&okx_symbol);
        ensure_client_order_id(&mut request, "okx")?;
        let body = okx_order_body(&request, &okx_symbol)?;
        if self.config.dry_run {
            let response = self.dry_run_order_response(&request, &okx_symbol);
            log::info!(
                "OKX Spot dry-run order state transition inst_id={} side={:?} type={:?} status={:?} client_order_id={:?}",
                okx_symbol,
                request.side,
                request.order_type,
                response.status,
                response.client_order_id
            );
            return Ok(response);
        }

        let response = self
            .send_signed_request(
                Method::POST,
                "/api/v5/trade/order",
                HashMap::new(),
                Some(body),
            )
            .await?;
        let ack = response
            .as_array()
            .and_then(|items| items.first())
            .ok_or_else(|| parse_error(format!("OKX order response missing data: {response}")))?;
        if ack.get("sCode").and_then(Value::as_str) != Some("0") {
            return Err(ExchangeError::OrderError(
                ack.get("sMsg")
                    .and_then(Value::as_str)
                    .unwrap_or("OKX order rejected")
                    .to_string(),
            )
            .into());
        }
        let order_id = required_str(ack, "ordId")?;
        log::info!(
            "OKX Spot order state transition inst_id={} order_id={} status={:?}",
            okx_symbol,
            order_id,
            OrderStatus::New
        );
        Ok(OrderResponse {
            exchange: "okx".to_string(),
            market_type: MarketType::Spot,
            symbol: internal_symbol_from_okx(&okx_symbol),
            order_id: order_id.to_string(),
            client_order_id: value_as_string(ack.get("clOrdId"))
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

    async fn place_quote_market_order(
        &self,
        mut request: QuoteMarketOrderRequest,
    ) -> ExchangeClientResult<OrderResponse> {
        request.validate()?;
        let okx_symbol = self.okx_symbol(&request.symbol)?;
        request.symbol = internal_symbol_from_okx(&okx_symbol);
        ensure_quote_market_client_order_id(&mut request, "okx")?;
        if self.config.dry_run {
            return Ok(self.dry_run_quote_market_order_response(&request, &okx_symbol));
        }

        let body = okx_quote_market_order_body(&request, &okx_symbol)?;
        let response = self
            .send_signed_request(
                Method::POST,
                "/api/v5/trade/order",
                HashMap::new(),
                Some(body),
            )
            .await?;
        let ack = response
            .as_array()
            .and_then(|items| items.first())
            .ok_or_else(|| parse_error(format!("OKX order response missing data: {response}")))?;
        if ack.get("sCode").and_then(Value::as_str) != Some("0") {
            return Err(ExchangeError::OrderError(
                ack.get("sMsg")
                    .and_then(Value::as_str)
                    .unwrap_or("OKX order rejected")
                    .to_string(),
            )
            .into());
        }
        Ok(OrderResponse {
            exchange: "okx".to_string(),
            market_type: MarketType::Spot,
            symbol: internal_symbol_from_okx(&okx_symbol),
            order_id: required_str(ack, "ordId")?.to_string(),
            client_order_id: value_as_string(ack.get("clOrdId"))
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

    async fn amend_order(
        &self,
        mut request: AmendOrderRequest,
    ) -> ExchangeClientResult<OrderResponse> {
        request.validate()?;
        if request.market_type != MarketType::Spot {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: "OkxSpotClient only supports MarketType::Spot".to_string(),
            });
        }
        let okx_symbol = self.okx_symbol(&request.symbol)?;
        request.symbol = internal_symbol_from_okx(&okx_symbol);
        ensure_amend_client_order_id(&request, "okx")?;
        if self.config.dry_run {
            return Ok(self.dry_run_amend_order_response(&request, &okx_symbol));
        }

        let body = okx_amend_order_body(&request, &okx_symbol)?;
        let response = self
            .send_signed_request(
                Method::POST,
                "/api/v5/trade/amend-order",
                HashMap::new(),
                Some(body),
            )
            .await?;
        let ack = response
            .as_array()
            .and_then(|items| items.first())
            .ok_or_else(|| parse_error(format!("OKX amend response missing data: {response}")))?;
        if ack.get("sCode").and_then(Value::as_str) != Some("0") {
            return Err(ExchangeError::OrderError(
                ack.get("sMsg")
                    .and_then(Value::as_str)
                    .unwrap_or("OKX amend rejected")
                    .to_string(),
            )
            .into());
        }
        Ok(OrderResponse {
            exchange: "okx".to_string(),
            market_type: MarketType::Spot,
            symbol: internal_symbol_from_okx(&okx_symbol),
            order_id: value_as_string(ack.get("ordId"))
                .or_else(|| request.order_id().map(str::to_string))
                .unwrap_or_default(),
            client_order_id: value_as_string(ack.get("clOrdId"))
                .or_else(|| request.new_client_order_id().map(str::to_string))
                .or_else(|| request.client_order_id().map(str::to_string)),
            side: OrderSide::Buy,
            position_side: PositionSide::None,
            order_type: OrderType::Limit,
            status: OrderStatus::New,
            price: None,
            quantity: request.new_quantity,
            filled_quantity: 0.0,
            average_price: None,
            created_at: Utc::now(),
            updated_at: None,
        })
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeClientResult<CancelOrderResponse> {
        request.validate()?;
        if request.market_type != MarketType::Spot {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: "OkxSpotClient only supports MarketType::Spot".to_string(),
            });
        }
        ensure_cancel_client_order_id(&request, "okx")?;
        let okx_symbol = self.okx_symbol(&request.symbol)?;
        if self.config.dry_run {
            log::info!(
                "OKX Spot dry-run cancel state transition inst_id={} order_id={:?} client_order_id={:?} status={:?}",
                okx_symbol,
                request.order_id,
                request.client_order_id,
                OrderStatus::Cancelled
            );
            return Ok(CancelOrderResponse {
                exchange: "okx".to_string(),
                market_type: MarketType::Spot,
                symbol: internal_symbol_from_okx(&okx_symbol),
                order_id: request.order_id().map(str::to_string),
                client_order_id: request.client_order_id().map(str::to_string),
                status: OrderStatus::Cancelled,
                cancelled_at: Utc::now(),
            });
        }

        let mut body = serde_json::Map::new();
        body.insert("instId".to_string(), Value::String(okx_symbol.clone()));
        if let Some(order_id) = request.order_id() {
            body.insert("ordId".to_string(), Value::String(order_id.to_string()));
        }
        if let Some(client_order_id) = request.client_order_id() {
            body.insert(
                "clOrdId".to_string(),
                Value::String(client_order_id.to_string()),
            );
        }
        let response = self
            .send_signed_request(
                Method::POST,
                "/api/v5/trade/cancel-order",
                HashMap::new(),
                Some(Value::Object(body)),
            )
            .await?;
        let ack = response
            .as_array()
            .and_then(|items| items.first())
            .ok_or_else(|| parse_error(format!("OKX cancel response missing data: {response}")))?;
        if ack.get("sCode").and_then(Value::as_str) != Some("0") {
            return Err(ExchangeError::OrderError(
                ack.get("sMsg")
                    .and_then(Value::as_str)
                    .unwrap_or("OKX cancel rejected")
                    .to_string(),
            )
            .into());
        }
        log::info!(
            "OKX Spot cancel state transition inst_id={} order_id={:?} client_order_id={:?} status={:?}",
            okx_symbol,
            value_as_string(ack.get("ordId")).or_else(|| request.order_id().map(str::to_string)),
            value_as_string(ack.get("clOrdId"))
                .or_else(|| request.client_order_id().map(str::to_string)),
            OrderStatus::Cancelled
        );
        Ok(CancelOrderResponse {
            exchange: "okx".to_string(),
            market_type: MarketType::Spot,
            symbol: internal_symbol_from_okx(&okx_symbol),
            order_id: value_as_string(ack.get("ordId")),
            client_order_id: value_as_string(ack.get("clOrdId")),
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
                reason: "OkxSpotClient only supports MarketType::Spot".to_string(),
            });
        }
        let okx_symbol = request
            .symbol
            .as_deref()
            .map(|symbol| self.okx_symbol(symbol))
            .transpose()?;
        if self.config.dry_run {
            return Ok(CancelAllOrdersResponse {
                exchange: "okx".to_string(),
                market_type: MarketType::Spot,
                symbol: okx_symbol.as_deref().map(internal_symbol_from_okx),
                cancelled_orders: 0,
                cancelled_at: Utc::now(),
            });
        }

        let mut params = HashMap::new();
        params.insert("instType".to_string(), "SPOT".to_string());
        if let Some(okx_symbol) = &okx_symbol {
            params.insert("instId".to_string(), okx_symbol.clone());
        }
        let pending = self
            .send_signed_request(Method::GET, "/api/v5/trade/orders-pending", params, None)
            .await?;
        let batch_bodies = okx_cancel_all_batch_bodies(&pending)?;
        let mut cancelled_orders = 0;
        for body in batch_bodies {
            let ack = self
                .send_signed_request(
                    Method::POST,
                    "/api/v5/trade/cancel-batch-orders",
                    HashMap::new(),
                    Some(body),
                )
                .await?;
            cancelled_orders += okx_cancel_batch_success_count(&ack)?;
        }
        Ok(CancelAllOrdersResponse {
            exchange: "okx".to_string(),
            market_type: MarketType::Spot,
            symbol: okx_symbol.as_deref().map(internal_symbol_from_okx),
            cancelled_orders,
            cancelled_at: Utc::now(),
        })
    }

    async fn get_order(&self, symbol: &str, order_id: &str) -> ExchangeClientResult<OrderResponse> {
        let okx_symbol = self.okx_symbol(symbol)?;
        let order_id = validate_order_lookup_id(order_id)?;
        let mut params = HashMap::new();
        params.insert("instId".to_string(), okx_symbol);
        params.insert("ordId".to_string(), order_id.to_string());
        let response = self
            .send_signed_request(Method::GET, "/api/v5/trade/order", params, None)
            .await?;
        parse_single_order_response(&response, "okx")
    }

    async fn get_open_orders(
        &self,
        symbol: Option<&str>,
    ) -> ExchangeClientResult<Vec<OrderResponse>> {
        let mut params = HashMap::new();
        params.insert("instType".to_string(), "SPOT".to_string());
        if let Some(symbol) = symbol {
            params.insert("instId".to_string(), self.okx_symbol(symbol)?);
        }
        let response = self
            .send_signed_request(Method::GET, "/api/v5/trade/orders-pending", params, None)
            .await?;
        let orders = response.as_array().ok_or_else(|| {
            parse_error(format!(
                "OKX open orders response is not an array: {response}"
            ))
        })?;
        orders
            .iter()
            .map(|order| parse_order_response(order, "okx"))
            .collect()
    }

    async fn get_fee_rate(&self, symbol: &str) -> ExchangeClientResult<FeeRate> {
        let okx_symbol = self.okx_symbol(symbol)?;
        let mut params = HashMap::new();
        params.insert("instType".to_string(), "SPOT".to_string());
        params.insert("instId".to_string(), okx_symbol);
        let response = self
            .send_signed_request(Method::GET, "/api/v5/account/trade-fee", params, None)
            .await?;
        parse_fee_rate(&response)
    }

    async fn get_recent_fills(&self, symbol: &str) -> ExchangeClientResult<Vec<TradeFill>> {
        let okx_symbol = self.okx_symbol(symbol)?;
        let mut params = HashMap::new();
        params.insert("instType".to_string(), "SPOT".to_string());
        params.insert("instId".to_string(), okx_symbol);
        params.insert("limit".to_string(), "100".to_string());
        let response = self
            .send_signed_request(Method::GET, "/api/v5/trade/fills-history", params, None)
            .await?;
        parse_fills(&response)
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
        let okx_symbols = symbols
            .iter()
            .map(|symbol| self.okx_symbol(symbol))
            .collect::<ExchangeClientResult<Vec<_>>>()?;
        let client = self.clone();
        let (tx, rx) = mpsc::channel(1024);
        tokio::spawn(async move {
            client.run_orderbook_stream(okx_symbols, tx).await;
        });
        Ok(rx)
    }

    async fn subscribe_user_stream(&self) -> ExchangeClientResult<mpsc::Receiver<UserStreamEvent>> {
        self.ensure_credentials()?;
        let client = self.clone();
        let (tx, rx) = mpsc::channel(1024);
        tokio::spawn(async move {
            client.run_user_stream(tx).await;
        });
        Ok(rx)
    }
}

impl OkxSpotClient {
    async fn run_orderbook_stream(self, symbols: Vec<String>, tx: mpsc::Sender<OrderBookSnapshot>) {
        let stale_after = StdDuration::from_millis(self.config.stale_after_ms);
        let reconnect_delay = StdDuration::from_millis(self.config.reconnect_delay_ms);
        let channel = if self.config.orderbook_depth <= 5 {
            "books5"
        } else {
            "books"
        };
        let args = symbols
            .iter()
            .map(|symbol| serde_json::json!({"channel": channel, "instId": symbol}))
            .collect::<Vec<_>>();
        let subscribe = serde_json::json!({"op": "subscribe", "args": args}).to_string();

        loop {
            log::info!(
                "Connecting OKX Spot orderbook stream symbols={} channel={}",
                symbols.join(","),
                channel
            );
            match connect_async(self.config.public_ws_url.as_str()).await {
                Ok((mut ws, _)) => {
                    if let Err(error) = ws.send(Message::Text(subscribe.clone())).await {
                        log::warn!("OKX Spot orderbook subscribe send failed: {}", error);
                        sleep(reconnect_delay).await;
                        continue;
                    }
                    loop {
                        match timeout(stale_after, ws.next()).await {
                            Err(_) => {
                                log::warn!(
                                    "OKX Spot orderbook stream stale for {:?}; reconnecting",
                                    stale_after
                                );
                                break;
                            }
                            Ok(Some(Ok(Message::Text(text)))) => {
                                if is_okx_event_message(&text) {
                                    continue;
                                }
                                match parse_stream_orderbook_message(&text) {
                                    Ok(snapshot) => {
                                        if tx.send(snapshot).await.is_err() {
                                            return;
                                        }
                                    }
                                    Err(error) => {
                                        log::warn!("OKX Spot orderbook parse error: {}", error);
                                    }
                                }
                            }
                            Ok(Some(Ok(Message::Ping(payload)))) => {
                                let _ = ws.send(Message::Pong(payload)).await;
                            }
                            Ok(Some(Ok(Message::Close(frame)))) => {
                                log::warn!("OKX Spot orderbook stream closed: {:?}", frame);
                                break;
                            }
                            Ok(Some(Ok(_))) => {}
                            Ok(Some(Err(error))) => {
                                log::warn!("OKX Spot orderbook websocket error: {}", error);
                                break;
                            }
                            Ok(None) => {
                                log::warn!("OKX Spot orderbook websocket ended");
                                break;
                            }
                        }
                    }
                }
                Err(error) => {
                    log::warn!("OKX Spot orderbook connect error: {}", error);
                }
            }
            sleep(reconnect_delay).await;
        }
    }

    async fn run_user_stream(self, tx: mpsc::Sender<UserStreamEvent>) {
        let reconnect_delay = StdDuration::from_millis(self.config.reconnect_delay_ms);
        let subscribe = serde_json::json!({
            "op": "subscribe",
            "args": [
                {"channel": "orders", "instType": "SPOT"},
                {"channel": "account"}
            ]
        })
        .to_string();

        loop {
            let login = match self.ws_login_message() {
                Ok(login) => login,
                Err(error) => {
                    log::warn!("OKX Spot private WS login build failed: {}", error);
                    sleep(reconnect_delay).await;
                    continue;
                }
            };
            log::info!("Connecting OKX Spot private user stream");
            match connect_async(self.config.private_ws_url.as_str()).await {
                Ok((mut ws, _)) => {
                    if let Err(error) = ws.send(Message::Text(login)).await {
                        log::warn!("OKX Spot private login send failed: {}", error);
                        sleep(reconnect_delay).await;
                        continue;
                    }
                    let mut logged_in = false;
                    loop {
                        match timeout(StdDuration::from_secs(30), ws.next()).await {
                            Err(_) => {
                                log::warn!("OKX Spot private stream stale; reconnecting");
                                let _ = tx
                                    .send(UserStreamEvent::Disconnected {
                                        reason: Some("stale private stream".to_string()),
                                    })
                                    .await;
                                break;
                            }
                            Ok(Some(Ok(Message::Text(text)))) => {
                                if !logged_in {
                                    if okx_login_succeeded(&text) {
                                        logged_in = true;
                                        if let Err(error) =
                                            ws.send(Message::Text(subscribe.clone())).await
                                        {
                                            log::warn!(
                                                "OKX Spot private subscribe send failed: {}",
                                                error
                                            );
                                            break;
                                        }
                                        continue;
                                    }
                                    if okx_login_failed(&text) {
                                        log::warn!("OKX Spot private login failed: {}", text);
                                        break;
                                    }
                                }
                                if is_okx_event_message(&text) {
                                    continue;
                                }
                                match parse_user_stream_message(&text) {
                                    Ok(events) => {
                                        for event in events {
                                            if tx.send(event).await.is_err() {
                                                return;
                                            }
                                        }
                                    }
                                    Err(error) => {
                                        log::warn!("OKX Spot user stream parse error: {}", error)
                                    }
                                }
                            }
                            Ok(Some(Ok(Message::Ping(payload)))) => {
                                let _ = ws.send(Message::Pong(payload)).await;
                            }
                            Ok(Some(Ok(Message::Close(frame)))) => {
                                log::warn!("OKX Spot private stream closed: {:?}", frame);
                                let _ = tx
                                    .send(UserStreamEvent::Disconnected {
                                        reason: Some("websocket close".to_string()),
                                    })
                                    .await;
                                break;
                            }
                            Ok(Some(Ok(_))) => {}
                            Ok(Some(Err(error))) => {
                                log::warn!("OKX Spot private websocket error: {}", error);
                                let _ = tx
                                    .send(UserStreamEvent::Disconnected {
                                        reason: Some(error.to_string()),
                                    })
                                    .await;
                                break;
                            }
                            Ok(None) => {
                                log::warn!("OKX Spot private websocket ended");
                                let _ = tx
                                    .send(UserStreamEvent::Disconnected {
                                        reason: Some("stream ended".to_string()),
                                    })
                                    .await;
                                break;
                            }
                        }
                    }
                }
                Err(error) => {
                    log::warn!("OKX Spot private connect error: {}", error);
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

pub fn normalize_okx_symbol(
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
    let normalized_key = trimmed.replace(['-', '/', '_'], "").to_ascii_uppercase();
    if let Some(mapped) = mappings
        .get(trimmed)
        .or_else(|| mappings.get(&trimmed.to_ascii_uppercase()))
        .or_else(|| mappings.get(&normalized_key))
    {
        return Ok(mapped.to_ascii_uppercase());
    }
    let upper = trimmed.replace(['/', '_'], "-").to_ascii_uppercase();
    if upper.contains('-') {
        return Ok(upper);
    }
    for quote in ["USDT", "USDC", "BTC", "ETH", "EUR", "USD"] {
        if let Some(base) = normalized_key.strip_suffix(quote) {
            if !base.is_empty() {
                return Ok(format!("{base}-{quote}"));
            }
        }
    }
    Err(ExchangeClientError::Validation {
        field: "symbol",
        reason: format!("could not infer OKX Spot symbol for {symbol}"),
    })
}

pub fn map_okx_order_status(state: &str) -> OrderStatus {
    match state.trim().to_ascii_lowercase().as_str() {
        "live" => OrderStatus::New,
        "partially_filled" => OrderStatus::PartiallyFilled,
        "filled" => OrderStatus::Filled,
        "canceled" | "cancelled" | "mmp_canceled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}

pub fn parse_balance_snapshot(
    value: &Value,
    exchange: impl Into<String>,
) -> ExchangeClientResult<BalanceSnapshot> {
    let data = value
        .as_array()
        .ok_or_else(|| parse_error(format!("OKX balance data is not an array: {value}")))?;
    let mut balances = Vec::new();
    for account in data {
        let details = account
            .get("details")
            .and_then(Value::as_array)
            .ok_or_else(|| parse_error(format!("OKX balance item missing details: {account}")))?;
        for detail in details {
            let asset = required_str(detail, "ccy")?.to_string();
            let available = number_from_str(
                detail
                    .get("availBal")
                    .or_else(|| detail.get("availEq"))
                    .or_else(|| detail.get("cashBal")),
            )
            .unwrap_or(0.0);
            let locked = number_from_str(detail.get("frozenBal")).unwrap_or(0.0);
            let total = number_from_str(detail.get("cashBal")).unwrap_or(available + locked);
            if total > 0.0 || available > 0.0 || locked > 0.0 {
                balances.push(AssetBalance::new(asset, total, available, locked));
            }
        }
    }
    Ok(BalanceSnapshot {
        exchange: exchange.into(),
        market_type: MarketType::Spot,
        balances,
        timestamp: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    value: &Value,
    exchange: impl Into<String>,
    fallback_symbol: Option<&str>,
) -> ExchangeClientResult<OrderBookSnapshot> {
    let payload = value
        .as_array()
        .and_then(|items| items.first())
        .or_else(|| {
            value
                .get("data")
                .and_then(Value::as_array)
                .and_then(|items| items.first())
        })
        .unwrap_or(value);
    let okx_symbol = value
        .get("arg")
        .and_then(|arg| arg.get("instId"))
        .and_then(Value::as_str)
        .or_else(|| payload.get("instId").and_then(Value::as_str))
        .or(fallback_symbol)
        .ok_or_else(|| parse_error(format!("OKX orderbook missing instId: {value}")))?;
    let bids = payload
        .get("bids")
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(format!("OKX orderbook missing bids: {payload}")))?;
    let asks = payload
        .get("asks")
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(format!("OKX orderbook missing asks: {payload}")))?;
    let bids = parse_levels(bids)?;
    let asks = parse_levels(asks)?;
    let exchange_timestamp = payload
        .get("ts")
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    let received_at = Utc::now();
    Ok(OrderBookSnapshot {
        exchange: exchange.into(),
        market_type: MarketType::Spot,
        symbol: internal_symbol_from_okx(okx_symbol),
        best_bid: bids.first().map(|level| level.price),
        best_ask: asks.first().map(|level| level.price),
        bids,
        asks,
        exchange_timestamp,
        latency_ms: exchange_timestamp
            .map(|ts| received_at.signed_duration_since(ts).num_milliseconds()),
        received_at,
        sequence: None,
        is_stale: false,
    })
}

pub fn parse_symbol_rules(value: &Value) -> ExchangeClientResult<Vec<SymbolRule>> {
    let instruments = value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| parse_error(format!("OKX instruments response is not an array: {value}")))?;
    instruments.iter().map(parse_symbol_rule).collect()
}

pub fn parse_symbol_rule(value: &Value) -> ExchangeClientResult<SymbolRule> {
    let okx_symbol = required_str(value, "instId")?.to_ascii_uppercase();
    let base_asset = required_str(value, "baseCcy")?.to_ascii_uppercase();
    let quote_asset = required_str(value, "quoteCcy")?.to_ascii_uppercase();
    let tick_size = number_from_str(value.get("tickSz")).unwrap_or(0.0);
    let step_size = number_from_str(value.get("lotSz")).unwrap_or(0.0);
    Ok(SymbolRule {
        exchange: "okx".to_string(),
        market_type: MarketType::Spot,
        internal_symbol: internal_symbol_from_okx(&okx_symbol),
        exchange_symbol: okx_symbol,
        base_asset,
        quote_asset,
        price_precision: precision_from_step(tick_size),
        quantity_precision: precision_from_step(step_size),
        tick_size,
        step_size,
        min_quantity: number_from_str(value.get("minSz")).unwrap_or(0.0),
        min_notional: number_from_str(value.get("minLmtAmt"))
            .or_else(|| number_from_str(value.get("minMktAmt")))
            .unwrap_or(0.0),
        max_quantity: number_from_str(value.get("maxLmtSz"))
            .or_else(|| number_from_str(value.get("maxMktSz"))),
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
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_ascii_lowercase()
            .as_str()
        {
            "live" => SymbolStatus::Trading,
            "suspend" | "test" | "settling" | "preopen" => SymbolStatus::Suspended,
            _ => SymbolStatus::Unknown,
        },
        raw_metadata: Some(value.clone()),
    })
}

pub fn parse_order_response(
    value: &Value,
    exchange: impl Into<String>,
) -> ExchangeClientResult<OrderResponse> {
    let okx_symbol = required_str(value, "instId")?;
    let quantity = number_from_str(value.get("sz")).unwrap_or(0.0);
    let filled_quantity = number_from_str(
        value
            .get("accFillSz")
            .or_else(|| value.get("fillSz"))
            .or_else(|| value.get("filledSz")),
    )
    .unwrap_or(0.0);
    Ok(OrderResponse {
        exchange: exchange.into(),
        market_type: MarketType::Spot,
        symbol: internal_symbol_from_okx(okx_symbol),
        order_id: required_str(value, "ordId")?.to_string(),
        client_order_id: value_as_string(value.get("clOrdId")).filter(|id| !id.is_empty()),
        side: parse_side(required_str(value, "side")?)?,
        position_side: PositionSide::None,
        order_type: parse_order_type(required_str(value, "ordType").unwrap_or("limit")),
        status: value
            .get("state")
            .and_then(Value::as_str)
            .map(map_okx_order_status)
            .unwrap_or(OrderStatus::Unknown),
        price: number_from_str(value.get("px")).filter(|price| *price > 0.0),
        quantity,
        filled_quantity,
        average_price: number_from_str(value.get("avgPx")).filter(|price| *price > 0.0),
        created_at: value
            .get("cTime")
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .unwrap_or_else(Utc::now),
        updated_at: value
            .get("uTime")
            .or_else(|| value.get("fillTime"))
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis),
    })
}

pub fn parse_private_order_update(
    value: &Value,
    exchange: impl Into<String> + Clone,
) -> ExchangeClientResult<(OrderResponse, Option<TradeFill>)> {
    let order = parse_order_response(value, exchange.clone())?;
    log::info!(
        "OKX Spot user stream order state transition symbol={} order_id={} status={:?} filled={:.8}/{:.8}",
        order.symbol,
        order.order_id,
        order.status,
        order.filled_quantity,
        order.quantity
    );
    let fill_quantity = number_from_str(value.get("fillSz")).unwrap_or(0.0);
    if fill_quantity <= 0.0 {
        return Ok((order, None));
    }
    let liquidity = match value
        .get("execType")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_uppercase()
        .as_str()
    {
        "M" => LiquidityRole::Maker,
        "T" => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    };
    let fee_amount =
        number_from_str(value.get("fillFee").or_else(|| value.get("fee"))).map(f64::abs);
    let fill = TradeFill {
        exchange: exchange.into(),
        market_type: MarketType::Spot,
        symbol: internal_symbol_from_okx(required_str(value, "instId")?),
        trade_id: value_as_string(value.get("tradeId")).filter(|id| !id.is_empty()),
        order_id: value_as_string(value.get("ordId")),
        client_order_id: value_as_string(value.get("clOrdId")).filter(|id| !id.is_empty()),
        side: parse_side(required_str(value, "side")?)?,
        price: number_from_str(value.get("fillPx")).unwrap_or(0.0),
        quantity: fill_quantity,
        fee_asset: value_as_string(value.get("fillFeeCcy").or_else(|| value.get("feeCcy")))
            .filter(|asset| !asset.is_empty()),
        fee_amount,
        liquidity,
        timestamp: value
            .get("fillTime")
            .or_else(|| value.get("uTime"))
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .unwrap_or_else(Utc::now),
    };
    Ok((order, Some(fill)))
}

pub fn parse_user_stream_message(text: &str) -> ExchangeClientResult<Vec<UserStreamEvent>> {
    let value: Value = serde_json::from_str(text).map_err(ExchangeError::from)?;
    let channel = value
        .get("arg")
        .and_then(|arg| arg.get("channel"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let data = value
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(format!("OKX user stream missing data array: {value}")))?;
    let mut events = Vec::new();
    match channel {
        "orders" => {
            for item in data {
                let (order, fill) = parse_private_order_update(item, "okx")?;
                events.push(UserStreamEvent::Order(order));
                if let Some(fill) = fill {
                    events.push(UserStreamEvent::Fill(fill));
                }
            }
        }
        "account" => {
            events.push(UserStreamEvent::Balance(parse_balance_snapshot(
                &Value::Array(data.clone()),
                "okx",
            )?));
        }
        _ => {
            return Err(parse_error(format!(
                "unsupported OKX Spot user stream channel {channel}: {value}"
            )));
        }
    }
    Ok(events)
}

pub fn parse_fills(value: &Value) -> ExchangeClientResult<Vec<TradeFill>> {
    let fills = value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| parse_error(format!("OKX fills response is not an array: {value}")))?;
    fills.iter().map(parse_fill).collect()
}

pub fn parse_fill(value: &Value) -> ExchangeClientResult<TradeFill> {
    let okx_symbol = required_str(value, "instId")?;
    let liquidity = match value
        .get("execType")
        .or_else(|| value.get("liquidity"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_uppercase()
        .as_str()
    {
        "M" | "MAKER" => LiquidityRole::Maker,
        "T" | "TAKER" => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    };
    Ok(TradeFill {
        exchange: "okx".to_string(),
        market_type: MarketType::Spot,
        symbol: internal_symbol_from_okx(okx_symbol),
        trade_id: value_as_string(value.get("tradeId").or_else(|| value.get("fillIdx")))
            .filter(|id| !id.is_empty()),
        order_id: value_as_string(value.get("ordId")).filter(|id| !id.is_empty()),
        client_order_id: value_as_string(value.get("clOrdId")).filter(|id| !id.is_empty()),
        side: parse_side(required_str(value, "side")?)?,
        price: number_from_str(value.get("fillPx").or_else(|| value.get("px"))).unwrap_or(0.0),
        quantity: number_from_str(value.get("fillSz").or_else(|| value.get("sz"))).unwrap_or(0.0),
        fee_asset: value_as_string(value.get("feeCcy").or_else(|| value.get("fillFeeCcy")))
            .filter(|asset| !asset.is_empty()),
        fee_amount: number_from_str(value.get("fee").or_else(|| value.get("fillFee")))
            .map(f64::abs),
        liquidity,
        timestamp: value
            .get("fillTime")
            .or_else(|| value.get("ts"))
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .unwrap_or_else(Utc::now),
    })
}

pub fn parse_stream_orderbook_message(text: &str) -> ExchangeClientResult<OrderBookSnapshot> {
    let value: Value = serde_json::from_str(text).map_err(ExchangeError::from)?;
    parse_orderbook_snapshot(&value, "okx", None)
}

fn parse_single_order_response(
    value: &Value,
    exchange: impl Into<String>,
) -> ExchangeClientResult<OrderResponse> {
    let order = value
        .as_array()
        .and_then(|items| items.first())
        .ok_or_else(|| parse_error(format!("OKX order response missing order item: {value}")))?;
    parse_order_response(order, exchange)
}

fn okx_order_body(request: &OrderRequest, okx_symbol: &str) -> ExchangeClientResult<Value> {
    let (base, _) = okx_symbol
        .split_once('-')
        .ok_or_else(|| parse_error(format!("OKX Spot symbol missing dash: {okx_symbol}")))?;
    let mut body = serde_json::Map::new();
    body.insert("instId".to_string(), Value::String(okx_symbol.to_string()));
    body.insert("tdMode".to_string(), Value::String("cash".to_string()));
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
        "sz".to_string(),
        Value::String(format_decimal(request.quantity)),
    );
    if let Some(client_order_id) = &request.client_order_id {
        body.insert(
            "clOrdId".to_string(),
            Value::String(client_order_id.clone()),
        );
    }
    match request.order_type {
        OrderType::Market => {
            body.insert("ordType".to_string(), Value::String("market".to_string()));
            body.insert(
                "tgtCcy".to_string(),
                Value::String(base.to_ascii_lowercase()),
            );
        }
        OrderType::Limit => {
            body.insert("ordType".to_string(), Value::String("limit".to_string()));
            body.insert(
                "px".to_string(),
                Value::String(format_decimal(required_price(request)?)),
            );
        }
        OrderType::PostOnly => {
            body.insert(
                "ordType".to_string(),
                Value::String("post_only".to_string()),
            );
            body.insert(
                "px".to_string(),
                Value::String(format_decimal(required_price(request)?)),
            );
        }
        OrderType::IOC => {
            body.insert("ordType".to_string(), Value::String("ioc".to_string()));
            body.insert(
                "px".to_string(),
                Value::String(format_decimal(required_price(request)?)),
            );
        }
        OrderType::FOK => {
            body.insert("ordType".to_string(), Value::String("fok".to_string()));
            body.insert(
                "px".to_string(),
                Value::String(format_decimal(required_price(request)?)),
            );
        }
    }
    if matches!(request.time_in_force, Some(TimeInForce::IOC)) {
        body.insert("ordType".to_string(), Value::String("ioc".to_string()));
    }
    if matches!(request.time_in_force, Some(TimeInForce::FOK)) {
        body.insert("ordType".to_string(), Value::String("fok".to_string()));
    }
    if matches!(request.time_in_force, Some(TimeInForce::GTX)) {
        body.insert(
            "ordType".to_string(),
            Value::String("post_only".to_string()),
        );
    }
    Ok(Value::Object(body))
}

fn okx_quote_market_order_body(
    request: &QuoteMarketOrderRequest,
    okx_symbol: &str,
) -> ExchangeClientResult<Value> {
    let mut body = serde_json::Map::new();
    body.insert("instId".to_string(), Value::String(okx_symbol.to_string()));
    body.insert("tdMode".to_string(), Value::String("cash".to_string()));
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
    body.insert("ordType".to_string(), Value::String("market".to_string()));
    body.insert(
        "sz".to_string(),
        Value::String(format_decimal(request.quote_quantity)),
    );
    body.insert("tgtCcy".to_string(), Value::String("quote_ccy".to_string()));
    if let Some(client_order_id) = &request.client_order_id {
        body.insert(
            "clOrdId".to_string(),
            Value::String(client_order_id.clone()),
        );
    }
    Ok(Value::Object(body))
}

fn okx_amend_order_body(
    request: &AmendOrderRequest,
    okx_symbol: &str,
) -> ExchangeClientResult<Value> {
    let mut body = serde_json::Map::new();
    body.insert("instId".to_string(), Value::String(okx_symbol.to_string()));
    body.insert(
        "newSz".to_string(),
        Value::String(format_decimal(request.new_quantity)),
    );
    if let Some(order_id) = request.order_id() {
        body.insert("ordId".to_string(), Value::String(order_id.to_string()));
    }
    if let Some(client_order_id) = request.client_order_id() {
        body.insert(
            "clOrdId".to_string(),
            Value::String(client_order_id.to_string()),
        );
    }
    if let Some(new_client_order_id) = request.new_client_order_id() {
        body.insert(
            "newClOrdId".to_string(),
            Value::String(new_client_order_id.to_string()),
        );
    }
    Ok(Value::Object(body))
}

fn okx_cancel_all_batch_bodies(value: &Value) -> ExchangeClientResult<Vec<Value>> {
    let orders = value.as_array().ok_or_else(|| {
        parse_error(format!(
            "OKX pending orders response is not an array: {value}"
        ))
    })?;
    let mut requests = Vec::new();
    for order in orders {
        let inst_id = required_str(order, "instId")?;
        let mut item = serde_json::Map::new();
        item.insert("instId".to_string(), Value::String(inst_id.to_string()));
        if let Some(order_id) = value_as_string(order.get("ordId")).filter(|id| !id.is_empty()) {
            item.insert("ordId".to_string(), Value::String(order_id));
        } else if let Some(client_order_id) =
            value_as_string(order.get("clOrdId")).filter(|id| !id.is_empty())
        {
            item.insert("clOrdId".to_string(), Value::String(client_order_id));
        } else {
            return Err(parse_error(format!(
                "OKX pending order missing ordId/clOrdId: {order}"
            )));
        }
        requests.push(Value::Object(item));
    }
    Ok(requests
        .chunks(20)
        .map(|chunk| Value::Array(chunk.to_vec()))
        .collect())
}

fn okx_cancel_batch_success_count(value: &Value) -> ExchangeClientResult<usize> {
    let items = value.as_array().ok_or_else(|| {
        parse_error(format!(
            "OKX cancel-batch response is not an array: {value}"
        ))
    })?;
    Ok(items
        .iter()
        .filter(|item| item.get("sCode").and_then(Value::as_str) == Some("0"))
        .count())
}

fn ensure_client_order_id(request: &mut OrderRequest, exchange: &str) -> ExchangeClientResult<()> {
    if request.client_order_id.is_none() {
        request.client_order_id =
            Some(generate_client_order_id(exchange, request.market_type, "spot").into_string());
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

fn ensure_quote_market_client_order_id(
    request: &mut QuoteMarketOrderRequest,
    exchange: &str,
) -> ExchangeClientResult<()> {
    if request.client_order_id.is_none() {
        request.client_order_id =
            Some(generate_client_order_id(exchange, request.market_type, "spot").into_string());
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

fn ensure_amend_client_order_id(
    request: &AmendOrderRequest,
    exchange: &str,
) -> ExchangeClientResult<()> {
    for client_order_id in [request.client_order_id(), request.new_client_order_id()]
        .into_iter()
        .flatten()
    {
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

async fn parse_okx_response(response: reqwest::Response) -> ExchangeClientResult<Value> {
    if !response.status().is_success() {
        let status = response.status().as_u16() as i32;
        let text = response
            .text()
            .await
            .unwrap_or_else(|_| "unknown OKX Spot error".to_string());
        return Err(ExchangeError::ApiError {
            code: status,
            message: text,
        }
        .into());
    }
    let envelope: Value = response.json().await.map_err(ExchangeError::from)?;
    let code = envelope
        .get("code")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if code != "0" {
        return Err(ExchangeError::ApiError {
            code: code.parse().unwrap_or(-1),
            message: envelope
                .get("msg")
                .and_then(Value::as_str)
                .unwrap_or("OKX API error")
                .to_string(),
        }
        .into());
    }
    Ok(envelope
        .get("data")
        .cloned()
        .unwrap_or(Value::Array(vec![])))
}

fn parse_fee_rate(value: &Value) -> ExchangeClientResult<FeeRate> {
    let item = value
        .as_array()
        .and_then(|items| items.first())
        .ok_or_else(|| parse_error(format!("OKX fee response missing data: {value}")))?;
    Ok(FeeRate::new(
        number_from_str(item.get("maker")).unwrap_or(0.0),
        number_from_str(item.get("taker")).unwrap_or(0.0),
        crate::exchanges::unified::FeeRateSource::ExchangeApi,
    ))
}

fn build_request_path(endpoint: &str, params: &HashMap<String, String>) -> String {
    if params.is_empty() {
        endpoint.to_string()
    } else {
        format!("{endpoint}?{}", SignatureHelper::build_query_string(params))
    }
}

fn okx_timestamp() -> String {
    Utc::now().format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string()
}

fn internal_symbol_from_okx(okx_symbol: &str) -> String {
    okx_symbol
        .split('-')
        .take(2)
        .collect::<String>()
        .to_ascii_uppercase()
}

fn parse_levels(levels: &[Value]) -> ExchangeClientResult<Vec<OrderBookLevel>> {
    let mut parsed = Vec::with_capacity(levels.len());
    for level in levels {
        let pair = level
            .as_array()
            .ok_or_else(|| parse_error(format!("OKX orderbook level is not an array: {level}")))?;
        if pair.len() < 2 {
            return Err(parse_error(format!(
                "OKX orderbook level has fewer than 2 entries: {level}"
            )));
        }
        let price = number_from_str(pair.first()).unwrap_or(0.0);
        let quantity = number_from_str(pair.get(1)).unwrap_or(0.0);
        if price > 0.0 && quantity >= 0.0 {
            parsed.push(OrderBookLevel { price, quantity });
        }
    }
    Ok(parsed)
}

fn parse_side(side: &str) -> ExchangeClientResult<OrderSide> {
    match side.to_ascii_lowercase().as_str() {
        "buy" => Ok(OrderSide::Buy),
        "sell" => Ok(OrderSide::Sell),
        _ => Err(parse_error(format!("unsupported OKX side: {side}"))),
    }
}

fn parse_order_type(order_type: &str) -> OrderType {
    match order_type.to_ascii_lowercase().as_str() {
        "market" => OrderType::Market,
        "post_only" => OrderType::PostOnly,
        "ioc" => OrderType::IOC,
        "fok" => OrderType::FOK,
        _ => OrderType::Limit,
    }
}

fn required_price(request: &OrderRequest) -> ExchangeClientResult<f64> {
    request
        .price
        .ok_or_else(|| ExchangeClientError::Validation {
            field: "price",
            reason: "price is required for OKX Spot limit-style orders".to_string(),
        })
}

fn required_str<'a>(value: &'a Value, field: &str) -> ExchangeClientResult<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| parse_error(format!("OKX payload missing string field {field}: {value}")))
}

fn number_from_str(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::String(raw) if !raw.is_empty() => raw.parse::<f64>().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn value_as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::String(raw) => raw.parse::<i64>().ok(),
        Value::Number(number) => number.as_i64(),
        _ => None,
    }
}

fn value_as_string(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(raw) => Some(raw.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn normalize_depth(depth: u16) -> u16 {
    if depth <= 5 {
        5
    } else {
        depth.min(400)
    }
}

fn format_decimal(value: f64) -> String {
    let formatted = format!("{value:.16}");
    formatted
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}

fn precision_from_step(step: f64) -> u32 {
    if step <= 0.0 || !step.is_finite() {
        return 0;
    }
    let text = format_decimal(step);
    text.split_once('.')
        .map(|(_, fraction)| fraction.trim_end_matches('0').len() as u32)
        .unwrap_or(0)
}

fn is_okx_event_message(text: &str) -> bool {
    serde_json::from_str::<Value>(text)
        .ok()
        .and_then(|value| value.get("event").cloned())
        .is_some()
}

fn okx_login_succeeded(text: &str) -> bool {
    serde_json::from_str::<Value>(text)
        .ok()
        .filter(|value| value.get("event").and_then(Value::as_str) == Some("login"))
        .and_then(|value| {
            value
                .get("code")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .as_deref()
        == Some("0")
}

fn okx_login_failed(text: &str) -> bool {
    serde_json::from_str::<Value>(text)
        .ok()
        .filter(|value| value.get("event").and_then(Value::as_str) == Some("login"))
        .and_then(|value| {
            value
                .get("code")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .is_some_and(|code| code != "0")
}

fn parse_error(message: String) -> ExchangeClientError {
    ExchangeError::ParseError(message).into()
}

fn default_rest_base_url() -> String {
    OKX_SPOT_REST_BASE.to_string()
}

fn default_public_ws_url() -> String {
    OKX_PUBLIC_WS_URL.to_string()
}

fn default_private_ws_url() -> String {
    OKX_PRIVATE_WS_URL.to_string()
}

fn default_orderbook_depth() -> u16 {
    DEFAULT_ORDERBOOK_DEPTH
}

fn default_stale_after_ms() -> u64 {
    DEFAULT_STALE_AFTER_MS
}

fn default_reconnect_delay_ms() -> u64 {
    DEFAULT_RECONNECT_DELAY_MS
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    #[derive(Debug, Clone)]
    struct SeenOkxRequest {
        method: String,
        path: String,
        query: HashMap<String, String>,
        headers: HashMap<String, String>,
        body: Option<Value>,
    }

    async fn spawn_okx_rest_server(
        responses: Vec<Value>,
    ) -> (String, Arc<Mutex<Vec<SeenOkxRequest>>>) {
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
                let request = parse_seen_okx_request(&request_text);
                seen_requests.lock().unwrap().push(request);
                let body = responses
                    .lock()
                    .unwrap()
                    .next()
                    .unwrap_or_else(|| json!({"code": "0", "msg": "", "data": []}));
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

    fn parse_seen_okx_request(request_text: &str) -> SeenOkxRequest {
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

        SeenOkxRequest {
            method,
            path: path.to_string(),
            query,
            headers,
            body,
        }
    }

    fn assert_signed_okx_request(request: &SeenOkxRequest, method: &str, path: &str) {
        assert_eq!(request.method, method);
        assert_eq!(request.path, path);
        assert_eq!(
            request.headers.get("ok-access-key").map(String::as_str),
            Some("key")
        );
        assert!(request
            .headers
            .get("ok-access-sign")
            .is_some_and(|value| !value.is_empty()));
        assert!(request
            .headers
            .get("ok-access-timestamp")
            .is_some_and(|value| !value.is_empty()));
        assert_eq!(
            request
                .headers
                .get("ok-access-passphrase")
                .map(String::as_str),
            Some("passphrase")
        );
    }

    #[test]
    fn okx_symbol_conversion_should_support_internal_dashed_and_mappings() {
        let mut mappings = HashMap::new();
        mappings.insert("BTCUSDT".to_string(), "BTC-USDT".to_string());
        mappings.insert("XBTUSDT".to_string(), "BTC-USDT".to_string());

        assert_eq!(
            normalize_okx_symbol("BTCUSDT", &mappings).unwrap(),
            "BTC-USDT"
        );
        assert_eq!(
            normalize_okx_symbol("BTC-USDT", &HashMap::new()).unwrap(),
            "BTC-USDT"
        );
        assert_eq!(
            normalize_okx_symbol("eth/usdt", &HashMap::new()).unwrap(),
            "ETH-USDT"
        );
        assert_eq!(
            normalize_okx_symbol("XBTUSDT", &mappings).unwrap(),
            "BTC-USDT"
        );
        assert_eq!(internal_symbol_from_okx("BTC-USDT"), "BTCUSDT");
    }

    #[test]
    fn okx_order_status_mapping_should_cover_spot_states() {
        assert_eq!(map_okx_order_status("live"), OrderStatus::New);
        assert_eq!(
            map_okx_order_status("partially_filled"),
            OrderStatus::PartiallyFilled
        );
        assert_eq!(map_okx_order_status("filled"), OrderStatus::Filled);
        assert_eq!(map_okx_order_status("canceled"), OrderStatus::Cancelled);
        assert_eq!(map_okx_order_status("mmp_canceled"), OrderStatus::Cancelled);
        assert_eq!(map_okx_order_status("rejected"), OrderStatus::Rejected);
        assert_eq!(map_okx_order_status("pending"), OrderStatus::Unknown);
    }

    #[test]
    fn okx_capabilities_should_match_implemented_private_routes() {
        let client = OkxSpotClient::new(OkxSpotConfig::default());
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
    fn okx_balance_parsing_should_normalize_details() {
        let payload = json!([{
            "details": [
                {"ccy": "BTC", "cashBal": "0.12000000", "availBal": "0.10000000", "frozenBal": "0.02000000"},
                {"ccy": "ETH", "cashBal": "0", "availBal": "0", "frozenBal": "0"},
                {"ccy": "USDT", "cashBal": "125.0", "availBal": "123.45", "frozenBal": "1.55"}
            ]
        }]);
        let snapshot = parse_balance_snapshot(&payload, "okx").unwrap();
        assert_eq!(snapshot.market_type, MarketType::Spot);
        assert_eq!(snapshot.balances.len(), 2);
        assert_eq!(snapshot.balances[0].asset, "BTC");
        assert_eq!(snapshot.balances[0].available, 0.1);
        assert_eq!(snapshot.balances[0].locked, 0.02);
        assert!((snapshot.balances[1].total - 125.0).abs() < f64::EPSILON);
    }

    #[test]
    fn okx_orderbook_parsing_should_capture_levels() {
        let payload = json!([{
            "bids": [["41006.8", "0.60038921", "0", "1"]],
            "asks": [["41006.9", "0.30178218", "0", "2"]],
            "ts": "1629966436396"
        }]);
        let snapshot = parse_orderbook_snapshot(&payload, "okx", Some("BTC-USDT")).unwrap();
        assert_eq!(snapshot.symbol, "BTCUSDT");
        assert_eq!(snapshot.bids[0].price, 41006.8);
        assert_eq!(snapshot.asks[0].quantity, 0.30178218);
        assert!(snapshot.exchange_timestamp.is_some());
    }

    #[tokio::test]
    async fn okx_orderbook_should_validate_depth_before_request() {
        let client = OkxSpotClient::new(OkxSpotConfig::default());

        let err = client.get_orderbook("BTCUSDT", 0).await.unwrap_err();

        assert!(matches!(
            err,
            ExchangeClientError::Validation { field: "depth", .. }
        ));
    }

    #[tokio::test]
    async fn okx_get_order_should_validate_order_id_before_request() {
        let client = OkxSpotClient::new(OkxSpotConfig::default());

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
    async fn okx_spot_client_should_route_common_private_rest_readbacks() {
        let (base_url, seen) = spawn_okx_rest_server(vec![
            json!({
                "code": "0",
                "msg": "",
                "data": [{
                    "details": [
                        {"ccy": "BTC", "cashBal": "0.12000000", "availBal": "0.10000000", "frozenBal": "0.02000000"},
                        {"ccy": "USDT", "cashBal": "125.0", "availBal": "123.45", "frozenBal": "1.55"}
                    ]
                }]
            }),
            json!({
                "code": "0",
                "msg": "",
                "data": [{
                    "instId": "BTC-USDT",
                    "ordId": "1001",
                    "clOrdId": "CLIENT1",
                    "side": "buy",
                    "ordType": "limit",
                    "state": "partially_filled",
                    "px": "65000",
                    "sz": "0.01",
                    "accFillSz": "0.004",
                    "avgPx": "64950",
                    "cTime": "1700000000000",
                    "uTime": "1700000001000"
                }]
            }),
            json!({
                "code": "0",
                "msg": "",
                "data": [{
                    "instId": "BTC-USDT",
                    "ordId": "1002",
                    "clOrdId": "CLIENT2",
                    "side": "sell",
                    "ordType": "limit",
                    "state": "live",
                    "px": "70000",
                    "sz": "0.02",
                    "accFillSz": "0",
                    "cTime": "1700000002000",
                    "uTime": "1700000002000"
                }]
            }),
            json!({
                "code": "0",
                "msg": "",
                "data": [{
                    "maker": "-0.0008",
                    "taker": "-0.001"
                }]
            }),
            json!({
                "code": "0",
                "msg": "",
                "data": [{
                    "instId": "BTC-USDT",
                    "tradeId": "fill-1",
                    "ordId": "1001",
                    "clOrdId": "CLIENT1",
                    "side": "buy",
                    "fillPx": "64950",
                    "fillSz": "0.004",
                    "feeCcy": "USDT",
                    "fee": "-0.2598",
                    "execType": "T",
                    "fillTime": "1700000001000"
                }]
            }),
        ])
        .await;
        let client = OkxSpotClient::new(OkxSpotConfig {
            api_key: "key".to_string(),
            api_secret: "secret".to_string(),
            passphrase: "passphrase".to_string(),
            rest_base_url: base_url,
            dry_run: false,
            ..OkxSpotConfig::default()
        });

        let balances = client.get_balances().await.unwrap();
        assert_eq!(balances.exchange, "okx");
        assert_eq!(balances.market_type, MarketType::Spot);
        assert_eq!(balances.balances.len(), 2);
        assert_eq!(balances.balances[0].asset, "BTC");
        assert_eq!(balances.balances[0].available, 0.1);

        let order = client.get_order("BTCUSDT", "1001").await.unwrap();
        assert_eq!(order.exchange, "okx");
        assert_eq!(order.market_type, MarketType::Spot);
        assert_eq!(order.symbol, "BTCUSDT");
        assert_eq!(order.order_id, "1001");
        assert_eq!(order.client_order_id.as_deref(), Some("CLIENT1"));
        assert_eq!(order.side, OrderSide::Buy);
        assert_eq!(order.status, OrderStatus::PartiallyFilled);
        assert_eq!(order.price, Some(65_000.0));
        assert_eq!(order.quantity, 0.01);
        assert_eq!(order.filled_quantity, 0.004);

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
        assert_eq!(fee_rate.maker, -0.0008);
        assert_eq!(fee_rate.taker, -0.001);

        let fills = client.get_recent_fills("BTCUSDT").await.unwrap();
        assert_eq!(fills.len(), 1);
        let fill = &fills[0];
        assert_eq!(fill.exchange, "okx");
        assert_eq!(fill.market_type, MarketType::Spot);
        assert_eq!(fill.symbol, "BTCUSDT");
        assert_eq!(fill.trade_id.as_deref(), Some("fill-1"));
        assert_eq!(fill.order_id.as_deref(), Some("1001"));
        assert_eq!(fill.client_order_id.as_deref(), Some("CLIENT1"));
        assert_eq!(fill.side, OrderSide::Buy);
        assert_eq!(fill.price, 64_950.0);
        assert_eq!(fill.quantity, 0.004);
        assert_eq!(fill.fee_asset.as_deref(), Some("USDT"));
        assert_eq!(fill.fee_amount, Some(0.2598));
        assert_eq!(fill.liquidity, LiquidityRole::Taker);

        let requests = seen.lock().unwrap().clone();
        assert_eq!(requests.len(), 5);
        assert_signed_okx_request(&requests[0], "GET", "/api/v5/account/balance");
        assert!(requests[0].query.is_empty());

        assert_signed_okx_request(&requests[1], "GET", "/api/v5/trade/order");
        assert_eq!(
            requests[1].query.get("instId").map(String::as_str),
            Some("BTC-USDT")
        );
        assert_eq!(
            requests[1].query.get("ordId").map(String::as_str),
            Some("1001")
        );

        assert_signed_okx_request(&requests[2], "GET", "/api/v5/trade/orders-pending");
        assert_eq!(
            requests[2].query.get("instType").map(String::as_str),
            Some("SPOT")
        );
        assert_eq!(
            requests[2].query.get("instId").map(String::as_str),
            Some("BTC-USDT")
        );

        assert_signed_okx_request(&requests[3], "GET", "/api/v5/account/trade-fee");
        assert_eq!(
            requests[3].query.get("instType").map(String::as_str),
            Some("SPOT")
        );
        assert_eq!(
            requests[3].query.get("instId").map(String::as_str),
            Some("BTC-USDT")
        );

        assert_signed_okx_request(&requests[4], "GET", "/api/v5/trade/fills-history");
        assert_eq!(
            requests[4].query.get("instType").map(String::as_str),
            Some("SPOT")
        );
        assert_eq!(
            requests[4].query.get("instId").map(String::as_str),
            Some("BTC-USDT")
        );
        assert_eq!(
            requests[4].query.get("limit").map(String::as_str),
            Some("100")
        );
    }

    #[tokio::test]
    async fn okx_spot_client_should_route_order_mutations() {
        let (base_url, seen) = spawn_okx_rest_server(vec![
            json!({
                "code": "0",
                "msg": "",
                "data": [{"ordId": "2001", "clOrdId": "LIMIT1", "sCode": "0", "sMsg": ""}]
            }),
            json!({
                "code": "0",
                "msg": "",
                "data": [{"ordId": "2002", "clOrdId": "QUOTE1", "sCode": "0", "sMsg": ""}]
            }),
            json!({
                "code": "0",
                "msg": "",
                "data": [{"ordId": "2001", "clOrdId": "LIMIT1", "sCode": "0", "sMsg": ""}]
            }),
            json!({
                "code": "0",
                "msg": "",
                "data": [
                    {
                        "instId": "BTC-USDT",
                        "ordId": "2003",
                        "clOrdId": "CANCELALL1",
                        "side": "sell",
                        "ordType": "limit",
                        "state": "live",
                        "px": "70000",
                        "sz": "0.02",
                        "accFillSz": "0"
                    }
                ]
            }),
            json!({
                "code": "0",
                "msg": "",
                "data": [{"ordId": "2003", "clOrdId": "CANCELALL1", "sCode": "0", "sMsg": ""}]
            }),
            json!({
                "code": "0",
                "msg": "",
                "data": [{"ordId": "2004", "clOrdId": "AMENDNEW", "sCode": "0", "sMsg": ""}]
            }),
        ])
        .await;
        let client = OkxSpotClient::new(OkxSpotConfig {
            api_key: "key".to_string(),
            api_secret: "secret".to_string(),
            passphrase: "passphrase".to_string(),
            rest_base_url: base_url,
            dry_run: false,
            ..OkxSpotConfig::default()
        });

        let placed = client
            .place_order(OrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                side: OrderSide::Buy,
                position_side: PositionSide::None,
                order_type: OrderType::Limit,
                time_in_force: Some(TimeInForce::GTC),
                quantity: 0.02,
                price: Some(65_000.0),
                client_order_id: Some("LIMIT1".to_string()),
                reduce_only: false,
            })
            .await
            .unwrap();
        assert_eq!(placed.exchange, "okx");
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
        assert_eq!(cancel_all.exchange, "okx");
        assert_eq!(cancel_all.market_type, MarketType::Spot);
        assert_eq!(cancel_all.symbol.as_deref(), Some("BTCUSDT"));
        assert_eq!(cancel_all.cancelled_orders, 1);

        let amended = client
            .amend_order(AmendOrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                order_id: Some("2004".to_string()),
                client_order_id: Some("AMENDOLD".to_string()),
                new_client_order_id: Some("AMENDNEW".to_string()),
                new_quantity: 0.015,
            })
            .await
            .unwrap();
        assert_eq!(amended.order_id, "2004");
        assert_eq!(amended.client_order_id.as_deref(), Some("AMENDNEW"));
        assert_eq!(amended.quantity, 0.015);

        assert!(!client.capabilities().supports_order_list);

        let requests = seen.lock().unwrap().clone();
        assert_eq!(requests.len(), 6);

        assert_signed_okx_request(&requests[0], "POST", "/api/v5/trade/order");
        let body = requests[0].body.as_ref().unwrap();
        assert_eq!(body["instId"], "BTC-USDT");
        assert_eq!(body["tdMode"], "cash");
        assert_eq!(body["side"], "buy");
        assert_eq!(body["ordType"], "limit");
        assert_eq!(body["sz"], "0.02");
        assert_eq!(body["px"], "65000");
        assert_eq!(body["clOrdId"], "LIMIT1");

        assert_signed_okx_request(&requests[1], "POST", "/api/v5/trade/order");
        let body = requests[1].body.as_ref().unwrap();
        assert_eq!(body["ordType"], "market");
        assert_eq!(body["sz"], "125.5");
        assert_eq!(body["tgtCcy"], "quote_ccy");
        assert_eq!(body["clOrdId"], "QUOTE1");

        assert_signed_okx_request(&requests[2], "POST", "/api/v5/trade/cancel-order");
        let body = requests[2].body.as_ref().unwrap();
        assert_eq!(body["instId"], "BTC-USDT");
        assert_eq!(body["ordId"], "2001");
        assert_eq!(body["clOrdId"], "LIMIT1");

        assert_signed_okx_request(&requests[3], "GET", "/api/v5/trade/orders-pending");
        assert_eq!(
            requests[3].query.get("instType").map(String::as_str),
            Some("SPOT")
        );
        assert_eq!(
            requests[3].query.get("instId").map(String::as_str),
            Some("BTC-USDT")
        );

        assert_signed_okx_request(&requests[4], "POST", "/api/v5/trade/cancel-batch-orders");
        let body = requests[4].body.as_ref().unwrap().as_array().unwrap();
        assert_eq!(body.len(), 1);
        assert_eq!(body[0]["instId"], "BTC-USDT");
        assert_eq!(body[0]["ordId"], "2003");

        assert_signed_okx_request(&requests[5], "POST", "/api/v5/trade/amend-order");
        let body = requests[5].body.as_ref().unwrap();
        assert_eq!(body["instId"], "BTC-USDT");
        assert_eq!(body["ordId"], "2004");
        assert_eq!(body["clOrdId"], "AMENDOLD");
        assert_eq!(body["newClOrdId"], "AMENDNEW");
        assert_eq!(body["newSz"], "0.015");
    }

    #[test]
    fn okx_instruments_should_parse_symbol_rules() {
        let payload = json!([{
            "instType": "SPOT",
            "instId": "BTC-USDT",
            "baseCcy": "BTC",
            "quoteCcy": "USDT",
            "tickSz": "0.1",
            "lotSz": "0.00000001",
            "minSz": "0.00001",
            "maxLmtSz": "9999999999",
            "minLmtAmt": "5",
            "state": "live"
        }]);
        let rules = parse_symbol_rules(&payload).unwrap();
        assert_eq!(rules.len(), 1);
        let rule = &rules[0];
        assert_eq!(rule.exchange, "okx");
        assert_eq!(rule.internal_symbol, "BTCUSDT");
        assert_eq!(rule.exchange_symbol, "BTC-USDT");
        assert_eq!(rule.price_precision, 1);
        assert_eq!(rule.quantity_precision, 8);
        assert_eq!(rule.tick_size, 0.1);
        assert_eq!(rule.step_size, 0.00000001);
        assert_eq!(rule.min_notional, 5.0);
        assert_eq!(rule.status, SymbolStatus::Trading);
        assert!(rule.supported_order_types.contains(&OrderType::PostOnly));
        assert!(rule.supported_time_in_force.contains(&TimeInForce::GTX));
    }

    #[test]
    fn okx_order_body_should_map_common_spot_order_types() {
        let mut request = OrderRequest {
            market_type: MarketType::Spot,
            symbol: "BTCUSDT".to_string(),
            side: OrderSide::Buy,
            position_side: PositionSide::None,
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTX),
            quantity: 0.01,
            price: Some(65_000.0),
            client_order_id: Some("crossarb-s-mk-1-deadbeef".to_string()),
            reduce_only: false,
        };

        let post_only = okx_order_body(&request, "BTC-USDT").unwrap();
        assert_eq!(post_only["tdMode"], "cash");
        assert_eq!(post_only["ordType"], "post_only");
        assert_eq!(post_only["px"], "65000");

        request.order_type = OrderType::Limit;
        request.time_in_force = Some(TimeInForce::IOC);
        let ioc = okx_order_body(&request, "BTC-USDT").unwrap();
        assert_eq!(ioc["ordType"], "ioc");

        request.order_type = OrderType::Market;
        request.time_in_force = None;
        request.price = None;
        let market = okx_order_body(&request, "BTC-USDT").unwrap();
        assert_eq!(market["ordType"], "market");
        assert_eq!(market["tgtCcy"], "btc");
    }

    #[tokio::test]
    async fn okx_place_order_should_ack_in_dry_run() {
        let client = OkxSpotClient::new(OkxSpotConfig {
            dry_run: true,
            ..OkxSpotConfig::default()
        });

        let ack = client
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

        assert_eq!(ack.exchange, "okx");
        assert_eq!(ack.market_type, MarketType::Spot);
        assert_eq!(ack.symbol, "BTCUSDT");
        assert_eq!(ack.side, OrderSide::Buy);
        assert_eq!(ack.order_type, OrderType::Limit);
        assert_eq!(ack.price, Some(65_000.0));
        assert_eq!(ack.quantity, 0.01);
        assert_eq!(ack.status, OrderStatus::New);
        assert!(ack.client_order_id.is_some());
        assert!(ack.order_id.starts_with("dry-okx-spot-"));
    }

    #[test]
    fn okx_amend_order_body_should_use_new_size() {
        let request = AmendOrderRequest {
            market_type: MarketType::Spot,
            symbol: "BTCUSDT".to_string(),
            order_id: Some("12345".to_string()),
            client_order_id: Some("CLIENT1".to_string()),
            new_client_order_id: Some("CLIENT2".to_string()),
            new_quantity: 0.0025,
        };

        let body = okx_amend_order_body(&request, "BTC-USDT").unwrap();
        assert_eq!(body["instId"], "BTC-USDT");
        assert_eq!(body["ordId"], "12345");
        assert_eq!(body["clOrdId"], "CLIENT1");
        assert_eq!(body["newClOrdId"], "CLIENT2");
        assert_eq!(body["newSz"], "0.0025");

        let client_only = AmendOrderRequest {
            order_id: Some("   ".to_string()),
            client_order_id: Some("CLIENT1".to_string()),
            new_client_order_id: Some("   ".to_string()),
            ..request
        };
        let body = okx_amend_order_body(&client_only, "BTC-USDT").unwrap();
        assert!(body.get("ordId").is_none());
        assert_eq!(body["clOrdId"], "CLIENT1");
        assert!(body.get("newClOrdId").is_none());
    }

    #[test]
    fn okx_quote_market_order_body_should_use_quote_currency_target() {
        let request = QuoteMarketOrderRequest {
            market_type: MarketType::Spot,
            symbol: "BTCUSDT".to_string(),
            side: OrderSide::Buy,
            quote_quantity: 125.5,
            client_order_id: Some("CLIENT1".to_string()),
        };

        let body = okx_quote_market_order_body(&request, "BTC-USDT").unwrap();
        assert_eq!(body["instId"], "BTC-USDT");
        assert_eq!(body["tdMode"], "cash");
        assert_eq!(body["side"], "buy");
        assert_eq!(body["ordType"], "market");
        assert_eq!(body["sz"], "125.5");
        assert_eq!(body["tgtCcy"], "quote_ccy");
        assert_eq!(body["clOrdId"], "CLIENT1");
    }

    #[tokio::test]
    async fn okx_quote_market_order_should_ack_in_dry_run() {
        let client = OkxSpotClient::new(OkxSpotConfig {
            dry_run: true,
            ..OkxSpotConfig::default()
        });

        let ack = client
            .place_quote_market_order(QuoteMarketOrderRequest::spot_buy("BTCUSDT", 25.0))
            .await
            .unwrap();

        assert_eq!(ack.exchange, "okx");
        assert_eq!(ack.market_type, MarketType::Spot);
        assert_eq!(ack.symbol, "BTCUSDT");
        assert_eq!(ack.side, OrderSide::Buy);
        assert_eq!(ack.order_type, OrderType::Market);
        assert_eq!(ack.quantity, 25.0);
        assert_eq!(ack.status, OrderStatus::New);
        assert!(ack.client_order_id.is_some());
    }

    #[tokio::test]
    async fn okx_amend_order_should_ack_in_dry_run() {
        let client = OkxSpotClient::new(OkxSpotConfig {
            dry_run: true,
            ..OkxSpotConfig::default()
        });

        let ack = client
            .amend_order(AmendOrderRequest::reduce_quantity_by_order_id(
                MarketType::Spot,
                "BTCUSDT",
                "12345",
                0.0025,
            ))
            .await
            .unwrap();

        assert_eq!(ack.exchange, "okx");
        assert_eq!(ack.market_type, MarketType::Spot);
        assert_eq!(ack.symbol, "BTCUSDT");
        assert_eq!(ack.order_id, "12345");
        assert_eq!(ack.quantity, 0.0025);
        assert_eq!(ack.status, OrderStatus::New);

        let client_id_ack = client
            .amend_order(AmendOrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                order_id: Some("   ".to_string()),
                client_order_id: Some("OKXAMEND1".to_string()),
                new_client_order_id: Some("OKXAMEND2".to_string()),
                new_quantity: 0.001,
            })
            .await
            .unwrap();
        assert!(client_id_ack.order_id.starts_with("dry-okx-spot-"));
        assert_eq!(client_id_ack.client_order_id.as_deref(), Some("OKXAMEND2"));
        assert_eq!(client_id_ack.quantity, 0.001);

        let invalid_new_client_id = client
            .amend_order(AmendOrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                order_id: Some("12345".to_string()),
                client_order_id: None,
                new_client_order_id: Some("bad/id".to_string()),
                new_quantity: 0.001,
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
    async fn okx_cancel_order_should_ack_and_validate_market_type_in_dry_run() {
        let client = OkxSpotClient::new(OkxSpotConfig {
            dry_run: true,
            ..OkxSpotConfig::default()
        });

        let ack = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                order_id: Some("12345".to_string()),
                client_order_id: None,
            })
            .await
            .unwrap();

        assert_eq!(ack.exchange, "okx");
        assert_eq!(ack.market_type, MarketType::Spot);
        assert_eq!(ack.symbol, "BTCUSDT");
        assert_eq!(ack.order_id.as_deref(), Some("12345"));
        assert_eq!(ack.status, OrderStatus::Cancelled);

        let ack = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                order_id: Some("   ".to_string()),
                client_order_id: Some("CANCELCLIENT1".to_string()),
            })
            .await
            .unwrap();
        assert_eq!(ack.order_id, None);
        assert_eq!(ack.client_order_id.as_deref(), Some("CANCELCLIENT1"));

        let error = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Perpetual,
                symbol: "BTCUSDT".to_string(),
                order_id: Some("12345".to_string()),
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
    async fn okx_cancel_all_orders_should_ack_and_validate_market_type_in_dry_run() {
        let client = OkxSpotClient::new(OkxSpotConfig {
            dry_run: true,
            ..OkxSpotConfig::default()
        });

        let ack = client
            .cancel_all_orders(CancelAllOrdersRequest::for_symbol(
                MarketType::Spot,
                "BTCUSDT",
            ))
            .await
            .unwrap();

        assert_eq!(ack.exchange, "okx");
        assert_eq!(ack.market_type, MarketType::Spot);
        assert_eq!(ack.symbol.as_deref(), Some("BTCUSDT"));
        assert_eq!(ack.cancelled_orders, 0);

        let error = client
            .cancel_all_orders(CancelAllOrdersRequest::for_symbol(
                MarketType::Perpetual,
                "BTCUSDT",
            ))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("only supports MarketType::Spot"));
    }

    #[tokio::test]
    async fn okx_order_list_should_validate_before_unsupported() {
        use crate::exchanges::unified::{
            OrderListConditionalLeg, OrderListLegType, OrderListRequest,
        };

        fn oco_request() -> OrderListRequest {
            OrderListRequest::Oco {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                list_client_order_id: Some("OKXOCOLIST1".to_string()),
                side: OrderSide::Sell,
                quantity: 0.01,
                above: OrderListConditionalLeg {
                    order_type: OrderListLegType::LimitMaker,
                    price: Some(70_000.0),
                    stop_price: None,
                    time_in_force: None,
                    client_order_id: Some("OKXABOVE1".to_string()),
                },
                below: OrderListConditionalLeg {
                    order_type: OrderListLegType::StopLossLimit,
                    price: Some(59_500.0),
                    stop_price: Some(60_000.0),
                    time_in_force: Some(TimeInForce::GTC),
                    client_order_id: Some("OKXBELOW1".to_string()),
                },
            }
        }

        let client = OkxSpotClient::new(OkxSpotConfig {
            dry_run: false,
            rest_base_url: "http://127.0.0.1:9".to_string(),
            ..OkxSpotConfig::default()
        });

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

        let mut invalid_client_id = oco_request();
        if let OrderListRequest::Oco {
            list_client_order_id,
            ..
        } = &mut invalid_client_id
        {
            *list_client_order_id = Some("bad/id".to_string());
        }
        assert!(matches!(
            client.place_order_list(invalid_client_id).await,
            Err(ExchangeClientError::Validation {
                field: "client_order_id",
                ..
            })
        ));
    }

    #[test]
    fn okx_cancel_all_batch_bodies_should_chunk_pending_orders() {
        let mut orders = Vec::new();
        for index in 0..21 {
            orders.push(json!({
                "instId": "BTC-USDT",
                "ordId": if index == 20 { "" } else { "order-id" },
                "clOrdId": format!("client-{index}")
            }));
        }
        let batches = okx_cancel_all_batch_bodies(&Value::Array(orders)).unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].as_array().unwrap().len(), 20);
        assert_eq!(batches[1].as_array().unwrap().len(), 1);
        assert_eq!(batches[0][0]["instId"], "BTC-USDT");
        assert_eq!(batches[0][0]["ordId"], "order-id");
        assert_eq!(batches[1][0]["clOrdId"], "client-20");
        assert!(batches[1][0].get("ordId").is_none());
    }

    #[test]
    fn okx_cancel_batch_success_count_should_handle_partial_failures() {
        let ack = json!([
            {"instId":"BTC-USDT","ordId":"1","sCode":"0","sMsg":""},
            {"instId":"BTC-USDT","ordId":"2","sCode":"51400","sMsg":"Order does not exist"},
            {"instId":"ETH-USDT","clOrdId":"client-3","sCode":"0","sMsg":""}
        ]);
        assert_eq!(okx_cancel_batch_success_count(&ack).unwrap(), 2);
    }

    #[test]
    fn okx_fills_history_should_parse_recent_fills() {
        let payload = json!([{
            "instType": "SPOT",
            "instId": "BTC-USDT",
            "tradeId": "98765",
            "ordId": "12345",
            "clOrdId": "cli-1",
            "side": "sell",
            "fillPx": "41001",
            "fillSz": "0.01",
            "fee": "-0.00001",
            "feeCcy": "BTC",
            "execType": "M",
            "fillTime": "1629966437000"
        }]);
        let fills = parse_fills(&payload).unwrap();
        assert_eq!(fills.len(), 1);
        let fill = &fills[0];
        assert_eq!(fill.exchange, "okx");
        assert_eq!(fill.symbol, "BTCUSDT");
        assert_eq!(fill.side, OrderSide::Sell);
        assert_eq!(fill.liquidity, LiquidityRole::Maker);
        assert_eq!(fill.fee_asset.as_deref(), Some("BTC"));
        assert_eq!(fill.fee_amount, Some(0.00001));
        assert_eq!(fill.trade_id.as_deref(), Some("98765"));
    }

    #[test]
    fn okx_private_order_update_should_parse_order_and_fill() {
        let payload = json!({
            "instType": "SPOT",
            "instId": "BTC-USDT",
            "ordId": "12345",
            "clOrdId": "cli-1",
            "side": "buy",
            "ordType": "limit",
            "state": "partially_filled",
            "px": "41000",
            "sz": "0.02",
            "accFillSz": "0.01",
            "avgPx": "41001",
            "fillSz": "0.01",
            "fillPx": "41001",
            "tradeId": "98765",
            "fillFee": "-0.00001",
            "fillFeeCcy": "BTC",
            "execType": "T",
            "cTime": "1629966436000",
            "uTime": "1629966437000",
            "fillTime": "1629966437000"
        });
        let (order, fill) = parse_private_order_update(&payload, "okx").unwrap();
        assert_eq!(order.status, OrderStatus::PartiallyFilled);
        assert_eq!(order.symbol, "BTCUSDT");
        assert_eq!(order.filled_quantity, 0.01);
        let fill = fill.unwrap();
        assert_eq!(fill.liquidity, LiquidityRole::Taker);
        assert_eq!(fill.fee_asset.as_deref(), Some("BTC"));
        assert_eq!(fill.fee_amount, Some(0.00001));
        assert_eq!(fill.trade_id.as_deref(), Some("98765"));
    }

    #[test]
    fn okx_private_user_stream_should_parse_account_and_filter_control_messages() {
        let login_ack = r#"{"event":"login","code":"0","msg":""}"#;
        let login_error = r#"{"event":"login","code":"60009","msg":"Login failed"}"#;
        let subscribe_ack = r#"{"event":"subscribe","arg":{"channel":"account"},"connId":"abc"}"#;

        assert!(okx_login_succeeded(login_ack));
        assert!(!okx_login_failed(login_ack));
        assert!(okx_login_failed(login_error));
        assert!(is_okx_event_message(subscribe_ack));

        let account_message = r#"{
            "arg":{"channel":"account"},
            "data":[{
                "details":[{
                    "ccy":"USDT",
                    "eq":"100.5",
                    "availEq":"80",
                    "frozenBal":"20.5",
                    "uTime":"1700000000000"
                }]
            }]
        }"#;
        let events = parse_user_stream_message(account_message).unwrap();

        assert_eq!(events.len(), 1);
        match &events[0] {
            UserStreamEvent::Balance(snapshot) => {
                assert_eq!(snapshot.exchange, "okx");
                assert_eq!(snapshot.market_type, MarketType::Spot);
                assert_eq!(snapshot.balances.len(), 1);
                let balance = &snapshot.balances[0];
                assert_eq!(balance.asset, "USDT");
                assert_eq!(balance.total, 100.5);
                assert_eq!(balance.available, 80.0);
                assert_eq!(balance.locked, 20.5);
            }
            other => panic!("expected OKX balance event, got {other:?}"),
        }
    }
}
