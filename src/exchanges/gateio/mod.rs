use std::collections::HashMap;
use std::time::Duration as StdDuration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha512};
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
    ExchangeErrorClass, ExchangeHealthStatus, FeeRate, FeeRateSource, LiquidityRole, MarketType,
    OrderBookLevel, OrderBookSnapshot, OrderRequest, OrderResponse, OrderSide, OrderStatus,
    OrderType, PositionSide, QuoteMarketOrderRequest, SymbolRule, SymbolStatus, TimeInForce,
    TradeFill, UserStreamEvent,
};
use crate::utils::SignatureHelper;

type HmacSha512 = Hmac<Sha512>;

const DEFAULT_REST_BASE_URL: &str = "https://api.gateio.ws/api/v4";
const DEFAULT_WS_URL: &str = "wss://api.gateio.ws/ws/v4/";
const DEFAULT_STALE_BOOK_MS: u64 = 10_000;
const DEFAULT_RECONNECT_INTERVAL_MS: u64 = 1_000;
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_DEPTH: u16 = 5;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GateIoSpotConfig {
    pub api_key: String,
    pub api_secret: String,
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
    pub fee_override: Option<FeeOverride>,
    #[serde(default)]
    pub log_raw_messages: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct FeeOverride {
    pub maker_fee_rate: f64,
    pub taker_fee_rate: f64,
}

impl Default for GateIoSpotConfig {
    fn default() -> Self {
        Self {
            api_key: std::env::var("GATEIO_API_KEY")
                .or_else(|_| std::env::var("GATE_API_KEY"))
                .unwrap_or_default(),
            api_secret: std::env::var("GATEIO_API_SECRET")
                .or_else(|_| std::env::var("GATE_API_SECRET"))
                .unwrap_or_default(),
            base_url: default_rest_base_url(),
            websocket_url: default_ws_url(),
            dry_run: true,
            stale_book_ms: DEFAULT_STALE_BOOK_MS,
            reconnect_interval_ms: DEFAULT_RECONNECT_INTERVAL_MS,
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            orderbook_depth: DEFAULT_DEPTH,
            enabled_symbols: Vec::new(),
            fee_override: None,
            log_raw_messages: false,
        }
    }
}

#[derive(Clone)]
pub struct GateIoSpotClient {
    config: GateIoSpotConfig,
    http: reqwest::Client,
    reservations: BalanceReservationManager,
}

impl GateIoSpotClient {
    pub fn new(config: GateIoSpotConfig) -> Self {
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
        generate_client_order_id("gateio", MarketType::Spot, "spot").into_string()
    }

    async fn send_public_request(
        &self,
        endpoint: &str,
        params: HashMap<String, String>,
    ) -> ExchangeClientResult<Value> {
        let url = build_url(&self.config.base_url, endpoint, &params);
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
        params: HashMap<String, String>,
        body: Option<Value>,
    ) -> ExchangeClientResult<Value> {
        self.ensure_credentials()?;
        let url = build_url(&self.config.base_url, endpoint, &params);
        let query = SignatureHelper::build_query_string(&params);
        let body_text = body
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(CoreExchangeError::from)?
            .unwrap_or_default();
        let timestamp = Utc::now().timestamp().to_string();
        let sign_path = signed_request_path(&self.config.base_url, endpoint);
        let signature = sign_request(
            &self.config.api_secret,
            method.as_str(),
            &sign_path,
            &query,
            &body_text,
            &timestamp,
        );
        let mut request = self
            .http
            .request(method, url)
            .header("KEY", &self.config.api_key)
            .header("Timestamp", &timestamp)
            .header("SIGN", signature)
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
                exchange: "gateio".to_string(),
                class: ExchangeErrorClass::AuthenticationFailed,
                code: None,
                message: "GATEIO_API_KEY and GATEIO_API_SECRET are required".to_string(),
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
                        reason: "Gate.io buy reservation requires explicit price".to_string(),
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
            exchange: "gateio".to_string(),
            market_type: MarketType::Spot,
            symbol: symbol.to_string(),
            order_id: format!("dry-gateio-{}", Utc::now().timestamp_millis()),
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
            exchange: "gateio".to_string(),
            market_type: MarketType::Spot,
            symbol: symbol.to_string(),
            order_id: format!("dry-gateio-{}", Utc::now().timestamp_millis()),
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
            exchange: "gateio".to_string(),
            market_type: MarketType::Spot,
            symbol: symbol.to_string(),
            order_id: request
                .order_id()
                .map(str::to_string)
                .unwrap_or_else(|| format!("dry-gateio-{}", Utc::now().timestamp_millis())),
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
impl ExchangeClient for GateIoSpotClient {
    fn market_type(&self) -> MarketType {
        MarketType::Spot
    }

    fn exchange_name(&self) -> &str {
        "gateio"
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let mut capabilities = ExchangeClientCapabilities::spot(self.exchange_name());
        capabilities.supports_cancel_all_orders = true;
        capabilities.supports_quote_market_order = true;
        capabilities.supports_amend_order = true;
        capabilities
    }

    fn normalize_symbol(&self, symbol: &str) -> ExchangeClientResult<String> {
        normalize_gateio_symbol(symbol).map(|symbol| symbol.replace('_', ""))
    }

    fn denormalize_symbol(&self, symbol: &str) -> ExchangeClientResult<String> {
        normalize_gateio_symbol(symbol)
    }

    async fn load_symbol_rules(&self) -> ExchangeClientResult<Vec<SymbolRule>> {
        let value = self
            .send_public_request("/spot/currency_pairs", HashMap::new())
            .await?;
        parse_symbol_rules(&value)
    }

    async fn get_balances(&self) -> ExchangeClientResult<BalanceSnapshot> {
        let value = self
            .send_signed_request(Method::GET, "/spot/accounts", HashMap::new(), None)
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
                        .balance_with_reservation("gateio", balance)
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
        let exchange_symbol = self.denormalize_symbol(symbol)?;
        validate_orderbook_depth(depth)?;
        let mut params = HashMap::new();
        params.insert("currency_pair".to_string(), exchange_symbol.clone());
        params.insert("limit".to_string(), normalize_depth(depth).to_string());
        let value = self.send_public_request("/spot/order_book", params).await?;
        parse_orderbook_snapshot(&value, &exchange_symbol, self.config.stale_book_ms)
    }

    async fn place_order(&self, mut request: OrderRequest) -> ExchangeClientResult<OrderResponse> {
        request.validate()?;
        if request.market_type != MarketType::Spot {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: "GateIoSpotClient only supports spot".to_string(),
            });
        }
        let exchange_symbol = self.denormalize_symbol(&request.symbol)?;
        request.symbol = exchange_symbol.replace('_', "");
        if request.client_order_id.is_none() {
            request.client_order_id = Some(Self::generate_client_order_id());
        }
        if let Some(client_order_id) = &request.client_order_id {
            validate_client_order_id("gateio", MarketType::Spot, client_order_id).map_err(
                |error| ExchangeClientError::Validation {
                    field: "client_order_id",
                    reason: error.to_string(),
                },
            )?;
        }
        let body = gateio_order_body(&request, &exchange_symbol)?;
        if self.config.dry_run {
            return Ok(self.dry_run_order_response(&request, &request.symbol));
        }
        let rule = self
            .get_symbol_rule(&request.symbol)
            .await?
            .unwrap_or_else(|| fallback_rule("gateio", &request.symbol, &exchange_symbol));
        validate_order_against_symbol_rule(&request, &rule)?;
        let mut reservation = self.reserve_for_order(&request, &rule).await?;

        let response = self
            .send_signed_request(Method::POST, "/spot/orders", HashMap::new(), Some(body))
            .await;
        match response {
            Ok(value) => parse_order_response(&value),
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
                "Gate.io Spot quote-sized market orders are only supported for market buys"
                    .to_string(),
            ));
        }
        let exchange_symbol = self.denormalize_symbol(&request.symbol)?;
        let symbol = exchange_symbol.replace('_', "");
        request.symbol = symbol.clone();
        ensure_quote_market_client_order_id(&mut request)?;
        if self.config.dry_run {
            return Ok(self.dry_run_quote_market_order_response(&request, &symbol));
        }
        let rule = self
            .get_symbol_rule(&symbol)
            .await?
            .unwrap_or_else(|| fallback_rule("gateio", &symbol, &exchange_symbol));
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
        let body = gateio_quote_market_order_body(&request, &exchange_symbol)?;
        let response = self
            .send_signed_request(Method::POST, "/spot/orders", HashMap::new(), Some(body))
            .await;
        match response {
            Ok(value) => parse_order_response(&value),
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
                reason: "GateIoSpotClient only supports spot".to_string(),
            });
        }
        ensure_amend_client_order_id(&request)?;
        if request.order_id().is_none() {
            return Err(ExchangeClientError::Unsupported(
                "Gate.io Spot amend requires exchange order_id; client_order_id lookup is not supported by the amend endpoint".to_string(),
            ));
        }
        if request.new_client_order_id().is_some() {
            return Err(ExchangeClientError::Unsupported(
                "Gate.io Spot amend_text is not equivalent to replacing client order id"
                    .to_string(),
            ));
        }
        let exchange_symbol = self.denormalize_symbol(&request.symbol)?;
        let symbol = exchange_symbol.replace('_', "");
        request.symbol = symbol.clone();
        if self.config.dry_run {
            return Ok(self.dry_run_amend_order_response(&request, &symbol));
        }
        let endpoint = format!(
            "/spot/orders/{}",
            request.order_id.as_deref().unwrap_or_default()
        );
        let body = gateio_amend_order_body(&request, &exchange_symbol)?;
        let value = self
            .send_signed_request(Method::PATCH, &endpoint, HashMap::new(), Some(body))
            .await?;
        parse_order_response(&value)
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeClientResult<CancelOrderResponse> {
        request.validate()?;
        if request.market_type != MarketType::Spot {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: "GateIoSpotClient only supports spot".to_string(),
            });
        }
        ensure_cancel_client_order_id(&request)?;
        let order_id = request
            .order_id
            .as_deref()
            .filter(|order_id| !order_id.trim().is_empty())
            .ok_or_else(|| {
                ExchangeClientError::Unsupported(
                    "Gate.io Spot cancel requires exchange order_id; client_order_id lookup is not supported by the cancel endpoint".to_string(),
                )
            })?
            .to_string();
        let exchange_symbol = self.denormalize_symbol(&request.symbol)?;
        if self.config.dry_run {
            return Ok(CancelOrderResponse {
                exchange: "gateio".to_string(),
                market_type: MarketType::Spot,
                symbol: exchange_symbol.replace('_', ""),
                order_id: request.order_id,
                client_order_id: request.client_order_id,
                status: OrderStatus::Cancelled,
                cancelled_at: Utc::now(),
            });
        }
        let mut params = HashMap::new();
        params.insert("currency_pair".to_string(), exchange_symbol.clone());
        let endpoint = format!("/spot/orders/{order_id}");
        let value = self
            .send_signed_request(Method::DELETE, &endpoint, params, None)
            .await?;
        Ok(CancelOrderResponse {
            exchange: "gateio".to_string(),
            market_type: MarketType::Spot,
            symbol: exchange_symbol.replace('_', ""),
            order_id: value_as_string(value.get("id")).or(request.order_id),
            client_order_id: value_as_string(value.get("text")).or(request.client_order_id),
            status: value
                .get("status")
                .and_then(Value::as_str)
                .map(map_gateio_order_status)
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
                reason: "GateIoSpotClient only supports spot".to_string(),
            });
        }
        let exchange_symbol = self.denormalize_symbol(request.validate_symbol_required()?)?;
        if self.config.dry_run {
            return Ok(CancelAllOrdersResponse {
                exchange: "gateio".to_string(),
                market_type: MarketType::Spot,
                symbol: Some(exchange_symbol.replace('_', "")),
                cancelled_orders: 0,
                cancelled_at: Utc::now(),
            });
        }
        let mut params = HashMap::new();
        params.insert("currency_pair".to_string(), exchange_symbol.clone());
        let value = self
            .send_signed_request(Method::DELETE, "/spot/orders", params, None)
            .await?;
        Ok(CancelAllOrdersResponse {
            exchange: "gateio".to_string(),
            market_type: MarketType::Spot,
            symbol: Some(exchange_symbol.replace('_', "")),
            cancelled_orders: gateio_cancel_all_cancelled_count(&value)?,
            cancelled_at: Utc::now(),
        })
    }

    async fn get_order(&self, symbol: &str, order_id: &str) -> ExchangeClientResult<OrderResponse> {
        let exchange_symbol = self.denormalize_symbol(symbol)?;
        let order_id = validate_order_lookup_id(order_id)?;
        let mut params = HashMap::new();
        params.insert("currency_pair".to_string(), exchange_symbol);
        let endpoint = format!("/spot/orders/{order_id}");
        let value = self
            .send_signed_request(Method::GET, &endpoint, params, None)
            .await?;
        parse_order_response(&value)
    }

    async fn get_open_orders(
        &self,
        symbol: Option<&str>,
    ) -> ExchangeClientResult<Vec<OrderResponse>> {
        let mut params = HashMap::new();
        if let Some(symbol) = symbol {
            params.insert(
                "currency_pair".to_string(),
                self.denormalize_symbol(symbol)?,
            );
        }
        let value = self
            .send_signed_request(Method::GET, "/spot/open_orders", params, None)
            .await?;
        let mut orders = Vec::new();
        for item in value.as_array().unwrap_or(&Vec::new()) {
            if let Some(items) = item.get("orders").and_then(Value::as_array) {
                for order in items {
                    orders.push(parse_order_response(order)?);
                }
            } else {
                orders.push(parse_order_response(item)?);
            }
        }
        Ok(orders)
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
        params.insert(
            "currency_pair".to_string(),
            self.denormalize_symbol(symbol)?,
        );
        let value = self
            .send_signed_request(Method::GET, "/spot/fee", params, None)
            .await?;
        Ok(FeeRate::new(
            number_from_str(value.get("maker_fee")).unwrap_or(0.002),
            number_from_str(value.get("taker_fee")).unwrap_or(0.002),
            FeeRateSource::ExchangeApi,
        ))
    }

    async fn get_recent_fills(&self, symbol: &str) -> ExchangeClientResult<Vec<TradeFill>> {
        let mut params = HashMap::new();
        params.insert(
            "currency_pair".to_string(),
            self.denormalize_symbol(symbol)?,
        );
        let value = self
            .send_signed_request(Method::GET, "/spot/my_trades", params, None)
            .await?;
        value
            .as_array()
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
            .map(|symbol| self.denormalize_symbol(symbol))
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
            "Gate.io Spot private user stream is not implemented; use REST polling fallback"
                .to_string(),
        ))
    }

    async fn health_check(&self) -> ExchangeClientResult<ExchangeHealthStatus> {
        Ok(ExchangeHealthStatus {
            exchange: "gateio".to_string(),
            market_type: MarketType::Spot,
            connected: self
                .send_public_request("/spot/currency_pairs", HashMap::new())
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

impl GateIoSpotClient {
    async fn run_orderbook_stream(self, symbols: Vec<String>, tx: mpsc::Sender<OrderBookSnapshot>) {
        let stale_after = StdDuration::from_millis(self.config.stale_book_ms);
        let reconnect_delay = StdDuration::from_millis(self.config.reconnect_interval_ms);
        let payload = json!({
            "time": Utc::now().timestamp(),
            "channel": "spot.order_book",
            "event": "subscribe",
            "payload": symbols.iter().map(|symbol| {
                json!([symbol, normalize_depth(self.config.orderbook_depth).to_string(), "100ms"])
            }).collect::<Vec<_>>()
        })
        .to_string();
        loop {
            match connect_async(self.config.websocket_url.as_str()).await {
                Ok((mut ws, _)) => {
                    let _ = ws.send(Message::Text(payload.clone())).await;
                    loop {
                        match timeout(stale_after, ws.next()).await {
                            Err(_) => break,
                            Ok(Some(Ok(Message::Text(text)))) => {
                                if self.config.log_raw_messages {
                                    log::debug!("Gate.io Spot WS raw={}", text);
                                }
                                match parse_ws_orderbook_message(&text, self.config.stale_book_ms) {
                                    Ok(Some(snapshot)) => {
                                        if tx.send(snapshot).await.is_err() {
                                            return;
                                        }
                                    }
                                    Ok(None) => {}
                                    Err(error) => {
                                        log::warn!("Gate.io Spot WS parse error: {}", error)
                                    }
                                }
                            }
                            Ok(Some(Ok(Message::Ping(payload)))) => {
                                let _ = ws.send(Message::Pong(payload)).await;
                            }
                            Ok(Some(Ok(Message::Close(_)))) | Ok(None) => break,
                            Ok(Some(Ok(_))) => {}
                            Ok(Some(Err(error))) => {
                                log::warn!("Gate.io Spot websocket error: {}", error);
                                break;
                            }
                        }
                    }
                }
                Err(error) => log::warn!("Gate.io Spot websocket connect error: {}", error),
            }
            sleep(reconnect_delay).await;
        }
    }
}

pub fn sign_request(
    secret: &str,
    method: &str,
    path: &str,
    query: &str,
    body: &str,
    timestamp: &str,
) -> String {
    let body_hash = hex::encode(Sha512::digest(body.as_bytes()));
    let prehash = format!("{method}\n{path}\n{query}\n{body_hash}\n{timestamp}");
    let mut mac =
        HmacSha512::new_from_slice(secret.as_bytes()).expect("HMAC supports any key length");
    mac.update(prehash.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn signed_request_path(base_url: &str, endpoint: &str) -> String {
    let base_path = reqwest::Url::parse(base_url)
        .ok()
        .map(|url| url.path().trim_end_matches('/').to_string())
        .unwrap_or_default();
    format!("{}{}", base_path, endpoint)
}

pub fn normalize_gateio_symbol(symbol: &str) -> ExchangeClientResult<String> {
    let normalized = symbol.trim().replace(['/', '-'], "_").to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(ExchangeClientError::Validation {
            field: "symbol",
            reason: "symbol must not be empty".to_string(),
        });
    }
    if normalized.contains('_') {
        return Ok(normalized);
    }
    split_compact_symbol(&normalized)
        .map(|(base, quote)| format!("{base}_{quote}"))
        .ok_or_else(|| ExchangeClientError::Validation {
            field: "symbol",
            reason: format!("cannot infer Gate.io currency_pair from {symbol}"),
        })
}

pub fn classify_gateio_error(label: Option<&str>, message: &str) -> ExchangeErrorClass {
    let label = label.unwrap_or_default().to_ascii_uppercase();
    let msg = message.to_ascii_lowercase();
    if label.contains("BALANCE") || msg.contains("insufficient") {
        ExchangeErrorClass::InsufficientBalance
    } else if label.contains("INVALID_CURRENCY")
        || msg.contains("currency_pair")
        || msg.contains("symbol")
    {
        ExchangeErrorClass::InvalidSymbol
    } else if label.contains("INVALID_PARAM") || msg.contains("precision") || msg.contains("size") {
        ExchangeErrorClass::InvalidPrecision
    } else if label.contains("RATE_LIMIT") || msg.contains("too many") {
        ExchangeErrorClass::RateLimited
    } else if label.contains("AUTH") || label.contains("INVALID_SIGNATURE") {
        ExchangeErrorClass::AuthenticationFailed
    } else if label.contains("ORDER_NOT_FOUND") {
        ExchangeErrorClass::OrderNotFound
    } else {
        ExchangeErrorClass::Unknown
    }
}

pub fn parse_symbol_rules(value: &Value) -> ExchangeClientResult<Vec<SymbolRule>> {
    value
        .as_array()
        .ok_or_else(|| parser_error("currency_pairs response is not an array", value))?
        .iter()
        .map(parse_symbol_rule)
        .collect()
}

pub fn parse_symbol_rule(value: &Value) -> ExchangeClientResult<SymbolRule> {
    let exchange_symbol = required_str(value, "id")?.to_ascii_uppercase();
    let base_asset = required_str(value, "base")?.to_ascii_uppercase();
    let quote_asset = required_str(value, "quote")?.to_ascii_uppercase();
    let tick_size = number_from_str(value.get("precision"))
        .map(|p| 10_f64.powi(-(p as i32)))
        .unwrap_or(0.0);
    let step_size = number_from_str(value.get("amount_precision"))
        .map(|p| 10_f64.powi(-(p as i32)))
        .unwrap_or(0.0);
    Ok(SymbolRule {
        exchange: "gateio".to_string(),
        market_type: MarketType::Spot,
        internal_symbol: exchange_symbol.replace('_', ""),
        exchange_symbol,
        base_asset,
        quote_asset,
        price_precision: number_from_str(value.get("precision")).unwrap_or(0.0) as u32,
        quantity_precision: number_from_str(value.get("amount_precision")).unwrap_or(0.0) as u32,
        tick_size,
        step_size,
        min_quantity: number_from_str(value.get("min_base_amount")).unwrap_or(0.0),
        min_notional: number_from_str(value.get("min_quote_amount")).unwrap_or(0.0),
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
        status: match value
            .get("trade_status")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
        {
            "tradable" => SymbolStatus::Trading,
            "disabled" => SymbolStatus::Suspended,
            _ => SymbolStatus::Unknown,
        },
        raw_metadata: Some(value.clone()),
    })
}

pub fn parse_balance_snapshot(value: &Value) -> ExchangeClientResult<BalanceSnapshot> {
    let mut balances = Vec::new();
    for item in value
        .as_array()
        .ok_or_else(|| parser_error("accounts response is not an array", value))?
    {
        let asset = required_str(item, "currency")?;
        let available = number_from_str(item.get("available")).unwrap_or(0.0);
        let locked = number_from_str(item.get("locked")).unwrap_or(0.0);
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
        exchange: "gateio".to_string(),
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
    let bids = parse_levels(
        value
            .get("bids")
            .and_then(Value::as_array)
            .ok_or_else(|| parser_error("orderbook missing bids", value))?,
    )?;
    let asks = parse_levels(
        value
            .get("asks")
            .and_then(Value::as_array)
            .ok_or_else(|| parser_error("orderbook missing asks", value))?,
    )?;
    let exchange_timestamp = value
        .get("current")
        .or_else(|| value.get("t"))
        .and_then(gateio_timestamp);
    let received_at = Utc::now();
    let latency_ms =
        exchange_timestamp.map(|ts| received_at.signed_duration_since(ts).num_milliseconds());
    Ok(OrderBookSnapshot {
        exchange: "gateio".to_string(),
        market_type: MarketType::Spot,
        symbol: symbol.replace('_', ""),
        best_bid: bids.first().map(|level| level.price),
        best_ask: asks.first().map(|level| level.price),
        bids,
        asks,
        exchange_timestamp,
        received_at,
        latency_ms,
        sequence: value
            .get("id")
            .or_else(|| value.get("u"))
            .and_then(Value::as_u64),
        is_stale: latency_ms
            .is_some_and(|latency| latency > stale_book_ms as i64 || latency < -1_000),
    })
}

pub fn parse_ws_orderbook_message(
    text: &str,
    stale_book_ms: u64,
) -> ExchangeClientResult<Option<OrderBookSnapshot>> {
    let value: Value = serde_json::from_str(text).map_err(CoreExchangeError::from)?;
    if value
        .get("event")
        .and_then(Value::as_str)
        .is_some_and(|event| event != "update")
        || value.get("error").is_some()
    {
        return Ok(None);
    }
    let result = value.get("result").unwrap_or(&value);
    let symbol = result
        .get("s")
        .or_else(|| result.get("currency_pair"))
        .and_then(Value::as_str)
        .unwrap_or("UNKNOWN");
    parse_orderbook_snapshot(result, symbol, stale_book_ms).map(Some)
}

pub fn parse_private_stream_message(text: &str) -> ExchangeClientResult<Vec<UserStreamEvent>> {
    let value: Value = serde_json::from_str(text).map_err(CoreExchangeError::from)?;
    if value
        .get("event")
        .and_then(Value::as_str)
        .is_some_and(|event| event != "update")
        || value.get("error").is_some()
    {
        return Ok(Vec::new());
    }
    let channel = value
        .get("channel")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let result = value.get("result").cloned().unwrap_or(Value::Null);
    let items = match result {
        Value::Array(items) => items,
        Value::Object(_) => vec![result],
        _ => Vec::new(),
    };
    let mut events = Vec::new();
    for item in items {
        if channel.contains("order") {
            events.push(UserStreamEvent::Order(parse_order_response(&item)?));
        } else if channel.contains("trade") || channel.contains("fill") {
            events.push(UserStreamEvent::Fill(parse_fill(&item)?));
        } else if channel.contains("balance") || channel.contains("account") {
            events.push(UserStreamEvent::Balance(parse_balance_snapshot(
                &Value::Array(vec![item]),
            )?));
        }
    }
    Ok(events)
}

pub fn parse_order_response(value: &Value) -> ExchangeClientResult<OrderResponse> {
    let exchange_symbol = value
        .get("currency_pair")
        .or_else(|| value.get("symbol"))
        .and_then(Value::as_str)
        .unwrap_or("UNKNOWN");
    let quantity = number_from_str(value.get("amount")).unwrap_or(0.0);
    let left = number_from_str(value.get("left")).unwrap_or(quantity);
    Ok(OrderResponse {
        exchange: "gateio".to_string(),
        market_type: MarketType::Spot,
        symbol: exchange_symbol.replace('_', ""),
        order_id: value_as_string(value.get("id")).unwrap_or_else(|| "unknown".to_string()),
        client_order_id: value_as_string(value.get("text")),
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
                .get("time_in_force")
                .or_else(|| value.get("tif"))
                .and_then(Value::as_str),
        ),
        status: value
            .get("status")
            .and_then(Value::as_str)
            .map(map_gateio_order_status)
            .unwrap_or(OrderStatus::Unknown),
        price: number_from_str(value.get("price")).filter(|price| *price > 0.0),
        quantity,
        filled_quantity: (quantity - left).max(0.0),
        average_price: number_from_str(value.get("avg_deal_price")),
        created_at: first_timestamp(value, &["create_time_ms", "create_time"])
            .unwrap_or_else(Utc::now),
        updated_at: first_timestamp(value, &["update_time_ms", "update_time"]),
    })
}

pub fn parse_fill(value: &Value) -> ExchangeClientResult<TradeFill> {
    let symbol = required_str(value, "currency_pair")
        .or_else(|_| required_str(value, "symbol"))
        .unwrap_or("UNKNOWN")
        .replace('_', "");
    Ok(TradeFill {
        exchange: "gateio".to_string(),
        market_type: MarketType::Spot,
        symbol,
        trade_id: value_as_string(value.get("id")),
        order_id: value_as_string(value.get("order_id")),
        client_order_id: value_as_string(value.get("text")),
        side: value
            .get("side")
            .and_then(Value::as_str)
            .map(parse_side)
            .transpose()?
            .unwrap_or(OrderSide::Buy),
        price: number_from_str(value.get("price")).unwrap_or(0.0),
        quantity: number_from_str(value.get("amount")).unwrap_or(0.0),
        fee_asset: value_as_string(value.get("fee_currency")),
        fee_amount: number_from_str(value.get("fee")),
        liquidity: match value
            .get("role")
            .and_then(Value::as_str)
            .unwrap_or_default()
        {
            "maker" => LiquidityRole::Maker,
            "taker" => LiquidityRole::Taker,
            _ => LiquidityRole::Unknown,
        },
        timestamp: first_timestamp(value, &["create_time_ms", "create_time"])
            .unwrap_or_else(Utc::now),
    })
}

pub fn map_gateio_order_status(status: &str) -> OrderStatus {
    match status.to_ascii_lowercase().as_str() {
        "open" => OrderStatus::New,
        "closed" => OrderStatus::Filled,
        "cancelled" | "canceled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}

fn gateio_order_body(request: &OrderRequest, exchange_symbol: &str) -> ExchangeClientResult<Value> {
    let mut body = json!({
        "currency_pair": exchange_symbol,
        "side": match request.side { OrderSide::Buy => "buy", OrderSide::Sell => "sell" },
        "type": match request.order_type { OrderType::Market => "market", _ => "limit" },
        "amount": request.quantity.to_string(),
    });
    if let Some(price) = request.price {
        body["price"] = Value::String(price.to_string());
    }
    if let Some(client_order_id) = &request.client_order_id {
        body["text"] = Value::String(gateio_client_text(client_order_id));
    }
    if let Some(tif) = request.time_in_force {
        body["time_in_force"] = Value::String(
            match tif {
                TimeInForce::GTC => "gtc",
                TimeInForce::IOC => "ioc",
                TimeInForce::FOK => "fok",
                TimeInForce::GTX => "poc",
            }
            .to_string(),
        );
    }
    Ok(body)
}

fn gateio_quote_market_order_body(
    request: &QuoteMarketOrderRequest,
    exchange_symbol: &str,
) -> ExchangeClientResult<Value> {
    if request.side != OrderSide::Buy {
        return Err(ExchangeClientError::Unsupported(
            "Gate.io Spot quote-sized market orders are only supported for market buys".to_string(),
        ));
    }
    let mut body = json!({
        "currency_pair": exchange_symbol,
        "side": "buy",
        "type": "market",
        "amount": request.quote_quantity.to_string(),
        "time_in_force": "ioc",
    });
    if let Some(client_order_id) = &request.client_order_id {
        body["text"] = Value::String(gateio_client_text(client_order_id));
    }
    Ok(body)
}

fn gateio_amend_order_body(
    request: &AmendOrderRequest,
    exchange_symbol: &str,
) -> ExchangeClientResult<Value> {
    if request.order_id().is_none() {
        return Err(ExchangeClientError::Unsupported(
            "Gate.io Spot amend requires exchange order_id; client_order_id lookup is not supported by the amend endpoint".to_string(),
        ));
    }
    if request.new_client_order_id().is_some() {
        return Err(ExchangeClientError::Unsupported(
            "Gate.io Spot amend_text is not equivalent to replacing client order id".to_string(),
        ));
    }
    Ok(json!({
        "currency_pair": exchange_symbol,
        "account": "spot",
        "amount": request.new_quantity.to_string(),
    }))
}

fn ensure_quote_market_client_order_id(
    request: &mut QuoteMarketOrderRequest,
) -> ExchangeClientResult<()> {
    if request.client_order_id.is_none() {
        request.client_order_id = Some(GateIoSpotClient::generate_client_order_id());
    }
    if let Some(client_order_id) = &request.client_order_id {
        validate_client_order_id("gateio", request.market_type, client_order_id).map_err(
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
        validate_client_order_id("gateio", request.market_type, client_order_id).map_err(
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
        validate_client_order_id("gateio", request.market_type, client_order_id).map_err(
            |error| ExchangeClientError::Validation {
                field: "client_order_id",
                reason: error.to_string(),
            },
        )?;
    }
    Ok(())
}

fn gateio_client_text(client_order_id: &str) -> String {
    const MAX_TEXT_LEN: usize = 28;
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
    if sanitized.len() + 2 > MAX_TEXT_LEN {
        sanitized.truncate(MAX_TEXT_LEN - 2);
    }
    format!("t-{sanitized}")
}

async fn parse_response(response: reqwest::Response) -> ExchangeClientResult<Value> {
    let status = response.status();
    let value: Value = response.json().await.map_err(CoreExchangeError::from)?;
    if !status.is_success() {
        let label = value.get("label").and_then(Value::as_str);
        let message = value
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("Gate.io request failed");
        return Err(ExchangeClientError::Classified(ExchangeError {
            exchange: "gateio".to_string(),
            class: classify_gateio_error(label, message),
            code: label.map(ToOwned::to_owned),
            message: message.to_string(),
        }));
    }
    Ok(value)
}

fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    let query = SignatureHelper::build_query_string(params);
    let mut url = format!("{}{}", base.trim_end_matches('/'), endpoint);
    if !query.is_empty() {
        url.push('?');
        url.push_str(&query);
    }
    url
}

fn parse_levels(levels: &[Value]) -> ExchangeClientResult<Vec<OrderBookLevel>> {
    levels
        .iter()
        .map(|level| {
            if let Some(array) = level.as_array() {
                return Ok(OrderBookLevel {
                    price: number_from_str(array.first()).unwrap_or(0.0),
                    quantity: number_from_str(array.get(1)).unwrap_or(0.0),
                });
            }
            Ok(OrderBookLevel {
                price: number_from_str(level.get("p")).unwrap_or(0.0),
                quantity: number_from_str(level.get("s")).unwrap_or(0.0),
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
        tif.map(str::to_ascii_lowercase),
    ) {
        ("market", _) => OrderType::Market,
        (_, Some(tif)) if tif == "ioc" => OrderType::IOC,
        (_, Some(tif)) if tif == "fok" => OrderType::FOK,
        (_, Some(tif)) if tif == "poc" => OrderType::PostOnly,
        _ => OrderType::Limit,
    }
}

fn fallback_rule(exchange: &str, symbol: &str, exchange_symbol: &str) -> SymbolRule {
    let (base, quote) =
        split_compact_symbol(symbol).unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
    SymbolRule {
        exchange: exchange.to_string(),
        market_type: MarketType::Spot,
        internal_symbol: symbol.to_string(),
        exchange_symbol: exchange_symbol.to_string(),
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

fn value_as_f64(value: &Value) -> Option<f64> {
    value.as_f64().or_else(|| value.as_str()?.parse().ok())
}

fn gateio_timestamp(value: &Value) -> Option<DateTime<Utc>> {
    let raw = value_as_f64(value)?;
    if raw > 1_000_000_000_000.0 {
        DateTime::<Utc>::from_timestamp_millis(raw as i64)
    } else {
        DateTime::<Utc>::from_timestamp_millis((raw * 1000.0) as i64)
    }
}

fn number_from_str(value: Option<&Value>) -> Option<f64> {
    value.and_then(|value| match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    })
}

fn gateio_cancel_all_cancelled_count(value: &Value) -> ExchangeClientResult<usize> {
    let items = value
        .as_array()
        .ok_or_else(|| parser_error("cancel-all response is not an array", value))?;
    Ok(items
        .iter()
        .filter(|item| {
            let message_failed = item
                .get("message")
                .and_then(Value::as_str)
                .is_some_and(|message| !message.trim().is_empty());
            let label_failed = item
                .get("label")
                .and_then(Value::as_str)
                .is_some_and(|label| !label.trim().is_empty());
            !message_failed && !label_failed
        })
        .count())
}

fn first_timestamp(value: &Value, keys: &[&str]) -> Option<DateTime<Utc>> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(value_as_i64))
        .and_then(|ts| {
            if ts > 10_000_000_000 {
                DateTime::<Utc>::from_timestamp_millis(ts)
            } else {
                DateTime::<Utc>::from_timestamp(ts, 0)
            }
        })
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

fn parser_error(message: &str, value: &Value) -> ExchangeClientError {
    ExchangeClientError::Classified(ExchangeError {
        exchange: "gateio".to_string(),
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
    struct SeenGateIoRequest {
        method: String,
        path: String,
        query: HashMap<String, String>,
        headers: HashMap<String, String>,
        body: Option<Value>,
    }

    async fn spawn_gateio_rest_server(
        responses: Vec<Value>,
    ) -> (String, Arc<Mutex<Vec<SeenGateIoRequest>>>) {
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
                let request = parse_seen_gateio_request(&request_text);
                seen_requests.lock().unwrap().push(request);
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

    fn parse_seen_gateio_request(request_text: &str) -> SeenGateIoRequest {
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

        SeenGateIoRequest {
            method,
            path: path.to_string(),
            query,
            headers,
            body,
        }
    }

    fn assert_signed_gateio_request(request: &SeenGateIoRequest, method: &str, path: &str) {
        assert_eq!(request.method, method);
        assert_eq!(request.path, path);
        assert_eq!(request.headers.get("key").map(String::as_str), Some("key"));
        assert!(request
            .headers
            .get("timestamp")
            .is_some_and(|value| !value.is_empty()));
        assert!(request
            .headers
            .get("sign")
            .is_some_and(|value| !value.is_empty()));
        assert_eq!(
            request.headers.get("content-type").map(String::as_str),
            Some("application/json")
        );
    }

    #[test]
    fn gateio_symbol_rule_should_project_compact_internal_symbol() {
        let value = json!([{
            "id": "BTC_USDT",
            "base": "BTC",
            "quote": "USDT",
            "precision": 2,
            "amount_precision": 6,
            "min_base_amount": "0.0001",
            "min_quote_amount": "1",
            "trade_status": "tradable"
        }]);
        let rules = parse_symbol_rules(&value).unwrap();
        assert_eq!(rules[0].internal_symbol, "BTCUSDT");
        assert_eq!(rules[0].exchange_symbol, "BTC_USDT");
        assert_eq!(rules[0].min_notional, 1.0);
    }

    #[test]
    fn gateio_capabilities_should_advertise_cancel_all() {
        let client = GateIoSpotClient::new(GateIoSpotConfig::default());
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
    fn gateio_orderbook_should_parse_object_levels() {
        let value = json!({
            "id": 7,
            "current": 1743054548.123,
            "bids": [{"p": "65000.1", "s": "0.12"}],
            "asks": [{"p": "65000.2", "s": "0.09"}]
        });
        let book = parse_orderbook_snapshot(&value, "BTC_USDT", 60_000).unwrap();
        assert_eq!(book.symbol, "BTCUSDT");
        assert_eq!(book.best_bid, Some(65000.1));
        assert_eq!(book.sequence, Some(7));
    }

    #[tokio::test]
    async fn gateio_orderbook_should_validate_depth_before_request() {
        let client = GateIoSpotClient::new(GateIoSpotConfig::default());

        let err = client.get_orderbook("BTCUSDT", 0).await.unwrap_err();

        assert!(matches!(
            err,
            ExchangeClientError::Validation { field: "depth", .. }
        ));
    }

    #[tokio::test]
    async fn gateio_get_order_should_validate_order_id_before_request() {
        let client = GateIoSpotClient::new(GateIoSpotConfig::default());

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
    async fn gateio_spot_client_should_route_common_private_rest_readbacks() {
        let order = json!({
            "id": "1001",
            "currency_pair": "BTC_USDT",
            "text": "t-CLIENT1",
            "side": "buy",
            "type": "limit",
            "time_in_force": "gtc",
            "status": "open",
            "price": "65000",
            "amount": "0.01",
            "left": "0.004",
            "avg_deal_price": "65010",
            "create_time_ms": "1743054548123",
            "update_time_ms": "1743054550000"
        });
        let (base_url, seen) = spawn_gateio_rest_server(vec![
            json!([
                {"currency": "BTC", "available": "0.5", "locked": "0.1"},
                {"currency": "USDT", "available": "123.45", "locked": "1.55"}
            ]),
            order.clone(),
            json!([{"currency_pair": "BTC_USDT", "orders": [order]}]),
            json!({"maker_fee": "0.001", "taker_fee": "0.0015"}),
            json!([{
                "id": "9001",
                "order_id": "1001",
                "currency_pair": "BTC_USDT",
                "text": "t-CLIENT1",
                "side": "buy",
                "price": "65010",
                "amount": "0.006",
                "fee": "0.39",
                "fee_currency": "USDT",
                "role": "taker",
                "create_time_ms": "1743054550000"
            }]),
        ])
        .await;
        let client = GateIoSpotClient::new(GateIoSpotConfig {
            api_key: "key".to_string(),
            api_secret: "secret".to_string(),
            base_url: format!("{base_url}/api/v4"),
            dry_run: false,
            ..GateIoSpotConfig::default()
        });

        let balances = client.get_balances().await.unwrap();
        assert_eq!(balances.exchange, "gateio");
        assert_eq!(balances.market_type, MarketType::Spot);
        assert_eq!(balances.balances.len(), 2);
        assert_eq!(balances.balances[0].asset, "BTC");
        assert_eq!(balances.balances[0].available, 0.5);
        assert_eq!(balances.balances[0].locked_by_exchange, 0.1);

        let order = client.get_order("BTCUSDT", "1001").await.unwrap();
        assert_eq!(order.exchange, "gateio");
        assert_eq!(order.market_type, MarketType::Spot);
        assert_eq!(order.symbol, "BTCUSDT");
        assert_eq!(order.order_id, "1001");
        assert_eq!(order.client_order_id.as_deref(), Some("t-CLIENT1"));
        assert_eq!(order.side, OrderSide::Buy);
        assert_eq!(order.status, OrderStatus::New);
        assert_eq!(order.price, Some(65_000.0));
        assert_eq!(order.quantity, 0.01);
        assert_eq!(order.filled_quantity, 0.006);
        assert_eq!(order.average_price, Some(65_010.0));

        let open_orders = client.get_open_orders(Some("BTCUSDT")).await.unwrap();
        assert_eq!(open_orders.len(), 1);
        assert_eq!(open_orders[0].symbol, "BTCUSDT");
        assert_eq!(open_orders[0].order_id, "1001");
        assert_eq!(open_orders[0].client_order_id.as_deref(), Some("t-CLIENT1"));
        assert_eq!(open_orders[0].side, OrderSide::Buy);
        assert_eq!(open_orders[0].status, OrderStatus::New);
        assert_eq!(open_orders[0].price, Some(65_000.0));
        assert_eq!(open_orders[0].quantity, 0.01);
        assert_eq!(open_orders[0].filled_quantity, 0.006);
        assert_eq!(open_orders[0].average_price, Some(65_010.0));

        let fee_rate = client.get_fee_rate("BTCUSDT").await.unwrap();
        assert_eq!(fee_rate.maker, 0.001);
        assert_eq!(fee_rate.taker, 0.0015);

        let fills = client.get_recent_fills("BTCUSDT").await.unwrap();
        assert_eq!(fills.len(), 1);
        let fill = &fills[0];
        assert_eq!(fill.exchange, "gateio");
        assert_eq!(fill.market_type, MarketType::Spot);
        assert_eq!(fill.symbol, "BTCUSDT");
        assert_eq!(fill.trade_id.as_deref(), Some("9001"));
        assert_eq!(fill.order_id.as_deref(), Some("1001"));
        assert_eq!(fill.client_order_id.as_deref(), Some("t-CLIENT1"));
        assert_eq!(fill.side, OrderSide::Buy);
        assert_eq!(fill.price, 65_010.0);
        assert_eq!(fill.quantity, 0.006);
        assert_eq!(fill.fee_asset.as_deref(), Some("USDT"));
        assert_eq!(fill.fee_amount, Some(0.39));
        assert_eq!(fill.liquidity, LiquidityRole::Taker);

        let requests = seen.lock().unwrap().clone();
        assert_eq!(requests.len(), 5);
        assert_signed_gateio_request(&requests[0], "GET", "/api/v4/spot/accounts");
        assert!(requests[0].query.is_empty());

        assert_signed_gateio_request(&requests[1], "GET", "/api/v4/spot/orders/1001");
        assert_eq!(
            requests[1].query.get("currency_pair").map(String::as_str),
            Some("BTC_USDT")
        );

        assert_signed_gateio_request(&requests[2], "GET", "/api/v4/spot/open_orders");
        assert_eq!(
            requests[2].query.get("currency_pair").map(String::as_str),
            Some("BTC_USDT")
        );

        assert_signed_gateio_request(&requests[3], "GET", "/api/v4/spot/fee");
        assert_eq!(
            requests[3].query.get("currency_pair").map(String::as_str),
            Some("BTC_USDT")
        );

        assert_signed_gateio_request(&requests[4], "GET", "/api/v4/spot/my_trades");
        assert_eq!(
            requests[4].query.get("currency_pair").map(String::as_str),
            Some("BTC_USDT")
        );
    }

    #[tokio::test]
    async fn gateio_spot_client_should_route_order_mutations() {
        let symbol_rule = json!([{
            "id": "HYPE_USDT",
            "base": "HYPE",
            "quote": "USDT",
            "precision": 2,
            "amount_precision": 6,
            "min_base_amount": "0.0001",
            "min_quote_amount": "1",
            "trade_status": "tradable"
        }]);
        let balances = json!([
            {"currency": "HYPE", "available": "100", "locked": "0"},
            {"currency": "USDT", "available": "1000", "locked": "0"}
        ]);
        let (base_url, seen) = spawn_gateio_rest_server(vec![
            symbol_rule.clone(),
            balances.clone(),
            json!({"maker_fee": "0.001", "taker_fee": "0.0015"}),
            json!({
                "id": "2001",
                "currency_pair": "HYPE_USDT",
                "text": "t-LIMIT1",
                "side": "buy",
                "type": "limit",
                "time_in_force": "gtc",
                "status": "open",
                "price": "10",
                "amount": "1.25",
                "left": "1.25",
                "create_time_ms": "1743054548123"
            }),
            symbol_rule,
            balances,
            json!({
                "id": "2002",
                "currency_pair": "HYPE_USDT",
                "text": "t-QUOTE1",
                "side": "buy",
                "type": "market",
                "time_in_force": "ioc",
                "status": "open",
                "price": "0",
                "amount": "25.5",
                "left": "25.5",
                "create_time_ms": "1743054549123"
            }),
            json!({
                "id": "2001",
                "currency_pair": "HYPE_USDT",
                "text": "t-LIMIT1",
                "status": "cancelled"
            }),
            json!([
                {"id": "2003", "currency_pair": "HYPE_USDT", "text": "t-CANCELALL1", "status": "cancelled"},
                {"id": "2004", "currency_pair": "HYPE_USDT", "text": "t-CANCELALL2", "status": "cancelled"}
            ]),
            json!({
                "id": "2005",
                "currency_pair": "HYPE_USDT",
                "text": "t-AMEND1",
                "side": "buy",
                "type": "limit",
                "status": "open",
                "price": "10",
                "amount": "0.5",
                "left": "0.5",
                "create_time_ms": "1743054550123"
            }),
        ])
        .await;
        let client = GateIoSpotClient::new(GateIoSpotConfig {
            api_key: "key".to_string(),
            api_secret: "secret".to_string(),
            base_url: format!("{base_url}/api/v4"),
            dry_run: false,
            ..GateIoSpotConfig::default()
        });

        let placed = client
            .place_order(OrderRequest {
                market_type: MarketType::Spot,
                symbol: "HYPEUSDT".to_string(),
                side: OrderSide::Buy,
                position_side: PositionSide::None,
                order_type: OrderType::Limit,
                time_in_force: Some(TimeInForce::GTC),
                quantity: 1.25,
                price: Some(10.0),
                client_order_id: Some("LIMIT1".to_string()),
                reduce_only: false,
            })
            .await
            .unwrap();
        assert_eq!(placed.exchange, "gateio");
        assert_eq!(placed.market_type, MarketType::Spot);
        assert_eq!(placed.order_id, "2001");
        assert_eq!(placed.client_order_id.as_deref(), Some("t-LIMIT1"));
        assert_eq!(placed.status, OrderStatus::New);

        let quote = client
            .place_quote_market_order(QuoteMarketOrderRequest {
                market_type: MarketType::Spot,
                symbol: "HYPEUSDT".to_string(),
                side: OrderSide::Buy,
                quote_quantity: 25.5,
                client_order_id: Some("QUOTE1".to_string()),
            })
            .await
            .unwrap();
        assert_eq!(quote.order_id, "2002");
        assert_eq!(quote.order_type, OrderType::Market);
        assert_eq!(quote.quantity, 25.5);

        let cancelled = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Spot,
                symbol: "HYPEUSDT".to_string(),
                order_id: Some("2001".to_string()),
                client_order_id: None,
            })
            .await
            .unwrap();
        assert_eq!(cancelled.order_id.as_deref(), Some("2001"));
        assert_eq!(cancelled.client_order_id.as_deref(), Some("t-LIMIT1"));
        assert_eq!(cancelled.status, OrderStatus::Cancelled);

        let cancel_all = client
            .cancel_all_orders(CancelAllOrdersRequest::for_symbol(
                MarketType::Spot,
                "HYPEUSDT",
            ))
            .await
            .unwrap();
        assert_eq!(cancel_all.exchange, "gateio");
        assert_eq!(cancel_all.market_type, MarketType::Spot);
        assert_eq!(cancel_all.symbol.as_deref(), Some("HYPEUSDT"));
        assert_eq!(cancel_all.cancelled_orders, 2);

        let amended = client
            .amend_order(AmendOrderRequest {
                market_type: MarketType::Spot,
                symbol: "HYPEUSDT".to_string(),
                order_id: Some("2005".to_string()),
                client_order_id: None,
                new_client_order_id: None,
                new_quantity: 0.5,
            })
            .await
            .unwrap();
        assert_eq!(amended.order_id, "2005");
        assert_eq!(amended.client_order_id.as_deref(), Some("t-AMEND1"));
        assert_eq!(amended.quantity, 0.5);

        assert!(!client.capabilities().supports_order_list);

        let requests = seen.lock().unwrap().clone();
        assert_eq!(requests.len(), 10);

        assert_eq!(requests[0].method, "GET");
        assert_eq!(requests[0].path, "/api/v4/spot/currency_pairs");
        assert_signed_gateio_request(&requests[1], "GET", "/api/v4/spot/accounts");
        assert_signed_gateio_request(&requests[2], "GET", "/api/v4/spot/fee");
        assert_eq!(
            requests[2].query.get("currency_pair").map(String::as_str),
            Some("HYPE_USDT")
        );

        assert_signed_gateio_request(&requests[3], "POST", "/api/v4/spot/orders");
        let body = requests[3].body.as_ref().unwrap();
        assert_eq!(body["currency_pair"], "HYPE_USDT");
        assert_eq!(body["side"], "buy");
        assert_eq!(body["type"], "limit");
        assert_eq!(body["amount"], "1.25");
        assert_eq!(body["price"], "10");
        assert_eq!(body["time_in_force"], "gtc");
        assert_eq!(body["text"], "t-LIMIT1");

        assert_eq!(requests[4].method, "GET");
        assert_eq!(requests[4].path, "/api/v4/spot/currency_pairs");
        assert_signed_gateio_request(&requests[5], "GET", "/api/v4/spot/accounts");

        assert_signed_gateio_request(&requests[6], "POST", "/api/v4/spot/orders");
        let body = requests[6].body.as_ref().unwrap();
        assert_eq!(body["currency_pair"], "HYPE_USDT");
        assert_eq!(body["side"], "buy");
        assert_eq!(body["type"], "market");
        assert_eq!(body["amount"], "25.5");
        assert_eq!(body["time_in_force"], "ioc");
        assert_eq!(body["text"], "t-QUOTE1");

        assert_signed_gateio_request(&requests[7], "DELETE", "/api/v4/spot/orders/2001");
        assert_eq!(
            requests[7].query.get("currency_pair").map(String::as_str),
            Some("HYPE_USDT")
        );

        assert_signed_gateio_request(&requests[8], "DELETE", "/api/v4/spot/orders");
        assert_eq!(
            requests[8].query.get("currency_pair").map(String::as_str),
            Some("HYPE_USDT")
        );

        assert_signed_gateio_request(&requests[9], "PATCH", "/api/v4/spot/orders/2005");
        let body = requests[9].body.as_ref().unwrap();
        assert_eq!(body["currency_pair"], "HYPE_USDT");
        assert_eq!(body["account"], "spot");
        assert_eq!(body["amount"], "0.5");
    }

    #[test]
    fn gateio_error_mapping_should_classify_common_errors() {
        assert_eq!(
            classify_gateio_error(Some("BALANCE_NOT_ENOUGH"), "balance"),
            ExchangeErrorClass::InsufficientBalance
        );
        assert_eq!(
            classify_gateio_error(Some("INVALID_PARAM_VALUE"), "invalid size"),
            ExchangeErrorClass::InvalidPrecision
        );
    }

    #[test]
    fn gateio_cancel_all_should_count_successful_items() {
        let value = json!([
            {"id":"1","currency_pair":"BTC_USDT","status":"cancelled","message":""},
            {"id":"2","currency_pair":"BTC_USDT","status":"cancelled"},
            {"id":"3","currency_pair":"BTC_USDT","label":"ORDER_NOT_FOUND","message":"not found"}
        ]);
        assert_eq!(gateio_cancel_all_cancelled_count(&value).unwrap(), 2);
    }

    #[test]
    fn gateio_order_body_should_prefix_client_text() {
        let mut request = OrderRequest::spot_market_buy("HYPEUSDT", 1.0);
        request.order_type = OrderType::Limit;
        request.price = Some(10.0);
        request.client_order_id = Some("ldry-gateio-123".to_string());
        let body = gateio_order_body(&request, "HYPE_USDT").unwrap();
        assert_eq!(body["text"], "t-ldry-gateio-123");
    }

    #[tokio::test]
    async fn gateio_place_order_should_ack_in_dry_run_without_symbol_rule_readback() {
        let client = GateIoSpotClient::new(GateIoSpotConfig {
            dry_run: true,
            ..GateIoSpotConfig::default()
        });

        let response = client
            .place_order(OrderRequest {
                market_type: MarketType::Spot,
                symbol: "HYPEUSDT".to_string(),
                side: OrderSide::Buy,
                position_side: PositionSide::None,
                order_type: OrderType::Limit,
                time_in_force: Some(TimeInForce::GTC),
                quantity: 1.0,
                price: Some(10.0),
                client_order_id: None,
                reduce_only: false,
            })
            .await
            .unwrap();

        assert_eq!(response.exchange, "gateio");
        assert_eq!(response.market_type, MarketType::Spot);
        assert_eq!(response.symbol, "HYPEUSDT");
        assert_eq!(response.side, OrderSide::Buy);
        assert_eq!(response.order_type, OrderType::Limit);
        assert_eq!(response.price, Some(10.0));
        assert_eq!(response.quantity, 1.0);
        assert_eq!(response.status, OrderStatus::New);
        assert!(response.client_order_id.is_some());
        assert!(response.order_id.starts_with("dry-gateio-"));
    }

    #[test]
    fn gateio_quote_market_order_body_should_use_quote_amount_for_buy() {
        let mut request = QuoteMarketOrderRequest::spot_buy("HYPEUSDT", 25.5);
        request.client_order_id = Some("ldry-gateio-quote-1".to_string());

        let body = gateio_quote_market_order_body(&request, "HYPE_USDT").unwrap();

        assert_eq!(body["currency_pair"], "HYPE_USDT");
        assert_eq!(body["side"], "buy");
        assert_eq!(body["type"], "market");
        assert_eq!(body["amount"], "25.5");
        assert_eq!(body["time_in_force"], "ioc");
        assert_eq!(body["text"], "t-ldry-gateio-quote-1");

        let sell = QuoteMarketOrderRequest {
            side: OrderSide::Sell,
            ..request
        };
        assert!(gateio_quote_market_order_body(&sell, "HYPE_USDT").is_err());
    }

    #[tokio::test]
    async fn gateio_quote_market_order_should_ack_in_dry_run() {
        let client = GateIoSpotClient::new(GateIoSpotConfig {
            dry_run: true,
            ..GateIoSpotConfig::default()
        });

        let response = client
            .place_quote_market_order(QuoteMarketOrderRequest::spot_buy("HYPEUSDT", 25.5))
            .await
            .unwrap();

        assert_eq!(response.exchange, "gateio");
        assert_eq!(response.symbol, "HYPEUSDT");
        assert_eq!(response.side, OrderSide::Buy);
        assert_eq!(response.order_type, OrderType::Market);
        assert_eq!(response.quantity, 25.5);
        assert!(response.client_order_id.is_some());
    }

    #[test]
    fn gateio_amend_order_body_should_use_amount() {
        let request = AmendOrderRequest::reduce_quantity_by_order_id(
            MarketType::Spot,
            "HYPEUSDT",
            "12345",
            0.5,
        );

        let body = gateio_amend_order_body(&request, "HYPE_USDT").unwrap();

        assert_eq!(body["currency_pair"], "HYPE_USDT");
        assert_eq!(body["account"], "spot");
        assert_eq!(body["amount"], "0.5");
        assert!(body.get("price").is_none());

        let client_only = AmendOrderRequest {
            order_id: None,
            client_order_id: Some("ldry-gateio-amend".to_string()),
            ..request.clone()
        };
        assert!(gateio_amend_order_body(&client_only, "HYPE_USDT").is_err());

        let blank_order_id = AmendOrderRequest {
            order_id: Some("   ".to_string()),
            client_order_id: Some("ldry-gateio-amend".to_string()),
            ..request.clone()
        };
        assert!(gateio_amend_order_body(&blank_order_id, "HYPE_USDT").is_err());

        let unsupported = AmendOrderRequest {
            new_client_order_id: Some("ldry-gateio-new".to_string()),
            ..request
        };
        assert!(gateio_amend_order_body(&unsupported, "HYPE_USDT").is_err());
    }

    #[tokio::test]
    async fn gateio_amend_order_should_ack_in_dry_run() {
        let client = GateIoSpotClient::new(GateIoSpotConfig {
            dry_run: true,
            ..GateIoSpotConfig::default()
        });

        let response = client
            .amend_order(AmendOrderRequest::reduce_quantity_by_order_id(
                MarketType::Spot,
                "HYPEUSDT",
                "12345",
                0.5,
            ))
            .await
            .unwrap();

        assert_eq!(response.exchange, "gateio");
        assert_eq!(response.symbol, "HYPEUSDT");
        assert_eq!(response.order_id, "12345");
        assert_eq!(response.quantity, 0.5);

        let response = client
            .amend_order(AmendOrderRequest {
                market_type: MarketType::Spot,
                symbol: "HYPEUSDT".to_string(),
                order_id: Some("12345".to_string()),
                client_order_id: Some("   ".to_string()),
                new_client_order_id: Some("   ".to_string()),
                new_quantity: 0.45,
            })
            .await
            .unwrap();
        assert_eq!(response.order_id, "12345");
        assert_eq!(response.client_order_id, None);

        let unsupported_new_client_id = client
            .amend_order(AmendOrderRequest {
                market_type: MarketType::Spot,
                symbol: "HYPEUSDT".to_string(),
                order_id: Some("12345".to_string()),
                client_order_id: None,
                new_client_order_id: Some("LDRYGATEIONEWCID".to_string()),
                new_quantity: 0.4,
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
                symbol: "HYPEUSDT".to_string(),
                order_id: None,
                client_order_id: Some("bad/id".to_string()),
                new_client_order_id: None,
                new_quantity: 0.4,
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
                symbol: "HYPEUSDT".to_string(),
                order_id: Some("12345".to_string()),
                client_order_id: None,
                new_client_order_id: Some("bad/id".to_string()),
                new_quantity: 0.4,
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
    async fn gateio_cancel_order_should_validate_market_type_in_dry_run() {
        let client = GateIoSpotClient::new(GateIoSpotConfig {
            dry_run: true,
            ..GateIoSpotConfig::default()
        });

        let response = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Spot,
                symbol: "HYPEUSDT".to_string(),
                order_id: Some("12345".to_string()),
                client_order_id: None,
            })
            .await
            .unwrap();
        assert_eq!(response.exchange, "gateio");
        assert_eq!(response.market_type, MarketType::Spot);
        assert_eq!(response.symbol, "HYPEUSDT");
        assert_eq!(response.order_id.as_deref(), Some("12345"));

        let err = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Perpetual,
                symbol: "HYPEUSDT".to_string(),
                order_id: Some("12345".to_string()),
                client_order_id: None,
            })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("only supports spot"));

        let err = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Spot,
                symbol: "HYPEUSDT".to_string(),
                order_id: Some("12345".to_string()),
                client_order_id: Some("bad/id".to_string()),
            })
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            ExchangeClientError::Validation {
                field: "client_order_id",
                ..
            }
        ));

        let err = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Spot,
                symbol: "HYPEUSDT".to_string(),
                order_id: None,
                client_order_id: Some("LDRYGATEIOCANCEL".to_string()),
            })
            .await
            .unwrap_err();
        assert!(
            matches!(err, ExchangeClientError::Unsupported(message) if message.contains("requires exchange order_id"))
        );
    }

    #[tokio::test]
    async fn gateio_cancel_all_orders_should_ack_and_validate_market_type_in_dry_run() {
        let client = GateIoSpotClient::new(GateIoSpotConfig {
            dry_run: true,
            ..GateIoSpotConfig::default()
        });

        let response = client
            .cancel_all_orders(CancelAllOrdersRequest::for_symbol(
                MarketType::Spot,
                "HYPEUSDT",
            ))
            .await
            .unwrap();
        assert_eq!(response.exchange, "gateio");
        assert_eq!(response.market_type, MarketType::Spot);
        assert_eq!(response.symbol.as_deref(), Some("HYPEUSDT"));
        assert_eq!(response.cancelled_orders, 0);

        let err = client
            .cancel_all_orders(CancelAllOrdersRequest::for_symbol(
                MarketType::Perpetual,
                "HYPEUSDT",
            ))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("only supports spot"));
    }

    #[tokio::test]
    async fn gateio_order_list_should_validate_before_unsupported() {
        use crate::exchanges::unified::{
            OrderListConditionalLeg, OrderListLegType, OrderListRequest,
        };

        fn oco_request() -> OrderListRequest {
            OrderListRequest::Oco {
                market_type: MarketType::Spot,
                symbol: "HYPEUSDT".to_string(),
                list_client_order_id: Some("GATEIOOCOLIST1".to_string()),
                side: OrderSide::Sell,
                quantity: 1.0,
                above: OrderListConditionalLeg {
                    order_type: OrderListLegType::LimitMaker,
                    price: Some(11.0),
                    stop_price: None,
                    time_in_force: None,
                    client_order_id: Some("GATEIOABOVE1".to_string()),
                },
                below: OrderListConditionalLeg {
                    order_type: OrderListLegType::StopLossLimit,
                    price: Some(8.0),
                    stop_price: Some(9.0),
                    time_in_force: Some(TimeInForce::GTC),
                    client_order_id: Some("GATEIOBELOW1".to_string()),
                },
            }
        }

        let client = GateIoSpotClient::new(GateIoSpotConfig {
            dry_run: false,
            base_url: "http://127.0.0.1:9".to_string(),
            ..GateIoSpotConfig::default()
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
    fn gateio_spot_private_stream_parser_should_parse_order_fill_balance_and_ignore_control() {
        let order_events = parse_private_stream_message(
            r#"{
                "channel":"spot.orders",
                "event":"update",
                "result":[{
                    "currency_pair":"BTC_USDT",
                    "id":"order-1",
                    "text":"client-1",
                    "side":"buy",
                    "type":"limit",
                    "time_in_force":"gtc",
                    "status":"open",
                    "price":"65000",
                    "amount":"0.02",
                    "left":"0.01",
                    "avg_deal_price":"65010",
                    "create_time_ms":"1700000000000",
                    "update_time_ms":"1700000000100"
                }]
            }"#,
        )
        .unwrap();
        match &order_events[0] {
            UserStreamEvent::Order(order) => {
                assert_eq!(order.exchange, "gateio");
                assert_eq!(order.market_type, MarketType::Spot);
                assert_eq!(order.symbol, "BTCUSDT");
                assert_eq!(order.order_id, "order-1");
                assert_eq!(order.client_order_id.as_deref(), Some("client-1"));
                assert_eq!(order.side, OrderSide::Buy);
                assert_eq!(order.order_type, OrderType::Limit);
                assert_eq!(order.status, OrderStatus::New);
                assert_eq!(order.price, Some(65_000.0));
                assert_eq!(order.quantity, 0.02);
                assert_eq!(order.filled_quantity, 0.01);
                assert_eq!(order.average_price, Some(65_010.0));
            }
            other => panic!("expected Gate.io order event, got {other:?}"),
        }

        let fill_events = parse_private_stream_message(
            r#"{
                "channel":"spot.usertrades",
                "event":"update",
                "result":{
                    "currency_pair":"BTC_USDT",
                    "id":"trade-1",
                    "order_id":"order-1",
                    "text":"client-1",
                    "side":"sell",
                    "price":"66000",
                    "amount":"0.03",
                    "fee_currency":"BTC",
                    "fee":"0.00003",
                    "role":"maker",
                    "create_time_ms":"1700000000200"
                }
            }"#,
        )
        .unwrap();
        match &fill_events[0] {
            UserStreamEvent::Fill(fill) => {
                assert_eq!(fill.exchange, "gateio");
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
                assert_eq!(fill.liquidity, LiquidityRole::Maker);
            }
            other => panic!("expected Gate.io fill event, got {other:?}"),
        }

        let balance_events = parse_private_stream_message(
            r#"{
                "channel":"spot.balances",
                "event":"update",
                "result":{
                    "currency":"USDT",
                    "available":"80",
                    "locked":"20"
                }
            }"#,
        )
        .unwrap();
        match &balance_events[0] {
            UserStreamEvent::Balance(snapshot) => {
                assert_eq!(snapshot.exchange, "gateio");
                assert_eq!(snapshot.market_type, MarketType::Spot);
                assert_eq!(snapshot.balances.len(), 1);
                let balance = &snapshot.balances[0];
                assert_eq!(balance.asset, "USDT");
                assert_eq!(balance.total, 100.0);
                assert_eq!(balance.available, 80.0);
                assert_eq!(balance.locked, 20.0);
            }
            other => panic!("expected Gate.io balance event, got {other:?}"),
        }

        assert!(parse_private_stream_message(
            r#"{"channel":"spot.orders","event":"subscribe","result":{"status":"success"}}"#
        )
        .unwrap()
        .is_empty());
        assert!(parse_private_stream_message(
            r#"{"channel":"spot.orders","event":"update","error":{"message":"bad auth"}}"#
        )
        .unwrap()
        .is_empty());
    }

    #[test]
    fn gateio_symbol_rule_and_order_body_should_expose_post_only() {
        let rule = parse_symbol_rule(&serde_json::json!({
            "id":"HYPE_USDT",
            "base":"HYPE",
            "quote":"USDT",
            "precision":4,
            "amount_precision":2,
            "min_base_amount":"0.1",
            "min_quote_amount":"5",
            "trade_status":"tradable"
        }))
        .unwrap();
        assert!(rule.supported_order_types.contains(&OrderType::PostOnly));
        assert!(rule.supported_order_types.contains(&OrderType::FOK));
        assert!(rule.supported_time_in_force.contains(&TimeInForce::GTX));

        let mut post_only = OrderRequest::spot_market_buy("HYPEUSDT", 1.0);
        post_only.order_type = OrderType::PostOnly;
        post_only.time_in_force = Some(TimeInForce::GTX);
        post_only.price = Some(10.0);
        let body = gateio_order_body(&post_only, "HYPE_USDT").unwrap();
        assert_eq!(body["type"], "limit");
        assert_eq!(body["time_in_force"], "poc");
        assert_eq!(parse_order_type("limit", Some("poc")), OrderType::PostOnly);
    }
}
