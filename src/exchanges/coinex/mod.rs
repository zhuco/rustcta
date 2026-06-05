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
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::core::error::ExchangeError as CoreExchangeError;
use crate::exchanges::client_order_id::{generate_client_order_id, validate_client_order_id};
use crate::exchanges::spot_reservation::{BalanceReservation, BalanceReservationManager};
use crate::exchanges::unified::{
    validate_order_against_symbol_rule, AssetBalance, BalanceSnapshot, CancelOrderRequest,
    CancelOrderResponse, ExchangeClient, ExchangeClientError, ExchangeClientResult, ExchangeError,
    ExchangeErrorClass, ExchangeHealthStatus, FeeRate, FeeRateSource, MarketType, OrderBookLevel,
    OrderBookSnapshot, OrderRequest, OrderResponse, OrderSide, OrderStatus, OrderType,
    PositionSide, SymbolRule, SymbolStatus, TimeInForce, TradeFill, UserStreamEvent,
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
            http: reqwest::Client::new(),
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
}

#[async_trait]
impl ExchangeClient for CoinExSpotClient {
    fn market_type(&self) -> MarketType {
        MarketType::Spot
    }

    fn exchange_name(&self) -> &str {
        "coinex"
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
        let rule = self
            .get_symbol_rule(&symbol)
            .await?
            .unwrap_or_else(|| fallback_rule(&symbol));
        validate_order_against_symbol_rule(&request, &rule)?;
        let mut reservation = self.reserve_for_order(&request, &rule).await?;
        if self.config.dry_run {
            return Ok(self.dry_run_order_response(&request, &symbol));
        }

        let body = coinex_order_body(&request, &symbol)?;
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

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeClientResult<CancelOrderResponse> {
        request.validate()?;
        let symbol = self.normalize_symbol(&request.symbol)?;
        if self.config.dry_run {
            return Ok(CancelOrderResponse {
                exchange: "coinex".to_string(),
                market_type: MarketType::Spot,
                symbol,
                order_id: request.order_id,
                client_order_id: request.client_order_id,
                status: OrderStatus::Cancelled,
                cancelled_at: Utc::now(),
            });
        }
        let mut body = serde_json::Map::new();
        body.insert("market".to_string(), Value::String(symbol.clone()));
        if let Some(order_id) = &request.order_id {
            body.insert("order_id".to_string(), Value::String(order_id.clone()));
        }
        if let Some(client_order_id) = &request.client_order_id {
            body.insert(
                "client_id".to_string(),
                Value::String(client_order_id.clone()),
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
            order_id: value_as_string(data.get("order_id")).or(request.order_id),
            client_order_id: value_as_string(data.get("client_id")).or(request.client_order_id),
            status: data
                .get("status")
                .and_then(Value::as_str)
                .map(map_coinex_order_status)
                .unwrap_or(OrderStatus::Cancelled),
            cancelled_at: Utc::now(),
        })
    }

    async fn get_order(&self, symbol: &str, order_id: &str) -> ExchangeClientResult<OrderResponse> {
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
            "params": symbols.iter().map(|symbol| {
                serde_json::json!([symbol, normalize_depth(self.config.orderbook_depth), "0", true])
            }).collect::<Vec<_>>(),
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
            OrderType::IOC,
            OrderType::FOK,
        ],
        supported_time_in_force: vec![TimeInForce::GTC, TimeInForce::IOC, TimeInForce::FOK],
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
    let exchange_timestamp = data
        .get("updated_at")
        .or_else(|| data.get("timestamp"))
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
            .or_else(|| data.get("sequence"))
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
    let params = value.get("params").unwrap_or(&value);
    let symbol = params
        .get("market")
        .and_then(Value::as_str)
        .or_else(|| value.get("market").and_then(Value::as_str))
        .unwrap_or("UNKNOWN");
    parse_orderbook_snapshot(params, symbol, stale_book_ms).map(Some)
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

fn coinex_order_type(
    request: &OrderRequest,
) -> ExchangeClientResult<(&'static str, Option<&'static str>)> {
    match request.order_type {
        OrderType::Market => Ok(("market", None)),
        OrderType::Limit => Ok((
            "limit",
            request.time_in_force.and_then(|tif| match tif {
                TimeInForce::GTC => None,
                TimeInForce::IOC => Some("IOC"),
                TimeInForce::FOK => Some("FOK"),
                TimeInForce::GTX => None,
            }),
        )),
        OrderType::IOC => Ok(("limit", Some("IOC"))),
        OrderType::FOK => Ok(("limit", Some("FOK"))),
        OrderType::PostOnly => Err(ExchangeClientError::Unsupported(
            "CoinEx Spot post-only is not enabled in this adapter".to_string(),
        )),
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
        ("limit", Some(tif)) if tif == "IOC" => OrderType::IOC,
        ("limit", Some(tif)) if tif == "FOK" => OrderType::FOK,
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
            OrderType::IOC,
            OrderType::FOK,
        ],
        supported_time_in_force: vec![TimeInForce::GTC, TimeInForce::IOC, TimeInForce::FOK],
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
    use super::*;

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
    fn coinex_symbol_rule_parsing_should_extract_precision_and_limits() {
        let value = serde_json::json!({"code":0,"data":[{"market":"CUDISUSDT","base_ccy":"CUDIS","quote_ccy":"USDT","price_precision":4,"amount_precision":2,"min_amount":"1","min_notional":"5","is_trading_available":true}]});
        let rules = parse_symbol_rules(&value).unwrap();
        assert_eq!(rules[0].internal_symbol, "CUDISUSDT");
        assert_eq!(rules[0].tick_size, 0.0001);
        assert_eq!(rules[0].step_size, 0.01);
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
