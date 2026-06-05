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
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::core::error::ExchangeError as CoreExchangeError;
use crate::exchanges::client_order_id::{generate_client_order_id, validate_client_order_id};
use crate::exchanges::spot_reservation::{BalanceReservation, BalanceReservationManager};
use crate::exchanges::unified::{
    validate_order_against_symbol_rule, AssetBalance, BalanceSnapshot, CancelOrderRequest,
    CancelOrderResponse, ExchangeClient, ExchangeClientError, ExchangeClientResult, ExchangeError,
    ExchangeErrorClass, ExchangeHealthStatus, FeeRate, FeeRateSource, LiquidityRole, MarketType,
    OrderBookLevel, OrderBookSnapshot, OrderRequest, OrderResponse, OrderSide, OrderStatus,
    OrderType, PositionSide, SymbolRule, SymbolStatus, TimeInForce, TradeFill, UserStreamEvent,
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
            http: reqwest::Client::new(),
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
        let query = SignatureHelper::build_query_string(&params);
        let body_text = body
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(CoreExchangeError::from)?
            .unwrap_or_default();
        let timestamp = Utc::now().timestamp().to_string();
        let signature = sign_request(
            &self.config.api_secret,
            method.as_str(),
            endpoint,
            &query,
            &body_text,
            &timestamp,
        );
        let mut request = self
            .http
            .request(method, build_url(&self.config.base_url, endpoint, &params))
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
}

#[async_trait]
impl ExchangeClient for GateIoSpotClient {
    fn market_type(&self) -> MarketType {
        MarketType::Spot
    }

    fn exchange_name(&self) -> &str {
        "gateio"
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
        let rule = self
            .get_symbol_rule(&request.symbol)
            .await?
            .unwrap_or_else(|| fallback_rule("gateio", &request.symbol, &exchange_symbol));
        validate_order_against_symbol_rule(&request, &rule)?;
        let mut reservation = self.reserve_for_order(&request, &rule).await?;
        if self.config.dry_run {
            return Ok(self.dry_run_order_response(&request, &request.symbol));
        }

        let body = gateio_order_body(&request, &exchange_symbol)?;
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

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeClientResult<CancelOrderResponse> {
        request.validate()?;
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
        let endpoint = format!(
            "/spot/orders/{}",
            request.order_id.as_deref().unwrap_or_default()
        );
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

    async fn get_order(&self, symbol: &str, order_id: &str) -> ExchangeClientResult<OrderResponse> {
        let exchange_symbol = self.denormalize_symbol(symbol)?;
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
        supported_order_types: vec![OrderType::Market, OrderType::Limit, OrderType::IOC],
        supported_time_in_force: vec![TimeInForce::GTC, TimeInForce::IOC, TimeInForce::FOK],
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
        .and_then(value_as_f64)
        .and_then(|secs| DateTime::<Utc>::from_timestamp_millis((secs * 1000.0) as i64));
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
        is_stale: latency_ms.is_some_and(|latency| latency > stale_book_ms as i64),
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
        body["text"] = Value::String(client_order_id.clone());
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
        supported_order_types: vec![OrderType::Market, OrderType::Limit, OrderType::IOC],
        supported_time_in_force: vec![TimeInForce::GTC, TimeInForce::IOC, TimeInForce::FOK],
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
    use super::*;

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
}
