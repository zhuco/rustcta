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

type HmacSha256 = Hmac<Sha256>;

const DEFAULT_REST_BASE_URL: &str = "https://api.bitget.com";
const DEFAULT_WS_URL: &str = "wss://ws.bitget.com/v2/ws/public";
const DEFAULT_STALE_BOOK_MS: u64 = 10_000;
const DEFAULT_RECONNECT_INTERVAL_MS: u64 = 1_000;
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_DEPTH: u16 = 5;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitgetSpotConfig {
    pub api_key: String,
    pub api_secret: String,
    #[serde(default)]
    pub passphrase: String,
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

impl Default for BitgetSpotConfig {
    fn default() -> Self {
        Self {
            api_key: std::env::var("BITGET_API_KEY").unwrap_or_default(),
            api_secret: std::env::var("BITGET_API_SECRET").unwrap_or_default(),
            passphrase: std::env::var("BITGET_API_PASSPHRASE")
                .or_else(|_| std::env::var("BITGET_PASSPHRASE"))
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
pub struct BitgetSpotClient {
    config: BitgetSpotConfig,
    http: reqwest::Client,
    reservations: BalanceReservationManager,
}

impl BitgetSpotClient {
    pub fn new(config: BitgetSpotConfig) -> Self {
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
        generate_client_order_id("bitget", MarketType::Spot, "spot").into_string()
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
        let query = SignatureHelper::build_query_string(&params);
        let request_path = if query.is_empty() {
            endpoint.to_string()
        } else {
            format!("{endpoint}?{query}")
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
            &timestamp,
            method.as_str(),
            &request_path,
            &body_text,
        );
        let mut request = self
            .http
            .request(method, build_url(&self.config.base_url, endpoint, &params))
            .header("ACCESS-KEY", &self.config.api_key)
            .header("ACCESS-SIGN", signature)
            .header("ACCESS-TIMESTAMP", timestamp)
            .header("ACCESS-PASSPHRASE", &self.config.passphrase)
            .header("Content-Type", "application/json")
            .timeout(StdDuration::from_millis(self.config.request_timeout_ms));
        if !body_text.is_empty() {
            request = request.body(body_text);
        }
        parse_response(request.send().await.map_err(CoreExchangeError::from)?).await
    }

    fn ensure_credentials(&self) -> ExchangeClientResult<()> {
        if self.config.api_key.trim().is_empty()
            || self.config.api_secret.trim().is_empty()
            || self.config.passphrase.trim().is_empty()
        {
            return Err(ExchangeClientError::Classified(ExchangeError {
                exchange: "bitget".to_string(),
                class: ExchangeErrorClass::AuthenticationFailed,
                code: None,
                message: "BITGET_API_KEY, BITGET_API_SECRET and BITGET_API_PASSPHRASE are required"
                    .to_string(),
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
                        reason: "Bitget buy reservation requires explicit price".to_string(),
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
            exchange: "bitget".to_string(),
            market_type: MarketType::Spot,
            symbol: symbol.to_string(),
            order_id: format!("dry-bitget-{}", Utc::now().timestamp_millis()),
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
impl ExchangeClient for BitgetSpotClient {
    fn market_type(&self) -> MarketType {
        MarketType::Spot
    }

    fn exchange_name(&self) -> &str {
        "bitget"
    }

    fn normalize_symbol(&self, symbol: &str) -> ExchangeClientResult<String> {
        normalize_bitget_symbol(symbol)
    }

    fn denormalize_symbol(&self, symbol: &str) -> ExchangeClientResult<String> {
        normalize_bitget_symbol(symbol)
    }

    async fn load_symbol_rules(&self) -> ExchangeClientResult<Vec<SymbolRule>> {
        let value = self
            .send_public_request("/api/v2/spot/public/symbols", HashMap::new())
            .await?;
        parse_symbol_rules(&value)
    }

    async fn get_balances(&self) -> ExchangeClientResult<BalanceSnapshot> {
        let value = self
            .send_signed_request(
                Method::GET,
                "/api/v2/spot/account/assets",
                HashMap::new(),
                None,
            )
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
                        .balance_with_reservation("bitget", balance)
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
        params.insert("symbol".to_string(), symbol.clone());
        params.insert("type".to_string(), "step0".to_string());
        params.insert("limit".to_string(), normalize_depth(depth).to_string());
        let value = self
            .send_public_request("/api/v2/spot/market/orderbook", params)
            .await?;
        parse_orderbook_snapshot(&value, &symbol, self.config.stale_book_ms)
    }

    async fn place_order(&self, mut request: OrderRequest) -> ExchangeClientResult<OrderResponse> {
        request.validate()?;
        if request.market_type != MarketType::Spot {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: "BitgetSpotClient only supports spot".to_string(),
            });
        }
        let symbol = self.normalize_symbol(&request.symbol)?;
        request.symbol = symbol.clone();
        if request.client_order_id.is_none() {
            request.client_order_id = Some(Self::generate_client_order_id());
        }
        if let Some(client_order_id) = &request.client_order_id {
            validate_client_order_id("bitget", MarketType::Spot, client_order_id).map_err(
                |error| ExchangeClientError::Validation {
                    field: "client_order_id",
                    reason: error.to_string(),
                },
            )?;
        }
        let rule = self
            .get_symbol_rule(&symbol)
            .await?
            .unwrap_or_else(|| fallback_rule("bitget", &symbol));
        validate_order_against_symbol_rule(&request, &rule)?;
        let mut reservation = self.reserve_for_order(&request, &rule).await?;
        if self.config.dry_run {
            return Ok(self.dry_run_order_response(&request, &symbol));
        }

        let body = bitget_order_body(&request, &symbol)?;
        let response = self
            .send_signed_request(
                Method::POST,
                "/api/v2/spot/trade/place-order",
                HashMap::new(),
                Some(body),
            )
            .await;
        match response {
            Ok(value) => parse_order_response(&value, Some(&request)),
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
                exchange: "bitget".to_string(),
                market_type: MarketType::Spot,
                symbol,
                order_id: request.order_id,
                client_order_id: request.client_order_id,
                status: OrderStatus::Cancelled,
                cancelled_at: Utc::now(),
            });
        }
        let body = json!({
            "symbol": symbol,
            "orderId": request.order_id.clone().unwrap_or_default(),
            "clientOid": request.client_order_id.clone().unwrap_or_default(),
        });
        let value = self
            .send_signed_request(
                Method::POST,
                "/api/v2/spot/trade/cancel-order",
                HashMap::new(),
                Some(body),
            )
            .await?;
        let data = value.get("data").unwrap_or(&value);
        Ok(CancelOrderResponse {
            exchange: "bitget".to_string(),
            market_type: MarketType::Spot,
            symbol,
            order_id: value_as_string(data.get("orderId")).or(request.order_id),
            client_order_id: value_as_string(data.get("clientOid")).or(request.client_order_id),
            status: OrderStatus::Cancelled,
            cancelled_at: Utc::now(),
        })
    }

    async fn get_order(&self, symbol: &str, order_id: &str) -> ExchangeClientResult<OrderResponse> {
        let symbol = self.normalize_symbol(symbol)?;
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol);
        params.insert("orderId".to_string(), order_id.to_string());
        let value = self
            .send_signed_request(Method::GET, "/api/v2/spot/trade/orderInfo", params, None)
            .await?;
        parse_order_response(&value, None)
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
            .send_signed_request(
                Method::GET,
                "/api/v2/spot/trade/unfilled-orders",
                params,
                None,
            )
            .await?;
        let data = value.get("data").unwrap_or(&value);
        data.as_array()
            .ok_or_else(|| parser_error("open orders response is not an array", &value))?
            .iter()
            .map(|item| parse_order_response(item, None))
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
            .send_signed_request(Method::GET, "/api/v2/spot/account/fee-rate", params, None)
            .await?;
        let data = value.get("data").unwrap_or(&value);
        Ok(FeeRate::new(
            number_from_str(data.get("makerFeeRate")).unwrap_or(0.001),
            number_from_str(data.get("takerFeeRate")).unwrap_or(0.001),
            FeeRateSource::ExchangeApi,
        ))
    }

    async fn get_recent_fills(&self, symbol: &str) -> ExchangeClientResult<Vec<TradeFill>> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), self.normalize_symbol(symbol)?);
        let value = self
            .send_signed_request(Method::GET, "/api/v2/spot/trade/fills", params, None)
            .await?;
        parse_fills(&value)
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
            "Bitget Spot private user stream is not implemented; use REST polling fallback"
                .to_string(),
        ))
    }

    async fn health_check(&self) -> ExchangeClientResult<ExchangeHealthStatus> {
        Ok(ExchangeHealthStatus {
            exchange: "bitget".to_string(),
            market_type: MarketType::Spot,
            connected: self
                .send_public_request("/api/v2/spot/public/symbols", HashMap::new())
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

impl BitgetSpotClient {
    async fn run_orderbook_stream(self, symbols: Vec<String>, tx: mpsc::Sender<OrderBookSnapshot>) {
        let stale_after = StdDuration::from_millis(self.config.stale_book_ms);
        let reconnect_delay = StdDuration::from_millis(self.config.reconnect_interval_ms);
        let args = symbols
            .iter()
            .map(|symbol| {
                json!({
                    "instType": "SPOT",
                    "channel": "books5",
                    "instId": symbol,
                })
            })
            .collect::<Vec<_>>();
        let subscribe = json!({"op": "subscribe", "args": args}).to_string();
        loop {
            match connect_async(self.config.websocket_url.as_str()).await {
                Ok((mut ws, _)) => {
                    let _ = ws.send(Message::Text(subscribe.clone())).await;
                    loop {
                        match timeout(stale_after, ws.next()).await {
                            Err(_) => break,
                            Ok(Some(Ok(Message::Text(text)))) => {
                                if self.config.log_raw_messages {
                                    log::debug!("Bitget Spot WS raw={}", text);
                                }
                                match parse_ws_orderbook_message(&text, self.config.stale_book_ms) {
                                    Ok(Some(snapshot)) => {
                                        if tx.send(snapshot).await.is_err() {
                                            return;
                                        }
                                    }
                                    Ok(None) => {}
                                    Err(error) => {
                                        log::warn!("Bitget Spot WS parse error: {}", error)
                                    }
                                }
                            }
                            Ok(Some(Ok(Message::Ping(payload)))) => {
                                let _ = ws.send(Message::Pong(payload)).await;
                            }
                            Ok(Some(Ok(Message::Close(_)))) | Ok(None) => break,
                            Ok(Some(Ok(_))) => {}
                            Ok(Some(Err(error))) => {
                                log::warn!("Bitget Spot websocket error: {}", error);
                                break;
                            }
                        }
                    }
                }
                Err(error) => log::warn!("Bitget Spot websocket connect error: {}", error),
            }
            sleep(reconnect_delay).await;
        }
    }
}

pub fn sign_request(secret: &str, timestamp: &str, method: &str, path: &str, body: &str) -> String {
    let prehash = format!("{timestamp}{method}{path}{body}");
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC supports any key length");
    mac.update(prehash.as_bytes());
    general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}

pub fn normalize_bitget_symbol(symbol: &str) -> ExchangeClientResult<String> {
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

pub fn classify_bitget_error(code: Option<&str>, message: &str) -> ExchangeErrorClass {
    let code = code.unwrap_or_default();
    let msg = message.to_ascii_lowercase();
    match code {
        "00000" => ExchangeErrorClass::Unknown,
        "40014" | "40015" | "401" | "40100" => ExchangeErrorClass::AuthenticationFailed,
        "40034" | "43011" => ExchangeErrorClass::InvalidSymbol,
        "40017" | "43028" | "43035" => ExchangeErrorClass::InvalidPrecision,
        "40725" | "43012" => ExchangeErrorClass::InsufficientBalance,
        "40762" | "43027" => ExchangeErrorClass::MinNotionalViolation,
        "40786" | "43001" => ExchangeErrorClass::OrderNotFound,
        "429" => ExchangeErrorClass::RateLimited,
        _ if msg.contains("insufficient") => ExchangeErrorClass::InsufficientBalance,
        _ if msg.contains("symbol") => ExchangeErrorClass::InvalidSymbol,
        _ if msg.contains("precision") || msg.contains("size") => {
            ExchangeErrorClass::InvalidPrecision
        }
        _ if msg.contains("rate") || msg.contains("too many") => ExchangeErrorClass::RateLimited,
        _ => ExchangeErrorClass::Unknown,
    }
}

pub fn parse_symbol_rules(value: &Value) -> ExchangeClientResult<Vec<SymbolRule>> {
    value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| parser_error("symbols response is not an array", value))?
        .iter()
        .map(parse_symbol_rule)
        .collect()
}

pub fn parse_symbol_rule(value: &Value) -> ExchangeClientResult<SymbolRule> {
    let exchange_symbol = required_str(value, "symbol")?.to_ascii_uppercase();
    let base_asset = value
        .get("baseCoin")
        .or_else(|| value.get("baseCoinName"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_ascii_uppercase();
    let quote_asset = value
        .get("quoteCoin")
        .or_else(|| value.get("quoteCoinName"))
        .and_then(Value::as_str)
        .unwrap_or("USDT")
        .to_ascii_uppercase();
    let price_precision = number_from_str(
        value
            .get("pricePrecision")
            .or_else(|| value.get("pricePlace")),
    )
    .unwrap_or(8.0) as u32;
    let quantity_precision = number_from_str(
        value
            .get("quantityPrecision")
            .or_else(|| value.get("volumePlace")),
    )
    .unwrap_or(8.0) as u32;
    let step_size = number_from_str(
        value
            .get("minTradeAmount")
            .or_else(|| value.get("sizeMultiplier")),
    )
    .unwrap_or_else(|| 10_f64.powi(-(quantity_precision as i32)));
    Ok(SymbolRule {
        exchange: "bitget".to_string(),
        market_type: MarketType::Spot,
        internal_symbol: exchange_symbol.clone(),
        exchange_symbol,
        base_asset: if base_asset.is_empty() {
            split_compact_symbol(
                value
                    .get("symbol")
                    .and_then(Value::as_str)
                    .unwrap_or("UNKNOWN"),
            )
            .map(|pair| pair.0)
            .unwrap_or_else(|| "UNKNOWN".to_string())
        } else {
            base_asset
        },
        quote_asset,
        price_precision,
        quantity_precision,
        tick_size: 10_f64.powi(-(price_precision as i32)),
        step_size,
        min_quantity: number_from_str(
            value
                .get("minTradeAmount")
                .or_else(|| value.get("minTradeNum")),
        )
        .unwrap_or(0.0),
        min_notional: number_from_str(
            value
                .get("minTradeUSDT")
                .or_else(|| value.get("minTradeUSDT")),
        )
        .unwrap_or(0.0),
        max_quantity: number_from_str(
            value
                .get("maxTradeAmount")
                .or_else(|| value.get("maxOrderQty")),
        ),
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
            .or_else(|| value.get("symbolStatus"))
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_ascii_lowercase()
            .as_str()
        {
            "online" | "normal" => SymbolStatus::Trading,
            "offline" | "suspend" => SymbolStatus::Suspended,
            _ => SymbolStatus::Unknown,
        },
        raw_metadata: Some(value.clone()),
    })
}

pub fn parse_balance_snapshot(value: &Value) -> ExchangeClientResult<BalanceSnapshot> {
    let data = value.get("data").unwrap_or(value);
    let mut balances = Vec::new();
    for item in data
        .as_array()
        .ok_or_else(|| parser_error("assets response is not an array", value))?
    {
        let asset = item
            .get("coin")
            .or_else(|| item.get("coinName"))
            .or_else(|| item.get("marginCoin"))
            .and_then(Value::as_str)
            .ok_or_else(|| parser_error("balance missing coin", item))?;
        let available = number_from_str(
            item.get("available")
                .or_else(|| item.get("availableAmount"))
                .or_else(|| item.get("availableBalance")),
        )
        .unwrap_or(0.0);
        let locked =
            number_from_str(item.get("frozen").or_else(|| item.get("locked"))).unwrap_or(0.0);
        let total = number_from_str(item.get("equity").or_else(|| item.get("totalAmount")))
            .unwrap_or(available + locked);
        if total > 0.0 || available > 0.0 || locked > 0.0 {
            balances.push(AssetBalance::new(asset, total, available, locked));
        }
    }
    Ok(BalanceSnapshot {
        exchange: "bitget".to_string(),
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
    let data = data
        .as_array()
        .and_then(|items| items.first())
        .unwrap_or(data);
    let bids = parse_levels(
        data.get("bids")
            .and_then(Value::as_array)
            .ok_or_else(|| parser_error("orderbook missing bids", value))?,
    )?;
    let asks = parse_levels(
        data.get("asks")
            .and_then(Value::as_array)
            .ok_or_else(|| parser_error("orderbook missing asks", value))?,
    )?;
    let exchange_timestamp = data
        .get("ts")
        .or_else(|| value.get("requestTime"))
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    let received_at = Utc::now();
    let latency_ms =
        exchange_timestamp.map(|ts| received_at.signed_duration_since(ts).num_milliseconds());
    Ok(OrderBookSnapshot {
        exchange: "bitget".to_string(),
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
            .get("seq")
            .or_else(|| data.get("checksum"))
            .and_then(value_as_u64),
        is_stale: latency_ms.is_some_and(|latency| latency > stale_book_ms as i64),
    })
}

pub fn parse_ws_orderbook_message(
    text: &str,
    stale_book_ms: u64,
) -> ExchangeClientResult<Option<OrderBookSnapshot>> {
    if text.trim() == "pong" {
        return Ok(None);
    }
    let value: Value = serde_json::from_str(text).map_err(CoreExchangeError::from)?;
    if value.get("event").is_some() || value.get("op").is_some() {
        return Ok(None);
    }
    let symbol = value
        .get("arg")
        .and_then(|arg| arg.get("instId"))
        .and_then(Value::as_str)
        .unwrap_or("UNKNOWN");
    parse_orderbook_snapshot(&value, symbol, stale_book_ms).map(Some)
}

pub fn parse_order_response(
    value: &Value,
    request: Option<&OrderRequest>,
) -> ExchangeClientResult<OrderResponse> {
    let data = value.get("data").unwrap_or(value);
    let data = data
        .as_array()
        .and_then(|items| items.first())
        .unwrap_or(data);
    let symbol = data
        .get("symbol")
        .and_then(Value::as_str)
        .or_else(|| request.map(|request| request.symbol.as_str()))
        .unwrap_or("UNKNOWN")
        .to_ascii_uppercase();
    let quantity = number_from_str(data.get("size").or_else(|| data.get("quantity")))
        .unwrap_or_else(|| request.map(|request| request.quantity).unwrap_or(0.0));
    let filled_quantity =
        number_from_str(data.get("filledSize").or_else(|| data.get("baseVolume"))).unwrap_or(0.0);
    Ok(OrderResponse {
        exchange: "bitget".to_string(),
        market_type: MarketType::Spot,
        symbol,
        order_id: value_as_string(data.get("orderId")).unwrap_or_else(|| "unknown".to_string()),
        client_order_id: value_as_string(data.get("clientOid")),
        side: data
            .get("side")
            .and_then(Value::as_str)
            .map(parse_side)
            .transpose()?
            .or_else(|| request.map(|request| request.side))
            .unwrap_or(OrderSide::Buy),
        position_side: PositionSide::None,
        order_type: parse_order_type(
            data.get("orderType")
                .and_then(Value::as_str)
                .unwrap_or("limit"),
            data.get("force").and_then(Value::as_str),
        ),
        status: data
            .get("status")
            .and_then(Value::as_str)
            .map(map_bitget_order_status)
            .unwrap_or(OrderStatus::New),
        price: number_from_str(data.get("price"))
            .or_else(|| request.and_then(|request| request.price)),
        quantity,
        filled_quantity,
        average_price: number_from_str(data.get("priceAvg").or_else(|| data.get("avgPrice"))),
        created_at: first_timestamp(data, &["cTime", "createTime"]).unwrap_or_else(Utc::now),
        updated_at: first_timestamp(data, &["uTime", "updateTime"]),
    })
}

pub fn parse_fills(value: &Value) -> ExchangeClientResult<Vec<TradeFill>> {
    let data = value.get("data").unwrap_or(value);
    let fills = data
        .get("fillList")
        .unwrap_or(data)
        .as_array()
        .ok_or_else(|| parser_error("fills response is not an array", value))?;
    fills.iter().map(parse_fill).collect()
}

pub fn parse_fill(value: &Value) -> ExchangeClientResult<TradeFill> {
    Ok(TradeFill {
        exchange: "bitget".to_string(),
        market_type: MarketType::Spot,
        symbol: required_str(value, "symbol")?.to_ascii_uppercase(),
        trade_id: value_as_string(value.get("tradeId")),
        order_id: value_as_string(value.get("orderId")),
        client_order_id: value_as_string(value.get("clientOid")),
        side: value
            .get("side")
            .and_then(Value::as_str)
            .map(parse_side)
            .transpose()?
            .unwrap_or(OrderSide::Buy),
        price: number_from_str(value.get("price")).unwrap_or(0.0),
        quantity: number_from_str(value.get("size")).unwrap_or(0.0),
        fee_asset: value_as_string(value.get("feeCcy")),
        fee_amount: number_from_str(value.get("fee")),
        liquidity: match value
            .get("tradeScope")
            .or_else(|| value.get("execType"))
            .and_then(Value::as_str)
            .unwrap_or_default()
        {
            "maker" => LiquidityRole::Maker,
            "taker" => LiquidityRole::Taker,
            _ => LiquidityRole::Unknown,
        },
        timestamp: first_timestamp(value, &["cTime", "uTime"]).unwrap_or_else(Utc::now),
    })
}

pub fn map_bitget_order_status(status: &str) -> OrderStatus {
    match status.to_ascii_lowercase().as_str() {
        "live" | "new" => OrderStatus::New,
        "partially_filled" | "partial-fill" => OrderStatus::PartiallyFilled,
        "filled" | "full-fill" => OrderStatus::Filled,
        "cancelled" | "canceled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}

fn bitget_order_body(request: &OrderRequest, symbol: &str) -> ExchangeClientResult<Value> {
    let mut body = json!({
        "symbol": symbol,
        "side": match request.side { OrderSide::Buy => "buy", OrderSide::Sell => "sell" },
        "orderType": match request.order_type { OrderType::Market => "market", _ => "limit" },
        "quantity": request.quantity.to_string(),
    });
    if let Some(price) = request.price {
        body["price"] = Value::String(price.to_string());
    }
    if let Some(client_order_id) = &request.client_order_id {
        body["clientOid"] = Value::String(client_order_id.clone());
    }
    if let Some(tif) = request.time_in_force {
        body["force"] = Value::String(
            match tif {
                TimeInForce::GTC => "gtc",
                TimeInForce::IOC => "ioc",
                TimeInForce::FOK => "fok",
                TimeInForce::GTX => "post_only",
            }
            .to_string(),
        );
    }
    Ok(body)
}

async fn parse_response(response: reqwest::Response) -> ExchangeClientResult<Value> {
    let status = response.status();
    let value: Value = response.json().await.map_err(CoreExchangeError::from)?;
    let code = value.get("code").and_then(Value::as_str);
    if !status.is_success() || code.is_some_and(|code| code != "00000") {
        let message = value
            .get("msg")
            .or_else(|| value.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("Bitget request failed");
        return Err(ExchangeClientError::Classified(ExchangeError {
            exchange: "bitget".to_string(),
            class: classify_bitget_error(code, message),
            code: code.map(ToOwned::to_owned),
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

fn parse_order_type(order_type: &str, force: Option<&str>) -> OrderType {
    match (
        order_type.to_ascii_lowercase().as_str(),
        force.map(str::to_ascii_lowercase),
    ) {
        ("market", _) => OrderType::Market,
        (_, Some(force)) if force == "ioc" => OrderType::IOC,
        (_, Some(force)) if force == "fok" => OrderType::FOK,
        (_, Some(force)) if force == "post_only" => OrderType::PostOnly,
        _ => OrderType::Limit,
    }
}

fn fallback_rule(exchange: &str, symbol: &str) -> SymbolRule {
    let (base, quote) =
        split_compact_symbol(symbol).unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
    SymbolRule {
        exchange: exchange.to_string(),
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

fn value_as_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
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
        6..=15 => 15,
        _ => 50,
    }
}

fn parser_error(message: &str, value: &Value) -> ExchangeClientError {
    ExchangeClientError::Classified(ExchangeError {
        exchange: "bitget".to_string(),
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
    fn bitget_symbol_rule_should_parse_spot_metadata() {
        let value = json!({
            "code": "00000",
            "data": [{
                "symbol": "BTCUSDT",
                "baseCoin": "BTC",
                "quoteCoin": "USDT",
                "pricePrecision": "2",
                "quantityPrecision": "6",
                "minTradeAmount": "0.0001",
                "minTradeUSDT": "1",
                "status": "online"
            }]
        });
        let rules = parse_symbol_rules(&value).unwrap();
        assert_eq!(rules[0].internal_symbol, "BTCUSDT");
        assert_eq!(rules[0].base_asset, "BTC");
        assert_eq!(rules[0].min_notional, 1.0);
    }

    #[test]
    fn bitget_orderbook_should_parse_wrapped_response() {
        let value = json!({
            "code": "00000",
            "requestTime": 1743054548123_i64,
            "data": {
                "asks": [["65000.2", "0.91"]],
                "bids": [["65000.1", "1.25"]],
                "ts": "1743054548123"
            }
        });
        let book = parse_orderbook_snapshot(&value, "BTCUSDT", 60_000).unwrap();
        assert_eq!(book.best_bid, Some(65000.1));
        assert_eq!(book.best_ask, Some(65000.2));
    }

    #[test]
    fn bitget_error_mapping_should_classify_common_errors() {
        assert_eq!(
            classify_bitget_error(Some("40725"), "insufficient balance"),
            ExchangeErrorClass::InsufficientBalance
        );
        assert_eq!(
            classify_bitget_error(Some("40762"), "size too small"),
            ExchangeErrorClass::MinNotionalViolation
        );
    }
}
