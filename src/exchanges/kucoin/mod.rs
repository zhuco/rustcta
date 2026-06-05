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
use crate::exchanges::spot_reservation::BalanceReservationManager;
use crate::exchanges::unified::{
    AssetBalance, BalanceSnapshot, CancelOrderRequest, CancelOrderResponse, ExchangeClient,
    ExchangeClientError, ExchangeClientResult, ExchangeError, ExchangeErrorClass,
    ExchangeHealthStatus, FeeRate, FeeRateSource, LiquidityRole, MarketType, OrderBookLevel,
    OrderBookSnapshot, OrderRequest, OrderResponse, OrderSide, OrderStatus, OrderType,
    PositionSide, SymbolRule, SymbolStatus, TimeInForce, TradeFill, UserStreamEvent,
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
    pub api_key: String,
    pub api_secret: String,
    pub api_passphrase: String,
    #[serde(default = "default_rest_base_url")]
    pub base_url: String,
    #[serde(default = "default_ws_url")]
    pub websocket_url: String,
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
            http: reqwest::Client::new(),
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
    ) -> ExchangeClientResult<Value> {
        self.ensure_credentials()?;
        let path = build_path(endpoint, &params);
        let url = format!("{}{}", self.config.base_url.trim_end_matches('/'), path);
        let timestamp = Utc::now().timestamp_millis().to_string();
        let body = "";
        let prehash = format!("{}{}{}{}", timestamp, method.as_str(), path, body);
        let signature = sign_base64(&self.config.api_secret, &prehash);
        let passphrase = sign_base64(&self.config.api_secret, &self.config.api_passphrase);
        let response = self
            .http
            .request(method, url)
            .header("KC-API-KEY", &self.config.api_key)
            .header("KC-API-SIGN", signature)
            .header("KC-API-TIMESTAMP", timestamp)
            .header("KC-API-PASSPHRASE", passphrase)
            .header("KC-API-KEY-VERSION", "2")
            .timeout(StdDuration::from_millis(self.config.request_timeout_ms))
            .send()
            .await
            .map_err(CoreExchangeError::from)?;
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
}

#[async_trait]
impl ExchangeClient for KuCoinSpotClient {
    fn market_type(&self) -> MarketType {
        MarketType::Spot
    }

    fn exchange_name(&self) -> &str {
        "kucoin"
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
            .send_signed_request(Method::GET, "/api/v1/accounts", HashMap::new())
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

    async fn place_order(&self, _request: OrderRequest) -> ExchangeClientResult<OrderResponse> {
        Err(classified(
            ExchangeErrorClass::UnsupportedCapability,
            Some("readonly".to_string()),
            "KuCoin Spot order placement is disabled in this task",
        ))
    }

    async fn cancel_order(
        &self,
        _request: CancelOrderRequest,
    ) -> ExchangeClientResult<CancelOrderResponse> {
        Err(classified(
            ExchangeErrorClass::UnsupportedCapability,
            Some("readonly".to_string()),
            "KuCoin Spot cancellation is disabled in this task",
        ))
    }

    async fn get_order(&self, symbol: &str, order_id: &str) -> ExchangeClientResult<OrderResponse> {
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            to_exchange_symbol(&self.normalize_symbol(symbol)?),
        );
        params.insert("orderId".to_string(), order_id.to_string());
        let value = self
            .send_signed_request(Method::GET, "/api/v1/orders", params)
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
            .send_signed_request(Method::GET, "/api/v1/orders", params)
            .await?;
        let items = value
            .get("items")
            .or_else(|| value.get("data").and_then(|data| data.get("items")))
            .and_then(Value::as_array)
            .cloned()
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
            .send_signed_request(Method::GET, "/api/v1/trade-fees", params)
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
            .send_signed_request(Method::GET, "/api/v1/fills", params)
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
        Err(ExchangeClientError::Unsupported(
            "KuCoin private user stream is not enabled for this read-only scanner".to_string(),
        ))
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
    let response = reqwest::Client::new()
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
        supported_order_types: vec![OrderType::Market, OrderType::Limit],
        supported_time_in_force: vec![TimeInForce::GTC, TimeInForce::IOC, TimeInForce::FOK],
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

fn parse_side(value: &str) -> OrderSide {
    if value.eq_ignore_ascii_case("sell") {
        OrderSide::Sell
    } else {
        OrderSide::Buy
    }
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
    use super::*;

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
    async fn kucoin_mutation_calls_should_be_unsupported() {
        let client = KuCoinSpotClient::new(KuCoinSpotConfig::default());
        let err = client
            .place_order(OrderRequest::spot_market_buy("BTCUSDT", 0.001))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            ExchangeClientError::Classified(ExchangeError {
                class: ExchangeErrorClass::UnsupportedCapability,
                ..
            })
        ));
    }
}
