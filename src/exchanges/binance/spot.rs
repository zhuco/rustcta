use std::collections::HashMap;
use std::time::Duration as StdDuration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::time::{interval, sleep, timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::core::error::ExchangeError;
use crate::exchanges::client_order_id::{generate_client_order_id, validate_client_order_id};
use crate::exchanges::unified::{
    AssetBalance, BalanceSnapshot, CancelOrderRequest, CancelOrderResponse, ExchangeClient,
    ExchangeClientError, ExchangeClientResult, FeeRate, LiquidityRole, MarketType, OrderBookLevel,
    OrderBookSnapshot, OrderRequest, OrderResponse, OrderSide, OrderStatus, OrderType,
    PositionSide, TimeInForce, TradeFill, UserStreamEvent,
};
use crate::utils::SignatureHelper;

const BINANCE_SPOT_REST_BASE: &str = "https://api.binance.com";
const BINANCE_SPOT_PUBLIC_WS_BASE: &str = "wss://stream.binance.com:9443";
const BINANCE_SPOT_PRIVATE_WS_BASE: &str = "wss://stream.binance.com:9443";
const DEFAULT_RECV_WINDOW_MS: u64 = 5_000;
const DEFAULT_ORDERBOOK_DEPTH: u16 = 5;
const DEFAULT_STALE_AFTER_MS: u64 = 10_000;
const DEFAULT_RECONNECT_DELAY_MS: u64 = 1_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceSpotConfig {
    pub api_key: String,
    pub api_secret: String,
    #[serde(default = "default_rest_base_url")]
    pub rest_base_url: String,
    #[serde(default = "default_public_ws_base_url")]
    pub public_ws_base_url: String,
    #[serde(default = "default_private_ws_base_url")]
    pub private_ws_base_url: String,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default = "default_recv_window_ms")]
    pub recv_window_ms: u64,
    #[serde(default = "default_orderbook_depth")]
    pub orderbook_depth: u16,
    #[serde(default = "default_stale_after_ms")]
    pub stale_after_ms: u64,
    #[serde(default = "default_reconnect_delay_ms")]
    pub reconnect_delay_ms: u64,
}

impl BinanceSpotConfig {
    pub fn from_env() -> Self {
        Self {
            api_key: std::env::var("BINANCE_SPOT_API_KEY").unwrap_or_default(),
            api_secret: std::env::var("BINANCE_SPOT_API_SECRET").unwrap_or_default(),
            rest_base_url: default_rest_base_url(),
            public_ws_base_url: default_public_ws_base_url(),
            private_ws_base_url: default_private_ws_base_url(),
            dry_run: true,
            recv_window_ms: DEFAULT_RECV_WINDOW_MS,
            orderbook_depth: DEFAULT_ORDERBOOK_DEPTH,
            stale_after_ms: DEFAULT_STALE_AFTER_MS,
            reconnect_delay_ms: DEFAULT_RECONNECT_DELAY_MS,
        }
    }
}

impl Default for BinanceSpotConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

#[derive(Clone)]
pub struct BinanceSpotClient {
    config: BinanceSpotConfig,
    http: reqwest::Client,
}

impl BinanceSpotClient {
    pub fn new(config: BinanceSpotConfig) -> Self {
        Self {
            config,
            http: reqwest::Client::new(),
        }
    }

    pub fn dry_run(&self) -> bool {
        self.config.dry_run
    }

    async fn send_public_request(
        &self,
        endpoint: &str,
        params: HashMap<String, String>,
    ) -> ExchangeClientResult<Value> {
        let url = self.build_url(endpoint, &params, None);
        let response = self
            .http
            .get(url)
            .send()
            .await
            .map_err(ExchangeError::from)?;
        parse_response(response).await
    }

    async fn send_api_key_request(
        &self,
        method: Method,
        endpoint: &str,
        params: HashMap<String, String>,
    ) -> ExchangeClientResult<Value> {
        self.ensure_api_key()?;
        let url = self.build_url(endpoint, &params, None);
        let response = self
            .http
            .request(method, url)
            .header("X-MBX-APIKEY", &self.config.api_key)
            .send()
            .await
            .map_err(ExchangeError::from)?;
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
            self.config.recv_window_ms.to_string(),
        );
        let query = SignatureHelper::build_query_string(&params);
        let signature = sign_raw_query(&self.config.api_secret, &query);
        let url = self.build_url(endpoint, &params, Some(&signature));
        let response = self
            .http
            .request(method, url)
            .header("X-MBX-APIKEY", &self.config.api_key)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .send()
            .await
            .map_err(ExchangeError::from)?;
        parse_response(response).await
    }

    fn build_url(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        signature: Option<&str>,
    ) -> String {
        let query = SignatureHelper::build_query_string(params);
        let mut url = format!("{}{}", self.config.rest_base_url, endpoint);
        if !query.is_empty() {
            url.push('?');
            url.push_str(&query);
        }
        if let Some(signature) = signature {
            if query.is_empty() {
                url.push('?');
            } else {
                url.push('&');
            }
            url.push_str("signature=");
            url.push_str(signature);
        }
        url
    }

    async fn create_listen_key(&self) -> ExchangeClientResult<String> {
        let response = self
            .send_api_key_request(Method::POST, "/api/v3/userDataStream", HashMap::new())
            .await?;
        response
            .get("listenKey")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| {
                parse_error(format!(
                    "Binance listenKey response missing key: {response}"
                ))
            })
    }

    async fn keepalive_listen_key(&self, listen_key: &str) -> ExchangeClientResult<()> {
        let mut params = HashMap::new();
        params.insert("listenKey".to_string(), listen_key.to_string());
        self.send_api_key_request(Method::PUT, "/api/v3/userDataStream", params)
            .await?;
        Ok(())
    }

    fn ensure_credentials(&self) -> ExchangeClientResult<()> {
        self.ensure_api_key()?;
        if self.config.api_secret.trim().is_empty() {
            return Err(ExchangeError::AuthError(
                "BINANCE_SPOT_API_SECRET is required for signed Binance Spot REST calls"
                    .to_string(),
            )
            .into());
        }
        Ok(())
    }

    fn ensure_api_key(&self) -> ExchangeClientResult<()> {
        if self.config.api_key.trim().is_empty() {
            return Err(ExchangeError::AuthError(
                "BINANCE_SPOT_API_KEY is required for Binance Spot private calls".to_string(),
            )
            .into());
        }
        Ok(())
    }

    fn dry_run_order_response(&self, request: &OrderRequest) -> OrderResponse {
        OrderResponse {
            exchange: "binance".to_string(),
            market_type: MarketType::Spot,
            symbol: self
                .normalize_symbol(&request.symbol)
                .unwrap_or_else(|_| request.symbol.clone()),
            order_id: format!("dry-binance-spot-{}", Utc::now().timestamp_millis()),
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
impl ExchangeClient for BinanceSpotClient {
    fn market_type(&self) -> MarketType {
        MarketType::Spot
    }

    fn exchange_name(&self) -> &str {
        "binance"
    }

    fn normalize_symbol(&self, symbol: &str) -> ExchangeClientResult<String> {
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

    async fn get_balances(&self) -> ExchangeClientResult<BalanceSnapshot> {
        let response = self
            .send_signed_request(Method::GET, "/api/v3/account", HashMap::new())
            .await?;
        parse_balance_snapshot(&response, "binance")
    }

    async fn get_orderbook(
        &self,
        symbol: &str,
        depth: u16,
    ) -> ExchangeClientResult<OrderBookSnapshot> {
        let symbol = self.normalize_symbol(symbol)?;
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.clone());
        params.insert("limit".to_string(), normalize_depth(depth).to_string());
        let response = self.send_public_request("/api/v3/depth", params).await?;
        parse_orderbook_snapshot(&response, "binance", &symbol)
    }

    async fn place_order(&self, mut request: OrderRequest) -> ExchangeClientResult<OrderResponse> {
        request.validate()?;
        if request.market_type != MarketType::Spot {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: "BinanceSpotClient only supports MarketType::Spot".to_string(),
            });
        }
        let symbol = self.normalize_symbol(&request.symbol)?;
        request.symbol = symbol.clone();
        ensure_client_order_id(&mut request, "binance")?;
        if self.config.dry_run {
            let response = self.dry_run_order_response(&request);
            log::info!(
                "Binance Spot dry-run order state transition symbol={} side={:?} type={:?} status={:?} client_order_id={:?}",
                symbol,
                request.side,
                request.order_type,
                response.status,
                response.client_order_id
            );
            return Ok(response);
        }

        let params = binance_order_params(&request, &symbol)?;
        let response = self
            .send_signed_request(Method::POST, "/api/v3/order", params)
            .await?;
        let parsed = parse_order_response(&response, "binance", MarketType::Spot)?;
        log::info!(
            "Binance Spot order state transition symbol={} order_id={} status={:?} filled={:.8}/{:.8}",
            parsed.symbol,
            parsed.order_id,
            parsed.status,
            parsed.filled_quantity,
            parsed.quantity
        );
        Ok(parsed)
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeClientResult<CancelOrderResponse> {
        request.validate()?;
        if request.market_type != MarketType::Spot {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: "BinanceSpotClient only supports MarketType::Spot".to_string(),
            });
        }
        let symbol = self.normalize_symbol(&request.symbol)?;
        if self.config.dry_run {
            log::info!(
                "Binance Spot dry-run cancel state transition symbol={} order_id={:?} client_order_id={:?} status={:?}",
                symbol,
                request.order_id,
                request.client_order_id,
                OrderStatus::Cancelled
            );
            return Ok(CancelOrderResponse {
                exchange: "binance".to_string(),
                market_type: MarketType::Spot,
                symbol,
                order_id: request.order_id,
                client_order_id: request.client_order_id,
                status: OrderStatus::Cancelled,
                cancelled_at: Utc::now(),
            });
        }

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.clone());
        if let Some(order_id) = &request.order_id {
            params.insert("orderId".to_string(), order_id.clone());
        }
        if let Some(client_order_id) = &request.client_order_id {
            params.insert("origClientOrderId".to_string(), client_order_id.clone());
        }
        let response = self
            .send_signed_request(Method::DELETE, "/api/v3/order", params)
            .await?;
        let status = response
            .get("status")
            .and_then(Value::as_str)
            .map(map_binance_order_status)
            .unwrap_or(OrderStatus::Cancelled);
        log::info!(
            "Binance Spot cancel state transition symbol={} order_id={:?} client_order_id={:?} status={:?}",
            symbol,
            value_as_string(response.get("orderId")).or(request.order_id),
            value_as_string(response.get("clientOrderId")).or(request.client_order_id),
            status
        );
        Ok(CancelOrderResponse {
            exchange: "binance".to_string(),
            market_type: MarketType::Spot,
            symbol,
            order_id: value_as_string(response.get("orderId")),
            client_order_id: value_as_string(response.get("clientOrderId")),
            status,
            cancelled_at: Utc::now(),
        })
    }

    async fn get_order(&self, symbol: &str, order_id: &str) -> ExchangeClientResult<OrderResponse> {
        let symbol = self.normalize_symbol(symbol)?;
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol);
        params.insert("orderId".to_string(), order_id.to_string());
        let response = self
            .send_signed_request(Method::GET, "/api/v3/order", params)
            .await?;
        parse_order_response(&response, "binance", MarketType::Spot)
    }

    async fn get_open_orders(
        &self,
        symbol: Option<&str>,
    ) -> ExchangeClientResult<Vec<OrderResponse>> {
        let mut params = HashMap::new();
        if let Some(symbol) = symbol {
            params.insert("symbol".to_string(), self.normalize_symbol(symbol)?);
        }
        let response = self
            .send_signed_request(Method::GET, "/api/v3/openOrders", params)
            .await?;
        let orders = response.as_array().ok_or_else(|| {
            parse_error(format!(
                "Binance open orders response is not an array: {response}"
            ))
        })?;
        orders
            .iter()
            .map(|order| parse_order_response(order, "binance", MarketType::Spot))
            .collect()
    }

    async fn get_fee_rate(&self, symbol: &str) -> ExchangeClientResult<FeeRate> {
        let symbol = self.normalize_symbol(symbol)?;
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.clone());
        let response = self
            .send_signed_request(Method::GET, "/sapi/v1/asset/tradeFee", params)
            .await?;
        parse_fee_rate(&response, &symbol)
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
        let normalized_symbols = symbols
            .iter()
            .map(|symbol| self.normalize_symbol(symbol))
            .collect::<ExchangeClientResult<Vec<_>>>()?;
        let client = self.clone();
        let (tx, rx) = mpsc::channel(1024);
        tokio::spawn(async move {
            client.run_orderbook_stream(normalized_symbols, tx).await;
        });
        Ok(rx)
    }

    async fn subscribe_user_stream(&self) -> ExchangeClientResult<mpsc::Receiver<UserStreamEvent>> {
        self.ensure_api_key()?;
        let client = self.clone();
        let (tx, rx) = mpsc::channel(1024);
        tokio::spawn(async move {
            client.run_user_stream(tx).await;
        });
        Ok(rx)
    }
}

impl BinanceSpotClient {
    async fn run_orderbook_stream(self, symbols: Vec<String>, tx: mpsc::Sender<OrderBookSnapshot>) {
        let depth = normalize_depth(self.config.orderbook_depth);
        let streams = symbols
            .iter()
            .map(|symbol| format!("{}@depth{}@100ms", symbol.to_ascii_lowercase(), depth))
            .collect::<Vec<_>>()
            .join("/");
        let url = format!(
            "{}/stream?streams={}",
            self.config.public_ws_base_url, streams
        );
        let stale_after = StdDuration::from_millis(self.config.stale_after_ms);
        let reconnect_delay = StdDuration::from_millis(self.config.reconnect_delay_ms);
        let mut last_sequence: HashMap<String, u64> = HashMap::new();

        loop {
            log::info!(
                "Connecting Binance Spot orderbook stream symbols={} depth={}",
                symbols.join(","),
                depth
            );
            match connect_async(url.as_str()).await {
                Ok((mut ws, _)) => loop {
                    match timeout(stale_after, ws.next()).await {
                        Err(_) => {
                            log::warn!(
                                "Binance Spot orderbook stream stale for {:?}; reconnecting",
                                stale_after
                            );
                            break;
                        }
                        Ok(Some(Ok(Message::Text(text)))) => {
                            match parse_stream_orderbook_message(&text) {
                                Ok(snapshot) => {
                                    if let Some(sequence) = snapshot.sequence {
                                        let previous =
                                            last_sequence.insert(snapshot.symbol.clone(), sequence);
                                        if previous.is_some_and(|prev| sequence <= prev) {
                                            log::warn!(
                                                "Binance Spot non-increasing orderbook sequence symbol={} previous={:?} current={}",
                                                snapshot.symbol,
                                                previous,
                                                sequence
                                            );
                                            continue;
                                        }
                                    }
                                    if tx.send(snapshot).await.is_err() {
                                        return;
                                    }
                                }
                                Err(error) => {
                                    log::warn!("Binance Spot orderbook parse error: {}", error);
                                }
                            }
                        }
                        Ok(Some(Ok(Message::Ping(_)))) => {}
                        Ok(Some(Ok(Message::Close(frame)))) => {
                            log::warn!("Binance Spot orderbook stream closed: {:?}", frame);
                            break;
                        }
                        Ok(Some(Ok(_))) => {}
                        Ok(Some(Err(error))) => {
                            log::warn!("Binance Spot orderbook websocket error: {}", error);
                            break;
                        }
                        Ok(None) => {
                            log::warn!("Binance Spot orderbook websocket ended");
                            break;
                        }
                    }
                },
                Err(error) => {
                    log::warn!("Binance Spot orderbook connect error: {}", error);
                }
            }
            sleep(reconnect_delay).await;
        }
    }

    async fn run_user_stream(self, tx: mpsc::Sender<UserStreamEvent>) {
        let reconnect_delay = StdDuration::from_millis(self.config.reconnect_delay_ms);
        loop {
            let listen_key = match self.create_listen_key().await {
                Ok(key) => key,
                Err(error) => {
                    log::warn!("Binance Spot listenKey creation failed: {}", error);
                    sleep(reconnect_delay).await;
                    continue;
                }
            };
            let url = format!(
                "{}/ws/{}",
                self.config.private_ws_base_url.trim_end_matches('/'),
                listen_key
            );
            log::info!("Connecting Binance Spot user stream");
            match connect_async(url.as_str()).await {
                Ok((mut ws, _)) => {
                    let mut keepalive = interval(StdDuration::from_secs(25 * 60));
                    loop {
                        tokio::select! {
                            _ = keepalive.tick() => {
                                if let Err(error) = self.keepalive_listen_key(&listen_key).await {
                                    log::warn!("Binance Spot listenKey keepalive failed: {}", error);
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
                                            Err(error) => log::warn!("Binance Spot user stream parse error: {}", error),
                                        }
                                    }
                                    Some(Ok(Message::Close(frame))) => {
                                        log::warn!("Binance Spot user stream closed: {:?}", frame);
                                        let _ = tx.send(UserStreamEvent::Disconnected {
                                            reason: Some("websocket close".to_string()),
                                        }).await;
                                        break;
                                    }
                                    Some(Ok(_)) => {}
                                    Some(Err(error)) => {
                                        log::warn!("Binance Spot user stream websocket error: {}", error);
                                        let _ = tx.send(UserStreamEvent::Disconnected {
                                            reason: Some(error.to_string()),
                                        }).await;
                                        break;
                                    }
                                    None => {
                                        log::warn!("Binance Spot user stream ended");
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
                    log::warn!("Binance Spot user stream connect error: {}", error);
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

pub fn sign_raw_query(secret: &str, query: &str) -> String {
    SignatureHelper::binance_signature(secret, query)
}

pub fn map_binance_order_status(status: &str) -> OrderStatus {
    match status.trim().to_ascii_uppercase().as_str() {
        "NEW" => OrderStatus::New,
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "FILLED" => OrderStatus::Filled,
        "CANCELED" | "CANCELLED" => OrderStatus::Cancelled,
        "REJECTED" => OrderStatus::Rejected,
        "EXPIRED" | "EXPIRED_IN_MATCH" => OrderStatus::Expired,
        _ => OrderStatus::Unknown,
    }
}

pub fn parse_balance_snapshot(
    value: &Value,
    exchange: impl Into<String>,
) -> ExchangeClientResult<BalanceSnapshot> {
    let balances = value
        .get("balances")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(format!(
                "Binance account response missing balances: {value}"
            ))
        })?;
    let mut normalized = Vec::new();
    for balance in balances {
        let asset = required_str(balance, "asset")?.to_string();
        let available = number_from_str(balance.get("free")).unwrap_or(0.0);
        let locked = number_from_str(balance.get("locked")).unwrap_or(0.0);
        let total = available + locked;
        if total > 0.0 || available > 0.0 || locked > 0.0 {
            normalized.push(AssetBalance::new(asset, total, available, locked));
        }
    }
    Ok(BalanceSnapshot {
        exchange: exchange.into(),
        market_type: MarketType::Spot,
        balances: normalized,
        timestamp: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    value: &Value,
    exchange: impl Into<String>,
    symbol: &str,
) -> ExchangeClientResult<OrderBookSnapshot> {
    let bids = value
        .get("bids")
        .or_else(|| value.get("b"))
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(format!("Binance orderbook response missing bids: {value}")))?;
    let asks = value
        .get("asks")
        .or_else(|| value.get("a"))
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(format!("Binance orderbook response missing asks: {value}")))?;
    let sequence = value
        .get("lastUpdateId")
        .or_else(|| value.get("u"))
        .and_then(Value::as_u64);
    let exchange_timestamp = value
        .get("E")
        .and_then(Value::as_i64)
        .and_then(timestamp_millis);

    let bids = parse_levels(bids)?;
    let asks = parse_levels(asks)?;
    let received_at = Utc::now();
    Ok(OrderBookSnapshot {
        exchange: exchange.into(),
        market_type: MarketType::Spot,
        symbol: symbol.to_ascii_uppercase(),
        best_bid: bids.first().map(|level| level.price),
        best_ask: asks.first().map(|level| level.price),
        bids,
        asks,
        exchange_timestamp,
        latency_ms: exchange_timestamp
            .map(|ts| received_at.signed_duration_since(ts).num_milliseconds()),
        received_at,
        sequence,
        is_stale: false,
    })
}

pub fn parse_order_response(
    value: &Value,
    exchange: impl Into<String>,
    market_type: MarketType,
) -> ExchangeClientResult<OrderResponse> {
    let symbol = required_str(value, "symbol")
        .or_else(|_| required_str(value, "s"))?
        .to_string();
    let order_id = value_as_string(value.get("orderId"))
        .or_else(|| value_as_string(value.get("i")))
        .ok_or_else(|| parse_error(format!("Binance order response missing orderId: {value}")))?;
    let side = parse_side(required_str(value, "side").or_else(|_| required_str(value, "S"))?)?;
    let order_type = parse_order_type(
        required_str(value, "type")
            .or_else(|_| required_str(value, "o"))
            .unwrap_or("LIMIT"),
        value
            .get("timeInForce")
            .or_else(|| value.get("f"))
            .and_then(Value::as_str),
    );
    let quantity = number_from_str(value.get("origQty").or_else(|| value.get("q"))).unwrap_or(0.0);
    let filled_quantity =
        number_from_str(value.get("executedQty").or_else(|| value.get("z"))).unwrap_or(0.0);
    let price = number_from_str(value.get("price").or_else(|| value.get("p"))).filter(|p| *p > 0.0);
    let average_price = average_price(value, filled_quantity);
    let created_at =
        first_timestamp_millis(value, &["transactTime", "time", "O", "E"]).unwrap_or_else(Utc::now);
    let updated_at = first_timestamp_millis(value, &["updateTime", "T", "E"]);

    Ok(OrderResponse {
        exchange: exchange.into(),
        market_type,
        symbol,
        order_id,
        client_order_id: value_as_string(value.get("clientOrderId").or_else(|| value.get("c"))),
        side,
        position_side: PositionSide::None,
        order_type,
        status: value
            .get("status")
            .or_else(|| value.get("X"))
            .and_then(Value::as_str)
            .map(map_binance_order_status)
            .unwrap_or(OrderStatus::Unknown),
        price,
        quantity,
        filled_quantity,
        average_price,
        created_at,
        updated_at,
    })
}

pub fn parse_execution_report(
    value: &Value,
    exchange: impl Into<String> + Clone,
) -> ExchangeClientResult<(OrderResponse, Option<TradeFill>)> {
    if value.get("e").and_then(Value::as_str) != Some("executionReport") {
        return Err(parse_error(format!(
            "expected Binance executionReport event: {value}"
        )));
    }
    let order = parse_order_response(value, exchange.clone(), MarketType::Spot)?;
    log::info!(
        "Binance Spot user stream order state transition symbol={} order_id={} status={:?} filled={:.8}/{:.8}",
        order.symbol,
        order.order_id,
        order.status,
        order.filled_quantity,
        order.quantity
    );

    let last_fill_quantity = number_from_str(value.get("l")).unwrap_or(0.0);
    if last_fill_quantity <= 0.0 {
        return Ok((order, None));
    }
    let last_fill_price = number_from_str(value.get("L")).unwrap_or(0.0);
    let side = parse_side(required_str(value, "S")?)?;
    let liquidity = value
        .get("m")
        .and_then(Value::as_bool)
        .map(|maker| {
            if maker {
                LiquidityRole::Maker
            } else {
                LiquidityRole::Taker
            }
        })
        .unwrap_or(LiquidityRole::Unknown);
    let fill = TradeFill {
        exchange: exchange.into(),
        market_type: MarketType::Spot,
        symbol: required_str(value, "s")?.to_string(),
        trade_id: value_as_string(value.get("t")).filter(|id| id != "-1"),
        order_id: value_as_string(value.get("i")),
        client_order_id: value_as_string(value.get("c")),
        side,
        price: last_fill_price,
        quantity: last_fill_quantity,
        fee_asset: value_as_string(value.get("N")).filter(|asset| !asset.is_empty()),
        fee_amount: number_from_str(value.get("n")),
        liquidity,
        timestamp: first_timestamp_millis(value, &["T", "E"]).unwrap_or_else(Utc::now),
    };
    Ok((order, Some(fill)))
}

pub fn parse_user_stream_message(text: &str) -> ExchangeClientResult<Vec<UserStreamEvent>> {
    let value: Value = serde_json::from_str(text).map_err(ExchangeError::from)?;
    let payload = value.get("data").unwrap_or(&value);
    match payload.get("e").and_then(Value::as_str) {
        Some("executionReport") => {
            let (order, fill) = parse_execution_report(payload, "binance")?;
            let mut events = vec![UserStreamEvent::Order(order)];
            if let Some(fill) = fill {
                events.push(UserStreamEvent::Fill(fill));
            }
            Ok(events)
        }
        Some("outboundAccountPosition") => Ok(vec![UserStreamEvent::Balance(
            parse_outbound_account_position(payload, "binance")?,
        )]),
        Some("balanceUpdate") => Ok(vec![UserStreamEvent::Balance(parse_balance_update(
            payload, "binance",
        )?)]),
        other => Err(parse_error(format!(
            "unsupported Binance Spot user stream event {:?}: {payload}",
            other
        ))),
    }
}

pub fn parse_stream_orderbook_message(text: &str) -> ExchangeClientResult<OrderBookSnapshot> {
    let value: Value = serde_json::from_str(text).map_err(ExchangeError::from)?;
    let payload = value.get("data").unwrap_or(&value);
    let symbol = payload
        .get("s")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| {
            value
                .get("stream")
                .and_then(Value::as_str)
                .and_then(|stream| stream.split('@').next())
                .map(|symbol| symbol.to_ascii_uppercase())
        })
        .ok_or_else(|| parse_error(format!("Binance orderbook stream missing symbol: {value}")))?;
    parse_orderbook_snapshot(payload, "binance", &symbol)
}

fn parse_outbound_account_position(
    value: &Value,
    exchange: impl Into<String>,
) -> ExchangeClientResult<BalanceSnapshot> {
    let balances = value.get("B").and_then(Value::as_array).ok_or_else(|| {
        parse_error(format!(
            "Binance outboundAccountPosition missing B: {value}"
        ))
    })?;
    let mut normalized = Vec::new();
    for balance in balances {
        let asset = required_str(balance, "a")?.to_string();
        let available = number_from_str(balance.get("f")).unwrap_or(0.0);
        let locked = number_from_str(balance.get("l")).unwrap_or(0.0);
        normalized.push(AssetBalance::new(
            asset,
            available + locked,
            available,
            locked,
        ));
    }
    Ok(BalanceSnapshot {
        exchange: exchange.into(),
        market_type: MarketType::Spot,
        balances: normalized,
        timestamp: first_timestamp_millis(value, &["E", "u"]).unwrap_or_else(Utc::now),
    })
}

fn parse_balance_update(
    value: &Value,
    exchange: impl Into<String>,
) -> ExchangeClientResult<BalanceSnapshot> {
    let asset = required_str(value, "a")?.to_string();
    let delta = number_from_str(value.get("d")).unwrap_or(0.0);
    Ok(BalanceSnapshot {
        exchange: exchange.into(),
        market_type: MarketType::Spot,
        balances: vec![AssetBalance::new(asset, delta, delta, 0.0)],
        timestamp: first_timestamp_millis(value, &["T", "E"]).unwrap_or_else(Utc::now),
    })
}

fn parse_fee_rate(value: &Value, symbol: &str) -> ExchangeClientResult<FeeRate> {
    let item = value
        .as_array()
        .and_then(|items| {
            items.iter().find(|item| {
                item.get("symbol")
                    .and_then(Value::as_str)
                    .map(|candidate| candidate.eq_ignore_ascii_case(symbol))
                    .unwrap_or(false)
            })
        })
        .or_else(|| if value.is_object() { Some(value) } else { None })
        .ok_or_else(|| {
            parse_error(format!(
                "Binance trade fee response missing {symbol}: {value}"
            ))
        })?;
    Ok(FeeRate::new(
        number_from_str(item.get("makerCommission")).unwrap_or(0.0),
        number_from_str(item.get("takerCommission")).unwrap_or(0.0),
        crate::exchanges::unified::FeeRateSource::ExchangeApi,
    ))
}

fn binance_order_params(
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
    params.insert("quantity".to_string(), format_decimal(request.quantity));
    if let Some(client_order_id) = &request.client_order_id {
        params.insert("newClientOrderId".to_string(), client_order_id.clone());
    }
    match request.order_type {
        OrderType::Market => {
            params.insert("type".to_string(), "MARKET".to_string());
        }
        OrderType::Limit => {
            params.insert("type".to_string(), "LIMIT".to_string());
            params.insert(
                "timeInForce".to_string(),
                request
                    .time_in_force
                    .unwrap_or(TimeInForce::GTC)
                    .as_binance(),
            );
            params.insert(
                "price".to_string(),
                format_decimal(required_price(request)?),
            );
        }
        OrderType::PostOnly => {
            params.insert("type".to_string(), "LIMIT_MAKER".to_string());
            params.insert(
                "price".to_string(),
                format_decimal(required_price(request)?),
            );
        }
        OrderType::IOC => {
            params.insert("type".to_string(), "LIMIT".to_string());
            params.insert("timeInForce".to_string(), "IOC".to_string());
            params.insert(
                "price".to_string(),
                format_decimal(required_price(request)?),
            );
        }
        OrderType::FOK => {
            params.insert("type".to_string(), "LIMIT".to_string());
            params.insert("timeInForce".to_string(), "FOK".to_string());
            params.insert(
                "price".to_string(),
                format_decimal(required_price(request)?),
            );
        }
    }
    Ok(params)
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

trait BinanceTimeInForce {
    fn as_binance(self) -> String;
}

impl BinanceTimeInForce for TimeInForce {
    fn as_binance(self) -> String {
        match self {
            TimeInForce::GTC => "GTC",
            TimeInForce::IOC => "IOC",
            TimeInForce::FOK => "FOK",
            TimeInForce::GTX => "GTX",
        }
        .to_string()
    }
}

fn required_price(request: &OrderRequest) -> ExchangeClientResult<f64> {
    request
        .price
        .ok_or_else(|| ExchangeClientError::Validation {
            field: "price",
            reason: "price is required for Binance Spot limit-style orders".to_string(),
        })
}

async fn parse_response(response: reqwest::Response) -> ExchangeClientResult<Value> {
    if response.status().is_success() {
        response
            .json::<Value>()
            .await
            .map_err(ExchangeError::from)
            .map_err(ExchangeClientError::from)
    } else {
        let status = response.status().as_u16() as i32;
        let text = response
            .text()
            .await
            .unwrap_or_else(|_| "unknown Binance Spot error".to_string());
        Err(ExchangeError::ApiError {
            code: status,
            message: text,
        }
        .into())
    }
}

fn parse_levels(levels: &[Value]) -> ExchangeClientResult<Vec<OrderBookLevel>> {
    let mut parsed = Vec::with_capacity(levels.len());
    for level in levels {
        let pair = level.as_array().ok_or_else(|| {
            parse_error(format!("Binance orderbook level is not an array: {level}"))
        })?;
        if pair.len() < 2 {
            return Err(parse_error(format!(
                "Binance orderbook level has fewer than 2 entries: {level}"
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
    match side.to_ascii_uppercase().as_str() {
        "BUY" => Ok(OrderSide::Buy),
        "SELL" => Ok(OrderSide::Sell),
        _ => Err(parse_error(format!("unsupported Binance side: {side}"))),
    }
}

fn parse_order_type(order_type: &str, tif: Option<&str>) -> OrderType {
    match order_type.to_ascii_uppercase().as_str() {
        "MARKET" => OrderType::Market,
        "LIMIT_MAKER" => OrderType::PostOnly,
        "LIMIT" => match tif.map(str::to_ascii_uppercase).as_deref() {
            Some("IOC") => OrderType::IOC,
            Some("FOK") => OrderType::FOK,
            Some("GTX") => OrderType::PostOnly,
            _ => OrderType::Limit,
        },
        _ => OrderType::Limit,
    }
}

fn average_price(value: &Value, filled_quantity: f64) -> Option<f64> {
    if filled_quantity <= 0.0 {
        return None;
    }
    let cumulative_quote =
        number_from_str(value.get("cummulativeQuoteQty").or_else(|| value.get("Z")))?;
    if cumulative_quote > 0.0 {
        Some(cumulative_quote / filled_quantity)
    } else {
        None
    }
}

fn required_str<'a>(value: &'a Value, field: &str) -> ExchangeClientResult<&'a str> {
    value.get(field).and_then(Value::as_str).ok_or_else(|| {
        parse_error(format!(
            "Binance payload missing string field {field}: {value}"
        ))
    })
}

fn number_from_str(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::String(raw) => raw.parse::<f64>().ok(),
        Value::Number(number) => number.as_f64(),
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

fn first_timestamp_millis(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields
        .iter()
        .filter_map(|field| value.get(*field).and_then(Value::as_i64))
        .find_map(timestamp_millis)
}

fn timestamp_millis(timestamp: i64) -> Option<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp_millis(timestamp)
}

fn normalize_depth(depth: u16) -> u16 {
    match depth {
        0..=5 => 5,
        6..=10 => 10,
        _ => 20,
    }
}

fn format_decimal(value: f64) -> String {
    let formatted = format!("{value:.16}");
    formatted
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}

fn parse_error(message: String) -> ExchangeClientError {
    ExchangeError::ParseError(message).into()
}

fn default_rest_base_url() -> String {
    BINANCE_SPOT_REST_BASE.to_string()
}

fn default_public_ws_base_url() -> String {
    BINANCE_SPOT_PUBLIC_WS_BASE.to_string()
}

fn default_private_ws_base_url() -> String {
    BINANCE_SPOT_PRIVATE_WS_BASE.to_string()
}

fn default_recv_window_ms() -> u64 {
    DEFAULT_RECV_WINDOW_MS
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

    use super::*;

    #[test]
    fn binance_rest_request_signing_should_match_documented_example() {
        let query = "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559";
        let signature = sign_raw_query(
            "NhqPtmdSJYVB3mq9F2ZAjgI4VdFOvZyY6XifdlLqbUeX7hY4tH8MGjdPVeQx1ioO",
            query,
        );
        assert_eq!(
            signature,
            "1df63f2e1799dde74ddff3876c338dce38b1cd5da51483e1beffe9c87a8550ba"
        );
    }

    #[test]
    fn binance_order_status_mapping_should_cover_spot_statuses() {
        assert_eq!(map_binance_order_status("NEW"), OrderStatus::New);
        assert_eq!(
            map_binance_order_status("PARTIALLY_FILLED"),
            OrderStatus::PartiallyFilled
        );
        assert_eq!(map_binance_order_status("FILLED"), OrderStatus::Filled);
        assert_eq!(map_binance_order_status("CANCELED"), OrderStatus::Cancelled);
        assert_eq!(map_binance_order_status("REJECTED"), OrderStatus::Rejected);
        assert_eq!(map_binance_order_status("EXPIRED"), OrderStatus::Expired);
        assert_eq!(
            map_binance_order_status("EXPIRED_IN_MATCH"),
            OrderStatus::Expired
        );
        assert_eq!(
            map_binance_order_status("PENDING_CANCEL"),
            OrderStatus::Unknown
        );
    }

    #[test]
    fn binance_balance_parsing_should_normalize_nonzero_assets() {
        let payload = json!({
            "balances": [
                {"asset": "BTC", "free": "0.10000000", "locked": "0.02000000"},
                {"asset": "ETH", "free": "0.00000000", "locked": "0.00000000"},
                {"asset": "USDT", "free": "123.45000000", "locked": "1.55000000"}
            ]
        });
        let snapshot = parse_balance_snapshot(&payload, "binance").unwrap();
        assert_eq!(snapshot.market_type, MarketType::Spot);
        assert_eq!(snapshot.balances.len(), 2);
        assert_eq!(snapshot.balances[0].asset, "BTC");
        assert_eq!(snapshot.balances[0].available, 0.1);
        assert_eq!(snapshot.balances[0].locked, 0.02);
        assert!((snapshot.balances[1].total - 125.0).abs() < f64::EPSILON);
    }

    #[test]
    fn binance_orderbook_parsing_should_capture_levels_and_sequence() {
        let payload = json!({
            "lastUpdateId": 1027024,
            "bids": [["4.00000000", "431.00000000"], ["3.99000000", "12.00000000"]],
            "asks": [["4.00000200", "12.00000000"]]
        });
        let snapshot = parse_orderbook_snapshot(&payload, "binance", "BNBBTC").unwrap();
        assert_eq!(snapshot.symbol, "BNBBTC");
        assert_eq!(snapshot.sequence, Some(1027024));
        assert_eq!(snapshot.bids.len(), 2);
        assert_eq!(snapshot.asks[0].price, 4.000002);
    }

    #[test]
    fn binance_user_stream_execution_report_should_parse_order_and_fill() {
        let payload = json!({
            "e": "executionReport",
            "E": 1499405658658_i64,
            "s": "ETHBTC",
            "c": "mUvoqJxFIILMdfAW5iGSOW",
            "S": "BUY",
            "o": "LIMIT",
            "f": "GTC",
            "q": "1.00000000",
            "p": "0.10264410",
            "X": "PARTIALLY_FILLED",
            "i": 4293153,
            "l": "0.25000000",
            "z": "0.25000000",
            "L": "0.10264410",
            "n": "0.00025000",
            "N": "ETH",
            "T": 1499405658657_i64,
            "t": 12345,
            "m": false,
            "Z": "0.025661025"
        });
        let (order, fill) = parse_execution_report(&payload, "binance").unwrap();
        assert_eq!(order.status, OrderStatus::PartiallyFilled);
        assert_eq!(order.order_id, "4293153");
        assert_eq!(order.filled_quantity, 0.25);
        assert_eq!(order.average_price, Some(0.1026441));
        let fill = fill.unwrap();
        assert_eq!(fill.liquidity, LiquidityRole::Taker);
        assert_eq!(fill.fee_asset.as_deref(), Some("ETH"));
        assert_eq!(fill.trade_id.as_deref(), Some("12345"));
    }

    #[tokio::test]
    async fn binance_spot_client_should_reject_perpetual_orders() {
        let client = BinanceSpotClient::new(BinanceSpotConfig::default());
        let request = OrderRequest {
            market_type: MarketType::Perpetual,
            symbol: "BTCUSDT".to_string(),
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            order_type: OrderType::Market,
            time_in_force: None,
            quantity: 0.01,
            price: None,
            client_order_id: None,
            reduce_only: false,
        };
        assert_eq!(client.market_type(), MarketType::Spot);
        let error = client.place_order(request).await.unwrap_err();
        assert!(matches!(
            error,
            ExchangeClientError::Validation {
                field: "market_type",
                ..
            }
        ));
    }
}
