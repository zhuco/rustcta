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
use tokio_tungstenite::tungstenite::Message;

use crate::core::error::ExchangeError;
use crate::core::ws_connect::connect_async;
use crate::exchanges::client_order_id::{generate_client_order_id, validate_client_order_id};
use crate::exchanges::unified::{
    validate_order_lookup_id, validate_orderbook_depth, AmendOrderRequest, AssetBalance,
    BalanceSnapshot, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeClient, ExchangeClientCapabilities, ExchangeClientError,
    ExchangeClientResult, FeeRate, LiquidityRole, MarketType, OrderBookLevel, OrderBookSnapshot,
    OrderListConditionalLeg, OrderListKind, OrderListLegType, OrderListOrderLeg, OrderListRequest,
    OrderListResponse, OrderRequest, OrderResponse, OrderSide, OrderStatus, OrderType,
    PositionSide, QuoteMarketOrderRequest, SymbolRule, SymbolStatus, TimeInForce, TradeFill,
    UserStreamEvent,
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
            http: crate::core::http2_fix::shared_http_client(),
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

    fn dry_run_quote_market_order_response(
        &self,
        request: &QuoteMarketOrderRequest,
    ) -> OrderResponse {
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
            order_type: OrderType::Market,
            status: OrderStatus::New,
            price: None,
            quantity: 0.0,
            filled_quantity: 0.0,
            average_price: None,
            created_at: Utc::now(),
            updated_at: None,
        }
    }

    fn dry_run_amend_order_response(&self, request: &AmendOrderRequest) -> OrderResponse {
        OrderResponse {
            exchange: "binance".to_string(),
            market_type: MarketType::Spot,
            symbol: self
                .normalize_symbol(&request.symbol)
                .unwrap_or_else(|_| request.symbol.clone()),
            order_id: request
                .order_id()
                .map(str::to_string)
                .unwrap_or_else(|| format!("dry-binance-spot-{}", Utc::now().timestamp_millis())),
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

    fn dry_run_order_list_response(&self, request: &OrderListRequest) -> OrderListResponse {
        let symbol = self
            .normalize_symbol(request.symbol())
            .unwrap_or_else(|_| request.symbol().to_string());
        let now = Utc::now();
        let orders = match request {
            OrderListRequest::Oco {
                side,
                quantity,
                above,
                below,
                ..
            } => vec![
                dry_run_order_list_order(&symbol, *side, *quantity, above, now),
                dry_run_order_list_order(&symbol, *side, *quantity, below, now),
            ],
            OrderListRequest::Oto {
                working, pending, ..
            } => vec![
                dry_run_order_list_leg_order(&symbol, working, now),
                dry_run_order_list_leg_order(&symbol, pending, now),
            ],
        };
        OrderListResponse {
            exchange: "binance".to_string(),
            market_type: MarketType::Spot,
            symbol,
            order_list_id: Some(format!("dry-binance-spot-{}", now.timestamp_millis())),
            list_client_order_id: order_list_client_id(request),
            kind: request.kind(),
            list_status_type: Some("EXEC_STARTED".to_string()),
            list_order_status: Some("EXECUTING".to_string()),
            orders,
            created_at: now,
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

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let mut capabilities = ExchangeClientCapabilities::spot(self.exchange_name());
        capabilities.supports_private_user_stream = true;
        capabilities.supports_cancel_all_orders = true;
        capabilities.supports_quote_market_order = true;
        capabilities.supports_amend_order = true;
        capabilities.supports_order_list = true;
        capabilities
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
        parse_balance_snapshot(&response, "binance")
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

    async fn place_quote_market_order(
        &self,
        mut request: QuoteMarketOrderRequest,
    ) -> ExchangeClientResult<OrderResponse> {
        request.validate()?;
        let symbol = self.normalize_symbol(&request.symbol)?;
        request.symbol = symbol.clone();
        ensure_quote_market_client_order_id(&mut request, "binance")?;
        if self.config.dry_run {
            return Ok(self.dry_run_quote_market_order_response(&request));
        }

        let params = binance_quote_market_order_params(&request, &symbol)?;
        let response = self
            .send_signed_request(Method::POST, "/api/v3/order", params)
            .await?;
        parse_order_response(&response, "binance", MarketType::Spot)
    }

    async fn amend_order(
        &self,
        mut request: AmendOrderRequest,
    ) -> ExchangeClientResult<OrderResponse> {
        request.validate()?;
        let symbol = self.normalize_symbol(&request.symbol)?;
        request.symbol = symbol.clone();
        ensure_amend_client_order_id(&request, "binance")?;
        if self.config.dry_run {
            return Ok(self.dry_run_amend_order_response(&request));
        }

        let params = binance_amend_order_params(&request, &symbol)?;
        let response = self
            .send_signed_request(Method::PUT, "/api/v3/order/amend/keepPriority", params)
            .await?;
        parse_amend_order_response(&response, "binance")
    }

    async fn place_order_list(
        &self,
        request: OrderListRequest,
    ) -> ExchangeClientResult<OrderListResponse> {
        request.validate()?;
        let symbol = self.normalize_symbol(request.symbol())?;
        ensure_order_list_client_ids(&request, "binance")?;
        if self.config.dry_run {
            return Ok(self.dry_run_order_list_response(&request));
        }

        let params = binance_order_list_params(&request, &symbol)?;
        let response = self
            .send_signed_request(Method::POST, binance_order_list_path(&request), params)
            .await?;
        parse_order_list_response(&response, "binance")
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
        ensure_cancel_client_order_id(&request, "binance")?;
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
        if let Some(order_id) = request.order_id() {
            params.insert("orderId".to_string(), order_id.to_string());
        }
        if let Some(client_order_id) = request.client_order_id() {
            params.insert("origClientOrderId".to_string(), client_order_id.to_string());
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

    async fn cancel_all_orders(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeClientResult<CancelAllOrdersResponse> {
        if request.market_type != MarketType::Spot {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: "Binance Spot client only supports spot cancel-all requests".to_string(),
            });
        }
        let symbol = self.normalize_symbol(request.validate_symbol_required()?)?;
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.clone());
        let response = self
            .send_signed_request(Method::DELETE, "/api/v3/openOrders", params)
            .await?;
        Ok(CancelAllOrdersResponse {
            exchange: "binance".to_string(),
            market_type: MarketType::Spot,
            symbol: Some(symbol),
            cancelled_orders: binance_cancel_all_cancelled_count(&response)?,
            cancelled_at: Utc::now(),
        })
    }

    async fn get_order(&self, symbol: &str, order_id: &str) -> ExchangeClientResult<OrderResponse> {
        let symbol = self.normalize_symbol(symbol)?;
        let order_id = validate_order_lookup_id(order_id)?;
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
            .send_signed_request(Method::GET, "/api/v3/account/commission", params)
            .await?;
        parse_fee_rate(&response, &symbol)
    }

    async fn get_recent_fills(&self, symbol: &str) -> ExchangeClientResult<Vec<TradeFill>> {
        let symbol = self.normalize_symbol(symbol)?;
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol);
        params.insert("limit".to_string(), "1000".to_string());
        let response = self
            .send_signed_request(Method::GET, "/api/v3/myTrades", params)
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
        "NEW" | "PENDING_NEW" => OrderStatus::New,
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

pub fn parse_symbol_rules(value: &Value) -> ExchangeClientResult<Vec<SymbolRule>> {
    let symbols = value
        .get("symbols")
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(format!("Binance exchangeInfo missing symbols: {value}")))?;
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
    let lot_size_filter = find_filter(&filters, "LOT_SIZE");
    let min_notional_filter =
        find_filter(&filters, "MIN_NOTIONAL").or_else(|| find_filter(&filters, "NOTIONAL"));
    let tick_size = number_from_str(price_filter.and_then(|filter| filter.get("tickSize")))
        .unwrap_or_else(|| {
            value
                .get("pricePrecision")
                .and_then(Value::as_u64)
                .map(|precision| 10_f64.powi(-(precision as i32)))
                .unwrap_or(0.0)
        });
    let step_size = number_from_str(lot_size_filter.and_then(|filter| filter.get("stepSize")))
        .unwrap_or_else(|| {
            value
                .get("baseAssetPrecision")
                .or_else(|| value.get("quantityPrecision"))
                .and_then(Value::as_u64)
                .map(|precision| 10_f64.powi(-(precision as i32)))
                .unwrap_or(0.0)
        });
    let mut supported_order_types = Vec::new();
    let exchange_order_types = value
        .get("orderTypes")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if exchange_order_types
        .iter()
        .any(|order_type| order_type.as_str() == Some("MARKET"))
    {
        supported_order_types.push(OrderType::Market);
    }
    if exchange_order_types
        .iter()
        .any(|order_type| order_type.as_str() == Some("LIMIT"))
    {
        supported_order_types.push(OrderType::Limit);
        supported_order_types.push(OrderType::IOC);
        supported_order_types.push(OrderType::FOK);
    }
    if exchange_order_types
        .iter()
        .any(|order_type| order_type.as_str() == Some("LIMIT_MAKER"))
    {
        supported_order_types.push(OrderType::PostOnly);
    }
    if supported_order_types.is_empty() {
        supported_order_types = vec![
            OrderType::Market,
            OrderType::Limit,
            OrderType::PostOnly,
            OrderType::IOC,
            OrderType::FOK,
        ];
    }
    Ok(SymbolRule {
        exchange: "binance".to_string(),
        market_type: MarketType::Spot,
        internal_symbol: exchange_symbol.clone(),
        exchange_symbol,
        base_asset,
        quote_asset,
        price_precision: value
            .get("pricePrecision")
            .and_then(Value::as_u64)
            .map(|precision| precision as u32)
            .unwrap_or_else(|| precision_from_step(tick_size)),
        quantity_precision: value
            .get("baseAssetPrecision")
            .or_else(|| value.get("quantityPrecision"))
            .and_then(Value::as_u64)
            .map(|precision| precision as u32)
            .unwrap_or_else(|| precision_from_step(step_size)),
        tick_size,
        step_size,
        min_quantity: number_from_str(lot_size_filter.and_then(|filter| filter.get("minQty")))
            .unwrap_or(0.0),
        min_notional: number_from_str(
            min_notional_filter.and_then(|filter| filter.get("minNotional")),
        )
        .unwrap_or(0.0),
        max_quantity: number_from_str(lot_size_filter.and_then(|filter| filter.get("maxQty"))),
        supported_order_types,
        supported_time_in_force: vec![TimeInForce::GTC, TimeInForce::IOC, TimeInForce::FOK],
        status: match value
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("UNKNOWN")
        {
            "TRADING" => SymbolStatus::Trading,
            "BREAK" | "HALT" => SymbolStatus::Suspended,
            "DELISTED" => SymbolStatus::Delisted,
            _ => SymbolStatus::Unknown,
        },
        raw_metadata: Some(value.clone()),
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
    let quantity = number_from_str(
        value
            .get("origQty")
            .or_else(|| value.get("q"))
            .or_else(|| value.get("qty")),
    )
    .unwrap_or(0.0);
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

fn parse_amend_order_response(
    value: &Value,
    exchange: impl Into<String>,
) -> ExchangeClientResult<OrderResponse> {
    let amended = value.get("amendedOrder").unwrap_or(value);
    parse_order_response(amended, exchange, MarketType::Spot)
}

fn parse_order_list_response(
    value: &Value,
    exchange: impl Into<String>,
) -> ExchangeClientResult<OrderListResponse> {
    let exchange_name = exchange.into();
    let kind = match required_str(value, "contingencyType")?
        .to_ascii_uppercase()
        .as_str()
    {
        "OCO" => OrderListKind::Oco,
        "OTO" => OrderListKind::Oto,
        other => {
            return Err(parse_error(format!(
                "unsupported Binance order list contingencyType {other}: {value}"
            )));
        }
    };
    let symbol = required_str(value, "symbol")?.to_string();
    let created_at = first_timestamp_millis(value, &["transactionTime", "transactTime"])
        .unwrap_or_else(Utc::now);
    let orders = value
        .get("orderReports")
        .and_then(Value::as_array)
        .map(|reports| {
            reports
                .iter()
                .filter_map(|item| {
                    parse_order_response(item, exchange_name.clone(), MarketType::Spot).ok()
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    Ok(OrderListResponse {
        exchange: exchange_name,
        market_type: MarketType::Spot,
        symbol,
        order_list_id: value_as_string(value.get("orderListId")),
        list_client_order_id: value_as_string(value.get("listClientOrderId")),
        kind,
        list_status_type: value_as_string(value.get("listStatusType")),
        list_order_status: value_as_string(value.get("listOrderStatus")),
        orders,
        created_at,
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

pub fn parse_fills(value: &Value) -> ExchangeClientResult<Vec<TradeFill>> {
    let fills = value
        .as_array()
        .ok_or_else(|| parse_error(format!("Binance fills response is not an array: {value}")))?;
    fills.iter().map(parse_fill).collect()
}

pub fn parse_fill(value: &Value) -> ExchangeClientResult<TradeFill> {
    let is_buyer = value
        .get("isBuyer")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let is_maker = value
        .get("isMaker")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    Ok(TradeFill {
        exchange: "binance".to_string(),
        market_type: MarketType::Spot,
        symbol: required_str(value, "symbol")?.to_ascii_uppercase(),
        trade_id: value_as_string(value.get("id")),
        order_id: value_as_string(value.get("orderId")),
        client_order_id: value_as_string(value.get("clientOrderId")),
        side: if is_buyer {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        },
        price: number_from_str(value.get("price")).unwrap_or(0.0),
        quantity: number_from_str(value.get("qty")).unwrap_or(0.0),
        fee_asset: value_as_string(value.get("commissionAsset")),
        fee_amount: number_from_str(value.get("commission")),
        liquidity: if is_maker {
            LiquidityRole::Maker
        } else {
            LiquidityRole::Taker
        },
        timestamp: first_timestamp_millis(value, &["time"]).unwrap_or_else(Utc::now),
    })
}

pub fn binance_cancel_all_cancelled_count(value: &Value) -> ExchangeClientResult<usize> {
    let items = value.as_array().ok_or_else(|| {
        parse_error(format!(
            "Binance cancel-all response is not an array: {value}"
        ))
    })?;
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
        parse_binance_commission_rate(item, "maker").unwrap_or(0.0),
        parse_binance_commission_rate(item, "taker").unwrap_or(0.0),
        crate::exchanges::unified::FeeRateSource::ExchangeApi,
    ))
}

fn parse_binance_commission_rate(item: &Value, field: &str) -> Option<f64> {
    let legacy_field = match field {
        "maker" => "makerCommission",
        "taker" => "takerCommission",
        _ => field,
    };
    item.get("standardCommission")
        .and_then(|commission| number_from_str(commission.get(field)))
        .or_else(|| {
            item.get("commissionRates")
                .and_then(|commission| number_from_str(commission.get(field)))
        })
        .or_else(|| number_from_str(item.get(legacy_field)))
}

fn binance_order_params(
    request: &OrderRequest,
    symbol: &str,
) -> ExchangeClientResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    params.insert("symbol".to_string(), symbol.to_string());
    params.insert(
        "side".to_string(),
        binance_side_param(request.side).to_string(),
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

fn binance_quote_market_order_params(
    request: &QuoteMarketOrderRequest,
    symbol: &str,
) -> ExchangeClientResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    params.insert("symbol".to_string(), symbol.to_string());
    params.insert(
        "side".to_string(),
        binance_side_param(request.side).to_string(),
    );
    params.insert("type".to_string(), "MARKET".to_string());
    params.insert(
        "quoteOrderQty".to_string(),
        format_decimal(request.quote_quantity),
    );
    if let Some(client_order_id) = &request.client_order_id {
        params.insert("newClientOrderId".to_string(), client_order_id.clone());
    }
    Ok(params)
}

fn binance_amend_order_params(
    request: &AmendOrderRequest,
    symbol: &str,
) -> ExchangeClientResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    params.insert("symbol".to_string(), symbol.to_string());
    params.insert("newQty".to_string(), format_decimal(request.new_quantity));
    if let Some(order_id) = request.order_id() {
        params.insert("orderId".to_string(), order_id.to_string());
    }
    if let Some(client_order_id) = request.client_order_id() {
        params.insert("origClientOrderId".to_string(), client_order_id.to_string());
    }
    if let Some(new_client_order_id) = request.new_client_order_id() {
        params.insert(
            "newClientOrderId".to_string(),
            new_client_order_id.to_string(),
        );
    }
    Ok(params)
}

fn binance_order_list_path(request: &OrderListRequest) -> &'static str {
    match request {
        OrderListRequest::Oco { .. } => "/api/v3/orderList/oco",
        OrderListRequest::Oto { .. } => "/api/v3/orderList/oto",
    }
}

fn binance_order_list_params(
    request: &OrderListRequest,
    symbol: &str,
) -> ExchangeClientResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    params.insert("symbol".to_string(), symbol.to_string());
    params.insert("newOrderRespType".to_string(), "RESULT".to_string());
    match request {
        OrderListRequest::Oco {
            list_client_order_id,
            side,
            quantity,
            above,
            below,
            ..
        } => {
            set_optional_param(&mut params, "listClientOrderId", list_client_order_id);
            params.insert("side".to_string(), binance_side_param(*side).to_string());
            params.insert("quantity".to_string(), format_decimal(*quantity));
            set_binance_conditional_leg_params(&mut params, "above", above);
            set_binance_conditional_leg_params(&mut params, "below", below);
        }
        OrderListRequest::Oto {
            list_client_order_id,
            working,
            pending,
            ..
        } => {
            set_optional_param(&mut params, "listClientOrderId", list_client_order_id);
            set_binance_order_list_leg_params(&mut params, "working", working);
            set_binance_order_list_leg_params(&mut params, "pending", pending);
        }
    }
    Ok(params)
}

fn set_binance_conditional_leg_params(
    params: &mut HashMap<String, String>,
    prefix: &str,
    leg: &OrderListConditionalLeg,
) {
    params.insert(
        format!("{prefix}Type"),
        binance_order_list_leg_type(leg.order_type).to_string(),
    );
    set_optional_param(
        params,
        &format!("{prefix}ClientOrderId"),
        &leg.client_order_id,
    );
    set_optional_decimal_param(params, &format!("{prefix}Price"), leg.price);
    set_optional_decimal_param(params, &format!("{prefix}StopPrice"), leg.stop_price);
    if let Some(time_in_force) = leg.time_in_force {
        params.insert(format!("{prefix}TimeInForce"), time_in_force.as_binance());
    }
}

fn set_binance_order_list_leg_params(
    params: &mut HashMap<String, String>,
    prefix: &str,
    leg: &OrderListOrderLeg,
) {
    params.insert(
        format!("{prefix}Type"),
        binance_order_list_leg_type(leg.order_type).to_string(),
    );
    params.insert(
        format!("{prefix}Side"),
        binance_side_param(leg.side).to_string(),
    );
    params.insert(format!("{prefix}Quantity"), format_decimal(leg.quantity));
    set_optional_param(
        params,
        &format!("{prefix}ClientOrderId"),
        &leg.client_order_id,
    );
    set_optional_decimal_param(params, &format!("{prefix}Price"), leg.price);
    set_optional_decimal_param(params, &format!("{prefix}StopPrice"), leg.stop_price);
    if let Some(time_in_force) = leg.time_in_force {
        params.insert(format!("{prefix}TimeInForce"), time_in_force.as_binance());
    }
}

fn set_optional_param(params: &mut HashMap<String, String>, key: &str, value: &Option<String>) {
    if let Some(value) = value {
        params.insert(key.to_string(), value.clone());
    }
}

fn set_optional_decimal_param(params: &mut HashMap<String, String>, key: &str, value: Option<f64>) {
    if let Some(value) = value {
        params.insert(key.to_string(), format_decimal(value));
    }
}

fn binance_order_list_leg_type(order_type: OrderListLegType) -> &'static str {
    match order_type {
        OrderListLegType::Market => "MARKET",
        OrderListLegType::Limit => "LIMIT",
        OrderListLegType::LimitMaker => "LIMIT_MAKER",
        OrderListLegType::StopLoss => "STOP_LOSS",
        OrderListLegType::StopLossLimit => "STOP_LOSS_LIMIT",
        OrderListLegType::TakeProfit => "TAKE_PROFIT",
        OrderListLegType::TakeProfitLimit => "TAKE_PROFIT_LIMIT",
    }
}

fn binance_side_param(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    }
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
    validate_optional_binance_client_order_id(
        exchange,
        request.market_type,
        request.client_order_id(),
    )
}

fn ensure_order_list_client_ids(
    request: &OrderListRequest,
    exchange: &str,
) -> ExchangeClientResult<()> {
    let mut client_ids = Vec::new();
    match request {
        OrderListRequest::Oco {
            list_client_order_id,
            above,
            below,
            ..
        } => {
            client_ids.push(list_client_order_id.as_deref());
            client_ids.push(above.client_order_id.as_deref());
            client_ids.push(below.client_order_id.as_deref());
        }
        OrderListRequest::Oto {
            list_client_order_id,
            working,
            pending,
            ..
        } => {
            client_ids.push(list_client_order_id.as_deref());
            client_ids.push(working.client_order_id.as_deref());
            client_ids.push(pending.client_order_id.as_deref());
        }
    }
    for client_order_id in client_ids.into_iter().flatten() {
        validate_client_order_id(exchange, MarketType::Spot, client_order_id).map_err(|error| {
            ExchangeClientError::Validation {
                field: "client_order_id",
                reason: error.to_string(),
            }
        })?;
    }
    Ok(())
}

fn validate_optional_binance_client_order_id(
    exchange: &str,
    market_type: MarketType,
    client_order_id: Option<&str>,
) -> ExchangeClientResult<()> {
    if let Some(client_order_id) = client_order_id {
        validate_client_order_id(exchange, market_type, client_order_id).map_err(|error| {
            ExchangeClientError::Validation {
                field: "client_order_id",
                reason: error.to_string(),
            }
        })?;
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

fn order_type_from_order_list_leg(order_type: OrderListLegType) -> OrderType {
    match order_type {
        OrderListLegType::Market => OrderType::Market,
        OrderListLegType::LimitMaker => OrderType::PostOnly,
        OrderListLegType::Limit
        | OrderListLegType::StopLoss
        | OrderListLegType::StopLossLimit
        | OrderListLegType::TakeProfit
        | OrderListLegType::TakeProfitLimit => OrderType::Limit,
    }
}

fn order_list_client_id(request: &OrderListRequest) -> Option<String> {
    match request {
        OrderListRequest::Oco {
            list_client_order_id,
            ..
        }
        | OrderListRequest::Oto {
            list_client_order_id,
            ..
        } => list_client_order_id.clone(),
    }
}

fn dry_run_order_list_order(
    symbol: &str,
    side: OrderSide,
    quantity: f64,
    leg: &OrderListConditionalLeg,
    now: DateTime<Utc>,
) -> OrderResponse {
    OrderResponse {
        exchange: "binance".to_string(),
        market_type: MarketType::Spot,
        symbol: symbol.to_string(),
        order_id: format!("dry-binance-spot-{}", now.timestamp_millis()),
        client_order_id: leg.client_order_id.clone(),
        side,
        position_side: PositionSide::None,
        order_type: order_type_from_order_list_leg(leg.order_type),
        status: OrderStatus::New,
        price: leg.price,
        quantity,
        filled_quantity: 0.0,
        average_price: None,
        created_at: now,
        updated_at: None,
    }
}

fn dry_run_order_list_leg_order(
    symbol: &str,
    leg: &OrderListOrderLeg,
    now: DateTime<Utc>,
) -> OrderResponse {
    OrderResponse {
        exchange: "binance".to_string(),
        market_type: MarketType::Spot,
        symbol: symbol.to_string(),
        order_id: format!("dry-binance-spot-{}", now.timestamp_millis()),
        client_order_id: leg.client_order_id.clone(),
        side: leg.side,
        position_side: PositionSide::None,
        order_type: order_type_from_order_list_leg(leg.order_type),
        status: OrderStatus::New,
        price: leg.price,
        quantity: leg.quantity,
        filled_quantity: 0.0,
        average_price: None,
        created_at: now,
        updated_at: None,
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

fn find_filter<'a>(filters: &'a [Value], filter_type: &str) -> Option<&'a Value> {
    filters.iter().find(|filter| {
        filter
            .get("filterType")
            .and_then(Value::as_str)
            .map(|candidate| candidate == filter_type)
            .unwrap_or(false)
    })
}

fn precision_from_step(step: f64) -> u32 {
    if !step.is_finite() || step <= 0.0 {
        return 0;
    }
    let text = format!("{step:.16}");
    text.trim_end_matches('0')
        .split('.')
        .nth(1)
        .map(|fraction| fraction.len() as u32)
        .unwrap_or(0)
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
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    #[derive(Debug, Clone)]
    struct SeenSpotRequest {
        method: String,
        path: String,
        query: HashMap<String, String>,
        headers: Vec<(String, String)>,
    }

    impl SeenSpotRequest {
        fn header(&self, name: &str) -> Option<&str> {
            self.headers
                .iter()
                .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
                .map(|(_, value)| value.as_str())
        }
    }

    async fn spawn_spot_rest_server(
        responses: Vec<Value>,
    ) -> (String, Arc<Mutex<Vec<SeenSpotRequest>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_for_task = Arc::clone(&seen);
        tokio::spawn(async move {
            for response in responses {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut buffer = Vec::new();
                let mut chunk = [0_u8; 1024];
                loop {
                    let read = stream.read(&mut chunk).await.unwrap();
                    if read == 0 {
                        break;
                    }
                    buffer.extend_from_slice(&chunk[..read]);
                    if buffer.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                let raw = String::from_utf8_lossy(&buffer);
                seen_for_task
                    .lock()
                    .unwrap()
                    .push(parse_seen_spot_request(&raw));
                let body = response.to_string();
                let reply = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream.write_all(reply.as_bytes()).await.unwrap();
            }
        });
        (format!("http://{}", address), seen)
    }

    fn parse_seen_spot_request(raw: &str) -> SeenSpotRequest {
        let mut lines = raw.split("\r\n");
        let request_line = lines.next().unwrap_or_default();
        let mut request_parts = request_line.split_whitespace();
        let method = request_parts.next().unwrap_or_default().to_string();
        let target = request_parts.next().unwrap_or_default();
        let (path, query_text) = target.split_once('?').unwrap_or((target, ""));
        let query = query_text
            .split('&')
            .filter(|pair| !pair.is_empty())
            .filter_map(|pair| {
                let (key, value) = pair.split_once('=')?;
                Some((key.to_string(), value.to_string()))
            })
            .collect::<HashMap<_, _>>();
        let headers = lines
            .take_while(|line| !line.is_empty())
            .filter_map(|line| {
                let (name, value) = line.split_once(':')?;
                Some((name.trim().to_string(), value.trim().to_string()))
            })
            .collect::<Vec<_>>();
        SeenSpotRequest {
            method,
            path: path.to_string(),
            query,
            headers,
        }
    }

    fn assert_signed_spot_request(request: &SeenSpotRequest) {
        assert_eq!(request.header("X-MBX-APIKEY"), Some("key"));
        assert!(request.query.contains_key("timestamp"));
        assert_eq!(
            request.query.get("recvWindow").map(String::as_str),
            Some("5000")
        );
        assert!(request.query.contains_key("signature"));
    }

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
    fn binance_spot_capabilities_should_match_implemented_streams() {
        let client = BinanceSpotClient::new(BinanceSpotConfig::default());
        let capabilities = client.capabilities();
        assert_eq!(capabilities.market_type, MarketType::Spot);
        assert!(capabilities.supports_public_ws);
        assert!(capabilities.supports_private_user_stream);
        assert!(capabilities.supports_open_orders);
        assert!(capabilities.supports_cancel_all_orders);
        assert!(capabilities.supports_quote_market_order);
        assert!(capabilities.supports_amend_order);
        assert!(capabilities.supports_order_list);
        assert!(capabilities.supports_fee_api);
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

    #[tokio::test]
    async fn binance_orderbook_should_validate_depth_before_request() {
        let client = BinanceSpotClient::new(BinanceSpotConfig::default());

        let err = client.get_orderbook("BTCUSDT", 0).await.unwrap_err();

        assert!(matches!(
            err,
            ExchangeClientError::Validation { field: "depth", .. }
        ));
    }

    #[test]
    fn binance_exchange_info_should_parse_symbol_rules() {
        let payload = json!({
            "symbols": [{
                "symbol": "BTCUSDT",
                "status": "TRADING",
                "baseAsset": "BTC",
                "baseAssetPrecision": 8,
                "quoteAsset": "USDT",
                "quotePrecision": 8,
                "orderTypes": ["LIMIT", "LIMIT_MAKER", "MARKET"],
                "filters": [
                    {"filterType": "PRICE_FILTER", "minPrice": "0.01000000", "maxPrice": "1000000.00000000", "tickSize": "0.01000000"},
                    {"filterType": "LOT_SIZE", "minQty": "0.00001000", "maxQty": "9000.00000000", "stepSize": "0.00001000"},
                    {"filterType": "MIN_NOTIONAL", "minNotional": "5.00000000"}
                ]
            }]
        });
        let rules = parse_symbol_rules(&payload).unwrap();
        assert_eq!(rules.len(), 1);
        let rule = &rules[0];
        assert_eq!(rule.exchange, "binance");
        assert_eq!(rule.internal_symbol, "BTCUSDT");
        assert_eq!(rule.base_asset, "BTC");
        assert_eq!(rule.quote_asset, "USDT");
        assert_eq!(rule.tick_size, 0.01);
        assert_eq!(rule.step_size, 0.00001);
        assert_eq!(rule.min_quantity, 0.00001);
        assert_eq!(rule.min_notional, 5.0);
        assert_eq!(rule.status, SymbolStatus::Trading);
        assert!(rule.supported_order_types.contains(&OrderType::Market));
        assert!(rule.supported_order_types.contains(&OrderType::PostOnly));
        assert!(rule.supported_order_types.contains(&OrderType::IOC));
        assert!(rule.supported_time_in_force.contains(&TimeInForce::FOK));
    }

    #[test]
    fn binance_account_commission_should_parse_fee_rate() {
        let payload = json!({
            "symbol": "BTCUSDT",
            "standardCommission": {
                "maker": "0.00000010",
                "taker": "0.00000020",
                "buyer": "0.00000030",
                "seller": "0.00000040"
            },
            "specialCommission": {
                "maker": "0.01000000",
                "taker": "0.02000000",
                "buyer": "0.03000000",
                "seller": "0.04000000"
            },
            "taxCommission": {
                "maker": "0.00000112",
                "taker": "0.00000114",
                "buyer": "0.00000118",
                "seller": "0.00000116"
            },
            "discount": {
                "enabledForAccount": true,
                "enabledForSymbol": true,
                "discountAsset": "BNB",
                "discount": "0.75000000"
            }
        });
        let fee_rate = parse_fee_rate(&payload, "BTCUSDT").unwrap();
        assert_eq!(fee_rate.maker, 0.00000010);
        assert_eq!(fee_rate.taker, 0.00000020);
    }

    #[test]
    fn binance_trade_fee_should_keep_legacy_fee_rate_compatibility() {
        let payload = json!([
            {
                "symbol": "ETHUSDT",
                "makerCommission": "0.00090000",
                "takerCommission": "0.00100000"
            },
            {
                "symbol": "BTCUSDT",
                "makerCommission": "0.00080000",
                "takerCommission": "0.00095000"
            }
        ]);
        let fee_rate = parse_fee_rate(&payload, "BTCUSDT").unwrap();
        assert_eq!(fee_rate.maker, 0.0008);
        assert_eq!(fee_rate.taker, 0.00095);
    }

    #[test]
    fn binance_account_info_commission_rates_should_parse_fee_rate() {
        let payload = json!({
            "commissionRates": {
                "maker": "0.00150000",
                "taker": "0.00160000",
                "buyer": "0.00000000",
                "seller": "0.00000000"
            }
        });
        let fee_rate = parse_fee_rate(&payload, "BTCUSDT").unwrap();
        assert_eq!(fee_rate.maker, 0.0015);
        assert_eq!(fee_rate.taker, 0.0016);
    }

    #[tokio::test]
    async fn binance_spot_client_should_route_common_private_rest_readbacks() {
        let (base_url, seen) = spawn_spot_rest_server(vec![
            json!({
                "balances": [
                    {"asset": "BTC", "free": "0.10", "locked": "0.02"},
                    {"asset": "USDT", "free": "100.00", "locked": "5.00"}
                ]
            }),
            json!({
                "symbol": "BTCUSDT",
                "orderId": 1001,
                "clientOrderId": "client-1",
                "price": "65000.00",
                "origQty": "0.02",
                "executedQty": "0.01",
                "cummulativeQuoteQty": "650",
                "status": "PARTIALLY_FILLED",
                "timeInForce": "GTC",
                "type": "LIMIT",
                "side": "BUY",
                "time": 1700000000000_i64,
                "updateTime": 1700000001000_i64
            }),
            json!([
                {
                    "symbol": "BTCUSDT",
                    "orderId": 1002,
                    "clientOrderId": "client-2",
                    "price": "66000.00",
                    "origQty": "0.03",
                    "executedQty": "0",
                    "status": "NEW",
                    "timeInForce": "GTC",
                    "type": "LIMIT",
                    "side": "SELL",
                    "time": 1700000002000_i64
                }
            ]),
            json!({
                "symbol": "BTCUSDT",
                "standardCommission": {
                    "maker": "0.00000010",
                    "taker": "0.00000020"
                }
            }),
            json!([
                {
                    "symbol": "BTCUSDT",
                    "id": 28457,
                    "orderId": 1001,
                    "clientOrderId": "client-1",
                    "price": "65000.12000000",
                    "qty": "0.01000000",
                    "commission": "0.00000750",
                    "commissionAsset": "BTC",
                    "time": 1700000003000_i64,
                    "isBuyer": true,
                    "isMaker": false
                }
            ]),
        ])
        .await;
        let client = BinanceSpotClient::new(BinanceSpotConfig {
            api_key: "key".to_string(),
            api_secret: "secret".to_string(),
            rest_base_url: base_url,
            dry_run: false,
            ..BinanceSpotConfig::default()
        });

        let balances = client.get_balances().await.unwrap();
        assert_eq!(balances.exchange, "binance");
        assert_eq!(balances.market_type, MarketType::Spot);
        assert_eq!(balances.balances.len(), 2);
        assert_eq!(balances.balances[0].asset, "BTC");
        assert_eq!(balances.balances[0].available, 0.10);
        assert_eq!(balances.balances[0].locked, 0.02);

        let order = client.get_order("BTCUSDT", "1001").await.unwrap();
        assert_eq!(order.exchange, "binance");
        assert_eq!(order.symbol, "BTCUSDT");
        assert_eq!(order.order_id, "1001");
        assert_eq!(order.client_order_id.as_deref(), Some("client-1"));
        assert_eq!(order.status, OrderStatus::PartiallyFilled);
        assert_eq!(order.side, OrderSide::Buy);
        assert_eq!(order.quantity, 0.02);
        assert_eq!(order.filled_quantity, 0.01);
        assert_eq!(order.average_price, Some(65_000.0));

        let open_orders = client.get_open_orders(Some("BTCUSDT")).await.unwrap();
        assert_eq!(open_orders.len(), 1);
        assert_eq!(open_orders[0].order_id, "1002");
        assert_eq!(open_orders[0].status, OrderStatus::New);
        assert_eq!(open_orders[0].side, OrderSide::Sell);

        let fee = client.get_fee_rate("BTCUSDT").await.unwrap();
        assert_eq!(fee.maker, 0.00000010);
        assert_eq!(fee.taker, 0.00000020);

        let fills = client.get_recent_fills("BTCUSDT").await.unwrap();
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].exchange, "binance");
        assert_eq!(fills[0].symbol, "BTCUSDT");
        assert_eq!(fills[0].trade_id.as_deref(), Some("28457"));
        assert_eq!(fills[0].order_id.as_deref(), Some("1001"));
        assert_eq!(fills[0].client_order_id.as_deref(), Some("client-1"));
        assert_eq!(fills[0].side, OrderSide::Buy);
        assert_eq!(fills[0].liquidity, LiquidityRole::Taker);
        assert_eq!(fills[0].price, 65000.12);
        assert_eq!(fills[0].quantity, 0.01);
        assert_eq!(fills[0].fee_asset.as_deref(), Some("BTC"));
        assert_eq!(fills[0].fee_amount, Some(0.0000075));

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 5);
        assert_eq!(seen[0].method, "GET");
        assert_eq!(seen[0].path, "/api/v3/account");
        assert_signed_spot_request(&seen[0]);

        assert_eq!(seen[1].method, "GET");
        assert_eq!(seen[1].path, "/api/v3/order");
        assert_signed_spot_request(&seen[1]);
        assert_eq!(
            seen[1].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
        assert_eq!(
            seen[1].query.get("orderId").map(String::as_str),
            Some("1001")
        );

        assert_eq!(seen[2].method, "GET");
        assert_eq!(seen[2].path, "/api/v3/openOrders");
        assert_signed_spot_request(&seen[2]);
        assert_eq!(
            seen[2].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );

        assert_eq!(seen[3].method, "GET");
        assert_eq!(seen[3].path, "/api/v3/account/commission");
        assert_signed_spot_request(&seen[3]);
        assert_eq!(
            seen[3].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );

        assert_eq!(seen[4].method, "GET");
        assert_eq!(seen[4].path, "/api/v3/myTrades");
        assert_signed_spot_request(&seen[4]);
        assert_eq!(
            seen[4].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
        assert_eq!(seen[4].query.get("limit").map(String::as_str), Some("1000"));
    }

    #[tokio::test]
    async fn binance_spot_client_should_route_order_mutations() {
        let (base_url, seen) = spawn_spot_rest_server(vec![
            json!({
                "symbol": "BTCUSDT",
                "orderId": 2001,
                "clientOrderId": "LIMIT1",
                "price": "65000.00",
                "origQty": "0.02",
                "executedQty": "0",
                "status": "NEW",
                "timeInForce": "GTC",
                "type": "LIMIT",
                "side": "BUY",
                "transactTime": 1700000000000_i64
            }),
            json!({
                "symbol": "BTCUSDT",
                "orderId": 2002,
                "clientOrderId": "QUOTE1",
                "price": "0",
                "origQty": "0",
                "executedQty": "0",
                "status": "NEW",
                "type": "MARKET",
                "side": "BUY",
                "transactTime": 1700000001000_i64
            }),
            json!({
                "symbol": "BTCUSDT",
                "orderId": 2001,
                "clientOrderId": "LIMIT1",
                "status": "CANCELED"
            }),
            json!([
                {
                    "symbol": "BTCUSDT",
                    "orderId": 2003,
                    "clientOrderId": "CANCELALL1",
                    "status": "CANCELED"
                },
                {
                    "orderListId": 3001,
                    "symbol": "BTCUSDT",
                    "orderReports": [
                        {"symbol":"BTCUSDT","orderId":2004,"clientOrderId":"oco-a","status":"CANCELED"},
                        {"symbol":"BTCUSDT","orderId":2005,"clientOrderId":"oco-b","status":"CANCELED"}
                    ]
                }
            ]),
            json!({
                "amendedOrder": {
                    "symbol": "BTCUSDT",
                    "orderId": 2006,
                    "clientOrderId": "AMENDNEW",
                    "price": "65000.00",
                    "origQty": "0.015",
                    "executedQty": "0",
                    "status": "NEW",
                    "timeInForce": "GTC",
                    "type": "LIMIT",
                    "side": "SELL",
                    "transactTime": 1700000002000_i64
                }
            }),
            json!({
                "orderListId": 4001,
                "contingencyType": "OCO",
                "listStatusType": "EXEC_STARTED",
                "listOrderStatus": "EXECUTING",
                "listClientOrderId": "OCOLIST1",
                "symbol": "BTCUSDT",
                "transactionTime": 1700000003000_i64,
                "orderReports": [
                    {
                        "symbol": "BTCUSDT",
                        "orderId": 4002,
                        "clientOrderId": "OCOABOVE",
                        "price": "70000.00",
                        "origQty": "0.02",
                        "executedQty": "0",
                        "status": "NEW",
                        "type": "LIMIT_MAKER",
                        "side": "SELL",
                        "transactTime": 1700000003000_i64
                    },
                    {
                        "symbol": "BTCUSDT",
                        "orderId": 4003,
                        "clientOrderId": "OCOBELOW",
                        "price": "62000.00",
                        "origQty": "0.02",
                        "executedQty": "0",
                        "status": "NEW",
                        "type": "STOP_LOSS_LIMIT",
                        "timeInForce": "GTC",
                        "side": "SELL",
                        "transactTime": 1700000003000_i64
                    }
                ]
            })
        ])
        .await;
        let client = BinanceSpotClient::new(BinanceSpotConfig {
            api_key: "key".to_string(),
            api_secret: "secret".to_string(),
            rest_base_url: base_url,
            dry_run: false,
            ..BinanceSpotConfig::default()
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
        assert_eq!(placed.exchange, "binance");
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

        let cancel = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                order_id: Some("2001".to_string()),
                client_order_id: Some("LIMIT1".to_string()),
            })
            .await
            .unwrap();
        assert_eq!(cancel.exchange, "binance");
        assert_eq!(cancel.status, OrderStatus::Cancelled);
        assert_eq!(cancel.order_id.as_deref(), Some("2001"));
        assert_eq!(cancel.client_order_id.as_deref(), Some("LIMIT1"));

        let cancel_all = client
            .cancel_all_orders(CancelAllOrdersRequest::for_symbol(
                MarketType::Spot,
                "BTCUSDT",
            ))
            .await
            .unwrap();
        assert_eq!(cancel_all.exchange, "binance");
        assert_eq!(cancel_all.cancelled_orders, 3);
        assert_eq!(cancel_all.symbol.as_deref(), Some("BTCUSDT"));

        let amended = client
            .amend_order(AmendOrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                order_id: Some("2006".to_string()),
                client_order_id: Some("AMENDOLD".to_string()),
                new_client_order_id: Some("AMENDNEW".to_string()),
                new_quantity: 0.015,
            })
            .await
            .unwrap();
        assert_eq!(amended.order_id, "2006");
        assert_eq!(amended.client_order_id.as_deref(), Some("AMENDNEW"));
        assert_eq!(amended.quantity, 0.015);

        let order_list = client
            .place_order_list(OrderListRequest::Oco {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                list_client_order_id: Some("OCOLIST1".to_string()),
                side: OrderSide::Sell,
                quantity: 0.02,
                above: OrderListConditionalLeg {
                    order_type: OrderListLegType::LimitMaker,
                    price: Some(70_000.0),
                    stop_price: None,
                    time_in_force: None,
                    client_order_id: Some("OCOABOVE".to_string()),
                },
                below: OrderListConditionalLeg {
                    order_type: OrderListLegType::StopLossLimit,
                    price: Some(62_000.0),
                    stop_price: Some(62_500.0),
                    time_in_force: Some(TimeInForce::GTC),
                    client_order_id: Some("OCOBELOW".to_string()),
                },
            })
            .await
            .unwrap();
        assert_eq!(order_list.exchange, "binance");
        assert_eq!(order_list.kind, OrderListKind::Oco);
        assert_eq!(order_list.order_list_id.as_deref(), Some("4001"));
        assert_eq!(order_list.list_client_order_id.as_deref(), Some("OCOLIST1"));
        assert_eq!(order_list.orders.len(), 2);

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 6);
        assert_eq!(seen[0].method, "POST");
        assert_eq!(seen[0].path, "/api/v3/order");
        assert_signed_spot_request(&seen[0]);
        assert_eq!(
            seen[0].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
        assert_eq!(seen[0].query.get("side").map(String::as_str), Some("BUY"));
        assert_eq!(seen[0].query.get("type").map(String::as_str), Some("LIMIT"));
        assert_eq!(
            seen[0].query.get("quantity").map(String::as_str),
            Some("0.02")
        );
        assert_eq!(
            seen[0].query.get("price").map(String::as_str),
            Some("65000")
        );
        assert_eq!(
            seen[0].query.get("timeInForce").map(String::as_str),
            Some("GTC")
        );
        assert_eq!(
            seen[0].query.get("newClientOrderId").map(String::as_str),
            Some("LIMIT1")
        );

        assert_eq!(seen[1].method, "POST");
        assert_eq!(seen[1].path, "/api/v3/order");
        assert_signed_spot_request(&seen[1]);
        assert_eq!(
            seen[1].query.get("type").map(String::as_str),
            Some("MARKET")
        );
        assert_eq!(
            seen[1].query.get("quoteOrderQty").map(String::as_str),
            Some("125.5")
        );
        assert_eq!(
            seen[1].query.get("newClientOrderId").map(String::as_str),
            Some("QUOTE1")
        );

        assert_eq!(seen[2].method, "DELETE");
        assert_eq!(seen[2].path, "/api/v3/order");
        assert_signed_spot_request(&seen[2]);
        assert_eq!(
            seen[2].query.get("orderId").map(String::as_str),
            Some("2001")
        );
        assert_eq!(
            seen[2].query.get("origClientOrderId").map(String::as_str),
            Some("LIMIT1")
        );

        assert_eq!(seen[3].method, "DELETE");
        assert_eq!(seen[3].path, "/api/v3/openOrders");
        assert_signed_spot_request(&seen[3]);
        assert_eq!(
            seen[3].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );

        assert_eq!(seen[4].method, "PUT");
        assert_eq!(seen[4].path, "/api/v3/order/amend/keepPriority");
        assert_signed_spot_request(&seen[4]);
        assert_eq!(
            seen[4].query.get("newQty").map(String::as_str),
            Some("0.015")
        );
        assert_eq!(
            seen[4].query.get("orderId").map(String::as_str),
            Some("2006")
        );
        assert_eq!(
            seen[4].query.get("origClientOrderId").map(String::as_str),
            Some("AMENDOLD")
        );
        assert_eq!(
            seen[4].query.get("newClientOrderId").map(String::as_str),
            Some("AMENDNEW")
        );

        assert_eq!(seen[5].method, "POST");
        assert_eq!(seen[5].path, "/api/v3/orderList/oco");
        assert_signed_spot_request(&seen[5]);
        assert_eq!(
            seen[5].query.get("listClientOrderId").map(String::as_str),
            Some("OCOLIST1")
        );
        assert_eq!(seen[5].query.get("side").map(String::as_str), Some("SELL"));
        assert_eq!(
            seen[5].query.get("quantity").map(String::as_str),
            Some("0.02")
        );
        assert_eq!(
            seen[5].query.get("aboveClientOrderId").map(String::as_str),
            Some("OCOABOVE")
        );
        assert_eq!(
            seen[5].query.get("belowClientOrderId").map(String::as_str),
            Some("OCOBELOW")
        );
        assert_eq!(
            seen[5].query.get("newOrderRespType").map(String::as_str),
            Some("RESULT")
        );
    }

    #[test]
    fn binance_my_trades_should_parse_recent_fills() {
        let payload = json!([{
            "symbol": "BTCUSDT",
            "id": 28457,
            "orderId": 100234,
            "price": "65000.12000000",
            "qty": "0.01000000",
            "quoteQty": "650.00120000",
            "commission": "0.00000750",
            "commissionAsset": "BTC",
            "time": 1700000000000_i64,
            "isBuyer": true,
            "isMaker": false,
            "isBestMatch": true
        }]);
        let fills = parse_fills(&payload).unwrap();
        assert_eq!(fills.len(), 1);
        let fill = &fills[0];
        assert_eq!(fill.exchange, "binance");
        assert_eq!(fill.symbol, "BTCUSDT");
        assert_eq!(fill.trade_id.as_deref(), Some("28457"));
        assert_eq!(fill.order_id.as_deref(), Some("100234"));
        assert_eq!(fill.side, OrderSide::Buy);
        assert_eq!(fill.liquidity, LiquidityRole::Taker);
        assert_eq!(fill.price, 65000.12);
        assert_eq!(fill.quantity, 0.01);
        assert_eq!(fill.fee_asset.as_deref(), Some("BTC"));
        assert_eq!(fill.fee_amount, Some(0.0000075));
    }

    #[test]
    fn binance_cancel_all_should_count_orders_and_order_lists() {
        let payload = json!([
            {
                "symbol": "BTCUSDT",
                "orderId": 11,
                "clientOrderId": "plain-cancel",
                "status": "CANCELED"
            },
            {
                "orderListId": 1929,
                "symbol": "BTCUSDT",
                "orderReports": [
                    {
                        "symbol": "BTCUSDT",
                        "orderId": 20,
                        "clientOrderId": "oco-1",
                        "status": "CANCELED"
                    },
                    {
                        "symbol": "BTCUSDT",
                        "orderId": 21,
                        "clientOrderId": "oco-2",
                        "status": "CANCELED"
                    }
                ]
            }
        ]);
        assert_eq!(binance_cancel_all_cancelled_count(&payload).unwrap(), 3);
    }

    #[test]
    fn binance_quote_market_order_params_should_use_quote_order_qty() {
        let request = QuoteMarketOrderRequest {
            market_type: MarketType::Spot,
            symbol: "BTCUSDT".to_string(),
            side: OrderSide::Buy,
            quote_quantity: 125.50,
            client_order_id: Some("quote-buy-1".to_string()),
        };

        let params = binance_quote_market_order_params(&request, "BTCUSDT").unwrap();
        assert_eq!(params.get("symbol").map(String::as_str), Some("BTCUSDT"));
        assert_eq!(params.get("side").map(String::as_str), Some("BUY"));
        assert_eq!(params.get("type").map(String::as_str), Some("MARKET"));
        assert_eq!(
            params.get("quoteOrderQty").map(String::as_str),
            Some("125.5")
        );
        assert!(!params.contains_key("quantity"));
        assert_eq!(
            params.get("newClientOrderId").map(String::as_str),
            Some("quote-buy-1")
        );
    }

    #[test]
    fn binance_amend_order_params_should_use_keep_priority_fields() {
        let request = AmendOrderRequest {
            market_type: MarketType::Spot,
            symbol: "BTCUSDT".to_string(),
            order_id: Some("33".to_string()),
            client_order_id: Some("old-client".to_string()),
            new_client_order_id: Some("new-client".to_string()),
            new_quantity: 0.005,
        };

        let params = binance_amend_order_params(&request, "BTCUSDT").unwrap();
        assert_eq!(params.get("symbol").map(String::as_str), Some("BTCUSDT"));
        assert_eq!(params.get("orderId").map(String::as_str), Some("33"));
        assert_eq!(
            params.get("origClientOrderId").map(String::as_str),
            Some("old-client")
        );
        assert_eq!(
            params.get("newClientOrderId").map(String::as_str),
            Some("new-client")
        );
        assert_eq!(params.get("newQty").map(String::as_str), Some("0.005"));

        let client_only = AmendOrderRequest {
            order_id: Some("   ".to_string()),
            client_order_id: Some("old-client".to_string()),
            new_client_order_id: Some("   ".to_string()),
            ..request
        };
        let params = binance_amend_order_params(&client_only, "BTCUSDT").unwrap();
        assert!(!params.contains_key("orderId"));
        assert_eq!(
            params.get("origClientOrderId").map(String::as_str),
            Some("old-client")
        );
        assert!(!params.contains_key("newClientOrderId"));
    }

    #[test]
    fn binance_amend_response_should_parse_amended_order() {
        let payload = json!({
            "transactTime": 1741926410255_i64,
            "executionId": 75,
            "amendedOrder": {
                "symbol": "BTCUSDT",
                "orderId": 33,
                "orderListId": -1,
                "origClientOrderId": "old-client",
                "clientOrderId": "new-client",
                "price": "6.00000000",
                "qty": "5.00000000",
                "executedQty": "0.00000000",
                "status": "NEW",
                "timeInForce": "GTC",
                "type": "LIMIT",
                "side": "SELL"
            }
        });

        let order = parse_amend_order_response(&payload, "binance").unwrap();
        assert_eq!(order.order_id, "33");
        assert_eq!(order.client_order_id.as_deref(), Some("new-client"));
        assert_eq!(order.side, OrderSide::Sell);
        assert_eq!(order.quantity, 5.0);
        assert_eq!(order.price, Some(6.0));
        assert_eq!(order.status, OrderStatus::New);
    }

    #[test]
    fn binance_order_list_params_should_build_oco_and_oto() {
        let oco = OrderListRequest::Oco {
            market_type: MarketType::Spot,
            symbol: "LTCBTC".to_string(),
            list_client_order_id: Some("OCOLIST1".to_string()),
            side: OrderSide::Sell,
            quantity: 5.0,
            above: OrderListConditionalLeg {
                order_type: OrderListLegType::LimitMaker,
                price: Some(3.0),
                stop_price: None,
                time_in_force: None,
                client_order_id: Some("ABOVE1".to_string()),
            },
            below: OrderListConditionalLeg {
                order_type: OrderListLegType::StopLossLimit,
                price: Some(1.0),
                stop_price: Some(1.0),
                time_in_force: Some(TimeInForce::GTC),
                client_order_id: Some("BELOW1".to_string()),
            },
        };
        assert_eq!(binance_order_list_path(&oco), "/api/v3/orderList/oco");
        let params = binance_order_list_params(&oco, "LTCBTC").unwrap();
        assert_eq!(
            params.get("listClientOrderId").map(String::as_str),
            Some("OCOLIST1")
        );
        assert_eq!(params.get("side").map(String::as_str), Some("SELL"));
        assert_eq!(params.get("quantity").map(String::as_str), Some("5"));
        assert_eq!(
            params.get("aboveType").map(String::as_str),
            Some("LIMIT_MAKER")
        );
        assert_eq!(params.get("abovePrice").map(String::as_str), Some("3"));
        assert_eq!(
            params.get("belowType").map(String::as_str),
            Some("STOP_LOSS_LIMIT")
        );
        assert_eq!(params.get("belowStopPrice").map(String::as_str), Some("1"));
        assert_eq!(
            params.get("belowTimeInForce").map(String::as_str),
            Some("GTC")
        );
        assert_eq!(
            params.get("newOrderRespType").map(String::as_str),
            Some("RESULT")
        );

        let oto = OrderListRequest::Oto {
            market_type: MarketType::Spot,
            symbol: "LTCBTC".to_string(),
            list_client_order_id: Some("OTOLIST1".to_string()),
            working: OrderListOrderLeg {
                side: OrderSide::Sell,
                order_type: OrderListLegType::Limit,
                quantity: 1.0,
                price: Some(1.0),
                stop_price: None,
                time_in_force: Some(TimeInForce::GTC),
                client_order_id: Some("WORKING1".to_string()),
            },
            pending: OrderListOrderLeg {
                side: OrderSide::Buy,
                order_type: OrderListLegType::Market,
                quantity: 5.0,
                price: None,
                stop_price: None,
                time_in_force: None,
                client_order_id: Some("PENDING1".to_string()),
            },
        };
        assert_eq!(binance_order_list_path(&oto), "/api/v3/orderList/oto");
        let params = binance_order_list_params(&oto, "LTCBTC").unwrap();
        assert_eq!(params.get("workingType").map(String::as_str), Some("LIMIT"));
        assert_eq!(params.get("workingSide").map(String::as_str), Some("SELL"));
        assert_eq!(
            params.get("workingTimeInForce").map(String::as_str),
            Some("GTC")
        );
        assert_eq!(
            params.get("pendingType").map(String::as_str),
            Some("MARKET")
        );
        assert_eq!(params.get("pendingSide").map(String::as_str), Some("BUY"));
        assert_eq!(params.get("pendingQuantity").map(String::as_str), Some("5"));
    }

    #[test]
    fn binance_order_list_response_should_parse_order_reports() {
        let payload = json!({
            "orderListId": 0,
            "contingencyType": "OTO",
            "listStatusType": "EXEC_STARTED",
            "listOrderStatus": "EXECUTING",
            "listClientOrderId": "OTOLIST1",
            "transactionTime": 1712289389158_i64,
            "symbol": "LTCBTC",
            "orderReports": [
                {
                    "symbol": "LTCBTC",
                    "orderId": 4,
                    "orderListId": 0,
                    "clientOrderId": "WORKING1",
                    "transactTime": 1712289389158_i64,
                    "price": "1.00000000",
                    "origQty": "1.00000000",
                    "executedQty": "0.00000000",
                    "status": "NEW",
                    "timeInForce": "GTC",
                    "type": "LIMIT",
                    "side": "SELL"
                },
                {
                    "symbol": "LTCBTC",
                    "orderId": 5,
                    "orderListId": 0,
                    "clientOrderId": "PENDING1",
                    "transactTime": 1712289389158_i64,
                    "price": "0.00000000",
                    "origQty": "5.00000000",
                    "executedQty": "0.00000000",
                    "status": "PENDING_NEW",
                    "timeInForce": "GTC",
                    "type": "MARKET",
                    "side": "BUY"
                }
            ]
        });

        let list = parse_order_list_response(&payload, "binance").unwrap();
        assert_eq!(list.kind, OrderListKind::Oto);
        assert_eq!(list.order_list_id.as_deref(), Some("0"));
        assert_eq!(list.list_client_order_id.as_deref(), Some("OTOLIST1"));
        assert_eq!(list.orders.len(), 2);
        assert_eq!(list.orders[0].order_id, "4");
        assert_eq!(list.orders[1].status, OrderStatus::New);
        assert_eq!(list.orders[1].order_type, OrderType::Market);
    }

    #[tokio::test]
    async fn binance_quote_market_order_should_ack_in_dry_run() {
        let client = BinanceSpotClient::new(BinanceSpotConfig {
            dry_run: true,
            ..BinanceSpotConfig::default()
        });

        let ack = client
            .place_quote_market_order(QuoteMarketOrderRequest::spot_buy("BTCUSDT", 25.0))
            .await
            .unwrap();

        assert_eq!(ack.exchange, "binance");
        assert_eq!(ack.market_type, MarketType::Spot);
        assert_eq!(ack.symbol, "BTCUSDT");
        assert_eq!(ack.side, OrderSide::Buy);
        assert_eq!(ack.order_type, OrderType::Market);
        assert_eq!(ack.status, OrderStatus::New);
        assert!(ack.client_order_id.is_some());
    }

    #[tokio::test]
    async fn binance_cancel_order_should_validate_client_order_id_in_dry_run() {
        let client = BinanceSpotClient::new(BinanceSpotConfig {
            dry_run: true,
            ..BinanceSpotConfig::default()
        });

        let ack = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                order_id: None,
                client_order_id: Some("CANCELCLIENT1".to_string()),
            })
            .await
            .unwrap();

        assert_eq!(ack.status, OrderStatus::Cancelled);
        assert_eq!(ack.client_order_id.as_deref(), Some("CANCELCLIENT1"));

        let ack = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                order_id: Some("   ".to_string()),
                client_order_id: Some("CANCELCLIENT2".to_string()),
            })
            .await
            .unwrap();

        assert_eq!(ack.status, OrderStatus::Cancelled);
        assert_eq!(ack.client_order_id.as_deref(), Some("CANCELCLIENT2"));

        let err = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                order_id: None,
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
    }

    #[tokio::test]
    async fn binance_amend_order_should_ack_in_dry_run() {
        let client = BinanceSpotClient::new(BinanceSpotConfig {
            dry_run: true,
            ..BinanceSpotConfig::default()
        });

        let ack = client
            .amend_order(AmendOrderRequest::reduce_quantity_by_order_id(
                MarketType::Spot,
                "BTCUSDT",
                "33",
                0.005,
            ))
            .await
            .unwrap();

        assert_eq!(ack.exchange, "binance");
        assert_eq!(ack.market_type, MarketType::Spot);
        assert_eq!(ack.symbol, "BTCUSDT");
        assert_eq!(ack.order_id, "33");
        assert_eq!(ack.quantity, 0.005);
        assert_eq!(ack.status, OrderStatus::New);

        let ack = client
            .amend_order(AmendOrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                order_id: Some("33".to_string()),
                client_order_id: Some("   ".to_string()),
                new_client_order_id: Some("   ".to_string()),
                new_quantity: 0.004,
            })
            .await
            .unwrap();
        assert_eq!(ack.order_id, "33");
        assert_eq!(ack.client_order_id, None);
    }

    #[tokio::test]
    async fn binance_order_list_should_ack_in_dry_run() {
        let client = BinanceSpotClient::new(BinanceSpotConfig {
            dry_run: true,
            ..BinanceSpotConfig::default()
        });

        let ack = client
            .place_order_list(OrderListRequest::Oco {
                market_type: MarketType::Spot,
                symbol: "LTCBTC".to_string(),
                list_client_order_id: Some("OCOLIST1".to_string()),
                side: OrderSide::Sell,
                quantity: 5.0,
                above: OrderListConditionalLeg {
                    order_type: OrderListLegType::LimitMaker,
                    price: Some(3.0),
                    stop_price: None,
                    time_in_force: None,
                    client_order_id: Some("ABOVE1".to_string()),
                },
                below: OrderListConditionalLeg {
                    order_type: OrderListLegType::StopLossLimit,
                    price: Some(1.0),
                    stop_price: Some(1.0),
                    time_in_force: Some(TimeInForce::GTC),
                    client_order_id: Some("BELOW1".to_string()),
                },
            })
            .await
            .unwrap();

        assert_eq!(ack.exchange, "binance");
        assert_eq!(ack.kind, OrderListKind::Oco);
        assert_eq!(ack.orders.len(), 2);
        assert_eq!(ack.orders[0].quantity, 5.0);
        assert_eq!(ack.orders[0].order_type, OrderType::PostOnly);
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
