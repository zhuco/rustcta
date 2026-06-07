use std::collections::{BTreeMap, HashMap};
use std::time::Duration as StdDuration;

use async_trait::async_trait;
use base64::{engine::general_purpose, Engine as _};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256, Sha512};

use crate::core::error::ExchangeError as CoreExchangeError;
use crate::exchanges::client_order_id::{generate_client_order_id, validate_client_order_id};
use crate::exchanges::unified::{
    validate_order_lookup_id, validate_orderbook_depth, AssetBalance, BalanceSnapshot,
    CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse,
    ExchangeClient, ExchangeClientCapabilities, ExchangeClientError, ExchangeClientResult,
    ExchangeError, ExchangeErrorClass, ExchangeHealthStatus, FeeRate, FeeRateSource, LiquidityRole,
    MarketType, OrderBookLevel, OrderBookSnapshot, OrderRequest, OrderResponse, OrderSide,
    OrderStatus, OrderType, PositionSide, QuoteMarketOrderRequest, SymbolRule, SymbolStatus,
    TimeInForce, TradeFill, UserStreamEvent,
};

type HmacSha512 = Hmac<Sha512>;

const DEFAULT_REST_BASE_URL: &str = "https://api.kraken.com";
const DEFAULT_WS_URL: &str = "wss://ws.kraken.com/v2";
const DEFAULT_STALE_BOOK_MS: u64 = 10_000;
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_DEPTH: u16 = 5;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KrakenSpotConfig {
    #[serde(default)]
    pub api_key: String,
    #[serde(default)]
    pub api_secret: String,
    #[serde(default = "default_rest_base_url")]
    pub base_url: String,
    #[serde(default = "default_ws_url")]
    pub websocket_url: String,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default = "default_stale_book_ms")]
    pub stale_book_ms: u64,
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    #[serde(default = "default_depth")]
    pub orderbook_depth: u16,
    #[serde(default)]
    pub enabled_symbols: Vec<String>,
    #[serde(default)]
    pub asset_aliases: HashMap<String, String>,
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

impl Default for KrakenSpotConfig {
    fn default() -> Self {
        Self {
            api_key: std::env::var("KRAKEN_API_KEY").unwrap_or_default(),
            api_secret: std::env::var("KRAKEN_API_SECRET").unwrap_or_default(),
            base_url: default_rest_base_url(),
            websocket_url: default_ws_url(),
            dry_run: true,
            stale_book_ms: DEFAULT_STALE_BOOK_MS,
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            orderbook_depth: DEFAULT_DEPTH,
            enabled_symbols: Vec::new(),
            asset_aliases: HashMap::new(),
            fee_override: None,
            log_raw_messages: false,
        }
    }
}

#[derive(Clone)]
pub struct KrakenSpotClient {
    config: KrakenSpotConfig,
    http: reqwest::Client,
}

impl KrakenSpotClient {
    pub fn new(config: KrakenSpotConfig) -> Self {
        Self {
            config,
            http: crate::core::http2_fix::shared_http_client(),
        }
    }

    pub fn generate_client_order_id() -> String {
        generate_client_order_id("kraken", MarketType::Spot, "spot").into_string()
    }

    pub fn build_signed_request_spec(
        &self,
        endpoint: &str,
        params: BTreeMap<String, String>,
        nonce: &str,
    ) -> ExchangeClientResult<KrakenSignedRequestSpec> {
        self.ensure_credentials()?;
        let path = format!("/0/private/{endpoint}");
        let mut body = BTreeMap::new();
        body.insert("nonce".to_string(), nonce.to_string());
        body.extend(params);
        let body_text = form_encode(&body);
        let signature = sign_kraken_spot_request(&self.config.api_secret, &path, &body_text)?;
        Ok(KrakenSignedRequestSpec {
            method: Method::POST,
            path,
            body: body_text,
            api_key: self.config.api_key.clone(),
            api_sign: signature,
        })
    }

    async fn send_public_request(
        &self,
        endpoint: &str,
        params: BTreeMap<String, String>,
    ) -> ExchangeClientResult<Value> {
        let url = build_public_url(&self.config.base_url, endpoint, &params);
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
        endpoint: &str,
        params: BTreeMap<String, String>,
    ) -> ExchangeClientResult<Value> {
        let nonce = Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_default()
            .to_string();
        let spec = self.build_signed_request_spec(endpoint, params, &nonce)?;
        let url = format!(
            "{}{}",
            self.config.base_url.trim_end_matches('/'),
            spec.path
        );
        let response = self
            .http
            .post(url)
            .header("API-Key", spec.api_key)
            .header("API-Sign", spec.api_sign)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .timeout(StdDuration::from_millis(self.config.request_timeout_ms))
            .body(spec.body)
            .send()
            .await
            .map_err(CoreExchangeError::from)?;
        parse_response(response).await
    }

    fn ensure_credentials(&self) -> ExchangeClientResult<()> {
        if self.config.api_key.trim().is_empty() || self.config.api_secret.trim().is_empty() {
            return Err(classified(
                ExchangeErrorClass::AuthenticationFailed,
                None,
                "KRAKEN_API_KEY and KRAKEN_API_SECRET are required",
            ));
        }
        Ok(())
    }

    fn dry_run_order_response(&self, request: &OrderRequest, symbol: &str) -> OrderResponse {
        OrderResponse {
            exchange: "kraken".to_string(),
            market_type: MarketType::Spot,
            symbol: symbol.to_string(),
            order_id: format!("dry-kraken-{}", Utc::now().timestamp_millis()),
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
            exchange: "kraken".to_string(),
            market_type: MarketType::Spot,
            symbol: symbol.to_string(),
            order_id: format!("dry-kraken-{}", Utc::now().timestamp_millis()),
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KrakenSignedRequestSpec {
    pub method: Method,
    pub path: String,
    pub body: String,
    pub api_key: String,
    pub api_sign: String,
}

#[async_trait]
impl ExchangeClient for KrakenSpotClient {
    fn market_type(&self) -> MarketType {
        MarketType::Spot
    }

    fn exchange_name(&self) -> &str {
        "kraken"
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let mut capabilities = ExchangeClientCapabilities::spot(self.exchange_name());
        capabilities.supports_cancel_all_orders = true;
        capabilities.supports_quote_market_order = true;
        capabilities.supports_private_user_stream = false;
        capabilities
    }

    fn normalize_symbol(&self, symbol: &str) -> ExchangeClientResult<String> {
        normalize_kraken_symbol(symbol, &self.config.asset_aliases)
    }

    fn denormalize_symbol(&self, symbol: &str) -> ExchangeClientResult<String> {
        Ok(to_kraken_pair_code(&self.normalize_symbol(symbol)?))
    }

    async fn load_symbol_rules(&self) -> ExchangeClientResult<Vec<SymbolRule>> {
        let response = self
            .send_public_request("AssetPairs", BTreeMap::new())
            .await?;
        parse_symbol_rules(&response)
    }

    async fn get_balances(&self) -> ExchangeClientResult<BalanceSnapshot> {
        let response = match self.send_signed_request("BalanceEx", BTreeMap::new()).await {
            Ok(response) => response,
            Err(_) => self.send_signed_request("Balance", BTreeMap::new()).await?,
        };
        parse_balance_snapshot(&response)
    }

    async fn get_orderbook(
        &self,
        symbol: &str,
        depth: u16,
    ) -> ExchangeClientResult<OrderBookSnapshot> {
        let symbol = self.normalize_symbol(symbol)?;
        validate_orderbook_depth(depth)?;
        let mut params = BTreeMap::new();
        params.insert("pair".to_string(), to_kraken_pair_code(&symbol));
        params.insert("count".to_string(), normalize_depth(depth).to_string());
        let response = self.send_public_request("Depth", params).await?;
        parse_orderbook_snapshot(&response, &symbol, self.config.stale_book_ms)
    }

    async fn place_order(&self, mut request: OrderRequest) -> ExchangeClientResult<OrderResponse> {
        request.validate()?;
        if request.market_type != MarketType::Spot {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: "KrakenSpotClient only supports MarketType::Spot".to_string(),
            });
        }
        let symbol = self.normalize_symbol(&request.symbol)?;
        request.symbol = symbol.clone();
        ensure_client_order_id(&mut request)?;
        let params = kraken_order_params(&request, &to_kraken_pair_code(&symbol))?;
        if self.config.dry_run {
            return Ok(self.dry_run_order_response(&request, &symbol));
        }
        let response = self.send_signed_request("AddOrder", params).await?;
        parse_order_ack(&response, &request, &symbol)
    }

    async fn place_quote_market_order(
        &self,
        mut request: QuoteMarketOrderRequest,
    ) -> ExchangeClientResult<OrderResponse> {
        request.validate()?;
        if request.side != OrderSide::Buy {
            return Err(ExchangeClientError::Unsupported(
                "Kraken Spot quote-sized market orders are only supported for market buys"
                    .to_string(),
            ));
        }
        let symbol = self.normalize_symbol(&request.symbol)?;
        request.symbol = symbol.clone();
        if let Some(client_order_id) = request.client_order_id.as_deref() {
            validate_client_order_id("kraken", MarketType::Spot, client_order_id).map_err(
                |error| ExchangeClientError::Validation {
                    field: "client_order_id",
                    reason: error.to_string(),
                },
            )?;
        } else {
            request.client_order_id = Some(Self::generate_client_order_id());
        }
        if self.config.dry_run {
            return Ok(self.dry_run_quote_market_order_response(&request, &symbol));
        }
        let mut params = BTreeMap::new();
        params.insert("pair".to_string(), to_kraken_pair_code(&symbol));
        params.insert("type".to_string(), "buy".to_string());
        params.insert("ordertype".to_string(), "market".to_string());
        params.insert("volume".to_string(), request.quote_quantity.to_string());
        params.insert("oflags".to_string(), "viqc".to_string());
        if let Some(client_order_id) = request.client_order_id.as_deref() {
            params.insert("cl_ord_id".to_string(), client_order_id.to_string());
        }
        let response = self.send_signed_request("AddOrder", params).await?;
        parse_quote_order_ack(&response, &request, &symbol)
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeClientResult<CancelOrderResponse> {
        request.validate()?;
        if request.market_type != MarketType::Spot {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: "KrakenSpotClient only supports MarketType::Spot".to_string(),
            });
        }
        let symbol = self.normalize_symbol(&request.symbol)?;
        let order_lookup_id = request
            .order_id()
            .or_else(|| request.client_order_id())
            .unwrap_or_default();
        validate_order_lookup_id(order_lookup_id)?;
        if self.config.dry_run {
            return Ok(CancelOrderResponse {
                exchange: "kraken".to_string(),
                market_type: MarketType::Spot,
                symbol,
                order_id: request.order_id().map(str::to_string),
                client_order_id: request.client_order_id().map(str::to_string),
                status: OrderStatus::Cancelled,
                cancelled_at: Utc::now(),
            });
        }
        let mut params = BTreeMap::new();
        params.insert(
            "txid".to_string(),
            request
                .order_id()
                .or_else(|| request.client_order_id())
                .unwrap_or_default()
                .to_string(),
        );
        self.send_signed_request("CancelOrder", params).await?;
        Ok(CancelOrderResponse {
            exchange: "kraken".to_string(),
            market_type: MarketType::Spot,
            symbol,
            order_id: request.order_id().map(str::to_string),
            client_order_id: request.client_order_id().map(str::to_string),
            status: OrderStatus::Cancelled,
            cancelled_at: Utc::now(),
        })
    }

    async fn cancel_all_orders(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeClientResult<CancelAllOrdersResponse> {
        request.validate_for_market_type(MarketType::Spot)?;
        if request.symbol.is_some() {
            return Err(ExchangeClientError::Unsupported(
                "Kraken Spot cancel-all is account-wide; symbol-scoped cancel-all is unsupported"
                    .to_string(),
            ));
        }
        if self.config.dry_run {
            return Ok(CancelAllOrdersResponse {
                exchange: "kraken".to_string(),
                market_type: MarketType::Spot,
                symbol: None,
                cancelled_orders: 0,
                cancelled_at: Utc::now(),
            });
        }
        let response = self
            .send_signed_request("CancelAll", BTreeMap::new())
            .await?;
        Ok(CancelAllOrdersResponse {
            exchange: "kraken".to_string(),
            market_type: MarketType::Spot,
            symbol: None,
            cancelled_orders: response
                .get("count")
                .or_else(|| {
                    response
                        .get("result")
                        .and_then(|result| result.get("count"))
                })
                .and_then(parse_json_usize)
                .unwrap_or(0),
            cancelled_at: Utc::now(),
        })
    }

    async fn get_order(&self, symbol: &str, order_id: &str) -> ExchangeClientResult<OrderResponse> {
        let symbol = self.normalize_symbol(symbol)?;
        validate_order_lookup_id(order_id)?;
        let mut params = BTreeMap::new();
        params.insert("txid".to_string(), order_id.to_string());
        let response = self.send_signed_request("QueryOrders", params).await?;
        parse_single_order(&response, &symbol, Some(order_id))
    }

    async fn get_open_orders(
        &self,
        symbol: Option<&str>,
    ) -> ExchangeClientResult<Vec<OrderResponse>> {
        let requested_symbol = symbol
            .map(|value| self.normalize_symbol(value))
            .transpose()?;
        let response = self
            .send_signed_request("OpenOrders", BTreeMap::new())
            .await?;
        let mut orders = parse_open_orders(&response)?;
        if let Some(symbol) = requested_symbol {
            orders.retain(|order| order.symbol == symbol);
        }
        Ok(orders)
    }

    async fn get_fee_rate(&self, symbol: &str) -> ExchangeClientResult<FeeRate> {
        self.normalize_symbol(symbol)?;
        if let Some(override_fee) = self.config.fee_override {
            return Ok(FeeRate::new(
                override_fee.maker_fee_rate,
                override_fee.taker_fee_rate,
                FeeRateSource::ConfigOverride,
            ));
        }
        if self.config.api_key.trim().is_empty() || self.config.api_secret.trim().is_empty() {
            return Ok(FeeRate::new(0.0016, 0.0026, FeeRateSource::DefaultFallback));
        }
        let mut params = BTreeMap::new();
        params.insert(
            "pair".to_string(),
            to_kraken_pair_code(&self.normalize_symbol(symbol)?),
        );
        let response = self.send_signed_request("TradeVolume", params).await?;
        Ok(parse_fee_rate(&response)
            .unwrap_or_else(|| FeeRate::new(0.0016, 0.0026, FeeRateSource::DefaultFallback)))
    }

    async fn get_recent_fills(&self, symbol: &str) -> ExchangeClientResult<Vec<TradeFill>> {
        let symbol = self.normalize_symbol(symbol)?;
        let response = self
            .send_signed_request("TradesHistory", BTreeMap::new())
            .await?;
        Ok(parse_recent_fills(&response)?
            .into_iter()
            .filter(|fill| fill.symbol == symbol)
            .collect())
    }

    async fn health_check(&self) -> ExchangeClientResult<ExchangeHealthStatus> {
        let result = self
            .send_public_request("Time", BTreeMap::new())
            .await
            .map(|_| true)
            .unwrap_or(false);
        Ok(ExchangeHealthStatus {
            exchange: "kraken".to_string(),
            market_type: MarketType::Spot,
            connected: result,
            public_ws_healthy: false,
            private_ws_healthy: false,
            rest_healthy: result,
            stale_books: Vec::new(),
            last_error: None,
            checked_at: Utc::now(),
        })
    }
}

pub fn sign_kraken_spot_request(
    api_secret: &str,
    uri_path: &str,
    encoded_payload: &str,
) -> ExchangeClientResult<String> {
    let nonce = encoded_payload
        .split('&')
        .find_map(|part| part.strip_prefix("nonce="))
        .ok_or_else(|| ExchangeClientError::Validation {
            field: "nonce",
            reason: "Kraken signed payload requires nonce".to_string(),
        })?;
    let mut sha = Sha256::new();
    sha.update(nonce.as_bytes());
    sha.update(encoded_payload.as_bytes());
    let payload_hash = sha.finalize();
    let mut message = Vec::with_capacity(uri_path.len() + payload_hash.len());
    message.extend_from_slice(uri_path.as_bytes());
    message.extend_from_slice(&payload_hash);
    let secret = general_purpose::STANDARD
        .decode(api_secret)
        .map_err(|error| ExchangeClientError::Validation {
            field: "api_secret",
            reason: format!("Kraken API secret must be base64: {error}"),
        })?;
    let mut mac =
        HmacSha512::new_from_slice(&secret).map_err(|error| ExchangeClientError::Validation {
            field: "api_secret",
            reason: format!("invalid Kraken API secret: {error}"),
        })?;
    mac.update(&message);
    Ok(general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
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

fn default_request_timeout_ms() -> u64 {
    DEFAULT_REQUEST_TIMEOUT_MS
}

fn default_depth() -> u16 {
    DEFAULT_DEPTH
}

fn build_public_url(base_url: &str, endpoint: &str, params: &BTreeMap<String, String>) -> String {
    let path = format!("/0/public/{endpoint}");
    if params.is_empty() {
        return format!("{}{}", base_url.trim_end_matches('/'), path);
    }
    format!(
        "{}{}?{}",
        base_url.trim_end_matches('/'),
        path,
        form_encode(params)
    )
}

fn form_encode(params: &BTreeMap<String, String>) -> String {
    params
        .iter()
        .map(|(key, value)| {
            format!(
                "{}={}",
                urlencoding::encode(key),
                urlencoding::encode(value)
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

fn normalize_depth(depth: u16) -> u16 {
    depth.clamp(1, 500)
}

fn ensure_client_order_id(request: &mut OrderRequest) -> ExchangeClientResult<()> {
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        validate_client_order_id("kraken", request.market_type, client_order_id).map_err(
            |error| ExchangeClientError::Validation {
                field: "client_order_id",
                reason: error.to_string(),
            },
        )?;
    } else {
        request.client_order_id = Some(KrakenSpotClient::generate_client_order_id());
    }
    Ok(())
}

fn kraken_order_params(
    request: &OrderRequest,
    exchange_symbol: &str,
) -> ExchangeClientResult<BTreeMap<String, String>> {
    let mut params = BTreeMap::new();
    params.insert("pair".to_string(), exchange_symbol.to_string());
    params.insert(
        "type".to_string(),
        match request.side {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        }
        .to_string(),
    );
    let ordertype = match request.order_type {
        OrderType::Market => "market",
        OrderType::Limit | OrderType::PostOnly | OrderType::IOC | OrderType::FOK => "limit",
    };
    params.insert("ordertype".to_string(), ordertype.to_string());
    params.insert("volume".to_string(), request.quantity.to_string());
    if request.order_type.is_limit_price_required() {
        params.insert(
            "price".to_string(),
            request
                .price
                .ok_or_else(|| ExchangeClientError::Validation {
                    field: "price",
                    reason: "price is required for Kraken limit-style orders".to_string(),
                })?
                .to_string(),
        );
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        params.insert("cl_ord_id".to_string(), client_order_id.to_string());
    }
    match request.order_type {
        OrderType::PostOnly => {
            params.insert("oflags".to_string(), "post".to_string());
        }
        OrderType::IOC => {
            params.insert("timeinforce".to_string(), "IOC".to_string());
        }
        OrderType::FOK => {
            params.insert("timeinforce".to_string(), "GTC".to_string());
            params.insert("oflags".to_string(), "fciq".to_string());
        }
        _ => {
            if let Some(time_in_force) = request.time_in_force {
                let tif = match time_in_force {
                    TimeInForce::GTC => "GTC",
                    TimeInForce::IOC => "IOC",
                    TimeInForce::FOK => {
                        return Err(ExchangeClientError::Unsupported(
                            "Kraken Spot REST has no native FOK time-in-force".to_string(),
                        ))
                    }
                    TimeInForce::GTX => {
                        params.insert("oflags".to_string(), "post".to_string());
                        "GTC"
                    }
                };
                params.insert("timeinforce".to_string(), tif.to_string());
            }
        }
    }
    Ok(params)
}

async fn parse_response(response: reqwest::Response) -> ExchangeClientResult<Value> {
    let status = response.status();
    let body = response.text().await.map_err(CoreExchangeError::from)?;
    let value: Value = serde_json::from_str(&body).map_err(CoreExchangeError::from)?;
    if !status.is_success() {
        return Err(classified(
            classify_message(&body),
            Some(status.as_u16().to_string()),
            format!("Kraken HTTP {status}: {body}"),
        ));
    }
    let errors = value
        .get("error")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter(|message| !message.trim().is_empty())
        .collect::<Vec<_>>();
    if let Some(message) = errors.first() {
        return Err(classified(
            classify_message(message),
            None,
            errors.join("; "),
        ));
    }
    Ok(value.get("result").cloned().unwrap_or(value))
}

fn classified(
    class: ExchangeErrorClass,
    code: Option<String>,
    message: impl Into<String>,
) -> ExchangeClientError {
    ExchangeClientError::Classified(ExchangeError {
        exchange: "kraken".to_string(),
        class,
        code,
        message: message.into(),
    })
}

fn classify_message(message: &str) -> ExchangeErrorClass {
    let lower = message.to_ascii_lowercase();
    if lower.contains("rate limit") || lower.contains("too many") {
        ExchangeErrorClass::RateLimited
    } else if lower.contains("invalid key")
        || lower.contains("invalid signature")
        || lower.contains("invalid nonce")
        || lower.contains("permission denied")
    {
        ExchangeErrorClass::AuthenticationFailed
    } else if lower.contains("permission") {
        ExchangeErrorClass::PermissionDenied
    } else if lower.contains("insufficient") {
        ExchangeErrorClass::InsufficientBalance
    } else if lower.contains("unknown order") || lower.contains("order not found") {
        ExchangeErrorClass::OrderNotFound
    } else if lower.contains("invalid price") {
        ExchangeErrorClass::InvalidPrecision
    } else if lower.contains("unknown asset pair") || lower.contains("invalid pair") {
        ExchangeErrorClass::InvalidSymbol
    } else if lower.contains("service") || lower.contains("unavailable") {
        ExchangeErrorClass::ExchangeUnavailable
    } else {
        ExchangeErrorClass::Unknown
    }
}

fn normalize_kraken_symbol(
    symbol: &str,
    aliases: &HashMap<String, String>,
) -> ExchangeClientResult<String> {
    let trimmed = symbol.trim();
    if trimmed.is_empty() {
        return Err(ExchangeClientError::Validation {
            field: "symbol",
            reason: "symbol must not be empty".to_string(),
        });
    }
    let normalized = trimmed
        .to_ascii_uppercase()
        .replace('-', "/")
        .replace('_', "/");
    if let Some((base, quote)) = normalized.split_once('/') {
        return Ok(format!(
            "{}/{}",
            normalize_asset(base, aliases),
            normalize_asset(quote, aliases)
        ));
    }
    for quote in ["USDT", "USDC", "USD", "EUR", "BTC", "ETH"] {
        if let Some(base) = normalized.strip_suffix(quote) {
            if !base.is_empty() {
                return Ok(format!(
                    "{}/{}",
                    normalize_asset(base, aliases),
                    normalize_asset(quote, aliases)
                ));
            }
        }
    }
    Err(ExchangeClientError::Validation {
        field: "symbol",
        reason: format!("unable to normalize Kraken symbol: {symbol}"),
    })
}

fn normalize_asset(asset: &str, aliases: &HashMap<String, String>) -> String {
    let raw = asset.trim().to_ascii_uppercase();
    let candidate = aliases.get(&raw).cloned().unwrap_or_else(|| {
        if raw.len() == 4 && (raw.starts_with('X') || raw.starts_with('Z')) {
            raw[1..].to_string()
        } else {
            raw.clone()
        }
    });
    match candidate.as_str() {
        "XBT" => "BTC".to_string(),
        "XDG" => "DOGE".to_string(),
        value => value.to_string(),
    }
}

fn to_kraken_pair_code(normalized: &str) -> String {
    normalized
        .replace("BTC", "XBT")
        .replace("DOGE", "XDG")
        .replace('/', "")
        .to_ascii_uppercase()
}

fn parse_symbol_rules(value: &Value) -> ExchangeClientResult<Vec<SymbolRule>> {
    let pairs = value.as_object().ok_or_else(|| {
        classified(
            ExchangeErrorClass::Unknown,
            None,
            "Kraken AssetPairs response missing result object",
        )
    })?;
    Ok(pairs
        .iter()
        .filter_map(|(pair_code, pair)| parse_symbol_rule(pair_code, pair))
        .collect())
}

fn parse_symbol_rule(pair_code: &str, value: &Value) -> Option<SymbolRule> {
    let wsname = value.get("wsname").and_then(Value::as_str);
    let altname = value
        .get("altname")
        .and_then(Value::as_str)
        .unwrap_or(pair_code);
    let normalized = wsname
        .and_then(|name| normalize_kraken_symbol(name, &HashMap::new()).ok())
        .or_else(|| normalize_kraken_symbol(altname, &HashMap::new()).ok())?;
    let (base, quote) = normalized.split_once('/')?;
    let base_asset = base.to_string();
    let quote_asset = quote.to_string();
    let pair_decimals = value
        .get("pair_decimals")
        .and_then(parse_json_u32)
        .unwrap_or(8);
    let lot_decimals = value
        .get("lot_decimals")
        .and_then(parse_json_u32)
        .unwrap_or(8);
    let tick_size = value
        .get("tick_size")
        .and_then(parse_json_f64)
        .unwrap_or_else(|| 10f64.powi(-(pair_decimals as i32)));
    let step_size = value
        .get("lot_multiplier")
        .and_then(parse_json_f64)
        .filter(|value| *value > 0.0)
        .unwrap_or_else(|| 10f64.powi(-(lot_decimals as i32)));
    let min_quantity = value
        .get("ordermin")
        .and_then(parse_json_f64)
        .unwrap_or(0.0);
    let min_notional = value.get("costmin").and_then(parse_json_f64).unwrap_or(0.0);
    Some(SymbolRule {
        exchange: "kraken".to_string(),
        market_type: MarketType::Spot,
        internal_symbol: normalized,
        exchange_symbol: altname.to_ascii_uppercase(),
        base_asset,
        quote_asset,
        price_precision: pair_decimals,
        quantity_precision: lot_decimals,
        tick_size,
        step_size,
        min_quantity,
        min_notional,
        max_quantity: None,
        supported_order_types: vec![
            OrderType::Market,
            OrderType::Limit,
            OrderType::PostOnly,
            OrderType::IOC,
        ],
        supported_time_in_force: vec![TimeInForce::GTC, TimeInForce::IOC, TimeInForce::GTX],
        status: SymbolStatus::Trading,
        raw_metadata: Some(value.clone()),
    })
}

fn parse_balance_snapshot(value: &Value) -> ExchangeClientResult<BalanceSnapshot> {
    let balances = value.as_object().ok_or_else(|| {
        classified(
            ExchangeErrorClass::Unknown,
            None,
            "Kraken balance response missing result object",
        )
    })?;
    Ok(BalanceSnapshot {
        exchange: "kraken".to_string(),
        market_type: MarketType::Spot,
        balances: balances
            .iter()
            .filter_map(|(asset, amount)| parse_balance(asset, amount))
            .filter(|balance| balance.total > 0.0 || balance.locked > 0.0)
            .collect(),
        timestamp: Utc::now(),
    })
}

fn parse_balance(asset: &str, value: &Value) -> Option<AssetBalance> {
    if let Some(total) = parse_json_f64(value) {
        return Some(AssetBalance::new(
            normalize_asset(asset, &HashMap::new()),
            total,
            total,
            0.0,
        ));
    }
    let total = value
        .get("balance")
        .or_else(|| value.get("total"))
        .and_then(parse_json_f64)?;
    let locked = value
        .get("hold_trade")
        .or_else(|| value.get("hold"))
        .and_then(parse_json_f64)
        .unwrap_or(0.0);
    Some(AssetBalance::new(
        normalize_asset(asset, &HashMap::new()),
        total,
        (total - locked).max(0.0),
        locked,
    ))
}

fn parse_orderbook_snapshot(
    value: &Value,
    symbol: &str,
    stale_book_ms: u64,
) -> ExchangeClientResult<OrderBookSnapshot> {
    let book_value = value
        .as_object()
        .and_then(|object| object.values().next())
        .ok_or_else(|| {
            classified(
                ExchangeErrorClass::Unknown,
                None,
                "Kraken Depth response missing book object",
            )
        })?;
    let bids = parse_kraken_levels(book_value.get("bids"));
    let asks = parse_kraken_levels(book_value.get("asks"));
    let exchange_timestamp = book_value
        .get("bids")
        .and_then(Value::as_array)
        .and_then(|levels| levels.first())
        .and_then(Value::as_array)
        .and_then(|level| level.get(2))
        .and_then(parse_json_f64)
        .and_then(timestamp_from_seconds);
    let received_at = Utc::now();
    Ok(OrderBookSnapshot {
        exchange: "kraken".to_string(),
        market_type: MarketType::Spot,
        symbol: symbol.to_string(),
        best_bid: bids.first().map(|level| level.price),
        best_ask: asks.first().map(|level| level.price),
        bids,
        asks,
        exchange_timestamp,
        received_at,
        latency_ms: exchange_timestamp
            .map(|ts| received_at.signed_duration_since(ts).num_milliseconds()),
        sequence: None,
        is_stale: exchange_timestamp
            .map(|ts| {
                received_at.signed_duration_since(ts).num_milliseconds() > stale_book_ms as i64
            })
            .unwrap_or(false),
    })
}

fn parse_kraken_levels(value: Option<&Value>) -> Vec<OrderBookLevel> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_array)
        .filter_map(|level| {
            Some(OrderBookLevel {
                price: level.first().and_then(parse_json_f64)?,
                quantity: level.get(1).and_then(parse_json_f64)?,
            })
        })
        .collect()
}

fn parse_order_ack(
    value: &Value,
    request: &OrderRequest,
    symbol: &str,
) -> ExchangeClientResult<OrderResponse> {
    let order_id = value
        .get("txid")
        .and_then(Value::as_array)
        .and_then(|ids| ids.first())
        .and_then(Value::as_str)
        .ok_or_else(|| {
            classified(
                ExchangeErrorClass::Unknown,
                None,
                "Kraken AddOrder response missing txid",
            )
        })?;
    Ok(OrderResponse {
        exchange: "kraken".to_string(),
        market_type: MarketType::Spot,
        symbol: symbol.to_string(),
        order_id: order_id.to_string(),
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
    })
}

fn parse_quote_order_ack(
    value: &Value,
    request: &QuoteMarketOrderRequest,
    symbol: &str,
) -> ExchangeClientResult<OrderResponse> {
    let order_id = value
        .get("txid")
        .and_then(Value::as_array)
        .and_then(|ids| ids.first())
        .and_then(Value::as_str)
        .ok_or_else(|| {
            classified(
                ExchangeErrorClass::Unknown,
                None,
                "Kraken AddOrder response missing txid",
            )
        })?;
    Ok(OrderResponse {
        exchange: "kraken".to_string(),
        market_type: MarketType::Spot,
        symbol: symbol.to_string(),
        order_id: order_id.to_string(),
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
    })
}

fn parse_single_order(
    value: &Value,
    fallback_symbol: &str,
    fallback_order_id: Option<&str>,
) -> ExchangeClientResult<OrderResponse> {
    let order = value
        .as_object()
        .and_then(|object| object.iter().next())
        .ok_or_else(|| {
            classified(
                ExchangeErrorClass::OrderNotFound,
                fallback_order_id.map(str::to_string),
                "Kraken QueryOrders response did not contain the requested order",
            )
        })?;
    parse_order_response(order.0, order.1, Some(fallback_symbol))
}

fn parse_open_orders(value: &Value) -> ExchangeClientResult<Vec<OrderResponse>> {
    let open = value
        .get("open")
        .and_then(Value::as_object)
        .or_else(|| value.as_object())
        .ok_or_else(|| {
            classified(
                ExchangeErrorClass::Unknown,
                None,
                "Kraken OpenOrders response missing open object",
            )
        })?;
    open.iter()
        .map(|(order_id, order)| parse_order_response(order_id, order, None))
        .collect()
}

fn parse_order_response(
    order_id: &str,
    value: &Value,
    fallback_symbol: Option<&str>,
) -> ExchangeClientResult<OrderResponse> {
    let descr = value.get("descr").unwrap_or(value);
    let symbol = descr
        .get("pair")
        .and_then(Value::as_str)
        .and_then(|pair| normalize_kraken_symbol(pair, &HashMap::new()).ok())
        .or_else(|| fallback_symbol.map(str::to_string))
        .unwrap_or_else(|| "UNKNOWN/UNKNOWN".to_string());
    let side = match descr
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("buy")
        .to_ascii_lowercase()
        .as_str()
    {
        "sell" => OrderSide::Sell,
        _ => OrderSide::Buy,
    };
    let order_type = match descr
        .get("ordertype")
        .and_then(Value::as_str)
        .unwrap_or("limit")
        .to_ascii_lowercase()
        .as_str()
    {
        "market" => OrderType::Market,
        _ => OrderType::Limit,
    };
    Ok(OrderResponse {
        exchange: "kraken".to_string(),
        market_type: MarketType::Spot,
        symbol,
        order_id: order_id.to_string(),
        client_order_id: value
            .get("cl_ord_id")
            .or_else(|| value.get("userref"))
            .and_then(value_to_string),
        side,
        position_side: PositionSide::None,
        order_type,
        status: OrderStatus::from_exchange_status(
            value
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
        ),
        price: descr
            .get("price")
            .or_else(|| value.get("price"))
            .and_then(parse_json_f64),
        quantity: value.get("vol").and_then(parse_json_f64).unwrap_or(0.0),
        filled_quantity: value
            .get("vol_exec")
            .and_then(parse_json_f64)
            .unwrap_or(0.0),
        average_price: value.get("price").and_then(parse_json_f64),
        created_at: value
            .get("opentm")
            .and_then(parse_json_f64)
            .and_then(timestamp_from_seconds)
            .unwrap_or_else(Utc::now),
        updated_at: value
            .get("closetm")
            .or_else(|| value.get("lastupdated"))
            .and_then(parse_json_f64)
            .and_then(timestamp_from_seconds),
    })
}

fn parse_fee_rate(value: &Value) -> Option<FeeRate> {
    let fees = value
        .get("fees")
        .and_then(Value::as_object)
        .or_else(|| value.get("fees_maker").and_then(Value::as_object))?;
    let first = fees.values().next()?;
    let taker = first
        .get("fee")
        .or_else(|| first.get("taker"))
        .and_then(parse_json_f64)
        .unwrap_or(0.26)
        / 100.0;
    let maker = first
        .get("fee_maker")
        .or_else(|| first.get("maker"))
        .and_then(parse_json_f64)
        .unwrap_or(0.16)
        / 100.0;
    Some(FeeRate::new(maker, taker, FeeRateSource::ExchangeApi))
}

fn parse_recent_fills(value: &Value) -> ExchangeClientResult<Vec<TradeFill>> {
    let trades = value
        .get("trades")
        .and_then(Value::as_object)
        .or_else(|| value.as_object())
        .ok_or_else(|| {
            classified(
                ExchangeErrorClass::Unknown,
                None,
                "Kraken TradesHistory response missing trades object",
            )
        })?;
    Ok(trades
        .iter()
        .filter_map(|(trade_id, trade)| parse_trade_fill(trade_id, trade))
        .collect())
}

fn parse_trade_fill(trade_id: &str, value: &Value) -> Option<TradeFill> {
    let symbol = value
        .get("pair")
        .and_then(Value::as_str)
        .and_then(|pair| normalize_kraken_symbol(pair, &HashMap::new()).ok())?;
    let side = match value.get("type").and_then(Value::as_str).unwrap_or("buy") {
        "sell" => OrderSide::Sell,
        _ => OrderSide::Buy,
    };
    Some(TradeFill {
        exchange: "kraken".to_string(),
        market_type: MarketType::Spot,
        symbol,
        trade_id: Some(trade_id.to_string()),
        order_id: value.get("ordertxid").and_then(value_to_string),
        client_order_id: value.get("cl_ord_id").and_then(value_to_string),
        side,
        price: value.get("price").and_then(parse_json_f64)?,
        quantity: value.get("vol").and_then(parse_json_f64)?,
        fee_asset: None,
        fee_amount: value.get("fee").and_then(parse_json_f64),
        liquidity: match value.get("maker").and_then(Value::as_bool) {
            Some(true) => LiquidityRole::Maker,
            Some(false) => LiquidityRole::Taker,
            None => LiquidityRole::Unknown,
        },
        timestamp: value
            .get("time")
            .and_then(parse_json_f64)
            .and_then(timestamp_from_seconds)
            .unwrap_or_else(Utc::now),
    })
}

fn parse_json_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}

fn parse_json_u32(value: &Value) -> Option<u32> {
    match value {
        Value::Number(number) => number.as_u64().and_then(|value| u32::try_from(value).ok()),
        Value::String(text) => text.parse::<u32>().ok(),
        _ => None,
    }
}

fn parse_json_usize(value: &Value) -> Option<usize> {
    match value {
        Value::Number(number) => number
            .as_u64()
            .and_then(|value| usize::try_from(value).ok()),
        Value::String(text) => text.parse::<usize>().ok(),
        _ => None,
    }
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn timestamp_from_seconds(value: f64) -> Option<DateTime<Utc>> {
    if !value.is_finite() {
        return None;
    }
    let seconds = value.trunc() as i64;
    let nanos = ((value.fract().abs()) * 1_000_000_000.0).round() as u32;
    DateTime::<Utc>::from_timestamp(seconds, nanos)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn kraken_signature_should_match_official_vector() {
        let signature = sign_kraken_spot_request(
            "kQH5HW/8p1uGOVjbgWA7FunAmGO8lsSUXNsu3eow76sz84Q18fWxnyRzBHCd3pd5nE9qa99HAZtuZuj6F1huXg==",
            "/0/private/AddOrder",
            "nonce=1616492376594&ordertype=limit&pair=XBTUSD&price=37500&type=buy&volume=1.25",
        )
        .unwrap();

        assert_eq!(
            signature,
            "4/dpxb3iT4tp/ZCVEwSnEsLxx0bqyhLpdfOpc6fn7OR8+UClSV5n9E6aSS8MPtnRfp32bAb0nmbRn6H8ndwLUQ=="
        );
    }

    #[test]
    fn kraken_symbol_rules_should_parse_asset_pairs() {
        let value = json!({
            "XXBTZUSD": {
                "altname": "XBTUSD",
                "wsname": "XBT/USD",
                "pair_decimals": 1,
                "lot_decimals": 8,
                "ordermin": "0.00005",
                "costmin": "0.5",
                "tick_size": "0.1"
            }
        });

        let rules = parse_symbol_rules(&value).unwrap();

        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].internal_symbol, "BTC/USD");
        assert_eq!(rules[0].exchange_symbol, "XBTUSD");
        assert_eq!(rules[0].tick_size, 0.1);
        assert_eq!(rules[0].min_quantity, 0.00005);
    }

    #[test]
    fn kraken_depth_should_parse_snapshot() {
        let value = json!({
            "XXBTZUSD": {
                "bids": [["37500.0", "1.25", "1700000000.123"]],
                "asks": [["37501.0", "0.5", "1700000000.124"]]
            }
        });

        let book = parse_orderbook_snapshot(&value, "BTC/USD", 10_000).unwrap();

        assert_eq!(book.symbol, "BTC/USD");
        assert_eq!(book.best_bid, Some(37500.0));
        assert_eq!(book.best_ask, Some(37501.0));
        assert_eq!(book.bids.len(), 1);
    }

    #[tokio::test]
    async fn kraken_place_order_should_ack_in_dry_run() {
        let client = KrakenSpotClient::new(KrakenSpotConfig {
            dry_run: true,
            ..KrakenSpotConfig::default()
        });
        let order = client
            .place_order(OrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTC/USD".to_string(),
                side: OrderSide::Buy,
                position_side: PositionSide::None,
                order_type: OrderType::Limit,
                time_in_force: Some(TimeInForce::GTC),
                quantity: 0.01,
                price: Some(10_000.0),
                client_order_id: None,
                reduce_only: false,
            })
            .await
            .unwrap();

        assert_eq!(order.exchange, "kraken");
        assert_eq!(order.symbol, "BTC/USD");
        assert_eq!(order.status, OrderStatus::New);
        assert!(order.client_order_id.is_some());
    }

    #[test]
    fn kraken_signed_spec_should_include_required_headers_and_body() {
        let client = KrakenSpotClient::new(KrakenSpotConfig {
            api_key: "key".to_string(),
            api_secret: "kQH5HW/8p1uGOVjbgWA7FunAmGO8lsSUXNsu3eow76sz84Q18fWxnyRzBHCd3pd5nE9qa99HAZtuZuj6F1huXg==".to_string(),
            dry_run: true,
            ..KrakenSpotConfig::default()
        });
        let mut params = BTreeMap::new();
        params.insert("pair".to_string(), "XBTUSD".to_string());

        let spec = client
            .build_signed_request_spec("Balance", params, "1616492376594")
            .unwrap();

        assert_eq!(spec.method, Method::POST);
        assert_eq!(spec.path, "/0/private/Balance");
        assert!(spec.body.contains("nonce=1616492376594"));
        assert!(spec.body.contains("pair=XBTUSD"));
        assert_eq!(spec.api_key, "key");
        assert!(!spec.api_sign.is_empty());
    }
}
