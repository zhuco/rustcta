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
            http: crate::core::http2_fix::shared_http_client(),
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
            .header("locale", "en-US")
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
                let fee_rate = match self.get_fee_rate(&request.symbol).await {
                    Ok(fee_rate) => fee_rate.taker_fee_rate.max(0.0),
                    Err(error) => {
                        log::warn!(
                            "bitget fee-rate lookup failed during reservation for {}; using default taker fee: {}",
                            request.symbol,
                            error
                        );
                        0.001
                    }
                };
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

    fn dry_run_quote_market_order_response(
        &self,
        request: &QuoteMarketOrderRequest,
        symbol: &str,
    ) -> OrderResponse {
        OrderResponse {
            exchange: "bitget".to_string(),
            market_type: MarketType::Spot,
            symbol: symbol.to_string(),
            order_id: format!("dry-bitget-{}", Utc::now().timestamp_millis()),
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
            exchange: "bitget".to_string(),
            market_type: MarketType::Spot,
            symbol: symbol.to_string(),
            order_id: request
                .order_id()
                .map(str::to_string)
                .unwrap_or_else(|| format!("dry-bitget-{}", Utc::now().timestamp_millis())),
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
impl ExchangeClient for BitgetSpotClient {
    fn market_type(&self) -> MarketType {
        MarketType::Spot
    }

    fn exchange_name(&self) -> &str {
        "bitget"
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let mut capabilities = ExchangeClientCapabilities::spot(self.exchange_name());
        capabilities.supports_cancel_all_orders = true;
        capabilities.supports_quote_market_order = true;
        capabilities.supports_amend_order = true;
        capabilities
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
        validate_orderbook_depth(depth)?;
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
        let body = bitget_order_body(&request, &symbol)?;
        if self.config.dry_run {
            return Ok(self.dry_run_order_response(&request, &symbol));
        }
        let rule = self
            .get_symbol_rule(&symbol)
            .await?
            .unwrap_or_else(|| fallback_rule("bitget", &symbol));
        validate_order_against_symbol_rule(&request, &rule)?;
        let mut reservation = self.reserve_for_order(&request, &rule).await?;

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
            Err(v2_error) if is_bitget_url_not_found(&v2_error) => {
                log::warn!(
                    "bitget spot v2 place-order returned 40404; retrying UTA v3 spot place-order"
                );
                let body = bitget_uta_order_body(&request, &symbol)?;
                let response = self
                    .send_signed_request(
                        Method::POST,
                        "/api/v3/trade/place-order",
                        HashMap::new(),
                        Some(body),
                    )
                    .await;
                match response {
                    Ok(value) => parse_order_response(&value, Some(&request)),
                    Err(v3_error) => {
                        if let Some(reservation) = reservation.as_mut() {
                            let _ = self.reservations.release(reservation);
                        }
                        Err(ExchangeClientError::Classified(ExchangeError {
                            exchange: "bitget".to_string(),
                            class: ExchangeErrorClass::Unknown,
                            code: None,
                            message: format!(
                                "bitget spot place-order failed on v2 ({v2_error}); UTA v3 fallback also failed ({v3_error})"
                            ),
                        }))
                    }
                }
            }
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
                "Bitget Spot quote-sized market orders are only supported for market buys"
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
            .unwrap_or_else(|| fallback_rule("bitget", &symbol));
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

        let body = bitget_quote_market_order_body(&request, &symbol)?;
        let response = self
            .send_signed_request(
                Method::POST,
                "/api/v2/spot/trade/place-order",
                HashMap::new(),
                Some(body),
            )
            .await;
        match response {
            Ok(value) => {
                let context = quote_market_context(&request);
                parse_order_response(&value, Some(&context))
            }
            Err(v2_error) if is_bitget_url_not_found(&v2_error) => {
                log::warn!(
                    "bitget spot v2 quote market place-order returned 40404; retrying UTA v3 spot place-order"
                );
                let body = bitget_uta_quote_market_order_body(&request, &symbol)?;
                let response = self
                    .send_signed_request(
                        Method::POST,
                        "/api/v3/trade/place-order",
                        HashMap::new(),
                        Some(body),
                    )
                    .await;
                match response {
                    Ok(value) => {
                        let context = quote_market_context(&request);
                        parse_order_response(&value, Some(&context))
                    }
                    Err(v3_error) => {
                        if let Some(reservation) = reservation.as_mut() {
                            let _ = self.reservations.release(reservation);
                        }
                        Err(ExchangeClientError::Classified(ExchangeError {
                            exchange: "bitget".to_string(),
                            class: ExchangeErrorClass::Unknown,
                            code: None,
                            message: format!(
                                "bitget spot quote market place-order failed on v2 ({v2_error}); UTA v3 fallback also failed ({v3_error})"
                            ),
                        }))
                    }
                }
            }
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
                reason: "BitgetSpotClient only supports spot".to_string(),
            });
        }
        ensure_amend_client_order_id(&request)?;
        if request.new_client_order_id().is_some() {
            return Err(ExchangeClientError::Unsupported(
                "Bitget Spot UTA modify-order does not support assigning a new client order id"
                    .to_string(),
            ));
        }
        let symbol = self.normalize_symbol(&request.symbol)?;
        request.symbol = symbol.clone();
        if self.config.dry_run {
            return Ok(self.dry_run_amend_order_response(&request, &symbol));
        }

        let body = bitget_uta_amend_order_body(&request, &symbol)?;
        let value = self
            .send_signed_request(
                Method::POST,
                "/api/v3/trade/modify-order",
                HashMap::new(),
                Some(body),
            )
            .await?;
        parse_amend_order_response(&value, &request, &symbol)
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeClientResult<CancelOrderResponse> {
        request.validate()?;
        if request.market_type != MarketType::Spot {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: "BitgetSpotClient only supports spot".to_string(),
            });
        }
        ensure_cancel_client_order_id(&request)?;
        let symbol = self.normalize_symbol(&request.symbol)?;
        if self.config.dry_run {
            return Ok(CancelOrderResponse {
                exchange: "bitget".to_string(),
                market_type: MarketType::Spot,
                symbol,
                order_id: request.order_id().map(str::to_string),
                client_order_id: request.client_order_id().map(str::to_string),
                status: OrderStatus::Cancelled,
                cancelled_at: Utc::now(),
            });
        }
        let body = json!({
            "symbol": symbol,
            "orderId": request.order_id().unwrap_or_default(),
            "clientOid": request.client_order_id().unwrap_or_default(),
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
            order_id: value_as_string(data.get("orderId"))
                .or_else(|| request.order_id().map(str::to_string)),
            client_order_id: value_as_string(data.get("clientOid"))
                .or_else(|| request.client_order_id().map(str::to_string)),
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
                reason: "BitgetSpotClient only supports spot".to_string(),
            });
        }
        let symbol = self.normalize_symbol(request.validate_symbol_required()?)?;
        if self.config.dry_run {
            return Ok(CancelAllOrdersResponse {
                exchange: "bitget".to_string(),
                market_type: MarketType::Spot,
                symbol: Some(symbol),
                cancelled_orders: 0,
                cancelled_at: Utc::now(),
            });
        }
        let value = self
            .send_signed_request(
                Method::POST,
                "/api/v2/spot/trade/cancel-symbol-order",
                HashMap::new(),
                Some(bitget_cancel_all_body(&symbol)),
            )
            .await?;
        Ok(CancelAllOrdersResponse {
            exchange: "bitget".to_string(),
            market_type: MarketType::Spot,
            symbol: Some(
                bitget_cancel_all_symbol(&value)
                    .unwrap_or_else(|| symbol.clone())
                    .replace(['-', '_', '/'], "")
                    .to_ascii_uppercase(),
            ),
            cancelled_orders: 0,
            cancelled_at: Utc::now(),
        })
    }

    async fn get_order(&self, symbol: &str, order_id: &str) -> ExchangeClientResult<OrderResponse> {
        let symbol = self.normalize_symbol(symbol)?;
        let order_id = validate_order_lookup_id(order_id)?;
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
        params.insert("category".to_string(), "SPOT".to_string());
        let value = match self
            .send_signed_request(Method::GET, "/api/v3/account/fee-rate", params, None)
            .await
        {
            Ok(value) => value,
            Err(v3_error) => {
                let mut fallback_params = HashMap::new();
                fallback_params.insert("symbol".to_string(), self.normalize_symbol(symbol)?);
                fallback_params.insert("businessType".to_string(), "spot".to_string());
                match self
                    .send_signed_request(
                        Method::GET,
                        "/api/v2/common/trade-rate",
                        fallback_params,
                        None,
                    )
                    .await
                {
                    Ok(value) => value,
                    Err(v2_error) => {
                        return Err(ExchangeClientError::Classified(ExchangeError {
                            exchange: "bitget".to_string(),
                            class: ExchangeErrorClass::Unknown,
                            code: None,
                            message: format!(
                                "bitget fee-rate lookup failed on v3 ({v3_error}); v2 fallback also failed ({v2_error})"
                            ),
                        }));
                    }
                }
            }
        };
        parse_fee_rate(&value)
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
    .filter(|value| *value > 0.0)
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

pub fn parse_private_stream_message(text: &str) -> ExchangeClientResult<Vec<UserStreamEvent>> {
    if text.trim() == "pong" {
        return Ok(Vec::new());
    }
    let value: Value = serde_json::from_str(text).map_err(CoreExchangeError::from)?;
    if value.get("event").is_some() || value.get("op").is_some() {
        return Ok(Vec::new());
    }
    let channel = value
        .get("arg")
        .and_then(|arg| arg.get("channel"))
        .or_else(|| value.get("channel"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let data = value.get("data").cloned().unwrap_or(Value::Null);
    let items = match data {
        Value::Array(items) => items,
        Value::Object(_) => vec![data],
        _ => Vec::new(),
    };
    let mut events = Vec::new();
    for item in items {
        if channel.contains("order") {
            events.push(UserStreamEvent::Order(parse_order_response(&item, None)?));
            if item.get("tradeId").is_some() || item.get("fillId").is_some() {
                events.push(UserStreamEvent::Fill(parse_fill(&item)?));
            }
        } else if channel.contains("fill") || channel.contains("trade") {
            events.push(UserStreamEvent::Fill(parse_fill(&item)?));
        } else if channel.contains("account")
            || channel.contains("asset")
            || channel.contains("balance")
        {
            events.push(UserStreamEvent::Balance(parse_balance_snapshot(
                &Value::Array(vec![item]),
            )?));
        }
    }
    Ok(events)
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

fn parse_fee_rate(value: &Value) -> ExchangeClientResult<FeeRate> {
    let item = value.get("data").unwrap_or(value);
    Ok(FeeRate::new(
        number_from_str(item.get("makerFeeRate")).unwrap_or(0.001),
        number_from_str(item.get("takerFeeRate")).unwrap_or(0.001),
        FeeRateSource::ExchangeApi,
    ))
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
        "size": request.quantity.to_string(),
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

fn bitget_quote_market_order_body(
    request: &QuoteMarketOrderRequest,
    symbol: &str,
) -> ExchangeClientResult<Value> {
    if request.side != OrderSide::Buy {
        return Err(ExchangeClientError::Unsupported(
            "Bitget Spot quote-sized market orders are only supported for market buys".to_string(),
        ));
    }
    let mut body = json!({
        "symbol": symbol,
        "side": "buy",
        "orderType": "market",
        "size": request.quote_quantity.to_string(),
    });
    if let Some(client_order_id) = &request.client_order_id {
        body["clientOid"] = Value::String(client_order_id.clone());
    }
    Ok(body)
}

fn bitget_cancel_all_body(symbol: &str) -> Value {
    json!({ "symbol": symbol })
}

fn bitget_cancel_all_symbol(value: &Value) -> Option<String> {
    value
        .get("data")
        .unwrap_or(value)
        .get("symbol")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn bitget_uta_order_body(request: &OrderRequest, symbol: &str) -> ExchangeClientResult<Value> {
    let mut body = json!({
        "category": "SPOT",
        "symbol": symbol,
        "side": match request.side { OrderSide::Buy => "buy", OrderSide::Sell => "sell" },
        "orderType": match request.order_type { OrderType::Market => "market", _ => "limit" },
        "qty": request.quantity.to_string(),
        "timeInForce": bitget_time_in_force(request),
    });
    if let Some(price) = request.price {
        body["price"] = Value::String(price.to_string());
    }
    if let Some(client_order_id) = &request.client_order_id {
        body["clientOid"] = Value::String(client_order_id.clone());
    }
    Ok(body)
}

fn bitget_uta_quote_market_order_body(
    request: &QuoteMarketOrderRequest,
    symbol: &str,
) -> ExchangeClientResult<Value> {
    if request.side != OrderSide::Buy {
        return Err(ExchangeClientError::Unsupported(
            "Bitget Spot UTA quote-sized market orders are only supported for market buys"
                .to_string(),
        ));
    }
    let mut body = json!({
        "category": "SPOT",
        "symbol": symbol,
        "side": "buy",
        "orderType": "market",
        "qty": request.quote_quantity.to_string(),
        "timeInForce": "gtc",
    });
    if let Some(client_order_id) = &request.client_order_id {
        body["clientOid"] = Value::String(client_order_id.clone());
    }
    Ok(body)
}

fn bitget_uta_amend_order_body(
    request: &AmendOrderRequest,
    symbol: &str,
) -> ExchangeClientResult<Value> {
    if request.new_client_order_id().is_some() {
        return Err(ExchangeClientError::Unsupported(
            "Bitget Spot UTA modify-order does not support assigning a new client order id"
                .to_string(),
        ));
    }
    let mut body = json!({
        "category": "SPOT",
        "symbol": symbol,
        "qty": request.new_quantity.to_string(),
        "autoCancel": "no",
    });
    if let Some(order_id) = request.order_id() {
        body["orderId"] = Value::String(order_id.to_string());
    }
    if let Some(client_order_id) = request.client_order_id() {
        body["clientOid"] = Value::String(client_order_id.to_string());
    }
    Ok(body)
}

fn bitget_time_in_force(request: &OrderRequest) -> &'static str {
    match request.time_in_force {
        Some(TimeInForce::IOC) => "ioc",
        Some(TimeInForce::FOK) => "fok",
        Some(TimeInForce::GTX) => "post_only",
        Some(TimeInForce::GTC) | None => "gtc",
    }
}

fn is_bitget_url_not_found(error: &ExchangeClientError) -> bool {
    let text = error.to_string();
    if text.contains("40404") || text.contains("Request URL NOT FOUND") {
        return true;
    }
    matches!(
        error,
        ExchangeClientError::Classified(ExchangeError {
            code: Some(code),
            ..
        }) if code == "40404"
    )
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

fn ensure_quote_market_client_order_id(
    request: &mut QuoteMarketOrderRequest,
) -> ExchangeClientResult<()> {
    if request.client_order_id.is_none() {
        request.client_order_id = Some(BitgetSpotClient::generate_client_order_id());
    }
    if let Some(client_order_id) = &request.client_order_id {
        validate_client_order_id("bitget", request.market_type, client_order_id).map_err(
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
        validate_client_order_id("bitget", request.market_type, client_order_id).map_err(
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
        validate_client_order_id("bitget", request.market_type, client_order_id).map_err(
            |error| ExchangeClientError::Validation {
                field: "client_order_id",
                reason: error.to_string(),
            },
        )?;
    }
    Ok(())
}

fn parse_amend_order_response(
    value: &Value,
    request: &AmendOrderRequest,
    symbol: &str,
) -> ExchangeClientResult<OrderResponse> {
    let data = value.get("data").unwrap_or(value);
    let order_id = value_as_string(data.get("orderId"))
        .or_else(|| request.order_id.clone())
        .ok_or_else(|| parser_error("amend response missing orderId", value))?;
    Ok(OrderResponse {
        exchange: "bitget".to_string(),
        market_type: MarketType::Spot,
        symbol: symbol.to_string(),
        order_id,
        client_order_id: value_as_string(data.get("clientOid"))
            .or_else(|| request.client_order_id.clone()),
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
    })
}

fn quote_market_context(request: &QuoteMarketOrderRequest) -> OrderRequest {
    OrderRequest {
        market_type: request.market_type,
        symbol: request.symbol.clone(),
        side: request.side,
        position_side: PositionSide::None,
        order_type: OrderType::Market,
        time_in_force: None,
        quantity: request.quote_quantity,
        price: None,
        client_order_id: request.client_order_id.clone(),
        reduce_only: false,
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
    use std::sync::{Arc, Mutex};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    #[derive(Debug, Clone)]
    struct SeenBitgetRequest {
        method: String,
        path: String,
        query: HashMap<String, String>,
        headers: HashMap<String, String>,
        body: Option<Value>,
    }

    async fn spawn_bitget_rest_server(
        responses: Vec<(u16, Value)>,
    ) -> (String, Arc<Mutex<Vec<SeenBitgetRequest>>>) {
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
                let request = parse_seen_bitget_request(&request_text);
                seen_requests.lock().unwrap().push(request);
                let (status, body) = responses
                    .lock()
                    .unwrap()
                    .next()
                    .unwrap_or_else(|| (200, json!({"code": "00000", "data": {}})));
                let body_text = body.to_string();
                let response = format!(
                    "HTTP/1.1 {status} OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body_text.len(),
                    body_text
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });

        (format!("http://{address}"), seen)
    }

    fn ok_response(data: Value) -> (u16, Value) {
        (200, json!({"code": "00000", "data": data}))
    }

    fn parse_seen_bitget_request(request_text: &str) -> SeenBitgetRequest {
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

        SeenBitgetRequest {
            method,
            path: path.to_string(),
            query,
            headers,
            body,
        }
    }

    fn assert_signed_bitget_request(request: &SeenBitgetRequest, method: &str, path: &str) {
        assert_eq!(request.method, method);
        assert_eq!(request.path, path);
        assert_eq!(
            request.headers.get("access-key").map(String::as_str),
            Some("key")
        );
        assert!(request
            .headers
            .get("access-sign")
            .is_some_and(|value| !value.is_empty()));
        assert!(request
            .headers
            .get("access-timestamp")
            .is_some_and(|value| !value.is_empty()));
        assert_eq!(
            request.headers.get("access-passphrase").map(String::as_str),
            Some("passphrase")
        );
        assert_eq!(
            request.headers.get("locale").map(String::as_str),
            Some("en-US")
        );
        assert_eq!(
            request.headers.get("content-type").map(String::as_str),
            Some("application/json")
        );
    }

    fn bitget_test_client(base_url: String) -> BitgetSpotClient {
        BitgetSpotClient::new(BitgetSpotConfig {
            api_key: "key".to_string(),
            api_secret: "secret".to_string(),
            passphrase: "passphrase".to_string(),
            base_url,
            dry_run: false,
            fee_override: None,
            ..BitgetSpotConfig::default()
        })
    }

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
        assert!(rules[0]
            .supported_order_types
            .contains(&OrderType::PostOnly));
        assert!(rules[0].supported_time_in_force.contains(&TimeInForce::GTX));

        let fallback = fallback_rule("bitget", "BTCUSDT");
        assert!(fallback
            .supported_order_types
            .contains(&OrderType::PostOnly));
        assert!(fallback.supported_order_types.contains(&OrderType::FOK));
        assert!(fallback.supported_time_in_force.contains(&TimeInForce::GTX));
    }

    #[test]
    fn bitget_capabilities_should_advertise_cancel_all() {
        let client = BitgetSpotClient::new(BitgetSpotConfig::default());
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
    fn bitget_fee_rate_should_parse_v3_and_v2_shapes() {
        let v3 = json!({
            "code": "00000",
            "data": {
                "symbol": "BTCUSDT",
                "makerFeeRate": "0.001",
                "takerFeeRate": "0.0015"
            }
        });
        let fee = parse_fee_rate(&v3).unwrap();
        assert_eq!(fee.maker_fee_rate, 0.001);
        assert_eq!(fee.taker_fee_rate, 0.0015);
        assert_eq!(fee.source, FeeRateSource::ExchangeApi);

        let v2 = json!({
            "symbol": "BTCUSDT",
            "makerFeeRate": "0.0008",
            "takerFeeRate": "0.0012"
        });
        let fee = parse_fee_rate(&v2).unwrap();
        assert_eq!(fee.maker_fee_rate, 0.0008);
        assert_eq!(fee.taker_fee_rate, 0.0012);
        assert_eq!(fee.source, FeeRateSource::ExchangeApi);
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

    #[tokio::test]
    async fn bitget_orderbook_should_validate_depth_before_request() {
        let client = BitgetSpotClient::new(BitgetSpotConfig::default());

        let err = client.get_orderbook("BTCUSDT", 0).await.unwrap_err();

        assert!(matches!(
            err,
            ExchangeClientError::Validation { field: "depth", .. }
        ));
    }

    #[tokio::test]
    async fn bitget_spot_client_should_route_common_private_rest_readbacks() {
        let (base_url, seen) = spawn_bitget_rest_server(vec![
            ok_response(json!([
                {"coin": "BTC", "available": "0.5", "frozen": "0.1", "equity": "0.6"},
                {"coin": "USDT", "available": "123.45", "frozen": "1.55", "equity": "125"}
            ])),
            ok_response(json!({
                "orderId": "1001",
                "symbol": "BTCUSDT",
                "clientOid": "CID1001",
                "side": "buy",
                "orderType": "limit",
                "status": "partial-fill",
                "price": "65000",
                "size": "0.01",
                "filledSize": "0.006",
                "priceAvg": "65010",
                "cTime": "1743054548123",
                "uTime": "1743054550000"
            })),
            ok_response(json!([{
                "orderId": "1002",
                "symbol": "BTCUSDT",
                "clientOid": "CID1002",
                "side": "sell",
                "orderType": "limit",
                "status": "live",
                "price": "70000",
                "size": "0.02",
                "filledSize": "0",
                "cTime": "1743054548123"
            }])),
            ok_response(json!({
                "symbol": "BTCUSDT",
                "makerFeeRate": "0.001",
                "takerFeeRate": "0.0015"
            })),
            ok_response(json!({
                "fillList": [{
                    "tradeId": "9001",
                    "orderId": "1001",
                    "clientOid": "CID1001",
                    "symbol": "BTCUSDT",
                    "side": "buy",
                    "price": "65010",
                    "size": "0.006",
                    "fee": "0.39",
                    "feeCcy": "USDT",
                    "tradeScope": "taker",
                    "cTime": "1743054550000"
                }]
            })),
        ])
        .await;
        let client = bitget_test_client(base_url);

        let balances = client.get_balances().await.unwrap();
        assert_eq!(balances.exchange, "bitget");
        assert_eq!(balances.market_type, MarketType::Spot);
        assert_eq!(balances.balances.len(), 2);
        assert_eq!(balances.balances[0].asset, "BTC");
        assert_eq!(balances.balances[0].available, 0.5);
        assert_eq!(balances.balances[0].locked_by_exchange, 0.1);

        let order = client.get_order("BTCUSDT", "1001").await.unwrap();
        assert_eq!(order.exchange, "bitget");
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
        assert_eq!(fill.exchange, "bitget");
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
        assert_eq!(fill.liquidity, LiquidityRole::Taker);

        let requests = seen.lock().unwrap().clone();
        assert_eq!(requests.len(), 5);
        assert_signed_bitget_request(&requests[0], "GET", "/api/v2/spot/account/assets");
        assert!(requests[0].query.is_empty());

        assert_signed_bitget_request(&requests[1], "GET", "/api/v2/spot/trade/orderInfo");
        assert_eq!(
            requests[1].query.get("orderId").map(String::as_str),
            Some("1001")
        );
        assert_eq!(
            requests[1].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );

        assert_signed_bitget_request(&requests[2], "GET", "/api/v2/spot/trade/unfilled-orders");
        assert_eq!(
            requests[2].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );

        assert_signed_bitget_request(&requests[3], "GET", "/api/v3/account/fee-rate");
        assert_eq!(
            requests[3].query.get("category").map(String::as_str),
            Some("SPOT")
        );
        assert_eq!(
            requests[3].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );

        assert_signed_bitget_request(&requests[4], "GET", "/api/v2/spot/trade/fills");
        assert_eq!(
            requests[4].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
    }

    #[tokio::test]
    async fn bitget_spot_client_should_fallback_fee_rate_to_v2() {
        let (base_url, seen) = spawn_bitget_rest_server(vec![
            (
                200,
                json!({"code": "40404", "msg": "Request URL NOT FOUND"}),
            ),
            ok_response(json!({
                "symbol": "BTCUSDT",
                "makerFeeRate": "0.0008",
                "takerFeeRate": "0.0012"
            })),
        ])
        .await;
        let client = bitget_test_client(base_url);

        let fee_rate = client.get_fee_rate("BTCUSDT").await.unwrap();

        assert_eq!(fee_rate.maker, 0.0008);
        assert_eq!(fee_rate.taker, 0.0012);
        assert_eq!(fee_rate.source, FeeRateSource::ExchangeApi);

        let requests = seen.lock().unwrap().clone();
        assert_eq!(requests.len(), 2);
        assert_signed_bitget_request(&requests[0], "GET", "/api/v3/account/fee-rate");
        assert_eq!(
            requests[0].query.get("category").map(String::as_str),
            Some("SPOT")
        );
        assert_eq!(
            requests[0].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );

        assert_signed_bitget_request(&requests[1], "GET", "/api/v2/common/trade-rate");
        assert_eq!(
            requests[1].query.get("businessType").map(String::as_str),
            Some("spot")
        );
        assert_eq!(
            requests[1].query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
    }

    #[tokio::test]
    async fn bitget_spot_client_should_route_order_mutations() {
        let (base_url, seen) = spawn_bitget_rest_server(vec![
            ok_response(json!([{
                "symbol": "HYPEUSDT",
                "baseCoin": "HYPE",
                "quoteCoin": "USDT",
                "pricePrecision": "2",
                "quantityPrecision": "6",
                "minTradeAmount": "0.0001",
                "minTradeUSDT": "1",
                "status": "online"
            }])),
            ok_response(json!([
                {"coin": "HYPE", "available": "100", "frozen": "0", "equity": "100"},
                {"coin": "USDT", "available": "1000", "frozen": "0", "equity": "1000"}
            ])),
            ok_response(json!({
                "symbol": "HYPEUSDT",
                "makerFeeRate": "0.001",
                "takerFeeRate": "0.0015"
            })),
            ok_response(json!({
                "orderId": "2001",
                "clientOid": "LIMIT1",
                "symbol": "HYPEUSDT",
                "side": "buy",
                "orderType": "limit",
                "price": "10",
                "size": "1.25",
                "status": "new",
                "cTime": "1743054548123"
            })),
            ok_response(json!([{
                "symbol": "HYPEUSDT",
                "baseCoin": "HYPE",
                "quoteCoin": "USDT",
                "pricePrecision": "2",
                "quantityPrecision": "6",
                "minTradeAmount": "0.0001",
                "minTradeUSDT": "1",
                "status": "online"
            }])),
            ok_response(json!([
                {"coin": "HYPE", "available": "100", "frozen": "0", "equity": "100"},
                {"coin": "USDT", "available": "1000", "frozen": "0", "equity": "1000"}
            ])),
            ok_response(json!({
                "orderId": "2002",
                "clientOid": "QUOTE1",
                "symbol": "HYPEUSDT",
                "side": "buy",
                "orderType": "market",
                "size": "25.5",
                "status": "new",
                "cTime": "1743054549123"
            })),
            ok_response(json!({
                "orderId": "2001",
                "clientOid": "LIMIT1"
            })),
            ok_response(json!({
                "symbol": "HYPEUSDT"
            })),
            ok_response(json!({
                "orderId": "2003",
                "clientOid": "AMEND1"
            })),
        ])
        .await;
        let client = bitget_test_client(base_url);

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
        assert_eq!(placed.exchange, "bitget");
        assert_eq!(placed.market_type, MarketType::Spot);
        assert_eq!(placed.order_id, "2001");
        assert_eq!(placed.client_order_id.as_deref(), Some("LIMIT1"));
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
                "HYPEUSDT",
            ))
            .await
            .unwrap();
        assert_eq!(cancel_all.exchange, "bitget");
        assert_eq!(cancel_all.market_type, MarketType::Spot);
        assert_eq!(cancel_all.symbol.as_deref(), Some("HYPEUSDT"));
        assert_eq!(cancel_all.cancelled_orders, 0);

        let amended = client
            .amend_order(AmendOrderRequest {
                market_type: MarketType::Spot,
                symbol: "HYPEUSDT".to_string(),
                order_id: Some("2003".to_string()),
                client_order_id: Some("AMEND1".to_string()),
                new_client_order_id: None,
                new_quantity: 0.5,
            })
            .await
            .unwrap();
        assert_eq!(amended.order_id, "2003");
        assert_eq!(amended.client_order_id.as_deref(), Some("AMEND1"));
        assert_eq!(amended.quantity, 0.5);

        assert!(!client.capabilities().supports_order_list);

        let requests = seen.lock().unwrap().clone();
        assert_eq!(requests.len(), 10);

        assert_eq!(requests[0].method, "GET");
        assert_eq!(requests[0].path, "/api/v2/spot/public/symbols");

        assert_signed_bitget_request(&requests[1], "GET", "/api/v2/spot/account/assets");
        assert!(requests[1].query.is_empty());

        assert_signed_bitget_request(&requests[2], "GET", "/api/v3/account/fee-rate");
        assert_eq!(
            requests[2].query.get("category").map(String::as_str),
            Some("SPOT")
        );
        assert_eq!(
            requests[2].query.get("symbol").map(String::as_str),
            Some("HYPEUSDT")
        );

        assert_signed_bitget_request(&requests[3], "POST", "/api/v2/spot/trade/place-order");
        let body = requests[3].body.as_ref().unwrap();
        assert_eq!(body["symbol"], "HYPEUSDT");
        assert_eq!(body["side"], "buy");
        assert_eq!(body["orderType"], "limit");
        assert_eq!(body["size"], "1.25");
        assert_eq!(body["price"], "10");
        assert_eq!(body["force"], "gtc");
        assert_eq!(body["clientOid"], "LIMIT1");

        assert_eq!(requests[4].method, "GET");
        assert_eq!(requests[4].path, "/api/v2/spot/public/symbols");

        assert_signed_bitget_request(&requests[5], "GET", "/api/v2/spot/account/assets");
        assert!(requests[5].query.is_empty());

        assert_signed_bitget_request(&requests[6], "POST", "/api/v2/spot/trade/place-order");
        let body = requests[6].body.as_ref().unwrap();
        assert_eq!(body["symbol"], "HYPEUSDT");
        assert_eq!(body["side"], "buy");
        assert_eq!(body["orderType"], "market");
        assert_eq!(body["size"], "25.5");
        assert_eq!(body["clientOid"], "QUOTE1");

        assert_signed_bitget_request(&requests[7], "POST", "/api/v2/spot/trade/cancel-order");
        let body = requests[7].body.as_ref().unwrap();
        assert_eq!(body["symbol"], "HYPEUSDT");
        assert_eq!(body["orderId"], "2001");
        assert_eq!(body["clientOid"], "LIMIT1");

        assert_signed_bitget_request(
            &requests[8],
            "POST",
            "/api/v2/spot/trade/cancel-symbol-order",
        );
        let body = requests[8].body.as_ref().unwrap();
        assert_eq!(body["symbol"], "HYPEUSDT");
        assert_eq!(body.as_object().unwrap().len(), 1);

        assert_signed_bitget_request(&requests[9], "POST", "/api/v3/trade/modify-order");
        let body = requests[9].body.as_ref().unwrap();
        assert_eq!(body["category"], "SPOT");
        assert_eq!(body["symbol"], "HYPEUSDT");
        assert_eq!(body["orderId"], "2003");
        assert_eq!(body["clientOid"], "AMEND1");
        assert_eq!(body["qty"], "0.5");
        assert_eq!(body["autoCancel"], "no");
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

    #[test]
    fn bitget_order_body_should_use_size_field() {
        let mut request = OrderRequest::spot_market_buy("HYPEUSDT", 1.25);
        request.order_type = OrderType::Limit;
        request.price = Some(10.0);
        request.client_order_id = Some("ldry-bitget-123".to_string());
        let body = bitget_order_body(&request, "HYPEUSDT").unwrap();
        assert_eq!(body["size"], "1.25");
        assert!(body.get("quantity").is_none());

        request.order_type = OrderType::PostOnly;
        request.time_in_force = Some(TimeInForce::GTX);
        let post_only = bitget_order_body(&request, "HYPEUSDT").unwrap();
        assert_eq!(post_only["orderType"], "limit");
        assert_eq!(post_only["force"], "post_only");
    }

    #[test]
    fn bitget_quote_market_order_body_should_use_quote_size_for_buy() {
        let request = QuoteMarketOrderRequest {
            market_type: MarketType::Spot,
            symbol: "HYPEUSDT".to_string(),
            side: OrderSide::Buy,
            quote_quantity: 25.5,
            client_order_id: Some("ldry-bitget-quote".to_string()),
        };

        let body = bitget_quote_market_order_body(&request, "HYPEUSDT").unwrap();
        assert_eq!(body["symbol"], "HYPEUSDT");
        assert_eq!(body["side"], "buy");
        assert_eq!(body["orderType"], "market");
        assert_eq!(body["size"], "25.5");
        assert_eq!(body["clientOid"], "ldry-bitget-quote");

        let uta = bitget_uta_quote_market_order_body(&request, "HYPEUSDT").unwrap();
        assert_eq!(uta["category"], "SPOT");
        assert_eq!(uta["qty"], "25.5");
        assert_eq!(uta["timeInForce"], "gtc");

        let mut sell = request.clone();
        sell.side = OrderSide::Sell;
        assert!(bitget_quote_market_order_body(&sell, "HYPEUSDT").is_err());
    }

    #[test]
    fn bitget_cancel_all_body_should_match_symbol_endpoint_contract() {
        let body = bitget_cancel_all_body("BTCUSDT");
        assert_eq!(body["symbol"], "BTCUSDT");
        assert_eq!(
            bitget_cancel_all_symbol(&json!({"code":"00000","data":{"symbol":"BTCUSDT"}}))
                .as_deref(),
            Some("BTCUSDT")
        );
    }

    #[test]
    fn bitget_uta_order_body_should_use_spot_category_and_qty() {
        let mut request = OrderRequest::spot_market_buy("HYPEUSDT", 1.25);
        request.order_type = OrderType::Limit;
        request.time_in_force = Some(TimeInForce::GTC);
        request.price = Some(10.0);
        request.client_order_id = Some("ldry-bitget-123".to_string());
        let body = bitget_uta_order_body(&request, "HYPEUSDT").unwrap();
        assert_eq!(body["category"], "SPOT");
        assert_eq!(body["qty"], "1.25");
        assert_eq!(body["timeInForce"], "gtc");

        request.order_type = OrderType::PostOnly;
        request.time_in_force = Some(TimeInForce::GTX);
        let post_only = bitget_uta_order_body(&request, "HYPEUSDT").unwrap();
        assert_eq!(post_only["orderType"], "limit");
        assert_eq!(post_only["timeInForce"], "post_only");
    }

    #[test]
    fn bitget_uta_amend_order_body_should_use_qty() {
        let request = AmendOrderRequest {
            market_type: MarketType::Spot,
            symbol: "HYPEUSDT".to_string(),
            order_id: Some("1001".to_string()),
            client_order_id: None,
            new_client_order_id: None,
            new_quantity: 0.5,
        };

        let body = bitget_uta_amend_order_body(&request, "HYPEUSDT").unwrap();

        assert_eq!(body["category"], "SPOT");
        assert_eq!(body["symbol"], "HYPEUSDT");
        assert_eq!(body["orderId"], "1001");
        assert_eq!(body["qty"], "0.5");
        assert_eq!(body["autoCancel"], "no");
        assert!(body.get("price").is_none());

        let client_oid_request = AmendOrderRequest {
            order_id: None,
            client_order_id: Some("ldry-bitget-amend".to_string()),
            ..request.clone()
        };
        let client_oid_body = bitget_uta_amend_order_body(&client_oid_request, "HYPEUSDT").unwrap();
        assert_eq!(client_oid_body["clientOid"], "ldry-bitget-amend");

        let client_oid_body = bitget_uta_amend_order_body(
            &AmendOrderRequest {
                order_id: Some("   ".to_string()),
                client_order_id: Some("ldry-bitget-amend".to_string()),
                ..request.clone()
            },
            "HYPEUSDT",
        )
        .unwrap();
        assert!(client_oid_body.get("orderId").is_none());
        assert_eq!(client_oid_body["clientOid"], "ldry-bitget-amend");

        let unsupported = AmendOrderRequest {
            new_client_order_id: Some("ldry-bitget-new".to_string()),
            ..request
        };
        assert!(bitget_uta_amend_order_body(&unsupported, "HYPEUSDT").is_err());
    }

    #[test]
    fn bitget_amend_order_response_should_parse_ack() {
        let request = AmendOrderRequest::reduce_quantity_by_order_id(
            MarketType::Spot,
            "HYPEUSDT",
            "1001",
            0.5,
        );

        let response = parse_amend_order_response(
            &json!({"code":"00000","data":{"orderId":"1002","clientOid":"ldry-bitget-amend"}}),
            &request,
            "HYPEUSDT",
        )
        .unwrap();

        assert_eq!(response.exchange, "bitget");
        assert_eq!(response.symbol, "HYPEUSDT");
        assert_eq!(response.order_id, "1002");
        assert_eq!(response.quantity, 0.5);
        assert_eq!(
            response.client_order_id.as_deref(),
            Some("ldry-bitget-amend")
        );
    }

    #[tokio::test]
    async fn bitget_amend_order_should_ack_in_dry_run() {
        let client = BitgetSpotClient::new(BitgetSpotConfig {
            dry_run: true,
            ..BitgetSpotConfig::default()
        });

        let ack = client
            .amend_order(AmendOrderRequest::reduce_quantity_by_order_id(
                MarketType::Spot,
                "HYPEUSDT",
                "1001",
                0.5,
            ))
            .await
            .unwrap();

        assert_eq!(ack.exchange, "bitget");
        assert_eq!(ack.symbol, "HYPEUSDT");
        assert_eq!(ack.order_id, "1001");
        assert_eq!(ack.quantity, 0.5);

        let ack = client
            .amend_order(AmendOrderRequest {
                market_type: MarketType::Spot,
                symbol: "HYPEUSDT".to_string(),
                order_id: Some("1001".to_string()),
                client_order_id: Some("   ".to_string()),
                new_client_order_id: Some("   ".to_string()),
                new_quantity: 0.45,
            })
            .await
            .unwrap();
        assert_eq!(ack.order_id, "1001");
        assert_eq!(ack.client_order_id, None);

        let unsupported = client
            .amend_order(AmendOrderRequest {
                market_type: MarketType::Spot,
                symbol: "HYPEUSDT".to_string(),
                order_id: Some("1001".to_string()),
                client_order_id: None,
                new_client_order_id: Some("LDRYBITGETNEW".to_string()),
                new_quantity: 0.4,
            })
            .await
            .unwrap_err();
        assert!(matches!(unsupported, ExchangeClientError::Unsupported(_)));

        let invalid_new_client_id = client
            .amend_order(AmendOrderRequest {
                market_type: MarketType::Spot,
                symbol: "HYPEUSDT".to_string(),
                order_id: Some("1001".to_string()),
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
    async fn bitget_place_order_should_ack_in_dry_run_without_symbol_rule_readback() {
        let client = BitgetSpotClient::new(BitgetSpotConfig {
            dry_run: true,
            ..BitgetSpotConfig::default()
        });

        let ack = client
            .place_order(OrderRequest {
                market_type: MarketType::Spot,
                symbol: "HYPEUSDT".to_string(),
                side: OrderSide::Buy,
                position_side: PositionSide::None,
                order_type: OrderType::Limit,
                time_in_force: Some(TimeInForce::GTC),
                quantity: 1.25,
                price: Some(10.0),
                client_order_id: None,
                reduce_only: false,
            })
            .await
            .unwrap();

        assert_eq!(ack.exchange, "bitget");
        assert_eq!(ack.market_type, MarketType::Spot);
        assert_eq!(ack.symbol, "HYPEUSDT");
        assert_eq!(ack.side, OrderSide::Buy);
        assert_eq!(ack.order_type, OrderType::Limit);
        assert_eq!(ack.price, Some(10.0));
        assert_eq!(ack.quantity, 1.25);
        assert_eq!(ack.status, OrderStatus::New);
        assert!(ack.client_order_id.is_some());
        assert!(ack.order_id.starts_with("dry-bitget-"));
    }

    #[tokio::test]
    async fn bitget_quote_market_order_should_ack_in_dry_run() {
        let client = BitgetSpotClient::new(BitgetSpotConfig {
            dry_run: true,
            ..BitgetSpotConfig::default()
        });

        let ack = client
            .place_quote_market_order(QuoteMarketOrderRequest::spot_buy("HYPEUSDT", 25.5))
            .await
            .unwrap();

        assert_eq!(ack.exchange, "bitget");
        assert_eq!(ack.market_type, MarketType::Spot);
        assert_eq!(ack.symbol, "HYPEUSDT");
        assert_eq!(ack.side, OrderSide::Buy);
        assert_eq!(ack.order_type, OrderType::Market);
        assert_eq!(ack.quantity, 25.5);
        assert_eq!(ack.status, OrderStatus::New);
        assert!(ack.client_order_id.is_some());
    }

    #[tokio::test]
    async fn bitget_cancel_order_should_validate_market_type_in_dry_run() {
        let client = BitgetSpotClient::new(BitgetSpotConfig {
            dry_run: true,
            ..BitgetSpotConfig::default()
        });

        let ack = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Spot,
                symbol: "HYPEUSDT".to_string(),
                order_id: Some("1001".to_string()),
                client_order_id: None,
            })
            .await
            .unwrap();
        assert_eq!(ack.exchange, "bitget");
        assert_eq!(ack.market_type, MarketType::Spot);
        assert_eq!(ack.symbol, "HYPEUSDT");
        assert_eq!(ack.order_id.as_deref(), Some("1001"));

        let ack = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Spot,
                symbol: "HYPEUSDT".to_string(),
                order_id: Some("   ".to_string()),
                client_order_id: Some("CANCELCLIENT1".to_string()),
            })
            .await
            .unwrap();
        assert_eq!(ack.order_id, None);
        assert_eq!(ack.client_order_id.as_deref(), Some("CANCELCLIENT1"));

        let err = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Perpetual,
                symbol: "HYPEUSDT".to_string(),
                order_id: Some("1001".to_string()),
                client_order_id: None,
            })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("only supports spot"));

        let err = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Spot,
                symbol: "HYPEUSDT".to_string(),
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
    async fn bitget_cancel_all_orders_should_ack_and_validate_market_type_in_dry_run() {
        let client = BitgetSpotClient::new(BitgetSpotConfig {
            dry_run: true,
            ..BitgetSpotConfig::default()
        });

        let ack = client
            .cancel_all_orders(CancelAllOrdersRequest::for_symbol(
                MarketType::Spot,
                "HYPEUSDT",
            ))
            .await
            .unwrap();
        assert_eq!(ack.exchange, "bitget");
        assert_eq!(ack.market_type, MarketType::Spot);
        assert_eq!(ack.symbol.as_deref(), Some("HYPEUSDT"));
        assert_eq!(ack.cancelled_orders, 0);

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
    async fn bitget_get_order_should_validate_order_id_before_request() {
        let client = BitgetSpotClient::new(BitgetSpotConfig {
            dry_run: true,
            ..BitgetSpotConfig::default()
        });

        let err = client.get_order("HYPEUSDT", "   ").await.unwrap_err();
        assert!(matches!(
            err,
            ExchangeClientError::Validation {
                field: "order_id",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn bitget_order_list_should_validate_before_unsupported() {
        use crate::exchanges::unified::{
            OrderListConditionalLeg, OrderListLegType, OrderListRequest,
        };

        fn oco_request() -> OrderListRequest {
            OrderListRequest::Oco {
                market_type: MarketType::Spot,
                symbol: "HYPEUSDT".to_string(),
                list_client_order_id: Some("BITGETOCOLIST1".to_string()),
                side: OrderSide::Sell,
                quantity: 1.0,
                above: OrderListConditionalLeg {
                    order_type: OrderListLegType::LimitMaker,
                    price: Some(11.0),
                    stop_price: None,
                    time_in_force: None,
                    client_order_id: Some("BITGETABOVE1".to_string()),
                },
                below: OrderListConditionalLeg {
                    order_type: OrderListLegType::StopLossLimit,
                    price: Some(8.0),
                    stop_price: Some(9.0),
                    time_in_force: Some(TimeInForce::GTC),
                    client_order_id: Some("BITGETBELOW1".to_string()),
                },
            }
        }

        let client = BitgetSpotClient::new(BitgetSpotConfig {
            dry_run: false,
            base_url: "http://127.0.0.1:9".to_string(),
            ..BitgetSpotConfig::default()
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
    fn bitget_spot_private_stream_parser_should_parse_order_fill_balance_and_ignore_control() {
        let order_events = parse_private_stream_message(
            r#"{
                "arg":{"channel":"spot.orders","instId":"BTCUSDT"},
                "data":[{
                    "symbol":"BTCUSDT",
                    "orderId":"order-1",
                    "clientOid":"client-1",
                    "side":"buy",
                    "orderType":"limit",
                    "status":"partial-fill",
                    "price":"65000",
                    "size":"0.02",
                    "filledSize":"0.01",
                    "priceAvg":"65010",
                    "tradeId":"trade-1",
                    "feeCcy":"USDT",
                    "fee":"0.13",
                    "tradeScope":"taker",
                    "cTime":"1700000000000",
                    "uTime":"1700000000100"
                }]
            }"#,
        )
        .unwrap();
        assert_eq!(order_events.len(), 2);
        match &order_events[0] {
            UserStreamEvent::Order(order) => {
                assert_eq!(order.exchange, "bitget");
                assert_eq!(order.market_type, MarketType::Spot);
                assert_eq!(order.symbol, "BTCUSDT");
                assert_eq!(order.order_id, "order-1");
                assert_eq!(order.client_order_id.as_deref(), Some("client-1"));
                assert_eq!(order.side, OrderSide::Buy);
                assert_eq!(order.order_type, OrderType::Limit);
                assert_eq!(order.status, OrderStatus::PartiallyFilled);
                assert_eq!(order.price, Some(65_000.0));
                assert_eq!(order.quantity, 0.02);
                assert_eq!(order.filled_quantity, 0.01);
                assert_eq!(order.average_price, Some(65_010.0));
            }
            other => panic!("expected Bitget order event, got {other:?}"),
        }
        match &order_events[1] {
            UserStreamEvent::Fill(fill) => {
                assert_eq!(fill.exchange, "bitget");
                assert_eq!(fill.market_type, MarketType::Spot);
                assert_eq!(fill.symbol, "BTCUSDT");
                assert_eq!(fill.trade_id.as_deref(), Some("trade-1"));
                assert_eq!(fill.order_id.as_deref(), Some("order-1"));
                assert_eq!(fill.client_order_id.as_deref(), Some("client-1"));
                assert_eq!(fill.side, OrderSide::Buy);
                assert_eq!(fill.price, 65_000.0);
                assert_eq!(fill.quantity, 0.02);
                assert_eq!(fill.fee_asset.as_deref(), Some("USDT"));
                assert_eq!(fill.fee_amount, Some(0.13));
                assert_eq!(fill.liquidity, LiquidityRole::Taker);
            }
            other => panic!("expected Bitget fill event, got {other:?}"),
        }

        let fill_events = parse_private_stream_message(
            r#"{
                "channel":"spot.trade",
                "data":{
                    "symbol":"BTCUSDT",
                    "tradeId":"trade-2",
                    "orderId":"order-2",
                    "clientOid":"client-2",
                    "side":"sell",
                    "price":"66000",
                    "size":"0.03",
                    "feeCcy":"BTC",
                    "fee":"0.00003",
                    "tradeScope":"maker"
                }
            }"#,
        )
        .unwrap();
        match &fill_events[0] {
            UserStreamEvent::Fill(fill) => {
                assert_eq!(fill.trade_id.as_deref(), Some("trade-2"));
                assert_eq!(fill.side, OrderSide::Sell);
                assert_eq!(fill.liquidity, LiquidityRole::Maker);
                assert_eq!(fill.fee_asset.as_deref(), Some("BTC"));
            }
            other => panic!("expected Bitget fill event, got {other:?}"),
        }

        let balance_events = parse_private_stream_message(
            r#"{
                "arg":{"channel":"spot.account"},
                "data":[{
                    "coin":"USDT",
                    "available":"80",
                    "frozen":"20",
                    "totalAmount":"100"
                }]
            }"#,
        )
        .unwrap();
        match &balance_events[0] {
            UserStreamEvent::Balance(snapshot) => {
                assert_eq!(snapshot.exchange, "bitget");
                assert_eq!(snapshot.market_type, MarketType::Spot);
                assert_eq!(snapshot.balances.len(), 1);
                let balance = &snapshot.balances[0];
                assert_eq!(balance.asset, "USDT");
                assert_eq!(balance.total, 100.0);
                assert_eq!(balance.available, 80.0);
                assert_eq!(balance.locked, 20.0);
            }
            other => panic!("expected Bitget balance event, got {other:?}"),
        }

        assert!(parse_private_stream_message("pong").unwrap().is_empty());
        assert!(
            parse_private_stream_message(r#"{"event":"subscribe","code":"0"}"#)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn bitget_url_not_found_detection_should_accept_text_error() {
        let error = ExchangeClientError::Unsupported(
            "ExchangeError { code: Some(\"40404\"), message: \"Request URL NOT FOUND\" }"
                .to_string(),
        );
        assert!(is_bitget_url_not_found(&error));
    }

    #[test]
    fn bitget_url_not_found_detection_should_accept_classified_error() {
        let error = ExchangeClientError::Classified(ExchangeError {
            exchange: "bitget".to_string(),
            class: ExchangeErrorClass::Unknown,
            code: Some("40404".to_string()),
            message: "Request URL NOT FOUND".to_string(),
        });
        assert!(is_bitget_url_not_found(&error));
    }
}
