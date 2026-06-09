use std::collections::BTreeMap;

use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse,
    OpenOrdersRequest, OpenOrdersResponse, OrderListConditionalLeg, OrderListLegType,
    OrderListOrderLeg, OrderListRequest, OrderListResponse, PlaceOrderRequest, PlaceOrderResponse,
    QueryOrderRequest, QueryOrderResponse, RecentFillsRequest, RecentFillsResponse, RequestContext,
    TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, OrderSide, OrderStatus, OrderType, TenantId};
use serde_json::{json, Value};

use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use crate::request_spec::ActualHttpRequest;

use super::parser::normalize_hitbtc_symbol;
use super::private_parser::{
    parse_account_balances, parse_fee_snapshots, parse_open_orders, parse_order_list_ack,
    parse_recent_fills, parse_spot_order_ack,
};
use super::signing::{hs256_authorization_header, rest_hs256_payload, sign_hs256_hex};
use super::HitbtcGatewayAdapter;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HitbtcOfflineRequest {
    pub method: String,
    pub path: String,
    pub query: BTreeMap<String, String>,
    pub headers: BTreeMap<String, String>,
    pub body: Option<Value>,
    pub raw_body: Option<String>,
}

impl HitbtcOfflineRequest {
    pub fn actual_http_request(&self) -> ActualHttpRequest {
        ActualHttpRequest::new(self.method.clone(), self.path.clone())
            .with_query(self.query.clone())
            .with_headers(self.headers.clone())
            .with_body(self.body.clone())
    }
}

pub fn build_place_order_request_spec(
    api_key: &str,
    api_secret: &str,
    timestamp_ms: u64,
    window_ms: u64,
) -> ExchangeApiResult<HitbtcOfflineRequest> {
    let body = place_order_form_body();
    signed_request(
        api_key,
        api_secret,
        "POST",
        "/api/3/spot/order",
        BTreeMap::new(),
        Some(SignedBody::Form(body)),
        timestamp_ms,
        window_ms,
    )
}

pub fn build_cancel_order_request_spec(
    api_key: &str,
    api_secret: &str,
    timestamp_ms: u64,
    window_ms: u64,
) -> ExchangeApiResult<HitbtcOfflineRequest> {
    signed_request(
        api_key,
        api_secret,
        "DELETE",
        "/api/3/spot/order/cli-hitbtc-1",
        BTreeMap::new(),
        None,
        timestamp_ms,
        window_ms,
    )
}

pub fn build_open_orders_request_spec(
    api_key: &str,
    api_secret: &str,
    timestamp_ms: u64,
    window_ms: u64,
) -> ExchangeApiResult<HitbtcOfflineRequest> {
    signed_request(
        api_key,
        api_secret,
        "GET",
        "/api/3/spot/order",
        BTreeMap::new(),
        None,
        timestamp_ms,
        window_ms,
    )
}

pub fn build_get_balances_request_spec(
    api_key: &str,
    api_secret: &str,
    timestamp_ms: u64,
    window_ms: u64,
) -> ExchangeApiResult<HitbtcOfflineRequest> {
    signed_request(
        api_key,
        api_secret,
        "GET",
        "/api/3/spot/balance",
        BTreeMap::new(),
        None,
        timestamp_ms,
        window_ms,
    )
}

pub fn build_get_fees_spot_request_spec(
    api_key: &str,
    api_secret: &str,
    timestamp_ms: u64,
    window_ms: u64,
) -> ExchangeApiResult<HitbtcOfflineRequest> {
    signed_request(
        api_key,
        api_secret,
        "GET",
        "/api/3/spot/fee",
        BTreeMap::new(),
        None,
        timestamp_ms,
        window_ms,
    )
}

pub fn build_recent_fills_request_spec(
    api_key: &str,
    api_secret: &str,
    timestamp_ms: u64,
    window_ms: u64,
) -> ExchangeApiResult<HitbtcOfflineRequest> {
    signed_request(
        api_key,
        api_secret,
        "GET",
        "/api/3/spot/history/trade",
        BTreeMap::new(),
        None,
        timestamp_ms,
        window_ms,
    )
}

pub fn build_query_order_request_spec(
    api_key: &str,
    api_secret: &str,
    timestamp_ms: u64,
    window_ms: u64,
) -> ExchangeApiResult<HitbtcOfflineRequest> {
    signed_request(
        api_key,
        api_secret,
        "GET",
        "/api/3/spot/order/cli-hitbtc-1",
        BTreeMap::new(),
        None,
        timestamp_ms,
        window_ms,
    )
}

pub fn build_place_order_request(
    api_key: &str,
    api_secret: &str,
    timestamp_ms: u64,
    window_ms: u64,
    request: &PlaceOrderRequest,
) -> ExchangeApiResult<HitbtcOfflineRequest> {
    let body = place_order_form_from_request(request)?;
    signed_request(
        api_key,
        api_secret,
        "POST",
        "/api/3/spot/order",
        BTreeMap::new(),
        Some(SignedBody::Form(body)),
        timestamp_ms,
        window_ms,
    )
}

pub fn build_cancel_order_request(
    api_key: &str,
    api_secret: &str,
    timestamp_ms: u64,
    window_ms: u64,
    request: &CancelOrderRequest,
) -> ExchangeApiResult<HitbtcOfflineRequest> {
    let client_order_id = request
        .client_order_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "hitbtc cancel_order requires client_order_id".to_string(),
        })?;
    signed_request(
        api_key,
        api_secret,
        "DELETE",
        &format!("/api/3/spot/order/{}", urlencoding::encode(client_order_id)),
        BTreeMap::new(),
        None,
        timestamp_ms,
        window_ms,
    )
}

pub fn build_query_order_request(
    api_key: &str,
    api_secret: &str,
    timestamp_ms: u64,
    window_ms: u64,
    request: &QueryOrderRequest,
) -> ExchangeApiResult<HitbtcOfflineRequest> {
    let client_order_id = request
        .client_order_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "hitbtc query_order requires client_order_id".to_string(),
        })?;
    signed_request(
        api_key,
        api_secret,
        "GET",
        &format!("/api/3/spot/order/{}", urlencoding::encode(client_order_id)),
        BTreeMap::new(),
        None,
        timestamp_ms,
        window_ms,
    )
}

pub fn build_open_orders_request(
    api_key: &str,
    api_secret: &str,
    timestamp_ms: u64,
    window_ms: u64,
    request: &OpenOrdersRequest,
) -> ExchangeApiResult<HitbtcOfflineRequest> {
    let mut query = BTreeMap::new();
    if let Some(symbol) = &request.symbol {
        query.insert("symbol".to_string(), normalize_hitbtc_symbol(symbol)?);
    }
    if let Some(page) = &request.page {
        if let Some(limit) = page.limit {
            query.insert("limit".to_string(), limit.min(1000).to_string());
        }
    }
    signed_request(
        api_key,
        api_secret,
        "GET",
        "/api/3/spot/order",
        query,
        None,
        timestamp_ms,
        window_ms,
    )
}

pub fn build_recent_fills_request(
    api_key: &str,
    api_secret: &str,
    timestamp_ms: u64,
    window_ms: u64,
    request: &RecentFillsRequest,
) -> ExchangeApiResult<HitbtcOfflineRequest> {
    let mut query = BTreeMap::new();
    if let Some(symbol) = &request.symbol {
        query.insert("symbol".to_string(), normalize_hitbtc_symbol(symbol)?);
    }
    if let Some(order_id) = &request.exchange_order_id {
        query.insert("order_id".to_string(), order_id.clone());
    }
    if let Some(from) = &request.from_trade_id {
        query.insert("from".to_string(), from.clone());
    }
    if let Some(start) = request.start_time {
        query.insert("from".to_string(), start.to_rfc3339());
    }
    if let Some(end) = request.end_time {
        query.insert("till".to_string(), end.to_rfc3339());
    }
    if let Some(limit) = request.limit {
        query.insert("limit".to_string(), limit.min(1000).to_string());
    }
    signed_request(
        api_key,
        api_secret,
        "GET",
        "/api/3/spot/history/trade",
        query,
        None,
        timestamp_ms,
        window_ms,
    )
}

pub fn build_amend_order_replace_request_spec(
    api_key: &str,
    api_secret: &str,
    timestamp_ms: u64,
    window_ms: u64,
) -> ExchangeApiResult<HitbtcOfflineRequest> {
    signed_request(
        api_key,
        api_secret,
        "PATCH",
        "/api/3/spot/order/cli-hitbtc-1",
        BTreeMap::new(),
        Some(SignedBody::Form(amend_order_replace_form_body())),
        timestamp_ms,
        window_ms,
    )
}

pub fn build_amend_order_request(
    api_key: &str,
    api_secret: &str,
    timestamp_ms: u64,
    window_ms: u64,
    request: &AmendOrderRequest,
) -> ExchangeApiResult<HitbtcOfflineRequest> {
    let client_order_id =
        request
            .client_order_id
            .as_deref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "hitbtc amend_order requires client_order_id".to_string(),
            })?;
    let mut fields = vec![("quantity", request.new_quantity.as_str())];
    if let Some(new_client_order_id) = request.new_client_order_id.as_deref() {
        fields.push(("new_client_order_id", new_client_order_id));
    }
    let body = form_body(fields);
    signed_request(
        api_key,
        api_secret,
        "PATCH",
        &format!("/api/3/spot/order/{}", urlencoding::encode(client_order_id)),
        BTreeMap::new(),
        Some(SignedBody::Form(body)),
        timestamp_ms,
        window_ms,
    )
}

pub fn build_place_order_list_otoco_request_spec(
    api_key: &str,
    api_secret: &str,
    timestamp_ms: u64,
    window_ms: u64,
) -> ExchangeApiResult<HitbtcOfflineRequest> {
    signed_request(
        api_key,
        api_secret,
        "POST",
        "/api/3/spot/order/list",
        BTreeMap::new(),
        Some(SignedBody::JsonString(place_order_list_otoco_json_body())),
        timestamp_ms,
        window_ms,
    )
}

pub fn build_place_order_list_request(
    api_key: &str,
    api_secret: &str,
    timestamp_ms: u64,
    window_ms: u64,
    request: &OrderListRequest,
) -> ExchangeApiResult<HitbtcOfflineRequest> {
    let body = match request {
        OrderListRequest::Oco {
            symbol,
            list_client_order_id,
            side,
            quantity,
            above,
            below,
            ..
        } => json!({
            "contingency_type": "oneCancelOther",
            "client_order_id": list_client_order_id,
            "orders": [
                conditional_leg_json(symbol, *side, quantity, above)?,
                conditional_leg_json(symbol, *side, quantity, below)?,
            ],
        }),
        OrderListRequest::Oto {
            symbol,
            list_client_order_id,
            working,
            pending,
            ..
        } => json!({
            "contingency_type": "oneTriggerOther",
            "client_order_id": list_client_order_id,
            "orders": [
                order_leg_json(symbol, working)?,
                order_leg_json(symbol, pending)?,
            ],
        }),
    };
    signed_request(
        api_key,
        api_secret,
        "POST",
        "/api/3/spot/order/list",
        BTreeMap::new(),
        Some(SignedBody::JsonString(body.to_string())),
        timestamp_ms,
        window_ms,
    )
}

pub fn build_batch_cancel_orders_request_spec(
    api_key: &str,
    api_secret: &str,
    timestamp_ms: u64,
    window_ms: u64,
) -> ExchangeApiResult<HitbtcOfflineRequest> {
    let mut query = BTreeMap::new();
    query.insert("symbol".to_string(), "ETHBTC".to_string());
    signed_request(
        api_key,
        api_secret,
        "DELETE",
        "/api/3/spot/order",
        query,
        None,
        timestamp_ms,
        window_ms,
    )
}

pub fn place_order_form_body() -> String {
    [
        ("client_order_id", "cli-hitbtc-1"),
        ("price", "0.046016"),
        ("quantity", "0.063"),
        ("side", "sell"),
        ("symbol", "ETHBTC"),
        ("type", "limit"),
    ]
    .into_iter()
    .map(|(key, value)| format!("{key}={value}"))
    .collect::<Vec<_>>()
    .join("&")
}

pub fn amend_order_replace_form_body() -> String {
    [
        ("new_client_order_id", "cli-hitbtc-1-r1"),
        ("price", "0.046020"),
        ("quantity", "0.050"),
    ]
    .into_iter()
    .map(|(key, value)| format!("{key}={value}"))
    .collect::<Vec<_>>()
    .join("&")
}

pub fn place_order_list_otoco_json_body() -> String {
    json!({
        "contingency_type": "oneTriggerOneCancelOther",
        "orders": [
            {
                "client_order_id": "cli-hitbtc-otoco-primary",
                "symbol": "ETHBTC",
                "side": "buy",
                "type": "limit",
                "time_in_force": "GTC",
                "quantity": "0.063",
                "price": "0.046016",
                "post_only": false
            },
            {
                "client_order_id": "cli-hitbtc-otoco-tp",
                "symbol": "ETHBTC",
                "side": "sell",
                "type": "limit",
                "time_in_force": "GTC",
                "quantity": "0.063",
                "price": "0.050000",
                "post_only": false
            },
            {
                "client_order_id": "cli-hitbtc-otoco-sl",
                "symbol": "ETHBTC",
                "side": "sell",
                "type": "stopMarket",
                "time_in_force": "GTC",
                "quantity": "0.063",
                "stop_price": "0.044050",
                "post_only": false
            }
        ]
    })
    .to_string()
}

enum SignedBody {
    Form(String),
    JsonString(String),
}

impl SignedBody {
    fn content_type(&self) -> &'static str {
        match self {
            Self::Form(_) => "application/x-www-form-urlencoded",
            Self::JsonString(_) => "application/json",
        }
    }

    fn raw(&self) -> &str {
        match self {
            Self::Form(value) | Self::JsonString(value) => value,
        }
    }

    fn actual_body(&self) -> ExchangeApiResult<Value> {
        match self {
            Self::Form(value) => Ok(Value::String(value.clone())),
            Self::JsonString(value) => {
                serde_json::from_str(value).map_err(|error| ExchangeApiError::InvalidRequest {
                    message: format!("invalid HitBTC JSON request fixture: {error}"),
                })
            }
        }
    }
}

fn signed_request(
    api_key: &str,
    api_secret: &str,
    method: &str,
    path: &str,
    query: BTreeMap<String, String>,
    body: Option<SignedBody>,
    timestamp_ms: u64,
    window_ms: u64,
) -> ExchangeApiResult<HitbtcOfflineRequest> {
    if api_key.trim().is_empty() || api_secret.trim().is_empty() {
        return Err(ExchangeApiError::Unsupported {
            operation: "hitbtc.private_request_spec_credentials",
        });
    }
    let query_string = encoded_query_string(&query);
    let body_text = body.as_ref().map(SignedBody::raw);
    let payload = rest_hs256_payload(
        method,
        path,
        query_string.as_deref(),
        body_text,
        timestamp_ms,
        Some(window_ms),
    );
    let signature = sign_hs256_hex(api_secret, &payload)?;
    let authorization =
        hs256_authorization_header(api_key, &signature, timestamp_ms, Some(window_ms));
    let mut headers = BTreeMap::new();
    headers.insert("authorization".to_string(), authorization);
    if let Some(body) = body.as_ref() {
        headers.insert("content-type".to_string(), body.content_type().to_string());
    } else if query.is_empty() {
        headers.insert(
            "content-type".to_string(),
            "application/x-www-form-urlencoded".to_string(),
        );
    }
    let actual_body = body.as_ref().map(SignedBody::actual_body).transpose()?;
    Ok(HitbtcOfflineRequest {
        method: method.to_string(),
        path: path.to_string(),
        query,
        headers,
        body: actual_body,
        raw_body: body_text.map(str::to_string),
    })
}

fn encoded_query_string(query: &BTreeMap<String, String>) -> Option<String> {
    if query.is_empty() {
        return None;
    }
    Some(
        query
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join("&"),
    )
}

impl HitbtcGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let signed = self.signed_get_balances_request()?;
        let value = self.rest.send_signed_request(&signed).await?;
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            balances: parse_account_balances(
                &self.exchange_id,
                tenant_id,
                account_id,
                &request.assets,
                &value,
            )?,
        })
    }

    pub(super) async fn get_fees_impl(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
        }
        let signed = self.signed_get_fees_request()?;
        let value = self.rest.send_signed_request(&signed).await?;
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fees: parse_fee_snapshots(&self.exchange_id, &request.symbols, &value)?,
        })
    }

    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let signed = self.signed_place_order_request(&request)?;
        let value = self.rest.send_signed_request(&signed).await?;
        let order = parse_spot_order_ack(&self.exchange_id, request.symbol.clone(), &value)?;
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

    pub(super) async fn cancel_order_impl(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let signed = self.signed_cancel_order_request(&request)?;
        let value = self.rest.send_signed_request(&signed).await?;
        let mut order = parse_spot_order_ack(&self.exchange_id, request.symbol.clone(), &value)?;
        order.status = OrderStatus::Cancelled;
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
            cancelled: true,
        })
    }

    pub(super) async fn amend_order_impl(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let signed = self.signed_amend_order_request(&request)?;
        let value = self.rest.send_signed_request(&signed).await?;
        let order = parse_spot_order_ack(&self.exchange_id, request.symbol.clone(), &value)?;
        Ok(AmendOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

    pub(super) async fn place_order_list_impl(
        &self,
        request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        ensure_exchange_api_schema(request.schema_version())?;
        self.ensure_exchange(&request.symbol().exchange)?;
        self.ensure_spot(request.symbol().market_type)?;
        let signed = self.signed_order_list_request(&request)?;
        let value = self.rest.send_signed_request(&signed).await?;
        parse_order_list_ack(
            &self.exchange_id,
            request.symbol().clone(),
            request.kind(),
            &value,
        )
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let signed = self.signed_query_order_request(&request)?;
        let value = self.rest.send_signed_request(&signed).await?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order: Some(parse_spot_order_ack(
                &self.exchange_id,
                request.symbol.clone(),
                &value,
            )?),
        })
    }

    pub(super) async fn get_open_orders_impl(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
        }
        let signed = self.signed_open_orders_request(&request)?;
        let value = self.rest.send_signed_request(&signed).await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_open_orders(&self.exchange_id, request.symbol.as_ref(), &value)?,
        })
    }

    pub(super) async fn get_recent_fills_impl(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "hitbtc get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let signed = self.signed_recent_fills_request(&request)?;
        let value = self.rest.send_signed_request(&signed).await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?,
        })
    }

    fn signed_place_order_request(
        &self,
        request: &PlaceOrderRequest,
    ) -> ExchangeApiResult<HitbtcOfflineRequest> {
        self.ensure_private_rest("hitbtc.place_order.private_rest_disabled")?;
        build_place_order_request(
            self.config.api_key.as_deref().unwrap_or_default(),
            self.config.api_secret.as_deref().unwrap_or_default(),
            Utc::now().timestamp_millis() as u64,
            self.config.auth_window_ms,
            request,
        )
    }

    fn signed_get_balances_request(&self) -> ExchangeApiResult<HitbtcOfflineRequest> {
        self.ensure_private_rest("hitbtc.get_balances.private_rest_disabled")?;
        build_get_balances_request_spec(
            self.config.api_key.as_deref().unwrap_or_default(),
            self.config.api_secret.as_deref().unwrap_or_default(),
            Utc::now().timestamp_millis() as u64,
            self.config.auth_window_ms,
        )
    }

    fn signed_get_fees_request(&self) -> ExchangeApiResult<HitbtcOfflineRequest> {
        self.ensure_private_rest("hitbtc.get_fees.private_rest_disabled")?;
        build_get_fees_spot_request_spec(
            self.config.api_key.as_deref().unwrap_or_default(),
            self.config.api_secret.as_deref().unwrap_or_default(),
            Utc::now().timestamp_millis() as u64,
            self.config.auth_window_ms,
        )
    }

    fn signed_cancel_order_request(
        &self,
        request: &CancelOrderRequest,
    ) -> ExchangeApiResult<HitbtcOfflineRequest> {
        self.ensure_private_rest("hitbtc.cancel_order.private_rest_disabled")?;
        build_cancel_order_request(
            self.config.api_key.as_deref().unwrap_or_default(),
            self.config.api_secret.as_deref().unwrap_or_default(),
            Utc::now().timestamp_millis() as u64,
            self.config.auth_window_ms,
            request,
        )
    }

    fn signed_amend_order_request(
        &self,
        request: &AmendOrderRequest,
    ) -> ExchangeApiResult<HitbtcOfflineRequest> {
        self.ensure_private_rest("hitbtc.amend_order.private_rest_disabled")?;
        build_amend_order_request(
            self.config.api_key.as_deref().unwrap_or_default(),
            self.config.api_secret.as_deref().unwrap_or_default(),
            Utc::now().timestamp_millis() as u64,
            self.config.auth_window_ms,
            request,
        )
    }

    fn signed_order_list_request(
        &self,
        request: &OrderListRequest,
    ) -> ExchangeApiResult<HitbtcOfflineRequest> {
        self.ensure_private_rest("hitbtc.place_order_list.private_rest_disabled")?;
        build_place_order_list_request(
            self.config.api_key.as_deref().unwrap_or_default(),
            self.config.api_secret.as_deref().unwrap_or_default(),
            Utc::now().timestamp_millis() as u64,
            self.config.auth_window_ms,
            request,
        )
    }

    fn signed_query_order_request(
        &self,
        request: &QueryOrderRequest,
    ) -> ExchangeApiResult<HitbtcOfflineRequest> {
        self.ensure_private_rest("hitbtc.query_order.private_rest_disabled")?;
        build_query_order_request(
            self.config.api_key.as_deref().unwrap_or_default(),
            self.config.api_secret.as_deref().unwrap_or_default(),
            Utc::now().timestamp_millis() as u64,
            self.config.auth_window_ms,
            request,
        )
    }

    fn signed_open_orders_request(
        &self,
        request: &OpenOrdersRequest,
    ) -> ExchangeApiResult<HitbtcOfflineRequest> {
        self.ensure_private_rest("hitbtc.get_open_orders.private_rest_disabled")?;
        build_open_orders_request(
            self.config.api_key.as_deref().unwrap_or_default(),
            self.config.api_secret.as_deref().unwrap_or_default(),
            Utc::now().timestamp_millis() as u64,
            self.config.auth_window_ms,
            request,
        )
    }

    fn signed_recent_fills_request(
        &self,
        request: &RecentFillsRequest,
    ) -> ExchangeApiResult<HitbtcOfflineRequest> {
        self.ensure_private_rest("hitbtc.get_recent_fills.private_rest_disabled")?;
        build_recent_fills_request(
            self.config.api_key.as_deref().unwrap_or_default(),
            self.config.api_secret.as_deref().unwrap_or_default(),
            Utc::now().timestamp_millis() as u64,
            self.config.auth_window_ms,
            request,
        )
    }

    fn ensure_private_rest(&self, operation: &'static str) -> ExchangeApiResult<()> {
        if !self.config.private_request_specs_enabled() {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        Ok(())
    }

    fn context_account(
        &self,
        context: &RequestContext,
    ) -> ExchangeApiResult<(TenantId, AccountId)> {
        let tenant_id =
            context
                .tenant_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "hitbtc private REST readback requires context.tenant_id".to_string(),
                })?;
        let account_id =
            context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "hitbtc private REST readback requires context.account_id".to_string(),
                })?;
        Ok((tenant_id, account_id))
    }
}

fn place_order_form_from_request(request: &PlaceOrderRequest) -> ExchangeApiResult<String> {
    if request.reduce_only {
        return Err(ExchangeApiError::InvalidRequest {
            message: "hitbtc spot order does not support reduce_only".to_string(),
        });
    }
    if request.post_only {
        return Err(ExchangeApiError::InvalidRequest {
            message: "hitbtc spot order post_only is not enabled in shared runtime".to_string(),
        });
    }
    if request.quote_quantity.is_some() {
        return Err(ExchangeApiError::Unsupported {
            operation: "hitbtc.quote_market_order_request_spec_only",
        });
    }
    let mut fields = vec![
        (
            "symbol".to_string(),
            normalize_hitbtc_symbol(&request.symbol)?,
        ),
        ("side".to_string(), side_text(request.side).to_string()),
        (
            "type".to_string(),
            order_type_text(request.order_type)?.to_string(),
        ),
        ("quantity".to_string(), request.quantity.clone()),
    ];
    if let Some(client_order_id) = &request.client_order_id {
        fields.push(("client_order_id".to_string(), client_order_id.clone()));
    }
    if let Some(price) = &request.price {
        fields.push(("price".to_string(), price.clone()));
    }
    if let Some(time_in_force) = request.time_in_force {
        fields.push((
            "time_in_force".to_string(),
            tif_text(time_in_force).to_string(),
        ));
    }
    Ok(form_body_owned(fields))
}

fn conditional_leg_json(
    symbol: &rustcta_exchange_api::SymbolScope,
    side: OrderSide,
    quantity: &str,
    leg: &OrderListConditionalLeg,
) -> ExchangeApiResult<Value> {
    let mut value = json!({
        "client_order_id": leg.client_order_id,
        "symbol": normalize_hitbtc_symbol(symbol)?,
        "side": side_text(side),
        "type": leg_type_text(leg.order_type)?,
        "quantity": quantity,
    });
    insert_optional(&mut value, "price", leg.price.as_deref());
    insert_optional(&mut value, "stop_price", leg.stop_price.as_deref());
    insert_time_in_force(&mut value, leg.time_in_force);
    Ok(value)
}

fn order_leg_json(
    symbol: &rustcta_exchange_api::SymbolScope,
    leg: &OrderListOrderLeg,
) -> ExchangeApiResult<Value> {
    let mut value = json!({
        "client_order_id": leg.client_order_id,
        "symbol": normalize_hitbtc_symbol(symbol)?,
        "side": side_text(leg.side),
        "type": leg_type_text(leg.order_type)?,
        "quantity": leg.quantity,
    });
    insert_optional(&mut value, "price", leg.price.as_deref());
    insert_optional(&mut value, "stop_price", leg.stop_price.as_deref());
    insert_time_in_force(&mut value, leg.time_in_force);
    Ok(value)
}

fn insert_optional(value: &mut Value, key: &str, item: Option<&str>) {
    if let Some(item) = item {
        value[key] = json!(item);
    }
}

fn insert_time_in_force(value: &mut Value, time_in_force: Option<TimeInForce>) {
    if let Some(time_in_force) = time_in_force {
        value["time_in_force"] = json!(match time_in_force {
            TimeInForce::GTC => "GTC",
            TimeInForce::IOC => "IOC",
            TimeInForce::FOK => "FOK",
            TimeInForce::GTX => "GTC",
        });
    }
}

fn side_text(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn order_type_text(order_type: OrderType) -> ExchangeApiResult<&'static str> {
    match order_type {
        OrderType::Market => Ok("market"),
        OrderType::Limit | OrderType::IOC | OrderType::FOK => Ok("limit"),
        OrderType::PostOnly => Err(ExchangeApiError::InvalidRequest {
            message: "hitbtc post-only order type is not enabled in shared runtime".to_string(),
        }),
        OrderType::StopMarket | OrderType::StopLimit => Err(ExchangeApiError::Unsupported {
            operation: "hitbtc.stop_order_request_spec_only",
        }),
    }
}

fn tif_text(time_in_force: TimeInForce) -> &'static str {
    match time_in_force {
        TimeInForce::GTC | TimeInForce::GTX => "GTC",
        TimeInForce::IOC => "IOC",
        TimeInForce::FOK => "FOK",
    }
}

fn leg_type_text(order_type: OrderListLegType) -> ExchangeApiResult<&'static str> {
    match order_type {
        OrderListLegType::Market => Ok("market"),
        OrderListLegType::Limit | OrderListLegType::LimitMaker => Ok("limit"),
        OrderListLegType::StopLoss => Ok("stopMarket"),
        OrderListLegType::StopLossLimit => Ok("stopLimit"),
        OrderListLegType::TakeProfit | OrderListLegType::TakeProfitLimit => {
            Err(ExchangeApiError::Unsupported {
                operation: "hitbtc.place_order_list_take_profit_leg",
            })
        }
    }
}

fn form_body(fields: Vec<(&str, &str)>) -> String {
    fields
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn form_body_owned(fields: Vec<(String, String)>) -> String {
    fields
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}
