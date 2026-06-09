use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, BatchCancelOrdersRequest, BatchCancelOrdersResponse,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    PlaceOrderRequest, PlaceOrderResponse, QueryOrderRequest, QueryOrderResponse,
    RecentFillsRequest, RecentFillsResponse, RequestContext, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, OrderSide, OrderType, TenantId};
use serde_json::{json, Value};

use super::parser::split_symbol;
use super::private_parser::{
    parse_balances_ack, parse_batch_cancel_orders_ack, parse_batch_place_orders_ack,
    parse_cancel_all_ack, parse_cancel_order_ack, parse_fee_snapshot, parse_open_orders_ack,
    parse_place_order_ack, parse_query_order_ack, parse_recent_fills_ack,
};
use super::signing::{signed_rest_headers, LatokenDigest};
use super::LatokenGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

pub const LATOKEN_BALANCES_PATH: &str = "/v2/auth/account";
pub const LATOKEN_OPEN_ORDERS_PATH: &str = "/v2/auth/order/active";
pub const LATOKEN_PLACE_ORDER_PATH: &str = "/v2/auth/order/place";
pub const LATOKEN_BATCH_PLACE_PATH: &str = "/v2/auth/order/placeBulk";
pub const LATOKEN_CANCEL_ORDER_PATH: &str = "/v2/auth/order/cancel";
pub const LATOKEN_BATCH_CANCEL_PATH: &str = "/v2/auth/order/cancelBulk";
pub const LATOKEN_CANCEL_ALL_PATH: &str = "/v2/auth/order/cancelAll";
pub const LATOKEN_RECENT_FILLS_PATH: &str = "/v2/auth/trade";
pub const LATOKEN_FEE_PATH_PREFIX: &str = "/v2/auth/trade/fee";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatokenPrivateRequestSpec {
    pub method: &'static str,
    pub path: &'static str,
    pub query_params: Vec<(String, String)>,
    pub body_params: Vec<(String, String)>,
    pub body: Option<Value>,
    pub api_key: String,
    pub signature: String,
    pub digest: String,
    pub canonical_payload: String,
}

pub fn build_signed_private_request_spec(
    method: &'static str,
    path: &'static str,
    query_params: Vec<(String, String)>,
    body_params: Vec<(String, String)>,
    body: Option<Value>,
    api_key: &str,
    api_secret: &str,
) -> ExchangeApiResult<LatokenPrivateRequestSpec> {
    let headers = signed_rest_headers(
        api_key,
        api_secret,
        method,
        path,
        &query_params,
        &body_params,
        LatokenDigest::Sha256,
    )?;
    Ok(LatokenPrivateRequestSpec {
        method,
        path,
        query_params,
        body_params,
        body,
        api_key: headers.api_key,
        signature: headers.signature,
        digest: headers.digest,
        canonical_payload: headers.payload,
    })
}

pub fn request_spec_body_from_order(
    request: &PlaceOrderRequest,
) -> ExchangeApiResult<(Value, Vec<(String, String)>)> {
    let (base, quote) = split_symbol(&request.symbol.exchange_symbol.symbol)?;
    let condition = time_in_force_as_str(request.time_in_force);
    let order_type = order_type_as_str(request.order_type);
    let mut body_params = vec![
        ("baseCurrency".to_string(), base.clone()),
        ("quoteCurrency".to_string(), quote.clone()),
        (
            "side".to_string(),
            order_side_as_str(request.side).to_string(),
        ),
        ("condition".to_string(), condition.to_string()),
        ("type".to_string(), order_type.to_string()),
    ];
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body_params.push(("clientOrderId".to_string(), client_order_id.to_string()));
    }
    if let Some(price) = request.price.as_deref() {
        body_params.push(("price".to_string(), price.to_string()));
    }
    body_params.push(("quantity".to_string(), request.quantity.clone()));

    let mut body = serde_json::Map::new();
    for (key, value) in &body_params {
        body.insert(key.clone(), json!(value));
    }
    Ok((Value::Object(body), body_params))
}

pub fn cancel_order_body(order_id: &str) -> (Value, Vec<(String, String)>) {
    (
        json!({ "id": order_id }),
        vec![("id".to_string(), order_id.to_string())],
    )
}

pub fn cancel_all_body(
    symbol: Option<&rustcta_exchange_api::SymbolScope>,
) -> ExchangeApiResult<(Value, Vec<(String, String)>)> {
    let Some(symbol) = symbol else {
        return Ok((json!({}), Vec::new()));
    };
    let (base, quote) = split_symbol(&symbol.exchange_symbol.symbol)?;
    let body = json!({
        "baseCurrency": base,
        "quoteCurrency": quote,
    });
    Ok((
        body,
        vec![
            ("baseCurrency".to_string(), base),
            ("quoteCurrency".to_string(), quote),
        ],
    ))
}

pub fn batch_place_body(
    request: &BatchPlaceOrdersRequest,
) -> ExchangeApiResult<(Value, Vec<(String, String)>)> {
    let mut orders = Vec::with_capacity(request.orders.len());
    let mut order_param_texts = Vec::with_capacity(request.orders.len());
    for order in &request.orders {
        let (body, params) = request_spec_body_from_order(order)?;
        order_param_texts.push(ordered_json_object_text(&params)?);
        orders.push(body);
    }
    Ok((
        json!({ "orders": orders }),
        vec![(
            "orders".to_string(),
            format!("[{}]", order_param_texts.join(",")),
        )],
    ))
}

fn ordered_json_object_text(params: &[(String, String)]) -> ExchangeApiResult<String> {
    let mut parts = Vec::with_capacity(params.len());
    for (key, value) in params {
        let key = serde_json::to_string(key).map_err(|error| {
            rustcta_exchange_api::ExchangeApiError::InvalidRequest {
                message: format!("failed to serialize LATOKEN batch place key: {error}"),
            }
        })?;
        let value = serde_json::to_string(value).map_err(|error| {
            rustcta_exchange_api::ExchangeApiError::InvalidRequest {
                message: format!("failed to serialize LATOKEN batch place value: {error}"),
            }
        })?;
        parts.push(format!("{key}:{value}"));
    }
    Ok(format!("{{{}}}", parts.join(",")))
}

pub fn batch_cancel_body(
    request: &BatchCancelOrdersRequest,
) -> ExchangeApiResult<(Value, Vec<(String, String)>)> {
    let mut orders = Vec::with_capacity(request.cancels.len());
    for cancel in &request.cancels {
        let order_id = cancel.exchange_order_id.as_deref().ok_or_else(|| {
            rustcta_exchange_api::ExchangeApiError::InvalidRequest {
                message: "LATOKEN batch cancel request requires exchange_order_id".to_string(),
            }
        })?;
        orders.push(json!({ "id": order_id }));
    }
    let orders_text = serde_json::to_string(&orders).map_err(|error| {
        rustcta_exchange_api::ExchangeApiError::InvalidRequest {
            message: format!("failed to serialize LATOKEN batch cancel body: {error}"),
        }
    })?;
    Ok((
        json!({ "orders": orders }),
        vec![("orders".to_string(), orders_text)],
    ))
}

impl LatokenGatewayAdapter {
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
        let query = vec![("zeros".to_string(), "false".to_string())];
        let (api_key, api_secret) =
            self.private_credentials("latoken.get_balances_private_rest_not_enabled")?;
        let headers = signed_rest_headers(
            &api_key,
            &api_secret,
            "GET",
            LATOKEN_BALANCES_PATH,
            &query,
            &[],
            LatokenDigest::Sha256,
        )?;
        let value = self
            .rest
            .send_signed_get(LATOKEN_BALANCES_PATH, &query, headers)
            .await?;
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: crate::adapters::response_metadata(
                request.exchange,
                request.context.request_id,
            ),
            balances: parse_balances_ack(
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
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "LATOKEN get_fees requires at least one symbol".to_string(),
            });
        }
        let (api_key, api_secret) =
            self.private_credentials("latoken.get_fees_private_rest_not_enabled")?;
        let mut fees = Vec::with_capacity(request.symbols.len());
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            let (base, quote) = split_symbol(&symbol.exchange_symbol.symbol)?;
            let path = format!("{LATOKEN_FEE_PATH_PREFIX}/{base}/{quote}");
            let headers = signed_rest_headers(
                &api_key,
                &api_secret,
                "GET",
                &path,
                &[],
                &[],
                LatokenDigest::Sha256,
            )?;
            let value = self.rest.send_signed_get(&path, &[], headers).await?;
            fees.push(parse_fee_snapshot(symbol, &value)?);
        }
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: crate::adapters::response_metadata(
                request
                    .symbols
                    .first()
                    .map(|symbol| symbol.exchange.clone())
                    .unwrap_or_else(|| self.exchange_id.clone()),
                request.context.request_id,
            ),
            fees,
        })
    }

    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let (api_key, api_secret) =
            self.private_credentials("latoken.place_order_private_rest_not_enabled")?;
        let (body, body_params) = request_spec_body_from_order(&request)?;
        let headers = signed_rest_headers(
            &api_key,
            &api_secret,
            "POST",
            LATOKEN_PLACE_ORDER_PATH,
            &[],
            &body_params,
            LatokenDigest::Sha256,
        )?;
        let value = self
            .rest
            .send_signed_json(LATOKEN_PLACE_ORDER_PATH, headers, &body)
            .await?;
        parse_place_order_ack(&self.exchange_id, &request, &value)
    }

    pub(super) async fn cancel_order_impl(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let order_id = request
            .exchange_order_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "LATOKEN cancel_order requires exchange_order_id".to_string(),
            })?;
        let (api_key, api_secret) =
            self.private_credentials("latoken.cancel_order_private_rest_not_enabled")?;
        let (body, body_params) = cancel_order_body(order_id);
        let headers = signed_rest_headers(
            &api_key,
            &api_secret,
            "POST",
            LATOKEN_CANCEL_ORDER_PATH,
            &[],
            &body_params,
            LatokenDigest::Sha256,
        )?;
        let value = self
            .rest
            .send_signed_json(LATOKEN_CANCEL_ORDER_PATH, headers, &body)
            .await?;
        parse_cancel_order_ack(&self.exchange_id, &request, &value)
    }

    pub(super) async fn cancel_all_orders_impl(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
        }
        let (api_key, api_secret) =
            self.private_credentials("latoken.cancel_all_orders_private_rest_not_enabled")?;
        let (body, body_params) = cancel_all_body(request.symbol.as_ref())?;
        let headers = signed_rest_headers(
            &api_key,
            &api_secret,
            "POST",
            LATOKEN_CANCEL_ALL_PATH,
            &[],
            &body_params,
            LatokenDigest::Sha256,
        )?;
        let value = self
            .rest
            .send_signed_json(LATOKEN_CANCEL_ALL_PATH, headers, &body)
            .await?;
        parse_cancel_all_ack(&self.exchange_id, &request, &value)
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let order_id = request
            .exchange_order_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "LATOKEN query_order requires exchange_order_id".to_string(),
            })?;
        let path = format!("/v2/auth/order/getOrder/{order_id}");
        let (api_key, api_secret) =
            self.private_credentials("latoken.query_order_private_rest_not_enabled")?;
        let headers = signed_rest_headers(
            &api_key,
            &api_secret,
            "GET",
            &path,
            &[],
            &[],
            LatokenDigest::Sha256,
        )?;
        let value = self.rest.send_signed_get(&path, &[], headers).await?;
        parse_query_order_ack(&self.exchange_id, &request.symbol, &value)
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
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "LATOKEN get_open_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (base, quote) = split_symbol(&symbol.exchange_symbol.symbol)?;
        let mut query = vec![
            ("baseCurrency".to_string(), base),
            ("quoteCurrency".to_string(), quote),
        ];
        if let Some(page) = &request.page {
            if let Some(limit) = page.limit {
                query.push(("limit".to_string(), limit.min(1000).to_string()));
            }
        }
        let (api_key, api_secret) =
            self.private_credentials("latoken.get_open_orders_private_rest_not_enabled")?;
        let headers = signed_rest_headers(
            &api_key,
            &api_secret,
            "GET",
            LATOKEN_OPEN_ORDERS_PATH,
            &query,
            &[],
            LatokenDigest::Sha256,
        )?;
        let value = self
            .rest
            .send_signed_get(LATOKEN_OPEN_ORDERS_PATH, &query, headers)
            .await?;
        parse_open_orders_ack(&self.exchange_id, symbol, &value)
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
                message: "LATOKEN get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let (base, quote) = split_symbol(&symbol.exchange_symbol.symbol)?;
        let mut query = vec![
            ("baseCurrency".to_string(), base),
            ("quoteCurrency".to_string(), quote),
        ];
        if let Some(order_id) = &request.exchange_order_id {
            query.push(("orderId".to_string(), order_id.clone()));
        }
        if let Some(limit) = request.limit {
            query.push(("limit".to_string(), limit.min(1000).to_string()));
        }
        let (api_key, api_secret) =
            self.private_credentials("latoken.get_recent_fills_private_rest_not_enabled")?;
        let headers = signed_rest_headers(
            &api_key,
            &api_secret,
            "GET",
            LATOKEN_RECENT_FILLS_PATH,
            &query,
            &[],
            LatokenDigest::Sha256,
        )?;
        let value = self
            .rest
            .send_signed_get(LATOKEN_RECENT_FILLS_PATH, &query, headers)
            .await?;
        parse_recent_fills_ack(&self.exchange_id, tenant_id, account_id, symbol, &value)
    }

    pub(super) async fn batch_place_orders_impl(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if request.orders.is_empty() {
            return Ok(BatchPlaceOrdersResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: crate::adapters::response_metadata(
                    request.exchange,
                    request.context.request_id,
                ),
                orders: Vec::new(),
                report: None,
            });
        }
        if request.orders.len() > 50 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "LATOKEN batch place accepts at most 50 orders".to_string(),
            });
        }
        for order in &request.orders {
            self.ensure_exchange(&order.symbol.exchange)?;
            self.ensure_spot(order.symbol.market_type)?;
        }
        let (api_key, api_secret) =
            self.private_credentials("latoken.batch_place_orders_private_rest_not_enabled")?;
        let (body, body_params) = batch_place_body(&request)?;
        let headers = signed_rest_headers(
            &api_key,
            &api_secret,
            "POST",
            LATOKEN_BATCH_PLACE_PATH,
            &[],
            &body_params,
            LatokenDigest::Sha256,
        )?;
        let value = self
            .rest
            .send_signed_json(LATOKEN_BATCH_PLACE_PATH, headers, &body)
            .await?;
        parse_batch_place_orders_ack(&self.exchange_id, &request, &value)
    }

    pub(super) async fn batch_cancel_orders_impl(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if request.cancels.is_empty() {
            return Ok(BatchCancelOrdersResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: crate::adapters::response_metadata(
                    request.exchange,
                    request.context.request_id,
                ),
                orders: Vec::new(),
                cancelled_count: 0,
                report: None,
            });
        }
        if request.cancels.len() > 50 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "LATOKEN batch cancel accepts at most 50 orders".to_string(),
            });
        }
        for cancel in &request.cancels {
            self.ensure_exchange(&cancel.symbol.exchange)?;
            self.ensure_spot(cancel.symbol.market_type)?;
        }
        let (api_key, api_secret) =
            self.private_credentials("latoken.batch_cancel_orders_private_rest_not_enabled")?;
        let (body, body_params) = batch_cancel_body(&request)?;
        let headers = signed_rest_headers(
            &api_key,
            &api_secret,
            "POST",
            LATOKEN_BATCH_CANCEL_PATH,
            &[],
            &body_params,
            LatokenDigest::Sha256,
        )?;
        let value = self
            .rest
            .send_signed_json(LATOKEN_BATCH_CANCEL_PATH, headers, &body)
            .await?;
        parse_batch_cancel_orders_ack(&self.exchange_id, &request, &value)
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
                    message: "LATOKEN private REST readback requires context.tenant_id".to_string(),
                })?;
        let account_id =
            context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "LATOKEN private REST readback requires context.account_id"
                        .to_string(),
                })?;
        Ok((tenant_id, account_id))
    }
}

fn order_side_as_str(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    }
}

fn order_type_as_str(order_type: OrderType) -> &'static str {
    match order_type {
        OrderType::Market => "MARKET",
        OrderType::Limit | OrderType::PostOnly | OrderType::IOC | OrderType::FOK => "LIMIT",
        OrderType::StopMarket | OrderType::StopLimit => "LIMIT",
    }
}

fn time_in_force_as_str(time_in_force: Option<TimeInForce>) -> &'static str {
    match time_in_force.unwrap_or(TimeInForce::GTC) {
        TimeInForce::GTC | TimeInForce::GTX => "GTC",
        TimeInForce::IOC => "IOC",
        TimeInForce::FOK => "FOK",
    }
}
