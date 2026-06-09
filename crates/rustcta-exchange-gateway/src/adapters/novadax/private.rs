#![cfg_attr(not(test), allow(dead_code))]

use std::collections::BTreeMap;

use chrono::Utc;
use rustcta_exchange_api::{
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchItemResult, BatchOperationReport,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelOrderRequest, CancelOrderResponse,
    ExchangeApiError, ExchangeApiResult, OpenOrdersRequest, OpenOrdersResponse, OrderState,
    PlaceOrderRequest, PlaceOrderResponse, QueryOrderRequest, QueryOrderResponse,
    RecentFillsRequest, RecentFillsResponse, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    ExchangeError, ExchangeErrorClass, Fill, FillStatus, LiquidityRole, OrderSide, OrderStatus,
    OrderType, PositionSide, SchemaVersion,
};
use serde_json::{json, Value};

use super::parser::novadax_symbol;
use super::signing::private_request_spec;
use super::NovadaxGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

pub const ACCOUNT_BALANCE_PATH: &str = "/v1/account/getBalance";
pub const CREATE_ORDER_PATH: &str = "/v1/orders/create";
pub const BATCH_CREATE_ORDER_PATH: &str = "/v1/orders/batch-create";
pub const CANCEL_ORDER_PATH: &str = "/v1/orders/cancel";
pub const BATCH_CANCEL_ORDER_PATH: &str = "/v1/orders/batch-cancel";
pub const CANCEL_BY_SYMBOL_PATH: &str = "/v1/orders/cancel-by-symbol";
pub const GET_ORDER_PATH: &str = "/v1/orders/get";
pub const LIST_ORDERS_PATH: &str = "/v1/orders/list";
pub const FILLS_PATH: &str = "/v1/orders/fills";

pub fn novadax_place_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    let order_type = match request.order_type {
        OrderType::Market => "MARKET",
        OrderType::Limit => "LIMIT",
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "novadax.unsupported_order_type",
            })
        }
    };
    let side = match request.side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    };
    let mut body = serde_json::Map::new();
    body.insert(
        "symbol".to_string(),
        json!(novadax_symbol(&request.symbol.exchange_symbol.symbol)),
    );
    body.insert("side".to_string(), json!(side));
    body.insert("type".to_string(), json!(order_type));
    if let Some(client_id) = &request.client_order_id {
        body.insert("clientOrderId".to_string(), json!(client_id));
    }
    if matches!(request.order_type, OrderType::Market)
        && matches!(request.side, OrderSide::Buy)
        && request.quote_quantity.is_some()
    {
        body.insert(
            "value".to_string(),
            json!(request.quote_quantity.as_deref()),
        );
    } else {
        body.insert("amount".to_string(), json!(request.quantity));
    }
    if matches!(request.order_type, OrderType::Limit) {
        body.insert(
            "price".to_string(),
            json!(request.price.as_deref().ok_or_else(|| {
                ExchangeApiError::InvalidRequest {
                    message: "novadax limit order requires price".to_string(),
                }
            })?),
        );
    }
    Ok(Value::Object(body))
}

pub fn create_order_request_spec_fixture() -> Value {
    let body = json!({
        "symbol": "BTC_BRL",
        "side": "BUY",
        "type": "LIMIT",
        "amount": "0.01",
        "price": "350000",
        "clientOrderId": "offline-fixture"
    });
    private_request_spec("POST", CREATE_ORDER_PATH, &BTreeMap::new(), Some(body))
}

pub fn novadax_batch_place_body(request: &BatchPlaceOrdersRequest) -> ExchangeApiResult<Value> {
    let orders = request
        .orders
        .iter()
        .map(novadax_place_order_body)
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    Ok(json!({ "orders": orders }))
}

pub fn novadax_batch_cancel_body(request: &BatchCancelOrdersRequest) -> ExchangeApiResult<Value> {
    let ids = request
        .cancels
        .iter()
        .map(|cancel| {
            cancel
                .exchange_order_id
                .as_deref()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "novadax batch cancel requires exchange_order_id".to_string(),
                })
        })
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    Ok(json!({ "ids": ids }))
}

impl NovadaxGatewayAdapter {
    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        let (api_key, api_secret) =
            self.private_credentials("novadax.place_order_private_rest_not_enabled")?;
        let body = novadax_place_order_body(&request)?;
        let value = self
            .rest
            .send_signed_json("POST", CREATE_ORDER_PATH, &api_key, &api_secret, &body)
            .await?;
        let row = unwrap_data(&value);
        let order = order_state_from_place(&self.exchange_id, &request, row);
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: crate::adapters::response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order,
        })
    }

    pub(super) async fn cancel_order_impl(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        let exchange_order_id = request.exchange_order_id.as_deref().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "novadax cancel_order requires exchange_order_id".to_string(),
            }
        })?;
        let (api_key, api_secret) =
            self.private_credentials("novadax.cancel_order_private_rest_not_enabled")?;
        let body = json!({ "id": exchange_order_id });
        let value = self
            .rest
            .send_signed_json("POST", CANCEL_ORDER_PATH, &api_key, &api_secret, &body)
            .await?;
        let row = unwrap_data(&value);
        let order = order_state_from_cancel(&self.exchange_id, &request, row);
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: crate::adapters::response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order,
            cancelled: true,
        })
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
        for order in &request.orders {
            self.ensure_exchange(&order.symbol.exchange)?;
            self.ensure_supported_market_type(order.symbol.market_type)?;
        }
        let (api_key, api_secret) =
            self.private_credentials("novadax.batch_place_orders_private_rest_not_enabled")?;
        let body = novadax_batch_place_body(&request)?;
        let value = self
            .rest
            .send_signed_json(
                "POST",
                BATCH_CREATE_ORDER_PATH,
                &api_key,
                &api_secret,
                &body,
            )
            .await?;
        let (orders, report) = parse_batch_place_ack(&self.exchange_id, &request, &value);
        Ok(BatchPlaceOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: crate::adapters::response_metadata(
                request.exchange,
                request.context.request_id,
            ),
            orders,
            report: Some(report),
        })
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
        for cancel in &request.cancels {
            self.ensure_exchange(&cancel.symbol.exchange)?;
            self.ensure_supported_market_type(cancel.symbol.market_type)?;
        }
        let (api_key, api_secret) =
            self.private_credentials("novadax.batch_cancel_orders_private_rest_not_enabled")?;
        let body = novadax_batch_cancel_body(&request)?;
        let value = self
            .rest
            .send_signed_json(
                "POST",
                BATCH_CANCEL_ORDER_PATH,
                &api_key,
                &api_secret,
                &body,
            )
            .await?;
        let (orders, report) = parse_batch_cancel_ack(&self.exchange_id, &request, &value);
        Ok(BatchCancelOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: crate::adapters::response_metadata(
                request.exchange,
                request.context.request_id,
            ),
            cancelled_count: orders.len() as u32,
            orders,
            report: Some(report),
        })
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        let order_id = request.exchange_order_id.as_deref().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "novadax query_order requires exchange_order_id".to_string(),
            }
        })?;
        let (api_key, api_secret) =
            self.private_credentials("novadax.query_order_private_rest_not_enabled")?;
        let params = BTreeMap::from([("id".to_string(), order_id.to_string())]);
        let value = self
            .rest
            .send_signed_get(GET_ORDER_PATH, &params, &api_key, &api_secret)
            .await?;
        let order = order_state_from_row(&self.exchange_id, &request.symbol, unwrap_data(&value))?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: crate::adapters::response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order: Some(order),
        })
    }

    pub(super) async fn get_open_orders_impl(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "novadax get_open_orders requires symbol scope".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let (api_key, api_secret) =
            self.private_credentials("novadax.get_open_orders_private_rest_not_enabled")?;
        let params = open_orders_query_with_limit(
            &symbol.exchange_symbol.symbol,
            request.page.as_ref().and_then(|page| page.limit),
        );
        let value = self
            .rest
            .send_signed_get(LIST_ORDERS_PATH, &params, &api_key, &api_secret)
            .await?;
        let orders = rows_from_items(&value)
            .iter()
            .map(|row| order_state_from_row(&self.exchange_id, symbol, row))
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: crate::adapters::response_metadata(
                request.exchange,
                request.context.request_id,
            ),
            orders,
        })
    }

    pub(super) async fn get_recent_fills_impl(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "novadax get_recent_fills requires symbol scope".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let tenant_id =
            request
                .context
                .tenant_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "novadax get_recent_fills requires tenant_id".to_string(),
                })?;
        let account_id =
            request
                .context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "novadax get_recent_fills requires account_id".to_string(),
                })?;
        let (api_key, api_secret) =
            self.private_credentials("novadax.get_recent_fills_private_rest_not_enabled")?;
        let params = fills_query_with_limit(
            &symbol.exchange_symbol.symbol,
            request
                .limit
                .or_else(|| request.page.as_ref().and_then(|page| page.limit)),
        );
        let value = self
            .rest
            .send_signed_get(FILLS_PATH, &params, &api_key, &api_secret)
            .await?;
        let fills = rows_from_items(&value)
            .iter()
            .map(|row| fill_from_row(&self.exchange_id, symbol, &tenant_id, &account_id, row))
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: crate::adapters::response_metadata(
                request.exchange,
                request.context.request_id,
            ),
            fills,
        })
    }
}

fn parse_batch_place_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &BatchPlaceOrdersRequest,
    value: &Value,
) -> (Vec<OrderState>, BatchOperationReport) {
    let rows = value
        .get("data")
        .and_then(Value::as_array)
        .or_else(|| value.as_array())
        .cloned()
        .unwrap_or_default();
    let mut orders = Vec::new();
    let mut results = Vec::with_capacity(request.orders.len());
    for (index, order_request) in request.orders.iter().enumerate() {
        if let Some(row) = rows.get(index) {
            let order = order_state_from_place(exchange_id, order_request, row);
            orders.push(order.clone());
            results.push(BatchItemResult::success(index, order));
        } else {
            results.push(BatchItemResult::failed(
                index,
                order_request.client_order_id.clone(),
                None,
                batch_error(exchange_id, "NovaDAX batch-create response omitted item"),
                None,
            ));
        }
    }
    (
        orders,
        BatchOperationReport {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange_id.clone(),
            total_items: request.orders.len(),
            results,
        },
    )
}

fn parse_batch_cancel_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &BatchCancelOrdersRequest,
    value: &Value,
) -> (Vec<OrderState>, BatchOperationReport) {
    let rows = value
        .get("data")
        .and_then(Value::as_array)
        .or_else(|| value.as_array())
        .cloned()
        .unwrap_or_default();
    let mut orders = Vec::new();
    let mut results = Vec::with_capacity(request.cancels.len());
    for (index, cancel_request) in request.cancels.iter().enumerate() {
        let row = rows.get(index).unwrap_or(&Value::Null);
        let order = order_state_from_cancel(exchange_id, cancel_request, row);
        orders.push(order.clone());
        results.push(BatchItemResult::success(index, order));
    }
    (
        orders,
        BatchOperationReport {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange_id.clone(),
            total_items: request.cancels.len(),
            results,
        },
    )
}

fn order_state_from_place(
    exchange_id: &rustcta_types::ExchangeId,
    request: &PlaceOrderRequest,
    value: &Value,
) -> OrderState {
    let now = Utc::now();
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request
            .client_order_id
            .clone()
            .or_else(|| string(value, "clientOrderId")),
        exchange_order_id: string(value, "id").or_else(|| string(value, "orderId")),
        side: request.side,
        position_side: Some(PositionSide::None),
        order_type: request.order_type,
        time_in_force: request.time_in_force,
        status: OrderStatus::New,
        quantity: request.quantity.clone(),
        price: request.price.clone(),
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: request.post_only,
        created_at: Some(now),
        updated_at: now,
    }
}

fn order_state_from_cancel(
    exchange_id: &rustcta_types::ExchangeId,
    request: &CancelOrderRequest,
    value: &Value,
) -> OrderState {
    let now = Utc::now();
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request.client_order_id.clone(),
        exchange_order_id: request
            .exchange_order_id
            .clone()
            .or_else(|| string(value, "id").or_else(|| string(value, "orderId"))),
        side: OrderSide::Buy,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Limit,
        time_in_force: None,
        status: OrderStatus::Cancelled,
        quantity: "0".to_string(),
        price: None,
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: None,
        updated_at: now,
    }
}

fn order_state_from_row(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let now = Utc::now();
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id: string(value, "clientOrderId"),
        exchange_order_id: string(value, "id").or_else(|| string(value, "orderId")),
        side: parse_side(value)?,
        position_side: Some(PositionSide::None),
        order_type: parse_order_type(value)?,
        time_in_force: None,
        status: parse_order_status(value),
        quantity: string(value, "amount").unwrap_or_else(|| "0".to_string()),
        price: string(value, "price"),
        filled_quantity: string(value, "filledAmount").unwrap_or_else(|| "0".to_string()),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: None,
        updated_at: now,
    })
}

fn fill_from_row(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: &SymbolScope,
    tenant_id: &rustcta_types::TenantId,
    account_id: &rustcta_types::AccountId,
    value: &Value,
) -> ExchangeApiResult<Fill> {
    let now = Utc::now();
    let price = parse_decimal_field(value, "price")?;
    let quantity = parse_decimal_field(value, "amount")?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "novadax fill parsing requires canonical_symbol".to_string(),
            })?;
    Ok(Fill {
        schema_version: SchemaVersion::current(),
        tenant_id: tenant_id.clone(),
        account_id: account_id.clone(),
        exchange_id: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol,
        exchange_symbol: Some(symbol.exchange_symbol.clone()),
        order_id: string(value, "orderId"),
        client_order_id: string(value, "clientOrderId"),
        fill_id: string(value, "id").or_else(|| string(value, "tradeId")),
        side: parse_side(value)?,
        position_side: PositionSide::None,
        status: FillStatus::Confirmed,
        liquidity_role: LiquidityRole::Unknown,
        price,
        quantity,
        quote_quantity: Some(price * quantity),
        fee_asset: string(value, "feeCurrency"),
        fee_amount: optional_decimal_field(value, "fee")?,
        fee_rate: None,
        realized_pnl: None,
        filled_at: now,
        received_at: now,
    })
}

fn parse_side(value: &Value) -> ExchangeApiResult<OrderSide> {
    match string(value, "side").as_deref() {
        Some("BUY") | Some("buy") => Ok(OrderSide::Buy),
        Some("SELL") | Some("sell") => Ok(OrderSide::Sell),
        other => Err(ExchangeApiError::InvalidRequest {
            message: format!("novadax order row has unsupported side {other:?}"),
        }),
    }
}

fn parse_order_type(value: &Value) -> ExchangeApiResult<OrderType> {
    match string(value, "type").as_deref() {
        Some("MARKET") | Some("market") => Ok(OrderType::Market),
        Some("LIMIT") | Some("limit") => Ok(OrderType::Limit),
        other => Err(ExchangeApiError::InvalidRequest {
            message: format!("novadax order row has unsupported type {other:?}"),
        }),
    }
}

fn parse_order_status(value: &Value) -> OrderStatus {
    match string(value, "status").as_deref() {
        Some("SUBMITTED") => OrderStatus::Open,
        Some("PARTIAL_FILLED") => OrderStatus::PartiallyFilled,
        Some("FILLED") => OrderStatus::Filled,
        Some("CANCELED") | Some("CANCELLED") => OrderStatus::Cancelled,
        Some("REJECTED") => OrderStatus::Rejected,
        Some("EXPIRED") => OrderStatus::Expired,
        _ => OrderStatus::Unknown,
    }
}

fn parse_decimal_field(value: &Value, field: &str) -> ExchangeApiResult<f64> {
    string(value, field)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("novadax row missing {field}"),
        })?
        .parse::<f64>()
        .map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("novadax row invalid {field}: {error}"),
        })
}

fn optional_decimal_field(value: &Value, field: &str) -> ExchangeApiResult<Option<f64>> {
    string(value, field)
        .map(|raw| {
            raw.parse::<f64>()
                .map_err(|error| ExchangeApiError::InvalidRequest {
                    message: format!("novadax row invalid {field}: {error}"),
                })
        })
        .transpose()
}

fn unwrap_data(value: &Value) -> &Value {
    value.get("data").unwrap_or(value)
}

fn rows_from_items(value: &Value) -> Vec<Value> {
    unwrap_data(value)
        .get("items")
        .or_else(|| unwrap_data(value).get("list"))
        .or_else(|| value.get("data"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

fn string(value: &Value, field: &str) -> Option<String> {
    match value.get(field)? {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn batch_error(exchange_id: &rustcta_types::ExchangeId, message: &str) -> ExchangeError {
    ExchangeError::new(
        exchange_id.clone(),
        ExchangeErrorClass::UnknownOrderState,
        message,
        Utc::now(),
    )
}

pub fn cancel_order_request_spec_fixture() -> Value {
    let body = json!({ "id": "order-1" });
    private_request_spec("POST", CANCEL_ORDER_PATH, &BTreeMap::new(), Some(body))
}

pub fn cancel_by_symbol_request_spec_fixture(symbol: &str) -> Value {
    let body = json!({ "symbol": novadax_symbol(symbol) });
    private_request_spec("POST", CANCEL_BY_SYMBOL_PATH, &BTreeMap::new(), Some(body))
}

pub fn open_orders_query(symbol: &str) -> BTreeMap<String, String> {
    open_orders_query_with_limit(symbol, None)
}

fn open_orders_query_with_limit(symbol: &str, limit: Option<u32>) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("symbol".to_string(), novadax_symbol(symbol)),
        ("status".to_string(), "SUBMITTED,PARTIAL_FILLED".to_string()),
        ("page".to_string(), "1".to_string()),
        (
            "limit".to_string(),
            limit.unwrap_or(100).min(100).to_string(),
        ),
    ])
}

pub fn fills_query(symbol: &str) -> BTreeMap<String, String> {
    fills_query_with_limit(symbol, None)
}

fn fills_query_with_limit(symbol: &str, limit: Option<u32>) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("symbol".to_string(), novadax_symbol(symbol)),
        ("page".to_string(), "1".to_string()),
        (
            "limit".to_string(),
            limit.unwrap_or(100).min(100).to_string(),
        ),
    ])
}
