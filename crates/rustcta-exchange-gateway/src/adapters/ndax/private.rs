#![cfg_attr(not(test), allow(dead_code))]

use std::collections::BTreeMap;

use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, OpenOrdersRequest, OpenOrdersResponse,
    PlaceOrderRequest, QueryOrderRequest, QueryOrderResponse, RecentFillsRequest,
    RecentFillsResponse, RequestContext, TenantId, TimeInForce,
};
use rustcta_types::{OrderSide, OrderType};
use serde_json::{json, Value};

use super::parser::{
    ndax_instrument_id, parse_open_orders, parse_query_order_state, parse_recent_fills,
};
use super::transport::signed_rest_request_spec;
use super::NdaxGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const GET_ACCOUNTS_PATH: &str = "/accounts";
pub const GET_ACCOUNT_POSITIONS_PATH: &str = "/accounts/{account_id}/positions";
pub const GET_OPEN_ORDERS_PATH: &str = "/orders";
pub const SEND_ORDER_PATH: &str = "/orders";
pub const CANCEL_ORDER_PATH_PREFIX: &str = "/orders";
pub const GET_FILLS_PATH: &str = "/fills";

pub fn balances_request_spec() -> Value {
    signed_rest_request_spec("GET", GET_ACCOUNTS_PATH, None)
}

pub fn query_order_path(order_id: &str) -> String {
    format!("{GET_OPEN_ORDERS_PATH}/{order_id}")
}

pub fn open_orders_request_spec(account_id: i64, instrument_id: Option<i64>) -> Value {
    let mut spec = signed_rest_request_spec("GET", GET_OPEN_ORDERS_PATH, None);
    let mut query = serde_json::Map::from_iter([("accountId".to_string(), json!(account_id))]);
    if let Some(instrument_id) = instrument_id {
        query.insert("instrumentId".to_string(), json!(instrument_id));
    }
    spec["query"] = Value::Object(query);
    spec
}

pub fn fills_request_spec(
    account_id: i64,
    instrument_id: Option<i64>,
    limit: Option<u32>,
) -> Value {
    let mut spec = signed_rest_request_spec("GET", GET_FILLS_PATH, None);
    let mut query = serde_json::Map::from_iter([("accountId".to_string(), json!(account_id))]);
    if let Some(instrument_id) = instrument_id {
        query.insert("instrumentId".to_string(), json!(instrument_id));
    }
    if let Some(limit) = limit {
        query.insert("limit".to_string(), json!(limit));
    }
    spec["query"] = Value::Object(query);
    spec
}

pub fn ndax_limit_order_body(
    request: &PlaceOrderRequest,
    account_id: i64,
    instrument_id: i64,
) -> ExchangeApiResult<Value> {
    if request.reduce_only {
        return Err(ExchangeApiError::Unsupported {
            operation: "ndax.reduce_only_unsupported_spot",
        });
    }
    if request.post_only || request.order_type == OrderType::PostOnly {
        return Err(ExchangeApiError::Unsupported {
            operation: "ndax.post_only_unverified",
        });
    }
    if request.order_type != OrderType::Limit {
        return Err(ExchangeApiError::Unsupported {
            operation: "ndax.only_limit_orders_mapped_offline",
        });
    }
    let price = request
        .price
        .as_ref()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "NDAX limit order request requires price".to_string(),
        })?;
    let mut body = serde_json::Map::new();
    body.insert("OMSId".to_string(), json!(1));
    body.insert("AccountId".to_string(), json!(account_id));
    body.insert("InstrumentId".to_string(), json!(instrument_id));
    body.insert("Side".to_string(), json!(ndax_order_side(request.side)));
    body.insert("OrderType".to_string(), json!(2));
    body.insert("Quantity".to_string(), json!(request.quantity));
    body.insert("LimitPrice".to_string(), json!(price));
    body.insert(
        "TimeInForce".to_string(),
        json!(ndax_time_in_force(request.time_in_force)?),
    );
    if let Some(client_order_id) = request.client_order_id.as_ref() {
        body.insert("ClientOrderId".to_string(), json!(client_order_id));
    }
    Ok(Value::Object(body))
}

pub fn place_order_request_spec(body: Value) -> Value {
    signed_rest_request_spec("POST", SEND_ORDER_PATH, Some(body))
}

pub fn cancel_order_body(account_id: i64, order_id: &str) -> Value {
    json!({
        "OMSId": 1,
        "AccountId": account_id,
        "OrderId": order_id
    })
}

pub fn cancel_order_request_spec(order_id: &str) -> Value {
    signed_rest_request_spec(
        "DELETE",
        &format!("{CANCEL_ORDER_PATH_PREFIX}/{order_id}"),
        None,
    )
}

fn ndax_order_side(side: OrderSide) -> i64 {
    match side {
        OrderSide::Buy => 0,
        OrderSide::Sell => 1,
    }
}

fn ndax_time_in_force(time_in_force: Option<TimeInForce>) -> ExchangeApiResult<i64> {
    match time_in_force.unwrap_or(TimeInForce::GTC) {
        TimeInForce::GTC => Ok(1),
        TimeInForce::IOC => Ok(3),
        TimeInForce::FOK => Ok(4),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "ndax.unsupported_time_in_force",
        }),
    }
}

impl NdaxGatewayAdapter {
    pub(super) fn ensure_private_rest(&self, operation: &'static str) -> ExchangeApiResult<()> {
        if !self.config.private_rest_enabled() {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        Ok(())
    }

    fn private_credentials(&self, operation: &'static str) -> ExchangeApiResult<(&str, &str)> {
        self.ensure_private_rest(operation)?;
        let api_key = self
            .config
            .api_key
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or(ExchangeApiError::Unsupported { operation })?;
        let api_secret = self
            .config
            .api_secret
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or(ExchangeApiError::Unsupported { operation })?;
        Ok((api_key, api_secret))
    }

    fn private_account_id(&self, operation: &'static str) -> ExchangeApiResult<i64> {
        self.ensure_private_rest(operation)?;
        self.config
            .account_id
            .ok_or(ExchangeApiError::Unsupported { operation })
    }

    async fn send_signed_get(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &BTreeMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let (api_key, api_secret) = self.private_credentials(operation)?;
        self.rest
            .send_signed_get(
                endpoint,
                params,
                api_key,
                api_secret,
                self.config.passphrase.as_deref(),
            )
            .await
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
                    message: "NDAX private REST readback requires context.tenant_id".to_string(),
                })?;
        let account_id =
            context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "NDAX private REST readback requires context.account_id".to_string(),
                })?;
        Ok((tenant_id, account_id))
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "ndax.query_order.client_order_id",
            });
        }
        let order_id = request.exchange_order_id.as_deref().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "NDAX query_order requires exchange_order_id".to_string(),
            }
        })?;
        let account_id = self.private_account_id("ndax.query_order_private_rest_disabled")?;
        let endpoint = query_order_path(order_id);
        let params = BTreeMap::from([("accountId".to_string(), account_id.to_string())]);
        let value = self
            .send_signed_get("ndax.query_order_private_rest_disabled", &endpoint, &params)
            .await?;
        Ok(QueryOrderResponse {
            schema_version: rustcta_exchange_api::EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order: parse_query_order_state(&self.exchange_id, &request.symbol, &value)?,
        })
    }

    pub(super) async fn get_open_orders_impl(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_supported_market_type(market_type)?;
        }
        if request.page.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "ndax.get_open_orders.pagination",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::Unsupported {
                operation: "ndax.get_open_orders.symbol_required",
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let instrument_id = ndax_instrument_id(&symbol.exchange_symbol.symbol).ok_or(
            ExchangeApiError::Unsupported {
                operation: "ndax.get_open_orders.instrument_id_unmapped",
            },
        )?;
        let account_id = self.private_account_id("ndax.get_open_orders_private_rest_disabled")?;
        let params = BTreeMap::from([
            ("accountId".to_string(), account_id.to_string()),
            ("instrumentId".to_string(), instrument_id.to_string()),
        ]);
        let value = self
            .send_signed_get(
                "ndax.get_open_orders_private_rest_disabled",
                GET_OPEN_ORDERS_PATH,
                &params,
            )
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: rustcta_exchange_api::EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_open_orders(&self.exchange_id, Some(symbol), &value)?,
        })
    }

    pub(super) async fn get_recent_fills_impl(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_supported_market_type(market_type)?;
        }
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "ndax.get_recent_fills.client_order_id",
            });
        }
        if request.from_trade_id.is_some()
            || request.start_time.is_some()
            || request.end_time.is_some()
            || request.page.is_some()
        {
            return Err(ExchangeApiError::Unsupported {
                operation: "ndax.get_recent_fills.filter_params",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::Unsupported {
                operation: "ndax.get_recent_fills.symbol_required",
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let instrument_id = ndax_instrument_id(&symbol.exchange_symbol.symbol).ok_or(
            ExchangeApiError::Unsupported {
                operation: "ndax.get_recent_fills.instrument_id_unmapped",
            },
        )?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let ndax_account_id =
            self.private_account_id("ndax.get_recent_fills_private_rest_disabled")?;
        let mut params = BTreeMap::from([
            ("accountId".to_string(), ndax_account_id.to_string()),
            ("instrumentId".to_string(), instrument_id.to_string()),
        ]);
        if let Some(limit) = request.limit {
            params.insert("limit".to_string(), limit.to_string());
        }
        let value = self
            .send_signed_get(
                "ndax.get_recent_fills_private_rest_disabled",
                GET_FILLS_PATH,
                &params,
            )
            .await?;
        let mut fills =
            parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?;
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            fills.retain(|fill| fill.order_id.as_deref() == Some(order_id));
        }
        if let Some(limit) = request.limit {
            fills.truncate(limit as usize);
        }
        Ok(RecentFillsResponse {
            schema_version: rustcta_exchange_api::EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
        })
    }
}
