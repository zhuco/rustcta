#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OpenOrdersRequest, OpenOrdersResponse, PlaceOrderRequest,
    QueryOrderRequest, QueryOrderResponse, RecentFillsRequest, RecentFillsResponse, RequestContext,
    TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{OrderSide, OrderType};
use serde_json::{json, Value};

use super::parser::bitso_book;
use super::private_parser::{parse_open_orders, parse_order_state, parse_recent_fills};
use super::BitsoGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const PLACE_ORDER_PATH: &str = "/orders";
pub const CANCEL_ORDER_PATH_PREFIX: &str = "/orders";
pub const OPEN_ORDERS_PATH: &str = "/open_orders";
pub const USER_TRADES_PATH: &str = "/user_trades";

pub fn bitso_place_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    let order_type = match request.order_type {
        OrderType::Market => "market",
        OrderType::Limit | OrderType::PostOnly | OrderType::IOC | OrderType::FOK => "limit",
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitso.unsupported_order_type",
            })
        }
    };
    let mut body = serde_json::Map::new();
    body.insert(
        "book".to_string(),
        json!(bitso_book(&request.symbol.exchange_symbol.symbol)),
    );
    body.insert(
        "side".to_string(),
        json!(match request.side {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        }),
    );
    body.insert("type".to_string(), json!(order_type));
    if let Some(client_id) = &request.client_order_id {
        body.insert("origin_id".to_string(), json!(client_id));
    }
    if matches!(request.order_type, OrderType::Market) {
        if let Some(quote_quantity) = &request.quote_quantity {
            body.insert("minor".to_string(), json!(quote_quantity));
        } else {
            body.insert("major".to_string(), json!(request.quantity));
        }
    } else {
        body.insert("major".to_string(), json!(request.quantity));
        body.insert(
            "price".to_string(),
            json!(request.price.as_deref().ok_or_else(|| {
                ExchangeApiError::InvalidRequest {
                    message: "bitso limit order requires price".to_string(),
                }
            })?),
        );
    }
    if request.post_only || request.order_type == OrderType::PostOnly {
        body.insert("time_in_force".to_string(), json!("postonly"));
    } else if let Some(tif) = request.time_in_force {
        body.insert(
            "time_in_force".to_string(),
            json!(bitso_time_in_force(tif)?),
        );
    }
    Ok(Value::Object(body))
}

pub fn bitso_cancel_order_path(exchange_order_id: &str) -> String {
    format!("{CANCEL_ORDER_PATH_PREFIX}/{exchange_order_id}")
}

pub fn create_order_request_spec_fixture() -> Value {
    json!({
        "method": "POST",
        "path": "/api/v3/orders",
        "auth": "bitso_hmac_sha256",
        "headers": {
            "Authorization": "Bitso <key>:<nonce>:<signature>",
            "Content-Type": "application/json"
        },
        "body": {
            "book": "btc_mxn",
            "side": "buy",
            "type": "limit",
            "major": "0.01",
            "price": "650000",
            "time_in_force": "goodtillcancelled",
            "origin_id": "offline-fixture"
        }
    })
}

impl BitsoGatewayAdapter {
    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitso.query_order.client_order_id",
            });
        }
        let order_id = request
            .exchange_order_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitso query_order requires exchange_order_id".to_string(),
            })?;
        let (api_key, api_secret) = self.private_credentials("bitso.query_order")?;
        let value = self
            .rest
            .send_signed_get(
                api_key,
                api_secret,
                &bitso_cancel_order_path(order_id.trim()),
                &HashMap::new(),
            )
            .await?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order: Some(parse_order_state(
                &self.exchange_id,
                Some(&request.symbol),
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
            self.ensure_supported_market_type(market_type)?;
        }
        if request.page.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitso.get_open_orders.pagination",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitso get_open_orders requires symbol book scope".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "book".to_string(),
            bitso_book(&symbol.exchange_symbol.symbol),
        );
        let (api_key, api_secret) = self.private_credentials("bitso.get_open_orders")?;
        let value = self
            .rest
            .send_signed_get(api_key, api_secret, OPEN_ORDERS_PATH, &params)
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
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
                operation: "bitso.get_recent_fills.client_order_id",
            });
        }
        if request.exchange_order_id.is_some()
            || request.from_trade_id.is_some()
            || request.start_time.is_some()
            || request.end_time.is_some()
            || request.page.is_some()
        {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitso.get_recent_fills.filters",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitso get_recent_fills requires symbol book scope".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "book".to_string(),
            bitso_book(&symbol.exchange_symbol.symbol),
        );
        if let Some(limit) = request.limit {
            if limit == 0 {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "bitso get_recent_fills limit must be positive".to_string(),
                });
            }
            params.insert("limit".to_string(), limit.min(100).to_string());
        }
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let (api_key, api_secret) = self.private_credentials("bitso.get_recent_fills")?;
        let value = self
            .rest
            .send_signed_get(api_key, api_secret, USER_TRADES_PATH, &params)
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?,
        })
    }

    pub(super) fn private_credentials(
        &self,
        operation: &'static str,
    ) -> ExchangeApiResult<(&str, &str)> {
        if !self.config.private_rest_available() {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        Ok((
            self.config.api_key.as_deref().unwrap_or_default(),
            self.config.api_secret.as_deref().unwrap_or_default(),
        ))
    }

    fn context_account(
        &self,
        context: &RequestContext,
    ) -> ExchangeApiResult<(
        rustcta_exchange_api::TenantId,
        rustcta_exchange_api::AccountId,
    )> {
        let tenant_id =
            context
                .tenant_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "bitso private REST readback requires context.tenant_id".to_string(),
                })?;
        let account_id =
            context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "bitso private REST readback requires context.account_id".to_string(),
                })?;
        Ok((tenant_id, account_id))
    }
}

fn bitso_time_in_force(tif: TimeInForce) -> ExchangeApiResult<&'static str> {
    match tif {
        TimeInForce::GTC => Ok("goodtillcancelled"),
        TimeInForce::IOC => Ok("immediateorcancel"),
        TimeInForce::FOK => Ok("fillorkill"),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "bitso.unsupported_time_in_force",
        }),
    }
}
