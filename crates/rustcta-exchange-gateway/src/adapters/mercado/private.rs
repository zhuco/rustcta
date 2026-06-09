#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OpenOrdersRequest, OpenOrdersResponse, PlaceOrderRequest,
    QueryOrderRequest, QueryOrderResponse, RecentFillsRequest, RecentFillsResponse,
};
use rustcta_types::{OrderSide, OrderType};
use serde_json::{json, Value};

use super::parser::mercado_symbol;
use super::private_parser::{parse_open_orders, parse_order_state, parse_recent_fills};
use super::MercadoGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const ACCOUNTS_PATH: &str = "/accounts";

pub fn mercado_orders_path(account_id: &str, symbol: &str) -> String {
    format!("/accounts/{account_id}/{}/orders", mercado_symbol(symbol))
}

pub fn mercado_order_path(account_id: &str, symbol: &str, order_id: &str) -> String {
    format!("{}/{order_id}", mercado_orders_path(account_id, symbol))
}

pub fn mercado_cancel_all_path(account_id: &str) -> String {
    format!("/accounts/{account_id}/cancel_all_open_orders")
}

pub fn mercado_recent_fills_path(account_id: &str, symbol: &str) -> String {
    format!("/accounts/{account_id}/{}/trades", mercado_symbol(symbol))
}

pub fn mercado_place_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    let order_type = match request.order_type {
        OrderType::Market => "market",
        OrderType::Limit => "limit",
        OrderType::PostOnly => "post-only",
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "mercado.unsupported_order_type",
            })
        }
    };
    let mut body = serde_json::Map::new();
    body.insert(
        "side".to_string(),
        json!(match request.side {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        }),
    );
    body.insert("type".to_string(), json!(order_type));
    if let Some(client_id) = &request.client_order_id {
        body.insert("externalId".to_string(), json!(client_id));
    }
    if let Some(quote_quantity) = &request.quote_quantity {
        body.insert("cost".to_string(), json!(quote_quantity));
    } else {
        body.insert("qty".to_string(), json!(request.quantity));
    }
    if !matches!(request.order_type, OrderType::Market) {
        body.insert(
            "limitPrice".to_string(),
            json!(request.price.as_deref().ok_or_else(|| {
                ExchangeApiError::InvalidRequest {
                    message: "mercado limit order requires price".to_string(),
                }
            })?),
        );
    }
    Ok(Value::Object(body))
}

pub fn create_order_request_spec_fixture() -> Value {
    json!({
        "method": "POST",
        "path": "/accounts/<accountId>/BTC-BRL/orders",
        "auth": "bearer",
        "headers": {
            "Authorization": "Bearer <redacted>",
            "Content-Type": "application/json"
        },
        "body": {
            "side": "buy",
            "type": "limit",
            "qty": "0.01",
            "limitPrice": "350000",
            "externalId": "offline-fixture"
        }
    })
}

impl MercadoGatewayAdapter {
    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "mercado.query_order.client_order_id",
            });
        }
        let order_id = request.exchange_order_id.as_deref().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "mercado query_order requires exchange_order_id".to_string(),
            }
        })?;
        let account_id = self.private_account_id("mercado.query_order")?;
        let value = self
            .send_bearer_get(
                "mercado.query_order",
                &mercado_order_path(account_id, &request.symbol.exchange_symbol.symbol, order_id),
                &HashMap::new(),
            )
            .await?;
        Ok(QueryOrderResponse {
            schema_version: rustcta_exchange_api::EXCHANGE_API_SCHEMA_VERSION,
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
                operation: "mercado.get_open_orders.pagination",
            });
        }
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
        }
        let account_id = self.private_account_id("mercado.get_open_orders")?;
        let value = self
            .send_bearer_get(
                "mercado.get_open_orders",
                &format!("/accounts/{account_id}/orders"),
                &HashMap::new(),
            )
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: rustcta_exchange_api::EXCHANGE_API_SCHEMA_VERSION,
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
            self.ensure_supported_market_type(market_type)?;
        }
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "mercado.get_recent_fills.client_order_id",
            });
        }
        if request.from_trade_id.is_some()
            || request.start_time.is_some()
            || request.end_time.is_some()
        {
            return Err(ExchangeApiError::Unsupported {
                operation: "mercado.get_recent_fills.filter_params",
            });
        }
        if request.page.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "mercado.get_recent_fills.pagination",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "mercado get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mercado_account_id = self.private_account_id("mercado.get_recent_fills")?;
        let value = self
            .send_bearer_get(
                "mercado.get_recent_fills",
                &mercado_recent_fills_path(mercado_account_id, &symbol.exchange_symbol.symbol),
                &HashMap::new(),
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
