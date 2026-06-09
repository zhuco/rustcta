#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OpenOrdersRequest, OpenOrdersResponse, PlaceOrderRequest,
    QueryOrderRequest, QueryOrderResponse, RecentFillsRequest, RecentFillsResponse, RequestContext,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{OrderSide, OrderType, TimeInForce};
use serde_json::{json, Value};

use super::parser::foxbit_symbol;
use super::private_parser::{parse_open_orders, parse_order_state, parse_recent_fills};
use super::FoxbitGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const ORDERS_PATH: &str = "/orders";
pub const BALANCES_PATH: &str = "/balances";
pub const OPEN_ORDERS_PATH: &str = "/orders";
pub const FILLS_PATH: &str = "/trades";

pub fn foxbit_order_path(order_id: &str) -> String {
    format!("{ORDERS_PATH}/{order_id}")
}

pub fn foxbit_place_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    let order_type = match request.order_type {
        OrderType::Market => "MARKET",
        OrderType::Limit => "LIMIT",
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "foxbit.unsupported_order_type",
            })
        }
    };
    let mut body = serde_json::Map::new();
    body.insert(
        "side".to_string(),
        json!(match request.side {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        }),
    );
    body.insert("type".to_string(), json!(order_type));
    body.insert(
        "market_symbol".to_string(),
        json!(foxbit_symbol(&request.symbol.exchange_symbol.symbol)),
    );
    if let Some(client_id) = &request.client_order_id {
        body.insert("client_order_id".to_string(), json!(client_id));
    }
    if let Some(quote_quantity) = &request.quote_quantity {
        body.insert("amount".to_string(), json!(quote_quantity));
    } else {
        body.insert("quantity".to_string(), json!(request.quantity));
    }
    if !matches!(request.order_type, OrderType::Market) {
        body.insert(
            "price".to_string(),
            json!(request.price.as_deref().ok_or_else(|| {
                ExchangeApiError::InvalidRequest {
                    message: "foxbit limit order requires price".to_string(),
                }
            })?),
        );
    }
    if let Some(time_in_force) = request.time_in_force {
        body.insert(
            "time_in_force".to_string(),
            json!(foxbit_time_in_force(time_in_force)),
        );
    }
    Ok(Value::Object(body))
}

fn foxbit_time_in_force(time_in_force: TimeInForce) -> &'static str {
    match time_in_force {
        TimeInForce::GTC => "GTC",
        TimeInForce::IOC => "IOC",
        TimeInForce::FOK => "FOK",
        TimeInForce::GTX => "GTX",
    }
}

pub fn create_order_request_spec_fixture() -> Value {
    super::transport::signed_request_spec(
        "POST",
        ORDERS_PATH,
        "",
        Some(json!({
            "side": "BUY",
            "type": "LIMIT",
            "market_symbol": "btcbrl",
            "quantity": "0.01",
            "price": "350000",
            "client_order_id": "offline-fixture"
        })),
    )
}

impl FoxbitGatewayAdapter {
    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "foxbit.query_order.client_order_id",
            });
        }
        let order_id = request
            .exchange_order_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "foxbit query_order requires exchange_order_id".to_string(),
            })?;
        let (api_key, api_secret, receive_window_ms) =
            self.private_credentials("foxbit.query_order")?;
        let value = self
            .rest
            .send_private_get(
                api_key,
                api_secret,
                receive_window_ms,
                &foxbit_order_path(order_id.trim()),
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
                operation: "foxbit.get_open_orders.pagination",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "foxbit get_open_orders requires symbol scope".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let params = HashMap::from([
            (
                "market_symbol".to_string(),
                foxbit_symbol(&symbol.exchange_symbol.symbol),
            ),
            ("state".to_string(), "ACTIVE".to_string()),
        ]);
        let (api_key, api_secret, receive_window_ms) =
            self.private_credentials("foxbit.get_open_orders")?;
        let value = self
            .rest
            .send_private_get(
                api_key,
                api_secret,
                receive_window_ms,
                OPEN_ORDERS_PATH,
                &params,
            )
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
        if request.client_order_id.is_some()
            || request.from_trade_id.is_some()
            || request.start_time.is_some()
            || request.end_time.is_some()
            || request.page.is_some()
        {
            return Err(ExchangeApiError::Unsupported {
                operation: "foxbit.get_recent_fills.filters",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "foxbit get_recent_fills requires symbol scope".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let mut params = HashMap::from([(
            "market_symbol".to_string(),
            foxbit_symbol(&symbol.exchange_symbol.symbol),
        )]);
        if let Some(order_id) = request
            .exchange_order_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            params.insert("order_id".to_string(), order_id.trim().to_string());
        }
        if let Some(limit) = request.limit {
            if limit == 0 {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "foxbit get_recent_fills limit must be positive".to_string(),
                });
            }
            params.insert("limit".to_string(), limit.min(100).to_string());
        }
        let (tenant_id, account_id) =
            self.context_account("foxbit.get_recent_fills", &request.context)?;
        let (api_key, api_secret, receive_window_ms) =
            self.private_credentials("foxbit.get_recent_fills")?;
        let value = self
            .rest
            .send_private_get(api_key, api_secret, receive_window_ms, FILLS_PATH, &params)
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?,
        })
    }

    fn private_credentials(
        &self,
        operation: &'static str,
    ) -> ExchangeApiResult<(&str, &str, Option<u64>)> {
        if !self.config.private_rest_available() {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        Ok((
            self.config.api_key.as_deref().unwrap_or_default(),
            self.config.api_secret.as_deref().unwrap_or_default(),
            self.config.receive_window_ms,
        ))
    }

    fn context_account(
        &self,
        operation: &'static str,
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
                    message: format!("{operation} requires context.tenant_id"),
                })?;
        let account_id =
            context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: format!("{operation} requires context.account_id"),
                })?;
        Ok((tenant_id, account_id))
    }
}
