#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    AmendOrderRequest, ExchangeApiError, ExchangeApiResult, OpenOrdersRequest, OpenOrdersResponse,
    PlaceOrderRequest, QueryOrderRequest, QueryOrderResponse, RecentFillsRequest,
    RecentFillsResponse, RequestContext, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, OrderSide, OrderType, TenantId};
use serde_json::{json, Value};

use super::parser::coinmate_pair;
use super::private_parser::{parse_open_orders, parse_order_state, parse_recent_fills};
use super::signing::coinmate_signed_auth_fields;
use super::CoinmateGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const BALANCES_PATH: &str = "/balances";
pub const FEES_PATH: &str = "/traderFees";
pub const OPEN_ORDERS_PATH: &str = "/openOrders";
pub const QUERY_ORDER_PATH: &str = "/orderById";
pub const ORDER_HISTORY_PATH: &str = "/orderHistory";
pub const TRADE_HISTORY_PATH: &str = "/tradeHistory";
pub const BUY_LIMIT_PATH: &str = "/buyLimit";
pub const SELL_LIMIT_PATH: &str = "/sellLimit";
pub const CANCEL_ORDER_PATH: &str = "/cancelOrder";
pub const CANCEL_ALL_ORDERS_PATH: &str = "/cancelAllOpenOrders";
pub const REPLACE_BUY_LIMIT_PATH: &str = "/replaceByBuyLimit";
pub const REPLACE_SELL_LIMIT_PATH: &str = "/replaceBySellLimit";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoinmatePrivateRequestSpec {
    pub method: &'static str,
    pub path: &'static str,
    pub body: Vec<(String, String)>,
}

pub fn coinmate_signed_form_request_spec(
    path: &'static str,
    body: Value,
    client_id: &str,
    public_key: &str,
    private_key: &str,
    nonce: &str,
) -> ExchangeApiResult<CoinmatePrivateRequestSpec> {
    let mut fields = coinmate_signed_auth_fields(client_id, public_key, private_key, nonce)?
        .into_iter()
        .collect::<Vec<_>>();
    let Value::Object(body) = body else {
        return Err(ExchangeApiError::InvalidRequest {
            message: "coinmate signed form body must be an object".to_string(),
        });
    };
    for (key, value) in body {
        fields.push((key, value_to_form_string(&value)));
    }
    Ok(CoinmatePrivateRequestSpec {
        method: "POST",
        path,
        body: fields,
    })
}

pub fn coinmate_limit_order_path(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => BUY_LIMIT_PATH,
        OrderSide::Sell => SELL_LIMIT_PATH,
    }
}

pub fn coinmate_replace_limit_path(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => REPLACE_BUY_LIMIT_PATH,
        OrderSide::Sell => REPLACE_SELL_LIMIT_PATH,
    }
}

pub struct CoinmateReplaceLimitSpec<'a> {
    pub side: OrderSide,
    pub order_id_to_be_replaced: &'a str,
    pub symbol: &'a str,
    pub amount: &'a str,
    pub price: &'a str,
    pub client_order_id: Option<&'a str>,
}

pub fn coinmate_replace_limit_form(
    spec: &CoinmateReplaceLimitSpec<'_>,
) -> ExchangeApiResult<Value> {
    non_empty(spec.order_id_to_be_replaced, "order_id_to_be_replaced")?;
    non_empty(spec.amount, "amount")?;
    non_empty(spec.price, "price")?;

    let mut body = serde_json::Map::new();
    body.insert(
        "orderIdToBeReplaced".to_string(),
        json!(spec.order_id_to_be_replaced),
    );
    body.insert(
        "currencyPair".to_string(),
        json!(coinmate_pair(spec.symbol)),
    );
    body.insert("amount".to_string(), json!(spec.amount));
    body.insert("price".to_string(), json!(spec.price));
    if let Some(client_order_id) = spec.client_order_id {
        body.insert("clientOrderId".to_string(), json!(client_order_id));
    }
    Ok(Value::Object(body))
}

pub fn coinmate_amend_order_live_guard(request: &AmendOrderRequest) -> ExchangeApiResult<()> {
    request
        .exchange_order_id
        .as_deref()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "coinmate amend_order requires exchange_order_id for replace-limit"
                .to_string(),
        })?;
    non_empty(&request.new_quantity, "new_quantity")?;
    Err(ExchangeApiError::Unsupported {
        operation:
            "coinmate.replace_order_offline_requires_side_price_nonce_and_reconciliation_guard",
    })
}

pub fn coinmate_limit_order_form(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    if request.reduce_only {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinmate.reduce_only_unsupported_spot",
        });
    }
    if !matches!(request.order_type, OrderType::Limit | OrderType::PostOnly) {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinmate.only_limit_orders_mapped_offline",
        });
    }
    let price = request
        .price
        .as_ref()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "coinmate limit order requires price".to_string(),
        })?;
    let mut body = serde_json::Map::new();
    body.insert(
        "currencyPair".to_string(),
        json!(coinmate_pair(&request.symbol.exchange_symbol.symbol)),
    );
    body.insert("amount".to_string(), json!(request.quantity));
    body.insert("price".to_string(), json!(price));
    if request.post_only || request.order_type == OrderType::PostOnly {
        body.insert("postOnly".to_string(), json!(true));
    }
    if let Some(time_in_force) = request.time_in_force {
        match time_in_force {
            TimeInForce::GTC => {}
            TimeInForce::IOC => {
                body.insert("immediateOrCancel".to_string(), json!(true));
            }
            _ => {
                return Err(ExchangeApiError::Unsupported {
                    operation: "coinmate.unsupported_time_in_force",
                })
            }
        }
    }
    if let Some(client_order_id) = &request.client_order_id {
        body.insert("clientOrderId".to_string(), json!(client_order_id));
    }
    Ok(Value::Object(body))
}

pub fn coinmate_cancel_order_form(order_id: &str) -> Value {
    json!({ "orderId": order_id })
}

pub fn coinmate_cancel_all_form(symbol: Option<&str>) -> Value {
    let mut body = serde_json::Map::new();
    if let Some(symbol) = symbol {
        body.insert("currencyPair".to_string(), json!(coinmate_pair(symbol)));
    }
    Value::Object(body)
}

pub fn coinmate_query_order_form(order_id: &str) -> Value {
    json!({ "id": order_id })
}

pub fn coinmate_history_form(symbol: &str, limit: Option<u32>) -> Value {
    let mut body = serde_json::Map::new();
    body.insert("currencyPair".to_string(), json!(coinmate_pair(symbol)));
    if let Some(limit) = limit {
        body.insert("limit".to_string(), json!(limit));
    }
    Value::Object(body)
}

pub fn redacted_auth_fields() -> Value {
    json!({
        "clientId": "<redacted:client_id>",
        "publicKey": "<redacted:public_key>",
        "nonce": "<nonce>",
        "signature": "<redacted:signature>"
    })
}

impl CoinmateGatewayAdapter {
    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinmate.query_order.client_order_id",
            });
        }
        let order_id = request.exchange_order_id.as_deref().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "coinmate query_order requires exchange_order_id".to_string(),
            }
        })?;
        let value = self
            .send_private_post(
                "coinmate.query_order",
                QUERY_ORDER_PATH,
                coinmate_query_order_form(order_id),
            )
            .await?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
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
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coinmate get_open_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let value = self
            .send_private_post(
                "coinmate.get_open_orders",
                OPEN_ORDERS_PATH,
                coinmate_history_form(&symbol.exchange_symbol.symbol, None),
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
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinmate.get_recent_fills.client_order_id",
            });
        }
        if request.from_trade_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinmate.get_recent_fills.from_trade_id",
            });
        }
        if request.start_time.is_some() || request.end_time.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinmate.get_recent_fills.time_window",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coinmate get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let (tenant_id, account_id) =
            context_account("coinmate.get_recent_fills", &request.context)?;
        let value = self
            .send_private_post(
                "coinmate.get_recent_fills",
                TRADE_HISTORY_PATH,
                coinmate_history_form(&symbol.exchange_symbol.symbol, request.limit),
            )
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?,
        })
    }

    async fn send_private_post(
        &self,
        operation: &'static str,
        path: &'static str,
        body: Value,
    ) -> ExchangeApiResult<Value> {
        let (client_id, public_key, private_key) = self.private_credentials(operation)?;
        let nonce = chrono::Utc::now().timestamp_millis().to_string();
        let spec = coinmate_signed_form_request_spec(
            path,
            body,
            client_id,
            public_key,
            private_key,
            &nonce,
        )?;
        self.rest.send_private_post(&spec).await
    }

    fn private_credentials(
        &self,
        operation: &'static str,
    ) -> ExchangeApiResult<(&str, &str, &str)> {
        if !self.config.enabled_private_rest || !self.config.private_auth_available() {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        Ok((
            self.config.client_id.as_deref().unwrap_or_default().trim(),
            self.config.public_key.as_deref().unwrap_or_default().trim(),
            self.config
                .private_key
                .as_deref()
                .unwrap_or_default()
                .trim(),
        ))
    }
}

fn non_empty<'a>(value: &'a str, field: &str) -> ExchangeApiResult<&'a str> {
    if value.trim().is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("coinmate {field} must not be empty"),
        });
    }
    Ok(value)
}

fn context_account(
    operation: &'static str,
    context: &RequestContext,
) -> ExchangeApiResult<(TenantId, AccountId)> {
    let tenant_id = context
        .tenant_id
        .clone()
        .ok_or(ExchangeApiError::Unsupported { operation })?;
    let account_id = context
        .account_id
        .clone()
        .ok_or(ExchangeApiError::Unsupported { operation })?;
    Ok((tenant_id, account_id))
}

fn value_to_form_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}
