use std::collections::HashMap;

use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, OpenOrdersRequest, OpenOrdersResponse, PageCursor, PlaceOrderRequest,
    PlaceOrderResponse, QueryOrderRequest, QueryOrderResponse, RecentFillsRequest,
    RecentFillsResponse, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{OrderSide, OrderStatus, OrderType};
use serde_json::{json, Value};

use super::parser::{format_decimal, symbol_scope_assets};
use super::private_parser::{
    parse_balances, parse_open_orders, parse_order_state, parse_recent_fills,
};
use super::CoinmetroGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl CoinmetroGatewayAdapter {
    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let body = coinmetro_order_body(&request)?;
        let value = self
            .send_bearer_post(
                "coinmetro.place_order",
                "/exchange/orders/create",
                &HashMap::new(),
                &body,
            )
            .await?;
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order: parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?,
        })
    }

    pub(super) async fn cancel_order_impl(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let order_id =
            request
                .exchange_order_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "coinmetro cancel_order requires exchange_order_id".to_string(),
                })?;
        let value = self
            .send_bearer_put(
                "coinmetro.cancel_order",
                &format!("/exchange/orders/cancel/{order_id}"),
                &HashMap::new(),
                &Value::Null,
            )
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .unwrap_or_else(|_| coinmetro_cancelled_order(&self.exchange_id, &request));
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order,
            cancelled: true,
        })
    }

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
        let value = self
            .send_bearer_get("coinmetro.get_balances", "/users/balances", &HashMap::new())
            .await?;
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            balances: parse_balances(
                &self.exchange_id,
                tenant_id,
                account_id,
                &request.assets,
                &value,
            )?,
        })
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let order_id =
            request
                .exchange_order_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "coinmetro query_order requires exchange_order_id".to_string(),
                })?;
        let value = self
            .send_bearer_get(
                "coinmetro.query_order",
                &format!("/exchange/orders/status/{order_id}"),
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
            self.ensure_spot(market_type)?;
        }
        let value = self
            .send_bearer_get(
                "coinmetro.get_open_orders",
                "/exchange/orders/active",
                &HashMap::new(),
            )
            .await?;
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
                message: "coinmetro get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let endpoint = match request.page.as_ref().and_then(|page| page.cursor.clone()) {
            Some(PageCursor::Timestamp { millis }) => format!("/exchange/fills/{millis}"),
            _ => "/exchange/fills".to_string(),
        };
        let value = self
            .send_bearer_get("coinmetro.get_recent_fills", &endpoint, &HashMap::new())
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?,
        })
    }
}

pub fn coinmetro_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    if request.reduce_only {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinmetro.reduce_only_unsupported_spot",
        });
    }
    if request.post_only || request.order_type == OrderType::PostOnly {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinmetro.post_only_unsupported",
        });
    }
    if request.client_order_id.is_some() {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinmetro.client_order_id_unsupported",
        });
    }
    let (base_asset, quote_asset) = symbol_scope_assets(&request.symbol)?;
    let mut body = json!({
        "orderType": match request.order_type {
            OrderType::Market => "market",
            _ => "limit",
        },
    });
    let object = body.as_object_mut().expect("object");
    match request.side {
        OrderSide::Buy => {
            object.insert("buyingCurrency".to_string(), json!(base_asset));
            object.insert("sellingCurrency".to_string(), json!(quote_asset));
            if request.order_type == OrderType::Market {
                if let Some(quote_quantity) = &request.quote_quantity {
                    object.insert("sellingQty".to_string(), json!(quote_quantity));
                } else {
                    object.insert("buyingQty".to_string(), json!(request.quantity));
                }
            } else {
                let price = required_price(request)?;
                object.insert("buyingQty".to_string(), json!(request.quantity));
                object.insert(
                    "sellingQty".to_string(),
                    json!(format_decimal(
                        decimal(&request.quantity)? * decimal(price)?
                    )),
                );
            }
        }
        OrderSide::Sell => {
            object.insert("buyingCurrency".to_string(), json!(quote_asset));
            object.insert("sellingCurrency".to_string(), json!(base_asset));
            if request.order_type == OrderType::Market {
                if request.quote_quantity.is_some() {
                    return Err(ExchangeApiError::Unsupported {
                        operation: "coinmetro.sell_quote_market_order_unsupported",
                    });
                }
                object.insert("sellingQty".to_string(), json!(request.quantity));
            } else {
                let price = required_price(request)?;
                object.insert("sellingQty".to_string(), json!(request.quantity));
                object.insert(
                    "buyingQty".to_string(),
                    json!(format_decimal(
                        decimal(&request.quantity)? * decimal(price)?
                    )),
                );
            }
        }
    }
    if let Some(time_in_force) = request.time_in_force {
        object.insert(
            "timeInForce".to_string(),
            json!(time_in_force_code(time_in_force)?),
        );
    }
    Ok(body)
}

pub fn coinmetro_cancel_path(order_id: &str) -> ExchangeApiResult<String> {
    let order_id = order_id.trim();
    if order_id.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "coinmetro order id must not be empty".to_string(),
        });
    }
    Ok(format!("/exchange/orders/cancel/{order_id}"))
}

pub fn coinmetro_query_path(order_id: &str) -> ExchangeApiResult<String> {
    let order_id = order_id.trim();
    if order_id.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "coinmetro order id must not be empty".to_string(),
        });
    }
    Ok(format!("/exchange/orders/status/{order_id}"))
}

fn required_price(request: &PlaceOrderRequest) -> ExchangeApiResult<&str> {
    request
        .price
        .as_deref()
        .filter(|price| !price.trim().is_empty())
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "coinmetro limit order requires price".to_string(),
        })
}

fn decimal(value: &str) -> ExchangeApiResult<f64> {
    value
        .trim()
        .parse::<f64>()
        .map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("invalid coinmetro decimal {value}: {error}"),
        })
}

fn time_in_force_code(time_in_force: TimeInForce) -> ExchangeApiResult<u8> {
    match time_in_force {
        TimeInForce::GTC => Ok(1),
        TimeInForce::IOC => Ok(2),
        TimeInForce::FOK => Ok(4),
        TimeInForce::GTX => Err(ExchangeApiError::Unsupported {
            operation: "coinmetro.gtx_time_in_force_unsupported",
        }),
    }
}

fn coinmetro_cancelled_order(
    exchange_id: &rustcta_types::ExchangeId,
    request: &CancelOrderRequest,
) -> rustcta_exchange_api::OrderState {
    rustcta_exchange_api::OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request.client_order_id.clone(),
        exchange_order_id: request.exchange_order_id.clone(),
        side: OrderSide::Buy,
        position_side: Some(rustcta_types::PositionSide::None),
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
        updated_at: chrono::Utc::now(),
    }
}
