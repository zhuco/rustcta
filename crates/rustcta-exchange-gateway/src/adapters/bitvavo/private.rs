use std::collections::HashMap;

use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, CancelAllOrdersRequest, CancelAllOrdersResponse,
    CancelOrderRequest, CancelOrderResponse, ExchangeApiError, ExchangeApiResult, FeesRequest,
    FeesResponse, OpenOrdersRequest, OpenOrdersResponse, PageCursor, PlaceOrderRequest,
    PlaceOrderResponse, QueryOrderRequest, QueryOrderResponse, RecentFillsRequest,
    RecentFillsResponse, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{OrderStatus, OrderType};
use serde_json::{json, Value};

use super::parser::normalize_bitvavo_symbol;
use super::private_parser::{
    parse_balances, parse_fee_snapshots, parse_open_orders, parse_order_state, parse_recent_fills,
};
use super::BitvavoGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl BitvavoGatewayAdapter {
    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let body = bitvavo_order_body(&request)?;
        let value = self
            .send_signed_post("bitvavo.place_order", "/order", &HashMap::new(), &body)
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
        let mut params = HashMap::new();
        params.insert(
            "market".to_string(),
            normalize_bitvavo_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        if let Some(order_id) = &request.exchange_order_id {
            params.insert("orderId".to_string(), order_id.clone());
        }
        if let Some(client_order_id) = &request.client_order_id {
            params.insert("clientOrderId".to_string(), client_order_id.clone());
        }
        if !params.contains_key("orderId") && !params.contains_key("clientOrderId") {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bitvavo cancel_order requires orderId or clientOrderId".to_string(),
            });
        }
        let value = self
            .send_signed_delete("bitvavo.cancel_order", "/order", &params, &Value::Null)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .unwrap_or_else(|_| bitvavo_cancelled_order(&self.exchange_id, &request));
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

    pub(super) async fn cancel_all_orders_impl(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        let mut params = HashMap::new();
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            params.insert(
                "market".to_string(),
                normalize_bitvavo_symbol(&symbol.exchange_symbol.symbol)?,
            );
        }
        let value = self
            .send_signed_delete(
                "bitvavo.cancel_all_orders",
                "/orders",
                &params,
                &Value::Null,
            )
            .await?;
        let orders = parse_open_orders(&self.exchange_id, request.symbol.as_ref(), &value)
            .unwrap_or_default();
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
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
            .send_signed_get("bitvavo.get_balances", "/balance", &HashMap::new())
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

    pub(super) async fn get_fees_impl(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
        }
        let value = self
            .send_signed_get("bitvavo.get_fees", "/account/fees", &HashMap::new())
            .await?;
        let exchange = request
            .symbols
            .first()
            .map(|symbol| symbol.exchange.clone())
            .unwrap_or_else(|| self.exchange_id.clone());
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange, request.context.request_id),
            fees: parse_fee_snapshots(&request.symbols, &value)?,
        })
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "market".to_string(),
            normalize_bitvavo_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        if let Some(order_id) = &request.exchange_order_id {
            params.insert("orderId".to_string(), order_id.clone());
        }
        if let Some(client_order_id) = &request.client_order_id {
            params.insert("clientOrderId".to_string(), client_order_id.clone());
        }
        let value = self
            .send_signed_get("bitvavo.query_order", "/order", &params)
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
        let mut params = HashMap::new();
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            params.insert(
                "market".to_string(),
                normalize_bitvavo_symbol(&symbol.exchange_symbol.symbol)?,
            );
        }
        if let Some(PageCursor::Id { id }) =
            request.page.as_ref().and_then(|page| page.cursor.clone())
        {
            params.insert("start".to_string(), id);
        }
        let value = self
            .send_signed_get("bitvavo.get_open_orders", "/ordersOpen", &params)
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
                message: "bitvavo get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut params = HashMap::new();
        params.insert(
            "market".to_string(),
            normalize_bitvavo_symbol(&symbol.exchange_symbol.symbol)?,
        );
        if let Some(limit) = request
            .limit
            .or_else(|| request.page.as_ref().and_then(|p| p.limit))
        {
            params.insert("limit".to_string(), limit.to_string());
        }
        let value = self
            .send_signed_get("bitvavo.get_recent_fills", "/trades", &params)
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?,
        })
    }
}

pub fn bitvavo_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    if request.reduce_only {
        return Err(ExchangeApiError::Unsupported {
            operation: "bitvavo.reduce_only_unsupported_spot",
        });
    }
    let mut body = json!({
        "market": normalize_bitvavo_symbol(&request.symbol.exchange_symbol.symbol)?,
        "side": match request.side {
            rustcta_types::OrderSide::Buy => "buy",
            rustcta_types::OrderSide::Sell => "sell",
        },
        "orderType": match request.order_type {
            OrderType::Market => "market",
            _ => "limit",
        },
    });
    let object = body.as_object_mut().expect("object");
    if let Some(client_order_id) = &request.client_order_id {
        object.insert("clientOrderId".to_string(), json!(client_order_id));
    }
    if request.order_type != OrderType::Market {
        let price = request
            .price
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitvavo limit order requires price".to_string(),
            })?;
        object.insert("price".to_string(), json!(price));
    }
    if let Some(quote_quantity) = &request.quote_quantity {
        object.insert("amountQuote".to_string(), json!(quote_quantity));
    } else {
        object.insert("amount".to_string(), json!(request.quantity));
    }
    if request.post_only || request.order_type == OrderType::PostOnly {
        object.insert("postOnly".to_string(), json!(true));
    }
    if let Some(time_in_force) = request.time_in_force {
        let value = match time_in_force {
            TimeInForce::IOC => "IOC",
            TimeInForce::FOK => "FOK",
            TimeInForce::GTX => {
                object.insert("postOnly".to_string(), json!(true));
                "GTC"
            }
            _ => "GTC",
        };
        object.insert("timeInForce".to_string(), json!(value));
    }
    Ok(body)
}

fn bitvavo_cancelled_order(
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
        side: rustcta_types::OrderSide::Buy,
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
