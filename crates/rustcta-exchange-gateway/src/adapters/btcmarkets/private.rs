use std::collections::HashMap;

use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse,
    OpenOrdersRequest, OpenOrdersResponse, OrderListRequest, OrderListResponse, PageCursor,
    PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse, QueryOrderRequest,
    QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse,
    TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{OrderSide, OrderStatus, OrderType};
use serde_json::{json, Value};

use super::parser::normalize_market_id;
use super::private_parser::{
    parse_balances, parse_cancel_ack, parse_fee_snapshots, parse_fills, parse_order_state,
    parse_orders,
};
use super::public::ensure_aud_spot_market;
use super::BtcMarketsGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl BtcMarketsGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        self.ensure_private_rest("btcmarkets.get_balances")?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let value = self
            .rest
            .signed_get("/v3/accounts/me/balances", &HashMap::new())
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

    pub(super) async fn get_positions_impl(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.unsupported_private("btcmarkets.spot_positions")
    }

    pub(super) async fn get_fees_impl(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            ensure_aud_spot_market(symbol)?;
        }
        self.ensure_private_rest("btcmarkets.get_fees")?;
        let value = self
            .rest
            .signed_get("/v3/accounts/me/trading-fees", &HashMap::new())
            .await?;
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
        ensure_aud_spot_market(&request.symbol)?;
        self.ensure_private_rest("btcmarkets.place_order")?;
        let body = btcmarkets_order_body(&request)?;
        let value = self
            .rest
            .signed_post("/v3/orders", &HashMap::new(), &body)
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

    pub(super) async fn place_quote_market_order_impl(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        ensure_aud_spot_market(&request.symbol)?;
        self.ensure_private_rest("btcmarkets.place_quote_market_order")?;
        let body = json!({
            "marketId": normalize_market_id(&request.symbol.exchange_symbol.symbol)?,
            "side": side_to_btcmarkets(request.side),
            "type": "Market",
            "targetAmount": request.quote_quantity,
            "clientOrderId": request.client_order_id,
        });
        let value = self
            .rest
            .signed_post("/v3/orders", &HashMap::new(), &body)
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
        ensure_aud_spot_market(&request.symbol)?;
        self.ensure_private_rest("btcmarkets.cancel_order")?;
        let id = request
            .exchange_order_id
            .clone()
            .or(request.client_order_id.clone())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "btcmarkets.cancel_order requires exchange_order_id or client_order_id"
                    .to_string(),
            })?;
        let value = self
            .rest
            .signed_delete(&format!("/v3/orders/{id}"), &HashMap::new())
            .await?;
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order: parse_cancel_ack(&self.exchange_id, &request.symbol, &value)?,
            cancelled: true,
        })
    }

    pub(super) async fn amend_order_impl(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        ensure_aud_spot_market(&request.symbol)?;
        self.unsupported_private("btcmarkets.replace_order_unmapped_price")
    }

    pub(super) async fn place_order_list_impl(
        &self,
        request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        ensure_exchange_api_schema(request.schema_version())?;
        self.ensure_exchange(&request.symbol().exchange)?;
        ensure_aud_spot_market(request.symbol())?;
        self.unsupported_private("btcmarkets.order_lists")
    }

    pub(super) async fn batch_place_orders_impl(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_private_rest("btcmarkets.batch_place_orders")?;
        if request.orders.is_empty() || request.orders.len() > 10 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "btcmarkets batch_place_orders requires 1..=10 orders".to_string(),
            });
        }
        let mut symbols = Vec::with_capacity(request.orders.len());
        let mut rows = Vec::with_capacity(request.orders.len());
        for order in &request.orders {
            self.ensure_exchange(&order.symbol.exchange)?;
            ensure_aud_spot_market(&order.symbol)?;
            if order.client_order_id.is_none() {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "btcmarkets native batch place requires client_order_id".to_string(),
                });
            }
            symbols.push(order.symbol.clone());
            rows.push(json!({ "placeOrder": btcmarkets_order_body(order)? }));
        }
        let value = self
            .rest
            .signed_post("/v3/batchorders", &HashMap::new(), &Value::Array(rows))
            .await?;
        let orders = value
            .get("placeOrders")
            .or_else(|| value.get("data").and_then(|data| data.get("placeOrders")))
            .map(|place_orders| parse_orders(&self.exchange_id, symbols.first(), place_orders))
            .transpose()?
            .unwrap_or_default();
        Ok(BatchPlaceOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
            report: None,
        })
    }

    pub(super) async fn batch_cancel_orders_impl(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_private_rest("btcmarkets.batch_cancel_orders")?;
        if request.cancels.is_empty() || request.cancels.len() > 10 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "btcmarkets batch_cancel_orders requires 1..=10 cancels".to_string(),
            });
        }
        for cancel in &request.cancels {
            self.ensure_exchange(&cancel.symbol.exchange)?;
            ensure_aud_spot_market(&cancel.symbol)?;
        }
        let rows = request
            .cancels
            .iter()
            .map(|cancel| {
                if let Some(order_id) = &cancel.exchange_order_id {
                    Ok(json!({ "cancelOrder": { "orderId": order_id } }))
                } else if let Some(client_order_id) = &cancel.client_order_id {
                    Ok(json!({ "cancelOrder": { "clientOrderId": client_order_id } }))
                } else {
                    Err(ExchangeApiError::InvalidRequest {
                        message: "btcmarkets batch cancel requires order identity".to_string(),
                    })
                }
            })
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let value = self
            .rest
            .signed_post("/v3/batchorders", &HashMap::new(), &Value::Array(rows))
            .await?;
        let first_symbol = request.cancels.first().map(|cancel| &cancel.symbol);
        let mut orders = value
            .get("cancelOrders")
            .or_else(|| value.get("data").and_then(|data| data.get("cancelOrders")))
            .map(|cancel_orders| parse_orders(&self.exchange_id, first_symbol, cancel_orders))
            .transpose()?
            .unwrap_or_default();
        for order in &mut orders {
            order.status = OrderStatus::Cancelled;
        }
        Ok(BatchCancelOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
            report: None,
        })
    }

    pub(super) async fn cancel_all_orders_impl(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_private_rest("btcmarkets.cancel_all_orders")?;
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        let mut params = HashMap::new();
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            ensure_aud_spot_market(symbol)?;
            params.insert(
                "marketId".to_string(),
                normalize_market_id(&symbol.exchange_symbol.symbol)?,
            );
        }
        let value = self.rest.signed_delete("/v3/orders", &params).await?;
        let mut orders =
            parse_orders(&self.exchange_id, request.symbol.as_ref(), &value).unwrap_or_default();
        for order in &mut orders {
            order.status = OrderStatus::Cancelled;
        }
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
        })
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        ensure_aud_spot_market(&request.symbol)?;
        self.ensure_private_rest("btcmarkets.query_order")?;
        let id = request
            .exchange_order_id
            .or(request.client_order_id)
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "btcmarkets.query_order requires exchange_order_id or client_order_id"
                    .to_string(),
            })?;
        let value = self
            .rest
            .signed_get(&format!("/v3/orders/{id}"), &HashMap::new())
            .await?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
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
        self.ensure_private_rest("btcmarkets.get_open_orders")?;
        let mut params = HashMap::new();
        params.insert("status".to_string(), "open".to_string());
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            ensure_aud_spot_market(symbol)?;
            params.insert(
                "marketId".to_string(),
                normalize_market_id(&symbol.exchange_symbol.symbol)?,
            );
        }
        let value = self.rest.signed_get("/v3/orders", &params).await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_orders(&self.exchange_id, request.symbol.as_ref(), &value)?,
        })
    }

    pub(super) async fn get_recent_fills_impl(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_private_rest("btcmarkets.get_recent_fills")?;
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut params = HashMap::new();
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            ensure_aud_spot_market(symbol)?;
            params.insert(
                "marketId".to_string(),
                normalize_market_id(&symbol.exchange_symbol.symbol)?,
            );
        }
        if let Some(order_id) = request.exchange_order_id.or(request.client_order_id) {
            params.insert("orderId".to_string(), order_id);
        }
        if let Some(limit) = request.limit {
            params.insert("limit".to_string(), limit.min(200).to_string());
        }
        if let Some(page) = request.page {
            if let Some(limit) = page.limit {
                params.insert("limit".to_string(), limit.min(200).to_string());
            }
            if let Some(cursor) = page.cursor {
                match cursor {
                    PageCursor::Id { id } | PageCursor::Token { token: id } => {
                        params.insert("before".to_string(), id);
                    }
                    PageCursor::Offset { .. }
                    | PageCursor::Timestamp { .. }
                    | PageCursor::TimeRange { .. } => {}
                }
            }
        }
        let value = self.rest.signed_get("/v3/trades", &params).await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_fills(
                &self.exchange_id,
                tenant_id,
                account_id,
                request.symbol.as_ref(),
                &value,
            )?,
        })
    }
}

pub fn btcmarkets_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    if request.reduce_only {
        return Err(ExchangeApiError::Unsupported {
            operation: "btcmarkets.reduce_only",
        });
    }
    let mut body = serde_json::Map::new();
    body.insert(
        "marketId".to_string(),
        json!(normalize_market_id(&request.symbol.exchange_symbol.symbol)?),
    );
    body.insert("side".to_string(), json!(side_to_btcmarkets(request.side)));
    body.insert(
        "type".to_string(),
        json!(order_type_to_btcmarkets(request.order_type)),
    );
    body.insert("amount".to_string(), json!(request.quantity.clone()));
    if let Some(price) = &request.price {
        body.insert("price".to_string(), json!(price));
    } else if request.order_type.requires_limit_price() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "btcmarkets limit order requires price".to_string(),
        });
    }
    if let Some(client_order_id) = &request.client_order_id {
        body.insert("clientOrderId".to_string(), json!(client_order_id));
    }
    if let Some(time_in_force) = request.time_in_force {
        body.insert(
            "timeInForce".to_string(),
            json!(time_in_force_to_btcmarkets(time_in_force)),
        );
    }
    if request.post_only || request.order_type == OrderType::PostOnly {
        body.insert("postOnly".to_string(), json!(true));
    }
    if let Some(quote_quantity) = &request.quote_quantity {
        body.insert("targetAmount".to_string(), json!(quote_quantity));
    }
    Ok(Value::Object(body))
}

fn side_to_btcmarkets(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "Bid",
        OrderSide::Sell => "Ask",
    }
}

fn order_type_to_btcmarkets(order_type: OrderType) -> &'static str {
    match order_type {
        OrderType::Market => "Market",
        OrderType::Limit | OrderType::PostOnly | OrderType::IOC | OrderType::FOK => "Limit",
        OrderType::StopMarket => "Stop",
        OrderType::StopLimit => "Stop Limit",
    }
}

fn time_in_force_to_btcmarkets(time_in_force: TimeInForce) -> &'static str {
    match time_in_force {
        TimeInForce::GTC | TimeInForce::GTX => "GTC",
        TimeInForce::IOC => "IOC",
        TimeInForce::FOK => "FOK",
    }
}

#[cfg(test)]
mod tests {
    use rustcta_exchange_api::{PlaceOrderRequest, RequestContext, SymbolScope};
    use rustcta_types::{
        CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType, TimeInForce,
    };
    use serde_json::Value;

    use super::btcmarkets_order_body;

    #[test]
    fn place_order_body_should_match_fixture_shape() {
        let request = PlaceOrderRequest {
            schema_version: rustcta_exchange_api::EXCHANGE_API_SCHEMA_VERSION,
            context: RequestContext::new(chrono::Utc::now()),
            symbol: SymbolScope {
                exchange: ExchangeId::new("btcmarkets").unwrap(),
                market_type: MarketType::Spot,
                canonical_symbol: Some(CanonicalSymbol::new("BTC", "AUD").unwrap()),
                exchange_symbol: ExchangeSymbol::new(
                    ExchangeId::new("btcmarkets").unwrap(),
                    MarketType::Spot,
                    "BTC-AUD",
                )
                .unwrap(),
            },
            client_order_id: Some("cli-123".to_string()),
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: "1.034".to_string(),
            price: Some("100.12".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: true,
        };
        let body = btcmarkets_order_body(&request).unwrap();
        let expected: Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/btcmarkets/request_specs/place_order.json"
        ))
        .unwrap();
        assert_eq!(body, expected["body"]);
    }
}
