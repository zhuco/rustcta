use serde_json::{json, Value};

use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse,
    OpenOrdersRequest, OpenOrdersResponse, OrderState, PlaceOrderRequest, PlaceOrderResponse,
    QueryOrderRequest, QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest,
    RecentFillsResponse, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};

use super::parser::normalize_coinspot_symbol;
use super::private_parser::{
    parse_account_balances, parse_fee_snapshots, parse_open_orders, parse_recent_fills,
};
use super::CoinspotGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl CoinspotGatewayAdapter {
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
            .send_signed_post(
                "coinspot.get_balances",
                "/api/v2/ro/my/balances",
                &json!({}),
            )
            .await?;
        let balances = parse_account_balances(
            &self.exchange_id,
            tenant_id,
            account_id,
            &request.assets,
            &value,
        )?;
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            balances,
        })
    }

    pub(super) async fn get_fees_impl(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "coinspot get_fees requires at least one symbol".to_string(),
            });
        }
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            normalize_coinspot_symbol(&symbol.exchange_symbol.symbol)?;
        }
        let value = self
            .send_signed_post("coinspot.get_fees", "/api/v2/ro/my/fees", &json!({}))
            .await?;
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fees: parse_fee_snapshots(&request.symbols, &value),
        })
    }

    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let (endpoint, body) = coinspot_order_endpoint_and_body(&request)?;
        let value = self
            .send_signed_post("coinspot.place_order", endpoint, &body)
            .await?;
        let order = order_state_from_place_ack(&self.exchange_id, &request, &value);
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

    pub(super) async fn place_quote_market_order_impl(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        if request.side != OrderSide::Buy {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinspot.quote_market_sell",
            });
        }
        let (coin, market) = normalize_coinspot_symbol(&request.symbol.exchange_symbol.symbol)?;
        let value = self
            .send_signed_post(
                "coinspot.place_quote_market_order",
                "/api/v2/my/buy/now",
                &json!({
                    "cointype": coin,
                    "markettype": market,
                    "amount": non_empty("quote_quantity", &request.quote_quantity)?,
                }),
            )
            .await?;
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order: OrderState {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                exchange: self.exchange_id.clone(),
                market_type: MarketType::Spot,
                canonical_symbol: request.symbol.canonical_symbol.clone(),
                exchange_symbol: request.symbol.exchange_symbol,
                client_order_id: request.client_order_id,
                exchange_order_id: value_text(value.get("id").or_else(|| value.get("orderid"))),
                side: request.side,
                position_side: Some(PositionSide::None),
                order_type: OrderType::Market,
                time_in_force: None,
                status: OrderStatus::Unknown,
                quantity: "0".to_string(),
                price: None,
                filled_quantity: "0".to_string(),
                average_fill_price: None,
                reduce_only: false,
                post_only: false,
                created_at: Some(chrono::Utc::now()),
                updated_at: chrono::Utc::now(),
            },
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
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coinspot get_open_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (coin, market) = normalize_coinspot_symbol(&symbol.exchange_symbol.symbol)?;
        let value = self
            .send_signed_post(
                "coinspot.get_open_orders",
                "/api/v2/ro/my/orders/open",
                &json!({
                    "cointype": coin,
                    "markettype": market,
                }),
            )
            .await?;
        let orders = parse_open_orders(&self.exchange_id, Some(symbol), &value)?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
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
        if request.start_time.is_some() || request.end_time.is_some() || request.limit.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinspot.fills_pagination_filters",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coinspot get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let (coin, market) = normalize_coinspot_symbol(&symbol.exchange_symbol.symbol)?;
        let value = self
            .send_signed_post(
                "coinspot.get_recent_fills",
                "/api/v2/ro/my/transactions",
                &json!({
                    "cointype": coin,
                    "markettype": market,
                }),
            )
            .await?;
        let fills = parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
        })
    }

    pub(super) fn unsupported_cancel_order(
        &self,
        _request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "coinspot.cancel_order_side_specific",
        })
    }

    pub(super) fn unsupported_cancel_all_orders(
        &self,
        _request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "coinspot.cancel_all_orders",
        })
    }

    pub(super) fn unsupported_amend_order(
        &self,
        _request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "coinspot.edit_order_side_specific",
        })
    }

    pub(super) fn unsupported_query_order(
        &self,
        _request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "coinspot.query_order_side_specific",
        })
    }

    pub(super) fn unsupported_batch_place_orders(
        &self,
        _request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "coinspot.batch_place_orders",
        })
    }

    pub(super) fn unsupported_batch_cancel_orders(
        &self,
        _request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "coinspot.batch_cancel_orders",
        })
    }
}

fn coinspot_order_endpoint_and_body(
    request: &PlaceOrderRequest,
) -> ExchangeApiResult<(&'static str, Value)> {
    if request.reduce_only {
        return Err(ExchangeApiError::InvalidRequest {
            message: "coinspot spot order does not support reduce_only".to_string(),
        });
    }
    if request.post_only || matches!(request.time_in_force, Some(TimeInForce::GTX)) {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinspot.post_only_order",
        });
    }
    if matches!(
        request.order_type,
        OrderType::StopMarket | OrderType::StopLimit | OrderType::IOC | OrderType::FOK
    ) || matches!(
        request.time_in_force,
        Some(TimeInForce::IOC | TimeInForce::FOK)
    ) {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinspot.advanced_order_type",
        });
    }
    let (coin, market) = normalize_coinspot_symbol(&request.symbol.exchange_symbol.symbol)?;
    let endpoint = match (request.side, request.order_type) {
        (OrderSide::Buy, OrderType::Market) => "/api/v2/my/buy/now",
        (OrderSide::Sell, OrderType::Market) => "/api/v2/my/sell/now",
        (OrderSide::Buy, OrderType::Limit) => "/api/v2/my/buy",
        (OrderSide::Sell, OrderType::Limit) => "/api/v2/my/sell",
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinspot.order_type",
            })
        }
    };
    let mut body = json!({
        "cointype": coin,
        "markettype": market,
        "amount": non_empty("quantity", &request.quantity)?,
    });
    if request.order_type == OrderType::Limit {
        body["rate"] = Value::String(non_empty(
            "price",
            request
                .price
                .as_deref()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "coinspot limit order requires price".to_string(),
                })?,
        )?);
    }
    Ok((endpoint, body))
}

fn order_state_from_place_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &PlaceOrderRequest,
    value: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request.client_order_id.clone(),
        exchange_order_id: value_text(value.get("id").or_else(|| value.get("orderid"))),
        side: request.side,
        position_side: request.position_side.or(Some(PositionSide::None)),
        order_type: request.order_type,
        time_in_force: request.time_in_force,
        status: OrderStatus::Unknown,
        quantity: request.quantity.clone(),
        price: request.price.clone(),
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: Some(chrono::Utc::now()),
        updated_at: chrono::Utc::now(),
    }
}

fn non_empty(field: &str, value: &str) -> ExchangeApiResult<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("coinspot {field} must not be empty"),
        });
    }
    Ok(trimmed.to_string())
}

fn value_text(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}
