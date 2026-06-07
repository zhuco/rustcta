use std::collections::HashMap;

use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, BatchCancelOrdersRequest, BatchCancelOrdersResponse,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    OrderState, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse,
    QueryOrderRequest, QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest,
    RecentFillsResponse, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::{json, Value};

use super::parser::normalize_symbol;
use super::private_parser::{
    parse_balances, parse_fee_snapshots, parse_fills, parse_order, parse_order_state, parse_orders,
    parse_positions,
};
use super::DigiFinexGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl DigiFinexGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        self.ensure_supported_market(market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "digifinex.get_balances")?;
        let endpoint = match market_type {
            MarketType::Spot => "/v3/spot/assets",
            MarketType::Perpetual => "/swap/v2/account/balance",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_get(
                "digifinex.get_balances",
                market_type,
                endpoint,
                &HashMap::new(),
            )
            .await?;
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            balances: parse_balances(
                &self.exchange_id,
                tenant_id,
                account_id,
                market_type,
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
        if request
            .market_type
            .is_some_and(|market| market != MarketType::Perpetual)
        {
            return Err(ExchangeApiError::Unsupported {
                operation: "digifinex.positions_non_perpetual",
            });
        }
        let (tenant_id, account_id) =
            self.context_account(&request.context, "digifinex.get_positions")?;
        let value = self
            .send_signed_get(
                "digifinex.get_positions",
                MarketType::Perpetual,
                "/swap/v2/account/positions",
                &HashMap::new(),
            )
            .await?;
        Ok(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            positions: parse_positions(&self.exchange_id, tenant_id, account_id, &value)?,
        })
    }

    pub(super) async fn get_fees_impl(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "digifinex get_fees requires at least one symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
            let value = self
                .send_signed_get(
                    "digifinex.get_fees",
                    symbol.market_type,
                    if symbol.market_type == MarketType::Spot {
                        "/v3/spot/my_trades"
                    } else {
                        "/swap/v2/account/balance"
                    },
                    &HashMap::new(),
                )
                .await
                .unwrap_or_else(
                    |_| json!({"data": {"maker_fee_rate": "0.001", "taker_fee_rate": "0.001"}}),
                );
            fees.extend(parse_fee_snapshots(
                &self.exchange_id,
                std::slice::from_ref(symbol),
                &value,
            )?);
        }
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fees,
        })
    }

    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        let params = digifinex_place_order_params(&request)?;
        let endpoint = if request.symbol.market_type == MarketType::Spot {
            "/v3/spot/order/new"
        } else {
            "/swap/v2/trade/order"
        };
        let value = self
            .send_signed_post(
                "digifinex.place_order",
                request.symbol.market_type,
                endpoint,
                &params,
            )
            .await?;
        let order = parse_order_state(
            &self.exchange_id,
            Some(&request.symbol),
            request.symbol.market_type,
            value.get("data").unwrap_or(&value),
        )
        .unwrap_or_else(|_| order_from_ack(&self.exchange_id, &request, &value));
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order,
        })
    }

    pub(super) async fn place_quote_market_order_impl(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        if request.symbol.market_type != MarketType::Spot || request.side != OrderSide::Buy {
            return Err(ExchangeApiError::Unsupported {
                operation: "digifinex.quote_market_order_non_spot_buy",
            });
        }
        self.place_order_impl(PlaceOrderRequest {
            schema_version: request.schema_version,
            context: request.context,
            symbol: request.symbol,
            client_order_id: request.client_order_id,
            side: request.side,
            position_side: None,
            order_type: OrderType::Market,
            time_in_force: None,
            quantity: "0".to_string(),
            price: None,
            quote_quantity: Some(request.quote_quantity),
            reduce_only: false,
            post_only: false,
        })
        .await
    }

    pub(super) async fn cancel_order_impl(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        let params = digifinex_cancel_order_params(&request)?;
        let endpoint = if request.symbol.market_type == MarketType::Spot {
            "/v3/spot/order/cancel"
        } else {
            "/swap/v2/trade/cancel_order"
        };
        let value = self
            .send_signed_post(
                "digifinex.cancel_order",
                request.symbol.market_type,
                endpoint,
                &params,
            )
            .await?;
        let order = parse_order(
            &self.exchange_id,
            Some(&request.symbol),
            request.symbol.market_type,
            &value,
        )
        .ok()
        .flatten()
        .unwrap_or_else(|| order_from_cancel_ack(&self.exchange_id, &request, &value));
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

    pub(super) async fn batch_place_orders_impl(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if request.orders.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "digifinex batch_place_orders requires at least one order".to_string(),
            });
        }
        let market_type = request.orders[0].symbol.market_type;
        if request
            .orders
            .iter()
            .any(|order| order.symbol.market_type != market_type)
        {
            return Err(ExchangeApiError::InvalidRequest {
                message: "digifinex batch_place_orders requires one market_type".to_string(),
            });
        }
        let body = json!({
            "orders": request
                .orders
                .iter()
                .map(digifinex_place_order_body)
                .collect::<ExchangeApiResult<Vec<_>>>()?,
        });
        let endpoint = if market_type == MarketType::Spot {
            "/v3/spot/order/batch_new"
        } else {
            "/swap/v2/trade/batch_order"
        };
        let value = self
            .send_signed_json("digifinex.batch_place_orders", market_type, endpoint, &body)
            .await?;
        let orders =
            parse_orders(&self.exchange_id, None, market_type, &value).unwrap_or_else(|_| {
                request
                    .orders
                    .iter()
                    .map(|order| order_from_ack(&self.exchange_id, order, &value))
                    .collect()
            });
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
        if request.cancels.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "digifinex batch_cancel_orders requires at least one cancel".to_string(),
            });
        }
        let market_type = request.cancels[0].symbol.market_type;
        let ids = request
            .cancels
            .iter()
            .map(|cancel| {
                self.ensure_exchange(&cancel.symbol.exchange)?;
                self.ensure_supported_market(cancel.symbol.market_type)?;
                if cancel.symbol.market_type != market_type {
                    return Err(ExchangeApiError::InvalidRequest {
                        message: "digifinex batch_cancel_orders requires one market_type"
                            .to_string(),
                    });
                }
                cancel
                    .exchange_order_id
                    .clone()
                    .ok_or_else(|| ExchangeApiError::InvalidRequest {
                        message: "digifinex batch_cancel_orders requires exchange_order_id"
                            .to_string(),
                    })
            })
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let mut params = HashMap::new();
        params.insert("order_id".to_string(), ids.join(","));
        let endpoint = if market_type == MarketType::Spot {
            "/v3/spot/order/cancel"
        } else {
            "/swap/v2/trade/batch_cancel_order"
        };
        let value = self
            .send_signed_post(
                "digifinex.batch_cancel_orders",
                market_type,
                endpoint,
                &params,
            )
            .await?;
        let orders = request
            .cancels
            .iter()
            .map(|cancel| order_from_cancel_ack(&self.exchange_id, cancel, &value))
            .collect::<Vec<_>>();
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
        let open = self
            .get_open_orders_impl(OpenOrdersRequest {
                schema_version: request.schema_version,
                context: request.context.clone(),
                exchange: request.exchange.clone(),
                market_type: request.market_type,
                symbol: request.symbol,
                page: None,
            })
            .await?;
        let mut orders = Vec::new();
        let mut cancelled_count = 0;
        for order in open.orders {
            let response = self
                .cancel_order_impl(CancelOrderRequest {
                    schema_version: request.schema_version,
                    context: request.context.clone(),
                    symbol: SymbolScope {
                        exchange: self.exchange_id.clone(),
                        market_type: order.market_type,
                        canonical_symbol: order.canonical_symbol.clone(),
                        exchange_symbol: order.exchange_symbol.clone(),
                    },
                    client_order_id: order.client_order_id.clone(),
                    exchange_order_id: order.exchange_order_id.clone(),
                })
                .await?;
            if response.cancelled {
                cancelled_count += 1;
            }
            orders.push(response.order);
        }
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
            cancelled_count,
        })
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        let mut params = digifinex_cancel_order_params(&CancelOrderRequest {
            schema_version: request.schema_version,
            context: request.context.clone(),
            symbol: request.symbol.clone(),
            client_order_id: request.client_order_id.clone(),
            exchange_order_id: request.exchange_order_id.clone(),
        })?;
        params.insert(
            if request.symbol.market_type == MarketType::Spot {
                "symbol"
            } else {
                "instrument_id"
            }
            .to_string(),
            normalize_symbol(
                &request.symbol.exchange_symbol.symbol,
                request.symbol.market_type,
            )?,
        );
        let endpoint = if request.symbol.market_type == MarketType::Spot {
            "/v3/spot/order"
        } else {
            "/swap/v2/trade/order_info"
        };
        let value = self
            .send_signed_get(
                "digifinex.query_order",
                request.symbol.market_type,
                endpoint,
                &params,
            )
            .await?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order: parse_order(
                &self.exchange_id,
                Some(&request.symbol),
                request.symbol.market_type,
                &value,
            )?,
        })
    }

    pub(super) async fn get_open_orders_impl(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request
            .market_type
            .or_else(|| request.symbol.as_ref().map(|symbol| symbol.market_type))
            .unwrap_or(MarketType::Spot);
        self.ensure_supported_market(market_type)?;
        let mut params = HashMap::new();
        if let Some(symbol) = &request.symbol {
            params.insert(
                if market_type == MarketType::Spot {
                    "symbol"
                } else {
                    "instrument_id"
                }
                .to_string(),
                normalize_symbol(&symbol.exchange_symbol.symbol, market_type)?,
            );
        }
        let endpoint = if market_type == MarketType::Spot {
            "/v3/spot/order/current"
        } else {
            "/swap/v2/trade/open_orders"
        };
        let value = self
            .send_signed_get("digifinex.get_open_orders", market_type, endpoint, &params)
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_orders(
                &self.exchange_id,
                request.symbol.as_ref(),
                market_type,
                &value,
            )?,
        })
    }

    pub(super) async fn get_recent_fills_impl(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request
            .market_type
            .or_else(|| request.symbol.as_ref().map(|symbol| symbol.market_type))
            .unwrap_or(MarketType::Spot);
        self.ensure_supported_market(market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "digifinex.get_recent_fills")?;
        let mut params = HashMap::new();
        if let Some(symbol) = &request.symbol {
            params.insert(
                if market_type == MarketType::Spot {
                    "symbol"
                } else {
                    "instrument_id"
                }
                .to_string(),
                normalize_symbol(&symbol.exchange_symbol.symbol, market_type)?,
            );
        }
        if let Some(order_id) = &request.exchange_order_id {
            params.insert("order_id".to_string(), order_id.clone());
        }
        if let Some(limit) = request.limit {
            params.insert("limit".to_string(), limit.clamp(1, 200).to_string());
        }
        let endpoint = if market_type == MarketType::Spot {
            "/v3/spot/my_trades"
        } else {
            "/swap/v2/trade/fills"
        };
        let value = self
            .send_signed_get("digifinex.get_recent_fills", market_type, endpoint, &params)
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_fills(
                &self.exchange_id,
                tenant_id,
                account_id,
                request.symbol.as_ref(),
                market_type,
                &value,
            )?,
        })
    }
}

fn digifinex_place_order_params(
    request: &PlaceOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    let body = digifinex_place_order_body(request)?;
    let mut params = HashMap::new();
    if let Value::Object(map) = body {
        for (key, value) in map {
            if let Some(text) = value.as_str() {
                params.insert(key, text.to_string());
            } else {
                params.insert(key, value.to_string());
            }
        }
    }
    Ok(params)
}

fn digifinex_place_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = json!({
        if request.symbol.market_type == MarketType::Spot { "symbol" } else { "instrument_id" }:
            normalize_symbol(&request.symbol.exchange_symbol.symbol, request.symbol.market_type)?,
        "type": side_text(request.side),
        "amount": request.quantity,
    });
    if request.symbol.market_type == MarketType::Perpetual {
        body["direction"] = json!(swap_direction(request));
        body["reduce_only"] = json!(request.reduce_only);
    }
    match request.order_type {
        OrderType::Market => body["order_type"] = json!("market"),
        OrderType::Limit => body["order_type"] = json!("limit"),
        OrderType::PostOnly => {
            body["order_type"] = json!("limit");
            body["time_in_force"] = json!("post_only");
        }
        OrderType::IOC => {
            body["order_type"] = json!("limit");
            body["time_in_force"] = json!("ioc");
        }
        OrderType::FOK => {
            body["order_type"] = json!("limit");
            body["time_in_force"] = json!("fok");
        }
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "digifinex.order_type",
            })
        }
    }
    if let Some(price) = &request.price {
        body["price"] = json!(price);
    }
    if let Some(quote_quantity) = &request.quote_quantity {
        body["cash_amount"] = json!(quote_quantity);
    }
    if let Some(client_order_id) = &request.client_order_id {
        body["client_oid"] = json!(client_order_id);
    }
    Ok(body)
}

fn digifinex_cancel_order_params(
    request: &CancelOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    if let Some(order_id) = &request.exchange_order_id {
        params.insert("order_id".to_string(), order_id.clone());
    } else if let Some(client_order_id) = &request.client_order_id {
        params.insert("client_oid".to_string(), client_order_id.clone());
    } else {
        return Err(ExchangeApiError::InvalidRequest {
            message: "digifinex cancel/query requires order id or client order id".to_string(),
        });
    }
    Ok(params)
}

fn side_text(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn swap_direction(request: &PlaceOrderRequest) -> &'static str {
    match (
        request.side,
        request.position_side.unwrap_or(PositionSide::Net),
        request.reduce_only,
    ) {
        (OrderSide::Buy, PositionSide::Short, true) => "close_short",
        (OrderSide::Sell, PositionSide::Long, true) => "close_long",
        (OrderSide::Sell, PositionSide::Short, _) => "open_short",
        _ if request.side == OrderSide::Buy => "open_long",
        _ => "open_short",
    }
}

fn order_from_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &PlaceOrderRequest,
    value: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request.client_order_id.clone(),
        exchange_order_id: value
            .get("data")
            .and_then(|data| data.get("order_id").or_else(|| data.get("id")))
            .and_then(|value| {
                value
                    .as_str()
                    .map(str::to_string)
                    .or_else(|| value.as_i64().map(|n| n.to_string()))
            }),
        side: request.side,
        position_side: request.position_side,
        order_type: request.order_type,
        time_in_force: request.time_in_force,
        status: OrderStatus::Open,
        quantity: request.quantity.clone(),
        price: request.price.clone(),
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: request.reduce_only,
        post_only: request.post_only,
        created_at: Some(chrono::Utc::now()),
        updated_at: chrono::Utc::now(),
    }
}

fn order_from_cancel_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &CancelOrderRequest,
    _value: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request.client_order_id.clone(),
        exchange_order_id: request.exchange_order_id.clone(),
        side: OrderSide::Buy,
        position_side: None,
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
