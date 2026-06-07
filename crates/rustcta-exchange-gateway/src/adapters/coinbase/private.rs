use std::collections::HashMap;

use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse,
    OpenOrdersRequest, OpenOrdersResponse, OrderListConditionalLeg, OrderListLegType,
    OrderListRequest, OrderListResponse, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest,
    PositionsResponse, QueryOrderRequest, QueryOrderResponse, QuoteMarketOrderRequest,
    RecentFillsRequest, RecentFillsResponse, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderType};
use serde_json::{json, Value};

use super::parser::normalize_coinbase_symbol;
use super::private_parser::{
    parse_account_balances, parse_amend_order_ack, parse_batch_cancel_orders, parse_cancel_order,
    parse_fee_snapshots, parse_intx_balances, parse_order_ack, parse_order_state, parse_orders,
    parse_positions, parse_recent_fills,
};
use super::CoinbaseGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl CoinbaseGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_supported_market(market_type)?;
        }
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let balances = if market_type == MarketType::Perpetual {
            let portfolio_uuid = self.config.perpetual_portfolio_uuid.as_deref().ok_or(
                ExchangeApiError::Unsupported {
                    operation: "coinbase.get_balances.missing_intx_portfolio_uuid",
                },
            )?;
            let endpoint = format!("/intx/balances/{portfolio_uuid}");
            let value = self
                .send_signed_get("coinbase.get_balances.intx", &endpoint, &HashMap::new())
                .await?;
            parse_intx_balances(
                &self.exchange_id,
                tenant_id,
                account_id,
                &request.assets,
                &value,
            )?
        } else {
            let mut params = HashMap::new();
            params.insert("limit".to_string(), "250".to_string());
            let value = self
                .send_signed_get("coinbase.get_balances", "/accounts", &params)
                .await?;
            parse_account_balances(
                &self.exchange_id,
                tenant_id,
                account_id,
                market_type,
                &request.assets,
                &value,
            )?
        };
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
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
        }
        let product_type = request
            .symbols
            .first()
            .map(|symbol| symbol.market_type)
            .unwrap_or(MarketType::Spot);
        let mut params = HashMap::new();
        if product_type == MarketType::Perpetual {
            params.insert("product_type".to_string(), "FUTURE".to_string());
            params.insert("contract_expiry_type".to_string(), "PERPETUAL".to_string());
            params.insert("product_venue".to_string(), "INTX".to_string());
        } else {
            params.insert("product_type".to_string(), "SPOT".to_string());
        }
        let value = self
            .send_signed_get("coinbase.get_fees", "/transaction_summary", &params)
            .await?;
        let fees = parse_fee_snapshots(&self.exchange_id, &request.symbols, &value)?;
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fees,
        })
    }

    pub(super) async fn get_positions_impl(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request.market_type.unwrap_or(MarketType::Perpetual);
        self.ensure_supported_market(market_type)?;
        if market_type == MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinbase.get_positions.spot",
            });
        }
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let endpoint = if market_type == MarketType::Perpetual {
            let portfolio_uuid = self.config.perpetual_portfolio_uuid.as_deref().ok_or(
                ExchangeApiError::Unsupported {
                    operation: "coinbase.get_positions.missing_intx_portfolio_uuid",
                },
            )?;
            format!("/intx/positions/{portfolio_uuid}")
        } else {
            "/cfm/positions".to_string()
        };
        let value = self
            .send_signed_get("coinbase.get_positions", &endpoint, &HashMap::new())
            .await?;
        let positions = parse_positions(
            &self.exchange_id,
            tenant_id,
            account_id,
            market_type,
            &request.symbols,
            &value,
        )?;
        Ok(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            positions,
        })
    }

    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        let body = coinbase_order_body(&request)?;
        let value = self
            .send_signed_post("coinbase.place_order", "/orders", &HashMap::new(), &body)
            .await?;
        let order = parse_order_ack(
            &self.exchange_id,
            &request.symbol,
            request.side,
            request.order_type,
            request.quantity,
            request.price,
            &value,
        )?;
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
        self.ensure_supported_market(request.symbol.market_type)?;
        let body = json!({
            "client_order_id": request.client_order_id.unwrap_or_default(),
            "product_id": normalize_coinbase_symbol(&request.symbol.exchange_symbol.symbol)?,
            "side": coinbase_side(request.side),
            "order_configuration": {
                "market_market_ioc": {
                    "quote_size": request.quote_quantity
                }
            }
        });
        let value = self
            .send_signed_post(
                "coinbase.place_quote_market_order",
                "/orders",
                &HashMap::new(),
                &body,
            )
            .await?;
        let order = parse_order_ack(
            &self.exchange_id,
            &request.symbol,
            request.side,
            OrderType::Market,
            "0".to_string(),
            None,
            &value,
        )?;
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

    pub(super) async fn cancel_order_impl(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        if request.client_order_id.is_some() && request.exchange_order_id.is_none() {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinbase.cancel_order_by_client_order_id",
            });
        }
        let exchange_order_id = request.exchange_order_id.as_deref().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "coinbase cancel_order requires exchange_order_id".to_string(),
            }
        })?;
        let body = json!({ "order_ids": [exchange_order_id] });
        let value = self
            .send_signed_post(
                "coinbase.cancel_order",
                "/orders/batch_cancel",
                &HashMap::new(),
                &body,
            )
            .await?;
        let order = parse_cancel_order(
            &self.exchange_id,
            &request.symbol,
            exchange_order_id,
            &value,
        )?;
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
            cancelled: true,
        })
    }

    pub(super) async fn amend_order_impl(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        if request.client_order_id.is_some() && request.exchange_order_id.is_none() {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinbase.amend_order_by_client_order_id",
            });
        }
        if request.new_client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinbase.amend_order.new_client_order_id",
            });
        }
        let exchange_order_id = request.exchange_order_id.as_deref().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "coinbase amend_order requires exchange_order_id".to_string(),
            }
        })?;
        let body = json!({
            "order_id": exchange_order_id,
            "size": non_empty(&request.new_quantity, "new_quantity")?
        });
        let value = self
            .send_signed_post(
                "coinbase.amend_order",
                "/orders/edit",
                &HashMap::new(),
                &body,
            )
            .await?;
        let order = parse_amend_order_ack(
            &self.exchange_id,
            &request.symbol,
            exchange_order_id,
            request.new_quantity,
            &value,
        )?;
        Ok(AmendOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

    pub(super) async fn cancel_all_orders_impl(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coinbase cancel_all_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market(symbol.market_type)?;
        if request
            .market_type
            .is_some_and(|market_type| market_type != symbol.market_type)
        {
            return Err(ExchangeApiError::InvalidRequest {
                message: "coinbase cancel_all_orders market_type does not match symbol".to_string(),
            });
        }
        let mut params = HashMap::new();
        params.insert("order_status".to_string(), "OPEN".to_string());
        params.insert("limit".to_string(), "100".to_string());
        params.insert(
            "product_id".to_string(),
            normalize_coinbase_symbol(&symbol.exchange_symbol.symbol)?,
        );
        let open_value = self
            .send_signed_get(
                "coinbase.cancel_all_orders.open_orders",
                "/orders/historical/batch",
                &params,
            )
            .await?;
        let open_orders = parse_orders(&self.exchange_id, Some(symbol), &open_value)?;
        let order_ids = open_orders
            .iter()
            .filter_map(|order| order.exchange_order_id.clone())
            .collect::<Vec<_>>();
        if order_ids.is_empty() {
            return Ok(CancelAllOrdersResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(request.exchange, request.context.request_id),
                cancelled_count: 0,
                orders: Vec::new(),
            });
        }
        let cancel_symbols = order_ids
            .iter()
            .cloned()
            .map(|order_id| (order_id, symbol.clone()))
            .collect::<Vec<_>>();
        let body = json!({ "order_ids": order_ids });
        let value = self
            .send_signed_post(
                "coinbase.cancel_all_orders",
                "/orders/batch_cancel",
                &HashMap::new(),
                &body,
            )
            .await?;
        let orders = parse_batch_cancel_orders(&self.exchange_id, &cancel_symbols, &value)?;
        let cancelled_count = orders.len() as u32;
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
        let order_lookup_id = request
            .exchange_order_id
            .as_deref()
            .or(request.client_order_id.as_deref())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coinbase query_order requires exchange_order_id or client_order_id"
                    .to_string(),
            })?;
        let endpoint = format!("/orders/historical/{order_lookup_id}");
        let value = self
            .send_signed_get("coinbase.query_order", &endpoint, &HashMap::new())
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order: Some(order),
        })
    }

    pub(super) async fn place_order_list_impl(
        &self,
        request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        let symbol = request.symbol().clone();
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market(symbol.market_type)?;
        let (schema_version, context_request_id, list_client_order_id, body) =
            coinbase_order_list_body(&request)?;
        ensure_exchange_api_schema(schema_version)?;
        let value = self
            .send_signed_post(
                "coinbase.place_order_list",
                "/orders",
                &HashMap::new(),
                &body,
            )
            .await?;
        let order = parse_order_ack(
            &self.exchange_id,
            &symbol,
            OrderSide::Sell,
            OrderType::StopLimit,
            oco_quantity_from_request(&request)?,
            None,
            &value,
        )?;
        Ok(OrderListResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(symbol.exchange.clone(), context_request_id),
            symbol,
            kind: request.kind(),
            order_list_id: value
                .get("success_response")
                .and_then(|response| response.get("order_id"))
                .and_then(Value::as_str)
                .map(str::to_string),
            list_client_order_id,
            list_status_type: Some("EXEC_STARTED".to_string()),
            list_order_status: Some("EXECUTING".to_string()),
            orders: vec![order],
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
                message: "coinbase batch_place_orders requires at least one order".to_string(),
            });
        }

        let mut prepared = Vec::with_capacity(request.orders.len());
        for order in &request.orders {
            ensure_exchange_api_schema(order.schema_version)?;
            self.ensure_exchange(&order.symbol.exchange)?;
            self.ensure_supported_market(order.symbol.market_type)?;
            let body = coinbase_order_body(order)?;
            prepared.push((order, body));
        }

        let mut orders = Vec::with_capacity(prepared.len());
        for (order, body) in prepared {
            let value = self
                .send_signed_post(
                    "coinbase.batch_place_orders.composed",
                    "/orders",
                    &HashMap::new(),
                    &body,
                )
                .await?;
            orders.push(parse_order_ack(
                &self.exchange_id,
                &order.symbol,
                order.side,
                order.order_type,
                order.quantity.clone(),
                order.price.clone(),
                &value,
            )?);
        }

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
                message: "coinbase batch_cancel_orders requires at least one cancel".to_string(),
            });
        }

        let mut order_ids = Vec::with_capacity(request.cancels.len());
        let mut cancel_symbols = Vec::with_capacity(request.cancels.len());
        for cancel in &request.cancels {
            self.ensure_exchange(&cancel.symbol.exchange)?;
            self.ensure_supported_market(cancel.symbol.market_type)?;
            if cancel.client_order_id.is_some() && cancel.exchange_order_id.is_none() {
                return Err(ExchangeApiError::Unsupported {
                    operation: "coinbase.batch_cancel_orders.by_client_order_id",
                });
            }
            let exchange_order_id = cancel.exchange_order_id.as_deref().ok_or_else(|| {
                ExchangeApiError::InvalidRequest {
                    message: "coinbase batch_cancel_orders requires exchange_order_id".to_string(),
                }
            })?;
            order_ids.push(exchange_order_id.to_string());
            cancel_symbols.push((exchange_order_id.to_string(), cancel.symbol.clone()));
        }

        let body = json!({ "order_ids": order_ids });
        let value = self
            .send_signed_post(
                "coinbase.batch_cancel_orders",
                "/orders/batch_cancel",
                &HashMap::new(),
                &body,
            )
            .await?;
        let orders = parse_batch_cancel_orders(&self.exchange_id, &cancel_symbols, &value)?;
        let cancelled_count = orders.len() as u32;
        Ok(BatchCancelOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
            cancelled_count,
            report: None,
        })
    }

    pub(super) async fn get_open_orders_impl(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_supported_market(market_type)?;
        }
        let mut params = HashMap::new();
        params.insert("order_status".to_string(), "OPEN".to_string());
        params.insert("limit".to_string(), "100".to_string());
        if let Some(symbol) = request.symbol.as_ref() {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
            params.insert(
                "product_id".to_string(),
                normalize_coinbase_symbol(&symbol.exchange_symbol.symbol)?,
            );
        }
        let value = self
            .send_signed_get(
                "coinbase.get_open_orders",
                "/orders/historical/batch",
                &params,
            )
            .await?;
        let orders = parse_orders(&self.exchange_id, request.symbol.as_ref(), &value)?;
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
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coinbase get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market(symbol.market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut params = HashMap::new();
        params.insert(
            "product_id".to_string(),
            normalize_coinbase_symbol(&symbol.exchange_symbol.symbol)?,
        );
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert("order_id".to_string(), order_id.to_string());
        }
        if let Some(limit) = request.limit {
            params.insert("limit".to_string(), limit.min(100).to_string());
        }
        if let Some(start) = request.start_time {
            params.insert("start_sequence_timestamp".to_string(), start.to_rfc3339());
        }
        if let Some(end) = request.end_time {
            params.insert("end_sequence_timestamp".to_string(), end.to_rfc3339());
        }
        let value = self
            .send_signed_get(
                "coinbase.get_recent_fills",
                "/orders/historical/fills",
                &params,
            )
            .await?;
        let fills = parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
        })
    }
}

fn coinbase_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    if request.reduce_only && request.symbol.market_type != MarketType::Perpetual {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinbase.place_order.reduce_only",
        });
    }
    let product_id = normalize_coinbase_symbol(&request.symbol.exchange_symbol.symbol)?;
    let side = coinbase_side(request.side);
    let client_order_id = request.client_order_id.clone().unwrap_or_default();
    let order_configuration = match request.order_type {
        OrderType::Market
            if request.symbol.market_type == MarketType::Perpetual
                && request.time_in_force == Some(TimeInForce::FOK) =>
        {
            json!({
                "market_market_fok": {
                    "base_size": non_empty(&request.quantity, "quantity")?
                }
            })
        }
        OrderType::Market => json!({
            "market_market_ioc": {
                "base_size": non_empty(&request.quantity, "quantity")?
            }
        }),
        OrderType::Limit | OrderType::PostOnly => {
            let post_only = request.post_only || request.order_type == OrderType::PostOnly;
            json!({
                "limit_limit_gtc": {
                    "base_size": non_empty(&request.quantity, "quantity")?,
                    "limit_price": non_empty_option(request.price.as_deref(), "price")?,
                    "post_only": post_only
                }
            })
        }
        OrderType::IOC => json!({
            "sor_limit_ioc": {
                "base_size": non_empty(&request.quantity, "quantity")?,
                "limit_price": non_empty_option(request.price.as_deref(), "price")?
            }
        }),
        OrderType::FOK => json!({
            "limit_limit_fok": {
                "base_size": non_empty(&request.quantity, "quantity")?,
                "limit_price": non_empty_option(request.price.as_deref(), "price")?
            }
        }),
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinbase.place_order.order_type",
            })
        }
    };
    if matches!(request.time_in_force, Some(TimeInForce::GTX))
        && request.order_type != OrderType::PostOnly
    {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinbase.place_order.gtx_without_post_only",
        });
    }
    let mut body = json!({
        "client_order_id": client_order_id,
        "product_id": product_id,
        "side": side,
        "order_configuration": order_configuration
    });
    if request.reduce_only {
        body["reduce_only"] = Value::Bool(true);
    }
    Ok(body)
}

fn coinbase_order_list_body(
    request: &OrderListRequest,
) -> ExchangeApiResult<(u16, Option<String>, Option<String>, Value)> {
    match request {
        OrderListRequest::Oco {
            schema_version,
            context,
            symbol,
            list_client_order_id,
            side,
            quantity,
            above,
            below,
        } => {
            if *side != OrderSide::Sell {
                return Err(ExchangeApiError::Unsupported {
                    operation: "coinbase.order_list.oco_buy_bracket",
                });
            }
            let (limit_leg, stop_leg) = oco_bracket_legs(above, below)?;
            let body = json!({
                "client_order_id": list_client_order_id.clone().unwrap_or_default(),
                "product_id": normalize_coinbase_symbol(&symbol.exchange_symbol.symbol)?,
                "side": "SELL",
                "order_configuration": {
                    "trigger_bracket_gtc": {
                        "base_size": non_empty(quantity, "quantity")?,
                        "limit_price": non_empty_option(limit_leg.price.as_deref(), "above.price")?,
                        "stop_trigger_price": non_empty_option(stop_leg.stop_price.as_deref(), "below.stop_price")?
                    }
                }
            });
            Ok((
                *schema_version,
                context.request_id.clone(),
                list_client_order_id.clone(),
                body,
            ))
        }
        OrderListRequest::Oto { schema_version, .. } => {
            ensure_exchange_api_schema(*schema_version)?;
            Err(ExchangeApiError::Unsupported {
                operation: "coinbase.order_list.oto_attached_order",
            })
        }
    }
}

fn oco_bracket_legs<'a>(
    above: &'a OrderListConditionalLeg,
    below: &'a OrderListConditionalLeg,
) -> ExchangeApiResult<(&'a OrderListConditionalLeg, &'a OrderListConditionalLeg)> {
    if above.order_type != OrderListLegType::Limit {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinbase.order_list.oco_above_leg",
        });
    }
    if below.order_type != OrderListLegType::StopLoss {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinbase.order_list.oco_below_leg",
        });
    }
    if below.price.is_some() {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinbase.order_list.stop_limit_price",
        });
    }
    if above.client_order_id.is_some() || below.client_order_id.is_some() {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinbase.order_list.leg_client_order_id",
        });
    }
    if above
        .time_in_force
        .is_some_and(|tif| tif != TimeInForce::GTC)
        || below
            .time_in_force
            .is_some_and(|tif| tif != TimeInForce::GTC)
    {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinbase.order_list.non_gtc_tif",
        });
    }
    non_empty_option(above.price.as_deref(), "above.price")?;
    non_empty_option(below.stop_price.as_deref(), "below.stop_price")?;
    Ok((above, below))
}

fn oco_quantity_from_request(request: &OrderListRequest) -> ExchangeApiResult<String> {
    match request {
        OrderListRequest::Oco { quantity, .. } => non_empty(quantity, "quantity"),
        OrderListRequest::Oto { .. } => Err(ExchangeApiError::Unsupported {
            operation: "coinbase.order_list.oto_attached_order",
        }),
    }
}

fn coinbase_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    }
}

fn non_empty(value: &str, field: &str) -> ExchangeApiResult<String> {
    if value.trim().is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("coinbase {field} must not be empty"),
        });
    }
    Ok(value.to_string())
}

fn non_empty_option(value: Option<&str>, field: &str) -> ExchangeApiResult<String> {
    non_empty(
        value.ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("coinbase {field} is required"),
        })?,
        field,
    )
}
