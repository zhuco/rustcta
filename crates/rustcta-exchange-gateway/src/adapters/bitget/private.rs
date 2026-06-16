use std::collections::HashMap;

use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse,
    ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse, OpenOrdersRequest,
    OpenOrdersResponse, OrderState, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest,
    PositionsResponse, QueryOrderRequest, QueryOrderResponse, QuoteMarketOrderRequest,
    RecentFillsRequest, RecentFillsResponse, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    ExchangeErrorClass, MarketType, OrderSide, OrderStatus, OrderType, PositionSide,
};
use serde_json::{json, Value};

use super::parser::normalize_bitget_symbol;
use super::private_parser::{
    parse_balances, parse_fees, parse_fills, parse_order, parse_orders, parse_positions,
};
use super::{BitgetGatewayAdapter, BITGET_PERP_MARGIN_COIN, BITGET_PERP_PRODUCT_TYPE};
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl BitgetGatewayAdapter {
    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.ensure_private_rest("bitget.place_order")?;
        let body = bitget_place_order_body(&request)?;
        let endpoint = order_endpoint(request.symbol.market_type);
        let value = self
            .rest
            .send_signed_post("bitget.place_order", endpoint, &body)
            .await?;
        let ack = bitget_ack_data(&value);
        let order = order_state_from_place_ack(&self.exchange_id, &request, ack);
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
        self.ensure_private_rest("bitget.place_quote_market_order")?;
        let body = bitget_quote_market_order_body(&request)?;
        let value = self
            .rest
            .send_signed_post(
                "bitget.place_quote_market_order",
                "/api/v2/spot/trade/place-order",
                &body,
            )
            .await?;
        let ack = bitget_ack_data(&value);
        let order = order_state_from_quote_ack(&self.exchange_id, &request, ack);
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
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.ensure_private_rest("bitget.cancel_order")?;
        let body = bitget_cancel_order_body(&request)?;
        let endpoint = cancel_endpoint(request.symbol.market_type);
        let value = self
            .rest
            .send_signed_post("bitget.cancel_order", endpoint, &body)
            .await?;
        let ack = bitget_ack_data(&value);
        let order = order_state_from_cancel_ack(&self.exchange_id, &request, ack);
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
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
        let market_type = request
            .symbol
            .as_ref()
            .map(|symbol| symbol.market_type)
            .or(request.market_type)
            .unwrap_or(MarketType::Spot);
        self.ensure_supported_market_type(market_type)?;
        if request
            .market_type
            .is_some_and(|requested| requested != market_type)
        {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bitget.cancel_all_orders market_type conflicts with symbol".to_string(),
            });
        }
        self.ensure_private_rest("bitget.cancel_all_orders")?;
        let body = bitget_cancel_all_body(request.symbol.as_ref(), market_type)?;
        let endpoint = cancel_all_endpoint(market_type);
        let value = self
            .rest
            .send_signed_post("bitget.cancel_all_orders", endpoint, &body)
            .await?;
        let ack = bitget_ack_data(&value);
        let order_id = value_text(ack.get("orderId"));
        let client_order_id = value_text(ack.get("clientOid"));
        let mut orders = Vec::new();
        if order_id.is_some() || client_order_id.is_some() {
            let symbol =
                request
                    .symbol
                    .as_ref()
                    .ok_or_else(|| ExchangeApiError::InvalidRequest {
                        message: "bitget.cancel_all_orders response had order id without symbol"
                            .to_string(),
                    })?;
            orders.push(order_state_from_cancel_fields(
                &self.exchange_id,
                symbol,
                order_id,
                client_order_id,
            ));
        }
        let cancelled_count = if orders.is_empty() {
            0
        } else {
            orders.len() as u32
        };
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
            cancelled_count,
        })
    }

    pub(super) async fn amend_order_impl(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.ensure_private_rest("bitget.amend_order")?;
        let body = bitget_amend_order_body(&request)?;
        let endpoint = amend_endpoint(request.symbol.market_type);
        let value = self
            .rest
            .send_signed_post("bitget.amend_order", endpoint, &body)
            .await?;
        let ack = bitget_ack_data(&value);
        let order = order_state_from_amend_ack(&self.exchange_id, &request, ack);
        Ok(AmendOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_private_rest("bitget.get_balances")?;
        let mut params = HashMap::new();
        let (market_type, endpoint) = match request.market_type {
            Some(market_type) => {
                self.ensure_supported_market_type(market_type)?;
                match market_type {
                    MarketType::Spot => (market_type, "/api/v2/spot/account/assets"),
                    MarketType::Perpetual => {
                        params.insert(
                            "productType".to_string(),
                            BITGET_PERP_PRODUCT_TYPE.to_string(),
                        );
                        (market_type, "/api/v2/mix/account/accounts")
                    }
                    _ => unreachable!("checked by ensure_supported_market_type"),
                }
            }
            None => (MarketType::Perpetual, "/api/v3/account/assets"),
        };
        let value = self
            .rest
            .send_signed_get("bitget.get_balances", endpoint, &params)
            .await?;
        let tenant_id =
            request
                .context
                .tenant_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "bitget.get_balances requires tenant_id in request context"
                        .to_string(),
                })?;
        let account_id =
            request
                .context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "bitget.get_balances requires account_id in request context"
                        .to_string(),
                })?;
        let balances = parse_balances(
            &self.exchange_id,
            tenant_id,
            account_id,
            &request.assets,
            market_type,
            &value,
        )?;
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            balances,
        })
    }

    pub(super) async fn get_positions_impl(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_supported_market_type(market_type)?;
            if market_type == MarketType::Spot {
                return Ok(PositionsResponse {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    metadata: response_metadata(request.exchange, request.context.request_id),
                    positions: Vec::new(),
                });
            }
        }
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange_id)?;
            self.ensure_supported_market_type(symbol.market_type)?;
            if symbol.market_type == MarketType::Spot {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "bitget.get_positions symbols must be perpetual".to_string(),
                });
            }
        }
        self.ensure_private_rest("bitget.get_positions")?;
        let tenant_id =
            request
                .context
                .tenant_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "bitget.get_positions requires tenant_id in request context"
                        .to_string(),
                })?;
        let account_id =
            request
                .context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "bitget.get_positions requires account_id in request context"
                        .to_string(),
                })?;
        let mut params = HashMap::new();
        params.insert(
            "productType".to_string(),
            BITGET_PERP_PRODUCT_TYPE.to_string(),
        );
        params.insert(
            "marginCoin".to_string(),
            BITGET_PERP_MARGIN_COIN.to_string(),
        );
        if let [symbol] = request.symbols.as_slice() {
            params.insert(
                "symbol".to_string(),
                normalize_bitget_symbol(&symbol.symbol)?,
            );
        }
        let value = self
            .rest
            .send_signed_get(
                "bitget.get_positions",
                "/api/v2/mix/position/all-position",
                &params,
            )
            .await?;
        let positions = parse_positions(
            &self.exchange_id,
            tenant_id,
            account_id,
            &request.symbols,
            &value,
        )?;
        Ok(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            positions,
        })
    }

    pub(super) async fn get_fees_impl(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_private_rest("bitget.get_fees")?;
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bitget.get_fees requires at least one symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
            let mut params = HashMap::new();
            params.insert(
                "symbol".to_string(),
                normalize_bitget_symbol(&symbol.exchange_symbol.symbol)?,
            );
            let endpoint = match symbol.market_type {
                MarketType::Spot => {
                    params.insert("category".to_string(), "SPOT".to_string());
                    "/api/v3/account/fee-rate"
                }
                MarketType::Perpetual => {
                    params.insert(
                        "productType".to_string(),
                        BITGET_PERP_PRODUCT_TYPE.to_string(),
                    );
                    "/api/v2/mix/market/contracts"
                }
                _ => unreachable!("checked by ensure_supported_market_type"),
            };
            let value = self
                .rest
                .send_signed_get("bitget.get_fees", endpoint, &params)
                .await?;
            fees.extend(parse_fees(&self.exchange_id, symbol, &value)?);
        }
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fees,
        })
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.ensure_private_rest("bitget.query_order")?;
        let mut base_params = HashMap::new();
        base_params.insert(
            "symbol".to_string(),
            normalize_bitget_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        if request.symbol.market_type == MarketType::Perpetual {
            base_params.insert(
                "productType".to_string(),
                BITGET_PERP_PRODUCT_TYPE.to_string(),
            );
        }
        let mut attempts = Vec::new();
        if let Some(exchange_order_id) = request.exchange_order_id.as_deref() {
            attempts.push(bitget_order_query_params(
                &base_params,
                "orderId",
                exchange_order_id,
            ));
        }
        if let Some(client_order_id) = request.client_order_id.as_deref() {
            attempts.push(bitget_order_query_params(
                &base_params,
                "clientOid",
                client_order_id,
            ));
        }
        if attempts.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bitget.query_order requires exchange_order_id or client_order_id"
                    .to_string(),
            });
        }
        let endpoint = query_endpoint(request.symbol.market_type);
        let attempt_count = attempts.len();
        for (index, params) in attempts.into_iter().enumerate() {
            match self
                .rest
                .send_signed_get("bitget.query_order", endpoint, &params)
                .await
            {
                Ok(value) => {
                    let order = parse_order(
                        &self.exchange_id,
                        Some(&request.symbol),
                        request.symbol.market_type,
                        &value,
                    )?;
                    if order.is_some() || index + 1 == attempt_count {
                        return Ok(QueryOrderResponse {
                            schema_version: EXCHANGE_API_SCHEMA_VERSION,
                            metadata: response_metadata(
                                self.exchange_id.clone(),
                                request.context.request_id,
                            ),
                            order,
                        });
                    }
                }
                Err(error) if index + 1 < attempt_count && is_order_lookup_retryable(&error) => {
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("bitget query_order has at least one lookup attempt")
    }

    pub(super) async fn get_open_orders_impl(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request
            .symbol
            .as_ref()
            .map(|symbol| symbol.market_type)
            .or(request.market_type)
            .unwrap_or(MarketType::Spot);
        self.ensure_supported_market_type(market_type)?;
        if request
            .market_type
            .is_some_and(|requested| requested != market_type)
        {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bitget.get_open_orders market_type conflicts with symbol".to_string(),
            });
        }
        self.ensure_private_rest("bitget.get_open_orders")?;
        let mut params = HashMap::new();
        if market_type == MarketType::Perpetual {
            params.insert(
                "productType".to_string(),
                BITGET_PERP_PRODUCT_TYPE.to_string(),
            );
        }
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
            params.insert(
                "symbol".to_string(),
                normalize_bitget_symbol(&symbol.exchange_symbol.symbol)?,
            );
        }
        let endpoint = open_orders_endpoint(market_type);
        let value = self
            .rest
            .send_signed_get("bitget.get_open_orders", endpoint, &params)
            .await?;
        let orders = parse_orders(
            &self.exchange_id,
            request.symbol.as_ref(),
            market_type,
            &value,
        )?;
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
            self.ensure_supported_market_type(market_type)?;
        }
        self.ensure_private_rest("bitget.get_recent_fills")?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitget.get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        if request
            .market_type
            .is_some_and(|market_type| market_type != symbol.market_type)
        {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bitget.get_recent_fills market_type conflicts with symbol".to_string(),
            });
        }
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_bitget_symbol(&symbol.exchange_symbol.symbol)?,
        );
        if symbol.market_type == MarketType::Perpetual {
            params.insert(
                "productType".to_string(),
                BITGET_PERP_PRODUCT_TYPE.to_string(),
            );
        }
        if let Some(exchange_order_id) = &request.exchange_order_id {
            params.insert("orderId".to_string(), exchange_order_id.clone());
        }
        if let Some(client_order_id) = &request.client_order_id {
            params.insert("clientOid".to_string(), client_order_id.clone());
        }
        if let Some(from_trade_id) = &request.from_trade_id {
            params.insert("idLessThan".to_string(), from_trade_id.clone());
        }
        if let Some(start_time) = request.start_time {
            params.insert(
                "startTime".to_string(),
                start_time.timestamp_millis().to_string(),
            );
        }
        if let Some(end_time) = request.end_time {
            params.insert(
                "endTime".to_string(),
                end_time.timestamp_millis().to_string(),
            );
        }
        params.insert(
            "limit".to_string(),
            request.limit.unwrap_or(100).clamp(1, 100).to_string(),
        );
        let endpoint = fills_endpoint(symbol.market_type);
        let value = self
            .rest
            .send_signed_get("bitget.get_recent_fills", endpoint, &params)
            .await?;
        let tenant_id =
            request
                .context
                .tenant_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "bitget.get_recent_fills requires tenant_id in request context"
                        .to_string(),
                })?;
        let account_id =
            request
                .context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "bitget.get_recent_fills requires account_id in request context"
                        .to_string(),
                })?;
        let fills = parse_fills(
            &self.exchange_id,
            tenant_id,
            account_id,
            Some(symbol),
            symbol.market_type,
            &value,
        )?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
        })
    }
}

fn bitget_place_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = json!({
        "symbol": normalize_bitget_symbol(&request.symbol.exchange_symbol.symbol)?,
        "side": bitget_order_side(request)?,
        "orderType": bitget_order_type(request.order_type)?,
        "size": non_empty("quantity", &request.quantity)?,
    });

    match request.symbol.market_type {
        MarketType::Spot => {
            if request.reduce_only {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "bitget spot order does not support reduce_only".to_string(),
                });
            }
        }
        MarketType::Perpetual => {
            body["productType"] = Value::String(BITGET_PERP_PRODUCT_TYPE.to_string());
            body["marginMode"] = Value::String("crossed".to_string());
            body["marginCoin"] = Value::String(BITGET_PERP_MARGIN_COIN.to_string());
            if uses_hedge_trade_side(request.position_side) {
                body["tradeSide"] =
                    Value::String(if request.reduce_only { "close" } else { "open" }.to_string());
            } else if request.reduce_only {
                body["reduceOnly"] = Value::String("yes".to_string());
            }
        }
        _ => unreachable!("checked by caller"),
    }

    if request.order_type == OrderType::Market {
        if request.symbol.market_type == MarketType::Perpetual {
            body["price"] = Value::String("0".to_string());
        }
    } else {
        let price = request
            .price
            .as_deref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitget limit-style order requires price".to_string(),
            })?;
        body["price"] = Value::String(non_empty("price", price)?);
    }
    body["force"] = Value::String(
        bitget_time_in_force(request.order_type, request.time_in_force, request.post_only)
            .to_string(),
    );
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["clientOid"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    Ok(body)
}

fn bitget_order_query_params(
    base_params: &HashMap<String, String>,
    id_field: &str,
    id_value: &str,
) -> HashMap<String, String> {
    let mut params = base_params.clone();
    params.insert(id_field.to_string(), id_value.to_string());
    params
}

fn is_order_lookup_retryable(error: &ExchangeApiError) -> bool {
    matches!(
        error,
        ExchangeApiError::Exchange(exchange_error)
            if exchange_error.class == ExchangeErrorClass::OrderNotFound
    )
}

fn bitget_quote_market_order_body(request: &QuoteMarketOrderRequest) -> ExchangeApiResult<Value> {
    if request.side != OrderSide::Buy {
        return Err(ExchangeApiError::Unsupported {
            operation: "bitget.quote_market_sell",
        });
    }
    let mut body = json!({
        "symbol": normalize_bitget_symbol(&request.symbol.exchange_symbol.symbol)?,
        "side": "buy",
        "orderType": "market",
        "size": non_empty("quote_quantity", &request.quote_quantity)?,
    });
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["clientOid"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    Ok(body)
}

fn bitget_cancel_order_body(request: &CancelOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = json!({
        "symbol": normalize_bitget_symbol(&request.symbol.exchange_symbol.symbol)?,
    });
    if request.symbol.market_type == MarketType::Perpetual {
        body["productType"] = Value::String(BITGET_PERP_PRODUCT_TYPE.to_string());
        body["marginCoin"] = Value::String(BITGET_PERP_MARGIN_COIN.to_string());
    }
    insert_order_identity(
        body.as_object_mut().expect("object"),
        request.exchange_order_id.as_deref(),
        request.client_order_id.as_deref(),
        "cancel_order",
    )?;
    Ok(body)
}

fn bitget_cancel_all_body(
    symbol: Option<&rustcta_exchange_api::SymbolScope>,
    market_type: MarketType,
) -> ExchangeApiResult<Value> {
    match market_type {
        MarketType::Spot => {
            let symbol = symbol.ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitget.cancel_all_orders spot requires symbol".to_string(),
            })?;
            Ok(json!({
                "symbol": normalize_bitget_symbol(&symbol.exchange_symbol.symbol)?,
            }))
        }
        MarketType::Perpetual => {
            let mut body = json!({
                "productType": BITGET_PERP_PRODUCT_TYPE,
                "marginCoin": BITGET_PERP_MARGIN_COIN,
            });
            if let Some(symbol) = symbol {
                body["symbol"] =
                    Value::String(normalize_bitget_symbol(&symbol.exchange_symbol.symbol)?);
            }
            Ok(body)
        }
        _ => unreachable!("checked by caller"),
    }
}

fn bitget_amend_order_body(request: &AmendOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = match request.symbol.market_type {
        MarketType::Spot => {
            if request
                .new_client_order_id
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
            {
                return Err(ExchangeApiError::Unsupported {
                    operation: "bitget.amend_new_client_order_id",
                });
            }
            json!({
                "category": "SPOT",
                "symbol": normalize_bitget_symbol(&request.symbol.exchange_symbol.symbol)?,
                "qty": non_empty("new_quantity", &request.new_quantity)?,
                "autoCancel": "no",
            })
        }
        MarketType::Perpetual => {
            let mut body = json!({
                "productType": BITGET_PERP_PRODUCT_TYPE,
                "symbol": normalize_bitget_symbol(&request.symbol.exchange_symbol.symbol)?,
                "marginCoin": BITGET_PERP_MARGIN_COIN,
                "newSize": non_empty("new_quantity", &request.new_quantity)?,
            });
            if let Some(new_client_order_id) = request.new_client_order_id.as_deref() {
                body["newClientOid"] =
                    Value::String(non_empty("new_client_order_id", new_client_order_id)?);
            }
            body
        }
        _ => unreachable!("checked by caller"),
    };
    insert_order_identity(
        body.as_object_mut().expect("object"),
        request.exchange_order_id.as_deref(),
        request.client_order_id.as_deref(),
        "amend_order",
    )?;
    Ok(body)
}

fn order_state_from_place_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &PlaceOrderRequest,
    ack: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: value_text(ack.get("clientOid"))
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_text(ack.get("orderId")),
        side: request.side,
        position_side: request.position_side.or(Some(PositionSide::None)),
        order_type: request.order_type,
        time_in_force: Some(resolve_time_in_force(
            request.order_type,
            request.time_in_force,
            request.post_only,
        )),
        status: OrderStatus::New,
        quantity: request.quantity.clone(),
        price: request.price.clone(),
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: request.reduce_only,
        post_only: matches!(request.time_in_force, Some(TimeInForce::GTX))
            || request.post_only
            || request.order_type == OrderType::PostOnly,
        created_at: Some(Utc::now()),
        updated_at: Utc::now(),
    }
}

fn order_state_from_quote_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &QuoteMarketOrderRequest,
    ack: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: value_text(ack.get("clientOid"))
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_text(ack.get("orderId")),
        side: request.side,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Market,
        time_in_force: None,
        status: OrderStatus::New,
        quantity: request.quote_quantity.clone(),
        price: None,
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: Some(Utc::now()),
        updated_at: Utc::now(),
    }
}

fn order_state_from_cancel_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &CancelOrderRequest,
    ack: &Value,
) -> OrderState {
    order_state_from_cancel_fields(
        exchange_id,
        &request.symbol,
        value_text(ack.get("orderId")).or_else(|| request.exchange_order_id.clone()),
        value_text(ack.get("clientOid")).or_else(|| request.client_order_id.clone()),
    )
}

fn order_state_from_cancel_fields(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: &rustcta_exchange_api::SymbolScope,
    order_id: Option<String>,
    client_order_id: Option<String>,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id,
        exchange_order_id: order_id,
        side: OrderSide::Buy,
        position_side: Some(if symbol.market_type == MarketType::Perpetual {
            PositionSide::Net
        } else {
            PositionSide::None
        }),
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        status: OrderStatus::Cancelled,
        quantity: "0".to_string(),
        price: None,
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: None,
        updated_at: Utc::now(),
    }
}

fn order_state_from_amend_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &AmendOrderRequest,
    ack: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: value_text(ack.get("clientOid"))
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_text(ack.get("orderId"))
            .or_else(|| request.exchange_order_id.clone()),
        side: OrderSide::Buy,
        position_side: Some(if request.symbol.market_type == MarketType::Perpetual {
            PositionSide::Net
        } else {
            PositionSide::None
        }),
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        status: OrderStatus::New,
        quantity: request.new_quantity.clone(),
        price: None,
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: None,
        updated_at: Utc::now(),
    }
}

fn bitget_ack_data(value: &Value) -> &Value {
    value.get("data").unwrap_or(value)
}

fn insert_order_identity(
    body: &mut serde_json::Map<String, Value>,
    exchange_order_id: Option<&str>,
    client_order_id: Option<&str>,
    operation: &str,
) -> ExchangeApiResult<()> {
    if let Some(order_id) = exchange_order_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        body.insert("orderId".to_string(), Value::String(order_id.to_string()));
    }
    if let Some(client_id) = client_order_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        body.insert(
            "clientOid".to_string(),
            Value::String(client_id.to_string()),
        );
    }
    if !body.contains_key("orderId") && !body.contains_key("clientOid") {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("bitget {operation} requires exchange_order_id or client_order_id"),
        });
    }
    Ok(())
}

fn order_endpoint(market_type: MarketType) -> &'static str {
    match market_type {
        MarketType::Spot => "/api/v2/spot/trade/place-order",
        MarketType::Perpetual => "/api/v2/mix/order/place-order",
        _ => unreachable!("checked by caller"),
    }
}

fn cancel_endpoint(market_type: MarketType) -> &'static str {
    match market_type {
        MarketType::Spot => "/api/v2/spot/trade/cancel-order",
        MarketType::Perpetual => "/api/v2/mix/order/cancel-order",
        _ => unreachable!("checked by caller"),
    }
}

fn cancel_all_endpoint(market_type: MarketType) -> &'static str {
    match market_type {
        MarketType::Spot => "/api/v2/spot/trade/cancel-symbol-order",
        MarketType::Perpetual => "/api/v2/mix/order/cancel-all-orders",
        _ => unreachable!("checked by caller"),
    }
}

fn amend_endpoint(market_type: MarketType) -> &'static str {
    match market_type {
        MarketType::Spot => "/api/v3/trade/modify-order",
        MarketType::Perpetual => "/api/v2/mix/order/modify-order",
        _ => unreachable!("checked by caller"),
    }
}

fn query_endpoint(market_type: MarketType) -> &'static str {
    match market_type {
        MarketType::Spot => "/api/v2/spot/trade/orderInfo",
        MarketType::Perpetual => "/api/v2/mix/order/detail",
        _ => unreachable!("checked by caller"),
    }
}

fn open_orders_endpoint(market_type: MarketType) -> &'static str {
    match market_type {
        MarketType::Spot => "/api/v2/spot/trade/unfilled-orders",
        MarketType::Perpetual => "/api/v2/mix/order/orders-pending",
        _ => unreachable!("checked by caller"),
    }
}

fn fills_endpoint(market_type: MarketType) -> &'static str {
    match market_type {
        MarketType::Spot => "/api/v2/spot/trade/fills",
        MarketType::Perpetual => "/api/v2/mix/order/fills",
        _ => unreachable!("checked by caller"),
    }
}

fn bitget_order_side(request: &PlaceOrderRequest) -> ExchangeApiResult<&'static str> {
    if request.symbol.market_type == MarketType::Perpetual {
        if let Some(position_side) = request.position_side {
            return Ok(match position_side {
                PositionSide::Long => "buy",
                PositionSide::Short => "sell",
                PositionSide::Net | PositionSide::None => bitget_side(request.side),
            });
        }
    }
    Ok(bitget_side(request.side))
}

fn bitget_order_type(order_type: OrderType) -> ExchangeApiResult<&'static str> {
    match order_type {
        OrderType::Market => Ok("market"),
        OrderType::Limit | OrderType::PostOnly | OrderType::IOC | OrderType::FOK => Ok("limit"),
        OrderType::StopMarket | OrderType::StopLimit => Err(ExchangeApiError::Unsupported {
            operation: "bitget.stop_order",
        }),
    }
}

fn uses_hedge_trade_side(position_side: Option<PositionSide>) -> bool {
    matches!(
        position_side,
        Some(PositionSide::Long | PositionSide::Short)
    )
}

fn bitget_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn resolve_time_in_force(
    order_type: OrderType,
    tif: Option<TimeInForce>,
    post_only: bool,
) -> TimeInForce {
    if post_only || order_type == OrderType::PostOnly {
        return TimeInForce::GTX;
    }
    tif.unwrap_or(match order_type {
        OrderType::IOC => TimeInForce::IOC,
        OrderType::FOK => TimeInForce::FOK,
        _ => TimeInForce::GTC,
    })
}

fn bitget_time_in_force(
    order_type: OrderType,
    tif: Option<TimeInForce>,
    post_only: bool,
) -> &'static str {
    match resolve_time_in_force(order_type, tif, post_only) {
        TimeInForce::IOC => "ioc",
        TimeInForce::FOK => "fok",
        TimeInForce::GTX => "post_only",
        TimeInForce::GTC => "gtc",
    }
}

fn non_empty(field: &str, value: &str) -> ExchangeApiResult<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("bitget {field} must not be empty"),
        });
    }
    Ok(value.to_string())
}

fn value_text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) if !text.is_empty() => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}
