use std::collections::HashMap;

use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse,
    ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse, OpenOrdersRequest,
    OpenOrdersResponse, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse,
    QueryOrderRequest, QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest,
    RecentFillsResponse, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeErrorClass, MarketType, OrderSide, OrderType};
use serde_json::{json, Value};

use super::parser::normalize_gateio_symbol;
use super::private_parser::{
    parse_balances, parse_fees, parse_fills, parse_open_orders, parse_order, parse_positions,
};
use super::GateIoGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl GateIoGatewayAdapter {
    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        let (endpoint, body) = match request.symbol.market_type {
            MarketType::Spot => ("/spot/orders", gateio_order_body(&request)?),
            MarketType::Perpetual => ("/futures/usdt/orders", gateio_futures_order_body(&request)?),
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_post("gateio.place_order", endpoint, &HashMap::new(), &body)
            .await?;
        let order = parse_order(
            &self.exchange_id,
            Some(&request.symbol),
            request.symbol.market_type,
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
        self.ensure_spot(request.symbol.market_type)?;
        let body = gateio_quote_market_order_body(&request)?;
        let value = self
            .send_signed_post(
                "gateio.place_quote_market_order",
                "/spot/orders",
                &HashMap::new(),
                &body,
            )
            .await?;
        let order = parse_order(
            &self.exchange_id,
            Some(&request.symbol),
            request.symbol.market_type,
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
        let order_id = gateio_order_lookup_id(
            request.exchange_order_id.as_deref(),
            request.client_order_id.as_deref(),
            request.symbol.market_type,
            "gateio.cancel_by_client_order_id",
        )?;
        let mut params = HashMap::new();
        let endpoint = match request.symbol.market_type {
            MarketType::Spot => {
                params.insert(
                    "currency_pair".to_string(),
                    normalize_gateio_symbol(&request.symbol.exchange_symbol.symbol)?,
                );
                format!("/spot/orders/{order_id}")
            }
            MarketType::Perpetual => {
                params.insert(
                    "contract".to_string(),
                    normalize_gateio_symbol(&request.symbol.exchange_symbol.symbol)?,
                );
                gateio_futures_order_path(&order_id)
            }
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_delete("gateio.cancel_order", &endpoint, &params)
            .await?;
        let order = parse_order(
            &self.exchange_id,
            Some(&request.symbol),
            request.symbol.market_type,
            &value,
        )?;
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
        self.ensure_optional_supported_market(request.market_type)?;
        let market_type = request
            .symbol
            .as_ref()
            .map(|symbol| symbol.market_type)
            .or(request.market_type)
            .unwrap_or(MarketType::Spot);
        self.ensure_supported_market(market_type)?;
        let mut params = HashMap::new();
        let (endpoint, fallback_symbol) =
            match market_type {
                MarketType::Spot => {
                    let symbol = request.symbol.as_ref().ok_or_else(|| {
                        ExchangeApiError::InvalidRequest {
                            message: "gateio.cancel_all_orders requires symbol".to_string(),
                        }
                    })?;
                    self.ensure_exchange(&symbol.exchange)?;
                    self.ensure_spot(symbol.market_type)?;
                    params.insert(
                        "currency_pair".to_string(),
                        normalize_gateio_symbol(&symbol.exchange_symbol.symbol)?,
                    );
                    ("/spot/orders", Some(symbol))
                }
                MarketType::Perpetual => {
                    if let Some(symbol) = request.symbol.as_ref() {
                        self.ensure_exchange(&symbol.exchange)?;
                        if symbol.market_type != MarketType::Perpetual {
                            return Err(ExchangeApiError::InvalidRequest {
                                message: "gateio.cancel_all_orders market_type/symbol mismatch"
                                    .to_string(),
                            });
                        }
                        params.insert(
                            "contract".to_string(),
                            normalize_gateio_symbol(&symbol.exchange_symbol.symbol)?,
                        );
                    }
                    ("/futures/usdt/orders", request.symbol.as_ref())
                }
                _ => unreachable!("checked by ensure_supported_market"),
            };
        let value = self
            .send_signed_delete("gateio.cancel_all_orders", endpoint, &params)
            .await?;
        let orders = parse_open_orders(&self.exchange_id, fallback_symbol, market_type, &value)?;
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
        })
    }

    pub(super) async fn amend_order_impl(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let order_id = request
            .exchange_order_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or(ExchangeApiError::Unsupported {
                operation: "gateio.amend_by_client_order_id",
            })?;
        let body = gateio_amend_order_body(&request)?;
        let endpoint = format!("/spot/orders/{order_id}");
        let value = self
            .send_signed_patch("gateio.amend_order", &endpoint, &HashMap::new(), &body)
            .await?;
        let order = parse_order(
            &self.exchange_id,
            Some(&request.symbol),
            request.symbol.market_type,
            &value,
        )?;
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
        self.ensure_optional_supported_market(request.market_type)?;
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        let (tenant_id, account_id) =
            self.context_account(&request.context, "gateio.get_balances")?;
        let endpoint = match market_type {
            MarketType::Spot => "/spot/accounts",
            MarketType::Perpetual => "/futures/usdt/accounts",
            _ => unreachable!("checked by ensure_optional_supported_market"),
        };
        let value = self
            .send_signed_get("gateio.get_balances", endpoint, &HashMap::new())
            .await?;
        let balances = parse_balances(
            &self.exchange_id,
            tenant_id,
            account_id,
            market_type,
            &request.assets,
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
        if request
            .market_type
            .is_some_and(|market_type| market_type != MarketType::Perpetual)
        {
            return Err(ExchangeApiError::Unsupported {
                operation: "gateio.positions_non_perpetual",
            });
        }
        let (tenant_id, account_id) =
            self.context_account(&request.context, "gateio.get_positions")?;
        let endpoint = if let Some(symbol) = request.symbols.first() {
            self.ensure_exchange(&symbol.exchange_id)?;
            if symbol.market_type != MarketType::Perpetual {
                return Err(ExchangeApiError::Unsupported {
                    operation: "gateio.positions_non_perpetual",
                });
            }
            format!(
                "/futures/usdt/positions/{}",
                normalize_gateio_symbol(&symbol.symbol)?
            )
        } else {
            "/futures/usdt/positions".to_string()
        };
        let value = match self
            .send_signed_get("gateio.get_positions", &endpoint, &HashMap::new())
            .await
        {
            Ok(value) => value,
            Err(error) if is_gateio_position_not_found(&error) && !request.symbols.is_empty() => {
                Value::Array(Vec::new())
            }
            Err(error) => return Err(error),
        };
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
                message: "gateio.get_fees requires at least one symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
            let mut params = HashMap::new();
            let endpoint = match symbol.market_type {
                MarketType::Spot => {
                    params.insert(
                        "currency_pair".to_string(),
                        normalize_gateio_symbol(&symbol.exchange_symbol.symbol)?,
                    );
                    "/spot/fee".to_string()
                }
                MarketType::Perpetual => format!(
                    "/futures/usdt/contracts/{}",
                    normalize_gateio_symbol(&symbol.exchange_symbol.symbol)?
                ),
                _ => unreachable!("checked by ensure_supported_market"),
            };
            let value = self
                .send_signed_get("gateio.get_fees", &endpoint, &params)
                .await?;
            fees.push(parse_fees(
                &self.exchange_id,
                symbol,
                symbol.market_type,
                &value,
            )?);
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
        self.ensure_supported_market(request.symbol.market_type)?;
        let order_id = gateio_order_lookup_id(
            request.exchange_order_id.as_deref(),
            request.client_order_id.as_deref(),
            request.symbol.market_type,
            "gateio.query_by_client_order_id",
        )?;
        let mut params = HashMap::new();
        let endpoint = match request.symbol.market_type {
            MarketType::Spot => {
                params.insert(
                    "currency_pair".to_string(),
                    normalize_gateio_symbol(&request.symbol.exchange_symbol.symbol)?,
                );
                format!("/spot/orders/{order_id}")
            }
            MarketType::Perpetual => {
                params.insert(
                    "contract".to_string(),
                    normalize_gateio_symbol(&request.symbol.exchange_symbol.symbol)?,
                );
                gateio_futures_order_path(&order_id)
            }
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_get("gateio.query_order", &endpoint, &params)
            .await?;
        let order = parse_order(
            &self.exchange_id,
            Some(&request.symbol),
            request.symbol.market_type,
            &value,
        )?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order: Some(order),
        })
    }

    pub(super) async fn get_open_orders_impl(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_optional_supported_market(request.market_type)?;
        let market_type = request
            .symbol
            .as_ref()
            .map(|symbol| symbol.market_type)
            .or(request.market_type)
            .unwrap_or(MarketType::Spot);
        self.ensure_supported_market(market_type)?;
        let mut params = HashMap::new();
        let endpoint = match market_type {
            MarketType::Spot => "/spot/open_orders",
            MarketType::Perpetual => {
                params.insert("status".to_string(), "open".to_string());
                "/futures/usdt/orders"
            }
            _ => unreachable!("checked by ensure_supported_market"),
        };
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            if symbol.market_type != market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "gateio.get_open_orders market_type/symbol mismatch".to_string(),
                });
            }
            match market_type {
                MarketType::Spot => {
                    params.insert(
                        "currency_pair".to_string(),
                        normalize_gateio_symbol(&symbol.exchange_symbol.symbol)?,
                    );
                }
                MarketType::Perpetual => {
                    params.insert(
                        "contract".to_string(),
                        normalize_gateio_symbol(&symbol.exchange_symbol.symbol)?,
                    );
                }
                _ => unreachable!("checked by ensure_supported_market"),
            }
        }
        let value = self
            .send_signed_get("gateio.get_open_orders", endpoint, &params)
            .await?;
        let orders = parse_open_orders(
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
        self.ensure_optional_supported_market(request.market_type)?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "gateio.get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market(symbol.market_type)?;
        if request
            .market_type
            .is_some_and(|market_type| market_type != symbol.market_type)
        {
            return Err(ExchangeApiError::InvalidRequest {
                message: "gateio.get_recent_fills market_type/symbol mismatch".to_string(),
            });
        }
        let (tenant_id, account_id) =
            self.context_account(&request.context, "gateio.get_recent_fills")?;
        let mut params = HashMap::new();
        let endpoint = match symbol.market_type {
            MarketType::Spot => {
                params.insert(
                    "currency_pair".to_string(),
                    normalize_gateio_symbol(&symbol.exchange_symbol.symbol)?,
                );
                "/spot/my_trades"
            }
            MarketType::Perpetual => {
                params.insert(
                    "contract".to_string(),
                    normalize_gateio_symbol(&symbol.exchange_symbol.symbol)?,
                );
                "/futures/usdt/my_trades"
            }
            _ => unreachable!("checked by ensure_supported_market"),
        };
        if let Some(limit) = request.limit {
            params.insert("limit".to_string(), limit.min(1000).to_string());
        }
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            let key = if symbol.market_type == MarketType::Perpetual {
                "order"
            } else {
                "order_id"
            };
            params.insert(key.to_string(), order_id.to_string());
        }
        if let Some(from_trade_id) = request.from_trade_id.as_deref() {
            params.insert("last_id".to_string(), from_trade_id.to_string());
        }
        if let Some(start_time) = request.start_time {
            params.insert("from".to_string(), start_time.timestamp().to_string());
        }
        if let Some(end_time) = request.end_time {
            params.insert("to".to_string(), end_time.timestamp().to_string());
        }
        let value = self
            .send_signed_get("gateio.get_recent_fills", endpoint, &params)
            .await?;
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

fn gateio_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    if request.reduce_only {
        return Err(ExchangeApiError::InvalidRequest {
            message: "gateio spot order does not support reduce_only".to_string(),
        });
    }
    let order_type = match request.order_type {
        OrderType::Market => "market",
        OrderType::StopMarket | OrderType::StopLimit => {
            return Err(ExchangeApiError::Unsupported {
                operation: "gateio.stop_order",
            });
        }
        _ => "limit",
    };
    let mut body = json!({
        "currency_pair": normalize_gateio_symbol(&request.symbol.exchange_symbol.symbol)?,
        "side": gateio_side(request.side),
        "type": order_type,
        "amount": non_empty("quantity", &request.quantity)?,
    });
    if request.order_type != OrderType::Market {
        let price = request
            .price
            .as_deref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "gateio limit-style order requires price".to_string(),
            })?;
        body["price"] = Value::String(non_empty("price", price)?);
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["text"] = Value::String(gateio_client_text(client_order_id)?);
    }
    if let Some(time_in_force) = request.time_in_force {
        body["time_in_force"] = Value::String(gateio_time_in_force(time_in_force).to_string());
    }
    Ok(body)
}

fn is_gateio_position_not_found(error: &ExchangeApiError) -> bool {
    match error {
        ExchangeApiError::Exchange(error) => {
            error.code.as_deref() == Some("POSITION_NOT_FOUND")
                || error.class == ExchangeErrorClass::InsufficientPosition
                || error
                    .raw
                    .as_ref()
                    .and_then(|value| value.get("label"))
                    .and_then(Value::as_str)
                    == Some("POSITION_NOT_FOUND")
        }
        _ => false,
    }
}

fn gateio_futures_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    if request.quote_quantity.is_some() {
        return Err(ExchangeApiError::Unsupported {
            operation: "gateio.futures_quote_quantity",
        });
    }
    if matches!(
        request.order_type,
        OrderType::StopMarket | OrderType::StopLimit
    ) {
        return Err(ExchangeApiError::Unsupported {
            operation: "gateio.futures_stop_order",
        });
    }
    let mut body = json!({
        "contract": normalize_gateio_symbol(&request.symbol.exchange_symbol.symbol)?,
        "size": gateio_signed_size(request.side, &request.quantity)?,
        "tif": gateio_futures_tif(request),
        "reduce_only": request.reduce_only,
    });
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["text"] = Value::String(gateio_client_text(client_order_id)?);
    }
    if request.order_type == OrderType::Market {
        body["price"] = Value::String("0".to_string());
    } else {
        let price = request
            .price
            .as_deref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "gateio futures limit-style order requires price".to_string(),
            })?;
        body["price"] = Value::String(non_empty("price", price)?);
    }
    Ok(body)
}

fn gateio_quote_market_order_body(request: &QuoteMarketOrderRequest) -> ExchangeApiResult<Value> {
    if request.side != OrderSide::Buy {
        return Err(ExchangeApiError::Unsupported {
            operation: "gateio.quote_market_sell",
        });
    }
    let mut body = json!({
        "currency_pair": normalize_gateio_symbol(&request.symbol.exchange_symbol.symbol)?,
        "side": "buy",
        "type": "market",
        "amount": non_empty("quote_quantity", &request.quote_quantity)?,
        "time_in_force": "ioc",
    });
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["text"] = Value::String(gateio_client_text(client_order_id)?);
    }
    Ok(body)
}

fn gateio_amend_order_body(request: &AmendOrderRequest) -> ExchangeApiResult<Value> {
    if request
        .new_client_order_id
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
    {
        return Err(ExchangeApiError::Unsupported {
            operation: "gateio.amend_new_client_order_id",
        });
    }
    Ok(json!({
        "currency_pair": normalize_gateio_symbol(&request.symbol.exchange_symbol.symbol)?,
        "account": "spot",
        "amount": non_empty("new_quantity", &request.new_quantity)?,
    }))
}

fn gateio_client_text(client_order_id: &str) -> ExchangeApiResult<String> {
    let client_order_id = non_empty("client_order_id", client_order_id)?;
    Ok(if client_order_id.starts_with("t-") {
        client_order_id
    } else {
        format!("t-{client_order_id}")
    })
}

fn gateio_order_lookup_id(
    exchange_order_id: Option<&str>,
    client_order_id: Option<&str>,
    market_type: MarketType,
    client_id_operation: &'static str,
) -> ExchangeApiResult<String> {
    if let Some(order_id) = exchange_order_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Ok(order_id.to_string());
    }
    if let Some(client_order_id) = client_order_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if market_type == MarketType::Perpetual {
            return gateio_client_text(client_order_id);
        }
        return Err(ExchangeApiError::Unsupported {
            operation: client_id_operation,
        });
    }
    Err(ExchangeApiError::InvalidRequest {
        message: "gateio order lookup requires exchange_order_id or client_order_id".to_string(),
    })
}

fn gateio_futures_order_path(order_id: &str) -> String {
    format!("/futures/usdt/orders/{}", urlencoding::encode(order_id))
}

fn gateio_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn gateio_signed_size(side: OrderSide, quantity: &str) -> ExchangeApiResult<String> {
    let quantity = non_empty("quantity", quantity)?;
    let number = quantity
        .parse::<f64>()
        .map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("invalid gateio futures quantity `{quantity}`: {error}"),
        })?;
    if !number.is_finite() || number == 0.0 {
        return Err(ExchangeApiError::InvalidRequest {
            message: "gateio futures quantity must be non-zero and finite".to_string(),
        });
    }
    let unsigned = quantity
        .trim_start_matches('+')
        .trim_start_matches('-')
        .to_string();
    Ok(match side {
        OrderSide::Buy => unsigned,
        OrderSide::Sell => format!("-{unsigned}"),
    })
}

fn gateio_futures_tif(request: &PlaceOrderRequest) -> &'static str {
    if request.order_type == OrderType::Market {
        return match request.time_in_force {
            Some(TimeInForce::FOK) => "fok",
            _ => "ioc",
        };
    }
    if request.post_only || request.order_type == OrderType::PostOnly {
        return "poc";
    }
    if let Some(time_in_force) = request.time_in_force {
        return gateio_time_in_force(time_in_force);
    }
    match request.order_type {
        OrderType::IOC => "ioc",
        OrderType::FOK => "fok",
        _ => "gtc",
    }
}

fn gateio_time_in_force(time_in_force: TimeInForce) -> &'static str {
    match time_in_force {
        TimeInForce::GTC => "gtc",
        TimeInForce::IOC => "ioc",
        TimeInForce::FOK => "fok",
        TimeInForce::GTX => "poc",
    }
}

fn non_empty(field: &str, value: &str) -> ExchangeApiResult<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("gateio {field} must not be empty"),
        });
    }
    Ok(value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rustcta_exchange_api::{RequestContext, SymbolScope};
    use rustcta_types::{CanonicalSymbol, ExchangeError, ExchangeId, ExchangeSymbol, PositionSide};
    use serde_json::json;

    #[test]
    fn gateio_position_not_found_error_should_be_treated_as_empty_position() {
        let mut exchange_error = ExchangeError::new(
            ExchangeId::new("gateio").expect("gateio exchange"),
            ExchangeErrorClass::InsufficientPosition,
            "Gate.io request failed: HTTP 400; label=POSITION_NOT_FOUND",
            Utc::now(),
        );
        exchange_error.code = Some("POSITION_NOT_FOUND".to_string());
        exchange_error.raw = Some(json!({"label": "POSITION_NOT_FOUND"}));

        assert!(is_gateio_position_not_found(&ExchangeApiError::Exchange(
            exchange_error
        )));
    }

    #[test]
    fn gateio_futures_market_order_should_always_use_ioc_or_fok() {
        let mut request = gateio_futures_market_request(None);
        let body = gateio_futures_order_body(&request).expect("market body");
        assert_eq!(body["price"], "0");
        assert_eq!(body["tif"], "ioc");

        request.time_in_force = Some(TimeInForce::GTC);
        let body = gateio_futures_order_body(&request).expect("market body");
        assert_eq!(body["tif"], "ioc");

        request.time_in_force = Some(TimeInForce::FOK);
        let body = gateio_futures_order_body(&request).expect("market body");
        assert_eq!(body["tif"], "fok");
    }

    fn gateio_futures_market_request(time_in_force: Option<TimeInForce>) -> PlaceOrderRequest {
        let exchange = ExchangeId::new("gateio").expect("exchange id");
        PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: RequestContext::new(Utc::now()),
            symbol: SymbolScope {
                exchange: exchange.clone(),
                market_type: MarketType::Perpetual,
                canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDT").expect("symbol")),
                exchange_symbol: ExchangeSymbol::new(exchange, MarketType::Perpetual, "BTC_USDT")
                    .expect("exchange symbol"),
            },
            client_order_id: Some("cid-gateio-market".to_string()),
            side: OrderSide::Buy,
            position_side: Some(PositionSide::Long),
            order_type: OrderType::Market,
            time_in_force,
            quantity: "1".to_string(),
            price: None,
            quote_quantity: None,
            reduce_only: true,
            post_only: false,
        }
    }
}
