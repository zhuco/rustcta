use std::collections::HashMap;

use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, CancelAllOrdersRequest, CancelAllOrdersResponse,
    CancelOrderRequest, CancelOrderResponse, ExchangeApiError, ExchangeApiResult,
    OpenOrdersRequest, OpenOrdersResponse, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest,
    PositionsResponse, QueryOrderRequest, QueryOrderResponse, QuoteMarketOrderRequest,
    RecentFillsRequest, RecentFillsResponse, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{OrderStatus, OrderType};
use serde_json::{json, Value};

use super::parser::normalize_alpaca_symbol;
use super::private_parser::{
    cancelled_order, parse_account_balance, parse_cancel_all_count, parse_order_state,
    parse_orders, parse_positions,
};
use super::AlpacaGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl AlpacaGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        self.ensure_private_rest("alpaca.get_balances")?;
        let (tenant_id, account_id) = self.context_identity(&request.context)?;
        let endpoint = format!("/v1/trading/accounts/{account_id}/account");
        let value = self
            .rest
            .send_broker_get(
                &endpoint,
                &HashMap::new(),
                &self.config.api_key,
                &self.config.api_secret,
            )
            .await?;
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            balances: vec![parse_account_balance(
                &self.exchange_id,
                tenant_id,
                account_id,
                &request.assets,
                &value,
            )?],
        })
    }

    pub(super) async fn get_positions_impl(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange_id)?;
            self.ensure_spot(symbol.market_type)?;
        }
        self.ensure_private_rest("alpaca.get_positions")?;
        let (tenant_id, account_id) = self.context_identity(&request.context)?;
        let endpoint = format!("/v1/trading/accounts/{account_id}/positions");
        let value = self
            .rest
            .send_broker_get(
                &endpoint,
                &HashMap::new(),
                &self.config.api_key,
                &self.config.api_secret,
            )
            .await?;
        Ok(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            positions: parse_positions(
                &self.exchange_id,
                tenant_id,
                account_id,
                &request.symbols,
                &value,
            )?,
        })
    }

    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.ensure_private_rest("alpaca.place_order")?;
        let (_tenant_id, account_id) = self.context_identity(&request.context)?;
        let body = alpaca_order_body(&request)?;
        let endpoint = format!("/v1/trading/accounts/{account_id}/orders");
        let value = self
            .rest
            .send_broker_post(
                &endpoint,
                &HashMap::new(),
                &body,
                &self.config.api_key,
                &self.config.api_secret,
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

    pub(super) async fn place_quote_market_order_impl(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        let place = PlaceOrderRequest {
            schema_version: request.schema_version,
            context: request.context,
            symbol: request.symbol,
            client_order_id: request.client_order_id,
            side: request.side,
            position_side: None,
            order_type: OrderType::Market,
            time_in_force: Some(TimeInForce::GTC),
            quantity: "0".to_string(),
            price: None,
            quote_quantity: Some(request.quote_quantity),
            reduce_only: false,
            post_only: false,
        };
        self.place_order_impl(place).await
    }

    pub(super) async fn cancel_order_impl(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.ensure_private_rest("alpaca.cancel_order")?;
        let (_tenant_id, account_id) = self.context_identity(&request.context)?;
        let order_id =
            request
                .exchange_order_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "alpaca cancel_order requires exchange_order_id".to_string(),
                })?;
        let endpoint = format!("/v1/trading/accounts/{account_id}/orders/{order_id}");
        let value = self
            .rest
            .send_broker_delete(
                &endpoint,
                &HashMap::new(),
                &self.config.api_key,
                &self.config.api_secret,
            )
            .await?;
        let order = if value.is_null() {
            cancelled_order(&self.exchange_id, &request)
        } else {
            parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
                .unwrap_or_else(|_| cancelled_order(&self.exchange_id, &request))
        };
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
        if request.symbol.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "alpaca.symbol_scoped_cancel_all_not_native",
            });
        }
        self.ensure_private_rest("alpaca.cancel_all_orders")?;
        let (_tenant_id, account_id) = self.context_identity(&request.context)?;
        let endpoint = format!("/v1/trading/accounts/{account_id}/orders");
        let value = self
            .rest
            .send_broker_delete(
                &endpoint,
                &HashMap::new(),
                &self.config.api_key,
                &self.config.api_secret,
            )
            .await?;
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: Vec::new(),
            cancelled_count: parse_cancel_all_count(&value),
        })
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.ensure_private_rest("alpaca.query_order")?;
        let (_tenant_id, account_id) = self.context_identity(&request.context)?;
        let mut params = HashMap::new();
        let endpoint = if let Some(order_id) = &request.exchange_order_id {
            format!("/v1/trading/accounts/{account_id}/orders/{order_id}")
        } else if let Some(client_order_id) = &request.client_order_id {
            params.insert("client_order_id".to_string(), client_order_id.clone());
            format!("/v1/trading/accounts/{account_id}/orders:by_client_order_id")
        } else {
            return Err(ExchangeApiError::InvalidRequest {
                message: "alpaca query_order requires exchange_order_id or client_order_id"
                    .to_string(),
            });
        };
        let value = self
            .rest
            .send_broker_get(
                &endpoint,
                &params,
                &self.config.api_key,
                &self.config.api_secret,
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
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
        }
        self.ensure_private_rest("alpaca.get_open_orders")?;
        let (_tenant_id, account_id) = self.context_identity(&request.context)?;
        let endpoint = format!("/v1/trading/accounts/{account_id}/orders");
        let mut params = HashMap::new();
        params.insert("status".to_string(), "open".to_string());
        params.insert("asset_class".to_string(), "crypto".to_string());
        if let Some(limit) = request.page.as_ref().and_then(|page| page.limit) {
            params.insert("limit".to_string(), limit.min(500).to_string());
        }
        if let Some(symbol) = &request.symbol {
            params.insert(
                "symbols".to_string(),
                normalize_alpaca_symbol(&symbol.exchange_symbol.symbol)?.replace('/', ""),
            );
        }
        let value = self
            .rest
            .send_broker_get(
                &endpoint,
                &params,
                &self.config.api_key,
                &self.config.api_secret,
            )
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_orders(&self.exchange_id, request.symbol.as_ref(), &value)?,
        })
    }

    pub(super) async fn get_recent_fills_impl(
        &self,
        _request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "alpaca.recent_fills_rest_not_verified_for_broker_account",
        })
    }
}

pub fn alpaca_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    if request.reduce_only {
        return Err(ExchangeApiError::Unsupported {
            operation: "alpaca.reduce_only_unsupported_spot",
        });
    }
    if request.post_only || request.order_type == OrderType::PostOnly {
        return Err(ExchangeApiError::Unsupported {
            operation: "alpaca.post_only_unsupported_crypto",
        });
    }
    if request.order_type == OrderType::StopLimit || request.order_type == OrderType::StopMarket {
        return Err(ExchangeApiError::Unsupported {
            operation: "alpaca.stop_orders_not_mapped_to_shared_request",
        });
    }
    if matches!(
        request.time_in_force,
        Some(TimeInForce::FOK | TimeInForce::GTX)
    ) {
        return Err(ExchangeApiError::Unsupported {
            operation: "alpaca.time_in_force_unsupported_crypto",
        });
    }
    let mut body = serde_json::Map::new();
    body.insert(
        "symbol".to_string(),
        json!(normalize_alpaca_symbol(
            &request.symbol.exchange_symbol.symbol
        )?),
    );
    body.insert(
        "side".to_string(),
        json!(match request.side {
            rustcta_types::OrderSide::Buy => "buy",
            rustcta_types::OrderSide::Sell => "sell",
        }),
    );
    body.insert(
        "time_in_force".to_string(),
        json!(match request.time_in_force.unwrap_or(TimeInForce::GTC) {
            TimeInForce::IOC => "ioc",
            _ => "gtc",
        }),
    );
    if let Some(client_order_id) = &request.client_order_id {
        body.insert("client_order_id".to_string(), json!(client_order_id));
    }
    if let Some(notional) = &request.quote_quantity {
        if request.order_type != OrderType::Market {
            return Err(ExchangeApiError::Unsupported {
                operation: "alpaca.notional_only_supported_for_market_orders",
            });
        }
        body.insert("type".to_string(), json!("market"));
        body.insert("notional".to_string(), json!(notional));
    } else {
        body.insert("qty".to_string(), json!(request.quantity));
        match request.order_type {
            OrderType::Market => {
                body.insert("type".to_string(), json!("market"));
            }
            OrderType::Limit | OrderType::IOC | OrderType::FOK => {
                let price =
                    request
                        .price
                        .as_ref()
                        .ok_or_else(|| ExchangeApiError::InvalidRequest {
                            message: "alpaca limit order requires price".to_string(),
                        })?;
                body.insert("type".to_string(), json!("limit"));
                body.insert("limit_price".to_string(), json!(price));
            }
            _ => unreachable!("unsupported order type returned earlier"),
        }
    }
    Ok(Value::Object(body))
}

pub fn cancelled_state_for_id(
    adapter: &AlpacaGatewayAdapter,
    symbol: &rustcta_exchange_api::SymbolScope,
    order_id: String,
) -> rustcta_exchange_api::OrderState {
    super::parser::empty_order_state(
        &adapter.exchange_id,
        symbol,
        Some(order_id),
        None,
        OrderStatus::Cancelled,
    )
}
