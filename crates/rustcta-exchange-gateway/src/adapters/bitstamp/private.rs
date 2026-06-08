use std::collections::HashMap;

use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse,
    ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse, OpenOrdersRequest,
    OpenOrdersResponse, PlaceOrderRequest, PlaceOrderResponse, QueryOrderRequest,
    QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse,
    TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{OrderSide, OrderType};

use super::parser::{
    normalize_market_symbol, parse_balances, parse_fees, parse_fills, parse_order_list,
    parse_order_state,
};
use super::BitstampGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl BitstampGatewayAdapter {
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
            .send_private_post(
                "bitstamp.get_balances",
                "/api/v2/account_balances/",
                &HashMap::new(),
            )
            .await?;
        let balances = parse_balances(
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
                message: "bitstamp get_fees requires at least one symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            let endpoint = format!(
                "/api/v2/fees/trading/{}/",
                normalize_market_symbol(&symbol.exchange_symbol.symbol)
            );
            let value = self
                .send_private_post("bitstamp.get_fees", &endpoint, &HashMap::new())
                .await?;
            fees.extend(parse_fees(
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
        self.ensure_spot(request.symbol.market_type)?;
        let market = normalize_market_symbol(&request.symbol.exchange_symbol.symbol);
        let side = bitstamp_side(request.side);
        let endpoint = if request.order_type == OrderType::Market {
            format!("/api/v2/{side}/market/{market}/")
        } else {
            format!("/api/v2/{side}/{market}/")
        };
        let mut params = HashMap::new();
        params.insert("amount".to_string(), request.quantity.clone());
        if request.order_type != OrderType::Market {
            params.insert(
                "price".to_string(),
                request
                    .price
                    .clone()
                    .ok_or_else(|| ExchangeApiError::InvalidRequest {
                        message: "bitstamp limit order requires price".to_string(),
                    })?,
            );
        }
        if let Some(client_order_id) = request.client_order_id.as_deref() {
            params.insert("client_order_id".to_string(), client_order_id.to_string());
        }
        if request.time_in_force == Some(TimeInForce::IOC) || request.order_type == OrderType::IOC {
            params.insert("ioc_order".to_string(), "true".to_string());
        } else if request.time_in_force == Some(TimeInForce::FOK)
            || request.order_type == OrderType::FOK
        {
            params.insert("fok_order".to_string(), "true".to_string());
        } else if request.post_only || request.order_type == OrderType::PostOnly {
            params.insert("moc_order".to_string(), "true".to_string());
        }
        let value = self
            .send_private_post("bitstamp.place_order", &endpoint, &params)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?;
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
                operation: "bitstamp.sell_quote_market_order",
            });
        }
        let market = normalize_market_symbol(&request.symbol.exchange_symbol.symbol);
        let endpoint = format!("/api/v2/buy/market/{market}/");
        let mut params = HashMap::new();
        params.insert("amount".to_string(), request.quote_quantity.clone());
        if let Some(client_order_id) = request.client_order_id.as_deref() {
            params.insert("client_order_id".to_string(), client_order_id.to_string());
        }
        let value = self
            .send_private_post("bitstamp.place_quote_market_order", &endpoint, &params)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?;
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
        self.ensure_spot(request.symbol.market_type)?;
        let mut params = order_identity_params(
            request.exchange_order_id.as_deref(),
            request.client_order_id.as_deref(),
            "cancel_order",
        )?;
        let value = self
            .send_private_post("bitstamp.cancel_order", "/api/v2/cancel_order/", &params)
            .await?;
        if value.as_bool() == Some(true) {
            params.insert("status".to_string(), "Canceled".to_string());
        }
        let order_value = if value.as_object().is_some() {
            value
        } else {
            serde_json::json!(params)
        };
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &order_value)?;
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
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        let endpoint = if let Some(symbol) = request.symbol.as_ref() {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            format!(
                "/api/v2/cancel_all_orders/{}/",
                normalize_market_symbol(&symbol.exchange_symbol.symbol)
            )
        } else {
            "/api/v2/cancel_all_orders/".to_string()
        };
        let value = self
            .send_private_post("bitstamp.cancel_all_orders", &endpoint, &HashMap::new())
            .await?;
        let orders = if value.as_array().is_some() {
            parse_order_list(&self.exchange_id, request.symbol.as_ref(), &value)?
        } else {
            Vec::new()
        };
        let cancelled_count = orders.len() as u32;
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
        self.ensure_spot(request.symbol.market_type)?;
        let mut params = order_identity_params(
            request.exchange_order_id.as_deref(),
            request.client_order_id.as_deref(),
            "amend_order",
        )?;
        params.insert("amount".to_string(), request.new_quantity.clone());
        if let Some(new_client_order_id) = request.new_client_order_id.as_deref() {
            params.insert(
                "new_client_order_id".to_string(),
                new_client_order_id.to_string(),
            );
        }
        let value = self
            .send_private_post("bitstamp.amend_order", "/api/v2/replace_order/", &params)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?;
        Ok(AmendOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let mut params = order_identity_params(
            request.exchange_order_id.as_deref(),
            request.client_order_id.as_deref(),
            "query_order",
        )?;
        params.insert("omit_transactions".to_string(), "false".to_string());
        let value = self
            .send_private_post("bitstamp.query_order", "/api/v2/order_status/", &params)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?;
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
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        let endpoint = if let Some(symbol) = request.symbol.as_ref() {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            format!(
                "/api/v2/open_orders/{}/",
                normalize_market_symbol(&symbol.exchange_symbol.symbol)
            )
        } else {
            "/api/v2/open_orders/".to_string()
        };
        let value = self
            .send_private_post("bitstamp.get_open_orders", &endpoint, &HashMap::new())
            .await?;
        let orders = parse_order_list(&self.exchange_id, request.symbol.as_ref(), &value)?;
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
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitstamp get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let endpoint = format!(
            "/api/v2/user_transactions/{}/",
            normalize_market_symbol(&symbol.exchange_symbol.symbol)
        );
        let mut params = HashMap::new();
        if let Some(limit) = request.limit {
            params.insert("limit".to_string(), limit.min(1000).to_string());
        }
        if let Some(from_trade_id) = request.from_trade_id.as_deref() {
            params.insert("since_id".to_string(), from_trade_id.to_string());
        }
        if let Some(start_time) = request.start_time {
            params.insert(
                "since_timestamp".to_string(),
                start_time.timestamp().to_string(),
            );
        }
        if let Some(end_time) = request.end_time {
            params.insert(
                "until_timestamp".to_string(),
                end_time.timestamp().to_string(),
            );
        }
        let value = self
            .send_private_post("bitstamp.get_recent_fills", &endpoint, &params)
            .await?;
        let fills = parse_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
        })
    }
}

fn order_identity_params(
    exchange_order_id: Option<&str>,
    client_order_id: Option<&str>,
    operation: &'static str,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    if let Some(exchange_order_id) = exchange_order_id {
        params.insert("id".to_string(), exchange_order_id.to_string());
    }
    if let Some(client_order_id) = client_order_id {
        params.insert("client_order_id".to_string(), client_order_id.to_string());
    }
    if params.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("bitstamp {operation} requires id or client_order_id"),
        });
    }
    Ok(params)
}

fn bitstamp_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}
