use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse,
    OpenOrdersRequest, OpenOrdersResponse, OrderListRequest, OrderListResponse, PlaceOrderRequest,
    PlaceOrderResponse, PositionsRequest, PositionsResponse, QueryOrderRequest, QueryOrderResponse,
    QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderType};

use super::parser::{independentreserve_asset, split_symbol_assets};
use super::private_parser::{
    order_state_from_cancel_ack, parse_balances, parse_fee_snapshots, parse_fills, parse_order,
    parse_orders,
};
use super::transport::IndependentReserveEndpointVersion;
use super::IndependentReserveGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl IndependentReserveGatewayAdapter {
    pub(super) async fn get_balances_private_rest(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_private_rest("independentreserve.get_balances")?;
        if request
            .market_type
            .is_some_and(|market_type| market_type != MarketType::Spot)
        {
            return self.unsupported_private("independentreserve.non_spot_balances");
        }
        let (tenant_id, account_id) =
            self.context_account(&request.context, "independentreserve.get_balances")?;
        let value = self
            .rest
            .send_private_post(
                IndependentReserveEndpointVersion::V1,
                "/Private/GetAccounts",
                &[],
            )
            .await?;
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange.clone(), request.context.request_id),
            balances: parse_balances(
                &self.exchange_id,
                tenant_id,
                account_id,
                &request.assets,
                &value,
            )?,
        })
    }

    pub(super) async fn get_positions_private_rest(
        &self,
        _request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        self.unsupported_private("independentreserve.spot_has_no_positions")
    }

    pub(super) async fn get_fees_private_rest(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "independentreserve.get_fees requires at least one symbol".to_string(),
            });
        }
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
            self.ensure_supported_quote(&symbol.exchange_symbol.symbol)?;
        }
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fees: parse_fee_snapshots(&request.symbols),
        })
    }

    pub(super) async fn place_order_private_rest(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.ensure_supported_quote(&request.symbol.exchange_symbol.symbol)?;
        self.ensure_private_rest("independentreserve.place_order")?;
        let params = independentreserve_place_order_params(&request)?;
        let endpoint = match request.order_type {
            OrderType::Market => "/Private/PlaceMarketOrder",
            OrderType::Limit => "/Private/PlaceLimitOrder",
            OrderType::IOC | OrderType::FOK | OrderType::PostOnly => {
                return self.unsupported_private("independentreserve.unsupported_order_type")
            }
            OrderType::StopMarket | OrderType::StopLimit => {
                return self.unsupported_private("independentreserve.stop_orders")
            }
        };
        let value = self
            .rest
            .send_private_post(IndependentReserveEndpointVersion::V1, endpoint, &params)
            .await?;
        let order =
            parse_order(&self.exchange_id, Some(&request.symbol), &value)?.ok_or_else(|| {
                ExchangeApiError::Exchange(rustcta_types::ExchangeError::new(
                    self.exchange_id.clone(),
                    rustcta_types::ExchangeErrorClass::UnknownOrderState,
                    "Independent Reserve place order response did not include order state",
                    chrono::Utc::now(),
                ))
            })?;
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order,
        })
    }

    pub(super) async fn place_quote_market_order_private_rest(
        &self,
        _request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.unsupported_private("independentreserve.quote_market_order")
    }

    pub(super) async fn cancel_order_private_rest(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.ensure_supported_quote(&request.symbol.exchange_symbol.symbol)?;
        self.ensure_private_rest("independentreserve.cancel_order")?;
        let order_id =
            request
                .exchange_order_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "independentreserve.cancel_order requires exchange_order_id/orderGuid"
                        .to_string(),
                })?;
        let params = vec![("orderGuid".to_string(), order_id.clone())];
        let value = self
            .rest
            .send_private_post(
                IndependentReserveEndpointVersion::V1,
                "/Private/CancelOrder",
                &params,
            )
            .await?;
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order: order_state_from_cancel_ack(
                &self.exchange_id,
                &request.symbol,
                Some(&value),
                Some(order_id),
                request.client_order_id,
            )?,
            cancelled: true,
        })
    }

    pub(super) async fn amend_order_private_rest(
        &self,
        _request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.unsupported_private("independentreserve.amend_order")
    }

    pub(super) async fn place_order_list_private_rest(
        &self,
        _request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        self.unsupported_private("independentreserve.order_list")
    }

    pub(super) async fn batch_place_orders_private_rest(
        &self,
        _request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        self.unsupported_private("independentreserve.batch_place_orders")
    }

    pub(super) async fn batch_cancel_orders_private_rest(
        &self,
        _request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        self.unsupported_private("independentreserve.batch_cancel_orders")
    }

    pub(super) async fn cancel_all_orders_private_rest(
        &self,
        _request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        self.unsupported_private("independentreserve.cancel_all_orders")
    }

    pub(super) async fn query_order_private_rest(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.ensure_supported_quote(&request.symbol.exchange_symbol.symbol)?;
        self.ensure_private_rest("independentreserve.query_order")?;
        let order_id =
            request
                .exchange_order_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "independentreserve.query_order requires exchange_order_id/orderGuid"
                        .to_string(),
                })?;
        let params = vec![("orderGuid".to_string(), order_id)];
        let value = self
            .rest
            .send_private_post(
                IndependentReserveEndpointVersion::V2,
                "/Private/GetOrderDetails",
                &params,
            )
            .await?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order: parse_order(&self.exchange_id, Some(&request.symbol), &value)?,
        })
    }

    pub(super) async fn get_open_orders_private_rest(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_private_rest("independentreserve.get_open_orders")?;
        if request
            .market_type
            .is_some_and(|market_type| market_type != MarketType::Spot)
        {
            return self.unsupported_private("independentreserve.non_spot_open_orders");
        }
        let mut params = Vec::new();
        let fallback_symbol = if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
            self.ensure_supported_quote(&symbol.exchange_symbol.symbol)?;
            let (base, quote) = split_symbol_assets(&symbol.exchange_symbol.symbol);
            params.push((
                "primaryCurrencyCode".to_string(),
                independentreserve_asset(&base),
            ));
            params.push((
                "secondaryCurrencyCode".to_string(),
                independentreserve_asset(&quote),
            ));
            Some(symbol.clone())
        } else {
            None
        };
        let value = self
            .rest
            .send_private_post(
                IndependentReserveEndpointVersion::V1,
                "/Private/GetOpenOrders",
                &params,
            )
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_orders(&self.exchange_id, fallback_symbol.as_ref(), &value)?,
        })
    }

    pub(super) async fn get_recent_fills_private_rest(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_private_rest("independentreserve.get_recent_fills")?;
        if request
            .market_type
            .is_some_and(|market_type| market_type != MarketType::Spot)
        {
            return self.unsupported_private("independentreserve.non_spot_recent_fills");
        }
        let (tenant_id, account_id) =
            self.context_account(&request.context, "independentreserve.get_recent_fills")?;
        let mut params = Vec::new();
        let fallback_symbol = if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
            self.ensure_supported_quote(&symbol.exchange_symbol.symbol)?;
            let (base, quote) = split_symbol_assets(&symbol.exchange_symbol.symbol);
            params.push((
                "primaryCurrencyCode".to_string(),
                independentreserve_asset(&base),
            ));
            params.push((
                "secondaryCurrencyCode".to_string(),
                independentreserve_asset(&quote),
            ));
            Some(symbol.clone())
        } else {
            None
        };
        if let Some(start_time) = request.start_time {
            params.push(("fromTimestampUtc".to_string(), start_time.to_rfc3339()));
        }
        if let Some(end_time) = request.end_time {
            params.push(("toTimestampUtc".to_string(), end_time.to_rfc3339()));
        }
        if let Some(limit) = request.limit {
            params.push(("pageSize".to_string(), limit.min(100).to_string()));
        }
        if let Some(order_id) = request.exchange_order_id {
            params.push(("orderGuid".to_string(), order_id));
        }
        let value = self
            .rest
            .send_private_post(
                IndependentReserveEndpointVersion::V1,
                "/Private/GetTrades",
                &params,
            )
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_fills(
                &self.exchange_id,
                tenant_id,
                account_id,
                fallback_symbol.as_ref(),
                &value,
            )?,
        })
    }
}

fn independentreserve_place_order_params(
    request: &PlaceOrderRequest,
) -> ExchangeApiResult<Vec<(String, String)>> {
    if request.reduce_only {
        return Err(ExchangeApiError::Unsupported {
            operation: "independentreserve.reduce_only",
        });
    }
    if request.post_only {
        return Err(ExchangeApiError::Unsupported {
            operation: "independentreserve.post_only",
        });
    }
    if request
        .time_in_force
        .is_some_and(|time_in_force| time_in_force != TimeInForce::GTC)
    {
        return Err(ExchangeApiError::Unsupported {
            operation: "independentreserve.non_gtc_time_in_force",
        });
    }
    let (base, quote) = split_symbol_assets(&request.symbol.exchange_symbol.symbol);
    let mut params = vec![
        (
            "primaryCurrencyCode".to_string(),
            independentreserve_asset(&base),
        ),
        (
            "secondaryCurrencyCode".to_string(),
            independentreserve_asset(&quote),
        ),
        ("volume".to_string(), request.quantity.clone()),
    ];
    if request.order_type == OrderType::Limit {
        params.push((
            "orderType".to_string(),
            match request.side {
                OrderSide::Buy => "LimitBid",
                OrderSide::Sell => "LimitOffer",
            }
            .to_string(),
        ));
        params.push((
            "price".to_string(),
            request
                .price
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "independentreserve limit order requires price".to_string(),
                })?,
        ));
    } else {
        params.push((
            "orderType".to_string(),
            match request.side {
                OrderSide::Buy => "MarketBid",
                OrderSide::Sell => "MarketOffer",
            }
            .to_string(),
        ));
    }
    if let Some(client_order_id) = request.client_order_id.clone() {
        params.push(("clientOrderIdentifier".to_string(), client_order_id));
    }
    Ok(params)
}

#[cfg(test)]
mod tests {
    use rustcta_exchange_api::{PlaceOrderRequest, RequestContext, EXCHANGE_API_SCHEMA_VERSION};
    use rustcta_types::{ExchangeId, MarketType, OrderSide, OrderType};

    use super::independentreserve_place_order_params;
    use crate::adapters::independentreserve::parser::symbol_scope;

    #[test]
    fn independentreserve_limit_order_params_should_use_fiat_market_and_client_id() {
        let exchange = ExchangeId::new("independentreserve").expect("exchange");
        let symbol = symbol_scope(&exchange, "BTC_SGD").expect("symbol");
        let request = PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: RequestContext::new(chrono::Utc::now()),
            symbol,
            client_order_id: Some("client-1".to_string()),
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: None,
            quantity: "0.1".to_string(),
            price: Some("100000".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: false,
        };

        let params = independentreserve_place_order_params(&request).expect("params");

        assert!(params.contains(&("primaryCurrencyCode".to_string(), "Xbt".to_string())));
        assert!(params.contains(&("secondaryCurrencyCode".to_string(), "Sgd".to_string())));
        assert!(params.contains(&("clientOrderIdentifier".to_string(), "client-1".to_string())));
        assert_eq!(request.symbol.market_type, MarketType::Spot);
    }
}
