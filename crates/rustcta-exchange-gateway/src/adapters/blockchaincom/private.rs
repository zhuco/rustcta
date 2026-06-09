#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;

use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, ExchangeApiError, ExchangeApiResult, FeesRequest,
    FeesResponse, OpenOrdersRequest, OpenOrdersResponse, QueryOrderRequest, QueryOrderResponse,
    RecentFillsRequest, RecentFillsResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::{
    parse_blockchaincom_balances, parse_blockchaincom_fees, parse_blockchaincom_fills,
    parse_blockchaincom_order, parse_blockchaincom_order_list,
};
use super::transport::api_token_json_request_spec;
use super::BlockchainComGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const ACCOUNTS_PATH: &str = "/accounts";
pub const FEES_PATH: &str = "/fees";
pub const ORDERS_PATH: &str = "/orders";
pub const TRADES_PATH: &str = "/trades";
pub const FILLS_PATH: &str = "/fills";
pub const INTERNAL_ORDERS_PATH: &str = "/internal/orders";

pub fn balances_request_spec_fixture() -> Value {
    api_token_json_request_spec("GET", ACCOUNTS_PATH, json!({}), None)
}

impl BlockchainComGatewayAdapter {
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
            .send_api_token_get("blockchaincom.get_balances", ACCOUNTS_PATH, &HashMap::new())
            .await?;
        let balances = parse_blockchaincom_balances(
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
                message: "blockchaincom get_fees requires at least one symbol".to_string(),
            });
        }
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
        }
        let value = self
            .send_api_token_get("blockchaincom.get_fees", FEES_PATH, &HashMap::new())
            .await?;
        let fees = parse_blockchaincom_fees(&self.exchange_id, &request.symbols, &value)?;
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
        self.ensure_spot(request.symbol.market_type)?;
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "blockchaincom.query_order.client_order_id",
            });
        }
        let order_id = request.exchange_order_id.as_deref().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "blockchaincom query_order requires exchange_order_id".to_string(),
            }
        })?;
        let endpoint = format!("/orders/{}", urlencoding::encode(order_id));
        let value = self
            .send_api_token_get("blockchaincom.query_order", &endpoint, &HashMap::new())
            .await?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order: Some(parse_blockchaincom_order(
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
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "blockchaincom get_open_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        if let Some(page) = request.page.as_ref() {
            page.validate(Some(100))
                .map_err(|message| ExchangeApiError::InvalidRequest {
                    message: format!("blockchaincom get_open_orders {message}"),
                })?;
            if page.cursor.is_some() {
                return Err(ExchangeApiError::Unsupported {
                    operation: "blockchaincom.get_open_orders.cursor",
                });
            }
        }
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            symbol.exchange_symbol.symbol.to_ascii_uppercase(),
        );
        params.insert("status".to_string(), "OPEN".to_string());
        if let Some(limit) = request.page.as_ref().and_then(|page| page.limit) {
            params.insert("limit".to_string(), limit.to_string());
        }
        let value = self
            .send_api_token_get("blockchaincom.get_open_orders", ORDERS_PATH, &params)
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_blockchaincom_order_list(&self.exchange_id, Some(symbol), &value)?,
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
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "blockchaincom.get_recent_fills.client_order_id",
            });
        }
        if request.exchange_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "blockchaincom.get_recent_fills.exchange_order_id",
            });
        }
        if request.from_trade_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "blockchaincom.get_recent_fills.from_trade_id",
            });
        }
        if request.start_time.is_some() || request.end_time.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "blockchaincom.get_recent_fills.time_window",
            });
        }
        if request
            .page
            .as_ref()
            .is_some_and(|page| page.cursor.is_some())
        {
            return Err(ExchangeApiError::Unsupported {
                operation: "blockchaincom.get_recent_fills.cursor",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "blockchaincom get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let limit = request
            .limit
            .or_else(|| request.page.as_ref().and_then(|page| page.limit))
            .unwrap_or(100);
        if limit == 0 || limit > 100 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "blockchaincom get_recent_fills limit must be between 1 and 100"
                    .to_string(),
            });
        }
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            symbol.exchange_symbol.symbol.to_ascii_uppercase(),
        );
        params.insert("limit".to_string(), limit.to_string());
        let value = self
            .send_api_token_get("blockchaincom.get_recent_fills", FILLS_PATH, &params)
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_blockchaincom_fills(
                &self.exchange_id,
                tenant_id,
                account_id,
                Some(symbol),
                &value,
            )?,
        })
    }
}

pub fn fees_request_spec_fixture() -> Value {
    api_token_json_request_spec("GET", FEES_PATH, json!({}), None)
}

pub fn open_orders_request_spec_fixture() -> Value {
    api_token_json_request_spec(
        "GET",
        ORDERS_PATH,
        json!({
            "symbol": "BTC-USD",
            "status": "OPEN"
        }),
        None,
    )
}

pub fn query_order_request_spec_fixture() -> Value {
    api_token_json_request_spec("GET", "/orders/{orderId}", json!({}), None)
}

pub fn recent_fills_request_spec_fixture() -> Value {
    api_token_json_request_spec(
        "GET",
        FILLS_PATH,
        json!({
            "symbol": "BTC-USD",
            "limit": "100"
        }),
        None,
    )
}

pub fn place_order_request_spec_fixture() -> Value {
    api_token_json_request_spec(
        "POST",
        ORDERS_PATH,
        json!({}),
        Some(json!({
            "clOrdId": "rustcta-fixture-0001",
            "symbol": "BTC-USD",
            "side": "BUY",
            "ordType": "LIMIT",
            "timeInForce": "GTC",
            "orderQty": "0.01",
            "price": "65000"
        })),
    )
}

pub fn cancel_order_request_spec_fixture() -> Value {
    api_token_json_request_spec("DELETE", "/orders/{orderId}", json!({}), None)
}

pub fn cancel_all_orders_request_spec_fixture() -> Value {
    api_token_json_request_spec(
        "DELETE",
        ORDERS_PATH,
        json!({
            "symbol": "BTC-USD"
        }),
        None,
    )
}
