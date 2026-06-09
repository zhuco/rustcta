use std::collections::HashMap;

use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, OpenOrdersRequest, OpenOrdersResponse,
    PageCursor, PlaceOrderRequest, QueryOrderRequest, QueryOrderResponse, RecentFillsRequest,
    RecentFillsResponse, RequestContext, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{OrderSide, OrderType};
use serde_json::{json, Value};

use super::parser::{
    normalize_bitteam_symbol, parse_private_open_orders, parse_private_order_state,
    parse_private_recent_fills,
};
use super::signing::basic_authorization_header;
use super::BitteamGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const BITTEAM_BALANCE_PATH: &str = "/trade/api/ccxt/balance";
pub const BITTEAM_QUERY_ORDER_PATH: &str = "/trade/api/ccxt/order";
pub const BITTEAM_PLACE_ORDER_PATH: &str = "/trade/api/ccxt/ordercreate";
pub const BITTEAM_CANCEL_ORDER_PATH: &str = "/trade/api/ccxt/cancelorder";
pub const BITTEAM_OPEN_ORDERS_PATH: &str = "/trade/api/ccxt/ordersOfUser";
pub const BITTEAM_RECENT_FILLS_PATH: &str = "/trade/api/ccxt/tradesOfUser";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitteamPrivateRequestSpec {
    pub method: &'static str,
    pub path: &'static str,
    pub authorization: String,
    pub body: Option<Value>,
}

pub fn build_basic_private_request_spec(
    method: &'static str,
    path: &'static str,
    body: Option<Value>,
    api_key: &str,
    api_secret: &str,
) -> ExchangeApiResult<BitteamPrivateRequestSpec> {
    Ok(BitteamPrivateRequestSpec {
        method,
        path,
        authorization: basic_authorization_header(api_key, api_secret)?,
        body,
    })
}

pub fn request_spec_body_from_order(request: &PlaceOrderRequest, pair_id: u64) -> Value {
    json!({
        "pairId": pair_id,
        "side": order_side_as_str(request.side),
        "type": order_type_as_str(request.order_type),
        "amount": request.quantity,
        "price": request.price.clone().unwrap_or_default()
    })
}

impl BitteamGatewayAdapter {
    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitteam.query_order.client_order_id",
            });
        }
        let order_id = request.exchange_order_id.as_deref().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "bitteam query_order requires exchange_order_id".to_string(),
            }
        })?;
        let authorization = self.private_authorization("bitteam.query_order")?;
        let value = self
            .rest
            .send_private_get(
                BITTEAM_QUERY_ORDER_PATH,
                &query_order_params(&request.symbol, order_id)?,
                &authorization,
            )
            .await?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order: Some(parse_private_order_state(
                &self.exchange_id,
                &request.symbol,
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
                message: "bitteam get_open_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let authorization = self.private_authorization("bitteam.get_open_orders")?;
        let value = self
            .rest
            .send_private_get(
                BITTEAM_OPEN_ORDERS_PATH,
                &open_orders_params(symbol, request.page.as_ref())?,
                &authorization,
            )
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_private_open_orders(&self.exchange_id, symbol, &value)?,
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
                operation: "bitteam.get_recent_fills.client_order_id",
            });
        }
        if request.start_time.is_some() || request.end_time.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitteam.get_recent_fills.time_window",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitteam get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (tenant_id, account_id) =
            context_account("bitteam.get_recent_fills", &request.context)?;
        let authorization = self.private_authorization("bitteam.get_recent_fills")?;
        let value = self
            .rest
            .send_private_get(
                BITTEAM_RECENT_FILLS_PATH,
                &recent_fills_params(
                    symbol,
                    request.exchange_order_id.as_deref(),
                    request.from_trade_id.as_deref(),
                    request
                        .limit
                        .or_else(|| request.page.as_ref().and_then(|page| page.limit)),
                    request.page.as_ref(),
                )?,
                &authorization,
            )
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_private_recent_fills(
                &self.exchange_id,
                tenant_id,
                account_id,
                symbol,
                &value,
            )?,
        })
    }

    fn private_authorization(&self, operation: &'static str) -> ExchangeApiResult<String> {
        if !self.config.private_rest_configured() {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        basic_authorization_header(&self.config.api_key, &self.config.api_secret)
    }
}

fn query_order_params(
    symbol: &rustcta_exchange_api::SymbolScope,
    exchange_order_id: &str,
) -> ExchangeApiResult<HashMap<String, String>> {
    let order_id = exchange_order_id.trim();
    if order_id.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bitteam query_order requires non-empty exchange_order_id".to_string(),
        });
    }
    let mut params = symbol_params(symbol)?;
    params.insert("orderId".to_string(), order_id.to_string());
    Ok(params)
}

fn open_orders_params(
    symbol: &rustcta_exchange_api::SymbolScope,
    page: Option<&rustcta_exchange_api::PageRequest>,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = paged_symbol_params(symbol, page)?;
    params.insert("type".to_string(), "active".to_string());
    Ok(params)
}

fn recent_fills_params(
    symbol: &rustcta_exchange_api::SymbolScope,
    exchange_order_id: Option<&str>,
    from_trade_id: Option<&str>,
    limit: Option<u32>,
    page: Option<&rustcta_exchange_api::PageRequest>,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = paged_symbol_params(symbol, page)?;
    if let Some(limit) = limit {
        params.insert("limit".to_string(), limit.min(1000).to_string());
    }
    if let Some(order_id) = exchange_order_id.filter(|value| !value.trim().is_empty()) {
        params.insert("orderId".to_string(), order_id.trim().to_string());
    }
    if let Some(trade_id) = from_trade_id.filter(|value| !value.trim().is_empty()) {
        params.insert("tradeId".to_string(), trade_id.trim().to_string());
    }
    Ok(params)
}

fn paged_symbol_params(
    symbol: &rustcta_exchange_api::SymbolScope,
    page: Option<&rustcta_exchange_api::PageRequest>,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = symbol_params(symbol)?;
    params.insert(
        "limit".to_string(),
        page.and_then(|page| page.limit)
            .unwrap_or(1000)
            .min(1000)
            .to_string(),
    );
    params.insert("offset".to_string(), page_offset(page)?.to_string());
    Ok(params)
}

fn symbol_params(
    symbol: &rustcta_exchange_api::SymbolScope,
) -> ExchangeApiResult<HashMap<String, String>> {
    Ok(HashMap::from([(
        "pair".to_string(),
        normalize_bitteam_symbol(&symbol.exchange_symbol.symbol)?,
    )]))
}

fn page_offset(page: Option<&rustcta_exchange_api::PageRequest>) -> ExchangeApiResult<u64> {
    match page.and_then(|page| page.cursor.as_ref()) {
        Some(PageCursor::Offset { offset }) => Ok(*offset),
        Some(_) => Err(ExchangeApiError::Unsupported {
            operation: "bitteam.offset_pagination_only",
        }),
        None => Ok(0),
    }
}

fn context_account(
    operation: &'static str,
    context: &RequestContext,
) -> ExchangeApiResult<(TenantId, AccountId)> {
    let tenant_id = context
        .tenant_id
        .clone()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("{operation} requires context.tenant_id"),
        })?;
    let account_id =
        context
            .account_id
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: format!("{operation} requires context.account_id"),
            })?;
    Ok((tenant_id, account_id))
}

fn order_side_as_str(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn order_type_as_str(order_type: OrderType) -> &'static str {
    match order_type {
        OrderType::Market => "market",
        OrderType::Limit => "limit",
        OrderType::PostOnly => "limit",
        OrderType::IOC => "limit",
        OrderType::FOK => "limit",
        OrderType::StopMarket | OrderType::StopLimit => "conditional",
    }
}
