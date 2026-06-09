#![allow(dead_code)]

use std::collections::HashMap;

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_exchange_api::{
    OpenOrdersRequest, OpenOrdersResponse, QueryOrderRequest, QueryOrderResponse,
    RecentFillsRequest, RecentFillsResponse, RequestContext, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::{
    normalize_bittrade_symbol, parse_bittrade_open_orders, parse_bittrade_order_state,
    parse_bittrade_recent_fills,
};
use super::BittradeGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const OPEN_ORDERS_PATH: &str = "/v1/order/openOrders";
pub const MATCH_RESULTS_PATH: &str = "/v1/order/matchresults";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BittradeRequestSpec {
    pub method: String,
    pub path: String,
    pub body: String,
}

pub fn accounts_spec() -> BittradeRequestSpec {
    BittradeRequestSpec {
        method: "GET".to_string(),
        path: "/v1/account/accounts".to_string(),
        body: String::new(),
    }
}

pub fn balances_spec(account_id: u64) -> BittradeRequestSpec {
    BittradeRequestSpec {
        method: "GET".to_string(),
        path: format!("/v1/account/accounts/{account_id}/balance"),
        body: String::new(),
    }
}

pub fn place_limit_order_spec(
    account_id: u64,
    symbol: &str,
    side: &str,
    price: &str,
    amount: &str,
) -> ExchangeApiResult<BittradeRequestSpec> {
    spec(
        "POST",
        "/v1/order/orders/place",
        json!({
            "account-id": account_id,
            "amount": amount,
            "price": price,
            "source": "api",
            "symbol": normalize_bittrade_symbol(symbol)?,
            "type": format!("{}-limit", normalize_side(side)?),
        }),
    )
}

pub fn cancel_order_spec(order_id: u64) -> BittradeRequestSpec {
    BittradeRequestSpec {
        method: "POST".to_string(),
        path: format!("/v1/order/orders/{order_id}/submitcancel"),
        body: "{}".to_string(),
    }
}

pub fn batch_cancel_orders_spec(order_ids: &[u64]) -> ExchangeApiResult<BittradeRequestSpec> {
    let order_ids = order_ids
        .iter()
        .map(|order_id| order_id.to_string())
        .collect::<Vec<_>>();
    spec(
        "POST",
        "/v1/order/orders/batchcancel",
        json!({ "order-ids": order_ids }),
    )
}

pub fn query_order_spec(order_id: u64) -> BittradeRequestSpec {
    BittradeRequestSpec {
        method: "GET".to_string(),
        path: format!("/v1/order/orders/{order_id}"),
        body: String::new(),
    }
}

impl BittradeGatewayAdapter {
    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "bittrade.query_order.client_order_id",
            });
        }
        let order_id = plain_order_id(
            request.exchange_order_id.as_deref(),
            "bittrade query_order requires exchange_order_id",
        )?;
        let (api_key, api_secret) = self.private_credentials("bittrade.query_order")?;
        let value = self
            .rest
            .send_private_get(
                api_key,
                api_secret,
                &query_order_path(order_id),
                &HashMap::new(),
            )
            .await?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order: Some(parse_bittrade_order_state(
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
                message: "bittrade get_open_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (api_key, api_secret) = self.private_credentials("bittrade.get_open_orders")?;
        let value = self
            .rest
            .send_private_get(
                api_key,
                api_secret,
                OPEN_ORDERS_PATH,
                &open_orders_params(symbol, request.page.as_ref().and_then(|page| page.limit))?,
            )
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_bittrade_open_orders(&self.exchange_id, Some(symbol), &value)?,
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
                operation: "bittrade.get_recent_fills.client_order_id",
            });
        }
        if request.from_trade_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "bittrade.get_recent_fills.from_trade_id",
            });
        }
        if request.start_time.is_some() || request.end_time.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "bittrade.get_recent_fills.time_window",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bittrade get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (tenant_id, account_id) =
            context_account("bittrade.get_recent_fills", &request.context)?;
        let (api_key, api_secret) = self.private_credentials("bittrade.get_recent_fills")?;
        let value = self
            .rest
            .send_private_get(
                api_key,
                api_secret,
                MATCH_RESULTS_PATH,
                &recent_fills_params(
                    symbol,
                    request.exchange_order_id.as_deref(),
                    request
                        .limit
                        .or_else(|| request.page.as_ref().and_then(|page| page.limit)),
                )?,
            )
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_bittrade_recent_fills(
                &self.exchange_id,
                tenant_id,
                account_id,
                symbol,
                &value,
            )?,
        })
    }

    fn private_credentials(&self, operation: &'static str) -> ExchangeApiResult<(&str, &str)> {
        if !self.config.enabled_private_rest {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        let api_key = self
            .config
            .api_key
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or(ExchangeApiError::Unsupported { operation })?;
        let api_secret = self
            .config
            .api_secret
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or(ExchangeApiError::Unsupported { operation })?;
        Ok((api_key, api_secret))
    }
}

fn spec(method: &str, path: &str, body: Value) -> ExchangeApiResult<BittradeRequestSpec> {
    Ok(BittradeRequestSpec {
        method: method.to_string(),
        path: path.to_string(),
        body: serde_json::to_string(&body).map_err(|error| ExchangeApiError::Serialization {
            message: error.to_string(),
        })?,
    })
}

fn normalize_side(side: &str) -> ExchangeApiResult<&'static str> {
    match side.trim().to_ascii_lowercase().as_str() {
        "buy" => Ok("buy"),
        "sell" => Ok("sell"),
        _ => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported bittrade side {side}"),
        }),
    }
}

fn query_order_path(order_id: &str) -> String {
    format!("/v1/order/orders/{}", urlencoding::encode(order_id))
}

pub(super) fn open_orders_params(
    symbol: &rustcta_exchange_api::SymbolScope,
    limit: Option<u32>,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::from([(
        "symbol".to_string(),
        normalize_bittrade_symbol(&symbol.exchange_symbol.symbol)?,
    )]);
    if let Some(limit) = limit {
        params.insert("size".to_string(), limit.min(500).to_string());
    }
    Ok(params)
}

pub(super) fn recent_fills_params(
    symbol: &rustcta_exchange_api::SymbolScope,
    exchange_order_id: Option<&str>,
    limit: Option<u32>,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::from([(
        "symbol".to_string(),
        normalize_bittrade_symbol(&symbol.exchange_symbol.symbol)?,
    )]);
    if let Some(order_id) = exchange_order_id.filter(|value| !value.trim().is_empty()) {
        params.insert(
            "order-id".to_string(),
            plain_order_id(Some(order_id), "")?.to_string(),
        );
    }
    if let Some(limit) = limit {
        params.insert("size".to_string(), limit.min(500).to_string());
    }
    Ok(params)
}

fn plain_order_id<'a>(
    order_id: Option<&'a str>,
    missing_message: &str,
) -> ExchangeApiResult<&'a str> {
    let order_id = order_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: missing_message.to_string(),
        })?;
    if order_id.contains('/') || order_id.contains('?') || order_id.contains('&') {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bittrade order id must be a plain id".to_string(),
        });
    }
    Ok(order_id)
}

fn context_account(
    operation: &'static str,
    context: &RequestContext,
) -> ExchangeApiResult<(
    rustcta_exchange_api::TenantId,
    rustcta_exchange_api::AccountId,
)> {
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

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::{
        accounts_spec, balances_spec, batch_cancel_orders_spec, cancel_order_spec,
        place_limit_order_spec, query_order_spec,
    };

    #[test]
    fn bittrade_request_specs_should_match_fixtures() {
        assert_spec("accounts_get.json", accounts_spec());
        assert_spec("balance_get.json", balances_spec(100009));
        assert_spec(
            "place_limit_order.json",
            place_limit_order_spec(100009, "BTC/JPY", "buy", "3000000", "0.01").expect("place"),
        );
        assert_spec("cancel_order.json", cancel_order_spec(59378));
        assert_spec(
            "batch_cancel_orders.json",
            batch_cancel_orders_spec(&[1, 2, 3]).expect("batch cancel"),
        );
        assert_spec("query_order.json", query_order_spec(59378));
    }

    fn assert_spec(name: &str, actual: super::BittradeRequestSpec) {
        let fixture: Value = serde_json::from_str(
            &std::fs::read_to_string(format!(
                "{}/../../tests/fixtures/exchanges/bittrade/request_specs/{name}",
                env!("CARGO_MANIFEST_DIR")
            ))
            .expect("fixture"),
        )
        .expect("fixture json");
        assert_eq!(actual.method, fixture["method"].as_str().unwrap());
        assert_eq!(actual.path, fixture["path"].as_str().unwrap());
        if actual.body.is_empty() {
            assert!(fixture["body"].is_null());
        } else {
            assert_eq!(
                serde_json::from_str::<Value>(&actual.body).unwrap(),
                fixture["body"]
            );
        }
    }
}
