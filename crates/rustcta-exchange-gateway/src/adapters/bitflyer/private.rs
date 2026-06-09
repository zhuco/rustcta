#![allow(dead_code)]

use std::collections::HashMap;

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_exchange_api::{
    OpenOrdersRequest, OpenOrdersResponse, PageCursor, QueryOrderRequest, QueryOrderResponse,
    RecentFillsRequest, RecentFillsResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::{normalize_product_code, parse_bitflyer_fills, parse_bitflyer_open_orders};
use super::BitflyerGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitflyerRequestSpec {
    pub method: String,
    pub path: String,
    pub body: String,
}

pub fn place_limit_order_spec(
    product_code: &str,
    side: &str,
    price: &str,
    size: &str,
    client_order_acceptance_id: Option<&str>,
) -> ExchangeApiResult<BitflyerRequestSpec> {
    let mut body = json!({
        "product_code": normalize_product_code(product_code)?,
        "child_order_type": "LIMIT",
        "side": normalize_side(side)?,
        "price": decimal_number(price)?,
        "size": decimal_number(size)?,
    });
    if let Some(client_id) = client_order_acceptance_id.filter(|value| !value.trim().is_empty()) {
        body["minute_to_expire"] = json!(43200);
        body["time_in_force"] = json!("GTC");
        body["client_order_acceptance_id"] = json!(client_id);
    }
    spec("POST", "/v1/me/sendchildorder", body)
}

pub fn cancel_order_spec(
    product_code: &str,
    child_order_acceptance_id: &str,
) -> ExchangeApiResult<BitflyerRequestSpec> {
    if child_order_acceptance_id.trim().is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bitflyer cancel requires child_order_acceptance_id".to_string(),
        });
    }
    spec(
        "POST",
        "/v1/me/cancelchildorder",
        json!({
            "product_code": normalize_product_code(product_code)?,
            "child_order_acceptance_id": child_order_acceptance_id,
        }),
    )
}

pub fn balances_spec() -> BitflyerRequestSpec {
    BitflyerRequestSpec {
        method: "GET".to_string(),
        path: "/v1/me/getbalance".to_string(),
        body: String::new(),
    }
}

pub fn query_order_params(
    product_code: &str,
    child_order_id: Option<&str>,
    child_order_acceptance_id: Option<&str>,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    params.insert(
        "product_code".to_string(),
        normalize_product_code(product_code)?,
    );
    if let Some(order_id) = child_order_id.filter(|value| !value.trim().is_empty()) {
        params.insert("child_order_id".to_string(), order_id.trim().to_string());
    }
    if let Some(client_order_id) =
        child_order_acceptance_id.filter(|value| !value.trim().is_empty())
    {
        params.insert(
            "child_order_acceptance_id".to_string(),
            client_order_id.trim().to_string(),
        );
    }
    if !params.contains_key("child_order_id") && !params.contains_key("child_order_acceptance_id") {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bitflyer query_order requires exchange_order_id or client_order_id"
                .to_string(),
        });
    }
    Ok(params)
}

pub fn open_orders_params(
    product_code: &str,
    limit: Option<u32>,
    cursor: Option<&PageCursor>,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    params.insert(
        "product_code".to_string(),
        normalize_product_code(product_code)?,
    );
    params.insert("child_order_state".to_string(), "ACTIVE".to_string());
    insert_page_params(&mut params, limit, cursor)?;
    Ok(params)
}

pub fn recent_fills_params(
    product_code: &str,
    child_order_id: Option<&str>,
    child_order_acceptance_id: Option<&str>,
    limit: Option<u32>,
    cursor: Option<&PageCursor>,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    params.insert(
        "product_code".to_string(),
        normalize_product_code(product_code)?,
    );
    if let Some(order_id) = child_order_id.filter(|value| !value.trim().is_empty()) {
        params.insert("child_order_id".to_string(), order_id.trim().to_string());
    }
    if let Some(client_order_id) =
        child_order_acceptance_id.filter(|value| !value.trim().is_empty())
    {
        params.insert(
            "child_order_acceptance_id".to_string(),
            client_order_id.trim().to_string(),
        );
    }
    insert_page_params(&mut params, limit, cursor)?;
    Ok(params)
}

impl BitflyerGatewayAdapter {
    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        let params = query_order_params(
            &request.symbol.exchange_symbol.symbol,
            request.exchange_order_id.as_deref(),
            request.client_order_id.as_deref(),
        )?;
        let (api_key, api_secret) = self.private_credentials("bitflyer.query_order")?;
        let value = self
            .rest
            .send_signed_get(api_key, api_secret, "/v1/me/getchildorders", &params)
            .await?;
        let orders = parse_bitflyer_open_orders(&self.exchange_id, Some(&request.symbol), &value)?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order: orders.into_iter().next(),
        })
    }

    pub(super) async fn get_open_orders_impl(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_supported_market_type(market_type)?;
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitflyer get_open_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let params = open_orders_params(
            &symbol.exchange_symbol.symbol,
            request.page.as_ref().and_then(|page| page.limit),
            request.page.as_ref().and_then(|page| page.cursor.as_ref()),
        )?;
        let (api_key, api_secret) = self.private_credentials("bitflyer.get_open_orders")?;
        let value = self
            .rest
            .send_signed_get(api_key, api_secret, "/v1/me/getchildorders", &params)
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_bitflyer_open_orders(&self.exchange_id, Some(symbol), &value)?,
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
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitflyer get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let params = recent_fills_params(
            &symbol.exchange_symbol.symbol,
            request.exchange_order_id.as_deref(),
            request.client_order_id.as_deref(),
            request
                .limit
                .or_else(|| request.page.as_ref().and_then(|page| page.limit)),
            request.page.as_ref().and_then(|page| page.cursor.as_ref()),
        )?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let (api_key, api_secret) = self.private_credentials("bitflyer.get_recent_fills")?;
        let value = self
            .rest
            .send_signed_get(api_key, api_secret, "/v1/me/getexecutions", &params)
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_bitflyer_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?,
        })
    }
}

fn insert_page_params(
    params: &mut HashMap<String, String>,
    limit: Option<u32>,
    cursor: Option<&PageCursor>,
) -> ExchangeApiResult<()> {
    if let Some(limit) = limit {
        if limit == 0 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bitflyer page limit must be positive".to_string(),
            });
        }
        params.insert("count".to_string(), limit.min(500).to_string());
    }
    if let Some(cursor) = cursor {
        match cursor {
            PageCursor::Id { id } | PageCursor::Token { token: id } if !id.trim().is_empty() => {
                params.insert("before".to_string(), id.trim().to_string());
            }
            PageCursor::Offset { offset } => {
                params.insert("skip".to_string(), offset.to_string());
            }
            _ => {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "bitflyer pagination supports id/token cursor or offset only"
                        .to_string(),
                });
            }
        }
    }
    Ok(())
}

fn spec(method: &str, path: &str, body: Value) -> ExchangeApiResult<BitflyerRequestSpec> {
    Ok(BitflyerRequestSpec {
        method: method.to_string(),
        path: path.to_string(),
        body: serde_json::to_string(&body).map_err(|error| ExchangeApiError::Serialization {
            message: error.to_string(),
        })?,
    })
}

fn normalize_side(side: &str) -> ExchangeApiResult<&'static str> {
    match side.trim().to_ascii_uppercase().as_str() {
        "BUY" => Ok("BUY"),
        "SELL" => Ok("SELL"),
        _ => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported bitflyer side {side}"),
        }),
    }
}

fn decimal_number(value: &str) -> ExchangeApiResult<Value> {
    let number = value
        .parse::<f64>()
        .map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("invalid bitflyer decimal {value}: {error}"),
        })?;
    Ok(json!(number))
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::{
        balances_spec, cancel_order_spec, open_orders_params, place_limit_order_spec,
        query_order_params, recent_fills_params,
    };

    #[test]
    fn bitflyer_request_specs_should_match_fixtures() {
        let place = place_limit_order_spec(
            "BTC_JPY",
            "BUY",
            "30000",
            "0.1",
            Some("JRF20260608-000000-000001"),
        )
        .expect("place spec");
        let expected: Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/bitflyer/request_specs/place_limit_order.json"
        ))
        .expect("place fixture");
        assert_eq!(place.method, expected["method"].as_str().unwrap());
        assert_eq!(place.path, expected["path"].as_str().unwrap());
        assert_eq!(
            serde_json::from_str::<Value>(&place.body).unwrap(),
            expected["body"]
        );

        let cancel =
            cancel_order_spec("BTC_JPY", "JRF20260608-000000-000001").expect("cancel spec");
        let expected_cancel: Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/bitflyer/request_specs/cancel_order.json"
        ))
        .expect("cancel fixture");
        assert_eq!(cancel.method, expected_cancel["method"].as_str().unwrap());
        assert_eq!(cancel.path, expected_cancel["path"].as_str().unwrap());
        assert_eq!(
            serde_json::from_str::<Value>(&cancel.body).unwrap(),
            expected_cancel["body"]
        );

        let balances = balances_spec();
        assert_eq!(balances.method, "GET");
        assert!(balances.body.is_empty());

        let query = query_order_params("BTC_JPY", None, Some("JRF20250327-000000-000001"))
            .expect("query params");
        assert_eq!(query["product_code"], "BTC_JPY");
        assert_eq!(
            query["child_order_acceptance_id"],
            "JRF20250327-000000-000001"
        );

        let open = open_orders_params("BTC_JPY", Some(50), None).expect("open params");
        assert_eq!(open["child_order_state"], "ACTIVE");
        assert_eq!(open["count"], "50");

        let fills = recent_fills_params(
            "BTC_JPY",
            Some("JOR20250327-000000-000001"),
            None,
            None,
            None,
        )
        .expect("fills params");
        assert_eq!(fills["child_order_id"], "JOR20250327-000000-000001");
    }
}
