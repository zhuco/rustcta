#![allow(dead_code)]

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_exchange_api::{
    OpenOrdersRequest, OpenOrdersResponse, RecentFillsRequest, RecentFillsResponse,
    EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::{normalize_zaif_pair, parse_zaif_open_orders, parse_zaif_recent_fills};
use super::ZaifGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZaifRequestSpec {
    pub method: String,
    pub path: String,
    pub body: String,
}

pub fn get_info2_spec(nonce: &str) -> ExchangeApiResult<ZaifRequestSpec> {
    form_spec(vec![("nonce", nonce), ("method", "get_info2")])
}

pub fn active_orders_spec(nonce: &str, pair: &str) -> ExchangeApiResult<ZaifRequestSpec> {
    let pair = normalize_zaif_pair(pair)?;
    form_spec(vec![
        ("nonce", nonce),
        ("method", "active_orders"),
        ("currency_pair", pair.as_str()),
    ])
}

pub fn trade_history_spec(
    nonce: &str,
    pair: &str,
    count: u32,
) -> ExchangeApiResult<ZaifRequestSpec> {
    let pair = normalize_zaif_pair(pair)?;
    let count = count.to_string();
    form_spec(vec![
        ("nonce", nonce),
        ("method", "trade_history"),
        ("currency_pair", pair.as_str()),
        ("count", count.as_str()),
    ])
}

impl ZaifGatewayAdapter {
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
                message: "zaif get_open_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let spec = active_orders_spec(&nonce(), &symbol.exchange_symbol.symbol)?;
        let (api_key, api_secret) = self.private_credentials("zaif.get_open_orders")?;
        let value = self
            .rest
            .send_signed_tapi(api_key, api_secret, &spec.body)
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_zaif_open_orders(&self.exchange_id, Some(symbol), &value)?,
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
                operation: "zaif.get_recent_fills.client_order_id",
            });
        }
        if request.exchange_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "zaif.get_recent_fills.exchange_order_id",
            });
        }
        if request.from_trade_id.is_some()
            || request.start_time.is_some()
            || request.end_time.is_some()
        {
            return Err(ExchangeApiError::Unsupported {
                operation: "zaif.get_recent_fills.cursor_or_time_filter",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "zaif get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let count = request
            .limit
            .or_else(|| request.page.as_ref().and_then(|page| page.limit))
            .unwrap_or(100)
            .clamp(1, 1000);
        let spec = trade_history_spec(&nonce(), &symbol.exchange_symbol.symbol, count)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let (api_key, api_secret) = self.private_credentials("zaif.get_recent_fills")?;
        let value = self
            .rest
            .send_signed_tapi(api_key, api_secret, &spec.body)
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_zaif_recent_fills(
                &self.exchange_id,
                tenant_id,
                account_id,
                symbol,
                &value,
            )?,
        })
    }
}

fn nonce() -> String {
    chrono::Utc::now().timestamp_millis().to_string()
}

pub fn trade_limit_spec(
    nonce: &str,
    pair: &str,
    action: &str,
    price: &str,
    amount: &str,
) -> ExchangeApiResult<ZaifRequestSpec> {
    let pair = normalize_zaif_pair(pair)?;
    form_spec(vec![
        ("nonce", nonce),
        ("method", "trade"),
        ("currency_pair", pair.as_str()),
        ("action", normalize_action(action)?),
        ("price", price),
        ("amount", amount),
    ])
}

pub fn cancel_order_spec(nonce: &str, order_id: &str) -> ExchangeApiResult<ZaifRequestSpec> {
    form_spec(vec![
        ("nonce", nonce),
        ("method", "cancel_order"),
        ("order_id", order_id),
    ])
}

fn form_spec(params: Vec<(&str, &str)>) -> ExchangeApiResult<ZaifRequestSpec> {
    Ok(ZaifRequestSpec {
        method: "POST".to_string(),
        path: "/tapi".to_string(),
        body: params
            .into_iter()
            .map(|(key, value)| format!("{}={}", form_encode(key), form_encode(value)))
            .collect::<Vec<_>>()
            .join("&"),
    })
}

fn normalize_action(action: &str) -> ExchangeApiResult<&'static str> {
    match action.trim().to_ascii_lowercase().as_str() {
        "bid" | "buy" => Ok("bid"),
        "ask" | "sell" => Ok("ask"),
        _ => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported zaif action {action}"),
        }),
    }
}

fn form_encode(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![byte as char]
            }
            b' ' => vec!['+'],
            _ => format!("%{byte:02X}").chars().collect(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::{
        active_orders_spec, cancel_order_spec, get_info2_spec, trade_history_spec, trade_limit_spec,
    };

    #[test]
    fn zaif_request_specs_should_match_fixtures() {
        let trade =
            trade_limit_spec("1710000000", "btc_jpy", "buy", "6500000", "0.01").expect("trade");
        assert_spec(
            trade,
            include_str!(
                "../../../../../tests/fixtures/exchanges/zaif/request_specs/trade_limit.json"
            ),
        );

        let cancel = cancel_order_spec("1710000001", "123456789").expect("cancel");
        assert_spec(
            cancel,
            include_str!(
                "../../../../../tests/fixtures/exchanges/zaif/request_specs/cancel_order.json"
            ),
        );

        assert_eq!(
            get_info2_spec("1710000002").expect("get_info2").body,
            "nonce=1710000002&method=get_info2"
        );
        assert_eq!(
            active_orders_spec("1710000003", "btc_jpy")
                .expect("active")
                .body,
            "nonce=1710000003&method=active_orders&currency_pair=btc_jpy"
        );
        assert_eq!(
            trade_history_spec("1710000004", "btc_jpy", 10)
                .expect("history")
                .body,
            "nonce=1710000004&method=trade_history&currency_pair=btc_jpy&count=10"
        );
    }

    fn assert_spec(spec: super::ZaifRequestSpec, fixture: &str) {
        let expected: Value = serde_json::from_str(fixture).expect("fixture");
        assert_eq!(spec.method, expected["method"].as_str().unwrap());
        assert_eq!(spec.path, expected["path"].as_str().unwrap());
        assert_eq!(spec.body, expected["body"].as_str().unwrap());
    }
}
