use crate::smart_money::{Direction, WalletId, WalletPositionSnapshot, WalletTrade};
use chrono::{DateTime, Utc};
use reqwest::Client;
use rust_decimal::Decimal;
use serde_json::{json, Value};
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct HyperliquidWalletClient {
    client: Client,
    info_url: String,
}

impl HyperliquidWalletClient {
    pub fn new(api_base_url: impl Into<String>) -> Self {
        let base = api_base_url.into().trim_end_matches('/').to_string();
        Self {
            client: Client::builder()
                .use_rustls_tls()
                .build()
                .unwrap_or_else(|_| Client::new()),
            info_url: format!("{base}/info"),
        }
    }

    pub async fn fetch_clearinghouse_state(
        &self,
        address: &str,
    ) -> Result<Value, HyperliquidWalletError> {
        let response = self
            .client
            .post(&self.info_url)
            .json(&json!({
                "type": "clearinghouseState",
                "user": address.to_ascii_lowercase(),
            }))
            .send()
            .await
            .map_err(|err| HyperliquidWalletError::Transport(err.to_string()))?;
        response
            .error_for_status()
            .map_err(|err| HyperliquidWalletError::Transport(err.to_string()))?
            .json::<Value>()
            .await
            .map_err(|err| HyperliquidWalletError::Json(err.to_string()))
    }

    pub async fn fetch_user_fills_by_time(
        &self,
        address: &str,
        start_time_millis: i64,
        end_time_millis: Option<i64>,
    ) -> Result<Value, HyperliquidWalletError> {
        let mut body = json!({
            "type": "userFillsByTime",
            "user": address.to_ascii_lowercase(),
            "startTime": start_time_millis,
        });
        if let Some(end_time_millis) = end_time_millis {
            body["endTime"] = json!(end_time_millis);
        }
        let response = self
            .client
            .post(&self.info_url)
            .json(&body)
            .send()
            .await
            .map_err(|err| HyperliquidWalletError::Transport(err.to_string()))?;
        response
            .error_for_status()
            .map_err(|err| HyperliquidWalletError::Transport(err.to_string()))?
            .json::<Value>()
            .await
            .map_err(|err| HyperliquidWalletError::Json(err.to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum HyperliquidWalletError {
    #[error("transport error: {0}")]
    Transport(String),
    #[error("json error: {0}")]
    Json(String),
    #[error("missing field {0}")]
    MissingField(&'static str),
    #[error("invalid decimal field {field}: {value}")]
    InvalidDecimal { field: &'static str, value: String },
    #[error("invalid timestamp {0}")]
    InvalidTimestamp(i64),
    #[error("expected array response")]
    ExpectedArray,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedHyperliquidWalletState {
    pub wallet_id: WalletId,
    pub address: String,
    pub equity_usdt: Decimal,
    pub margin_used_usdt: Decimal,
    pub withdrawable_usdt: Decimal,
    pub positions: Vec<WalletPositionSnapshot>,
}

pub fn parse_clearinghouse_state(
    wallet_id: WalletId,
    address: impl Into<String>,
    raw: &Value,
    observed_at: DateTime<Utc>,
) -> Result<ParsedHyperliquidWalletState, HyperliquidWalletError> {
    let address = address.into().to_ascii_lowercase();
    let margin_summary = raw
        .get("marginSummary")
        .ok_or(HyperliquidWalletError::MissingField("marginSummary"))?;
    let equity_usdt = decimal_value(margin_summary, "accountValue")?;
    let margin_used_usdt = optional_decimal_value(margin_summary, "totalMarginUsed")
        .or_else(|| optional_decimal_value(raw, "marginUsed"))
        .unwrap_or(Decimal::ZERO);
    let withdrawable_usdt = optional_decimal_value(raw, "withdrawable").unwrap_or(Decimal::ZERO);
    let asset_positions = raw
        .get("assetPositions")
        .and_then(Value::as_array)
        .ok_or(HyperliquidWalletError::MissingField("assetPositions"))?;
    let mut positions = Vec::new();
    for item in asset_positions {
        let position = item
            .get("position")
            .ok_or(HyperliquidWalletError::MissingField("position"))?;
        let coin = string_value(position, "coin")?;
        let signed_qty = decimal_value(position, "szi")?;
        if signed_qty == Decimal::ZERO {
            continue;
        }
        let direction = if signed_qty > Decimal::ZERO {
            Direction::Long
        } else {
            Direction::Short
        };
        let entry_price = optional_decimal_value(position, "entryPx").unwrap_or(Decimal::ZERO);
        let notional_usdt = optional_decimal_value(position, "positionValue")
            .unwrap_or_else(|| signed_qty.abs() * entry_price);
        let mark_price = if signed_qty != Decimal::ZERO {
            notional_usdt.abs() / signed_qty.abs()
        } else {
            Decimal::ZERO
        };
        let position_margin_used =
            optional_decimal_value(position, "marginUsed").unwrap_or(margin_used_usdt);
        let leverage = position
            .get("leverage")
            .and_then(|leverage| optional_decimal_value(leverage, "value"))
            .unwrap_or_else(|| {
                if equity_usdt > Decimal::ZERO {
                    notional_usdt.abs() / equity_usdt
                } else {
                    Decimal::ZERO
                }
            });
        let unrealized_pnl_usdt =
            optional_decimal_value(position, "unrealizedPnl").unwrap_or(Decimal::ZERO);

        positions.push(WalletPositionSnapshot {
            wallet_id: wallet_id.clone(),
            symbol: format!("{coin}USDT"),
            direction,
            quantity: signed_qty.abs(),
            entry_price,
            mark_price,
            notional_usdt: notional_usdt.abs(),
            equity_usdt,
            margin_used_usdt: position_margin_used,
            leverage,
            unrealized_pnl_usdt,
            observed_at,
        });
    }

    Ok(ParsedHyperliquidWalletState {
        wallet_id,
        address,
        equity_usdt,
        margin_used_usdt,
        withdrawable_usdt,
        positions,
    })
}

pub fn parse_user_fills(
    wallet_id: WalletId,
    raw: &Value,
) -> Result<Vec<WalletTrade>, HyperliquidWalletError> {
    let fills = raw
        .as_array()
        .ok_or(HyperliquidWalletError::ExpectedArray)?;
    fills
        .iter()
        .map(|fill| parse_user_fill(wallet_id.clone(), fill))
        .collect()
}

fn parse_user_fill(
    wallet_id: WalletId,
    fill: &Value,
) -> Result<WalletTrade, HyperliquidWalletError> {
    let coin = string_value(fill, "coin")?;
    let price = decimal_value(fill, "px")?;
    let quantity = decimal_value(fill, "sz")?;
    let side = string_value(fill, "side").unwrap_or("B");
    let direction = match side {
        "B" | "buy" | "Buy" => Direction::Long,
        "A" | "sell" | "Sell" => Direction::Short,
        _ => Direction::Flat,
    };
    let time_millis = fill
        .get("time")
        .and_then(Value::as_i64)
        .ok_or(HyperliquidWalletError::MissingField("time"))?;
    let executed_at = DateTime::<Utc>::from_timestamp_millis(time_millis)
        .ok_or(HyperliquidWalletError::InvalidTimestamp(time_millis))?;
    let fee_usdt = optional_decimal_value(fill, "fee").unwrap_or(Decimal::ZERO);
    let realized_pnl_usdt = optional_decimal_value(fill, "closedPnl").unwrap_or(Decimal::ZERO);
    let external_id = fill.get("hash").or_else(|| fill.get("oid")).map(|value| {
        value
            .as_str()
            .map(ToString::to_string)
            .unwrap_or_else(|| value.to_string())
    });

    Ok(WalletTrade {
        wallet_id,
        symbol: format!("{coin}USDT"),
        direction,
        price,
        quantity,
        notional_usdt: price * quantity,
        fee_usdt,
        realized_pnl_usdt,
        executed_at,
        external_id,
    })
}

fn string_value<'a>(
    value: &'a Value,
    field: &'static str,
) -> Result<&'a str, HyperliquidWalletError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or(HyperliquidWalletError::MissingField(field))
}

fn decimal_value(value: &Value, field: &'static str) -> Result<Decimal, HyperliquidWalletError> {
    optional_decimal_value(value, field).ok_or(HyperliquidWalletError::MissingField(field))
}

fn optional_decimal_value(value: &Value, field: &'static str) -> Option<Decimal> {
    let raw = value.get(field)?;
    if let Some(s) = raw.as_str() {
        Decimal::from_str(s).ok()
    } else if let Some(n) = raw.as_i64() {
        Some(Decimal::from(n))
    } else if let Some(n) = raw.as_u64() {
        Some(Decimal::from(n))
    } else {
        raw.as_f64()
            .and_then(rust_decimal::Decimal::from_f64_retain)
    }
}
