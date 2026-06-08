#![cfg_attr(not(test), allow(dead_code))]

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{ExchangeApiResult, SymbolScope};
use rustcta_types::ExchangeId;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::parser::{
    currency_for_symbol, decimal_as_f64, normalize_deribit_symbol, parse_error, required_str,
    result_array, string_or_number, value_as_i64,
};
use super::DeribitGatewayAdapter;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeribitOptionContract {
    pub instrument_name: String,
    pub base_currency: String,
    pub quote_currency: String,
    pub settlement_currency: String,
    pub option_type: String,
    pub strike: f64,
    pub expiration_timestamp: Option<DateTime<Utc>>,
    pub contract_size: Option<f64>,
    pub tick_size: Option<f64>,
    pub min_trade_amount: Option<f64>,
    pub active: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeribitGreeksSnapshot {
    pub instrument_name: String,
    pub underlying_price: Option<f64>,
    pub mark_iv: Option<f64>,
    pub bid_iv: Option<f64>,
    pub ask_iv: Option<f64>,
    pub delta: Option<f64>,
    pub gamma: Option<f64>,
    pub theta: Option<f64>,
    pub vega: Option<f64>,
    pub rho: Option<f64>,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeribitSettlementEvent {
    pub instrument_name: String,
    pub settlement_type: String,
    pub session_bankruptcy: Option<f64>,
    pub session_profit_loss: Option<f64>,
    pub socialized: Option<f64>,
    pub funded: Option<f64>,
    pub timestamp: DateTime<Utc>,
}

impl DeribitGatewayAdapter {
    pub async fn fetch_option_contracts(
        &self,
        currency: &str,
    ) -> ExchangeApiResult<Vec<DeribitOptionContract>> {
        let mut params = std::collections::HashMap::new();
        params.insert("currency".to_string(), currency.to_ascii_uppercase());
        params.insert("kind".to_string(), "option".to_string());
        params.insert("expired".to_string(), "false".to_string());
        let value = self
            .rest
            .send_public_get("/api/v2/public/get_instruments", &params)
            .await?;
        parse_option_contracts(&self.exchange_id, &value)
    }

    pub async fn fetch_greeks(
        &self,
        symbol: SymbolScope,
    ) -> ExchangeApiResult<DeribitGreeksSnapshot> {
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market(symbol.market_type)?;
        let mut params = std::collections::HashMap::new();
        params.insert(
            "instrument_name".to_string(),
            normalize_deribit_symbol(&symbol)?,
        );
        let value = self
            .rest
            .send_public_get("/api/v2/public/ticker", &params)
            .await?;
        parse_greeks_snapshot(&self.exchange_id, &value)
    }

    pub async fn fetch_settlement_history(
        &self,
        symbol: Option<SymbolScope>,
        count: Option<u32>,
    ) -> ExchangeApiResult<Vec<DeribitSettlementEvent>> {
        let mut params = std::collections::HashMap::new();
        if let Some(symbol) = symbol {
            self.ensure_exchange(&symbol.exchange)?;
            params.insert(
                "instrument_name".to_string(),
                normalize_deribit_symbol(&symbol)?,
            );
        } else {
            params.insert("currency".to_string(), "BTC".to_string());
        }
        params.insert("type".to_string(), "settlement".to_string());
        params.insert(
            "count".to_string(),
            count.unwrap_or(20).clamp(1, 100).to_string(),
        );
        let value = self
            .rest
            .send_public_get("/api/v2/public/get_last_settlements_by_instrument", &params)
            .await?;
        parse_settlement_events(&self.exchange_id, &value)
    }
}

pub fn parse_option_contracts(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<DeribitOptionContract>> {
    result_array(
        exchange_id,
        value,
        "deribit option contracts response is not an array",
    )?
    .iter()
    .map(|item| parse_option_contract(exchange_id, item))
    .collect()
}

pub fn parse_greeks_snapshot(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<DeribitGreeksSnapshot> {
    let result = value.get("result").unwrap_or(value);
    let greeks = result.get("greeks").unwrap_or(result);
    Ok(DeribitGreeksSnapshot {
        instrument_name: required_str(exchange_id, result, "instrument_name")?.to_string(),
        underlying_price: result.get("underlying_price").and_then(decimal_as_f64),
        mark_iv: result.get("mark_iv").and_then(decimal_as_f64),
        bid_iv: result.get("bid_iv").and_then(decimal_as_f64),
        ask_iv: result.get("ask_iv").and_then(decimal_as_f64),
        delta: greeks.get("delta").and_then(decimal_as_f64),
        gamma: greeks.get("gamma").and_then(decimal_as_f64),
        theta: greeks.get("theta").and_then(decimal_as_f64),
        vega: greeks.get("vega").and_then(decimal_as_f64),
        rho: greeks.get("rho").and_then(decimal_as_f64),
        observed_at: result
            .get("timestamp")
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .unwrap_or_else(Utc::now),
    })
}

pub fn parse_settlement_events(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<DeribitSettlementEvent>> {
    let result = value.get("result").unwrap_or(value);
    let rows = result
        .get("settlements")
        .or_else(|| result.get("events"))
        .unwrap_or(result)
        .as_array()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "deribit settlements response is not an array",
                value,
            )
        })?;
    rows.iter()
        .map(|row| {
            Ok(DeribitSettlementEvent {
                instrument_name: required_str(exchange_id, row, "instrument_name")?.to_string(),
                settlement_type: row
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("settlement")
                    .to_string(),
                session_bankruptcy: row.get("session_bankruptcy").and_then(decimal_as_f64),
                session_profit_loss: row.get("session_profit_loss").and_then(decimal_as_f64),
                socialized: row.get("socialized").and_then(decimal_as_f64),
                funded: row.get("funded").and_then(decimal_as_f64),
                timestamp: row
                    .get("timestamp")
                    .and_then(value_as_i64)
                    .and_then(DateTime::<Utc>::from_timestamp_millis)
                    .unwrap_or_else(Utc::now),
            })
        })
        .collect()
}

fn parse_option_contract(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<DeribitOptionContract> {
    let instrument_name = required_str(exchange_id, value, "instrument_name")?.to_string();
    Ok(DeribitOptionContract {
        instrument_name,
        base_currency: value
            .get("base_currency")
            .and_then(Value::as_str)
            .unwrap_or("BTC")
            .to_ascii_uppercase(),
        quote_currency: value
            .get("quote_currency")
            .and_then(Value::as_str)
            .unwrap_or("USD")
            .to_ascii_uppercase(),
        settlement_currency: value
            .get("settlement_currency")
            .and_then(Value::as_str)
            .map(str::to_ascii_uppercase)
            .unwrap_or_else(|| currency_for_symbol_name(value)),
        option_type: value
            .get("option_type")
            .and_then(Value::as_str)
            .or_else(|| value.get("optionType").and_then(Value::as_str))
            .unwrap_or("unknown")
            .to_string(),
        strike: value.get("strike").and_then(decimal_as_f64).unwrap_or(0.0),
        expiration_timestamp: value
            .get("expiration_timestamp")
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis),
        contract_size: value.get("contract_size").and_then(decimal_as_f64),
        tick_size: value.get("tick_size").and_then(decimal_as_f64),
        min_trade_amount: value.get("min_trade_amount").and_then(decimal_as_f64),
        active: value
            .get("is_active")
            .and_then(Value::as_bool)
            .unwrap_or(true),
    })
}

fn currency_for_symbol_name(value: &Value) -> String {
    value
        .get("instrument_name")
        .and_then(Value::as_str)
        .map(|name| {
            let scope = SymbolScope {
                exchange: ExchangeId::new("deribit").expect("literal exchange id"),
                market_type: rustcta_types::MarketType::Option,
                canonical_symbol: None,
                exchange_symbol: rustcta_types::ExchangeSymbol::new(
                    ExchangeId::new("deribit").expect("literal exchange id"),
                    rustcta_types::MarketType::Option,
                    name,
                )
                .expect("fixture symbol"),
            };
            currency_for_symbol(&scope)
        })
        .or_else(|| string_or_number(value.get("settlement_currency")))
        .unwrap_or_else(|| "USD".to_string())
}
