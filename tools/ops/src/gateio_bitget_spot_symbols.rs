use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use clap::Args;
use serde_json::Value;

const GATEIO_REST_BASE_URL: &str = "https://api.gateio.ws/api/v4";
const BITGET_REST_BASE_URL: &str = "https://api.bitget.com";

#[derive(Debug, Clone, Args)]
pub struct GateioBitgetSpotSymbolsArgs {
    #[arg(long, default_value_t = 200)]
    pub limit: usize,
    #[arg(long)]
    pub yaml: bool,
    #[arg(long)]
    pub exclude_mainstream: bool,
    #[arg(long)]
    pub sort_volume: bool,
    #[arg(long)]
    pub show_effective_min_notional: bool,
    #[arg(long)]
    pub max_effective_min_notional: Option<f64>,
    #[arg(long, value_name = "SYMBOL")]
    pub exclude: Vec<String>,
    #[arg(long, value_name = "SYMBOL")]
    pub pin: Option<String>,
}

pub async fn run_gateio_bitget_spot_symbols(
    args: GateioBitgetSpotSymbolsArgs,
) -> Result<Vec<String>> {
    let excludes = args
        .exclude
        .iter()
        .map(|symbol| normalize_symbol(symbol))
        .collect::<BTreeSet<_>>();
    let pin = args.pin.as_deref().map(normalize_symbol);

    let (gateio_rules, bitget_rules) =
        tokio::join!(load_gateio_symbol_rules(), load_bitget_symbol_rules());
    let gateio_rules = gateio_rules?
        .into_iter()
        .filter(|rule| rule.quote_asset == "USDT" && rule.status == SymbolStatus::Trading)
        .map(|rule| (normalize_symbol(&rule.internal_symbol), rule))
        .collect::<BTreeMap<_, _>>();
    let bitget_rules = bitget_rules?
        .into_iter()
        .filter(|rule| rule.quote_asset == "USDT" && rule.status == SymbolStatus::Trading)
        .map(|rule| (normalize_symbol(&rule.internal_symbol), rule))
        .collect::<BTreeMap<_, _>>();
    let gateio_symbols = gateio_rules.keys().cloned().collect::<BTreeSet<_>>();
    let bitget_symbols = bitget_rules.keys().cloned().collect::<BTreeSet<_>>();

    let mut symbols = gateio_symbols
        .intersection(&bitget_symbols)
        .cloned()
        .collect::<Vec<_>>();
    if args.exclude_mainstream {
        symbols.retain(|symbol| !is_mainstream_symbol(symbol));
    }
    if !excludes.is_empty() {
        symbols.retain(|symbol| !excludes.contains(&normalize_symbol(symbol)));
    }

    let needs_tickers = args.sort_volume
        || args.max_effective_min_notional.is_some()
        || args.show_effective_min_notional;
    let ticker_stats = if needs_tickers {
        let (gateio_stats, bitget_stats) =
            tokio::join!(fetch_gateio_ticker_stats(), fetch_bitget_ticker_stats());
        Some((gateio_stats?, bitget_stats?))
    } else {
        None
    };

    if let Some(max_effective_min_notional) = args.max_effective_min_notional {
        let (gateio_stats, bitget_stats) = ticker_stats
            .as_ref()
            .expect("ticker stats loaded for min-notional filter");
        symbols.retain(|symbol| {
            let Some(gateio_rule) = gateio_rules.get(symbol) else {
                return false;
            };
            let Some(bitget_rule) = bitget_rules.get(symbol) else {
                return false;
            };
            let gateio_min = effective_min_notional(
                gateio_rule,
                gateio_stats.get(symbol).and_then(|stats| stats.last_price),
            );
            let bitget_min = effective_min_notional(
                bitget_rule,
                bitget_stats.get(symbol).and_then(|stats| stats.last_price),
            );
            gateio_min <= max_effective_min_notional && bitget_min <= max_effective_min_notional
        });
    }

    if args.sort_volume {
        let (gateio_stats, bitget_stats) = ticker_stats
            .as_ref()
            .expect("ticker stats loaded for volume sorting");
        symbols.sort_by(|left, right| {
            let left_volume = combined_quote_volume(left, gateio_stats, bitget_stats);
            let right_volume = combined_quote_volume(right, gateio_stats, bitget_stats);
            right_volume
                .partial_cmp(&left_volume)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.cmp(right))
        });
    }

    if let Some(pin) = pin {
        pin_first(&mut symbols, &pin);
    }
    symbols.truncate(args.limit);

    if args.show_effective_min_notional {
        let (gateio_stats, bitget_stats) = ticker_stats
            .as_ref()
            .expect("ticker stats loaded for min-notional output");
        Ok(symbols
            .into_iter()
            .map(|symbol| {
                let gateio_min = gateio_rules
                    .get(&symbol)
                    .map(|rule| {
                        effective_min_notional(
                            rule,
                            gateio_stats.get(&symbol).and_then(|stats| stats.last_price),
                        )
                    })
                    .unwrap_or(f64::INFINITY);
                let bitget_min = bitget_rules
                    .get(&symbol)
                    .map(|rule| {
                        effective_min_notional(
                            rule,
                            bitget_stats.get(&symbol).and_then(|stats| stats.last_price),
                        )
                    })
                    .unwrap_or(f64::INFINITY);
                format!(
                    "{symbol}\tgateio_min={gateio_min:.6}\tbitget_min={bitget_min:.6}\tmax_min={:.6}",
                    gateio_min.max(bitget_min)
                )
            })
            .collect())
    } else if args.yaml {
        Ok(symbols
            .into_iter()
            .map(|symbol| format!("  - {symbol}"))
            .collect())
    } else {
        Ok(symbols)
    }
}

#[derive(Debug, Clone, PartialEq)]
struct SymbolRule {
    internal_symbol: String,
    quote_asset: String,
    status: SymbolStatus,
    step_size: f64,
    min_quantity: f64,
    min_notional: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SymbolStatus {
    Trading,
    Suspended,
    Unknown,
}

#[derive(Debug, Clone, Default)]
struct TickerStats {
    quote_volume: f64,
    last_price: Option<f64>,
}

async fn load_gateio_symbol_rules() -> Result<Vec<SymbolRule>> {
    let value = reqwest::get(format!("{GATEIO_REST_BASE_URL}/spot/currency_pairs"))
        .await?
        .json::<Value>()
        .await?;
    parse_gateio_symbol_rules(&value)
}

async fn load_bitget_symbol_rules() -> Result<Vec<SymbolRule>> {
    let value = reqwest::get(format!("{BITGET_REST_BASE_URL}/api/v2/spot/public/symbols"))
        .await?
        .json::<Value>()
        .await?;
    parse_bitget_symbol_rules(&value)
}

fn parse_gateio_symbol_rules(value: &Value) -> Result<Vec<SymbolRule>> {
    value
        .as_array()
        .context("Gate.io currency_pairs response is not an array")?
        .iter()
        .map(parse_gateio_symbol_rule)
        .collect()
}

fn parse_gateio_symbol_rule(value: &Value) -> Result<SymbolRule> {
    let exchange_symbol = required_str(value, "id")?.to_ascii_uppercase();
    Ok(SymbolRule {
        internal_symbol: exchange_symbol.replace('_', ""),
        quote_asset: required_str(value, "quote")?.to_ascii_uppercase(),
        status: match value
            .get("trade_status")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
        {
            "tradable" => SymbolStatus::Trading,
            "disabled" => SymbolStatus::Suspended,
            _ => SymbolStatus::Unknown,
        },
        step_size: number_from_str(value.get("amount_precision"))
            .map(|precision| 10_f64.powi(-(precision as i32)))
            .unwrap_or(0.0),
        min_quantity: number_from_str(value.get("min_base_amount")).unwrap_or(0.0),
        min_notional: number_from_str(value.get("min_quote_amount")).unwrap_or(0.0),
    })
}

fn parse_bitget_symbol_rules(value: &Value) -> Result<Vec<SymbolRule>> {
    value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .context("Bitget symbols response is not an array")?
        .iter()
        .map(parse_bitget_symbol_rule)
        .collect()
}

fn parse_bitget_symbol_rule(value: &Value) -> Result<SymbolRule> {
    let exchange_symbol = required_str(value, "symbol")?.to_ascii_uppercase();
    let quote_asset = value
        .get("quoteCoin")
        .or_else(|| value.get("quoteCoinName"))
        .and_then(Value::as_str)
        .unwrap_or("USDT")
        .to_ascii_uppercase();
    let quantity_precision = number_from_str(
        value
            .get("quantityPrecision")
            .or_else(|| value.get("volumePlace")),
    )
    .unwrap_or(8.0) as u32;
    let step_size = number_from_str(
        value
            .get("minTradeAmount")
            .or_else(|| value.get("sizeMultiplier")),
    )
    .filter(|value| *value > 0.0)
    .unwrap_or_else(|| 10_f64.powi(-(quantity_precision as i32)));

    Ok(SymbolRule {
        internal_symbol: exchange_symbol,
        quote_asset,
        status: match value
            .get("status")
            .or_else(|| value.get("symbolStatus"))
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_ascii_lowercase()
            .as_str()
        {
            "online" | "normal" => SymbolStatus::Trading,
            "offline" | "suspend" => SymbolStatus::Suspended,
            _ => SymbolStatus::Unknown,
        },
        step_size,
        min_quantity: number_from_str(
            value
                .get("minTradeAmount")
                .or_else(|| value.get("minTradeNum")),
        )
        .unwrap_or(0.0),
        min_notional: number_from_str(value.get("minTradeUSDT")).unwrap_or(0.0),
    })
}

async fn fetch_gateio_ticker_stats() -> Result<BTreeMap<String, TickerStats>> {
    let value = reqwest::get(format!("{GATEIO_REST_BASE_URL}/spot/tickers"))
        .await?
        .json::<Value>()
        .await?;
    let mut stats = BTreeMap::new();
    if let Some(items) = value.as_array() {
        for item in items {
            let Some(symbol) = item
                .get("currency_pair")
                .and_then(Value::as_str)
                .map(normalize_symbol)
            else {
                continue;
            };
            stats.insert(
                symbol,
                TickerStats {
                    quote_volume: number_from_fields(item, &["quote_volume", "quoteVolume"])
                        .unwrap_or(0.0),
                    last_price: number_from_fields(item, &["last", "last_price", "close"]),
                },
            );
        }
    }
    Ok(stats)
}

async fn fetch_bitget_ticker_stats() -> Result<BTreeMap<String, TickerStats>> {
    let value = reqwest::get(format!("{BITGET_REST_BASE_URL}/api/v2/spot/market/tickers"))
        .await?
        .json::<Value>()
        .await?;
    let mut stats = BTreeMap::new();
    let items = value
        .get("data")
        .and_then(Value::as_array)
        .or_else(|| value.as_array());
    if let Some(items) = items {
        for item in items {
            let Some(symbol) = item
                .get("symbol")
                .and_then(Value::as_str)
                .map(normalize_symbol)
            else {
                continue;
            };
            stats.insert(
                symbol,
                TickerStats {
                    quote_volume: number_from_fields(
                        item,
                        &[
                            "usdtVolume",
                            "quoteVolume",
                            "quoteVol",
                            "quote_volume",
                            "turnover24h",
                        ],
                    )
                    .unwrap_or(0.0),
                    last_price: number_from_fields(
                        item,
                        &["lastPr", "last", "close", "closePrice"],
                    ),
                },
            );
        }
    }
    Ok(stats)
}

fn combined_quote_volume(
    symbol: &str,
    gateio_stats: &BTreeMap<String, TickerStats>,
    bitget_stats: &BTreeMap<String, TickerStats>,
) -> f64 {
    let symbol = normalize_symbol(symbol);
    gateio_stats
        .get(&symbol)
        .map(|stats| stats.quote_volume)
        .unwrap_or(0.0)
        .min(
            bitget_stats
                .get(&symbol)
                .map(|stats| stats.quote_volume)
                .unwrap_or(0.0),
        )
}

fn effective_min_notional(rule: &SymbolRule, last_price: Option<f64>) -> f64 {
    let Some(last_price) = last_price.filter(|price| price.is_finite() && *price > 0.0) else {
        return f64::INFINITY;
    };
    let quantity_from_notional = if rule.min_notional > 0.0 {
        rule.min_notional / last_price
    } else {
        0.0
    };
    let required_quantity = rule.min_quantity.max(quantity_from_notional);
    round_up_to_step(required_quantity, rule.step_size) * last_price
}

fn round_up_to_step(quantity: f64, step_size: f64) -> f64 {
    if !quantity.is_finite() || quantity <= 0.0 {
        return 0.0;
    }
    if !step_size.is_finite() || step_size <= 0.0 {
        return quantity;
    }
    (quantity / step_size).ceil() * step_size
}

fn number_from_fields(value: &Value, fields: &[&str]) -> Option<f64> {
    fields.iter().find_map(|field| {
        value.get(*field).and_then(|item| match item {
            Value::Number(number) => number.as_f64(),
            Value::String(text) => text.parse::<f64>().ok(),
            _ => None,
        })
    })
}

fn number_from_str(value: Option<&Value>) -> Option<f64> {
    value.and_then(|value| match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    })
}

fn required_str<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .with_context(|| format!("missing field {field}: {value}"))
}

fn normalize_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .replace(['-', '_', '/'], "")
        .to_ascii_uppercase()
}

fn is_mainstream_symbol(symbol: &str) -> bool {
    let normalized = symbol.trim().to_ascii_uppercase();
    let base = normalized.strip_suffix("USDT").unwrap_or(&normalized);
    matches!(
        base,
        "BTC"
            | "ETH"
            | "BNB"
            | "SOL"
            | "XRP"
            | "DOGE"
            | "ADA"
            | "TRX"
            | "TON"
            | "LTC"
            | "BCH"
            | "DOT"
            | "AVAX"
            | "MATIC"
            | "POL"
            | "LINK"
            | "UNI"
            | "ATOM"
            | "ETC"
            | "FIL"
            | "AAVE"
            | "ARB"
            | "OP"
            | "APT"
            | "SUI"
            | "NEAR"
            | "ICP"
            | "INJ"
            | "TIA"
            | "SEI"
            | "ALGO"
            | "VET"
            | "XLM"
            | "HBAR"
            | "CRO"
            | "FTM"
            | "RENDER"
            | "RNDR"
            | "USDC"
            | "WBTC"
            | "WETH"
            | "STETH"
            | "XAUT"
            | "PAXG"
    )
}

fn pin_first(symbols: &mut Vec<String>, symbol: &str) {
    let Some(index) = symbols
        .iter()
        .position(|candidate| candidate.eq_ignore_ascii_case(symbol))
    else {
        return;
    };
    let symbol = symbols.remove(index);
    symbols.insert(0, symbol);
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parsers_should_preserve_legacy_symbol_normalization_and_rules() {
        let gateio = parse_gateio_symbol_rules(&json!([
            {
                "id": "abc_usdt",
                "base": "ABC",
                "quote": "USDT",
                "trade_status": "tradable",
                "amount_precision": "2",
                "min_base_amount": "0.01",
                "min_quote_amount": "5"
            }
        ]))
        .expect("parse gateio");
        assert_eq!(gateio[0].internal_symbol, "ABCUSDT");
        assert_eq!(gateio[0].status, SymbolStatus::Trading);
        assert_eq!(gateio[0].step_size, 0.01);

        let bitget = parse_bitget_symbol_rules(&json!({
            "data": [
                {
                    "symbol": "ABCUSDT",
                    "quoteCoin": "USDT",
                    "status": "online",
                    "quantityPrecision": "4",
                    "minTradeAmount": "0.0001",
                    "minTradeUSDT": "1"
                }
            ]
        }))
        .expect("parse bitget");
        assert_eq!(bitget[0].internal_symbol, "ABCUSDT");
        assert_eq!(bitget[0].status, SymbolStatus::Trading);
        assert_eq!(bitget[0].step_size, 0.0001);
    }

    #[test]
    fn formatting_helpers_should_match_legacy_behavior() {
        assert_eq!(normalize_symbol("abc/usdt"), "ABCUSDT");
        assert!(is_mainstream_symbol("BTCUSDT"));
        assert!(!is_mainstream_symbol("ABCUSDT"));

        let mut symbols = vec!["ABCUSDT".to_string(), "XYZUSDT".to_string()];
        pin_first(&mut symbols, &normalize_symbol("xyz_usdt"));
        assert_eq!(symbols, vec!["XYZUSDT", "ABCUSDT"]);
    }
}
