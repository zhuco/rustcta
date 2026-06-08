use rustcta_types::MarketType;
use serde::{Deserialize, Deserializer, Serialize};

fn default_true() -> bool {
    true
}

fn default_exchanges() -> Vec<String> {
    vec!["mexc".to_string(), "coinex".to_string()]
}

fn default_target_mode() -> String {
    "small_live_taker_taker".to_string()
}

fn default_max_book_age_ms() -> u64 {
    1_000
}

fn default_minimum_quote_balance_usdt() -> f64 {
    10.0
}

fn default_market_type() -> MarketType {
    MarketType::Spot
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LivePreflightConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_target_mode")]
    pub target_mode: String,
    #[serde(default = "default_exchanges")]
    pub exchanges: Vec<String>,
    #[serde(
        default = "default_market_type",
        deserialize_with = "deserialize_market_type"
    )]
    pub market_type: MarketType,
    #[serde(default)]
    pub symbols: Vec<String>,
    pub max_live_notional_per_trade: Option<f64>,
    pub max_total_live_notional: Option<f64>,
    #[serde(default = "default_true")]
    pub require_monitoring_enabled: bool,
    #[serde(default = "default_true")]
    pub require_recorder_enabled: bool,
    #[serde(default = "default_true")]
    pub require_websocket_fresh: bool,
    #[serde(default = "default_max_book_age_ms")]
    pub max_book_age_ms: u64,
    #[serde(default = "default_true")]
    pub require_fee_model: bool,
    #[serde(default = "default_true")]
    pub require_disabled_registry: bool,
    #[serde(default = "default_true")]
    pub require_kill_switch: bool,
    #[serde(default = "default_true")]
    pub require_balances: bool,
    #[serde(default = "default_true")]
    pub require_symbol_rules: bool,
    #[serde(default = "default_true")]
    pub require_order_validation: bool,
    #[serde(default)]
    pub require_private_stream: bool,
    #[serde(default = "default_true")]
    pub allow_rest_order_polling_fallback: bool,
    #[serde(default = "default_true")]
    pub require_api_key_read_permission: bool,
    #[serde(default)]
    pub require_api_key_trade_permission: bool,
    #[serde(default = "default_true")]
    pub require_withdraw_permission_absent: bool,
    #[serde(default = "default_minimum_quote_balance_usdt")]
    pub minimum_quote_balance_usdt: f64,
    #[serde(default)]
    pub minimum_base_inventory_usdt: f64,
    #[serde(default = "default_true")]
    pub fail_on_unmanaged_position_overlap: bool,
}

impl Default for LivePreflightConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            target_mode: default_target_mode(),
            exchanges: default_exchanges(),
            market_type: MarketType::Spot,
            symbols: Vec::new(),
            max_live_notional_per_trade: None,
            max_total_live_notional: None,
            require_monitoring_enabled: true,
            require_recorder_enabled: true,
            require_websocket_fresh: true,
            max_book_age_ms: default_max_book_age_ms(),
            require_fee_model: true,
            require_disabled_registry: true,
            require_kill_switch: true,
            require_balances: true,
            require_symbol_rules: true,
            require_order_validation: true,
            require_private_stream: false,
            allow_rest_order_polling_fallback: true,
            require_api_key_read_permission: true,
            require_api_key_trade_permission: false,
            require_withdraw_permission_absent: true,
            minimum_quote_balance_usdt: default_minimum_quote_balance_usdt(),
            minimum_base_inventory_usdt: 0.0,
            fail_on_unmanaged_position_overlap: true,
        }
    }
}

fn deserialize_market_type<'de, D>(deserializer: D) -> Result<MarketType, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    match value.trim().to_ascii_lowercase().as_str() {
        "spot" => Ok(MarketType::Spot),
        "perpetual" | "perp" | "futures" | "future" => Ok(MarketType::Perpetual),
        other => Err(serde::de::Error::custom(format!(
            "unsupported market_type: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_preflight_config_should_accept_legacy_market_type_spellings() {
        for (market_type, expected) in [
            ("Spot", MarketType::Spot),
            ("spot", MarketType::Spot),
            ("Perpetual", MarketType::Perpetual),
            ("perp", MarketType::Perpetual),
            ("futures", MarketType::Perpetual),
        ] {
            let config = serde_yaml::from_str::<LivePreflightConfig>(&format!(
                "enabled: true\nmarket_type: {market_type}\n"
            ))
            .unwrap();
            assert_eq!(config.market_type, expected);
        }
    }
}
