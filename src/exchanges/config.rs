//! Strategy-independent exchange runtime configuration contracts.

use std::collections::HashMap;

use crate::exchanges::private_perp::PrivateWsRunConfig;
use crate::execution::PositionMode;
use crate::market::ExchangeId;

pub trait ExchangeRuntimeSettings {
    fn is_disabled(&self) -> bool;
    fn env_prefix(&self) -> Option<&str>;
    fn account_id(&self) -> Option<&str>;
    fn demo_trading(&self) -> bool;
    fn private_rest_base_url(&self) -> Option<&str>;
    fn private_ws_url(&self) -> Option<&str>;
    fn private_ws_enabled(&self) -> bool;
    fn private_ws_run(&self) -> PrivateWsRunConfig;
    fn position_mode_text(&self) -> Option<&str>;
}

pub trait ExchangeRegistryConfig {
    type Entry: ExchangeRuntimeSettings;

    fn exchange_runtime(&self) -> &HashMap<ExchangeId, Self::Entry>;
}

pub fn position_mode_from_config_value(value: &str) -> PositionMode {
    if matches!(
        value.to_ascii_lowercase().as_str(),
        "hedge" | "hedged" | "long_short"
    ) {
        PositionMode::Hedge
    } else {
        PositionMode::OneWay
    }
}
