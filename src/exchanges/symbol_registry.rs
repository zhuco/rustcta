use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::exchanges::client_order_id::{validate_client_order_id, ClientOrderIdValidationError};
use crate::exchanges::unified::{
    validate_min_notional, validate_price_tick, validate_quantity_step, ExchangeClientError,
    MarketType, OrderRequest, OrderType, SymbolRule, SymbolStatus, TimeInForce,
};

pub use crate::exchanges::unified::MarketType as UnifiedMarketType;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SymbolKey {
    pub exchange: String,
    pub market_type: MarketType,
    pub internal_symbol: String,
}

impl SymbolKey {
    pub fn new(exchange: &str, market_type: MarketType, internal_symbol: &str) -> Self {
        Self {
            exchange: normalize_exchange(exchange),
            market_type,
            internal_symbol: normalize_internal_symbol(internal_symbol),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InstrumentType {
    Spot,
    LinearPerpetual,
    InversePerpetual,
    DeliveryFuture,
    Unknown,
}

pub type InstrumentId = String;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpotSymbol {
    pub exchange: String,
    pub instrument_id: InstrumentId,
    pub internal_symbol: String,
    pub exchange_symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PerpetualSymbol {
    pub exchange: String,
    pub instrument_id: InstrumentId,
    pub internal_symbol: String,
    pub exchange_symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub settlement_asset: String,
    pub instrument_type: InstrumentType,
    pub contract_size: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SymbolInstrument {
    Spot(SpotSymbol),
    Perpetual(PerpetualSymbol),
}

impl SymbolInstrument {
    pub fn symbol_key(&self) -> SymbolKey {
        match self {
            Self::Spot(symbol) => {
                SymbolKey::new(&symbol.exchange, MarketType::Spot, &symbol.internal_symbol)
            }
            Self::Perpetual(symbol) => SymbolKey::new(
                &symbol.exchange,
                MarketType::Perpetual,
                &symbol.internal_symbol,
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExchangeSymbolMapping {
    pub exchange: String,
    pub market_type: MarketType,
    pub internal_symbol: String,
    pub exchange_symbol: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SymbolRegistryConfig {
    #[serde(default)]
    pub mappings: Vec<ExchangeSymbolMapping>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UnifiedSymbolRegistry {
    rules: HashMap<SymbolKey, SymbolRule>,
    exchange_to_internal: HashMap<(String, MarketType, String), SymbolKey>,
    overrides: HashMap<(String, MarketType, String), String>,
}

impl UnifiedSymbolRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_rules(rules: impl IntoIterator<Item = SymbolRule>) -> Self {
        let mut registry = Self::new();
        registry.register_rules(rules);
        registry
    }

    pub fn with_config(mut self, config: SymbolRegistryConfig) -> Self {
        for mapping in config.mappings {
            self.register_mapping(mapping);
        }
        self
    }

    pub fn register_mapping(&mut self, mapping: ExchangeSymbolMapping) {
        self.overrides.insert(
            (
                normalize_exchange(&mapping.exchange),
                mapping.market_type,
                normalize_internal_symbol(&mapping.internal_symbol),
            ),
            normalize_exchange_symbol(&mapping.exchange_symbol),
        );
    }

    pub fn register_rules(&mut self, rules: impl IntoIterator<Item = SymbolRule>) {
        for rule in rules {
            self.register_rule(rule);
        }
    }

    pub fn register_rule(&mut self, mut rule: SymbolRule) {
        rule.exchange = normalize_exchange(&rule.exchange);
        rule.internal_symbol = normalize_internal_symbol(&rule.internal_symbol);
        rule.exchange_symbol = normalize_exchange_symbol(&rule.exchange_symbol);
        rule.base_asset = rule.base_asset.trim().to_ascii_uppercase();
        rule.quote_asset = rule.quote_asset.trim().to_ascii_uppercase();
        let key = SymbolKey::new(&rule.exchange, rule.market_type, &rule.internal_symbol);
        self.exchange_to_internal.insert(
            (
                key.exchange.clone(),
                key.market_type,
                rule.exchange_symbol.clone(),
            ),
            key.clone(),
        );
        self.rules.insert(key, rule);
    }

    pub fn get_rule(
        &self,
        exchange: &str,
        market_type: MarketType,
        internal_symbol: &str,
    ) -> Option<&SymbolRule> {
        self.rules
            .get(&SymbolKey::new(exchange, market_type, internal_symbol))
    }

    pub fn internal_to_exchange(
        &self,
        exchange: &str,
        market_type: MarketType,
        internal_symbol: &str,
    ) -> Result<String, SymbolRegistryError> {
        let key = SymbolKey::new(exchange, market_type, internal_symbol);
        if let Some(mapped) = self.overrides.get(&(
            key.exchange.clone(),
            key.market_type,
            key.internal_symbol.clone(),
        )) {
            return Ok(mapped.clone());
        }
        self.rules
            .get(&key)
            .map(|rule| rule.exchange_symbol.clone())
            .ok_or(SymbolRegistryError::UnknownSymbol(key))
    }

    pub fn exchange_to_internal(
        &self,
        exchange: &str,
        market_type: MarketType,
        exchange_symbol: &str,
    ) -> Result<String, SymbolRegistryError> {
        self.exchange_to_internal
            .get(&(
                normalize_exchange(exchange),
                market_type,
                normalize_exchange_symbol(exchange_symbol),
            ))
            .map(|key| key.internal_symbol.clone())
            .ok_or_else(|| SymbolRegistryError::UnknownExchangeSymbol {
                exchange: normalize_exchange(exchange),
                market_type,
                exchange_symbol: exchange_symbol.to_string(),
            })
    }

    pub fn supported_symbols(&self, exchange: &str, market_type: MarketType) -> Vec<String> {
        let exchange = normalize_exchange(exchange);
        self.rules
            .keys()
            .filter(|key| key.exchange == exchange && key.market_type == market_type)
            .map(|key| key.internal_symbol.clone())
            .collect()
    }

    pub fn validate_order(&self, request: &OrderRequest) -> Result<(), SymbolRegistryError> {
        self.validate_order_for_exchange("", request)
    }

    pub fn validate_order_for_exchange(
        &self,
        exchange: &str,
        request: &OrderRequest,
    ) -> Result<(), SymbolRegistryError> {
        let normalized = normalize_internal_symbol(&request.symbol);
        let exchange_symbol = normalize_exchange_symbol(&request.symbol);
        let requested_exchange = normalize_exchange(exchange);
        let rule = self
            .rules
            .values()
            .find(|rule| {
                rule.market_type == request.market_type
                    && (requested_exchange.is_empty() || rule.exchange == requested_exchange)
                    && (rule.internal_symbol == normalized
                        || rule.exchange_symbol == exchange_symbol)
            })
            .ok_or_else(|| SymbolRegistryError::RequestSymbolUnknown(request.symbol.clone()))?;
        validate_order_with_rule(request, rule, rule.exchange.as_str())
            .map_err(SymbolRegistryError::Validation)
    }

    pub fn validate_order_by_key(
        &self,
        key: &SymbolKey,
        request: &OrderRequest,
    ) -> Result<(), SymbolRegistryError> {
        let rule = self
            .rules
            .get(key)
            .ok_or_else(|| SymbolRegistryError::UnknownSymbol(key.clone()))?;
        validate_order_with_rule(request, rule, &key.exchange)
            .map_err(SymbolRegistryError::Validation)
    }
}

pub fn validate_order_with_rule(
    request: &OrderRequest,
    rule: &SymbolRule,
    exchange: &str,
) -> Result<(), ExchangeClientError> {
    request.validate()?;
    if request.market_type != rule.market_type {
        return Err(ExchangeClientError::Validation {
            field: "market_type",
            reason: format!(
                "request market {:?} does not match symbol rule market {:?}",
                request.market_type, rule.market_type
            ),
        });
    }
    if rule.status != SymbolStatus::Trading {
        return Err(ExchangeClientError::Validation {
            field: "symbol",
            reason: format!("symbol {} is not tradable", rule.internal_symbol),
        });
    }
    if !rule.supported_order_types.contains(&request.order_type) {
        return Err(ExchangeClientError::Unsupported(format!(
            "{:?} is not supported for {}",
            request.order_type, rule.internal_symbol
        )));
    }
    if let Some(tif) = request.time_in_force {
        if !rule.supported_time_in_force.contains(&tif) {
            return Err(ExchangeClientError::Unsupported(format!(
                "{:?} is not supported for {}",
                tif, rule.internal_symbol
            )));
        }
    }
    validate_quantity_step(request.quantity, rule.step_size)?;
    if request.quantity + 1e-12 < rule.min_quantity {
        return Err(ExchangeClientError::Validation {
            field: "quantity",
            reason: format!(
                "quantity {} is below min_quantity {}",
                request.quantity, rule.min_quantity
            ),
        });
    }
    if let Some(price) = request.price {
        validate_price_tick(price, rule.tick_size)?;
        validate_min_notional(request.quantity, Some(price), rule.min_notional)?;
    }
    if let Some(client_order_id) = &request.client_order_id {
        validate_client_order_id(exchange, request.market_type, client_order_id).map_err(
            |error| ExchangeClientError::Validation {
                field: "client_order_id",
                reason: error.to_string(),
            },
        )?;
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum SymbolRegistryError {
    #[error("unknown symbol {0:?}")]
    UnknownSymbol(SymbolKey),
    #[error("unknown exchange symbol {exchange:?} {market_type:?} {exchange_symbol}")]
    UnknownExchangeSymbol {
        exchange: String,
        market_type: MarketType,
        exchange_symbol: String,
    },
    #[error("request symbol is unknown: {0}")]
    RequestSymbolUnknown(String),
    #[error(transparent)]
    Validation(ExchangeClientError),
    #[error(transparent)]
    ClientOrderId(#[from] ClientOrderIdValidationError),
}

pub fn normalize_internal_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .to_ascii_uppercase()
        .replace(['-', '_', '/'], "")
}

pub fn normalize_exchange_symbol(symbol: &str) -> String {
    symbol.trim().to_ascii_uppercase()
}

fn normalize_exchange(exchange: &str) -> String {
    exchange.trim().to_ascii_lowercase()
}

pub fn infer_spot_rule(
    exchange: &str,
    internal_symbol: &str,
    exchange_symbol: &str,
    quote_asset: &str,
) -> SymbolRule {
    let internal = normalize_internal_symbol(internal_symbol);
    let quote = quote_asset.trim().to_ascii_uppercase();
    let base = internal
        .strip_suffix(&quote)
        .filter(|value| !value.is_empty())
        .unwrap_or("BASE")
        .to_string();
    SymbolRule {
        exchange: normalize_exchange(exchange),
        market_type: MarketType::Spot,
        internal_symbol: internal,
        exchange_symbol: normalize_exchange_symbol(exchange_symbol),
        base_asset: base,
        quote_asset: quote,
        price_precision: 8,
        quantity_precision: 8,
        tick_size: 0.00000001,
        step_size: 0.00000001,
        min_quantity: 0.0,
        min_notional: 0.0,
        max_quantity: None,
        supported_order_types: vec![OrderType::Market, OrderType::Limit, OrderType::IOC],
        supported_time_in_force: vec![TimeInForce::GTC, TimeInForce::IOC],
        status: SymbolStatus::Trading,
        raw_metadata: None,
    }
}

pub fn spot_symbol_from_rule(rule: &SymbolRule) -> Option<SpotSymbol> {
    (rule.market_type == MarketType::Spot).then(|| SpotSymbol {
        exchange: rule.exchange.clone(),
        instrument_id: format!("{}:spot:{}", rule.exchange, rule.internal_symbol),
        internal_symbol: rule.internal_symbol.clone(),
        exchange_symbol: rule.exchange_symbol.clone(),
        base_asset: rule.base_asset.clone(),
        quote_asset: rule.quote_asset.clone(),
    })
}

pub fn perpetual_symbol_from_rule(rule: &SymbolRule) -> Option<PerpetualSymbol> {
    if rule.market_type != MarketType::Perpetual {
        return None;
    }
    let metadata = rule.raw_metadata.as_ref();
    let settlement_asset = metadata
        .and_then(|value| value.get("settlement_asset"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or(&rule.quote_asset)
        .to_ascii_uppercase();
    let instrument_type = metadata
        .and_then(|value| value.get("instrument_type"))
        .and_then(serde_json::Value::as_str)
        .map(parse_instrument_type)
        .unwrap_or(InstrumentType::LinearPerpetual);
    let contract_size = metadata
        .and_then(|value| value.get("contract_size"))
        .and_then(serde_json::Value::as_f64);
    Some(PerpetualSymbol {
        exchange: rule.exchange.clone(),
        instrument_id: format!("{}:perpetual:{}", rule.exchange, rule.internal_symbol),
        internal_symbol: rule.internal_symbol.clone(),
        exchange_symbol: rule.exchange_symbol.clone(),
        base_asset: rule.base_asset.clone(),
        quote_asset: rule.quote_asset.clone(),
        settlement_asset,
        instrument_type,
        contract_size,
    })
}

fn parse_instrument_type(value: &str) -> InstrumentType {
    match value.trim().to_ascii_lowercase().as_str() {
        "spot" => InstrumentType::Spot,
        "linear_perpetual" | "linearperpetual" | "linear_perp" | "swap" => {
            InstrumentType::LinearPerpetual
        }
        "inverse_perpetual" | "inverseperpetual" | "inverse_perp" => {
            InstrumentType::InversePerpetual
        }
        "delivery_future" | "future" | "futures" => InstrumentType::DeliveryFuture,
        _ => InstrumentType::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchanges::unified::{OrderSide, PositionSide};

    fn rule(
        exchange: &str,
        market_type: MarketType,
        internal: &str,
        exchange_symbol: &str,
    ) -> SymbolRule {
        let mut rule = infer_spot_rule(exchange, internal, exchange_symbol, "USDT");
        rule.market_type = market_type;
        rule.tick_size = 0.01;
        rule.step_size = 0.001;
        rule.min_quantity = 0.001;
        rule.min_notional = 5.0;
        rule
    }

    fn limit_order(symbol: &str) -> OrderRequest {
        OrderRequest {
            market_type: MarketType::Spot,
            symbol: symbol.to_string(),
            side: OrderSide::Buy,
            position_side: PositionSide::None,
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: 0.01,
            price: Some(50_000.0),
            client_order_id: Some("MX_SPT_ARB_1".to_string()),
            reduce_only: false,
        }
    }

    #[test]
    fn spot_and_perpetual_mappings_should_round_trip() {
        let mut registry = UnifiedSymbolRegistry::from_rules([
            rule("mexc", MarketType::Spot, "BTCUSDT", "BTCUSDT"),
            rule("coinex", MarketType::Spot, "BTCUSDT", "BTCUSDT"),
            rule("okx", MarketType::Spot, "BTCUSDT", "BTC-USDT"),
            rule("okx", MarketType::Perpetual, "BTCUSDT", "BTC-USDT-SWAP"),
            rule("gateio", MarketType::Spot, "BTCUSDT", "BTC_USDT"),
        ]);
        registry.register_mapping(ExchangeSymbolMapping {
            exchange: "okx".to_string(),
            market_type: MarketType::Spot,
            internal_symbol: "ETHUSDT".to_string(),
            exchange_symbol: "ETH-USDT".to_string(),
        });

        assert_eq!(
            registry
                .internal_to_exchange("okx", MarketType::Spot, "BTCUSDT")
                .unwrap(),
            "BTC-USDT"
        );
        assert_eq!(
            registry
                .internal_to_exchange("okx", MarketType::Perpetual, "BTCUSDT")
                .unwrap(),
            "BTC-USDT-SWAP"
        );
        assert_eq!(
            registry
                .internal_to_exchange("gateio", MarketType::Spot, "BTCUSDT")
                .unwrap(),
            "BTC_USDT"
        );
        assert_eq!(
            registry
                .internal_to_exchange("mexc", MarketType::Spot, "BTCUSDT")
                .unwrap(),
            "BTCUSDT"
        );
        assert_eq!(
            registry
                .internal_to_exchange("coinex", MarketType::Spot, "BTCUSDT")
                .unwrap(),
            "BTCUSDT"
        );
        assert_eq!(
            registry
                .exchange_to_internal("okx", MarketType::Spot, "BTC-USDT")
                .unwrap(),
            "BTCUSDT"
        );
    }

    #[test]
    fn unknown_and_suspended_symbols_are_rejected() {
        let mut suspended = rule("mexc", MarketType::Spot, "BTCUSDT", "BTCUSDT");
        suspended.status = SymbolStatus::Suspended;
        let registry = UnifiedSymbolRegistry::from_rules([suspended]);

        assert!(registry
            .internal_to_exchange("mexc", MarketType::Spot, "ETHUSDT")
            .is_err());
        let error = registry
            .validate_order(&limit_order("BTCUSDT"))
            .unwrap_err();
        assert!(matches!(error, SymbolRegistryError::Validation(_)));
    }

    #[test]
    fn validation_should_reject_min_notional_tick_step_and_client_id() {
        let registry = UnifiedSymbolRegistry::from_rules([rule(
            "mexc",
            MarketType::Spot,
            "BTCUSDT",
            "BTCUSDT",
        )]);
        let mut order = limit_order("BTCUSDT");
        order.price = Some(50_000.005);
        assert!(registry.validate_order(&order).is_err());

        order.price = Some(50_000.0);
        order.quantity = 0.0005;
        assert!(registry.validate_order(&order).is_err());

        order.quantity = 0.001;
        order.price = Some(1.0);
        assert!(registry.validate_order(&order).is_err());

        order.price = Some(50_000.0);
        order.quantity = 0.01;
        order.client_order_id = Some("bad/id".to_string());
        assert!(registry.validate_order(&order).is_err());
    }

    #[test]
    fn exchange_scoped_validation_should_use_matching_client_id_policy() {
        let registry = UnifiedSymbolRegistry::from_rules([
            rule("mexc", MarketType::Spot, "BTCUSDT", "BTCUSDT"),
            rule("okx", MarketType::Spot, "BTCUSDT", "BTC-USDT"),
        ]);
        let mut order = limit_order("BTCUSDT");
        order.client_order_id = Some("OKX_SPT_ARB_1".to_string());

        registry
            .validate_order_for_exchange("okx", &order)
            .expect("OKX policy should accept OKX-prefixed uppercase id");
        assert!(registry.validate_order_for_exchange("mexc", &order).is_ok());
    }

    #[test]
    fn perpetual_contract_metadata_can_be_stored_in_raw_rule_metadata() {
        let mut perp = rule("okx", MarketType::Perpetual, "BTCUSDT", "BTC-USDT-SWAP");
        perp.raw_metadata = Some(serde_json::json!({
            "instrument_type": "linear_perpetual",
            "settlement_asset": "USDT",
            "contract_size": 0.01
        }));
        let registry = UnifiedSymbolRegistry::from_rules([perp]);
        let loaded = registry
            .get_rule("okx", MarketType::Perpetual, "BTCUSDT")
            .unwrap();
        assert_eq!(
            loaded.raw_metadata.as_ref().unwrap()["settlement_asset"],
            "USDT"
        );
        let symbol = perpetual_symbol_from_rule(loaded).unwrap();
        assert_eq!(symbol.instrument_type, InstrumentType::LinearPerpetual);
        assert_eq!(symbol.settlement_asset, "USDT");
        assert_eq!(symbol.contract_size, Some(0.01));
    }

    #[test]
    fn spot_symbol_projection_should_preserve_mapping_fields() {
        let rule = rule("gateio", MarketType::Spot, "BTCUSDT", "BTC_USDT");
        let spot = spot_symbol_from_rule(&rule).unwrap();
        assert_eq!(spot.exchange, "gateio");
        assert_eq!(spot.internal_symbol, "BTCUSDT");
        assert_eq!(spot.exchange_symbol, "BTC_USDT");
        assert_eq!(
            SymbolInstrument::Spot(spot).symbol_key(),
            SymbolKey::new("gateio", MarketType::Spot, "BTCUSDT")
        );
    }
}
