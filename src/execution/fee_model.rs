use std::collections::HashMap;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::exchanges::unified::{MarketType, OrderSide};
use crate::utils::money;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeeRole {
    Maker,
    Taker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeeSource {
    ExchangeApi,
    ConfigDefault,
    SymbolOverride,
    VipOverride,
    PlatformTokenDiscount,
    Fallback,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeeAssetMode {
    Quote,
    Base,
    PlatformToken,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlatformTokenDiscount {
    pub exchange: String,
    pub token: String,
    #[serde(default)]
    pub enabled: bool,
    pub discount_multiplier: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeeRate {
    pub exchange: String,
    pub market_type: MarketType,
    pub symbol: Option<String>,
    pub maker_fee_bps: f64,
    pub taker_fee_bps: f64,
    pub fee_asset: Option<String>,
    #[serde(default)]
    pub rebate_ratio: Option<f64>,
    pub platform_token: Option<String>,
    #[serde(default)]
    pub platform_discount_enabled: bool,
    pub discount_multiplier: Option<f64>,
    pub source: FeeSource,
    pub updated_at: DateTime<Utc>,
}

impl FeeRate {
    pub fn fee_bps(&self, role: FeeRole) -> f64 {
        match role {
            FeeRole::Maker => self.maker_fee_bps,
            FeeRole::Taker => self.taker_fee_bps,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FeeLookupKey {
    pub exchange: String,
    pub market_type: MarketType,
    pub symbol: Option<String>,
    pub liquidity_role: FeeRole,
}

impl FeeLookupKey {
    pub fn spot_taker(exchange: impl Into<String>, symbol: impl Into<String>) -> Self {
        Self {
            exchange: exchange.into(),
            market_type: MarketType::Spot,
            symbol: Some(symbol.into()),
            liquidity_role: FeeRole::Taker,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeeLookupResult {
    pub raw_rate: FeeRate,
    pub effective_rate: FeeRate,
    pub fee_bps: f64,
    pub raw_fee_bps: f64,
    pub platform_discount_applied: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeeCalculation {
    pub exchange: String,
    pub market_type: MarketType,
    pub symbol: Option<String>,
    pub role: FeeRole,
    pub fee_asset_mode: FeeAssetMode,
    pub fee_asset: Option<String>,
    pub notional: f64,
    pub raw_fee_bps: f64,
    pub effective_fee_bps: f64,
    pub fee_amount: f64,
    pub source: FeeSource,
    pub platform_discount_applied: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RoundTripFeeCalculation {
    pub buy_fee: FeeCalculation,
    pub sell_fee: FeeCalculation,
    pub gross_pnl: f64,
    pub net_pnl: f64,
    pub total_fee: f64,
    pub total_fee_bps: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FeeConfigFile {
    pub fees: FeeConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeeConfig {
    #[serde(default)]
    pub fallback: HashMap<String, FeePairConfig>,
    #[serde(default)]
    pub defaults: HashMap<String, HashMap<String, FeePairConfig>>,
    #[serde(default)]
    pub symbol_overrides: Vec<SymbolFeeOverride>,
    #[serde(default)]
    pub side_overrides: Vec<SideFeeOverride>,
    #[serde(default)]
    pub vip_overrides: Vec<SymbolFeeOverride>,
    #[serde(default)]
    pub exchange_api: Vec<SymbolFeeOverride>,
    #[serde(default)]
    pub platform_tokens: Vec<PlatformTokenDiscount>,
    #[serde(default)]
    pub prefer_exchange_api_fees: bool,
}

impl Default for FeeConfig {
    fn default() -> Self {
        let mut fallback = HashMap::new();
        fallback.insert(
            "spot".to_string(),
            FeePairConfig {
                maker_bps: 20.0,
                taker_bps: 20.0,
                fee_asset: Some("quote".to_string()),
                rebate_ratio: None,
            },
        );
        fallback.insert(
            "perpetual".to_string(),
            FeePairConfig {
                maker_bps: 2.0,
                taker_bps: 5.0,
                fee_asset: Some("quote".to_string()),
                rebate_ratio: None,
            },
        );

        let defaults = HashMap::from([
            (
                "mexc".to_string(),
                HashMap::from([
                    (
                        "spot".to_string(),
                        FeePairConfig {
                            maker_bps: 0.0,
                            taker_bps: 5.0,
                            fee_asset: Some("quote".to_string()),
                            rebate_ratio: None,
                        },
                    ),
                    (
                        "perpetual".to_string(),
                        FeePairConfig {
                            maker_bps: 0.0,
                            taker_bps: 2.0,
                            fee_asset: Some("quote".to_string()),
                            rebate_ratio: None,
                        },
                    ),
                ]),
            ),
            (
                "coinex".to_string(),
                HashMap::from([
                    (
                        "spot".to_string(),
                        FeePairConfig {
                            maker_bps: 20.0,
                            taker_bps: 20.0,
                            fee_asset: Some("quote".to_string()),
                            rebate_ratio: None,
                        },
                    ),
                    (
                        "perpetual".to_string(),
                        FeePairConfig {
                            maker_bps: 3.0,
                            taker_bps: 5.0,
                            fee_asset: Some("quote".to_string()),
                            rebate_ratio: None,
                        },
                    ),
                ]),
            ),
            (
                "gateio".to_string(),
                HashMap::from([
                    (
                        "spot".to_string(),
                        FeePairConfig {
                            maker_bps: 10.0,
                            taker_bps: 10.0,
                            fee_asset: Some("quote".to_string()),
                            rebate_ratio: None,
                        },
                    ),
                    (
                        "perpetual".to_string(),
                        FeePairConfig {
                            maker_bps: 2.0,
                            taker_bps: 5.0,
                            fee_asset: Some("quote".to_string()),
                            rebate_ratio: None,
                        },
                    ),
                ]),
            ),
            (
                "bitget".to_string(),
                HashMap::from([
                    (
                        "spot".to_string(),
                        FeePairConfig {
                            maker_bps: 10.0,
                            taker_bps: 10.0,
                            fee_asset: Some("quote".to_string()),
                            rebate_ratio: None,
                        },
                    ),
                    (
                        "perpetual".to_string(),
                        FeePairConfig {
                            maker_bps: 2.0,
                            taker_bps: 6.0,
                            fee_asset: Some("quote".to_string()),
                            rebate_ratio: None,
                        },
                    ),
                ]),
            ),
            (
                "kucoin".to_string(),
                HashMap::from([
                    (
                        "spot".to_string(),
                        FeePairConfig {
                            maker_bps: 10.0,
                            taker_bps: 10.0,
                            fee_asset: Some("quote".to_string()),
                            rebate_ratio: None,
                        },
                    ),
                    (
                        "perpetual".to_string(),
                        FeePairConfig {
                            maker_bps: 2.0,
                            taker_bps: 6.0,
                            fee_asset: Some("quote".to_string()),
                            rebate_ratio: None,
                        },
                    ),
                ]),
            ),
            (
                "binance".to_string(),
                HashMap::from([
                    (
                        "spot".to_string(),
                        FeePairConfig {
                            maker_bps: 10.0,
                            taker_bps: 10.0,
                            fee_asset: Some("quote".to_string()),
                            rebate_ratio: None,
                        },
                    ),
                    (
                        "perpetual".to_string(),
                        FeePairConfig {
                            maker_bps: 2.0,
                            taker_bps: 5.0,
                            fee_asset: Some("quote".to_string()),
                            rebate_ratio: None,
                        },
                    ),
                ]),
            ),
            (
                "okx".to_string(),
                HashMap::from([
                    (
                        "spot".to_string(),
                        FeePairConfig {
                            maker_bps: 8.0,
                            taker_bps: 10.0,
                            fee_asset: Some("quote".to_string()),
                            rebate_ratio: None,
                        },
                    ),
                    (
                        "perpetual".to_string(),
                        FeePairConfig {
                            maker_bps: 2.0,
                            taker_bps: 5.0,
                            fee_asset: Some("quote".to_string()),
                            rebate_ratio: None,
                        },
                    ),
                ]),
            ),
        ]);

        Self {
            fallback,
            defaults,
            symbol_overrides: Vec::new(),
            side_overrides: Vec::new(),
            vip_overrides: Vec::new(),
            exchange_api: Vec::new(),
            platform_tokens: Vec::new(),
            prefer_exchange_api_fees: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeePairConfig {
    pub maker_bps: f64,
    pub taker_bps: f64,
    #[serde(default)]
    pub fee_asset: Option<String>,
    #[serde(default)]
    pub rebate_ratio: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolFeeOverride {
    pub exchange: String,
    pub market_type: String,
    #[serde(default)]
    pub symbol: Option<String>,
    pub maker_bps: f64,
    pub taker_bps: f64,
    #[serde(default)]
    pub fee_asset: Option<String>,
    #[serde(default)]
    pub rebate_ratio: Option<f64>,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SideFeeOverride {
    pub exchange: String,
    pub market_type: String,
    pub side: OrderSide,
    #[serde(default)]
    pub symbol: Option<String>,
    pub maker_bps: f64,
    pub taker_bps: f64,
    #[serde(default)]
    pub fee_asset: Option<String>,
    #[serde(default)]
    pub rebate_ratio: Option<f64>,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct FeeModel {
    fallback: HashMap<MarketType, FeePairConfig>,
    defaults: HashMap<(String, MarketType), FeePairConfig>,
    symbol_overrides: HashMap<(String, MarketType, String), FeePairConfig>,
    side_overrides: HashMap<(String, MarketType, OrderSide, Option<String>), FeePairConfig>,
    vip_overrides: HashMap<(String, MarketType, Option<String>), FeePairConfig>,
    exchange_api: HashMap<(String, MarketType, Option<String>), FeePairConfig>,
    platform_tokens: HashMap<String, PlatformTokenDiscount>,
    prefer_exchange_api_fees: bool,
}

impl Default for FeeModel {
    fn default() -> Self {
        Self::from_config(FeeConfig::default())
    }
}

impl FeeModel {
    pub fn from_config(config: FeeConfig) -> Self {
        let fallback = config
            .fallback
            .into_iter()
            .filter_map(|(market, fees)| parse_market_type(&market).map(|market| (market, fees)))
            .collect();
        let defaults = config
            .defaults
            .into_iter()
            .flat_map(|(exchange, markets)| {
                markets.into_iter().filter_map(move |(market, fees)| {
                    parse_market_type(&market)
                        .map(|market| (normalize_exchange(&exchange), market, fees))
                })
            })
            .map(|(exchange, market, fees)| ((exchange, market), fees))
            .collect();
        let symbol_overrides = config
            .symbol_overrides
            .into_iter()
            .filter_map(|item| override_key(item).map(|(key, fees)| (key.require_symbol(), fees)))
            .collect();
        let side_overrides = config
            .side_overrides
            .into_iter()
            .filter_map(side_override_key)
            .collect();
        let vip_overrides = config
            .vip_overrides
            .into_iter()
            .filter_map(override_key)
            .map(|(key, fees)| ((key.0, key.1, key.2), fees))
            .collect();
        let exchange_api = config
            .exchange_api
            .into_iter()
            .filter_map(override_key)
            .map(|(key, fees)| ((key.0, key.1, key.2), fees))
            .collect();
        let platform_tokens = config
            .platform_tokens
            .into_iter()
            .map(|token| (normalize_exchange(&token.exchange), normalize_token(token)))
            .collect();

        Self {
            fallback,
            defaults,
            symbol_overrides,
            side_overrides,
            vip_overrides,
            exchange_api,
            platform_tokens,
            prefer_exchange_api_fees: config.prefer_exchange_api_fees,
        }
    }

    pub fn load_from_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = fs::read_to_string(path)
            .with_context(|| format!("failed to read fee config {}", path.display()))?;
        let config = serde_yaml::from_str::<FeeConfigFile>(&text)
            .map(|file| file.fees)
            .or_else(|_| serde_yaml::from_str::<FeeConfig>(&text))
            .with_context(|| format!("failed to parse fee config {}", path.display()))?;
        Ok(Self::from_config(config))
    }

    pub fn load_or_default(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref();
        if !path.exists() {
            log::warn!(
                "fee config {} missing; using conservative default/fallback fees",
                path.display()
            );
            return Self::default();
        }
        match Self::load_from_path(path) {
            Ok(model) => model,
            Err(error) => {
                log::warn!(
                    "fee config {} could not be loaded: {}; using conservative fallback fees",
                    path.display(),
                    error
                );
                Self::default()
            }
        }
    }

    pub fn lookup(&self, key: &FeeLookupKey) -> FeeLookupResult {
        self.lookup_for_side(key, None)
    }

    pub fn lookup_for_side(&self, key: &FeeLookupKey, side: Option<OrderSide>) -> FeeLookupResult {
        let raw_rate = self.raw_rate(key, side);
        let mut effective_rate = raw_rate.clone();
        let mut platform_discount_applied = false;
        if let Some(discount) = self.platform_discount(&key.exchange) {
            if discount.enabled {
                let multiplier = discount.discount_multiplier.unwrap_or(1.0);
                effective_rate.maker_fee_bps *= multiplier;
                effective_rate.taker_fee_bps *= multiplier;
                effective_rate.platform_discount_enabled = true;
                effective_rate.platform_token = Some(discount.token.clone());
                effective_rate.discount_multiplier = Some(multiplier);
                effective_rate.source = FeeSource::PlatformTokenDiscount;
                platform_discount_applied = true;
            }
        }
        if let Some(rebate_ratio) = raw_rate.rebate_ratio {
            let multiplier = (1.0 - rebate_ratio.clamp(0.0, 1.0)).max(0.0);
            effective_rate.maker_fee_bps *= multiplier;
            effective_rate.taker_fee_bps *= multiplier;
        }
        FeeLookupResult {
            fee_bps: effective_rate.fee_bps(key.liquidity_role),
            raw_fee_bps: raw_rate.fee_bps(key.liquidity_role),
            raw_rate,
            effective_rate,
            platform_discount_applied,
        }
    }

    pub fn calculate_fee(&self, key: &FeeLookupKey, notional: f64) -> FeeCalculation {
        self.calculate_fee_for_side(key, None, notional)
    }

    pub fn calculate_fee_for_side(
        &self,
        key: &FeeLookupKey,
        side: Option<OrderSide>,
        notional: f64,
    ) -> FeeCalculation {
        let lookup = self.lookup_for_side(key, side);
        let fee_amount = money::fee_amount_f64(notional, lookup.fee_bps)
            .unwrap_or_else(|| notional.max(0.0) * lookup.fee_bps / 10_000.0);
        FeeCalculation {
            exchange: normalize_exchange(&key.exchange),
            market_type: key.market_type,
            symbol: key.symbol.as_ref().map(|symbol| normalize_symbol(symbol)),
            role: key.liquidity_role,
            fee_asset_mode: fee_asset_mode(lookup.effective_rate.fee_asset.as_deref()),
            fee_asset: lookup.effective_rate.fee_asset.clone(),
            notional,
            raw_fee_bps: lookup.raw_fee_bps,
            effective_fee_bps: lookup.fee_bps,
            fee_amount,
            source: lookup.effective_rate.source,
            platform_discount_applied: lookup.platform_discount_applied,
        }
    }

    pub fn calculate_buy_sell(
        &self,
        buy_key: &FeeLookupKey,
        sell_key: &FeeLookupKey,
        buy_notional: f64,
        sell_notional: f64,
    ) -> RoundTripFeeCalculation {
        let buy_fee = self.calculate_fee_for_side(buy_key, Some(OrderSide::Buy), buy_notional);
        let sell_fee = self.calculate_fee_for_side(sell_key, Some(OrderSide::Sell), sell_notional);
        let gross_pnl =
            money::subtract_f64(sell_notional, buy_notional, "sell_notional", "buy_notional")
                .unwrap_or(sell_notional - buy_notional);
        let total_fee = money::add_f64(
            buy_fee.fee_amount,
            sell_fee.fee_amount,
            "buy_fee",
            "sell_fee",
        )
        .unwrap_or(buy_fee.fee_amount + sell_fee.fee_amount);
        let net_pnl = money::subtract_f64(gross_pnl, total_fee, "gross_pnl", "total_fee")
            .unwrap_or(gross_pnl - total_fee);
        let total_fee_bps = if buy_notional > 0.0 {
            money::divide_f64(total_fee, buy_notional, "total_fee", "buy_notional")
                .and_then(|value| money::multiply_f64(value, 10_000.0, "fee_ratio", "bps_scale"))
                .unwrap_or(total_fee / buy_notional * 10_000.0)
        } else {
            0.0
        };
        RoundTripFeeCalculation {
            buy_fee,
            sell_fee,
            gross_pnl,
            net_pnl,
            total_fee,
            total_fee_bps,
        }
    }

    pub fn with_spot_taker_override(
        mut self,
        exchange: &str,
        symbol: Option<&str>,
        taker_bps: f64,
    ) -> Self {
        let pair = FeePairConfig {
            maker_bps: taker_bps,
            taker_bps,
            fee_asset: Some("quote".to_string()),
            rebate_ratio: None,
        };
        match symbol {
            Some(symbol) => {
                self.symbol_overrides.insert(
                    (
                        normalize_exchange(exchange),
                        MarketType::Spot,
                        normalize_symbol(symbol),
                    ),
                    pair,
                );
            }
            None => {
                self.defaults
                    .insert((normalize_exchange(exchange), MarketType::Spot), pair);
            }
        }
        self
    }

    pub fn summary_rates(&self) -> Vec<FeeRate> {
        let now = Utc::now();
        let mut rates = Vec::new();
        rates.extend(self.fallback.iter().map(|(market_type, fees)| {
            rate_from_pair(
                "fallback",
                *market_type,
                None,
                fees.clone(),
                FeeSource::Fallback,
                now,
            )
        }));
        rates.extend(self.defaults.iter().map(|((exchange, market_type), fees)| {
            rate_from_pair(
                exchange,
                *market_type,
                None,
                fees.clone(),
                FeeSource::ConfigDefault,
                now,
            )
        }));
        rates.extend(self.symbol_overrides.iter().map(
            |((exchange, market_type, symbol), fees)| {
                rate_from_pair(
                    exchange,
                    *market_type,
                    Some(symbol.clone()),
                    fees.clone(),
                    FeeSource::SymbolOverride,
                    now,
                )
            },
        ));
        rates.extend(
            self.vip_overrides
                .iter()
                .map(|((exchange, market_type, symbol), fees)| {
                    rate_from_pair(
                        exchange,
                        *market_type,
                        symbol.clone(),
                        fees.clone(),
                        FeeSource::VipOverride,
                        now,
                    )
                }),
        );
        rates.extend(
            self.exchange_api
                .iter()
                .map(|((exchange, market_type, symbol), fees)| {
                    rate_from_pair(
                        exchange,
                        *market_type,
                        symbol.clone(),
                        fees.clone(),
                        FeeSource::ExchangeApi,
                        now,
                    )
                }),
        );
        for rate in &mut rates {
            if let Some(discount) = self.platform_discount(&rate.exchange) {
                rate.platform_discount_enabled = discount.enabled;
                rate.platform_token = Some(discount.token.clone());
                rate.discount_multiplier = discount.discount_multiplier;
            }
        }
        rates.sort_by(|left, right| {
            (
                left.exchange.as_str(),
                format!("{:?}", left.market_type),
                left.symbol.as_deref().unwrap_or_default(),
                format!("{:?}", left.source),
            )
                .cmp(&(
                    right.exchange.as_str(),
                    format!("{:?}", right.market_type),
                    right.symbol.as_deref().unwrap_or_default(),
                    format!("{:?}", right.source),
                ))
        });
        rates
    }

    fn raw_rate(&self, key: &FeeLookupKey, side: Option<OrderSide>) -> FeeRate {
        let exchange = normalize_exchange(&key.exchange);
        let symbol = key.symbol.as_ref().map(|symbol| normalize_symbol(symbol));
        let now = Utc::now();

        if self.prefer_exchange_api_fees {
            if let Some(fees) = self
                .exchange_api
                .get(&(exchange.clone(), key.market_type, symbol.clone()))
                .or_else(|| {
                    self.exchange_api
                        .get(&(exchange.clone(), key.market_type, None))
                })
            {
                return rate_from_pair(
                    &exchange,
                    key.market_type,
                    symbol,
                    fees.clone(),
                    FeeSource::ExchangeApi,
                    now,
                );
            }
        }
        if let Some(symbol) = &symbol {
            if let Some(fees) =
                self.symbol_overrides
                    .get(&(exchange.clone(), key.market_type, symbol.clone()))
            {
                return rate_from_pair(
                    &exchange,
                    key.market_type,
                    Some(symbol.clone()),
                    fees.clone(),
                    FeeSource::SymbolOverride,
                    now,
                );
            }
        }
        if let Some(side) = side {
            if let Some(fees) = symbol
                .as_ref()
                .and_then(|symbol| {
                    self.side_overrides.get(&(
                        exchange.clone(),
                        key.market_type,
                        side,
                        Some(symbol.clone()),
                    ))
                })
                .or_else(|| {
                    self.side_overrides
                        .get(&(exchange.clone(), key.market_type, side, None))
                })
            {
                return rate_from_pair(
                    &exchange,
                    key.market_type,
                    symbol,
                    fees.clone(),
                    FeeSource::SymbolOverride,
                    now,
                );
            }
        }
        if let Some(fees) = self
            .vip_overrides
            .get(&(exchange.clone(), key.market_type, symbol.clone()))
            .or_else(|| {
                self.vip_overrides
                    .get(&(exchange.clone(), key.market_type, None))
            })
        {
            return rate_from_pair(
                &exchange,
                key.market_type,
                symbol,
                fees.clone(),
                FeeSource::VipOverride,
                now,
            );
        }
        if !self.prefer_exchange_api_fees {
            if let Some(fees) = self
                .exchange_api
                .get(&(exchange.clone(), key.market_type, symbol.clone()))
                .or_else(|| {
                    self.exchange_api
                        .get(&(exchange.clone(), key.market_type, None))
                })
            {
                return rate_from_pair(
                    &exchange,
                    key.market_type,
                    symbol,
                    fees.clone(),
                    FeeSource::ExchangeApi,
                    now,
                );
            }
        }
        if let Some(fees) = self.defaults.get(&(exchange.clone(), key.market_type)) {
            return rate_from_pair(
                &exchange,
                key.market_type,
                symbol,
                fees.clone(),
                FeeSource::ConfigDefault,
                now,
            );
        }
        let fees = self
            .fallback
            .get(&key.market_type)
            .cloned()
            .unwrap_or(FeePairConfig {
                maker_bps: 20.0,
                taker_bps: 20.0,
                fee_asset: Some("quote".to_string()),
                rebate_ratio: None,
            });
        log::warn!(
            "fee model fallback used exchange={} market={:?} symbol={:?}",
            exchange,
            key.market_type,
            symbol
        );
        rate_from_pair(
            &exchange,
            key.market_type,
            symbol,
            fees,
            FeeSource::Fallback,
            now,
        )
    }

    fn platform_discount(&self, exchange: &str) -> Option<&PlatformTokenDiscount> {
        self.platform_tokens.get(&normalize_exchange(exchange))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct OverrideKey(String, MarketType, Option<String>);

impl OverrideKey {
    fn require_symbol(self) -> (String, MarketType, String) {
        (self.0, self.1, self.2.unwrap_or_default())
    }
}

fn override_key(item: SymbolFeeOverride) -> Option<(OverrideKey, FeePairConfig)> {
    let market = parse_market_type(&item.market_type)?;
    let key = OverrideKey(
        normalize_exchange(&item.exchange),
        market,
        item.symbol.as_ref().map(|symbol| normalize_symbol(symbol)),
    );
    let fees = FeePairConfig {
        maker_bps: item.maker_bps,
        taker_bps: item.taker_bps,
        fee_asset: item.fee_asset,
        rebate_ratio: item.rebate_ratio,
    };
    Some((key, fees))
}

fn side_override_key(
    item: SideFeeOverride,
) -> Option<(
    (String, MarketType, OrderSide, Option<String>),
    FeePairConfig,
)> {
    let market = parse_market_type(&item.market_type)?;
    let key = (
        normalize_exchange(&item.exchange),
        market,
        item.side,
        item.symbol.as_ref().map(|symbol| normalize_symbol(symbol)),
    );
    let fees = FeePairConfig {
        maker_bps: item.maker_bps,
        taker_bps: item.taker_bps,
        fee_asset: item.fee_asset,
        rebate_ratio: item.rebate_ratio,
    };
    Some((key, fees))
}

fn rate_from_pair(
    exchange: &str,
    market_type: MarketType,
    symbol: Option<String>,
    fees: FeePairConfig,
    source: FeeSource,
    updated_at: DateTime<Utc>,
) -> FeeRate {
    FeeRate {
        exchange: exchange.to_string(),
        market_type,
        symbol,
        maker_fee_bps: fees.maker_bps,
        taker_fee_bps: fees.taker_bps,
        fee_asset: fees.fee_asset,
        rebate_ratio: fees.rebate_ratio,
        platform_token: None,
        platform_discount_enabled: false,
        discount_multiplier: None,
        source,
        updated_at,
    }
}

fn normalize_exchange(exchange: &str) -> String {
    exchange.trim().to_ascii_lowercase()
}

fn normalize_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .replace(['-', '_', '/'], "")
        .to_ascii_uppercase()
}

fn normalize_token(mut token: PlatformTokenDiscount) -> PlatformTokenDiscount {
    token.exchange = normalize_exchange(&token.exchange);
    token.token = token.token.trim().to_ascii_uppercase();
    token
}

fn parse_market_type(value: &str) -> Option<MarketType> {
    match value.trim().to_ascii_lowercase().as_str() {
        "spot" => Some(MarketType::Spot),
        "perpetual" | "perp" | "futures" | "future" => Some(MarketType::Perpetual),
        _ => None,
    }
}

fn fee_asset_mode(value: Option<&str>) -> FeeAssetMode {
    match value
        .unwrap_or("quote")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "quote" | "quote_asset" => FeeAssetMode::Quote,
        "base" | "base_asset" => FeeAssetMode::Base,
        "platform_token" | "platform" => FeeAssetMode::PlatformToken,
        _ => FeeAssetMode::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn model() -> FeeModel {
        let mut config = FeeConfig::default();
        config.symbol_overrides.push(SymbolFeeOverride {
            exchange: "mexc".to_string(),
            market_type: "spot".to_string(),
            symbol: Some("CUDISUSDT".to_string()),
            maker_bps: 0.0,
            taker_bps: 3.0,
            fee_asset: Some("quote".to_string()),
            rebate_ratio: None,
            reason: Some("test".to_string()),
        });
        config.side_overrides.push(SideFeeOverride {
            exchange: "gateio".to_string(),
            market_type: "spot".to_string(),
            side: OrderSide::Buy,
            symbol: Some("VSNUSDT".to_string()),
            maker_bps: 30.0,
            taker_bps: 30.0,
            fee_asset: Some("base".to_string()),
            rebate_ratio: Some(0.8),
            reason: Some("test".to_string()),
        });
        config.side_overrides.push(SideFeeOverride {
            exchange: "gateio".to_string(),
            market_type: "spot".to_string(),
            side: OrderSide::Sell,
            symbol: Some("VSNUSDT".to_string()),
            maker_bps: 10.0,
            taker_bps: 10.0,
            fee_asset: Some("quote".to_string()),
            rebate_ratio: Some(0.8),
            reason: Some("test".to_string()),
        });
        config.platform_tokens.push(PlatformTokenDiscount {
            exchange: "binance".to_string(),
            token: "BNB".to_string(),
            enabled: true,
            discount_multiplier: Some(0.75),
        });
        config.platform_tokens.push(PlatformTokenDiscount {
            exchange: "coinex".to_string(),
            token: "CET".to_string(),
            enabled: false,
            discount_multiplier: Some(0.8),
        });
        FeeModel::from_config(config)
    }

    #[test]
    fn fee_model_should_lookup_default_exchange_spot_fee() {
        let result = model().lookup(&FeeLookupKey::spot_taker("binance", "BTCUSDT"));
        assert_eq!(result.raw_fee_bps, 10.0);
        assert_eq!(result.raw_rate.source, FeeSource::ConfigDefault);
    }

    #[test]
    fn fee_model_should_lookup_default_exchange_perpetual_fee() {
        let key = FeeLookupKey {
            exchange: "okx".to_string(),
            market_type: MarketType::Perpetual,
            symbol: Some("BTCUSDT".to_string()),
            liquidity_role: FeeRole::Taker,
        };
        assert_eq!(model().lookup(&key).fee_bps, 5.0);
    }

    #[test]
    fn fee_model_symbol_override_should_take_priority() {
        let result = model().lookup(&FeeLookupKey::spot_taker("mexc", "CUDISUSDT"));
        assert_eq!(result.fee_bps, 3.0);
        assert_eq!(result.effective_rate.source, FeeSource::SymbolOverride);
    }

    #[test]
    fn fee_model_should_use_fallback_when_unknown() {
        let result = model().lookup(&FeeLookupKey::spot_taker("unknown", "BTCUSDT"));
        assert_eq!(result.fee_bps, 20.0);
        assert_eq!(result.effective_rate.source, FeeSource::Fallback);
    }

    #[test]
    fn fee_model_maker_fee_should_differ_from_taker_fee() {
        let maker = FeeLookupKey {
            liquidity_role: FeeRole::Maker,
            ..FeeLookupKey::spot_taker("okx", "BTCUSDT")
        };
        let taker = FeeLookupKey::spot_taker("okx", "BTCUSDT");
        assert_eq!(model().lookup(&maker).fee_bps, 8.0);
        assert_eq!(model().lookup(&taker).fee_bps, 10.0);
    }

    #[test]
    fn fee_model_platform_token_discount_should_apply_when_enabled() {
        let result = model().lookup(&FeeLookupKey::spot_taker("binance", "BTCUSDT"));
        assert_eq!(result.raw_fee_bps, 10.0);
        assert_eq!(result.fee_bps, 7.5);
        assert!(result.platform_discount_applied);
        assert_eq!(result.effective_rate.platform_token.as_deref(), Some("BNB"));
    }

    #[test]
    fn fee_model_platform_token_discount_should_not_apply_when_disabled() {
        let result = model().lookup(&FeeLookupKey::spot_taker("coinex", "BTCUSDT"));
        assert_eq!(result.fee_bps, 20.0);
        assert!(!result.platform_discount_applied);
    }

    #[test]
    fn fee_model_side_override_should_apply_rebate_after_platform_discount() {
        let result = model().lookup_for_side(
            &FeeLookupKey::spot_taker("gateio", "VSNUSDT"),
            Some(OrderSide::Buy),
        );
        assert_eq!(result.raw_fee_bps, 30.0);
        assert!((result.fee_bps - 6.0).abs() < 1e-12);
        assert_eq!(result.effective_rate.fee_asset.as_deref(), Some("base"));

        let sell = model().lookup_for_side(
            &FeeLookupKey::spot_taker("gateio", "VSNUSDT"),
            Some(OrderSide::Sell),
        );
        assert_eq!(sell.raw_fee_bps, 10.0);
        assert!((sell.fee_bps - 2.0).abs() < 1e-12);
        assert_eq!(sell.effective_rate.fee_asset.as_deref(), Some("quote"));
    }

    #[test]
    fn fee_model_calculation_should_report_gross_and_net_pnl() {
        let calc = model().calculate_buy_sell(
            &FeeLookupKey::spot_taker("mexc", "CUDISUSDT"),
            &FeeLookupKey::spot_taker("coinex", "CUDISUSDT"),
            100.0,
            101.0,
        );
        assert!((calc.gross_pnl - 1.0).abs() < 1e-12);
        assert!((calc.total_fee - 0.232).abs() < 1e-12);
        assert!((calc.net_pnl - 0.768).abs() < 1e-12);
        assert_eq!(calc.buy_fee.source, FeeSource::SymbolOverride);
    }
}
