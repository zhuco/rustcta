use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{SpotFeeSource, SpotSpotTakerArbitrageConfig};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct SpotFeeRate {
    pub maker_bps: f64,
    pub taker_bps: f64,
    pub source: SpotFeeSource,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpotFeeLookup {
    pub exchange: String,
    pub maker_bps: f64,
    pub taker_bps: f64,
    pub source: SpotFeeSource,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpotFeeModel {
    default_taker_bps: f64,
    venue_rates: BTreeMap<String, SpotFeeRate>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SpotFeeOverride {
    pub maker_bps: Option<f64>,
    pub taker_bps: Option<f64>,
    pub source: Option<SpotFeeSource>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SpotFeeOverrides {
    venue_rates: BTreeMap<String, SpotFeeOverride>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct EstimatedPairFees {
    pub buy_taker_bps: f64,
    pub sell_taker_bps: f64,
    pub total_taker_bps: f64,
}

impl SpotFeeModel {
    pub fn from_strategy_config(config: &SpotSpotTakerArbitrageConfig) -> Self {
        let default_taker_bps = config.taker_fee_bps_override.unwrap_or(10.0);
        let venue_rates = ["mexc", "coinex", "gateio", "bitget"]
            .into_iter()
            .map(|exchange| {
                let taker_bps = config.taker_fee_bps(exchange);
                let maker_bps = config
                    .venue_fee_override(exchange)
                    .map(|fee| fee.maker_fee_rate * 10_000.0)
                    .unwrap_or(taker_bps);
                let source = if config.taker_fee_bps_override.is_some()
                    || (taker_bps - default_taker_bps).abs() > f64::EPSILON
                {
                    SpotFeeSource::Config
                } else {
                    SpotFeeSource::Default
                };
                (
                    exchange.to_string(),
                    SpotFeeRate {
                        maker_bps,
                        taker_bps,
                        source,
                    },
                )
            })
            .collect();
        Self {
            default_taker_bps,
            venue_rates,
        }
    }

    pub fn from_strategy_config_and_account_fees(
        config: &SpotSpotTakerArbitrageConfig,
        overrides: &SpotFeeOverrides,
    ) -> Self {
        let mut model = Self::from_strategy_config(config);
        for (exchange, override_rate) in &overrides.venue_rates {
            let current = model.lookup(exchange);
            model.venue_rates.insert(
                normalize_exchange(exchange),
                SpotFeeRate {
                    maker_bps: override_rate.maker_bps.unwrap_or(current.maker_bps),
                    taker_bps: override_rate.taker_bps.unwrap_or(current.taker_bps),
                    source: override_rate.source.unwrap_or(SpotFeeSource::Exchange),
                },
            );
        }
        model
    }

    pub fn lookup(&self, exchange: &str) -> SpotFeeLookup {
        let key = normalize_exchange(exchange);
        let rate = self.venue_rates.get(&key).copied().unwrap_or(SpotFeeRate {
            maker_bps: self.default_taker_bps,
            taker_bps: self.default_taker_bps,
            source: SpotFeeSource::Default,
        });
        SpotFeeLookup {
            exchange: key,
            maker_bps: rate.maker_bps,
            taker_bps: rate.taker_bps,
            source: rate.source,
        }
    }

    pub fn buy_sell_taker_bps(&self, buy_exchange: &str, sell_exchange: &str) -> (f64, f64) {
        (
            self.lookup(buy_exchange).taker_bps,
            self.lookup(sell_exchange).taker_bps,
        )
    }

    pub fn estimate_pair_fees(&self, buy_exchange: &str, sell_exchange: &str) -> EstimatedPairFees {
        let (buy_taker_bps, sell_taker_bps) = self.buy_sell_taker_bps(buy_exchange, sell_exchange);
        EstimatedPairFees {
            buy_taker_bps,
            sell_taker_bps,
            total_taker_bps: buy_taker_bps + sell_taker_bps,
        }
    }
}

impl SpotFeeOverrides {
    pub fn insert(&mut self, exchange: impl Into<String>, override_rate: SpotFeeOverride) {
        self.venue_rates
            .insert(normalize_exchange(&exchange.into()), override_rate);
    }

    pub fn from_account_fee_lookups(
        lookups: impl IntoIterator<Item = SpotFeeLookup>,
    ) -> SpotFeeOverrides {
        let mut overrides = SpotFeeOverrides::default();
        for lookup in lookups {
            overrides.insert(
                lookup.exchange,
                SpotFeeOverride {
                    maker_bps: Some(lookup.maker_bps),
                    taker_bps: Some(lookup.taker_bps),
                    source: Some(lookup.source),
                },
            );
        }
        overrides
    }
}

pub fn fee_model_from_strategy_config(config: &SpotSpotTakerArbitrageConfig) -> SpotFeeModel {
    SpotFeeModel::from_strategy_config(config)
}

fn normalize_exchange(exchange: &str) -> String {
    match exchange.trim().to_ascii_lowercase().as_str() {
        "gate" | "gate.io" => "gateio".to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fee_model_should_use_strategy_overrides_without_exchange_adapters() {
        let mut config = minimal_config();
        config.gateio.fee_override = Some(crate::VenueFeeOverride {
            maker_fee_rate: 0.0004,
            taker_fee_rate: 0.0009,
        });

        let model = SpotFeeModel::from_strategy_config(&config);

        assert_eq!(model.lookup("gate.io").exchange, "gateio");
        assert_eq!(model.lookup("gate.io").maker_bps, 4.0);
        assert_eq!(model.lookup("gate.io").taker_bps, 9.0);
        assert_eq!(model.lookup("bitget").taker_bps, 10.0);
        assert_eq!(model.buy_sell_taker_bps("gateio", "bitget"), (9.0, 10.0));
        assert_eq!(
            model.estimate_pair_fees("gateio", "bitget").total_taker_bps,
            19.0
        );
    }

    #[test]
    fn fee_model_should_prefer_global_taker_override() {
        let mut config = minimal_config();
        config.taker_fee_bps_override = Some(7.5);

        let model = fee_model_from_strategy_config(&config);

        assert_eq!(model.lookup("mexc").taker_bps, 7.5);
        assert_eq!(model.lookup("unknown").taker_bps, 7.5);
    }

    #[test]
    fn fee_model_should_prefer_account_fee_overrides() {
        let mut account_fees = SpotFeeOverrides::default();
        account_fees.insert(
            "gate.io",
            SpotFeeOverride {
                maker_bps: Some(2.5),
                taker_bps: Some(6.0),
                source: Some(SpotFeeSource::Exchange),
            },
        );

        let model =
            SpotFeeModel::from_strategy_config_and_account_fees(&minimal_config(), &account_fees);

        let gate = model.lookup("gateio");
        assert_eq!(gate.maker_bps, 2.5);
        assert_eq!(gate.taker_bps, 6.0);
        assert_eq!(gate.source, SpotFeeSource::Exchange);
    }

    fn minimal_config() -> SpotSpotTakerArbitrageConfig {
        serde_yaml::from_str(
            "exchanges: [gateio, bitget]\n\
             symbols: [BTCUSDT]\n\
             max_notional_per_trade: 10\n\
             min_notional_per_trade: 1\n\
             max_notional_per_symbol: 100\n\
             max_total_notional: 100\n\
             min_net_spread_bps: 0\n\
             min_depth_notional: 1\n",
        )
        .expect("minimal config")
    }
}
