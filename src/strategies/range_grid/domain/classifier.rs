use super::{
    config::{IndicatorConfig, SymbolConfig},
    model::{IndicatorSnapshot, MarketRegime, RegimeDecision},
};

/// 震荡/趋势判定器
pub struct RangeRegimeClassifier<'a> {
    pub symbol_config: &'a SymbolConfig,
    pub indicator_config: &'a IndicatorConfig,
}

impl<'a> RangeRegimeClassifier<'a> {
    pub fn new(symbol_config: &'a SymbolConfig, indicator_config: &'a IndicatorConfig) -> Self {
        Self {
            symbol_config,
            indicator_config,
        }
    }

    pub fn evaluate(
        &self,
        snapshot: &IndicatorSnapshot,
        current_regime: MarketRegime,
        grid_active: bool,
    ) -> RegimeDecision {
        let core_symbol = core_symbol_config(self.symbol_config);
        let core_indicator_config = core_indicator_config(self.indicator_config);
        let classifier = rustcta_strategy_range_grid::RangeRegimeClassifier::new(
            &core_symbol,
            &core_indicator_config,
        );
        let decision = classifier.evaluate(
            &core_indicator_snapshot(snapshot),
            core_market_regime(current_regime),
            grid_active,
        );
        regime_decision_from_core(decision)
    }
}

fn core_symbol_config(symbol: &SymbolConfig) -> rustcta_strategy_range_grid::SymbolConfig {
    rustcta_strategy_range_grid::SymbolConfig {
        config_id: symbol.config_id.clone(),
        enabled: symbol.enabled,
        account: rustcta_strategy_range_grid::AccountConfig {
            id: symbol.account.id.clone(),
            exchange: symbol.account.exchange.clone(),
            env_prefix: symbol.account.env_prefix.clone(),
        },
        symbol: symbol.symbol.clone(),
        precision: rustcta_strategy_range_grid::SymbolPrecision {
            price_digits: symbol.precision.price_digits,
            amount_digits: symbol.precision.amount_digits,
        },
        grid: rustcta_strategy_range_grid::GridConfig {
            grid_spacing_pct: symbol.grid.grid_spacing_pct,
            levels_per_side: symbol.grid.levels_per_side,
            base_order_notional: symbol.grid.base_order_notional,
            max_position_notional: symbol.grid.max_position_notional,
        },
        regime_filters: rustcta_strategy_range_grid::RegimeFilterConfig {
            min_liquidity_usd: symbol.regime_filters.min_liquidity_usd,
            require_conditions: symbol.regime_filters.require_conditions,
            adx_max: symbol.regime_filters.adx_max,
            bbw_quantile: symbol.regime_filters.bbw_quantile,
            slope_threshold: symbol.regime_filters.slope_threshold,
        },
        order: rustcta_strategy_range_grid::OrderConfig {
            post_only: symbol.order.post_only,
            tif: symbol.order.tif.clone(),
        },
        market_exit: rustcta_strategy_range_grid::MarketExitConfig {
            use_market_order: symbol.market_exit.use_market_order,
        },
    }
}

fn core_indicator_config(config: &IndicatorConfig) -> rustcta_strategy_range_grid::IndicatorConfig {
    rustcta_strategy_range_grid::IndicatorConfig {
        lower_timeframe: rustcta_strategy_range_grid::LowerTimeframeConfig {
            timeframe: config.lower_timeframe.timeframe.clone(),
            bollinger: rustcta_strategy_range_grid::BollingerConfig {
                length: config.lower_timeframe.bollinger.length,
                k: config.lower_timeframe.bollinger.k,
            },
            rsi: rustcta_strategy_range_grid::RsiConfig {
                length: config.lower_timeframe.rsi.length,
                overbought: config.lower_timeframe.rsi.overbought,
                oversold: config.lower_timeframe.rsi.oversold,
            },
            atr: rustcta_strategy_range_grid::AtrConfig {
                length: config.lower_timeframe.atr.length,
                stop_multiplier: config.lower_timeframe.atr.stop_multiplier,
                trail_multiplier: config.lower_timeframe.atr.trail_multiplier,
            },
        },
        higher_timeframe: rustcta_strategy_range_grid::HigherTimeframeConfig {
            timeframe: config.higher_timeframe.timeframe.clone(),
            adx_length: config.higher_timeframe.adx_length,
            adx_threshold: config.higher_timeframe.adx_threshold,
            bbw_window: config.higher_timeframe.bbw_window,
            bbw_quantile: config.higher_timeframe.bbw_quantile,
            slope_threshold: config.higher_timeframe.slope_threshold,
            choppiness: config
                .higher_timeframe
                .choppiness
                .as_ref()
                .map(|choppiness| rustcta_strategy_range_grid::ChoppinessConfig {
                    enabled: choppiness.enabled,
                    threshold: choppiness.threshold,
                }),
        },
    }
}

fn core_indicator_snapshot(
    snapshot: &IndicatorSnapshot,
) -> rustcta_strategy_range_grid::IndicatorSnapshot {
    rustcta_strategy_range_grid::IndicatorSnapshot {
        timestamp: snapshot.timestamp,
        last_price: snapshot.last_price,
        lower_band_ratio: snapshot.lower_band_ratio,
        z_score: snapshot.z_score,
        rsi: snapshot.rsi,
        atr: snapshot.atr,
        adx: snapshot.adx,
        bbw_percentile: snapshot.bbw_percentile,
        slope_metric: snapshot.slope_metric,
        choppiness: snapshot.choppiness,
    }
}

fn core_market_regime(regime: MarketRegime) -> rustcta_strategy_range_grid::MarketRegime {
    match regime {
        MarketRegime::Range => rustcta_strategy_range_grid::MarketRegime::Range,
        MarketRegime::TrendUp => rustcta_strategy_range_grid::MarketRegime::TrendUp,
        MarketRegime::TrendDown => rustcta_strategy_range_grid::MarketRegime::TrendDown,
        MarketRegime::Cooldown => rustcta_strategy_range_grid::MarketRegime::Cooldown,
        MarketRegime::Disabled => rustcta_strategy_range_grid::MarketRegime::Disabled,
    }
}

fn market_regime_from_core(regime: rustcta_strategy_range_grid::MarketRegime) -> MarketRegime {
    match regime {
        rustcta_strategy_range_grid::MarketRegime::Range => MarketRegime::Range,
        rustcta_strategy_range_grid::MarketRegime::TrendUp => MarketRegime::TrendUp,
        rustcta_strategy_range_grid::MarketRegime::TrendDown => MarketRegime::TrendDown,
        rustcta_strategy_range_grid::MarketRegime::Cooldown => MarketRegime::Cooldown,
        rustcta_strategy_range_grid::MarketRegime::Disabled => MarketRegime::Disabled,
    }
}

fn regime_decision_from_core(
    decision: rustcta_strategy_range_grid::RegimeDecision,
) -> RegimeDecision {
    RegimeDecision {
        new_regime: market_regime_from_core(decision.new_regime),
        activate_grid: decision.activate_grid,
        deactivate_grid: decision.deactivate_grid,
        reason: decision.reason,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategies::range_grid::domain::config::{
        AccountConfig, AtrConfig, BollingerConfig, GridConfig, HigherTimeframeConfig,
        IndicatorConfig, LowerTimeframeConfig, MarketExitConfig, OrderConfig, RegimeFilterConfig,
        RsiConfig, SymbolConfig, SymbolPrecision,
    };

    fn sample_symbol(require_conditions: usize) -> SymbolConfig {
        SymbolConfig {
            config_id: "TEST".to_string(),
            enabled: true,
            account: AccountConfig {
                id: "acc".to_string(),
                exchange: "binance".to_string(),
                env_prefix: "BIN".to_string(),
            },
            symbol: "BTCUSDT".to_string(),
            precision: SymbolPrecision {
                price_digits: Some(2),
                amount_digits: Some(3),
            },
            grid: GridConfig {
                grid_spacing_pct: 0.3,
                levels_per_side: 3,
                base_order_notional: 100.0,
                max_position_notional: 900.0,
            },
            regime_filters: RegimeFilterConfig {
                min_liquidity_usd: 1_000_000.0,
                require_conditions,
                adx_max: 25.0,
                bbw_quantile: 0.5,
                slope_threshold: 0.2,
            },
            order: OrderConfig {
                post_only: true,
                tif: Some("GTC".to_string()),
            },
            market_exit: MarketExitConfig {
                use_market_order: true,
            },
        }
    }

    fn sample_indicator_config() -> IndicatorConfig {
        IndicatorConfig {
            lower_timeframe: LowerTimeframeConfig {
                timeframe: "5m".to_string(),
                bollinger: BollingerConfig { length: 20, k: 2.0 },
                rsi: RsiConfig {
                    length: 14,
                    overbought: 70.0,
                    oversold: 30.0,
                },
                atr: AtrConfig {
                    length: 14,
                    stop_multiplier: 2.5,
                    trail_multiplier: 1.5,
                },
            },
            higher_timeframe: HigherTimeframeConfig {
                timeframe: "15m".to_string(),
                adx_length: 14,
                adx_threshold: 20.0,
                bbw_window: 400,
                bbw_quantile: 0.35,
                slope_threshold: 0.1,
                choppiness: None,
            },
        }
    }

    #[test]
    fn classifier_identifies_range() {
        let symbol_cfg = sample_symbol(3);
        let indicator_cfg = sample_indicator_config();
        let classifier = RangeRegimeClassifier::new(&symbol_cfg, &indicator_cfg);

        let snapshot = IndicatorSnapshot {
            timestamp: chrono::Utc::now(),
            last_price: 100.0,
            lower_band_ratio: 0.5,
            z_score: 0.1,
            rsi: 50.0,
            atr: 1.5,
            adx: 15.0,
            bbw_percentile: 0.2,
            slope_metric: 0.05,
            choppiness: Some(70.0),
        };

        let decision = classifier.evaluate(&snapshot, MarketRegime::TrendUp, false);
        assert_eq!(decision.new_regime, MarketRegime::Range);
        assert!(decision.activate_grid);
        assert!(!decision.deactivate_grid);
    }

    #[test]
    fn classifier_identifies_trend() {
        let symbol_cfg = sample_symbol(2);
        let indicator_cfg = sample_indicator_config();
        let classifier = RangeRegimeClassifier::new(&symbol_cfg, &indicator_cfg);

        let snapshot = IndicatorSnapshot {
            timestamp: chrono::Utc::now(),
            last_price: 100.0,
            lower_band_ratio: 0.9,
            z_score: 3.0,
            rsi: 80.0,
            atr: 3.5,
            adx: 40.0,
            bbw_percentile: 0.8,
            slope_metric: 0.6,
            choppiness: Some(20.0),
        };

        let decision = classifier.evaluate(&snapshot, MarketRegime::Range, true);
        assert_ne!(decision.new_regime, MarketRegime::Range);
        assert!(decision.deactivate_grid);
    }
}
