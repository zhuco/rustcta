mod dto {
    pub use crate::types::BacktestKline as Kline;
}

include!("indicators_impl.rs");

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};

    use super::{
        adx, atr, atr_percentile, bollinger_width, choppiness_index, donchian_high, donchian_low,
        ema, ema_slope, keltner_channel, macd_histogram, mfi, roc, rsi, stoch_rsi,
        supertrend_direction, volume_ratio, volume_zscore, vwap_deviation,
    };
    use crate::types::BacktestKline;

    fn kline(index: usize, close: f64, volume: f64) -> BacktestKline {
        let open_time =
            Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap() + Duration::minutes(index as i64);
        BacktestKline {
            symbol: "BTC/USDC".to_string(),
            interval: "1m".to_string(),
            open_time,
            close_time: open_time + Duration::seconds(59),
            open: close - 0.5,
            high: close + 1.0,
            low: close - 1.0,
            close,
            volume,
            quote_volume: volume * close,
            trade_count: 10,
        }
    }

    fn sample_klines() -> Vec<BacktestKline> {
        (0..80)
            .map(|index| {
                kline(
                    index,
                    100.0 + index as f64 * 0.5,
                    1_000.0 + index as f64 * 10.0,
                )
            })
            .collect()
    }

    #[test]
    fn moving_average_and_breakout_series_should_match_expected_windows() {
        let candles = sample_klines();
        let closes = candles.iter().map(|item| item.close).collect::<Vec<_>>();

        let ema_values = ema(&closes, 3);
        assert_eq!(ema_values[0], None);
        assert_eq!(ema_values[2], Some(100.5));

        let donchian_highs = donchian_high(&candles, 5);
        let donchian_lows = donchian_low(&candles, 5);
        assert_eq!(donchian_highs[4], Some(103.0));
        assert_eq!(donchian_lows[4], Some(99.0));

        let width = bollinger_width(&closes, 20, 2.0);
        assert!(width[19].is_some_and(|value| value > 0.0));
    }

    #[test]
    fn momentum_and_volatility_series_should_stay_in_valid_ranges() {
        let candles = sample_klines();
        let closes = candles.iter().map(|item| item.close).collect::<Vec<_>>();

        let atr_values = atr(&candles, 14);
        let adx_values = adx(&candles, 14);
        let rsi_values = rsi(&closes, 14);
        let macd_values = macd_histogram(&closes, 12, 26, 9);
        let supertrend = supertrend_direction(&candles, 10, 3.0);
        assert!(atr_values.iter().flatten().all(|value| *value > 0.0));
        assert!(adx_values.iter().flatten().all(|value| *value >= 0.0));
        assert!(rsi_values
            .iter()
            .flatten()
            .all(|value| *value >= 0.0 && *value <= 100.0));
        assert!(macd_values.iter().flatten().any(|value| *value != 0.0));
        assert!(supertrend
            .iter()
            .flatten()
            .any(|value| *value == 1 || *value == -1));
    }

    #[test]
    fn extended_short_cycle_indicators_should_calculate_root_free() {
        let candles = sample_klines();
        let closes = candles.iter().map(|item| item.close).collect::<Vec<_>>();

        let slope = ema_slope(&closes, 10, 5);
        assert!(slope.iter().flatten().any(|value| *value > 0.0));

        let keltner = keltner_channel(&candles, 20, 10, 1.5);
        assert!(keltner.upper.iter().flatten().any(|value| *value > 0.0));
        assert!(keltner.lower.iter().flatten().any(|value| *value > 0.0));

        let vwap = vwap_deviation(&candles, 20);
        assert!(vwap.iter().flatten().any(|value| value.is_finite()));

        let mfi_values = mfi(&candles, 14);
        assert!(mfi_values
            .iter()
            .flatten()
            .all(|value| *value >= 0.0 && *value <= 100.0));

        let stoch_values = stoch_rsi(&closes, 14, 14);
        assert!(stoch_values
            .iter()
            .flatten()
            .all(|value| *value >= 0.0 && *value <= 100.0));

        let chop = choppiness_index(&candles, 14);
        assert!(chop
            .iter()
            .flatten()
            .all(|value| *value >= 0.0 && *value <= 100.0));

        let atr_pct = atr_percentile(&candles, 14, 30);
        assert!(atr_pct
            .iter()
            .flatten()
            .all(|value| *value >= 0.0 && *value <= 100.0));

        let volume_z = volume_zscore(&candles, 20);
        assert!(volume_z.iter().flatten().any(|value| value.is_finite()));

        let ratio = volume_ratio(&candles, 10);
        assert!(ratio[10].is_some_and(|value| value > 1.0));

        let roc_values = roc(&closes, 10);
        assert!(roc_values[10].is_some_and(|value| value > 0.0));
    }
}
