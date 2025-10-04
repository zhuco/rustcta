use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};

use crate::core::types::{Interval, Kline, MarketType};
use crate::cta::account_manager::AccountManager;
use crate::utils::indicators::functions;

use crate::strategies::range_grid::domain::config::{
    IndicatorConfig, PrecisionManagementConfig, SymbolConfig,
};
use crate::strategies::range_grid::domain::model::{IndicatorSnapshot, PairPrecision};

use tokio::task;

#[derive(Clone)]
pub struct PrecisionService {
    account_manager: Arc<AccountManager>,
    market_type: MarketType,
    write_back: bool,
    config_path: Option<PathBuf>,
    lock_path: Option<PathBuf>,
    backup_suffix: Option<String>,
}

impl PrecisionService {
    pub fn new(
        account_manager: Arc<AccountManager>,
        market_type: MarketType,
        precision_cfg: &PrecisionManagementConfig,
    ) -> Self {
        Self {
            account_manager,
            market_type,
            write_back: precision_cfg.write_back,
            config_path: precision_cfg.config_path.as_ref().map(|p| PathBuf::from(p)),
            lock_path: precision_cfg.lock_path.as_ref().map(PathBuf::from),
            backup_suffix: precision_cfg.backup_suffix.clone(),
        }
    }

    pub async fn fetch_precision(&self, symbol_cfg: &SymbolConfig) -> Result<PairPrecision> {
        let account = self
            .account_manager
            .get_account(&symbol_cfg.account.id)
            .ok_or_else(|| anyhow!("账户 {} 未注册", symbol_cfg.account.id))?;

        let info = account
            .exchange
            .get_symbol_info(&symbol_cfg.symbol, self.market_type)
            .await?;

        let price_digits = infer_digits(info.tick_size);
        let amount_digits = infer_digits(info.step_size);

        Ok(PairPrecision {
            price_digits,
            amount_digits,
            price_step: info.tick_size,
            amount_step: info.step_size,
            min_notional: info.min_notional,
        })
    }

    pub async fn write_back_precision(
        &self,
        symbol_cfg: &SymbolConfig,
        precision: &PairPrecision,
    ) -> Result<bool> {
        if !self.write_back {
            return Ok(false);
        }
        let Some(config_path) = &self.config_path else {
            return Ok(false);
        };

        let config_path = config_path.clone();
        let lock_path = self.lock_path.clone();
        let backup_suffix = self.backup_suffix.clone();
        let config_id = symbol_cfg.config_id.clone();
        let price_digits = precision.price_digits;
        let amount_digits = precision.amount_digits;

        let updated = task::spawn_blocking(move || {
            use anyhow::Context;
            use serde_yaml::{Mapping, Value};
            use std::fs::{self, File};
            use std::io::{ErrorKind, Write};
            use std::thread;
            use std::time::Duration;

            let mut lock_handle: Option<File> = None;
            if let Some(ref lock) = lock_path {
                let attempts = 5;
                for attempt in 0..attempts {
                    match std::fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(lock)
                    {
                        Ok(handle) => {
                            lock_handle = Some(handle);
                            break;
                        }
                        Err(err) if err.kind() == ErrorKind::AlreadyExists => {
                            thread::sleep(Duration::from_millis(100 * (attempt as u64 + 1)));
                            continue;
                        }
                        Err(err) => {
                            return Err(anyhow!(
                                "创建精度写入锁失败: {} ({})",
                                lock.display(),
                                err
                            ));
                        }
                    }
                }

                if lock_handle.is_none() {
                    return Err(anyhow!("等待精度写入锁超时: {}", lock.display()));
                }
            }

            let content = fs::read_to_string(&config_path)
                .with_context(|| format!("读取配置文件失败: {}", config_path.display()))?;
            let mut doc: Value = serde_yaml::from_str(&content)
                .with_context(|| format!("解析 YAML 失败: {}", config_path.display()))?;

            let mut changed = false;

            if let Value::Mapping(ref mut root) = doc {
                if let Some(symbols_value) = root.get_mut(&Value::String("symbols".into())) {
                    if let Value::Sequence(ref mut seq) = symbols_value {
                        for entry in seq.iter_mut() {
                            if let Value::Mapping(ref mut symbol_map) = entry {
                                let id_matches = symbol_map
                                    .get(&Value::String("config_id".into()))
                                    .and_then(|v| v.as_str())
                                    == Some(config_id.as_str());

                                if !id_matches {
                                    continue;
                                }

                                let precision_key = Value::String("precision".into());
                                if !symbol_map.contains_key(&precision_key) {
                                    symbol_map.insert(
                                        precision_key.clone(),
                                        Value::Mapping(Mapping::new()),
                                    );
                                }

                                if let Some(Value::Mapping(ref mut precision_map)) =
                                    symbol_map.get_mut(&precision_key)
                                {
                                    use serde_yaml::Number;

                                    let price_key = Value::String("price_digits".into());
                                    let amount_key = Value::String("amount_digits".into());

                                    let mut update_field = |key: Value, target: u32| -> bool {
                                        let current = precision_map
                                            .get(&key)
                                            .and_then(|v| v.as_i64())
                                            .map(|v| v as u32);
                                        if current == Some(target) {
                                            return false;
                                        }
                                        precision_map.insert(
                                            key,
                                            Value::Number(Number::from(target as i64)),
                                        );
                                        true
                                    };

                                    if update_field(price_key, price_digits) {
                                        changed = true;
                                    }
                                    if update_field(amount_key, amount_digits) {
                                        changed = true;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if changed {
                if let Some(suffix) = backup_suffix {
                    let file_name = config_path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .ok_or_else(|| anyhow!("配置文件名非法"))?;
                    let backup_name = format!("{}{}", file_name, suffix);
                    let backup_path = config_path
                        .parent()
                        .unwrap_or_else(|| Path::new("."))
                        .join(backup_name);
                    fs::copy(&config_path, &backup_path).with_context(|| {
                        format!(
                            "创建配置备份失败: {} -> {}",
                            config_path.display(),
                            backup_path.display()
                        )
                    })?;
                }

                let updated = serde_yaml::to_string(&doc)?;
                let tmp_name = format!(
                    "{}.tmp",
                    config_path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .ok_or_else(|| anyhow!("配置文件名非法"))?
                );
                let tmp_path = config_path
                    .parent()
                    .unwrap_or_else(|| Path::new("."))
                    .join(tmp_name);
                fs::write(&tmp_path, updated)
                    .with_context(|| format!("写入临时配置失败: {}", tmp_path.display()))?;
                fs::rename(&tmp_path, &config_path)
                    .with_context(|| format!("替换配置文件失败: {}", config_path.display()))?;
            }

            if let Some(lock) = lock_path {
                drop(lock_handle);
                let _ = fs::remove_file(lock);
            }

            Ok(changed)
        })
        .await??;

        Ok(updated)
    }
}

#[derive(Clone)]
pub struct IndicatorService {
    account_manager: Arc<AccountManager>,
    market_type: MarketType,
}

impl IndicatorService {
    pub fn new(account_manager: Arc<AccountManager>, market_type: MarketType) -> Self {
        Self {
            account_manager,
            market_type,
        }
    }

    pub async fn load_snapshot(
        &self,
        symbol_cfg: &SymbolConfig,
        indicator_cfg: &IndicatorConfig,
    ) -> Result<IndicatorSnapshot> {
        let account = self
            .account_manager
            .get_account(&symbol_cfg.account.id)
            .ok_or_else(|| anyhow!("账户 {} 未注册", symbol_cfg.account.id))?;

        let exchange = account.exchange.clone();

        let lower_interval = Interval::from_string(&indicator_cfg.lower_timeframe.timeframe)?;
        let higher_interval = Interval::from_string(&indicator_cfg.higher_timeframe.timeframe)?;

        let lower_limit = (indicator_cfg.lower_timeframe.bollinger.length.max(200) as u32).max(200);
        let higher_limit = indicator_cfg.higher_timeframe.bbw_window.max(200) as u32;

        let mut lower_klines = exchange
            .get_klines(
                &symbol_cfg.symbol,
                lower_interval,
                self.market_type,
                Some(lower_limit),
            )
            .await?;
        let mut higher_klines = exchange
            .get_klines(
                &symbol_cfg.symbol,
                higher_interval,
                self.market_type,
                Some(higher_limit as u32),
            )
            .await?;

        ensure_sorted(&mut lower_klines);
        ensure_sorted(&mut higher_klines);

        compute_snapshot(indicator_cfg, &lower_klines, &higher_klines)
    }
}

fn ensure_sorted(klines: &mut Vec<Kline>) {
    klines.sort_by_key(|k| k.close_time);
    klines.dedup_by_key(|k| k.close_time);
}

fn compute_snapshot(
    cfg: &IndicatorConfig,
    lower: &[Kline],
    higher: &[Kline],
) -> Result<IndicatorSnapshot> {
    if lower.len() < cfg.lower_timeframe.bollinger.length + 5 {
        return Err(anyhow!("5m K线数据不足"));
    }
    if higher.len() < cfg.lower_timeframe.bollinger.length + 5 {
        return Err(anyhow!("15m K线数据不足"));
    }

    let closes_5m: Vec<f64> = lower.iter().map(|k| k.close).collect();
    let highs_5m: Vec<f64> = lower.iter().map(|k| k.high).collect();
    let lows_5m: Vec<f64> = lower.iter().map(|k| k.low).collect();

    let (upper_5m, mid_5m, lower_5m) = functions::bollinger_bands(
        &closes_5m,
        cfg.lower_timeframe.bollinger.length,
        cfg.lower_timeframe.bollinger.k,
    )
    .ok_or_else(|| anyhow!("无法计算5m布林带"))?;

    let sigma_5m = ((upper_5m - lower_5m) / 2.0) / cfg.lower_timeframe.bollinger.k.max(1e-9);
    let last_price = *closes_5m.last().unwrap_or(&mid_5m);
    let denom = (upper_5m - lower_5m).abs().max(1e-9);
    let lower_band_ratio = (last_price - lower_5m) / denom;
    let z_score = if sigma_5m.abs() < 1e-9 {
        0.0
    } else {
        (last_price - mid_5m) / sigma_5m
    };

    let atr = functions::atr(
        &highs_5m,
        &lows_5m,
        &closes_5m,
        cfg.lower_timeframe.atr.length,
    )
    .ok_or_else(|| anyhow!("无法计算ATR"))?;
    let rsi = functions::rsi(&closes_5m, cfg.lower_timeframe.rsi.length)
        .ok_or_else(|| anyhow!("无法计算RSI"))?;

    let closes_15m: Vec<f64> = higher.iter().map(|k| k.close).collect();
    let highs_15m: Vec<f64> = higher.iter().map(|k| k.high).collect();
    let lows_15m: Vec<f64> = higher.iter().map(|k| k.low).collect();

    let (upper_15m, mid_15m, lower_15m) = functions::bollinger_bands(
        &closes_15m,
        cfg.lower_timeframe.bollinger.length,
        cfg.lower_timeframe.bollinger.k,
    )
    .ok_or_else(|| anyhow!("无法计算15m布林带"))?;

    let bbw_current = if mid_15m.abs() < 1e-9 {
        0.0
    } else {
        (upper_15m - lower_15m) / mid_15m.abs()
    };

    let bbw_percentile = compute_bbw_percentile(
        &closes_15m,
        cfg.lower_timeframe.bollinger.length,
        cfg.higher_timeframe.bbw_window,
        cfg.lower_timeframe.bollinger.k,
        bbw_current,
    );

    let slope_metric = compute_slope_metric(
        &closes_15m,
        cfg.lower_timeframe.bollinger.length,
        cfg.higher_timeframe.slope_threshold,
        cfg.lower_timeframe.bollinger.k,
    );

    let adx = functions::adx(
        &highs_15m,
        &lows_15m,
        &closes_15m,
        cfg.higher_timeframe.adx_length as usize,
    )
    .unwrap_or(0.0);

    let choppiness = cfg
        .higher_timeframe
        .choppiness
        .as_ref()
        .filter(|c| c.enabled)
        .and_then(|c| compute_choppiness(&highs_15m, &lows_15m, &closes_15m, c.threshold as usize));

    let timestamp = lower.last().map(|k| k.close_time).unwrap_or_else(Utc::now);

    Ok(IndicatorSnapshot {
        timestamp,
        last_price,
        lower_band_ratio,
        z_score,
        rsi,
        atr,
        adx,
        bbw_percentile,
        slope_metric,
        choppiness,
    })
}

fn compute_bbw_percentile(
    closes: &[f64],
    length: usize,
    window: usize,
    k: f64,
    current_bbw: f64,
) -> f64 {
    let window = window.max(length).min(closes.len());
    if window <= length {
        return 0.5;
    }

    let mut history = Vec::with_capacity(window - length);
    for idx in length..window {
        let slice = &closes[idx - length..=idx];
        if let Some((upper, mid, lower)) = functions::bollinger_bands(slice, length, k) {
            if mid.abs() > 1e-9 {
                history.push((upper - lower) / mid.abs());
            }
        }
    }

    if history.is_empty() {
        return 0.5;
    }

    let mut below = 0usize;
    for value in &history {
        if *value <= current_bbw {
            below += 1;
        }
    }

    below as f64 / history.len() as f64
}

fn compute_slope_metric(closes: &[f64], length: usize, _threshold: f64, k: f64) -> f64 {
    if closes.len() < length + 2 {
        return 0.0;
    }

    let mut mids = Vec::new();
    let mut sigmas = Vec::new();
    for idx in length..closes.len() {
        let slice = &closes[idx - length..=idx];
        if let Some((upper, mid, lower)) = functions::bollinger_bands(slice, length, k) {
            mids.push(mid);
            sigmas.push(((upper - lower) / 2.0) / k.max(1e-9));
        }
    }

    if mids.len() < 2 {
        return 0.0;
    }

    let latest_mid = *mids.last().unwrap();
    let prev_mid = mids[mids.len() - 2];
    let sigma = sigmas.last().copied().unwrap_or(1.0).abs().max(1e-9);
    (latest_mid - prev_mid) / sigma
}

fn compute_choppiness(highs: &[f64], lows: &[f64], closes: &[f64], lookback: usize) -> Option<f64> {
    if lookback < 2 || closes.len() < lookback {
        return None;
    }

    let start = closes.len() - lookback;
    let mut tr_sum = 0.0;
    for i in start..closes.len() {
        let high_low = highs[i] - lows[i];
        let high_close = (highs[i] - closes[i - 1]).abs();
        let low_close = (lows[i] - closes[i - 1]).abs();
        tr_sum += high_low.max(high_close).max(low_close);
    }

    let highest = highs[start..].iter().fold(f64::MIN, |acc, &v| acc.max(v));
    let lowest = lows[start..].iter().fold(f64::MAX, |acc, &v| acc.min(v));
    let range = (highest - lowest).abs().max(1e-9);
    let ratio = tr_sum / range;
    Some(100.0 * (ratio.log10() / (lookback as f64).log10()))
}

fn infer_digits(step: f64) -> u32 {
    if step <= 0.0 {
        return 6;
    }
    let mut digits = 0u32;
    let mut value = step;
    while (value - value.round()).abs() > 1e-9 && digits < 10 {
        value *= 10.0;
        digits += 1;
    }
    if digits == 0 {
        let mut k = 0u32;
        let mut s = step;
        while s < 1.0 && k < 10 {
            s *= 10.0;
            k += 1;
        }
        return k;
    }
    digits
}
