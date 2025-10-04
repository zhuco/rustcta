//! 趋势分析模块

use chrono::{DateTime, Utc};
use log::{debug, info, warn};
use std::collections::HashMap;
use std::sync::Arc;

use crate::core::error::ExchangeError;
use crate::core::exchange::Exchange;
use crate::core::types::{Interval, Kline, MarketType};
use crate::cta::AccountManager;
use crate::strategies::trend::config::IndicatorConfig;
use crate::utils::indicators::{
    calculate_adx, calculate_atr, calculate_ema, calculate_macd, calculate_rsi,
};

/// 趋势方向
#[derive(Debug, Clone, PartialEq)]
pub enum TrendDirection {
    StrongBullish, // 强势上涨
    Bullish,       // 上涨
    Neutral,       // 中性
    Bearish,       // 下跌
    StrongBearish, // 强势下跌
}

/// 趋势信号
#[derive(Debug, Clone)]
pub struct TrendSignal {
    pub symbol: String,
    pub direction: TrendDirection,
    pub strength: f64,        // -100 到 +100
    pub confidence: f64,      // 0 到 100
    pub primary_trend: f64,   // 主要趋势分数
    pub secondary_trend: f64, // 次要趋势分数
    pub momentum: f64,        // 动量分数
    pub volume_confirmation: bool,
    pub volume_ratio: f64, // 成交量比率
    pub timeframe_aligned: bool,
    pub entry_zones: Vec<PriceZone>,
    pub timestamp: DateTime<Utc>,
}

/// 价格区域
#[derive(Debug, Clone)]
pub struct PriceZone {
    pub zone_type: ZoneType,
    pub price: f64,
    pub strength: f64,
}

#[derive(Debug, Clone)]
pub enum ZoneType {
    Support,
    Resistance,
    FibonacciLevel,
    MovingAverage,
}

/// 趋势分析器
#[derive(Clone)]
pub struct TrendAnalyzer {
    config: IndicatorConfig,
    kline_cache: HashMap<String, Vec<Kline>>,
    indicator_cache: HashMap<String, IndicatorValues>,
    account_manager: Option<Arc<AccountManager>>,
}

/// 缓存的指标值
#[derive(Debug, Clone)]
struct IndicatorValues {
    ema_fast: Vec<f64>,
    ema_slow: Vec<f64>,
    atr: Vec<f64>,
    adx: f64,
    rsi: f64,
    macd_line: f64,
    macd_signal: f64,
    macd_histogram: f64,
    bb_upper: f64,
    bb_middle: f64,
    bb_lower: f64,
    last_update: DateTime<Utc>,
}

impl TrendAnalyzer {
    /// 创建新的趋势分析器
    pub fn new(config: IndicatorConfig) -> Self {
        Self {
            config,
            kline_cache: HashMap::new(),
            indicator_cache: HashMap::new(),
            account_manager: None,
        }
    }

    /// 设置账户管理器
    pub fn set_account_manager(&mut self, account_manager: Arc<AccountManager>) {
        self.account_manager = Some(account_manager);
    }

    /// 分析趋势
    pub async fn analyze(&mut self, symbol: &str) -> Result<TrendSignal, ExchangeError> {
        debug!("分析 {} 趋势", symbol);

        // 获取多时间框架K线数据（这里模拟，实际应从交易所获取）
        let primary_tf = self.config.primary_timeframe.clone();
        let secondary_tf = self.config.secondary_timeframe.clone();
        let trigger_tf = self.config.trigger_timeframe.clone();

        let klines_primary = self.get_klines(symbol, &primary_tf).await?;
        let klines_secondary = self.get_klines(symbol, &secondary_tf).await?;
        let klines_trigger = self.get_klines(symbol, &trigger_tf).await?;

        // 计算各时间框架的趋势
        let primary_trend = self.calculate_timeframe_trend(&klines_primary)?;
        let secondary_trend = self.calculate_timeframe_trend(&klines_secondary)?;
        let trigger_trend = self.calculate_timeframe_trend(&klines_trigger)?;

        // 计算综合趋势分数
        let trend_score = self.combine_trends(primary_trend, secondary_trend, trigger_trend);

        // 判断趋势方向
        let direction = self.determine_direction(trend_score);

        // 计算置信度
        let confidence = self.calculate_confidence(primary_trend, secondary_trend, trigger_trend);

        // 检查时间框架一致性
        let timeframe_aligned = self.check_alignment(primary_trend, secondary_trend, trigger_trend);

        // 识别关键价格区域
        let entry_zones = self.identify_entry_zones(&klines_trigger)?;

        // 检查成交量确认（简化版）
        let volume_confirmation = self.check_volume_confirmation(&klines_trigger);

        // 添加调试信息
        info!("📊 {} 趋势分析详情:", symbol);
        info!("  - 主时间框架(1h): {:.2}", primary_trend);
        info!("  - 次时间框架(30m): {:.2}", secondary_trend);
        info!("  - 触发框架(5m): {:.2}", trigger_trend);
        info!("  - 时间框架对齐: {}", timeframe_aligned);
        info!(
            "  - 趋势方向: {:?}, 强度: {:.2}, 置信度: {:.2}",
            direction,
            trend_score.abs(),
            confidence
        );
        info!("  - 成交量确认: {}", volume_confirmation);

        Ok(TrendSignal {
            symbol: symbol.to_string(),
            direction,
            strength: trend_score.abs(),
            confidence,
            primary_trend,
            secondary_trend,
            momentum: trigger_trend,
            volume_confirmation,
            volume_ratio: 1.0, // TODO: 实际计算成交量比率
            timeframe_aligned,
            entry_zones,
            timestamp: Utc::now(),
        })
    }

    /// 计算单个时间框架的趋势
    fn calculate_timeframe_trend(&mut self, klines: &[Kline]) -> Result<f64, ExchangeError> {
        if klines.len() < 50 {
            return Ok(0.0); // 数据不足
        }

        let closes: Vec<f64> = klines.iter().map(|k| k.close).collect();

        // 计算EMA
        let ema_fast = calculate_ema(&closes, self.config.fast_ema).unwrap_or(0.0);
        let ema_slow = calculate_ema(&closes, self.config.slow_ema).unwrap_or(0.0);

        // 计算ADX
        let highs: Vec<f64> = klines.iter().map(|k| k.high).collect();
        let lows: Vec<f64> = klines.iter().map(|k| k.low).collect();
        let adx = calculate_adx(&highs, &lows, &closes, self.config.adx_period).unwrap_or(0.0);

        // 计算RSI
        let rsi = calculate_rsi(&closes, self.config.rsi_period).unwrap_or(50.0);

        // 计算MACD
        let macd_result = calculate_macd(&closes);
        let (macd_line, macd_signal, macd_histogram) = macd_result.unwrap_or((0.0, 0.0, 0.0));

        // 综合评分
        let mut score = 0.0;

        // EMA趋势（权重30%）
        if ema_fast > ema_slow {
            score += 30.0;
        } else {
            score -= 30.0;
        }

        // ADX强度（权重20%）
        if adx > self.config.adx_threshold {
            score += 20.0 * (adx / 100.0);
        }

        // RSI动量（权重25%）
        if rsi > 50.0 {
            score += 25.0 * ((rsi - 50.0) / 50.0);
        } else {
            score -= 25.0 * ((50.0 - rsi) / 50.0);
        }

        // MACD信号（权重25%）
        if macd_histogram > 0.0 {
            score += 25.0;
        } else {
            score -= 25.0;
        }

        Ok(score)
    }

    /// 合并多时间框架趋势
    fn combine_trends(&self, primary: f64, secondary: f64, trigger: f64) -> f64 {
        // 加权平均：主要40%，次要35%，触发25%
        primary * 0.4 + secondary * 0.35 + trigger * 0.25
    }

    /// 判断趋势方向
    fn determine_direction(&self, score: f64) -> TrendDirection {
        match score {
            s if s > 50.0 => TrendDirection::StrongBullish,
            s if s > 20.0 => TrendDirection::Bullish,
            s if s < -50.0 => TrendDirection::StrongBearish,
            s if s < -20.0 => TrendDirection::Bearish,
            _ => TrendDirection::Neutral,
        }
    }

    /// 计算置信度
    fn calculate_confidence(&self, primary: f64, secondary: f64, trigger: f64) -> f64 {
        // 检查一致性
        let all_positive = primary > 0.0 && secondary > 0.0 && trigger > 0.0;
        let all_negative = primary < 0.0 && secondary < 0.0 && trigger < 0.0;

        let mut confidence: f64 = 50.0;

        if all_positive || all_negative {
            confidence = 80.0;
        }

        // 根据强度调整
        let avg_strength = (primary.abs() + secondary.abs() + trigger.abs()) / 3.0;
        confidence = confidence.min(100.0f64).max(0.0f64);

        confidence
    }

    /// 检查时间框架一致性 - 适用于日内交易
    fn check_alignment(&self, primary: f64, secondary: f64, trigger: f64) -> bool {
        // 日内交易策略：只要主要时间框架(1h)和次要时间框架(30m)方向一致即可
        // 触发时间框架(5m)可以不同，允许捕捉短期反转机会
        let main_aligned = (primary > 0.0 && secondary > 0.0) || (primary < 0.0 && secondary < 0.0);

        // 如果主要框架强势且一致，忽略触发框架
        let strong_trend = primary.abs() > 30.0 && secondary.abs() > 30.0;

        if strong_trend && main_aligned {
            debug!(
                "强趋势对齐: primary={:.2}, secondary={:.2}, trigger={:.2}",
                primary, secondary, trigger
            );
            return true;
        }

        // 完全一致性检查（三个时间框架都一致）
        let full_aligned = (primary > 0.0 && secondary > 0.0 && trigger > 0.0)
            || (primary < 0.0 && secondary < 0.0 && trigger < 0.0);

        debug!("时间框架对齐检查: primary={:.2}, secondary={:.2}, trigger={:.2}, main_aligned={}, full_aligned={}", 
            primary, secondary, trigger, main_aligned, full_aligned);

        // 返回主要框架一致即可（放宽条件）
        main_aligned
    }

    /// 识别入场区域
    fn identify_entry_zones(&self, klines: &[Kline]) -> Result<Vec<PriceZone>, ExchangeError> {
        let mut zones = Vec::new();

        if klines.is_empty() {
            return Ok(zones);
        }

        let closes: Vec<f64> = klines.iter().map(|k| k.close).collect();
        let current_price = *closes.last().unwrap();

        // 添加EMA支撑/阻力
        let ema20 = calculate_ema(&closes, 20);
        if let Some(ema_val) = ema20 {
            zones.push(PriceZone {
                zone_type: ZoneType::MovingAverage,
                price: ema_val,
                strength: 70.0,
            });
        }

        // 添加近期高低点
        let recent_high = klines.iter().map(|k| k.high).fold(0.0, f64::max);
        let recent_low = klines.iter().map(|k| k.low).fold(f64::MAX, f64::min);

        zones.push(PriceZone {
            zone_type: ZoneType::Resistance,
            price: recent_high,
            strength: 80.0,
        });

        zones.push(PriceZone {
            zone_type: ZoneType::Support,
            price: recent_low,
            strength: 80.0,
        });

        // 添加斐波那契回调位
        let range = recent_high - recent_low;
        zones.push(PriceZone {
            zone_type: ZoneType::FibonacciLevel,
            price: recent_low + range * 0.382,
            strength: 60.0,
        });

        zones.push(PriceZone {
            zone_type: ZoneType::FibonacciLevel,
            price: recent_low + range * 0.618,
            strength: 60.0,
        });

        Ok(zones)
    }

    /// 检查成交量确认
    fn check_volume_confirmation(&self, klines: &[Kline]) -> bool {
        if klines.len() < 20 {
            return false;
        }

        let volumes: Vec<f64> = klines.iter().map(|k| k.volume).collect();
        let avg_volume = volumes.iter().sum::<f64>() / volumes.len() as f64;
        let recent_volume = volumes.last().unwrap_or(&0.0);

        // 成交量大于平均值1.5倍
        *recent_volume > avg_volume * 1.5
    }

    /// 获取K线数据（模拟）
    async fn get_klines(
        &mut self,
        symbol: &str,
        timeframe: &str,
    ) -> Result<Vec<Kline>, ExchangeError> {
        // 从交易所获取真实K线数据
        if let Some(account_manager) = &self.account_manager {
            // 使用binance_hcr账户获取K线
            if let Some(account) = account_manager.get_account("binance_hcr") {
                debug!("获取 {} {} K线数据", symbol, timeframe);

                // 转换时间框架到Interval类型
                let interval = match timeframe {
                    "1m" => Interval::OneMinute,
                    "5m" => Interval::FiveMinutes,
                    "15m" => Interval::FifteenMinutes,
                    "30m" => Interval::ThirtyMinutes,
                    "1h" => Interval::OneHour,
                    "4h" => Interval::FourHours,
                    "1d" => Interval::OneDay,
                    _ => Interval::OneHour, // 默认1小时
                };

                // 获取100根K线
                let klines = account
                    .exchange
                    .get_klines(symbol, interval, MarketType::Spot, Some(100))
                    .await?;

                if klines.is_empty() {
                    warn!("没有获取到 {} 的K线数据", symbol);
                } else {
                    debug!("获取到 {} 根K线", klines.len());
                }

                return Ok(klines);
            }
        }

        // 如果没有账户管理器，返回空数据
        warn!("无法获取K线数据：没有可用的账户");
        Ok(Vec::new())
    }
}

impl TrendSignal {
    /// 是否为强趋势
    pub fn is_strong(&self) -> bool {
        matches!(
            self.direction,
            TrendDirection::StrongBullish | TrendDirection::StrongBearish
        ) && self.confidence > 70.0
    }

    /// 是否可交易
    pub fn is_tradeable(&self) -> bool {
        // 对于日内交易，放宽条件：
        // 1. 不是中性趋势
        // 2. 置信度大于60%
        // 注：timeframe_aligned不再作为必要条件，因为日内交易允许短期框架不一致
        !matches!(self.direction, TrendDirection::Neutral) && self.confidence > 60.0
    }

    /// 判断是否趋势反转
    pub fn is_reversal(&self) -> bool {
        // 检查是否有趋势反转信号
        matches!(self.direction, TrendDirection::Neutral)
            || self.momentum < -50.0
            || self.volume_ratio < 0.5
    }
}
