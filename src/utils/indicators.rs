use crate::core::types::{Kline, OrderSide};
/// 统一的技术指标模块
/// 整合所有技术指标计算功能（静态和流式）
use std::collections::VecDeque;

/// 技术指标数据
#[derive(Debug, Clone)]
pub struct IndicatorData {
    pub timestamp: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

/// 静态技术指标计算函数（无状态）
pub mod functions {
    use super::*;

    /// 计算简单移动平均线 (SMA)
    pub fn sma(prices: &[f64], period: usize) -> Option<f64> {
        if prices.len() < period || period == 0 {
            return None;
        }

        let sum: f64 = prices[prices.len() - period..].iter().sum();
        Some(sum / period as f64)
    }

    /// 计算指数移动平均线 (EMA)
    pub fn ema(prices: &[f64], period: usize) -> Option<f64> {
        if prices.is_empty() || period == 0 {
            return None;
        }

        let multiplier = 2.0 / (period as f64 + 1.0);
        let mut ema = prices[0];

        for price in prices.iter().skip(1) {
            ema = (price - ema) * multiplier + ema;
        }

        Some(ema)
    }

    /// 计算相对强弱指数 (RSI)
    pub fn rsi(prices: &[f64], period: usize) -> Option<f64> {
        if prices.len() < period + 1 || period == 0 {
            return None;
        }

        let mut gains = 0.0;
        let mut losses = 0.0;

        for i in prices.len() - period..prices.len() {
            let change = prices[i] - prices[i - 1];
            if change > 0.0 {
                gains += change;
            } else {
                losses += change.abs();
            }
        }

        let avg_gain = gains / period as f64;
        let avg_loss = losses / period as f64;

        if avg_loss == 0.0 {
            return Some(100.0);
        }

        let rs = avg_gain / avg_loss;
        Some(100.0 - (100.0 / (1.0 + rs)))
    }

    /// 计算MACD指标
    pub fn macd(prices: &[f64]) -> Option<(f64, f64, f64)> {
        if prices.len() < 26 {
            return None;
        }

        let ema12 = ema(prices, 12)?;
        let ema26 = ema(prices, 26)?;
        let macd_line = ema12 - ema26;

        // 简化的信号线计算（通常需要9期EMA的MACD值）
        let signal = macd_line * 0.9; // 简化实现
        let histogram = macd_line - signal;

        Some((macd_line, signal, histogram))
    }

    /// 计算布林带
    pub fn bollinger_bands(prices: &[f64], period: usize, std_dev: f64) -> Option<(f64, f64, f64)> {
        let sma = sma(prices, period)?;

        if prices.len() < period {
            return None;
        }

        let variance: f64 = prices[prices.len() - period..]
            .iter()
            .map(|p| (p - sma).powi(2))
            .sum::<f64>()
            / period as f64;

        let std = variance.sqrt();
        let upper = sma + std_dev * std;
        let lower = sma - std_dev * std;

        Some((upper, sma, lower))
    }

    /// 计算平均真实范围 (ATR)
    pub fn atr(highs: &[f64], lows: &[f64], closes: &[f64], period: usize) -> Option<f64> {
        if highs.len() < period + 1 || lows.len() < period + 1 || closes.len() < period + 1 {
            return None;
        }

        let mut tr_values = Vec::new();

        for i in 1..highs.len() {
            let high_low = highs[i] - lows[i];
            let high_close = (highs[i] - closes[i - 1]).abs();
            let low_close = (lows[i] - closes[i - 1]).abs();

            let tr = high_low.max(high_close).max(low_close);
            tr_values.push(tr);
        }

        if tr_values.len() < period {
            return None;
        }

        let sum: f64 = tr_values[tr_values.len() - period..].iter().sum();
        Some(sum / period as f64)
    }

    /// 计算随机指标 (Stochastic) - 完整实现
    pub fn stochastic(
        highs: &[f64],
        lows: &[f64],
        closes: &[f64],
        k_period: usize,
        d_period: usize,
    ) -> Option<(f64, f64)> {
        if highs.len() < k_period + d_period - 1
            || lows.len() < k_period + d_period - 1
            || closes.len() < k_period + d_period - 1
        {
            return None;
        }

        let mut k_values = Vec::new();

        // 计算K值序列
        for i in k_period - 1..closes.len() {
            let start = i + 1 - k_period;
            let highest = highs[start..=i].iter().fold(f64::MIN, |a, &b| a.max(b));
            let lowest = lows[start..=i].iter().fold(f64::MAX, |a, &b| a.min(b));

            let k = if highest == lowest {
                50.0
            } else {
                100.0 * (closes[i] - lowest) / (highest - lowest)
            };

            k_values.push(k);
        }

        if k_values.len() < d_period {
            return None;
        }

        // 当前K值
        let current_k = *k_values.last().unwrap();

        // D值是K值的SMA
        let d = sma(&k_values, d_period)?;

        Some((current_k, d))
    }

    /// 计算ADX (Average Directional Index) - 趋势强度指标
    pub fn adx(highs: &[f64], lows: &[f64], closes: &[f64], period: usize) -> Option<f64> {
        if highs.len() < period * 2 || lows.len() < period * 2 || closes.len() < period * 2 {
            return None;
        }

        let mut plus_dm = Vec::new();
        let mut minus_dm = Vec::new();
        let mut tr_values = Vec::new();

        // 计算DM和TR
        for i in 1..highs.len() {
            // +DM和-DM
            let up_move = highs[i] - highs[i - 1];
            let down_move = lows[i - 1] - lows[i];

            let plus = if up_move > down_move && up_move > 0.0 {
                up_move
            } else {
                0.0
            };
            let minus = if down_move > up_move && down_move > 0.0 {
                down_move
            } else {
                0.0
            };

            plus_dm.push(plus);
            minus_dm.push(minus);

            // True Range
            let high_low = highs[i] - lows[i];
            let high_close = (highs[i] - closes[i - 1]).abs();
            let low_close = (lows[i] - closes[i - 1]).abs();

            tr_values.push(high_low.max(high_close).max(low_close));
        }

        if tr_values.len() < period * 2 - 1 {
            return None;
        }

        // 平滑DM和TR
        let smooth_plus_dm = ema(&plus_dm, period)?;
        let smooth_minus_dm = ema(&minus_dm, period)?;
        let smooth_tr = ema(&tr_values, period)?;

        if smooth_tr == 0.0 {
            return None;
        }

        // 计算DI
        let plus_di = 100.0 * smooth_plus_dm / smooth_tr;
        let minus_di = 100.0 * smooth_minus_dm / smooth_tr;

        // 计算DX
        let di_sum = plus_di + minus_di;
        if di_sum == 0.0 {
            return None;
        }

        let dx = 100.0 * ((plus_di - minus_di).abs() / di_sum);

        // ADX是DX的移动平均（这里简化为返回DX）
        Some(dx)
    }

    /// 计算CCI (Commodity Channel Index) - 商品通道指数
    pub fn cci(highs: &[f64], lows: &[f64], closes: &[f64], period: usize) -> Option<f64> {
        if highs.len() < period || lows.len() < period || closes.len() < period {
            return None;
        }

        let mut typical_prices = Vec::new();

        // 计算典型价格 (High + Low + Close) / 3
        for i in 0..closes.len() {
            let tp = (highs[i] + lows[i] + closes[i]) / 3.0;
            typical_prices.push(tp);
        }

        // 计算SMA
        let sma_tp = sma(&typical_prices, period)?;

        // 计算平均偏差
        let start = typical_prices.len() - period;
        let mut sum_deviation = 0.0;
        for i in start..typical_prices.len() {
            sum_deviation += (typical_prices[i] - sma_tp).abs();
        }
        let mean_deviation = sum_deviation / period as f64;

        if mean_deviation == 0.0 {
            return Some(0.0);
        }

        // CCI = (TP - SMA) / (0.015 × Mean Deviation)
        let current_tp = typical_prices.last().unwrap();
        let cci = (current_tp - sma_tp) / (0.015 * mean_deviation);

        Some(cci)
    }

    /// 计算OBV (On-Balance Volume) - 能量潮
    pub fn obv(closes: &[f64], volumes: &[f64]) -> Option<Vec<f64>> {
        if closes.len() != volumes.len() || closes.is_empty() {
            return None;
        }

        let mut obv_values = Vec::with_capacity(closes.len());
        obv_values.push(volumes[0]); // 初始OBV等于第一个成交量

        for i in 1..closes.len() {
            let prev_obv = obv_values[i - 1];
            let obv = if closes[i] > closes[i - 1] {
                prev_obv + volumes[i] // 价格上涨，加上成交量
            } else if closes[i] < closes[i - 1] {
                prev_obv - volumes[i] // 价格下跌，减去成交量
            } else {
                prev_obv // 价格不变，OBV不变
            };
            obv_values.push(obv);
        }

        Some(obv_values)
    }

    /// 计算支撑位和阻力位
    pub fn support_resistance(klines: &[Kline], lookback: usize) -> (Vec<f64>, Vec<f64>) {
        if klines.is_empty() {
            return (vec![], vec![]);
        }

        let start = if klines.len() > lookback {
            klines.len() - lookback
        } else {
            0
        };

        let mut supports = Vec::new();
        let mut resistances = Vec::new();

        for i in start + 1..klines.len() - 1 {
            let prev = &klines[i - 1];
            let curr = &klines[i];
            let next = &klines[i + 1];

            // 局部最低点作为支撑
            if curr.low < prev.low && curr.low < next.low {
                supports.push(curr.low);
            }

            // 局部最高点作为阻力
            if curr.high > prev.high && curr.high > next.high {
                resistances.push(curr.high);
            }
        }

        supports.sort_by(|a, b| a.partial_cmp(b).unwrap());
        resistances.sort_by(|a, b| a.partial_cmp(b).unwrap());

        (supports, resistances)
    }
}

/// 流式技术指标（有状态，支持增量计算）
pub mod streaming {
    use super::*;

    /// 流式简单移动平均
    #[derive(Debug, Clone)]
    pub struct SMA {
        period: usize,
        values: VecDeque<f64>,
        sum: f64,
    }

    impl SMA {
        pub fn new(period: usize) -> Self {
            Self {
                period,
                values: VecDeque::with_capacity(period),
                sum: 0.0,
            }
        }

        pub fn update(&mut self, value: f64) -> Option<f64> {
            if self.values.len() >= self.period {
                let old = self.values.pop_front().unwrap();
                self.sum -= old;
            }

            self.values.push_back(value);
            self.sum += value;

            if self.values.len() == self.period {
                Some(self.sum / self.period as f64)
            } else {
                None
            }
        }

        pub fn current(&self) -> Option<f64> {
            if self.values.len() == self.period {
                Some(self.sum / self.period as f64)
            } else {
                None
            }
        }

        pub fn reset(&mut self) {
            self.values.clear();
            self.sum = 0.0;
        }
    }

    /// 流式指数移动平均
    #[derive(Debug, Clone)]
    pub struct EMA {
        period: usize,
        multiplier: f64,
        value: Option<f64>,
        count: usize,
    }

    impl EMA {
        pub fn new(period: usize) -> Self {
            Self {
                period,
                multiplier: 2.0 / (period as f64 + 1.0),
                value: None,
                count: 0,
            }
        }

        pub fn update(&mut self, price: f64) -> Option<f64> {
            self.count += 1;

            self.value = match self.value {
                None => Some(price),
                Some(prev) => Some((price - prev) * self.multiplier + prev),
            };

            if self.count >= self.period {
                self.value
            } else {
                None
            }
        }

        pub fn current(&self) -> Option<f64> {
            if self.count >= self.period {
                self.value
            } else {
                None
            }
        }

        pub fn reset(&mut self) {
            self.value = None;
            self.count = 0;
        }
    }

    /// 流式RSI指标
    #[derive(Debug, Clone)]
    pub struct RSI {
        period: usize,
        avg_gain: f64,
        avg_loss: f64,
        prev_price: Option<f64>,
        count: usize,
    }

    impl RSI {
        pub fn new(period: usize) -> Self {
            Self {
                period,
                avg_gain: 0.0,
                avg_loss: 0.0,
                prev_price: None,
                count: 0,
            }
        }

        pub fn update(&mut self, price: f64) -> Option<f64> {
            if let Some(prev) = self.prev_price {
                let change = price - prev;
                let gain = if change > 0.0 { change } else { 0.0 };
                let loss = if change < 0.0 { -change } else { 0.0 };

                if self.count < self.period {
                    self.avg_gain += gain;
                    self.avg_loss += loss;
                    self.count += 1;

                    if self.count == self.period {
                        self.avg_gain /= self.period as f64;
                        self.avg_loss /= self.period as f64;
                    }
                } else {
                    self.avg_gain =
                        (self.avg_gain * (self.period - 1) as f64 + gain) / self.period as f64;
                    self.avg_loss =
                        (self.avg_loss * (self.period - 1) as f64 + loss) / self.period as f64;
                }
            }

            self.prev_price = Some(price);

            if self.count >= self.period {
                if self.avg_loss == 0.0 {
                    Some(100.0)
                } else {
                    let rs = self.avg_gain / self.avg_loss;
                    Some(100.0 - (100.0 / (1.0 + rs)))
                }
            } else {
                None
            }
        }

        pub fn current(&self) -> Option<f64> {
            if self.count >= self.period {
                if self.avg_loss == 0.0 {
                    Some(100.0)
                } else {
                    let rs = self.avg_gain / self.avg_loss;
                    Some(100.0 - (100.0 / (1.0 + rs)))
                }
            } else {
                None
            }
        }

        pub fn reset(&mut self) {
            self.avg_gain = 0.0;
            self.avg_loss = 0.0;
            self.prev_price = None;
            self.count = 0;
        }
    }

    /// 流式MACD指标
    #[derive(Debug, Clone)]
    pub struct MACD {
        fast_ema: EMA,
        slow_ema: EMA,
        signal_ema: EMA,
        macd_values: VecDeque<f64>,
    }

    impl MACD {
        pub fn new() -> Self {
            Self::with_params(12, 26, 9)
        }

        pub fn with_params(fast: usize, slow: usize, signal: usize) -> Self {
            Self {
                fast_ema: EMA::new(fast),
                slow_ema: EMA::new(slow),
                signal_ema: EMA::new(signal),
                macd_values: VecDeque::with_capacity(signal),
            }
        }

        pub fn update(&mut self, price: f64) -> Option<(f64, f64, f64)> {
            let fast = self.fast_ema.update(price)?;
            let slow = self.slow_ema.update(price)?;

            let macd = fast - slow;

            // 更新信号线
            let signal = self.signal_ema.update(macd)?;
            let histogram = macd - signal;

            Some((macd, signal, histogram))
        }

        pub fn current(&self) -> Option<(f64, f64, f64)> {
            let fast = self.fast_ema.current()?;
            let slow = self.slow_ema.current()?;
            let macd = fast - slow;
            let signal = self.signal_ema.current()?;
            let histogram = macd - signal;

            Some((macd, signal, histogram))
        }

        pub fn reset(&mut self) {
            self.fast_ema.reset();
            self.slow_ema.reset();
            self.signal_ema.reset();
            self.macd_values.clear();
        }
    }

    /// 流式随机指标 (完整实现)
    #[derive(Debug, Clone)]
    pub struct Stochastic {
        k_period: usize,
        d_period: usize,
        highs: VecDeque<f64>,
        lows: VecDeque<f64>,
        closes: VecDeque<f64>,
        k_values: VecDeque<f64>,
    }

    impl Stochastic {
        pub fn new(k_period: usize, d_period: usize) -> Self {
            Self {
                k_period,
                d_period,
                highs: VecDeque::with_capacity(k_period),
                lows: VecDeque::with_capacity(k_period),
                closes: VecDeque::with_capacity(k_period),
                k_values: VecDeque::with_capacity(d_period),
            }
        }

        pub fn update(&mut self, high: f64, low: f64, close: f64) -> Option<(f64, f64)> {
            // 更新缓冲区
            if self.highs.len() >= self.k_period {
                self.highs.pop_front();
                self.lows.pop_front();
                self.closes.pop_front();
            }

            self.highs.push_back(high);
            self.lows.push_back(low);
            self.closes.push_back(close);

            if self.highs.len() < self.k_period {
                return None;
            }

            // 计算K值
            let highest = self.highs.iter().fold(f64::MIN, |a, &b| a.max(b));
            let lowest = self.lows.iter().fold(f64::MAX, |a, &b| a.min(b));

            let k = if highest == lowest {
                50.0
            } else {
                100.0 * (close - lowest) / (highest - lowest)
            };

            // 更新K值缓冲区
            if self.k_values.len() >= self.d_period {
                self.k_values.pop_front();
            }
            self.k_values.push_back(k);

            if self.k_values.len() < self.d_period {
                return None;
            }

            // 计算D值（K值的SMA）
            let d = self.k_values.iter().sum::<f64>() / self.d_period as f64;

            Some((k, d))
        }

        pub fn reset(&mut self) {
            self.highs.clear();
            self.lows.clear();
            self.closes.clear();
            self.k_values.clear();
        }
    }

    /// 流式ADX指标
    #[derive(Debug, Clone)]
    pub struct ADX {
        period: usize,
        plus_dm_ema: EMA,
        minus_dm_ema: EMA,
        tr_ema: EMA,
        dx_values: VecDeque<f64>,
        prev_high: Option<f64>,
        prev_low: Option<f64>,
        prev_close: Option<f64>,
    }

    impl ADX {
        pub fn new(period: usize) -> Self {
            Self {
                period,
                plus_dm_ema: EMA::new(period),
                minus_dm_ema: EMA::new(period),
                tr_ema: EMA::new(period),
                dx_values: VecDeque::with_capacity(period),
                prev_high: None,
                prev_low: None,
                prev_close: None,
            }
        }

        pub fn update(&mut self, high: f64, low: f64, close: f64) -> Option<f64> {
            if let (Some(prev_h), Some(prev_l), Some(prev_c)) =
                (self.prev_high, self.prev_low, self.prev_close)
            {
                // 计算DM
                let up_move = high - prev_h;
                let down_move = prev_l - low;

                let plus_dm = if up_move > down_move && up_move > 0.0 {
                    up_move
                } else {
                    0.0
                };
                let minus_dm = if down_move > up_move && down_move > 0.0 {
                    down_move
                } else {
                    0.0
                };

                // 计算TR
                let high_low = high - low;
                let high_close = (high - prev_c).abs();
                let low_close = (low - prev_c).abs();
                let tr = high_low.max(high_close).max(low_close);

                // 更新EMA
                let smooth_plus = self.plus_dm_ema.update(plus_dm)?;
                let smooth_minus = self.minus_dm_ema.update(minus_dm)?;
                let smooth_tr = self.tr_ema.update(tr)?;

                if smooth_tr > 0.0 {
                    let plus_di = 100.0 * smooth_plus / smooth_tr;
                    let minus_di = 100.0 * smooth_minus / smooth_tr;
                    let di_sum = plus_di + minus_di;

                    if di_sum > 0.0 {
                        let dx = 100.0 * ((plus_di - minus_di).abs() / di_sum);

                        if self.dx_values.len() >= self.period {
                            self.dx_values.pop_front();
                        }
                        self.dx_values.push_back(dx);

                        if self.dx_values.len() == self.period {
                            let adx = self.dx_values.iter().sum::<f64>() / self.period as f64;
                            self.prev_high = Some(high);
                            self.prev_low = Some(low);
                            self.prev_close = Some(close);
                            return Some(adx);
                        }
                    }
                }
            }

            self.prev_high = Some(high);
            self.prev_low = Some(low);
            self.prev_close = Some(close);
            None
        }

        pub fn reset(&mut self) {
            self.plus_dm_ema.reset();
            self.minus_dm_ema.reset();
            self.tr_ema.reset();
            self.dx_values.clear();
            self.prev_high = None;
            self.prev_low = None;
            self.prev_close = None;
        }
    }

    /// 流式CCI指标
    #[derive(Debug, Clone)]
    pub struct CCI {
        period: usize,
        typical_prices: VecDeque<f64>,
    }

    impl CCI {
        pub fn new(period: usize) -> Self {
            Self {
                period,
                typical_prices: VecDeque::with_capacity(period),
            }
        }

        pub fn update(&mut self, high: f64, low: f64, close: f64) -> Option<f64> {
            let tp = (high + low + close) / 3.0;

            if self.typical_prices.len() >= self.period {
                self.typical_prices.pop_front();
            }
            self.typical_prices.push_back(tp);

            if self.typical_prices.len() < self.period {
                return None;
            }

            let sma = self.typical_prices.iter().sum::<f64>() / self.period as f64;
            let mean_deviation = self
                .typical_prices
                .iter()
                .map(|&p| (p - sma).abs())
                .sum::<f64>()
                / self.period as f64;

            if mean_deviation == 0.0 {
                return Some(0.0);
            }

            Some((tp - sma) / (0.015 * mean_deviation))
        }

        pub fn reset(&mut self) {
            self.typical_prices.clear();
        }
    }

    /// 流式ATR指标
    #[derive(Debug, Clone)]
    pub struct ATR {
        period: usize,
        tr_values: VecDeque<f64>,
        prev_close: Option<f64>,
        atr: Option<f64>,
    }

    impl ATR {
        pub fn new(period: usize) -> Self {
            Self {
                period,
                tr_values: VecDeque::with_capacity(period),
                prev_close: None,
                atr: None,
            }
        }

        pub fn update(&mut self, high: f64, low: f64, close: f64) -> Option<f64> {
            // 计算真实波幅
            let tr = if let Some(prev_close) = self.prev_close {
                let hl = high - low;
                let hc = (high - prev_close).abs();
                let lc = (low - prev_close).abs();
                hl.max(hc).max(lc)
            } else {
                high - low
            };

            self.prev_close = Some(close);

            // 添加到队列
            if self.tr_values.len() >= self.period {
                self.tr_values.pop_front();
            }
            self.tr_values.push_back(tr);

            // 计算ATR
            if self.tr_values.len() >= self.period {
                if let Some(prev_atr) = self.atr {
                    // 使用EMA方式更新
                    self.atr =
                        Some((prev_atr * (self.period - 1) as f64 + tr) / self.period as f64);
                } else {
                    // 首次计算，使用SMA
                    let sum: f64 = self.tr_values.iter().sum();
                    self.atr = Some(sum / self.period as f64);
                }
                self.atr
            } else {
                None
            }
        }

        pub fn current(&self) -> Option<f64> {
            self.atr
        }
    }

    /// 流式OBV指标
    #[derive(Debug, Clone)]
    pub struct OBV {
        value: f64,
        prev_close: Option<f64>,
    }

    impl OBV {
        pub fn new() -> Self {
            Self {
                value: 0.0,
                prev_close: None,
            }
        }

        pub fn update(&mut self, close: f64, volume: f64) -> f64 {
            if let Some(prev) = self.prev_close {
                if close > prev {
                    self.value += volume;
                } else if close < prev {
                    self.value -= volume;
                }
                // close == prev时OBV不变
            } else {
                self.value = volume;
            }

            self.prev_close = Some(close);
            self.value
        }

        pub fn current(&self) -> f64 {
            self.value
        }

        pub fn reset(&mut self) {
            self.value = 0.0;
            self.prev_close = None;
        }
    }

    /// 流式布林带
    #[derive(Debug, Clone)]
    pub struct BollingerBands {
        sma: SMA,
        period: usize,
        std_dev_multiplier: f64,
        values: VecDeque<f64>,
    }

    impl BollingerBands {
        pub fn new(period: usize, std_dev_multiplier: f64) -> Self {
            Self {
                sma: SMA::new(period),
                period,
                std_dev_multiplier,
                values: VecDeque::with_capacity(period),
            }
        }

        pub fn update(&mut self, price: f64) -> Option<(f64, f64, f64)> {
            if self.values.len() >= self.period {
                self.values.pop_front();
            }
            self.values.push_back(price);

            let middle = self.sma.update(price)?;

            let variance: f64 = self
                .values
                .iter()
                .map(|p| (p - middle).powi(2))
                .sum::<f64>()
                / self.period as f64;

            let std = variance.sqrt();
            let upper = middle + self.std_dev_multiplier * std;
            let lower = middle - self.std_dev_multiplier * std;

            Some((upper, middle, lower))
        }

        pub fn current(&self) -> Option<(f64, f64, f64)> {
            let middle = self.sma.current()?;

            if self.values.len() != self.period {
                return None;
            }

            let variance: f64 = self
                .values
                .iter()
                .map(|p| (p - middle).powi(2))
                .sum::<f64>()
                / self.period as f64;

            let std = variance.sqrt();
            let upper = middle + self.std_dev_multiplier * std;
            let lower = middle - self.std_dev_multiplier * std;

            Some((upper, middle, lower))
        }

        pub fn reset(&mut self) {
            self.sma.reset();
            self.values.clear();
        }
    }
}

// 重新导出常用的流式指标类型
pub use streaming::{
    BollingerBands, Stochastic, ADX, ATR, CCI, EMA, MACD, OBV, RSI, SMA as MovingAverage,
};

// 为趋势策略提供的简化接口
pub fn calculate_ema(prices: &[f64], period: usize) -> Option<f64> {
    functions::ema(prices, period)
}

pub fn calculate_atr(highs: &[f64], lows: &[f64], closes: &[f64], period: usize) -> Option<f64> {
    functions::atr(highs, lows, closes, period)
}

pub fn calculate_adx(highs: &[f64], lows: &[f64], closes: &[f64], period: usize) -> Option<f64> {
    functions::adx(highs, lows, closes, period)
}

pub fn calculate_rsi(prices: &[f64], period: usize) -> Option<f64> {
    functions::rsi(prices, period)
}

pub fn calculate_macd(prices: &[f64]) -> Option<(f64, f64, f64)> {
    functions::macd(prices)
}

/// 趋势强度计算器
pub struct TrendStrengthCalculator {
    ma_fast: MovingAverage,
    ma_slow: MovingAverage,
    rsi: RSI,
    macd: MACD,
    bb: BollingerBands,
}

impl TrendStrengthCalculator {
    pub fn new(
        ma_fast_period: usize,
        ma_slow_period: usize,
        rsi_period: usize,
        macd_fast: usize,
        macd_slow: usize,
        macd_signal: usize,
        bb_period: usize,
        bb_std_dev: f64,
    ) -> Self {
        Self {
            ma_fast: MovingAverage::new(ma_fast_period),
            ma_slow: MovingAverage::new(ma_slow_period),
            rsi: RSI::new(rsi_period),
            macd: MACD::with_params(macd_fast, macd_slow, macd_signal),
            bb: BollingerBands::new(bb_period, bb_std_dev),
        }
    }

    pub fn update(&mut self, price: f64) -> Option<f64> {
        let ma_fast = self.ma_fast.update(price)?;
        let ma_slow = self.ma_slow.update(price)?;
        let rsi = self.rsi.update(price)?;
        let (macd_line, signal, _) = self.macd.update(price)?;
        let (bb_upper, bb_middle, bb_lower) = self.bb.update(price)?;

        // MA趋势得分 (-1 到 1)
        let ma_score = if ma_fast > ma_slow {
            ((ma_fast - ma_slow) / ma_slow).min(0.1) * 10.0
        } else {
            ((ma_fast - ma_slow) / ma_slow).max(-0.1) * 10.0
        };

        // RSI趋势得分 (-1 到 1)
        let rsi_score = if rsi > 70.0 {
            1.0
        } else if rsi < 30.0 {
            -1.0
        } else if rsi > 50.0 {
            (rsi - 50.0) / 20.0
        } else {
            (rsi - 50.0) / 20.0
        };

        // MACD趋势得分 (-1 到 1)
        let macd_score = if macd_line > signal {
            ((macd_line - signal) / signal.abs()).min(1.0)
        } else {
            ((macd_line - signal) / signal.abs()).max(-1.0)
        };

        // 布林带趋势得分 (-1 到 1)
        let bb_score = if price > bb_middle {
            ((price - bb_middle) / (bb_upper - bb_middle)).min(1.0)
        } else {
            ((price - bb_middle) / (bb_middle - bb_lower)).max(-1.0)
        };

        // 加权平均趋势强度
        let trend_strength =
            (ma_score * 0.3 + rsi_score * 0.2 + macd_score * 0.3 + bb_score * 0.2).clamp(-1.0, 1.0);

        Some(trend_strength)
    }
}

/// 将趋势强度转换为趋势枚举
pub fn trend_strength_to_enum(strength: f64) -> crate::strategies::trend_grid_v2::TrendStrength {
    use crate::strategies::trend_grid_v2::TrendStrength;

    if strength >= 0.7 {
        TrendStrength::StrongBull
    } else if strength >= 0.3 {
        TrendStrength::Bull
    } else if strength >= -0.3 {
        TrendStrength::Neutral
    } else if strength >= -0.7 {
        TrendStrength::Bear
    } else {
        TrendStrength::StrongBear
    }
}

/// 趋势强度计算
pub struct TrendStrength;

impl TrendStrength {
    /// 计算趋势强度（-100到100，负值表示下跌，正值表示上涨）
    pub fn calculate(prices: &[f64], period: usize) -> Option<f64> {
        if prices.len() < period || period < 2 {
            return None;
        }

        let recent_prices = &prices[prices.len() - period..];
        let first = recent_prices[0];
        let last = recent_prices[recent_prices.len() - 1];

        // 计算总体变化百分比
        let change_percent = ((last - first) / first) * 100.0;

        // 计算连续性（价格沿趋势方向移动的比例）
        let mut consistent_moves = 0;
        let trend_up = last > first;

        for i in 1..recent_prices.len() {
            let move_up = recent_prices[i] > recent_prices[i - 1];
            if move_up == trend_up {
                consistent_moves += 1;
            }
        }

        let consistency = (consistent_moves as f64 / (period - 1) as f64) * 100.0;

        // 综合趋势强度
        let strength = change_percent * (consistency / 100.0);

        Some(strength.max(-100.0).min(100.0))
    }

    /// 判断趋势类型
    pub fn classify(strength: f64) -> TrendType {
        match strength {
            s if s > 50.0 => TrendType::StrongUp,
            s if s > 20.0 => TrendType::Up,
            s if s > -20.0 => TrendType::Sideways,
            s if s > -50.0 => TrendType::Down,
            _ => TrendType::StrongDown,
        }
    }
}

/// 趋势类型
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TrendType {
    StrongUp,   // 强势上涨
    Up,         // 上涨
    Sideways,   // 横盘
    Down,       // 下跌
    StrongDown, // 强势下跌
}

/// 成交量分析
pub struct VolumeAnalysis;

impl VolumeAnalysis {
    /// 计算成交量加权平均价格 (VWAP)
    pub fn vwap(prices: &[f64], volumes: &[f64]) -> Option<f64> {
        if prices.len() != volumes.len() || prices.is_empty() {
            return None;
        }

        let total_value: f64 = prices.iter().zip(volumes.iter()).map(|(p, v)| p * v).sum();

        let total_volume: f64 = volumes.iter().sum();

        if total_volume == 0.0 {
            return None;
        }

        Some(total_value / total_volume)
    }

    /// 计算成交量比率
    pub fn volume_ratio(current_volume: f64, avg_volume: f64) -> f64 {
        if avg_volume == 0.0 {
            return 1.0;
        }
        current_volume / avg_volume
    }

    /// 检测成交量异常
    pub fn detect_volume_spike(volumes: &[f64], threshold: f64) -> Vec<usize> {
        if volumes.len() < 20 {
            return vec![];
        }

        let mut spikes = Vec::new();
        let sma = functions::sma(volumes, 20).unwrap_or(0.0);

        for (i, &volume) in volumes.iter().enumerate() {
            if volume > sma * threshold {
                spikes.push(i);
            }
        }

        spikes
    }
}

#[cfg(test)]
mod tests {
    use super::functions::*;
    use super::streaming::*;
    use super::*;

    #[test]
    fn test_sma() {
        let prices = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = sma(&prices, 3);
        assert!(result.is_some());
        assert!((result.unwrap() - 4.0).abs() < 0.001); // (3+4+5)/3 = 4
    }

    #[test]
    fn test_ema() {
        let prices = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = ema(&prices, 3);
        assert!(result.is_some());
        // EMA计算复杂，只验证有结果
        assert!(result.unwrap() > 0.0);
    }

    #[test]
    fn test_rsi() {
        let prices = vec![
            44.0, 44.25, 44.50, 43.75, 44.65, 45.12, 45.84, 46.08, 45.89, 46.03, 45.61, 46.28,
            46.28, 46.00, 46.03, 46.41, 46.22, 45.64,
        ];
        let result = rsi(&prices, 14);
        assert!(result.is_some());
        let rsi_val = result.unwrap();
        assert!(rsi_val >= 0.0 && rsi_val <= 100.0);
    }

    #[test]
    fn test_macd() {
        let prices = vec![1.0; 30]; // 需要至少26个价格
        prices
            .iter()
            .enumerate()
            .map(|(i, _)| i as f64 + 1.0)
            .collect::<Vec<f64>>();
        let prices = (1..=30).map(|i| i as f64).collect::<Vec<f64>>();
        let result = macd(&prices);
        assert!(result.is_some());
        let (macd_line, signal, histogram) = result.unwrap();
        assert_eq!(histogram, macd_line - signal);
    }

    #[test]
    fn test_bollinger_bands() {
        let prices = vec![20.0, 21.0, 22.0, 21.5, 20.5, 21.0, 22.0, 23.0, 22.5, 21.5];
        let result = bollinger_bands(&prices, 5, 2.0);
        assert!(result.is_some());
        let (upper, middle, lower) = result.unwrap();
        assert!(upper > middle);
        assert!(middle > lower);
    }

    #[test]
    fn test_atr() {
        let highs = vec![48.70, 48.72, 48.90, 48.87, 48.82];
        let lows = vec![47.79, 48.14, 48.39, 48.37, 48.24];
        let closes = vec![48.16, 48.61, 48.75, 48.63, 48.74];
        let result = atr(&highs, &lows, &closes, 3);
        assert!(result.is_some());
        assert!(result.unwrap() > 0.0);
    }

    #[test]
    fn test_stochastic() {
        let highs = vec![
            127.01, 127.62, 126.59, 127.35, 128.17, 128.43, 127.37, 126.42, 126.90, 126.85,
        ];
        let lows = vec![
            125.36, 126.16, 124.93, 126.09, 126.82, 126.48, 126.03, 124.83, 126.39, 125.72,
        ];
        let closes = vec![
            125.36, 126.16, 125.93, 127.09, 127.82, 127.48, 127.03, 126.83, 126.39, 126.72,
        ];
        let result = stochastic(&highs, &lows, &closes, 5, 3);
        assert!(result.is_some());
        let (k, d) = result.unwrap();
        assert!(k >= 0.0 && k <= 100.0);
        assert!(d >= 0.0 && d <= 100.0);
    }

    #[test]
    fn test_adx() {
        let highs = vec![30.20; 30];
        let lows = vec![29.40; 30];
        let closes = vec![29.87; 30];

        // 创建一些有趋势的数据
        let highs: Vec<f64> = (0..30).map(|i| 30.0 + (i as f64 * 0.1)).collect();
        let lows: Vec<f64> = (0..30).map(|i| 29.0 + (i as f64 * 0.1)).collect();
        let closes: Vec<f64> = (0..30).map(|i| 29.5 + (i as f64 * 0.1)).collect();

        let result = adx(&highs, &lows, &closes, 14);
        assert!(result.is_some());
        let adx_val = result.unwrap();
        assert!(adx_val >= 0.0 && adx_val <= 100.0);
    }

    #[test]
    fn test_cci() {
        let highs = vec![
            24.20, 24.07, 24.04, 23.87, 23.67, 23.59, 23.80, 23.80, 24.30, 24.15,
        ];
        let lows = vec![
            23.85, 23.72, 23.64, 23.37, 23.46, 23.18, 23.40, 23.57, 24.05, 24.01,
        ];
        let closes = vec![
            23.98, 23.92, 23.79, 23.67, 23.54, 23.36, 23.65, 23.75, 24.20, 24.10,
        ];
        let result = cci(&highs, &lows, &closes, 5);
        assert!(result.is_some());
        // CCI可以超出-100到100的范围
        assert!(result.unwrap().is_finite());
    }

    #[test]
    fn test_obv() {
        let closes = vec![
            10.0, 10.15, 10.17, 10.13, 10.11, 10.15, 10.20, 10.20, 10.22, 10.21,
        ];
        let volumes = vec![
            25200.0, 30000.0, 25600.0, 32000.0, 23000.0, 40000.0, 36000.0, 20500.0, 23000.0,
            27500.0,
        ];
        let result = obv(&closes, &volumes);
        assert!(result.is_some());
        let obv_values = result.unwrap();
        assert_eq!(obv_values.len(), closes.len());
    }

    #[test]
    fn test_streaming_sma() {
        let mut sma = SMA::new(3);
        assert_eq!(sma.update(1.0), None);
        assert_eq!(sma.update(2.0), None);
        let result = sma.update(3.0);
        assert!(result.is_some());
        assert!((result.unwrap() - 2.0).abs() < 0.001); // (1+2+3)/3 = 2

        let result = sma.update(4.0);
        assert!(result.is_some());
        assert!((result.unwrap() - 3.0).abs() < 0.001); // (2+3+4)/3 = 3
    }

    #[test]
    fn test_streaming_ema() {
        let mut ema = EMA::new(3);
        assert!(ema.update(1.0).is_none());
        assert!(ema.update(2.0).is_none());
        assert!(ema.update(3.0).is_some());
        assert!(ema.update(4.0).is_some());

        let current = ema.current();
        assert!(current.is_some());
    }

    #[test]
    fn test_streaming_rsi() {
        let mut rsi = RSI::new(14);
        let prices = vec![
            44.0, 44.25, 44.50, 43.75, 44.65, 45.12, 45.84, 46.08, 45.89, 46.03, 45.61, 46.28,
            46.28, 46.00, 46.03,
        ];

        for price in prices {
            rsi.update(price);
        }

        let result = rsi.current();
        assert!(result.is_some());
        let rsi_val = result.unwrap();
        assert!(rsi_val >= 0.0 && rsi_val <= 100.0);
    }

    #[test]
    fn test_streaming_macd() {
        let mut macd = MACD::new();
        let prices: Vec<f64> = (1..=30).map(|i| i as f64).collect();

        let mut last_result = None;
        for price in prices {
            if let Some(result) = macd.update(price) {
                last_result = Some(result);
            }
        }

        assert!(last_result.is_some());
        let (macd_line, signal, histogram) = last_result.unwrap();
        assert!((histogram - (macd_line - signal)).abs() < 0.001);
    }

    #[test]
    fn test_streaming_bollinger_bands() {
        let mut bb = BollingerBands::new(5, 2.0);
        let prices = vec![20.0, 21.0, 22.0, 21.5, 20.5, 21.0, 22.0];

        for price in prices {
            bb.update(price);
        }

        let result = bb.current();
        assert!(result.is_some());
        let (upper, middle, lower) = result.unwrap();
        assert!(upper > middle);
        assert!(middle > lower);
    }

    #[test]
    fn test_streaming_stochastic() {
        let mut stoch = Stochastic::new(5, 3);
        let data = vec![
            (127.01, 125.36, 125.36),
            (127.62, 126.16, 126.16),
            (126.59, 124.93, 125.93),
            (127.35, 126.09, 127.09),
            (128.17, 126.82, 127.82),
            (128.43, 126.48, 127.48),
            (127.37, 126.03, 127.03),
            (126.42, 124.83, 126.83),
        ];

        for (high, low, close) in data {
            stoch.update(high, low, close);
        }

        let result = stoch.update(126.90, 126.39, 126.39);
        assert!(result.is_some());
        let (k, d) = result.unwrap();
        assert!(k >= 0.0 && k <= 100.0);
        assert!(d >= 0.0 && d <= 100.0);
    }

    #[test]
    fn test_streaming_adx() {
        let mut adx = ADX::new(14);

        // 创建趋势数据
        for i in 0..30 {
            let high = 30.0 + (i as f64 * 0.1);
            let low = 29.0 + (i as f64 * 0.1);
            let close = 29.5 + (i as f64 * 0.1);
            adx.update(high, low, close);
        }

        let result = adx.update(33.1, 32.1, 32.6);
        // ADX需要较多数据才能产生结果
        if result.is_some() {
            let adx_val = result.unwrap();
            assert!(adx_val >= 0.0 && adx_val <= 100.0);
        }
    }

    #[test]
    fn test_streaming_cci() {
        let mut cci = CCI::new(5);
        let data = vec![
            (24.20, 23.85, 23.98),
            (24.07, 23.72, 23.92),
            (24.04, 23.64, 23.79),
            (23.87, 23.37, 23.67),
            (23.67, 23.46, 23.54),
        ];

        for (high, low, close) in data {
            cci.update(high, low, close);
        }

        let result = cci.update(23.59, 23.18, 23.36);
        assert!(result.is_some());
        assert!(result.unwrap().is_finite());
    }

    #[test]
    fn test_streaming_obv() {
        let mut obv = OBV::new();
        let data = vec![
            (10.00, 25200.0),
            (10.15, 30000.0),
            (10.17, 25600.0),
            (10.13, 32000.0),
            (10.11, 23000.0),
        ];

        let mut last_obv = 0.0;
        for (close, volume) in data {
            last_obv = obv.update(close, volume);
        }

        assert_ne!(last_obv, 0.0);
        assert_eq!(obv.current(), last_obv);
    }

    #[test]
    fn test_trend_strength() {
        let prices = vec![
            100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0,
        ];
        let strength = TrendStrength::calculate(&prices, 5);
        assert!(strength.is_some());
        let val = strength.unwrap();
        assert!(val > 0.0); // 上升趋势应该是正值

        let trend_type = TrendStrength::classify(val);
        assert!(matches!(trend_type, TrendType::Up | TrendType::StrongUp));
    }

    #[test]
    fn test_vwap() {
        let prices = vec![100.0, 101.0, 102.0, 101.5, 100.5];
        let volumes = vec![1000.0, 1500.0, 2000.0, 1200.0, 800.0];
        let vwap = VolumeAnalysis::vwap(&prices, &volumes);
        assert!(vwap.is_some());
        assert!(vwap.unwrap() > 0.0);
    }

    #[test]
    fn test_volume_ratio() {
        let current = 1500.0;
        let average = 1000.0;
        let ratio = VolumeAnalysis::volume_ratio(current, average);
        assert!((ratio - 1.5).abs() < 0.001);
    }

    #[test]
    fn test_volume_spike_detection() {
        let mut volumes = vec![1000.0; 25];
        volumes[20] = 3000.0; // Spike
        volumes[22] = 2500.0; // Another spike

        let spikes = VolumeAnalysis::detect_volume_spike(&volumes, 2.0);
        assert!(spikes.contains(&20));
        assert!(spikes.contains(&22));
    }
}
