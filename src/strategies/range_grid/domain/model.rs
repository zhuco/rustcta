use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::core::types::{Order, OrderSide, OrderType};

use super::config::SymbolConfig;

/// 市场状态枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MarketRegime {
    Range,
    TrendUp,
    TrendDown,
    Cooldown,
    Disabled,
}

impl MarketRegime {
    pub fn is_range(self) -> bool {
        matches!(self, MarketRegime::Range)
    }
}

/// 指标快照
#[derive(Debug, Clone, Default)]
pub struct IndicatorSnapshot {
    pub timestamp: DateTime<Utc>,
    pub last_price: f64,
    pub lower_band_ratio: f64,
    pub z_score: f64,
    pub rsi: f64,
    pub atr: f64,
    pub adx: f64,
    pub bbw_percentile: f64,
    pub slope_metric: f64,
    pub choppiness: Option<f64>,
}

/// 网格订单计划
#[derive(Debug, Clone)]
pub struct GridOrderPlan {
    pub client_id: String,
    pub price: f64,
    pub quantity: f64,
    pub side: OrderSide,
    pub order_type: OrderType,
}

/// 网格计划集合
#[derive(Debug, Clone, Default)]
pub struct GridPlan {
    pub center_price: f64,
    pub orders: Vec<GridOrderPlan>,
}

/// 精度信息
#[derive(Debug, Clone)]
pub struct PairPrecision {
    pub price_digits: u32,
    pub amount_digits: u32,
    pub price_step: f64,
    pub amount_step: f64,
    pub min_notional: Option<f64>,
}

impl Default for PairPrecision {
    fn default() -> Self {
        Self {
            price_digits: 0,
            amount_digits: 0,
            price_step: 0.0,
            amount_step: 0.0,
            min_notional: None,
        }
    }
}

/// 单个交易对运行态
#[derive(Debug)]
pub struct PairRuntimeState {
    pub config: SymbolConfig,
    pub regime: MarketRegime,
    pub grid_active: bool,
    pub need_rebuild: bool,
    pub cooling_down: bool,
    pub cooldown_until: Option<DateTime<Utc>>,
    pub precision: Option<PairPrecision>,
    pub indicator: Option<IndicatorSnapshot>,
    pub current_plan: Option<GridPlan>,
    pub open_orders: HashMap<String, ActiveOrder>,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub net_position: f64,
    pub current_price: f64,
    pub last_grid_rebuild: Option<DateTime<Utc>>,
    pub last_notification: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub pending_center_price: Option<f64>,
    pub last_fill_price: Option<f64>,
}

impl PairRuntimeState {
    pub fn new(config: SymbolConfig) -> Self {
        Self {
            config,
            regime: MarketRegime::Disabled,
            grid_active: false,
            need_rebuild: false,
            cooling_down: false,
            cooldown_until: None,
            precision: None,
            indicator: None,
            current_plan: None,
            open_orders: HashMap::new(),
            realized_pnl: 0.0,
            unrealized_pnl: 0.0,
            net_position: 0.0,
            current_price: 0.0,
            last_grid_rebuild: None,
            last_notification: None,
            started_at: None,
            pending_center_price: None,
            last_fill_price: None,
        }
    }

    pub fn mark_precision(&mut self, precision: PairPrecision) {
        self.precision = Some(precision);
    }

    pub fn mark_indicator(&mut self, indicator: IndicatorSnapshot) {
        let price = indicator.last_price;
        self.indicator = Some(indicator);
        self.current_price = price;
    }

    pub fn mark_regime(&mut self, regime: MarketRegime) {
        self.regime = regime;
    }

    pub fn activate_grid(&mut self, plan: GridPlan) {
        self.grid_active = true;
        self.current_plan = Some(plan);
        self.last_grid_rebuild = Some(Utc::now());
        self.started_at = self.started_at.or_else(|| Some(Utc::now()));
    }

    pub fn deactivate_grid(&mut self) {
        self.grid_active = false;
        self.current_plan = None;
        self.open_orders.clear();
    }
}

#[derive(Debug, Clone)]
pub struct ActiveOrder {
    pub order: Order,
    pub client_id: Option<String>,
}

impl ActiveOrder {
    pub fn new(order: Order, client_id: Option<String>) -> Self {
        Self { order, client_id }
    }
}

/// 判定结果
#[derive(Debug, Clone)]
pub struct RegimeDecision {
    pub new_regime: MarketRegime,
    pub activate_grid: bool,
    pub deactivate_grid: bool,
    pub reason: Option<String>,
}

impl RegimeDecision {
    pub fn stay(regime: MarketRegime) -> Self {
        Self {
            new_regime: regime,
            activate_grid: false,
            deactivate_grid: false,
            reason: None,
        }
    }
}
