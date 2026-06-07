use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use chrono::{DateTime, Utc};
use rustcta_strategy_sdk::OrderSide;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HedgedGridCoreConfig {
    pub symbol: String,
    #[serde(default = "default_require_hedge_mode")]
    pub require_hedge_mode: bool,
    #[serde(default)]
    pub price_reference: PriceReference,
    #[serde(default)]
    pub risk_reference: RiskReference,
    pub grid: GridConfig,
    #[serde(default)]
    pub follow: FollowConfig,
    #[serde(default)]
    pub execution: ExecutionConfig,
    pub precision: PrecisionConfig,
    #[serde(default)]
    pub fees: FeeConfig,
    pub risk: RiskLimits,
}

impl HedgedGridCoreConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.symbol.trim().is_empty() {
            return Err("symbol must not be empty".to_string());
        }
        if !(1..=10).contains(&self.grid.levels_per_side) {
            return Err("levels_per_side must be in 1..=10".to_string());
        }
        if let Some(abs) = self.grid.grid_spacing_abs {
            if abs <= 0.0 {
                return Err("grid_spacing_abs must be positive".to_string());
            }
        } else if self.grid.grid_spacing_pct <= 0.0 {
            return Err("grid_spacing_pct must be positive".to_string());
        }
        if let Some(qty) = self.grid.order_qty {
            if qty <= 0.0 {
                return Err("order_qty must be positive".to_string());
            }
        } else if self.grid.order_notional <= 0.0 {
            return Err("order_notional must be positive".to_string());
        }
        if self.precision.tick_size <= 0.0 || self.precision.step_size <= 0.0 {
            return Err("tick_size and step_size must be positive".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GridConfig {
    pub levels_per_side: usize,
    #[serde(default)]
    pub grid_spacing_pct: f64,
    #[serde(default)]
    pub grid_spacing_abs: Option<f64>,
    pub order_notional: f64,
    #[serde(default)]
    pub order_qty: Option<f64>,
    #[serde(default = "default_fill_remaining_slots_with_opens")]
    pub fill_remaining_slots_with_opens: bool,
    #[serde(default)]
    pub strict_pairing: bool,
    #[serde(default = "default_refill_open_slots_enabled")]
    pub refill_open_slots_enabled: bool,
    #[serde(default = "default_normalize_open_grid_enabled")]
    pub normalize_open_grid_enabled: bool,
    #[serde(default = "default_follow_open_enabled")]
    pub follow_open_enabled: bool,
    #[serde(default = "default_repair_near_gap_enabled")]
    pub repair_near_gap_enabled: bool,
}

fn default_fill_remaining_slots_with_opens() -> bool {
    true
}

fn default_refill_open_slots_enabled() -> bool {
    true
}

fn default_normalize_open_grid_enabled() -> bool {
    true
}

fn default_follow_open_enabled() -> bool {
    true
}

fn default_repair_near_gap_enabled() -> bool {
    false
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FollowConfig {
    #[serde(default = "default_max_gap_steps")]
    pub max_gap_steps: f64,
    #[serde(default = "default_follow_cooldown_ms")]
    pub follow_cooldown_ms: u64,
    #[serde(default = "default_max_follow_actions_per_minute")]
    pub max_follow_actions_per_minute: usize,
}

impl Default for FollowConfig {
    fn default() -> Self {
        Self {
            max_gap_steps: default_max_gap_steps(),
            follow_cooldown_ms: default_follow_cooldown_ms(),
            max_follow_actions_per_minute: default_max_follow_actions_per_minute(),
        }
    }
}

fn default_max_gap_steps() -> f64 {
    1.0
}

fn default_follow_cooldown_ms() -> u64 {
    800
}

fn default_max_follow_actions_per_minute() -> usize {
    30
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExecutionConfig {
    #[serde(default = "default_cooldown_ms")]
    pub cooldown_ms: u64,
    #[serde(default = "default_post_only")]
    pub post_only: bool,
    #[serde(default = "default_post_only_retries")]
    pub post_only_retries: u32,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            cooldown_ms: default_cooldown_ms(),
            post_only: default_post_only(),
            post_only_retries: default_post_only_retries(),
        }
    }
}

fn default_cooldown_ms() -> u64 {
    500
}

fn default_post_only() -> bool {
    true
}

fn default_post_only_retries() -> u32 {
    3
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PrecisionConfig {
    pub tick_size: f64,
    pub step_size: f64,
    #[serde(default)]
    pub min_qty: Option<f64>,
    #[serde(default)]
    pub min_notional: Option<f64>,
    #[serde(default)]
    pub price_digits: Option<u32>,
    #[serde(default)]
    pub qty_digits: Option<u32>,
}

impl PrecisionConfig {
    pub fn resolve(&self) -> ResolvedPrecision {
        ResolvedPrecision {
            tick_size: self.tick_size,
            step_size: self.step_size,
            min_qty: self.min_qty.unwrap_or(self.step_size),
            min_notional: self.min_notional.unwrap_or(0.0),
            price_digits: self
                .price_digits
                .unwrap_or_else(|| precision_from_step(self.tick_size)),
            qty_digits: self
                .qty_digits
                .unwrap_or_else(|| precision_from_step(self.step_size)),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedPrecision {
    pub tick_size: f64,
    pub step_size: f64,
    pub min_qty: f64,
    pub min_notional: f64,
    pub price_digits: u32,
    pub qty_digits: u32,
}

impl ResolvedPrecision {
    pub fn quantize_price(&self, price: f64) -> f64 {
        if price <= 0.0 {
            return 0.0;
        }
        apply_precision(price, self.tick_size, self.price_digits)
    }

    pub fn quantize_price_for_side(&self, price: f64, side: &OrderSide) -> f64 {
        if price <= 0.0 {
            return 0.0;
        }
        let eps = 1e-9;
        let adjusted = if self.tick_size > 0.0 {
            let multiples = price / self.tick_size;
            match side {
                OrderSide::Buy => (multiples + eps).floor() * self.tick_size,
                OrderSide::Sell => (multiples - eps).ceil() * self.tick_size,
            }
        } else {
            price
        };
        let factor = 10f64.powi(self.price_digits as i32);
        match side {
            OrderSide::Buy => ((adjusted * factor) + eps).floor() / factor,
            OrderSide::Sell => ((adjusted * factor) - eps).ceil() / factor,
        }
    }

    pub fn quantize_qty(&self, qty: f64) -> f64 {
        if qty <= 0.0 {
            return 0.0;
        }
        apply_precision(qty, self.step_size, self.qty_digits)
    }

    pub fn quantize_qty_up(&self, qty: f64) -> f64 {
        if qty <= 0.0 {
            return 0.0;
        }
        let eps = 1e-9;
        let adjusted = if self.step_size > 0.0 {
            ((qty / self.step_size) - eps).ceil() * self.step_size
        } else {
            qty
        };
        let factor = 10f64.powi(self.qty_digits as i32);
        ((adjusted * factor) - eps).ceil() / factor
    }

    pub fn quantize_qty_nearest(&self, qty: f64) -> f64 {
        if qty <= 0.0 {
            return 0.0;
        }
        let adjusted = if self.step_size > 0.0 {
            (qty / self.step_size).round() * self.step_size
        } else {
            qty
        };
        let factor = 10f64.powi(self.qty_digits as i32);
        (adjusted * factor).round() / factor
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FeeConfig {
    pub maker_fee: f64,
    pub taker_fee: f64,
}

impl Default for FeeConfig {
    fn default() -> Self {
        Self {
            maker_fee: 0.0,
            taker_fee: 0.0004,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RiskLimits {
    pub max_net_notional: f64,
    pub max_total_notional: f64,
    pub margin_ratio_limit: f64,
    pub funding_rate_limit: f64,
    pub funding_cost_limit: f64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PriceReference {
    Mid,
    Last,
}

impl Default for PriceReference {
    fn default() -> Self {
        Self::Mid
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RiskReference {
    Mark,
    Last,
}

impl Default for RiskReference {
    fn default() -> Self {
        Self::Mark
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MarketSnapshot {
    pub best_bid: f64,
    pub best_ask: f64,
    pub last_price: f64,
    pub mark_price: f64,
    pub timestamp: DateTime<Utc>,
}

impl MarketSnapshot {
    pub fn mid(&self) -> f64 {
        (self.best_bid + self.best_ask) * 0.5
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PositionState {
    pub long_qty: f64,
    pub short_qty: f64,
    pub long_entry_price: f64,
    pub short_entry_price: f64,
    pub long_available: f64,
    pub short_available: f64,
    pub equity: f64,
    pub maintenance_margin: f64,
    pub mark_price: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PositionSide {
    Long,
    Short,
}

impl PositionSide {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Long => "LONG",
            Self::Short => "SHORT",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderIntent {
    OpenLongBuy,
    CloseLongSell,
    OpenShortSell,
    CloseShortBuy,
}

impl OrderIntent {
    pub fn side(&self) -> OrderSide {
        match self {
            OrderIntent::OpenLongBuy | OrderIntent::CloseShortBuy => OrderSide::Buy,
            OrderIntent::OpenShortSell | OrderIntent::CloseLongSell => OrderSide::Sell,
        }
    }

    pub fn position_side(&self) -> PositionSide {
        match self {
            OrderIntent::OpenLongBuy | OrderIntent::CloseLongSell => PositionSide::Long,
            OrderIntent::OpenShortSell | OrderIntent::CloseShortBuy => PositionSide::Short,
        }
    }

    pub fn is_open(&self) -> bool {
        matches!(self, Self::OpenLongBuy | Self::OpenShortSell)
    }

    pub fn is_close(&self) -> bool {
        matches!(self, Self::CloseLongSell | Self::CloseShortBuy)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderRecord {
    pub id: String,
    pub intent: OrderIntent,
    pub price: f64,
    pub qty: f64,
    pub filled_qty: f64,
    pub created_at: DateTime<Utc>,
    pub retries: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderSlot {
    pub id: String,
    pub intent: OrderIntent,
    pub price: f64,
    pub qty: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GridSideBook {
    pub side: OrderSide,
    pub slots: Vec<OrderSlot>,
}

impl GridSideBook {
    pub fn new(side: OrderSide) -> Self {
        Self {
            side,
            slots: Vec::new(),
        }
    }

    pub fn insert(&mut self, slot: OrderSlot) {
        self.slots.push(slot);
        self.sort();
    }

    pub fn remove(&mut self, order_id: &str) -> Option<OrderSlot> {
        self.slots
            .iter()
            .position(|slot| slot.id == order_id)
            .map(|index| self.slots.remove(index))
    }

    pub fn update_qty(&mut self, order_id: &str, qty: f64) {
        if let Some(slot) = self.slots.iter_mut().find(|slot| slot.id == order_id) {
            slot.qty = qty;
        }
    }

    pub fn nearest(&self) -> Option<&OrderSlot> {
        self.slots.first()
    }

    pub fn farthest(&self) -> Option<&OrderSlot> {
        self.slots.last()
    }

    pub fn nearest_price(&self) -> Option<f64> {
        self.nearest().map(|slot| slot.price)
    }

    pub fn farthest_price(&self) -> Option<f64> {
        self.farthest().map(|slot| slot.price)
    }

    pub fn count_by_intent(&self, intent: OrderIntent) -> usize {
        self.slots
            .iter()
            .filter(|slot| slot.intent == intent)
            .count()
    }

    pub fn total_qty_for_intent(&self, intent: OrderIntent) -> f64 {
        self.slots
            .iter()
            .filter(|slot| slot.intent == intent)
            .map(|slot| slot.qty)
            .sum()
    }

    fn sort(&mut self) {
        match self.side {
            OrderSide::Buy => self.slots.sort_by(|a, b| {
                b.price
                    .partial_cmp(&a.price)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }),
            OrderSide::Sell => self.slots.sort_by(|a, b| {
                a.price
                    .partial_cmp(&b.price)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct OrderLedger {
    pub orders: HashMap<String, OrderRecord>,
}

impl OrderLedger {
    pub fn new() -> Self {
        Self {
            orders: HashMap::new(),
        }
    }

    pub fn insert(&mut self, record: OrderRecord) {
        self.orders.insert(record.id.clone(), record);
    }

    pub fn remove(&mut self, order_id: &str) -> Option<OrderRecord> {
        self.orders.remove(order_id)
    }

    pub fn get(&self, order_id: &str) -> Option<&OrderRecord> {
        self.orders.get(order_id)
    }

    pub fn get_mut(&mut self, order_id: &str) -> Option<&mut OrderRecord> {
        self.orders.get_mut(order_id)
    }

    pub fn all_ids(&self) -> Vec<String> {
        let mut ids = self.orders.keys().cloned().collect::<Vec<_>>();
        ids.sort();
        ids
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderDraft {
    pub id: String,
    pub intent: OrderIntent,
    pub price: f64,
    pub qty: f64,
    pub post_only: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FillEvent {
    pub order_id: String,
    pub intent: OrderIntent,
    pub fill_qty: f64,
    pub fill_price: f64,
    pub timestamp: DateTime<Utc>,
    pub partial: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum EngineAction {
    Place(OrderDraft),
    Cancel { order_id: String, reason: String },
}

#[derive(Debug, Clone, PartialEq)]
pub struct GridPlan {
    pub symbol: String,
    pub reference_price: f64,
    pub risk: RiskState,
    pub orders: Vec<OrderDraft>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RiskState {
    pub net_notional: f64,
    pub total_notional: f64,
    pub margin_ratio: f64,
    pub funding_rate: f64,
    pub expected_funding_cost: f64,
    pub flags: RiskFlags,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct RiskFlags {
    pub block_open_long: bool,
    pub block_open_short: bool,
    pub block_risk_opens: bool,
    pub only_close: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EngineState {
    pub buy_book: GridSideBook,
    pub sell_book: GridSideBook,
    pub ledger: OrderLedger,
    pub position: PositionState,
    pub risk: RiskState,
    pub last_follow_action: Option<DateTime<Utc>>,
    pub follow_actions: VecDeque<DateTime<Utc>>,
    pub funding_rate: f64,
    pub next_id: u64,
    pub kill_switch: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GridEngine {
    config: HedgedGridCoreConfig,
    precision: ResolvedPrecision,
    state: EngineState,
}

impl RiskState {
    pub fn evaluate(limits: &RiskLimits, position: &PositionState, funding_rate: f64) -> Self {
        let long_notional = position.long_qty * position.mark_price;
        let short_notional = position.short_qty * position.mark_price;
        let net_notional = long_notional - short_notional;
        let total_notional = long_notional.abs() + short_notional.abs();
        let margin_ratio = if position.equity > 0.0 {
            position.maintenance_margin / position.equity
        } else {
            1.0
        };
        let expected_funding_cost = funding_rate * total_notional;
        let mut flags = RiskFlags::default();
        if limits.max_net_notional > 0.0 && net_notional > limits.max_net_notional {
            flags.block_open_long = true;
        }
        if limits.max_net_notional > 0.0 && net_notional < -limits.max_net_notional {
            flags.block_open_short = true;
        }
        if limits.max_total_notional > 0.0 && total_notional > limits.max_total_notional {
            flags.block_risk_opens = true;
            flags.only_close = true;
        }
        if margin_ratio > limits.margin_ratio_limit {
            flags.block_risk_opens = true;
            flags.only_close = true;
        }
        if funding_rate.abs() > limits.funding_rate_limit
            || expected_funding_cost.abs() > limits.funding_cost_limit
        {
            flags.block_risk_opens = true;
        }
        Self {
            net_notional,
            total_notional,
            margin_ratio,
            funding_rate,
            expected_funding_cost,
            flags,
        }
    }

    pub fn allow_open_long(&self) -> bool {
        !self.flags.block_risk_opens && !self.flags.block_open_long
    }

    pub fn allow_open_short(&self) -> bool {
        !self.flags.block_risk_opens && !self.flags.block_open_short
    }
}

impl GridEngine {
    pub fn new(config: HedgedGridCoreConfig, hedge_mode_enabled: bool) -> Result<Self, String> {
        config.validate()?;
        if config.require_hedge_mode && !hedge_mode_enabled {
            return Err("account hedge mode is required".to_string());
        }
        let precision = config.precision.resolve();
        let risk = RiskState::evaluate(&config.risk, &PositionState::default(), 0.0);
        Ok(Self {
            config,
            precision,
            state: EngineState {
                buy_book: GridSideBook::new(OrderSide::Buy),
                sell_book: GridSideBook::new(OrderSide::Sell),
                ledger: OrderLedger::new(),
                position: PositionState::default(),
                risk,
                last_follow_action: None,
                follow_actions: VecDeque::new(),
                funding_rate: 0.0,
                next_id: 1,
                kill_switch: false,
            },
        })
    }

    pub fn config(&self) -> &HedgedGridCoreConfig {
        &self.config
    }

    pub fn position(&self) -> PositionState {
        self.state.position.clone()
    }

    pub fn buy_orders(&self) -> Vec<OrderSlot> {
        self.state.buy_book.slots.clone()
    }

    pub fn sell_orders(&self) -> Vec<OrderSlot> {
        self.state.sell_book.slots.clone()
    }

    pub fn configured_order_qty(&self, reference_price: f64) -> Option<f64> {
        self.engine_qty_from_notional(reference_price)
    }

    pub fn low_inventory_threshold_qty(&self, reference_price: f64) -> Option<f64> {
        self.configured_order_qty(reference_price)
            .map(|qty| qty * self.config.grid.levels_per_side as f64)
    }

    pub fn order_ids(&self) -> Vec<String> {
        self.state.ledger.all_ids()
    }

    pub fn order_record(&self, order_id: &str) -> Option<OrderRecord> {
        self.state.ledger.get(order_id).cloned()
    }

    pub fn handle_order_reject(&mut self, order_id: &str) {
        let Some(record) = self.state.ledger.remove(order_id) else {
            return;
        };
        match record.intent.side() {
            OrderSide::Buy => {
                self.state.buy_book.remove(order_id);
            }
            OrderSide::Sell => {
                self.state.sell_book.remove(order_id);
            }
        }
    }

    pub fn update_position(&mut self, position: PositionState) {
        self.state.position = position;
        self.refresh_risk();
    }

    pub fn update_funding_rate(&mut self, funding_rate: f64) {
        self.state.funding_rate = funding_rate;
        self.refresh_risk();
    }

    pub fn update_market(&mut self, snapshot: &MarketSnapshot) {
        self.state.position.mark_price = match self.config.risk_reference {
            RiskReference::Mark => snapshot.mark_price,
            RiskReference::Last => snapshot.last_price,
        };
        self.refresh_risk();
    }

    pub fn rebuild_grid(&mut self, snapshot: &MarketSnapshot) -> Vec<EngineAction> {
        let mut actions = self.cancel_all_orders("rebuild");
        self.update_market(snapshot);
        let reference = self.reference_price(snapshot);
        let levels = self.config.grid.levels_per_side;
        let buy_prices = self.engine_price_levels(reference, levels, OrderSide::Buy);
        let sell_prices = self.engine_price_levels(reference, levels, OrderSide::Sell);
        actions.extend(self.build_open_orders(OrderSide::Buy, &buy_prices));
        actions.extend(self.build_open_orders(OrderSide::Sell, &sell_prices));
        if !self.config.grid.strict_pairing {
            actions.extend(self.build_close_orders(OrderSide::Buy, &buy_prices));
            actions.extend(self.build_close_orders(OrderSide::Sell, &sell_prices));
        }
        actions
    }

    pub fn handle_fill(&mut self, fill: FillEvent, snapshot: &MarketSnapshot) -> Vec<EngineAction> {
        self.update_market(snapshot);
        let fill = self.normalize_fill(fill);
        let order_context = self
            .state
            .ledger
            .get(&fill.order_id)
            .map(|record| (record.price, record.filled_qty));
        let order_price = order_context.map(|context| context.0);
        let completed_fill_qty = order_context
            .map(|context| self.normalize_fill_qty(context.1 + fill.fill_qty))
            .unwrap_or(fill.fill_qty);
        self.apply_fill(&fill);

        if self.state.kill_switch || fill.partial {
            return Vec::new();
        }

        let mut roll_fill = fill;
        roll_fill.fill_qty = completed_fill_qty;
        let mut actions = match roll_fill.intent {
            OrderIntent::OpenLongBuy => {
                self.roll_after_open_long(&roll_fill, snapshot, order_price)
            }
            OrderIntent::OpenShortSell => {
                self.roll_after_open_short(&roll_fill, snapshot, order_price)
            }
            OrderIntent::CloseLongSell => self.roll_after_close_long(&roll_fill),
            OrderIntent::CloseShortBuy => self.roll_after_close_short(&roll_fill),
        };
        actions.extend(self.repair_grid_shape_after_fill(snapshot));
        actions
    }

    pub fn maybe_follow(&mut self, snapshot: &MarketSnapshot) -> Vec<EngineAction> {
        if self.state.kill_switch {
            return Vec::new();
        }
        self.update_market(snapshot);
        if !self.has_inventory_shortage_for_follow() {
            return Vec::new();
        }

        let mid = snapshot.mid();
        let spacing = self.config.grid.grid_spacing_pct;
        let spacing_abs = self.grid_spacing_abs();
        let base = 1.0 + spacing;
        let max_gap = self.config.follow.max_gap_steps;
        let now = snapshot.timestamp;

        if let Some(last) = self.state.last_follow_action {
            let elapsed_ms = now.signed_duration_since(last).num_milliseconds();
            if elapsed_ms < self.config.follow.follow_cooldown_ms as i64 {
                return Vec::new();
            }
        }
        self.prune_follow_actions(now);
        let follow_limit = self.config.follow.max_follow_actions_per_minute;
        if follow_limit > 0 && self.state.follow_actions.len() >= follow_limit {
            return Vec::new();
        }

        let mut actions = Vec::new();
        if let Some(best_buy) =
            self.best_open_price_for_side(OrderSide::Buy, OrderIntent::OpenLongBuy)
        {
            if best_buy > 0.0 && mid > 0.0 {
                let gap_up = match spacing_abs {
                    Some(abs) if abs > 0.0 => (mid - best_buy) / abs,
                    _ => (mid / best_buy).ln() / base.ln(),
                };
                if gap_up > max_gap {
                    actions.extend(self.follow_buy_side(best_buy));
                }
            }
        }
        if let Some(best_sell) =
            self.best_open_price_for_side(OrderSide::Sell, OrderIntent::OpenShortSell)
        {
            if best_sell > 0.0 && mid > 0.0 {
                let gap_down = match spacing_abs {
                    Some(abs) if abs > 0.0 => (best_sell - mid) / abs,
                    _ => (best_sell / mid).ln() / base.ln(),
                };
                if gap_down > max_gap {
                    actions.extend(self.follow_sell_side(best_sell));
                }
            }
        }

        if !actions.is_empty() {
            self.state.last_follow_action = Some(now);
            self.state.follow_actions.push_back(now);
        }
        actions
    }

    pub fn reconcile_inventory(&mut self, snapshot: &MarketSnapshot) -> Vec<EngineAction> {
        self.update_market(snapshot);
        if self.state.kill_switch {
            return Vec::new();
        }
        if self.config.grid.strict_pairing {
            let mut actions = Vec::new();
            actions.extend(self.enforce_close_limit(OrderSide::Sell));
            actions.extend(self.enforce_close_limit(OrderSide::Buy));
            actions.extend(self.enforce_open_limit(OrderSide::Sell));
            actions.extend(self.enforce_open_limit(OrderSide::Buy));
            return actions;
        }

        let can_open_long = self.allow_open_intent(OrderIntent::OpenLongBuy);
        let can_open_short = self.allow_open_intent(OrderIntent::OpenShortSell);
        if can_open_long && can_open_short {
            let buy_open_count = self
                .book_for_side(OrderSide::Buy)
                .count_by_intent(OrderIntent::OpenLongBuy);
            let sell_open_count = self
                .book_for_side(OrderSide::Sell)
                .count_by_intent(OrderIntent::OpenShortSell);
            if buy_open_count == 0 || sell_open_count == 0 {
                return self.rebuild_grid(snapshot);
            }
        }

        let mut actions = Vec::new();
        actions.extend(self.enforce_close_coverage(OrderSide::Sell));
        actions.extend(self.enforce_close_coverage(OrderSide::Buy));
        actions.extend(self.enforce_close_limit(OrderSide::Sell));
        actions.extend(self.enforce_close_limit(OrderSide::Buy));
        actions.extend(self.refill_close_slots(OrderSide::Sell, snapshot));
        actions.extend(self.refill_close_slots(OrderSide::Buy, snapshot));
        actions.extend(self.normalize_close_grid(OrderSide::Sell));
        actions.extend(self.normalize_close_grid(OrderSide::Buy));
        actions.extend(self.enforce_open_limit(OrderSide::Sell));
        actions.extend(self.enforce_open_limit(OrderSide::Buy));
        actions.extend(self.refill_open_slots(OrderSide::Sell, snapshot));
        actions.extend(self.refill_open_slots(OrderSide::Buy, snapshot));
        actions.extend(self.normalize_open_grid(OrderSide::Sell));
        actions.extend(self.normalize_open_grid(OrderSide::Buy));
        actions.extend(self.normalize_close_grid(OrderSide::Sell));
        actions.extend(self.normalize_close_grid(OrderSide::Buy));
        actions
    }

    pub fn trigger_kill_switch(&mut self, reason: &str) -> Vec<EngineAction> {
        self.state.kill_switch = true;
        self.cancel_all_orders(reason)
    }

    pub fn handle_post_only_reject(&mut self, order_id: &str) -> Vec<EngineAction> {
        let retries = self.config.execution.post_only_retries;
        let Some(record) = self.state.ledger.get(order_id).cloned() else {
            return Vec::new();
        };
        if record.retries >= retries {
            return self.cancel_order(order_id, "post_only_reject");
        }
        let new_price = match record.intent.side() {
            OrderSide::Buy => record.price - self.precision.tick_size,
            OrderSide::Sell => record.price + self.precision.tick_size,
        };
        let mut actions = self.cancel_order(order_id, "post_only_reprice");
        if let Some(action) =
            self.place_order(record.intent, new_price, record.qty, record.retries + 1)
        {
            actions.push(action);
        }
        actions
    }

    fn refresh_risk(&mut self) {
        self.state.risk = RiskState::evaluate(
            &self.config.risk,
            &self.state.position,
            self.state.funding_rate,
        );
        if self.state.risk.flags.only_close {
            self.cancel_open_orders();
        }
    }

    fn has_inventory_shortage_for_follow(&self) -> bool {
        if self.config.grid.strict_pairing {
            return false;
        }
        let desired = self.config.grid.levels_per_side;
        let close_long_count = self
            .book_for_side(OrderSide::Sell)
            .count_by_intent(OrderIntent::CloseLongSell);
        let close_short_count = self
            .book_for_side(OrderSide::Buy)
            .count_by_intent(OrderIntent::CloseShortBuy);
        close_long_count < desired || close_short_count < desired
    }

    fn reference_price(&self, snapshot: &MarketSnapshot) -> f64 {
        match self.config.price_reference {
            PriceReference::Mid => snapshot.mid(),
            PriceReference::Last => snapshot.last_price,
        }
    }

    fn engine_price_levels(&self, reference: f64, levels: usize, side: OrderSide) -> Vec<f64> {
        let mut prices = Vec::with_capacity(levels);
        let spacing_abs = self.grid_spacing_abs();
        for level in 1..=levels {
            let price = match spacing_abs {
                Some(abs) => match side {
                    OrderSide::Buy => reference - abs * level as f64,
                    OrderSide::Sell => reference + abs * level as f64,
                },
                None => {
                    let spacing = self.config.grid.grid_spacing_pct;
                    match side {
                        OrderSide::Buy => reference * (1.0 - spacing).powi(level as i32),
                        OrderSide::Sell => reference * (1.0 + spacing).powi(level as i32),
                    }
                }
            };
            if price > 0.0 {
                prices.push(price);
            }
        }
        prices
    }

    fn build_open_orders(&mut self, side: OrderSide, prices: &[f64]) -> Vec<EngineAction> {
        if !self.config.grid.fill_remaining_slots_with_opens {
            return Vec::new();
        }
        let intent = Self::open_intent_for_side(side);
        if !self.allow_open_intent(intent) {
            return Vec::new();
        }
        prices
            .iter()
            .filter_map(|price| {
                let qty = self.engine_qty_from_notional(*price)?;
                self.place_order(intent, *price, qty, 0)
            })
            .collect()
    }

    fn build_close_orders(&mut self, side: OrderSide, prices: &[f64]) -> Vec<EngineAction> {
        let intent = Self::close_intent_for_side(side.clone());
        let mut remaining = self.available_for_close(side.clone());
        if remaining <= 0.0 {
            return Vec::new();
        }
        let mut actions = Vec::new();
        for price in prices {
            let price = self.precision.quantize_price_for_side(*price, &side);
            let Some(qty) = self.engine_qty_from_notional(price) else {
                continue;
            };
            if remaining >= qty {
                if let Some(action) = self.place_order(intent, price, qty, 0) {
                    actions.push(action);
                    remaining -= qty;
                }
            }
        }
        actions
    }

    fn roll_after_open_long(
        &mut self,
        fill: &FillEvent,
        _snapshot: &MarketSnapshot,
        _open_price: Option<f64>,
    ) -> Vec<EngineAction> {
        let mut actions = Vec::new();
        let levels = self.config.grid.levels_per_side;
        let open_price = self.step_price(fill.fill_price, OrderSide::Buy, levels);
        if let Some(action) = self.place_open_at_price(OrderSide::Buy, open_price) {
            actions.push(action);
        }
        let close_price = self.step_price(fill.fill_price, OrderSide::Sell, 1);
        if let Some(action) =
            self.place_close_from_fill(OrderIntent::CloseLongSell, close_price, fill.fill_qty)
        {
            actions.push(action);
        }
        if self
            .book_for_side(OrderSide::Sell)
            .count_by_intent(OrderIntent::CloseLongSell)
            > levels
        {
            if let Some(action) =
                self.cancel_farthest_close_order(OrderSide::Sell, "roll_open_long")
            {
                actions.push(action);
            }
        }
        actions
    }

    fn roll_after_open_short(
        &mut self,
        fill: &FillEvent,
        _snapshot: &MarketSnapshot,
        _open_price: Option<f64>,
    ) -> Vec<EngineAction> {
        let mut actions = Vec::new();
        let levels = self.config.grid.levels_per_side;
        let open_price = self.step_price(fill.fill_price, OrderSide::Sell, levels);
        if let Some(action) = self.place_open_at_price(OrderSide::Sell, open_price) {
            actions.push(action);
        }
        let close_price = self.step_price(fill.fill_price, OrderSide::Buy, 1);
        if let Some(action) =
            self.place_close_from_fill(OrderIntent::CloseShortBuy, close_price, fill.fill_qty)
        {
            actions.push(action);
        }
        if self
            .book_for_side(OrderSide::Buy)
            .count_by_intent(OrderIntent::CloseShortBuy)
            > levels
        {
            if let Some(action) =
                self.cancel_farthest_close_order(OrderSide::Buy, "roll_open_short")
            {
                actions.push(action);
            }
        }
        actions
    }

    fn roll_after_close_long(&mut self, fill: &FillEvent) -> Vec<EngineAction> {
        let mut actions = Vec::new();
        let levels = self.config.grid.levels_per_side;
        if !self.config.grid.strict_pairing {
            let close_price = self.step_price(fill.fill_price, OrderSide::Sell, levels);
            if let Some(action) = self.place_close_at_price(OrderSide::Sell, close_price) {
                actions.push(action);
            }
        }
        let open_price = self.step_price(fill.fill_price, OrderSide::Buy, 1);
        if let Some(action) = self.place_open_at_price(OrderSide::Buy, open_price) {
            actions.push(action);
        }
        if self
            .book_for_side(OrderSide::Buy)
            .count_by_intent(OrderIntent::OpenLongBuy)
            > levels
        {
            if let Some(action) = self.cancel_farthest_open_order(OrderSide::Buy, "roll_close_long")
            {
                actions.push(action);
            }
        }
        actions
    }

    fn roll_after_close_short(&mut self, fill: &FillEvent) -> Vec<EngineAction> {
        let mut actions = Vec::new();
        let levels = self.config.grid.levels_per_side;
        if !self.config.grid.strict_pairing {
            let close_price = self.step_price(fill.fill_price, OrderSide::Buy, levels);
            if let Some(action) = self.place_close_at_price(OrderSide::Buy, close_price) {
                actions.push(action);
            }
        }
        let open_price = self.step_price(fill.fill_price, OrderSide::Sell, 1);
        if let Some(action) = self.place_open_at_price(OrderSide::Sell, open_price) {
            actions.push(action);
        }
        if self
            .book_for_side(OrderSide::Sell)
            .count_by_intent(OrderIntent::OpenShortSell)
            > levels
        {
            if let Some(action) =
                self.cancel_farthest_open_order(OrderSide::Sell, "roll_close_short")
            {
                actions.push(action);
            }
        }
        actions
    }

    fn open_intent_for_side(side: OrderSide) -> OrderIntent {
        match side {
            OrderSide::Buy => OrderIntent::OpenLongBuy,
            OrderSide::Sell => OrderIntent::OpenShortSell,
        }
    }

    fn close_intent_for_side(side: OrderSide) -> OrderIntent {
        match side {
            OrderSide::Buy => OrderIntent::CloseShortBuy,
            OrderSide::Sell => OrderIntent::CloseLongSell,
        }
    }

    fn grid_spacing_abs(&self) -> Option<f64> {
        self.config
            .grid
            .grid_spacing_abs
            .filter(|value| *value > 0.0)
    }

    fn step_price(&self, base: f64, side: OrderSide, steps: usize) -> f64 {
        if let Some(abs) = self.grid_spacing_abs() {
            let offset = abs * steps as f64;
            match side {
                OrderSide::Buy => base - offset,
                OrderSide::Sell => base + offset,
            }
        } else {
            let offset = self.config.grid.grid_spacing_pct * steps as f64;
            match side {
                OrderSide::Buy => base * (1.0 - offset),
                OrderSide::Sell => base * (1.0 + offset),
            }
        }
    }

    fn available_for_close(&self, side: OrderSide) -> f64 {
        match side {
            OrderSide::Buy => self.state.position.short_available,
            OrderSide::Sell => self.state.position.long_available,
        }
    }

    fn book_for_side(&self, side: OrderSide) -> &GridSideBook {
        match side {
            OrderSide::Buy => &self.state.buy_book,
            OrderSide::Sell => &self.state.sell_book,
        }
    }

    fn farthest_order_id_for_intent(&self, side: OrderSide, intent: OrderIntent) -> Option<String> {
        self.book_for_side(side)
            .slots
            .iter()
            .rev()
            .find(|slot| slot.intent == intent)
            .map(|slot| slot.id.clone())
    }

    fn cancel_farthest_open_order(
        &mut self,
        side: OrderSide,
        reason: &str,
    ) -> Option<EngineAction> {
        let intent = Self::open_intent_for_side(side.clone());
        let order_id = self.farthest_order_id_for_intent(side, intent)?;
        self.cancel_order(&order_id, reason).pop()
    }

    fn cancel_farthest_close_order(
        &mut self,
        side: OrderSide,
        reason: &str,
    ) -> Option<EngineAction> {
        let intent = Self::close_intent_for_side(side.clone());
        let order_id = self.farthest_order_id_for_intent(side, intent)?;
        self.cancel_order(&order_id, reason).pop()
    }

    fn best_open_price_for_side(&self, side: OrderSide, intent: OrderIntent) -> Option<f64> {
        self.book_for_side(side)
            .slots
            .iter()
            .filter(|slot| slot.intent == intent)
            .map(|slot| slot.price)
            .reduce(|current, price| match intent.side() {
                OrderSide::Buy if price > current => price,
                OrderSide::Sell if price < current => price,
                _ => current,
            })
    }

    fn follow_buy_side(&mut self, best_buy: f64) -> Vec<EngineAction> {
        if !self.config.grid.fill_remaining_slots_with_opens
            || !self.config.grid.follow_open_enabled
        {
            return Vec::new();
        }
        let new_price = match self.grid_spacing_abs() {
            Some(abs) => best_buy + abs,
            None => best_buy * (1.0 + self.config.grid.grid_spacing_pct),
        };
        let mut actions = Vec::new();
        if let Some(action) = self.place_open_at_price(OrderSide::Buy, new_price) {
            actions.push(action);
            if let Some(cancel) = self.cancel_farthest_open_order(OrderSide::Buy, "follow_buy") {
                actions.push(cancel);
            }
        }
        actions
    }

    fn follow_sell_side(&mut self, best_sell: f64) -> Vec<EngineAction> {
        if !self.config.grid.fill_remaining_slots_with_opens
            || !self.config.grid.follow_open_enabled
        {
            return Vec::new();
        }
        let new_price = match self.grid_spacing_abs() {
            Some(abs) => best_sell - abs,
            None => best_sell * (1.0 - self.config.grid.grid_spacing_pct),
        };
        let mut actions = Vec::new();
        if let Some(action) = self.place_open_at_price(OrderSide::Sell, new_price) {
            actions.push(action);
            if let Some(cancel) = self.cancel_farthest_open_order(OrderSide::Sell, "follow_sell") {
                actions.push(cancel);
            }
        }
        actions
    }

    fn repair_grid_shape_after_fill(&mut self, snapshot: &MarketSnapshot) -> Vec<EngineAction> {
        if self.config.grid.strict_pairing || self.state.kill_switch {
            return Vec::new();
        }
        let mut actions = Vec::new();
        if self.config.grid.repair_near_gap_enabled {
            actions.extend(self.repair_near_gap(snapshot));
        }
        actions.extend(self.trim_far_orders_after_fill(OrderSide::Buy));
        actions.extend(self.trim_far_orders_after_fill(OrderSide::Sell));
        actions
    }

    fn repair_near_gap(&mut self, snapshot: &MarketSnapshot) -> Vec<EngineAction> {
        if !self.config.grid.fill_remaining_slots_with_opens {
            return Vec::new();
        }
        let Some(spacing_abs) = self.grid_spacing_abs() else {
            return Vec::new();
        };
        let highest_buy = self.best_open_price(OrderSide::Buy);
        let lowest_sell = self.best_open_price(OrderSide::Sell);
        let (Some(highest_buy), Some(lowest_sell)) = (highest_buy, lowest_sell) else {
            return Vec::new();
        };
        let expected_gap = spacing_abs * 2.0;
        let tolerance = self.precision.tick_size.max(1e-8) * 0.5;
        let actual_gap = lowest_sell - highest_buy;
        if actual_gap <= expected_gap + tolerance {
            return Vec::new();
        }
        let missing_steps = ((actual_gap - expected_gap) / spacing_abs).floor() as usize;
        if missing_steps == 0 {
            return Vec::new();
        }

        let reference = self.reference_price(snapshot);
        let mut actions = Vec::new();
        for step in 1..=missing_steps {
            let buy_price = lowest_sell - spacing_abs * (step as f64 + 1.0);
            if buy_price > highest_buy + tolerance
                && buy_price < lowest_sell - tolerance
                && (reference <= 0.0 || buy_price < reference - tolerance)
            {
                if let Some(action) = self.place_open_at_price(OrderSide::Buy, buy_price) {
                    actions.push(action);
                }
            }

            let sell_price = highest_buy + spacing_abs * (step as f64 + 1.0);
            if sell_price > highest_buy + tolerance
                && sell_price < lowest_sell - tolerance
                && (reference <= 0.0 || sell_price > reference + tolerance)
            {
                if let Some(action) = self.place_open_at_price(OrderSide::Sell, sell_price) {
                    actions.push(action);
                }
            }
        }
        actions
    }

    fn trim_far_orders_after_fill(&mut self, side: OrderSide) -> Vec<EngineAction> {
        if self.config.grid.strict_pairing {
            return Vec::new();
        }
        let mut actions = Vec::new();
        actions.extend(
            self.trim_far_orders_for_intent(side.clone(), Self::open_intent_for_side(side.clone())),
        );
        actions.extend(
            self.trim_far_orders_for_intent(side.clone(), Self::close_intent_for_side(side)),
        );
        actions
    }

    fn trim_far_orders_for_intent(
        &mut self,
        side: OrderSide,
        intent: OrderIntent,
    ) -> Vec<EngineAction> {
        let desired = self.config.grid.levels_per_side;
        if desired == 0 {
            return Vec::new();
        }
        let slots = self
            .book_for_side(side.clone())
            .slots
            .iter()
            .filter(|slot| slot.intent == intent)
            .cloned()
            .collect::<Vec<_>>();
        if slots.len() <= desired {
            return Vec::new();
        }

        let target_keys = self.near_target_keys_for_side(side, desired);
        let mut kept_per_key = HashSet::new();
        let mut cancel_ids = Vec::new();
        for slot in slots.iter() {
            let key = self.price_key(slot.price);
            if !target_keys.contains(&key) || !kept_per_key.insert(key) {
                cancel_ids.push(slot.id.clone());
            }
        }

        let mut remaining_count = slots.len().saturating_sub(cancel_ids.len());
        if remaining_count > desired {
            for slot in slots.iter().rev() {
                if remaining_count <= desired {
                    break;
                }
                if cancel_ids.iter().any(|id| id == &slot.id) {
                    continue;
                }
                cancel_ids.push(slot.id.clone());
                remaining_count -= 1;
            }
        }

        cancel_ids
            .into_iter()
            .flat_map(|order_id| self.cancel_order(&order_id, "trim_far_orders_after_fill"))
            .collect()
    }

    fn near_target_keys_for_side(&self, side: OrderSide, desired: usize) -> HashSet<i64> {
        let mut grouped = BTreeMap::new();
        for slot in self.book_for_side(side.clone()).slots.iter() {
            grouped
                .entry(self.price_key(slot.price))
                .or_insert(slot.price);
        }
        let prices = match side {
            OrderSide::Buy => grouped
                .values()
                .rev()
                .copied()
                .take(desired)
                .collect::<Vec<_>>(),
            OrderSide::Sell => grouped.values().copied().take(desired).collect::<Vec<_>>(),
        };
        prices
            .into_iter()
            .map(|price| self.price_key(price))
            .collect()
    }

    fn best_open_price(&self, side: OrderSide) -> Option<f64> {
        let intent = Self::open_intent_for_side(side.clone());
        self.book_for_side(side.clone())
            .slots
            .iter()
            .filter(|slot| slot.intent == intent)
            .map(|slot| slot.price)
            .reduce(|current, price| match side {
                OrderSide::Buy if price > current => price,
                OrderSide::Sell if price < current => price,
                _ => current,
            })
    }

    fn enforce_close_coverage(&mut self, side: OrderSide) -> Vec<EngineAction> {
        if self.config.grid.strict_pairing {
            return Vec::new();
        }
        let covered_ids = self.covered_close_order_ids(side.clone());
        let slots = self.book_for_side(side).slots.clone();
        let mut actions = Vec::new();
        for slot in slots.iter().rev() {
            if slot.intent.is_close() && !covered_ids.contains(&slot.id) {
                actions.extend(self.cancel_order(&slot.id, "inventory_shortage"));
            }
        }
        actions
    }

    fn enforce_close_limit(&mut self, side: OrderSide) -> Vec<EngineAction> {
        let intent = Self::close_intent_for_side(side.clone());
        let desired = self.config.grid.levels_per_side;
        let mut actions = Vec::new();
        let mut count = self.book_for_side(side.clone()).count_by_intent(intent);
        while count > desired {
            if let Some(order_id) = self.farthest_order_id_for_intent(side.clone(), intent) {
                actions.extend(self.cancel_order(&order_id, "close_limit"));
                count -= 1;
            } else {
                break;
            }
        }
        actions
    }

    fn enforce_open_limit(&mut self, side: OrderSide) -> Vec<EngineAction> {
        let intent = Self::open_intent_for_side(side.clone());
        let desired = self.config.grid.levels_per_side;
        let mut actions = Vec::new();
        let mut count = self.book_for_side(side.clone()).count_by_intent(intent);
        while count > desired {
            if let Some(order_id) = self.farthest_order_id_for_intent(side.clone(), intent) {
                actions.extend(self.cancel_order(&order_id, "open_limit"));
                count -= 1;
            } else {
                break;
            }
        }
        actions
    }

    fn refill_open_slots(
        &mut self,
        side: OrderSide,
        snapshot: &MarketSnapshot,
    ) -> Vec<EngineAction> {
        if !self.config.grid.fill_remaining_slots_with_opens
            || !self.config.grid.refill_open_slots_enabled
        {
            return Vec::new();
        }
        let desired = self.config.grid.levels_per_side;
        let intent = Self::open_intent_for_side(side.clone());
        if !self.allow_open_intent(intent) {
            return Vec::new();
        }
        let current_len = self.book_for_side(side.clone()).count_by_intent(intent);
        if current_len >= desired {
            return Vec::new();
        }

        let spacing = self.config.grid.grid_spacing_pct;
        let spacing_abs = self.grid_spacing_abs();
        let reference = self.reference_price(snapshot);
        let mut price = self
            .best_open_price_for_side(side.clone(), intent)
            .unwrap_or_else(|| match spacing_abs {
                Some(abs) => match side {
                    OrderSide::Buy => reference - abs * desired as f64,
                    OrderSide::Sell => reference + abs * desired as f64,
                },
                None => match side {
                    OrderSide::Buy => reference * (1.0 - spacing).powi(desired as i32),
                    OrderSide::Sell => reference * (1.0 + spacing).powi(desired as i32),
                },
            });

        let mut actions = Vec::new();
        let mut total_count = current_len;
        while total_count < desired {
            price = match spacing_abs {
                Some(abs) => match side {
                    OrderSide::Buy => price - abs,
                    OrderSide::Sell => price + abs,
                },
                None => match side {
                    OrderSide::Buy => price * (1.0 - spacing),
                    OrderSide::Sell => price * (1.0 + spacing),
                },
            };
            if let Some(action) = self.place_open_at_price(side.clone(), price) {
                actions.push(action);
                total_count += 1;
            } else {
                break;
            }
        }
        actions
    }

    fn normalize_open_grid(&mut self, side: OrderSide) -> Vec<EngineAction> {
        if !self.config.grid.fill_remaining_slots_with_opens {
            return Vec::new();
        }
        let levels = self.config.grid.levels_per_side;
        if levels == 0 {
            return Vec::new();
        }
        let intent = Self::open_intent_for_side(side.clone());
        if !self.allow_open_intent(intent) {
            return Vec::new();
        }

        let open_slots = self
            .book_for_side(side.clone())
            .slots
            .iter()
            .filter(|slot| slot.intent == intent)
            .cloned()
            .collect::<Vec<_>>();
        if open_slots.is_empty() {
            return Vec::new();
        }
        let anchor = open_slots
            .iter()
            .map(|slot| slot.price)
            .reduce(|current, price| match side {
                OrderSide::Buy if price > current => price,
                OrderSide::Sell if price < current => price,
                _ => current,
            })
            .expect("open slots not empty");

        let mut target_prices = Vec::with_capacity(levels);
        let mut target_keys = std::collections::HashSet::with_capacity(levels);
        for step in 0..levels {
            let price = self.step_price(anchor, side.clone(), step);
            let quantized = self.precision.quantize_price_for_side(price, &side);
            let key = self.price_key(quantized);
            if target_keys.insert(key) {
                target_prices.push(quantized);
            }
        }

        let mut actions = Vec::new();
        let mut kept_keys = std::collections::HashSet::new();
        for slot in open_slots {
            let key = self.price_key(slot.price);
            if !target_keys.contains(&key) || !kept_keys.insert(key) {
                actions.extend(self.cancel_order(&slot.id, "normalize_open_grid"));
            }
        }

        if !self.config.grid.normalize_open_grid_enabled {
            return actions;
        }
        for price in target_prices {
            let key = self.price_key(price);
            if kept_keys.contains(&key) {
                continue;
            }
            if let Some(action) = self.place_open_at_price(side.clone(), price) {
                actions.push(action);
                kept_keys.insert(key);
            }
        }
        actions
    }

    fn normalize_close_grid(&mut self, side: OrderSide) -> Vec<EngineAction> {
        if self.config.grid.strict_pairing {
            return Vec::new();
        }
        let levels = self.config.grid.levels_per_side;
        if levels == 0 {
            return Vec::new();
        }
        let intent = Self::close_intent_for_side(side.clone());
        let open_intent = Self::open_intent_for_side(side.clone());
        let close_slots = self
            .book_for_side(side.clone())
            .slots
            .iter()
            .filter(|slot| slot.intent == intent)
            .cloned()
            .collect::<Vec<_>>();
        let open_slots = self
            .book_for_side(side.clone())
            .slots
            .iter()
            .filter(|slot| slot.intent == open_intent)
            .cloned()
            .take(levels)
            .collect::<Vec<_>>();

        let mut target_prices = Vec::with_capacity(levels);
        let mut target_keys = std::collections::HashSet::with_capacity(levels);
        for slot in open_slots {
            let quantized = self.precision.quantize_price_for_side(slot.price, &side);
            let key = self.price_key(quantized);
            if target_keys.insert(key) {
                target_prices.push(quantized);
            }
        }
        if target_prices.len() < levels {
            let anchor = close_slots
                .iter()
                .map(|slot| slot.price)
                .chain(target_prices.iter().copied())
                .reduce(|current, price| match side {
                    OrderSide::Buy if price > current => price,
                    OrderSide::Sell if price < current => price,
                    _ => current,
                });
            let Some(anchor) = anchor else {
                return Vec::new();
            };
            for step in 0..levels {
                if target_prices.len() >= levels {
                    break;
                }
                let price = self.step_price(anchor, side.clone(), step);
                let quantized = self.precision.quantize_price_for_side(price, &side);
                let key = self.price_key(quantized);
                if target_keys.insert(key) {
                    target_prices.push(quantized);
                }
            }
        }

        let mut actions = Vec::new();
        let mut kept_keys = std::collections::HashSet::new();
        let mut remaining = self.available_for_close(side.clone());
        for slot in close_slots {
            let key = self.price_key(slot.price);
            let keep =
                target_keys.contains(&key) && !kept_keys.contains(&key) && remaining >= slot.qty;
            if keep {
                kept_keys.insert(key);
                remaining -= slot.qty;
            } else {
                actions.extend(self.cancel_order(&slot.id, "normalize_close_grid"));
            }
        }
        for price in target_prices {
            let key = self.price_key(price);
            if kept_keys.contains(&key) {
                continue;
            }
            let Some(qty) = self.engine_qty_from_notional(price) else {
                continue;
            };
            if remaining < qty {
                continue;
            }
            if let Some(action) = self.place_order(intent, price, qty, 0) {
                actions.push(action);
                kept_keys.insert(key);
                remaining -= qty;
            }
        }
        actions
    }

    fn refill_close_slots(
        &mut self,
        side: OrderSide,
        snapshot: &MarketSnapshot,
    ) -> Vec<EngineAction> {
        let desired = self.config.grid.levels_per_side;
        let intent = Self::close_intent_for_side(side.clone());
        let available = self.available_for_close(side.clone());
        if available <= 0.0 {
            return Vec::new();
        }
        let existing_slots = self
            .book_for_side(side.clone())
            .slots
            .iter()
            .filter(|slot| slot.intent == intent)
            .cloned()
            .collect::<Vec<_>>();
        if existing_slots.len() >= desired {
            return Vec::new();
        }
        let mut remaining = available - existing_slots.iter().map(|slot| slot.qty).sum::<f64>();
        if remaining <= 0.0 {
            return Vec::new();
        }
        let mut existing_keys = existing_slots
            .iter()
            .map(|slot| self.price_key(slot.price))
            .collect::<std::collections::HashSet<_>>();
        let candidates =
            self.engine_price_levels(self.reference_price(snapshot), desired, side.clone());
        let mut actions = Vec::new();
        let mut total_count = existing_slots.len();
        for candidate in candidates {
            if total_count >= desired {
                break;
            }
            let price = self.precision.quantize_price_for_side(candidate, &side);
            let key = self.price_key(price);
            if existing_keys.contains(&key) {
                continue;
            }
            let Some(qty) = self.engine_qty_from_notional(price) else {
                continue;
            };
            if remaining < qty {
                continue;
            }
            if let Some(action) = self.place_order(intent, price, qty, 0) {
                actions.push(action);
                remaining -= qty;
                total_count += 1;
                existing_keys.insert(key);
            }
        }
        actions
    }

    fn covered_close_order_ids(&self, side: OrderSide) -> std::collections::HashSet<String> {
        let (book, available) = match side {
            OrderSide::Buy => (&self.state.buy_book, self.state.position.short_available),
            OrderSide::Sell => (&self.state.sell_book, self.state.position.long_available),
        };
        let mut remaining = available;
        let mut covered = std::collections::HashSet::new();
        for slot in book.slots.iter() {
            if slot.intent.is_close() && remaining >= slot.qty {
                covered.insert(slot.id.clone());
                remaining -= slot.qty;
            }
        }
        covered
    }

    fn place_open_at_price(&mut self, side: OrderSide, price: f64) -> Option<EngineAction> {
        let intent = Self::open_intent_for_side(side);
        let qty = self.engine_qty_from_notional(price)?;
        self.place_order(intent, price, qty, 0)
    }

    fn place_close_at_price(&mut self, side: OrderSide, price: f64) -> Option<EngineAction> {
        let intent = Self::close_intent_for_side(side.clone());
        let available = self.available_for_close(side.clone());
        if available <= 0.0 {
            return None;
        }
        let booked = self.book_for_side(side).total_qty_for_intent(intent);
        let qty = self.engine_qty_from_notional(price)?;
        if available - booked < qty {
            return None;
        }
        self.place_order(intent, price, qty, 0)
    }

    fn place_close_from_fill(
        &mut self,
        intent: OrderIntent,
        price: f64,
        qty: f64,
    ) -> Option<EngineAction> {
        self.place_order(intent, price, qty, 0)
    }

    fn place_order(
        &mut self,
        intent: OrderIntent,
        price: f64,
        qty: f64,
        retries: u32,
    ) -> Option<EngineAction> {
        if intent.is_open() && !self.allow_open_intent(intent) {
            return None;
        }
        let side = intent.side();
        let price = self.adjust_close_price_for_cost(intent, price)?;
        let price = self.precision.quantize_price_for_side(price, &side);
        let qty = self.precision.quantize_qty(qty);
        if price <= 0.0 || qty <= 0.0 || qty < self.precision.min_qty {
            return None;
        }
        if self.precision.min_notional > 0.0 && price * qty < self.precision.min_notional {
            return None;
        }
        if intent.is_open() && self.would_exceed_total_notional(price, qty) {
            return None;
        }
        let price_key = self.price_key(price);
        if self
            .book_for_side(side.clone())
            .slots
            .iter()
            .any(|slot| slot.intent == intent && self.price_key(slot.price) == price_key)
        {
            return None;
        }
        let id = self.next_order_id();
        self.state.ledger.insert(OrderRecord {
            id: id.clone(),
            intent,
            price,
            qty,
            filled_qty: 0.0,
            created_at: Utc::now(),
            retries,
        });
        let slot = OrderSlot {
            id: id.clone(),
            intent,
            price,
            qty,
        };
        match side {
            OrderSide::Buy => self.state.buy_book.insert(slot),
            OrderSide::Sell => self.state.sell_book.insert(slot),
        }
        Some(EngineAction::Place(OrderDraft {
            id,
            intent,
            price,
            qty,
            post_only: self.config.execution.post_only,
        }))
    }

    fn adjust_close_price_for_cost(&self, intent: OrderIntent, raw_price: f64) -> Option<f64> {
        if raw_price <= 0.0 {
            return None;
        }
        match intent {
            OrderIntent::CloseLongSell if self.state.position.long_available <= 0.0 => None,
            OrderIntent::CloseShortBuy if self.state.position.short_available <= 0.0 => None,
            _ => Some(raw_price),
        }
    }

    fn engine_qty_from_notional(&self, price: f64) -> Option<f64> {
        if price <= 0.0 {
            return None;
        }
        if let Some(order_qty) = self.config.grid.order_qty {
            let mut qty = self.precision.quantize_qty(order_qty);
            if qty < self.precision.min_qty {
                qty = self.precision.quantize_qty_up(self.precision.min_qty);
            }
            if qty <= 0.0 {
                return None;
            }
            if self.precision.min_notional > 0.0 && price * qty < self.precision.min_notional {
                return None;
            }
            return Some(qty);
        }
        let target_notional = self
            .config
            .grid
            .order_notional
            .max(self.precision.min_notional);
        let mut qty = self.precision.quantize_qty_up(target_notional / price);
        if qty < self.precision.min_qty {
            qty = self.precision.quantize_qty_up(self.precision.min_qty);
        }
        if qty <= 0.0 {
            return None;
        }
        if self.precision.min_notional > 0.0 && price * qty < self.precision.min_notional {
            return None;
        }
        Some(qty)
    }

    fn allow_open_intent(&self, intent: OrderIntent) -> bool {
        match intent {
            OrderIntent::OpenLongBuy => self.state.risk.allow_open_long(),
            OrderIntent::OpenShortSell => self.state.risk.allow_open_short(),
            _ => true,
        }
    }

    fn would_exceed_total_notional(&self, price: f64, qty: f64) -> bool {
        let max_total = self.config.risk.max_total_notional;
        if max_total <= 0.0 || price <= 0.0 || qty <= 0.0 {
            return false;
        }
        self.current_and_pending_open_notional() + price * qty > max_total
    }

    fn current_and_pending_open_notional(&self) -> f64 {
        let mark_price = self.state.position.mark_price.max(0.0);
        let position_notional =
            (self.state.position.long_qty.abs() + self.state.position.short_qty.abs()) * mark_price;
        let pending_open_notional = self
            .state
            .ledger
            .orders
            .values()
            .filter(|record| record.intent.is_open())
            .map(|record| record.price.abs().max(mark_price) * record.qty.abs())
            .sum::<f64>();
        position_notional + pending_open_notional
    }

    fn cancel_open_orders(&mut self) {
        let ids = self
            .state
            .ledger
            .orders
            .values()
            .filter(|record| record.intent.is_open())
            .map(|record| record.id.clone())
            .collect::<Vec<_>>();
        for id in ids {
            let _ = self.cancel_order(&id, "risk_only_close");
        }
    }

    fn cancel_all_orders(&mut self, reason: &str) -> Vec<EngineAction> {
        self.state
            .ledger
            .all_ids()
            .into_iter()
            .flat_map(|id| self.cancel_order(&id, reason))
            .collect()
    }

    fn cancel_order(&mut self, order_id: &str, reason: &str) -> Vec<EngineAction> {
        let Some(record) = self.state.ledger.remove(order_id) else {
            return Vec::new();
        };
        match record.intent.side() {
            OrderSide::Buy => {
                self.state.buy_book.remove(order_id);
            }
            OrderSide::Sell => {
                self.state.sell_book.remove(order_id);
            }
        }
        vec![EngineAction::Cancel {
            order_id: order_id.to_string(),
            reason: reason.to_string(),
        }]
    }

    fn price_key(&self, price: f64) -> i64 {
        let factor = 10f64.powi(self.precision.price_digits as i32);
        (price * factor).round() as i64
    }

    fn next_order_id(&mut self) -> String {
        let id = format!("grid-{}", self.state.next_id);
        self.state.next_id += 1;
        id
    }

    fn normalize_fill(&self, mut fill: FillEvent) -> FillEvent {
        fill.fill_qty = self.normalize_fill_qty(fill.fill_qty);
        fill
    }

    fn normalize_fill_qty(&self, qty: f64) -> f64 {
        if qty <= 0.0 {
            return 0.0;
        }
        if self.precision.step_size <= 0.0 {
            return qty;
        }
        let snapped = self.precision.quantize_qty_nearest(qty);
        let tolerance = (self.precision.step_size * 1e-6).max(1e-9);
        if (snapped - qty).abs() <= tolerance {
            snapped
        } else {
            self.precision.quantize_qty(qty)
        }
    }

    fn snap_position_qty(&self, qty: f64) -> f64 {
        if qty <= 0.0 {
            return 0.0;
        }
        let snapped = self.precision.quantize_qty_nearest(qty);
        if snapped < self.precision.min_qty * 0.5 {
            0.0
        } else {
            snapped
        }
    }

    fn apply_fill(&mut self, fill: &FillEvent) {
        let fill_qty = self.normalize_fill_qty(fill.fill_qty);
        let mut remove_order = false;
        let mut intent_side = None;
        if let Some(record) = self.state.ledger.get_mut(&fill.order_id) {
            record.filled_qty += fill_qty;
            let remaining = (record.qty - fill_qty).max(0.0);
            if fill.partial && remaining > 0.0 {
                record.qty = remaining;
                match record.intent.side() {
                    OrderSide::Buy => self.state.buy_book.update_qty(&fill.order_id, remaining),
                    OrderSide::Sell => self.state.sell_book.update_qty(&fill.order_id, remaining),
                }
            } else {
                remove_order = true;
                intent_side = Some(record.intent.side());
            }
        }
        if remove_order {
            let _ = self.state.ledger.remove(&fill.order_id);
            if let Some(side) = intent_side {
                match side {
                    OrderSide::Buy => {
                        self.state.buy_book.remove(&fill.order_id);
                    }
                    OrderSide::Sell => {
                        self.state.sell_book.remove(&fill.order_id);
                    }
                }
            }
        }
        match fill.intent {
            OrderIntent::OpenLongBuy => {
                self.state.position.long_qty =
                    self.snap_position_qty(self.state.position.long_qty + fill_qty);
                self.state.position.long_available =
                    self.snap_position_qty(self.state.position.long_available + fill_qty);
            }
            OrderIntent::CloseLongSell => {
                self.state.position.long_qty =
                    self.snap_position_qty((self.state.position.long_qty - fill_qty).max(0.0));
                self.state.position.long_available = self
                    .snap_position_qty((self.state.position.long_available - fill_qty).max(0.0));
            }
            OrderIntent::OpenShortSell => {
                self.state.position.short_qty =
                    self.snap_position_qty(self.state.position.short_qty + fill_qty);
                self.state.position.short_available =
                    self.snap_position_qty(self.state.position.short_available + fill_qty);
            }
            OrderIntent::CloseShortBuy => {
                self.state.position.short_qty =
                    self.snap_position_qty((self.state.position.short_qty - fill_qty).max(0.0));
                self.state.position.short_available = self
                    .snap_position_qty((self.state.position.short_available - fill_qty).max(0.0));
            }
        }
        self.refresh_risk();
    }

    fn prune_follow_actions(&mut self, now: DateTime<Utc>) {
        while let Some(front) = self.state.follow_actions.front() {
            if now.signed_duration_since(*front).num_seconds() > 60 {
                self.state.follow_actions.pop_front();
            } else {
                break;
            }
        }
    }
}

pub fn build_initial_grid_plan(
    config: &HedgedGridCoreConfig,
    snapshot: &MarketSnapshot,
    position: &PositionState,
    funding_rate: f64,
) -> Result<GridPlan, String> {
    config.validate()?;
    let mut position = position.clone();
    position.mark_price = match config.risk_reference {
        RiskReference::Mark => snapshot.mark_price,
        RiskReference::Last => snapshot.last_price,
    };
    let risk = RiskState::evaluate(&config.risk, &position, funding_rate);
    let reference_price = match config.price_reference {
        PriceReference::Mid => snapshot.mid(),
        PriceReference::Last => snapshot.last_price,
    };
    let precision = config.precision.resolve();
    let mut next_id = 1_u64;
    let mut orders = Vec::new();

    if config.grid.fill_remaining_slots_with_opens && risk.allow_open_long() {
        orders.extend(build_open_orders(
            config,
            &precision,
            reference_price,
            OrderSide::Buy,
            &mut next_id,
        ));
    }
    if config.grid.fill_remaining_slots_with_opens && risk.allow_open_short() {
        orders.extend(build_open_orders(
            config,
            &precision,
            reference_price,
            OrderSide::Sell,
            &mut next_id,
        ));
    }

    Ok(GridPlan {
        symbol: config.symbol.clone(),
        reference_price,
        risk,
        orders,
    })
}

fn build_open_orders(
    config: &HedgedGridCoreConfig,
    precision: &ResolvedPrecision,
    reference_price: f64,
    side: OrderSide,
    next_id: &mut u64,
) -> Vec<OrderDraft> {
    let intent = match side {
        OrderSide::Buy => OrderIntent::OpenLongBuy,
        OrderSide::Sell => OrderIntent::OpenShortSell,
    };
    price_levels(config, reference_price, &side)
        .into_iter()
        .filter_map(|price| {
            let price = precision.quantize_price_for_side(price, &side);
            let qty = qty_from_notional(config, precision, price)?;
            let id = format!("grid-{}", *next_id);
            *next_id += 1;
            Some(OrderDraft {
                id,
                intent,
                price,
                qty,
                post_only: config.execution.post_only,
            })
        })
        .collect()
}

pub fn price_levels(
    config: &HedgedGridCoreConfig,
    reference_price: f64,
    side: &OrderSide,
) -> Vec<f64> {
    let mut prices = Vec::with_capacity(config.grid.levels_per_side);
    for level in 1..=config.grid.levels_per_side {
        let price = match config.grid.grid_spacing_abs.filter(|value| *value > 0.0) {
            Some(abs) => match side {
                OrderSide::Buy => reference_price - abs * level as f64,
                OrderSide::Sell => reference_price + abs * level as f64,
            },
            None => match side {
                OrderSide::Buy => {
                    reference_price * (1.0 - config.grid.grid_spacing_pct).powi(level as i32)
                }
                OrderSide::Sell => {
                    reference_price * (1.0 + config.grid.grid_spacing_pct).powi(level as i32)
                }
            },
        };
        if price > 0.0 {
            prices.push(price);
        }
    }
    prices
}

fn qty_from_notional(
    config: &HedgedGridCoreConfig,
    precision: &ResolvedPrecision,
    price: f64,
) -> Option<f64> {
    if price <= 0.0 {
        return None;
    }
    if let Some(order_qty) = config.grid.order_qty {
        let mut qty = precision.quantize_qty_up(order_qty);
        if qty < precision.min_qty {
            qty = precision.quantize_qty_up(precision.min_qty);
        }
        if qty <= 0.0 {
            return None;
        }
        if precision.min_notional > 0.0 && price * qty < precision.min_notional {
            return None;
        }
        return Some(qty);
    }

    let target_notional = config.grid.order_notional.max(precision.min_notional);
    let mut qty = precision.quantize_qty_up(target_notional / price);
    if qty < precision.min_qty {
        qty = precision.quantize_qty_up(precision.min_qty);
    }
    if qty <= 0.0 {
        return None;
    }
    if precision.min_notional > 0.0 && price * qty < precision.min_notional {
        return None;
    }
    Some(qty)
}

fn default_require_hedge_mode() -> bool {
    true
}

fn precision_from_step(step: f64) -> u32 {
    if step == 0.0 {
        return 8;
    }
    let formatted = format!("{step:.10}");
    let parts = formatted.split('.').collect::<Vec<_>>();
    if parts.len() > 1 {
        parts[1].trim_end_matches('0').len() as u32
    } else {
        0
    }
}

fn apply_precision(value: f64, step: f64, digits: u32) -> f64 {
    let eps = 1e-9;
    let adjusted = if step > 0.0 {
        ((value / step) + eps).floor() * step
    } else {
        value
    };
    let factor = 10f64.powi(digits as i32);
    ((adjusted * factor) + eps).floor() / factor
}
