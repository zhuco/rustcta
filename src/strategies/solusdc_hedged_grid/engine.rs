use chrono::{DateTime, Duration as ChronoDuration, Utc};
use std::collections::{HashSet, VecDeque};

use crate::core::types::OrderSide;

use super::config::{ResolvedPrecision, StrategyConfig};
use super::ledger::{GridSideBook, OrderIntent, OrderLedger, OrderRecord, OrderSlot};
use super::risk::RiskState;

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone, Default)]
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

#[derive(Debug, Clone)]
pub struct FillEvent {
    pub order_id: String,
    pub intent: OrderIntent,
    pub fill_qty: f64,
    pub fill_price: f64,
    pub timestamp: DateTime<Utc>,
    pub partial: bool,
}

#[derive(Debug, Clone)]
pub enum EngineAction {
    Place(OrderDraft),
    Cancel { order_id: String, reason: String },
}

#[derive(Debug, Clone)]
pub struct OrderDraft {
    pub id: String,
    pub intent: OrderIntent,
    pub price: f64,
    pub qty: f64,
    pub post_only: bool,
}

struct EngineState {
    buy_book: GridSideBook,
    sell_book: GridSideBook,
    ledger: OrderLedger,
    position: PositionState,
    risk: RiskState,
    last_mid: Option<f64>,
    last_fill_action: Option<DateTime<Utc>>,
    last_follow_action: Option<DateTime<Utc>>,
    follow_actions: VecDeque<DateTime<Utc>>,
    funding_rate: f64,
    next_id: u64,
    kill_switch: bool,
}

pub struct GridEngine {
    config: StrategyConfig,
    precision: ResolvedPrecision,
    state: EngineState,
}

impl GridEngine {
    pub fn new(config: StrategyConfig, hedge_mode_enabled: bool) -> Result<Self, String> {
        config.validate()?;
        if config.require_hedge_mode && !hedge_mode_enabled {
            return Err("账户未开启 Hedge Mode".to_string());
        }
        let precision = config.precision.resolve();
        let risk = RiskState::evaluate(&config.risk, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
        Ok(Self {
            config,
            precision,
            state: EngineState {
                buy_book: GridSideBook::new(OrderSide::Buy),
                sell_book: GridSideBook::new(OrderSide::Sell),
                ledger: OrderLedger::new(),
                position: PositionState::default(),
                risk,
                last_mid: None,
                last_fill_action: None,
                last_follow_action: None,
                follow_actions: VecDeque::new(),
                funding_rate: 0.0,
                next_id: 1,
                kill_switch: false,
            },
        })
    }

    pub fn config(&self) -> &StrategyConfig {
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

    pub fn order_ids(&self) -> Vec<String> {
        self.state.ledger.all_ids()
    }

    pub fn order_record(&self, order_id: &str) -> Option<OrderRecord> {
        self.state.ledger.get(order_id).cloned()
    }

    pub fn handle_order_reject(&mut self, order_id: &str) {
        let record = match self.state.ledger.remove(order_id) {
            Some(record) => record,
            None => return,
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
        self.state.last_mid = Some(snapshot.mid());
        let mark_price = match self.config.risk_reference {
            super::config::RiskReference::Mark => snapshot.mark_price,
            super::config::RiskReference::Last => snapshot.last_price,
        };
        self.state.position.mark_price = mark_price;
        self.refresh_risk();
    }

    pub fn rebuild_grid(&mut self, snapshot: &MarketSnapshot) -> Vec<EngineAction> {
        let mut actions = self.cancel_all_orders("rebuild");
        self.update_market(snapshot);
        let reference = self.reference_price(snapshot);
        let levels = self.config.grid.levels_per_side;
        let buy_prices = self.price_levels(reference, levels, OrderSide::Buy);
        let sell_prices = self.price_levels(reference, levels, OrderSide::Sell);
        actions.extend(self.build_open_orders(OrderSide::Buy, &buy_prices));
        actions.extend(self.build_open_orders(OrderSide::Sell, &sell_prices));
        if !self.config.grid.strict_pairing {
            let close_buy_prices = self.price_levels(reference, levels, OrderSide::Buy);
            let close_sell_prices = self.price_levels(reference, levels, OrderSide::Sell);
            actions.extend(self.build_close_orders(OrderSide::Buy, &close_buy_prices));
            actions.extend(self.build_close_orders(OrderSide::Sell, &close_sell_prices));
        }
        actions
    }

    pub fn handle_fill(&mut self, fill: FillEvent, snapshot: &MarketSnapshot) -> Vec<EngineAction> {
        self.update_market(snapshot);
        let order_price = self
            .state
            .ledger
            .get(&fill.order_id)
            .map(|record| record.price);
        self.apply_fill(&fill);

        if self.state.kill_switch {
            return Vec::new();
        }

        self.state.last_fill_action = Some(fill.timestamp);

        let actions = match fill.intent {
            OrderIntent::OpenLongBuy => self.roll_after_open_long(&fill, snapshot, order_price),
            OrderIntent::OpenShortSell => self.roll_after_open_short(&fill, snapshot, order_price),
            OrderIntent::CloseLongSell => self.roll_after_close_long(&fill, snapshot),
            OrderIntent::CloseShortBuy => self.roll_after_close_short(&fill, snapshot),
        };

        // 成交事件路径保持最小动作集：
        // 每笔成交仅执行对应滚动（补2撤1），其余归一化交给周期 reconcile。
        actions
    }

    pub fn maybe_follow(&mut self, snapshot: &MarketSnapshot) -> Vec<EngineAction> {
        if self.state.kill_switch {
            return Vec::new();
        }
        // 仅在“库存不足导致平仓挂单无法补齐”时触发追价，
        // 避免库存充足时频繁在现价附近追单。
        self.update_market(snapshot);
        if !self.has_inventory_shortage_for_follow() {
            return Vec::new();
        }
        let mid = snapshot.mid();
        let spacing = self.config.grid.grid_spacing_pct;
        let spacing_abs = self.grid_spacing_abs();
        let base = 1.0 + spacing;
        let max_gap = self.config.follow.max_gap_steps;
        let cooldown = self.config.follow.follow_cooldown_ms;
        let now = snapshot.timestamp;

        if let Some(last) = self.state.last_follow_action {
            if now - last < ChronoDuration::milliseconds(cooldown as i64) {
                return Vec::new();
            }
        }

        self.prune_follow_actions(now);
        // 约定: 0 表示不限制，避免配置为 0 时意外完全禁用 follow。
        let follow_limit = self.config.follow.max_follow_actions_per_minute;
        if follow_limit > 0 && self.state.follow_actions.len() >= follow_limit {
            return Vec::new();
        }

        let mut actions = Vec::new();
        let buy_intent = Self::open_intent_for_side(OrderSide::Buy);
        if let Some(best_buy) = self.nearest_price_for_intent(OrderSide::Buy, buy_intent) {
            if best_buy > 0.0 && mid > 0.0 {
                let gap_up = match spacing_abs {
                    Some(abs) if abs > 0.0 => (mid - best_buy) / abs,
                    _ => (mid / best_buy).ln() / base.ln(),
                };
                if gap_up > max_gap {
                    actions.extend(self.follow_buy_side(best_buy, mid));
                }
            }
        }
        let sell_intent = Self::open_intent_for_side(OrderSide::Sell);
        if let Some(best_sell) = self.nearest_price_for_intent(OrderSide::Sell, sell_intent) {
            if best_sell > 0.0 && mid > 0.0 {
                let gap_dn = match spacing_abs {
                    Some(abs) if abs > 0.0 => (best_sell - mid) / abs,
                    _ => (best_sell / mid).ln() / base.ln(),
                };
                if gap_dn > max_gap {
                    actions.extend(self.follow_sell_side(best_sell, mid));
                }
            }
        }

        if !actions.is_empty() {
            self.state.last_follow_action = Some(now);
            self.state.follow_actions.push_back(now);
        }

        actions
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

    pub fn reconcile_inventory(&mut self, snapshot: &MarketSnapshot) -> Vec<EngineAction> {
        self.update_market(snapshot);

        // 兜底自愈: 任一侧开仓网格被意外清空时，直接重建双向网格，
        // 保证单一交易对长期运行下始终有多空两侧挂单。
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
        if !self.config.grid.strict_pairing {
            actions.extend(self.enforce_close_coverage(OrderSide::Sell));
            actions.extend(self.enforce_close_coverage(OrderSide::Buy));
            actions.extend(self.enforce_close_limit(OrderSide::Sell));
            actions.extend(self.enforce_close_limit(OrderSide::Buy));
            actions.extend(self.refill_close_slots(OrderSide::Sell, snapshot));
            actions.extend(self.refill_close_slots(OrderSide::Buy, snapshot));
            actions.extend(self.normalize_close_grid(OrderSide::Sell));
            actions.extend(self.normalize_close_grid(OrderSide::Buy));
        } else {
            actions.extend(self.enforce_close_limit(OrderSide::Sell));
            actions.extend(self.enforce_close_limit(OrderSide::Buy));
        }
        actions.extend(self.enforce_open_limit(OrderSide::Sell));
        actions.extend(self.enforce_open_limit(OrderSide::Buy));
        actions.extend(self.refill_open_slots(OrderSide::Sell, snapshot));
        actions.extend(self.refill_open_slots(OrderSide::Buy, snapshot));
        actions.extend(self.normalize_open_grid(OrderSide::Sell));
        actions.extend(self.normalize_open_grid(OrderSide::Buy));
        if !self.config.grid.strict_pairing {
            // open 网格归一化后，再把 close 镜像到同侧 open 价位，
            // 保证“开+平”共用同一组价格。
            actions.extend(self.normalize_close_grid(OrderSide::Sell));
            actions.extend(self.normalize_close_grid(OrderSide::Buy));
        }
        actions
    }

    pub fn trigger_kill_switch(&mut self, reason: &str) -> Vec<EngineAction> {
        self.state.kill_switch = true;
        self.cancel_all_orders(reason)
    }

    pub fn handle_post_only_reject(&mut self, order_id: &str) -> Vec<EngineAction> {
        let retries = self.config.execution.post_only_retries;
        let record = match self.state.ledger.get(order_id).cloned() {
            Some(record) => record,
            None => return Vec::new(),
        };

        if record.retries >= retries {
            return self.cancel_order(order_id, "post_only_reject");
        }

        let tick = self.precision.tick_size;
        let new_price = match record.intent.side() {
            OrderSide::Buy => record.price - tick,
            OrderSide::Sell => record.price + tick,
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
        let position = &self.state.position;
        let (long_qty, short_qty, mark_price, funding_rate) = if self.config.grid.strict_pairing {
            // 严格配对模式忽略历史仓位敞口，避免因旧仓导致开仓网格被风险拦截
            (0.0, 0.0, 0.0, 0.0)
        } else {
            (
                position.long_qty,
                position.short_qty,
                position.mark_price,
                self.state.funding_rate,
            )
        };
        self.state.risk = RiskState::evaluate(
            &self.config.risk,
            long_qty,
            short_qty,
            mark_price,
            position.equity,
            position.maintenance_margin,
            funding_rate,
        );
        if self.state.risk.flags.only_close {
            self.cancel_open_orders();
        }
    }

    fn reference_price(&self, snapshot: &MarketSnapshot) -> f64 {
        match self.config.price_reference {
            super::config::PriceReference::Mid => snapshot.mid(),
            super::config::PriceReference::Last => snapshot.last_price,
        }
    }

    fn price_levels(&self, reference: f64, levels: usize, side: OrderSide) -> Vec<f64> {
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
        if intent.is_open() && !self.allow_open_intent(intent) {
            return Vec::new();
        }

        let mut actions = Vec::new();
        for price in prices {
            if let Some(qty) = self.qty_from_notional(*price) {
                if let Some(action) = self.place_order(intent, *price, qty, 0) {
                    actions.push(action);
                }
            }
        }
        actions
    }

    fn build_close_orders(&mut self, side: OrderSide, prices: &[f64]) -> Vec<EngineAction> {
        let intent = Self::close_intent_for_side(side);
        let mut remaining = self.available_for_close(side);
        if remaining <= 0.0 {
            return Vec::new();
        }

        let mut actions = Vec::new();
        let mut seen = HashSet::new();
        for price in prices {
            let quantized_price = self.precision.quantize_price_for_side(*price, side);
            let price_key = self.price_key(quantized_price);
            if !seen.insert(price_key) {
                continue;
            }
            if let Some(qty) = self.qty_from_notional(quantized_price) {
                if remaining >= qty {
                    if let Some(action) = self.place_order(intent, quantized_price, qty, 0) {
                        actions.push(action);
                        remaining -= qty;
                    }
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
        let base_price = fill.fill_price;
        let open_price = self.step_price(base_price, OrderSide::Buy, levels);
        if let Some(action) = self.place_open_at_price(OrderSide::Buy, open_price) {
            actions.push(action);
        }

        let close_price = self.step_price(base_price, OrderSide::Sell, 1);
        if let Some(action) =
            self.place_close_from_fill(OrderIntent::CloseLongSell, close_price, fill.fill_qty)
        {
            actions.push(action);
        }

        let close_intent = Self::close_intent_for_side(OrderSide::Sell);
        let close_count = self
            .book_for_side(OrderSide::Sell)
            .count_by_intent(close_intent);
        if close_count > levels {
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
        let base_price = fill.fill_price;
        let open_price = self.step_price(base_price, OrderSide::Sell, levels);
        if let Some(action) = self.place_open_at_price(OrderSide::Sell, open_price) {
            actions.push(action);
        }

        let close_price = self.step_price(base_price, OrderSide::Buy, 1);
        if let Some(action) =
            self.place_close_from_fill(OrderIntent::CloseShortBuy, close_price, fill.fill_qty)
        {
            actions.push(action);
        }

        let close_intent = Self::close_intent_for_side(OrderSide::Buy);
        let close_count = self
            .book_for_side(OrderSide::Buy)
            .count_by_intent(close_intent);
        if close_count > levels {
            if let Some(action) =
                self.cancel_farthest_close_order(OrderSide::Buy, "roll_open_short")
            {
                actions.push(action);
            }
        }
        actions
    }

    fn roll_after_close_long(
        &mut self,
        fill: &FillEvent,
        _snapshot: &MarketSnapshot,
    ) -> Vec<EngineAction> {
        let mut actions = Vec::new();
        let levels = self.config.grid.levels_per_side;
        let base_price = fill.fill_price;
        if !self.config.grid.strict_pairing {
            let close_price = self.step_price(base_price, OrderSide::Sell, levels);
            if let Some(action) = self.place_close_at_price(OrderSide::Sell, close_price) {
                actions.push(action);
            }
        }

        let open_price = self.step_price(base_price, OrderSide::Buy, 1);
        if let Some(action) = self.place_open_at_price(OrderSide::Buy, open_price) {
            actions.push(action);
        }

        let open_intent = Self::open_intent_for_side(OrderSide::Buy);
        let open_count = self
            .book_for_side(OrderSide::Buy)
            .count_by_intent(open_intent);
        if open_count > levels {
            if let Some(action) = self.cancel_farthest_open_order(OrderSide::Buy, "roll_close_long")
            {
                actions.push(action);
            }
        }

        actions
    }

    fn roll_after_close_short(
        &mut self,
        fill: &FillEvent,
        _snapshot: &MarketSnapshot,
    ) -> Vec<EngineAction> {
        let mut actions = Vec::new();
        let levels = self.config.grid.levels_per_side;
        let base_price = fill.fill_price;
        if !self.config.grid.strict_pairing {
            let close_price = self.step_price(base_price, OrderSide::Buy, levels);
            if let Some(action) = self.place_close_at_price(OrderSide::Buy, close_price) {
                actions.push(action);
            }
        }

        let open_price = self.step_price(base_price, OrderSide::Sell, 1);
        if let Some(action) = self.place_open_at_price(OrderSide::Sell, open_price) {
            actions.push(action);
        }

        let open_intent = Self::open_intent_for_side(OrderSide::Sell);
        let open_count = self
            .book_for_side(OrderSide::Sell)
            .count_by_intent(open_intent);
        if open_count > levels {
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
            let spacing = self.config.grid.grid_spacing_pct;
            let offset = spacing * steps as f64;
            match side {
                OrderSide::Buy => base * (1.0 - offset),
                OrderSide::Sell => base * (1.0 + offset),
            }
        }
    }

    fn close_price_candidates(&self, side: OrderSide, snapshot: &MarketSnapshot) -> Vec<f64> {
        let reference = self.reference_price(snapshot);
        let levels = self.config.grid.levels_per_side;
        self.price_levels(reference, levels, side)
    }

    fn close_price_exists(&self, side: OrderSide, price: f64) -> bool {
        let intent = Self::close_intent_for_side(side);
        let quantized = self.precision.quantize_price_for_side(price, side);
        let key = self.price_key(quantized);
        self.book_for_side(side)
            .slots
            .iter()
            .any(|slot| slot.intent == intent && self.price_key(slot.price) == key)
    }

    fn price_key(&self, price: f64) -> i64 {
        let factor = 10f64.powi(self.precision.price_digits as i32);
        (price * factor).round() as i64
    }

    fn available_for_close(&self, side: OrderSide) -> f64 {
        if self.config.grid.strict_pairing {
            return f64::MAX;
        }
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

    fn farthest_price_for_intent(&self, side: OrderSide, intent: OrderIntent) -> Option<f64> {
        self.book_for_side(side)
            .slots
            .iter()
            .rev()
            .find(|slot| slot.intent == intent)
            .map(|slot| slot.price)
    }

    fn nearest_price_for_intent(&self, side: OrderSide, intent: OrderIntent) -> Option<f64> {
        self.book_for_side(side)
            .slots
            .iter()
            .find(|slot| slot.intent == intent)
            .map(|slot| slot.price)
    }

    fn farthest_order_id_for_intent(&self, side: OrderSide, intent: OrderIntent) -> Option<String> {
        self.book_for_side(side)
            .slots
            .iter()
            .rev()
            .find(|slot| slot.intent == intent)
            .map(|slot| slot.id.clone())
    }

    fn nearest_order_id_for_intent(&self, side: OrderSide, intent: OrderIntent) -> Option<String> {
        self.book_for_side(side)
            .slots
            .iter()
            .find(|slot| slot.intent == intent)
            .map(|slot| slot.id.clone())
    }

    fn cancel_farthest_open_order(
        &mut self,
        side: OrderSide,
        reason: &str,
    ) -> Option<EngineAction> {
        let intent = Self::open_intent_for_side(side);
        let order_id = self.farthest_order_id_for_intent(side, intent)?;
        self.cancel_order(&order_id, reason).pop()
    }

    fn cancel_farthest_close_order(
        &mut self,
        side: OrderSide,
        reason: &str,
    ) -> Option<EngineAction> {
        let intent = Self::close_intent_for_side(side);
        let order_id = self.farthest_order_id_for_intent(side, intent)?;
        self.cancel_order(&order_id, reason).pop()
    }

    fn cancel_nearest_open_order(&mut self, side: OrderSide, reason: &str) -> Option<EngineAction> {
        let intent = Self::open_intent_for_side(side);
        let order_id = self.nearest_order_id_for_intent(side, intent)?;
        self.cancel_order(&order_id, reason).pop()
    }

    fn place_farthest_open_order(
        &mut self,
        side: OrderSide,
        snapshot: &MarketSnapshot,
    ) -> Option<EngineAction> {
        if !self.config.grid.fill_remaining_slots_with_opens {
            return None;
        }
        let spacing = self.config.grid.grid_spacing_pct;
        let spacing_abs = self.grid_spacing_abs();
        let levels = self.config.grid.levels_per_side;
        let reference = self.reference_price(snapshot);
        let intent = Self::open_intent_for_side(side);

        if !self.allow_open_intent(intent) {
            return None;
        }

        let base_price = self
            .farthest_price_for_intent(side, intent)
            .unwrap_or_else(|| match spacing_abs {
                Some(abs) => match side {
                    OrderSide::Buy => reference - abs * levels as f64,
                    OrderSide::Sell => reference + abs * levels as f64,
                },
                None => match side {
                    OrderSide::Buy => reference * (1.0 - spacing).powi(levels as i32),
                    OrderSide::Sell => reference * (1.0 + spacing).powi(levels as i32),
                },
            });
        let new_price = match spacing_abs {
            Some(abs) => match side {
                OrderSide::Buy => base_price - abs,
                OrderSide::Sell => base_price + abs,
            },
            None => match side {
                OrderSide::Buy => base_price * (1.0 - spacing),
                OrderSide::Sell => base_price * (1.0 + spacing),
            },
        };
        let qty = self.qty_from_notional(new_price)?;
        self.place_order(intent, new_price, qty, 0)
    }

    fn place_open_at_price(&mut self, side: OrderSide, price: f64) -> Option<EngineAction> {
        let intent = Self::open_intent_for_side(side);
        if !self.allow_open_intent(intent) {
            return None;
        }
        let qty = self.qty_from_notional(price)?;
        self.place_order(intent, price, qty, 0)
    }

    fn place_farthest_close_order(
        &mut self,
        side: OrderSide,
        snapshot: &MarketSnapshot,
    ) -> Option<EngineAction> {
        let intent = Self::close_intent_for_side(side);
        let available = self.available_for_close(side);
        if available <= 0.0 {
            return None;
        }

        let booked = self.book_for_side(side).total_qty_for_intent(intent);
        if available <= booked {
            return None;
        }
        let candidates = self.close_price_candidates(side, snapshot);
        for candidate in candidates.into_iter().rev() {
            if self.close_price_exists(side, candidate) {
                continue;
            }
            let quantized_price = self.precision.quantize_price_for_side(candidate, side);
            let qty = self.qty_from_notional(quantized_price)?;
            if available - booked < qty {
                continue;
            }
            if let Some(action) = self.place_order(intent, quantized_price, qty, 0) {
                return Some(action);
            }
        }
        None
    }

    fn place_close_at_price(&mut self, side: OrderSide, price: f64) -> Option<EngineAction> {
        let intent = Self::close_intent_for_side(side);
        let available = self.available_for_close(side);
        if available <= 0.0 {
            return None;
        }
        let booked = self.book_for_side(side).total_qty_for_intent(intent);
        let qty = self.qty_from_notional(price)?;
        if available - booked < qty {
            return None;
        }
        self.place_order(intent, price, qty, 0)
    }

    fn trim_excess_close_orders(&mut self) -> Vec<EngineAction> {
        let mut actions = Vec::new();
        if !self.config.grid.strict_pairing {
            actions.extend(self.enforce_close_coverage(OrderSide::Sell));
            actions.extend(self.enforce_close_coverage(OrderSide::Buy));
        }
        actions.extend(self.enforce_close_limit(OrderSide::Sell));
        actions.extend(self.enforce_close_limit(OrderSide::Buy));
        actions
    }

    fn follow_buy_side(&mut self, best_buy: f64, mid: f64) -> Vec<EngineAction> {
        if !self.config.grid.fill_remaining_slots_with_opens {
            return Vec::new();
        }
        let mut actions = Vec::new();
        let mut placed = false;
        let spacing = self.config.grid.grid_spacing_pct;
        let new_price = match self.grid_spacing_abs() {
            Some(abs) => best_buy + abs,
            None => best_buy * (1.0 + spacing),
        };
        let intent = OrderIntent::OpenLongBuy;
        if self.allow_open_intent(intent) {
            if let Some(qty) = self.qty_from_notional(new_price) {
                if let Some(action) = self.place_order(intent, new_price, qty, 0) {
                    actions.push(action);
                    placed = true;
                }
            }
        }
        if placed {
            if let Some(action) = self.cancel_farthest_open_order(OrderSide::Buy, "follow_buy") {
                actions.push(action);
            }
        }
        let _ = mid;
        actions
    }

    fn follow_sell_side(&mut self, best_sell: f64, mid: f64) -> Vec<EngineAction> {
        if !self.config.grid.fill_remaining_slots_with_opens {
            return Vec::new();
        }
        let mut actions = Vec::new();
        let mut placed = false;
        let spacing = self.config.grid.grid_spacing_pct;
        let new_price = match self.grid_spacing_abs() {
            Some(abs) => best_sell - abs,
            None => best_sell * (1.0 - spacing),
        };
        let intent = OrderIntent::OpenShortSell;
        if self.allow_open_intent(intent) {
            if let Some(qty) = self.qty_from_notional(new_price) {
                if let Some(action) = self.place_order(intent, new_price, qty, 0) {
                    actions.push(action);
                    placed = true;
                }
            }
        }
        if placed {
            if let Some(action) = self.cancel_farthest_open_order(OrderSide::Sell, "follow_sell") {
                actions.push(action);
            }
        }
        let _ = mid;
        actions
    }

    fn enforce_close_coverage(&mut self, side: OrderSide) -> Vec<EngineAction> {
        if self.config.grid.strict_pairing {
            return Vec::new();
        }
        let covered_ids = self.covered_close_order_ids(side);
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
        let intent = Self::close_intent_for_side(side);
        let desired = self.config.grid.levels_per_side;
        let mut actions = Vec::new();
        let mut count = self.book_for_side(side).count_by_intent(intent);
        while count > desired {
            if let Some(order_id) = self.farthest_order_id_for_intent(side, intent) {
                actions.extend(self.cancel_order(&order_id, "close_limit"));
                count -= 1;
            } else {
                break;
            }
        }
        actions
    }

    fn enforce_open_limit(&mut self, side: OrderSide) -> Vec<EngineAction> {
        let intent = Self::open_intent_for_side(side);
        let desired = self.config.grid.levels_per_side;
        let mut actions = Vec::new();
        let mut count = self.book_for_side(side).count_by_intent(intent);
        while count > desired {
            if let Some(order_id) = self.farthest_order_id_for_intent(side, intent) {
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
        if !self.config.grid.fill_remaining_slots_with_opens {
            return Vec::new();
        }
        let desired = self.config.grid.levels_per_side;
        let intent = Self::open_intent_for_side(side);
        let current_len = self.book_for_side(side).count_by_intent(intent);
        if current_len >= desired {
            return Vec::new();
        }

        let mut actions = Vec::new();
        let spacing = self.config.grid.grid_spacing_pct;
        let spacing_abs = self.grid_spacing_abs();
        let reference = self.reference_price(snapshot);
        let mut price = self
            .farthest_price_for_intent(side, intent)
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

        if !self.allow_open_intent(intent) {
            return Vec::new();
        }

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
            if let Some(qty) = self.qty_from_notional(price) {
                if let Some(action) = self.place_order(intent, price, qty, 0) {
                    actions.push(action);
                    total_count += 1;
                }
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
        let intent = Self::open_intent_for_side(side);
        if !self.allow_open_intent(intent) {
            return Vec::new();
        }

        let open_slots: Vec<OrderSlot> = self
            .book_for_side(side)
            .slots
            .iter()
            .filter(|slot| slot.intent == intent)
            .cloned()
            .collect();
        if open_slots.is_empty() {
            return Vec::new();
        }

        let mut anchor_price = None;
        for slot in &open_slots {
            anchor_price = match anchor_price {
                None => Some(slot.price),
                Some(current) => match side {
                    OrderSide::Buy if slot.price > current => Some(slot.price),
                    OrderSide::Sell if slot.price < current => Some(slot.price),
                    _ => Some(current),
                },
            };
        }
        let anchor_price = match anchor_price {
            Some(price) => price,
            None => return Vec::new(),
        };

        let mut target_prices = Vec::with_capacity(levels);
        let mut target_keys = HashSet::with_capacity(levels);
        for step in 0..levels {
            let price = self.step_price(anchor_price, side, step);
            let quantized = self.precision.quantize_price_for_side(price, side);
            let key = self.price_key(quantized);
            if target_keys.insert(key) {
                target_prices.push(quantized);
            }
        }

        let mut actions = Vec::new();
        let mut kept_keys = HashSet::new();
        for slot in open_slots {
            let key = self.price_key(slot.price);
            if !target_keys.contains(&key) || !kept_keys.insert(key) {
                actions.extend(self.cancel_order(&slot.id, "normalize_open_grid"));
            }
        }

        for price in target_prices {
            let key = self.price_key(price);
            if kept_keys.contains(&key) {
                continue;
            }
            if let Some(qty) = self.qty_from_notional(price) {
                if let Some(action) = self.place_order(intent, price, qty, 0) {
                    actions.push(action);
                    kept_keys.insert(key);
                }
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
        let intent = Self::close_intent_for_side(side);
        let open_intent = Self::open_intent_for_side(side);
        let close_slots: Vec<OrderSlot> = self
            .book_for_side(side)
            .slots
            .iter()
            .filter(|slot| slot.intent == intent)
            .cloned()
            .collect();

        let mut target_prices = Vec::with_capacity(levels);
        let mut target_keys = HashSet::with_capacity(levels);

        // 非 strict_pairing 模式下，close 订单优先镜像同侧 open 订单价位：
        // - 卖侧: 平多与开空共用价格
        // - 买侧: 平空与开多共用价格
        let open_slots: Vec<OrderSlot> = self
            .book_for_side(side)
            .slots
            .iter()
            .filter(|slot| slot.intent == open_intent)
            .cloned()
            .collect();
        if !open_slots.is_empty() {
            for slot in open_slots.into_iter().take(levels) {
                let quantized = self.precision.quantize_price_for_side(slot.price, side);
                let key = self.price_key(quantized);
                if target_keys.insert(key) {
                    target_prices.push(quantized);
                }
            }
        } else if !close_slots.is_empty() {
            // 兜底：当 open 侧为空（如 only-close 风险模式）时，
            // 保持原有 close 梯队形态，避免 close 订单被全部撤空。
            let mut anchor_price = None;
            for slot in &close_slots {
                anchor_price = match anchor_price {
                    None => Some(slot.price),
                    Some(current) => match side {
                        OrderSide::Buy if slot.price > current => Some(slot.price),
                        OrderSide::Sell if slot.price < current => Some(slot.price),
                        _ => Some(current),
                    },
                };
            }
            let anchor_price = match anchor_price {
                Some(price) => price,
                None => return Vec::new(),
            };
            for step in 0..levels {
                let price = self.step_price(anchor_price, side, step);
                let quantized = self.precision.quantize_price_for_side(price, side);
                let key = self.price_key(quantized);
                if target_keys.insert(key) {
                    target_prices.push(quantized);
                }
            }
        } else {
            return Vec::new();
        }

        let mut actions = Vec::new();
        let mut kept_keys = HashSet::new();
        let mut remaining = self.available_for_close(side);
        for slot in close_slots {
            let key = self.price_key(slot.price);
            let should_keep =
                target_keys.contains(&key) && !kept_keys.contains(&key) && remaining >= slot.qty;
            if should_keep {
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
            if let Some(qty) = self.qty_from_notional(price) {
                if remaining < qty {
                    continue;
                }
                if let Some(action) = self.place_order(intent, price, qty, 0) {
                    actions.push(action);
                    kept_keys.insert(key);
                    remaining -= qty;
                }
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
        let intent = Self::close_intent_for_side(side);
        let available = self.available_for_close(side);
        if available <= 0.0 {
            return Vec::new();
        }

        let existing_slots: Vec<OrderSlot> = self
            .book_for_side(side)
            .slots
            .iter()
            .filter(|slot| slot.intent == intent)
            .cloned()
            .collect();
        let current_len = existing_slots.len();
        if current_len >= desired {
            return Vec::new();
        }

        let existing_qty: f64 = existing_slots.iter().map(|slot| slot.qty).sum();
        let mut remaining = available - existing_qty;
        if remaining <= 0.0 {
            return Vec::new();
        }

        let mut actions = Vec::new();
        let mut total_count = current_len;
        let mut existing_keys: HashSet<i64> = existing_slots
            .iter()
            .map(|slot| self.price_key(slot.price))
            .collect();
        let candidates = self.close_price_candidates(side, snapshot);
        for candidate in candidates {
            if total_count >= desired {
                break;
            }
            let quantized_price = self.precision.quantize_price_for_side(candidate, side);
            let price_key = self.price_key(quantized_price);
            if existing_keys.contains(&price_key) {
                continue;
            }
            if let Some(qty) = self.qty_from_notional(quantized_price) {
                if remaining < qty {
                    continue;
                }
                if let Some(action) = self.place_order(intent, quantized_price, qty, 0) {
                    actions.push(action);
                    remaining -= qty;
                    total_count += 1;
                    existing_keys.insert(price_key);
                }
            }
        }

        actions
    }

    fn cancel_open_orders(&mut self) {
        let open_ids: Vec<String> = self
            .state
            .ledger
            .orders
            .values()
            .filter(|record| record.intent.is_open())
            .map(|record| record.id.clone())
            .collect();
        for id in open_ids {
            let _ = self.cancel_order(&id, "risk_only_close");
        }
    }

    fn cancel_all_orders(&mut self, reason: &str) -> Vec<EngineAction> {
        let ids = self.state.ledger.all_ids();
        let mut actions = Vec::new();
        for id in ids {
            actions.extend(self.cancel_order(&id, reason));
        }
        actions
    }

    fn cancel_order(&mut self, order_id: &str, reason: &str) -> Vec<EngineAction> {
        let mut actions = Vec::new();
        if let Some(record) = self.state.ledger.remove(order_id) {
            match record.intent.side() {
                OrderSide::Buy => {
                    self.state.buy_book.remove(order_id);
                }
                OrderSide::Sell => {
                    self.state.sell_book.remove(order_id);
                }
            }
            actions.push(EngineAction::Cancel {
                order_id: order_id.to_string(),
                reason: reason.to_string(),
            });
        }
        actions
    }

    fn covered_close_order_ids(&self, side: OrderSide) -> HashSet<String> {
        let (book, available) = match side {
            OrderSide::Buy => (&self.state.buy_book, self.state.position.short_available),
            OrderSide::Sell => (&self.state.sell_book, self.state.position.long_available),
        };

        let mut remaining = available;
        let mut covered = HashSet::new();
        for slot in book.slots.iter() {
            if slot.intent.is_close() && remaining >= slot.qty {
                covered.insert(slot.id.clone());
                remaining -= slot.qty;
            }
        }
        covered
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
        let mut price = if self.config.grid.strict_pairing {
            if price > 0.0 {
                price
            } else {
                return None;
            }
        } else {
            self.adjust_close_price_for_cost(intent, price)?
        };
        price = self.precision.quantize_price_for_side(price, side);
        let qty = self.precision.quantize_qty(qty);
        if price <= 0.0 || qty <= 0.0 {
            return None;
        }
        if qty < self.precision.min_qty {
            return None;
        }
        if self.precision.min_notional > 0.0 && price * qty < self.precision.min_notional {
            return None;
        }
        let price_key = self.price_key(price);
        if self
            .book_for_side(side)
            .slots
            .iter()
            .any(|slot| slot.intent == intent && self.price_key(slot.price) == price_key)
        {
            return None;
        }

        let id = self.next_order_id();
        let record = OrderRecord {
            id: id.clone(),
            intent,
            price,
            qty,
            filled_qty: 0.0,
            created_at: Utc::now(),
            retries,
        };
        self.state.ledger.insert(record);
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

    fn place_close_from_fill(
        &mut self,
        intent: OrderIntent,
        price: f64,
        qty: f64,
    ) -> Option<EngineAction> {
        self.place_order(intent, price, qty, 0)
    }

    fn qty_from_notional(&self, price: f64) -> Option<f64> {
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
        let mut qty = target_notional / price;
        qty = self.precision.quantize_qty_up(qty);
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

    fn adjust_close_price_for_cost(&self, intent: OrderIntent, raw_price: f64) -> Option<f64> {
        if raw_price <= 0.0 {
            return None;
        }
        match intent {
            OrderIntent::CloseLongSell => {
                if self.state.position.long_available <= 0.0 {
                    return None;
                }
                Some(raw_price)
            }
            OrderIntent::CloseShortBuy => {
                if self.state.position.short_available <= 0.0 {
                    return None;
                }
                Some(raw_price)
            }
            _ => Some(raw_price),
        }
    }

    fn allow_open_intent(&self, intent: OrderIntent) -> bool {
        if self.config.grid.strict_pairing {
            return !self.state.risk.flags.only_close;
        }
        match intent {
            OrderIntent::OpenLongBuy => self.state.risk.allow_open_long(),
            OrderIntent::OpenShortSell => self.state.risk.allow_open_short(),
            _ => true,
        }
    }

    fn next_order_id(&mut self) -> String {
        let id = format!("grid-{}", self.state.next_id);
        self.state.next_id += 1;
        id
    }

    fn apply_fill(&mut self, fill: &FillEvent) {
        let mut remove_order = false;
        let mut intent_side = None;
        if let Some(record) = self.state.ledger.get_mut(&fill.order_id) {
            record.filled_qty += fill.fill_qty;
            let remaining = (record.qty - fill.fill_qty).max(0.0);
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
                self.state.position.long_qty += fill.fill_qty;
                self.state.position.long_available += fill.fill_qty;
            }
            OrderIntent::CloseLongSell => {
                self.state.position.long_qty =
                    (self.state.position.long_qty - fill.fill_qty).max(0.0);
                self.state.position.long_available =
                    (self.state.position.long_available - fill.fill_qty).max(0.0);
            }
            OrderIntent::OpenShortSell => {
                self.state.position.short_qty += fill.fill_qty;
                self.state.position.short_available += fill.fill_qty;
            }
            OrderIntent::CloseShortBuy => {
                self.state.position.short_qty =
                    (self.state.position.short_qty - fill.fill_qty).max(0.0);
                self.state.position.short_available =
                    (self.state.position.short_available - fill.fill_qty).max(0.0);
            }
        }

        self.refresh_risk();
    }

    fn prune_follow_actions(&mut self, now: DateTime<Utc>) {
        while let Some(front) = self.state.follow_actions.front() {
            if now - *front > ChronoDuration::seconds(60) {
                self.state.follow_actions.pop_front();
            } else {
                break;
            }
        }
    }
}
