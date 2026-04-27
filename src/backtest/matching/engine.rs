use std::collections::HashMap;

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};

use crate::backtest::matching::book::{DepthDelta, OrderBookState};
use crate::backtest::matching::ledger::{BacktestLedger, FillResult, FundingSettlement};
use crate::backtest::schema::BacktestEvent;
use crate::backtest::strategy::StrategySignal;
use crate::core::types::{Kline, MarketType, OrderSide, OrderType};

const DEFAULT_MAKER_FEE_BPS: f64 = 2.0;
const FLOAT_EPSILON: f64 = 1e-9;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitOrderStatus {
    Active,
    Filled,
    Cancelled,
    Expired,
}

#[derive(Debug, Clone)]
pub struct LimitOrder {
    pub id: u64,
    pub symbol: String,
    pub side: OrderSide,
    pub limit_price: f64,
    pub initial_quantity: f64,
    pub remaining_quantity: f64,
    pub queue_ahead_qty: f64,
    pub market_type: MarketType,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub status: LimitOrderStatus,
}

impl LimitOrder {
    pub fn filled_quantity(&self) -> f64 {
        (self.initial_quantity - self.remaining_quantity).max(0.0)
    }

    pub fn is_terminal(&self) -> bool {
        self.status != LimitOrderStatus::Active
    }
}

#[derive(Debug, Clone, Default)]
pub struct LimitOrderProcessingResult {
    pub filled_order_ids: Vec<u64>,
    pub cancelled_order_ids: Vec<u64>,
    pub fills: Vec<LimitOrderFillUpdate>,
}

#[derive(Debug, Clone)]
pub struct LimitOrderFillUpdate {
    pub order_id: u64,
    pub fill: FillResult,
    pub is_terminal: bool,
}

#[derive(Debug, Clone)]
pub struct BacktestEngineState {
    order_books: HashMap<String, OrderBookState>,
    limit_orders: HashMap<u64, LimitOrder>,
    ledger: BacktestLedger,
    max_depth: usize,
    next_order_id: u64,
}

impl BacktestEngineState {
    pub fn new(settlement_currency: &str, initial_cash: f64, max_depth: usize) -> Self {
        Self {
            order_books: HashMap::new(),
            limit_orders: HashMap::new(),
            ledger: BacktestLedger::new(settlement_currency, initial_cash),
            max_depth,
            next_order_id: 1,
        }
    }

    pub fn seed_order_book(
        &mut self,
        symbol: &str,
        bids: Vec<[f64; 2]>,
        asks: Vec<[f64; 2]>,
        last_update_id: u64,
        timestamp: DateTime<Utc>,
    ) -> Result<()> {
        let mut book = OrderBookState::new(symbol, self.max_depth);
        book.apply_snapshot(bids, asks, last_update_id, timestamp);
        self.order_books.insert(symbol.to_string(), book);
        Ok(())
    }

    pub fn apply_event(&mut self, event: &BacktestEvent) -> Result<LimitOrderProcessingResult> {
        let mut result = LimitOrderProcessingResult::default();
        match event {
            BacktestEvent::DepthDelta(event) => {
                result.cancelled_order_ids = self.expire_orders(event.logical_ts);

                let active_ids = self.active_order_ids_for_symbol(&event.symbol);
                let pre_same_side_qty = {
                    let book = self
                        .order_books
                        .entry(event.symbol.clone())
                        .or_insert_with(|| OrderBookState::new(&event.symbol, self.max_depth));

                    active_ids
                        .iter()
                        .filter_map(|order_id| {
                            self.limit_orders.get(order_id).map(|order| {
                                (
                                    *order_id,
                                    same_side_book_qty(book, order.side, order.limit_price),
                                )
                            })
                        })
                        .collect::<HashMap<_, _>>()
                };

                let book = self
                    .order_books
                    .entry(event.symbol.clone())
                    .or_insert_with(|| OrderBookState::new(&event.symbol, self.max_depth));
                book.apply_delta(DepthDelta {
                    first_update_id: event.first_update_id,
                    final_update_id: event.final_update_id,
                    bids: event.bids.clone(),
                    asks: event.asks.clone(),
                    timestamp: event.logical_ts,
                })?;

                for order_id in active_ids {
                    let Some(order) = self.limit_orders.get_mut(&order_id) else {
                        continue;
                    };
                    if order.is_terminal() {
                        continue;
                    }

                    let post_same_side_qty =
                        same_side_book_qty(book, order.side, order.limit_price);
                    let pre_same_side_qty =
                        pre_same_side_qty.get(&order_id).copied().unwrap_or(0.0);
                    let depleted = (pre_same_side_qty - post_same_side_qty).max(0.0);
                    order.queue_ahead_qty = (order.queue_ahead_qty - depleted).max(0.0);
                }

                let match_result =
                    self.match_active_limit_orders(&event.symbol, event.logical_ts)?;
                result.filled_order_ids = match_result.filled_order_ids;
                result.fills = match_result.fills;
            }
            BacktestEvent::MarkPrice(event) => {
                self.ledger
                    .apply_mark_price(&event.symbol, event.mark_price, event.logical_ts);
            }
            BacktestEvent::FundingRate(event) => {
                self.ledger.apply_funding(FundingSettlement {
                    symbol: event.symbol.clone(),
                    rate: event.funding_rate,
                    mark_price: event.mark_price.unwrap_or(0.0),
                    timestamp: event.logical_ts,
                });
            }
            BacktestEvent::Trade(event) => {
                result.cancelled_order_ids = self.expire_orders(event.logical_ts);
                let trade_result = self.match_active_limit_orders_on_trade(
                    &event.symbol,
                    event.trade.side,
                    event.trade.price,
                    event.trade.amount.abs(),
                    event.logical_ts,
                )?;
                result.filled_order_ids = trade_result.filled_order_ids;
                result.fills = trade_result.fills;
                self.ledger
                    .apply_mark_price(&event.symbol, event.trade.price, event.logical_ts);
            }
            _ => {}
        }

        Ok(result)
    }

    pub fn execute_market_signal(
        &mut self,
        signal: &StrategySignal,
        market_type: MarketType,
        notional: f64,
        taker_fee_bps: f64,
    ) -> Result<FillResult> {
        let quantity = if signal.price.abs() < f64::EPSILON {
            0.0
        } else {
            notional / signal.price
        };
        self.execute_market_order(
            &signal.symbol,
            signal.side,
            signal.logical_ts,
            signal.price,
            quantity,
            market_type,
            taker_fee_bps,
            0.0,
        )
    }

    pub fn execute_market_order(
        &mut self,
        symbol: &str,
        side: OrderSide,
        timestamp: DateTime<Utc>,
        price: f64,
        quantity: f64,
        market_type: MarketType,
        taker_fee_bps: f64,
        market_slippage_bps: f64,
    ) -> Result<FillResult> {
        let executed_quantity = quantity.abs();
        let raw_execution_price =
            self.market_execution_price(symbol, side, price, executed_quantity);
        let execution_price =
            apply_adverse_market_slippage(raw_execution_price, side, market_slippage_bps);
        let fee_paid = executed_quantity * execution_price * (taker_fee_bps / 10_000.0);
        let fill = FillResult {
            symbol: symbol.to_string(),
            side,
            order_type: OrderType::Market,
            market_type,
            quantity: executed_quantity,
            price: execution_price,
            is_maker: false,
            fee_paid,
            timestamp,
        };

        self.ledger.apply_fill(fill.clone())?;
        self.ledger
            .apply_mark_price(symbol, execution_price, timestamp);

        Ok(fill)
    }

    pub fn place_limit_order(
        &mut self,
        symbol: &str,
        side: OrderSide,
        limit_price: f64,
        quantity: f64,
        market_type: MarketType,
        created_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Result<u64> {
        if limit_price <= 0.0 {
            return Err(anyhow!("limit price must be positive"));
        }
        if quantity <= 0.0 {
            return Err(anyhow!("limit quantity must be positive"));
        }
        if expires_at <= created_at {
            return Err(anyhow!("limit order expiry must be after creation time"));
        }

        if let Some(book) = self.order_books.get(symbol) {
            if would_cross_book(book, side, limit_price) {
                return Err(anyhow!(
                    "post-only limit order would cross the current order book for {}",
                    symbol
                ));
            }
        }

        let queue_ahead_qty = self
            .order_books
            .get(symbol)
            .map(|book| same_side_book_qty(book, side, limit_price))
            .unwrap_or(0.0)
            + self.local_queue_ahead_qty(symbol, side, limit_price);

        let order_id = self.next_order_id;
        self.next_order_id += 1;

        self.limit_orders.insert(
            order_id,
            LimitOrder {
                id: order_id,
                symbol: symbol.to_string(),
                side,
                limit_price,
                initial_quantity: quantity,
                remaining_quantity: quantity,
                queue_ahead_qty,
                market_type,
                created_at,
                expires_at,
                status: LimitOrderStatus::Active,
            },
        );

        Ok(order_id)
    }

    pub fn process_limit_orders_on_kline(
        &mut self,
        kline: &Kline,
        market_type: MarketType,
        maker_fee_bps: f64,
    ) -> Result<LimitOrderProcessingResult> {
        let mut result = LimitOrderProcessingResult::default();
        let order_ids = self.active_order_ids_for_symbol(&kline.symbol);

        for order_id in order_ids {
            let Some(order) = self.limit_orders.get(&order_id).cloned() else {
                continue;
            };

            if kline.close_time <= order.created_at {
                continue;
            }

            if limit_order_touched(&order, kline) {
                let fill =
                    self.fill_on_kline(order_id, kline.close_time, market_type, maker_fee_bps)?;
                if let Some(fill) = fill {
                    result.filled_order_ids.push(order_id);
                    result.fills.push(LimitOrderFillUpdate {
                        order_id,
                        fill,
                        is_terminal: true,
                    });
                }
                continue;
            }

            if kline.close_time >= order.expires_at && self.expire_order(order_id).is_some() {
                result.cancelled_order_ids.push(order_id);
            }
        }

        Ok(result)
    }

    pub fn cancel_limit_order(&mut self, order_id: u64) -> Option<LimitOrder> {
        let (symbol, side, price, removed_qty) = {
            let order = self.limit_orders.get_mut(&order_id)?;
            if order.is_terminal() {
                return Some(order.clone());
            }

            order.status = LimitOrderStatus::Cancelled;
            (
                order.symbol.clone(),
                order.side,
                order.limit_price,
                order.remaining_quantity,
            )
        };

        self.advance_following_orders(&symbol, side, price, order_id, removed_qty);
        self.limit_orders.get(&order_id).cloned()
    }

    pub fn expire_orders(&mut self, now: DateTime<Utc>) -> Vec<u64> {
        let expired_ids = self
            .limit_orders
            .iter()
            .filter(|(_, order)| !order.is_terminal() && order.expires_at <= now)
            .map(|(order_id, _)| *order_id)
            .collect::<Vec<_>>();

        for order_id in &expired_ids {
            self.expire_order(*order_id);
        }

        expired_ids
    }

    pub fn pending_limit_order_count(&self) -> usize {
        self.limit_orders
            .values()
            .filter(|order| !order.is_terminal())
            .count()
    }

    pub fn active_limit_orders(&self, symbol: &str) -> Vec<&LimitOrder> {
        let mut orders = self
            .limit_orders
            .values()
            .filter(|order| order.symbol == symbol && !order.is_terminal())
            .collect::<Vec<_>>();
        orders.sort_by(|lhs, rhs| {
            lhs.created_at
                .cmp(&rhs.created_at)
                .then(lhs.id.cmp(&rhs.id))
        });
        orders
    }

    pub fn order(&self, order_id: u64) -> Option<&LimitOrder> {
        self.limit_orders.get(&order_id)
    }

    pub fn order_book(&self, symbol: &str) -> Option<&OrderBookState> {
        self.order_books.get(symbol)
    }

    pub fn match_limit_orders_at_book(
        &mut self,
        symbol: &str,
        timestamp: DateTime<Utc>,
    ) -> Result<LimitOrderProcessingResult> {
        self.match_active_limit_orders(symbol, timestamp)
    }

    pub fn ledger(&self) -> &BacktestLedger {
        &self.ledger
    }

    pub fn ledger_mut(&mut self) -> &mut BacktestLedger {
        &mut self.ledger
    }

    fn active_order_ids_for_symbol(&self, symbol: &str) -> Vec<u64> {
        let mut order_ids = self
            .limit_orders
            .iter()
            .filter(|(_, order)| order.symbol == symbol && !order.is_terminal())
            .map(|(order_id, _)| *order_id)
            .collect::<Vec<_>>();
        order_ids.sort_by(|lhs, rhs| {
            let left = self
                .limit_orders
                .get(lhs)
                .expect("active order should exist");
            let right = self
                .limit_orders
                .get(rhs)
                .expect("active order should exist");
            left.created_at
                .cmp(&right.created_at)
                .then(left.id.cmp(&right.id))
        });
        order_ids
    }

    fn local_queue_ahead_qty(&self, symbol: &str, side: OrderSide, limit_price: f64) -> f64 {
        self.limit_orders
            .values()
            .filter(|order| {
                !order.is_terminal()
                    && order.symbol == symbol
                    && order.side == side
                    && same_price(order.limit_price, limit_price)
            })
            .map(|order| order.remaining_quantity)
            .sum()
    }

    fn fill_on_kline(
        &mut self,
        order_id: u64,
        timestamp: DateTime<Utc>,
        market_type: MarketType,
        maker_fee_bps: f64,
    ) -> Result<Option<FillResult>> {
        let snapshot = self.limit_orders.get(&order_id).cloned();
        let Some(order) = snapshot else {
            return Ok(None);
        };
        if order.is_terminal() || order.remaining_quantity <= FLOAT_EPSILON {
            return Ok(None);
        }

        let quantity = order.remaining_quantity;
        let fee_paid = quantity * order.limit_price * (maker_fee_bps / 10_000.0);
        let fill = FillResult {
            symbol: order.symbol.clone(),
            side: order.side,
            order_type: OrderType::Limit,
            market_type,
            quantity,
            price: order.limit_price,
            is_maker: true,
            fee_paid,
            timestamp,
        };

        self.ledger.apply_fill(fill.clone())?;
        self.ledger
            .apply_mark_price(&order.symbol, order.limit_price, timestamp);

        if let Some(order) = self.limit_orders.get_mut(&order_id) {
            order.remaining_quantity = 0.0;
            order.status = LimitOrderStatus::Filled;
        }
        self.advance_following_orders(
            &order.symbol,
            order.side,
            order.limit_price,
            order_id,
            quantity,
        );

        Ok(Some(fill))
    }

    fn expire_order(&mut self, order_id: u64) -> Option<()> {
        let (symbol, side, price, removed_qty) = {
            let order = self.limit_orders.get_mut(&order_id)?;
            if order.is_terminal() {
                return None;
            }

            order.status = LimitOrderStatus::Expired;
            (
                order.symbol.clone(),
                order.side,
                order.limit_price,
                order.remaining_quantity,
            )
        };

        self.advance_following_orders(&symbol, side, price, order_id, removed_qty);
        Some(())
    }

    fn match_active_limit_orders(
        &mut self,
        symbol: &str,
        timestamp: DateTime<Utc>,
    ) -> Result<LimitOrderProcessingResult> {
        let mut result = LimitOrderProcessingResult::default();
        let order_ids = self.active_order_ids_for_symbol(symbol);
        for order_id in order_ids {
            let order_result = self.match_single_limit_order(order_id, timestamp)?;
            result
                .filled_order_ids
                .extend(order_result.filled_order_ids);
            result.fills.extend(order_result.fills);
        }
        Ok(result)
    }

    fn match_active_limit_orders_on_trade(
        &mut self,
        symbol: &str,
        aggressor_side: OrderSide,
        trade_price: f64,
        trade_quantity: f64,
        timestamp: DateTime<Utc>,
    ) -> Result<LimitOrderProcessingResult> {
        let mut result = LimitOrderProcessingResult::default();
        if trade_quantity <= FLOAT_EPSILON {
            return Ok(result);
        }

        let compatible_side = opposite_side(aggressor_side);
        let order_ids = self
            .active_order_ids_for_symbol(symbol)
            .into_iter()
            .filter(|order_id| {
                self.limit_orders.get(order_id).is_some_and(|order| {
                    order.side == compatible_side
                        && order.created_at < timestamp
                        && same_price(order.limit_price, trade_price)
                })
            })
            .collect::<Vec<_>>();

        let mut remaining_trade_qty = trade_quantity;
        for order_id in order_ids {
            if remaining_trade_qty <= FLOAT_EPSILON {
                break;
            }

            let Some(snapshot) = self.limit_orders.get(&order_id).cloned() else {
                continue;
            };
            if snapshot.is_terminal() || snapshot.remaining_quantity <= FLOAT_EPSILON {
                continue;
            }

            if snapshot.queue_ahead_qty > FLOAT_EPSILON {
                let queue_depleted = snapshot.queue_ahead_qty.min(remaining_trade_qty);
                self.deplete_queue_ahead(
                    &snapshot.symbol,
                    snapshot.side,
                    snapshot.limit_price,
                    queue_depleted,
                );
                remaining_trade_qty = (remaining_trade_qty - queue_depleted).max(0.0);
            }

            let Some(order) = self.limit_orders.get(&order_id).cloned() else {
                continue;
            };
            if order.queue_ahead_qty > FLOAT_EPSILON || order.remaining_quantity <= FLOAT_EPSILON {
                continue;
            }

            let fill_qty = order.remaining_quantity.min(remaining_trade_qty);
            if fill_qty <= FLOAT_EPSILON {
                continue;
            }

            let fee_paid = fill_qty * order.limit_price * (DEFAULT_MAKER_FEE_BPS / 10_000.0);
            let fill = FillResult {
                symbol: order.symbol.clone(),
                side: order.side,
                order_type: OrderType::Limit,
                market_type: order.market_type,
                quantity: fill_qty,
                price: order.limit_price,
                is_maker: true,
                fee_paid,
                timestamp,
            };
            self.ledger.apply_fill(fill.clone())?;
            self.ledger
                .apply_mark_price(&order.symbol, order.limit_price, timestamp);

            if let Some(order) = self.limit_orders.get_mut(&order_id) {
                order.remaining_quantity = (order.remaining_quantity - fill_qty).max(0.0);
                if order.remaining_quantity <= FLOAT_EPSILON {
                    order.remaining_quantity = 0.0;
                    order.status = LimitOrderStatus::Filled;
                    result.filled_order_ids.push(order_id);
                }
                result.fills.push(LimitOrderFillUpdate {
                    order_id,
                    fill,
                    is_terminal: order.remaining_quantity <= FLOAT_EPSILON,
                });
            }
            self.advance_following_orders(
                &order.symbol,
                order.side,
                order.limit_price,
                order_id,
                fill_qty,
            );
            remaining_trade_qty = (remaining_trade_qty - fill_qty).max(0.0);
        }

        Ok(result)
    }

    fn match_single_limit_order(
        &mut self,
        order_id: u64,
        timestamp: DateTime<Utc>,
    ) -> Result<LimitOrderProcessingResult> {
        let mut result = LimitOrderProcessingResult::default();
        loop {
            let snapshot = self.limit_orders.get(&order_id).cloned();
            let Some(order) = snapshot else {
                return Ok(result);
            };
            if order.is_terminal()
                || order.remaining_quantity <= FLOAT_EPSILON
                || order.queue_ahead_qty > FLOAT_EPSILON
            {
                return Ok(result);
            }

            let next_level =
                self.order_books
                    .get(&order.symbol)
                    .and_then(|book| match order.side {
                        OrderSide::Buy => book
                            .best_ask()
                            .filter(|level| level[0] <= order.limit_price),
                        OrderSide::Sell => book
                            .best_bid()
                            .filter(|level| level[0] >= order.limit_price),
                    });

            let Some([price, available_qty]) = next_level else {
                return Ok(result);
            };
            if available_qty <= FLOAT_EPSILON {
                return Ok(result);
            }

            let fill_qty = order.remaining_quantity.min(available_qty);
            let consumed_qty = {
                let Some(book) = self.order_books.get_mut(&order.symbol) else {
                    return Ok(result);
                };

                match order.side {
                    OrderSide::Buy => book.consume_ask_qty(price, fill_qty),
                    OrderSide::Sell => book.consume_bid_qty(price, fill_qty),
                }
            };
            if consumed_qty <= FLOAT_EPSILON {
                return Ok(result);
            }

            let fee_paid = consumed_qty * price * (DEFAULT_MAKER_FEE_BPS / 10_000.0);
            let fill = FillResult {
                symbol: order.symbol.clone(),
                side: order.side,
                order_type: OrderType::Limit,
                market_type: order.market_type,
                quantity: consumed_qty,
                price,
                is_maker: true,
                fee_paid,
                timestamp,
            };
            self.ledger.apply_fill(fill.clone())?;
            self.ledger
                .apply_mark_price(&order.symbol, price, timestamp);

            if let Some(order) = self.limit_orders.get_mut(&order_id) {
                order.remaining_quantity = (order.remaining_quantity - consumed_qty).max(0.0);
                if order.remaining_quantity <= FLOAT_EPSILON {
                    order.remaining_quantity = 0.0;
                    order.status = LimitOrderStatus::Filled;
                    result.filled_order_ids.push(order_id);
                }
                result.fills.push(LimitOrderFillUpdate {
                    order_id,
                    fill,
                    is_terminal: order.remaining_quantity <= FLOAT_EPSILON,
                });
            }
            self.advance_following_orders(
                &order.symbol,
                order.side,
                order.limit_price,
                order_id,
                consumed_qty,
            );
        }
    }

    fn advance_following_orders(
        &mut self,
        symbol: &str,
        side: OrderSide,
        limit_price: f64,
        leader_id: u64,
        queue_delta: f64,
    ) {
        if queue_delta <= FLOAT_EPSILON {
            return;
        }

        let Some(leader) = self.limit_orders.get(&leader_id).cloned() else {
            return;
        };

        let follower_ids = self
            .limit_orders
            .iter()
            .filter(|(order_id, order)| {
                **order_id != leader_id
                    && !order.is_terminal()
                    && order.symbol == symbol
                    && order.side == side
                    && same_price(order.limit_price, limit_price)
                    && is_later_in_queue(order, &leader)
            })
            .map(|(order_id, _)| *order_id)
            .collect::<Vec<_>>();

        for follower_id in follower_ids {
            if let Some(order) = self.limit_orders.get_mut(&follower_id) {
                order.queue_ahead_qty = (order.queue_ahead_qty - queue_delta).max(0.0);
            }
        }
    }

    fn deplete_queue_ahead(
        &mut self,
        symbol: &str,
        side: OrderSide,
        limit_price: f64,
        queue_delta: f64,
    ) {
        if queue_delta <= FLOAT_EPSILON {
            return;
        }

        let order_ids = self
            .limit_orders
            .iter()
            .filter(|(_, order)| {
                !order.is_terminal()
                    && order.symbol == symbol
                    && order.side == side
                    && same_price(order.limit_price, limit_price)
            })
            .map(|(order_id, _)| *order_id)
            .collect::<Vec<_>>();

        for order_id in order_ids {
            if let Some(order) = self.limit_orders.get_mut(&order_id) {
                order.queue_ahead_qty = (order.queue_ahead_qty - queue_delta).max(0.0);
            }
        }
    }

    fn market_execution_price(
        &mut self,
        symbol: &str,
        side: OrderSide,
        reference_price: f64,
        quantity: f64,
    ) -> f64 {
        if quantity <= FLOAT_EPSILON {
            return reference_price;
        }

        let Some(book) = self.order_books.get_mut(symbol) else {
            return reference_price;
        };

        let mut consumed_levels = match side {
            OrderSide::Buy => book.consume_market_buy_qty(quantity),
            OrderSide::Sell => book.consume_market_sell_qty(quantity),
        };
        if consumed_levels.is_empty() {
            return reference_price;
        }

        let consumed_qty = consumed_levels.iter().map(|level| level[1]).sum::<f64>();
        if consumed_qty + FLOAT_EPSILON < quantity {
            let residual_qty = quantity - consumed_qty;
            let fallback_price = consumed_levels
                .last()
                .map(|level| match side {
                    OrderSide::Buy => level[0].max(reference_price),
                    OrderSide::Sell => level[0].min(reference_price),
                })
                .unwrap_or(reference_price);
            consumed_levels.push([fallback_price, residual_qty]);
        }

        let total_notional = consumed_levels
            .iter()
            .map(|level| level[0] * level[1])
            .sum::<f64>();
        total_notional / quantity
    }
}

fn apply_adverse_market_slippage(price: f64, side: OrderSide, slippage_bps: f64) -> f64 {
    if !price.is_finite() || price <= FLOAT_EPSILON {
        return price;
    }

    let bounded_bps = if slippage_bps.is_finite() {
        slippage_bps.clamp(0.0, 9_999.0)
    } else {
        0.0
    };
    let slip = bounded_bps / 10_000.0;

    match side {
        OrderSide::Buy => price * (1.0 + slip),
        OrderSide::Sell => price * (1.0 - slip),
    }
}

fn same_side_book_qty(book: &OrderBookState, side: OrderSide, price: f64) -> f64 {
    match side {
        OrderSide::Buy => book.bid_qty_at(price),
        OrderSide::Sell => book.ask_qty_at(price),
    }
}

fn opposite_side(side: OrderSide) -> OrderSide {
    match side {
        OrderSide::Buy => OrderSide::Sell,
        OrderSide::Sell => OrderSide::Buy,
    }
}

fn would_cross_book(book: &OrderBookState, side: OrderSide, limit_price: f64) -> bool {
    match side {
        OrderSide::Buy => book
            .best_ask()
            .map(|level| level[0] <= limit_price)
            .unwrap_or(false),
        OrderSide::Sell => book
            .best_bid()
            .map(|level| level[0] >= limit_price)
            .unwrap_or(false),
    }
}

fn is_later_in_queue(candidate: &LimitOrder, leader: &LimitOrder) -> bool {
    candidate.created_at > leader.created_at
        || (candidate.created_at == leader.created_at && candidate.id > leader.id)
}

fn limit_order_touched(order: &LimitOrder, kline: &Kline) -> bool {
    match order.side {
        OrderSide::Buy => kline.low <= order.limit_price,
        OrderSide::Sell => kline.high >= order.limit_price,
    }
}

fn same_price(lhs: f64, rhs: f64) -> bool {
    (lhs - rhs).abs() <= FLOAT_EPSILON
}
