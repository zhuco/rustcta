use super::config::{ExecutionConfig, StrategyConfig, TradingConfig, TrendAdaptiveMMConfig};
use super::risk::{RiskAction, RiskDecision, RiskEngine};
use super::signal::{SignalEngine, SignalOutput};
use super::state::{
    ceil_quantity, precision_from_step, round_price_for_side, round_quantity, ActiveOrder,
    StrategyState, SymbolMetadata,
};
use crate::core::error::ExchangeError;
use crate::core::types::{
    MarketType, Order, OrderRequest, OrderSide, OrderStatus, OrderType, Position, Ticker, Trade,
};
use crate::cta::{account_manager::AccountInfo, AccountManager};
use crate::strategies::Strategy;
use crate::utils::order_id::OrderIdGenerator;
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use log::{debug, info, warn};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

type StrategyResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug, Clone)]
struct DesiredOrder {
    side: OrderSide,
    price: f64,
    quantity: f64,
    level: usize,
}

#[derive(Debug, Default)]
struct QuotePlan {
    orders: Vec<DesiredOrder>,
    spread_bps: f64,
}

#[derive(Debug, Clone)]
struct PlacementContext {
    buy_penalty: u32,
    sell_penalty: u32,
    last_reject_buy: Option<DateTime<Utc>>,
    last_reject_sell: Option<DateTime<Utc>>,
    last_place_buy: Option<DateTime<Utc>>,
    last_place_sell: Option<DateTime<Utc>>,
}

fn is_post_only_reject(msg: &str) -> bool {
    let lower = msg.to_ascii_lowercase();
    lower.contains("post only")
        || msg.contains("-5022")
        || msg.contains("-5021")
        || msg.contains("POST_ONLY_REJECT")
}

pub struct TrendAdaptiveMMStrategy {
    config: TrendAdaptiveMMConfig,
    market_type: MarketType,
    metadata: SymbolMetadata,
    account: Arc<AccountInfo>,
    signal_engine: Arc<Mutex<SignalEngine>>,
    risk_engine: Arc<Mutex<RiskEngine>>,
    state: Arc<RwLock<StrategyState>>,
    order_id_gen: OrderIdGenerator,
    sanitized_symbol: String,
}

impl TrendAdaptiveMMStrategy {
    pub async fn new(
        config: TrendAdaptiveMMConfig,
        account_manager: Arc<AccountManager>,
    ) -> StrategyResult<Self> {
        let account = account_manager
            .get_account(&config.strategy.account_id)
            .ok_or_else(|| format!("account {} not found", config.strategy.account_id))?;

        let exchange = account.exchange.clone();
        let market_type = parse_market_type(&config.trading.market_type);

        let mut symbol_candidates = vec![config.trading.symbol.clone()];
        if config.trading.symbol.contains('/') {
            symbol_candidates.push(config.trading.symbol.replace('/', ""));
        }

        let mut trading_pair_opt = None;

        for candidate in symbol_candidates {
            match exchange.get_symbol_info(&candidate, market_type).await {
                Ok(info) => {
                    trading_pair_opt = Some(info);
                    break;
                }
                Err(e) => {
                    debug!("symbol lookup {} failed: {}", candidate, e);
                    continue;
                }
            }
        }

        let pair = trading_pair_opt
            .ok_or_else(|| {
                ExchangeError::Other(format!(
                    "unable to load trading pair info for {}",
                    config.trading.symbol
                ))
            })
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let api_symbol = pair.symbol.clone();
        let display_symbol = if config.trading.symbol.contains('/') {
            config.trading.symbol.clone()
        } else {
            format!("{}/{}", pair.base_asset, pair.quote_asset)
        };

        let min_notional = config
            .trading
            .min_notional_override
            .or(pair.min_notional)
            .unwrap_or(0.0);

        let metadata = SymbolMetadata {
            display_symbol,
            api_symbol: api_symbol.clone(),
            base_asset: pair.base_asset.clone(),
            quote_asset: pair.quote_asset.clone(),
            tick_size: pair.tick_size.max(1e-8),
            step_size: pair.step_size.max(1e-8),
            min_order_size: pair.min_order_size.max(0.0),
            min_notional,
            price_precision: precision_from_step(pair.tick_size),
            quantity_precision: precision_from_step(pair.step_size),
        };

        let signal_engine = SignalEngine::new(config.signal.clone());
        let risk_engine = RiskEngine::new(config.risk.clone(), config.trading.clone());

        let order_id_gen = OrderIdGenerator::new(&config.strategy.name, &account.exchange_name);

        info!(
            "TrendAdaptiveMM {} initialized on {} (api symbol: {})",
            config.strategy.name, metadata.display_symbol, metadata.api_symbol
        );

        let strategy = Self {
            sanitized_symbol: sanitize_symbol(&api_symbol),
            config,
            market_type,
            metadata,
            account,
            signal_engine: Arc::new(Mutex::new(signal_engine)),
            risk_engine: Arc::new(Mutex::new(risk_engine)),
            state: Arc::new(RwLock::new(StrategyState::default())),
            order_id_gen,
        };

        strategy.sync_inventory_from_exchange().await?;

        Ok(strategy)
    }

    pub fn market_type(&self) -> MarketType {
        self.market_type
    }

    pub fn metadata(&self) -> &SymbolMetadata {
        &self.metadata
    }

    pub fn trading_config(&self) -> &TradingConfig {
        &self.config.trading
    }

    pub fn execution_config(&self) -> &ExecutionConfig {
        &self.config.execution
    }

    pub fn account(&self) -> Arc<AccountInfo> {
        self.account.clone()
    }

    pub fn sanitized_symbol(&self) -> &str {
        &self.sanitized_symbol
    }

    pub async fn active_orders_snapshot(&self) -> Vec<ActiveOrder> {
        let state = self.state.read().await;
        state
            .active_buy
            .values()
            .chain(state.active_sell.values())
            .cloned()
            .collect()
    }

    pub async fn sync_inventory_from_exchange(&self) -> StrategyResult<()> {
        let inventory = match self.market_type {
            MarketType::Spot => self.fetch_spot_inventory().await?,
            MarketType::Futures => self.fetch_futures_inventory().await?,
        };

        let mut state = self.state.write().await;
        state.update_inventory(inventory);
        debug!("synced inventory from exchange: {:.6}", inventory);
        Ok(())
    }

    async fn fetch_spot_inventory(&self) -> StrategyResult<f64> {
        let balances = self
            .account
            .exchange
            .get_balance(MarketType::Spot)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let base_upper = self.metadata.base_asset.to_ascii_uppercase();
        let amount = balances
            .iter()
            .find(|b| b.currency.to_ascii_uppercase() == base_upper)
            .map(|b| b.total)
            .unwrap_or(0.0);

        Ok(amount)
    }

    async fn fetch_futures_inventory(&self) -> StrategyResult<f64> {
        let positions = self
            .account
            .exchange
            .get_positions(Some(&self.metadata.api_symbol))
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let mut net = 0.0;
        let symbol_key = self.sanitized_symbol();

        for position in positions {
            if sanitize_symbol(&position.symbol) != symbol_key {
                continue;
            }

            let size = extract_position_size(&position);
            let signed = match position.side.to_ascii_lowercase().as_str() {
                "long" | "buy" => size.abs(),
                "short" | "sell" => -size.abs(),
                _ => size,
            };
            net += signed;
        }

        Ok(net)
    }

    pub async fn start(&self) -> StrategyResult<()> {
        info!(
            "TrendAdaptiveMM {} starting on {}",
            self.config.strategy.name, self.metadata.display_symbol
        );
        self.sync_inventory_from_exchange().await?;
        Ok(())
    }

    pub async fn stop(&self) -> StrategyResult<()> {
        info!(
            "TrendAdaptiveMM {} stopping; cancelling resting orders",
            self.config.strategy.name
        );
        self.cancel_all_orders().await;
        Ok(())
    }

    fn symbol_matches(&self, symbol: &str) -> bool {
        sanitize_symbol(symbol) == self.sanitized_symbol
    }

    fn base_config(&self) -> &StrategyConfig {
        &self.config.strategy
    }

    fn build_quote_plan(
        &self,
        signal: &SignalOutput,
        decision: &RiskDecision,
        inventory: f64,
        ctx: &PlacementContext,
    ) -> QuotePlan {
        let mut plan = QuotePlan::default();

        let mid = signal.mid_price;
        if mid <= 0.0 {
            return plan;
        }

        let base_spread_bps = self.config.trading.base_spread_bps;
        let vol_ratio = if self.config.trading.reference_volatility <= 0.0 {
            1.0
        } else {
            (signal.volatility / self.config.trading.reference_volatility).max(0.0)
        };

        let spread_multiplier = decision.spread_multiplier
            * (1.0 + self.config.trading.volatility_spread_k * (vol_ratio - 1.0).max(0.0));

        let mut spread_bps = base_spread_bps * spread_multiplier;
        spread_bps = spread_bps
            .max(self.config.trading.min_spread_bps)
            .min(self.config.trading.max_spread_bps);
        plan.spread_bps = spread_bps;

        let spread_price = mid * spread_bps / 10_000.0;
        let base_qty =
            (self.config.trading.base_order_size * decision.size_multiplier.max(0.0)).max(0.0);

        if base_qty <= 0.0 {
            return plan;
        }

        let max_inventory = self.config.trading.max_inventory.max(1e-6);
        let target_bias = decision.target_inventory_bias;
        let positive_gap = (target_bias - inventory).max(0.0) / max_inventory;
        let negative_gap = (inventory - target_bias).max(0.0) / max_inventory;

        let skew_bps = self.config.trading.skew_bps.min(spread_bps / 1.8);
        let skew_offset = (signal.alpha.clamp(-1.0, 1.0) * skew_bps / 10_000.0) * mid;

        let tick = self.metadata.tick_size;

        let mut bid_price = round_price_for_side(
            mid - spread_price / 2.0 + skew_offset,
            tick,
            OrderSide::Buy,
            self.metadata.price_precision,
        );
        let mut ask_price = round_price_for_side(
            mid + spread_price / 2.0 - skew_offset,
            tick,
            OrderSide::Sell,
            self.metadata.price_precision,
        );

        if tick > 0.0 {
            if ctx.buy_penalty > 0 {
                let adjusted = (bid_price - tick * ctx.buy_penalty as f64).max(tick);
                bid_price = round_price_for_side(
                    adjusted,
                    tick,
                    OrderSide::Buy,
                    self.metadata.price_precision,
                );
            }

            if ctx.sell_penalty > 0 {
                let adjusted = ask_price + tick * ctx.sell_penalty as f64;
                ask_price = round_price_for_side(
                    adjusted,
                    tick,
                    OrderSide::Sell,
                    self.metadata.price_precision,
                );
            }
        }

        if ask_price <= bid_price {
            ask_price = bid_price + tick;
            ask_price = round_price_for_side(
                ask_price,
                tick,
                OrderSide::Sell,
                self.metadata.price_precision,
            );
        }

        let mut bid_qty = base_qty * (1.0 + positive_gap.min(1.5));
        let mut ask_qty = base_qty * (1.0 + negative_gap.min(1.5));

        bid_qty = self.adjust_quantity(bid_price, bid_qty);
        ask_qty = self.adjust_quantity(ask_price, ask_qty);

        if bid_qty > 0.0 {
            plan.bid = Some(DesiredOrder {
                side: OrderSide::Buy,
                price: bid_price,
                quantity: bid_qty,
            });
        }

        if ask_qty > 0.0 {
            plan.ask = Some(DesiredOrder {
                side: OrderSide::Sell,
                price: ask_price,
                quantity: ask_qty,
            });
        }

        if matches!(decision.action, RiskAction::Hedge) {
            if inventory > 0.0 {
                plan.bid = None;
            } else if inventory < 0.0 {
                plan.ask = None;
            }
        }

        plan
    }

    fn adjust_quantity(&self, price: f64, quantity: f64) -> f64 {
        if quantity <= 0.0 {
            return 0.0;
        }

        let mut adjusted = quantity;

        if self.metadata.min_order_size > 0.0 {
            adjusted = adjusted.max(self.metadata.min_order_size);
        }

        adjusted = round_quantity(
            adjusted,
            self.metadata.step_size,
            self.metadata.quantity_precision,
        );
        if adjusted <= 0.0 {
            adjusted = self.metadata.step_size;
        }

        if self.metadata.min_notional() > 0.0 {
            let notional = price * adjusted;
            if notional < self.metadata.min_notional() {
                let required = self.metadata.min_notional() / price;
                adjusted = ceil_quantity(
                    required,
                    self.metadata.step_size,
                    self.metadata.quantity_precision,
                );
            }
        }

        round_quantity(
            adjusted,
            self.metadata.step_size,
            self.metadata.quantity_precision,
        )
    }

    async fn apply_quote_plan(
        &self,
        plan: QuotePlan,
        decision: &RiskDecision,
        ctx: &PlacementContext,
    ) {
        let exchange = self.account.exchange.clone();
        let mut cancels: Vec<(OrderSide, String)> = Vec::new();
        let mut placements: Vec<DesiredOrder> = Vec::new();
        let now = Utc::now();
        let cooldown = if self.config.execution.order_refresh_cooldown_ms > 0 {
            Some(Duration::milliseconds(
                self.config.execution.order_refresh_cooldown_ms as i64,
            ))
        } else {
            None
        };

        {
            let state = self.state.read().await;
            let price_tol =
                self.metadata.tick_size * (self.config.execution.maker_offset_ticks.max(1) as f64);
            let qty_tol = self.metadata.step_size.max(1e-8);

            let existing_buy = state.active_buy.clone();
            let existing_sell = state.active_sell.clone();

            match (plan.bid.as_ref(), existing_buy.as_ref()) {
                (Some(desired), Some(active)) => {
                    let needs_update = !approx_equal(active.price, desired.price, price_tol)
                        || !approx_equal(active.quantity, desired.quantity, qty_tol);
                    if needs_update {
                        let within_cooldown = cooldown.map_or(false, |cd| {
                            ctx.last_place_buy
                                .map(|ts| now.signed_duration_since(ts) < cd)
                                .unwrap_or(false)
                        });

                        if within_cooldown {
                            // keep existing order until冷却结束
                        } else {
                            cancels.push((OrderSide::Buy, active.order_id.clone()));
                            placements.push(desired.clone());
                        }
                    }
                }
                (Some(desired), None) => placements.push(desired.clone()),
                (None, Some(active)) => cancels.push((OrderSide::Buy, active.order_id.clone())),
                (None, None) => {}
            }

            match (plan.ask.as_ref(), existing_sell.as_ref()) {
                (Some(desired), Some(active)) => {
                    let needs_update = !approx_equal(active.price, desired.price, price_tol)
                        || !approx_equal(active.quantity, desired.quantity, qty_tol);
                    if needs_update {
                        let within_cooldown = cooldown.map_or(false, |cd| {
                            ctx.last_place_sell
                                .map(|ts| now.signed_duration_since(ts) < cd)
                                .unwrap_or(false)
                        });

                        if within_cooldown {
                            // skip refresh to避免重复撞价
                        } else {
                            cancels.push((OrderSide::Sell, active.order_id.clone()));
                            placements.push(desired.clone());
                        }
                    }
                }
                (Some(desired), None) => placements.push(desired.clone()),
                (None, Some(active)) => cancels.push((OrderSide::Sell, active.order_id.clone())),
                (None, None) => {}
            }
        }

        for (side, order_id) in cancels {
            match exchange
                .cancel_order(&order_id, &self.metadata.api_symbol, self.market_type)
                .await
            {
                Ok(_) => {
                    let mut state = self.state.write().await;
                    match side {
                        OrderSide::Buy => state.active_buy = None,
                        OrderSide::Sell => state.active_sell = None,
                    }
                    debug!("cancelled {:?} order {}", side, order_id);
                }
                Err(e) => {
                    warn!("cancel order {} failed: {}", order_id, e);
                }
            }
        }

        for desired in placements {
            let mut request = OrderRequest::new(
                self.metadata.api_symbol.clone(),
                desired.side,
                OrderType::Limit,
                desired.quantity,
                Some(desired.price),
                self.market_type,
            );
            if self.market_type == MarketType::Spot {
                request.time_in_force = Some("GTX".to_string());
            }
            request.post_only = Some(true);
            request.reduce_only = Some(false);
            request.client_order_id =
                Some(self.order_id_gen.generate_with_tag(match desired.side {
                    OrderSide::Buy => "B",
                    OrderSide::Sell => "S",
                }));

            match exchange.create_order(request).await {
                Ok(order) => {
                    debug!(
                        "placed {:?} order {:?} @ {} x {}",
                        desired.side, order.id, desired.price, desired.quantity
                    );
                    let mut state = self.state.write().await;
                    let record = ActiveOrder::new(
                        order.id.clone(),
                        desired.side,
                        desired.price,
                        desired.quantity,
                    );
                    match desired.side {
                        OrderSide::Buy => {
                            state.active_buy = Some(record);
                            state.last_order_place_buy = Some(now);
                            state.post_only_penalty_buy = 0;
                            state.last_post_only_reject_buy = None;
                        }
                        OrderSide::Sell => {
                            state.active_sell = Some(record);
                            state.last_order_place_sell = Some(now);
                            state.post_only_penalty_sell = 0;
                            state.last_post_only_reject_sell = None;
                        }
                    }
                    state.last_quote_time = Some(Utc::now());
                    state.last_risk_action = Some(
                        match decision.action {
                            RiskAction::Proceed => "proceed",
                            RiskAction::Downgrade => "downgrade",
                            RiskAction::Neutralize => "neutralize",
                            RiskAction::Hedge => "hedge",
                            RiskAction::Stop => "stop",
                        }
                        .to_string(),
                    );
                    state.paused = false;
                }
                Err(e) => {
                    let err_msg = e.to_string();
                    warn!(
                        "failed to place {:?} order price={} qty={}: {}",
                        desired.side, desired.price, desired.quantity, err_msg
                    );
                    if is_post_only_reject(&err_msg) {
                        let mut state = self.state.write().await;
                        let step = self.config.execution.post_only_retry_step.max(1);
                        let max_penalty = self.config.execution.post_only_retry_max.max(step);
                        match desired.side {
                            OrderSide::Buy => {
                                state.post_only_penalty_buy =
                                    (state.post_only_penalty_buy + step).min(max_penalty);
                                state.last_post_only_reject_buy = Some(now);
                            }
                            OrderSide::Sell => {
                                state.post_only_penalty_sell =
                                    (state.post_only_penalty_sell + step).min(max_penalty);
                                state.last_post_only_reject_sell = Some(now);
                            }
                        }
                    }
                }
            }
        }
    }

    async fn cancel_all_orders(&self) {
        let exchange = self.account.exchange.clone();
        let mut existing: Vec<(OrderSide, String)> = Vec::new();
        {
            let state = self.state.read().await;
            if let Some(order) = state.active_buy.as_ref() {
                existing.push((OrderSide::Buy, order.order_id.clone()));
            }
            if let Some(order) = state.active_sell.as_ref() {
                existing.push((OrderSide::Sell, order.order_id.clone()));
            }
        }

        for (side, order_id) in existing {
            match exchange
                .cancel_order(&order_id, &self.metadata.api_symbol, self.market_type)
                .await
            {
                Ok(_) => {
                    let mut state = self.state.write().await;
                    match side {
                        OrderSide::Buy => state.active_buy = None,
                        OrderSide::Sell => state.active_sell = None,
                    }
                    debug!("cancelled {:?} order {}", side, order_id);
                }
                Err(e) => warn!("cancel order {} failed: {}", order_id, e),
            }
        }
    }
}

#[async_trait]
impl Strategy for TrendAdaptiveMMStrategy {
    async fn name(&self) -> String {
        self.base_config().name.clone()
    }

    async fn on_tick(&self, ticker: Ticker) -> StrategyResult<()> {
        if !self.symbol_matches(&ticker.symbol) {
            return Ok(());
        }

        let signal = {
            let mut engine = self.signal_engine.lock().await;
            engine.update(&ticker)
        };

        let now = Utc::now();
        let (snapshot, placement_ctx) = {
            let mut state = self.state.write().await;

            if let Some(ts) = state.last_post_only_reject_buy {
                let elapsed = now.signed_duration_since(ts).num_milliseconds();
                if elapsed >= self.config.execution.post_only_retry_cooldown_ms as i64 {
                    state.post_only_penalty_buy = 0;
                    state.last_post_only_reject_buy = None;
                }
            }

            if let Some(ts) = state.last_post_only_reject_sell {
                let elapsed = now.signed_duration_since(ts).num_milliseconds();
                if elapsed >= self.config.execution.post_only_retry_cooldown_ms as i64 {
                    state.post_only_penalty_sell = 0;
                    state.last_post_only_reject_sell = None;
                }
            }

            let ctx = PlacementContext {
                buy_penalty: state.post_only_penalty_buy,
                sell_penalty: state.post_only_penalty_sell,
                last_reject_buy: state.last_post_only_reject_buy.clone(),
                last_reject_sell: state.last_post_only_reject_sell.clone(),
                last_place_buy: state.last_order_place_buy.clone(),
                last_place_sell: state.last_order_place_sell.clone(),
            };

            (state.snapshot(), ctx)
        };

        let decision = {
            let mut risk = self.risk_engine.lock().await;
            risk.evaluate(&snapshot, &signal)
        };

        match decision.action {
            RiskAction::Stop => {
                warn!("risk stop triggered, cancelling all orders");
                self.cancel_all_orders().await;
                let mut state = self.state.write().await;
                state.paused = true;
                state.last_alpha = signal.alpha;
                state.last_volatility = signal.volatility;
                state.target_inventory = decision.target_inventory_bias;
                return Ok(());
            }
            RiskAction::Hedge => {
                warn!("inventory hedge triggered, cancelling same-side orders");
            }
            _ => {}
        }

        {
            let mut state = self.state.write().await;
            state.target_inventory = decision.target_inventory_bias;
            state.last_alpha = signal.alpha;
            state.last_volatility = signal.volatility;
        }

        let plan = self.build_quote_plan(&signal, &decision, snapshot.inventory, &placement_ctx);
        self.apply_quote_plan(plan, &decision, &placement_ctx).await;

        Ok(())
    }

    async fn on_order_update(&self, order: Order) -> StrategyResult<()> {
        if !self.symbol_matches(&order.symbol) {
            return Ok(());
        }

        let mut state = self.state.write().await;
        match order.side {
            OrderSide::Buy => {
                let mut fill_delta = 0.0;
                let mut remove = false;
                if let Some(active) = state.active_buy.as_mut() {
                    if active.order_id == order.id {
                        fill_delta = (order.filled - active.filled).max(0.0);
                        active.filled = order.filled;
                        let terminal = order.status == OrderStatus::Closed
                            || order.status == OrderStatus::Canceled
                            || order.status == OrderStatus::Expired
                            || order.status == OrderStatus::Rejected
                            || (order.status == OrderStatus::PartiallyFilled
                                && order.remaining <= 0.0);
                        if terminal {
                            remove = true;
                        }
                    }
                }
                if fill_delta > 0.0 {
                    state.inventory += fill_delta;
                    state.last_inventory_sync = Some(Utc::now());
                }
                if remove {
                    state.active_buy = None;
                }
            }
            OrderSide::Sell => {
                let mut fill_delta = 0.0;
                let mut remove = false;
                if let Some(active) = state.active_sell.as_mut() {
                    if active.order_id == order.id {
                        fill_delta = (order.filled - active.filled).max(0.0);
                        active.filled = order.filled;
                        let terminal = order.status == OrderStatus::Closed
                            || order.status == OrderStatus::Canceled
                            || order.status == OrderStatus::Expired
                            || order.status == OrderStatus::Rejected
                            || (order.status == OrderStatus::PartiallyFilled
                                && order.remaining <= 0.0);
                        if terminal {
                            remove = true;
                        }
                    }
                }
                if fill_delta > 0.0 {
                    state.inventory -= fill_delta;
                    state.last_inventory_sync = Some(Utc::now());
                }
                if remove {
                    state.active_sell = None;
                }
            }
        }

        Ok(())
    }

    async fn on_trade(&self, trade: Trade) -> StrategyResult<()> {
        if !self.symbol_matches(&trade.symbol) {
            return Ok(());
        }

        debug!(
            "trade event: side={:?} qty={:.6} price={:.4}",
            trade.side, trade.amount, trade.price
        );
        Ok(())
    }

    async fn get_status(&self) -> StrategyResult<String> {
        let state = self.state.read().await;
        let order_count =
            state.active_buy.is_some() as usize + state.active_sell.is_some() as usize;
        Ok(format!(
            "T-AMM inventory={:.4}, target={:.4}, alpha={:.3}, vol={:.2}, orders={}",
            state.inventory,
            state.target_inventory,
            state.last_alpha,
            state.last_volatility,
            order_count
        ))
    }
}

fn parse_market_type(value: &str) -> MarketType {
    match value.to_ascii_lowercase().as_str() {
        "spot" => MarketType::Spot,
        "futures" | "perpetual" | "swap" => MarketType::Futures,
        _ => MarketType::Spot,
    }
}

fn sanitize_symbol(symbol: &str) -> String {
    symbol.replace('/', "").to_ascii_uppercase()
}

fn extract_position_size(position: &Position) -> f64 {
    if position.size != 0.0 {
        position.size
    } else if position.contracts != 0.0 {
        position.contracts
    } else if position.amount != 0.0 {
        position.amount
    } else {
        0.0
    }
}

fn approx_equal(a: f64, b: f64, tolerance: f64) -> bool {
    (a - b).abs() <= tolerance.max(1e-9)
}
