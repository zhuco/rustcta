use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::core::error::ExchangeError;
use crate::exchanges::client_order_id::{generate_client_order_id, validate_client_order_id};
use crate::exchanges::unified::{
    validate_order_lookup_id, validate_orderbook_depth, AssetBalance, BalanceSnapshot,
    CancelOrderRequest, CancelOrderResponse, ExchangeClient, ExchangeClientCapabilities,
    ExchangeClientError, ExchangeClientResult, FeeRate, LiquidityRole, MarketType, OrderBookLevel,
    OrderBookSnapshot, OrderRequest, OrderResponse, OrderSide, OrderStatus, OrderType,
    PositionSide, TradeFill, UserStreamEvent,
};

const DEFAULT_STALE_BOOK_AFTER_MS: i64 = 10_000;
const DEFAULT_MAKER_FEE_RATE: f64 = 0.0002;
const DEFAULT_TAKER_FEE_RATE: f64 = 0.0006;
const FLOAT_EPSILON: f64 = 1e-9;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperExchangeConfig {
    #[serde(default = "default_exchange_name")]
    pub exchange_name: String,
    #[serde(default = "default_market_type")]
    pub market_type: MarketType,
    #[serde(default = "default_quote_asset")]
    pub quote_asset: String,
    #[serde(default = "default_maker_fee_rate")]
    pub maker_fee_rate: f64,
    #[serde(default = "default_taker_fee_rate")]
    pub taker_fee_rate: f64,
    #[serde(default)]
    pub slippage_bps: f64,
    #[serde(default = "default_stale_book_after_ms")]
    pub stale_book_after_ms: i64,
    #[serde(default)]
    pub initial_balances: Vec<AssetBalance>,
}

impl Default for PaperExchangeConfig {
    fn default() -> Self {
        Self {
            exchange_name: default_exchange_name(),
            market_type: default_market_type(),
            quote_asset: default_quote_asset(),
            maker_fee_rate: DEFAULT_MAKER_FEE_RATE,
            taker_fee_rate: DEFAULT_TAKER_FEE_RATE,
            slippage_bps: 0.0,
            stale_book_after_ms: DEFAULT_STALE_BOOK_AFTER_MS,
            initial_balances: Vec::new(),
        }
    }
}

#[derive(Clone)]
pub struct PaperExchangeClient {
    exchange_name: String,
    state: Arc<Mutex<PaperExchangeState>>,
}

impl PaperExchangeClient {
    pub fn new(config: PaperExchangeConfig) -> Self {
        let exchange_name = config.exchange_name.clone();
        Self {
            exchange_name,
            state: Arc::new(Mutex::new(PaperExchangeState::new(config))),
        }
    }

    pub fn with_balances(config: PaperExchangeConfig, balances: Vec<AssetBalance>) -> Self {
        let mut config = config;
        config.initial_balances = balances;
        Self::new(config)
    }

    pub fn update_orderbook(
        &self,
        book: OrderBookSnapshot,
    ) -> ExchangeClientResult<Vec<TradeFill>> {
        let mut state = self.lock_state()?;
        state.upsert_orderbook(book)?;
        let fills = state.process_resting_orders(Utc::now())?;
        Ok(fills)
    }

    pub fn recorded_orders(&self) -> ExchangeClientResult<Vec<OrderResponse>> {
        Ok(self.lock_state()?.order_journal.clone())
    }

    pub fn recorded_fills(&self) -> ExchangeClientResult<Vec<TradeFill>> {
        Ok(self.lock_state()?.fill_journal.clone())
    }

    pub fn set_balance(&self, asset: impl Into<String>, total: f64) -> ExchangeClientResult<()> {
        let mut state = self.lock_state()?;
        let asset = asset.into().to_ascii_uppercase();
        state
            .balances
            .insert(asset.clone(), AssetBalance::new(asset, total, total, 0.0));
        Ok(())
    }

    fn lock_state(&self) -> ExchangeClientResult<std::sync::MutexGuard<'_, PaperExchangeState>> {
        self.state.lock().map_err(|_| {
            ExchangeError::Other("paper exchange state lock poisoned".to_string()).into()
        })
    }
}

#[async_trait]
impl ExchangeClient for PaperExchangeClient {
    fn market_type(&self) -> MarketType {
        self.state
            .lock()
            .map(|state| state.config.market_type)
            .unwrap_or(MarketType::Spot)
    }

    fn exchange_name(&self) -> &str {
        &self.exchange_name
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let mut capabilities = ExchangeClientCapabilities::spot(self.exchange_name());
        capabilities.supports_private_user_stream = true;
        capabilities
    }

    fn normalize_symbol(&self, symbol: &str) -> ExchangeClientResult<String> {
        normalize_symbol(symbol)
    }

    async fn get_balances(&self) -> ExchangeClientResult<BalanceSnapshot> {
        let state = self.lock_state()?;
        Ok(state.balance_snapshot())
    }

    async fn get_orderbook(
        &self,
        symbol: &str,
        depth: u16,
    ) -> ExchangeClientResult<OrderBookSnapshot> {
        let symbol = normalize_symbol(symbol)?;
        validate_orderbook_depth(depth)?;
        let state = self.lock_state()?;
        let mut book = state
            .books
            .get(&symbol)
            .cloned()
            .ok_or_else(|| order_error(format!("paper order book missing for {symbol}")))?;
        book.bids.truncate(depth as usize);
        book.asks.truncate(depth as usize);
        Ok(book)
    }

    async fn place_order(&self, mut request: OrderRequest) -> ExchangeClientResult<OrderResponse> {
        request.validate()?;
        let mut state = self.lock_state()?;
        if request.market_type != state.config.market_type {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: format!(
                    "paper client market {:?} does not match order market {:?}",
                    state.config.market_type, request.market_type
                ),
            });
        }
        ensure_client_order_id(&mut request)?;
        state.place_order(request, Utc::now())
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeClientResult<CancelOrderResponse> {
        request.validate()?;
        let mut state = self.lock_state()?;
        if request.market_type != state.config.market_type {
            return Err(ExchangeClientError::Validation {
                field: "market_type",
                reason: format!(
                    "paper client market {:?} does not match cancel market {:?}",
                    state.config.market_type, request.market_type
                ),
            });
        }
        validate_cancel_client_order_id(&request)?;
        state.cancel_order(request, Utc::now())
    }

    async fn get_order(&self, symbol: &str, order_id: &str) -> ExchangeClientResult<OrderResponse> {
        let state = self.lock_state()?;
        let symbol = normalize_symbol(symbol)?;
        let order_id = validate_order_lookup_id(order_id)?;
        let order = state
            .orders
            .get(order_id)
            .ok_or_else(|| order_error(format!("paper order {order_id} not found")))?;
        if order.response.symbol != symbol {
            return Err(order_error(format!(
                "paper order {order_id} belongs to {}, not {symbol}",
                order.response.symbol
            )));
        }
        Ok(order.response.clone())
    }

    async fn get_open_orders(
        &self,
        symbol: Option<&str>,
    ) -> ExchangeClientResult<Vec<OrderResponse>> {
        let state = self.lock_state()?;
        let symbol = symbol.map(normalize_symbol).transpose()?;
        Ok(state
            .orders
            .values()
            .filter(|order| order.active)
            .filter(|order| {
                symbol
                    .as_ref()
                    .map(|symbol| &order.response.symbol == symbol)
                    .unwrap_or(true)
            })
            .map(|order| order.response.clone())
            .collect())
    }

    async fn get_fee_rate(&self, _symbol: &str) -> ExchangeClientResult<FeeRate> {
        self.normalize_symbol(_symbol)?;
        let state = self.lock_state()?;
        Ok(FeeRate::new(
            state.config.maker_fee_rate,
            state.config.taker_fee_rate,
            crate::exchanges::unified::FeeRateSource::ConfigOverride,
        ))
    }

    async fn subscribe_user_stream(&self) -> ExchangeClientResult<mpsc::Receiver<UserStreamEvent>> {
        let (tx, rx) = mpsc::channel(1024);
        self.lock_state()?.user_streams.push(tx);
        Ok(rx)
    }

    async fn subscribe_orderbook(
        &self,
        symbols: Vec<String>,
    ) -> ExchangeClientResult<mpsc::Receiver<OrderBookSnapshot>> {
        if symbols.is_empty() {
            return Err(ExchangeClientError::Validation {
                field: "symbols",
                reason: "at least one symbol is required".to_string(),
            });
        }
        let normalized_symbols = symbols
            .into_iter()
            .map(|symbol| normalize_symbol(&symbol))
            .collect::<ExchangeClientResult<Vec<_>>>()?;
        let (tx, rx) = mpsc::channel(1024);
        self.lock_state()?
            .orderbook_streams
            .push(OrderBookSubscriber {
                symbols: normalized_symbols,
                sender: tx,
            });
        Ok(rx)
    }
}

#[derive(Debug)]
struct PaperExchangeState {
    config: PaperExchangeConfig,
    balances: HashMap<String, AssetBalance>,
    books: HashMap<String, OrderBookSnapshot>,
    orders: HashMap<String, PaperOrder>,
    order_journal: Vec<OrderResponse>,
    fill_journal: Vec<TradeFill>,
    user_streams: Vec<mpsc::Sender<UserStreamEvent>>,
    orderbook_streams: Vec<OrderBookSubscriber>,
    next_order_id: u64,
    next_trade_id: u64,
}

impl PaperExchangeState {
    fn new(config: PaperExchangeConfig) -> Self {
        let mut balances = HashMap::new();
        for balance in &config.initial_balances {
            balances.insert(balance.asset.to_ascii_uppercase(), balance.clone());
        }
        Self {
            config,
            balances,
            books: HashMap::new(),
            orders: HashMap::new(),
            order_journal: Vec::new(),
            fill_journal: Vec::new(),
            user_streams: Vec::new(),
            orderbook_streams: Vec::new(),
            next_order_id: 1,
            next_trade_id: 1,
        }
    }

    fn balance_snapshot(&self) -> BalanceSnapshot {
        BalanceSnapshot {
            exchange: self.config.exchange_name.clone(),
            market_type: self.config.market_type,
            balances: self.balances.values().cloned().collect(),
            timestamp: Utc::now(),
        }
    }

    fn upsert_orderbook(&mut self, mut book: OrderBookSnapshot) -> ExchangeClientResult<()> {
        book.symbol = normalize_symbol(&book.symbol)?;
        book.bids
            .sort_by(|left, right| right.price.total_cmp(&left.price));
        book.asks
            .sort_by(|left, right| left.price.total_cmp(&right.price));
        self.books.insert(book.symbol.clone(), book.clone());
        self.publish_orderbook(book);
        Ok(())
    }

    fn place_order(
        &mut self,
        mut request: OrderRequest,
        now: DateTime<Utc>,
    ) -> ExchangeClientResult<OrderResponse> {
        request.symbol = normalize_symbol(&request.symbol)?;
        let order_id = self.allocate_order_id();
        let mut order = PaperOrder::new(
            &self.config.exchange_name,
            self.config.market_type,
            order_id,
            request.clone(),
            now,
        );

        let result = match request.order_type {
            OrderType::Market => self.execute_market_order(&request, &mut order, now),
            OrderType::Limit => self.execute_limit_order(&request, &mut order, now),
            OrderType::PostOnly => self.execute_post_only_order(&request, &mut order, now),
            OrderType::IOC => self.execute_ioc_order(&request, &mut order, now),
            OrderType::FOK => self.execute_fok_order(&request, &mut order, now),
        };

        if let Err(error) = result {
            order.response.status = OrderStatus::Rejected;
            order.active = false;
            order.reject_reason = Some(error.to_string());
        }

        let response = order.response.clone();
        self.record_order(order);
        Ok(response)
    }

    fn execute_market_order(
        &mut self,
        request: &OrderRequest,
        order: &mut PaperOrder,
        now: DateTime<Utc>,
    ) -> ExchangeClientResult<()> {
        let plan = {
            let book = self.fresh_book(&request.symbol, now)?;
            taker_fill_plan(
                book,
                request.side,
                request.quantity,
                None,
                self.config.slippage_bps,
                FillRequirement::Full,
            )?
        };
        if plan.filled_quantity + FLOAT_EPSILON < request.quantity {
            return Err(order_error(
                "insufficient order book depth for market order",
            ));
        }
        self.ensure_taker_balance(request.side, &request.symbol, &plan)?;
        self.consume_book_levels(&request.symbol, request.side, None, plan.filled_quantity);
        self.apply_taker_fill(order, &plan, now)
    }

    fn execute_limit_order(
        &mut self,
        request: &OrderRequest,
        order: &mut PaperOrder,
        now: DateTime<Utc>,
    ) -> ExchangeClientResult<()> {
        let limit_price = request
            .price
            .ok_or_else(|| ExchangeClientError::Validation {
                field: "price",
                reason: "limit order requires price".to_string(),
            })?;
        let crosses = {
            let book = self.fresh_book(&request.symbol, now)?;
            crosses_book(book, request.side, limit_price)
        };
        if crosses {
            let plan = {
                let book = self.fresh_book(&request.symbol, now)?;
                taker_fill_plan(
                    book,
                    request.side,
                    request.quantity,
                    Some(limit_price),
                    0.0,
                    FillRequirement::Partial,
                )?
            };
            self.ensure_limit_balance(request, &plan, request.quantity - plan.filled_quantity)?;
            if plan.filled_quantity > 0.0 {
                self.consume_book_levels(
                    &request.symbol,
                    request.side,
                    Some(limit_price),
                    plan.filled_quantity,
                );
                self.apply_taker_fill(order, &plan, now)?;
            }
            let remaining = (request.quantity - plan.filled_quantity).max(0.0);
            if remaining > FLOAT_EPSILON {
                self.reserve_resting_order(
                    order,
                    request.side,
                    &request.symbol,
                    limit_price,
                    remaining,
                )?;
                order.active = true;
                order.remaining_quantity = remaining;
                order.response.status = OrderStatus::PartiallyFilled;
            }
            Ok(())
        } else {
            self.ensure_limit_balance(request, &FillPlan::default(), request.quantity)?;
            self.reserve_resting_order(
                order,
                request.side,
                &request.symbol,
                limit_price,
                request.quantity,
            )?;
            order.active = true;
            order.response.status = OrderStatus::New;
            Ok(())
        }
    }

    fn execute_post_only_order(
        &mut self,
        request: &OrderRequest,
        order: &mut PaperOrder,
        now: DateTime<Utc>,
    ) -> ExchangeClientResult<()> {
        let limit_price = request
            .price
            .ok_or_else(|| ExchangeClientError::Validation {
                field: "price",
                reason: "post-only order requires price".to_string(),
            })?;
        let crosses = {
            let book = self.fresh_book(&request.symbol, now)?;
            crosses_book(book, request.side, limit_price)
        };
        if crosses {
            return Err(order_error(
                "post-only order would cross the paper order book",
            ));
        }
        self.ensure_limit_balance(request, &FillPlan::default(), request.quantity)?;
        self.reserve_resting_order(
            order,
            request.side,
            &request.symbol,
            limit_price,
            request.quantity,
        )?;
        order.active = true;
        order.response.status = OrderStatus::New;
        Ok(())
    }

    fn execute_ioc_order(
        &mut self,
        request: &OrderRequest,
        order: &mut PaperOrder,
        now: DateTime<Utc>,
    ) -> ExchangeClientResult<()> {
        let limit_price = request
            .price
            .ok_or_else(|| ExchangeClientError::Validation {
                field: "price",
                reason: "IOC order requires price".to_string(),
            })?;
        let plan = {
            let book = self.fresh_book(&request.symbol, now)?;
            taker_fill_plan(
                book,
                request.side,
                request.quantity,
                Some(limit_price),
                0.0,
                FillRequirement::Partial,
            )?
        };
        if plan.filled_quantity <= FLOAT_EPSILON {
            order.response.status = OrderStatus::Expired;
            order.active = false;
            return Ok(());
        }
        self.ensure_taker_balance(request.side, &request.symbol, &plan)?;
        self.consume_book_levels(
            &request.symbol,
            request.side,
            Some(limit_price),
            plan.filled_quantity,
        );
        self.apply_taker_fill(order, &plan, now)?;
        if plan.filled_quantity + FLOAT_EPSILON < request.quantity {
            order.response.status = OrderStatus::PartiallyFilled;
            order.active = false;
        }
        Ok(())
    }

    fn execute_fok_order(
        &mut self,
        request: &OrderRequest,
        order: &mut PaperOrder,
        now: DateTime<Utc>,
    ) -> ExchangeClientResult<()> {
        let limit_price = request
            .price
            .ok_or_else(|| ExchangeClientError::Validation {
                field: "price",
                reason: "FOK order requires price".to_string(),
            })?;
        let plan = {
            let book = self.fresh_book(&request.symbol, now)?;
            taker_fill_plan(
                book,
                request.side,
                request.quantity,
                Some(limit_price),
                0.0,
                FillRequirement::Full,
            )?
        };
        if plan.filled_quantity + FLOAT_EPSILON < request.quantity {
            order.response.status = OrderStatus::Expired;
            order.active = false;
            return Ok(());
        }
        self.ensure_taker_balance(request.side, &request.symbol, &plan)?;
        self.consume_book_levels(
            &request.symbol,
            request.side,
            Some(limit_price),
            plan.filled_quantity,
        );
        self.apply_taker_fill(order, &plan, now)
    }

    fn ensure_limit_balance(
        &self,
        request: &OrderRequest,
        immediate_plan: &FillPlan,
        rest_quantity: f64,
    ) -> ExchangeClientResult<()> {
        let (base, quote) = infer_symbol_assets(&request.symbol, &self.config.quote_asset);
        match request.side {
            OrderSide::Buy => {
                let limit_price = request.price.unwrap_or(0.0);
                let immediate_fee = immediate_plan.notional * self.config.taker_fee_rate;
                let reserve = rest_quantity * limit_price * (1.0 + self.config.maker_fee_rate);
                self.ensure_available(&quote, immediate_plan.notional + immediate_fee + reserve)
            }
            OrderSide::Sell => self.ensure_available(&base, request.quantity),
        }
    }

    fn ensure_taker_balance(
        &self,
        side: OrderSide,
        symbol: &str,
        plan: &FillPlan,
    ) -> ExchangeClientResult<()> {
        let (base, quote) = infer_symbol_assets(symbol, &self.config.quote_asset);
        match side {
            OrderSide::Buy => {
                let fee = plan.notional * self.config.taker_fee_rate;
                self.ensure_available(&quote, plan.notional + fee)
            }
            OrderSide::Sell => self.ensure_available(&base, plan.filled_quantity),
        }
    }

    fn ensure_available(&self, asset: &str, amount: f64) -> ExchangeClientResult<()> {
        let available = self
            .balances
            .get(asset)
            .map(|balance| balance.available)
            .unwrap_or(0.0);
        if available + FLOAT_EPSILON < amount {
            Err(order_error(format!(
                "insufficient paper balance for {asset}: required={amount:.8}, available={available:.8}"
            )))
        } else {
            Ok(())
        }
    }

    fn reserve_resting_order(
        &mut self,
        order: &mut PaperOrder,
        side: OrderSide,
        symbol: &str,
        limit_price: f64,
        quantity: f64,
    ) -> ExchangeClientResult<()> {
        let (base, quote) = infer_symbol_assets(symbol, &self.config.quote_asset);
        match side {
            OrderSide::Buy => {
                let reserve = quantity * limit_price * (1.0 + self.config.maker_fee_rate);
                self.lock_balance(&quote, reserve)?;
                order.reserved_quote += reserve;
            }
            OrderSide::Sell => {
                self.lock_balance(&base, quantity)?;
                order.reserved_base += quantity;
            }
        }
        order.remaining_quantity = quantity;
        Ok(())
    }

    fn apply_taker_fill(
        &mut self,
        order: &mut PaperOrder,
        plan: &FillPlan,
        now: DateTime<Utc>,
    ) -> ExchangeClientResult<()> {
        if plan.filled_quantity <= FLOAT_EPSILON {
            return Ok(());
        }
        let fee = plan.notional * self.config.taker_fee_rate;
        let symbol = order.response.symbol.clone();
        let side = order.response.side;
        self.settle_fill(
            order,
            FillSettlement {
                symbol,
                side,
                quantity: plan.filled_quantity,
                price: plan.average_price,
                fee,
                liquidity: LiquidityRole::Taker,
                now,
            },
        )?;
        Ok(())
    }

    fn apply_maker_fill(
        &mut self,
        order_id: &str,
        quantity: f64,
        price: f64,
        now: DateTime<Utc>,
    ) -> ExchangeClientResult<Option<TradeFill>> {
        let mut order = self
            .orders
            .remove(order_id)
            .ok_or_else(|| order_error(format!("paper order {order_id} not found")))?;
        if !order.active || quantity <= FLOAT_EPSILON {
            self.orders.insert(order_id.to_string(), order);
            return Ok(None);
        }

        let fill_quantity = quantity.min(order.remaining_quantity);
        let notional = fill_quantity * price;
        let fee = notional * self.config.maker_fee_rate;
        let symbol = order.response.symbol.clone();
        let side = order.response.side;
        self.consume_book_levels(&symbol, side, Some(price), fill_quantity);
        let fill = self.settle_fill(
            &mut order,
            FillSettlement {
                symbol,
                side,
                quantity: fill_quantity,
                price,
                fee,
                liquidity: LiquidityRole::Maker,
                now,
            },
        )?;

        match order.response.side {
            OrderSide::Buy => {
                order.reserved_quote = (order.reserved_quote - notional - fee).max(0.0)
            }
            OrderSide::Sell => order.reserved_base = (order.reserved_base - fill_quantity).max(0.0),
        }

        if order.remaining_quantity <= FLOAT_EPSILON {
            self.release_order_reserves(&mut order)?;
            order.active = false;
            order.response.status = OrderStatus::Filled;
        } else {
            order.active = true;
            order.response.status = OrderStatus::PartiallyFilled;
        }
        order.response.updated_at = Some(now);
        let response = order.response.clone();
        self.order_journal.push(response.clone());
        self.publish(UserStreamEvent::Order(response));
        self.orders.insert(order_id.to_string(), order);
        Ok(Some(fill))
    }

    fn settle_fill(
        &mut self,
        order: &mut PaperOrder,
        settlement: FillSettlement,
    ) -> ExchangeClientResult<TradeFill> {
        let (base, quote) = infer_symbol_assets(&settlement.symbol, &self.config.quote_asset);
        let notional = settlement.quantity * settlement.price;
        match (settlement.side, settlement.liquidity) {
            (OrderSide::Buy, LiquidityRole::Maker) => {
                self.debit_locked(&quote, notional + settlement.fee)?;
                self.credit_available(&base, settlement.quantity);
            }
            (OrderSide::Buy, _) => {
                self.debit_available(&quote, notional + settlement.fee)?;
                self.credit_available(&base, settlement.quantity);
            }
            (OrderSide::Sell, LiquidityRole::Maker) => {
                self.debit_locked(&base, settlement.quantity)?;
                self.credit_available(&quote, notional - settlement.fee);
            }
            (OrderSide::Sell, _) => {
                self.debit_available(&base, settlement.quantity)?;
                self.credit_available(&quote, notional - settlement.fee);
            }
        }

        order.apply_fill(settlement.quantity, settlement.price, settlement.now);
        let fill = TradeFill {
            exchange: self.config.exchange_name.clone(),
            market_type: self.config.market_type,
            side: settlement.side,
            symbol: settlement.symbol,
            trade_id: Some(self.allocate_trade_id()),
            order_id: Some(order.response.order_id.clone()),
            client_order_id: order.response.client_order_id.clone(),
            price: settlement.price,
            quantity: settlement.quantity,
            fee_asset: Some(quote),
            fee_amount: Some(settlement.fee),
            liquidity: settlement.liquidity,
            timestamp: settlement.now,
        };
        self.fill_journal.push(fill.clone());
        self.publish(UserStreamEvent::Fill(fill.clone()));
        self.publish(UserStreamEvent::Balance(self.balance_snapshot()));

        if order.remaining_quantity <= FLOAT_EPSILON {
            order.remaining_quantity = 0.0;
            order.active = false;
            order.response.status = OrderStatus::Filled;
        } else {
            order.response.status = OrderStatus::PartiallyFilled;
        }
        order.response.updated_at = Some(settlement.now);
        Ok(fill)
    }

    fn process_resting_orders(
        &mut self,
        now: DateTime<Utc>,
    ) -> ExchangeClientResult<Vec<TradeFill>> {
        let active_ids = self
            .orders
            .iter()
            .filter(|(_, order)| order.active)
            .map(|(order_id, _)| order_id.clone())
            .collect::<Vec<_>>();
        let mut fills = Vec::new();

        for order_id in active_ids {
            let Some(order) = self.orders.get(&order_id) else {
                continue;
            };
            let Some(book) = self.books.get(&order.response.symbol) else {
                continue;
            };
            if self.book_is_stale(book, now) {
                continue;
            }
            let Some(limit_price) = order.response.price else {
                continue;
            };
            let fill_quantity = maker_cross_quantity(book, order.response.side, limit_price)
                .min(order.remaining_quantity);
            if fill_quantity <= FLOAT_EPSILON {
                continue;
            }
            if let Some(fill) = self.apply_maker_fill(&order_id, fill_quantity, limit_price, now)? {
                fills.push(fill);
            }
        }
        Ok(fills)
    }

    fn cancel_order(
        &mut self,
        request: CancelOrderRequest,
        now: DateTime<Utc>,
    ) -> ExchangeClientResult<CancelOrderResponse> {
        let lookup = request
            .order_id()
            .map(str::to_string)
            .or_else(|| {
                request.client_order_id().and_then(|client_id| {
                    self.orders
                        .values()
                        .find(|order| order.response.client_order_id.as_deref() == Some(client_id))
                        .map(|order| order.response.order_id.clone())
                })
            })
            .ok_or_else(|| order_error("paper cancel target not found"))?;

        let mut order = self
            .orders
            .remove(&lookup)
            .ok_or_else(|| order_error(format!("paper order {lookup} not found")))?;
        self.release_order_reserves(&mut order)?;
        order.active = false;
        order.response.status = OrderStatus::Cancelled;
        order.response.updated_at = Some(now);
        let response = order.response.clone();
        self.order_journal.push(response.clone());
        self.publish(UserStreamEvent::Order(response.clone()));
        self.orders.insert(lookup, order);
        Ok(CancelOrderResponse {
            exchange: self.config.exchange_name.clone(),
            market_type: self.config.market_type,
            symbol: normalize_symbol(&request.symbol)?,
            order_id: response.order_id.into(),
            client_order_id: response.client_order_id,
            status: OrderStatus::Cancelled,
            cancelled_at: now,
        })
    }

    fn record_order(&mut self, order: PaperOrder) {
        let response = order.response.clone();
        self.order_journal.push(response.clone());
        self.publish(UserStreamEvent::Order(response));
        self.orders.insert(order.response.order_id.clone(), order);
    }

    fn release_order_reserves(&mut self, order: &mut PaperOrder) -> ExchangeClientResult<()> {
        let (base, quote) = infer_symbol_assets(&order.response.symbol, &self.config.quote_asset);
        if order.reserved_quote > FLOAT_EPSILON {
            self.unlock_balance(&quote, order.reserved_quote)?;
            order.reserved_quote = 0.0;
        }
        if order.reserved_base > FLOAT_EPSILON {
            self.unlock_balance(&base, order.reserved_base)?;
            order.reserved_base = 0.0;
        }
        Ok(())
    }

    fn fresh_book(
        &self,
        symbol: &str,
        now: DateTime<Utc>,
    ) -> ExchangeClientResult<&OrderBookSnapshot> {
        let book = self
            .books
            .get(symbol)
            .ok_or_else(|| order_error(format!("paper order book missing for {symbol}")))?;
        if self.book_is_stale(book, now) {
            return Err(order_error(format!(
                "paper order book for {symbol} is stale: received_at={}",
                book.received_at
            )));
        }
        if book.bids.is_empty() || book.asks.is_empty() {
            return Err(order_error(format!(
                "paper order book for {symbol} has empty side"
            )));
        }
        Ok(book)
    }

    fn consume_book_levels(
        &mut self,
        symbol: &str,
        side: OrderSide,
        limit_price: Option<f64>,
        quantity: f64,
    ) {
        let Some(book) = self.books.get_mut(symbol) else {
            return;
        };
        let levels = match side {
            OrderSide::Buy => &mut book.asks,
            OrderSide::Sell => &mut book.bids,
        };
        let mut remaining = quantity;
        for level in levels.iter_mut() {
            if !level_is_executable(level.price, side, limit_price) {
                break;
            }
            let consumed = remaining.min(level.quantity);
            level.quantity = (level.quantity - consumed).max(0.0);
            remaining -= consumed;
            if remaining <= FLOAT_EPSILON {
                break;
            }
        }
        levels.retain(|level| level.quantity > FLOAT_EPSILON);
    }

    fn book_is_stale(&self, book: &OrderBookSnapshot, now: DateTime<Utc>) -> bool {
        now.signed_duration_since(book.received_at)
            > Duration::milliseconds(self.config.stale_book_after_ms)
    }

    fn lock_balance(&mut self, asset: &str, amount: f64) -> ExchangeClientResult<()> {
        self.debit_available(asset, amount)?;
        let balance = self.ensure_balance(asset);
        balance.locked += amount;
        balance.total = balance.available + balance.locked;
        Ok(())
    }

    fn unlock_balance(&mut self, asset: &str, amount: f64) -> ExchangeClientResult<()> {
        self.debit_locked(asset, amount)?;
        let balance = self.ensure_balance(asset);
        balance.available += amount;
        balance.total = balance.available + balance.locked;
        Ok(())
    }

    fn debit_available(&mut self, asset: &str, amount: f64) -> ExchangeClientResult<()> {
        self.ensure_available(asset, amount)?;
        let balance = self.ensure_balance(asset);
        balance.available = (balance.available - amount).max(0.0);
        balance.total = balance.available + balance.locked;
        Ok(())
    }

    fn debit_locked(&mut self, asset: &str, amount: f64) -> ExchangeClientResult<()> {
        let balance = self.ensure_balance(asset);
        if balance.locked + FLOAT_EPSILON < amount {
            return Err(order_error(format!(
                "insufficient locked paper balance for {asset}: required={amount:.8}, locked={:.8}",
                balance.locked
            )));
        }
        balance.locked = (balance.locked - amount).max(0.0);
        balance.total = balance.available + balance.locked;
        Ok(())
    }

    fn credit_available(&mut self, asset: &str, amount: f64) {
        let balance = self.ensure_balance(asset);
        balance.available += amount;
        balance.total = balance.available + balance.locked;
    }

    fn ensure_balance(&mut self, asset: &str) -> &mut AssetBalance {
        let key = asset.to_ascii_uppercase();
        self.balances
            .entry(key.clone())
            .or_insert(AssetBalance::new(key, 0.0, 0.0, 0.0))
    }

    fn allocate_order_id(&mut self) -> String {
        let id = format!("paper-{}", self.next_order_id);
        self.next_order_id += 1;
        id
    }

    fn allocate_trade_id(&mut self) -> String {
        let id = format!("paper-fill-{}", self.next_trade_id);
        self.next_trade_id += 1;
        id
    }

    fn publish(&mut self, event: UserStreamEvent) {
        self.user_streams.retain(|sender| !sender.is_closed());
        for sender in &self.user_streams {
            let _ = sender.try_send(event.clone());
        }
    }

    fn publish_orderbook(&mut self, book: OrderBookSnapshot) {
        self.orderbook_streams
            .retain(|subscriber| !subscriber.sender.is_closed());
        for subscriber in &self.orderbook_streams {
            if subscriber.symbols.is_empty() || subscriber.symbols.contains(&book.symbol) {
                let _ = subscriber.sender.try_send(book.clone());
            }
        }
    }
}

#[derive(Debug)]
struct OrderBookSubscriber {
    symbols: Vec<String>,
    sender: mpsc::Sender<OrderBookSnapshot>,
}

#[derive(Debug, Clone)]
struct PaperOrder {
    response: OrderResponse,
    remaining_quantity: f64,
    reserved_quote: f64,
    reserved_base: f64,
    active: bool,
    reject_reason: Option<String>,
}

#[derive(Debug, Clone)]
struct FillSettlement {
    symbol: String,
    side: OrderSide,
    quantity: f64,
    price: f64,
    fee: f64,
    liquidity: LiquidityRole,
    now: DateTime<Utc>,
}

impl PaperOrder {
    fn new(
        exchange: &str,
        market_type: MarketType,
        order_id: String,
        request: OrderRequest,
        now: DateTime<Utc>,
    ) -> Self {
        Self {
            remaining_quantity: request.quantity,
            reserved_quote: 0.0,
            reserved_base: 0.0,
            active: false,
            reject_reason: None,
            response: OrderResponse {
                exchange: exchange.to_string(),
                market_type,
                symbol: request.symbol,
                order_id,
                client_order_id: request.client_order_id,
                side: request.side,
                position_side: PositionSide::None,
                order_type: request.order_type,
                status: OrderStatus::New,
                price: request.price,
                quantity: request.quantity,
                filled_quantity: 0.0,
                average_price: None,
                created_at: now,
                updated_at: None,
            },
        }
    }

    fn apply_fill(&mut self, quantity: f64, price: f64, now: DateTime<Utc>) {
        let previous_filled = self.response.filled_quantity;
        let new_filled = previous_filled + quantity;
        let previous_notional = self.response.average_price.unwrap_or(0.0) * previous_filled;
        self.response.average_price = Some((previous_notional + quantity * price) / new_filled);
        self.response.filled_quantity = new_filled;
        self.remaining_quantity = (self.response.quantity - new_filled).max(0.0);
        self.response.updated_at = Some(now);
    }
}

#[derive(Debug, Clone, Copy)]
enum FillRequirement {
    Full,
    Partial,
}

#[derive(Debug, Clone, Default)]
struct FillPlan {
    filled_quantity: f64,
    average_price: f64,
    notional: f64,
}

fn taker_fill_plan(
    book: &OrderBookSnapshot,
    side: OrderSide,
    quantity: f64,
    limit_price: Option<f64>,
    slippage_bps: f64,
    requirement: FillRequirement,
) -> ExchangeClientResult<FillPlan> {
    let levels = match side {
        OrderSide::Buy => &book.asks,
        OrderSide::Sell => &book.bids,
    };
    let mut remaining = quantity;
    let mut raw_notional = 0.0;
    let mut filled = 0.0;

    for level in levels {
        if !level_is_executable(level.price, side, limit_price) {
            break;
        }
        let fill_qty = remaining.min(level.quantity);
        raw_notional += fill_qty * level.price;
        filled += fill_qty;
        remaining -= fill_qty;
        if remaining <= FLOAT_EPSILON {
            break;
        }
    }

    if matches!(requirement, FillRequirement::Full) && filled + FLOAT_EPSILON < quantity {
        return Ok(FillPlan::default());
    }
    if filled <= FLOAT_EPSILON {
        return Ok(FillPlan::default());
    }
    let raw_average = raw_notional / filled;
    let average_price = apply_adverse_slippage(raw_average, side, slippage_bps);
    Ok(FillPlan {
        filled_quantity: filled,
        average_price,
        notional: filled * average_price,
    })
}

fn level_is_executable(price: f64, side: OrderSide, limit_price: Option<f64>) -> bool {
    match (side, limit_price) {
        (_, None) => true,
        (OrderSide::Buy, Some(limit)) => price <= limit + FLOAT_EPSILON,
        (OrderSide::Sell, Some(limit)) => price + FLOAT_EPSILON >= limit,
    }
}

fn crosses_book(book: &OrderBookSnapshot, side: OrderSide, price: f64) -> bool {
    match side {
        OrderSide::Buy => book
            .asks
            .first()
            .map(|ask| price + FLOAT_EPSILON >= ask.price)
            .unwrap_or(false),
        OrderSide::Sell => book
            .bids
            .first()
            .map(|bid| price <= bid.price + FLOAT_EPSILON)
            .unwrap_or(false),
    }
}

fn maker_cross_quantity(book: &OrderBookSnapshot, side: OrderSide, limit_price: f64) -> f64 {
    match side {
        OrderSide::Buy => book
            .asks
            .iter()
            .take_while(|level| level.price <= limit_price + FLOAT_EPSILON)
            .map(|level| level.quantity)
            .sum(),
        OrderSide::Sell => book
            .bids
            .iter()
            .take_while(|level| level.price + FLOAT_EPSILON >= limit_price)
            .map(|level| level.quantity)
            .sum(),
    }
}

fn apply_adverse_slippage(price: f64, side: OrderSide, slippage_bps: f64) -> f64 {
    let rate = if slippage_bps.is_finite() {
        slippage_bps.max(0.0) / 10_000.0
    } else {
        0.0
    };
    match side {
        OrderSide::Buy => price * (1.0 + rate),
        OrderSide::Sell => price * (1.0 - rate),
    }
}

fn normalize_symbol(symbol: &str) -> ExchangeClientResult<String> {
    let normalized = symbol
        .trim()
        .replace(['/', '-', '_'], "")
        .to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(ExchangeClientError::Validation {
            field: "symbol",
            reason: "symbol must not be empty".to_string(),
        });
    }
    Ok(normalized)
}

fn ensure_client_order_id(request: &mut OrderRequest) -> ExchangeClientResult<()> {
    if request.client_order_id.is_none() {
        request.client_order_id =
            Some(generate_client_order_id("paper", request.market_type, "paper").into_string());
    }
    if let Some(client_order_id) = &request.client_order_id {
        validate_client_order_id("paper", request.market_type, client_order_id).map_err(
            |error| ExchangeClientError::Validation {
                field: "client_order_id",
                reason: error.to_string(),
            },
        )?;
    }
    Ok(())
}

fn validate_cancel_client_order_id(request: &CancelOrderRequest) -> ExchangeClientResult<()> {
    if let Some(client_order_id) = &request.client_order_id {
        validate_client_order_id("paper", request.market_type, client_order_id).map_err(
            |error| ExchangeClientError::Validation {
                field: "client_order_id",
                reason: error.to_string(),
            },
        )?;
    }
    Ok(())
}

fn infer_symbol_assets(symbol: &str, default_quote: &str) -> (String, String) {
    let normalized = symbol.replace(['/', '-', '_'], "").to_ascii_uppercase();
    for quote in ["USDT", "USDC", "USD", "BTC", "ETH", "EUR"] {
        if let Some(base) = normalized.strip_suffix(quote) {
            if !base.is_empty() {
                return (base.to_string(), quote.to_string());
            }
        }
    }
    let quote = default_quote.to_ascii_uppercase();
    let base = normalized
        .strip_suffix(&quote)
        .filter(|base| !base.is_empty())
        .unwrap_or(&normalized);
    (base.to_string(), quote)
}

fn order_error(message: impl Into<String>) -> ExchangeClientError {
    ExchangeError::OrderError(message.into()).into()
}

fn default_exchange_name() -> String {
    "paper".to_string()
}

fn default_market_type() -> MarketType {
    MarketType::Spot
}

fn default_quote_asset() -> String {
    "USDT".to_string()
}

fn default_maker_fee_rate() -> f64 {
    DEFAULT_MAKER_FEE_RATE
}

fn default_taker_fee_rate() -> f64 {
    DEFAULT_TAKER_FEE_RATE
}

fn default_stale_book_after_ms() -> i64 {
    DEFAULT_STALE_BOOK_AFTER_MS
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client_with_balances(balances: Vec<(&str, f64)>) -> PaperExchangeClient {
        PaperExchangeClient::with_balances(
            PaperExchangeConfig {
                maker_fee_rate: 0.0002,
                taker_fee_rate: 0.001,
                ..PaperExchangeConfig::default()
            },
            balances
                .into_iter()
                .map(|(asset, total)| AssetBalance::new(asset, total, total, 0.0))
                .collect(),
        )
    }

    fn fresh_book() -> OrderBookSnapshot {
        OrderBookSnapshot {
            exchange: "paper-source".to_string(),
            market_type: MarketType::Spot,
            symbol: "BTCUSDT".to_string(),
            best_bid: Some(99.0),
            best_ask: Some(100.0),
            bids: vec![
                OrderBookLevel {
                    price: 99.0,
                    quantity: 1.0,
                },
                OrderBookLevel {
                    price: 98.0,
                    quantity: 2.0,
                },
            ],
            asks: vec![
                OrderBookLevel {
                    price: 100.0,
                    quantity: 1.0,
                },
                OrderBookLevel {
                    price: 101.0,
                    quantity: 2.0,
                },
            ],
            exchange_timestamp: Some(Utc::now()),
            received_at: Utc::now(),
            latency_ms: Some(0),
            sequence: Some(1),
            is_stale: false,
        }
    }

    fn limit_request(side: OrderSide, quantity: f64, price: f64) -> OrderRequest {
        OrderRequest {
            market_type: MarketType::Spot,
            symbol: "BTCUSDT".to_string(),
            side,
            position_side: PositionSide::None,
            order_type: OrderType::Limit,
            time_in_force: None,
            quantity,
            price: Some(price),
            client_order_id: None,
            reduce_only: false,
        }
    }

    #[test]
    fn paper_capabilities_should_match_implemented_streams() {
        let client = PaperExchangeClient::new(PaperExchangeConfig::default());
        let capabilities = client.capabilities();

        assert_eq!(capabilities.market_type, MarketType::Spot);
        assert!(capabilities.supports_public_ws);
        assert!(capabilities.supports_private_user_stream);
        assert!(!capabilities.supports_quote_market_order);
        assert!(!capabilities.supports_amend_order);
        assert!(!capabilities.supports_order_list);
    }

    #[tokio::test]
    async fn paper_market_buy_should_consume_asks_and_charge_taker_fee() {
        let client = client_with_balances(vec![("USDT", 10_000.0)]);
        client.update_orderbook(fresh_book()).unwrap();

        let order = client
            .place_order(OrderRequest::spot_market_buy("BTCUSDT", 0.5))
            .await
            .unwrap();

        assert_eq!(order.status, OrderStatus::Filled);
        assert_eq!(order.average_price, Some(100.0));
        let balances = client.get_balances().await.unwrap();
        let usdt = balances
            .balances
            .iter()
            .find(|b| b.asset == "USDT")
            .unwrap();
        let btc = balances.balances.iter().find(|b| b.asset == "BTC").unwrap();
        assert!((usdt.available - 9949.95).abs() < 1e-9);
        assert!((btc.available - 0.5).abs() < 1e-9);
        let book = client.get_orderbook("BTCUSDT", 5).await.unwrap();
        assert!((book.asks[0].quantity - 0.5).abs() < 1e-9);
    }

    #[tokio::test]
    async fn paper_get_order_should_validate_order_id_before_lookup() {
        let client = PaperExchangeClient::new(PaperExchangeConfig::default());

        let err = client.get_order("BTCUSDT", "   ").await.unwrap_err();
        assert!(matches!(
            err,
            ExchangeClientError::Validation {
                field: "order_id",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn paper_orderbook_should_validate_depth_before_lookup() {
        let client = client_with_balances(vec![("USDT", 10_000.0)]);
        client.update_orderbook(fresh_book()).unwrap();

        let err = client.get_orderbook("BTCUSDT", 0).await.unwrap_err();
        assert!(matches!(
            err,
            ExchangeClientError::Validation { field: "depth", .. }
        ));
    }

    #[tokio::test]
    async fn paper_cancel_order_should_validate_client_order_id_before_lookup() {
        let client = PaperExchangeClient::new(PaperExchangeConfig::default());

        let err = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                order_id: None,
                client_order_id: Some("bad/id".to_string()),
            })
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            ExchangeClientError::Validation {
                field: "client_order_id",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn paper_cancel_order_should_ignore_blank_order_id_when_client_id_is_present() {
        let client = client_with_balances(vec![("USDT", 1_000.0)]);
        client.update_orderbook(fresh_book()).unwrap();
        let order = client
            .place_order(OrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                side: OrderSide::Buy,
                position_side: PositionSide::None,
                order_type: OrderType::Limit,
                time_in_force: None,
                quantity: 0.1,
                price: Some(95.0),
                client_order_id: Some("PAPER-CANCEL-1".to_string()),
                reduce_only: false,
            })
            .await
            .unwrap();
        assert_eq!(order.status, OrderStatus::New);

        let cancelled = client
            .cancel_order(CancelOrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                order_id: Some("   ".to_string()),
                client_order_id: Some("PAPER-CANCEL-1".to_string()),
            })
            .await
            .unwrap();
        assert_eq!(cancelled.status, OrderStatus::Cancelled);
        assert_eq!(cancelled.order_id.as_deref(), Some(order.order_id.as_str()));
    }

    #[tokio::test]
    async fn paper_fee_rate_should_validate_symbol() {
        let client = PaperExchangeClient::new(PaperExchangeConfig::default());

        let err = client.get_fee_rate("   ").await.unwrap_err();
        assert!(matches!(
            err,
            ExchangeClientError::Validation {
                field: "symbol",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn paper_orderbook_subscription_should_reject_empty_symbols() {
        let client = PaperExchangeClient::new(PaperExchangeConfig::default());

        let err = client.subscribe_orderbook(Vec::new()).await.unwrap_err();
        assert!(matches!(
            err,
            ExchangeClientError::Validation {
                field: "symbols",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn paper_market_sell_should_consume_bids_and_charge_taker_fee() {
        let client = client_with_balances(vec![("BTC", 1.0)]);
        client.update_orderbook(fresh_book()).unwrap();

        let order = client
            .place_order(OrderRequest {
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                side: OrderSide::Sell,
                position_side: PositionSide::None,
                order_type: OrderType::Market,
                time_in_force: None,
                quantity: 0.5,
                price: None,
                client_order_id: None,
                reduce_only: false,
            })
            .await
            .unwrap();

        assert_eq!(order.status, OrderStatus::Filled);
        assert_eq!(order.average_price, Some(99.0));
        let balances = client.get_balances().await.unwrap();
        let usdt = balances
            .balances
            .iter()
            .find(|b| b.asset == "USDT")
            .unwrap();
        assert!((usdt.available - 49.4505).abs() < 1e-9);
    }

    #[tokio::test]
    async fn paper_limit_maker_should_fill_when_market_crosses_price() {
        let client = client_with_balances(vec![("USDT", 1_000.0)]);
        client.update_orderbook(fresh_book()).unwrap();
        let order = client
            .place_order(limit_request(OrderSide::Buy, 0.5, 99.0))
            .await
            .unwrap();
        assert_eq!(order.status, OrderStatus::New);

        let mut crossed = fresh_book();
        crossed.asks = vec![OrderBookLevel {
            price: 98.5,
            quantity: 0.25,
        }];
        let fills = client.update_orderbook(crossed).unwrap();

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].liquidity, LiquidityRole::Maker);
        assert_eq!(fills[0].quantity, 0.25);
        assert_eq!(fills[0].price, 99.0);
        let order = client.get_order("BTCUSDT", &order.order_id).await.unwrap();
        assert_eq!(order.status, OrderStatus::PartiallyFilled);
        assert_eq!(order.filled_quantity, 0.25);
    }

    #[tokio::test]
    async fn paper_post_only_should_reject_crossing_order() {
        let client = client_with_balances(vec![("USDT", 1_000.0)]);
        client.update_orderbook(fresh_book()).unwrap();
        let mut request = limit_request(OrderSide::Buy, 0.1, 100.0);
        request.order_type = OrderType::PostOnly;

        let order = client.place_order(request).await.unwrap();

        assert_eq!(order.status, OrderStatus::Rejected);
        assert!(client.get_open_orders(None).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn paper_ioc_should_allow_partial_fill_and_cancel_remainder() {
        let client = client_with_balances(vec![("USDT", 1_000.0)]);
        client.update_orderbook(fresh_book()).unwrap();
        let mut request = limit_request(OrderSide::Buy, 2.0, 100.0);
        request.order_type = OrderType::IOC;

        let order = client.place_order(request).await.unwrap();

        assert_eq!(order.status, OrderStatus::PartiallyFilled);
        assert_eq!(order.filled_quantity, 1.0);
        assert!(client.get_open_orders(None).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn paper_fok_should_fill_all_or_cancel_without_balance_change() {
        let client = client_with_balances(vec![("USDT", 1_000.0)]);
        client.update_orderbook(fresh_book()).unwrap();
        let mut request = limit_request(OrderSide::Buy, 2.0, 100.0);
        request.order_type = OrderType::FOK;

        let order = client.place_order(request).await.unwrap();

        assert_eq!(order.status, OrderStatus::Expired);
        assert_eq!(order.filled_quantity, 0.0);
        let balances = client.get_balances().await.unwrap();
        let usdt = balances
            .balances
            .iter()
            .find(|b| b.asset == "USDT")
            .unwrap();
        assert_eq!(usdt.available, 1_000.0);
    }

    #[tokio::test]
    async fn paper_fee_deduction_should_be_recorded_on_fill() {
        let client = client_with_balances(vec![("USDT", 1_000.0)]);
        client.update_orderbook(fresh_book()).unwrap();

        client
            .place_order(OrderRequest::spot_market_buy("BTCUSDT", 0.1))
            .await
            .unwrap();

        let fills = client.recorded_fills().unwrap();
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].fee_asset.as_deref(), Some("USDT"));
        assert!((fills[0].fee_amount.unwrap() - 0.01).abs() < 1e-9);
    }

    #[tokio::test]
    async fn paper_order_should_reject_when_balance_is_insufficient() {
        let client = client_with_balances(vec![("USDT", 10.0)]);
        client.update_orderbook(fresh_book()).unwrap();

        let order = client
            .place_order(OrderRequest::spot_market_buy("BTCUSDT", 0.5))
            .await
            .unwrap();

        assert_eq!(order.status, OrderStatus::Rejected);
        assert!(client
            .recorded_orders()
            .unwrap()
            .iter()
            .any(|o| o.status == OrderStatus::Rejected));
    }

    #[tokio::test]
    async fn paper_market_order_should_reject_when_depth_is_insufficient() {
        let client = client_with_balances(vec![("USDT", 10_000.0)]);
        client.update_orderbook(fresh_book()).unwrap();

        let order = client
            .place_order(OrderRequest::spot_market_buy("BTCUSDT", 10.0))
            .await
            .unwrap();

        assert_eq!(order.status, OrderStatus::Rejected);
        assert!(client.recorded_fills().unwrap().is_empty());
    }

    #[tokio::test]
    async fn paper_order_should_reject_when_order_book_is_stale() {
        let client = client_with_balances(vec![("USDT", 10_000.0)]);
        let mut book = fresh_book();
        book.received_at = Utc::now() - Duration::seconds(30);
        client.update_orderbook(book).unwrap();

        let order = client
            .place_order(OrderRequest::spot_market_buy("BTCUSDT", 0.1))
            .await
            .unwrap();

        assert_eq!(order.status, OrderStatus::Rejected);
        assert!(client.recorded_fills().unwrap().is_empty());
    }
}
