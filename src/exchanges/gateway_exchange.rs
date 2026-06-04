use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use serde_json::json;

use crate::core::error::ExchangeError;
use crate::core::exchange::Exchange;
use crate::core::types::{
    AccountSnapshot, Balance, BatchOrderError, BatchOrderRequest, BatchOrderResponse, ExchangeInfo,
    IndexPriceKline, Interval, Kline, MarkPrice, MarkPriceKline, MarketType, OpenInterest, Order,
    OrderBook, OrderRequest, OrderSide as CoreOrderSide, OrderStatus, OrderType as CoreOrderType,
    Position, Statistics24h, Ticker, Trade, TradeFee, TradingPair,
};
use crate::core::websocket::WebSocketClient;
use crate::execution::{
    CancelAllCommand, CancelBatchCommand, CancelCommand, FillQuery, LeverageCommand, OrderCommand,
    OrderCommandStatus, OrderIntent, OrderSide, OrderType, PositionMode, PositionSide, TimeInForce,
    TradingAdapter,
};
use crate::market::{
    canonical_from_exchange_symbol, exchange_symbol_for, CanonicalSymbol, ExchangeId,
    ExchangeSymbol, InstrumentMeta, MarketDataAdapter,
};

pub struct GatewayExchange {
    name: String,
    exchange_id: ExchangeId,
    market: Box<dyn MarketDataAdapter + Send + Sync>,
    trading: Arc<dyn TradingAdapter>,
    instruments: HashMap<CanonicalSymbol, InstrumentMeta>,
    position_mode: PositionMode,
}

impl GatewayExchange {
    pub fn new(
        exchange_id: ExchangeId,
        market: Box<dyn MarketDataAdapter + Send + Sync>,
        trading: Arc<dyn TradingAdapter>,
        instruments: impl IntoIterator<Item = InstrumentMeta>,
        position_mode: PositionMode,
    ) -> Self {
        let instruments = instruments
            .into_iter()
            .filter(|instrument| instrument.exchange == exchange_id)
            .map(|instrument| (instrument.canonical_symbol.clone(), instrument))
            .collect();
        Self {
            name: exchange_id.as_str().to_string(),
            exchange_id,
            market,
            trading,
            instruments,
            position_mode,
        }
    }

    pub fn exchange_id(&self) -> &ExchangeId {
        &self.exchange_id
    }

    pub fn position_mode(&self) -> PositionMode {
        self.position_mode
    }

    pub fn supports_hedge_mode(&self) -> bool {
        self.trading.capabilities().supports_hedge_mode
    }

    pub async fn read_symbol_position_mode(
        &self,
        symbol: &str,
    ) -> crate::core::types::Result<Option<PositionMode>> {
        let exchange_symbol = self.exchange_symbol_for_input(symbol)?;
        let config = self
            .trading
            .get_symbol_account_config(&exchange_symbol)
            .await
            .map_err(to_exchange_error)?;
        Ok(config.position_mode)
    }

    fn require_futures(&self, market_type: MarketType) -> crate::core::types::Result<()> {
        if market_type == MarketType::Futures {
            Ok(())
        } else {
            Err(ExchangeError::UnsupportedMarketType {
                market_type,
                exchange: self.name.clone(),
            })
        }
    }

    fn canonical_for_input(&self, symbol: &str) -> crate::core::types::Result<CanonicalSymbol> {
        CanonicalSymbol::parse(symbol)
            .or_else(|| canonical_from_exchange_symbol(&self.exchange_id, symbol))
            .ok_or_else(|| ExchangeError::SymbolError(format!("无效交易对: {symbol}")))
    }

    fn exchange_symbol_for_input(
        &self,
        symbol: &str,
    ) -> crate::core::types::Result<ExchangeSymbol> {
        let canonical = self.canonical_for_input(symbol)?;
        Ok(self
            .instruments
            .get(&canonical)
            .map(|instrument| instrument.exchange_symbol.clone())
            .unwrap_or_else(|| exchange_symbol_for(&self.exchange_id, &canonical)))
    }

    fn instrument_for_input(&self, symbol: &str) -> crate::core::types::Result<InstrumentMeta> {
        let canonical = self.canonical_for_input(symbol)?;
        self.instruments
            .get(&canonical)
            .cloned()
            .ok_or_else(|| ExchangeError::SymbolNotFound {
                symbol: canonical.as_pair(),
                market_type: MarketType::Futures,
            })
    }

    fn order_command_from_request(
        &self,
        request: &OrderRequest,
    ) -> crate::core::types::Result<OrderCommand> {
        self.require_futures(request.market_type)?;
        let canonical = self.canonical_for_input(&request.symbol)?;
        let exchange_symbol = self
            .instruments
            .get(&canonical)
            .map(|instrument| instrument.exchange_symbol.clone())
            .unwrap_or_else(|| exchange_symbol_for(&self.exchange_id, &canonical));
        let side = map_core_side(request.side);
        let position_side = if self.position_mode.is_hedge() {
            request
                .params
                .as_ref()
                .and_then(|params| params.get("positionSide"))
                .map(|value| parse_position_side(value))
                .unwrap_or_else(|| default_position_side(side))
        } else {
            PositionSide::Net
        };
        let reduce_only = request.reduce_only.unwrap_or(false)
            || (self.position_mode.is_hedge() && is_hedge_close(side, position_side));
        Ok(OrderCommand {
            command_id: format!(
                "cmd-{}",
                request
                    .client_order_id
                    .as_deref()
                    .unwrap_or("gateway-order")
            ),
            bundle_id: "gateway-exchange".to_string(),
            exchange: self.exchange_id.clone(),
            canonical_symbol: canonical,
            exchange_symbol,
            intent: intent_for(side, position_side, reduce_only),
            side,
            position_side,
            order_type: map_core_order_type(request.order_type)?,
            quantity: request.amount,
            price: request.price,
            time_in_force: map_time_in_force(request),
            post_only: request.post_only.unwrap_or(false),
            reduce_only,
            client_order_id: request
                .client_order_id
                .clone()
                .unwrap_or_else(|| format!("gw-{}", Utc::now().timestamp_millis())),
            max_slippage_pct: None,
            status: OrderCommandStatus::Planned,
            created_at: Utc::now(),
        })
    }
}

#[async_trait]
impl Exchange for GatewayExchange {
    fn name(&self) -> &str {
        &self.name
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn exchange_info(&self) -> crate::core::types::Result<ExchangeInfo> {
        Ok(ExchangeInfo {
            name: self.name.clone(),
            symbols: self
                .instruments
                .values()
                .map(|instrument| crate::core::types::Symbol {
                    base: instrument.base.clone(),
                    quote: instrument.quote.clone(),
                    symbol: instrument.canonical_symbol.as_pair(),
                    market_type: MarketType::Futures,
                    exchange_specific: Some(instrument.exchange_symbol.symbol.clone()),
                })
                .collect(),
            currencies: vec!["USDT".to_string(), "USDC".to_string()],
            spot_enabled: false,
            futures_enabled: true,
        })
    }

    async fn get_balance(
        &self,
        market_type: MarketType,
    ) -> crate::core::types::Result<Vec<Balance>> {
        self.require_futures(market_type)?;
        self.trading
            .get_balances()
            .await
            .map_err(to_exchange_error)
            .map(|balances| {
                balances
                    .into_iter()
                    .map(|balance| Balance {
                        currency: balance.asset,
                        total: balance.total,
                        free: balance.available,
                        used: balance.locked,
                        market_type,
                    })
                    .collect()
            })
    }

    async fn get_ticker(
        &self,
        symbol: &str,
        market_type: MarketType,
    ) -> crate::core::types::Result<Ticker> {
        self.require_futures(market_type)?;
        let book = self.get_orderbook(symbol, market_type, Some(5)).await?;
        let bid = book.bids.first().map(|level| level[0]).unwrap_or_default();
        let ask = book.asks.first().map(|level| level[0]).unwrap_or_default();
        let last = match (bid > 0.0, ask > 0.0) {
            (true, true) => (bid + ask) / 2.0,
            (true, false) => bid,
            (false, true) => ask,
            (false, false) => 0.0,
        };
        Ok(Ticker {
            symbol: self.canonical_for_input(symbol)?.as_pair(),
            high: last,
            low: last,
            bid,
            ask,
            last,
            volume: 0.0,
            timestamp: book.timestamp,
        })
    }

    async fn get_all_tickers(
        &self,
        market_type: MarketType,
    ) -> crate::core::types::Result<Vec<Ticker>> {
        self.require_futures(market_type)?;
        Ok(Vec::new())
    }

    async fn get_orderbook(
        &self,
        symbol: &str,
        market_type: MarketType,
        limit: Option<u32>,
    ) -> crate::core::types::Result<OrderBook> {
        self.require_futures(market_type)?;
        let exchange_symbol = self.exchange_symbol_for_input(symbol)?;
        let depth = limit.unwrap_or(5).min(u16::MAX as u32) as u16;
        let book = self
            .market
            .fetch_orderbook_snapshot(&exchange_symbol, depth.max(1))
            .await
            .map_err(to_exchange_error)?;
        Ok(OrderBook {
            symbol: book.canonical_symbol.as_pair(),
            bids: book
                .bids
                .into_iter()
                .map(|level| [level.price, level.quantity])
                .collect(),
            asks: book
                .asks
                .into_iter()
                .map(|level| [level.price, level.quantity])
                .collect(),
            timestamp: book.recv_ts,
            info: json!({
                "exchange": self.exchange_id.as_str(),
                "exchange_symbol": exchange_symbol.symbol,
            }),
        })
    }

    async fn create_order(&self, request: OrderRequest) -> crate::core::types::Result<Order> {
        let command = self.order_command_from_request(&request)?;
        let ack = self
            .trading
            .place_order(command.clone())
            .await
            .map_err(to_exchange_error)?;
        Ok(order_from_ack(&command, ack.exchange_order_id, ack.status))
    }

    async fn cancel_order(
        &self,
        order_id: &str,
        symbol: &str,
        market_type: MarketType,
    ) -> crate::core::types::Result<Order> {
        self.require_futures(market_type)?;
        let canonical = self.canonical_for_input(symbol)?;
        let exchange_symbol = exchange_symbol_for(&self.exchange_id, &canonical);
        let command = CancelCommand {
            exchange: self.exchange_id.clone(),
            canonical_symbol: canonical.clone(),
            exchange_symbol,
            client_order_id: None,
            exchange_order_id: Some(order_id.to_string()),
            reason: None,
            requested_at: Utc::now(),
        };
        let ack = self
            .trading
            .cancel_order(command)
            .await
            .map_err(to_exchange_error)?;
        Ok(Order {
            id: ack
                .exchange_order_id
                .unwrap_or_else(|| order_id.to_string()),
            symbol: canonical.as_pair(),
            side: CoreOrderSide::Buy,
            order_type: CoreOrderType::Limit,
            amount: 0.0,
            price: None,
            filled: 0.0,
            remaining: 0.0,
            status: OrderStatus::Canceled,
            market_type,
            timestamp: ack.acknowledged_at,
            last_trade_timestamp: None,
            info: json!({ "client_order_id": ack.client_order_id }),
        })
    }

    async fn get_order(
        &self,
        order_id: &str,
        symbol: &str,
        market_type: MarketType,
    ) -> crate::core::types::Result<Order> {
        self.require_futures(market_type)?;
        let exchange_symbol = self.exchange_symbol_for_input(symbol)?;
        let state = self
            .trading
            .get_order(crate::execution::OrderQuery {
                exchange: self.exchange_id.clone(),
                exchange_symbol,
                client_order_id: None,
                exchange_order_id: Some(order_id.to_string()),
            })
            .await
            .map_err(to_exchange_error)?;
        Ok(order_from_state(state, market_type))
    }

    async fn get_open_orders(
        &self,
        symbol: Option<&str>,
        market_type: MarketType,
    ) -> crate::core::types::Result<Vec<Order>> {
        self.require_futures(market_type)?;
        let exchange_symbol = symbol
            .map(|symbol| self.exchange_symbol_for_input(symbol))
            .transpose()?;
        let orders = self
            .trading
            .get_open_orders(exchange_symbol.as_ref())
            .await
            .map_err(to_exchange_error)?;
        Ok(orders
            .into_iter()
            .map(|order| order_from_state(order, market_type))
            .collect())
    }

    async fn get_order_history(
        &self,
        _symbol: Option<&str>,
        _market_type: MarketType,
        _limit: Option<u32>,
    ) -> crate::core::types::Result<Vec<Order>> {
        Ok(Vec::new())
    }

    async fn get_trades(
        &self,
        _symbol: &str,
        _market_type: MarketType,
        _limit: Option<u32>,
    ) -> crate::core::types::Result<Vec<Trade>> {
        Err(ExchangeError::NotSupported(
            "public trades are not wired for gateway exchange".to_string(),
        ))
    }

    async fn get_my_trades(
        &self,
        symbol: Option<&str>,
        market_type: MarketType,
        limit: Option<u32>,
    ) -> crate::core::types::Result<Vec<Trade>> {
        self.require_futures(market_type)?;
        let (canonical, exchange_symbol) = match symbol {
            Some(symbol) => {
                let canonical = self.canonical_for_input(symbol)?;
                let exchange_symbol = exchange_symbol_for(&self.exchange_id, &canonical);
                (Some(canonical), Some(exchange_symbol))
            }
            None => (None, None),
        };
        let fills = self
            .trading
            .get_fills(FillQuery {
                exchange: self.exchange_id.clone(),
                canonical_symbol: canonical,
                exchange_symbol,
                client_order_id: None,
                exchange_order_id: None,
                from_trade_id: None,
                start_time: None,
                end_time: None,
                limit,
            })
            .await
            .map_err(to_exchange_error)?;
        Ok(fills
            .into_iter()
            .map(|fill| Trade {
                id: fill.trade_id,
                symbol: fill.canonical_symbol.as_pair(),
                side: map_exec_side(fill.side),
                amount: fill.quantity,
                price: fill.price,
                timestamp: fill.filled_at,
                order_id: fill.exchange_order_id,
                fee: fill.fee.map(|cost| crate::core::types::Fee {
                    currency: fill.fee_asset.unwrap_or_else(|| "USDT".to_string()),
                    cost,
                    rate: fill.fee_rate,
                }),
            })
            .collect())
    }

    async fn get_klines(
        &self,
        _symbol: &str,
        _interval: Interval,
        _market_type: MarketType,
        _limit: Option<u32>,
    ) -> crate::core::types::Result<Vec<Kline>> {
        Err(ExchangeError::NotSupported(
            "klines are not wired for gateway exchange".to_string(),
        ))
    }

    async fn get_24h_statistics(
        &self,
        _symbol: &str,
        _market_type: MarketType,
    ) -> crate::core::types::Result<Statistics24h> {
        Err(ExchangeError::NotSupported(
            "24h statistics are not wired for gateway exchange".to_string(),
        ))
    }

    async fn get_all_24h_statistics(
        &self,
        _market_type: MarketType,
    ) -> crate::core::types::Result<Vec<Statistics24h>> {
        Ok(Vec::new())
    }

    async fn get_trade_fee(
        &self,
        symbol: &str,
        market_type: MarketType,
    ) -> crate::core::types::Result<TradeFee> {
        self.require_futures(market_type)?;
        let exchange_symbol = self.exchange_symbol_for_input(symbol)?;
        let fee = self
            .trading
            .get_trade_fee(&exchange_symbol)
            .await
            .map_err(to_exchange_error)?;
        Ok(TradeFee {
            symbol: fee.canonical_symbol.as_pair(),
            maker: fee.maker,
            taker: fee.taker,
            percentage: true,
            tier_based: true,
            maker_fee: Some(fee.maker),
            taker_fee: Some(fee.taker),
        })
    }

    async fn get_account_snapshot(
        &self,
        _market_type: MarketType,
    ) -> crate::core::types::Result<AccountSnapshot> {
        Err(ExchangeError::NotSupported(
            "account snapshot is not wired for gateway exchange".to_string(),
        ))
    }

    async fn get_positions(
        &self,
        symbol: Option<&str>,
    ) -> crate::core::types::Result<Vec<Position>> {
        let exchange_symbol = symbol
            .map(|symbol| self.exchange_symbol_for_input(symbol))
            .transpose()?;
        let positions = self
            .trading
            .get_positions(exchange_symbol.as_ref())
            .await
            .map_err(to_exchange_error)?;
        Ok(positions
            .into_iter()
            .map(|position| {
                let quantity = position.quantity.abs();
                Position {
                    symbol: position.canonical_symbol.as_pair(),
                    side: match position.position_side {
                        PositionSide::Long => "LONG",
                        PositionSide::Short => "SHORT",
                        PositionSide::Net => "NET",
                    }
                    .to_string(),
                    contracts: quantity,
                    contract_size: 1.0,
                    entry_price: position.entry_price.unwrap_or_default(),
                    mark_price: position.mark_price.unwrap_or_default(),
                    unrealized_pnl: position.unrealized_pnl.unwrap_or_default(),
                    percentage: 0.0,
                    margin: 0.0,
                    margin_ratio: 0.0,
                    leverage: None,
                    margin_type: None,
                    size: quantity,
                    amount: quantity,
                    timestamp: position.updated_at,
                }
            })
            .collect())
    }

    async fn set_leverage(&self, symbol: &str, leverage: u32) -> crate::core::types::Result<()> {
        let canonical = self.canonical_for_input(symbol)?;
        let exchange_symbol = exchange_symbol_for(&self.exchange_id, &canonical);
        self.trading
            .set_leverage(LeverageCommand {
                exchange: self.exchange_id.clone(),
                canonical_symbol: canonical,
                exchange_symbol,
                leverage,
                requested_at: Utc::now(),
            })
            .await
            .map_err(to_exchange_error)?;
        Ok(())
    }

    async fn cancel_all_orders(
        &self,
        symbol: Option<&str>,
        market_type: MarketType,
    ) -> crate::core::types::Result<Vec<Order>> {
        self.require_futures(market_type)?;
        let (canonical_symbol, exchange_symbol) = match symbol {
            Some(symbol) => {
                let canonical = self.canonical_for_input(symbol)?;
                let exchange_symbol = exchange_symbol_for(&self.exchange_id, &canonical);
                (Some(canonical), Some(exchange_symbol))
            }
            None => (None, None),
        };
        self.trading
            .cancel_all_orders(CancelAllCommand {
                exchange: self.exchange_id.clone(),
                canonical_symbol,
                exchange_symbol,
                requested_at: Utc::now(),
            })
            .await
            .map_err(to_exchange_error)?;
        Ok(Vec::new())
    }

    async fn cancel_multiple_orders(
        &self,
        order_ids: Vec<String>,
        symbol: &str,
        market_type: MarketType,
    ) -> crate::core::types::Result<Vec<Order>> {
        self.require_futures(market_type)?;
        let canonical = self.canonical_for_input(symbol)?;
        let exchange_symbol = exchange_symbol_for(&self.exchange_id, &canonical);
        let commands = order_ids.iter().map(|order_id| CancelCommand {
            exchange: self.exchange_id.clone(),
            canonical_symbol: canonical.clone(),
            exchange_symbol: exchange_symbol.clone(),
            client_order_id: None,
            exchange_order_id: Some(order_id.clone()),
            reason: None,
            requested_at: Utc::now(),
        });
        let ack = self
            .trading
            .cancel_batch_orders(CancelBatchCommand::new(
                self.exchange_id.clone(),
                commands,
                Utc::now(),
            ))
            .await
            .map_err(to_exchange_error)?;
        Ok(ack
            .order_acks
            .into_iter()
            .map(|ack| Order {
                id: ack.exchange_order_id.unwrap_or_default(),
                symbol: canonical.as_pair(),
                side: CoreOrderSide::Buy,
                order_type: CoreOrderType::Limit,
                amount: 0.0,
                price: None,
                filled: 0.0,
                remaining: 0.0,
                status: OrderStatus::Canceled,
                market_type,
                timestamp: ack.acknowledged_at,
                last_trade_timestamp: None,
                info: json!({ "client_order_id": ack.client_order_id }),
            })
            .collect())
    }

    async fn get_server_time(&self) -> crate::core::types::Result<chrono::DateTime<Utc>> {
        Ok(Utc::now())
    }

    async fn ping(&self) -> crate::core::types::Result<()> {
        Ok(())
    }

    async fn create_batch_orders(
        &self,
        batch_request: BatchOrderRequest,
    ) -> crate::core::types::Result<BatchOrderResponse> {
        let mut successful_orders = Vec::new();
        let mut failed_orders = Vec::new();
        for order_request in batch_request.orders {
            match self.create_order(order_request.clone()).await {
                Ok(order) => successful_orders.push(order),
                Err(err) => failed_orders.push(BatchOrderError {
                    order_request,
                    error_message: err.to_string(),
                    error_code: None,
                }),
            }
        }
        Ok(BatchOrderResponse {
            successful_orders,
            failed_orders,
        })
    }

    async fn get_all_spot_symbols(&self) -> crate::core::types::Result<Vec<TradingPair>> {
        Ok(Vec::new())
    }

    async fn get_all_futures_symbols(&self) -> crate::core::types::Result<Vec<TradingPair>> {
        Ok(self
            .instruments
            .values()
            .map(trading_pair_from_instrument)
            .collect())
    }

    async fn get_symbol_info(
        &self,
        symbol: &str,
        market_type: MarketType,
    ) -> crate::core::types::Result<TradingPair> {
        self.require_futures(market_type)?;
        let instrument = self.instrument_for_input(symbol)?;
        Ok(trading_pair_from_instrument(&instrument))
    }

    async fn create_websocket_client(
        &self,
        _market_type: MarketType,
    ) -> crate::core::types::Result<Box<dyn WebSocketClient>> {
        Err(ExchangeError::NotSupported(
            "websocket client is not exposed through GatewayExchange".to_string(),
        ))
    }

    async fn get_mark_price(
        &self,
        symbol: Option<&str>,
    ) -> crate::core::types::Result<Vec<MarkPrice>> {
        let symbols = match symbol {
            Some(symbol) => vec![self.canonical_for_input(symbol)?],
            None => self.instruments.keys().cloned().collect(),
        };
        let funding = self
            .market
            .load_funding(&symbols)
            .await
            .map_err(to_exchange_error)?;
        Ok(funding
            .into_iter()
            .map(|item| MarkPrice {
                symbol: item.canonical_symbol.as_pair(),
                mark_price: item.mark_price.unwrap_or_default(),
                index_price: item.index_price.unwrap_or_default(),
                estimated_settle_price: item.mark_price.unwrap_or_default(),
                last_funding_rate: item.funding_rate,
                next_funding_time: item.next_funding_time.unwrap_or_else(Utc::now),
                timestamp: item.recv_ts,
            })
            .collect())
    }

    async fn get_mark_price_klines(
        &self,
        _symbol: &str,
        _interval: Interval,
        _limit: Option<u32>,
    ) -> crate::core::types::Result<Vec<MarkPriceKline>> {
        Err(ExchangeError::NotSupported(
            "mark price klines are not wired for gateway exchange".to_string(),
        ))
    }

    async fn get_index_price_klines(
        &self,
        _symbol: &str,
        _interval: Interval,
        _limit: Option<u32>,
    ) -> crate::core::types::Result<Vec<IndexPriceKline>> {
        Err(ExchangeError::NotSupported(
            "index price klines are not wired for gateway exchange".to_string(),
        ))
    }

    async fn get_open_interest(&self, _symbol: &str) -> crate::core::types::Result<OpenInterest> {
        Err(ExchangeError::NotSupported(
            "open interest is not wired for gateway exchange".to_string(),
        ))
    }
}

fn to_exchange_error(err: anyhow::Error) -> ExchangeError {
    ExchangeError::Other(err.to_string())
}

fn map_core_side(side: CoreOrderSide) -> OrderSide {
    match side {
        CoreOrderSide::Buy => OrderSide::Buy,
        CoreOrderSide::Sell => OrderSide::Sell,
    }
}

fn map_exec_side(side: OrderSide) -> CoreOrderSide {
    match side {
        OrderSide::Buy => CoreOrderSide::Buy,
        OrderSide::Sell => CoreOrderSide::Sell,
    }
}

fn map_core_order_type(order_type: CoreOrderType) -> crate::core::types::Result<OrderType> {
    match order_type {
        CoreOrderType::Limit => Ok(OrderType::Limit),
        CoreOrderType::Market => Ok(OrderType::Market),
        _ => Err(ExchangeError::UnsupportedOrderType {
            order_type,
            exchange: "gateway".to_string(),
        }),
    }
}

fn map_exec_order_type(order_type: OrderType) -> CoreOrderType {
    match order_type {
        OrderType::Limit => CoreOrderType::Limit,
        OrderType::Market => CoreOrderType::Market,
    }
}

fn map_time_in_force(request: &OrderRequest) -> TimeInForce {
    if request.post_only.unwrap_or(false) {
        return TimeInForce::PostOnly;
    }
    match request
        .time_in_force
        .as_deref()
        .unwrap_or("GTC")
        .to_ascii_uppercase()
        .as_str()
    {
        "IOC" => TimeInForce::Ioc,
        "FOK" => TimeInForce::Fok,
        "GTX" | "POC" | "POST_ONLY" => TimeInForce::PostOnly,
        _ => TimeInForce::Gtc,
    }
}

fn parse_position_side(value: &str) -> PositionSide {
    match value.trim().to_ascii_uppercase().as_str() {
        "LONG" | "BUY" | "DUAL_LONG" => PositionSide::Long,
        "SHORT" | "SELL" | "DUAL_SHORT" => PositionSide::Short,
        _ => PositionSide::Net,
    }
}

fn default_position_side(side: OrderSide) -> PositionSide {
    match side {
        OrderSide::Buy => PositionSide::Long,
        OrderSide::Sell => PositionSide::Short,
    }
}

fn is_hedge_close(side: OrderSide, position_side: PositionSide) -> bool {
    matches!(
        (side, position_side),
        (OrderSide::Sell, PositionSide::Long) | (OrderSide::Buy, PositionSide::Short)
    )
}

fn intent_for(side: OrderSide, position_side: PositionSide, reduce_only: bool) -> OrderIntent {
    match (side, position_side, reduce_only) {
        (OrderSide::Buy, PositionSide::Long, false) => OrderIntent::OpenLongMaker,
        (OrderSide::Sell, PositionSide::Short, false) => OrderIntent::OpenShortMaker,
        (OrderSide::Sell, PositionSide::Long, true) => OrderIntent::CloseLongMaker,
        (OrderSide::Buy, PositionSide::Short, true) => OrderIntent::CloseShortMaker,
        (OrderSide::Buy, _, _) => OrderIntent::HedgeLongTaker,
        (OrderSide::Sell, _, _) => OrderIntent::HedgeShortTaker,
    }
}

fn order_status(status: OrderCommandStatus) -> OrderStatus {
    match status {
        OrderCommandStatus::PartiallyFilled => OrderStatus::PartiallyFilled,
        OrderCommandStatus::Filled => OrderStatus::Closed,
        OrderCommandStatus::CancelRequested | OrderCommandStatus::Cancelled => {
            OrderStatus::Canceled
        }
        OrderCommandStatus::Rejected | OrderCommandStatus::Failed => OrderStatus::Rejected,
        OrderCommandStatus::Planned
        | OrderCommandStatus::Submitted
        | OrderCommandStatus::Accepted => OrderStatus::Open,
    }
}

fn order_from_ack(
    command: &OrderCommand,
    exchange_order_id: Option<String>,
    status: OrderCommandStatus,
) -> Order {
    Order {
        id: exchange_order_id.unwrap_or_else(|| command.client_order_id.clone()),
        symbol: command.canonical_symbol.as_pair(),
        side: map_exec_side(command.side),
        order_type: map_exec_order_type(command.order_type),
        amount: command.quantity,
        price: command.price,
        filled: 0.0,
        remaining: command.quantity,
        status: order_status(status),
        market_type: MarketType::Futures,
        timestamp: Utc::now(),
        last_trade_timestamp: None,
        info: json!({
            "clientOrderId": command.client_order_id,
            "client_order_id": command.client_order_id,
            "positionSide": match command.position_side {
                PositionSide::Long => "LONG",
                PositionSide::Short => "SHORT",
                PositionSide::Net => "BOTH",
            },
            "reduceOnly": command.reduce_only,
        }),
    }
}

fn order_from_state(state: crate::execution::OrderState, market_type: MarketType) -> Order {
    Order {
        id: state
            .exchange_order_id
            .clone()
            .or_else(|| state.client_order_id.clone())
            .unwrap_or_default(),
        symbol: state.canonical_symbol.as_pair(),
        side: map_exec_side(state.side),
        order_type: map_exec_order_type(state.order_type),
        amount: state.quantity,
        price: state.price,
        filled: state.filled_quantity,
        remaining: (state.quantity - state.filled_quantity).max(0.0),
        status: order_status(state.status),
        market_type,
        timestamp: state.updated_at,
        last_trade_timestamp: None,
        info: json!({
            "clientOrderId": state.client_order_id,
            "client_order_id": state.client_order_id,
            "positionSide": match state.position_side {
                PositionSide::Long => "LONG",
                PositionSide::Short => "SHORT",
                PositionSide::Net => "BOTH",
            },
            "reduceOnly": state.reduce_only,
        }),
    }
}

fn trading_pair_from_instrument(instrument: &InstrumentMeta) -> TradingPair {
    TradingPair {
        symbol: instrument.canonical_symbol.as_pair(),
        base_asset: instrument.base.clone(),
        quote_asset: instrument.quote.clone(),
        status: format!("{:?}", instrument.status),
        min_order_size: instrument.min_qty,
        max_order_size: f64::MAX,
        tick_size: instrument.price_tick,
        step_size: instrument.quantity_step,
        min_notional: Some(instrument.min_notional),
        is_trading: instrument.status.allows_new_entries(),
        market_type: MarketType::Futures,
    }
}
