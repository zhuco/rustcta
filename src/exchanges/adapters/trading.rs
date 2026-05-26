use crate::core::exchange::Exchange;
use crate::core::types::{
    Balance as CoreBalance, MarketType, Order as CoreOrder, OrderRequest,
    OrderSide as CoreOrderSide, OrderStatus as CoreOrderStatus, OrderType as CoreOrderType,
    Position as CorePosition,
};
use crate::execution::{
    CancelAck, CancelCommand, ClosePositionAck, ClosePositionCommand, ExchangeBalance,
    ExchangePosition, LeverageAck, LeverageCommand, OrderAck, OrderCommand, OrderCommandStatus,
    OrderQuery, OrderSide, OrderState, OrderType, PositionMode, PositionModeAck,
    PositionModeCommand, PositionSide, TimeInForce, TradingAdapter, TradingCapabilities,
};
use crate::market::{
    canonical_from_exchange_symbol, exchange_symbol_for, CanonicalSymbol, ExchangeId,
    ExchangeSymbol, InstrumentMeta, RoundingMode,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub struct ExchangeTradingAdapter {
    exchange: Arc<dyn Exchange>,
    exchange_id: ExchangeId,
    capabilities: TradingCapabilities,
    private_trading_enabled: bool,
    disabled_reason: Option<&'static str>,
    instruments: HashMap<CanonicalSymbol, InstrumentMeta>,
    position_mode: Mutex<PositionMode>,
}

impl ExchangeTradingAdapter {
    pub fn new(exchange_id: ExchangeId, exchange: Arc<dyn Exchange>) -> Self {
        let support = private_trading_support_for(&exchange_id);
        Self {
            exchange,
            exchange_id,
            capabilities: support.capabilities,
            private_trading_enabled: support.private_trading_enabled,
            disabled_reason: support.disabled_reason,
            instruments: HashMap::new(),
            position_mode: Mutex::new(PositionMode::OneWay),
        }
    }

    pub fn with_instruments(
        mut self,
        instruments: impl IntoIterator<Item = InstrumentMeta>,
    ) -> Self {
        for instrument in instruments {
            if instrument.exchange == self.exchange_id {
                self.instruments
                    .insert(instrument.canonical_symbol.clone(), instrument);
            }
        }
        self
    }

    pub fn register_instrument(&mut self, instrument: InstrumentMeta) {
        if instrument.exchange == self.exchange_id {
            self.instruments
                .insert(instrument.canonical_symbol.clone(), instrument);
        }
    }

    pub fn with_position_mode(mut self, position_mode: PositionMode) -> Self {
        *self
            .position_mode
            .get_mut()
            .expect("position mode mutex poisoned") = position_mode;
        self
    }

    fn instrument_for(&self, canonical_symbol: &CanonicalSymbol) -> Option<&InstrumentMeta> {
        self.instruments.get(canonical_symbol)
    }

    fn ensure_private_trading_enabled(&self, operation: &str) -> Result<()> {
        if self.private_trading_enabled {
            return Ok(());
        }

        Err(anyhow!(
            "{} private trading adapter is disabled for {}: {}",
            self.exchange_id,
            operation,
            self.disabled_reason
                .unwrap_or("exchange is not enabled in the private trading adapter registry")
        ))
    }

    fn ensure_order_type_supported(&self, order_type: OrderType) -> Result<()> {
        match order_type {
            OrderType::Market if !self.capabilities.supports_market_orders => Err(anyhow!(
                "{} trading adapter does not support market orders",
                self.exchange_id
            )),
            OrderType::Limit if !self.capabilities.supports_limit_orders => Err(anyhow!(
                "{} trading adapter does not support limit orders",
                self.exchange_id
            )),
            _ => Ok(()),
        }
    }

    fn ensure_close_position_supported(&self) -> Result<()> {
        if self.capabilities.supports_close_position {
            Ok(())
        } else {
            Err(anyhow!(
                "{} trading adapter does not support close position",
                self.exchange_id
            ))
        }
    }

    fn ensure_leverage_supported(&self) -> Result<()> {
        if self.capabilities.supports_leverage {
            Ok(())
        } else {
            Err(anyhow!(
                "{} trading adapter does not support leverage",
                self.exchange_id
            ))
        }
    }

    fn ensure_position_mode_supported(&self) -> Result<()> {
        if self.capabilities.supports_position_mode_change {
            Ok(())
        } else {
            Err(anyhow!(
                "{} trading adapter does not support position mode changes",
                self.exchange_id
            ))
        }
    }
}

#[async_trait]
impl TradingAdapter for ExchangeTradingAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> TradingCapabilities {
        self.capabilities.clone()
    }

    async fn place_order(&self, command: OrderCommand) -> Result<OrderAck> {
        self.ensure_private_trading_enabled("place_order")?;
        self.ensure_order_type_supported(command.order_type)?;
        let position_mode = *self
            .position_mode
            .lock()
            .map_err(|_| anyhow!("position mode mutex poisoned"))?;
        let request = command_to_order_request(
            self.exchange_id.clone(),
            position_mode,
            &command,
            self.instrument_for(&command.canonical_symbol),
        )?;
        let order = self.exchange.create_order(request).await?;

        Ok(OrderAck {
            exchange: self.exchange_id.clone(),
            client_order_id: command.client_order_id,
            exchange_order_id: Some(order.id),
            accepted: true,
            status: map_order_status(&order.status),
            message: None,
            acknowledged_at: Utc::now(),
        })
    }

    async fn cancel_order(&self, command: CancelCommand) -> Result<CancelAck> {
        self.ensure_private_trading_enabled("cancel_order")?;
        let order_id = command
            .exchange_order_id
            .as_deref()
            .or(command.client_order_id.as_deref())
            .ok_or_else(|| anyhow!("cancel requires exchange_order_id or client_order_id"))?;
        let order = self
            .exchange
            .cancel_order(
                order_id,
                &command.exchange_symbol.symbol,
                MarketType::Futures,
            )
            .await?;

        Ok(CancelAck {
            exchange: self.exchange_id.clone(),
            client_order_id: command.client_order_id,
            exchange_order_id: Some(order.id),
            accepted: true,
            status: map_order_status(&order.status),
            message: None,
            acknowledged_at: Utc::now(),
        })
    }

    async fn get_order(&self, query: OrderQuery) -> Result<OrderState> {
        self.ensure_private_trading_enabled("get_order")?;
        let order_id = query
            .exchange_order_id
            .as_deref()
            .or(query.client_order_id.as_deref())
            .ok_or_else(|| anyhow!("order query requires exchange_order_id or client_order_id"))?;
        let order = self
            .exchange
            .get_order(order_id, &query.exchange_symbol.symbol, MarketType::Futures)
            .await?;

        Ok(map_order_state(
            self.exchange_id.clone(),
            query.exchange_symbol,
            query.client_order_id,
            Some(order.id.clone()),
            order,
        ))
    }

    async fn get_open_orders(&self, symbol: Option<&ExchangeSymbol>) -> Result<Vec<OrderState>> {
        self.ensure_private_trading_enabled("get_open_orders")?;
        let orders = self
            .exchange
            .get_open_orders(
                symbol.map(|symbol| symbol.symbol.as_str()),
                MarketType::Futures,
            )
            .await?;

        Ok(orders
            .into_iter()
            .map(|order| {
                let exchange_symbol =
                    ExchangeSymbol::new(self.exchange_id.clone(), order.symbol.clone());
                map_order_state(
                    self.exchange_id.clone(),
                    exchange_symbol,
                    None,
                    Some(order.id.clone()),
                    order,
                )
            })
            .collect())
    }

    async fn get_positions(
        &self,
        symbol: Option<&ExchangeSymbol>,
    ) -> Result<Vec<ExchangePosition>> {
        self.ensure_private_trading_enabled("get_positions")?;
        let positions = self
            .exchange
            .get_positions(symbol.map(|symbol| symbol.symbol.as_str()))
            .await?;
        Ok(positions
            .into_iter()
            .map(|position| map_position(self.exchange_id.clone(), position))
            .collect())
    }

    async fn get_balances(&self) -> Result<Vec<ExchangeBalance>> {
        self.ensure_private_trading_enabled("get_balances")?;
        let balances = self.exchange.get_balance(MarketType::Futures).await?;
        Ok(balances
            .into_iter()
            .map(|balance| map_balance(self.exchange_id.clone(), balance))
            .collect())
    }

    async fn set_leverage(&self, command: LeverageCommand) -> Result<LeverageAck> {
        self.ensure_private_trading_enabled("set_leverage")?;
        self.ensure_leverage_supported()?;
        self.exchange
            .set_leverage(&command.canonical_symbol.as_pair(), command.leverage)
            .await?;

        Ok(LeverageAck {
            exchange: self.exchange_id.clone(),
            canonical_symbol: command.canonical_symbol,
            exchange_symbol: command.exchange_symbol,
            leverage: command.leverage,
            accepted: true,
            message: None,
            acknowledged_at: Utc::now(),
        })
    }

    async fn set_position_mode(&self, command: PositionModeCommand) -> Result<PositionModeAck> {
        self.ensure_private_trading_enabled("set_position_mode")?;
        self.ensure_position_mode_supported()?;
        self.exchange
            .set_position_mode(command.mode.is_hedge())
            .await?;
        *self
            .position_mode
            .lock()
            .map_err(|_| anyhow!("position mode mutex poisoned"))? = command.mode;

        Ok(PositionModeAck {
            exchange: self.exchange_id.clone(),
            mode: command.mode,
            accepted: true,
            message: None,
            acknowledged_at: Utc::now(),
        })
    }

    async fn close_position(&self, command: ClosePositionCommand) -> Result<ClosePositionAck> {
        self.ensure_private_trading_enabled("close_position")?;
        self.ensure_close_position_supported()?;
        self.ensure_order_type_supported(command.order_type)?;
        let position_mode = *self
            .position_mode
            .lock()
            .map_err(|_| anyhow!("position mode mutex poisoned"))?;
        let request = close_to_order_request(
            self.exchange_id.clone(),
            position_mode,
            &command,
            self.instrument_for(&command.canonical_symbol),
        )?;
        let order = self.exchange.create_order(request).await?;

        Ok(ClosePositionAck {
            exchange: self.exchange_id.clone(),
            client_order_id: command.client_order_id,
            exchange_order_id: Some(order.id),
            accepted: true,
            status: map_order_status(&order.status),
            message: None,
            acknowledged_at: Utc::now(),
        })
    }

    async fn load_symbol_rules(&self, symbol: &ExchangeSymbol) -> Result<Option<InstrumentMeta>> {
        if let Some(canonical) = canonical_from_exchange_symbol(&symbol.exchange, &symbol.symbol) {
            if let Some(instrument) = self.instruments.get(&canonical) {
                return Ok(Some(instrument.clone()));
            }
        }

        self.ensure_private_trading_enabled("load_symbol_rules")?;

        let trading_pair = self
            .exchange
            .get_symbol_info(
                &symbol_to_canonical(&symbol.symbol).as_pair(),
                MarketType::Futures,
            )
            .await?;
        Ok(Some(InstrumentMeta::new(
            self.exchange_id.clone(),
            symbol_to_canonical(&trading_pair.symbol),
            exchange_symbol_for(
                &self.exchange_id,
                &symbol_to_canonical(&trading_pair.symbol),
            ),
            trading_pair.base_asset,
            trading_pair.quote_asset,
            "USDT",
            crate::market::ContractType::LinearPerpetual,
            1.0,
            trading_pair.tick_size,
            trading_pair.step_size,
            trading_pair.min_order_size,
            trading_pair.min_notional.unwrap_or(0.0),
            crate::market::decimal_places(trading_pair.tick_size),
            crate::market::decimal_places(trading_pair.step_size),
            if trading_pair.is_trading {
                crate::market::InstrumentStatus::Trading
            } else {
                crate::market::InstrumentStatus::Paused
            },
        )))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivateTradingSupport {
    pub exchange: ExchangeId,
    pub private_trading_enabled: bool,
    pub capabilities: TradingCapabilities,
    pub disabled_reason: Option<&'static str>,
}

pub fn private_trading_support_for(exchange: &ExchangeId) -> PrivateTradingSupport {
    match exchange {
        ExchangeId::Binance | ExchangeId::Okx => PrivateTradingSupport {
            exchange: exchange.clone(),
            private_trading_enabled: true,
            capabilities: TradingCapabilities::default(),
            disabled_reason: None,
        },
        ExchangeId::Bitget | ExchangeId::Gate => PrivateTradingSupport {
            exchange: exchange.clone(),
            private_trading_enabled: false,
            capabilities: disabled_trading_capabilities(),
            disabled_reason: Some(
                "core Exchange private trading implementation is not wired; public market adapter only",
            ),
        },
        ExchangeId::Other(_) => PrivateTradingSupport {
            exchange: exchange.clone(),
            private_trading_enabled: false,
            capabilities: disabled_trading_capabilities(),
            disabled_reason: Some("exchange is not registered for private trading"),
        },
    }
}

fn disabled_trading_capabilities() -> TradingCapabilities {
    TradingCapabilities {
        supports_market_orders: false,
        supports_limit_orders: false,
        supports_post_only: false,
        supports_ioc: false,
        supports_fok: false,
        supports_reduce_only: false,
        supports_hedge_mode: false,
        supports_client_order_id: false,
        supports_leverage: false,
        supports_position_mode_change: false,
        supports_close_position: false,
    }
}

fn command_to_order_request(
    exchange: ExchangeId,
    position_mode: PositionMode,
    command: &OrderCommand,
    instrument: Option<&InstrumentMeta>,
) -> Result<OrderRequest> {
    let (quantity, price) = normalize_order_fields(
        instrument,
        command.quantity,
        command.price,
        command.order_type,
    )?;
    let mut params = HashMap::new();
    if let Some(position_side) =
        position_side_param(&exchange, position_mode, command.position_side)
    {
        params.insert("positionSide".to_string(), position_side.to_string());
    }

    Ok(OrderRequest {
        symbol: command.canonical_symbol.as_pair(),
        side: map_core_side(command.side),
        order_type: map_core_order_type(command.order_type),
        amount: quantity,
        price,
        market_type: MarketType::Futures,
        params: Some(params),
        client_order_id: Some(command.client_order_id.clone()),
        time_in_force: Some(map_time_in_force(command.time_in_force).to_string()),
        reduce_only: Some(command.reduce_only),
        post_only: Some(
            command.post_only || matches!(command.time_in_force, TimeInForce::PostOnly),
        ),
    })
}

fn close_to_order_request(
    exchange: ExchangeId,
    position_mode: PositionMode,
    command: &ClosePositionCommand,
    instrument: Option<&InstrumentMeta>,
) -> Result<OrderRequest> {
    let (quantity, price) = normalize_order_fields(
        instrument,
        command.quantity,
        command.price,
        command.order_type,
    )?;
    let mut params = HashMap::new();
    if let Some(position_side) =
        position_side_param(&exchange, position_mode, command.position_side)
    {
        params.insert("positionSide".to_string(), position_side.to_string());
    }

    Ok(OrderRequest {
        symbol: command.canonical_symbol.as_pair(),
        side: map_core_side(command.order_side()),
        order_type: map_core_order_type(command.order_type),
        amount: quantity,
        price,
        market_type: MarketType::Futures,
        params: Some(params),
        client_order_id: Some(command.client_order_id.clone()),
        time_in_force: Some(map_time_in_force(command.time_in_force).to_string()),
        reduce_only: should_send_reduce_only(&exchange, position_mode).then_some(true),
        post_only: Some(matches!(command.time_in_force, TimeInForce::PostOnly)),
    })
}

fn position_side_param(
    exchange: &ExchangeId,
    position_mode: PositionMode,
    position_side: PositionSide,
) -> Option<&'static str> {
    match (exchange, position_mode, position_side) {
        (ExchangeId::Binance, PositionMode::OneWay, _) => None,
        (ExchangeId::Okx, PositionMode::OneWay, _) => None,
        (_, PositionMode::OneWay, _) => None,
        (_, PositionMode::Hedge, PositionSide::Long) => Some("LONG"),
        (_, PositionMode::Hedge, PositionSide::Short) => Some("SHORT"),
        (_, PositionMode::Hedge, PositionSide::Net) => Some("BOTH"),
    }
}

fn should_send_reduce_only(exchange: &ExchangeId, position_mode: PositionMode) -> bool {
    !matches!(
        (exchange, position_mode),
        (ExchangeId::Binance, PositionMode::Hedge)
    )
}

fn normalize_order_fields(
    instrument: Option<&InstrumentMeta>,
    quantity: f64,
    price: Option<f64>,
    order_type: OrderType,
) -> Result<(f64, Option<f64>)> {
    let Some(instrument) = instrument else {
        return Ok((quantity, price));
    };
    let normalized = instrument.normalize_order_input(
        quantity,
        price,
        RoundingMode::Floor,
        RoundingMode::Nearest,
    );
    if !normalized.is_valid() {
        return Err(anyhow!(
            "order violates precision rules for {}: {:?}",
            instrument.exchange_symbol.symbol,
            normalized.violations
        ));
    }
    if matches!(order_type, OrderType::Limit) && normalized.price.is_none() {
        return Err(anyhow!("limit order requires price"));
    }
    Ok((normalized.quantity, normalized.price))
}

fn map_core_side(side: OrderSide) -> CoreOrderSide {
    match side {
        OrderSide::Buy => CoreOrderSide::Buy,
        OrderSide::Sell => CoreOrderSide::Sell,
    }
}

fn map_core_order_type(order_type: OrderType) -> CoreOrderType {
    match order_type {
        OrderType::Limit => CoreOrderType::Limit,
        OrderType::Market => CoreOrderType::Market,
    }
}

fn map_time_in_force(time_in_force: TimeInForce) -> &'static str {
    match time_in_force {
        TimeInForce::Gtc => "GTC",
        TimeInForce::Ioc => "IOC",
        TimeInForce::Fok => "FOK",
        TimeInForce::PostOnly => "GTX",
    }
}

fn map_order_status(status: &CoreOrderStatus) -> OrderCommandStatus {
    match status {
        CoreOrderStatus::Open => OrderCommandStatus::Accepted,
        CoreOrderStatus::Pending | CoreOrderStatus::Triggered => OrderCommandStatus::Submitted,
        CoreOrderStatus::PartiallyFilled => OrderCommandStatus::PartiallyFilled,
        CoreOrderStatus::Closed => OrderCommandStatus::Filled,
        CoreOrderStatus::Canceled | CoreOrderStatus::Expired => OrderCommandStatus::Cancelled,
        CoreOrderStatus::Rejected => OrderCommandStatus::Rejected,
    }
}

fn map_execution_side(side: CoreOrderSide) -> OrderSide {
    match side {
        CoreOrderSide::Buy => OrderSide::Buy,
        CoreOrderSide::Sell => OrderSide::Sell,
    }
}

fn map_execution_order_type(order_type: CoreOrderType) -> OrderType {
    match order_type {
        CoreOrderType::Market => OrderType::Market,
        _ => OrderType::Limit,
    }
}

fn map_order_state(
    exchange: ExchangeId,
    exchange_symbol: ExchangeSymbol,
    client_order_id: Option<String>,
    exchange_order_id: Option<String>,
    order: CoreOrder,
) -> OrderState {
    OrderState {
        exchange,
        canonical_symbol: symbol_to_canonical(&exchange_symbol.symbol),
        exchange_symbol,
        client_order_id,
        exchange_order_id,
        side: map_execution_side(order.side),
        position_side: extract_position_side(&order.info),
        order_type: map_execution_order_type(order.order_type),
        quantity: order.amount,
        price: order.price,
        filled_quantity: order.filled,
        average_fill_price: extract_average_fill_price(&order),
        time_in_force: TimeInForce::Gtc,
        reduce_only: false,
        status: map_order_status(&order.status),
        updated_at: order.last_trade_timestamp.unwrap_or(order.timestamp),
    }
}

fn map_position(exchange: ExchangeId, position: CorePosition) -> ExchangePosition {
    let exchange_symbol = ExchangeSymbol::new(exchange.clone(), position.symbol.clone());
    let position_side = match position.side.to_ascii_uppercase().as_str() {
        "LONG" | "BUY" => PositionSide::Long,
        "SHORT" | "SELL" => PositionSide::Short,
        _ => PositionSide::Net,
    };
    let quantity = if position.contracts != 0.0 {
        position.contracts.abs()
    } else if position.size != 0.0 {
        position.size.abs()
    } else {
        position.amount.abs()
    };

    ExchangePosition {
        exchange,
        canonical_symbol: symbol_to_canonical(&exchange_symbol.symbol),
        exchange_symbol,
        position_side,
        quantity,
        entry_price: Some(position.entry_price),
        mark_price: Some(position.mark_price),
        unrealized_pnl: Some(position.unrealized_pnl),
        updated_at: position.timestamp,
    }
}

fn map_balance(exchange: ExchangeId, balance: CoreBalance) -> ExchangeBalance {
    ExchangeBalance {
        exchange,
        asset: balance.currency,
        total: balance.total,
        available: balance.free,
        locked: balance.used,
        updated_at: Utc::now(),
    }
}

fn symbol_to_canonical(symbol: &str) -> CanonicalSymbol {
    if let Some(canonical) =
        canonical_from_exchange_symbol(&ExchangeId::Other("generic".into()), symbol)
    {
        return canonical;
    }
    CanonicalSymbol::new(symbol, "USDT")
}

fn extract_position_side(info: &serde_json::Value) -> PositionSide {
    let side = info
        .get("positionSide")
        .or_else(|| info.get("position_side"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("BOTH")
        .to_ascii_uppercase();
    match side.as_str() {
        "LONG" => PositionSide::Long,
        "SHORT" => PositionSide::Short,
        _ => PositionSide::Net,
    }
}

fn extract_average_fill_price(order: &CoreOrder) -> Option<f64> {
    order
        .info
        .get("avgPrice")
        .or_else(|| order.info.get("average"))
        .or_else(|| order.info.get("avgFillPrice"))
        .and_then(|value| match value {
            serde_json::Value::Number(number) => number.as_f64(),
            serde_json::Value::String(text) => text.parse().ok(),
            _ => None,
        })
        .or(order.price)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchanges::MockExchange;
    use crate::execution::{BundleLeg, OrderIntent};
    use crate::market::{ContractType, InstrumentStatus};
    use std::sync::Arc;

    fn limit_order_command(exchange: ExchangeId, exchange_symbol: &str) -> OrderCommand {
        OrderCommand::new(
            crate::market::RuntimeMode::LiveSmall,
            "bundle-1",
            BundleLeg::Maker,
            1,
            exchange.clone(),
            CanonicalSymbol::new("BTC", "USDT"),
            ExchangeSymbol::new(exchange, exchange_symbol),
            OrderIntent::OpenLongMaker,
            OrderSide::Buy,
            PositionSide::Long,
            OrderType::Limit,
            0.001,
            Some(65000.0),
            TimeInForce::PostOnly,
            true,
            false,
            None,
            Utc::now(),
        )
    }

    #[test]
    fn trading_adapter_should_map_basic_order_fields() {
        assert_eq!(map_core_side(OrderSide::Buy), CoreOrderSide::Buy);
        assert_eq!(
            map_core_order_type(OrderType::Market),
            CoreOrderType::Market
        );
        assert_eq!(map_time_in_force(TimeInForce::PostOnly), "GTX");
    }

    #[test]
    fn trading_adapter_registry_should_disable_unwired_private_exchanges() {
        let bitget = private_trading_support_for(&ExchangeId::Bitget);
        assert!(!bitget.private_trading_enabled);
        assert!(!bitget.capabilities.supports_limit_orders);
        assert!(!bitget.capabilities.supports_market_orders);
        assert!(!bitget.capabilities.supports_leverage);
        assert!(bitget
            .disabled_reason
            .unwrap()
            .contains("public market adapter only"));

        let gate = private_trading_support_for(&ExchangeId::Gate);
        assert!(!gate.private_trading_enabled);
        assert!(!gate.capabilities.supports_close_position);

        let binance = private_trading_support_for(&ExchangeId::Binance);
        assert!(binance.private_trading_enabled);
        assert!(binance.capabilities.supports_limit_orders);
    }

    #[tokio::test]
    async fn trading_adapter_should_guard_bitget_before_core_order_call() {
        let adapter =
            ExchangeTradingAdapter::new(ExchangeId::Bitget, Arc::new(MockExchange::new("bitget")));

        let err = adapter
            .place_order(limit_order_command(ExchangeId::Bitget, "BTCUSDT"))
            .await
            .expect_err("bitget private trading should be disabled");

        assert!(err.to_string().contains("disabled"));
        assert!(err.to_string().contains("place_order"));
        assert!(!adapter.capabilities().supports_limit_orders);
    }

    #[test]
    fn trading_adapter_should_normalize_symbols_to_canonical() {
        assert_eq!(symbol_to_canonical("BTC-USDT-SWAP").as_pair(), "BTC/USDT");
        assert_eq!(symbol_to_canonical("ETH_USDT").as_pair(), "ETH/USDT");
    }

    #[test]
    fn trading_adapter_should_build_reduce_only_close_order() {
        let command = ClosePositionCommand::market(
            ExchangeId::Binance,
            CanonicalSymbol::new("BTC", "USDT"),
            ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT"),
            PositionSide::Long,
            0.1234,
            "close-1",
            Utc::now(),
        );

        let request =
            close_to_order_request(ExchangeId::Binance, PositionMode::Hedge, &command, None)
                .expect("close request");

        assert_eq!(request.symbol, "BTC/USDT");
        assert_eq!(request.side, CoreOrderSide::Sell);
        assert_eq!(request.reduce_only, None);
        assert_eq!(
            request
                .params
                .as_ref()
                .and_then(|params| params.get("positionSide"))
                .map(String::as_str),
            Some("LONG")
        );
    }

    #[test]
    fn trading_adapter_should_apply_precision_rules_before_order_send() {
        let instrument = InstrumentMeta::new(
            ExchangeId::Binance,
            CanonicalSymbol::new("BTC", "USDT"),
            ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT"),
            "BTC",
            "USDT",
            "USDT",
            ContractType::LinearPerpetual,
            1.0,
            0.1,
            0.001,
            0.001,
            5.0,
            1,
            3,
            InstrumentStatus::Trading,
        );
        let mut command = limit_order_command(ExchangeId::Binance, "BTCUSDT");
        command.quantity = 0.00149;
        command.price = Some(65000.04);

        let request = command_to_order_request(
            ExchangeId::Binance,
            PositionMode::OneWay,
            &command,
            Some(&instrument),
        )
        .expect("normalized request");

        assert_eq!(request.symbol, "BTC/USDT");
        assert_eq!(request.amount, 0.001);
        assert_eq!(request.price, Some(65000.0));
        assert_eq!(request.post_only, Some(true));
        assert_eq!(request.reduce_only, Some(false));
    }

    #[test]
    fn trading_adapter_should_separate_one_way_and_hedge_params() {
        assert_eq!(
            position_side_param(
                &ExchangeId::Binance,
                PositionMode::OneWay,
                PositionSide::Long
            ),
            None
        );
        assert_eq!(
            position_side_param(
                &ExchangeId::Binance,
                PositionMode::Hedge,
                PositionSide::Short
            ),
            Some("SHORT")
        );
        assert!(!should_send_reduce_only(
            &ExchangeId::Binance,
            PositionMode::Hedge
        ));
        assert!(should_send_reduce_only(
            &ExchangeId::Binance,
            PositionMode::OneWay
        ));
    }
}
