use crate::core::types::{
    Balance as WsBalance, ExecutionReport as WsExecutionReport, Order as WsOrder, OrderStatus,
    Position as WsPosition, Trade as WsTrade, WsMessage,
};
use crate::execution::{
    classify_position_drift, recommended_actions_for_severity, ExchangeBalance, ExchangeErrorClass,
    ExchangePosition, FillEvent, FillLiquidity, OrderCommandStatus, OrderSide, OrderSnapshot,
    OrderState, OrderType, PositionSide, PositionSnapshot, RateLimitState, ReconcileReport,
    TimeInForce,
};
use crate::market::{canonical_from_exchange_symbol, CanonicalSymbol, ExchangeId, ExchangeSymbol};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrivateEvent {
    pub exchange: ExchangeId,
    pub account_id: Option<String>,
    pub event_id: Option<String>,
    pub exchange_sequence: Option<u64>,
    pub transaction_time: Option<DateTime<Utc>>,
    pub received_at: DateTime<Utc>,
    pub kind: PrivateEventKind,
    pub raw: Option<serde_json::Value>,
}

impl PrivateEvent {
    pub fn new(exchange: ExchangeId, kind: PrivateEventKind, received_at: DateTime<Utc>) -> Self {
        Self {
            exchange,
            account_id: None,
            event_id: None,
            exchange_sequence: None,
            transaction_time: None,
            received_at,
            kind,
            raw: None,
        }
    }

    pub fn order(order: OrderState, received_at: DateTime<Utc>) -> Self {
        Self::new(
            order.exchange.clone(),
            PrivateEventKind::Order(order),
            received_at,
        )
    }

    pub fn fill(fill: FillEvent, received_at: DateTime<Utc>) -> Self {
        Self::new(
            fill.exchange.clone(),
            PrivateEventKind::Fill(fill),
            received_at,
        )
    }

    pub fn position(position: ExchangePosition, received_at: DateTime<Utc>) -> Self {
        Self::new(
            position.exchange.clone(),
            PrivateEventKind::Position(position),
            received_at,
        )
    }

    pub fn balance(balance: ExchangeBalance, received_at: DateTime<Utc>) -> Self {
        Self::new(
            balance.exchange.clone(),
            PrivateEventKind::Balance(balance),
            received_at,
        )
    }

    pub fn with_account_id(mut self, account_id: impl Into<String>) -> Self {
        self.account_id = Some(account_id.into());
        self
    }

    pub fn with_event_id(mut self, event_id: impl Into<String>) -> Self {
        self.event_id = Some(event_id.into());
        self
    }

    pub fn with_exchange_sequence(mut self, exchange_sequence: u64) -> Self {
        self.exchange_sequence = Some(exchange_sequence);
        self
    }

    pub fn with_raw(mut self, raw: serde_json::Value) -> Self {
        self.raw = Some(raw);
        self
    }

    pub fn from_ws_message(
        exchange: ExchangeId,
        message: WsMessage,
        received_at: DateTime<Utc>,
    ) -> Option<Self> {
        match message {
            WsMessage::ExecutionReport(report) => Some(Self::order(
                OrderState::from_execution_report(exchange, report),
                received_at,
            )),
            WsMessage::Order(order) => Some(Self::order(
                OrderState::from_ws_order(exchange, order),
                received_at,
            )),
            WsMessage::Position(position) => Some(Self::position(
                ExchangePosition::from_ws_position(exchange, position),
                received_at,
            )),
            WsMessage::Balance(balance) => Some(Self::balance(
                ExchangeBalance::from_ws_balance(exchange, balance, received_at),
                received_at,
            )),
            WsMessage::Trade(trade) => Some(Self::fill(
                FillEvent::from_ws_trade(exchange, trade),
                received_at,
            )),
            WsMessage::Error(message) => Some(Self::new(
                exchange,
                PrivateEventKind::Error(PrivateErrorEvent {
                    class: ExchangeErrorClass::Unknown,
                    endpoint: None,
                    code: None,
                    message,
                    client_order_id: None,
                    exchange_order_id: None,
                    retry_after_ms: None,
                    occurred_at: received_at,
                }),
                received_at,
            )),
            WsMessage::Text(_) => None,
            WsMessage::Kline(_) | WsMessage::Ticker(_) | WsMessage::OrderBook(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PrivateEventKind {
    Order(OrderState),
    Fill(FillEvent),
    Position(ExchangePosition),
    Balance(ExchangeBalance),
    FundingSettlement {
        canonical_symbol: CanonicalSymbol,
        position_side: PositionSide,
        funding_pnl_usdt: f64,
        funding_rate: f64,
        settled_at: DateTime<Utc>,
    },
    Error(PrivateErrorEvent),
    RateLimit(RateLimitState),
    Heartbeat,
    StreamDisconnected {
        reason: Option<String>,
        disconnected_at: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrivateErrorEvent {
    pub class: ExchangeErrorClass,
    pub endpoint: Option<String>,
    pub code: Option<String>,
    pub message: String,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub retry_after_ms: Option<u64>,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PrivateStreamEvent {
    OrderUpdate(OrderSnapshot),
    PositionUpdate(PositionSnapshot),
    FundingSettlement {
        exchange: ExchangeId,
        canonical_symbol: CanonicalSymbol,
        position_side: PositionSide,
        funding_pnl_usdt: f64,
        funding_rate: f64,
        settled_at: DateTime<Utc>,
    },
    Heartbeat {
        exchange: ExchangeId,
        received_at: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountSyncConfig {
    pub quantity_tolerance: f64,
    pub orphan_tolerance: f64,
    pub stale_after_ms: i64,
}

impl Default for AccountSyncConfig {
    fn default() -> Self {
        Self {
            quantity_tolerance: 1e-8,
            orphan_tolerance: 1e-6,
            stale_after_ms: 10_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountSyncState {
    config: AccountSyncConfig,
    positions: HashMap<PositionKey, PositionSnapshot>,
    open_orders: HashMap<OrderKey, OrderSnapshot>,
    balances: HashMap<BalanceKey, ExchangeBalance>,
    last_event_at: HashMap<ExchangeId, DateTime<Utc>>,
    last_disconnect_at: HashMap<ExchangeId, DateTime<Utc>>,
    funding_events: Vec<PrivateStreamEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct PositionKey {
    exchange: ExchangeId,
    canonical_symbol: CanonicalSymbol,
    position_side: PositionSide,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct OrderKey {
    exchange: ExchangeId,
    canonical_symbol: CanonicalSymbol,
    id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct BalanceKey {
    exchange: ExchangeId,
    asset: String,
}

impl AccountSyncState {
    pub fn new(config: AccountSyncConfig) -> Self {
        Self {
            config,
            positions: HashMap::new(),
            open_orders: HashMap::new(),
            balances: HashMap::new(),
            last_event_at: HashMap::new(),
            last_disconnect_at: HashMap::new(),
            funding_events: Vec::new(),
        }
    }

    pub fn apply_event(&mut self, event: PrivateStreamEvent) {
        match event {
            PrivateStreamEvent::OrderUpdate(order) => {
                self.last_event_at
                    .insert(order.exchange.clone(), order.updated_at);
                let key = OrderKey::from_snapshot(&order);
                if order.is_open() {
                    self.open_orders.insert(key, order);
                } else {
                    self.open_orders.remove(&key);
                }
            }
            PrivateStreamEvent::PositionUpdate(position) => {
                self.last_event_at
                    .insert(position.exchange.clone(), position.updated_at);
                self.positions
                    .insert(PositionKey::from_snapshot(&position), position);
            }
            PrivateStreamEvent::FundingSettlement { ref exchange, .. } => {
                self.last_event_at.insert(exchange.clone(), Utc::now());
                self.funding_events.push(event);
            }
            PrivateStreamEvent::Heartbeat {
                exchange,
                received_at,
            } => {
                self.last_event_at.insert(exchange, received_at);
            }
        }
    }

    pub fn apply_private_event(&mut self, event: PrivateEvent) {
        let exchange = event.exchange.clone();
        let received_at = event.received_at;
        self.last_event_at.insert(exchange.clone(), received_at);
        self.last_disconnect_at.remove(&exchange);
        match event.kind {
            PrivateEventKind::Order(order) => {
                let snapshot = OrderSnapshot::from_order_state(order);
                self.apply_event(PrivateStreamEvent::OrderUpdate(snapshot));
            }
            PrivateEventKind::Fill(fill) => {
                let key = OrderKey::from_fill(&fill);
                let snapshot = OrderSnapshot::from_fill(fill.clone());
                let existed = self.open_orders.contains_key(&key);
                let mut should_remove = false;
                self.open_orders
                    .entry(key)
                    .and_modify(|existing| {
                        existing.client_order_id = existing
                            .client_order_id
                            .clone()
                            .or_else(|| fill.client_order_id.clone());
                        existing.exchange_order_id = existing
                            .exchange_order_id
                            .clone()
                            .or_else(|| fill.exchange_order_id.clone());
                        existing.filled_quantity =
                            (existing.filled_quantity + fill.quantity).min(existing.quantity);
                        existing.status = if existing.filled_quantity >= existing.quantity {
                            OrderCommandStatus::Filled
                        } else {
                            OrderCommandStatus::PartiallyFilled
                        };
                        existing.updated_at = fill.filled_at;
                        should_remove = existing.status == OrderCommandStatus::Filled;
                    })
                    .or_insert(snapshot);
                if existed && should_remove {
                    self.open_orders.remove(&OrderKey::from_fill(&fill));
                }
            }
            PrivateEventKind::Position(position) => {
                let snapshot = PositionSnapshot::from_exchange_position(position);
                self.apply_event(PrivateStreamEvent::PositionUpdate(snapshot));
            }
            PrivateEventKind::Balance(balance) => {
                self.apply_balance_event(balance, received_at);
            }
            PrivateEventKind::FundingSettlement {
                canonical_symbol,
                position_side,
                funding_pnl_usdt,
                funding_rate,
                settled_at,
            } => self.apply_event(PrivateStreamEvent::FundingSettlement {
                exchange: exchange.clone(),
                canonical_symbol,
                position_side,
                funding_pnl_usdt,
                funding_rate,
                settled_at,
            }),
            PrivateEventKind::Error(_) => {}
            PrivateEventKind::RateLimit(_) => {}
            PrivateEventKind::Heartbeat => {
                self.last_event_at.insert(exchange, received_at);
            }
            PrivateEventKind::StreamDisconnected { reason, .. } => {
                let _ = reason;
                self.last_disconnect_at.insert(exchange, received_at);
            }
        }
    }

    pub fn needs_resync(&self, exchange: &ExchangeId, now: DateTime<Utc>) -> bool {
        self.last_disconnect_at.contains_key(exchange)
            || self.is_stale(exchange, now)
            || self
                .last_event_at
                .get(exchange)
                .map(|last| now.signed_duration_since(*last).num_seconds() > 60)
                .unwrap_or(true)
    }

    pub fn account_snapshot(&self) -> Vec<ExchangeBalance> {
        self.balances.values().cloned().collect()
    }

    pub fn balance(&self, exchange: &ExchangeId, asset: &str) -> Option<&ExchangeBalance> {
        self.balances.get(&BalanceKey {
            exchange: exchange.clone(),
            asset: asset.to_string(),
        })
    }

    fn apply_balance_event(&mut self, balance: ExchangeBalance, received_at: DateTime<Utc>) {
        let key = BalanceKey {
            exchange: balance.exchange.clone(),
            asset: balance.asset.clone(),
        };
        self.balances.insert(
            key,
            ExchangeBalance {
                updated_at: received_at,
                ..balance
            },
        );
    }

    pub fn position(
        &self,
        exchange: &ExchangeId,
        symbol: &CanonicalSymbol,
        side: PositionSide,
    ) -> Option<&PositionSnapshot> {
        self.positions.get(&PositionKey {
            exchange: exchange.clone(),
            canonical_symbol: symbol.clone(),
            position_side: side,
        })
    }

    pub fn open_orders(&self) -> Vec<&OrderSnapshot> {
        self.open_orders.values().collect()
    }

    pub fn is_stale(&self, exchange: &ExchangeId, now: DateTime<Utc>) -> bool {
        self.last_event_at
            .get(exchange)
            .map(|last| {
                now.signed_duration_since(*last).num_milliseconds() > self.config.stale_after_ms
            })
            .unwrap_or(true)
    }

    pub fn reconcile_position(
        &self,
        local_position: Option<&PositionSnapshot>,
        exchange_position: Option<&PositionSnapshot>,
        exchange: ExchangeId,
        symbol: CanonicalSymbol,
        checked_at: DateTime<Utc>,
    ) -> ReconcileReport {
        let severity = classify_position_drift(
            local_position,
            exchange_position,
            self.config.quantity_tolerance,
            self.config.orphan_tolerance,
        );
        ReconcileReport::new(
            exchange,
            symbol,
            severity,
            local_position.cloned(),
            exchange_position.cloned(),
            Vec::new(),
            Vec::new(),
            (severity != crate::execution::ReconcileSeverity::Ok)
                .then(|| format!("private stream position drift: {:?}", severity)),
            recommended_actions_for_severity(severity),
            None,
            checked_at,
        )
    }
}

impl Default for AccountSyncState {
    fn default() -> Self {
        Self::new(AccountSyncConfig::default())
    }
}

impl PositionKey {
    fn from_snapshot(snapshot: &PositionSnapshot) -> Self {
        Self {
            exchange: snapshot.exchange.clone(),
            canonical_symbol: snapshot.canonical_symbol.clone(),
            position_side: snapshot.position_side,
        }
    }
}

impl OrderKey {
    fn from_snapshot(snapshot: &OrderSnapshot) -> Self {
        let id = snapshot
            .client_order_id
            .clone()
            .or_else(|| snapshot.exchange_order_id.clone())
            .unwrap_or_else(|| format!("unknown-{}", snapshot.updated_at.timestamp_millis()));
        Self {
            exchange: snapshot.exchange.clone(),
            canonical_symbol: snapshot.canonical_symbol.clone(),
            id,
        }
    }

    fn from_fill(fill: &FillEvent) -> Self {
        let id = fill
            .client_order_id
            .clone()
            .or_else(|| fill.exchange_order_id.clone())
            .unwrap_or_else(|| fill.trade_id.clone());
        Self {
            exchange: fill.exchange.clone(),
            canonical_symbol: fill.canonical_symbol.clone(),
            id,
        }
    }
}

impl OrderSnapshot {
    fn from_order_state(state: OrderState) -> Self {
        Self {
            exchange: state.exchange,
            canonical_symbol: state.canonical_symbol,
            exchange_symbol: Some(state.exchange_symbol),
            client_order_id: state.client_order_id,
            exchange_order_id: state.exchange_order_id,
            status: state.status,
            quantity: state.quantity,
            filled_quantity: state.filled_quantity,
            updated_at: state.updated_at,
        }
    }

    fn from_fill(fill: FillEvent) -> Self {
        Self {
            exchange: fill.exchange,
            canonical_symbol: fill.canonical_symbol,
            exchange_symbol: Some(fill.exchange_symbol),
            client_order_id: fill.client_order_id,
            exchange_order_id: fill.exchange_order_id,
            status: OrderCommandStatus::PartiallyFilled,
            quantity: fill.quantity,
            filled_quantity: fill.quantity,
            updated_at: fill.filled_at,
        }
    }
}

impl PositionSnapshot {
    fn from_exchange_position(position: ExchangePosition) -> Self {
        Self {
            exchange: position.exchange,
            canonical_symbol: position.canonical_symbol,
            exchange_symbol: Some(position.exchange_symbol),
            position_side: position.position_side,
            quantity: position.quantity,
            entry_price: position.entry_price,
            unrealized_pnl: position.unrealized_pnl,
            updated_at: position.updated_at,
        }
    }
}

impl ExchangeBalance {
    fn from_ws_balance(
        exchange: ExchangeId,
        balance: WsBalance,
        received_at: DateTime<Utc>,
    ) -> Self {
        Self {
            exchange,
            asset: balance.currency,
            total: balance.total,
            available: balance.free,
            locked: balance.used,
            updated_at: received_at,
        }
    }
}

impl ExchangePosition {
    fn from_ws_position(exchange: ExchangeId, position: WsPosition) -> Self {
        let position_side = parse_position_side(&position.side);
        let quantity = signed_position_quantity(&position, position_side);
        Self {
            exchange: exchange.clone(),
            canonical_symbol: canonical_symbol_from_exchange_symbol(&exchange, &position.symbol),
            exchange_symbol: ExchangeSymbol::new(exchange, position.symbol),
            position_side,
            quantity,
            entry_price: Some(position.entry_price),
            mark_price: Some(position.mark_price),
            unrealized_pnl: Some(position.unrealized_pnl),
            updated_at: position.timestamp,
        }
    }
}

impl OrderState {
    fn from_execution_report(exchange: ExchangeId, report: WsExecutionReport) -> Self {
        let exchange_symbol = ExchangeSymbol::new(exchange.clone(), report.symbol.clone());
        let canonical_symbol = canonical_symbol_from_exchange_symbol(&exchange, &report.symbol);
        Self {
            exchange,
            canonical_symbol,
            exchange_symbol,
            client_order_id: report.client_order_id,
            exchange_order_id: Some(report.order_id),
            side: map_core_order_side(report.side),
            position_side: PositionSide::Net,
            order_type: map_core_order_type(report.order_type),
            quantity: report.amount,
            price: Some(report.price),
            filled_quantity: report.executed_amount,
            average_fill_price: Some(report.executed_price),
            time_in_force: TimeInForce::Gtc,
            reduce_only: false,
            status: report.status.into(),
            updated_at: report.timestamp,
        }
    }

    fn from_ws_order(exchange: ExchangeId, order: WsOrder) -> Self {
        let exchange_symbol = ExchangeSymbol::new(exchange.clone(), order.symbol.clone());
        let canonical_symbol = canonical_symbol_from_exchange_symbol(&exchange, &order.symbol);
        Self {
            exchange,
            canonical_symbol,
            exchange_symbol,
            client_order_id: None,
            exchange_order_id: Some(order.id),
            side: map_core_order_side(order.side),
            position_side: PositionSide::Net,
            order_type: map_core_order_type(order.order_type),
            quantity: order.amount,
            price: order.price,
            filled_quantity: order.filled,
            average_fill_price: order.price,
            time_in_force: TimeInForce::Gtc,
            reduce_only: false,
            status: order.status.into(),
            updated_at: order.timestamp,
        }
    }
}

impl FillEvent {
    fn from_ws_trade(exchange: ExchangeId, trade: WsTrade) -> Self {
        let exchange_symbol = ExchangeSymbol::new(exchange.clone(), trade.symbol.clone());
        let canonical_symbol = canonical_symbol_from_exchange_symbol(&exchange, &trade.symbol);
        Self {
            exchange,
            canonical_symbol,
            exchange_symbol,
            trade_id: trade.id,
            client_order_id: None,
            exchange_order_id: trade.order_id,
            side: map_core_order_side(trade.side),
            position_side: PositionSide::Net,
            liquidity: FillLiquidity::Unknown,
            price: trade.price,
            quantity: trade.amount,
            quote_quantity: trade.amount * trade.price,
            fee: trade.fee.as_ref().map(|fee| fee.cost),
            fee_asset: trade.fee.as_ref().map(|fee| fee.currency.clone()),
            fee_rate: trade.fee.as_ref().and_then(|fee| fee.rate),
            realized_pnl: None,
            reduce_only: None,
            filled_at: trade.timestamp,
            received_at: trade.timestamp,
        }
    }
}

impl From<OrderStatus> for OrderCommandStatus {
    fn from(value: OrderStatus) -> Self {
        match value {
            OrderStatus::Open => Self::Accepted,
            OrderStatus::Closed => Self::Filled,
            OrderStatus::PartiallyFilled => Self::PartiallyFilled,
            OrderStatus::Canceled => Self::Cancelled,
            OrderStatus::Expired => Self::Cancelled,
            OrderStatus::Rejected => Self::Rejected,
            OrderStatus::Pending => Self::Planned,
            OrderStatus::Triggered => Self::Submitted,
        }
    }
}

impl From<OrderCommandStatus> for OrderStatus {
    fn from(value: OrderCommandStatus) -> Self {
        match value {
            OrderCommandStatus::Submitted | OrderCommandStatus::Accepted => Self::Open,
            OrderCommandStatus::PartiallyFilled => Self::PartiallyFilled,
            OrderCommandStatus::Filled => Self::Closed,
            OrderCommandStatus::CancelRequested | OrderCommandStatus::Cancelled => Self::Canceled,
            OrderCommandStatus::Rejected => Self::Rejected,
            OrderCommandStatus::Planned => Self::Pending,
            OrderCommandStatus::Failed => Self::Rejected,
        }
    }
}

fn map_core_order_side(side: crate::core::types::OrderSide) -> OrderSide {
    match side {
        crate::core::types::OrderSide::Buy => OrderSide::Buy,
        crate::core::types::OrderSide::Sell => OrderSide::Sell,
    }
}

fn map_core_order_type(order_type: crate::core::types::OrderType) -> OrderType {
    match order_type {
        crate::core::types::OrderType::Limit => OrderType::Limit,
        crate::core::types::OrderType::Market => OrderType::Market,
        _ => OrderType::Market,
    }
}

fn canonical_symbol_from_exchange_symbol(exchange: &ExchangeId, symbol: &str) -> CanonicalSymbol {
    canonical_from_exchange_symbol(exchange, symbol)
        .unwrap_or_else(|| CanonicalSymbol::parse(symbol).unwrap_or_else(|| CanonicalSymbol::new(symbol, "USDT")))
}

fn parse_position_side(side: &str) -> PositionSide {
    match side.to_ascii_lowercase().as_str() {
        "long" | "buy" | "bid" => PositionSide::Long,
        "short" | "sell" | "ask" => PositionSide::Short,
        _ => PositionSide::Net,
    }
}

fn signed_position_quantity(position: &WsPosition, side: PositionSide) -> f64 {
    let raw = if position.contracts.abs() > f64::EPSILON {
        position.contracts
    } else if position.size.abs() > f64::EPSILON {
        position.size
    } else {
        position.amount
    };

    match side {
        PositionSide::Short => -raw.abs(),
        PositionSide::Long | PositionSide::Net => raw.abs(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{
        Balance as WsBalance, ExecutionReport as WsExecutionReport, MarketType, Order as WsOrder,
        OrderSide as CoreOrderSide, OrderStatus, OrderType as CoreOrderType,
        Position as WsPosition, Trade as WsTrade,
    };
    use crate::market::ExchangeSymbol;

    fn position(qty: f64, updated_at: DateTime<Utc>) -> PositionSnapshot {
        PositionSnapshot {
            exchange: ExchangeId::Binance,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: Some(ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT")),
            position_side: PositionSide::Long,
            quantity: qty,
            entry_price: Some(65000.0),
            unrealized_pnl: Some(1.0),
            updated_at,
        }
    }

    fn ws_execution_report(updated_at: DateTime<Utc>, status: OrderStatus) -> WsExecutionReport {
        WsExecutionReport {
            symbol: "BTCUSDT".to_string(),
            order_id: "exchange-1".to_string(),
            client_order_id: Some("client-1".to_string()),
            side: CoreOrderSide::Buy,
            order_type: CoreOrderType::Limit,
            status,
            price: 65_000.0,
            amount: 0.10,
            executed_amount: 0.05,
            executed_price: 65_000.0,
            commission: 0.65,
            commission_asset: "USDT".to_string(),
            timestamp: updated_at,
            is_maker: true,
        }
    }

    fn ws_order(updated_at: DateTime<Utc>, status: OrderStatus) -> WsOrder {
        WsOrder {
            id: "exchange-1".to_string(),
            symbol: "BTCUSDT".to_string(),
            side: CoreOrderSide::Buy,
            order_type: CoreOrderType::Limit,
            amount: 0.10,
            price: Some(65_000.0),
            filled: 0.05,
            remaining: 0.05,
            status,
            market_type: MarketType::Futures,
            timestamp: updated_at,
            last_trade_timestamp: Some(updated_at),
            info: serde_json::Value::Null,
        }
    }

    fn ws_trade(updated_at: DateTime<Utc>) -> WsTrade {
        WsTrade {
            id: "trade-1".to_string(),
            symbol: "BTCUSDT".to_string(),
            side: CoreOrderSide::Buy,
            amount: 0.05,
            price: 65_000.0,
            timestamp: updated_at,
            order_id: Some("exchange-1".to_string()),
            fee: Some(crate::core::types::Fee {
                currency: "USDT".to_string(),
                cost: 0.65,
                rate: Some(0.0002),
            }),
        }
    }

    fn ws_position(updated_at: DateTime<Utc>) -> WsPosition {
        WsPosition {
            symbol: "BTCUSDT".to_string(),
            side: "long".to_string(),
            contracts: 0.05,
            contract_size: 1.0,
            entry_price: 65_000.0,
            mark_price: 65_100.0,
            unrealized_pnl: 5.0,
            percentage: 0.01,
            margin: 50.0,
            margin_ratio: 0.5,
            leverage: Some(10),
            margin_type: Some("cross".to_string()),
            size: 0.05,
            amount: 0.05,
            timestamp: updated_at,
        }
    }

    fn ws_balance(updated_at: DateTime<Utc>) -> WsBalance {
        WsBalance {
            currency: "USDT".to_string(),
            total: 10_000.0,
            free: 8_000.0,
            used: 2_000.0,
            market_type: MarketType::Futures,
        }
    }

    #[test]
    fn execution_report_should_sync_order_state() {
        let now = Utc::now();
        let mut state = AccountSyncState::default();

        let event = PrivateEvent::from_ws_message(
            ExchangeId::Binance,
            WsMessage::ExecutionReport(ws_execution_report(now, OrderStatus::PartiallyFilled)),
            now,
        )
        .expect("execution report should map to private event");

        state.apply_private_event(event);

        let order = state.open_orders().first().copied().expect("open order");
        assert_eq!(order.exchange, ExchangeId::Binance);
        assert_eq!(order.client_order_id.as_deref(), Some("client-1"));
        assert_eq!(order.exchange_order_id.as_deref(), Some("exchange-1"));
        assert_eq!(order.status, OrderCommandStatus::PartiallyFilled);
        assert_eq!(order.filled_quantity, 0.05);
    }

    #[test]
    fn trade_should_sync_fill_and_increment_order_progress() {
        let now = Utc::now();
        let mut state = AccountSyncState::default();

        let order_event = PrivateEvent::from_ws_message(
            ExchangeId::Binance,
            WsMessage::Order(ws_order(now, OrderStatus::Open)),
            now,
        )
        .expect("order should map to private event");
        state.apply_private_event(order_event);

        let fill_event = PrivateEvent::from_ws_message(
            ExchangeId::Binance,
            WsMessage::Trade(ws_trade(now)),
            now,
        )
        .expect("trade should map to private event");
        state.apply_private_event(fill_event);

        assert!(state.open_orders().is_empty());
    }

    #[test]
    fn position_update_should_sync_position_state() {
        let now = Utc::now();
        let mut state = AccountSyncState::default();

        let event = PrivateEvent::from_ws_message(
            ExchangeId::Binance,
            WsMessage::Position(ws_position(now)),
            now,
        )
        .expect("position should map to private event");
        state.apply_private_event(event);

        let snapshot = state
            .position(
                &ExchangeId::Binance,
                &CanonicalSymbol::new("BTC", "USDT"),
                PositionSide::Long,
            )
            .expect("position snapshot");
        assert_eq!(snapshot.quantity, 0.05);
        assert_eq!(snapshot.entry_price, Some(65_000.0));
        assert_eq!(snapshot.unrealized_pnl, Some(5.0));
    }

    #[test]
    fn balance_update_should_sync_balance_state() {
        let now = Utc::now();
        let mut state = AccountSyncState::default();

        let event = PrivateEvent::from_ws_message(
            ExchangeId::Binance,
            WsMessage::Balance(ws_balance(now)),
            now,
        )
        .expect("balance should map to private event");
        state.apply_private_event(event);

        let balance = state
            .balance(&ExchangeId::Binance, "USDT")
            .expect("balance snapshot");
        assert_eq!(balance.total, 10_000.0);
        assert_eq!(balance.available, 8_000.0);
        assert_eq!(balance.locked, 2_000.0);
    }

    #[test]
    fn stale_stream_should_require_resync() {
        let now = Utc::now();
        let mut state = AccountSyncState::new(AccountSyncConfig {
            quantity_tolerance: 0.001,
            orphan_tolerance: 0.01,
            stale_after_ms: 5_000,
        });

        state.apply_event(PrivateStreamEvent::Heartbeat {
            exchange: ExchangeId::Binance,
            received_at: now - chrono::Duration::seconds(20),
        });

        assert!(state.needs_resync(&ExchangeId::Binance, now));
    }

    #[test]
    fn error_message_should_map_to_private_error_event() {
        let now = Utc::now();
        let event = PrivateEvent::from_ws_message(
            ExchangeId::Binance,
            WsMessage::Error("ws failure".to_string()),
            now,
        )
        .expect("error should map to private event");

        match event.kind {
            PrivateEventKind::Error(err) => {
                assert_eq!(err.class, ExchangeErrorClass::Unknown);
                assert_eq!(err.message, "ws failure");
                assert_eq!(err.occurred_at, now);
            }
            other => panic!("expected error event, got {other:?}"),
        }
    }

    #[test]
    fn account_sync_should_reconcile_private_stream_position() {
        let now = Utc::now();
        let state = AccountSyncState::new(AccountSyncConfig {
            quantity_tolerance: 0.001,
            orphan_tolerance: 0.01,
            stale_after_ms: 10_000,
        });

        let report = state.reconcile_position(
            Some(&position(0.1, now)),
            Some(&position(0.2, now)),
            ExchangeId::Binance,
            CanonicalSymbol::new("BTC", "USDT"),
            now,
        );

        assert!(report.alert_required);
        assert!(report
            .recommended_actions
            .contains(&crate::execution::ReconcileAction::EnterCloseOnly));
    }
}
