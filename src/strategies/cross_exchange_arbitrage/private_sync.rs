//! Private account stream and REST reconciliation helpers for cross-arb runtime.

use super::{
    maker_fill_from_tracked_private_event, BinancePrivateWsRuntimeSpec,
    CrossArbExecutionCoordinator, CrossArbRuntime, PrivateWsRuntimeSpec,
};
use crate::core::types::MarketType;
use crate::exchanges::adapters::run_private_ws;
use crate::execution::{
    parse_binance_futures_user_data_event, ExchangeBalance, ExchangeErrorClass, ExchangePosition,
    FillEvent, FillQuery, OrderState, PrivateErrorEvent, PrivateEvent, PrivateEventKind,
    ReconcileSeverity, TradingAdapter,
};
use crate::market::{CanonicalSymbol, ExchangeId, ExchangeSymbol};
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio::time::{sleep, timeout, Duration as TokioDuration};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[derive(Debug, Clone, PartialEq)]
pub struct PrivateRestSnapshot {
    pub exchange: ExchangeId,
    pub balances: Vec<ExchangeBalance>,
    pub positions: Vec<ExchangePosition>,
    pub open_orders: Vec<OrderState>,
    pub fills: Vec<FillEvent>,
    pub fetched_at: DateTime<Utc>,
}

impl PrivateRestSnapshot {
    pub fn into_private_events(self) -> Vec<PrivateEvent> {
        let fetched_at = self.fetched_at;
        let mut events = Vec::new();
        events.extend(
            self.balances
                .into_iter()
                .map(|balance| PrivateEvent::balance(balance, fetched_at)),
        );
        events.extend(
            self.positions
                .into_iter()
                .map(|position| PrivateEvent::position(position, fetched_at)),
        );
        events.extend(
            self.open_orders
                .into_iter()
                .map(|order| PrivateEvent::order(order, fetched_at)),
        );
        events.extend(
            self.fills
                .into_iter()
                .map(|fill| PrivateEvent::fill(fill, fetched_at)),
        );
        events
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivateRestAuditPlan {
    pub exchange: ExchangeId,
    pub symbols: Vec<ExchangeSymbol>,
    pub include_recent_fills: bool,
}

#[derive(Clone)]
pub struct PrivateRuntimeSync {
    runtime: Arc<RwLock<CrossArbRuntime>>,
    adapters: HashMap<ExchangeId, Arc<dyn TradingAdapter>>,
    execution: Option<Arc<RwLock<CrossArbExecutionCoordinator>>>,
}

impl PrivateRuntimeSync {
    pub fn new(
        runtime: Arc<RwLock<CrossArbRuntime>>,
        adapters: impl IntoIterator<Item = Arc<dyn TradingAdapter>>,
    ) -> Self {
        Self::with_execution(runtime, adapters, None)
    }

    pub fn with_execution(
        runtime: Arc<RwLock<CrossArbRuntime>>,
        adapters: impl IntoIterator<Item = Arc<dyn TradingAdapter>>,
        execution: Option<Arc<RwLock<CrossArbExecutionCoordinator>>>,
    ) -> Self {
        let adapters = adapters
            .into_iter()
            .map(|adapter| (adapter.exchange(), adapter))
            .collect();
        Self {
            runtime,
            adapters,
            execution,
        }
    }

    pub async fn ingest_private_event(&self, event: PrivateEvent) {
        let hedge = if let Some(execution) = &self.execution {
            let tracked = if let PrivateEventKind::Fill(fill) = &event.kind {
                execution.read().await.tracked_order_for_fill(fill)
            } else {
                None
            };
            let mut runtime = self.runtime.write().await;
            runtime.on_private_event(event.clone());
            tracked.as_ref().and_then(|tracked| {
                maker_fill_from_tracked_private_event(&mut runtime, tracked, &event)
            })
        } else {
            let mut runtime = self.runtime.write().await;
            runtime.on_private_event(event);
            None
        };

        if let (Some(execution), Some(fill)) = (&self.execution, hedge) {
            let decision = execution
                .write()
                .await
                .execute_hedge_for_maker_fill(fill)
                .await;
            if decision.blocked_reason.is_some() || decision.requires_reconcile {
                let mut runtime = self.runtime.write().await;
                runtime.state.risk_events.push(super::RiskEventReadModel {
                    event_id: format!(
                        "hedge-decision-{}",
                        decision.plan.created_at.timestamp_millis()
                    ),
                    canonical_symbol: decision
                        .plan
                        .commands
                        .first()
                        .map(|command| command.canonical_symbol.clone()),
                    exchange: decision
                        .plan
                        .commands
                        .first()
                        .map(|command| command.exchange.clone()),
                    reason: super::RejectReason::RouteUnhealthy,
                    message: decision
                        .blocked_reason
                        .clone()
                        .unwrap_or_else(|| "hedge requires reconciliation".to_string()),
                    created_at: decision.plan.created_at,
                });
            }
        }
    }

    pub async fn ingest_private_events(&self, events: impl IntoIterator<Item = PrivateEvent>) {
        let mut runtime = self.runtime.write().await;
        for event in events {
            runtime.on_private_event(event);
        }
    }

    pub async fn fetch_rest_snapshot(
        &self,
        plan: &PrivateRestAuditPlan,
    ) -> Result<PrivateRestSnapshot> {
        let adapter = self.adapters.get(&plan.exchange).ok_or_else(|| {
            anyhow!(
                "missing private trading adapter for {} REST audit",
                plan.exchange
            )
        })?;
        fetch_rest_snapshot(adapter.as_ref(), plan).await
    }

    pub async fn run_rest_audit(&self, plan: &PrivateRestAuditPlan) -> Result<PrivateRestSnapshot> {
        let snapshot = self.fetch_rest_snapshot(plan).await?;
        self.ingest_private_events(snapshot.clone().into_private_events())
            .await;
        self.record_position_reconciliation(&snapshot).await;
        Ok(snapshot)
    }

    async fn record_position_reconciliation(&self, snapshot: &PrivateRestSnapshot) {
        let mut runtime = self.runtime.write().await;
        let decisions = runtime.state.reconcile_positions(
            &snapshot.positions,
            runtime.state.config.reconciliation.quantity_tolerance,
            runtime.state.config.reconciliation.orphan_tolerance,
            snapshot.fetched_at,
        );

        if decisions.is_empty() && snapshot.positions.is_empty() {
            runtime.state.record_reconciliation_result(
                snapshot.exchange.clone(),
                "*".to_string(),
                ReconcileSeverity::Ok,
                snapshot.fetched_at,
            );
            return;
        }
        for decision in decisions {
            runtime.state.record_reconciliation_result(
                decision.exchange,
                decision.canonical_symbol.to_string(),
                decision.severity,
                snapshot.fetched_at,
            );
        }
    }

    pub fn start_private_ws_supervisors(
        &self,
        specs: impl IntoIterator<Item = PrivateWsRuntimeSpec>,
        channel_capacity: usize,
    ) -> mpsc::Receiver<PrivateEvent> {
        let (tx, rx) = mpsc::channel(channel_capacity.max(1));
        for spec in specs {
            tokio::spawn(run_private_ws(
                spec.exchange,
                spec.auth,
                spec.symbols,
                spec.config,
                tx.clone(),
            ));
        }
        rx
    }

    pub fn start_binance_private_ws_supervisors(
        &self,
        specs: impl IntoIterator<Item = BinancePrivateWsRuntimeSpec>,
        channel_capacity: usize,
    ) -> mpsc::Receiver<PrivateEvent> {
        let (tx, rx) = mpsc::channel(channel_capacity.max(1));
        for spec in specs {
            tokio::spawn(run_binance_futures_private_ws(
                spec.exchange,
                spec.config,
                tx.clone(),
            ));
        }
        rx
    }

    pub async fn pump_private_events(&self, rx: &mut mpsc::Receiver<PrivateEvent>) {
        while let Some(event) = rx.recv().await {
            self.ingest_private_event(event).await;
        }
    }
}

pub async fn fetch_rest_snapshot(
    adapter: &(dyn TradingAdapter + Send + Sync),
    plan: &PrivateRestAuditPlan,
) -> Result<PrivateRestSnapshot> {
    let fetched_at = Utc::now();
    let balances = adapter.get_balances().await?;
    let mut positions = Vec::new();
    let mut open_orders = Vec::new();
    let mut fills = Vec::new();

    for symbol in &plan.symbols {
        positions.extend(adapter.get_positions(Some(symbol)).await?);
        open_orders.extend(adapter.get_open_orders(Some(symbol)).await?);
        if plan.include_recent_fills {
            let mut query = FillQuery::for_symbol(
                plan.exchange.clone(),
                canonical_for_symbol(symbol),
                symbol.clone(),
            );
            query.limit = Some(100);
            fills.extend(adapter.get_fills(query).await?);
        }
    }

    Ok(PrivateRestSnapshot {
        exchange: plan.exchange.clone(),
        balances,
        positions: dedupe_positions(positions),
        open_orders: dedupe_orders(open_orders),
        fills: dedupe_fills(fills),
        fetched_at,
    })
}

fn canonical_for_symbol(symbol: &ExchangeSymbol) -> CanonicalSymbol {
    crate::market::canonical_from_exchange_symbol(&symbol.exchange, &symbol.symbol).unwrap_or_else(
        || {
            CanonicalSymbol::parse(&symbol.symbol).unwrap_or_else(|| {
                CanonicalSymbol::new(symbol.symbol.trim_end_matches("USDT"), "USDT")
            })
        },
    )
}

fn dedupe_positions(positions: Vec<ExchangePosition>) -> Vec<ExchangePosition> {
    let mut seen = HashSet::new();
    positions
        .into_iter()
        .filter(|position| {
            seen.insert((
                position.exchange.clone(),
                position.canonical_symbol.clone(),
                position.position_side,
            ))
        })
        .collect()
}

fn dedupe_orders(orders: Vec<OrderState>) -> Vec<OrderState> {
    let mut seen = HashSet::new();
    orders
        .into_iter()
        .filter(|order| {
            let id = order
                .client_order_id
                .clone()
                .or_else(|| order.exchange_order_id.clone())
                .unwrap_or_else(|| order.updated_at.timestamp_millis().to_string());
            seen.insert((order.exchange.clone(), order.canonical_symbol.clone(), id))
        })
        .collect()
}

fn dedupe_fills(fills: Vec<FillEvent>) -> Vec<FillEvent> {
    let mut seen = HashSet::new();
    fills
        .into_iter()
        .filter(|fill| seen.insert((fill.exchange.clone(), fill.trade_id.clone())))
        .collect()
}

pub fn disconnected_event(
    exchange: ExchangeId,
    reason: impl Into<String>,
    now: DateTime<Utc>,
) -> PrivateEvent {
    PrivateEvent::new(
        exchange,
        PrivateEventKind::StreamDisconnected {
            reason: Some(reason.into()),
            disconnected_at: now,
        },
        now,
    )
}

pub async fn run_binance_futures_private_ws(
    exchange: Arc<dyn crate::core::exchange::Exchange>,
    config: crate::exchanges::adapters::PrivateWsRunConfig,
    tx: mpsc::Sender<PrivateEvent>,
) -> Result<()> {
    loop {
        if tx.is_closed() {
            return Ok(());
        }

        let result =
            run_binance_futures_private_ws_once(exchange.clone(), config, tx.clone()).await;
        if tx.is_closed() {
            return Ok(());
        }
        if let Err(err) = result {
            emit_private_ws_error(
                &tx,
                ExchangeId::Binance,
                ExchangeErrorClass::Network,
                Some("binance futures private websocket".to_string()),
                err.to_string(),
            )
            .await;
            emit_private_ws_disconnected(&tx, ExchangeId::Binance, Some(err.to_string())).await;
        }
        sleep(TokioDuration::from_millis(config.reconnect_delay_ms.max(1))).await;
    }
}

async fn run_binance_futures_private_ws_once(
    exchange: Arc<dyn crate::core::exchange::Exchange>,
    config: crate::exchanges::adapters::PrivateWsRunConfig,
    tx: mpsc::Sender<PrivateEvent>,
) -> Result<()> {
    let listen_key = exchange
        .create_user_data_stream(MarketType::Futures)
        .await?;
    let url = format!("wss://fstream.binance.com/ws/{listen_key}");
    let connect = timeout(
        TokioDuration::from_millis(config.connect_timeout_ms.max(1)),
        connect_async(&url),
    )
    .await
    .map_err(|_| anyhow!("binance futures private websocket connect timed out"))??;
    let (ws, _) = connect;
    let (mut write, mut read) = ws.split();
    let mut keepalive = tokio::time::interval(TokioDuration::from_millis(
        config.heartbeat_interval_ms.max(1_000),
    ));

    loop {
        tokio::select! {
            _ = keepalive.tick() => {
                exchange.keepalive_user_data_stream(&listen_key, MarketType::Futures).await?;
                write.send(Message::Ping(Vec::new())).await?;
                if !publish_private_event(&tx, PrivateEvent::new(ExchangeId::Binance, PrivateEventKind::Heartbeat, Utc::now())).await {
                    return Ok(());
                }
            }
            message = read.next() => {
                match message {
                    Some(Ok(Message::Text(raw))) => {
                        publish_binance_raw_private_event(&tx, &raw, &url).await;
                    }
                    Some(Ok(Message::Binary(raw))) => {
                        match String::from_utf8(raw) {
                            Ok(text) => publish_binance_raw_private_event(&tx, &text, &url).await,
                            Err(err) => {
                                emit_private_ws_error(
                                    &tx,
                                    ExchangeId::Binance,
                                    ExchangeErrorClass::Decode,
                                    Some(url.clone()),
                                    err.to_string(),
                                ).await;
                            }
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        write.send(Message::Pong(payload)).await?;
                    }
                    Some(Ok(Message::Pong(_))) => {
                        if !publish_private_event(&tx, PrivateEvent::new(ExchangeId::Binance, PrivateEventKind::Heartbeat, Utc::now())).await {
                            return Ok(());
                        }
                    }
                    Some(Ok(Message::Close(frame))) => {
                        let reason = frame.map(|frame| frame.reason.to_string());
                        emit_private_ws_disconnected(&tx, ExchangeId::Binance, reason).await;
                        return Ok(());
                    }
                    Some(Err(err)) => {
                        emit_private_ws_disconnected(&tx, ExchangeId::Binance, Some(err.to_string())).await;
                        return Err(err.into());
                    }
                    None => {
                        emit_private_ws_disconnected(
                            &tx,
                            ExchangeId::Binance,
                            Some("binance futures private websocket stream ended".to_string()),
                        ).await;
                        return Ok(());
                    }
                    _ => {}
                }
            }
        }
    }
}

async fn publish_binance_raw_private_event(
    tx: &mpsc::Sender<PrivateEvent>,
    raw: &str,
    endpoint: &str,
) {
    let received_at = Utc::now();
    match parse_binance_futures_user_data_event(raw, received_at) {
        Ok(events) => {
            for event in events {
                if !publish_private_event(tx, event).await {
                    return;
                }
            }
        }
        Err(err) => {
            emit_private_ws_error(
                tx,
                ExchangeId::Binance,
                ExchangeErrorClass::Decode,
                Some(endpoint.to_string()),
                err.to_string(),
            )
            .await;
        }
    }
}

async fn publish_private_event(tx: &mpsc::Sender<PrivateEvent>, event: PrivateEvent) -> bool {
    tx.send(event).await.is_ok()
}

async fn emit_private_ws_error(
    tx: &mpsc::Sender<PrivateEvent>,
    exchange: ExchangeId,
    class: ExchangeErrorClass,
    endpoint: Option<String>,
    message: String,
) {
    let now = Utc::now();
    let _ = tx
        .send(PrivateEvent::new(
            exchange,
            PrivateEventKind::Error(PrivateErrorEvent {
                class,
                endpoint,
                code: None,
                message,
                client_order_id: None,
                exchange_order_id: None,
                retry_after_ms: None,
                occurred_at: now,
            }),
            now,
        ))
        .await;
}

async fn emit_private_ws_disconnected(
    tx: &mpsc::Sender<PrivateEvent>,
    exchange: ExchangeId,
    reason: Option<String>,
) {
    let now = Utc::now();
    let _ = tx
        .send(disconnected_event(
            exchange,
            reason.unwrap_or_else(|| "private websocket disconnected".to_string()),
            now,
        ))
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::{
        CancelAck, CancelCommand, ClosePositionAck, ClosePositionCommand, LeverageAck,
        LeverageCommand, OrderAck, OrderCommand, OrderCommandStatus, OrderQuery, PositionModeAck,
        PositionModeCommand, PositionSide, TradingCapabilities,
    };
    use crate::market::RuntimeMode;
    use async_trait::async_trait;

    struct MockTradingAdapter {
        exchange: ExchangeId,
    }

    #[async_trait]
    impl TradingAdapter for MockTradingAdapter {
        fn exchange(&self) -> ExchangeId {
            self.exchange.clone()
        }

        fn capabilities(&self) -> TradingCapabilities {
            TradingCapabilities::default()
        }

        async fn place_order(&self, command: OrderCommand) -> Result<OrderAck> {
            Ok(OrderAck::dry_run(&command, Utc::now()))
        }

        async fn cancel_order(&self, command: CancelCommand) -> Result<CancelAck> {
            Ok(CancelAck::dry_run(&command, Utc::now()))
        }

        async fn get_order(&self, _query: OrderQuery) -> Result<OrderState> {
            anyhow::bail!("not used")
        }

        async fn get_open_orders(
            &self,
            symbol: Option<&ExchangeSymbol>,
        ) -> Result<Vec<OrderState>> {
            let Some(symbol) = symbol else {
                return Ok(Vec::new());
            };
            Ok(vec![OrderState {
                exchange: self.exchange.clone(),
                canonical_symbol: canonical_for_symbol(symbol),
                exchange_symbol: symbol.clone(),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: Some("order-1".to_string()),
                side: crate::execution::OrderSide::Buy,
                position_side: PositionSide::Long,
                order_type: crate::execution::OrderType::Limit,
                quantity: 1.0,
                price: Some(100.0),
                filled_quantity: 0.0,
                average_fill_price: None,
                time_in_force: crate::execution::TimeInForce::Gtc,
                reduce_only: false,
                status: OrderCommandStatus::Accepted,
                updated_at: Utc::now(),
            }])
        }

        async fn get_positions(
            &self,
            symbol: Option<&ExchangeSymbol>,
        ) -> Result<Vec<ExchangePosition>> {
            let Some(symbol) = symbol else {
                return Ok(Vec::new());
            };
            Ok(vec![ExchangePosition {
                exchange: self.exchange.clone(),
                canonical_symbol: canonical_for_symbol(symbol),
                exchange_symbol: symbol.clone(),
                position_side: PositionSide::Long,
                quantity: 1.0,
                entry_price: Some(100.0),
                mark_price: Some(101.0),
                unrealized_pnl: Some(1.0),
                updated_at: Utc::now(),
            }])
        }

        async fn get_balances(&self) -> Result<Vec<ExchangeBalance>> {
            Ok(vec![ExchangeBalance {
                exchange: self.exchange.clone(),
                asset: "USDT".to_string(),
                total: 1000.0,
                available: 900.0,
                locked: 100.0,
                updated_at: Utc::now(),
            }])
        }

        async fn get_fills(&self, query: FillQuery) -> Result<Vec<FillEvent>> {
            let symbol = query.exchange_symbol.unwrap();
            Ok(vec![FillEvent {
                exchange: self.exchange.clone(),
                canonical_symbol: canonical_for_symbol(&symbol),
                exchange_symbol: symbol,
                trade_id: "trade-1".to_string(),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: Some("order-1".to_string()),
                side: crate::execution::OrderSide::Buy,
                position_side: PositionSide::Long,
                liquidity: crate::execution::FillLiquidity::Taker,
                price: 100.0,
                quantity: 1.0,
                quote_quantity: 100.0,
                fee: Some(0.05),
                fee_asset: Some("USDT".to_string()),
                fee_rate: Some(0.0005),
                realized_pnl: None,
                reduce_only: Some(false),
                filled_at: Utc::now(),
                received_at: Utc::now(),
            }])
        }

        async fn set_leverage(&self, command: LeverageCommand) -> Result<LeverageAck> {
            Ok(LeverageAck::dry_run(&command, Utc::now()))
        }

        async fn set_position_mode(&self, command: PositionModeCommand) -> Result<PositionModeAck> {
            Ok(PositionModeAck::dry_run(&command, Utc::now()))
        }

        async fn close_position(&self, command: ClosePositionCommand) -> Result<ClosePositionAck> {
            Ok(ClosePositionAck::dry_run(&command, Utc::now()))
        }
    }

    #[tokio::test]
    async fn private_sync_should_fetch_rest_snapshot_and_convert_events() {
        let adapter = MockTradingAdapter {
            exchange: ExchangeId::Bitget,
        };
        let plan = PrivateRestAuditPlan {
            exchange: ExchangeId::Bitget,
            symbols: vec![ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT")],
            include_recent_fills: true,
        };

        let snapshot = fetch_rest_snapshot(&adapter, &plan).await.unwrap();
        let events = snapshot.into_private_events();

        assert_eq!(events.len(), 4);
        assert!(events
            .iter()
            .any(|event| matches!(event.kind, PrivateEventKind::Balance(_))));
        assert!(events
            .iter()
            .any(|event| matches!(event.kind, PrivateEventKind::Fill(_))));
    }

    #[tokio::test]
    async fn private_sync_should_ingest_rest_audit_into_runtime() {
        let runtime = Arc::new(RwLock::new(CrossArbRuntime::new(
            super::super::CrossExchangeArbitrageConfig {
                mode: RuntimeMode::Simulation,
                ..Default::default()
            },
            Utc::now(),
        )));
        let sync = PrivateRuntimeSync::new(
            runtime.clone(),
            [Arc::new(MockTradingAdapter {
                exchange: ExchangeId::Bitget,
            }) as Arc<dyn TradingAdapter>],
        );
        let plan = PrivateRestAuditPlan {
            exchange: ExchangeId::Bitget,
            symbols: vec![ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT")],
            include_recent_fills: true,
        };

        sync.run_rest_audit(&plan).await.unwrap();

        let runtime = runtime.read().await;
        assert_eq!(
            runtime.state.account_sync[&ExchangeId::Bitget]
                .account_snapshot()
                .len(),
            1
        );
        assert!(!runtime.storage.events().is_empty());
    }

    #[test]
    fn private_sync_should_build_disconnected_event() {
        let now = Utc::now();
        let event = disconnected_event(ExchangeId::Gate, "network", now);

        assert!(matches!(
            event.kind,
            PrivateEventKind::StreamDisconnected { .. }
        ));
        assert_eq!(event.exchange, ExchangeId::Gate);
    }
}
