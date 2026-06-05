use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::time::{sleep, Duration};

use crate::exchanges::unified::{
    ExchangeClientError, ExchangeErrorClass, MarketType, OrderResponse, OrderStatus, TradeFill,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderReconciliationConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    #[serde(default = "default_max_poll_attempts")]
    pub max_poll_attempts: u32,
    #[serde(default = "default_order_timeout_ms")]
    pub order_timeout_ms: u64,
    #[serde(default = "default_true")]
    pub allow_recent_fills_fallback: bool,
    #[serde(default = "default_true")]
    pub allow_open_orders_fallback: bool,
    #[serde(default = "default_true")]
    pub unknown_status_is_critical: bool,
}

impl Default for OrderReconciliationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            poll_interval_ms: default_poll_interval_ms(),
            max_poll_attempts: default_max_poll_attempts(),
            order_timeout_ms: default_order_timeout_ms(),
            allow_recent_fills_fallback: true,
            allow_open_orders_fallback: true,
            unknown_status_is_critical: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderPollingState {
    pub exchange: String,
    pub market_type: MarketType,
    pub symbol: String,
    pub exchange_order_id: Option<String>,
    pub client_order_id: Option<String>,
    pub attempts: u32,
    pub started_at: DateTime<Utc>,
    pub last_status: Option<OrderStatus>,
    pub last_error: Option<String>,
}

impl OrderPollingState {
    pub fn new(
        exchange: impl Into<String>,
        market_type: MarketType,
        symbol: impl Into<String>,
        exchange_order_id: Option<String>,
        client_order_id: Option<String>,
    ) -> Self {
        Self {
            exchange: exchange.into(),
            market_type,
            symbol: symbol.into(),
            exchange_order_id,
            client_order_id,
            attempts: 0,
            started_at: Utc::now(),
            last_status: None,
            last_error: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconciliationSeverity {
    Info,
    Warn,
    Error,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderReconciliationOutcome {
    Filled,
    PartiallyFilled,
    Cancelled,
    Rejected,
    Expired,
    Unknown,
    OrderNotFound,
    Timeout,
    RateLimited,
    InconsistentStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FillReconciliationResult {
    pub fills: Vec<TradeFill>,
    pub total_filled_quantity: f64,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderReconciliationResult {
    pub exchange: String,
    pub market_type: MarketType,
    pub symbol: String,
    pub exchange_order_id: Option<String>,
    pub client_order_id: Option<String>,
    pub outcome: OrderReconciliationOutcome,
    pub severity: ReconciliationSeverity,
    pub final_order: Option<OrderResponse>,
    pub fills: Option<FillReconciliationResult>,
    pub attempts: u32,
    pub timed_out: bool,
    pub order_not_found: bool,
    pub rate_limited: bool,
    pub inconsistent_status: bool,
    pub message: String,
    pub completed_at: DateTime<Utc>,
}

#[async_trait]
pub trait OrderReconciliationClient: Send + Sync {
    async fn get_order(
        &self,
        symbol: &str,
        order_id: Option<&str>,
        client_order_id: Option<&str>,
    ) -> Result<Option<OrderResponse>, ExchangeClientError>;

    async fn get_recent_fills(&self, symbol: &str) -> Result<Vec<TradeFill>, ExchangeClientError>;

    async fn get_open_orders(
        &self,
        symbol: Option<&str>,
    ) -> Result<Vec<OrderResponse>, ExchangeClientError>;
}

pub async fn reconcile_order<C: OrderReconciliationClient>(
    config: &OrderReconciliationConfig,
    client: &C,
    mut state: OrderPollingState,
) -> OrderReconciliationResult {
    if !config.enabled {
        return result(
            &state,
            OrderReconciliationOutcome::Unknown,
            ReconciliationSeverity::Warn,
            None,
            None,
            "order reconciliation disabled",
        );
    }

    for attempt in 1..=config.max_poll_attempts {
        state.attempts = attempt;
        if Utc::now()
            .signed_duration_since(state.started_at)
            .num_milliseconds()
            >= config.order_timeout_ms as i64
        {
            return timeout_result(&state);
        }

        match client
            .get_order(
                &state.symbol,
                state.exchange_order_id.as_deref(),
                state.client_order_id.as_deref(),
            )
            .await
        {
            Ok(Some(order)) => {
                state.last_status = Some(order.status);
                if let Some(outcome) = final_outcome(order.status) {
                    return result_for_order(&state, order, outcome);
                }
            }
            Ok(None) => {
                state.last_error = Some("order not found".to_string());
                if let Some(fallback) = fallback_lookup(config, client, &state).await {
                    return fallback;
                }
                return result(
                    &state,
                    OrderReconciliationOutcome::OrderNotFound,
                    ReconciliationSeverity::Error,
                    None,
                    None,
                    "order not found by REST query",
                );
            }
            Err(error) => {
                state.last_error = Some(error.to_string());
                if is_rate_limit(&error) {
                    return result(
                        &state,
                        OrderReconciliationOutcome::RateLimited,
                        ReconciliationSeverity::Critical,
                        None,
                        None,
                        "REST rate limit encountered",
                    );
                }
                if is_order_not_found(&error) {
                    if let Some(fallback) = fallback_lookup(config, client, &state).await {
                        return fallback;
                    }
                    return result(
                        &state,
                        OrderReconciliationOutcome::OrderNotFound,
                        ReconciliationSeverity::Error,
                        None,
                        None,
                        "order not found by REST query",
                    );
                }
            }
        }

        sleep(Duration::from_millis(config.poll_interval_ms)).await;
    }
    timeout_result(&state)
}

async fn fallback_lookup<C: OrderReconciliationClient>(
    config: &OrderReconciliationConfig,
    client: &C,
    state: &OrderPollingState,
) -> Option<OrderReconciliationResult> {
    if config.allow_recent_fills_fallback {
        if let Ok(fills) = client.get_recent_fills(&state.symbol).await {
            let matched = fills
                .into_iter()
                .filter(|fill| {
                    state
                        .exchange_order_id
                        .as_ref()
                        .is_some_and(|id| fill.order_id.as_ref() == Some(id))
                        || state
                            .client_order_id
                            .as_ref()
                            .is_some_and(|id| fill.client_order_id.as_ref() == Some(id))
                })
                .collect::<Vec<_>>();
            if !matched.is_empty() {
                let total_filled_quantity = matched.iter().map(|fill| fill.quantity).sum();
                return Some(result(
                    state,
                    OrderReconciliationOutcome::Filled,
                    ReconciliationSeverity::Info,
                    None,
                    Some(FillReconciliationResult {
                        fills: matched,
                        total_filled_quantity,
                        source: "recent_fills".to_string(),
                    }),
                    "order matched through recent fills fallback",
                ));
            }
        }
    }

    if config.allow_open_orders_fallback {
        if let Ok(open_orders) = client.get_open_orders(Some(&state.symbol)).await {
            if let Some(order) = open_orders.into_iter().find(|order| {
                state
                    .exchange_order_id
                    .as_ref()
                    .is_some_and(|id| &order.order_id == id)
                    || state
                        .client_order_id
                        .as_ref()
                        .is_some_and(|id| order.client_order_id.as_ref() == Some(id))
            }) {
                return Some(result_for_order(
                    state,
                    order,
                    OrderReconciliationOutcome::PartiallyFilled,
                ));
            }
        }
    }
    None
}

fn final_outcome(status: OrderStatus) -> Option<OrderReconciliationOutcome> {
    match status {
        OrderStatus::Filled => Some(OrderReconciliationOutcome::Filled),
        OrderStatus::PartiallyFilled => Some(OrderReconciliationOutcome::PartiallyFilled),
        OrderStatus::Cancelled => Some(OrderReconciliationOutcome::Cancelled),
        OrderStatus::Rejected => Some(OrderReconciliationOutcome::Rejected),
        OrderStatus::Expired => Some(OrderReconciliationOutcome::Expired),
        OrderStatus::Unknown => Some(OrderReconciliationOutcome::Unknown),
        OrderStatus::New => None,
    }
}

fn result_for_order(
    state: &OrderPollingState,
    order: OrderResponse,
    outcome: OrderReconciliationOutcome,
) -> OrderReconciliationResult {
    let severity = match outcome {
        OrderReconciliationOutcome::Filled => ReconciliationSeverity::Info,
        OrderReconciliationOutcome::PartiallyFilled => ReconciliationSeverity::Warn,
        OrderReconciliationOutcome::Cancelled | OrderReconciliationOutcome::Expired => {
            ReconciliationSeverity::Warn
        }
        OrderReconciliationOutcome::Rejected | OrderReconciliationOutcome::Unknown => {
            ReconciliationSeverity::Critical
        }
        _ => ReconciliationSeverity::Error,
    };
    result(
        state,
        outcome,
        severity,
        Some(order),
        None,
        format!("final order status reconciled as {outcome:?}"),
    )
}

fn timeout_result(state: &OrderPollingState) -> OrderReconciliationResult {
    result(
        state,
        OrderReconciliationOutcome::Timeout,
        ReconciliationSeverity::Critical,
        None,
        None,
        "order reconciliation timed out",
    )
}

fn result(
    state: &OrderPollingState,
    outcome: OrderReconciliationOutcome,
    severity: ReconciliationSeverity,
    final_order: Option<OrderResponse>,
    fills: Option<FillReconciliationResult>,
    message: impl Into<String>,
) -> OrderReconciliationResult {
    OrderReconciliationResult {
        exchange: state.exchange.clone(),
        market_type: state.market_type,
        symbol: state.symbol.clone(),
        exchange_order_id: state.exchange_order_id.clone(),
        client_order_id: state.client_order_id.clone(),
        outcome,
        severity,
        final_order,
        fills,
        attempts: state.attempts,
        timed_out: matches!(outcome, OrderReconciliationOutcome::Timeout),
        order_not_found: matches!(outcome, OrderReconciliationOutcome::OrderNotFound),
        rate_limited: matches!(outcome, OrderReconciliationOutcome::RateLimited),
        inconsistent_status: matches!(outcome, OrderReconciliationOutcome::InconsistentStatus),
        message: message.into(),
        completed_at: Utc::now(),
    }
}

fn is_rate_limit(error: &ExchangeClientError) -> bool {
    matches!(
        error,
        ExchangeClientError::Classified(classified)
            if classified.class == ExchangeErrorClass::RateLimited
    )
}

fn is_order_not_found(error: &ExchangeClientError) -> bool {
    matches!(
        error,
        ExchangeClientError::Classified(classified)
            if classified.class == ExchangeErrorClass::OrderNotFound
    )
}

fn default_true() -> bool {
    true
}

fn default_poll_interval_ms() -> u64 {
    500
}

fn default_max_poll_attempts() -> u32 {
    20
}

fn default_order_timeout_ms() -> u64 {
    10_000
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchanges::unified::{
        ExchangeError, LiquidityRole, OrderSide, OrderType, PositionSide,
    };
    use std::sync::{Arc, Mutex};

    type MockOrderResponses = Vec<Result<Option<OrderResponse>, ExchangeClientError>>;

    #[derive(Clone, Default)]
    struct MockClient {
        orders: Arc<Mutex<MockOrderResponses>>,
        fills: Arc<Mutex<Vec<TradeFill>>>,
        open_orders: Arc<Mutex<Vec<OrderResponse>>>,
    }

    #[async_trait]
    impl OrderReconciliationClient for MockClient {
        async fn get_order(
            &self,
            _symbol: &str,
            _order_id: Option<&str>,
            _client_order_id: Option<&str>,
        ) -> Result<Option<OrderResponse>, ExchangeClientError> {
            self.orders.lock().unwrap().remove(0)
        }

        async fn get_recent_fills(
            &self,
            _symbol: &str,
        ) -> Result<Vec<TradeFill>, ExchangeClientError> {
            Ok(self.fills.lock().unwrap().clone())
        }

        async fn get_open_orders(
            &self,
            _symbol: Option<&str>,
        ) -> Result<Vec<OrderResponse>, ExchangeClientError> {
            Ok(self.open_orders.lock().unwrap().clone())
        }
    }

    fn order(status: OrderStatus) -> OrderResponse {
        OrderResponse {
            exchange: "mexc".to_string(),
            market_type: MarketType::Spot,
            symbol: "BTCUSDT".to_string(),
            order_id: "1".to_string(),
            client_order_id: Some("cid".to_string()),
            side: OrderSide::Buy,
            position_side: PositionSide::None,
            order_type: OrderType::IOC,
            status,
            price: Some(100.0),
            quantity: 1.0,
            filled_quantity: if status == OrderStatus::Filled {
                1.0
            } else {
                0.5
            },
            average_price: Some(100.0),
            created_at: Utc::now(),
            updated_at: Some(Utc::now()),
        }
    }

    fn state() -> OrderPollingState {
        OrderPollingState::new(
            "mexc",
            MarketType::Spot,
            "BTCUSDT",
            Some("1".to_string()),
            Some("cid".to_string()),
        )
    }

    #[tokio::test]
    async fn filled_order_via_get_order() {
        let client = MockClient::default();
        client
            .orders
            .lock()
            .unwrap()
            .push(Ok(Some(order(OrderStatus::Filled))));
        let result = reconcile_order(&OrderReconciliationConfig::default(), &client, state()).await;
        assert_eq!(result.outcome, OrderReconciliationOutcome::Filled);
    }

    #[tokio::test]
    async fn partially_filled_order_via_get_order() {
        let client = MockClient::default();
        client
            .orders
            .lock()
            .unwrap()
            .push(Ok(Some(order(OrderStatus::PartiallyFilled))));
        let result = reconcile_order(&OrderReconciliationConfig::default(), &client, state()).await;
        assert_eq!(result.outcome, OrderReconciliationOutcome::PartiallyFilled);
    }

    #[tokio::test]
    async fn cancelled_order_via_get_order() {
        let client = MockClient::default();
        client
            .orders
            .lock()
            .unwrap()
            .push(Ok(Some(order(OrderStatus::Cancelled))));
        let result = reconcile_order(&OrderReconciliationConfig::default(), &client, state()).await;
        assert_eq!(result.outcome, OrderReconciliationOutcome::Cancelled);
    }

    #[tokio::test]
    async fn order_not_found_is_structured() {
        let client = MockClient::default();
        client.orders.lock().unwrap().push(Ok(None));
        let result = reconcile_order(&OrderReconciliationConfig::default(), &client, state()).await;
        assert_eq!(result.outcome, OrderReconciliationOutcome::OrderNotFound);
        assert!(result.order_not_found);
    }

    #[tokio::test]
    async fn timeout_returns_critical() {
        let client = MockClient::default();
        client
            .orders
            .lock()
            .unwrap()
            .push(Ok(Some(order(OrderStatus::New))));
        let config = OrderReconciliationConfig {
            max_poll_attempts: 1,
            poll_interval_ms: 1,
            ..OrderReconciliationConfig::default()
        };
        let result = reconcile_order(&config, &client, state()).await;
        assert_eq!(result.outcome, OrderReconciliationOutcome::Timeout);
        assert_eq!(result.severity, ReconciliationSeverity::Critical);
    }

    #[tokio::test]
    async fn recent_fills_fallback_works() {
        let client = MockClient::default();
        client.orders.lock().unwrap().push(Ok(None));
        client.fills.lock().unwrap().push(TradeFill {
            exchange: "mexc".to_string(),
            market_type: MarketType::Spot,
            symbol: "BTCUSDT".to_string(),
            trade_id: Some("t1".to_string()),
            order_id: Some("1".to_string()),
            client_order_id: Some("cid".to_string()),
            side: OrderSide::Buy,
            price: 100.0,
            quantity: 1.0,
            fee_asset: Some("USDT".to_string()),
            fee_amount: Some(0.01),
            liquidity: LiquidityRole::Taker,
            timestamp: Utc::now(),
        });
        let result = reconcile_order(&OrderReconciliationConfig::default(), &client, state()).await;
        assert_eq!(result.outcome, OrderReconciliationOutcome::Filled);
        assert!(result.fills.is_some());
    }

    #[tokio::test]
    async fn rate_limit_is_structured() {
        let client = MockClient::default();
        client
            .orders
            .lock()
            .unwrap()
            .push(Err(ExchangeClientError::Classified(ExchangeError {
                exchange: "mexc".to_string(),
                class: ExchangeErrorClass::RateLimited,
                code: None,
                message: "too many requests".to_string(),
            })));
        let result = reconcile_order(&OrderReconciliationConfig::default(), &client, state()).await;
        assert_eq!(result.outcome, OrderReconciliationOutcome::RateLimited);
        assert!(result.rate_limited);
    }
}
