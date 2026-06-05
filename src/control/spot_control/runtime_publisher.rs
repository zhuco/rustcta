use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::sync::{RwLock, Semaphore};
use tokio::time::{sleep, Duration, Instant};

use crate::control::spot_control::lifecycle::{normalize_exchange, normalize_symbol};
use crate::data::{BookCache, BookHealth};
use crate::exchanges::spot_reservation::BalanceReservationManager;
use crate::exchanges::symbol_registry::UnifiedSymbolRegistry;
use crate::exchanges::unified::{
    ExchangeClient, ExchangeClientError, ExchangeErrorClass, MarketType, SymbolRule,
};
use crate::execution::{FeeModel, OrderReconciliationResult};
use crate::live_preflight::{LivePreflightReport, SmallLiveGateReport};
use crate::risk::{DisabledRegistry, KillSwitch, UnmanagedPosition};
use crate::web::RecorderHealthView;

use super::{
    EnableMode, EnabledDirection, RuntimeComponentMetadata, RuntimeDataAuthority,
    RuntimePollComponent, RuntimePublisherConsistencyConfig, RuntimePublisherHealthHandle,
    RuntimePublisherLimitsConfig, RuntimePublisherPollingConfig, RuntimePublisherStartupReport,
    SnapshotComponentStatus, SnapshotConsistency, SnapshotConsistencyReport,
    SpotControlRuntimeCache, SpotControlRuntimePublisherConfig, SpotControlRuntimeSnapshot,
    SpotControlService, SpotControlSnapshotBuilder, SpotControlSnapshotConfig,
    SpotControlSnapshotInputs, SpotControlSnapshotStore,
};

pub type RuntimeExchangeClient = Arc<dyn ExchangeClient>;

#[derive(Clone)]
pub struct SpotControlRuntimePublisher {
    pub exchange_clients: BTreeMap<String, RuntimeExchangeClient>,
    pub symbol_registry: UnifiedSymbolRegistry,
    pub book_cache: BookCache,
    pub book_health: BookHealth,
    pub fee_model: FeeModel,
    pub disabled_registry: DisabledRegistry,
    pub reservation_manager: BalanceReservationManager,
    pub reconciliation_services: RuntimeReconciliationServices,
    pub kill_switch: Option<KillSwitch>,
    pub live_preflight: Option<LivePreflightReport>,
    pub small_live_gate: Option<SmallLiveGateReport>,
    pub recorder_health: Option<RecorderHealthView>,
    pub snapshot_config: SpotControlSnapshotConfig,
    pub snapshot_store: Option<Arc<dyn SpotControlSnapshotStore>>,
    pub publisher_health: RuntimePublisherHealthHandle,
    pub config: SpotControlRuntimePublisherConfig,
    cache: SpotControlRuntimeCache,
    scheduler: Arc<RwLock<RuntimePollingScheduler>>,
    refreshed_symbol_rules: Arc<RwLock<Vec<SymbolRule>>>,
}

#[derive(Debug, Clone, Default)]
pub struct RuntimeReconciliationServices {
    pub unmanaged_positions: Vec<UnmanagedPosition>,
    pub order_reconciliation_state: Option<OrderReconciliationResult>,
}

#[derive(Clone)]
pub struct SpotControlRuntimePublisherDeps {
    pub exchange_clients: BTreeMap<String, RuntimeExchangeClient>,
    pub symbol_registry: UnifiedSymbolRegistry,
    pub book_cache: BookCache,
    pub book_health: BookHealth,
    pub fee_model: FeeModel,
    pub disabled_registry: DisabledRegistry,
    pub reservation_manager: BalanceReservationManager,
    pub reconciliation_services: RuntimeReconciliationServices,
    pub kill_switch: Option<KillSwitch>,
    pub live_preflight: Option<LivePreflightReport>,
    pub small_live_gate: Option<SmallLiveGateReport>,
    pub recorder_health: Option<RecorderHealthView>,
    pub snapshot_config: SpotControlSnapshotConfig,
    pub snapshot_store: Option<Arc<dyn SpotControlSnapshotStore>>,
}

impl SpotControlRuntimePublisher {
    pub fn new(
        config: SpotControlRuntimePublisherConfig,
        deps: SpotControlRuntimePublisherDeps,
    ) -> Self {
        Self {
            exchange_clients: deps.exchange_clients,
            symbol_registry: deps.symbol_registry,
            book_cache: deps.book_cache,
            book_health: deps.book_health,
            fee_model: deps.fee_model,
            disabled_registry: deps.disabled_registry,
            reservation_manager: deps.reservation_manager,
            reconciliation_services: deps.reconciliation_services,
            kill_switch: deps.kill_switch,
            live_preflight: deps.live_preflight,
            small_live_gate: deps.small_live_gate,
            recorder_health: deps.recorder_health,
            snapshot_config: deps.snapshot_config,
            snapshot_store: deps.snapshot_store,
            publisher_health: RuntimePublisherHealthHandle::default(),
            scheduler: Arc::new(RwLock::new(RuntimePollingScheduler::new(
                config.polling.clone(),
                config.limits.clone(),
            ))),
            refreshed_symbol_rules: Arc::new(RwLock::new(Vec::new())),
            cache: SpotControlRuntimeCache::default(),
            config,
        }
    }

    pub fn cache(&self) -> SpotControlRuntimeCache {
        self.cache.clone()
    }

    pub async fn health(&self) -> super::RuntimePublisherHealth {
        self.publisher_health.snapshot().await
    }

    pub async fn latest_acceptable_snapshot(
        &self,
        control: &SpotControlService,
        symbol: &str,
    ) -> Option<SpotControlRuntimeSnapshot> {
        let snapshot = control.latest_runtime_snapshot(symbol).await?;
        snapshot
            .consistency_report
            .as_ref()
            .is_some_and(|report| report.allows_control())
            .then_some(snapshot)
    }

    pub async fn poll_once(&self) {
        self.publisher_health.mark_running().await;
        for (exchange, client) in &self.exchange_clients {
            if !client.capabilities().supports_spot {
                continue;
            }
            self.poll_exchange(exchange, client.clone()).await;
        }
    }

    pub async fn publish_once_for_symbols(
        &self,
        control: &SpotControlService,
        requests: &[RuntimeSnapshotBuildRequest],
    ) -> Vec<Result<SpotControlRuntimeSnapshot, String>> {
        let mut output = Vec::new();
        for request in requests {
            let result = match self.build_snapshot(request).await {
                Ok(snapshot) => self.persist_and_publish(control, snapshot).await,
                Err(error) => Err(error),
            };
            output.push(result);
        }
        output
    }

    pub async fn request_refresh_and_wait(
        &self,
        control: &SpotControlService,
        request: RuntimeSnapshotBuildRequest,
    ) -> Result<SpotControlRuntimeSnapshot, String> {
        self.poll_once().await;
        let mut results = self.publish_once_for_symbols(control, &[request]).await;
        results
            .pop()
            .unwrap_or_else(|| Err("runtime publisher did not produce a snapshot".to_string()))
    }

    pub async fn startup_report(
        &self,
        control: &SpotControlService,
        symbols: &[String],
    ) -> RuntimePublisherStartupReport {
        let mut report = RuntimePublisherStartupReport {
            restored_symbols: symbols
                .iter()
                .map(|symbol| normalize_symbol(symbol))
                .collect::<Vec<_>>(),
            write_actions_unblocked: false,
            ..RuntimePublisherStartupReport::default()
        };
        if let Some(store) = &self.snapshot_store {
            for symbol in symbols {
                match store.get_latest_snapshot(symbol).await {
                    Ok(Some(mut snapshot)) => {
                        snapshot.warnings.push(
                            "restored historical snapshot is stale until fresh polling completes"
                                .to_string(),
                        );
                        snapshot.consistency_report = Some(SnapshotConsistencyReport {
                            snapshot_id: snapshot.snapshot_id.clone(),
                            generated_at: snapshot.generated_at,
                            oldest_required_component_at: None,
                            newest_component_at: None,
                            maximum_component_age_ms: snapshot.age_ms(),
                            component_time_skew_ms: 0,
                            status: SnapshotConsistency::Stale,
                            warnings: snapshot.warnings.clone(),
                            critical_errors: vec![
                                "startup historical snapshot cannot unblock write actions"
                                    .to_string(),
                            ],
                        });
                        control.record_runtime_snapshot(snapshot).await;
                        report.loaded_snapshot_count += 1;
                    }
                    Ok(None) => report.blockers.push(format!(
                        "no persisted snapshot for {}",
                        normalize_symbol(symbol)
                    )),
                    Err(error) => report.blockers.push(error),
                }
            }
        }
        let health = self.publisher_health.snapshot().await;
        for (exchange, item) in health.per_exchange_health {
            if item.last_successful_request.is_some() {
                report.fresh_exchanges.push(exchange);
            } else {
                report.stale_exchanges.push(exchange);
            }
        }
        report
            .blockers
            .push("fresh runtime publisher snapshot required before write actions".to_string());
        report
    }

    pub async fn run(
        self,
        control: SpotControlService,
        mut requests: tokio::sync::watch::Receiver<Vec<RuntimeSnapshotBuildRequest>>,
    ) {
        if !self.config.enabled {
            return;
        }
        self.publisher_health.mark_running().await;
        loop {
            self.poll_once().await;
            let current_requests = requests.borrow_and_update().clone();
            let _ = self
                .publish_once_for_symbols(&control, &current_requests)
                .await;
            sleep(Duration::from_millis(
                self.config.polling.snapshot_interval_ms.max(100),
            ))
            .await;
        }
    }

    async fn poll_exchange(&self, exchange: &str, client: RuntimeExchangeClient) {
        let semaphore = Arc::new(Semaphore::new(
            self.config
                .limits
                .maximum_concurrent_requests_per_exchange
                .max(1),
        ));
        if self
            .should_poll(exchange, RuntimePollComponent::Balances)
            .await
        {
            let _permit = semaphore.clone().acquire_owned().await;
            self.poll_balances(exchange, client.clone()).await;
        }
        if self
            .should_poll(exchange, RuntimePollComponent::OpenOrders)
            .await
        {
            let _permit = semaphore.clone().acquire_owned().await;
            self.poll_open_orders(exchange, client.clone()).await;
        }
        if self
            .should_poll(exchange, RuntimePollComponent::RecentFills)
            .await
        {
            let _permit = semaphore.clone().acquire_owned().await;
            self.poll_recent_fills(exchange, client.clone()).await;
        }
        if self
            .should_poll(exchange, RuntimePollComponent::SymbolRules)
            .await
        {
            let _permit = semaphore.clone().acquire_owned().await;
            self.poll_symbol_rules(exchange, client.clone()).await;
        }
        if self.should_poll(exchange, RuntimePollComponent::Fees).await {
            let _permit = semaphore.acquire_owned().await;
            self.poll_fee_probe(exchange, client).await;
        }
    }

    async fn should_poll(&self, exchange: &str, component: RuntimePollComponent) -> bool {
        let mut scheduler = self.scheduler.write().await;
        scheduler.should_poll(exchange, component, Utc::now())
    }

    async fn poll_balances(&self, exchange: &str, client: RuntimeExchangeClient) {
        self.before_request().await;
        let started = Instant::now();
        match client.get_balances().await {
            Ok(snapshot) => {
                let mut critical_errors = self
                    .cache
                    .apply_balance_success(
                        snapshot,
                        &self.reservation_manager,
                        &self.reconciliation_services.unmanaged_positions,
                    )
                    .await;
                if !critical_errors.is_empty() {
                    self.publisher_health
                        .mark_snapshot_persist_failed(critical_errors.remove(0))
                        .await;
                }
                self.record_success(exchange, RuntimePollComponent::Balances, started)
                    .await;
            }
            Err(error) => {
                self.cache
                    .apply_balance_failure(exchange, error.to_string())
                    .await;
                self.record_failure(exchange, RuntimePollComponent::Balances, &error)
                    .await;
            }
        }
    }

    async fn poll_open_orders(&self, exchange: &str, client: RuntimeExchangeClient) {
        self.before_request().await;
        let started = Instant::now();
        match client.get_open_orders(None).await {
            Ok(orders) => {
                let _warnings = self.cache.apply_open_orders_success(exchange, orders).await;
                self.record_success(exchange, RuntimePollComponent::OpenOrders, started)
                    .await;
            }
            Err(error) => {
                self.cache
                    .apply_open_orders_failure(exchange, error.to_string())
                    .await;
                self.record_failure(exchange, RuntimePollComponent::OpenOrders, &error)
                    .await;
            }
        }
    }

    async fn poll_recent_fills(&self, exchange: &str, client: RuntimeExchangeClient) {
        self.before_request().await;
        let started = Instant::now();
        let symbols = self
            .symbol_registry
            .supported_symbols(exchange, MarketType::Spot);
        if symbols.is_empty() {
            self.cache
                .apply_fills_unsupported(exchange, "no managed symbols for fill polling")
                .await;
            return;
        }
        let mut all = Vec::new();
        for symbol in symbols {
            match client.get_recent_fills(&symbol).await {
                Ok(fills) => all.extend(fills),
                Err(ExchangeClientError::Unsupported(error)) => {
                    self.cache.apply_fills_unsupported(exchange, error).await;
                    return;
                }
                Err(error) => {
                    self.cache
                        .apply_fills_failure(exchange, error.to_string())
                        .await;
                    self.record_failure(exchange, RuntimePollComponent::RecentFills, &error)
                        .await;
                    return;
                }
            }
        }
        self.cache.apply_fills_success(exchange, all).await;
        self.record_success(exchange, RuntimePollComponent::RecentFills, started)
            .await;
    }

    async fn poll_symbol_rules(&self, exchange: &str, client: RuntimeExchangeClient) {
        self.before_request().await;
        let started = Instant::now();
        match client.load_symbol_rules().await {
            Ok(mut rules) => {
                rules.retain(|rule| rule.market_type == MarketType::Spot);
                let mut guard = self.refreshed_symbol_rules.write().await;
                guard.retain(|rule| {
                    normalize_exchange(&rule.exchange) != normalize_exchange(exchange)
                });
                guard.extend(rules);
                self.record_success(exchange, RuntimePollComponent::SymbolRules, started)
                    .await;
            }
            Err(ExchangeClientError::Unsupported(_)) => {
                let mut scheduler = self.scheduler.write().await;
                scheduler.record_poll(exchange, RuntimePollComponent::SymbolRules, Utc::now());
            }
            Err(error) => {
                self.record_failure(exchange, RuntimePollComponent::SymbolRules, &error)
                    .await;
            }
        }
    }

    async fn poll_fee_probe(&self, exchange: &str, client: RuntimeExchangeClient) {
        self.before_request().await;
        let started = Instant::now();
        let symbol = self
            .symbol_registry
            .supported_symbols(exchange, MarketType::Spot)
            .into_iter()
            .next();
        let Some(symbol) = symbol else {
            return;
        };
        match client.get_fee_rate(&symbol).await {
            Ok(_) | Err(ExchangeClientError::Unsupported(_)) => {
                self.record_success(exchange, RuntimePollComponent::Fees, started)
                    .await;
            }
            Err(error) => {
                self.record_failure(exchange, RuntimePollComponent::Fees, &error)
                    .await;
            }
        }
    }

    async fn build_snapshot(
        &self,
        request: &RuntimeSnapshotBuildRequest,
    ) -> Result<SpotControlRuntimeSnapshot, String> {
        let symbol = normalize_symbol(&request.symbol);
        let selected_exchanges = request
            .selected_exchanges
            .iter()
            .map(|exchange| normalize_exchange(exchange))
            .collect::<Vec<_>>();
        let mut inputs = self.cache.inputs_for_snapshot(&selected_exchanges).await;
        inputs.symbol_registry = Some(self.symbol_registry.clone());
        inputs.symbol_rules = self.refreshed_symbol_rules.read().await.clone();
        inputs.books = self.books_for(&symbol, &selected_exchanges).await;
        inputs.exchange_health = self.exchange_health_statuses(&selected_exchanges).await;
        inputs.fee_model = Some(self.fee_model.clone());
        inputs.disabled_registry = Some(self.disabled_registry.clone());
        inputs.unmanaged_positions = self.reconciliation_services.unmanaged_positions.clone();
        inputs.order_reconciliation_state = self
            .reconciliation_services
            .order_reconciliation_state
            .clone();
        inputs.kill_switch_state = self.kill_switch.as_ref().map(KillSwitch::state);
        inputs.live_preflight_state = self.live_preflight.clone();
        inputs.small_live_gate_state = self.small_live_gate.clone();
        inputs.recorder_health = self.recorder_health.clone();
        inputs.operation_lock_allows = true;

        let mut snapshot = SpotControlSnapshotBuilder::new(self.snapshot_config.clone(), inputs)
            .build(
                &symbol,
                &selected_exchanges,
                &request.directions,
                request.mode,
            );
        snapshot.fill_ownership = self
            .cache
            .fill_records_for_exchanges(&selected_exchanges)
            .await;
        snapshot.source_metadata = self.source_metadata(&snapshot, &selected_exchanges).await;
        snapshot
            .data_sources
            .push("spot_control_runtime_publisher".to_string());
        let consistency = self.consistency_report(&snapshot, &self.config.consistency);
        snapshot.consistency_report = Some(consistency);
        Ok(snapshot)
    }

    async fn books_for(
        &self,
        symbol: &str,
        exchanges: &[String],
    ) -> Vec<crate::exchanges::unified::OrderBookSnapshot> {
        let mut books = Vec::new();
        for exchange in exchanges {
            if let Some(book) = self
                .book_cache
                .get_book(exchange, MarketType::Spot, symbol)
                .await
            {
                books.push(book.into_snapshot());
            }
        }
        books
    }

    async fn exchange_health_statuses(&self, exchanges: &[String]) -> Vec<SnapshotComponentStatus> {
        let health = self.book_health.snapshot().await;
        exchanges
            .iter()
            .map(|exchange| {
                let exchange = normalize_exchange(exchange);
                match health.iter().find(|item| item.exchange == exchange) {
                    Some(item) if item.connected => SnapshotComponentStatus::fresh(
                        format!("exchange_health:{exchange}"),
                        item.last_message_at
                            .or(item.last_book_update_at)
                            .unwrap_or_else(Utc::now),
                        "book_health",
                    ),
                    Some(_) => SnapshotComponentStatus::error(
                        format!("exchange_health:{exchange}"),
                        "book_health",
                        "exchange WebSocket health is disconnected",
                    ),
                    None => SnapshotComponentStatus::missing(
                        format!("exchange_health:{exchange}"),
                        "book_health",
                    ),
                }
            })
            .collect()
    }

    async fn source_metadata(
        &self,
        snapshot: &SpotControlRuntimeSnapshot,
        selected_exchanges: &[String],
    ) -> Vec<RuntimeComponentMetadata> {
        let mut metadata = self.cache.source_metadata(selected_exchanges).await;
        for status in &snapshot.component_statuses {
            metadata.push(RuntimeComponentMetadata {
                component: status.component.clone(),
                authority: authority_for_component(&status.component),
                fetched_at: status.updated_at.unwrap_or(snapshot.generated_at),
                exchange_timestamp_optional: None,
                age_ms: status.age_ms.unwrap_or_default(),
                freshness_status: status.status,
                error_optional: status.warning_optional.clone(),
            });
        }
        metadata
    }

    fn consistency_report(
        &self,
        snapshot: &SpotControlRuntimeSnapshot,
        config: &RuntimePublisherConsistencyConfig,
    ) -> SnapshotConsistencyReport {
        let mut report =
            SnapshotConsistencyReport::from_snapshot(snapshot, config.max_component_time_skew_ms);
        for status in &snapshot.component_statuses {
            if status.component.starts_with("book:")
                && status.age_ms.unwrap_or(i64::MAX) > config.max_book_age_ms as i64
            {
                report
                    .critical_errors
                    .push(format!("{} exceeds book age policy", status.component));
            }
            if status.component.starts_with("fee:")
                && status.age_ms.unwrap_or_default() > (config.max_fee_age_seconds * 1_000) as i64
            {
                report
                    .critical_errors
                    .push(format!("{} exceeds fee age policy", status.component));
            }
        }
        for metadata in &snapshot.source_metadata {
            if metadata.component.starts_with("balances:")
                && metadata.age_ms > config.max_balance_age_ms as i64
            {
                report
                    .critical_errors
                    .push(format!("{} exceeds balance age policy", metadata.component));
            }
            if metadata.component.starts_with("open_orders:")
                && metadata.age_ms > config.max_open_orders_age_ms as i64
            {
                report.critical_errors.push(format!(
                    "{} exceeds open order age policy",
                    metadata.component
                ));
            }
            if metadata.authority == RuntimeDataAuthority::ConfigFallback
                && metadata.component.starts_with("balances:")
            {
                report
                    .critical_errors
                    .push("ConfigFallback cannot be live account authority".to_string());
            }
        }
        if !report.critical_errors.is_empty() {
            report.status = if report
                .critical_errors
                .iter()
                .any(|item| item.contains("missing"))
            {
                SnapshotConsistency::MissingCriticalData
            } else {
                SnapshotConsistency::Stale
            };
        }
        report
    }

    async fn persist_and_publish(
        &self,
        control: &SpotControlService,
        snapshot: SpotControlRuntimeSnapshot,
    ) -> Result<SpotControlRuntimeSnapshot, String> {
        if let Some(store) = &self.snapshot_store {
            match store.persist_snapshot(&snapshot).await {
                Ok(()) => self.publisher_health.mark_snapshot_persisted().await,
                Err(error) => {
                    self.publisher_health
                        .mark_snapshot_persist_failed(error.clone())
                        .await;
                    if self
                        .config
                        .snapshot_store
                        .require_persistence_for_write_commands
                    {
                        return Err(error);
                    }
                }
            }
        }
        self.publisher_health.mark_snapshot_generated().await;
        control.record_runtime_snapshot(snapshot.clone()).await;
        Ok(snapshot)
    }

    async fn before_request(&self) {
        let spacing = self.config.limits.minimum_request_spacing_ms;
        let jitter = self.config.limits.jitter_ms;
        let jitter_ms = if jitter == 0 {
            0
        } else {
            rand::thread_rng().gen_range(0..=jitter)
        };
        sleep(Duration::from_millis(spacing + jitter_ms)).await;
    }

    async fn record_success(
        &self,
        exchange: &str,
        component: RuntimePollComponent,
        started: Instant,
    ) {
        self.scheduler
            .write()
            .await
            .record_poll(exchange, component, Utc::now());
        self.publisher_health
            .mark_success(exchange, component, started.elapsed().as_millis() as i64)
            .await;
    }

    async fn record_failure(
        &self,
        exchange: &str,
        component: RuntimePollComponent,
        error: &ExchangeClientError,
    ) {
        let rate_limited = is_rate_limit(error);
        self.scheduler
            .write()
            .await
            .record_failure(exchange, component, Utc::now(), rate_limited);
        self.publisher_health
            .mark_failure(
                exchange,
                component,
                error.to_string(),
                rate_limited,
                &self.config.limits,
            )
            .await;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeSnapshotBuildRequest {
    pub symbol: String,
    pub selected_exchanges: Vec<String>,
    pub directions: Vec<EnabledDirection>,
    pub mode: EnableMode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimePollingScheduler {
    polling: RuntimePublisherPollingConfig,
    limits: RuntimePublisherLimitsConfig,
    last_poll: BTreeMap<(String, RuntimePollComponent), DateTime<Utc>>,
    backoff_until: BTreeMap<String, DateTime<Utc>>,
}

impl RuntimePollingScheduler {
    pub fn new(
        polling: RuntimePublisherPollingConfig,
        limits: RuntimePublisherLimitsConfig,
    ) -> Self {
        Self {
            polling,
            limits,
            last_poll: BTreeMap::new(),
            backoff_until: BTreeMap::new(),
        }
    }

    pub fn should_poll(
        &mut self,
        exchange: &str,
        component: RuntimePollComponent,
        now: DateTime<Utc>,
    ) -> bool {
        let exchange = normalize_exchange(exchange);
        if self
            .backoff_until
            .get(&exchange)
            .is_some_and(|until| *until > now)
        {
            return false;
        }
        let interval_ms = self.interval_ms(component);
        let key = (exchange, component);
        self.last_poll
            .get(&key)
            .map(|last| now.signed_duration_since(*last).num_milliseconds() >= interval_ms as i64)
            .unwrap_or(true)
    }

    pub fn record_poll(
        &mut self,
        exchange: &str,
        component: RuntimePollComponent,
        now: DateTime<Utc>,
    ) {
        self.last_poll
            .insert((normalize_exchange(exchange), component), now);
    }

    pub fn record_failure(
        &mut self,
        exchange: &str,
        component: RuntimePollComponent,
        now: DateTime<Utc>,
        rate_limited: bool,
    ) {
        self.record_poll(exchange, component, now);
        let pause_ms = if rate_limited {
            self.limits.pause_after_rate_limit_ms
        } else {
            self.limits.exponential_backoff_initial_ms
        };
        self.backoff_until.insert(
            normalize_exchange(exchange),
            now + chrono::Duration::milliseconds(pause_ms as i64),
        );
    }

    fn interval_ms(&self, component: RuntimePollComponent) -> u64 {
        match component {
            RuntimePollComponent::Balances => self.polling.balances_interval_ms,
            RuntimePollComponent::OpenOrders => self.polling.open_orders_interval_ms,
            RuntimePollComponent::RecentFills => self.polling.recent_fills_interval_ms,
            RuntimePollComponent::SymbolRules => self.polling.symbol_rules_interval_seconds * 1_000,
            RuntimePollComponent::Fees => self.polling.fee_interval_seconds * 1_000,
        }
    }
}

fn is_rate_limit(error: &ExchangeClientError) -> bool {
    matches!(
        error,
        ExchangeClientError::Classified(classified)
            if classified.class == ExchangeErrorClass::RateLimited
    ) || error
        .to_string()
        .to_ascii_lowercase()
        .contains("rate limit")
}

fn authority_for_component(component: &str) -> RuntimeDataAuthority {
    if component.starts_with("book:") || component.starts_with("exchange_health:") {
        RuntimeDataAuthority::ExchangeWebSocket
    } else if component.starts_with("symbol_rule:") {
        RuntimeDataAuthority::LocalRegistry
    } else if component.starts_with("fee:") {
        RuntimeDataAuthority::ConfigFallback
    } else if component.contains("reconciliation") {
        RuntimeDataAuthority::ReconciliationService
    } else {
        RuntimeDataAuthority::LocalRegistry
    }
}
