#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Instant};
use tower::ServiceExt;

use rustcta::control::spot_control::{
    classify_order_ownership, snapshot_store_from_config, EnableMode, EnabledDirection,
    JsonlSpotControlSnapshotStore, OrderOwnershipClass, RuntimeDataAuthority,
    RuntimePublisherLimitsConfig, RuntimePublisherPollingConfig, RuntimeReconciliationServices,
    RuntimeSnapshotBuildRequest, SnapshotConsistency, SpotControlRuntimeCache,
    SpotControlRuntimePublisher, SpotControlRuntimePublisherConfig,
    SpotControlRuntimePublisherDeps, SpotControlService, SpotControlSnapshotConfig,
    SpotControlSnapshotReplay, SpotControlSnapshotStore, SpotSymbolControlConfig,
};
use rustcta::data::{BookCache, BookHealth, BookSource};
use rustcta::exchanges::spot_reservation::BalanceReservationManager;
use rustcta::exchanges::symbol_registry::UnifiedSymbolRegistry;
use rustcta::exchanges::unified::{
    AssetBalance, BalanceSnapshot, CancelOrderRequest, CancelOrderResponse, ExchangeClient,
    ExchangeClientError, ExchangeClientResult, ExchangeError, ExchangeErrorClass, FeeRate,
    MarketType, OrderBookSnapshot, OrderRequest, OrderResponse, OrderSide, OrderStatus, TradeFill,
    UserStreamEvent,
};
use rustcta::execution::{reconcile_balances, FeeModel};
use rustcta::risk::{DisabledRegistry, KillSwitch, KillSwitchConfig};
use rustcta::web::{router, DashboardReadModel, MonitoringConfig, MonitoringState};

pub const ENABLE_ENV: &str = "ENABLE_LIVE_READONLY_TESTS";

#[derive(Debug, Clone)]
pub struct LiveReadonlyTestConfig {
    pub symbol: String,
    pub market_type: MarketType,
    pub duration_seconds: u64,
    pub output_dir: PathBuf,
    pub require_no_withdraw_permission: bool,
    pub max_rest_requests_per_minute: u64,
    pub safe_debug_balances: bool,
}

impl LiveReadonlyTestConfig {
    pub fn from_env() -> Option<Self> {
        if !std::env::var(ENABLE_ENV)
            .map(|value| value.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
        {
            eprintln!("{ENABLE_ENV}=true is not set; skipping live read-only network test");
            return None;
        }
        let market_type =
            std::env::var("LIVE_TEST_MARKET_TYPE").unwrap_or_else(|_| "spot".to_string());
        if !market_type.eq_ignore_ascii_case("spot") {
            eprintln!("LIVE_TEST_MARKET_TYPE={market_type} is not spot; skipping");
            return None;
        }
        Some(Self {
            symbol: std::env::var("LIVE_TEST_SYMBOL").unwrap_or_else(|_| "BTCUSDT".to_string()),
            market_type: MarketType::Spot,
            duration_seconds: std::env::var("LIVE_TEST_DURATION_SECONDS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(120),
            output_dir: std::env::var("LIVE_TEST_OUTPUT_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("data/live_readonly_tests")),
            require_no_withdraw_permission: std::env::var(
                "LIVE_TEST_REQUIRE_NO_WITHDRAW_PERMISSION",
            )
            .map(|value| value.eq_ignore_ascii_case("true"))
            .unwrap_or(true),
            max_rest_requests_per_minute: std::env::var("LIVE_TEST_MAX_REST_REQUESTS_PER_MINUTE")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(30),
            safe_debug_balances: std::env::var("LIVE_TEST_SAFE_DEBUG_BALANCES")
                .map(|value| value.eq_ignore_ascii_case("true"))
                .unwrap_or(false),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PermissionStatus {
    Pass,
    Fail,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadOnlyPermissionReport {
    pub exchange: String,
    pub credentials_valid: bool,
    pub read_permission: PermissionStatus,
    pub trade_permission: PermissionStatus,
    pub withdrawal_permission: PermissionStatus,
    pub permission_introspection_supported: bool,
    pub warnings: Vec<String>,
    pub critical_errors: Vec<String>,
}

impl ReadOnlyPermissionReport {
    pub fn unknown(exchange: &str) -> Self {
        Self {
            exchange: exchange.to_string(),
            credentials_valid: false,
            read_permission: PermissionStatus::Unknown,
            trade_permission: PermissionStatus::Unknown,
            withdrawal_permission: PermissionStatus::Unknown,
            permission_introspection_supported: false,
            warnings: vec![
                "permission introspection is unsupported; operator must verify read-only key manually"
                    .to_string(),
            ],
            critical_errors: Vec::new(),
        }
    }

    pub fn apply_withdraw_policy(&mut self, require_no_withdraw_permission: bool) {
        if require_no_withdraw_permission && self.withdrawal_permission == PermissionStatus::Pass {
            self.critical_errors.push(
                "withdrawal permission is enabled; read-only validation requires it disabled"
                    .to_string(),
            );
        }
        if !self.permission_introspection_supported {
            self.warnings.push(
                "withdrawal permission could not be inspected by API; verify manually in exchange UI"
                    .to_string(),
            );
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LatencyStats {
    pub count: u64,
    pub min_ms: Option<i64>,
    pub max_ms: Option<i64>,
    pub avg_ms: Option<f64>,
}

impl LatencyStats {
    pub fn record(&mut self, latency_ms: i64) {
        self.count += 1;
        self.min_ms = Some(self.min_ms.unwrap_or(latency_ms).min(latency_ms));
        self.max_ms = Some(self.max_ms.unwrap_or(latency_ms).max(latency_ms));
        self.avg_ms = Some(match self.avg_ms {
            Some(avg) => avg * 0.8 + latency_ms as f64 * 0.2,
            None => latency_ms as f64,
        });
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeRateLimitProfile {
    pub exchange: String,
    pub endpoint_group: String,
    pub configured_limit: String,
    pub conservative_safe_limit: String,
    pub minimum_request_spacing_ms: u64,
    pub maximum_concurrency: usize,
    pub backoff_policy: String,
    pub observed_rate_limit_events: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveReadOnlyExchangeReport {
    pub exchange: String,
    pub test_started_at: DateTime<Utc>,
    pub test_duration_seconds: u64,
    pub credential_status: String,
    pub permission_report: ReadOnlyPermissionReport,
    pub balance_endpoint_supported: bool,
    pub balance_poll_success_count: u64,
    pub balance_poll_failure_count: u64,
    pub balance_latency_stats: LatencyStats,
    pub open_orders_supported: bool,
    pub open_order_count: usize,
    pub unknown_order_count: usize,
    pub open_order_mapping_warnings: Vec<String>,
    pub recent_fills_supported: bool,
    pub recent_fill_count: usize,
    pub fill_mapping_warnings: Vec<String>,
    pub public_websocket_connected: bool,
    pub book_events_received: u64,
    pub book_stale_count: u64,
    pub reconnect_count: u64,
    pub sequence_gap_count: u64,
    pub snapshots_generated: u64,
    pub snapshots_persisted: u64,
    pub strong_enough_for_control_count: u64,
    pub observation_only_count: u64,
    pub rate_limit_events: u64,
    pub backoff_events: u64,
    pub mutation_calls_detected: u64,
    pub rate_limit_profile: ExchangeRateLimitProfile,
    pub warnings: Vec<String>,
    pub critical_errors: Vec<String>,
}

impl LiveReadOnlyExchangeReport {
    pub fn new(exchange: &str, config: &LiveReadonlyTestConfig) -> Self {
        Self {
            exchange: exchange.to_string(),
            test_started_at: Utc::now(),
            test_duration_seconds: config.duration_seconds,
            credential_status: "not_checked".to_string(),
            permission_report: ReadOnlyPermissionReport::unknown(exchange),
            balance_endpoint_supported: false,
            balance_poll_success_count: 0,
            balance_poll_failure_count: 0,
            balance_latency_stats: LatencyStats::default(),
            open_orders_supported: false,
            open_order_count: 0,
            unknown_order_count: 0,
            open_order_mapping_warnings: Vec::new(),
            recent_fills_supported: false,
            recent_fill_count: 0,
            fill_mapping_warnings: Vec::new(),
            public_websocket_connected: false,
            book_events_received: 0,
            book_stale_count: 0,
            reconnect_count: 0,
            sequence_gap_count: 0,
            snapshots_generated: 0,
            snapshots_persisted: 0,
            strong_enough_for_control_count: 0,
            observation_only_count: 0,
            rate_limit_events: 0,
            backoff_events: 0,
            mutation_calls_detected: 0,
            rate_limit_profile: ExchangeRateLimitProfile {
                exchange: exchange.to_string(),
                endpoint_group: "spot_readonly".to_string(),
                configured_limit: format!(
                    "{} requests/minute test cap",
                    config.max_rest_requests_per_minute
                ),
                conservative_safe_limit: "start at 20-30 private REST requests/minute until production telemetry confirms venue-specific headroom".to_string(),
                minimum_request_spacing_ms: 2_000,
                maximum_concurrency: 1,
                backoff_policy: "pause 60s on rate-limit, exponential backoff on other exchange errors".to_string(),
                observed_rate_limit_events: 0,
            },
            warnings: Vec::new(),
            critical_errors: Vec::new(),
        }
    }

    pub fn assert_no_critical_errors(&self) {
        assert!(
            self.critical_errors.is_empty(),
            "{} critical errors: {:?}",
            self.exchange,
            self.critical_errors
        );
        assert_eq!(
            self.mutation_calls_detected, 0,
            "{} mutation calls detected",
            self.exchange
        );
    }
}

#[derive(Debug, Default)]
pub struct MutationCallDetector {
    calls: Mutex<HashMap<&'static str, u64>>,
}

impl MutationCallDetector {
    pub fn record(&self, method: &'static str) {
        *self
            .calls
            .lock()
            .expect("mutation detector lock")
            .entry(method)
            .or_default() += 1;
    }

    pub fn count(&self, method: &'static str) -> u64 {
        self.calls
            .lock()
            .expect("mutation detector lock")
            .get(method)
            .copied()
            .unwrap_or_default()
    }

    pub fn total_mutations(&self) -> u64 {
        self.count("place_order") + self.count("cancel_order")
    }
}

#[derive(Clone)]
pub struct ReadOnlyGuardClient<C> {
    inner: C,
    detector: Arc<MutationCallDetector>,
}

impl<C> ReadOnlyGuardClient<C> {
    pub fn new(inner: C, detector: Arc<MutationCallDetector>) -> Self {
        Self { inner, detector }
    }

    pub fn detector(&self) -> Arc<MutationCallDetector> {
        self.detector.clone()
    }
}

#[async_trait]
impl<C> ExchangeClient for ReadOnlyGuardClient<C>
where
    C: ExchangeClient + Clone + Send + Sync + 'static,
{
    fn market_type(&self) -> MarketType {
        self.inner.market_type()
    }

    fn exchange_name(&self) -> &str {
        self.inner.exchange_name()
    }

    fn capabilities(&self) -> rustcta::exchanges::unified::ExchangeClientCapabilities {
        self.inner.capabilities()
    }

    fn normalize_symbol(&self, symbol: &str) -> ExchangeClientResult<String> {
        self.inner.normalize_symbol(symbol)
    }

    async fn get_balances(&self) -> ExchangeClientResult<BalanceSnapshot> {
        self.inner.get_balances().await
    }

    async fn get_orderbook(
        &self,
        symbol: &str,
        depth: u16,
    ) -> ExchangeClientResult<OrderBookSnapshot> {
        self.inner.get_orderbook(symbol, depth).await
    }

    async fn place_order(&self, _request: OrderRequest) -> ExchangeClientResult<OrderResponse> {
        self.detector.record("place_order");
        Err(ExchangeClientError::Classified(ExchangeError {
            exchange: self.exchange_name().to_string(),
            class: ExchangeErrorClass::PermissionDenied,
            code: Some("readonly_guard".to_string()),
            message: "mutation endpoint place_order is forbidden in live read-only tests"
                .to_string(),
        }))
    }

    async fn cancel_order(
        &self,
        _request: CancelOrderRequest,
    ) -> ExchangeClientResult<CancelOrderResponse> {
        self.detector.record("cancel_order");
        Err(ExchangeClientError::Classified(ExchangeError {
            exchange: self.exchange_name().to_string(),
            class: ExchangeErrorClass::PermissionDenied,
            code: Some("readonly_guard".to_string()),
            message: "mutation endpoint cancel_order is forbidden in live read-only tests"
                .to_string(),
        }))
    }

    async fn get_order(&self, symbol: &str, order_id: &str) -> ExchangeClientResult<OrderResponse> {
        self.inner.get_order(symbol, order_id).await
    }

    async fn get_open_orders(
        &self,
        symbol: Option<&str>,
    ) -> ExchangeClientResult<Vec<OrderResponse>> {
        self.inner.get_open_orders(symbol).await
    }

    async fn get_fee_rate(&self, symbol: &str) -> ExchangeClientResult<FeeRate> {
        self.inner.get_fee_rate(symbol).await
    }

    async fn load_symbol_rules(
        &self,
    ) -> ExchangeClientResult<Vec<rustcta::exchanges::unified::SymbolRule>> {
        self.inner.load_symbol_rules().await
    }

    async fn get_symbol_rule(
        &self,
        symbol: &str,
    ) -> ExchangeClientResult<Option<rustcta::exchanges::unified::SymbolRule>> {
        self.inner.get_symbol_rule(symbol).await
    }

    fn denormalize_symbol(&self, symbol: &str) -> ExchangeClientResult<String> {
        self.inner.denormalize_symbol(symbol)
    }

    async fn get_recent_fills(&self, symbol: &str) -> ExchangeClientResult<Vec<TradeFill>> {
        self.inner.get_recent_fills(symbol).await
    }

    async fn health_check(
        &self,
    ) -> ExchangeClientResult<rustcta::exchanges::unified::ExchangeHealthStatus> {
        self.inner.health_check().await
    }

    async fn subscribe_orderbook(
        &self,
        symbols: Vec<String>,
    ) -> ExchangeClientResult<mpsc::Receiver<OrderBookSnapshot>> {
        self.inner.subscribe_orderbook(symbols).await
    }

    async fn subscribe_user_stream(&self) -> ExchangeClientResult<mpsc::Receiver<UserStreamEvent>> {
        self.inner.subscribe_user_stream().await
    }
}

pub async fn validate_authenticated_readonly<C>(
    client: &ReadOnlyGuardClient<C>,
    config: &LiveReadonlyTestConfig,
    report: &mut LiveReadOnlyExchangeReport,
) where
    C: ExchangeClient + Clone + Send + Sync + 'static,
{
    let mut permission = ReadOnlyPermissionReport::unknown(client.exchange_name());
    let mut last_balances: Option<Vec<AssetBalance>> = None;
    for poll_index in 0..2 {
        let started = Instant::now();
        match client.get_balances().await {
            Ok(snapshot) => {
                permission.credentials_valid = true;
                permission.read_permission = PermissionStatus::Pass;
                report.credential_status = "valid_for_balance_endpoint".to_string();
                report.balance_endpoint_supported = true;
                report.balance_poll_success_count += 1;
                report
                    .balance_latency_stats
                    .record(started.elapsed().as_millis() as i64);
                validate_balance_snapshot(&snapshot, report);
                let reconciliation = reconcile_balances(
                    client.exchange_name(),
                    MarketType::Spot,
                    &snapshot.balances,
                    &[],
                );
                if !reconciliation.clean {
                    report.warnings.push(format!(
                        "balance reconciliation max severity {:?}",
                        reconciliation.max_severity
                    ));
                }
                if let Some(previous) = &last_balances {
                    validate_missing_asset_rows_preserve_last_known(
                        client.exchange_name(),
                        previous,
                        &snapshot.balances,
                    )
                    .await;
                }
                last_balances = Some(snapshot.balances);
            }
            Err(error) => {
                report.balance_poll_failure_count += 1;
                if is_auth_error(&error) {
                    report.credential_status = "invalid_or_missing".to_string();
                    permission.credentials_valid = false;
                    permission.read_permission = PermissionStatus::Fail;
                    report
                        .critical_errors
                        .push("credentials failed read-only balance endpoint".to_string());
                    break;
                }
                if is_rate_limit_error(&error) {
                    report.rate_limit_events += 1;
                    report.rate_limit_profile.observed_rate_limit_events += 1;
                }
                report.warnings.push(format!(
                    "balance poll {poll_index} failed: {}",
                    sanitize_error(&error)
                ));
            }
        }
        sleep(request_spacing(config)).await;
    }
    permission.apply_withdraw_policy(config.require_no_withdraw_permission);
    report.permission_report = permission;

    match client.get_open_orders(None).await {
        Ok(open_orders) => {
            report.open_orders_supported = true;
            report.open_order_count = open_orders.len();
            validate_open_orders(&open_orders, report);
        }
        Err(error) => {
            if is_rate_limit_error(&error) {
                report.rate_limit_events += 1;
            }
            report.open_order_mapping_warnings.push(format!(
                "open orders unavailable: {}",
                sanitize_error(&error)
            ));
        }
    }
    sleep(request_spacing(config)).await;

    match client.get_recent_fills(&config.symbol).await {
        Ok(fills) => {
            report.recent_fills_supported = true;
            validate_fills(&fills, report);
        }
        Err(ExchangeClientError::Unsupported(error)) => {
            report
                .fill_mapping_warnings
                .push(format!("recent fills unsupported: {error}"));
        }
        Err(error) => {
            if is_rate_limit_error(&error) {
                report.rate_limit_events += 1;
            }
            report.fill_mapping_warnings.push(format!(
                "recent fills unavailable: {}",
                sanitize_error(&error)
            ));
        }
    }
}

pub async fn validate_public_websocket<C>(
    client: &ReadOnlyGuardClient<C>,
    config: &LiveReadonlyTestConfig,
    report: &mut LiveReadOnlyExchangeReport,
) -> Option<BookCache>
where
    C: ExchangeClient + Clone + Send + Sync + 'static,
{
    let cache = BookCache::default();
    let mut receiver = match client
        .subscribe_orderbook(vec![config.symbol.clone()])
        .await
    {
        Ok(receiver) => receiver,
        Err(error) => {
            report.warnings.push(format!(
                "public WebSocket unavailable: {}",
                sanitize_error(&error)
            ));
            return Some(cache);
        }
    };
    let started = Instant::now();
    let deadline = Duration::from_secs(config.duration_seconds.clamp(5, 30));
    while started.elapsed() < deadline && report.book_events_received < 3 {
        match timeout(Duration::from_secs(10), receiver.recv()).await {
            Ok(Some(snapshot)) => {
                validate_book_snapshot(&snapshot, report);
                cache
                    .update_from_snapshot(snapshot, BookSource::Websocket)
                    .await;
                report.public_websocket_connected = true;
                report.book_events_received += 1;
            }
            Ok(None) => {
                report.book_stale_count += 1;
                break;
            }
            Err(_) => {
                report.book_stale_count += 1;
                break;
            }
        }
    }
    if report.book_events_received == 0 {
        report
            .warnings
            .push("no public book events received during test window".to_string());
    }
    Some(cache)
}

pub async fn run_runtime_publisher_session<C1, C2>(
    gateio: Option<ReadOnlyGuardClient<C1>>,
    bitget: Option<ReadOnlyGuardClient<C2>>,
    config: &LiveReadonlyTestConfig,
) -> LiveReadOnlyExchangeReport
where
    C1: ExchangeClient + Clone + Send + Sync + 'static,
    C2: ExchangeClient + Clone + Send + Sync + 'static,
{
    let mut report = LiveReadOnlyExchangeReport::new("runtime_publisher", config);
    let mut clients: BTreeMap<String, Arc<dyn ExchangeClient>> = BTreeMap::new();
    let mut all_rules = Vec::new();
    let cache = BookCache::default();
    let health = BookHealth::default();
    let mut selected_exchanges = Vec::new();

    if let Some(client) = gateio {
        if let Ok(mut rules) = client.load_symbol_rules().await {
            rules.retain(|rule| rule.internal_symbol == config.symbol);
            all_rules.extend(rules);
        }
        start_runtime_book_feed(
            client.clone(),
            config.symbol.clone(),
            cache.clone(),
            health.clone(),
        );
        selected_exchanges.push("gateio".to_string());
        clients.insert("gateio".to_string(), Arc::new(client));
    }
    if let Some(client) = bitget {
        if let Ok(mut rules) = client.load_symbol_rules().await {
            rules.retain(|rule| rule.internal_symbol == config.symbol);
            all_rules.extend(rules);
        }
        start_runtime_book_feed(
            client.clone(),
            config.symbol.clone(),
            cache.clone(),
            health.clone(),
        );
        selected_exchanges.push("bitget".to_string());
        clients.insert("bitget".to_string(), Arc::new(client));
    }

    if clients.is_empty() {
        report
            .warnings
            .push("no credentialed clients available for runtime publisher session".to_string());
        return report;
    }
    sleep(Duration::from_secs(5)).await;
    let registry = UnifiedSymbolRegistry::from_rules(all_rules);
    let publisher_config = publisher_config(config);
    let store = snapshot_store_from_config(&publisher_config);
    let control = SpotControlService::new_in_memory(SpotSymbolControlConfig {
        enabled: true,
        runtime_publisher: publisher_config.clone(),
        ..SpotSymbolControlConfig::default()
    });
    let publisher = SpotControlRuntimePublisher::new(
        publisher_config,
        SpotControlRuntimePublisherDeps {
            exchange_clients: clients,
            symbol_registry: registry,
            book_cache: cache,
            book_health: health,
            fee_model: FeeModel::default(),
            disabled_registry: DisabledRegistry::new(),
            reservation_manager: BalanceReservationManager::default(),
            reconciliation_services: RuntimeReconciliationServices::default(),
            kill_switch: Some(KillSwitch::new(KillSwitchConfig::default())),
            live_preflight: None,
            small_live_gate: None,
            recorder_health: None,
            snapshot_config: SpotControlSnapshotConfig::default(),
            snapshot_store: store.clone(),
        },
    );

    let request = RuntimeSnapshotBuildRequest {
        symbol: config.symbol.clone(),
        selected_exchanges: selected_exchanges.clone(),
        directions: publisher_directions(&selected_exchanges),
        mode: EnableMode::LiveDryRun,
    };
    let mut latest_snapshot_id = None;
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(config.duration_seconds.max(10)) {
        publisher.poll_once().await;
        let results = publisher
            .publish_once_for_symbols(&control, std::slice::from_ref(&request))
            .await;
        for result in results {
            match result {
                Ok(snapshot) => {
                    latest_snapshot_id = Some(snapshot.snapshot_id.clone());
                    match snapshot
                        .consistency_report
                        .as_ref()
                        .map(|report| report.status)
                    {
                        Some(SnapshotConsistency::StrongEnoughForControl) => {
                            report.strong_enough_for_control_count += 1
                        }
                        Some(_) | None => report.observation_only_count += 1,
                    }
                    verify_snapshot_authorities(&snapshot, &mut report);
                    if let Some(store) = &store {
                        match store.get_snapshot(&snapshot.snapshot_id).await {
                            Ok(Some(_)) => {}
                            Ok(None) => report
                                .critical_errors
                                .push("persisted snapshot not found by id".to_string()),
                            Err(error) => report.critical_errors.push(error),
                        }
                        let replay = SpotControlSnapshotReplay::new(store.clone())
                            .replay(&snapshot.snapshot_id)
                            .await;
                        match replay {
                            Ok(Some(replay)) if replay.matching => {}
                            Ok(Some(replay)) => report.critical_errors.push(format!(
                                "snapshot replay mismatch: {:?}",
                                replay.differences
                            )),
                            Ok(None) => report
                                .critical_errors
                                .push("snapshot replay missing persisted snapshot".to_string()),
                            Err(error) => report.critical_errors.push(error),
                        }
                    }
                }
                Err(error) => report
                    .warnings
                    .push(format!("snapshot build failed: {error}")),
            }
        }
        sleep(Duration::from_secs(2)).await;
    }

    let health = publisher.health().await;
    report.snapshots_generated = health.snapshots_generated;
    report.snapshots_persisted = health.snapshots_persisted;
    report.rate_limit_events = health
        .per_exchange_health
        .values()
        .map(|item| item.rate_limit_events)
        .sum();
    report.backoff_events = health
        .per_exchange_health
        .values()
        .filter(|item| item.backoff_until.is_some())
        .count() as u64;
    report.rate_limit_profile.observed_rate_limit_events = report.rate_limit_events;

    let startup = publisher
        .startup_report(&control, std::slice::from_ref(&config.symbol))
        .await;
    if startup.write_actions_unblocked {
        report
            .critical_errors
            .push("startup report unexpectedly unblocked write actions".to_string());
    }
    if startup
        .blockers
        .iter()
        .all(|blocker| !blocker.contains("fresh runtime publisher snapshot required"))
    {
        report
            .warnings
            .push("startup report did not include fresh snapshot blocker".to_string());
    }
    if let Some(snapshot_id) = latest_snapshot_id {
        let state = MonitoringState::from_read_model(
            public_monitoring_config(),
            DashboardReadModel::default(),
        )
        .with_control_service(control)
        .with_runtime_publisher(publisher);
        report
            .warnings
            .extend(validate_dashboard_routes(state, &config.symbol, &snapshot_id).await);
    } else {
        report
            .critical_errors
            .push("runtime publisher did not generate a snapshot".to_string());
    }
    report
}

pub async fn validate_dashboard_routes(
    state: MonitoringState,
    symbol: &str,
    snapshot_id: &str,
) -> Vec<String> {
    let mut warnings = Vec::new();
    let routes = vec![
        "/api/control/runtime-publisher/status".to_string(),
        "/api/control/runtime-publisher/exchanges".to_string(),
        "/api/control/runtime-publisher/components".to_string(),
        "/api/control/runtime-publisher/errors".to_string(),
        format!("/api/control/symbols/{symbol}/runtime-snapshot"),
        format!("/api/control/symbols/{symbol}/data-health"),
        format!("/api/control/symbols/{symbol}/inventory-ownership"),
        format!("/api/control/symbols/{symbol}/open-order-ownership"),
        format!("/api/control/symbols/{symbol}/fill-ownership"),
        format!("/api/control/symbols/{symbol}/consistency"),
        format!("/api/control/symbols/{symbol}/snapshots"),
        format!("/api/control/snapshots/{snapshot_id}"),
        format!("/api/control/snapshots/{snapshot_id}/replay"),
    ];
    for route in routes {
        match router(state.clone())
            .oneshot(
                Request::builder()
                    .uri(route.clone())
                    .body(Body::empty())
                    .expect("dashboard route request"),
            )
            .await
        {
            Ok(response) if response.status() == StatusCode::OK => {}
            Ok(response) => warnings.push(format!("route {route} returned {}", response.status())),
            Err(error) => warnings.push(format!("route {route} failed: {error}")),
        }
    }
    warnings
}

pub fn write_report(report: &LiveReadOnlyExchangeReport, config: &LiveReadonlyTestConfig) {
    fs::create_dir_all(&config.output_dir).expect("create live readonly output dir");
    let path = config.output_dir.join(format!(
        "{}_{}.json",
        report.exchange,
        Utc::now().format("%Y%m%dT%H%M%SZ")
    ));
    let text = serde_json::to_string_pretty(report).expect("serialize live readonly report");
    assert_no_secret_leak(&text);
    fs::write(path, text).expect("write live readonly report");
}

pub fn assert_no_secret_leak(text: &str) {
    for env in [
        "GATEIO_API_KEY",
        "GATEIO_API_SECRET",
        "BITGET_API_KEY",
        "BITGET_API_SECRET",
        "BITGET_API_PASSPHRASE",
        "BITGET_PASSPHRASE",
        "MEXC_API_KEY",
        "MEXC_API_SECRET",
        "COINEX_API_KEY",
        "COINEX_API_SECRET",
        "KUCOIN_API_KEY",
        "KUCOIN_API_SECRET",
        "KUCOIN_API_PASSPHRASE",
    ] {
        if let Ok(secret) = std::env::var(env) {
            if !secret.trim().is_empty() {
                assert!(
                    !text.contains(&secret),
                    "sanitized report leaked value from {env}"
                );
            }
        }
    }
    for forbidden in ["api_key", "api_secret", "signature", "authorization"] {
        assert!(
            !text.to_ascii_lowercase().contains(forbidden),
            "sanitized report contains forbidden token {forbidden}"
        );
    }
}

pub fn has_gateio_credentials() -> bool {
    present("GATEIO_API_KEY") && present("GATEIO_API_SECRET")
}

pub fn has_bitget_credentials() -> bool {
    present("BITGET_API_KEY")
        && present("BITGET_API_SECRET")
        && (present("BITGET_API_PASSPHRASE") || present("BITGET_PASSPHRASE"))
}

pub fn has_mexc_credentials() -> bool {
    present("MEXC_API_KEY") && present("MEXC_API_SECRET")
}

pub fn has_coinex_credentials() -> bool {
    present("COINEX_API_KEY") && present("COINEX_API_SECRET")
}

pub fn has_kucoin_credentials() -> bool {
    present("KUCOIN_API_KEY") && present("KUCOIN_API_SECRET") && present("KUCOIN_API_PASSPHRASE")
}

fn present(env: &str) -> bool {
    std::env::var(env)
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
}

fn validate_balance_snapshot(snapshot: &BalanceSnapshot, report: &mut LiveReadOnlyExchangeReport) {
    for balance in &snapshot.balances {
        if balance.total < -1e-12 {
            report
                .critical_errors
                .push(format!("{} total is negative", balance.asset));
        }
        if balance.available < -1e-12 {
            report
                .critical_errors
                .push(format!("{} available is negative", balance.asset));
        }
        let exchange_locked = balance.locked_by_exchange.max(balance.locked);
        if exchange_locked < -1e-12 {
            report
                .critical_errors
                .push(format!("{} exchange locked is negative", balance.asset));
        }
        if balance.available + exchange_locked > balance.total + 1e-8 {
            report.warnings.push(format!(
                "{} available + exchange_locked materially exceeds total",
                balance.asset
            ));
        }
        if balance.locally_reserved > 0.0
            && (balance.locally_reserved - exchange_locked).abs() < 1e-12
        {
            report.warnings.push(format!(
                "{} local reservation equals exchange locked; verify no double count",
                balance.asset
            ));
        }
    }
}

async fn validate_missing_asset_rows_preserve_last_known(
    exchange: &str,
    previous: &[AssetBalance],
    current: &[AssetBalance],
) {
    let Some(first_previous) = previous.first() else {
        return;
    };
    let cache = SpotControlRuntimeCache::default();
    let reservations = BalanceReservationManager::default();
    cache
        .apply_balance_success(
            BalanceSnapshot {
                exchange: exchange.to_string(),
                market_type: MarketType::Spot,
                balances: previous.to_vec(),
                timestamp: Utc::now(),
            },
            &reservations,
            &[],
        )
        .await;
    let reduced = current
        .iter()
        .filter(|item| item.asset != first_previous.asset)
        .cloned()
        .collect::<Vec<_>>();
    cache
        .apply_balance_success(
            BalanceSnapshot {
                exchange: exchange.to_string(),
                market_type: MarketType::Spot,
                balances: reduced,
                timestamp: Utc::now(),
            },
            &reservations,
            &[],
        )
        .await;
    let state = cache.snapshot().await;
    let balances = state
        .balances_by_exchange
        .get(&exchange.trim().to_ascii_lowercase())
        .and_then(|component| component.value_optional.as_ref())
        .expect("cache balances");
    assert!(balances
        .iter()
        .any(|item| item.asset == first_previous.asset && item.total == first_previous.total));
}

fn validate_open_orders(orders: &[OrderResponse], report: &mut LiveReadOnlyExchangeReport) {
    for order in orders {
        if order.order_id.trim().is_empty() {
            report
                .open_order_mapping_warnings
                .push("open order has empty order_id".to_string());
        }
        if order.symbol.trim().is_empty() {
            report
                .open_order_mapping_warnings
                .push(format!("open order {} has empty symbol", order.order_id));
        }
        if order.quantity < 0.0 || order.filled_quantity < 0.0 {
            report.critical_errors.push(format!(
                "open order {} has negative quantity fields",
                order.order_id
            ));
        }
        if order.filled_quantity > order.quantity + 1e-12 {
            report.open_order_mapping_warnings.push(format!(
                "open order {} filled quantity exceeds original quantity",
                order.order_id
            ));
        }
        if order.status == OrderStatus::Unknown {
            report
                .open_order_mapping_warnings
                .push(format!("open order {} status unknown", order.order_id));
        }
        let ownership = classify_order_ownership(order.client_order_id.as_deref());
        if matches!(
            ownership,
            OrderOwnershipClass::Unknown | OrderOwnershipClass::ManualOperator
        ) {
            report.unknown_order_count += 1;
        }
        if order.side == OrderSide::Sell
            && matches!(
                ownership,
                OrderOwnershipClass::Unknown | OrderOwnershipClass::ManualOperator
            )
        {
            report.open_order_mapping_warnings.push(format!(
                "unknown/manual sell order {} must reduce sellable inventory",
                order.order_id
            ));
        }
    }
    if orders.is_empty() {
        report
            .open_order_mapping_warnings
            .push("no open orders observed; mapping could not be fully exercised".to_string());
    }
}

fn validate_fills(fills: &[TradeFill], report: &mut LiveReadOnlyExchangeReport) {
    let mut seen = HashSet::new();
    for fill in fills {
        let fill_id = fill
            .trade_id
            .clone()
            .or(fill.order_id.clone())
            .unwrap_or_default();
        if fill_id.trim().is_empty() {
            report
                .fill_mapping_warnings
                .push("fill missing both trade_id and order_id".to_string());
        } else if !seen.insert(format!("{}:{fill_id}", fill.exchange)) {
            report
                .fill_mapping_warnings
                .push(format!("duplicate fill id observed: {fill_id}"));
        }
        if fill.symbol.trim().is_empty() {
            report
                .fill_mapping_warnings
                .push("fill symbol empty".to_string());
        }
        if fill.quantity <= 0.0 || fill.price <= 0.0 {
            report
                .fill_mapping_warnings
                .push(format!("fill {fill_id} has non-positive price or quantity"));
        }
        if classify_order_ownership(fill.client_order_id.as_deref()) == OrderOwnershipClass::Unknown
        {
            report.fill_mapping_warnings.push(format!(
                "fill {fill_id} ownership is unknown; not strategy-owned"
            ));
        }
    }
    report.recent_fill_count = seen.len();
    if fills.is_empty() {
        report.fill_mapping_warnings.push(
            "no recent fills observed; no order was submitted to manufacture data".to_string(),
        );
    }
}

fn validate_book_snapshot(snapshot: &OrderBookSnapshot, report: &mut LiveReadOnlyExchangeReport) {
    if snapshot.best_bid.is_none() {
        report.warnings.push("book best_bid missing".to_string());
    }
    if snapshot.best_ask.is_none() {
        report.warnings.push("book best_ask missing".to_string());
    }
    if let (Some(bid), Some(ask)) = (snapshot.best_bid, snapshot.best_ask) {
        if bid >= ask {
            report
                .warnings
                .push(format!("best bid {bid} is not below best ask {ask}"));
        }
    }
    if snapshot.is_stale {
        report.book_stale_count += 1;
    }
    if snapshot.sequence.is_none() {
        report
            .warnings
            .push("book sequence unavailable; sequence validation is best-effort".to_string());
    }
}

fn verify_snapshot_authorities(
    snapshot: &rustcta::control::spot_control::SpotControlRuntimeSnapshot,
    report: &mut LiveReadOnlyExchangeReport,
) {
    for metadata in &snapshot.source_metadata {
        if metadata.component.starts_with("book:")
            && metadata.authority != RuntimeDataAuthority::ExchangeWebSocket
        {
            report.critical_errors.push(format!(
                "{} did not use WebSocket authority",
                metadata.component
            ));
        }
        if (metadata.component.starts_with("balances:")
            || metadata.component.starts_with("open_orders:")
            || metadata.component.starts_with("recent_fills:"))
            && !matches!(
                metadata.authority,
                RuntimeDataAuthority::ExchangeRest | RuntimeDataAuthority::Unsupported
            )
        {
            report.critical_errors.push(format!(
                "{} did not use REST/Unsupported authority",
                metadata.component
            ));
        }
    }
    if snapshot
        .consistency_report
        .as_ref()
        .is_some_and(|report| report.status != SnapshotConsistency::StrongEnoughForControl)
    {
        report.warnings.push(format!(
            "snapshot {} not StrongEnoughForControl; blockers are preserved",
            snapshot.snapshot_id
        ));
    }
}

fn start_runtime_book_feed<C>(
    client: ReadOnlyGuardClient<C>,
    symbol: String,
    cache: BookCache,
    health: BookHealth,
) where
    C: ExchangeClient + Clone + Send + Sync + 'static,
{
    tokio::spawn(async move {
        let Ok(mut receiver) = client.subscribe_orderbook(vec![symbol]).await else {
            return;
        };
        while let Some(snapshot) = receiver.recv().await {
            health
                .mark_message(
                    &snapshot.exchange,
                    snapshot.market_type,
                    &snapshot.symbol,
                    snapshot.latency_ms,
                    Utc::now(),
                )
                .await;
            cache
                .update_from_snapshot(snapshot, BookSource::Websocket)
                .await;
        }
    });
}

fn publisher_config(config: &LiveReadonlyTestConfig) -> SpotControlRuntimePublisherConfig {
    SpotControlRuntimePublisherConfig {
        enabled: true,
        polling: RuntimePublisherPollingConfig {
            balances_interval_ms: 5_000,
            open_orders_interval_ms: 5_000,
            recent_fills_interval_ms: 10_000,
            symbol_rules_interval_seconds: 3_600,
            fee_interval_seconds: 1_800,
            reconciliation_interval_ms: 5_000,
            snapshot_interval_ms: 1_000,
        },
        limits: RuntimePublisherLimitsConfig {
            maximum_concurrent_requests_per_exchange: 1,
            minimum_request_spacing_ms: request_spacing(config).as_millis() as u64,
            jitter_ms: 100,
            exponential_backoff_initial_ms: 1_000,
            exponential_backoff_max_ms: 30_000,
            pause_after_rate_limit_ms: 60_000,
        },
        snapshot_store: rustcta::control::spot_control::RuntimePublisherSnapshotStoreConfig {
            enabled: true,
            backend: "jsonl".to_string(),
            path: config
                .output_dir
                .join("runtime_publisher_snapshots.jsonl")
                .to_string_lossy()
                .to_string(),
            require_persistence_for_write_commands: true,
            maximum_retained_snapshots_per_symbol: 10_000,
        },
        ..SpotControlRuntimePublisherConfig::default()
    }
}

fn publisher_directions(exchanges: &[String]) -> Vec<EnabledDirection> {
    if exchanges.len() >= 2 {
        vec![EnabledDirection {
            buy_exchange: exchanges[0].clone(),
            sell_exchange: exchanges[1].clone(),
        }]
    } else {
        Vec::new()
    }
}

fn request_spacing(config: &LiveReadonlyTestConfig) -> Duration {
    let rpm = config.max_rest_requests_per_minute.max(1);
    Duration::from_millis((60_000 / rpm).max(500))
}

pub fn is_rate_limit_error(error: &ExchangeClientError) -> bool {
    matches!(
        error,
        ExchangeClientError::Classified(classified)
            if classified.class == ExchangeErrorClass::RateLimited
    ) || error.to_string().to_ascii_lowercase().contains("rate")
}

fn is_auth_error(error: &ExchangeClientError) -> bool {
    matches!(
        error,
        ExchangeClientError::Classified(classified)
            if classified.class == ExchangeErrorClass::AuthenticationFailed
                || classified.class == ExchangeErrorClass::PermissionDenied
    )
}

fn sanitize_error(error: &ExchangeClientError) -> String {
    let text = error.to_string();
    let mut sanitized = text;
    for env in [
        "GATEIO_API_KEY",
        "GATEIO_API_SECRET",
        "BITGET_API_KEY",
        "BITGET_API_SECRET",
        "BITGET_API_PASSPHRASE",
        "BITGET_PASSPHRASE",
        "MEXC_API_KEY",
        "MEXC_API_SECRET",
        "COINEX_API_KEY",
        "COINEX_API_SECRET",
        "KUCOIN_API_KEY",
        "KUCOIN_API_SECRET",
        "KUCOIN_API_PASSPHRASE",
    ] {
        if let Ok(secret) = std::env::var(env) {
            if !secret.trim().is_empty() {
                sanitized = sanitized.replace(&secret, "[redacted]");
            }
        }
    }
    sanitized
}

pub fn offline_test_report(exchange: &str) -> LiveReadOnlyExchangeReport {
    let config = LiveReadonlyTestConfig {
        symbol: "BTCUSDT".to_string(),
        market_type: MarketType::Spot,
        duration_seconds: 1,
        output_dir: PathBuf::from("target/live_readonly_tests"),
        require_no_withdraw_permission: true,
        max_rest_requests_per_minute: 30,
        safe_debug_balances: false,
    };
    LiveReadOnlyExchangeReport::new(exchange, &config)
}

pub fn test_snapshot_store(path: PathBuf) -> Arc<dyn SpotControlSnapshotStore> {
    Arc::new(JsonlSpotControlSnapshotStore::new(path))
}

pub fn public_monitoring_config() -> MonitoringConfig {
    MonitoringConfig {
        enabled: true,
        require_token: false,
        ..MonitoringConfig::default()
    }
}
