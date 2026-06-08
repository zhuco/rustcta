use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rustcta_strategy_sdk::{
    AccountPermission, MarketDataChannel, MarketType, RequiredAccountPermission, RiskCapability,
    RiskCapabilityDeclaration, StrategyConfigSchema, StrategyContext, StrategyEvent,
    StrategyInstanceId, StrategyRuntime, StrategySnapshot, StrategySnapshotSchema, StrategySpec,
    StrategyStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub mod core;
pub mod live_plan;

pub use core::{
    build_startup_markdown, candidate_from_snapshot, candidate_orderable, planned_quantity,
    select_exchange_funding, ExchangeFundingSelection, FundingCandidate, FundingCoreConfig,
    FundingInstrument, FundingScanReport, FundingSnapshot, FundingSymbol,
};
pub use live_plan::{
    build_live_plan, build_live_plan_at, build_live_plan_markdown, build_live_result_markdown,
    funding_client_order_id, next_scan_time, plan_entry_from_selection, ActionAckSummary,
    CancelAckSummary, FillSummary, FundingLiveExchangePlan, FundingLiveExchangeResult,
    FundingLiveExchangeSkip, FundingLivePlan, FundingLiveStatus,
};

pub const STRATEGY_KIND: &str = "funding_arbitrage";
pub const DISPLAY_NAME: &str = "Funding Arbitrage";
pub const MIGRATED_FROM: &str = "src/strategies/funding_rate_arbitrage";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FundingArbitrageStrategyInfo {
    pub strategy_kind: String,
    pub migrated_from: String,
    pub initialized_at: DateTime<Utc>,
}

impl Default for FundingArbitrageStrategyInfo {
    fn default() -> Self {
        Self {
            strategy_kind: STRATEGY_KIND.to_string(),
            migrated_from: MIGRATED_FROM.to_string(),
            initialized_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundingArbitrageConfig {
    pub venues: Vec<String>,
    pub symbols: Vec<String>,
    pub min_funding_edge_bps: f64,
    pub max_position_notional_quote: String,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FundingArbitrageSnapshotPayload {
    pub migrated_from: String,
    pub handled_events: u64,
    pub started_at: Option<DateTime<Utc>>,
    pub last_event_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct FundingArbitrageRuntime {
    instance_id: StrategyInstanceId,
    strategy_id: String,
    run_id: String,
    status: StrategyStatus,
    started_at: Option<DateTime<Utc>>,
    last_event_at: Option<DateTime<Utc>>,
    handled_events: u64,
}

impl FundingArbitrageRuntime {
    pub fn new() -> Self {
        Self {
            instance_id: StrategyInstanceId::new("unstarted"),
            strategy_id: STRATEGY_KIND.to_string(),
            run_id: "unstarted".to_string(),
            status: StrategyStatus::Stopped,
            started_at: None,
            last_event_at: None,
            handled_events: 0,
        }
    }

    fn snapshot_payload(&self) -> FundingArbitrageSnapshotPayload {
        FundingArbitrageSnapshotPayload {
            migrated_from: MIGRATED_FROM.to_string(),
            handled_events: self.handled_events,
            started_at: self.started_at,
            last_event_at: self.last_event_at,
        }
    }
}

impl Default for FundingArbitrageRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StrategyRuntime for FundingArbitrageRuntime {
    fn spec(&self) -> StrategySpec {
        strategy_spec()
    }

    async fn start(&mut self, ctx: StrategyContext) -> anyhow::Result<()> {
        self.instance_id = ctx.instance_id().clone();
        self.strategy_id = ctx.strategy_id().to_string();
        self.run_id = ctx.run_id().to_string();
        self.started_at = Some(ctx.started_at());
        self.status = StrategyStatus::Running;
        Ok(())
    }

    async fn stop(&mut self) -> anyhow::Result<()> {
        self.status = StrategyStatus::Stopped;
        Ok(())
    }

    async fn handle_event(&mut self, event: StrategyEvent) -> anyhow::Result<()> {
        self.handled_events += 1;
        self.last_event_at = Some(event_timestamp(&event));
        if matches!(event, StrategyEvent::Stopping(_)) {
            self.status = StrategyStatus::Stopping;
        }
        Ok(())
    }

    async fn snapshot(&self) -> anyhow::Result<StrategySnapshot> {
        Ok(StrategySnapshot {
            schema_version: 1,
            instance_id: self.instance_id.clone(),
            strategy_kind: STRATEGY_KIND.to_string(),
            strategy_id: self.strategy_id.clone(),
            run_id: self.run_id.clone(),
            captured_at: Utc::now(),
            status: self.status.clone(),
            payload: serde_json::to_value(self.snapshot_payload())?,
            health: Vec::new(),
        })
    }
}

pub fn strategy_spec() -> StrategySpec {
    StrategySpec {
        schema_version: 1,
        strategy_kind: STRATEGY_KIND.to_string(),
        display_name: DISPLAY_NAME.to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        description: Some(
            "Partially migrated funding arbitrage strategy with adapter-free selection core."
                .to_string(),
        ),
        config_schema: config_schema(),
        snapshot_schema: snapshot_schema(),
        supported_commands: Vec::new(),
        risk_capabilities: vec![
            risk_capability(
                RiskCapability::PlaceOrders,
                "Places spot/perpetual funding arbitrage orders",
            ),
            risk_capability(RiskCapability::CancelOrders, "Cancels stale funding orders"),
            risk_capability(RiskCapability::Hedging, "Maintains hedged funding exposure"),
            risk_capability(
                RiskCapability::CrossAccountRead,
                "Reads balances, positions, and funding state across venues",
            ),
        ],
        market_data_subscriptions: Vec::new(),
        required_account_permissions: account_permissions(&[
            AccountPermission::ReadBalances,
            AccountPermission::ReadPositions,
            AccountPermission::ReadOrders,
            AccountPermission::TradeSpot,
            AccountPermission::TradePerpetual,
            AccountPermission::CancelOrders,
        ]),
        metadata: BTreeMap::from([
            ("legacy_module".to_string(), json!(MIGRATED_FROM)),
            ("partial_core_migration".to_string(), json!(true)),
            (
                "migrated_core_modules".to_string(),
                json!([
                    "core_config",
                    "scanner_selection",
                    "startup_markdown",
                    "live_plan",
                    "live_result_markdown",
                    "schedule_time"
                ]),
            ),
            (
                "remaining_legacy_modules".to_string(),
                json!([
                    "live_adapter_runtime",
                    "adapter_scanner",
                    "notification_send"
                ]),
            ),
            (
                "market_data_channels".to_string(),
                json!([MarketDataChannel::FundingRate]),
            ),
            (
                "primary_market_type".to_string(),
                json!(MarketType::Perpetual),
            ),
        ]),
    }
}

pub fn config_schema() -> StrategyConfigSchema {
    StrategyConfigSchema {
        schema_version: 1,
        json_schema: json!({
            "type": "object",
            "additionalProperties": false,
            "required": [
                "venues",
                "symbols",
                "min_funding_edge_bps",
                "max_position_notional_quote"
            ],
            "properties": {
                "venues": {
                    "type": "array",
                    "minItems": 1,
                    "items": { "type": "string", "minLength": 1 }
                },
                "symbols": {
                    "type": "array",
                    "minItems": 1,
                    "items": { "type": "string", "minLength": 1 }
                },
                "min_funding_edge_bps": { "type": "number", "minimum": 0.0 },
                "max_position_notional_quote": {
                    "type": "string",
                    "pattern": "^[0-9]+(\\.[0-9]+)?$"
                },
                "dry_run": { "type": "boolean", "default": true }
            }
        }),
    }
}

pub fn snapshot_schema() -> StrategySnapshotSchema {
    StrategySnapshotSchema {
        schema_version: 1,
        json_schema: common_snapshot_schema(),
    }
}

fn common_snapshot_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["migrated_from", "handled_events"],
        "properties": {
            "migrated_from": { "type": "string" },
            "handled_events": { "type": "integer", "minimum": 0 },
            "started_at": { "type": ["string", "null"], "format": "date-time" },
            "last_event_at": { "type": ["string", "null"], "format": "date-time" }
        }
    })
}

fn event_timestamp(event: &StrategyEvent) -> DateTime<Utc> {
    match event {
        StrategyEvent::Started(event) | StrategyEvent::Stopping(event) => event.occurred_at,
        StrategyEvent::Execution(event) => event.occurred_at,
        StrategyEvent::MarketData(event) => event.received_at,
        StrategyEvent::Account(event) => event.received_at,
        StrategyEvent::OperatorCommand(command) => command.requested_at,
        StrategyEvent::Timer(event) => event.fired_at,
    }
}

fn risk_capability(
    capability: RiskCapability,
    description: impl Into<String>,
) -> RiskCapabilityDeclaration {
    RiskCapabilityDeclaration {
        capability,
        description: Some(description.into()),
        limits: json!({ "configured_per_instance": true }),
    }
}

fn account_permissions(permissions: &[AccountPermission]) -> Vec<RequiredAccountPermission> {
    permissions
        .iter()
        .cloned()
        .map(|permission| RequiredAccountPermission {
            account_id: None,
            permission,
            reason: Some("Required by configured strategy runtime".to_string()),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustcta_strategy_sdk::{
        ExecutionCancelAck, ExecutionCancelCommand, ExecutionIntent, ExecutionIntentAck,
        ExecutionOrderAck, ExecutionOrderCommand, SdkResult, StrategyExecutionClient,
    };
    use std::sync::Arc;

    struct NoopExecutionClient;

    #[async_trait]
    impl StrategyExecutionClient for NoopExecutionClient {
        async fn submit_order(
            &self,
            command: ExecutionOrderCommand,
        ) -> SdkResult<ExecutionOrderAck> {
            Ok(ExecutionOrderAck {
                schema_version: command.schema_version,
                accepted: true,
                client_order_id: command.client_order_id,
                execution_order_id: None,
                reason: None,
                received_at: Utc::now(),
            })
        }

        async fn cancel_order(
            &self,
            command: ExecutionCancelCommand,
        ) -> SdkResult<ExecutionCancelAck> {
            Ok(ExecutionCancelAck {
                schema_version: command.schema_version,
                accepted: true,
                client_order_id: command.client_order_id,
                execution_order_id: command.execution_order_id,
                reason: None,
                received_at: Utc::now(),
            })
        }

        async fn submit_raw_intent(
            &self,
            intent: ExecutionIntent,
        ) -> SdkResult<ExecutionIntentAck> {
            Ok(ExecutionIntentAck {
                schema_version: intent.schema_version,
                accepted: true,
                intent_kind: intent.intent_kind,
                reason: None,
                received_at: Utc::now(),
                payload: Value::Null,
            })
        }
    }

    #[test]
    fn spec_should_expose_config_and_snapshot_schemas() {
        let spec = strategy_spec();
        assert_eq!(spec.strategy_kind, STRATEGY_KIND);
        assert_eq!(spec.config_schema.schema_version, 1);
        assert_eq!(spec.config_schema.json_schema["type"], json!("object"));
        assert!(spec.config_schema.json_schema["required"]
            .as_array()
            .is_some_and(|required| !required.is_empty()));
        assert_eq!(spec.snapshot_schema.schema_version, 1);
        assert_eq!(spec.snapshot_schema.json_schema["type"], json!("object"));
        assert!(spec.snapshot_schema.json_schema["properties"]["handled_events"].is_object());
        assert_secret_free(&serde_json::to_value(spec).expect("spec should serialize"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn snapshot_should_be_secret_free() {
        let mut runtime = FundingArbitrageRuntime::new();
        let execution: Arc<dyn StrategyExecutionClient> = Arc::new(NoopExecutionClient);
        let ctx = StrategyContext::new(
            StrategyInstanceId::new("instance-1"),
            "tenant-1",
            "account-1",
            "strategy-1",
            "run-1",
            json!({
                "api_key": "must-not-leak",
                "secret": "must-not-leak",
                "symbols": ["BTC/USDT"]
            }),
            execution,
        );

        runtime.start(ctx).await.expect("runtime should start");
        let snapshot = runtime.snapshot().await.expect("snapshot should build");

        assert_eq!(snapshot.strategy_kind, STRATEGY_KIND);
        assert_eq!(snapshot.status, StrategyStatus::Running);
        assert_secret_free(&snapshot.payload);
        assert_secret_free(&serde_json::to_value(snapshot).expect("snapshot should serialize"));
    }

    #[test]
    fn manifest_should_not_depend_on_exchange_adapters() {
        let manifest = include_str!("../Cargo.toml");
        assert!(manifest.contains("rustcta-strategy-sdk.workspace = true"));
        for forbidden in [
            "rustcta-exchange-api",
            "rustcta-exchange-gateway",
            "src/exchanges",
            "gateio",
            "kucoin",
            "okx",
            "binance",
            "bitget",
            "mexc",
        ] {
            assert!(
                !manifest.contains(forbidden),
                "{forbidden} should not appear in manifest"
            );
        }
    }

    #[test]
    fn migrated_core_should_validate_observe_and_live_modes() {
        let mut observe = FundingCoreConfig::default();
        observe.execution.notional_usdt = 50.0;
        assert!(
            observe.validate().is_ok(),
            "observe mode should not validate live execution sizing"
        );

        let mut live = FundingCoreConfig {
            mode: "live".to_string(),
            ..FundingCoreConfig::default()
        };
        assert!(live.validate().is_err());
        live.selection.max_seconds_to_settlement_at_scan = Some(120);
        assert!(live.validate().is_ok());
        live.execution.notional_usdt = 20.01;
        assert!(live.validate().is_err());
    }

    #[test]
    fn migrated_core_should_select_threshold_equal_candidate() {
        let mut config = FundingCoreConfig::default();
        config.selection.max_seconds_to_settlement_at_scan = Some(600);
        let now = Utc::now();
        let selection = select_exchange_funding(
            "bitget",
            [FundingSnapshot {
                exchange: "bitget".to_string(),
                canonical_symbol: FundingSymbol::new("btc", "usdt"),
                exchange_symbol: Some("BTCUSDT".to_string()),
                funding_rate: config.selection.min_funding_rate,
                predicted_funding_rate: None,
                mark_price: Some(100.0),
                index_price: None,
                next_funding_time: Some(now + chrono::Duration::seconds(600)),
                recv_ts: now,
            }],
            &[funding_instrument_for_test()],
            &config,
            now,
        );

        let selected = selection.selected.expect("threshold equal should qualify");
        assert!(selected.qualifies);
        assert_eq!(selected.funding_rate_pct, -0.5);
        assert_eq!(selected.seconds_to_settlement, Some(600));
    }

    #[test]
    fn migrated_core_should_apply_snapshot_age_and_symbol_filters() {
        let mut config = FundingCoreConfig::default();
        config.universe.symbol_blocklist = vec!["BTC/USDT".to_string()];
        let now = Utc::now();
        assert!(!core::symbol_allowed(
            &FundingSymbol::new("btc", "usdt"),
            &config
        ));
        assert!(core::symbol_allowed(
            &FundingSymbol::new("eth", "usdt"),
            &config
        ));
        assert!(!core::symbol_allowed(
            &FundingSymbol::new("草根文化", "usdt"),
            &config
        ));

        config.universe.symbol_blocklist.clear();
        let fresh_at_limit = FundingSnapshot {
            exchange: "gate".to_string(),
            canonical_symbol: FundingSymbol::new("eth", "usdt"),
            exchange_symbol: Some("ETH_USDT".to_string()),
            funding_rate: -0.01,
            predicted_funding_rate: None,
            mark_price: Some(100.0),
            index_price: None,
            next_funding_time: Some(now + chrono::Duration::minutes(5)),
            recv_ts: now
                - chrono::Duration::milliseconds(config.selection.max_funding_snapshot_age_ms),
        };
        assert!(candidate_from_snapshot(fresh_at_limit, &config, now).is_some());

        let stale = FundingSnapshot {
            recv_ts: now
                - chrono::Duration::milliseconds(config.selection.max_funding_snapshot_age_ms + 1),
            exchange_symbol: Some("ETH_USDT".to_string()),
            ..funding_snapshot_for_test(now)
        };
        assert!(candidate_from_snapshot(stale, &config, now).is_none());
    }

    #[test]
    fn migrated_core_should_preserve_orderable_wide_defaults() {
        let config = FundingCoreConfig::default();
        let now = Utc::now();
        let mut candidate =
            candidate_from_snapshot(funding_snapshot_for_test(now), &config, now).unwrap();

        assert!(candidate_orderable(&candidate, &[], &config));
        candidate.mark_price = None;
        candidate.index_price = None;
        assert!(candidate_orderable(
            &candidate,
            &[funding_instrument_for_test()],
            &config
        ));

        candidate.mark_price = Some(10.0);
        let mut restrictive = funding_instrument_for_test();
        restrictive.min_qty = 100.0;
        assert!(!candidate_orderable(&candidate, &[restrictive], &config));
    }

    #[test]
    fn migrated_core_should_render_startup_markdown() {
        let now = Utc::now();
        let report = FundingScanReport {
            generated_at: now,
            threshold: -0.005,
            threshold_pct: -0.5,
            selections: vec![
                ExchangeFundingSelection {
                    exchange: "bitget".to_string(),
                    selected: Some(
                        candidate_from_snapshot(
                            funding_snapshot_for_test(now),
                            &FundingCoreConfig::default(),
                            now,
                        )
                        .expect("candidate"),
                    ),
                    scanned_symbols: 1,
                    funding_snapshots: 1,
                    eligible_candidates: 1,
                    skipped_reason: None,
                },
                ExchangeFundingSelection {
                    exchange: "gate".to_string(),
                    selected: None,
                    scanned_symbols: 2,
                    funding_snapshots: 0,
                    eligible_candidates: 0,
                    skipped_reason: Some("no funding snapshots loaded".to_string()),
                },
            ],
            errors: vec![core::ExchangeScanError {
                exchange: "binance".to_string(),
                stage: "load_funding",
                message: "timeout".to_string(),
            }],
        };

        let markdown = build_startup_markdown(&report);

        assert!(markdown.contains("no orders"));
        assert!(markdown.contains("BTC/USDT"));
        assert!(markdown.contains("-0.6000%"));
        assert!(markdown.contains("no funding snapshots loaded"));
        assert!(markdown.contains("timeout"));
        assert!(markdown.contains("does not submit any orders"));
    }

    fn assert_secret_free(value: &Value) {
        let encoded = value.to_string().to_ascii_lowercase();
        for forbidden in [
            "api_key",
            "secret",
            "passphrase",
            "password",
            "token",
            "credential",
        ] {
            assert!(
                !encoded.contains(forbidden),
                "{forbidden} should not appear in serialized runtime output"
            );
        }
    }

    fn funding_snapshot_for_test(now: chrono::DateTime<Utc>) -> FundingSnapshot {
        FundingSnapshot {
            exchange: "bitget".to_string(),
            canonical_symbol: FundingSymbol::new("btc", "usdt"),
            exchange_symbol: Some("BTCUSDT".to_string()),
            funding_rate: -0.006,
            predicted_funding_rate: None,
            mark_price: Some(100.0),
            index_price: None,
            next_funding_time: Some(now + chrono::Duration::minutes(5)),
            recv_ts: now,
        }
    }

    fn funding_instrument_for_test() -> FundingInstrument {
        FundingInstrument {
            exchange: "bitget".to_string(),
            canonical_symbol: FundingSymbol::new("btc", "usdt"),
            exchange_symbol: "BTCUSDT".to_string(),
            contract_type: "linear_perpetual".to_string(),
            status: "trading".to_string(),
            contract_size: 1.0,
            quantity_step: 0.001,
            min_qty: 0.001,
            min_notional: 5.0,
        }
    }
}
