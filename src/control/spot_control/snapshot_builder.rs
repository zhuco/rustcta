use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::exchanges::symbol_registry::UnifiedSymbolRegistry;
use crate::exchanges::unified::{
    AssetBalance, MarketType, OrderBookSnapshot, OrderResponse, OrderSide, OrderStatus, OrderType,
    SymbolRule, SymbolStatus, TimeInForce,
};
use crate::execution::{
    BalanceMismatchSeverity, BalanceReconciliationReport, FeeLookupKey, FeeModel, FeeRole,
    OrderReconciliationResult,
};
use crate::live_preflight::{LivePreflightReport, LiveReadinessDecision, SmallLiveGateReport};
use crate::risk::{DisabledRegistry, KillSwitchState, UnmanagedPosition};
use crate::web::{BookView, DashboardReadModel, InventoryView, RecorderHealthView};

use super::{
    normalize_exchange, normalize_exchange_list, normalize_symbol, DirectionReadiness,
    DirectionReadinessStatus, DustPosition, EffectiveTradability, EnableMode, EnabledDirection,
    InventoryOwnership, LiquidationPreview, MarketLiquidationPlan, OpenOrderOwnership,
    PassiveLiquidationSession, PassiveLiquidationStatus, RuntimeBalanceView,
    RuntimeComponentFreshness, RuntimeDisabledState, RuntimeFeeView, RuntimeReservationView,
    RuntimeUnmanagedPosition, SnapshotComponentStatus, SpotControlRuntimeSnapshot,
};

fn default_max_snapshot_age_ms() -> u64 {
    2_000
}
fn default_max_book_age_ms() -> u64 {
    1_000
}
fn default_max_balance_age_ms() -> u64 {
    5_000
}
fn default_max_symbol_rule_age_seconds() -> u64 {
    86_400
}
fn default_max_fee_age_seconds() -> u64 {
    3_600
}
fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpotControlSnapshotConfig {
    #[serde(default = "default_max_snapshot_age_ms")]
    pub max_snapshot_age_ms: u64,
    #[serde(default = "default_max_book_age_ms")]
    pub max_book_age_ms: u64,
    #[serde(default = "default_max_balance_age_ms")]
    pub max_balance_age_ms: u64,
    #[serde(default = "default_max_symbol_rule_age_seconds")]
    pub max_symbol_rule_age_seconds: u64,
    #[serde(default = "default_max_fee_age_seconds")]
    pub max_fee_age_seconds: u64,
    #[serde(default = "default_true")]
    pub require_exchange_health: bool,
    #[serde(default = "default_true")]
    pub require_balance_reconciliation_clean: bool,
    #[serde(default = "default_true")]
    pub fail_on_unknown_order_ownership: bool,
    #[serde(default = "default_true")]
    pub fail_on_unknown_inventory_ownership: bool,
}

impl Default for SpotControlSnapshotConfig {
    fn default() -> Self {
        Self {
            max_snapshot_age_ms: default_max_snapshot_age_ms(),
            max_book_age_ms: default_max_book_age_ms(),
            max_balance_age_ms: default_max_balance_age_ms(),
            max_symbol_rule_age_seconds: default_max_symbol_rule_age_seconds(),
            max_fee_age_seconds: default_max_fee_age_seconds(),
            require_exchange_health: true,
            require_balance_reconciliation_clean: true,
            fail_on_unknown_order_ownership: true,
            fail_on_unknown_inventory_ownership: true,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SpotControlSnapshotInputs {
    pub symbol_registry: Option<UnifiedSymbolRegistry>,
    pub symbol_rules: Vec<SymbolRule>,
    pub books: Vec<OrderBookSnapshot>,
    pub exchange_health: Vec<SnapshotComponentStatus>,
    pub fee_model: Option<FeeModel>,
    pub balances: Vec<RuntimeBalanceView>,
    pub unmanaged_positions: Vec<UnmanagedPosition>,
    pub disabled_registry: Option<DisabledRegistry>,
    pub open_orders: Vec<OpenOrderOwnership>,
    pub order_reconciliation_state: Option<OrderReconciliationResult>,
    pub balance_reconciliation_state: Option<BalanceReconciliationReport>,
    pub kill_switch_state: Option<KillSwitchState>,
    pub live_preflight_state: Option<LivePreflightReport>,
    pub small_live_gate_state: Option<SmallLiveGateReport>,
    pub recorder_health: Option<RecorderHealthView>,
    pub operation_lock_allows: bool,
    pub other_strategy_inventory: Vec<OtherStrategyInventory>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtherStrategyInventory {
    pub exchange: String,
    pub symbol: String,
    pub asset: String,
    pub quantity: f64,
    pub owning_strategy: String,
}

pub struct SpotControlSnapshotBuilder {
    config: SpotControlSnapshotConfig,
    inputs: SpotControlSnapshotInputs,
}

impl SpotControlSnapshotBuilder {
    pub fn new(config: SpotControlSnapshotConfig, inputs: SpotControlSnapshotInputs) -> Self {
        Self { config, inputs }
    }

    pub fn config(&self) -> &SpotControlSnapshotConfig {
        &self.config
    }

    pub fn from_dashboard_model(
        config: SpotControlSnapshotConfig,
        model: &DashboardReadModel,
    ) -> Self {
        let balances = model
            .inventory
            .iter()
            .map(balance_from_inventory_view)
            .collect::<Vec<_>>();
        let books = model
            .books
            .iter()
            .filter_map(cached_book_from_view)
            .collect::<Vec<_>>();
        let unmanaged_positions = model
            .unmanaged_positions
            .iter()
            .map(|item| UnmanagedPosition {
                exchange: item.exchange.clone(),
                market_type: item.market_type,
                symbol: item.symbol.clone(),
                asset: item.asset.clone(),
                quantity: item.quantity,
                reason: item.reason.clone(),
                created_at: item.created_at,
            })
            .collect::<Vec<_>>();
        let balance_reconciliation_state = model.balance_reconciliation.clone().or_else(|| {
            Some(crate::execution::reconcile_dashboard_inventory(
                &model.inventory,
            ))
        });
        let exchange_health = model
            .exchanges
            .iter()
            .map(|exchange| {
                let updated_at = exchange
                    .last_message_at
                    .or(exchange.last_book_update_at)
                    .unwrap_or_else(Utc::now);
                if exchange.connected && exchange.public_ws_connected {
                    SnapshotComponentStatus::fresh(
                        format!("exchange_health:{}", exchange.exchange),
                        updated_at,
                        "monitoring_dashboard",
                    )
                } else {
                    SnapshotComponentStatus::error(
                        format!("exchange_health:{}", exchange.exchange),
                        "monitoring_dashboard",
                        "exchange health is not connected",
                    )
                }
            })
            .collect::<Vec<_>>();
        Self::new(
            config,
            SpotControlSnapshotInputs {
                symbol_rules: model.spot_symbol_rules.clone(),
                books,
                fee_model: Some(fee_model_from_dashboard(model)),
                exchange_health,
                balances,
                unmanaged_positions,
                balance_reconciliation_state,
                kill_switch_state: model.kill_switch.clone(),
                live_preflight_state: model.live_preflight.clone(),
                small_live_gate_state: model.small_live_gate.clone(),
                recorder_health: Some(model.recorder.clone()),
                operation_lock_allows: true,
                ..SpotControlSnapshotInputs::default()
            },
        )
    }

    pub fn build(
        &self,
        symbol: &str,
        selected_exchanges: &[String],
        directions: &[EnabledDirection],
        mode: EnableMode,
    ) -> SpotControlRuntimeSnapshot {
        let generated_at = Utc::now();
        let symbol = normalize_symbol(symbol);
        let selected_exchanges = normalize_exchange_list(selected_exchanges);
        let mut warnings = Vec::new();
        let mut critical_errors = Vec::new();
        let mut component_statuses = Vec::new();
        let mut symbol_rules = BTreeMap::new();
        let mut books = BTreeMap::new();
        let mut fees = BTreeMap::new();
        let mut disabled_state = Vec::new();

        for exchange in &selected_exchanges {
            let key = exchange_symbol_key(exchange, &symbol);
            match self.lookup_rule(exchange, &symbol) {
                Some(rule) => {
                    let status = if rule.status == SymbolStatus::Trading {
                        SnapshotComponentStatus::fresh(
                            format!("symbol_rule:{key}"),
                            generated_at,
                            "symbol_registry",
                        )
                    } else {
                        SnapshotComponentStatus::error(
                            format!("symbol_rule:{key}"),
                            "symbol_registry",
                            "symbol is not trading",
                        )
                    };
                    component_statuses.push(status.clone());
                    if status.is_critical_failure() {
                        critical_errors.push(format!("{}: {:?}", status.component, status.status));
                    }
                    symbol_rules.insert(exchange.clone(), rule);
                }
                None => {
                    let status = SnapshotComponentStatus::missing(
                        format!("symbol_rule:{key}"),
                        "symbol_registry",
                    );
                    critical_errors.push(format!("missing symbol rule for {key}"));
                    component_statuses.push(status);
                }
            }

            match self.lookup_book(exchange, &symbol) {
                Some(book) => {
                    let age_ms = generated_at
                        .signed_duration_since(book.received_at)
                        .num_milliseconds()
                        .max(0);
                    let status = if !book.is_stale && age_ms <= self.config.max_book_age_ms as i64 {
                        SnapshotComponentStatus::fresh(
                            format!("book:{key}"),
                            book.received_at,
                            "book_cache",
                        )
                    } else {
                        SnapshotComponentStatus::stale(
                            format!("book:{key}"),
                            book.received_at,
                            "book_cache",
                            format!("book age {age_ms}ms exceeds policy"),
                        )
                    };
                    if status.status == RuntimeComponentFreshness::Stale {
                        critical_errors.push(format!("stale book for {key}"));
                    }
                    component_statuses.push(status);
                    books.insert(exchange.clone(), book);
                }
                None => {
                    critical_errors.push(format!("missing book for {key}"));
                    component_statuses.push(SnapshotComponentStatus::missing(
                        format!("book:{key}"),
                        "book_cache",
                    ));
                }
            }

            let fee_lookup = self.inputs.fee_model.as_ref().map(|model| {
                model.lookup(&FeeLookupKey {
                    exchange: exchange.clone(),
                    market_type: MarketType::Spot,
                    symbol: Some(symbol.clone()),
                    liquidity_role: FeeRole::Taker,
                })
            });
            if let Some(lookup) = fee_lookup {
                let view = RuntimeFeeView::from_lookup(exchange.clone(), symbol.clone(), lookup);
                let age = generated_at
                    .signed_duration_since(view.updated_at)
                    .num_seconds()
                    .max(0);
                if view.fallback {
                    warnings.push(format!("fee fallback used for {key}"));
                }
                if age > self.config.max_fee_age_seconds as i64 {
                    critical_errors.push(format!("stale fee for {key}"));
                    component_statuses.push(SnapshotComponentStatus::stale(
                        format!("fee:{key}"),
                        view.updated_at,
                        "fee_model",
                        "fee data exceeds freshness policy",
                    ));
                } else {
                    component_statuses.push(SnapshotComponentStatus::fresh(
                        format!("fee:{key}"),
                        view.updated_at,
                        "fee_model",
                    ));
                }
                fees.insert(exchange.clone(), view);
            } else {
                critical_errors.push(format!("missing fee model for {key}"));
                component_statuses.push(SnapshotComponentStatus::missing(
                    format!("fee:{key}"),
                    "fee_model",
                ));
            }

            let disabled = self.inputs.disabled_registry.as_ref().and_then(|registry| {
                registry.check_symbol(exchange, MarketType::Spot, &symbol, generated_at)
            });
            if let Some(decision) = disabled {
                critical_errors.push(format!(
                    "DisabledRegistry blocks {key}: {}",
                    decision.reason
                ));
                disabled_state.push(RuntimeDisabledState {
                    exchange: exchange.clone(),
                    market_type: MarketType::Spot,
                    symbol: symbol.clone(),
                    blocked: true,
                    reason: Some(decision.reason),
                    source: "disabled_registry".to_string(),
                });
            } else {
                disabled_state.push(RuntimeDisabledState {
                    exchange: exchange.clone(),
                    market_type: MarketType::Spot,
                    symbol: symbol.clone(),
                    blocked: false,
                    reason: None,
                    source: "disabled_registry".to_string(),
                });
            }
        }

        component_statuses.extend(self.inputs.exchange_health.clone());
        if self.config.require_exchange_health
            && selected_exchanges.iter().any(|exchange| {
                !self.inputs.exchange_health.iter().any(|status| {
                    status.component == format!("exchange_health:{exchange}")
                        && status.status == RuntimeComponentFreshness::Fresh
                })
            })
        {
            critical_errors.push("required exchange health is missing or unhealthy".to_string());
        }

        let unmanaged_positions = self
            .inputs
            .unmanaged_positions
            .iter()
            .filter(|item| normalize_symbol(&item.symbol) == symbol)
            .map(|item| RuntimeUnmanagedPosition {
                exchange: normalize_exchange(&item.exchange),
                market_type: item.market_type,
                symbol: normalize_symbol(&item.symbol),
                asset: item.asset.to_ascii_uppercase(),
                quantity: item.quantity,
                reason: item.reason.clone(),
            })
            .collect::<Vec<_>>();
        let inventory_ownership = self.compute_inventory_ownership(
            &symbol,
            &selected_exchanges,
            &symbol_rules,
            &unmanaged_positions,
            &mut critical_errors,
        );
        let reservations = inventory_ownership
            .iter()
            .map(|item| RuntimeReservationView {
                exchange: item.exchange.clone(),
                asset: item.asset.clone(),
                locally_reserved: item.locally_reserved,
                source: "inventory_ownership".to_string(),
            })
            .collect::<Vec<_>>();
        let open_orders = self.inputs.open_orders.clone();
        if self.config.fail_on_unknown_order_ownership
            && open_orders.iter().any(|order| !order.ownership_known)
        {
            critical_errors.push("unknown open order ownership blocks control action".to_string());
        }
        if self.config.require_balance_reconciliation_clean {
            if let Some(report) = &self.inputs.balance_reconciliation_state {
                if !report.clean || report.max_severity >= BalanceMismatchSeverity::Error {
                    critical_errors.push(format!(
                        "balance reconciliation is not clean: {:?}",
                        report.max_severity
                    ));
                }
            } else {
                critical_errors.push("balance reconciliation state is missing".to_string());
            }
        }
        if let Some(kill) = &self.inputs.kill_switch_state {
            if kill.active {
                critical_errors.push("KillSwitch is active".to_string());
            }
            if matches!(mode, EnableMode::LiveDryRun) && !kill.allow_live_dry_run {
                critical_errors.push("KillSwitch blocks live_dry_run".to_string());
            }
            if matches!(mode, EnableMode::FutureSmallLive) && !kill.allow_live_orders {
                critical_errors.push("KillSwitch blocks live orders".to_string());
            }
        } else {
            critical_errors.push("KillSwitch state is missing".to_string());
        }
        if matches!(mode, EnableMode::LiveDryRun | EnableMode::FutureSmallLive) {
            if let Some(preflight) = &self.inputs.live_preflight_state {
                if preflight.decision == LiveReadinessDecision::Blocked {
                    critical_errors.push("LivePreflight blocks requested mode".to_string());
                }
            } else {
                critical_errors.push("LivePreflight state is missing".to_string());
            }
        }
        if matches!(mode, EnableMode::FutureSmallLive)
            && self.inputs.small_live_gate_state.is_none()
        {
            critical_errors.push("SmallLiveGate state is missing for FutureSmallLive".to_string());
        }
        if !self.inputs.operation_lock_allows {
            critical_errors.push("symbol operation lock conflict".to_string());
        }

        let direction_readiness = self.direction_readiness(
            directions,
            &symbol_rules,
            &books,
            &fees,
            &inventory_ownership,
            &disabled_state,
            mode,
        );
        let liquidation_preview = self.liquidation_preview(
            &symbol,
            &selected_exchanges,
            &symbol_rules,
            &books,
            &fees,
            &inventory_ownership,
            &mut warnings,
        );
        let effective_tradability = Some(EffectiveTradability::evaluate(
            direction_readiness
                .iter()
                .any(|direction| direction.readiness == DirectionReadinessStatus::Ready),
            !disabled_state.iter().any(|item| item.blocked),
            self.inputs
                .kill_switch_state
                .as_ref()
                .is_some_and(|state| !state.active),
            !inventory_ownership.iter().any(|item| !item.ownership_known),
            self.inputs.operation_lock_allows,
        ));

        SpotControlRuntimeSnapshot {
            generated_at,
            snapshot_id: Uuid::new_v4().to_string(),
            symbol,
            selected_exchanges,
            symbol_rules,
            books,
            book_health: component_statuses
                .iter()
                .filter(|item| item.component.starts_with("book:"))
                .cloned()
                .collect(),
            exchange_health: self.inputs.exchange_health.clone(),
            fees,
            balances: self.inputs.balances.clone(),
            reservations,
            inventory_ownership,
            unmanaged_positions,
            disabled_state,
            open_orders,
            fill_ownership: Vec::new(),
            order_reconciliation_state: self.inputs.order_reconciliation_state.clone(),
            balance_reconciliation_state: self.inputs.balance_reconciliation_state.clone(),
            kill_switch_state: self.inputs.kill_switch_state.clone(),
            live_preflight_state: self.inputs.live_preflight_state.clone(),
            small_live_gate_state: self.inputs.small_live_gate_state.clone(),
            recorder_health: self.inputs.recorder_health.clone(),
            direction_readiness,
            effective_tradability,
            liquidation_preview,
            data_sources: vec![
                "symbol_registry".to_string(),
                "book_cache".to_string(),
                "fee_model".to_string(),
                "balance_reconciliation".to_string(),
                "disabled_registry".to_string(),
                "kill_switch".to_string(),
                "live_preflight".to_string(),
            ],
            schema_version: 1,
            source_metadata: Vec::new(),
            consistency_report: None,
            component_statuses,
            warnings,
            critical_errors,
        }
    }

    fn lookup_rule(&self, exchange: &str, symbol: &str) -> Option<SymbolRule> {
        self.inputs
            .symbol_registry
            .as_ref()
            .and_then(|registry| {
                registry
                    .get_rule(exchange, MarketType::Spot, symbol)
                    .cloned()
            })
            .or_else(|| {
                self.inputs
                    .symbol_rules
                    .iter()
                    .find(|rule| {
                        normalize_exchange(&rule.exchange) == normalize_exchange(exchange)
                            && rule.market_type == MarketType::Spot
                            && normalize_symbol(&rule.internal_symbol) == normalize_symbol(symbol)
                    })
                    .cloned()
            })
    }

    fn lookup_book(&self, exchange: &str, symbol: &str) -> Option<OrderBookSnapshot> {
        self.inputs
            .books
            .iter()
            .find(|book| {
                normalize_exchange(&book.exchange) == normalize_exchange(exchange)
                    && book.market_type == MarketType::Spot
                    && normalize_symbol(&book.symbol) == normalize_symbol(symbol)
            })
            .cloned()
    }

    fn compute_inventory_ownership(
        &self,
        symbol: &str,
        exchanges: &[String],
        rules: &BTreeMap<String, SymbolRule>,
        unmanaged_positions: &[RuntimeUnmanagedPosition],
        critical_errors: &mut Vec<String>,
    ) -> Vec<InventoryOwnership> {
        let mut output = Vec::new();
        for exchange in exchanges {
            let Some(rule) = rules.get(exchange) else {
                continue;
            };
            for asset in [&rule.base_asset, &rule.quote_asset] {
                let balance = self.inputs.balances.iter().find(|balance| {
                    normalize_exchange(&balance.exchange) == *exchange
                        && balance.asset.eq_ignore_ascii_case(asset)
                });
                let unmanaged = unmanaged_positions
                    .iter()
                    .filter(|item| {
                        item.exchange == *exchange && item.asset.eq_ignore_ascii_case(asset)
                    })
                    .map(|item| item.quantity.max(0.0))
                    .sum::<f64>();
                let other_strategy = self
                    .inputs
                    .other_strategy_inventory
                    .iter()
                    .filter(|item| {
                        normalize_exchange(&item.exchange) == *exchange
                            && normalize_symbol(&item.symbol) == normalize_symbol(symbol)
                            && item.asset.eq_ignore_ascii_case(asset)
                    })
                    .map(|item| item.quantity.max(0.0))
                    .sum::<f64>();
                let unknown_sell_remaining = self
                    .inputs
                    .open_orders
                    .iter()
                    .filter(|order| {
                        order.exchange == *exchange
                            && normalize_symbol(&order.symbol) == normalize_symbol(symbol)
                            && order.side == OrderSide::Sell
                            && !order.ownership_known
                            && asset.eq_ignore_ascii_case(&rule.base_asset)
                    })
                    .map(|order| order.remaining_quantity.max(0.0))
                    .sum::<f64>();
                let ownership_known = balance.is_some() && unknown_sell_remaining <= 1e-12;
                if !ownership_known && self.config.fail_on_unknown_inventory_ownership {
                    critical_errors.push(format!(
                        "unknown inventory ownership for {exchange}:{asset}"
                    ));
                }
                let item = InventoryOwnership::compute(
                    exchange,
                    symbol,
                    asset,
                    balance.map(|item| item.total).unwrap_or_default(),
                    balance
                        .map(|item| item.locked_by_exchange)
                        .unwrap_or_default(),
                    balance
                        .map(|item| item.locally_reserved)
                        .unwrap_or_default()
                        + unknown_sell_remaining,
                    unmanaged,
                    other_strategy,
                    ownership_known,
                );
                output.push(item);
            }
        }
        output
    }

    fn direction_readiness(
        &self,
        directions: &[EnabledDirection],
        rules: &BTreeMap<String, SymbolRule>,
        books: &BTreeMap<String, OrderBookSnapshot>,
        fees: &BTreeMap<String, RuntimeFeeView>,
        inventory: &[InventoryOwnership],
        disabled_state: &[RuntimeDisabledState],
        mode: EnableMode,
    ) -> Vec<DirectionReadiness> {
        directions
            .iter()
            .map(|direction| {
                let buy_exchange = normalize_exchange(&direction.buy_exchange);
                let sell_exchange = normalize_exchange(&direction.sell_exchange);
                let buy_rule = rules.get(&buy_exchange);
                let sell_rule = rules.get(&sell_exchange);
                let quote_asset = buy_rule
                    .map(|rule| rule.quote_asset.clone())
                    .unwrap_or_else(|| "USDT".to_string());
                let base_asset = sell_rule
                    .map(|rule| rule.base_asset.clone())
                    .unwrap_or_else(|| "BASE".to_string());
                let buy_quote_available = inventory
                    .iter()
                    .find(|item| item.exchange == buy_exchange && item.asset == quote_asset)
                    .map(|item| item.effective_sellable_managed_quantity)
                    .unwrap_or_default();
                let sell_base_available = inventory
                    .iter()
                    .find(|item| item.exchange == sell_exchange && item.asset == base_asset)
                    .map(|item| item.effective_sellable_managed_quantity)
                    .unwrap_or_default();
                let buy_book_fresh =
                    fresh_book(books.get(&buy_exchange), self.config.max_book_age_ms);
                let sell_book_fresh =
                    fresh_book(books.get(&sell_exchange), self.config.max_book_age_ms);
                let buy_symbol_tradable =
                    buy_rule.is_some_and(|rule| rule.status == SymbolStatus::Trading);
                let sell_symbol_tradable =
                    sell_rule.is_some_and(|rule| rule.status == SymbolStatus::Trading);
                let buy_fee_known = fees
                    .get(&buy_exchange)
                    .is_some_and(|fee| !fee.fallback && fee.authoritative);
                let sell_fee_known = fees
                    .get(&sell_exchange)
                    .is_some_and(|fee| !fee.fallback && fee.authoritative);
                let disabled = disabled_state.iter().any(|item| {
                    item.blocked
                        && (item.exchange == buy_exchange || item.exchange == sell_exchange)
                });
                let mut reasons = Vec::new();
                let readiness = if disabled {
                    reasons.push("DisabledRegistry blocks this direction".to_string());
                    DirectionReadinessStatus::Disabled
                } else if !self.inputs.operation_lock_allows {
                    reasons.push("operation lock conflict".to_string());
                    DirectionReadinessStatus::Locked
                } else if !buy_symbol_tradable || !sell_symbol_tradable {
                    reasons.push("symbol rule missing or not tradable".to_string());
                    DirectionReadinessStatus::Blocked
                } else if matches!(mode, EnableMode::ObserveOnly) {
                    DirectionReadinessStatus::ObserveOnly
                } else if !buy_book_fresh || !sell_book_fresh {
                    reasons.push("book missing or stale".to_string());
                    DirectionReadinessStatus::BookStale
                } else if buy_quote_available <= 0.0 {
                    reasons.push("buy quote inventory missing".to_string());
                    DirectionReadinessStatus::BuyInventoryMissing
                } else if sell_base_available <= 0.0 {
                    reasons.push("sell base inventory missing".to_string());
                    DirectionReadinessStatus::SellInventoryMissing
                } else if !buy_fee_known || !sell_fee_known {
                    reasons.push("fee fallback or unknown fee source".to_string());
                    DirectionReadinessStatus::FeeFallbackWarning
                } else {
                    DirectionReadinessStatus::Ready
                };
                DirectionReadiness {
                    buy_exchange,
                    sell_exchange: sell_exchange.clone(),
                    buy_quote_available,
                    sell_base_available,
                    buy_book_fresh,
                    sell_book_fresh,
                    buy_symbol_tradable,
                    sell_symbol_tradable,
                    buy_fee_known,
                    sell_fee_known,
                    max_safe_notional: buy_quote_available
                        .min(book_bid_notional(books.get(&sell_exchange))),
                    readiness,
                    reasons,
                }
            })
            .collect()
    }

    fn liquidation_preview(
        &self,
        symbol: &str,
        exchanges: &[String],
        rules: &BTreeMap<String, SymbolRule>,
        books: &BTreeMap<String, OrderBookSnapshot>,
        fees: &BTreeMap<String, RuntimeFeeView>,
        inventory: &[InventoryOwnership],
        warnings: &mut Vec<String>,
    ) -> LiquidationPreview {
        let mut preview = LiquidationPreview::default();
        for exchange in exchanges {
            let Some(rule) = rules.get(exchange) else {
                preview.rejected = true;
                preview
                    .reasons
                    .push(format!("missing symbol rule for {exchange}"));
                continue;
            };
            let Some(base) = inventory
                .iter()
                .find(|item| item.exchange == *exchange && item.asset == rule.base_asset)
            else {
                preview.rejected = true;
                preview
                    .reasons
                    .push(format!("missing inventory ownership for {exchange}"));
                continue;
            };
            if !base.ownership_known {
                preview.rejected = true;
                preview
                    .reasons
                    .push(format!("unknown inventory ownership for {exchange}"));
                continue;
            }
            let book = books.get(exchange);
            if !fresh_book(book, self.config.max_book_age_ms) {
                preview.rejected = true;
                preview
                    .reasons
                    .push(format!("stale or missing book for {exchange}"));
                continue;
            }
            let book = book.expect("fresh_book checked Some");
            let rounded_quantity =
                round_down_step(base.effective_sellable_managed_quantity, rule.step_size);
            let best_bid = book.best_bid.unwrap_or_default();
            let executable_vwap = executable_vwap(&book.bids, rounded_quantity).unwrap_or_default();
            let fee_bps = fees
                .get(exchange)
                .map(|fee| fee.taker_fee_bps)
                .unwrap_or(20.0);
            let estimated_proceeds = rounded_quantity * executable_vwap;
            let estimated_fee = estimated_proceeds * fee_bps / 10_000.0;
            let estimated_slippage_bps = if best_bid > 0.0 && executable_vwap > 0.0 {
                ((best_bid - executable_vwap).max(0.0) / best_bid) * 10_000.0
            } else {
                0.0
            };
            let worst_allowed_price = best_bid * (1.0 - 30.0 / 10_000.0);
            let valid_notional = estimated_proceeds >= rule.min_notional
                && rounded_quantity >= rule.min_quantity
                && rounded_quantity > 0.0;
            if !valid_notional && base.effective_sellable_managed_quantity > 0.0 {
                preview.dust.push(DustPosition {
                    exchange: exchange.clone(),
                    symbol: symbol.to_string(),
                    asset: rule.base_asset.clone(),
                    quantity: base.effective_sellable_managed_quantity,
                    estimated_value_usdt: base.effective_sellable_managed_quantity * best_bid,
                    reason: "below min quantity or min notional after precision rounding"
                        .to_string(),
                    detected_at: Utc::now(),
                    managed_or_unmanaged: "managed".to_string(),
                });
            }
            preview.market_plans.push(MarketLiquidationPlan {
                command_id: "preview".to_string(),
                exchange: exchange.clone(),
                symbol: symbol.to_string(),
                managed_sellable_quantity: base.effective_sellable_managed_quantity,
                rounded_quantity,
                best_bid,
                executable_vwap,
                worst_allowed_price,
                estimated_fee,
                estimated_slippage_bps,
                estimated_proceeds,
                estimated_loss: 0.0,
                validation_status: if valid_notional {
                    "valid_dry_run_preview".to_string()
                } else {
                    "dust_or_invalid_order_size".to_string()
                },
                would_submit_order: false,
                rejection_reason_optional: (!valid_notional).then(|| {
                    "quantity is below exchange minimums or precision constraints".to_string()
                }),
            });
            if super::supports_post_only(rule) {
                let safe_price = round_down_tick(book.best_ask.unwrap_or(best_bid), rule.tick_size);
                preview.passive_sessions.push(PassiveLiquidationSession {
                    session_id: Uuid::new_v4().to_string(),
                    command_id: "preview".to_string(),
                    exchange: exchange.clone(),
                    symbol: symbol.to_string(),
                    initial_managed_quantity: rounded_quantity,
                    remaining_quantity: rounded_quantity,
                    filled_quantity: 0.0,
                    average_sell_price: safe_price,
                    fees_paid: 0.0,
                    realized_proceeds: 0.0,
                    active_order_id_optional: None,
                    started_at: Utc::now(),
                    last_reprice_at: None,
                    status: PassiveLiquidationStatus::Planning,
                    stop_reason_optional: Some("dry-run preview only".to_string()),
                });
            } else {
                warnings.push(format!("PostOnly unsupported for {exchange}:{symbol}"));
            }
        }
        preview
    }
}

pub fn validate_snapshot_freshness(
    snapshot: &SpotControlRuntimeSnapshot,
    config: &SpotControlSnapshotConfig,
) -> Vec<String> {
    let mut errors = Vec::new();
    if snapshot.age_ms() > config.max_snapshot_age_ms as i64 {
        errors.push(format!(
            "snapshot {} is stale: age={}ms",
            snapshot.snapshot_id,
            snapshot.age_ms()
        ));
    }
    for component in &snapshot.component_statuses {
        if component.is_critical_failure() {
            errors.push(format!(
                "snapshot component {} is {:?}",
                component.component, component.status
            ));
        }
    }
    errors.extend(snapshot.critical_errors.clone());
    errors.sort();
    errors.dedup();
    errors
}

pub fn enable_validation_from_runtime_snapshot(
    snapshot: &SpotControlRuntimeSnapshot,
) -> super::EnableValidationSnapshot {
    let mut output = super::EnableValidationSnapshot {
        kill_switch_allows_paper: snapshot
            .kill_switch_state
            .as_ref()
            .is_some_and(|state| state.allow_paper_trading && !state.active),
        kill_switch_allows_live_dry_run: snapshot
            .kill_switch_state
            .as_ref()
            .is_some_and(|state| state.allow_live_dry_run && !state.active),
        kill_switch_allows_live_orders: snapshot
            .kill_switch_state
            .as_ref()
            .is_some_and(|state| state.allow_live_orders && !state.active),
        live_preflight_allows_live_dry_run: snapshot
            .live_preflight_state
            .as_ref()
            .is_some_and(|state| state.decision != LiveReadinessDecision::Blocked),
        live_preflight_allows_live_orders: snapshot
            .live_preflight_state
            .as_ref()
            .is_some_and(|state| state.decision == LiveReadinessDecision::ReadyForSmallLive),
        small_live_gate_allows: snapshot.small_live_gate_state.is_some(),
        ..super::EnableValidationSnapshot::default()
    };
    for exchange in &snapshot.selected_exchanges {
        let key = super::EnableValidationSnapshot::key(exchange, &snapshot.symbol);
        output.adapter_exchanges.insert(exchange.clone());
        output.spot_supported_exchanges.insert(exchange.clone());
        if snapshot.symbol_rules.contains_key(exchange) {
            output.exchange_symbols.insert(key.clone());
            output.symbol_mappings.insert(key.clone());
            output.symbol_rules.insert(key.clone());
            if snapshot
                .symbol_rules
                .get(exchange)
                .is_some_and(|rule| rule.status == SymbolStatus::Trading)
            {
                output.tradable_symbols.insert(key.clone());
            }
        }
        if snapshot
            .component(&format!("book:{key}"))
            .is_some_and(|status| status.status == RuntimeComponentFreshness::Fresh)
        {
            output.fresh_books.insert(key.clone());
        }
        if snapshot.fees.contains_key(exchange) {
            output.fee_model_available.insert(key.clone());
        }
        if snapshot
            .balances
            .iter()
            .any(|balance| balance.exchange == *exchange)
        {
            output.balance_available.insert(key.clone());
        }
        if snapshot
            .disabled_state
            .iter()
            .any(|item| item.exchange == *exchange && item.blocked)
        {
            output.disabled_registry_blocks.insert(key.clone());
        }
        if snapshot
            .inventory_ownership
            .iter()
            .any(|item| item.exchange == *exchange && !item.ownership_known)
        {
            output.unmanaged_conflicts.insert(key);
        }
    }
    output.inventory = snapshot
        .inventory_ownership
        .iter()
        .filter_map(|item| {
            let rule = snapshot.symbol_rules.get(&item.exchange)?;
            if item.asset != rule.base_asset {
                return None;
            }
            let quote = snapshot.inventory_ownership.iter().find(|candidate| {
                candidate.exchange == item.exchange && candidate.asset == rule.quote_asset
            });
            Some(super::ExchangeSymbolInventory {
                exchange: item.exchange.clone(),
                symbol: item.symbol.clone(),
                base_asset: rule.base_asset.clone(),
                quote_asset: rule.quote_asset.clone(),
                base_total: item.total_balance,
                base_available: item.effective_sellable_managed_quantity,
                base_reserved: item.locally_reserved,
                base_unmanaged: item.unmanaged_quantity,
                quote_total: quote.map(|item| item.total_balance).unwrap_or_default(),
                quote_available: quote
                    .map(|item| item.effective_sellable_managed_quantity)
                    .unwrap_or_default(),
                quote_reserved: quote.map(|item| item.locally_reserved).unwrap_or_default(),
                target_base_inventory: 0.0,
                minimum_base_inventory: rule.min_quantity,
                maximum_base_inventory: rule.max_quantity.unwrap_or(f64::MAX),
                inventory_status: if !item.ownership_known {
                    super::InventoryReadiness::UnmanagedConflict
                } else if item.effective_sellable_managed_quantity >= rule.min_quantity {
                    super::InventoryReadiness::ReadySellOnly
                } else {
                    super::InventoryReadiness::InsufficientBase
                },
            })
        })
        .collect();
    output
}

pub fn cached_book_from_view(view: &BookView) -> Option<OrderBookSnapshot> {
    Some(OrderBookSnapshot {
        exchange: normalize_exchange(&view.exchange),
        market_type: view.market_type,
        symbol: normalize_symbol(&view.symbol),
        bids: view
            .best_bid
            .map(|price| crate::exchanges::unified::OrderBookLevel {
                price,
                quantity: 1.0,
            })
            .into_iter()
            .collect(),
        asks: view
            .best_ask
            .map(|price| crate::exchanges::unified::OrderBookLevel {
                price,
                quantity: 1.0,
            })
            .into_iter()
            .collect(),
        best_bid: view.best_bid,
        best_ask: view.best_ask,
        exchange_timestamp: None,
        received_at: Utc::now() - Duration::milliseconds(view.book_age_ms.max(0)),
        latency_ms: view.latency_ms,
        sequence: view.sequence,
        is_stale: view.is_stale,
    })
}

fn balance_from_inventory_view(view: &InventoryView) -> RuntimeBalanceView {
    RuntimeBalanceView {
        exchange: normalize_exchange(&view.exchange),
        market_type: view.market_type,
        asset: view.asset.to_ascii_uppercase(),
        total: view.total,
        available: view.available,
        locked_by_exchange: view.locked_by_exchange,
        locally_reserved: view.locally_reserved,
        effective_available: view.effective_available,
        source: "monitoring_dashboard".to_string(),
        updated_at: None,
    }
}

fn fee_model_from_dashboard(model: &DashboardReadModel) -> FeeModel {
    let mut config = crate::execution::FeeConfig::default();
    config.symbol_overrides = model
        .fees
        .iter()
        .filter_map(|fee| {
            let market_type = match fee.market_type {
                MarketType::Spot => "spot",
                MarketType::Perpetual => "perpetual",
            };
            Some(crate::execution::SymbolFeeOverride {
                exchange: fee.exchange.clone(),
                market_type: market_type.to_string(),
                symbol: fee.symbol.clone(),
                maker_bps: fee.maker_fee_bps,
                taker_bps: fee.taker_fee_bps,
                fee_asset: Some("quote".to_string()),
                reason: Some(format!("dashboard_fee_source:{:?}", fee.source)),
            })
        })
        .collect();
    FeeModel::from_config(config)
}

fn exchange_symbol_key(exchange: &str, symbol: &str) -> String {
    format!(
        "{}:{}",
        normalize_exchange(exchange),
        normalize_symbol(symbol)
    )
}

fn fresh_book(book: Option<&OrderBookSnapshot>, max_book_age_ms: u64) -> bool {
    let Some(book) = book else {
        return false;
    };
    !book.is_stale
        && Utc::now()
            .signed_duration_since(book.received_at)
            .num_milliseconds()
            .max(0)
            <= max_book_age_ms as i64
}

fn book_bid_notional(book: Option<&OrderBookSnapshot>) -> f64 {
    book.map(|book| {
        book.bids
            .iter()
            .map(|level| level.price * level.quantity)
            .sum()
    })
    .unwrap_or_default()
}

fn executable_vwap(
    levels: &[crate::exchanges::unified::OrderBookLevel],
    quantity: f64,
) -> Option<f64> {
    if quantity <= 0.0 {
        return None;
    }
    let mut remaining = quantity;
    let mut notional = 0.0;
    let mut filled = 0.0;
    for level in levels {
        if remaining <= 0.0 {
            break;
        }
        let take = remaining.min(level.quantity);
        notional += take * level.price;
        filled += take;
        remaining -= take;
    }
    (filled > 0.0).then_some(notional / filled)
}

fn round_down_step(quantity: f64, step: f64) -> f64 {
    if step <= 0.0 {
        return quantity.max(0.0);
    }
    (quantity / step).floor() * step
}

fn round_down_tick(price: f64, tick: f64) -> f64 {
    if tick <= 0.0 {
        return price.max(0.0);
    }
    (price / tick).floor() * tick
}
