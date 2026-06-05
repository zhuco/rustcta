use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use chrono::Utc;
use tokio::sync::RwLock;

use crate::control::spot_control::lifecycle::{normalize_exchange, normalize_symbol};
use crate::exchanges::spot_reservation::BalanceReservationManager;
use crate::exchanges::unified::{
    AssetBalance, BalanceSnapshot, MarketType, OrderResponse, OrderSide, TradeFill,
};
use crate::execution::{reconcile_balances, BalanceReconciliationReport};
use crate::risk::UnmanagedPosition;

use super::{
    fill_dedupe_key, FillOwnershipRecord, OrderOwnershipClass, RuntimeBalanceView,
    RuntimeComponentFreshness, RuntimeComponentValue, RuntimeDataAuthority, RuntimeReservationView,
    SpotControlSnapshotInputs,
};

#[derive(Debug, Clone, Default)]
pub struct SpotControlRuntimeCache {
    inner: Arc<RwLock<RuntimeCacheState>>,
}

#[derive(Debug, Clone, Default)]
pub struct RuntimeCacheState {
    pub balances_by_exchange: BTreeMap<String, RuntimeComponentValue<Vec<RuntimeBalanceView>>>,
    pub open_orders_by_exchange:
        BTreeMap<String, RuntimeComponentValue<Vec<super::OpenOrderOwnership>>>,
    pub fills_by_exchange: BTreeMap<String, RuntimeComponentValue<Vec<FillOwnershipRecord>>>,
    pub balance_reconciliation_by_exchange:
        BTreeMap<String, RuntimeComponentValue<BalanceReconciliationReport>>,
    pub last_errors: Vec<String>,
}

impl SpotControlRuntimeCache {
    pub async fn snapshot(&self) -> RuntimeCacheState {
        self.inner.read().await.clone()
    }

    pub async fn apply_balance_success(
        &self,
        snapshot: BalanceSnapshot,
        reservation_manager: &BalanceReservationManager,
        unmanaged_positions: &[UnmanagedPosition],
    ) -> Vec<String> {
        let exchange = normalize_exchange(&snapshot.exchange);
        let _ = reservation_manager.update_balances(&exchange, &snapshot.balances);
        let mut critical_errors = Vec::new();
        let mut normalized = snapshot
            .balances
            .iter()
            .map(|balance| {
                normalize_balance(
                    &exchange,
                    snapshot.market_type,
                    balance,
                    reservation_manager,
                )
            })
            .inspect(|view| detect_impossible_balance(view, &mut critical_errors))
            .collect::<Vec<_>>();
        let mut guard = self.inner.write().await;
        if let Some(previous) = guard
            .balances_by_exchange
            .get(&exchange)
            .and_then(|component| component.value_optional.as_ref())
        {
            for previous_balance in previous {
                if !normalized
                    .iter()
                    .any(|balance| balance.asset == previous_balance.asset)
                {
                    let mut stale = previous_balance.clone();
                    stale.source = "last_valid_missing_from_exchange_response".to_string();
                    normalized.push(stale);
                }
            }
        }
        let reconciled_balances = normalized
            .iter()
            .map(|view| {
                AssetBalance::new(
                    view.asset.clone(),
                    view.total,
                    view.available,
                    view.locked_by_exchange,
                )
                .with_reservation(view.locally_reserved)
            })
            .collect::<Vec<_>>();
        let report = reconcile_balances(
            &exchange,
            snapshot.market_type,
            &reconciled_balances,
            unmanaged_positions,
        );
        guard.balances_by_exchange.insert(
            exchange.clone(),
            RuntimeComponentValue::fresh(
                normalized,
                RuntimeDataAuthority::ExchangeRest,
                Utc::now(),
                Some(snapshot.timestamp),
            ),
        );
        guard.balance_reconciliation_by_exchange.insert(
            exchange.clone(),
            RuntimeComponentValue::fresh(
                report,
                RuntimeDataAuthority::ReconciliationService,
                Utc::now(),
                None,
            ),
        );
        critical_errors
    }

    pub async fn apply_balance_failure(&self, exchange: &str, error: impl Into<String>) {
        let exchange = normalize_exchange(exchange);
        let error = error.into();
        let mut guard = self.inner.write().await;
        if let Some(previous) = guard.balances_by_exchange.remove(&exchange) {
            guard.balances_by_exchange.insert(
                exchange.clone(),
                previous.stale_from_last_valid(error.clone()),
            );
        } else {
            guard.balances_by_exchange.insert(
                exchange.clone(),
                RuntimeComponentValue::missing(RuntimeDataAuthority::ExchangeRest, error.clone()),
            );
        }
        guard
            .last_errors
            .push(format!("{exchange}: balances: {error}"));
    }

    pub async fn apply_open_orders_success(
        &self,
        exchange: &str,
        orders: Vec<OrderResponse>,
    ) -> Vec<String> {
        let exchange = normalize_exchange(exchange);
        let mut warnings = Vec::new();
        let ownership = orders
            .iter()
            .map(|order| {
                let class = classify_order_ownership(order.client_order_id.as_deref());
                if class == OrderOwnershipClass::Unknown {
                    warnings.push(format!(
                        "unknown open order ownership {}:{}",
                        exchange, order.order_id
                    ));
                }
                let mut view = super::OpenOrderOwnership::from_order(
                    order,
                    format!("{class:?}"),
                    !matches!(
                        class,
                        OrderOwnershipClass::Unknown | OrderOwnershipClass::ManualOperator
                    ),
                );
                view.ownership_class = class;
                if matches!(class, OrderOwnershipClass::ManualOperator) {
                    view.ownership_known = false;
                }
                view
            })
            .collect::<Vec<_>>();
        let mut guard = self.inner.write().await;
        guard.open_orders_by_exchange.insert(
            exchange,
            RuntimeComponentValue::fresh(
                ownership,
                RuntimeDataAuthority::ExchangeRest,
                Utc::now(),
                None,
            ),
        );
        warnings
    }

    pub async fn apply_open_orders_failure(&self, exchange: &str, error: impl Into<String>) {
        let exchange = normalize_exchange(exchange);
        let error = error.into();
        let mut guard = self.inner.write().await;
        if let Some(previous) = guard.open_orders_by_exchange.remove(&exchange) {
            guard.open_orders_by_exchange.insert(
                exchange.clone(),
                previous.stale_from_last_valid(error.clone()),
            );
        } else {
            guard.open_orders_by_exchange.insert(
                exchange.clone(),
                RuntimeComponentValue::missing(RuntimeDataAuthority::ExchangeRest, error.clone()),
            );
        }
        guard
            .last_errors
            .push(format!("{exchange}: open_orders: {error}"));
    }

    pub async fn apply_fills_success(&self, exchange: &str, fills: Vec<TradeFill>) {
        let exchange = normalize_exchange(exchange);
        let mut seen = BTreeSet::new();
        let records = fills
            .into_iter()
            .filter_map(|fill| {
                let fill_id = fill.trade_id.clone().or(fill.order_id.clone())?;
                let key = fill_dedupe_key(&exchange, &fill_id);
                if !seen.insert(key) {
                    return None;
                }
                Some(FillOwnershipRecord {
                    exchange: exchange.clone(),
                    symbol: normalize_symbol(&fill.symbol),
                    fill_id,
                    order_id: fill.order_id,
                    client_order_id_optional: fill.client_order_id.clone(),
                    ownership_class: classify_order_ownership(fill.client_order_id.as_deref()),
                    side: fill.side,
                    quantity: fill.quantity,
                    price: fill.price,
                    fee: fill.fee_amount,
                    fee_asset: fill.fee_asset,
                    liquidity_role: fill.liquidity,
                    timestamp: fill.timestamp,
                })
            })
            .collect::<Vec<_>>();
        self.inner.write().await.fills_by_exchange.insert(
            exchange,
            RuntimeComponentValue::fresh(
                records,
                RuntimeDataAuthority::ExchangeRest,
                Utc::now(),
                None,
            ),
        );
    }

    pub async fn apply_fills_unsupported(&self, exchange: &str, error: impl Into<String>) {
        self.inner.write().await.fills_by_exchange.insert(
            normalize_exchange(exchange),
            RuntimeComponentValue::unsupported(error.into()),
        );
    }

    pub async fn apply_fills_failure(&self, exchange: &str, error: impl Into<String>) {
        let exchange = normalize_exchange(exchange);
        let error = error.into();
        let mut guard = self.inner.write().await;
        if let Some(previous) = guard.fills_by_exchange.remove(&exchange) {
            guard.fills_by_exchange.insert(
                exchange.clone(),
                previous.stale_from_last_valid(error.clone()),
            );
        } else {
            guard.fills_by_exchange.insert(
                exchange.clone(),
                RuntimeComponentValue::missing(RuntimeDataAuthority::ExchangeRest, error.clone()),
            );
        }
        guard
            .last_errors
            .push(format!("{exchange}: fills: {error}"));
    }

    pub async fn inputs_for_snapshot(&self, exchanges: &[String]) -> SpotControlSnapshotInputs {
        let guard = self.inner.read().await;
        let exchanges = exchanges
            .iter()
            .map(|item| normalize_exchange(item))
            .collect::<BTreeSet<_>>();
        let balances = guard
            .balances_by_exchange
            .iter()
            .filter(|(exchange, _)| exchanges.contains(*exchange))
            .filter_map(|(_, component)| component.value_optional.clone())
            .flatten()
            .collect::<Vec<_>>();
        let open_orders = guard
            .open_orders_by_exchange
            .iter()
            .filter(|(exchange, _)| exchanges.contains(*exchange))
            .filter_map(|(_, component)| component.value_optional.clone())
            .flatten()
            .collect::<Vec<_>>();
        let balance_reconciliation_state = merge_balance_reports(
            guard
                .balance_reconciliation_by_exchange
                .iter()
                .filter(|(exchange, _)| exchanges.contains(*exchange))
                .filter_map(|(_, component)| component.value_optional.clone())
                .collect(),
        );
        SpotControlSnapshotInputs {
            balances,
            open_orders,
            balance_reconciliation_state,
            ..SpotControlSnapshotInputs::default()
        }
    }

    pub async fn source_metadata(
        &self,
        exchanges: &[String],
    ) -> Vec<super::RuntimeComponentMetadata> {
        let guard = self.inner.read().await;
        let exchanges = exchanges
            .iter()
            .map(|item| normalize_exchange(item))
            .collect::<BTreeSet<_>>();
        let mut metadata = Vec::new();
        for (exchange, component) in &guard.balances_by_exchange {
            if exchanges.contains(exchange) {
                metadata.push(component.metadata(format!("balances:{exchange}")));
            }
        }
        for (exchange, component) in &guard.open_orders_by_exchange {
            if exchanges.contains(exchange) {
                metadata.push(component.metadata(format!("open_orders:{exchange}")));
            }
        }
        for (exchange, component) in &guard.fills_by_exchange {
            if exchanges.contains(exchange) {
                metadata.push(component.metadata(format!("recent_fills:{exchange}")));
            }
        }
        metadata
    }

    pub async fn fill_records_for_exchanges(
        &self,
        exchanges: &[String],
    ) -> Vec<FillOwnershipRecord> {
        let guard = self.inner.read().await;
        let exchanges = exchanges
            .iter()
            .map(|item| normalize_exchange(item))
            .collect::<BTreeSet<_>>();
        guard
            .fills_by_exchange
            .iter()
            .filter(|(exchange, _)| exchanges.contains(*exchange))
            .filter_map(|(_, component)| component.value_optional.clone())
            .flatten()
            .collect()
    }
}

fn normalize_balance(
    exchange: &str,
    market_type: MarketType,
    balance: &AssetBalance,
    reservation_manager: &BalanceReservationManager,
) -> RuntimeBalanceView {
    let locally_reserved = reservation_manager.locally_reserved(exchange, &balance.asset);
    RuntimeBalanceView {
        exchange: normalize_exchange(exchange),
        market_type,
        asset: balance.asset.to_ascii_uppercase(),
        total: balance.total,
        available: balance.available,
        locked_by_exchange: balance.locked_by_exchange.max(balance.locked),
        locally_reserved,
        effective_available: (balance.available - locally_reserved).max(0.0),
        source: "exchange_rest_balances+local_reservations".to_string(),
        updated_at: Some(Utc::now()),
    }
}

fn detect_impossible_balance(view: &RuntimeBalanceView, critical_errors: &mut Vec<String>) {
    if view.available < -1e-12 {
        critical_errors.push(format!(
            "impossible balance {}:{} available < 0",
            view.exchange, view.asset
        ));
    }
    if view.locally_reserved > view.total + 1e-12 {
        critical_errors.push(format!(
            "impossible balance {}:{} reserved > total",
            view.exchange, view.asset
        ));
    }
    if view.effective_available > view.available + 1e-12 {
        critical_errors.push(format!(
            "impossible balance {}:{} effective available exceeds available",
            view.exchange, view.asset
        ));
    }
}

pub fn classify_order_ownership(client_order_id: Option<&str>) -> OrderOwnershipClass {
    let Some(client_order_id) = client_order_id.map(str::trim).filter(|id| !id.is_empty()) else {
        return OrderOwnershipClass::ManualOperator;
    };
    let id = client_order_id.to_ascii_lowercase();
    if id.starts_with("spotctl") || id.starts_with("spot-control") || id.starts_with("sc-") {
        OrderOwnershipClass::SpotControl
    } else if id.starts_with("arb") || id.contains("arbitrage") {
        OrderOwnershipClass::ArbitrageStrategy
    } else if id.starts_with("strat") || id.starts_with("grid") || id.starts_with("mm") {
        OrderOwnershipClass::OtherKnownStrategy
    } else {
        OrderOwnershipClass::Unknown
    }
}

pub fn unknown_sell_order_quantity(
    open_orders: &[super::OpenOrderOwnership],
) -> HashMap<String, f64> {
    let mut totals = HashMap::new();
    for order in open_orders {
        if order.side == OrderSide::Sell && !order.ownership_known {
            *totals
                .entry(format!("{}:{}", order.exchange, order.symbol))
                .or_insert(0.0) += order.remaining_quantity;
        }
    }
    totals
}

fn merge_balance_reports(
    reports: Vec<BalanceReconciliationReport>,
) -> Option<BalanceReconciliationReport> {
    let mut iter = reports.into_iter();
    let mut merged = iter.next()?;
    for report in iter {
        merged.statuses.extend(report.statuses);
        merged.max_severity = merged.max_severity.max(report.max_severity);
        merged.clean = merged.clean && report.clean;
        merged.timestamp = merged.timestamp.max(report.timestamp);
    }
    Some(merged)
}
