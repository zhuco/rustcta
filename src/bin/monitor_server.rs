use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::extract::State;
use axum::response::Html;
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, NaiveDate, Utc};
use clap::Parser;
use rustcta::core::config::Config;
use rustcta::core::types::{Balance, MarketType, OrderSide, Position, Trade};
use rustcta::cta::account_manager::{AccountConfig, AccountManager};
use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use tokio::sync::RwLock;
use tokio::time::{Duration, MissedTickBehavior};
use tower_http::cors::{Any, CorsLayer};

#[derive(Parser, Debug)]
#[command(name = "monitor-server", version, about = "RustCTA 实时监控页面服务")]
struct Args {
    #[arg(long, default_value = "0.0.0.0")]
    host: String,
    #[arg(long, default_value_t = 8088)]
    port: u16,
    #[arg(long, default_value = "config/accounts.yml")]
    accounts: String,
    #[arg(long, default_value = "config/monitoring.yml")]
    monitoring: String,
    #[arg(long, default_value_t = 3)]
    refresh_secs: u64,
    #[arg(long, default_value_t = 20)]
    trades_refresh_secs: u64,
}

#[derive(Clone)]
struct AppState {
    snapshot: Arc<RwLock<DashboardSnapshot>>,
}

#[derive(Debug, Clone, Serialize)]
struct DashboardSnapshot {
    generated_at: DateTime<Utc>,
    summary: SummaryCard,
    strategies: Vec<StrategyCard>,
    warnings: Vec<String>,
    notes: Vec<String>,
}

impl Default for DashboardSnapshot {
    fn default() -> Self {
        Self {
            generated_at: Utc::now(),
            summary: SummaryCard::default(),
            strategies: Vec::new(),
            warnings: Vec::new(),
            notes: vec![
                "仓位与未实现PNL来自交易所接口，实时准确性最高。".to_string(),
                "已实现PNL优先使用交易所成交回报(realizedPnl)的7天汇总；若交易所不提供则回退到本地估算。".to_string(),
                "爆仓价/距离为本地模型估算：对冲网格会纳入网格间距与每单大小；全仓场景会按稳定币权益池(USDT/USDC等)按仓位名义分摊。".to_string(),
            ],
        }
    }
}

#[derive(Debug, Clone, Serialize, Default)]
struct SummaryCard {
    strategies_total: usize,
    strategies_online: usize,
    open_positions: usize,
    total_account_equity: f64,
    gross_notional: f64,
    net_notional: f64,
    unrealized_pnl: f64,
    realized_pnl_est: f64,
    total_pnl_est: f64,
    volume_1h: f64,
    volume_24h: f64,
    volume_7d: f64,
}

#[derive(Debug, Clone, Serialize)]
struct StrategyCard {
    id: String,
    name: String,
    kind: String,
    account_id: String,
    market_type: String,
    symbols_count: usize,
    monitored_symbols: Vec<String>,
    status: String,
    last_update: DateTime<Utc>,
    open_positions: usize,
    account_equity_total: f64,
    gross_notional: f64,
    net_notional: f64,
    unrealized_pnl: f64,
    realized_pnl_est: f64,
    total_pnl_est: f64,
    account_pnl_since_start: f64,
    account_pnl_today: f64,
    volume_1h: f64,
    volume_24h: f64,
    volume_7d: f64,
    recent_trade_count: usize,
    last_trade_at: Option<DateTime<Utc>>,
    positions: Vec<PositionRow>,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct PositionRow {
    symbol: String,
    side: String,
    qty: f64,
    entry_price: f64,
    mark_price: f64,
    notional: f64,
    unrealized_pnl: f64,
    liq_price_est: Option<f64>,
    liq_distance_pct: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct AccountsFile {
    accounts: HashMap<String, AccountItem>,
}

#[derive(Debug, Deserialize)]
struct AccountItem {
    exchange: String,
    env_prefix: String,
    enabled: Option<bool>,
    settings: Option<AccountSettings>,
}

#[derive(Debug, Deserialize)]
struct AccountSettings {
    max_positions: Option<u32>,
    max_orders_per_symbol: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct MonitoringConfigFile {
    refresh_secs: Option<u64>,
    trades_refresh_secs: Option<u64>,
    strategies: Option<Vec<StrategySource>>,
}

#[derive(Debug, Clone, Deserialize)]
struct StrategySource {
    id: String,
    name: Option<String>,
    kind: String,
    config_path: String,
    enabled: Option<bool>,
}

#[derive(Debug, Clone)]
struct StrategyTarget {
    id: String,
    name: String,
    kind: String,
    account_id: String,
    market_type: MarketType,
    symbols_query: HashSet<String>,
    symbols_norm: HashSet<String>,
    grid_symbol_configs: HashMap<String, GridSymbolConfig>,
    source_config: String,
}

#[derive(Debug, Clone, Default)]
struct GridSymbolConfig {
    spacing_pct: Option<f64>,
    spacing_abs: Option<f64>,
    order_size_base: Option<f64>,
    order_notional: Option<f64>,
    levels_per_side: u32,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct EquityKey {
    account_id: String,
    market_type: MarketType,
}

#[derive(Debug, Clone)]
struct EquityBaseline {
    start_total: f64,
    day_start_total: f64,
    day_marker: NaiveDate,
}

#[derive(Debug, Clone)]
struct AccountMarketSnapshot {
    positions: Vec<Position>,
    balances: Vec<Balance>,
    equity_total: f64,
    last_update: DateTime<Utc>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct TradeStoreKey {
    strategy_id: String,
    account_id: String,
    symbol: String,
    market_type: MarketType,
}

#[derive(Debug, Clone)]
struct TradeSample {
    timestamp: DateTime<Utc>,
    quote_volume: f64,
    side: OrderSide,
    qty: f64,
    price: f64,
    fee: f64,
    exchange_realized_pnl: Option<f64>,
}

#[derive(Debug, Clone, Default)]
struct PnlState {
    net_qty: f64,
    avg_cost: f64,
    realized_pnl: f64,
}

#[derive(Debug, Clone, Default)]
struct TradeStore {
    samples: VecDeque<TradeSample>,
    seen_ids: HashSet<String>,
    seen_queue: VecDeque<String>,
}

const TRADE_RETENTION_DAYS: i64 = 8;
const TRADE_SAMPLE_LIMIT: usize = 20_000;
const TRADE_DEDUP_CACHE_SIZE: usize = 20_000;

struct MonitorEngine {
    account_manager: Arc<AccountManager>,
    targets: Vec<StrategyTarget>,
    trades_refresh_interval: Duration,
    last_trades_poll: Option<DateTime<Utc>>,
    trade_stores: HashMap<TradeStoreKey, TradeStore>,
    equity_baselines: HashMap<EquityKey, EquityBaseline>,
}

impl MonitorEngine {
    fn new(
        account_manager: Arc<AccountManager>,
        targets: Vec<StrategyTarget>,
        trades_refresh_secs: u64,
    ) -> Self {
        Self {
            account_manager,
            targets,
            trades_refresh_interval: Duration::from_secs(trades_refresh_secs.max(5)),
            last_trades_poll: None,
            trade_stores: HashMap::new(),
            equity_baselines: HashMap::new(),
        }
    }

    async fn refresh(&mut self) -> DashboardSnapshot {
        let now = Utc::now();
        let mut warnings = Vec::new();

        let unique_keys = self.unique_equity_keys();
        let mut account_snapshots: HashMap<EquityKey, AccountMarketSnapshot> = HashMap::new();
        for key in unique_keys {
            match self.fetch_account_market_snapshot(&key).await {
                Ok(snapshot) => {
                    self.update_equity_baseline(&key, snapshot.equity_total, now.date_naive());
                    account_snapshots.insert(key, snapshot);
                }
                Err(err) => {
                    warnings.push(format!(
                        "账户 {} {:?} 拉取失败: {}",
                        key.account_id, key.market_type, err
                    ));
                }
            }
        }

        let should_poll_trades = self
            .last_trades_poll
            .map(|t| (now - t).num_seconds() >= self.trades_refresh_interval.as_secs() as i64)
            .unwrap_or(true);

        if should_poll_trades {
            self.poll_trades().await;
            self.last_trades_poll = Some(now);
        }

        let mut strategies = Vec::new();
        for target in &self.targets {
            let key = EquityKey {
                account_id: target.account_id.clone(),
                market_type: target.market_type,
            };

            if let Some(snapshot) = account_snapshots.get(&key) {
                strategies.push(self.build_strategy_card(target, snapshot, now));
            } else {
                strategies.push(self.build_offline_card(target, now));
            }
        }

        let summary = aggregate_summary(&strategies);

        DashboardSnapshot {
            generated_at: now,
            summary,
            strategies,
            warnings,
            notes: vec![
                "仓位与未实现PNL直接读取交易所。".to_string(),
                "已实现PNL优先使用交易所成交回报(realizedPnl)的7天汇总；若不可用则回退到本地成交序列估算。".to_string(),
                "账户权益变化 = 交易所余额总额差分，通常比逐笔估算更接近真实。".to_string(),
                "爆仓价/距离为模型估算：网格策略会模拟按配置继续加仓；全仓场景会按稳定币权益池(USDT/USDC等)按仓位名义分摊。"
                    .to_string(),
            ],
        }
    }

    fn unique_equity_keys(&self) -> Vec<EquityKey> {
        let mut seen = HashSet::new();
        let mut keys = Vec::new();
        for t in &self.targets {
            let key = EquityKey {
                account_id: t.account_id.clone(),
                market_type: t.market_type,
            };
            if seen.insert((key.account_id.clone(), key.market_type)) {
                keys.push(key);
            }
        }
        keys
    }

    async fn fetch_account_market_snapshot(
        &self,
        key: &EquityKey,
    ) -> Result<AccountMarketSnapshot> {
        let account = self
            .account_manager
            .get_account(&key.account_id)
            .ok_or_else(|| anyhow::anyhow!("账户 {} 不存在", key.account_id))?;

        let positions = match key.market_type {
            MarketType::Futures => account
                .exchange
                .get_positions(None)
                .await
                .with_context(|| format!("{} futures get_positions", key.account_id))?,
            MarketType::Spot => Vec::new(),
        };

        let balances = account
            .exchange
            .get_balance(key.market_type)
            .await
            .with_context(|| format!("{} {:?} get_balance", key.account_id, key.market_type))?;

        let equity_total = balances.iter().map(|b| b.total).sum::<f64>();

        Ok(AccountMarketSnapshot {
            positions,
            balances,
            equity_total,
            last_update: Utc::now(),
        })
    }

    fn update_equity_baseline(
        &mut self,
        key: &EquityKey,
        current_total: f64,
        current_day: NaiveDate,
    ) {
        self.equity_baselines
            .entry(key.clone())
            .and_modify(|b| {
                if b.day_marker != current_day {
                    b.day_marker = current_day;
                    b.day_start_total = current_total;
                }
            })
            .or_insert(EquityBaseline {
                start_total: current_total,
                day_start_total: current_total,
                day_marker: current_day,
            });
    }

    async fn poll_trades(&mut self) {
        let targets = self.targets.clone();
        for target in &targets {
            let account = match self.account_manager.get_account(&target.account_id) {
                Some(a) => a,
                None => continue,
            };

            if target.symbols_query.is_empty() {
                if let Ok(mut trades) = account
                    .exchange
                    .get_my_trades(None, target.market_type, Some(200))
                    .await
                {
                    trades.sort_by_key(|t| t.timestamp);
                    for trade in trades {
                        self.register_trade(target, "ALL", &trade);
                    }
                }
                continue;
            }

            for symbol in &target.symbols_query {
                match account
                    .exchange
                    .get_my_trades(Some(symbol), target.market_type, Some(200))
                    .await
                {
                    Ok(mut trades) => {
                        trades.sort_by_key(|t| t.timestamp);
                        for trade in trades {
                            self.register_trade(target, symbol, &trade);
                        }
                    }
                    Err(err) => {
                        log::warn!(
                            "拉取成交失败 strategy={} account={} symbol={} err={}",
                            target.id,
                            target.account_id,
                            symbol,
                            err
                        );
                    }
                }
            }
        }
    }

    fn register_trade(&mut self, target: &StrategyTarget, symbol_hint: &str, trade: &Trade) {
        let symbol = if trade.symbol.is_empty() {
            symbol_hint.to_string()
        } else {
            trade.symbol.clone()
        };

        let key = TradeStoreKey {
            strategy_id: target.id.clone(),
            account_id: target.account_id.clone(),
            symbol: normalize_symbol(&symbol),
            market_type: target.market_type,
        };

        let store = self.trade_stores.entry(key).or_default();
        let uid = format!(
            "{}:{}:{}",
            trade.id,
            trade.order_id.clone().unwrap_or_default(),
            trade.timestamp.timestamp_millis()
        );

        if store.seen_ids.contains(&uid) {
            return;
        }

        let qty = trade.amount.max(0.0);
        let price = trade.price.max(0.0);
        let fee_cost = trade.fee.as_ref().map(|f| f.cost).unwrap_or(0.0);

        store.samples.push_back(TradeSample {
            timestamp: trade.timestamp,
            quote_volume: (qty * price).abs(),
            side: trade.side.clone(),
            qty,
            price,
            fee: fee_cost,
            exchange_realized_pnl: trade.fee.as_ref().and_then(|f| f.rate),
        });
        store.seen_ids.insert(uid.clone());
        store.seen_queue.push_back(uid);

        while store.samples.len() > TRADE_SAMPLE_LIMIT {
            store.samples.pop_front();
        }

        while store.seen_queue.len() > TRADE_DEDUP_CACHE_SIZE {
            if let Some(old) = store.seen_queue.pop_front() {
                store.seen_ids.remove(&old);
            }
        }

        let cutoff = Utc::now() - chrono::Duration::days(TRADE_RETENTION_DAYS);
        while let Some(front) = store.samples.front() {
            if front.timestamp < cutoff {
                store.samples.pop_front();
            } else {
                break;
            }
        }
    }

    fn build_strategy_card(
        &self,
        target: &StrategyTarget,
        snapshot: &AccountMarketSnapshot,
        now: DateTime<Utc>,
    ) -> StrategyCard {
        let mut warnings = Vec::new();

        let mut positions = if target.market_type == MarketType::Futures {
            let matched_positions = snapshot
                .positions
                .iter()
                .filter(|p| {
                    target.symbols_norm.is_empty()
                        || target.symbols_norm.contains(&normalize_symbol(&p.symbol))
                })
                .collect::<Vec<_>>();

            let account_margin_pool = stable_margin_pool(&snapshot.balances);
            let account_notional_total = matched_positions
                .iter()
                .map(|p| derive_position_qty(p).abs() * p.mark_price.abs())
                .sum::<f64>();

            matched_positions
                .into_iter()
                .filter_map(|p| {
                    position_to_row(target, p, account_margin_pool, account_notional_total)
                })
                .collect::<Vec<_>>()
        } else {
            self.spot_balances_to_rows(target, snapshot)
        };

        positions.sort_by(|a, b| {
            b.notional
                .partial_cmp(&a.notional)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let open_positions = positions.len();
        let gross_notional = positions.iter().map(|p| p.notional.abs()).sum::<f64>();
        let net_notional = positions
            .iter()
            .map(|p| {
                if p.side == "LONG" {
                    p.notional
                } else {
                    -p.notional
                }
            })
            .sum::<f64>();
        let unrealized_pnl = positions.iter().map(|p| p.unrealized_pnl).sum::<f64>();

        let trade_keys = self
            .trade_stores
            .keys()
            .filter(|k| k.strategy_id == target.id)
            .cloned()
            .collect::<Vec<_>>();

        let mut realized_pnl_est = 0.0;
        let mut volume_1h = 0.0;
        let mut volume_24h = 0.0;
        let mut volume_7d = 0.0;
        let mut recent_trade_count = 0usize;
        let mut last_trade_at: Option<DateTime<Utc>> = None;

        let cutoff_1h = now - chrono::Duration::hours(1);
        let cutoff_24h = now - chrono::Duration::hours(24);
        let cutoff_7d = now - chrono::Duration::days(7);

        for key in trade_keys {
            if let Some(store) = self.trade_stores.get(&key) {
                realized_pnl_est += estimate_realized_pnl_since(&store.samples, cutoff_7d);
                for sample in &store.samples {
                    if sample.timestamp >= cutoff_7d {
                        volume_7d += sample.quote_volume;
                        if sample.timestamp >= cutoff_24h {
                            volume_24h += sample.quote_volume;
                            if sample.timestamp >= cutoff_1h {
                                recent_trade_count += 1;
                                volume_1h += sample.quote_volume;
                            }
                        }
                    }
                    if last_trade_at.map(|t| sample.timestamp > t).unwrap_or(true) {
                        last_trade_at = Some(sample.timestamp);
                    }
                }
            }
        }

        let total_pnl_est = realized_pnl_est + unrealized_pnl;

        let eq_key = EquityKey {
            account_id: target.account_id.clone(),
            market_type: target.market_type,
        };

        let (account_pnl_since_start, account_pnl_today) = self
            .equity_baselines
            .get(&eq_key)
            .map(|b| {
                (
                    snapshot.equity_total - b.start_total,
                    snapshot.equity_total - b.day_start_total,
                )
            })
            .unwrap_or((0.0, 0.0));

        if now - snapshot.last_update > chrono::Duration::seconds(15) {
            warnings.push("数据刷新已超过15秒".to_string());
        }

        let liq_risk_count = positions
            .iter()
            .filter(|p| p.liq_distance_pct.map(|v| v <= 0.0).unwrap_or(false))
            .count();
        if liq_risk_count > 0 {
            warnings.push(format!(
                "{} 个仓位估算距离爆仓<=0%，请核对杠杆与保证金",
                liq_risk_count
            ));
        }

        let status = if warnings.is_empty() {
            "ONLINE"
        } else {
            "DEGRADED"
        };

        let mut monitored_symbols = target.symbols_query.iter().cloned().collect::<Vec<_>>();
        monitored_symbols.sort();

        StrategyCard {
            id: target.id.clone(),
            name: target.name.clone(),
            kind: target.kind.clone(),
            account_id: target.account_id.clone(),
            market_type: market_type_label(target.market_type).to_string(),
            symbols_count: monitored_symbols.len(),
            monitored_symbols,
            status: status.to_string(),
            last_update: snapshot.last_update,
            open_positions,
            account_equity_total: snapshot.equity_total,
            gross_notional,
            net_notional,
            unrealized_pnl,
            realized_pnl_est,
            total_pnl_est,
            account_pnl_since_start,
            account_pnl_today,
            volume_1h,
            volume_24h,
            volume_7d,
            recent_trade_count,
            last_trade_at,
            positions,
            warnings,
        }
    }

    fn build_offline_card(&self, target: &StrategyTarget, now: DateTime<Utc>) -> StrategyCard {
        let mut monitored_symbols = target.symbols_query.iter().cloned().collect::<Vec<_>>();
        monitored_symbols.sort();

        StrategyCard {
            id: target.id.clone(),
            name: target.name.clone(),
            kind: target.kind.clone(),
            account_id: target.account_id.clone(),
            market_type: market_type_label(target.market_type).to_string(),
            symbols_count: monitored_symbols.len(),
            monitored_symbols,
            status: "OFFLINE".to_string(),
            last_update: now,
            open_positions: 0,
            account_equity_total: 0.0,
            gross_notional: 0.0,
            net_notional: 0.0,
            unrealized_pnl: 0.0,
            realized_pnl_est: 0.0,
            total_pnl_est: 0.0,
            account_pnl_since_start: 0.0,
            account_pnl_today: 0.0,
            volume_1h: 0.0,
            volume_24h: 0.0,
            volume_7d: 0.0,
            recent_trade_count: 0,
            last_trade_at: None,
            positions: Vec::new(),
            warnings: vec!["账户数据不可用".to_string()],
        }
    }

    fn spot_balances_to_rows(
        &self,
        target: &StrategyTarget,
        snapshot: &AccountMarketSnapshot,
    ) -> Vec<PositionRow> {
        let mut rows = Vec::new();
        let watched = target
            .symbols_query
            .iter()
            .map(|s| split_symbol_pair(s).0)
            .collect::<HashSet<_>>();

        for b in &snapshot.balances {
            if b.total.abs() < 1e-10 {
                continue;
            }
            if !watched.is_empty() && !watched.contains(&b.currency.to_uppercase()) {
                continue;
            }

            rows.push(PositionRow {
                symbol: b.currency.clone(),
                side: "LONG".to_string(),
                qty: b.total,
                entry_price: 0.0,
                mark_price: 0.0,
                notional: 0.0,
                unrealized_pnl: 0.0,
                liq_price_est: None,
                liq_distance_pct: None,
            });
        }

        rows
    }
}

fn aggregate_summary(cards: &[StrategyCard]) -> SummaryCard {
    let strategies_total = cards.len();
    let strategies_online = cards
        .iter()
        .filter(|c| c.status == "ONLINE" || c.status == "DEGRADED")
        .count();

    let open_positions = cards.iter().map(|c| c.open_positions).sum();
    let mut account_seen = HashSet::new();
    let total_account_equity = cards
        .iter()
        .filter_map(|c| {
            let key = format!("{}|{}", c.account_id, c.market_type);
            if account_seen.insert(key) {
                Some(c.account_equity_total)
            } else {
                None
            }
        })
        .sum();
    let gross_notional = cards.iter().map(|c| c.gross_notional).sum();
    let net_notional = cards.iter().map(|c| c.net_notional).sum();
    let unrealized_pnl = cards.iter().map(|c| c.unrealized_pnl).sum();
    let realized_pnl_est = cards.iter().map(|c| c.realized_pnl_est).sum();
    let total_pnl_est = cards.iter().map(|c| c.total_pnl_est).sum();
    let volume_1h = cards.iter().map(|c| c.volume_1h).sum();
    let volume_24h = cards.iter().map(|c| c.volume_24h).sum();
    let volume_7d = cards.iter().map(|c| c.volume_7d).sum();

    SummaryCard {
        strategies_total,
        strategies_online,
        open_positions,
        total_account_equity,
        gross_notional,
        net_notional,
        unrealized_pnl,
        realized_pnl_est,
        total_pnl_est,
        volume_1h,
        volume_24h,
        volume_7d,
    }
}

fn apply_trade_to_pnl_state(state: &mut PnlState, side: OrderSide, qty: f64, price: f64, fee: f64) {
    if qty <= 0.0 || price <= 0.0 {
        return;
    }

    let mut remaining = qty;
    let mut realized = 0.0;

    match side {
        OrderSide::Buy => {
            if state.net_qty < -1e-12 {
                let short_qty = -state.net_qty;
                let close_qty = remaining.min(short_qty);
                realized += (state.avg_cost - price) * close_qty;
                state.net_qty += close_qty;
                remaining -= close_qty;
                if state.net_qty.abs() <= 1e-12 {
                    state.net_qty = 0.0;
                    state.avg_cost = 0.0;
                }
            }

            if remaining > 1e-12 {
                if state.net_qty > 1e-12 {
                    let total_qty = state.net_qty + remaining;
                    state.avg_cost =
                        (state.avg_cost * state.net_qty + price * remaining) / total_qty.max(1e-12);
                    state.net_qty = total_qty;
                } else {
                    state.net_qty = remaining;
                    state.avg_cost = price;
                }
            }
        }
        OrderSide::Sell => {
            if state.net_qty > 1e-12 {
                let long_qty = state.net_qty;
                let close_qty = remaining.min(long_qty);
                realized += (price - state.avg_cost) * close_qty;
                state.net_qty -= close_qty;
                remaining -= close_qty;
                if state.net_qty.abs() <= 1e-12 {
                    state.net_qty = 0.0;
                    state.avg_cost = 0.0;
                }
            }

            if remaining > 1e-12 {
                if state.net_qty < -1e-12 {
                    let total_short_qty = (-state.net_qty) + remaining;
                    state.avg_cost = (state.avg_cost * (-state.net_qty) + price * remaining)
                        / total_short_qty.max(1e-12);
                    state.net_qty = -total_short_qty;
                } else {
                    state.net_qty = -remaining;
                    state.avg_cost = price;
                }
            }
        }
    }

    state.realized_pnl += realized - fee;
}

fn estimate_realized_pnl_since(samples: &VecDeque<TradeSample>, cutoff: DateTime<Utc>) -> f64 {
    if samples.iter().any(|s| s.exchange_realized_pnl.is_some()) {
        return samples
            .iter()
            .filter(|s| s.timestamp >= cutoff)
            .map(|s| s.exchange_realized_pnl.unwrap_or(0.0))
            .filter(|v| v.is_finite())
            .sum();
    }

    let mut ordered = samples.iter().cloned().collect::<Vec<_>>();
    ordered.sort_by_key(|s| s.timestamp);

    let mut state = PnlState::default();
    let mut realized_since = 0.0;
    for sample in ordered {
        let before = state.realized_pnl;
        apply_trade_to_pnl_state(
            &mut state,
            sample.side,
            sample.qty,
            sample.price,
            sample.fee,
        );
        if sample.timestamp >= cutoff {
            realized_since += state.realized_pnl - before;
        }
    }

    realized_since
}

fn is_stable_margin_currency(currency: &str) -> bool {
    matches!(
        currency.trim().to_ascii_uppercase().as_str(),
        "USDT" | "USDC" | "BUSD" | "FDUSD" | "USD"
    )
}

fn stable_margin_pool(balances: &[Balance]) -> f64 {
    balances
        .iter()
        .filter(|b| is_stable_margin_currency(&b.currency))
        .map(|b| b.total.max(0.0))
        .sum::<f64>()
}

fn position_to_row(
    target: &StrategyTarget,
    pos: &Position,
    account_margin_pool: f64,
    account_notional_total: f64,
) -> Option<PositionRow> {
    let qty = derive_position_qty(pos);
    if qty.abs() < 1e-10 {
        return None;
    }

    let side = if qty >= 0.0 { "LONG" } else { "SHORT" };
    let notional = qty.abs() * pos.mark_price.abs();

    let (liq_price_est, liq_distance_pct) = estimate_position_liq(
        target,
        pos,
        qty,
        notional,
        account_margin_pool,
        account_notional_total,
    );

    Some(PositionRow {
        symbol: pos.symbol.clone(),
        side: side.to_string(),
        qty,
        entry_price: pos.entry_price,
        mark_price: pos.mark_price,
        notional,
        unrealized_pnl: pos.unrealized_pnl,
        liq_price_est,
        liq_distance_pct,
    })
}

fn estimate_position_liq(
    target: &StrategyTarget,
    pos: &Position,
    qty: f64,
    notional: f64,
    account_margin_pool: f64,
    account_notional_total: f64,
) -> (Option<f64>, Option<f64>) {
    if qty.abs() < 1e-10 || pos.entry_price <= 0.0 || pos.mark_price <= 0.0 || notional <= 0.0 {
        return (None, None);
    }

    let is_grid = is_grid_kind(target.kind.as_str());
    let margin_type = pos.margin_type.as_deref().unwrap_or_default();
    let is_cross_margin = margin_type.eq_ignore_ascii_case("cross");
    let shared_margin_alloc = if account_margin_pool > 1e-9 && account_notional_total > 1e-9 {
        account_margin_pool * (notional / account_notional_total)
    } else {
        0.0
    };

    if !is_grid && pos.margin <= 1e-9 && shared_margin_alloc <= 1e-9 {
        return (None, None);
    }

    let symbol_key = normalize_symbol(&pos.symbol);
    let grid_cfg = target.grid_symbol_configs.get(&symbol_key);
    let leverage = estimate_effective_leverage(pos, notional);

    let mut total_qty = qty.abs();
    let mut weighted_cost = pos.entry_price * total_qty;
    let mut margin_budget = if pos.margin > 1e-9 {
        pos.margin.abs()
    } else if shared_margin_alloc > 1e-9 {
        shared_margin_alloc
    } else {
        notional / leverage
    };

    if is_cross_margin && shared_margin_alloc > 1e-9 {
        margin_budget = margin_budget.max(shared_margin_alloc);
    }

    if is_grid {
        if let Some(cfg) = grid_cfg {
            for level in 1..=cfg.levels_per_side {
                let Some(level_price) = grid_level_price(pos.entry_price, qty, level, cfg) else {
                    continue;
                };

                let add_qty =
                    if let Some(order_size_base) = cfg.order_size_base.filter(|v| *v > 0.0) {
                        order_size_base
                    } else if let Some(order_notional) = cfg.order_notional.filter(|v| *v > 0.0) {
                        order_notional / level_price
                    } else {
                        0.0
                    };

                if add_qty <= 0.0 {
                    continue;
                }

                total_qty += add_qty;
                weighted_cost += add_qty * level_price;

                let add_notional =
                    if let Some(order_notional) = cfg.order_notional.filter(|v| *v > 0.0) {
                        order_notional
                    } else {
                        add_qty * level_price
                    };
                margin_budget += add_notional / leverage;
            }
        }
    }

    if total_qty <= 1e-9 || margin_budget <= 1e-9 {
        return (None, None);
    }

    let avg_cost = weighted_cost / total_qty;
    let liq_price = if qty >= 0.0 {
        avg_cost - margin_budget / total_qty
    } else {
        avg_cost + margin_budget / total_qty
    };

    if !liq_price.is_finite() || liq_price <= 0.0 {
        return (None, None);
    }

    let distance_pct = if qty >= 0.0 {
        (pos.mark_price - liq_price) / pos.mark_price * 100.0
    } else {
        (liq_price - pos.mark_price) / pos.mark_price * 100.0
    };

    if !distance_pct.is_finite() {
        return (Some(liq_price), None);
    }

    (Some(liq_price), Some(distance_pct))
}

fn is_grid_kind(kind: &str) -> bool {
    matches!(
        kind.trim().to_ascii_lowercase().as_str(),
        "hedged_grid" | "solusdc_hedged_grid"
    )
}

fn estimate_effective_leverage(pos: &Position, notional: f64) -> f64 {
    if let Some(lv) = pos.leverage {
        if lv > 0 {
            return (lv as f64).clamp(1.0, 125.0);
        }
    }

    if pos.margin > 1e-9 && notional > 1e-9 {
        let lv = (notional / pos.margin).abs();
        if lv.is_finite() && lv > 0.0 {
            return lv.clamp(1.0, 125.0);
        }
    }

    10.0
}

fn grid_level_price(entry_price: f64, qty: f64, level: u32, cfg: &GridSymbolConfig) -> Option<f64> {
    if level == 0 || entry_price <= 0.0 {
        return None;
    }

    let level_f = level as f64;
    let delta = if let Some(pct) = cfg.spacing_pct.filter(|v| *v > 0.0) {
        entry_price * pct * level_f
    } else if let Some(abs) = cfg.spacing_abs.filter(|v| *v > 0.0) {
        abs * level_f
    } else {
        return None;
    };

    let price = if qty >= 0.0 {
        entry_price - delta
    } else {
        entry_price + delta
    };

    if price > 0.0 && price.is_finite() {
        Some(price)
    } else {
        None
    }
}

fn derive_position_qty(pos: &Position) -> f64 {
    if pos.amount.abs() > 1e-12 {
        return pos.amount;
    }

    if pos.contracts.abs() > 1e-12 {
        return signed_by_side(pos.contracts, &pos.side);
    }

    if pos.size.abs() > 1e-12 {
        return signed_by_side(pos.size, &pos.side);
    }

    0.0
}

fn signed_by_side(value: f64, side: &str) -> f64 {
    let v = value.abs();
    let side_upper = side.to_uppercase();
    if side_upper.contains("SHORT") || side_upper.contains("SELL") {
        -v
    } else {
        v
    }
}

fn normalize_symbol(symbol: &str) -> String {
    symbol
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>()
        .to_uppercase()
}

fn normalize_query_symbol(symbol: &str) -> String {
    let s = symbol.trim().to_uppercase();
    if s.contains('/') || s.contains('-') {
        return s.replace('-', "/");
    }

    for quote in ["USDT", "USDC", "BUSD", "FDUSD", "BTC", "ETH"] {
        if s.ends_with(quote) && s.len() > quote.len() {
            let base = &s[..s.len() - quote.len()];
            if !base.is_empty() {
                return format!("{}/{}", base, quote);
            }
        }
    }

    s
}

fn split_symbol_pair(symbol: &str) -> (String, String) {
    if symbol.contains('/') {
        let mut parts = symbol.split('/');
        let base = parts.next().unwrap_or(symbol).trim().to_uppercase();
        let quote = parts.next().unwrap_or("USDT").trim().to_uppercase();
        return (base, quote);
    }

    if symbol.contains('-') {
        let mut parts = symbol.split('-');
        let base = parts.next().unwrap_or(symbol).trim().to_uppercase();
        let quote = parts.next().unwrap_or("USDT").trim().to_uppercase();
        return (base, quote);
    }

    (symbol.to_uppercase(), "USDT".to_string())
}

fn market_type_label(mt: MarketType) -> &'static str {
    match mt {
        MarketType::Spot => "Spot",
        MarketType::Futures => "Futures",
    }
}

fn parse_market_type(raw: Option<&str>) -> MarketType {
    match raw
        .unwrap_or("futures")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "spot" => MarketType::Spot,
        _ => MarketType::Futures,
    }
}

fn parse_grid_symbol_config(grid: Option<&Value>, symbol: &str) -> GridSymbolConfig {
    let Some(grid) = grid else {
        return GridSymbolConfig::default();
    };

    let spacing_mode = grid
        .get("spacing_mode")
        .and_then(|v| v.as_str())
        .unwrap_or("percentage")
        .to_ascii_lowercase();
    let spacing = yaml_f64(grid.get("spacing"));

    let mut spacing_pct = yaml_f64(grid.get("grid_spacing_pct"));
    let mut spacing_abs = yaml_f64(grid.get("grid_spacing_abs"));

    if spacing_pct.is_none() && spacing_abs.is_none() {
        match spacing_mode.as_str() {
            "absolute" | "abs" => spacing_abs = spacing,
            _ => spacing_pct = spacing,
        }
    }

    let levels_per_side = yaml_u32(grid.get("levels_per_side")).unwrap_or(0);
    let order_size_base = yaml_f64(grid.get("order_size"));
    let order_notional =
        yaml_f64(grid.get("order_notional")).or_else(|| yaml_f64(grid.get("notional_per_order")));

    if spacing_pct.unwrap_or(0.0) <= 0.0
        && spacing_abs.unwrap_or(0.0) <= 0.0
        && (order_size_base.unwrap_or(0.0) > 0.0 || order_notional.unwrap_or(0.0) > 0.0)
    {
        log::warn!(
            "交易对 {} 网格参数缺少 spacing，爆仓估算将退化为非网格模型",
            symbol
        );
    }

    GridSymbolConfig {
        spacing_pct: spacing_pct.filter(|v| *v > 0.0),
        spacing_abs: spacing_abs.filter(|v| *v > 0.0),
        order_size_base: order_size_base.filter(|v| *v > 0.0),
        order_notional: order_notional.filter(|v| *v > 0.0),
        levels_per_side,
    }
}

fn yaml_f64(v: Option<&Value>) -> Option<f64> {
    let v = v?;
    if let Some(x) = v.as_f64() {
        return Some(x);
    }
    if let Some(x) = v.as_i64() {
        return Some(x as f64);
    }
    if let Some(x) = v.as_u64() {
        return Some(x as f64);
    }
    v.as_str().and_then(|s| s.parse::<f64>().ok())
}

fn yaml_u32(v: Option<&Value>) -> Option<u32> {
    let v = v?;
    if let Some(x) = v.as_u64() {
        return u32::try_from(x).ok();
    }
    if let Some(x) = v.as_i64() {
        if x >= 0 {
            return u32::try_from(x as u64).ok();
        }
    }
    v.as_str().and_then(|s| s.parse::<u32>().ok())
}

fn load_monitoring_sources(path: &Path) -> Result<(Option<u64>, Option<u64>, Vec<StrategySource>)> {
    if !path.exists() {
        return Ok((None, None, default_sources()));
    }

    let text = std::fs::read_to_string(path)
        .with_context(|| format!("读取监控配置失败: {}", path.display()))?;
    let cfg: MonitoringConfigFile = serde_yaml::from_str(&text)
        .with_context(|| format!("解析监控配置失败: {}", path.display()))?;

    let sources = cfg.strategies.unwrap_or_else(default_sources);
    Ok((cfg.refresh_secs, cfg.trades_refresh_secs, sources))
}

fn looks_like_workspace_root(path: &Path) -> bool {
    path.join("Cargo.toml").is_file() && path.join("config").is_dir()
}

fn detect_workspace_root() -> PathBuf {
    let mut candidates: Vec<PathBuf> = Vec::new();

    if let Ok(explicit_root) = std::env::var("RUSTCTA_WORKSPACE") {
        let p = PathBuf::from(explicit_root);
        if p.is_absolute() {
            candidates.push(p);
        } else if let Ok(cwd) = std::env::current_dir() {
            candidates.push(cwd.join(p));
        }
    }

    if let Ok(cwd) = std::env::current_dir() {
        candidates.push(cwd);
    }

    if let Ok(exe_path) = std::env::current_exe() {
        let mut current = exe_path.parent().map(Path::to_path_buf);
        for _ in 0..6 {
            let Some(path) = current.clone() else {
                break;
            };
            candidates.push(path.clone());
            current = path.parent().map(Path::to_path_buf);
        }
    }

    candidates.push(PathBuf::from(env!("CARGO_MANIFEST_DIR")));

    let mut seen = HashSet::new();
    for candidate in candidates {
        let canonical = candidate.canonicalize().unwrap_or(candidate);
        if !seen.insert(canonical.clone()) {
            continue;
        }
        if looks_like_workspace_root(&canonical) {
            return canonical;
        }
    }

    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

fn resolve_config_path(workspace_root: &Path, raw_path: &str) -> PathBuf {
    let configured = PathBuf::from(raw_path);
    if configured.is_absolute() {
        configured
    } else {
        workspace_root.join(configured)
    }
}

fn default_sources() -> Vec<StrategySource> {
    vec![
        StrategySource {
            id: "hedged_grid".to_string(),
            name: Some("对冲网格".to_string()),
            kind: "hedged_grid".to_string(),
            config_path: "config/hedged_grid_dual_basket.yml".to_string(),
            enabled: Some(true),
        },
        StrategySource {
            id: "trend_follow".to_string(),
            name: Some("趋势跟踪".to_string()),
            kind: "trend".to_string(),
            config_path: "config/trend_following.yml".to_string(),
            enabled: Some(true),
        },
        StrategySource {
            id: "mean_reversion".to_string(),
            name: Some("均值回归".to_string()),
            kind: "mean_reversion".to_string(),
            config_path: "config/mean_reversion.yml".to_string(),
            enabled: Some(true),
        },
    ]
}

fn expand_sources_to_targets(
    sources: &[StrategySource],
    workspace_root: &Path,
) -> Vec<StrategyTarget> {
    let mut targets = Vec::new();

    for source in sources {
        if source.enabled == Some(false) {
            continue;
        }

        let abs_path = workspace_root.join(&source.config_path);
        let text = match std::fs::read_to_string(&abs_path) {
            Ok(t) => t,
            Err(err) => {
                log::warn!("跳过监控源 {}: 读取配置失败: {}", source.id, err);
                continue;
            }
        };

        let value: Value = match serde_yaml::from_str(&text) {
            Ok(v) => v,
            Err(err) => {
                log::warn!("跳过监控源 {}: 解析 YAML 失败: {}", source.id, err);
                continue;
            }
        };

        let normalized_kind = source.kind.trim().to_ascii_lowercase();
        let mut generated = match normalized_kind.as_str() {
            "hedged_grid" => parse_hedged_grid_source(source, &value),
            "solusdc_hedged_grid" => parse_solusdc_hedged_grid_source(source, &value),
            "trend" | "trend_follow" | "trend_following" | "trend_intraday" => {
                parse_trend_source(source, &value)
            }
            "mean_reversion" => parse_mean_reversion_source(source, &value),
            other => {
                log::warn!("未知监控源类型 {} (id={})", other, source.id);
                Vec::new()
            }
        };

        for target in &mut generated {
            target.source_config = source.config_path.clone();
        }

        targets.extend(generated);
    }

    targets
}

fn parse_trend_source(source: &StrategySource, value: &Value) -> Vec<StrategyTarget> {
    let account_id = value
        .get("account_id")
        .and_then(|v| v.as_str())
        .unwrap_or("binance_main")
        .to_string();

    let symbols_query = value
        .get("symbols")
        .and_then(|v| v.as_sequence())
        .map(|seq| {
            seq.iter()
                .filter_map(|v| v.as_str())
                .map(normalize_query_symbol)
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default();
    let symbols_norm = symbols_query
        .iter()
        .map(|s| normalize_symbol(s))
        .collect::<HashSet<_>>();

    vec![StrategyTarget {
        id: source.id.clone(),
        name: source
            .name
            .clone()
            .unwrap_or_else(|| "趋势跟踪".to_string()),
        kind: "trend".to_string(),
        account_id,
        market_type: MarketType::Futures,
        symbols_query,
        symbols_norm,
        grid_symbol_configs: HashMap::new(),
        source_config: source.config_path.clone(),
    }]
}

fn parse_mean_reversion_source(source: &StrategySource, value: &Value) -> Vec<StrategyTarget> {
    let account_id = value
        .get("account")
        .and_then(|v| v.get("account_id"))
        .and_then(|v| v.as_str())
        .unwrap_or("binance_main")
        .to_string();

    let market_type = parse_market_type(
        value
            .get("account")
            .and_then(|v| v.get("market_type"))
            .and_then(|v| v.as_str()),
    );

    let symbols_query = value
        .get("symbols")
        .and_then(|v| v.as_sequence())
        .map(|seq| {
            seq.iter()
                .filter_map(|item| {
                    if item.is_string() {
                        item.as_str().map(normalize_query_symbol)
                    } else {
                        let enabled = item
                            .get("enabled")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(true);
                        if !enabled {
                            return None;
                        }
                        item.get("symbol")
                            .and_then(|v| v.as_str())
                            .map(normalize_query_symbol)
                    }
                })
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default();
    let symbols_norm = symbols_query
        .iter()
        .map(|s| normalize_symbol(s))
        .collect::<HashSet<_>>();

    vec![StrategyTarget {
        id: source.id.clone(),
        name: source
            .name
            .clone()
            .unwrap_or_else(|| "均值回归".to_string()),
        kind: "mean_reversion".to_string(),
        account_id,
        market_type,
        symbols_query,
        symbols_norm,
        grid_symbol_configs: HashMap::new(),
        source_config: source.config_path.clone(),
    }]
}

fn parse_hedged_grid_source(source: &StrategySource, value: &Value) -> Vec<StrategyTarget> {
    let default_account = value
        .get("base_account")
        .and_then(|v| v.get("account_id"))
        .and_then(|v| v.as_str())
        .unwrap_or("binance_main")
        .to_string();

    let market_type = parse_market_type(
        value
            .get("base_account")
            .and_then(|v| v.get("market_type"))
            .and_then(|v| v.as_str())
            .or_else(|| {
                value
                    .get("strategy")
                    .and_then(|v| v.get("market_type"))
                    .and_then(|v| v.as_str())
            }),
    );

    let mut account_symbols_query: HashMap<String, HashSet<String>> = HashMap::new();
    let mut account_grid_configs: HashMap<String, HashMap<String, GridSymbolConfig>> =
        HashMap::new();

    if let Some(symbols) = value.get("symbols").and_then(|v| v.as_sequence()) {
        for item in symbols {
            let enabled = item
                .get("enabled")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            if !enabled {
                continue;
            }
            let Some(symbol) = item.get("symbol").and_then(|v| v.as_str()) else {
                continue;
            };
            let account_id = item
                .get("account_id")
                .and_then(|v| v.as_str())
                .unwrap_or(&default_account)
                .to_string();
            let symbol_query = normalize_query_symbol(symbol);
            let symbol_norm = normalize_symbol(&symbol_query);
            let grid_cfg = parse_grid_symbol_config(item.get("grid"), symbol_query.as_str());
            account_symbols_query
                .entry(account_id.clone())
                .or_default()
                .insert(symbol_query);
            account_grid_configs
                .entry(account_id)
                .or_default()
                .insert(symbol_norm, grid_cfg);
        }
    }

    let mut targets = Vec::new();
    for (account_id, symbols_query) in account_symbols_query {
        let id = if symbols_query.is_empty() {
            source.id.clone()
        } else {
            format!("{}@{}", source.id, account_id)
        };
        let symbols_norm = symbols_query
            .iter()
            .map(|s| normalize_symbol(s))
            .collect::<HashSet<_>>();
        let grid_symbol_configs = account_grid_configs.remove(&account_id).unwrap_or_default();

        targets.push(StrategyTarget {
            id,
            name: source
                .name
                .clone()
                .unwrap_or_else(|| "对冲网格".to_string()),
            kind: "hedged_grid".to_string(),
            account_id,
            market_type,
            symbols_query,
            symbols_norm,
            grid_symbol_configs,
            source_config: source.config_path.clone(),
        });
    }

    targets
}

fn parse_solusdc_hedged_grid_source(source: &StrategySource, value: &Value) -> Vec<StrategyTarget> {
    let account_id = value
        .get("account")
        .and_then(|v| v.get("account_id"))
        .and_then(|v| v.as_str())
        .unwrap_or("binance_main")
        .to_string();

    let market_type = parse_market_type(
        value
            .get("account")
            .and_then(|v| v.get("market_type"))
            .and_then(|v| v.as_str()),
    );

    let symbol = value
        .get("engine")
        .and_then(|v| v.get("symbol"))
        .and_then(|v| v.as_str())
        .unwrap_or("XRP/USDC");

    let mut symbols_query = HashSet::new();
    symbols_query.insert(normalize_query_symbol(symbol));
    let symbols_norm = symbols_query
        .iter()
        .map(|s| normalize_symbol(s))
        .collect::<HashSet<_>>();
    let mut grid_symbol_configs = HashMap::new();
    if let Some(symbol_query) = symbols_query.iter().next() {
        grid_symbol_configs.insert(
            normalize_symbol(symbol_query),
            parse_grid_symbol_config(
                value.get("engine").and_then(|v| v.get("grid")),
                symbol_query.as_str(),
            ),
        );
    }

    vec![StrategyTarget {
        id: source.id.clone(),
        name: source
            .name
            .clone()
            .unwrap_or_else(|| "对冲网格".to_string()),
        kind: "solusdc_hedged_grid".to_string(),
        account_id,
        market_type,
        symbols_query,
        symbols_norm,
        grid_symbol_configs,
        source_config: source.config_path.clone(),
    }]
}

async fn build_account_manager(accounts_path: &Path) -> Result<Arc<AccountManager>> {
    let config = Config {
        name: "Binance".to_string(),
        testnet: false,
        spot_base_url: "https://api.binance.com".to_string(),
        futures_base_url: "https://fapi.binance.com".to_string(),
        ws_spot_url: "wss://stream.binance.com:9443/ws".to_string(),
        ws_futures_url: "wss://fstream.binance.com/ws".to_string(),
    };

    let mut account_manager = AccountManager::new(config);

    let text = std::fs::read_to_string(accounts_path)
        .with_context(|| format!("读取账户配置失败: {}", accounts_path.display()))?;
    let file: AccountsFile = serde_yaml::from_str(&text)
        .with_context(|| format!("解析账户配置失败: {}", accounts_path.display()))?;

    for (account_id, item) in file.accounts {
        if item.enabled == Some(false) {
            continue;
        }

        let cfg = AccountConfig {
            id: account_id.clone(),
            exchange: item.exchange.clone(),
            api_key_env: item.env_prefix.clone(),
            enabled: true,
            max_positions: item
                .settings
                .as_ref()
                .and_then(|s| s.max_positions)
                .unwrap_or(10),
            max_orders_per_symbol: item
                .settings
                .as_ref()
                .and_then(|s| s.max_orders_per_symbol)
                .unwrap_or(20),
        };

        if let Err(err) = account_manager.add_account(cfg).await {
            log::warn!(
                "加载账户失败 id={} exchange={} err={}，该账户将被忽略",
                account_id,
                item.exchange,
                err
            );
        } else {
            log::info!("监控账户已加载: {} ({})", account_id, item.exchange);
        }
    }

    Ok(Arc::new(account_manager))
}

async fn index() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

async fn healthz() -> &'static str {
    "ok"
}

async fn api_monitor(State(state): State<AppState>) -> Json<DashboardSnapshot> {
    Json(state.snapshot.read().await.clone())
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();

    let cwd = std::env::current_dir()?;
    let workspace_root = detect_workspace_root();

    // 优先加载工作区根目录下的 .env，保证在任意目录启动都能读取到账号配置。
    let workspace_env = workspace_root.join(".env");
    if workspace_env.exists() {
        let _ = dotenv::from_path(&workspace_env);
    } else {
        dotenv::dotenv().ok();
    }

    let host = std::env::var("MONITOR_HOST").unwrap_or(args.host);
    let port = std::env::var("MONITOR_PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(args.port);

    if workspace_root != cwd {
        log::info!(
            "当前目录 {}，自动切换配置根目录到 {}",
            cwd.display(),
            workspace_root.display()
        );
    }

    let monitoring_path = resolve_config_path(&workspace_root, &args.monitoring);
    let (refresh_override, trades_refresh_override, sources) =
        load_monitoring_sources(&monitoring_path)?;

    let refresh_secs = refresh_override.unwrap_or(args.refresh_secs).max(1);
    let trades_refresh_secs = trades_refresh_override
        .unwrap_or(args.trades_refresh_secs)
        .max(5);

    let targets = expand_sources_to_targets(&sources, &workspace_root);
    if targets.is_empty() {
        log::warn!("未解析到任何监控目标，请检查 {}", monitoring_path.display());
    } else {
        log::info!("监控目标数量: {}", targets.len());
        for t in &targets {
            log::info!(
                "  - {} [{}] account={} market={:?} symbols={} config={}",
                t.id,
                t.kind,
                t.account_id,
                t.market_type,
                t.symbols_query.len(),
                t.source_config
            );
        }
    }

    let accounts_path = resolve_config_path(&workspace_root, &args.accounts);
    let account_manager = build_account_manager(&accounts_path).await?;

    let state = AppState {
        snapshot: Arc::new(RwLock::new(DashboardSnapshot::default())),
    };

    let mut engine = MonitorEngine::new(account_manager, targets, trades_refresh_secs);

    {
        let snapshot_ref = state.snapshot.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(refresh_secs));
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                let data = engine.refresh().await;
                *snapshot_ref.write().await = data;
                interval.tick().await;
            }
        });
    }

    let app = Router::new()
        .route("/", get(index))
        .route("/healthz", get(healthz))
        .route("/api/monitor", get(api_monitor))
        .with_state(state)
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        );

    let addr: SocketAddr = format!("{}:{}", host, port)
        .parse()
        .with_context(|| "host/port 解析失败")?;

    log::info!("监控页面启动成功: http://{}", addr);
    log::info!("外网访问请放通端口 {} 并绑定到 0.0.0.0", port);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

const DASHBOARD_HTML: &str = r#"<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>RustCTA 实时监控</title>
  <style>
    :root {
      --bg: #09111a;
      --panel: #111c2a;
      --panel-2: #162536;
      --text: #e9f1ff;
      --sub: #9db0cc;
      --pos: #26c281;
      --neg: #ff5d5d;
      --warn: #ffb347;
      --line: #23374f;
      --accent: #35a7ff;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "JetBrains Mono", "SFMono-Regular", Menlo, Consolas, monospace;
      background: radial-gradient(1200px 700px at 10% -10%, #17304a, var(--bg));
      color: var(--text);
      padding: 20px;
    }
    .wrap { max-width: 1500px; margin: 0 auto; }
    h1 { margin: 0 0 6px; font-size: 26px; }
    .sub { color: var(--sub); margin-bottom: 16px; }
    .grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 16px;
    }
    .card {
      background: linear-gradient(160deg, var(--panel), var(--panel-2));
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 12px 14px;
    }
    .k { color: var(--sub); font-size: 12px; }
    .v { font-size: 22px; margin-top: 6px; font-weight: 700; }
    .small { font-size: 12px; color: var(--sub); }
    .section-title { margin: 14px 0 8px; font-size: 16px; color: #d5e5ff; }
    .strategy {
      border: 1px solid var(--line);
      border-radius: 14px;
      margin-bottom: 12px;
      overflow: hidden;
      background: #0e1927;
    }
    .strategy-hd {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      padding: 10px 12px;
      background: #122133;
      border-bottom: 1px solid var(--line);
      align-items: center;
    }
    .pill {
      border: 1px solid var(--accent);
      color: var(--accent);
      border-radius: 999px;
      padding: 2px 8px;
      font-size: 11px;
    }
    .pill.offline { border-color: var(--neg); color: var(--neg); }
    .kv {
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 8px;
      padding: 10px 12px;
    }
    .kv .item { background: rgba(255,255,255,0.02); border-radius: 10px; padding: 8px; }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 12px;
    }
    th, td {
      padding: 7px 8px;
      border-bottom: 1px solid #1b2e44;
      text-align: right;
      white-space: nowrap;
    }
    th:first-child, td:first-child { text-align: left; }
    .table-wrap { padding: 0 12px 10px; overflow-x: auto; }
    .pos { color: var(--pos); }
    .neg { color: var(--neg); }
    .warns {
      margin: 8px 0;
      padding: 10px 12px;
      border: 1px solid #5a3a14;
      border-radius: 10px;
      background: rgba(255, 179, 71, 0.08);
      color: #ffd39e;
      font-size: 12px;
    }
    .notes {
      margin-top: 10px;
      font-size: 12px;
      color: var(--sub);
      line-height: 1.7;
    }
    @media (max-width: 1200px) {
      .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .kv { grid-template-columns: repeat(3, minmax(0, 1fr)); }
    }
    @media (max-width: 768px) {
      .grid { grid-template-columns: 1fr; }
      .kv { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>RustCTA 实时监控</h1>
    <div class="sub" id="stamp">加载中...</div>

    <div class="grid" id="summary"></div>

    <div id="globalWarn"></div>

    <div class="section-title">策略详情</div>
    <div id="strategies"></div>

    <div class="notes" id="notes"></div>
  </div>

  <script>
    function fmt(v, d = 2) {
      const n = Number(v || 0);
      if (!Number.isFinite(n)) return '-';
      return n.toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });
    }
    function cls(v) { return Number(v) >= 0 ? 'pos' : 'neg'; }

    function renderSummary(s) {
        const cards = [
        ['在线策略', `${s.strategies_online}/${s.strategies_total}`],
        ['账户总权益', fmt(s.total_account_equity)],
        ['总名义仓位', fmt(s.gross_notional)],
        ['净名义仓位', fmt(s.net_notional)],
        ['未实现PNL', `<span class="${cls(s.unrealized_pnl)}">${fmt(s.unrealized_pnl)}</span>`],
        ['估算已实现PNL(7d)', `<span class="${cls(s.realized_pnl_est)}">${fmt(s.realized_pnl_est)}</span>`],
        ['估算总PNL(7d+未实现)', `<span class="${cls(s.total_pnl_est)}">${fmt(s.total_pnl_est)}</span>`],
        ['成交额 1h / 24h / 7d', `${fmt(s.volume_1h)} / ${fmt(s.volume_24h)} / ${fmt(s.volume_7d)}`],
      ];

      const html = cards.map(([k, v]) => `
        <div class="card">
          <div class="k">${k}</div>
          <div class="v">${v}</div>
        </div>
      `).join('');
      document.getElementById('summary').innerHTML = html;
    }

    function renderWarnings(list) {
      if (!list || !list.length) {
        document.getElementById('globalWarn').innerHTML = '';
        return;
      }
      document.getElementById('globalWarn').innerHTML = `
        <div class="warns"><b>全局告警</b><br>${list.map(x => `- ${x}`).join('<br>')}</div>
      `;
    }

    function renderStrategies(items) {
      const html = (items || []).map(s => {
        const warnHtml = (s.warnings && s.warnings.length)
          ? `<div class="warns">${s.warnings.map(x => `- ${x}`).join('<br>')}</div>`
          : '';

        const posRows = (s.positions || []).map(p => {
          const sideZh = p.side === 'LONG' ? '多' : (p.side === 'SHORT' ? '空' : p.side);
          const liqDist = p.liq_distance_pct == null ? '-' : `${fmt(p.liq_distance_pct, 2)}%`;
          const liqDistCls = p.liq_distance_pct == null ? '' : cls(p.liq_distance_pct);
          return `
          <tr>
            <td>${p.symbol}</td>
            <td>${sideZh}</td>
            <td>${fmt(p.qty, 6)}</td>
            <td>${fmt(p.entry_price, 6)}</td>
            <td>${fmt(p.mark_price, 6)}</td>
            <td>${fmt(p.notional)}</td>
            <td class="${cls(p.unrealized_pnl)}">${fmt(p.unrealized_pnl)}</td>
            <td>${p.liq_price_est == null ? '-' : fmt(p.liq_price_est, 6)}</td>
            <td class="${liqDistCls}">${liqDist}</td>
          </tr>
        `;
        }).join('') || '<tr><td colspan="9" style="text-align:center;color:#8ea5c4">当前无持仓</td></tr>';

        const statusCls = s.status === 'OFFLINE' ? 'offline' : '';

        return `
          <div class="strategy">
            <div class="strategy-hd">
              <div>
                <b>${s.name}</b>
                <span class="small">[${s.kind}] 账户: ${s.account_id} / ${s.market_type}</span>
              </div>
              <span class="pill ${statusCls}">${s.status}</span>
            </div>
            <div class="kv">
              <div class="item"><div class="k">监控交易对</div><div>${s.symbols_count}</div></div>
              <div class="item"><div class="k">持仓数</div><div>${s.open_positions}</div></div>
              <div class="item"><div class="k">未实现PNL</div><div class="${cls(s.unrealized_pnl)}">${fmt(s.unrealized_pnl)}</div></div>
              <div class="item"><div class="k">估算已实现PNL(7d)</div><div class="${cls(s.realized_pnl_est)}">${fmt(s.realized_pnl_est)}</div></div>
              <div class="item"><div class="k">估算总PNL(7d+未实现)</div><div class="${cls(s.total_pnl_est)}">${fmt(s.total_pnl_est)}</div></div>
              <div class="item"><div class="k">账户PNL(今日)</div><div class="${cls(s.account_pnl_today)}">${fmt(s.account_pnl_today)}</div></div>
              <div class="item"><div class="k">账户PNL(运行期)</div><div class="${cls(s.account_pnl_since_start)}">${fmt(s.account_pnl_since_start)}</div></div>
              <div class="item"><div class="k">成交额 1h/24h/7d</div><div>${fmt(s.volume_1h)} / ${fmt(s.volume_24h)} / ${fmt(s.volume_7d)}</div></div>
              <div class="item"><div class="k">近1h成交笔数</div><div>${s.recent_trade_count}</div></div>
              <div class="item"><div class="k">最后成交时间</div><div>${s.last_trade_at || '-'}</div></div>
            </div>
            ${warnHtml}
            <div class="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>交易对</th><th>持仓方向</th><th>数量</th><th>开仓均价</th><th>标记价格</th><th>名义价值</th><th>未实现盈亏</th><th>爆仓价(估)</th><th>距离(%)</th>
                  </tr>
                </thead>
                <tbody>${posRows}</tbody>
              </table>
            </div>
          </div>
        `;
      }).join('');
      document.getElementById('strategies').innerHTML = html;
    }

    function renderNotes(notes) {
      document.getElementById('notes').innerHTML = (notes || []).map(x => `- ${x}`).join('<br>');
    }

    async function pull() {
      try {
        const resp = await fetch('/api/monitor', { cache: 'no-store' });
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const data = await resp.json();
        document.getElementById('stamp').textContent = `更新时间: ${data.generated_at}`;
        renderSummary(data.summary || {});
        renderWarnings(data.warnings || []);
        renderStrategies(data.strategies || []);
        renderNotes(data.notes || []);
      } catch (err) {
        document.getElementById('stamp').textContent = `拉取失败: ${err}`;
      }
    }

    pull();
    setInterval(pull, 2000);
  </script>
</body>
</html>
"#;
