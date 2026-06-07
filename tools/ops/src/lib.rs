use serde::{Deserialize, Serialize};
use std::path::Path;
use std::str::FromStr;

pub mod account_position_render;
pub mod gateio_bitget_spot_symbols;
pub mod ws_proxy_probe;

pub use account_position_render::{run_account_position_render, AccountPositionRenderArgs};
pub use gateio_bitget_spot_symbols::{run_gateio_bitget_spot_symbols, GateioBitgetSpotSymbolsArgs};
pub use rustcta_reporting::{run_trend_report, TrendReportArgs};
pub use ws_proxy_probe::{run_ws_proxy_probe, WsProxyProbeArgs};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyBinTarget {
    AppControlApi,
    AppCli,
    AppSupervisor,
    AppBacktest,
    ToolOps,
    StrategyRuntime,
    LegacyCompat,
}

impl LegacyBinTarget {
    pub const ALL: [Self; 7] = [
        Self::AppControlApi,
        Self::AppCli,
        Self::AppSupervisor,
        Self::AppBacktest,
        Self::ToolOps,
        Self::StrategyRuntime,
        Self::LegacyCompat,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::AppControlApi => "app_control_api",
            Self::AppCli => "app_cli",
            Self::AppSupervisor => "app_supervisor",
            Self::AppBacktest => "app_backtest",
            Self::ToolOps => "tool_ops",
            Self::StrategyRuntime => "strategy_runtime",
            Self::LegacyCompat => "legacy_compat",
        }
    }
}

impl FromStr for LegacyBinTarget {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let normalized = value.trim().replace('-', "_").to_ascii_lowercase();
        match normalized.as_str() {
            "app_control_api" | "control_api" | "control-api" => Ok(Self::AppControlApi),
            "app_cli" | "cli" => Ok(Self::AppCli),
            "app_supervisor" | "supervisor" => Ok(Self::AppSupervisor),
            "app_backtest" | "tool_backtest" | "backtest" => Ok(Self::AppBacktest),
            "tool_ops" | "tools_ops" | "tools" | "ops" => Ok(Self::ToolOps),
            "strategy_runtime" | "strategy" | "runtime" => Ok(Self::StrategyRuntime),
            "legacy_compat" | "compat" | "legacy" => Ok(Self::LegacyCompat),
            _ => Err(format!("unknown legacy bin target: {value}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyBinMigration {
    pub source: &'static str,
    pub target: LegacyBinTarget,
    pub rationale: &'static str,
}

pub const LEGACY_BIN_MIGRATIONS: &[LegacyBinMigration] = &[
    LegacyBinMigration {
        source: "account_position_reporter.rs",
        target: LegacyBinTarget::ToolOps,
        rationale:
            "operator account reporting tool; extract non-root helpers before tools/ops ownership",
    },
    LegacyBinMigration {
        source: "backtest.rs",
        target: LegacyBinTarget::AppBacktest,
        rationale: "offline backtest/research app executable; separate from production services",
    },
    LegacyBinMigration {
        source: "bitget_order_canary.rs",
        target: LegacyBinTarget::ToolOps,
        rationale: "temporary live-exchange canary; migrate as documented ops tool",
    },
    LegacyBinMigration {
        source: "bitget_spot_order_canary.rs",
        target: LegacyBinTarget::ToolOps,
        rationale: "temporary live-exchange canary; migrate as documented ops tool",
    },
    LegacyBinMigration {
        source: "control_api.rs",
        target: LegacyBinTarget::AppControlApi,
        rationale:
            "HTTP control service belongs in apps/control-api with logic in rustcta-control-api",
    },
    LegacyBinMigration {
        source: "cross_arb_account_audit.rs",
        target: LegacyBinTarget::ToolOps,
        rationale: "operator audit command, not a strategy runtime",
    },
    LegacyBinMigration {
        source: "cross_arb_fee_audit.rs",
        target: LegacyBinTarget::ToolOps,
        rationale: "operator audit command, not a strategy runtime",
    },
    LegacyBinMigration {
        source: "cross_arb_live.rs",
        target: LegacyBinTarget::StrategyRuntime,
        rationale: "strategy process entrypoint should move with strategy crate/runtime wrapper",
    },
    LegacyBinMigration {
        source: "cross_arb_observe.rs",
        target: LegacyBinTarget::StrategyRuntime,
        rationale: "strategy observation runtime should move with strategy crate/runtime wrapper",
    },
    LegacyBinMigration {
        source: "cross_arb_order_admin.rs",
        target: LegacyBinTarget::ToolOps,
        rationale:
            "operator order administration tool; keep temporary live mutation status explicit",
    },
    LegacyBinMigration {
        source: "cross_arb_preflight.rs",
        target: LegacyBinTarget::AppCli,
        rationale: "preflight command belongs under industrial CLI composition",
    },
    LegacyBinMigration {
        source: "exchange_order_canary.rs",
        target: LegacyBinTarget::ToolOps,
        rationale: "temporary live-exchange canary; migrate as documented ops tool",
    },
    LegacyBinMigration {
        source: "funding_arb_live.rs",
        target: LegacyBinTarget::StrategyRuntime,
        rationale: "strategy process entrypoint should move with strategy crate/runtime wrapper",
    },
    LegacyBinMigration {
        source: "funding_arb_observe.rs",
        target: LegacyBinTarget::StrategyRuntime,
        rationale: "strategy observation runtime should move with strategy crate/runtime wrapper",
    },
    LegacyBinMigration {
        source: "gateio_bitget_spot_symbols.rs",
        target: LegacyBinTarget::ToolOps,
        rationale: "symbol discovery/reporting tool, not a platform service",
    },
    LegacyBinMigration {
        source: "hyperliquid_self_test.rs",
        target: LegacyBinTarget::ToolOps,
        rationale: "operator connectivity/self-test tool",
    },
    LegacyBinMigration {
        source: "short_ladder_mtf_grid.rs",
        target: LegacyBinTarget::AppBacktest,
        rationale: "offline short-ladder grid research command belongs under the backtest app",
    },
    LegacyBinMigration {
        source: "smart_money_binance_collector.rs",
        target: LegacyBinTarget::ToolOps,
        rationale: "collector utility needs explicit ops ownership before service promotion",
    },
    LegacyBinMigration {
        source: "smart_money_hyperliquid_wallet_ingestion.rs",
        target: LegacyBinTarget::ToolOps,
        rationale: "data ingestion tool, not a production service boundary yet",
    },
    LegacyBinMigration {
        source: "smart_money_portfolio_service.rs",
        target: LegacyBinTarget::ToolOps,
        rationale: "current behavior is dry-run portfolio config reporting, not a daemon",
    },
    LegacyBinMigration {
        source: "trend_report.rs",
        target: LegacyBinTarget::ToolOps,
        rationale: "operator trend reporter backed by rustcta-reporting",
    },
    LegacyBinMigration {
        source: "ws_proxy_probe.rs",
        target: LegacyBinTarget::ToolOps,
        rationale: "connectivity probe tool",
    },
];

pub fn migration_for(source: &str) -> Option<&'static LegacyBinMigration> {
    LEGACY_BIN_MIGRATIONS
        .iter()
        .find(|migration| migration.source == source)
}

pub fn migrations_by_target(target: LegacyBinTarget) -> Vec<&'static LegacyBinMigration> {
    LEGACY_BIN_MIGRATIONS
        .iter()
        .filter(|migration| migration.target == target)
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyBinVerification {
    pub scanned_dir: String,
    pub discovered_bins: Vec<String>,
    pub classified_bins: Vec<String>,
    pub unclassified_bins: Vec<String>,
    pub stale_migrations: Vec<String>,
}

impl LegacyBinVerification {
    pub fn is_clean(&self) -> bool {
        self.unclassified_bins.is_empty() && self.stale_migrations.is_empty()
    }
}

pub fn verify_legacy_bin_migrations(
    src_bin_dir: impl AsRef<Path>,
) -> std::io::Result<LegacyBinVerification> {
    let src_bin_dir = src_bin_dir.as_ref();
    let mut discovered_bins = std::fs::read_dir(src_bin_dir)?
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "rs")
        })
        .map(|entry| entry.file_name().to_string_lossy().to_string())
        .collect::<Vec<_>>();
    discovered_bins.sort();

    let mut classified_bins = discovered_bins
        .iter()
        .filter(|source| migration_for(source).is_some())
        .cloned()
        .collect::<Vec<_>>();
    classified_bins.sort();

    let mut unclassified_bins = discovered_bins
        .iter()
        .filter(|source| migration_for(source).is_none())
        .cloned()
        .collect::<Vec<_>>();
    unclassified_bins.sort();

    let mut stale_migrations = LEGACY_BIN_MIGRATIONS
        .iter()
        .filter(|migration| {
            !discovered_bins
                .iter()
                .any(|source| source == migration.source)
        })
        .map(|migration| migration.source.to_string())
        .collect::<Vec<_>>();
    stale_migrations.sort();

    Ok(LegacyBinVerification {
        scanned_dir: src_bin_dir.display().to_string(),
        discovered_bins,
        classified_bins,
        unclassified_bins,
        stale_migrations,
    })
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SmartMoneyOpsConfig {
    pub binance_collector: SmartMoneyBinanceCollectorConfig,
    pub hyperliquid_wallet_ingestion: SmartMoneyHyperliquidWalletIngestionConfig,
    pub portfolio: SmartMoneyPortfolioConfig,
}

impl SmartMoneyOpsConfig {
    pub fn load_yaml(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path)
            .map_err(|err| anyhow::anyhow!("failed to read config {}: {err}", path.display()))?;
        serde_yaml::from_str(&raw)
            .map_err(|err| anyhow::anyhow!("failed to parse config {}: {err}", path.display()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct SmartMoneyBinanceCollectorConfig {
    pub enabled: bool,
    pub symbols: Vec<String>,
    pub intervals: Vec<String>,
    pub collect_trades: bool,
    pub collect_orderbook: bool,
    pub orderbook_depth: u16,
    pub poll_interval_secs: u64,
}

impl Default for SmartMoneyBinanceCollectorConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            symbols: Vec::new(),
            intervals: vec!["1m".to_string()],
            collect_trades: true,
            collect_orderbook: true,
            orderbook_depth: 20,
            poll_interval_secs: 5,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct SmartMoneyHyperliquidWalletIngestionConfig {
    pub enabled: bool,
    pub wallets: Vec<SmartMoneyTrackedWalletConfig>,
    pub ingest_positions: bool,
    pub ingest_fills: bool,
    pub poll_interval_secs: u64,
    pub lookback_days: u32,
}

impl Default for SmartMoneyHyperliquidWalletIngestionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            wallets: Vec::new(),
            ingest_positions: true,
            ingest_fills: true,
            poll_interval_secs: 30,
            lookback_days: 30,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct SmartMoneyTrackedWalletConfig {
    pub enabled: bool,
}

impl Default for SmartMoneyTrackedWalletConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SmartMoneyPortfolioConfig {
    pub enabled: bool,
    pub constraints: SmartMoneyPortfolioConstraintsConfig,
    pub symbol_overrides: std::collections::BTreeMap<String, SmartMoneyPortfolioConstraintsConfig>,
    pub rebalance_interval_secs: u64,
    pub alpha_stale_after_secs: u64,
    pub close_only: bool,
    pub dry_run: bool,
}

impl Default for SmartMoneyPortfolioConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            constraints: SmartMoneyPortfolioConstraintsConfig::default(),
            symbol_overrides: std::collections::BTreeMap::new(),
            rebalance_interval_secs: 60,
            alpha_stale_after_secs: 120,
            close_only: false,
            dry_run: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct SmartMoneyPortfolioConstraintsConfig {
    pub initial_capital_usdt: String,
    pub standard_entry_notional_usdt: String,
    pub max_leverage: String,
    pub max_gross_notional_usdt: String,
    pub max_single_asset_gross_share: String,
}

impl Default for SmartMoneyPortfolioConstraintsConfig {
    fn default() -> Self {
        Self {
            initial_capital_usdt: "2000".to_string(),
            standard_entry_notional_usdt: "1000".to_string(),
            max_leverage: "10".to_string(),
            max_gross_notional_usdt: "20000".to_string(),
            max_single_asset_gross_share: "0.35".to_string(),
        }
    }
}

pub fn smart_money_binance_collector_summary(
    config_path: impl AsRef<Path>,
) -> anyhow::Result<Vec<String>> {
    let config = SmartMoneyOpsConfig::load_yaml(config_path)?;
    let collector = &config.binance_collector;
    Ok(vec![
        format!(
            "smart_money_binance_collector dry-run: enabled={}, symbols={}, intervals={}, trades={}, orderbook={}, depth={}, poll_interval_secs={}",
            collector.enabled,
            collector.symbols.len(),
            collector.intervals.len(),
            collector.collect_trades,
            collector.collect_orderbook,
            collector.orderbook_depth,
            collector.poll_interval_secs
        ),
        "no Binance network connections were opened".to_string(),
    ])
}

pub fn smart_money_hyperliquid_wallet_ingestion_summary(
    config_path: impl AsRef<Path>,
) -> anyhow::Result<Vec<String>> {
    let config = SmartMoneyOpsConfig::load_yaml(config_path)?;
    let ingestion = &config.hyperliquid_wallet_ingestion;
    let enabled_wallets = ingestion
        .wallets
        .iter()
        .filter(|wallet| wallet.enabled)
        .count();
    Ok(vec![
        format!(
            "smart_money_hyperliquid_wallet_ingestion dry-run: enabled={}, wallets={}, enabled_wallets={}, positions={}, fills={}, lookback_days={}, poll_interval_secs={}",
            ingestion.enabled,
            ingestion.wallets.len(),
            enabled_wallets,
            ingestion.ingest_positions,
            ingestion.ingest_fills,
            ingestion.lookback_days,
            ingestion.poll_interval_secs
        ),
        "no Hyperliquid network connections were opened".to_string(),
    ])
}

pub fn smart_money_portfolio_service_summary(
    config_path: impl AsRef<Path>,
) -> anyhow::Result<Vec<String>> {
    let config = SmartMoneyOpsConfig::load_yaml(config_path)?;
    let portfolio = &config.portfolio;
    let constraints = &portfolio.constraints;
    Ok(vec![
        format!(
            "smart_money_portfolio_service dry-run: enabled={}, dry_run={}, close_only={}, rebalance_interval_secs={}, alpha_stale_after_secs={}",
            portfolio.enabled,
            portfolio.dry_run,
            portfolio.close_only,
            portfolio.rebalance_interval_secs,
            portfolio.alpha_stale_after_secs
        ),
        format!(
            "portfolio constraints: initial_capital_usdt={}, standard_entry_notional_usdt={}, max_leverage={}, max_gross_notional_usdt={}, max_single_asset_gross_share={}, symbol_overrides={}",
            constraints.initial_capital_usdt,
            constraints.standard_entry_notional_usdt,
            constraints.max_leverage,
            constraints.max_gross_notional_usdt,
            constraints.max_single_asset_gross_share,
            portfolio.symbol_overrides.len()
        ),
        "no portfolio orders or network connections were created".to_string(),
    ])
}

pub fn print_lines(lines: &[String]) {
    for line in lines {
        println!("{line}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    #[test]
    fn all_current_direct_legacy_bins_should_have_migration_targets() {
        let verification =
            verify_legacy_bin_migrations("../../src/bin").expect("verify legacy bins");
        let legacy_bins = verification.discovered_bins;
        assert!(
            !legacy_bins.is_empty(),
            "src/bin should still contain legacy bins during migration"
        );

        assert!(
            verification.unclassified_bins.is_empty(),
            "missing migration targets: {:?}",
            verification.unclassified_bins
        );
        assert!(
            verification.stale_migrations.is_empty(),
            "stale migration targets: {:?}",
            verification.stale_migrations
        );
    }

    #[test]
    fn migration_targets_should_cover_control_cli_tools_and_strategy_runtimes() {
        for target in [
            LegacyBinTarget::AppControlApi,
            LegacyBinTarget::AppCli,
            LegacyBinTarget::AppBacktest,
            LegacyBinTarget::ToolOps,
            LegacyBinTarget::StrategyRuntime,
        ] {
            assert!(
                !migrations_by_target(target).is_empty(),
                "target {target:?} should have at least one migration"
            );
        }
    }

    #[test]
    fn tools_ops_package_should_not_depend_on_legacy_root_crate() {
        let manifest = std::fs::read_to_string("Cargo.toml").expect("read tools/ops manifest");
        for (line_number, line) in manifest.lines().enumerate() {
            let trimmed = line.trim_start();
            let depends_on_root_package = trimmed
                .split_once('=')
                .is_some_and(|(dependency_name, _)| dependency_name.trim() == "rustcta")
                || trimmed.contains("package = \"rustcta\"");
            assert!(
                !depends_on_root_package,
                "tools/ops Cargo.toml must not depend on legacy root rustcta at line {}: {}",
                line_number + 1,
                line
            );
        }

        let legacy_root_path = ["rustcta", "::"].concat();
        for path in rust_source_files(Path::new("src")) {
            let source = std::fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
            assert!(
                !source.contains(&legacy_root_path),
                "{} imports the legacy root crate; extract a non-root helper crate first",
                path.display()
            );
        }
    }

    #[test]
    fn root_dependent_reporters_should_remain_deferred_until_helpers_are_extracted() {
        let migration =
            migration_for("account_position_reporter.rs").expect("reporter migration entry");
        assert_eq!(migration.target, LegacyBinTarget::ToolOps);
        assert!(
            migration.rationale.contains("extract non-root"),
            "account_position_reporter should document the non-root helper extraction prerequisite"
        );

        let trend = migration_for("trend_report.rs").expect("trend reporter migration entry");
        assert_eq!(trend.target, LegacyBinTarget::ToolOps);
        assert!(
            trend.rationale.contains("rustcta-reporting"),
            "trend_report should document its non-root helper crate"
        );
    }

    #[test]
    fn legacy_bin_target_should_parse_cli_aliases() {
        assert_eq!(
            "tools".parse::<LegacyBinTarget>().unwrap(),
            LegacyBinTarget::ToolOps
        );
        assert_eq!(
            "control-api".parse::<LegacyBinTarget>().unwrap(),
            LegacyBinTarget::AppControlApi
        );
        assert_eq!(
            "strategy".parse::<LegacyBinTarget>().unwrap(),
            LegacyBinTarget::StrategyRuntime
        );
        assert_eq!(
            "tool-backtest".parse::<LegacyBinTarget>().unwrap(),
            LegacyBinTarget::AppBacktest
        );
    }

    fn rust_source_files(root: &Path) -> Vec<PathBuf> {
        let mut files = Vec::new();
        collect_rust_source_files(root, &mut files);
        files.sort();
        files
    }

    fn collect_rust_source_files(path: &Path, files: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(path)
            .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
        {
            let entry = entry.expect("read dir entry");
            let path = entry.path();
            if path.is_dir() {
                collect_rust_source_files(&path, files);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                files.push(path);
            }
        }
    }

    #[test]
    fn smart_money_ops_config_should_load_partial_views() {
        let config =
            SmartMoneyOpsConfig::load_yaml("../../config/smart_money.yml").expect("load config");
        assert!(!config.binance_collector.symbols.is_empty());
        assert!(
            !config.portfolio.constraints.initial_capital_usdt.is_empty(),
            "portfolio defaults or config values should be present"
        );
    }
}
