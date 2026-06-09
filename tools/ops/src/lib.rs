use std::path::Path;

pub mod account_position_render;
pub mod gateio_bitget_spot_symbols;
pub mod hyperliquid_self_test;
pub mod private_ws_observe;
pub mod private_ws_probe;
pub mod safety;
pub mod ws_config_probe;
pub mod ws_proxy_probe;

pub use account_position_render::{run_account_position_render, AccountPositionRenderArgs};
pub use gateio_bitget_spot_symbols::{run_gateio_bitget_spot_symbols, GateioBitgetSpotSymbolsArgs};
pub use hyperliquid_self_test::{hyperliquid_self_test_safety_plan, HyperliquidSelfTestPlanArgs};
pub use private_ws_probe::{run_private_ws_probe, PrivateWsProbeArgs, PrivateWsProbeReport};
pub use rustcta_reporting::{run_trend_report, TrendReportArgs};
pub use rustcta_smart_money::{
    BinanceCollectorConfig as SmartMoneyBinanceCollectorConfig,
    HyperliquidWalletIngestionConfig as SmartMoneyHyperliquidWalletIngestionConfig,
    PortfolioConstraintsConfig as SmartMoneyPortfolioConstraintsConfig, SmartMoneyPortfolioConfig,
    SmartMoneyServiceConfig as SmartMoneyOpsConfig,
    TrackedWalletConfig as SmartMoneyTrackedWalletConfig,
};
pub use safety::{
    bitget_perp_order_canary_safety_plan, bitget_spot_order_canary_safety_plan,
    cross_arb_account_audit_safety_plan, cross_arb_fee_audit_safety_plan,
    cross_arb_order_admin_safety_plan, cross_arb_ws_opportunity_probe_safety_plan,
    exchange_order_canary_safety_plan, funding_arb_observe_safety_plan, AdminAction,
    AdminPositionSide, BitgetPerpOrderCanarySafetyArgs, BitgetSpotOrderCanarySafetyArgs,
    CanaryMode, CanarySide, CrossArbAccountAuditSafetyArgs, CrossArbFeeAuditSafetyArgs,
    CrossArbOrderAdminSafetyArgs, CrossArbWsOpportunityProbeSafetyArgs,
    ExchangeOrderCanarySafetyArgs, FundingArbObserveSafetyArgs, SafetyGateMode, SafetyPlan,
    SpotSide,
};
pub use ws_config_probe::{run_ws_config_probe, WsConfigProbeArgs, WsConfigProbeReport};
pub use ws_proxy_probe::{run_ws_proxy_probe, WsProxyProbeArgs};

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
            config
                .portfolio
                .constraints
                .initial_capital_usdt
                .is_sign_positive(),
            "portfolio defaults or config values should be present"
        );
    }

    #[test]
    fn smart_money_summaries_should_remain_bounded_dry_run_reports() {
        let config_path = "../../config/smart_money.yml";

        let binance =
            smart_money_binance_collector_summary(config_path).expect("binance collector summary");
        assert_eq!(binance.len(), 2);
        assert!(binance[0].starts_with("smart_money_binance_collector dry-run:"));
        assert_eq!(binance[1], "no Binance network connections were opened");

        let hyperliquid = smart_money_hyperliquid_wallet_ingestion_summary(config_path)
            .expect("hyperliquid wallet summary");
        assert_eq!(hyperliquid.len(), 2);
        assert!(hyperliquid[0].starts_with("smart_money_hyperliquid_wallet_ingestion dry-run:"));
        assert_eq!(
            hyperliquid[1],
            "no Hyperliquid network connections were opened"
        );

        let portfolio =
            smart_money_portfolio_service_summary(config_path).expect("portfolio summary");
        assert_eq!(portfolio.len(), 3);
        assert!(portfolio[0].starts_with("smart_money_portfolio_service dry-run:"));
        assert!(portfolio[1].starts_with("portfolio constraints:"));
        assert_eq!(
            portfolio[2],
            "no portfolio orders or network connections were created"
        );
    }
}
