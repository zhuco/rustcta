use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Args;
use rustcta_reporting::{
    build_account_position_markdown_report, build_account_position_report,
    AccountPositionReportInput, AccountPositionReporterConfig,
};

#[derive(Debug, Clone, Args)]
pub struct AccountPositionRenderArgs {
    #[arg(long, default_value = "config/account_position_reporter.yml")]
    pub config: String,
    #[arg(long)]
    pub input: PathBuf,
}

pub fn run_account_position_render(args: AccountPositionRenderArgs) -> Result<String> {
    let config = AccountPositionReporterConfig::from_file(&args.config)?;
    let input = load_account_position_report_input(&args.input)?;
    Ok(render_account_position_report(&config, input))
}

pub fn render_account_position_report(
    config: &AccountPositionReporterConfig,
    input: AccountPositionReportInput,
) -> String {
    let report = build_account_position_report(input);
    build_account_position_markdown_report(config, &report)
}

fn load_account_position_report_input(path: &Path) -> Result<AccountPositionReportInput> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_str(&raw).with_context(|| format!("failed to parse {}", path.display()))
}

#[cfg(test)]
mod tests {
    use rustcta_reporting::{AccountPositionInput, AccountPositionReportInput};

    use super::*;

    #[test]
    fn account_position_render_should_build_markdown_without_live_provider() {
        let config = AccountPositionReporterConfig {
            account_id: "ops-render".to_string(),
            exchange: "fixture".to_string(),
            env_prefix: "FIXTURE".to_string(),
            market_type: "futures".to_string(),
            interval_secs: 600,
            webhook_url: "https://example.invalid/webhook".to_string(),
            report_title: "Render Only Report".to_string(),
            send_on_start: false,
            min_position_value_usdt: 1.0,
            stablecoins: vec!["USDT".to_string()],
        };
        let input = AccountPositionReportInput {
            account_id: "ops-render".to_string(),
            total_equity_usdt: 1500.0,
            usdt_balance_value: 800.0,
            min_position_value_usdt: 1.0,
            positions: vec![AccountPositionInput {
                symbol: "BTCUSDT".to_string(),
                side: "LONG".to_string(),
                contracts: 0.0,
                contract_size: 1.0,
                mark_price: 50_000.0,
                unrealized_pnl: 12.5,
                size: 0.0,
                amount: 0.01,
            }],
        };

        let markdown = render_account_position_report(&config, input);

        assert!(markdown.contains("## Render Only Report"));
        assert!(markdown.contains("`BTCUSDT` 总持仓 `500.00U`"));
        assert!(markdown.contains("未实现PNL"));
    }
}
