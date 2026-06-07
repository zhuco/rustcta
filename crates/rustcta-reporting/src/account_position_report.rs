use std::collections::HashMap;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize)]
pub struct AccountPositionReporterConfig {
    pub account_id: String,
    #[serde(default = "default_exchange")]
    pub exchange: String,
    pub env_prefix: String,
    #[serde(default = "default_market_type")]
    pub market_type: String,
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u64,
    pub webhook_url: String,
    #[serde(default = "default_report_title")]
    pub report_title: String,
    #[serde(default = "default_true")]
    pub send_on_start: bool,
    #[serde(default = "default_min_position_value_usdt")]
    pub min_position_value_usdt: f64,
    #[serde(default = "default_stablecoins")]
    pub stablecoins: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountPositionMarketType {
    Spot,
    Futures,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountBalanceInput {
    pub currency: String,
    pub total: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountPositionInput {
    pub symbol: String,
    pub side: String,
    pub contracts: f64,
    pub contract_size: f64,
    pub mark_price: f64,
    pub unrealized_pnl: f64,
    pub size: f64,
    pub amount: f64,
}

#[async_trait]
pub trait AccountPositionProvider {
    async fn get_balances(
        &self,
        market_type: AccountPositionMarketType,
    ) -> Result<Vec<AccountBalanceInput>>;

    async fn get_positions(&self) -> Result<Vec<AccountPositionInput>>;

    async fn get_last_price(
        &self,
        symbol: &str,
        market_type: AccountPositionMarketType,
    ) -> Result<f64>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountPositionReportInput {
    pub account_id: String,
    pub total_equity_usdt: f64,
    pub usdt_balance_value: f64,
    pub min_position_value_usdt: f64,
    pub positions: Vec<AccountPositionInput>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SymbolExposure {
    pub symbol: String,
    pub long_value_usdt: f64,
    pub short_value_usdt: f64,
    pub long_qty: f64,
    pub short_qty: f64,
    pub unrealized_pnl_usdt: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountPositionReport {
    pub account_id: String,
    pub total_equity_usdt: f64,
    pub long_total_usdt: f64,
    pub short_total_usdt: f64,
    pub net_exposure_usdt: f64,
    pub usdt_balance_value: f64,
    pub total_unrealized_pnl_usdt: f64,
    pub exposures: Vec<SymbolExposure>,
}

#[derive(Debug, Serialize)]
struct WecomMarkdownWebhookMessage {
    msgtype: String,
    markdown: WecomMarkdownWebhookContent,
}

#[derive(Debug, Serialize)]
struct WecomMarkdownWebhookContent {
    content: String,
}

#[derive(Debug, Deserialize)]
struct WecomMarkdownWebhookResponse {
    errcode: i64,
    errmsg: String,
}

fn default_exchange() -> String {
    "binance".to_string()
}

fn default_market_type() -> String {
    "futures".to_string()
}

fn default_interval_secs() -> u64 {
    600
}

fn default_report_title() -> String {
    "自动仓位报告".to_string()
}

fn default_true() -> bool {
    true
}

fn default_min_position_value_usdt() -> f64 {
    1.0
}

fn default_stablecoins() -> Vec<String> {
    vec![
        "USDT".to_string(),
        "USDC".to_string(),
        "BUSD".to_string(),
        "FDUSD".to_string(),
        "TUSD".to_string(),
        "USDS".to_string(),
    ]
}

impl AccountPositionReporterConfig {
    pub fn from_file(path: &str) -> Result<Self> {
        let raw =
            std::fs::read_to_string(path).with_context(|| format!("failed to read {path}"))?;
        let config: Self =
            serde_yaml::from_str(&raw).with_context(|| format!("failed to parse {path}"))?;
        Ok(config)
    }

    pub fn market_type(&self) -> Result<AccountPositionMarketType> {
        match self.market_type.trim().to_ascii_lowercase().as_str() {
            "spot" => Ok(AccountPositionMarketType::Spot),
            "futures" | "future" | "perp" | "perpetual" => Ok(AccountPositionMarketType::Futures),
            other => Err(anyhow!("unsupported market_type: {other}")),
        }
    }

    pub fn stablecoins_uppercase(&self) -> Vec<String> {
        self.stablecoins
            .iter()
            .map(|item| item.trim().to_ascii_uppercase())
            .collect()
    }
}

pub fn build_account_position_report(input: AccountPositionReportInput) -> AccountPositionReport {
    let mut by_symbol: HashMap<String, SymbolExposure> = HashMap::new();
    let mut long_total_usdt = 0.0;
    let mut short_total_usdt = 0.0;
    let mut total_unrealized_pnl_usdt = 0.0;

    for position in input.positions {
        let qty = position_qty(&position);
        if qty <= 0.0 {
            continue;
        }

        let notional_usdt = qty * position.mark_price * normalized_contract_size(&position);
        if notional_usdt < input.min_position_value_usdt {
            continue;
        }

        total_unrealized_pnl_usdt += position.unrealized_pnl;

        let entry = by_symbol
            .entry(position.symbol.clone())
            .or_insert_with(|| SymbolExposure {
                symbol: position.symbol.clone(),
                long_value_usdt: 0.0,
                short_value_usdt: 0.0,
                long_qty: 0.0,
                short_qty: 0.0,
                unrealized_pnl_usdt: 0.0,
            });

        entry.unrealized_pnl_usdt += position.unrealized_pnl;

        if is_long_position(&position) {
            entry.long_value_usdt += notional_usdt;
            entry.long_qty += qty;
            long_total_usdt += notional_usdt;
        } else {
            entry.short_value_usdt += notional_usdt;
            entry.short_qty += qty;
            short_total_usdt += notional_usdt;
        }
    }

    let mut exposures: Vec<SymbolExposure> = by_symbol.into_values().collect();
    exposures.sort_by(|left, right| {
        gross_exposure(right)
            .partial_cmp(&gross_exposure(left))
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    AccountPositionReport {
        account_id: input.account_id,
        total_equity_usdt: input.total_equity_usdt,
        long_total_usdt,
        short_total_usdt,
        net_exposure_usdt: long_total_usdt - short_total_usdt,
        usdt_balance_value: input.usdt_balance_value,
        total_unrealized_pnl_usdt,
        exposures,
    }
}

pub async fn collect_account_position_report<P>(
    config: &AccountPositionReporterConfig,
    provider: &P,
) -> Result<AccountPositionReport>
where
    P: AccountPositionProvider + Sync,
{
    let market_type = config.market_type()?;
    let balances = provider
        .get_balances(market_type)
        .await
        .context("failed to fetch balances")?;
    let positions = provider
        .get_positions()
        .await
        .context("failed to fetch positions")?;

    let stablecoins = config.stablecoins_uppercase();
    let total_equity_usdt =
        estimate_balances_usdt_value(provider, &balances, market_type, &stablecoins).await?;
    let usdt_balance_value = sum_usdt_balance_value(&balances);

    Ok(build_account_position_report(AccountPositionReportInput {
        account_id: config.account_id.clone(),
        total_equity_usdt,
        usdt_balance_value,
        min_position_value_usdt: config.min_position_value_usdt,
        positions,
    }))
}

pub fn build_account_position_markdown_report(
    config: &AccountPositionReporterConfig,
    report: &AccountPositionReport,
) -> String {
    build_account_position_markdown_report_at(config, report, Utc::now())
}

pub fn build_account_position_markdown_report_at(
    config: &AccountPositionReporterConfig,
    report: &AccountPositionReport,
    generated_at: DateTime<Utc>,
) -> String {
    let generated_at = generated_at.format("%Y-%m-%d %H:%M:%S UTC");
    let pnl_color = color_for_value(report.total_unrealized_pnl_usdt);
    let net_color = color_for_value(report.net_exposure_usdt);

    let mut content = String::new();
    content.push_str(&format!("## {}\n\n", config.report_title));
    content.push_str(&format!(
        "> 账户: `{}`\n> 时间: `{}`\n> 总权益: <font color=\"info\">{:.2} USDT</font>\n> 多头合计: `{:.2} USDT`\n> 空头合计: `{:.2} USDT`\n> 净敞口: <font color=\"{}\">{:+.2} USDT</font>\n> USDT价值: `{:.2} USDT`\n> 未实现PNL: <font color=\"{}\">{:+.2} USDT</font>\n\n",
        report.account_id,
        generated_at,
        report.total_equity_usdt,
        report.long_total_usdt,
        report.short_total_usdt,
        net_color,
        report.net_exposure_usdt,
        report.usdt_balance_value,
        pnl_color,
        report.total_unrealized_pnl_usdt,
    ));

    if report.exposures.is_empty() {
        content.push_str("### 持仓明细\n\n当前无持仓。\n");
        return content;
    }

    content.push_str("### 持仓明细\n\n");

    for (index, exposure) in report.exposures.iter().enumerate() {
        let net_value = exposure.long_value_usdt - exposure.short_value_usdt;
        let gross_value = exposure.long_value_usdt + exposure.short_value_usdt;
        let net_color = color_for_value(net_value);
        let pnl_color = color_for_value(exposure.unrealized_pnl_usdt);

        content.push_str(&format!(
            "{}. `{}` 总持仓 `{:.2}U` | 多头 `{:.2}U` | 空头 `{:.2}U` | 净敞口 <font color=\"{}\">{:+.2}U</font> | PNL <font color=\"{}\">{:+.2}U</font> | 数量 `多 {:.4} / 空 {:.4}`\n",
            index + 1,
            exposure.symbol,
            gross_value,
            exposure.long_value_usdt,
            exposure.short_value_usdt,
            net_color,
            net_value,
            pnl_color,
            exposure.unrealized_pnl_usdt,
            exposure.long_qty,
            exposure.short_qty,
        ));
    }

    content
}

pub async fn send_account_position_markdown_webhook(
    client: &Client,
    webhook_url: &str,
    content: String,
) -> Result<()> {
    let payload = WecomMarkdownWebhookMessage {
        msgtype: "markdown".to_string(),
        markdown: WecomMarkdownWebhookContent { content },
    };

    let response = client
        .post(webhook_url)
        .json(&payload)
        .send()
        .await
        .context("failed to send webhook request")?;

    let status = response.status();
    let body = response
        .text()
        .await
        .context("failed to read webhook response body")?;

    validate_wecom_markdown_webhook_response(status, &body)
}

fn validate_wecom_markdown_webhook_response(status: StatusCode, body: &str) -> Result<()> {
    if !status.is_success() {
        return Err(anyhow!("webhook http status={} body={}", status, body));
    }

    let parsed: WecomMarkdownWebhookResponse =
        serde_json::from_str(body).context("failed to parse webhook response json")?;
    if parsed.errcode != 0 {
        return Err(anyhow!(
            "webhook returned errcode={} errmsg={}",
            parsed.errcode,
            parsed.errmsg
        ));
    }

    Ok(())
}

pub fn sum_usdt_balance_value(balances: &[AccountBalanceInput]) -> f64 {
    balances
        .iter()
        .filter(|balance| balance.currency.eq_ignore_ascii_case("USDT"))
        .map(|balance| balance.total.max(0.0))
        .sum()
}

pub fn is_stable_valued_asset(asset: &str, stablecoins: &[String]) -> bool {
    stablecoins
        .iter()
        .any(|stablecoin| asset == stablecoin || asset.ends_with(stablecoin))
}

pub async fn estimate_balances_usdt_value<P>(
    provider: &P,
    balances: &[AccountBalanceInput],
    market_type: AccountPositionMarketType,
    stablecoins: &[String],
) -> Result<f64>
where
    P: AccountPositionProvider + Sync,
{
    let mut total_usdt_value = 0.0;
    let mut price_cache: HashMap<String, f64> = HashMap::new();

    for balance in balances {
        if balance.total <= 0.0 {
            continue;
        }

        let asset = balance.currency.trim().to_ascii_uppercase();
        let price = if is_stable_valued_asset(&asset, stablecoins) {
            1.0
        } else if let Some(price) = price_cache.get(&asset) {
            *price
        } else {
            let resolved = resolve_asset_price_usdt(provider, &asset, market_type).await?;
            price_cache.insert(asset.clone(), resolved);
            resolved
        };

        total_usdt_value += balance.total * price;
    }

    Ok(total_usdt_value)
}

pub async fn resolve_asset_price_usdt<P>(
    provider: &P,
    asset: &str,
    market_type: AccountPositionMarketType,
) -> Result<f64>
where
    P: AccountPositionProvider + Sync,
{
    let candidates = [
        (format!("{asset}/USDT"), AccountPositionMarketType::Spot),
        (format!("{asset}/USDT"), market_type),
        (format!("{asset}/USDC"), AccountPositionMarketType::Spot),
        (format!("{asset}/USDC"), market_type),
    ];

    for (symbol, query_market_type) in candidates {
        if let Ok(price) = provider.get_last_price(&symbol, query_market_type).await {
            if price > 0.0 {
                return Ok(price);
            }
        }
    }

    Err(anyhow!("unable to price asset in USDT: {asset}"))
}

fn is_long_position(position: &AccountPositionInput) -> bool {
    let side = position.side.trim().to_ascii_uppercase();
    if side == "LONG" {
        return true;
    }
    if side == "SHORT" {
        return false;
    }
    position.amount >= 0.0
}

fn normalized_contract_size(position: &AccountPositionInput) -> f64 {
    if position.contract_size > 0.0 {
        position.contract_size
    } else {
        1.0
    }
}

fn position_qty(position: &AccountPositionInput) -> f64 {
    if position.amount.abs() > 0.0 {
        position.amount.abs()
    } else if position.contracts > 0.0 {
        position.contracts.abs()
    } else {
        position.size.abs()
    }
}

fn gross_exposure(exposure: &SymbolExposure) -> f64 {
    exposure.long_value_usdt + exposure.short_value_usdt
}

fn color_for_value(value: f64) -> &'static str {
    if value > 0.0 {
        "warning"
    } else if value < 0.0 {
        "info"
    } else {
        "comment"
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[derive(Debug, Clone)]
    struct StaticAccountPositionProvider {
        balances: Vec<AccountBalanceInput>,
        positions: Vec<AccountPositionInput>,
        prices: HashMap<(String, AccountPositionMarketType), f64>,
    }

    #[async_trait]
    impl AccountPositionProvider for StaticAccountPositionProvider {
        async fn get_balances(
            &self,
            _market_type: AccountPositionMarketType,
        ) -> Result<Vec<AccountBalanceInput>> {
            Ok(self.balances.clone())
        }

        async fn get_positions(&self) -> Result<Vec<AccountPositionInput>> {
            Ok(self.positions.clone())
        }

        async fn get_last_price(
            &self,
            symbol: &str,
            market_type: AccountPositionMarketType,
        ) -> Result<f64> {
            self.prices
                .get(&(symbol.to_string(), market_type))
                .copied()
                .ok_or_else(|| anyhow!("missing static price {symbol} {market_type:?}"))
        }
    }

    fn config() -> AccountPositionReporterConfig {
        AccountPositionReporterConfig {
            account_id: "acct-a".to_string(),
            exchange: "binance".to_string(),
            env_prefix: "BINANCE".to_string(),
            market_type: "futures".to_string(),
            interval_secs: 600,
            webhook_url: "https://example.invalid/webhook".to_string(),
            report_title: "测试仓位报告".to_string(),
            send_on_start: true,
            min_position_value_usdt: 1.0,
            stablecoins: default_stablecoins(),
        }
    }

    #[test]
    fn account_position_report_should_aggregate_and_sort_exposures() {
        let report = build_account_position_report(AccountPositionReportInput {
            account_id: "acct-a".to_string(),
            total_equity_usdt: 1_200.0,
            usdt_balance_value: 900.0,
            min_position_value_usdt: 5.0,
            positions: vec![
                AccountPositionInput {
                    symbol: "ETHUSDT".to_string(),
                    side: "LONG".to_string(),
                    contracts: 0.0,
                    contract_size: 1.0,
                    mark_price: 2000.0,
                    unrealized_pnl: 7.5,
                    size: 0.0,
                    amount: 0.2,
                },
                AccountPositionInput {
                    symbol: "BTCUSDT".to_string(),
                    side: "SHORT".to_string(),
                    contracts: 0.0,
                    contract_size: 1.0,
                    mark_price: 50_000.0,
                    unrealized_pnl: -4.0,
                    size: 0.0,
                    amount: -0.01,
                },
                AccountPositionInput {
                    symbol: "DUSTUSDT".to_string(),
                    side: "LONG".to_string(),
                    contracts: 0.0,
                    contract_size: 1.0,
                    mark_price: 1.0,
                    unrealized_pnl: 10.0,
                    size: 0.0,
                    amount: 1.0,
                },
            ],
        });

        assert_eq!(report.long_total_usdt, 400.0);
        assert_eq!(report.short_total_usdt, 500.0);
        assert_eq!(report.net_exposure_usdt, -100.0);
        assert_eq!(report.total_unrealized_pnl_usdt, 3.5);
        assert_eq!(report.exposures.len(), 2);
        assert_eq!(report.exposures[0].symbol, "BTCUSDT");
        assert_eq!(report.exposures[1].symbol, "ETHUSDT");
    }

    #[test]
    fn account_position_markdown_should_render_empty_and_populated_reports() {
        let config = config();
        let generated_at = Utc.with_ymd_and_hms(2026, 6, 7, 1, 2, 3).unwrap();
        let report = build_account_position_report(AccountPositionReportInput {
            account_id: "acct-a".to_string(),
            total_equity_usdt: 100.0,
            usdt_balance_value: 80.0,
            min_position_value_usdt: 1.0,
            positions: vec![AccountPositionInput {
                symbol: "ETHUSDT".to_string(),
                side: "LONG".to_string(),
                contracts: 0.0,
                contract_size: 1.0,
                mark_price: 2000.0,
                unrealized_pnl: 3.0,
                size: 0.0,
                amount: 0.1,
            }],
        });

        let markdown = build_account_position_markdown_report_at(&config, &report, generated_at);
        assert!(markdown.contains("## 测试仓位报告"));
        assert!(markdown.contains("2026-06-07 01:02:03 UTC"));
        assert!(markdown.contains("`ETHUSDT` 总持仓 `200.00U`"));

        let empty = build_account_position_markdown_report_at(
            &config,
            &AccountPositionReport {
                account_id: "acct-a".to_string(),
                total_equity_usdt: 0.0,
                long_total_usdt: 0.0,
                short_total_usdt: 0.0,
                net_exposure_usdt: 0.0,
                usdt_balance_value: 0.0,
                total_unrealized_pnl_usdt: 0.0,
                exposures: Vec::new(),
            },
            generated_at,
        );
        assert!(empty.contains("当前无持仓。"));
    }

    #[test]
    fn account_position_config_and_balance_helpers_should_stay_root_free() {
        let config = config();
        assert_eq!(
            config.market_type().expect("market type"),
            AccountPositionMarketType::Futures
        );
        assert_eq!(
            config.stablecoins_uppercase(),
            vec!["USDT", "USDC", "BUSD", "FDUSD", "TUSD", "USDS"]
        );
        assert!(is_stable_valued_asset(
            "USDT",
            &config.stablecoins_uppercase()
        ));
        assert!(is_stable_valued_asset(
            "BTCUSDT",
            &config.stablecoins_uppercase()
        ));

        let balances = vec![
            AccountBalanceInput {
                currency: "USDT".to_string(),
                total: 42.0,
            },
            AccountBalanceInput {
                currency: "usdt".to_string(),
                total: -10.0,
            },
            AccountBalanceInput {
                currency: "BTC".to_string(),
                total: 1.0,
            },
        ];
        assert_eq!(sum_usdt_balance_value(&balances), 42.0);
    }

    #[test]
    fn wecom_webhook_response_should_accept_success_payload() {
        validate_wecom_markdown_webhook_response(StatusCode::OK, r#"{"errcode":0,"errmsg":"ok"}"#)
            .expect("valid webhook response");
    }

    #[test]
    fn wecom_webhook_response_should_reject_nonzero_errcode() {
        let err = validate_wecom_markdown_webhook_response(
            StatusCode::OK,
            r#"{"errcode":40058,"errmsg":"bad webhook"}"#,
        )
        .expect_err("nonzero errcode should fail");
        assert!(err.to_string().contains("errcode=40058"));
    }

    #[test]
    fn wecom_webhook_response_should_reject_http_error() {
        let err = validate_wecom_markdown_webhook_response(
            StatusCode::BAD_GATEWAY,
            r#"{"errcode":0,"errmsg":"ok"}"#,
        )
        .expect_err("http error should fail");
        assert!(err.to_string().contains("webhook http status=502"));
    }

    #[tokio::test]
    async fn account_position_provider_should_collect_and_price_report_inputs() {
        let config = config();
        let provider = StaticAccountPositionProvider {
            balances: vec![
                AccountBalanceInput {
                    currency: "USDT".to_string(),
                    total: 100.0,
                },
                AccountBalanceInput {
                    currency: "BTC".to_string(),
                    total: 0.01,
                },
            ],
            positions: vec![AccountPositionInput {
                symbol: "ETHUSDT".to_string(),
                side: "LONG".to_string(),
                contracts: 0.0,
                contract_size: 1.0,
                mark_price: 2000.0,
                unrealized_pnl: 3.0,
                size: 0.0,
                amount: 0.1,
            }],
            prices: HashMap::from([(
                ("BTC/USDT".to_string(), AccountPositionMarketType::Spot),
                50_000.0,
            )]),
        };

        let report = collect_account_position_report(&config, &provider)
            .await
            .expect("collect report");

        assert_eq!(report.account_id, "acct-a");
        assert_eq!(report.total_equity_usdt, 600.0);
        assert_eq!(report.usdt_balance_value, 100.0);
        assert_eq!(report.long_total_usdt, 200.0);
        assert_eq!(report.exposures.len(), 1);
        assert_eq!(report.exposures[0].symbol, "ETHUSDT");
    }

    #[tokio::test]
    async fn account_position_pricing_should_fallback_to_market_type_candidates() {
        let provider = StaticAccountPositionProvider {
            balances: Vec::new(),
            positions: Vec::new(),
            prices: HashMap::from([(
                ("ALT/USDC".to_string(), AccountPositionMarketType::Futures),
                2.5,
            )]),
        };

        let price = resolve_asset_price_usdt(&provider, "ALT", AccountPositionMarketType::Futures)
            .await
            .expect("fallback price");

        assert_eq!(price, 2.5);
    }
}
