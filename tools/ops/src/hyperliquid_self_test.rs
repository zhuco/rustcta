use anyhow::{bail, Result};
use clap::Args;
use serde::Serialize;

#[derive(Debug, Clone, Args)]
pub struct HyperliquidSelfTestPlanArgs {
    #[arg(long, default_value = "ETH/USDC")]
    pub symbol: String,
    #[arg(long, default_value_t = true)]
    pub testnet: bool,
    #[arg(long, default_value_t = false)]
    pub run_orders: bool,
    #[arg(long, default_value_t = false)]
    pub execute: bool,
    #[arg(long, default_value_t = false)]
    pub confirm_live_order: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HyperliquidSelfTestGateMode {
    SecretFreePlanOnly,
    LegacyLiveRequired,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct HyperliquidSelfTestPlan {
    pub legacy_binary: &'static str,
    pub new_command: &'static str,
    pub gate_mode: HyperliquidSelfTestGateMode,
    pub execute_requested: bool,
    pub live_order_confirmed: bool,
    pub planned_side_effects: Vec<&'static str>,
    pub safety_checks: Vec<&'static str>,
    pub output_fields_preserved: Vec<&'static str>,
    pub request: HyperliquidSelfTestRequest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HyperliquidSelfTestRequest {
    pub symbol: String,
    pub testnet: bool,
    pub run_orders: bool,
    pub credential_env: Vec<&'static str>,
}

pub fn hyperliquid_self_test_safety_plan(args: HyperliquidSelfTestPlanArgs) -> Result<String> {
    if args.execute && !args.confirm_live_order {
        bail!("--execute requires --confirm-live-order");
    }
    if args.run_orders && (!args.execute || !args.confirm_live_order) {
        bail!("--run-orders requires --execute --confirm-live-order");
    }
    validate_hyperliquid_symbol(&args.symbol)?;

    let gate_mode = if args.execute || args.run_orders {
        HyperliquidSelfTestGateMode::LegacyLiveRequired
    } else {
        HyperliquidSelfTestGateMode::SecretFreePlanOnly
    };

    let planned_side_effects = if args.run_orders {
        vec![
            "legacy binary may submit post-only test orders",
            "legacy binary may cancel open orders for the selected symbol",
            "legacy binary may read recent fills",
        ]
    } else {
        Vec::new()
    };

    let plan = HyperliquidSelfTestPlan {
        legacy_binary: "hyperliquid_self_test",
        new_command: "rustcta-tools-ops probe hyperliquid-self-test",
        gate_mode,
        execute_requested: args.execute,
        live_order_confirmed: args.confirm_live_order,
        planned_side_effects,
        safety_checks: vec![
            "tools/ops does not read Hyperliquid wallet or private key values",
            "--execute requires --confirm-live-order",
            "--run-orders requires --execute --confirm-live-order",
            "legacy binary remains owner of Hyperliquid adapter bootstrap",
            "order placement remains opt-in through HYPERLIQUID_RUN_ORDERS in the legacy binary",
        ],
        output_fields_preserved: vec![
            "testnet",
            "run_orders",
            "vault",
            "http",
            "account",
            "signer",
            "symbol_count",
            "symbol",
            "balances",
            "positions",
            "ticker",
            "open_order_ack",
            "batch_order_count",
            "open_order_count",
            "cancelled_count",
            "recent_fill_count",
        ],
        request: HyperliquidSelfTestRequest {
            symbol: args.symbol,
            testnet: args.testnet,
            run_orders: args.run_orders,
            credential_env: vec![
                "HYPERLIQUID_WALLET_ADDRESS",
                "HYPERLIQUID_API_KEY",
                "HYPERLIQUID_PRIVATE_KEY",
                "HYPERLIQUID_API_SECRET",
                "HYPERLIQUID_API_WALLET",
            ],
        },
    };

    serde_json::to_string_pretty(&plan).map_err(Into::into)
}

fn validate_hyperliquid_symbol(symbol: &str) -> Result<()> {
    let mut parts = symbol.split('/');
    let base = parts.next().unwrap_or_default().trim();
    let quote = parts.next().unwrap_or_default().trim();
    if parts.next().is_some() || base.is_empty() || quote.is_empty() {
        bail!("invalid --symbol {symbol:?}; expected BASE/USDC");
    }
    if !quote.eq_ignore_ascii_case("USDC") {
        bail!("hyperliquid self-test currently expects a USDC quote symbol");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hyperliquid_self_test_plan_should_not_require_secrets_or_network() {
        let report = hyperliquid_self_test_safety_plan(HyperliquidSelfTestPlanArgs {
            symbol: "ETH/USDC".to_string(),
            testnet: true,
            run_orders: false,
            execute: false,
            confirm_live_order: false,
        })
        .expect("plan");
        let value: serde_json::Value = serde_json::from_str(&report).expect("json");

        assert_eq!(value["legacy_binary"], "hyperliquid_self_test");
        assert_eq!(value["gate_mode"], "secret_free_plan_only");
        assert_eq!(value["planned_side_effects"], serde_json::json!([]));
        assert_eq!(value["request"]["symbol"], "ETH/USDC");
        assert!(value["safety_checks"]
            .as_array()
            .expect("safety checks")
            .iter()
            .any(
                |check| check == "tools/ops does not read Hyperliquid wallet or private key values"
            ));
    }

    #[test]
    fn hyperliquid_order_path_requires_execute_and_confirmation() {
        let err = hyperliquid_self_test_safety_plan(HyperliquidSelfTestPlanArgs {
            symbol: "ETH/USDC".to_string(),
            testnet: true,
            run_orders: true,
            execute: false,
            confirm_live_order: false,
        })
        .unwrap_err();
        assert!(err
            .to_string()
            .contains("--run-orders requires --execute --confirm-live-order"));
    }
}
