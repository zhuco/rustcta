use anyhow::Result;
use rustcta_cross_arb_live_runner::{run_live_runner, LiveRunnerArgs};
use rustcta_gateway_app::GatewayAppConfig;

#[tokio::main]
async fn main() -> Result<()> {
    let args = LiveRunnerArgs::from_env_args()?;
    let config = gateway_config_from_env();
    let loaded_adapters = config.adapters.clone();
    let gateway = config.build_gateway()?;
    run_live_runner(args, gateway, loaded_adapters).await
}

fn gateway_config_from_env() -> GatewayAppConfig {
    GatewayAppConfig::from_env_reader(gateway_env)
}

fn gateway_env(key: &str) -> Option<String> {
    match key {
        "RUSTCTA_GATEWAY_ADAPTERS" => {
            first_env(&[key]).or_else(|| Some("binance,bitget,gateio".to_string()))
        }
        "RUSTCTA_BINANCE_API_KEY" => first_env(&[key, "BINANCE_0_API_KEY", "BINANCE_API_KEY"]),
        "RUSTCTA_BINANCE_API_SECRET" => {
            first_env(&[key, "BINANCE_0_API_SECRET", "BINANCE_API_SECRET"])
        }
        "RUSTCTA_BITGET_API_KEY" => first_env(&[key, "BITGET_API_KEY"]),
        "RUSTCTA_BITGET_API_SECRET" => first_env(&[key, "BITGET_API_SECRET"]),
        "RUSTCTA_BITGET_API_PASSPHRASE" => {
            first_env(&[key, "BITGET_PASSPHRASE", "BITGET_API_PASSPHRASE"])
        }
        "RUSTCTA_GATEIO_API_KEY" => first_env(&[
            key,
            "GATEIO_API_KEY",
            "GATE_API_KEY",
            "GATE__16076371__API_KEY",
        ]),
        "RUSTCTA_GATEIO_API_SECRET" => first_env(&[
            key,
            "GATEIO_API_SECRET",
            "GATE_API_SECRET",
            "GATE__16076371__API_SECRET",
        ]),
        _ => std::env::var(key)
            .ok()
            .filter(|value| !value.trim().is_empty()),
    }
}

fn first_env(keys: &[&str]) -> Option<String> {
    keys.iter()
        .filter_map(|key| std::env::var(key).ok())
        .map(|value| value.trim().to_string())
        .find(|value| !value.is_empty())
}
