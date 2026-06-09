use anyhow::{bail, Result};
use rustcta_control_api_app::ControlApiAppConfig;
use tokio::net::TcpListener;

const CANONICAL_CONTROL_API_BIND: &str = "127.0.0.1:8091";
const ALLOW_NON_CANONICAL_BIND_ENV: &str = "RUSTCTA_CONTROL_API_ALLOW_NON_CANONICAL_BIND";

#[tokio::main]
async fn main() -> Result<()> {
    let config = ControlApiAppConfig::from_env();
    validate_canonical_bind(&config.bind_addr)?;
    config.clean_cross_arb_exchange_config_on_startup().await?;
    let app = config.build_router()?;
    let listener = TcpListener::bind(&config.bind_addr).await?;
    println!(
        "rustcta-control-api listening on http://{}",
        config.bind_addr
    );
    axum::serve(listener, app).await?;
    Ok(())
}

fn validate_canonical_bind(bind_addr: &str) -> Result<()> {
    if bind_addr == CANONICAL_CONTROL_API_BIND || allow_non_canonical_bind_for_smoke_test() {
        return Ok(());
    }

    bail!(
        "refusing to start rustcta-control-api on {bind_addr}; only {CANONICAL_CONTROL_API_BIND} \
         is allowed for the Web control panel"
    )
}

fn allow_non_canonical_bind_for_smoke_test() -> bool {
    std::env::var(ALLOW_NON_CANONICAL_BIND_ENV)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}
