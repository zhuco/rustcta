use std::sync::Arc;

use anyhow::Result;
use rustcta_exchange_gateway::gateway_router;
use rustcta_gateway_app::GatewayAppConfig;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    let config = GatewayAppConfig::from_env();
    let gateway = config.build_gateway()?;
    let gateway = Arc::new(gateway);
    let listener = TcpListener::bind(&config.bind_addr).await?;

    println!(
        "rustcta-gateway listening on http://{} adapters={}",
        config.bind_addr,
        config.adapters.join(",")
    );
    axum::serve(listener, gateway_router(gateway)).await?;
    Ok(())
}
