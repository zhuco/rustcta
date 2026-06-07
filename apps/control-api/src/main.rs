use anyhow::Result;
use rustcta_control_api_app::ControlApiAppConfig;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    let config = ControlApiAppConfig::from_env();
    let app = config.build_router()?;
    let listener = TcpListener::bind(&config.bind_addr).await?;
    println!(
        "rustcta-control-api listening on http://{}",
        config.bind_addr
    );
    axum::serve(listener, app).await?;
    Ok(())
}
