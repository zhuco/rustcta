use std::net::SocketAddr;

use anyhow::{anyhow, Context, Result};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use super::models::MonitoringConfig;
use super::{router, MonitoringState};

pub async fn spawn_monitoring_server(
    config: MonitoringConfig,
    state: MonitoringState,
) -> Result<Option<JoinHandle<Result<()>>>> {
    if !config.enabled {
        return Ok(None);
    }
    let addr: SocketAddr = config
        .bind_addr
        .parse()
        .with_context(|| format!("invalid monitoring bind_addr {}", config.bind_addr))?;
    if !config.expose_publicly && !addr.ip().is_loopback() {
        return Err(anyhow!(
            "monitoring dashboard refuses non-loopback bind_addr={} unless expose_publicly=true",
            addr
        ));
    }
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind monitoring dashboard on {addr}"))?;
    let local_addr = listener.local_addr()?;
    log::info!("monitoring dashboard listening on http://{}", local_addr);
    let app = router(state);
    Ok(Some(tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .context("monitoring server failed")
    })))
}
