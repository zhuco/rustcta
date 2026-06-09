//! Runtime logging initialization shared by RustCTA binaries.

use tracing_subscriber::EnvFilter;

pub fn init_tracing(default_filter: &str) {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .with_target(true)
        .compact()
        .try_init();
}
