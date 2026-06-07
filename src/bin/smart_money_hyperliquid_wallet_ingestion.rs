#[path = "common/legacy_tools_ops_shim.rs"]
mod legacy_tools_ops_shim;

fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    rustcta::utils::init_tracing_logger("info");
    legacy_tools_ops_shim::run_tools_ops(
        &["smart-money", "hyperliquid-wallet-ingestion"],
        std::env::args_os().skip(1),
    )
}
