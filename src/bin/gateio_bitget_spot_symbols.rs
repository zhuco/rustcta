#![allow(clippy::all)]
#[path = "common/legacy_tools_ops_shim.rs"]
mod legacy_tools_ops_shim;

fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    legacy_tools_ops_shim::run_tools_ops(
        &["symbols", "gateio-bitget-spot"],
        std::env::args_os().skip(1),
    )
}
