#[path = "common/legacy_tools_ops_shim.rs"]
mod legacy_tools_ops_shim;

fn main() -> anyhow::Result<()> {
    legacy_tools_ops_shim::run_tools_ops(&["ws-proxy-probe"], std::env::args_os().skip(1))
}
