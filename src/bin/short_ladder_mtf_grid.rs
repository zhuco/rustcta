#![allow(clippy::all)]
use clap::Parser;
use rustcta::backtest::runtime::{
    render_command_output, run_short_ladder_mtf_grid, BacktestCommandOutput, ShortLadderMtfGridArgs,
};

#[derive(Debug, Parser)]
#[command(name = "short-ladder-mtf-grid", bin_name = "short-ladder-mtf-grid")]
struct ShortLadderMtfGridCli {
    #[command(flatten)]
    args: ShortLadderMtfGridArgs,
}

fn main() -> anyhow::Result<()> {
    let cli = ShortLadderMtfGridCli::parse();
    let manifest = run_short_ladder_mtf_grid(cli.args)?;
    render_command_output(BacktestCommandOutput::ShortLadderMtfGrid(manifest));
    Ok(())
}
