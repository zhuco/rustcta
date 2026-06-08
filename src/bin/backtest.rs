#![allow(clippy::all)]
use clap::Parser;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = rustcta::backtest::runtime::BacktestCli::parse();
    let output = rustcta::backtest::runtime::execute_cli(cli).await?;
    rustcta::backtest::runtime::render_command_output(output);

    Ok(())
}
