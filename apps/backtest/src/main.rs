use clap::Parser;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = rustcta_backtest::runtime::BacktestCli::parse();
    let output = rustcta_backtest::runtime::execute_cli(cli).await?;
    rustcta_backtest::runtime::render_command_output(output);

    Ok(())
}
