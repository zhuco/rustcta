use anyhow::{anyhow, Result};

const QUOTE_CURRENCIES: &[&str] = &[
    "USDT", "USDC", "BUSD", "TUSD", "FDUSD", "USDS", "DAI", "PAX", "USDP", "UST", "HUSD", "USD",
    "EUR", "GBP", "JPY", "KRW", "TRY", "BRL", "AUD", "RUB", "NGN", "UAH", "VAI", "BTC", "ETH",
    "BNB", "XRP", "ADA", "DOT", "SOL", "MATIC", "AVAX", "TRX", "LTC", "BCH", "EOS",
];

pub fn normalize_symbol_input(symbol: &str) -> Result<String> {
    let trimmed = symbol.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("invalid symbol {symbol}: empty symbol"));
    }

    for separator in ['/', '-', '_'] {
        if trimmed.contains(separator) {
            let parts = trimmed
                .split(separator)
                .filter(|part| !part.is_empty())
                .collect::<Vec<_>>();
            if parts.len() == 2 {
                return Ok(format!(
                    "{}/{}",
                    parts[0].to_ascii_uppercase(),
                    parts[1].to_ascii_uppercase()
                ));
            }
            return Err(anyhow!("invalid symbol {symbol}: expected BASE/QUOTE"));
        }
    }

    let symbol_upper = trimmed.to_ascii_uppercase();
    for quote in QUOTE_CURRENCIES {
        if symbol_upper.ends_with(quote) {
            let base = &symbol_upper[..symbol_upper.len() - quote.len()];
            if !base.is_empty() {
                return Ok(format!("{base}/{quote}"));
            }
        }
    }

    Err(anyhow!(
        "invalid symbol {symbol}: unable to infer quote asset"
    ))
}

pub fn to_binance_futures_symbol(symbol: &str) -> Result<String> {
    let normalized = normalize_symbol_input(symbol)?;
    let (base, quote) = normalized
        .split_once('/')
        .ok_or_else(|| anyhow!("invalid symbol {symbol}: expected BASE/QUOTE"))?;
    Ok(format!("{base}{quote}"))
}

#[cfg(test)]
mod tests {
    use super::{normalize_symbol_input, to_binance_futures_symbol};

    #[test]
    fn normalizes_common_backtest_symbols_without_legacy_root() {
        assert_eq!(normalize_symbol_input("BTCUSDT").unwrap(), "BTC/USDT");
        assert_eq!(normalize_symbol_input("btc-usdc").unwrap(), "BTC/USDC");
        assert_eq!(normalize_symbol_input("eth_usdt").unwrap(), "ETH/USDT");
        assert_eq!(normalize_symbol_input("SOL/USDT").unwrap(), "SOL/USDT");
    }

    #[test]
    fn converts_to_binance_futures_symbol_without_legacy_root() {
        assert_eq!(to_binance_futures_symbol("BTC/USDT").unwrap(), "BTCUSDT");
        assert_eq!(to_binance_futures_symbol("ethusdc").unwrap(), "ETHUSDC");
    }

    #[test]
    fn rejects_ambiguous_symbols() {
        assert!(normalize_symbol_input("").is_err());
        assert!(normalize_symbol_input("BTC").is_err());
        assert!(normalize_symbol_input("BTC/USDT/EXTRA").is_err());
    }
}
