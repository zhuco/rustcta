use crate::{MarketDataMode, SpotSpotTakerArbitrageConfig};
use rustcta_strategy_sdk::{
    MarketDataChannel, MarketDataSubscription as SdkMarketDataSubscription,
    MarketType as SdkMarketType,
};

pub fn market_data_subscriptions(
    config: &SpotSpotTakerArbitrageConfig,
) -> Vec<SdkMarketDataSubscription> {
    if config.market_data_mode != MarketDataMode::WebsocketCache || !config.websocket.enabled {
        return Vec::new();
    }
    let exchanges = if config.websocket.exchanges.is_empty() {
        &config.exchanges
    } else {
        &config.websocket.exchanges
    };
    let symbols = if config.websocket.symbols.is_empty() {
        &config.symbols
    } else {
        &config.websocket.symbols
    };
    exchanges
        .iter()
        .flat_map(|exchange| {
            symbols.iter().map(move |symbol| SdkMarketDataSubscription {
                exchange_id: normalize_exchange(exchange),
                symbol: normalize_symbol(symbol),
                market_type: SdkMarketType::Spot,
                channels: vec![MarketDataChannel::OrderBookDepth],
            })
        })
        .collect()
}

pub fn replay_input_path(config: &SpotSpotTakerArbitrageConfig) -> Option<&str> {
    (config.market_data_mode == MarketDataMode::Replay && config.replay.enabled)
        .then_some(config.replay.input_path.as_str())
}

fn normalize_exchange(exchange: &str) -> String {
    match exchange.trim().to_ascii_lowercase().as_str() {
        "gate" | "gate.io" => "gateio".to_string(),
        other => other.to_string(),
    }
}

fn normalize_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .replace(['-', '_', '/'], "")
        .to_ascii_uppercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn market_data_subscriptions_should_use_configured_subscription_universe() {
        let mut config = minimal_config();
        config.market_data_mode = MarketDataMode::WebsocketCache;
        config.websocket.enabled = true;
        config.websocket.exchanges = vec!["gate.io".to_string(), "bitget".to_string()];
        config.websocket.symbols = vec!["vsn-usdt".to_string()];

        let subscriptions = market_data_subscriptions(&config);

        assert_eq!(subscriptions.len(), 2);
        assert_eq!(subscriptions[0].exchange_id, "gateio");
        assert_eq!(subscriptions[0].symbol, "VSNUSDT");
        assert_eq!(subscriptions[0].market_type, SdkMarketType::Spot);
        assert_eq!(
            subscriptions[0].channels,
            vec![MarketDataChannel::OrderBookDepth]
        );
    }

    #[test]
    fn replay_helper_should_respect_mode() {
        let mut config = minimal_config();
        config.market_data_mode = MarketDataMode::RestPolling;
        config.rest_polling.enabled = true;

        assert!(market_data_subscriptions(&config).is_empty());
        assert_eq!(replay_input_path(&config), None);

        config.market_data_mode = MarketDataMode::Replay;
        config.replay.enabled = true;
        config.replay.input_path = "data/books.jsonl".to_string();
        assert_eq!(replay_input_path(&config), Some("data/books.jsonl"));
    }

    fn minimal_config() -> SpotSpotTakerArbitrageConfig {
        serde_yaml::from_str(
            "exchanges: [gateio, bitget]\n\
             symbols: [BTCUSDT]\n\
             max_notional_per_trade: 10\n\
             min_notional_per_trade: 1\n\
             max_notional_per_symbol: 100\n\
             max_total_notional: 100\n\
             min_net_spread_bps: 0\n\
             min_depth_notional: 1\n",
        )
        .expect("minimal config")
    }
}
