use std::collections::HashMap;
use std::sync::{Arc, RwLock as StdRwLock};

use anyhow::Result;
use chrono::{Duration as ChronoDuration, Utc};
use rustcta::core::config::Config;
use rustcta::core::types::{Kline, OrderSide};
use rustcta::cta::account_manager::{AccountConfig, AccountManager};
use rustcta::strategies::common::{
    application::deps::StrategyDepsBuilder, application::status::StrategyState,
    risk::build_unified_risk_evaluator, Strategy, StrategyInstance,
};
use rustcta::strategies::trend::order_tracker::OrderTracker;
use rustcta::strategies::trend::{
    config::TrendConfig, execution_engine::TrendExecutionEngine, signal_generator::SignalGenerator,
    signal_generator::SignalMetadata, signal_generator::SignalType, signal_generator::TakeProfit,
    signal_generator::TradeSignal, trend_analyzer::TrendAnalyzer, TrendIntradayStrategy,
};
use rustcta::strategies::{
    common::shared_data::publish_trend_inputs,
    mean_reversion::{
        config::SymbolConfig,
        indicators::{BollingerSnapshot, IndicatorOutputs},
        model::{SymbolSnapshot, VolumeSnapshot},
    },
};
use tokio::time::Duration;

async fn build_account_manager() -> Arc<AccountManager> {
    std::env::set_var("RUSTCTA_OFFLINE", "1");

    let config = Config {
        name: "Binance".to_string(),
        testnet: false,
        spot_base_url: "https://api.binance.com".to_string(),
        futures_base_url: "https://fapi.binance.com".to_string(),
        ws_spot_url: "wss://stream.binance.com:9443/ws".to_string(),
        ws_futures_url: "wss://fstream.binance.com/ws".to_string(),
    };

    let mut account_manager = AccountManager::new(config);
    account_manager
        .add_account(AccountConfig {
            id: "binance_main".to_string(),
            exchange: "binance".to_string(),
            enabled: true,
            api_key_env: "BINANCE_0".to_string(),
            max_positions: 10,
            max_orders_per_symbol: 10,
        })
        .await
        .expect("failed to add mock account");

    Arc::new(account_manager)
}

fn build_klines(symbol: &str, interval: &str, count: usize, start: f64) -> Vec<Kline> {
    let step_minutes = match interval {
        "1m" => 1,
        "5m" => 5,
        "15m" => 15,
        _ => 5,
    } as i64;

    (0..count)
        .map(|i| {
            let base = start + i as f64 * 0.8;
            let close_time =
                Utc::now() - ChronoDuration::minutes((count - i - 1) as i64 * step_minutes);
            Kline {
                symbol: symbol.to_string(),
                interval: interval.to_string(),
                open_time: close_time - ChronoDuration::minutes(step_minutes),
                close_time,
                open: base * 0.995,
                high: base * 1.01,
                low: base * 0.99,
                close: base * 1.005,
                volume: 1_000.0 + i as f64 * 50.0,
                quote_volume: (1_000.0 + i as f64 * 50.0) * base,
                trade_count: 100,
            }
        })
        .collect()
}

fn build_shared_snapshot(symbol: &str) -> (SymbolSnapshot, IndicatorOutputs) {
    let fifteen = build_klines(symbol, "15m", 120, 150.0);
    let five = build_klines(symbol, "5m", 120, 155.0);
    let one = build_klines(symbol, "1m", 240, 156.0);
    let last_price = five.last().map(|k| k.close).unwrap_or(160.0);

    let snapshot = SymbolSnapshot {
        config: SymbolConfig {
            symbol: symbol.to_string(),
            enabled: true,
            min_quote_volume_5m: 0.0,
            depth_multiplier: 1.0,
            depth_levels: 5,
            enforce_spread_rule: false,
            blackout_windows: Vec::new(),
            overrides: HashMap::new(),
            allow_short: Some(true),
        },
        one_minute: one,
        five_minute: five.clone(),
        fifteen_minute: fifteen.clone(),
        one_hour: Vec::new(),
        bbw_history: vec![0.1; 120],
        mid_history: vec![last_price; 120],
        sigma_history: vec![1.0; 120],
        long_position: None,
        short_position: None,
        last_depth: None,
        last_volume: Some(VolumeSnapshot {
            window_minutes: 5,
            quote_volume: 15_000.0,
            timestamp: Utc::now(),
        }),
        frozen: false,
    };

    let boll_5m = BollingerSnapshot {
        upper: last_price * 1.02,
        middle: last_price,
        lower: last_price * 0.98,
        sigma: 15.0,
        band_percent: 0.75,
        z_score: 1.2,
    };

    let boll_15m = BollingerSnapshot {
        upper: last_price * 1.03,
        middle: last_price * 0.99,
        lower: last_price * 0.97,
        sigma: 18.0,
        band_percent: 0.8,
        z_score: 1.4,
    };

    let indicators = IndicatorOutputs {
        timestamp: Utc::now(),
        last_price,
        bollinger_5m: boll_5m,
        bollinger_15m: boll_15m,
        rsi: 65.0,
        atr: 4.0,
        adx: 32.0,
        bbw: 0.12,
        bbw_percentile: 0.8,
        slope_metric: 0.6,
        choppiness: Some(40.0),
        recent_volume_quote: 120_000.0,
        volume_window_minutes: 15,
    };

    (snapshot, indicators)
}

fn sample_trade_signal(symbol: &str) -> TradeSignal {
    TradeSignal {
        symbol: symbol.to_string(),
        signal_type: SignalType::TrendBreakout,
        side: OrderSide::Buy,
        entry_price: 200.0,
        stop_loss: 195.0,
        take_profits: vec![TakeProfit {
            price: 210.0,
            ratio: 0.5,
        }],
        suggested_size: 0.05,
        risk_reward_ratio: 2.0,
        confidence: 80.0,
        timeframe_aligned: true,
        has_structure_support: true,
        expire_time: Utc::now() + ChronoDuration::seconds(60),
        metadata: SignalMetadata {
            trend_strength: 70.0,
            volume_confirmed: true,
            key_level_nearby: false,
            pattern_name: None,
            generated_at: Utc::now(),
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trend_intraday_continues_running_after_first_order() -> Result<()> {
    let account_manager = build_account_manager().await;

    let risk_evaluator = build_unified_risk_evaluator("trend_intraday_test", None, None);
    let deps = StrategyDepsBuilder::new()
        .with_account_manager(account_manager.clone())
        .with_risk_evaluator(risk_evaluator)
        .build()?;

    let mut config = TrendConfig::default();
    config.name = "trend_intraday_test".to_string();
    config.account_id = "binance_main".to_string();
    config.symbols = vec!["BTC/USDC".to_string()];
    config.poll_interval_secs = 1;
    config.signal_cooldown_secs = 1;
    config.max_daily_trades = 5;
    config.pyramid_enabled = false;
    config.pyramid_levels.clear();
    config.position_config.pyramid_enabled = false;
    config.position_config.pyramid_levels.clear();
    config.risk_config.max_total_exposure = 1.0;

    let (snapshot, indicators) = build_shared_snapshot("BTC/USDC");
    publish_trend_inputs("BTC/USDC", &snapshot, &indicators, None);

    let mut analyzer = TrendAnalyzer::new(config.indicator_config.clone(), config.scoring.clone());
    analyzer.set_account_manager(account_manager.clone());
    let trend_signal = analyzer
        .analyze("BTC/USDC")
        .await
        .expect("trend analysis should succeed");
    assert!(
        trend_signal.is_tradeable(),
        "trend signal expected to be tradeable in test fixture"
    );

    let mut generator = SignalGenerator::new(config.signal_config.clone());
    generator.set_account_manager(account_manager.clone());
    let generated = generator
        .generate("BTC/USDC", &trend_signal)
        .await
        .expect("signal generator should succeed");
    assert!(
        generated.is_some(),
        "signal generator expected to create trade signal"
    );

    let strategy = TrendIntradayStrategy::create(config, deps)?;
    strategy.start().await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    let status = strategy.status().await?;
    assert_eq!(status.state, StrategyState::Running);
    assert!(
        !status.positions.is_empty(),
        "strategy should hold position after first order"
    );

    tokio::time::sleep(Duration::from_secs(2)).await;

    let status_after = strategy.status().await?;
    assert_eq!(status_after.state, StrategyState::Running);

    strategy.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn execution_engine_places_maker_order() -> Result<()> {
    let account_manager = build_account_manager().await;
    let config = TrendConfig::default();
    let order_tracker = OrderTracker::new();
    let symbol_precisions = Arc::new(StdRwLock::new(HashMap::new()));
    let engine = TrendExecutionEngine::new(
        account_manager.clone(),
        "binance_main",
        config.execution.clone(),
        order_tracker,
        symbol_precisions,
    );

    let signal = sample_trade_signal("ETH/USDC");
    let report = engine.execute_order(&signal, 0.05).await?;

    assert_eq!(report.order.symbol, "ETH/USDC");
    assert!(report.used_maker);
    assert!(report.order.price.unwrap() <= signal.entry_price);

    Ok(())
}
