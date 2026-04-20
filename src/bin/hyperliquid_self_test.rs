use dotenv::dotenv;
use rustcta::core::{
    config::{ApiKeys, Config},
    types::{MarketType, OrderRequest, OrderSide, OrderType},
};
use rustcta::exchanges::hyperliquid::HyperliquidExchange;
use rustcta::Exchange;
use std::env;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    env_logger::init();

    // 环境变量读取
    let vault_address = env::var("HYPERLIQUID_WALLET_ADDRESS")
        .or_else(|_| env::var("HYPERLIQUID_API_KEY"))
        .expect("缺少 HYPERLIQUID_WALLET_ADDRESS (或 HYPERLIQUID_API_KEY)");
    let priv_key = env::var("HYPERLIQUID_PRIVATE_KEY")
        .or_else(|_| env::var("HYPERLIQUID_API_SECRET"))
        .expect("缺少 HYPERLIQUID_PRIVATE_KEY (或 HYPERLIQUID_API_SECRET)");
    let is_testnet = env::var("HYPERLIQUID_TESTNET")
        .or_else(|_| env::var("HYPERLIQUID_USE_TESTNET"))
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(true);
    let run_orders = env::var("HYPERLIQUID_RUN_ORDERS")
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);

    println!(
        "Hyperliquid 自测启动 | testnet={} | run_orders={} | vault={}",
        is_testnet, run_orders, vault_address
    );

    // 构造简单 Config（testnet 标志会被使用）
    let cfg = Config {
        name: "hyperliquid".to_string(),
        testnet: is_testnet,
        spot_base_url: "".to_string(),
        futures_base_url: "".to_string(),
        ws_spot_url: "".to_string(),
        ws_futures_url: "".to_string(),
    };
    let api_keys = ApiKeys {
        api_key: vault_address.clone(),
        api_secret: priv_key.clone(),
        passphrase: None,
        memo: None,
    };

    let exchange = HyperliquidExchange::new(cfg, api_keys);
    if let Ok(api_wallet) = env::var("HYPERLIQUID_API_WALLET") {
        if !api_wallet.trim().is_empty()
            && !api_wallet.eq_ignore_ascii_case(exchange.signing_address())
        {
            println!(
                "警告: 提供的 API Wallet 地址={} 与私钥推导的签名地址={} 不一致，请检查私钥与地址对应关系。",
                api_wallet,
                exchange.signing_address()
            );
        }
    }
    println!(
        "HTTP: {} | account={} | signer={}",
        exchange.rest_base_url(),
        exchange.account_address(),
        exchange.signing_address()
    );
    if exchange.account_address() != exchange.signing_address() {
        println!("使用 API Wallet 签名，确保该 API Wallet 已在对应网络授权。");
    }

    // 基础信息
    let info = exchange.exchange_info().await?;
    println!("交易对数量: {}", info.symbols.len());
    let symbol = env::var("HYPERLIQUID_SYMBOL").unwrap_or_else(|_| "ETH/USDC".to_string());
    println!("选用交易对: {}", symbol);

    // 余额与持仓
    let balances = exchange.get_balance(MarketType::Futures).await?;
    println!("余额: {:?}", balances);
    let positions = exchange.get_positions(None).await?;
    println!("持仓: {:?}", positions);

    // 行情
    let ticker = exchange.get_ticker(&symbol, MarketType::Futures).await?;
    println!(
        "行情 bid={} ask={} last={}",
        ticker.bid, ticker.ask, ticker.last
    );

    // 挂单/撤单/批量单（可选）
    if run_orders {
        let qty: f64 = env::var("HYPERLIQUID_QTY")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.01);
        // Hyperliquid ETH 永续 tick size 0.1（部分资产 0.01），这里按 0.1 对齐，避免精度拒单
        let tick = 0.1_f64;
        let align_down = |px: f64| (px / tick).floor() * tick;
        let align_up = |px: f64| (px / tick).ceil() * tick;
        // 尽量避免成交：买单压低，卖单抬高
        let buy_price = align_down((ticker.bid * 0.9).max(1.0));
        let sell_price = align_up((ticker.ask * 1.1).max(1.0));

        let buy_req = OrderRequest {
            symbol: symbol.clone(),
            side: OrderSide::Buy,
            order_type: OrderType::Limit,
            amount: qty,
            price: Some(buy_price),
            market_type: MarketType::Futures,
            params: None,
            client_order_id: None,
            time_in_force: Some("GTC".to_string()),
            reduce_only: Some(false),
            post_only: None,
        };
        let sell_req = OrderRequest {
            symbol: symbol.clone(),
            side: OrderSide::Sell,
            order_type: OrderType::Limit,
            amount: qty,
            price: Some(sell_price),
            market_type: MarketType::Futures,
            params: None,
            client_order_id: None,
            time_in_force: Some("GTC".to_string()),
            reduce_only: Some(false),
            post_only: None,
        };

        println!("下买单 price={} qty={}", buy_price, buy_req.amount);
        let buy_order = exchange.create_order(buy_req.clone()).await?;
        println!("买单返回: {:?}", buy_order.id);

        println!("批量下双边单");
        let batch = rustcta::core::types::BatchOrderRequest {
            orders: vec![sell_req.clone(), buy_req.clone()],
            market_type: MarketType::Futures,
        };
        let batch_resp = exchange.create_batch_orders(batch).await?;
        println!("批量下单成功数: {}", batch_resp.successful_orders.len());

        // 查询挂单
        let open_orders = exchange
            .get_open_orders(Some(&symbol), MarketType::Futures)
            .await?;
        println!("当前挂单数量: {}", open_orders.len());

        // 全撤
        let cancelled = exchange
            .cancel_all_orders(Some(&symbol), MarketType::Futures)
            .await?;
        println!("已撤{}笔", cancelled.len());

        // 成交补数（简单轮询 userFills）
        let fills = exchange
            .get_my_trades(Some(&symbol), MarketType::Futures, Some(50))
            .await?;
        println!("最近成交数: {}", fills.len());
    } else {
        println!("run_orders 未开启，跳过下单/撤单测试");
    }

    Ok(())
}
