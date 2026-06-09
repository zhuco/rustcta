use rustcta_exchange_api::{
    AccountId, ExchangeClient, OpenOrdersRequest, PublicStreamKind, PublicStreamSubscription,
    QueryOrderRequest, RecentFillsRequest, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderStatus};
use serde_json::json;

use super::parser::{
    parse_bitflyer_fills, parse_bitflyer_open_orders, parse_orderbook_snapshot,
    parse_public_board_snapshot_message, parse_symbol_rules,
};
use super::streams::{bitflyer_public_order_book_ws_policy, public_subscription_spec};
use super::{BitflyerGatewayAdapter, BitflyerGatewayConfig};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("bitflyer").expect("exchange")
}

fn context(request_id: &str) -> rustcta_exchange_api::RequestContext {
    rustcta_exchange_api::RequestContext {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        tenant_id: None,
        account_id: None,
        run_id: None,
        request_id: Some(request_id.to_string()),
        requested_at: chrono::Utc::now(),
    }
}

fn context_with_account(request_id: &str) -> rustcta_exchange_api::RequestContext {
    rustcta_exchange_api::RequestContext {
        tenant_id: Some(TenantId::new("tenant-bitflyer").expect("tenant")),
        account_id: Some(AccountId::new("account-bitflyer").expect("account")),
        ..context(request_id)
    }
}

fn symbol_scope(product_code: &str, market_type: MarketType) -> rustcta_exchange_api::SymbolScope {
    rustcta_exchange_api::SymbolScope {
        exchange: exchange_id(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "JPY").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), market_type, product_code)
            .expect("symbol"),
    }
}

#[test]
fn bitflyer_parser_fixtures_should_cover_markets_and_board() {
    let markets: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitflyer/markets_success.json"
    ))
    .expect("markets");
    let rules = parse_symbol_rules(&exchange_id(), &markets).expect("rules");
    assert!(rules
        .iter()
        .any(|rule| rule.symbol.exchange_symbol.symbol == "BTC_JPY"));
    assert!(rules
        .iter()
        .any(|rule| rule.symbol.exchange_symbol.symbol == "FX_BTC_JPY"));

    let board: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitflyer/orderbook_success.json"
    ))
    .expect("board");
    let snapshot = parse_orderbook_snapshot(
        &exchange_id(),
        symbol_scope("BTC_JPY", MarketType::Spot),
        &board,
    )
    .expect("snapshot");
    assert_eq!(snapshot.best_bid().unwrap().price, 29999.0);
    assert_eq!(snapshot.best_ask().unwrap().price, 30001.0);
}

#[tokio::test]
async fn bitflyer_adapter_capabilities_and_unsupported_boundaries_should_be_explicit() {
    let adapter = BitflyerGatewayAdapter::new(BitflyerGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.market_types.contains(&MarketType::Spot));
    assert!(capabilities.market_types.contains(&MarketType::Margin));

    let err = adapter
        .get_fees(rustcta_exchange_api::FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![symbol_scope("BTC_JPY", MarketType::Spot)],
        })
        .await
        .expect_err("fees unsupported");
    assert!(format!("{err:?}").contains("bitflyer.get_fees"));
}

#[test]
fn bitflyer_private_parser_fixtures_should_cover_orders_and_fills() {
    let orders_value: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitflyer/order_state_success.json"
    ))
    .expect("orders fixture");
    let orders = parse_bitflyer_open_orders(
        &exchange_id(),
        Some(&symbol_scope("BTC_JPY", MarketType::Spot)),
        &orders_value,
    )
    .expect("orders");
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].status, OrderStatus::New);
    assert_eq!(
        orders[0].client_order_id.as_deref(),
        Some("JRF20250327-000000-000001")
    );
    assert_eq!(
        orders[0].exchange_order_id.as_deref(),
        Some("JOR20250327-000000-000001")
    );

    let fills_value: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitflyer/fills_success.json"
    ))
    .expect("fills fixture");
    let fills = parse_bitflyer_fills(
        &exchange_id(),
        TenantId::new("tenant-bitflyer").expect("tenant"),
        AccountId::new("account-bitflyer").expect("account"),
        &symbol_scope("BTC_JPY", MarketType::Spot),
        &fills_value,
    )
    .expect("fills");
    assert_eq!(fills.len(), 1);
    assert_eq!(
        fills[0].client_order_id.as_deref(),
        Some("JRF20250327-000000-000001")
    );
    assert_eq!(fills[0].fee_asset.as_deref(), Some("BTC"));
}

#[tokio::test]
async fn bitflyer_read_only_core_runtime_should_be_credential_guarded() {
    let adapter = BitflyerGatewayAdapter::new(BitflyerGatewayConfig::default()).expect("adapter");
    let symbol = symbol_scope("BTC_JPY", MarketType::Spot);

    let query_err = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query-order"),
            symbol: symbol.clone(),
            client_order_id: Some("JRF20250327-000000-000001".to_string()),
            exchange_order_id: None,
        })
        .await
        .expect_err("query requires credentials");
    assert!(format!("{query_err:?}").contains("bitflyer.query_order"));

    let open_err = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open-orders"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol.clone()),
            page: None,
        })
        .await
        .expect_err("open orders requires credentials");
    assert!(format!("{open_err:?}").contains("bitflyer.get_open_orders"));

    let fills_err = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context_with_account("recent-fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol),
            client_order_id: None,
            exchange_order_id: None,
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(50),
            page: None,
        })
        .await
        .expect_err("recent fills requires credentials");
    assert!(format!("{fills_err:?}").contains("bitflyer.get_recent_fills"));
}

#[test]
fn bitflyer_public_ws_subscription_should_use_lightning_channels() {
    let snapshot_subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws-snapshot"),
        symbol: symbol_scope("BTC_JPY", MarketType::Spot),
        kind: PublicStreamKind::OrderBookSnapshot,
    };
    let snapshot_spec = public_subscription_spec(
        &snapshot_subscription,
        "wss://ws.lightstream.bitflyer.com/json-rpc",
    )
    .expect("snapshot subscription");
    assert_eq!(snapshot_spec.channel, "lightning_board_snapshot_BTC_JPY");
    assert_eq!(snapshot_spec.subscribe_payload["method"], "subscribe");
    let snapshot_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitflyer/ws/public_board_snapshot_subscribe.json"
    ))
    .expect("snapshot subscription fixture");
    assert_eq!(snapshot_spec.subscribe_payload, snapshot_fixture);

    let delta_subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws-delta"),
        symbol: symbol_scope("BTC_JPY", MarketType::Spot),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let delta_spec = public_subscription_spec(
        &delta_subscription,
        "wss://ws.lightstream.bitflyer.com/json-rpc",
    )
    .expect("delta subscription");
    assert_eq!(delta_spec.channel, "lightning_board_BTC_JPY");
    assert_eq!(delta_spec.subscribe_payload["method"], "subscribe");
    let delta_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitflyer/ws/public_board_subscribe.json"
    ))
    .expect("delta subscription fixture");
    assert_eq!(delta_spec.subscribe_payload, delta_fixture);
}

#[test]
fn bitflyer_public_order_book_ws_policy_should_document_unsequenced_resync() {
    let policy = bitflyer_public_order_book_ws_policy();
    assert_eq!(policy.protocol, "json_rpc_2");
    assert_eq!(
        policy.snapshot_channel_template,
        "lightning_board_snapshot_{product_code}"
    );
    assert_eq!(
        policy.delta_channel_template,
        "lightning_board_{product_code}"
    );
    assert_eq!(policy.fixed_update_interval_ms, None);
    assert_eq!(policy.depth, None);
    assert_eq!(policy.sequence_field, None);
    assert_eq!(policy.checksum, None);
    assert_eq!(policy.rest_snapshot_endpoint, "/v1/getboard");
    assert!(policy.resync_strategy.contains("GET /v1/getboard"));
}

#[test]
fn bitflyer_public_board_snapshot_message_should_parse_without_sequence() {
    let value: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitflyer/ws/public_board_snapshot_message.json"
    ))
    .expect("board snapshot message");
    let book = parse_public_board_snapshot_message(
        &exchange_id(),
        symbol_scope("BTC_JPY", MarketType::Spot),
        &value,
    )
    .expect("parsed board snapshot");
    assert_eq!(book.best_bid().unwrap().price, 4_999_999.0);
    assert_eq!(book.best_ask().unwrap().price, 5_000_001.0);
    assert_eq!(book.sequence, None);
}

#[test]
fn bitflyer_public_request_paths_are_documented() {
    let _ = json!({
        "markets": "/v1/getmarkets",
        "board": "/v1/getboard?product_code=BTC_JPY"
    });
}
