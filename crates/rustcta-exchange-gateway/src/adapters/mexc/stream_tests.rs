use rustcta_exchange_api::{
    ExchangeClient, PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::streams::{
    mexc_ping_payload, mexc_public_subscription_spec, parse_mexc_spot_aggre_depth_delta,
    parse_mexc_spot_book_ticker, parse_mexc_spot_limit_depth_snapshot,
};
use super::test_support::{context, exchange_id, perpetual_symbol_scope, symbol_scope};
use super::{MexcGatewayAdapter, MexcGatewayConfig};

#[test]
fn mexc_spot_public_ws_spec_should_cover_10ms_depth_and_book_ticker() {
    let delta = mexc_public_subscription_spec(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("mexc-depth"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    })
    .expect("delta spec");
    assert_eq!(delta.url, "wss://wbs-api.mexc.com/ws");
    assert_eq!(
        delta.channel,
        "spot@public.aggre.depth.v3.api.pb@10ms@BTCUSDT"
    );
    assert_eq!(delta.subscribe_payload["method"], "SUBSCRIPTION");
    assert_eq!(delta.unsubscribe_payload["method"], "UNSUBSCRIPTION");

    let partial = mexc_public_subscription_spec(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("mexc-partial-depth"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookSnapshot,
    })
    .expect("partial depth spec");
    assert_eq!(
        partial.channel,
        "spot@public.limit.depth.v3.api.pb@BTCUSDT@20"
    );

    let ticker = mexc_public_subscription_spec(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("mexc-book-ticker"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::Ticker,
    })
    .expect("ticker spec");
    assert_eq!(
        ticker.channel,
        "spot@public.aggre.bookTicker.v3.api.pb@10ms@BTCUSDT"
    );
    assert_eq!(mexc_ping_payload()["method"], "PING");
}

#[test]
fn mexc_contract_public_ws_spec_should_cover_depth_profiles() {
    let delta = mexc_public_subscription_spec(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("mexc-contract-depth"),
        symbol: perpetual_symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    })
    .expect("contract delta spec");
    assert_eq!(delta.url, "wss://contract.mexc.com/edge");
    assert_eq!(delta.channel, "sub.depth");
    assert_eq!(delta.subscribe_payload["param"]["symbol"], "BTC_USDT");
    assert_eq!(delta.subscribe_payload["param"]["compress"], true);

    let snapshot = mexc_public_subscription_spec(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("mexc-contract-depth-full"),
        symbol: perpetual_symbol_scope(),
        kind: PublicStreamKind::OrderBookSnapshot,
    })
    .expect("contract full-depth spec");
    assert_eq!(snapshot.channel, "sub.depth.full");
    assert_eq!(snapshot.subscribe_payload["param"]["limit"], 20);
}

#[test]
fn mexc_spot_ws_parser_fixtures_should_extract_sequence_and_bbo() {
    let delta_fixture = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/mexc/ws/spot_aggre_depth_10ms.json"
    ))
    .expect("delta fixture");
    let delta = parse_mexc_spot_aggre_depth_delta(&exchange_id(), &symbol_scope(), &delta_fixture)
        .expect("delta");
    assert_eq!(delta.first_sequence, Some(101));
    assert_eq!(delta.last_sequence, Some(102));
    assert_eq!(delta.bids[0].price, 65000.0);
    assert_eq!(delta.asks[0].quantity, 0.8);

    let partial_fixture = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/mexc/ws/spot_limit_depth_20.json"
    ))
    .expect("partial fixture");
    let snapshot =
        parse_mexc_spot_limit_depth_snapshot(&exchange_id(), symbol_scope(), &partial_fixture)
            .expect("snapshot");
    assert_eq!(snapshot.sequence, Some(102));
    assert_eq!(snapshot.bids.len(), 2);
    assert_eq!(snapshot.asks[0].price, 65010.0);

    let ticker_fixture = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/mexc/ws/spot_book_ticker_10ms.json"
    ))
    .expect("ticker fixture");
    let ticker = parse_mexc_spot_book_ticker(&exchange_id(), MarketType::Spot, &ticker_fixture)
        .expect("ticker");
    assert_eq!(ticker.symbol, "BTCUSDT");
    assert_eq!(ticker.sequence, Some(102));
    assert_eq!(ticker.bid_price, 65000.0);
    assert_eq!(ticker.ask_quantity, 0.8);
}

#[tokio::test]
async fn mexc_adapter_should_ack_public_ws_and_expose_native_capability() {
    let adapter = MexcGatewayAdapter::new(MexcGatewayConfig::default()).expect("adapter");
    let id = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("mexc-public-id"),
            symbol: symbol_scope(),
            kind: PublicStreamKind::OrderBookDelta,
        })
        .await
        .expect("public stream id");
    assert!(id.contains("spot@public.aggre.depth.v3.api.pb@10ms@BTCUSDT"));

    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_public_streams);
    assert!(capabilities.order_book.supports_sequence);
    assert_eq!(capabilities.max_order_book_depth, Some(1000));
}
