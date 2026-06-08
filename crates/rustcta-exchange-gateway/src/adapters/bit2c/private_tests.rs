use rustcta_exchange_api::{
    BalancesRequest, ExchangeApiError, ExchangeClient, EXCHANGE_API_SCHEMA_VERSION,
};

use super::signing::{canonical_form, signed_form};
use super::test_support::{context, exchange_id};
use super::{Bit2cGatewayAdapter, Bit2cGatewayConfig};
use crate::adapters::AdapterBackedGateway;

#[test]
fn bit2c_signing_should_match_offline_request_spec_vector() {
    let params = [
        ("Amount", "0.01"),
        ("Price", "99000"),
        ("IsBid", "true"),
        ("Pair", "BtcNis"),
    ];
    let signed = signed_form("secret", &params, 1_700_000_000_000).expect("signed form");

    assert_eq!(
        signed.encoded_body,
        "Amount=0.01&Price=99000&IsBid=true&Pair=BtcNis&nonce=1700000000000"
    );
    assert_eq!(
        signed.signature,
        "eam1//1ne9qUfumUqe8/GAz/kCVxwf+TigdsKWPDdrzpHoWRJ67I+wSHIzzpoUgIq5yQ2L1fZmZO26FIPD6f7w=="
    );
    assert_eq!(
        canonical_form(&[] as &[(&str, &str)], 1_700_000_000_000),
        "nonce=1700000000000"
    );
}

#[test]
fn bit2c_named_registration_should_accept_adapter_id() {
    AdapterBackedGateway::with_named_adapters("bit2c-test", ["bit2c"])
        .expect("bit2c named registration");
}

#[tokio::test]
async fn bit2c_adapter_should_keep_private_rest_disabled_even_with_credentials() {
    let adapter = Bit2cGatewayAdapter::new(Bit2cGatewayConfig {
        api_key: Some("fixture_key".to_string()),
        api_secret: Some("fixture_secret".to_string()),
        enabled_private_rest: true,
        ..Bit2cGatewayConfig::default()
    })
    .expect("adapter");

    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_cancel_order);
    assert!(!capabilities.supports_public_streams);
    assert!(!capabilities.supports_private_streams);

    let error = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: None,
            assets: Vec::new(),
        })
        .await
        .expect_err("private read must be disabled");

    match error {
        ExchangeApiError::Unsupported { operation } => {
            assert_eq!(operation, "bit2c.balances_private_rest_disabled");
        }
        other => panic!("unexpected error: {other:?}"),
    }
}
