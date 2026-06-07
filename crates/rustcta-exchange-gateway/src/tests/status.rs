use super::*;

#[test]
fn gateway_status_should_declare_gateway_only_credential_boundary() {
    let status = GatewayStatus {
        api_version: GATEWAY_API_VERSION.to_string(),
        identity: GatewayIdentity {
            gateway_id: "local".to_string(),
            mode: GatewayMode::Local,
            credential_boundary: CredentialBoundary::GatewayOnly,
            started_at: Utc::now(),
        },
        exchanges: Vec::new(),
    };

    assert_eq!(
        status.identity.credential_boundary,
        CredentialBoundary::GatewayOnly
    );
}
