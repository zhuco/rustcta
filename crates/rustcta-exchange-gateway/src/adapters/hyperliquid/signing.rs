use rmp_serde::Serializer;
use secp256k1::{ecdsa::RecoverableSignature, Message, Secp256k1, SecretKey};
use serde::Serialize;
use serde_json::{json, Value};
use sha3::{Digest, Keccak256};

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};

use super::config::HyperliquidGatewayConfig;

#[derive(Debug, Clone)]
pub struct HyperliquidPrivateCredentials {
    #[allow(dead_code)]
    pub account_address: String,
    pub vault_address: Option<String>,
    pub private_key_hex: String,
    pub is_mainnet: bool,
}

impl HyperliquidPrivateCredentials {
    pub fn from_config(config: &HyperliquidGatewayConfig) -> Option<Self> {
        if !config.private_rest_available() {
            return None;
        }
        Some(Self {
            account_address: config.normalized_account_address()?,
            vault_address: config.normalized_vault_address(),
            private_key_hex: config.signing_private_key.clone().unwrap_or_default(),
            is_mainnet: config.is_mainnet(),
        })
    }
}

pub fn keccak(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Keccak256::new();
    hasher.update(bytes);
    hasher.finalize().into()
}

#[allow(dead_code)]
pub fn signing_address_from_private_key(private_key_hex: &str) -> ExchangeApiResult<String> {
    let secret = secret_key(private_key_hex)?;
    let secp = Secp256k1::new();
    let pubkey = secp256k1::PublicKey::from_secret_key(&secp, &secret);
    let uncompressed = pubkey.serialize_uncompressed();
    let hash = keccak(&uncompressed[1..]);
    Ok(format!("0x{}", hex::encode(&hash[12..])))
}

pub fn sign_l1_action<T: Serialize>(
    private_key_hex: &str,
    action: &T,
    vault_address: Option<&str>,
    nonce: u64,
    expires_after: Option<u64>,
    is_mainnet: bool,
) -> ExchangeApiResult<Value> {
    let secret = secret_key(private_key_hex)?;
    let connection_id = action_hash(action, vault_address, nonce, expires_after)?;
    let digest = eip712_digest(connection_id, is_mainnet);
    let message = Message::from_slice(&digest).map_err(validation_error)?;
    let secp = Secp256k1::new();
    let signature: RecoverableSignature = secp.sign_ecdsa_recoverable(&message, &secret);
    let (recovery_id, compact) = signature.serialize_compact();
    Ok(json!({
        "r": format!("0x{}", hex::encode(&compact[..32])),
        "s": format!("0x{}", hex::encode(&compact[32..])),
        "v": recovery_id.to_i32() + 27,
    }))
}

pub fn action_hash<T: Serialize>(
    action: &T,
    vault_address: Option<&str>,
    nonce: u64,
    expires_after: Option<u64>,
) -> ExchangeApiResult<[u8; 32]> {
    let mut buf = Vec::new();
    let mut serializer = Serializer::new(&mut buf).with_struct_map();
    action
        .serialize(&mut serializer)
        .map_err(validation_error)?;
    buf.extend_from_slice(&nonce.to_be_bytes());
    match vault_address
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(address) => {
            buf.push(1u8);
            buf.extend_from_slice(&address_to_bytes(address)?);
        }
        None => buf.push(0u8),
    }
    if let Some(expires_after) = expires_after {
        buf.push(0u8);
        buf.extend_from_slice(&expires_after.to_be_bytes());
    }
    Ok(keccak(&buf))
}

fn eip712_digest(connection_id: [u8; 32], is_mainnet: bool) -> [u8; 32] {
    let typehash_agent = keccak(b"Agent(string source,bytes32 connectionId)");
    let typehash_domain = keccak(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    );
    let source_hash = keccak(if is_mainnet { b"a" } else { b"b" });

    let mut agent_enc = Vec::with_capacity(96);
    agent_enc.extend_from_slice(&typehash_agent);
    agent_enc.extend_from_slice(&source_hash);
    agent_enc.extend_from_slice(&connection_id);
    let struct_hash = keccak(&agent_enc);

    let mut chain_id_bytes = [0u8; 32];
    chain_id_bytes[24..].copy_from_slice(&1337u64.to_be_bytes());
    let mut domain_enc = Vec::with_capacity(160);
    domain_enc.extend_from_slice(&typehash_domain);
    domain_enc.extend_from_slice(&keccak(b"Exchange"));
    domain_enc.extend_from_slice(&keccak(b"1"));
    domain_enc.extend_from_slice(&chain_id_bytes);
    domain_enc.extend_from_slice(&[0u8; 32]);
    let domain_hash = keccak(&domain_enc);

    let mut final_enc = Vec::with_capacity(66);
    final_enc.extend_from_slice(&[0x19, 0x01]);
    final_enc.extend_from_slice(&domain_hash);
    final_enc.extend_from_slice(&struct_hash);
    keccak(&final_enc)
}

fn secret_key(private_key_hex: &str) -> ExchangeApiResult<SecretKey> {
    let key = private_key_hex.trim().trim_start_matches("0x");
    let bytes = hex::decode(key).map_err(validation_error)?;
    SecretKey::from_slice(&bytes).map_err(validation_error)
}

fn address_to_bytes(address: &str) -> ExchangeApiResult<[u8; 20]> {
    let decoded = hex::decode(address.trim().trim_start_matches("0x")).map_err(validation_error)?;
    if decoded.len() != 20 {
        return Err(ExchangeApiError::InvalidRequest {
            message: "hyperliquid address must be 20 bytes".to_string(),
        });
    }
    let mut bytes = [0u8; 20];
    bytes.copy_from_slice(&decoded);
    Ok(bytes)
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{action_hash, sign_l1_action, signing_address_from_private_key};

    #[test]
    fn hyperliquid_signing_should_build_recoverable_signature() {
        let private_key = "0000000000000000000000000000000000000000000000000000000000000001";
        let action = json!({
            "type": "order",
            "orders": [{"a": 0, "b": true, "p": "65000", "s": "0.001", "r": false, "t": {"limit": {"tif": "Gtc"}}}],
            "grouping": "na"
        });
        let hash = action_hash(&action, None, 1_700_000_000_000, None).expect("hash");
        assert_eq!(hash.len(), 32);
        let signature = sign_l1_action(private_key, &action, None, 1_700_000_000_000, None, true)
            .expect("signature");
        assert!(signature["r"].as_str().unwrap().starts_with("0x"));
        assert!(signature["s"].as_str().unwrap().starts_with("0x"));
        assert!(signature["v"].as_i64().unwrap() == 27 || signature["v"].as_i64().unwrap() == 28);
        assert_eq!(
            signing_address_from_private_key(private_key).expect("address"),
            "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf"
        );
    }
}
