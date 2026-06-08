use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use secp256k1::{ecdsa::RecoverableSignature, Message, Secp256k1, SecretKey};
use sha3::{Digest, Keccak256};

const ASTER_DOMAIN_NAME: &str = "AsterSignTransaction";
const ASTER_DOMAIN_VERSION: &str = "1";
const ASTER_CHAIN_ID: u64 = 1666;

pub fn sign_message(private_key_hex: &str, message_text: &str) -> ExchangeApiResult<String> {
    let secret = secret_key(private_key_hex)?;
    let digest = eip712_digest(message_text);
    let message = Message::from_slice(&digest).map_err(validation_error)?;
    let secp = Secp256k1::new();
    let signature: RecoverableSignature = secp.sign_ecdsa_recoverable(&message, &secret);
    let (recovery_id, compact) = signature.serialize_compact();
    let mut bytes = Vec::with_capacity(65);
    bytes.extend_from_slice(&compact);
    bytes.push((recovery_id.to_i32() + 27) as u8);
    Ok(format!("0x{}", hex::encode(bytes)))
}

pub fn signing_address_from_private_key(private_key_hex: &str) -> ExchangeApiResult<String> {
    let secret = secret_key(private_key_hex)?;
    let secp = Secp256k1::new();
    let pubkey = secp256k1::PublicKey::from_secret_key(&secp, &secret);
    let uncompressed = pubkey.serialize_uncompressed();
    let hash = keccak(&uncompressed[1..]);
    Ok(format!("0x{}", hex::encode(&hash[12..])))
}

fn eip712_digest(message_text: &str) -> [u8; 32] {
    let typehash_domain = keccak(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    );
    let typehash_message = keccak(b"Message(string msg)");

    let mut message_enc = Vec::with_capacity(64);
    message_enc.extend_from_slice(&typehash_message);
    message_enc.extend_from_slice(&keccak(message_text.as_bytes()));
    let message_hash = keccak(&message_enc);

    let mut chain_id = [0u8; 32];
    chain_id[24..].copy_from_slice(&ASTER_CHAIN_ID.to_be_bytes());
    let mut domain_enc = Vec::with_capacity(160);
    domain_enc.extend_from_slice(&typehash_domain);
    domain_enc.extend_from_slice(&keccak(ASTER_DOMAIN_NAME.as_bytes()));
    domain_enc.extend_from_slice(&keccak(ASTER_DOMAIN_VERSION.as_bytes()));
    domain_enc.extend_from_slice(&chain_id);
    domain_enc.extend_from_slice(&[0u8; 32]);
    let domain_hash = keccak(&domain_enc);

    let mut encoded = Vec::with_capacity(66);
    encoded.extend_from_slice(&[0x19, 0x01]);
    encoded.extend_from_slice(&domain_hash);
    encoded.extend_from_slice(&message_hash);
    keccak(&encoded)
}

fn keccak(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Keccak256::new();
    hasher.update(bytes);
    hasher.finalize().into()
}

fn secret_key(private_key_hex: &str) -> ExchangeApiResult<SecretKey> {
    let key = private_key_hex.trim().trim_start_matches("0x");
    let bytes = hex::decode(key).map_err(validation_error)?;
    SecretKey::from_slice(&bytes).map_err(validation_error)
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::{sign_message, signing_address_from_private_key};

    #[test]
    fn aster_signing_should_build_eip712_signature() {
        let private_key = "0000000000000000000000000000000000000000000000000000000000000001";
        let message = "nonce=1748310859508867&signer=0x7e5f4552091a69125d5dfcb7b8c2659029395bdf&symbol=ASTERUSDT&type=LIMIT";
        let signature = sign_message(private_key, message).expect("signature");
        assert_eq!(signature.len(), 132);
        assert!(signature.starts_with("0x"));
        assert_eq!(
            signing_address_from_private_key(private_key).expect("address"),
            "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf"
        );
    }
}
