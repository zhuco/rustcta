use std::fmt;

use base64::{engine::general_purpose, Engine as _};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Sha256, Sha512};

type HmacSha256 = Hmac<Sha256>;
type HmacSha512 = Hmac<Sha512>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SigningVector {
    pub exchange: String,
    pub name: String,
    pub algorithm: SigningAlgorithm,
    pub secret: String,
    pub payload: String,
    pub expected_signature: String,
    #[serde(default)]
    pub payload_format: SigningPayloadFormat,
    #[serde(default)]
    pub timestamp: Option<String>,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub request_path: Option<String>,
    #[serde(default)]
    pub body: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SigningAlgorithm {
    HmacSha256Hex,
    HmacSha256Base64,
    HmacSha512Hex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SigningPayloadFormat {
    #[default]
    RawPayload,
    TimestampMethodPathBody,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SigningSpecError {
    pub vector: String,
    pub expected: String,
    pub actual: String,
}

impl fmt::Display for SigningSpecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} signing vector expected {} got {}",
            self.vector, self.expected, self.actual
        )
    }
}

impl std::error::Error for SigningSpecError {}

impl SigningVector {
    pub fn canonical_payload(&self) -> Option<String> {
        match self.payload_format {
            SigningPayloadFormat::RawPayload => None,
            SigningPayloadFormat::TimestampMethodPathBody => Some(format!(
                "{}{}{}{}",
                self.timestamp.as_deref()?,
                self.method.as_deref()?.to_ascii_uppercase(),
                self.request_path.as_deref()?,
                self.body.as_deref().unwrap_or_default()
            )),
        }
    }

    pub fn compute_signature(&self) -> String {
        match self.algorithm {
            SigningAlgorithm::HmacSha256Hex => {
                let mut mac = HmacSha256::new_from_slice(self.secret.as_bytes())
                    .expect("HMAC accepts secret keys of any length");
                mac.update(self.payload.as_bytes());
                hex::encode(mac.finalize().into_bytes())
            }
            SigningAlgorithm::HmacSha256Base64 => {
                let mut mac = HmacSha256::new_from_slice(self.secret.as_bytes())
                    .expect("HMAC accepts secret keys of any length");
                mac.update(self.payload.as_bytes());
                general_purpose::STANDARD.encode(mac.finalize().into_bytes())
            }
            SigningAlgorithm::HmacSha512Hex => {
                let mut mac = HmacSha512::new_from_slice(self.secret.as_bytes())
                    .expect("HMAC accepts secret keys of any length");
                mac.update(self.payload.as_bytes());
                hex::encode(mac.finalize().into_bytes())
            }
        }
    }

    pub fn verify(&self) -> Result<(), SigningSpecError> {
        if let Some(canonical_payload) = self.canonical_payload() {
            if canonical_payload != self.payload {
                return Err(SigningSpecError {
                    vector: self.name.clone(),
                    expected: self.payload.clone(),
                    actual: canonical_payload,
                });
            }
        }
        let actual = self.compute_signature();
        if actual == self.expected_signature {
            Ok(())
        } else {
            Err(SigningSpecError {
                vector: self.name.clone(),
                expected: self.expected_signature.clone(),
                actual,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signing_vector_should_verify_hmac_sha256_hex() {
        let vector = SigningVector {
            exchange: "binance".to_string(),
            name: "documented-limit-order".to_string(),
            algorithm: SigningAlgorithm::HmacSha256Hex,
            secret: "NhqPtmdSJYVB3mq9F2ZAjgI4VdFOvZyY6XifdlLqbUeX7hY4tH8MGjdPVeQx1ioO"
                .to_string(),
            payload: "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559".to_string(),
            expected_signature:
                "1df63f2e1799dde74ddff3876c338dce38b1cd5da51483e1beffe9c87a8550ba"
                    .to_string(),
            payload_format: SigningPayloadFormat::RawPayload,
            timestamp: Some("1499827319559".to_string()),
            method: Some("POST".to_string()),
            request_path: Some("/api/v3/order".to_string()),
            body: None,
        };

        vector.verify().expect("signature verifies");
    }

    #[test]
    fn signing_vector_should_verify_hmac_sha256_base64() {
        let vector = SigningVector {
            exchange: "okx".to_string(),
            name: "place-order".to_string(),
            algorithm: SigningAlgorithm::HmacSha256Base64,
            secret: "secret".to_string(),
            payload: "2024-01-02T03:04:05.678ZPOST/api/v5/trade/order{\"clOrdId\":\"LIMIT1\",\"instId\":\"BTC-USDT\",\"ordType\":\"limit\",\"px\":\"65000\",\"side\":\"buy\",\"sz\":\"0.02\",\"tdMode\":\"cash\"}".to_string(),
            expected_signature: "agFbSG/WUO5xRh3DyF9Jy0sx4mcplsuNgYveXWZD6EE=".to_string(),
            payload_format: SigningPayloadFormat::TimestampMethodPathBody,
            timestamp: Some("2024-01-02T03:04:05.678Z".to_string()),
            method: Some("POST".to_string()),
            request_path: Some("/api/v5/trade/order".to_string()),
            body: Some("{\"clOrdId\":\"LIMIT1\",\"instId\":\"BTC-USDT\",\"ordType\":\"limit\",\"px\":\"65000\",\"side\":\"buy\",\"sz\":\"0.02\",\"tdMode\":\"cash\"}".to_string()),
        };

        vector.verify().expect("signature verifies");
    }

    #[test]
    fn signing_vector_should_verify_hmac_sha512_hex() {
        let vector = SigningVector {
            exchange: "gateio".to_string(),
            name: "place_order_limit".to_string(),
            algorithm: SigningAlgorithm::HmacSha512Hex,
            secret: "gate-secret".to_string(),
            payload: "POST\n/api/v4/spot/orders\ncurrency_pair=BTC_USDT\n5a0b107f560498d655aaaf0ad6b9bdcbf14019b311cfd9f7fa58fb220d70acfc69c49ba3a42640e6803b406e74ab31dbc2101879f1d1a79d5012b7135d68dc63\n1700000000".to_string(),
            expected_signature: "9332e3844b59e4d1c9e1881c19514b5a988251e361d5f5f01308808af66a78093ec09f39589c7a33888dbfcc20de049477976478097d99f461896a0ad81ee828".to_string(),
            payload_format: SigningPayloadFormat::RawPayload,
            timestamp: Some("1700000000".to_string()),
            method: Some("POST".to_string()),
            request_path: Some("/api/v4/spot/orders?currency_pair=BTC_USDT".to_string()),
            body: Some("{\"currency_pair\":\"BTC_USDT\",\"side\":\"buy\",\"type\":\"limit\",\"amount\":\"0.02\",\"price\":\"65000\"}".to_string()),
        };

        vector.verify().expect("signature verifies");
    }

    #[test]
    fn signing_vector_should_report_mismatch() {
        let vector = SigningVector {
            exchange: "binance".to_string(),
            name: "bad-vector".to_string(),
            algorithm: SigningAlgorithm::HmacSha256Hex,
            secret: "secret".to_string(),
            payload: "payload".to_string(),
            expected_signature: "wrong".to_string(),
            payload_format: SigningPayloadFormat::RawPayload,
            timestamp: None,
            method: None,
            request_path: None,
            body: None,
        };

        let error = vector.verify().expect_err("mismatch");
        assert_eq!(error.vector, "bad-vector");
        assert_eq!(error.expected, "wrong");
    }
}
