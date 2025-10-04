use base64::{engine::general_purpose, Engine as _};
use chrono::Utc;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::HashMap;

type HmacSha256 = Hmac<Sha256>;

/// 统一的签名辅助工具，覆盖当前保留的中心化交易所
pub struct SignatureHelper;

impl SignatureHelper {
    /// Binance 签名: HMAC-SHA256(query_string)
    pub fn binance_signature(secret: &str, query_string: &str) -> String {
        let mut mac =
            HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC 支持任意长度密钥");
        mac.update(query_string.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    /// OKX 签名: Base64(HMAC-SHA256(timestamp + method + path + body))
    pub fn okx_signature(
        secret: &str,
        timestamp: &str,
        method: &str,
        request_path: &str,
        body: &str,
    ) -> String {
        let prehash = format!("{}{}{}{}", timestamp, method, request_path, body);
        let mut mac =
            HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC 支持任意长度密钥");
        mac.update(prehash.as_bytes());
        general_purpose::STANDARD.encode(mac.finalize().into_bytes())
    }

    /// Bitmart v2 签名: HMAC-SHA256(timestamp + body)
    pub fn bitmart_signature(secret: &str, timestamp: &str, body: &str) -> String {
        let prehash = format!("{}{}", timestamp, body);
        let mut mac =
            HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC 支持任意长度密钥");
        mac.update(prehash.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    /// Bitmart v3 签名: HMAC-SHA256(timestamp#memo#body)
    pub fn bitmart_signature_v3(secret: &str, timestamp: &str, memo: &str, body: &str) -> String {
        let prehash = format!("{}#{}#{}", timestamp, memo, body);
        let mut mac =
            HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC 支持任意长度密钥");
        mac.update(prehash.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    /// Bybit 签名: HMAC-SHA256(排序后的 query string)
    pub fn bybit_signature(secret: &str, params: &HashMap<String, String>) -> String {
        let mut sorted: Vec<_> = params.iter().collect();
        sorted.sort_by(|a, b| a.0.cmp(b.0));
        let query: String = sorted
            .into_iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join("&");

        let mut mac =
            HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC 支持任意长度密钥");
        mac.update(query.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    /// URL 编码并保持键排序（多家交易所签名都依赖此逻辑）
    pub fn build_query_string(params: &HashMap<String, String>) -> String {
        let mut pairs: Vec<String> = params
            .iter()
            .map(|(k, v)| format!("{}={}", urlencoding::encode(k), urlencoding::encode(v)))
            .collect();
        pairs.sort();
        pairs.join("&")
    }

    /// 毫秒级时间戳
    pub fn timestamp() -> u64 {
        Utc::now().timestamp_millis() as u64
    }

    /// 秒级时间戳
    pub fn timestamp_seconds() -> u64 {
        Utc::now().timestamp() as u64
    }

    /// HTX(火币) 签名: Base64(HMAC-SHA256(method\nhost\npath\nquery))
    pub fn htx_signature(
        secret: &str,
        method: &str,
        host: &str,
        path: &str,
        query_string: &str,
    ) -> String {
        let sign_string = format!("{}\n{}\n{}\n{}", method, host, path, query_string);
        let mut mac =
            HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC 支持任意长度密钥");
        mac.update(sign_string.as_bytes());
        general_purpose::STANDARD.encode(mac.finalize().into_bytes())
    }

    /// 通用 HMAC-SHA256 帮助方法
    pub fn hmac_sha256(secret: &str, data: &str) -> String {
        let mut mac =
            HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC 支持任意长度密钥");
        mac.update(data.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    /// 统一的 URL 编码封装
    pub fn url_encode(value: &str) -> String {
        urlencoding::encode(value).to_string()
    }
}
