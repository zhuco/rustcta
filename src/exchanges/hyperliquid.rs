use crate::core::{
    error::ExchangeError,
    exchange::Exchange,
    types::*,
    websocket::{ConnectionState, WebSocketClient},
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use parking_lot::RwLock;
use reqwest::Client;
use rmp_serde::Serializer;
use secp256k1::{ecdsa::RecoverableSignature, Message as SecpMessage, Secp256k1, SecretKey};
use serde::Serialize;
use serde_json::Value;
use sha3::{Digest, Keccak256};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async, tungstenite::protocol::Message as WsMessage, MaybeTlsStream, WebSocketStream,
};

/// Hyperliquid 交易所实现（仅单向持仓）
pub struct HyperliquidExchange {
    client: Client,
    base_url: String,
    ws_url: String,
    account_address: String,
    vault_address: Option<String>,
    signing_key: SecretKey,
    signing_address: String,
    nonce: AtomicU64,
    asset_map: RwLock<HashMap<String, u32>>,
}

#[derive(Debug, Serialize, Clone)]
struct OrderWire {
    // 字段顺序与官方 Python SDK 对齐
    a: u32,
    b: bool,
    p: String,
    s: String,
    r: bool,
    t: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    c: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
struct OrderAction {
    #[serde(rename = "type")]
    action_type: &'static str,
    orders: Vec<OrderWire>,
    grouping: &'static str,
}

#[derive(Debug, Serialize, Clone)]
struct CancelWire {
    a: u32,
    o: u64,
}

#[derive(Debug, Serialize, Clone)]
struct CancelAction {
    #[serde(rename = "type")]
    action_type: &'static str,
    cancels: Vec<CancelWire>,
}

#[derive(Debug, Serialize, Clone)]
struct UpdateLeverageAction {
    #[serde(rename = "type")]
    action_type: &'static str,
    asset: u32,
    is_cross: bool,
    leverage: u32,
}

impl HyperliquidExchange {
    fn float_to_wire(v: f64) -> String {
        let mut s = format!("{:.8}", v);
        while s.contains('.') && s.ends_with('0') {
            s.pop();
        }
        if s.ends_with('.') {
            s.pop();
        }
        if s == "-0" {
            s = "0".to_string();
        }
        if s.is_empty() {
            "0".to_string()
        } else {
            s
        }
    }

    fn keccak(bytes: &[u8]) -> [u8; 32] {
        let mut hasher = Keccak256::new();
        hasher.update(bytes);
        hasher.finalize().into()
    }

    fn signing_address_from_key(secret: &SecretKey) -> String {
        let secp = Secp256k1::new();
        let pubkey = secp256k1::PublicKey::from_secret_key(&secp, secret);
        let uncompressed = pubkey.serialize_uncompressed();
        let hash = Self::keccak(&uncompressed[1..]); // skip 0x04 prefix
        format!("0x{}", hex::encode(&hash[12..]))
    }

    fn address_to_bytes(addr: &str) -> Result<[u8; 20]> {
        let cleaned = addr.trim_start_matches("0x");
        let mut bytes = [0u8; 20];
        let decoded = hex::decode(cleaned)
            .map_err(|e| ExchangeError::AuthError(format!("地址解析失败: {}", e)))?;
        if decoded.len() != 20 {
            return Err(ExchangeError::AuthError("地址长度必须20字节".to_string()));
        }
        bytes.copy_from_slice(&decoded);
        Ok(bytes)
    }

    fn action_hash<T: Serialize>(
        action: &T,
        vault_address: &str,
        nonce: u64,
        expires_after: Option<u64>,
    ) -> Result<[u8; 32]> {
        let mut buf = Vec::new();
        let mut serializer = Serializer::new(&mut buf).with_struct_map();
        action
            .serialize(&mut serializer)
            .map_err(|e| ExchangeError::ParseError(e.to_string()))?;

        buf.extend_from_slice(&nonce.to_be_bytes());

        if vault_address.is_empty() {
            buf.push(0u8);
        } else {
            buf.push(1u8);
            let addr_bytes = Self::address_to_bytes(vault_address)?;
            buf.extend_from_slice(&addr_bytes);
        }

        if let Some(exp) = expires_after {
            buf.push(0u8);
            buf.extend_from_slice(&exp.to_be_bytes());
        }

        Ok(Self::keccak(&buf))
    }

    fn eip712_digest(connection_id: [u8; 32], is_mainnet: bool) -> [u8; 32] {
        // Type hashes
        let typehash_agent = Self::keccak(b"Agent(string source,bytes32 connectionId)");
        let typehash_domain = Self::keccak(
            b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
        );

        // Agent struct hash
        let source = if is_mainnet { "a" } else { "b" };
        let source_hash = Self::keccak(source.as_bytes());
        let mut agent_enc = Vec::with_capacity(32 * 3);
        agent_enc.extend_from_slice(&typehash_agent);
        agent_enc.extend_from_slice(&source_hash);
        agent_enc.extend_from_slice(&connection_id);
        let struct_hash = Self::keccak(&agent_enc);

        // Domain separator
        let name_hash = Self::keccak(b"Exchange");
        let version_hash = Self::keccak(b"1");
        let chain_id: u64 = 1337;
        let mut chain_id_bytes = [0u8; 32];
        chain_id_bytes[24..].copy_from_slice(&chain_id.to_be_bytes());
        let verifying_contract = [0u8; 32]; // zero address

        let mut domain_enc = Vec::with_capacity(32 * 5);
        domain_enc.extend_from_slice(&typehash_domain);
        domain_enc.extend_from_slice(&name_hash);
        domain_enc.extend_from_slice(&version_hash);
        domain_enc.extend_from_slice(&chain_id_bytes);
        domain_enc.extend_from_slice(&verifying_contract);
        let domain_hash = Self::keccak(&domain_enc);

        // Final digest: keccak(0x1901 || domain_hash || struct_hash)
        let mut final_enc = Vec::with_capacity(2 + 32 + 32);
        final_enc.extend_from_slice(&[0x19, 0x01]);
        final_enc.extend_from_slice(&domain_hash);
        final_enc.extend_from_slice(&struct_hash);
        Self::keccak(&final_enc)
    }

    pub fn new(
        config: crate::core::config::Config,
        api_keys: crate::core::config::ApiKeys,
    ) -> Self {
        let env_testnet = std::env::var("HYPERLIQUID_TESTNET")
            .or_else(|_| std::env::var("HYPERLIQUID_USE_TESTNET"))
            .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
            .ok();
        let is_testnet = env_testnet.unwrap_or(config.testnet);
        let base_url = if is_testnet {
            "https://api.hyperliquid-testnet.xyz".to_string()
        } else {
            "https://api.hyperliquid.xyz".to_string()
        };
        let ws_url = if is_testnet {
            "wss://api.hyperliquid-testnet.xyz/ws".to_string()
        } else {
            "wss://api.hyperliquid.xyz/ws".to_string()
        };

        let account_address = std::env::var("HYPERLIQUID_ACCOUNT_ADDRESS")
            .or_else(|_| std::env::var("HYPERLIQUID_WALLET_ADDRESS"))
            .unwrap_or_else(|_| api_keys.api_key.clone())
            .to_lowercase();
        let vault_address = std::env::var("HYPERLIQUID_VAULT_ADDRESS")
            .ok()
            .map(|v| v.to_lowercase())
            .or_else(|| Some(account_address.clone()));
        let priv_key_hex = api_keys.api_secret.trim_start_matches("0x");
        let priv_bytes =
            hex::decode(priv_key_hex).expect("HYPERLIQUID 私钥格式错误（需 hex, 无0x）");
        let signing_key =
            SecretKey::from_slice(&priv_bytes).expect("无法加载私钥（需32字节 secp256k1）");
        let signing_address = Self::signing_address_from_key(&signing_key);

        Self {
            client: Client::new(),
            base_url,
            ws_url,
            account_address,
            vault_address,
            signing_key,
            signing_address,
            nonce: AtomicU64::new(Utc::now().timestamp_millis() as u64),
            asset_map: RwLock::new(HashMap::new()),
        }
    }

    pub fn account_address(&self) -> &str {
        &self.account_address
    }

    pub fn signing_address(&self) -> &str {
        &self.signing_address
    }

    pub fn rest_base_url(&self) -> &str {
        &self.base_url
    }

    fn to_coin(&self, symbol: &str) -> String {
        let s = symbol.to_uppercase();
        s.replace("/USDC", "")
            .replace("/USDT", "")
            .replace("-PERP", "")
    }

    fn normalize_symbol(&self, coin: &str) -> String {
        if coin.contains('/') {
            coin.to_uppercase()
        } else {
            format!("{}/USDC", coin.to_uppercase())
        }
    }

    fn next_nonce(&self) -> u64 {
        self.nonce.fetch_add(1, Ordering::SeqCst)
    }

    fn sign_action<T: Serialize>(&self, action: &T, nonce: u64) -> Result<Value> {
        let hash = Self::action_hash(
            action,
            self.vault_address.as_deref().unwrap_or(""),
            nonce,
            None,
        )?;
        let digest = Self::eip712_digest(hash, !self.base_url.contains("testnet"));
        let msg = SecpMessage::from_slice(&digest)
            .map_err(|e| ExchangeError::AuthError(e.to_string()))?;
        let secp = Secp256k1::new();
        let signature: RecoverableSignature = secp.sign_ecdsa_recoverable(&msg, &self.signing_key);
        let (rid, compact) = signature.serialize_compact();
        let v = (rid.to_i32() + 27) as u8;
        // 便于排查签名恢复结果
        if let Ok(recovered) = secp.recover_ecdsa(&msg, &signature) {
            let uncompressed = recovered.serialize_uncompressed();
            let addr_hash = Self::keccak(&uncompressed[1..]);
            let recovered_addr = format!("0x{}", hex::encode(&addr_hash[12..]));
            log::info!(
                "Hyperliquid sign debug: action_hash={} digest={} v={} recovered_addr={}",
                hex::encode(hash),
                hex::encode(digest),
                v,
                recovered_addr
            );
        } else {
            log::warn!("Hyperliquid sign debug: failed to recover address");
        }
        Ok(serde_json::json!({
            "r": format!("0x{}", hex::encode(&compact[..32])),
            "s": format!("0x{}", hex::encode(&compact[32..])),
            "v": v,
        }))
    }

    async fn info(&self, payload: &Value) -> Result<Value> {
        let resp = self
            .client
            .post(format!("{}/info", self.base_url))
            .json(payload)
            .send()
            .await
            .map_err(ExchangeError::NetworkError)?;

        if !resp.status().is_success() {
            let code = resp.status().as_u16() as i32;
            let err = resp.text().await.unwrap_or_default();
            return Err(ExchangeError::ApiError { code, message: err });
        }
        resp.json()
            .await
            .map_err(|e| ExchangeError::ParseError(e.to_string()))
    }

    async fn exchange<T: Serialize>(&self, action: &T) -> Result<Value> {
        let nonce = self.next_nonce();
        let signature = self.sign_action(&action, nonce)?;
        let mut payload = serde_json::json!({
            "action": action,
            "nonce": nonce,
            "signature": signature,
        });
        if let (Some(vault), Some(map)) = (self.vault_address.clone(), payload.as_object_mut()) {
            map.insert("vaultAddress".to_string(), Value::String(vault));
        }
        log::info!(
            "Hyperliquid exchange request url={}/exchange payload={}",
            self.base_url,
            payload
        );
        let resp = self
            .client
            .post(format!("{}/exchange", self.base_url))
            .json(&payload)
            .send()
            .await
            .map_err(ExchangeError::NetworkError)?;

        if !resp.status().is_success() {
            let code = resp.status().as_u16() as i32;
            let err = resp.text().await.unwrap_or_default();
            log::error!(
                "Hyperliquid exchange error {} body {} payload {}",
                code,
                err,
                payload
            );
            return Err(ExchangeError::ApiError { code, message: err });
        }
        resp.json()
            .await
            .map_err(|e| ExchangeError::ParseError(e.to_string()))
    }

    async fn get_asset_id(&self, coin: &str) -> Result<u32> {
        if let Some(id) = self.asset_map.read().get(coin) {
            return Ok(*id);
        }
        let meta = self.info(&serde_json::json!({"type": "meta"})).await?;
        let mut map = HashMap::new();
        if let Some(universe) = meta.get("universe").and_then(|u| u.as_array()) {
            for (idx, entry) in universe.iter().enumerate() {
                if let Some(name) = entry.get("name").and_then(|v| v.as_str()) {
                    map.insert(name.to_string(), idx as u32);
                }
            }
        }
        let mut guard = self.asset_map.write();
        *guard = map;
        guard
            .get(coin)
            .copied()
            .ok_or_else(|| ExchangeError::SymbolNotFound {
                symbol: coin.to_string(),
                market_type: MarketType::Futures,
            })
    }

    fn build_order_from_open(&self, item: &Value, market_type: MarketType) -> Result<Order> {
        let coin = item
            .get("coin")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ExchangeError::ParseError("缺少coin".to_string()))?;
        let symbol = self.normalize_symbol(coin);
        let side = match item.get("side").and_then(|s| s.as_str()) {
            Some("B") => OrderSide::Buy,
            _ => OrderSide::Sell,
        };
        let price = item
            .get("limitPx")
            .and_then(|p| p.as_str())
            .and_then(|s| s.parse::<f64>().ok());
        let amount = item
            .get("sz")
            .and_then(|s| s.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let oid = item
            .get("oid")
            .map(|v| v.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        Ok(Order {
            id: oid,
            symbol,
            side,
            order_type: OrderType::Limit,
            amount,
            price,
            filled: 0.0,
            remaining: amount,
            status: OrderStatus::Open,
            market_type,
            timestamp: Utc::now(),
            last_trade_timestamp: None,
            info: item.clone(),
        })
    }
}

#[async_trait]
impl Exchange for HyperliquidExchange {
    fn name(&self) -> &str {
        "hyperliquid"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn exchange_info(&self) -> Result<ExchangeInfo> {
        let meta = self.info(&serde_json::json!({"type": "meta"})).await?;
        let mut symbols = Vec::new();
        if let Some(universe) = meta.get("universe").and_then(|u| u.as_array()) {
            for entry in universe {
                if let Some(name) = entry.get("name").and_then(|v| v.as_str()) {
                    let symbol = self.normalize_symbol(name);
                    symbols.push(Symbol {
                        base: name.to_string(),
                        quote: "USDC".to_string(),
                        symbol,
                        market_type: MarketType::Futures,
                        exchange_specific: None,
                    });
                }
            }
        }
        Ok(ExchangeInfo {
            name: "hyperliquid".to_string(),
            symbols,
            currencies: vec!["USDC".to_string()],
            spot_enabled: false,
            futures_enabled: true,
        })
    }

    async fn get_balance(&self, _market_type: MarketType) -> Result<Vec<Balance>> {
        let resp = self
            .info(&serde_json::json!({"type": "clearinghouseState", "user": self.account_address}))
            .await?;
        let mut balances = Vec::new();
        if let Some(summary) = resp.get("marginSummary") {
            let total = summary
                .get("accountValue")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            let used = summary
                .get("totalMarginUsed")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            balances.push(Balance {
                currency: "USDC".to_string(),
                total,
                free: (total - used).max(0.0),
                used,
                market_type: MarketType::Futures,
            });
        }
        Ok(balances)
    }

    async fn get_ticker(&self, symbol: &str, _market_type: MarketType) -> Result<Ticker> {
        let coin = self.to_coin(symbol);
        let resp = self
            .info(&serde_json::json!({"type": "l2Book", "coin": coin}))
            .await?;

        let levels = resp
            .get("levels")
            .and_then(|v| v.as_array())
            .ok_or_else(|| ExchangeError::ParseError("缺少levels".to_string()))?;
        let bids: Vec<Value> = levels
            .get(0)
            .and_then(|b| b.as_array())
            .cloned()
            .unwrap_or_default();
        let asks: Vec<Value> = levels
            .get(1)
            .and_then(|a| a.as_array())
            .cloned()
            .unwrap_or_default();
        let best_bid = bids.get(0);
        let best_ask = asks.get(0);

        let bid = best_bid
            .and_then(|v| v.get("px"))
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let ask = best_ask
            .and_then(|v| v.get("px"))
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);

        Ok(Ticker {
            symbol: self.normalize_symbol(&coin),
            high: 0.0,
            low: 0.0,
            bid,
            ask,
            last: if bid > 0.0 && ask > 0.0 {
                (bid + ask) / 2.0
            } else {
                bid.max(ask)
            },
            volume: 0.0,
            timestamp: Utc::now(),
        })
    }

    async fn get_all_tickers(&self, market_type: MarketType) -> Result<Vec<Ticker>> {
        let mid = self.info(&serde_json::json!({"type": "allMids"})).await?;
        let mut result = Vec::new();
        if let Some(obj) = mid.as_object() {
            for (coin, px) in obj {
                if let Some(price) = px.as_str().and_then(|s| s.parse::<f64>().ok()) {
                    result.push(Ticker {
                        symbol: self.normalize_symbol(coin),
                        high: 0.0,
                        low: 0.0,
                        bid: price,
                        ask: price,
                        last: price,
                        volume: 0.0,
                        timestamp: Utc::now(),
                    });
                }
            }
        }
        if result.is_empty() {
            // 回退逐个查询少量符号
            let info = self.exchange_info().await?;
            for sym in info.symbols.iter().take(20) {
                if let Ok(t) = self.get_ticker(&sym.symbol, market_type).await {
                    result.push(t);
                }
            }
        }
        Ok(result)
    }

    async fn get_orderbook(
        &self,
        symbol: &str,
        _market_type: MarketType,
        limit: Option<u32>,
    ) -> Result<OrderBook> {
        let coin = self.to_coin(symbol);
        let resp = self
            .info(&serde_json::json!({"type": "l2Book", "coin": coin}))
            .await?;
        let levels = resp
            .get("levels")
            .and_then(|v| v.as_array())
            .ok_or_else(|| ExchangeError::ParseError("缺少levels".to_string()))?;
        let bids: Vec<Value> = levels
            .get(0)
            .and_then(|b| b.as_array())
            .cloned()
            .unwrap_or_default();
        let asks: Vec<Value> = levels
            .get(1)
            .and_then(|a| a.as_array())
            .cloned()
            .unwrap_or_default();
        let depth = limit.unwrap_or(50) as usize;
        let map_side = |side: &[Value]| -> Vec<[f64; 2]> {
            side.iter()
                .take(depth)
                .filter_map(|v| {
                    let px = v.get("px")?.as_str()?.parse::<f64>().ok()?;
                    let sz = v.get("sz")?.as_str()?.parse::<f64>().ok()?;
                    Some([px, sz])
                })
                .collect()
        };
        Ok(OrderBook {
            symbol: self.normalize_symbol(&coin),
            bids: map_side(&bids),
            asks: map_side(&asks),
            timestamp: Utc::now(),
        })
    }

    async fn create_order(&self, order_request: OrderRequest) -> Result<Order> {
        let coin = self.to_coin(&order_request.symbol);
        let asset = self.get_asset_id(&coin).await?;
        let is_buy = matches!(order_request.side, OrderSide::Buy);
        let tif = match order_request.time_in_force.as_deref() {
            Some("IOC") | Some("ioc") => "Ioc",
            Some("FOK") | Some("fok") => "Fok",
            _ => "Gtc",
        };
        let order_type = match order_request.order_type {
            OrderType::Market => serde_json::json!({"market": {}}),
            _ => serde_json::json!({"limit": {"tif": tif}}),
        };
        let price_num = order_request.price.unwrap_or_default();
        let wire = OrderWire {
            a: asset,
            b: is_buy,
            p: Self::float_to_wire(price_num),
            s: Self::float_to_wire(order_request.amount),
            r: order_request.reduce_only.unwrap_or(false),
            t: order_type,
            c: order_request.client_order_id.clone(),
        };
        let action = OrderAction {
            action_type: "order",
            orders: vec![wire],
            grouping: "na",
        };
        let resp = self.exchange(&action).await?;
        log::info!("Hyperliquid create_order resp: {}", resp);
        let statuses = resp
            .get("response")
            .and_then(|r| r.get("data"))
            .and_then(|d| d.get("statuses"))
            .and_then(|s| s.as_array())
            .cloned()
            .unwrap_or_default();
        let mut order_id = "unknown".to_string();
        if let Some(status) = statuses.get(0) {
            if let Some(rest) = status.get("resting") {
                order_id = rest
                    .get("oid")
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "unknown".to_string());
            } else if let Some(err) = status.get("error") {
                log::warn!("order rejected: {}", err);
            }
        }

        Ok(Order {
            id: order_id,
            symbol: order_request.symbol.clone(),
            side: order_request.side,
            order_type: order_request.order_type,
            amount: order_request.amount,
            price: order_request.price,
            filled: 0.0,
            remaining: order_request.amount,
            status: OrderStatus::Open,
            market_type: order_request.market_type,
            timestamp: Utc::now(),
            last_trade_timestamp: None,
            info: resp,
        })
    }

    async fn cancel_order(
        &self,
        order_id: &str,
        symbol: &str,
        _market_type: MarketType,
    ) -> Result<Order> {
        let coin = self.to_coin(symbol);
        let asset = self.get_asset_id(&coin).await?;
        let o = order_id.parse::<u64>().unwrap_or(0);
        let action = CancelAction {
            action_type: "cancel",
            cancels: vec![CancelWire { a: asset, o }],
        };
        let resp = self.exchange(&action).await?;
        Ok(Order {
            id: order_id.to_string(),
            symbol: self.normalize_symbol(&coin),
            side: OrderSide::Buy,
            order_type: OrderType::Limit,
            amount: 0.0,
            price: None,
            filled: 0.0,
            remaining: 0.0,
            status: OrderStatus::Canceled,
            market_type: MarketType::Futures,
            timestamp: Utc::now(),
            last_trade_timestamp: None,
            info: resp,
        })
    }

    async fn get_order(
        &self,
        order_id: &str,
        symbol: &str,
        market_type: MarketType,
    ) -> Result<Order> {
        let open = self.get_open_orders(Some(symbol), market_type).await?;
        if let Some(found) = open.into_iter().find(|o| o.id == order_id) {
            return Ok(found);
        }
        let hist = self
            .get_order_history(Some(symbol), market_type, Some(2000))
            .await?;
        hist.into_iter()
            .find(|o| o.id == order_id)
            .ok_or_else(|| ExchangeError::OrderNotFound {
                order_id: order_id.to_string(),
                symbol: symbol.to_string(),
            })
    }

    async fn get_open_orders(
        &self,
        symbol: Option<&str>,
        market_type: MarketType,
    ) -> Result<Vec<Order>> {
        let resp = self
            .info(&serde_json::json!({"type": "openOrders", "user": self.account_address}))
            .await?;
        let mut orders = Vec::new();
        if let Some(arr) = resp.as_array() {
            for item in arr {
                let order = self.build_order_from_open(item, market_type)?;
                if let Some(sym) = symbol {
                    if order.symbol != sym {
                        continue;
                    }
                }
                orders.push(order);
            }
        }
        Ok(orders)
    }

    async fn get_order_history(
        &self,
        symbol: Option<&str>,
        _market_type: MarketType,
        _limit: Option<u32>,
    ) -> Result<Vec<Order>> {
        let resp = self
            .info(&serde_json::json!({"type": "historicalOrders", "user": self.account_address}))
            .await
            .unwrap_or(Value::Null);
        let mut orders = Vec::new();
        if let Some(arr) = resp.as_array() {
            for item in arr {
                let coin = item.get("coin").and_then(|v| v.as_str()).unwrap_or("");
                let norm = self.normalize_symbol(coin);
                if let Some(sym) = symbol {
                    if sym != norm {
                        continue;
                    }
                }
                let side = match item.get("side").and_then(|s| s.as_str()) {
                    Some("B") => OrderSide::Buy,
                    _ => OrderSide::Sell,
                };
                let amount = item
                    .get("sz")
                    .and_then(|s| s.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                let price = item
                    .get("limitPx")
                    .and_then(|p| p.as_str())
                    .and_then(|s| s.parse::<f64>().ok());
                let filled = item
                    .get("filled")
                    .and_then(|p| p.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                let status = match item.get("status").and_then(|s| s.as_str()) {
                    Some("canceled") => OrderStatus::Canceled,
                    Some("filled") => OrderStatus::Closed,
                    Some("open") => OrderStatus::Open,
                    _ => OrderStatus::Pending,
                };
                let ts = item
                    .get("timestamp")
                    .and_then(|t| t.as_i64())
                    .and_then(DateTime::from_timestamp_millis)
                    .unwrap_or_else(Utc::now);
                orders.push(Order {
                    id: item
                        .get("oid")
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "unknown".to_string()),
                    symbol: norm,
                    side,
                    order_type: OrderType::Limit,
                    amount,
                    price,
                    filled,
                    remaining: (amount - filled).max(0.0),
                    status,
                    market_type: MarketType::Futures,
                    timestamp: ts,
                    last_trade_timestamp: None,
                    info: item.clone(),
                });
            }
        }
        Ok(orders)
    }

    async fn get_trades(
        &self,
        symbol: &str,
        _market_type: MarketType,
        limit: Option<u32>,
    ) -> Result<Vec<Trade>> {
        let coin = self.to_coin(symbol);
        let resp = self
            .info(&serde_json::json!({"type": "recentTrades", "coin": coin}))
            .await?;
        let mut trades = Vec::new();
        if let Some(arr) = resp.as_array() {
            for item in arr.iter().take(limit.unwrap_or(50) as usize) {
                let px = item
                    .get("px")
                    .and_then(|p| p.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                let sz = item
                    .get("sz")
                    .and_then(|s| s.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                let time = item
                    .get("time")
                    .and_then(|t| t.as_i64())
                    .and_then(DateTime::from_timestamp_millis)
                    .unwrap_or_else(Utc::now);
                let side = match item.get("side").and_then(|s| s.as_str()) {
                    Some("B") => OrderSide::Buy,
                    _ => OrderSide::Sell,
                };
                trades.push(Trade {
                    id: item
                        .get("hash")
                        .map(|h| h.to_string())
                        .unwrap_or_else(|| "unknown".to_string()),
                    symbol: self.normalize_symbol(&coin),
                    side,
                    amount: sz,
                    price: px,
                    timestamp: time,
                    order_id: item.get("oid").map(|o| o.to_string()),
                    fee: None,
                });
            }
        }
        Ok(trades)
    }

    async fn get_my_trades(
        &self,
        symbol: Option<&str>,
        _market_type: MarketType,
        limit: Option<u32>,
    ) -> Result<Vec<Trade>> {
        let resp = self
            .info(&serde_json::json!({"type": "userFills", "user": self.account_address}))
            .await?;
        let mut trades = Vec::new();
        if let Some(arr) = resp.as_array() {
            for item in arr.iter().take(limit.unwrap_or(200) as usize) {
                let coin = item.get("coin").and_then(|v| v.as_str()).unwrap_or("");
                let norm = self.normalize_symbol(coin);
                if let Some(sym) = symbol {
                    if sym != norm {
                        continue;
                    }
                }
                let px = item
                    .get("px")
                    .and_then(|p| p.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                let sz = item
                    .get("sz")
                    .and_then(|s| s.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                let time = item
                    .get("time")
                    .and_then(|t| t.as_i64())
                    .and_then(DateTime::from_timestamp_millis)
                    .unwrap_or_else(Utc::now);
                let side = match item.get("side").and_then(|s| s.as_str()) {
                    Some("B") => OrderSide::Buy,
                    _ => OrderSide::Sell,
                };
                trades.push(Trade {
                    id: item
                        .get("hash")
                        .map(|h| h.to_string())
                        .unwrap_or_else(|| "unknown".to_string()),
                    symbol: norm,
                    side,
                    amount: sz,
                    price: px,
                    timestamp: time,
                    order_id: item.get("oid").map(|o| o.to_string()),
                    fee: None,
                });
            }
        }
        Ok(trades)
    }

    async fn get_klines(
        &self,
        symbol: &str,
        interval: Interval,
        _market_type: MarketType,
        limit: Option<u32>,
    ) -> Result<Vec<Kline>> {
        let coin = self.to_coin(symbol);
        let end = Utc::now().timestamp_millis();
        let start = end - (limit.unwrap_or(200) as i64) * 60_000;
        let req = serde_json::json!({
            "type": "candleSnapshot",
            "req": {
                "coin": coin,
                "interval": interval.to_string(),
                "startTime": start,
                "endTime": end
            }
        });
        let resp = self.info(&req).await?;
        let mut klines = Vec::new();
        if let Some(arr) = resp.as_array() {
            for item in arr {
                let open_time = item
                    .get("t")
                    .and_then(|t| t.as_i64())
                    .and_then(DateTime::from_timestamp_millis)
                    .unwrap_or_else(Utc::now);
                let close_time = item
                    .get("T")
                    .and_then(|t| t.as_i64())
                    .and_then(DateTime::from_timestamp_millis)
                    .unwrap_or(open_time);
                let open = item
                    .get("o")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                let high = item
                    .get("h")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                let low = item
                    .get("l")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                let close = item
                    .get("c")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                let volume = item
                    .get("v")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                klines.push(Kline {
                    symbol: self.normalize_symbol(&coin),
                    interval: interval.to_string(),
                    open_time,
                    close_time,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    quote_volume: 0.0,
                    trade_count: item.get("n").and_then(|n| n.as_u64()).unwrap_or(0),
                });
            }
        }
        Ok(klines)
    }

    async fn get_24h_statistics(
        &self,
        symbol: &str,
        _market_type: MarketType,
    ) -> Result<Statistics24h> {
        let coin = self.to_coin(symbol);
        let stats = self
            .info(&serde_json::json!({"type": "metaAndAssetCtxs"}))
            .await
            .unwrap_or(Value::Null);
        let mut s = Statistics24h {
            symbol: self.normalize_symbol(&coin),
            open: 0.0,
            high: 0.0,
            low: 0.0,
            close: 0.0,
            volume: 0.0,
            quote_volume: 0.0,
            change: 0.0,
            change_percent: 0.0,
            timestamp: Utc::now(),
            price_change: None,
            price_change_percent: None,
            weighted_avg_price: None,
            open_price: None,
            high_price: None,
            low_price: None,
            close_price: None,
            count: None,
        };
        if let Some(arr) = stats.get(1).and_then(|v| v.as_array()) {
            if let Some(item) = arr.iter().find(|i| {
                i.get("coin")
                    .and_then(|c| c.as_str())
                    .map(|c| c.eq_ignore_ascii_case(&coin))
                    .unwrap_or(false)
            }) {
                s.close = item
                    .get("markPx")
                    .or_else(|| item.get("midPx"))
                    .or_else(|| item.get("prevDayPx"))
                    .and_then(|p| p.as_str())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                s.open = item
                    .get("prevDayPx")
                    .and_then(|p| p.as_str())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                s.high = s.close.max(s.open);
                s.low = s.close.min(s.open);
                s.volume = item
                    .get("dayNtlVlm")
                    .and_then(|p| p.as_str())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                if s.open > 0.0 {
                    s.change = s.close - s.open;
                    s.change_percent = s.change / s.open * 100.0;
                }
            }
        }
        Ok(s)
    }

    async fn get_all_24h_statistics(&self, market_type: MarketType) -> Result<Vec<Statistics24h>> {
        let info = self.exchange_info().await?;
        let mut result = Vec::new();
        for s in info.symbols {
            if let Ok(stat) = self.get_24h_statistics(&s.symbol, market_type).await {
                result.push(stat);
            }
        }
        Ok(result)
    }

    async fn get_trade_fee(&self, symbol: &str, _market_type: MarketType) -> Result<TradeFee> {
        Ok(TradeFee {
            symbol: symbol.to_string(),
            maker: 0.0002,
            taker: 0.0005,
            percentage: true,
            tier_based: false,
            maker_fee: Some(0.0002),
            taker_fee: Some(0.0005),
        })
    }

    async fn get_account_snapshot(&self, market_type: MarketType) -> Result<AccountSnapshot> {
        let balances = self.get_balance(market_type).await?;
        let total = balances.iter().map(|b| b.total).sum::<f64>();
        Ok(AccountSnapshot {
            account_type: "FUTURES".to_string(),
            balances: balances.clone(),
            timestamp: Utc::now(),
            total_balance_btc: Some(0.0),
            total_balance_usdt: Some(total),
        })
    }

    async fn get_positions(&self, symbol: Option<&str>) -> Result<Vec<Position>> {
        let resp = self
            .info(&serde_json::json!({"type": "clearinghouseState", "user": self.account_address}))
            .await?;
        let mut positions = Vec::new();
        if let Some(arr) = resp.get("assetPositions").and_then(|v| v.as_array()) {
            for item in arr {
                let pos = item.get("position").unwrap_or(item);
                let coin = pos.get("coin").and_then(|c| c.as_str()).unwrap_or("");
                let norm = self.normalize_symbol(coin);
                if let Some(sym) = symbol {
                    if sym != norm {
                        continue;
                    }
                }
                let size = pos
                    .get("szi")
                    .and_then(|s| s.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                if size.abs() < 1e-9 {
                    continue;
                }
                let entry = pos
                    .get("entryPx")
                    .and_then(|p| p.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                let mark = pos
                    .get("markPx")
                    .and_then(|p| p.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(entry);
                let unrealized = pos
                    .get("unrealizedPnl")
                    .and_then(|p| p.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                let margin = pos
                    .get("marginUsed")
                    .and_then(|p| p.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                positions.push(Position {
                    symbol: norm,
                    side: if size >= 0.0 {
                        "LONG".to_string()
                    } else {
                        "SHORT".to_string()
                    },
                    contracts: size.abs(),
                    contract_size: 1.0,
                    entry_price: entry,
                    mark_price: mark,
                    unrealized_pnl: unrealized,
                    percentage: if entry > 0.0 {
                        unrealized / (entry * size.abs()) * 100.0
                    } else {
                        0.0
                    },
                    margin,
                    margin_ratio: 0.0,
                    leverage: pos
                        .get("leverage")
                        .and_then(|l| l.get("value"))
                        .and_then(|v| v.as_u64())
                        .map(|v| v as u32),
                    margin_type: Some("CROSS".to_string()),
                    size: size.abs(),
                    amount: size.abs() * mark,
                    timestamp: Utc::now(),
                });
            }
        }
        Ok(positions)
    }

    async fn set_leverage(&self, symbol: &str, leverage: u32) -> Result<()> {
        let coin = self.to_coin(symbol);
        let asset = self.get_asset_id(&coin).await?;
        let action = UpdateLeverageAction {
            action_type: "updateLeverage",
            asset,
            is_cross: true,
            leverage,
        };
        self.exchange(&action).await?;
        Ok(())
    }

    async fn cancel_all_orders(
        &self,
        symbol: Option<&str>,
        market_type: MarketType,
    ) -> Result<Vec<Order>> {
        let open = self.get_open_orders(symbol, market_type).await?;
        let mut result = Vec::new();
        for o in open {
            if let Ok(cancelled) = self.cancel_order(&o.id, &o.symbol, market_type).await {
                result.push(cancelled);
            }
        }
        Ok(result)
    }

    async fn get_server_time(&self) -> Result<DateTime<Utc>> {
        Ok(Utc::now())
    }

    async fn ping(&self) -> Result<()> {
        let _ = self.exchange_info().await?;
        Ok(())
    }

    async fn create_batch_orders(
        &self,
        batch_request: BatchOrderRequest,
    ) -> Result<BatchOrderResponse> {
        if batch_request.orders.is_empty() {
            return Ok(BatchOrderResponse {
                successful_orders: vec![],
                failed_orders: vec![],
            });
        }
        let mut orders_payload = Vec::new();
        for o in &batch_request.orders {
            let coin = self.to_coin(&o.symbol);
            let asset = self.get_asset_id(&coin).await?;
            let tif = match o.time_in_force.as_deref() {
                Some("IOC") | Some("ioc") => "Ioc",
                Some("FOK") | Some("fok") => "Fok",
                _ => "Gtc",
            };
            let order_type = match o.order_type {
                OrderType::Market => serde_json::json!({"market": {}}),
                _ => serde_json::json!({"limit": {"tif": tif}}),
            };
            orders_payload.push(OrderWire {
                a: asset,
                b: matches!(o.side, OrderSide::Buy),
                p: Self::float_to_wire(o.price.unwrap_or_default()),
                s: Self::float_to_wire(o.amount),
                r: o.reduce_only.unwrap_or(false),
                t: order_type,
                c: o.client_order_id.clone(),
            });
        }
        let action = OrderAction {
            action_type: "order",
            orders: orders_payload,
            grouping: "na",
        };
        let resp = self.exchange(&action).await?;
        log::info!("Hyperliquid batch_order resp: {}", resp);
        let mut successful = Vec::new();
        if let Some(statuses) = resp
            .get("response")
            .and_then(|r| r.get("data"))
            .and_then(|d| d.get("statuses"))
            .and_then(|s| s.as_array())
        {
            for (idx, st) in statuses.iter().enumerate() {
                if let Some(resting) = st.get("resting") {
                    let oid = resting
                        .get("oid")
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "unknown".to_string());
                    let req = &batch_request.orders[idx];
                    successful.push(Order {
                        id: oid,
                        symbol: req.symbol.clone(),
                        side: req.side,
                        order_type: req.order_type,
                        amount: req.amount,
                        price: req.price,
                        filled: 0.0,
                        remaining: req.amount,
                        status: OrderStatus::Open,
                        market_type: batch_request.market_type,
                        timestamp: Utc::now(),
                        last_trade_timestamp: None,
                        info: st.clone(),
                    });
                }
            }
        }
        Ok(BatchOrderResponse {
            successful_orders: successful,
            failed_orders: Vec::new(),
        })
    }

    async fn get_all_spot_symbols(&self) -> Result<Vec<TradingPair>> {
        Ok(Vec::new())
    }

    async fn get_all_futures_symbols(&self) -> Result<Vec<TradingPair>> {
        let meta = self.info(&serde_json::json!({"type": "meta"})).await?;
        let mut result = Vec::new();
        if let Some(universe) = meta.get("universe").and_then(|u| u.as_array()) {
            for (idx, entry) in universe.iter().enumerate() {
                if let Some(name) = entry.get("name").and_then(|v| v.as_str()) {
                    let sz_dec = entry
                        .get("szDecimals")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(4) as u32;
                    let step_size = 10f64.powi(-(sz_dec as i32));
                    let tick_size = 0.01_f64;
                    {
                        let mut guard = self.asset_map.write();
                        guard.entry(name.to_string()).or_insert(idx as u32);
                    }
                    result.push(TradingPair {
                        symbol: format!("{}/USDC", name),
                        base_asset: name.to_string(),
                        quote_asset: "USDC".to_string(),
                        status: "TRADING".to_string(),
                        min_order_size: 0.0,
                        max_order_size: f64::MAX,
                        tick_size,
                        step_size,
                        min_notional: Some(0.0),
                        is_trading: true,
                        market_type: MarketType::Futures,
                    });
                }
            }
        }
        Ok(result)
    }

    async fn get_symbol_info(&self, symbol: &str, _market_type: MarketType) -> Result<TradingPair> {
        let coin = self.to_coin(symbol);
        let meta = self.info(&serde_json::json!({"type": "meta"})).await?;
        if let Some(universe) = meta.get("universe").and_then(|u| u.as_array()) {
            for (idx, entry) in universe.iter().enumerate() {
                if entry
                    .get("name")
                    .and_then(|v| v.as_str())
                    .map(|n| n.eq_ignore_ascii_case(&coin))
                    .unwrap_or(false)
                {
                    let sz_dec = entry
                        .get("szDecimals")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(4) as u32;
                    let step_size = 10f64.powi(-(sz_dec as i32));
                    let tick_size = 0.01_f64;
                    {
                        let mut guard = self.asset_map.write();
                        guard.entry(coin.to_uppercase()).or_insert(idx as u32);
                    }
                    return Ok(TradingPair {
                        symbol: self.normalize_symbol(&coin),
                        base_asset: coin.to_uppercase(),
                        quote_asset: "USDC".to_string(),
                        status: "TRADING".to_string(),
                        min_order_size: 0.0,
                        max_order_size: f64::MAX,
                        tick_size,
                        step_size,
                        min_notional: Some(0.0),
                        is_trading: true,
                        market_type: MarketType::Futures,
                    });
                }
            }
        }
        Err(ExchangeError::SymbolNotFound {
            symbol: symbol.to_string(),
            market_type: MarketType::Futures,
        })
    }

    async fn create_websocket_client(
        &self,
        _market_type: MarketType,
    ) -> Result<Box<dyn WebSocketClient>> {
        Ok(Box::new(HyperliquidWebSocketClient::new(
            self.ws_url.clone(),
        )))
    }
}

pub struct HyperliquidWebSocketClient {
    url: String,
    state: ConnectionState,
    stream: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    ping_message: String,
}

impl HyperliquidWebSocketClient {
    pub fn new(url: String) -> Self {
        Self {
            url,
            state: ConnectionState::Disconnected,
            stream: None,
            ping_message: r#"{"method":"ping"}"#.to_string(),
        }
    }
}

#[async_trait]
impl WebSocketClient for HyperliquidWebSocketClient {
    async fn connect(&mut self) -> Result<()> {
        self.state = ConnectionState::Connecting;
        let (stream, _) = connect_async(&self.url)
            .await
            .map_err(|e| ExchangeError::WebSocketError(e.to_string()))?;
        self.stream = Some(stream);
        self.state = ConnectionState::Connected;
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        if let Some(stream) = &mut self.stream {
            stream
                .close(None)
                .await
                .map_err(|e| ExchangeError::WebSocketError(e.to_string()))?;
        }
        self.state = ConnectionState::Disconnected;
        Ok(())
    }

    async fn send(&mut self, message: String) -> Result<()> {
        if let Some(stream) = &mut self.stream {
            stream
                .send(WsMessage::Text(message))
                .await
                .map_err(|e| ExchangeError::WebSocketError(e.to_string()))
        } else {
            Err(ExchangeError::Other("WebSocket未连接".to_string()))
        }
    }

    async fn receive(&mut self) -> Result<Option<String>> {
        if let Some(stream) = &mut self.stream {
            match stream.next().await {
                Some(Ok(WsMessage::Text(txt))) => Ok(Some(txt)),
                Some(Ok(WsMessage::Binary(bin))) => {
                    Ok(Some(String::from_utf8_lossy(&bin).to_string()))
                }
                Some(Ok(WsMessage::Ping(_))) => {
                    stream
                        .send(WsMessage::Pong(Vec::new()))
                        .await
                        .map_err(|e| ExchangeError::WebSocketError(e.to_string()))?;
                    Ok(None)
                }
                Some(Ok(WsMessage::Pong(_))) => Ok(None),
                Some(Ok(WsMessage::Close(_))) => {
                    self.state = ConnectionState::Disconnected;
                    Ok(None)
                }
                Some(Ok(_)) => Ok(None),
                Some(Err(e)) => Err(ExchangeError::WebSocketError(e.to_string())),
                None => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    async fn ping(&self) -> Result<()> {
        Ok(())
    }

    fn get_state(&self) -> ConnectionState {
        self.state.clone()
    }
}
