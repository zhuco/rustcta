// src/utils/order_id.rs

use rand::distributions::Alphanumeric;
use rand::Rng;

/// 生成一个指定长度的随机字母数字字符串作为订单ID
pub fn generate_order_id(length: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}
