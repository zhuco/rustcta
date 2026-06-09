# Huobi Legacy Gateway Profile

Status: legacy `huobi` profile backed by the HTX gateway adapter implementation.

`huobi` is intentionally not a copied adapter. It uses the same REST/WS domains, V2 HMAC-SHA256 signing, parser helpers, request-spec fixtures, and capability boundaries as `htx`, but registers with exchange id `huobi` so gateway named registration can keep `htx` and `huobi` distinct.

Use `docs/交易所网关/适配器/htx_adapter.md` for the canonical endpoint and validation detail. Huobi-specific config is provided in `config/huobi_gateway_example.yml`, disabled by default.

## Official Position Detail

仓位接口核验见 [仓位接口官方核验 P1 第二批](../仓位接口官方核验_P1_第二批.md)。HTX/Huobi USDT-M swap profile 有 `/linear-swap-api/v1/swap_cross_position_info` 和 `positions_cross` private stream 语义；`huobi` legacy profile 现在通过 `HtxGatewayAdapter` 以 exchange id `huobi` 复用 signed REST position runtime。

写法：`get_positions=运行`。默认无 `HUOBI_PRIVATE_REST_ENABLED` 或 API key/secret 时仍 fail-closed；启用后发送 `POST /linear-swap-api/v1/swap_cross_position_info`，按 `contract_code` 可选过滤，解析 direction、volume、entry/mark/liquidation price、unrealized PnL 和 leverage。`positions_cross.*` 仍作为私有流对账线索记录；当前测试证明 mock REST/request-spec/parser/readback guard，不代表 live credential 已验证。

费率项目未实现/未启用：HTX/Huobi USDT-M `POST /linear-swap-api/v1/swap_fee` readback 已补 `request_specs/get_fees_linear_swap.json` 离线 request-spec 边界，但 legacy `huobi` profile 还没有接入共享 `get_fees` delegation、contract-code parser 和 read-only guard。

Validation:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/huobi/endpoint_mapping.yaml
cargo test -p rustcta-exchange-gateway htx --lib --message-format short
```

## P2 Position Runtime (2026-06-09)

Huobi legacy `get_positions` now reuses the guarded HTX USDT-M cross-position runtime under exchange id `huobi`. Focused tests cover signed query shape, request body, secret redaction, request-spec fixture matching, and parser output; matrix status is `运行`.
