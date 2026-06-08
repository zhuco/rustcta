# Huobi Legacy Gateway Profile

Status: legacy `huobi` profile backed by the HTX gateway adapter implementation.

`huobi` is intentionally not a copied adapter. It uses the same REST/WS domains, V2 HMAC-SHA256 signing, parser helpers, request-spec fixtures, and capability boundaries as `htx`, but registers with exchange id `huobi` so gateway named registration can keep `htx` and `huobi` distinct.

Use `docs/交易所网关/适配器/htx_adapter.md` for the canonical endpoint and validation detail. Huobi-specific config is provided in `config/huobi_gateway_example.yml`, disabled by default.

## Official Position Detail

仓位接口核验见 [仓位接口官方核验 P1 第二批](../仓位接口官方核验_P1_第二批.md)。HTX/Huobi USDT-M swap profile 有 `/linear-swap-api/v1/swap_cross_position_info` 和 `positions_cross` private stream 语义；但 `huobi` legacy profile 当前矩阵仍是 `get_positions=-`，不能直接把 `htx` runtime 算作 `huobi` 已实现。

写法：官方支持，项目未实现/未启用。后续补 `huobi` profile 的 `get_positions` endpoint mapping、request spec/parser delegation 和 REST/WS reconciliation 后，再把矩阵改成运行实现。

Validation:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/huobi/endpoint_mapping.yaml
cargo test -p rustcta-exchange-gateway htx --lib --message-format short
```
