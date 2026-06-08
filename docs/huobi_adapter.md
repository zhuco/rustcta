# Huobi Legacy Gateway Profile

Status: legacy `huobi` profile backed by the HTX gateway adapter implementation.

`huobi` is intentionally not a copied adapter. It uses the same REST/WS domains, V2 HMAC-SHA256 signing, parser helpers, request-spec fixtures, and capability boundaries as `htx`, but registers with exchange id `huobi` so gateway named registration can keep `htx` and `huobi` distinct.

Use `docs/htx_adapter.md` for the canonical endpoint and validation detail. Huobi-specific config is provided in `config/huobi_gateway_example.yml`, disabled by default.

Validation:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/huobi/endpoint_mapping.yaml
cargo test -p rustcta-exchange-gateway htx --lib --message-format short
```
