#!/usr/bin/env bash
set -euo pipefail

GATEWAY_URL="${RUSTCTA_GATEWAY_URL:-http://127.0.0.1:18081}"
TENANT_ID="${RUSTCTA_TENANT_ID:-server}"
ACCOUNT_ID="${RUSTCTA_ACCOUNT_ID:-default}"
RUN_ID="${RUSTCTA_RUN_ID:-server-live}"
EXCHANGE_ID="${RUSTCTA_EXCHANGE_ID:-binance}"
SYMBOL="${RUSTCTA_SYMBOL:-SOLUSDC}"
MARKET_TYPE="${RUSTCTA_MARKET_TYPE:-perpetual}"

now_utc() {
    date -u +"%Y-%m-%dT%H:%M:%SZ"
}

post_platform_json() {
    local path="$1"
    local payload="$2"
    local body_file
    local status
    local body
    body_file="$(mktemp)"
    status="$(curl -sS \
        -H "content-type: application/json" \
        -X POST \
        --data "${payload}" \
        -o "${body_file}" \
        -w "%{http_code}" \
        "${GATEWAY_URL}${path}")"
    body="$(<"${body_file}")"
    rm -f "${body_file}"
    if [[ "${status}" -lt 200 || "${status}" -ge 300 ]]; then
        printf 'platform_request_failed path=%s status=%s body=%s\n' "${path}" "${status}" "${body}" >&2
        return 22
    fi
    printf '%s' "${body}"
}

request_json() {
    local extra_fields="$1"
    printf '{"schema_version":1,"tenant_id":"%s","account_id":"%s","run_id":"%s","exchange_id":"%s","symbol":"%s","market_type":"%s","requested_at":"%s"%s}' \
        "${TENANT_ID}" \
        "${ACCOUNT_ID}" \
        "${RUN_ID}" \
        "${EXCHANGE_ID}" \
        "${SYMBOL}" \
        "${MARKET_TYPE}" \
        "$(now_utc)" \
        "${extra_fields}"
}

echo "gateway=${GATEWAY_URL}"
curl -fsS "${GATEWAY_URL}/health" >/dev/null

snapshot_payload="$(request_json "")"
snapshot_response="$(post_platform_json "/strategy-platform/market-snapshot" "${snapshot_payload}")"
printf '%s' "${snapshot_response}" | python3 -c '
import json
import sys

payload = json.load(sys.stdin)
exchange_id = payload.get("exchange_id")
symbol = payload.get("symbol")
best_bid = payload.get("best_bid")
best_ask = payload.get("best_ask")
tick_size = payload.get("tick_size")
step_size = payload.get("step_size")
min_notional = payload.get("min_notional")
print(
    "market_snapshot={}:{} bid={} ask={} tick={} step={} min_notional={}".format(
        exchange_id,
        symbol,
        best_bid,
        best_ask,
        tick_size,
        step_size,
        min_notional,
    )
)
if payload.get("market_type") != "perpetual":
    print("unexpected market_type={}".format(payload.get("market_type")), file=sys.stderr)
    sys.exit(2)
'

account_payload="$(request_json "")"
account_response="$(post_platform_json "/strategy-platform/account-config" "${account_payload}")"
printf '%s' "${account_response}" | python3 -c '
import json
import sys

payload = json.load(sys.stdin)
position_mode = payload.get("position_mode")
margin_mode = payload.get("margin_mode")
leverage = payload.get("leverage")
print(
    "account_config="
    f"position_mode={position_mode} margin_mode={margin_mode} leverage={leverage}"
)
if position_mode != "hedge":
    print("SOLUSDC contract grid requires Binance USD-M hedge position mode", file=sys.stderr)
    sys.exit(3)
'

echo "preflight=ok"
