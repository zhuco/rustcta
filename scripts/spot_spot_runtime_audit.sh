#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

LOG_DIR="${LOG_DIR:-logs/control_panel}"
SNAPSHOT_PATH="${SNAPSHOT_PATH:-data/control_api/dashboard_snapshot.json}"
LOG_FILE="${1:-}"

if [[ -z "$LOG_FILE" ]]; then
  LOG_FILE="$(ls -t "$LOG_DIR"/strategy_*.log 2>/dev/null | head -n 1 || true)"
fi

if [[ -z "$LOG_FILE" || ! -f "$LOG_FILE" ]]; then
  echo "strategy_log=missing"
  exit 1
fi

echo "strategy_log=$LOG_FILE"
echo "strategy_processes=$(pgrep -af 'spot-spot-arbitrage-runtime' | grep -v 'bash -c' | wc -l)"
pgrep -af 'spot-spot-arbitrage-runtime' || true

echo
echo "[orders]"
grep -E '订单结果 intent=|现货套利成交记录|现货套利库存低于目标|现货套利库存超目标|现货套利库存偏移|现货套利方向会加重库存偏移' "$LOG_FILE" | tail -n 80 || true

echo
echo "[summary]"
trade_count="$(grep -c '现货套利成交记录' "$LOG_FILE" || true)"
top_up_count="$(grep -c 'intent=inventory_top_up' "$LOG_FILE" || true)"
trim_count="$(grep -c 'intent=inventory_trim_excess' "$LOG_FILE" || true)"
rebalance_count="$(grep -Ec 'intent=(inventory_rebalance|blocked_inventory_rebalance)' "$LOG_FILE" || true)"
error_count="$(grep -Ec 'spot live order submit failed|validation failed for balance|effective_available=-|ERROR|WARN' "$LOG_FILE" || true)"
last_report="$(grep 'spot_spot_taker_arbitrage report' "$LOG_FILE" | tail -n 1 || true)"
last_trade="$(grep '现货套利成交记录' "$LOG_FILE" | tail -n 1 || true)"
now_epoch="$(date -u +%s)"
last_report_epoch=""
last_trade_epoch=""
if [[ -n "$last_report" ]]; then
  last_report_epoch="$(date -u -d "$(printf '%s\n' "$last_report" | sed -n 's/^\[\([^ ]*\) .*/\1/p')" +%s 2>/dev/null || true)"
fi
if [[ -n "$last_trade" ]]; then
  last_trade_epoch="$(date -u -d "$(printf '%s\n' "$last_trade" | sed -n 's/^\[\([^ ]*\) .*/\1/p')" +%s 2>/dev/null || true)"
fi
net_pnl_sum="$(grep '现货套利成交记录' "$LOG_FILE" | awk '
  {
    for (i = 1; i <= NF; i++) {
      if ($i ~ /^net_pnl=/) {
        split($i, a, "=");
        sum += a[2];
      }
    }
  }
  END { printf "%.8f", sum + 0 }
')"
echo "trade_count=$trade_count"
echo "net_pnl_sum=$net_pnl_sum"
echo "inventory_top_up_results=$top_up_count"
echo "inventory_trim_results=$trim_count"
echo "inventory_rebalance_results=$rebalance_count"
echo "error_count=$error_count"
if [[ -n "$last_trade_epoch" ]]; then
  echo "seconds_since_last_trade=$((now_epoch - last_trade_epoch))"
else
  echo "seconds_since_last_trade=none"
fi
if [[ -n "$last_report_epoch" ]]; then
  echo "seconds_since_last_report=$((now_epoch - last_report_epoch))"
else
  echo "seconds_since_last_report=none"
fi
echo "last_trade=${last_trade:-none}"
echo "last_report=${last_report:-none}"

echo
echo "[balances]"
if [[ -f "$SNAPSHOT_PATH" ]] && command -v jq >/dev/null 2>&1; then
  jq -r '
    paths as $p
    | select(getpath($p) | type == "object")
    | getpath($p)
    | select(.exchange? and .asset? and (.asset == "VSN" or .asset == "USDT"))
    | [.exchange, .asset, .total, .available, .locally_reserved, .effective_available]
    | @tsv
  ' "$SNAPSHOT_PATH" | sort -u
else
  echo "snapshot_unavailable path=$SNAPSHOT_PATH"
fi
