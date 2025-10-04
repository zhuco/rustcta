#!/bin/bash
# 通过策略管理器启动所有策略

echo "开始启动所有策略..."

# 策略列表
strategies=(
  "trend_grid_ena_001"
  "trend_grid_ordi_001"
  "poisson_ltc_001"
  "poisson_avax_001"
  "poisson_near_001"
  "poisson_link_001"
  "poisson_doge_001"
  "as_ada_001"
  "as_sol_001"
  "as_xrp_001"
  "copy_trading_001"
)

# 逐个启动策略
for strategy in "${strategies[@]}"; do
  echo "启动策略: $strategy"
  curl -X POST "http://localhost:8888/api/strategies/$strategy/start" 2>/dev/null | jq -c
  sleep 2
done

echo "所有策略启动完成"

# 显示策略状态
echo ""
echo "当前策略状态:"
curl -s http://localhost:8888/api/strategies | jq -r '.data[] | "\(.id): \(.status)"'