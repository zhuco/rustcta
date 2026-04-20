#!/usr/bin/env python3
"""
ORDI/USDT 5 分钟历史拉取与网格回测脚本。

功能概览：
1. 通过 Binance 公共 REST API 拉取指定时间范围的 K 线数据（默认近 2 年）。
2. 将数据落盘为 CSV，便于复用与离线分析。
3. 使用简化的等差网格策略在历史数据上进行回测，统计收益、最大回撤、最大库存等指标。

注意事项：
- 当前运行环境若无法访问外部网络，将无法实时报价拉取，但依旧可以读取已有 CSV 进行回测。
- 回测假设在每根 K 线内部价格按 “开 -> 低 -> 高 -> 收”（多头）或 “开 -> 高 -> 低 -> 收”（空头）的顺序波动，
  用于推断限价单触发顺序；该假设与真实撮合存在偏差，请谨慎解读结果。
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import math
import os
import sys
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple

try:
    import requests  # type: ignore
except ImportError:  # pragma: no cover - 运行环境无 requests 时给出友好提示
    requests = None  # type: ignore


BINANCE_REST = "https://api.binance.com/api/v3/klines"
INTERVAL_MS = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
}


@dataclass
class Candle:
    open_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class Position:
    entry_price: float
    quantity: float
    target_price: float
    entry_time: int


@dataclass
class TradeRecord:
    entry_price: float
    exit_price: float
    quantity: float
    profit: float
    entry_time: int
    exit_time: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="获取 ORDIUSDT 5 分钟历史数据并执行简化网格回测",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--symbol", default="ORDIUSDT", help="交易对符号")
    parser.add_argument("--interval", default="5m", choices=INTERVAL_MS.keys(), help="K 线周期")
    parser.add_argument("--years", type=float, default=2.0, help="回溯年份")
    parser.add_argument("--output", default="data/ORDIUSDT_5m.csv", help="历史数据输出路径")
    parser.add_argument("--skip-fetch", action="store_true", help="跳过远程拉取，直接使用本地 CSV")
    parser.add_argument("--lower", type=float, default=5.0, help="网格下边界价格")
    parser.add_argument("--upper", type=float, default=100.0, help="网格上边界价格")
    parser.add_argument("--levels-per-side", type=int, default=12, help="单侧网格层数")
    parser.add_argument("--spacing-pct", type=float, default=None, help="网格间距（百分比，0.1 表示 0.1%%）")
    parser.add_argument("--order-notional", type=float, default=50.0, help="每单名义金额（USDT）")
    parser.add_argument("--max-inventory", type=float, default=5_000.0, help="最大持仓本金（USDT）")
    parser.add_argument("--initial-cash", type=float, default=None, help="初始现金（USDT），默认按最大持仓 1.2 倍设置")
    parser.add_argument(
        "--path-assumption",
        choices=("low-first", "high-first"),
        default="low-first",
        help="单根 K 线内的价格运行假设",
    )
    return parser.parse_args()


def ensure_data_folder(path: str) -> None:
    folder = os.path.dirname(os.path.abspath(path))
    if folder and not os.path.exists(folder):
        os.makedirs(folder, exist_ok=True)


def fetch_klines(
    symbol: str,
    interval: str,
    start_time_ms: int,
    end_time_ms: int,
    output_path: str,
) -> List[Candle]:
    if requests is None:
        raise RuntimeError("未检测到 requests 库，无法请求 Binance API。请安装 requests 或手动准备 CSV。")

    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": 1000,
        "startTime": start_time_ms,
    }
    all_rows: List[Candle] = []
    next_start = start_time_ms
    interval_ms = INTERVAL_MS[interval]

    while True:
        params["startTime"] = next_start
        params["endTime"] = end_time_ms
        resp = requests.get(BINANCE_REST, params=params, timeout=10)
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break

        for row in batch:
            candle = Candle(
                open_time=int(row[0]),
                open=float(row[1]),
                high=float(row[2]),
                low=float(row[3]),
                close=float(row[4]),
                volume=float(row[5]),
            )
            if candle.open_time > end_time_ms:
                break
            all_rows.append(candle)

        next_start = batch[-1][0] + interval_ms
        if next_start > end_time_ms:
            break

    ensure_data_folder(output_path)
    with open(output_path, "w", newline="") as fp:
        writer = csv.writer(fp)
        writer.writerow(["open_time", "open", "high", "low", "close", "volume"])
        for c in all_rows:
            writer.writerow([c.open_time, c.open, c.high, c.low, c.close, c.volume])

    return all_rows


def load_klines_from_csv(path: str) -> List[Candle]:
    candles: List[Candle] = []
    with open(path, newline="") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            candles.append(
                Candle(
                    open_time=int(row["open_time"]),
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=float(row["volume"]),
                )
            )
    if not candles:
        raise RuntimeError(f"文件 {path} 未包含任何有效数据")
    return candles


class GridBacktester:
    def __init__(
        self,
        lower: float,
        upper: float,
        levels_per_side: int,
        spacing_pct: Optional[float],
        order_notional: float,
        max_inventory: float,
        initial_cash: Optional[float],
        path_assumption: str,
    ) -> None:
        if upper <= lower:
            raise ValueError("upper 必须大于 lower")
        if levels_per_side <= 0:
            raise ValueError("levels_per_side 必须大于 0")
        self.lower = lower
        self.upper = upper
        self.levels_per_side = levels_per_side
        self.spacing_pct = spacing_pct
        self.order_notional = order_notional
        self.max_inventory = max_inventory
        self.initial_cash = initial_cash if initial_cash is not None else max_inventory * 1.2
        self.path_assumption = path_assumption

        self.center_price = (lower + upper) / 2
        if spacing_pct is not None:
            self.step = self.center_price * (spacing_pct / 100.0)
        else:
            self.step = (upper - lower) / (levels_per_side * 2)

        if self.step <= 0:
            raise ValueError("计算得到的网格步长无效")

        self.buy_levels = [
            self.center_price - self.step * i for i in range(1, levels_per_side + 1)
        ]
        self.buy_levels.sort(reverse=True)  # 便于下行遍历

        self.positions: Dict[float, List[Position]] = {}
        self.active_buy_flags: Dict[float, bool] = {lvl: True for lvl in self.buy_levels}

        self.cash = self.initial_cash
        self.inventory_qty = 0.0
        self.invested_capital = 0.0
        self.realized_pnl = 0.0
        self.trades: List[TradeRecord] = []
        self.equity_curve: List[Tuple[int, float]] = []
        self.max_inventory_notional = 0.0

    def _segment_cross_levels(self, start: float, end: float) -> Tuple[List[float], List[float]]:
        if math.isclose(start, end, rel_tol=1e-12, abs_tol=1e-12):
            return [], []
        if end < start:
            buy_hits = [lvl for lvl in self.buy_levels if end <= lvl <= start and self.active_buy_flags[lvl]]
            sell_hits: List[float] = []
        else:
            buy_hits = []
            sell_targets = []
            for target in sorted(self.positions.keys()):
                if start <= target <= end:
                    sell_targets.append(target)
            sell_hits = sell_targets
        return buy_hits, sell_hits  # buy 按 desc, sell 按 asc

    def _fill_buy(self, price: float, timestamp: int) -> None:
        if not self.active_buy_flags.get(price, False):
            return
        if self.invested_capital + self.order_notional > self.max_inventory:
            return
        quantity = self.order_notional / price
        if quantity <= 0:
            return

        self.cash -= self.order_notional
        self.inventory_qty += quantity
        self.invested_capital += self.order_notional

        target_price = price + self.step
        pos = Position(entry_price=price, quantity=quantity, target_price=target_price, entry_time=timestamp)
        self.positions.setdefault(target_price, []).append(pos)
        self.active_buy_flags[price] = False

    def _fill_sell(self, target_price: float, timestamp: int) -> None:
        positions = self.positions.get(target_price)
        if not positions:
            return
        for pos in positions:
            proceeds = pos.quantity * target_price
            cost = pos.quantity * pos.entry_price
            profit = proceeds - cost
            self.cash += proceeds
            self.inventory_qty -= pos.quantity
            self.invested_capital = max(0.0, self.invested_capital - cost)
            self.realized_pnl += profit
            self.trades.append(
                TradeRecord(
                    entry_price=pos.entry_price,
                    exit_price=target_price,
                    quantity=pos.quantity,
                    profit=profit,
                    entry_time=pos.entry_time,
                    exit_time=timestamp,
                )
            )
            self.active_buy_flags[pos.entry_price] = True
        del self.positions[target_price]

    def on_candle(self, candle: Candle) -> None:
        path: List[float]
        if self.path_assumption == "low-first":
            if candle.close >= candle.open:
                path = [candle.open, candle.low, candle.high, candle.close]
            else:
                path = [candle.open, candle.high, candle.low, candle.close]
        else:  # "high-first"
            if candle.close >= candle.open:
                path = [candle.open, candle.high, candle.low, candle.close]
            else:
                path = [candle.open, candle.low, candle.high, candle.close]

        current_price = path[0]
        for next_price in path[1:]:
            buy_hits, sell_hits = self._segment_cross_levels(current_price, next_price)

            if current_price > next_price:  # 价格向下，先处理买单
                for level in buy_hits:
                    self._fill_buy(level, candle.open_time)
            else:  # 价格向上，处理卖单
                for target in sell_hits:
                    self._fill_sell(target, candle.open_time)

            current_price = next_price

        equity = self.cash + self.inventory_qty * candle.close
        self.equity_curve.append((candle.open_time, equity))
        inventory_notional = self.inventory_qty * candle.close
        self.max_inventory_notional = max(self.max_inventory_notional, inventory_notional)

    def finalize(self) -> Dict[str, float]:
        if not self.equity_curve:
            raise RuntimeError("未处理任何 K 线，无法生成统计结果")

        equity_values = [eq for _, eq in self.equity_curve]
        initial_equity = self.equity_curve[0][1]
        final_equity = equity_values[-1]
        max_equity = equity_values[0]
        max_drawdown = 0.0
        for eq in equity_values:
            if eq > max_equity:
                max_equity = eq
            drawdown = (max_equity - eq) / max_equity if max_equity > 0 else 0.0
            if drawdown > max_drawdown:
                max_drawdown = drawdown

        gross_profit = sum(tr.profit for tr in self.trades if tr.profit > 0)
        gross_loss = sum(tr.profit for tr in self.trades if tr.profit < 0)

        return {
            "initial_equity": initial_equity,
            "final_equity": final_equity,
            "net_pnl": final_equity - initial_equity,
            "realized_pnl": self.realized_pnl,
            "max_drawdown_pct": max_drawdown * 100.0,
            "total_trades": len(self.trades),
            "gross_profit": gross_profit,
            "gross_loss": gross_loss,
            "max_inventory_notional": self.max_inventory_notional,
        }


def human_time(ms: int) -> str:
    return dt.datetime.utcfromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M")


def main() -> None:
    args = parse_args()
    interval_ms = INTERVAL_MS[args.interval]
    end_time = dt.datetime.utcnow()
    start_time = end_time - dt.timedelta(days=args.years * 365)
    start_ms = int(start_time.timestamp() * 1000)
    end_ms = int(end_time.timestamp() * 1000)

    if args.skip_fetch:
        if not os.path.exists(args.output):
            print(f"[错误] 找不到历史数据文件 {args.output}，请先运行脚本拉取数据或指定正确路径。", file=sys.stderr)
            sys.exit(1)
        candles = load_klines_from_csv(args.output)
    else:
        try:
            candles = fetch_klines(args.symbol, args.interval, start_ms, end_ms, args.output)
        except Exception as exc:  # pragma: no cover - 网络失败时提示
            print(f"[警告] 远程拉取失败：{exc}\n尝试使用本地文件 {args.output}...", file=sys.stderr)
            if not os.path.exists(args.output):
                print("[错误] 无法获取历史数据。请在可访问网络的环境运行或手动准备 CSV。", file=sys.stderr)
                sys.exit(1)
            candles = load_klines_from_csv(args.output)

    print(f"[信息] 历史数据覆盖范围：{human_time(candles[0].open_time)} -> {human_time(candles[-1].open_time)}")
    print(f"[信息] 共计 {len(candles)} 根 {args.interval} K 线。")

    backtester = GridBacktester(
        lower=args.lower,
        upper=args.upper,
        levels_per_side=args.levels_per_side,
        spacing_pct=args.spacing_pct,
        order_notional=args.order_notional,
        max_inventory=args.max_inventory,
        initial_cash=args.initial_cash,
        path_assumption=args.path_assumption,
    )

    for candle in candles:
        backtester.on_candle(candle)

    summary = backtester.finalize()
    print("\n=== 回测总结 ===")
    print(f"初始权益: {summary['initial_equity']:.2f} USDT")
    print(f"期末权益: {summary['final_equity']:.2f} USDT")
    print(f"净收益: {summary['net_pnl']:.2f} USDT")
    print(f"已实现盈亏: {summary['realized_pnl']:.2f} USDT")
    print(f"最大回撤: {summary['max_drawdown_pct']:.2f}%")
    print(f"成交笔数: {summary['total_trades']}")
    print(f"毛盈利: {summary['gross_profit']:.2f} USDT")
    print(f"毛亏损: {summary['gross_loss']:.2f} USDT")
    print(f"最大库存名义金额: {summary['max_inventory_notional']:.2f} USDT")

    if backtester.positions:
        remaining = sum(
            pos.quantity * pos.entry_price for positions in backtester.positions.values() for pos in positions
        )
        print(f"[提示] 仍有 {len(backtester.positions)} 个目标价待触发，当前未平仓成本合计约 {remaining:.2f} USDT。")


if __name__ == "__main__":
    main()

