#!/usr/bin/env python3
"""Compute correlation matrix for proposed hedged grid symbols."""

import math
import sys
import time
from typing import Dict, List

import requests

BINANCE_ENDPOINT = "https://fapi.binance.com/fapi/v1/klines"
INTERVAL = "15m"
LIMIT = 1500  # last 60 hours of 5m data

SYMBOL_GROUPS: Dict[str, Dict[str, str]] = {
    "long": {
        "ETHUSDC": "ETH/USDC",
        "BNBUSDC": "BNB/USDC",
        "SOLUSDC": "SOL/USDC",
        "XRPUSDC": "XRP/USDC",
        "LINKUSDC": "LINK/USDC",
        "AVAXUSDC": "AVAX/USDC",
        "NEARUSDC": "NEAR/USDC",
        "FILUSDC": "FIL/USDC",
        "TIAUSDC": "TIA/USDC",
        "ENAUSDC": "ENA/USDC",
        "ADAUSDC": "ADA/USDC",
        "AAVEUSDC": "AAVE/USDC",
        "UNIUSDC": "UNI/USDC",
        "SUIUSDC": "SUI/USDC",
        "LTCUSDC": "LTC/USDC",
    },
    "short": {
        "DOGEUSDC": "DOGE/USDC",
        "1000PEPEUSDC": "1000PEPE/USDC",
        "WLDUSDC": "WLD/USDC",
        "1000SHIBUSDC": "1000SHIB/USDC",
        "WIFUSDC": "WIF/USDC",
        "ORDIUSDC": "ORDI/USDC",
        "1000BONKUSDC": "1000BONK/USDC",
        "BOMEUSDC": "BOME/USDC",
        "CRVUSDC": "CRV/USDC",
        "KAITOUSDC": "KAITO/USDC",
        "IPUSDC": "IP/USDC",
        "TRUMPUSDC": "TRUMP/USDC",
        "PNUTUSDC": "PNUT/USDC",
        "PENGUUSDC": "PENGU/USDC",
        "WLFIUSDC": "WLFI/USDC",
    },
}


def fetch_closes(symbol: str) -> List[float]:
    params = {
        "symbol": symbol,
        "interval": INTERVAL,
        "limit": LIMIT,
    }
    resp = requests.get(BINANCE_ENDPOINT, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    if not data:
        raise ValueError(f"No kline data for {symbol}")
    closes = [float(item[4]) for item in data]
    return closes


def log_returns(prices: List[float]) -> List[float]:
    returns = []
    for prev, curr in zip(prices, prices[1:]):
        if prev <= 0 or curr <= 0:
            returns.append(0.0)
        else:
            returns.append(math.log(curr / prev))
    return returns


def corrcoef(series: Dict[str, List[float]]) -> Dict[str, Dict[str, float]]:
    symbols = list(series.keys())
    matrix: Dict[str, Dict[str, float]] = {s: {} for s in symbols}
    for i, sym_i in enumerate(symbols):
        xi = series[sym_i]
        mean_i = sum(xi) / len(xi)
        var_i = sum((x - mean_i) ** 2 for x in xi)
        for j, sym_j in enumerate(symbols):
            if j < i:
                matrix[sym_i][sym_j] = matrix[sym_j][sym_i]
                continue
            xj = series[sym_j]
            mean_j = sum(xj) / len(xj)
            var_j = sum((x - mean_j) ** 2 for x in xj)
            cov = sum((a - mean_i) * (b - mean_j) for a, b in zip(xi, xj))
            denom = math.sqrt(var_i * var_j) if var_i > 0 and var_j > 0 else 0.0
            corr = cov / denom if denom else 0.0
            matrix[sym_i][sym_j] = corr
            matrix[sym_j][sym_i] = corr
    return matrix


def main() -> None:
    print(f"Fetching {LIMIT} bars of {INTERVAL} data from Binance...")
    series: Dict[str, List[float]] = {}
    for group, symbols in SYMBOL_GROUPS.items():
        for api_symbol, label in symbols.items():
            try:
                closes = fetch_closes(api_symbol)
                returns = log_returns(closes)
                if len(returns) < 2:
                    raise ValueError("insufficient data")
                series[label] = returns
                print(f"  ✓ {label}: {len(returns)} returns")
                time.sleep(0.2)
            except Exception as exc:  # noqa: BLE001
                print(f"  ✗ {label} ({api_symbol}) failed: {exc}", file=sys.stderr)
                sys.exit(1)

    matrix = corrcoef(series)
    labels = list(series.keys())
    header = "symbol" + "".join(f"\t{lbl}" for lbl in labels)
    print("\nCorrelation matrix (log returns, {} interval, last ~60h):".format(INTERVAL))
    print(header)
    for lbl in labels:
        row = "\t".join(f"{matrix[lbl][other]: .3f}" for other in labels)
        print(f"{lbl}\t{row}")

    long_series = list(SYMBOL_GROUPS["long"].values())
    short_series = list(SYMBOL_GROUPS["short"].values())

    long_avg = [sum(vals) / len(vals) for vals in zip(*[series[label] for label in long_series])]
    short_avg = [sum(vals) / len(vals) for vals in zip(*[series[label] for label in short_series])]
    agg = corrcoef({"long_avg": long_avg, "short_avg": short_avg})
    print(
        "\nAggregate correlation (long basket vs short basket):"
        f" {agg['long_avg']['short_avg']:.3f}"
    )


if __name__ == "__main__":
    main()
