#!/usr/bin/env python3
"""Paper-trade Binance event-contract style GPT predictions.

This script uses public Binance market data only. It never reads exchange API
keys, never signs requests, and never submits orders.
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import json
import math
import os
import socket
import ssl
import sqlite3
import struct
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_SYMBOLS = ("BTCUSDT", "ETHUSDT")
DEFAULT_HORIZONS_MINUTES = (10, 30, 60)


def load_local_env_file() -> None:
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if not env_path.exists():
        return
    with env_path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


load_local_env_file()

DEFAULT_OPENAI_CHAT_URL = None
DEFAULT_MODEL = os.environ.get("OPENAI_MODEL", "gpt-5.5")
USER_AGENT = "rustcta-binance-event-gpt-simulator/0.1"

SYSTEM_PROMPT = """You are a second-pass risk reviewer for Binance BTC/ETH event-contract paper trading, not financial advice.

The program computes market regime, support/resistance, VWAP, EMA, ATR, ADX, range position, volume, and a rule_gate for each symbol and horizon. Your job is not to invent trades. Your job is to audit the rule_gate.

Strict rules:
1. Use only the input JSON. Do not assume external news, on-chain data, macro data, or order-book data not provided.
2. Each requested symbol+horizon must return exactly one JSON prediction object.
3. If rule_gate.allow_trade is false, output decision NO_TRADE, direction NONE, probability 0.50 to 0.58.
4. If rule_gate.allow_trade is true, you may output TRADE only in rule_gate.allowed_direction. Otherwise output NO_TRADE.
5. TRADE is allowed only when price is near a meaningful support/resistance level, a confirmed retest, or a trend pullback near VWAP/EMA. Do not trade in the middle of a range.
6. Reject trades during high-volatility chop, after extended spikes/dumps, when BTC/ETH signals conflict, or when the key-level logic is weak.
7. probability is the calibrated chance that the selected direction is correct at expiry. It must be 0.50 to 0.90.
8. Assign probability >= 0.65 only for a clean key-level setup with regime, VWAP/EMA, momentum, and volatility mostly aligned.
9. For weak but allowed setups, use 0.60 to 0.64. For no edge, use NO_TRADE and probability below 0.60.
10. Output JSON only. No Markdown.
"""


@dataclass(frozen=True)
class Candle:
    open_time_ms: int
    close_time_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(frozen=True)
class PricePoint:
    exchange_time_ms: int
    local_time_ms: int
    price: float
    source: str


@dataclass(frozen=True)
class Prediction:
    symbol: str
    horizon_minutes: int
    decision: str
    direction: str
    probability: float
    setup: str
    reject_reason: str
    rationale: str
    risk_flags: list[str]
    raw: dict[str, Any]


@dataclass
class Trade:
    trade_id: str
    symbol: str
    horizon_minutes: int
    direction: str
    probability: float
    signal_threshold: float
    entry_epoch_ms: int
    entry_exchange_epoch_ms: int
    entry_price: float
    entry_price_source: str
    expiry_epoch_ms: int
    rationale: str
    risk_flags: list[str]
    status: str = "OPEN"
    settled_epoch_ms: int | None = None
    expiry_price: float | None = None
    settlement_lag_ms: int | None = None
    success: bool | None = None
    return_pct: float | None = None
    pnl_units: float | None = None

    def to_row(self, session_id: str) -> dict[str, Any]:
        return {
            "session_id": session_id,
            "trade_id": self.trade_id,
            "symbol": self.symbol,
            "horizon_minutes": self.horizon_minutes,
            "direction": self.direction,
            "probability": f"{self.probability:.4f}",
            "signal_threshold": f"{self.signal_threshold:.4f}",
            "entry_time_utc": iso_utc_ms(self.entry_epoch_ms),
            "entry_epoch_ms": self.entry_epoch_ms,
            "entry_exchange_time_utc": iso_utc_ms(self.entry_exchange_epoch_ms),
            "entry_exchange_epoch_ms": self.entry_exchange_epoch_ms,
            "entry_price": f"{self.entry_price:.8f}",
            "entry_price_source": self.entry_price_source,
            "expiry_time_utc": iso_utc_ms(self.expiry_epoch_ms),
            "expiry_epoch_ms": self.expiry_epoch_ms,
            "status": self.status,
            "settled_time_utc": (
                iso_utc_ms(self.settled_epoch_ms) if self.settled_epoch_ms else ""
            ),
            "settled_epoch_ms": self.settled_epoch_ms or "",
            "expiry_price": (
                f"{self.expiry_price:.8f}" if self.expiry_price is not None else ""
            ),
            "settlement_lag_ms": self.settlement_lag_ms or "",
            "success": "" if self.success is None else str(self.success).lower(),
            "return_pct": f"{self.return_pct:.6f}" if self.return_pct is not None else "",
            "pnl_units": f"{self.pnl_units:.4f}" if self.pnl_units is not None else "",
            "rationale": self.rationale,
            "risk_flags": "|".join(self.risk_flags),
        }


class MarketState:
    def __init__(self, symbols: list[str], max_bars: int) -> None:
        self.symbols = symbols
        self.prices: dict[str, PricePoint] = {}
        self.klines: dict[str, deque[Candle]] = {
            symbol: deque(maxlen=max_bars) for symbol in symbols
        }

    def load_candles(self, symbol: str, candles: list[Candle]) -> None:
        bars = self.klines[symbol]
        for candle in candles:
            self._append_or_replace_candle(bars, candle)
        if candles and symbol not in self.prices:
            last = candles[-1]
            self.prices[symbol] = PricePoint(
                exchange_time_ms=last.close_time_ms,
                local_time_ms=now_ms(),
                price=last.close,
                source="rest_kline_close",
            )

    def update_rest_index_price(
        self,
        symbol: str,
        price: float,
        exchange_time_ms: int,
        source: str = "rest_premium_index",
    ) -> None:
        if symbol not in self.klines:
            return
        self.prices[symbol] = PricePoint(
            exchange_time_ms=exchange_time_ms,
            local_time_ms=now_ms(),
            price=price,
            source=source,
        )

    def update_from_stream_payload(self, payload: dict[str, Any]) -> None:
        data = payload.get("data", payload)
        event_type = data.get("e")
        if event_type == "markPriceUpdate":
            self._update_mark_price(data)
        elif event_type == "kline":
            self._update_kline(data)
        elif {"s", "b", "a"}.issubset(data):
            self._update_book_ticker(data)

    def current_price(self, symbol: str, max_age_ms: int) -> PricePoint | None:
        point = self.prices.get(symbol)
        if point is None:
            return None
        if now_ms() - point.local_time_ms > max_age_ms:
            return None
        return point

    def ready(self, min_bars: int, max_price_age_ms: int) -> tuple[bool, str]:
        missing_prices = [
            symbol
            for symbol in self.symbols
            if self.current_price(symbol, max_price_age_ms) is None
        ]
        if missing_prices:
            return False, f"waiting for fresh prices: {','.join(missing_prices)}"

        missing_bars = [
            f"{symbol}:{len(self.klines[symbol])}/{min_bars}"
            for symbol in self.symbols
            if len(self.klines[symbol]) < min_bars
        ]
        if missing_bars:
            return False, f"warming 1m bars: {','.join(missing_bars)}"

        return True, "ready"

    def prompt_snapshot(self, horizons: list[int], max_price_age_ms: int) -> dict[str, Any]:
        return {
            "timestamp_utc": iso_utc_ms(now_ms()),
            "price_source_for_entry_and_settlement": (
                "Binance USD-M futures index price from markPrice stream field i; "
                "REST /fapi/v1/premiumIndex indexPrice is used as fallback and "
                "forced again after the model response for entry recording. "
                "1m indicators use USD-M futures klines with REST fallback."
            ),
            "direction_definition": (
                "UP succeeds if expiry index_price is strictly greater than "
                "entry index_price; DOWN succeeds if strictly lower."
            ),
            "horizons_minutes": horizons,
            "symbols": [
                self._symbol_snapshot(symbol, max_price_age_ms, horizons)
                for symbol in self.symbols
            ],
        }

    def _update_mark_price(self, data: dict[str, Any]) -> None:
        symbol = str(data.get("s", "")).upper()
        if symbol not in self.klines:
            return
        price_text = data.get("i") or data.get("p")
        try:
            price = float(price_text)
        except (TypeError, ValueError):
            return
        self.prices[symbol] = PricePoint(
            exchange_time_ms=int(data.get("E") or now_ms()),
            local_time_ms=now_ms(),
            price=price,
            source="mark_price_index",
        )

    def _update_book_ticker(self, data: dict[str, Any]) -> None:
        symbol = str(data.get("s", "")).upper()
        if symbol not in self.klines:
            return
        try:
            bid = float(data["b"])
            ask = float(data["a"])
        except (KeyError, TypeError, ValueError):
            return
        if bid <= 0 or ask <= 0:
            return
        self.prices[symbol] = PricePoint(
            exchange_time_ms=now_ms(),
            local_time_ms=now_ms(),
            price=(bid + ask) / 2.0,
            source="spot_book_ticker_mid",
        )

    def _update_kline(self, data: dict[str, Any]) -> None:
        kline = data.get("k") or {}
        if not kline.get("x"):
            return
        symbol = str(kline.get("s") or data.get("s") or "").upper()
        if symbol not in self.klines:
            return
        try:
            candle = Candle(
                open_time_ms=int(kline["t"]),
                close_time_ms=int(kline["T"]),
                open=float(kline["o"]),
                high=float(kline["h"]),
                low=float(kline["l"]),
                close=float(kline["c"]),
                volume=float(kline["v"]),
            )
        except (KeyError, TypeError, ValueError):
            return
        self._append_or_replace_candle(self.klines[symbol], candle)

    @staticmethod
    def _append_or_replace_candle(bars: deque[Candle], candle: Candle) -> None:
        if bars and bars[-1].open_time_ms == candle.open_time_ms:
            bars[-1] = candle
            return
        if bars and bars[-1].open_time_ms > candle.open_time_ms:
            return
        bars.append(candle)

    def _symbol_snapshot(
        self,
        symbol: str,
        max_price_age_ms: int,
        horizons: list[int],
    ) -> dict[str, Any]:
        bars = list(self.klines[symbol])
        price_point = self.current_price(symbol, max_price_age_ms)
        current = price_point.price if price_point else (bars[-1].close if bars else None)
        closes = [bar.close for bar in bars]
        volumes = [bar.volume for bar in bars]

        snapshot: dict[str, Any] = {
            "symbol": symbol,
            "entry_index_price": round_float(current),
            "price_age_ms": now_ms() - price_point.local_time_ms if price_point else None,
            "bars_1m_available": len(bars),
            "last_closed_bar_utc": iso_utc_ms(bars[-1].close_time_ms) if bars else None,
            "returns_pct": {},
            "ema_distance_pct": {},
            "realized_volatility_pct": {},
            "volume": {},
            "range_position": {},
            "key_levels": {},
            "vwap": {},
            "trend": {},
            "volatility": {},
            "rule_gates": {},
            "last_bar": None,
        }
        if current is None or not bars:
            return snapshot

        for minutes in (1, 3, 5, 10, 15, 30, 60, 120):
            snapshot["returns_pct"][f"{minutes}m"] = pct_from_history(
                current, closes, minutes
            )

        for period in (9, 21, 55):
            ema_value = ema(closes, period)
            snapshot["ema_distance_pct"][f"ema_{period}"] = (
                pct_change(current, ema_value) if ema_value else None
            )

        for window in (10, 30, 60):
            snapshot["realized_volatility_pct"][f"{window}m"] = realized_vol_pct(
                closes, window
            )

        key_levels = key_level_snapshot(current, bars)
        trend = trend_snapshot(current, closes, bars)
        volatility = volatility_snapshot(current, bars)
        vwap_data = vwap_snapshot(current, bars, 60)
        snapshot["volume"] = volume_snapshot(volumes)
        snapshot["range_position"] = {
            "position_30m": range_position(current, bars, 30),
            "position_60m": range_position(current, bars, 60),
        }
        snapshot["key_levels"] = key_levels
        snapshot["vwap"] = vwap_data
        snapshot["trend"] = trend
        snapshot["volatility"] = volatility
        snapshot["rule_gates"] = {
            f"{horizon}m": rule_gate_for_horizon(
                horizon_minutes=horizon,
                current=current,
                returns=snapshot["returns_pct"],
                range_positions=snapshot["range_position"],
                key_levels=key_levels,
                trend=trend,
                volatility=volatility,
                vwap_data=vwap_data,
            )
            for horizon in horizons
        }
        last = bars[-1]
        snapshot["last_bar"] = {
            "open": round_float(last.open),
            "high": round_float(last.high),
            "low": round_float(last.low),
            "close": round_float(last.close),
            "volume": round_float(last.volume),
            "return_pct": pct_change(last.close, last.open),
            "range_pct": pct_change(last.high, last.low),
        }
        return snapshot


class OpenAIChatClient:
    def __init__(
        self,
        api_key: str,
        model: str,
        url: str,
        timeout_seconds: float,
        proxy_url: str | None,
        use_json_schema: bool,
    ) -> None:
        self.api_key = api_key
        self.model = model
        self.url = url
        self.timeout_seconds = timeout_seconds
        self.use_json_schema = use_json_schema
        self.opener = build_url_opener(proxy_url)

    def predict(
        self,
        snapshot: dict[str, Any],
        symbols: list[str],
        horizons: list[int],
    ) -> tuple[list[Prediction], str]:
        payload: dict[str, Any] = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": build_user_prompt(snapshot)},
            ],
            "temperature": 0.1,
        }
        if self.use_json_schema:
            payload["response_format"] = {
                "type": "json_schema",
                "json_schema": {
                    "name": "event_direction_predictions",
                    "strict": True,
                    "schema": prediction_response_schema(symbols, horizons),
                },
            }
        else:
            payload["response_format"] = {"type": "json_object"}

        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            self.url,
            data=body,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": USER_AGENT,
            },
            method="POST",
        )
        try:
            with self.opener.open(request, timeout=self.timeout_seconds) as response:
                raw_response = response.read().decode("utf-8")
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"OpenAI HTTP {error.code}: {detail[:1000]}") from error

        parsed = json.loads(raw_response)
        content = parsed["choices"][0]["message"]["content"]
        raw_prediction = json.loads(content)
        return normalize_predictions(raw_prediction, symbols, horizons), content


class SqliteStore:
    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self.conn = sqlite3.connect(str(db_path), timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._init_schema()

    def append_prediction(self, row: dict[str, Any]) -> None:
        fields = [
            "session_id",
            "prediction_time_utc",
            "prediction_epoch_ms",
            "symbol",
            "horizon_minutes",
            "decision",
            "direction",
            "probability",
            "setup",
            "reject_reason",
            "accepted",
            "entry_price",
            "entry_price_time_utc",
            "entry_price_epoch_ms",
            "entry_exchange_time_utc",
            "entry_exchange_epoch_ms",
            "entry_price_source",
            "trade_id",
            "accepted_thresholds",
            "rationale",
            "risk_flags",
            "raw_prediction_json",
        ]
        values = [row.get(field, "") for field in fields]
        placeholders = ",".join("?" for _ in fields)
        self.conn.execute(
            f"INSERT INTO predictions ({','.join(fields)}) VALUES ({placeholders})",
            values,
        )
        self.conn.commit()

    def upsert_trades(self, trades: list[Trade], session_id: str) -> None:
        fields = [
            "session_id",
            "trade_id",
            "symbol",
            "horizon_minutes",
            "direction",
            "probability",
            "signal_threshold",
            "entry_time_utc",
            "entry_epoch_ms",
            "entry_exchange_time_utc",
            "entry_exchange_epoch_ms",
            "entry_price",
            "entry_price_source",
            "expiry_time_utc",
            "expiry_epoch_ms",
            "status",
            "settled_time_utc",
            "settled_epoch_ms",
            "expiry_price",
            "settlement_lag_ms",
            "success",
            "return_pct",
            "pnl_units",
            "rationale",
            "risk_flags",
        ]
        update_fields = [field for field in fields if field != "trade_id"]
        sql = (
            f"INSERT INTO trades ({','.join(fields)}) VALUES "
            f"({','.join('?' for _ in fields)}) "
            "ON CONFLICT(trade_id) DO UPDATE SET "
            + ",".join(f"{field}=excluded.{field}" for field in update_fields)
        )
        rows = []
        for trade in trades:
            row = trade.to_row(session_id)
            rows.append([row.get(field, "") for field in fields])
        if rows:
            self.conn.executemany(sql, rows)
            self.conn.commit()

    def _init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS predictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                prediction_time_utc TEXT NOT NULL,
                prediction_epoch_ms INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                horizon_minutes INTEGER NOT NULL,
                decision TEXT,
                direction TEXT NOT NULL,
                probability REAL NOT NULL,
                setup TEXT,
                reject_reason TEXT,
                accepted TEXT NOT NULL,
                entry_price REAL,
                entry_price_time_utc TEXT,
                entry_price_epoch_ms INTEGER,
                entry_exchange_time_utc TEXT,
                entry_exchange_epoch_ms INTEGER,
                entry_price_source TEXT,
                trade_id TEXT,
                accepted_thresholds TEXT,
                rationale TEXT,
                risk_flags TEXT,
                raw_prediction_json TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_predictions_time
                ON predictions(prediction_epoch_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_predictions_symbol_horizon
                ON predictions(symbol, horizon_minutes, prediction_epoch_ms DESC);

            CREATE TABLE IF NOT EXISTS trades (
                trade_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                horizon_minutes INTEGER NOT NULL,
                direction TEXT NOT NULL,
                probability REAL NOT NULL,
                signal_threshold REAL NOT NULL,
                entry_time_utc TEXT NOT NULL,
                entry_epoch_ms INTEGER NOT NULL,
                entry_exchange_time_utc TEXT,
                entry_exchange_epoch_ms INTEGER,
                entry_price REAL NOT NULL,
                entry_price_source TEXT,
                expiry_time_utc TEXT NOT NULL,
                expiry_epoch_ms INTEGER NOT NULL,
                status TEXT NOT NULL,
                settled_time_utc TEXT,
                settled_epoch_ms INTEGER,
                expiry_price REAL,
                settlement_lag_ms INTEGER,
                success TEXT,
                return_pct REAL,
                pnl_units REAL,
                rationale TEXT,
                risk_flags TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_trades_status_expiry
                ON trades(status, expiry_epoch_ms);
            CREATE INDEX IF NOT EXISTS idx_trades_threshold
                ON trades(signal_threshold, status);
            CREATE INDEX IF NOT EXISTS idx_trades_symbol_horizon
                ON trades(symbol, horizon_minutes, signal_threshold);
            """
        )
        self.conn.commit()


class CsvLogs:
    def __init__(
        self,
        output_dir: Path,
        session_id: str,
        db_store: SqliteStore | None = None,
    ) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        self.session_id = session_id
        self.predictions_path = output_dir / f"event_gpt_predictions_{session_id}.csv"
        self.trades_path = output_dir / f"event_gpt_trades_{session_id}.csv"
        self.summary_path = output_dir / f"event_gpt_summary_{session_id}.json"
        self.db_store = db_store
        self._init_predictions()

    def append_prediction(
        self,
        prediction_time_ms: int,
        prediction: Prediction,
        accepted: bool,
        entry_price_point: PricePoint | None,
        trade_ids: list[str],
        accepted_thresholds: list[float],
    ) -> None:
        row = {
            "session_id": self.session_id,
            "prediction_time_utc": iso_utc_ms(prediction_time_ms),
            "prediction_epoch_ms": prediction_time_ms,
            "symbol": prediction.symbol,
            "horizon_minutes": prediction.horizon_minutes,
            "decision": prediction.decision,
            "direction": prediction.direction,
            "probability": f"{prediction.probability:.4f}",
            "setup": prediction.setup,
            "reject_reason": prediction.reject_reason,
            "accepted": str(accepted).lower(),
            "entry_price": (
                f"{entry_price_point.price:.8f}" if entry_price_point is not None else ""
            ),
            "entry_price_time_utc": (
                iso_utc_ms(entry_price_point.local_time_ms)
                if entry_price_point is not None
                else ""
            ),
            "entry_price_epoch_ms": (
                entry_price_point.local_time_ms if entry_price_point is not None else ""
            ),
            "entry_exchange_time_utc": (
                iso_utc_ms(entry_price_point.exchange_time_ms)
                if entry_price_point is not None
                else ""
            ),
            "entry_exchange_epoch_ms": (
                entry_price_point.exchange_time_ms if entry_price_point is not None else ""
            ),
            "entry_price_source": entry_price_point.source if entry_price_point else "",
            "trade_id": "|".join(trade_ids),
            "accepted_thresholds": "|".join(
                f"{threshold:.4f}" for threshold in accepted_thresholds
            ),
            "rationale": prediction.rationale,
            "risk_flags": "|".join(prediction.risk_flags),
            "raw_prediction_json": json.dumps(
                prediction.raw, ensure_ascii=False, separators=(",", ":")
            ),
        }
        with self.predictions_path.open("a", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
            writer.writerow(row)
        if self.db_store:
            self.db_store.append_prediction(row)

    def write_trades(self, trades: list[Trade]) -> None:
        fields = [
            "session_id",
            "trade_id",
            "symbol",
            "horizon_minutes",
            "direction",
            "probability",
            "signal_threshold",
            "entry_time_utc",
            "entry_epoch_ms",
            "entry_exchange_time_utc",
            "entry_exchange_epoch_ms",
            "entry_price",
            "entry_price_source",
            "expiry_time_utc",
            "expiry_epoch_ms",
            "status",
            "settled_time_utc",
            "settled_epoch_ms",
            "expiry_price",
            "settlement_lag_ms",
            "success",
            "return_pct",
            "pnl_units",
            "rationale",
            "risk_flags",
        ]
        with self.trades_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            for trade in trades:
                writer.writerow(trade.to_row(self.session_id))
        if self.db_store:
            self.db_store.upsert_trades(trades, self.session_id)

    def write_summary(self, trades: list[Trade]) -> None:
        summary = summarize_trades(trades)
        with self.summary_path.open("w", encoding="utf-8") as handle:
            json.dump(summary, handle, ensure_ascii=False, indent=2)

    def _init_predictions(self) -> None:
        fields = [
            "session_id",
            "prediction_time_utc",
            "prediction_epoch_ms",
            "symbol",
            "horizon_minutes",
            "decision",
            "direction",
            "probability",
            "setup",
            "reject_reason",
            "accepted",
            "entry_price",
            "entry_price_time_utc",
            "entry_price_epoch_ms",
            "entry_exchange_time_utc",
            "entry_exchange_epoch_ms",
            "entry_price_source",
            "trade_id",
            "accepted_thresholds",
            "rationale",
            "risk_flags",
            "raw_prediction_json",
        ]
        if self.predictions_path.exists():
            return
        with self.predictions_path.open("w", encoding="utf-8", newline="") as handle:
            csv.DictWriter(handle, fieldnames=fields).writeheader()


def main() -> int:
    args = parse_args()
    if args.print_prompt_template:
        print_prompt_template()
        return 0

    symbols = parse_csv_symbols(args.symbols)
    horizons = parse_csv_ints(args.horizons)
    validate_horizons(horizons)
    args.signal_thresholds = sorted(set(parse_csv_floats(args.signal_thresholds)))
    if not args.signal_thresholds:
        args.signal_thresholds = [args.probability_threshold]

    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key and not args.mock_gpt:
        print("OPENAI_API_KEY is required unless --mock-gpt is used.", file=sys.stderr)
        return 2

    if args.test_openai:
        return test_openai_connection(
            api_key=api_key,
            args=args,
            symbols=symbols,
            horizons=horizons,
        )

    session_id = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    db_store = SqliteStore(Path(args.db_path)) if args.db_path else None
    logs = CsvLogs(Path(args.output_dir), session_id, db_store=db_store)
    state = MarketState(symbols, max_bars=args.max_bars)
    trades: list[Trade] = []
    trade_counter = 0

    binance_proxy = first_non_empty(
        args.proxy,
        os.environ.get("BINANCE_PROXY"),
        os.environ.get("HTTPS_PROXY"),
        os.environ.get("HTTP_PROXY"),
    )
    openai_proxy = first_non_empty(args.openai_proxy, os.environ.get("OPENAI_PROXY"))
    ws_url = args.binance_ws_url or build_binance_ws_url(symbols)

    if args.rest_warmup_bars > 0:
        warmup_rest_klines(
            state=state,
            symbols=symbols,
            limit=args.rest_warmup_bars,
            proxy_url=binance_proxy,
            timeout_seconds=args.http_timeout_seconds,
        )

    client = None
    if not args.mock_gpt:
        client = OpenAIChatClient(
            api_key=api_key,
            model=args.model,
            url=args.openai_chat_url,
            timeout_seconds=args.openai_timeout_seconds,
            proxy_url=openai_proxy,
            use_json_schema=not args.no_json_schema,
        )

    print(f"session_id={session_id}")
    print(f"binance_ws={ws_url}")
    print(f"binance_proxy={mask_proxy(binance_proxy) if binance_proxy else 'direct'}")
    print(f"openai_model={'mock' if args.mock_gpt else args.model}")
    print(f"prediction_log={logs.predictions_path}")
    print(f"trade_log={logs.trades_path}")
    print(f"summary_log={logs.summary_path}")
    print(f"sqlite_db={db_store.db_path if db_store else 'disabled'}")

    next_predict_ms = now_ms()
    reconnect_delay = args.reconnect_delay_seconds

    while True:
        sock: ssl.SSLSocket | socket.socket | None = None
        try:
            print("connecting Binance websocket...")
            sock = websocket_connect(
                ws_url,
                timeout_seconds=args.ws_timeout_seconds,
                proxy_url=binance_proxy,
            )
            print("Binance websocket connected.")
            reconnect_delay = args.reconnect_delay_seconds

            while True:
                try:
                    opcode, payload = websocket_recv_frame(sock)
                    handle_websocket_frame(sock, opcode, payload, state)
                except socket.timeout:
                    pass

                settled = settle_expired_trades(
                    trades=trades,
                    state=state,
                    max_price_age_ms=args.max_price_age_ms,
                )
                if settled:
                    logs.write_trades(trades)
                    logs.write_summary(trades)
                    print_summary_line(trades, args.max_settled_trades)

                current_ms = now_ms()
                if current_ms >= next_predict_ms:
                    next_predict_ms = current_ms + args.prediction_interval_sec * 1000
                    ok, reason = state.ready(args.min_bars, args.max_price_age_ms)
                    if not ok:
                        refreshed = refresh_rest_market_data(
                            state=state,
                            symbols=symbols,
                            min_bars=args.min_bars,
                            max_price_age_ms=args.max_price_age_ms,
                            proxy_url=binance_proxy,
                            timeout_seconds=args.http_timeout_seconds,
                            reason="readiness_fallback",
                            quiet=False,
                        )
                        if refreshed:
                            ok, reason = state.ready(args.min_bars, args.max_price_age_ms)
                    if not ok:
                        print(f"{iso_utc_ms(current_ms)} skip prediction: {reason}")
                    else:
                        trade_counter = run_prediction_cycle(
                            args=args,
                            state=state,
                            client=client,
                            symbols=symbols,
                            horizons=horizons,
                            logs=logs,
                            trades=trades,
                            trade_counter=trade_counter,
                            binance_proxy=binance_proxy,
                        )

                if count_settled(trades) >= args.max_settled_trades:
                    logs.write_trades(trades)
                    logs.write_summary(trades)
                    print_summary_line(trades, args.max_settled_trades)
                    return 0

        except KeyboardInterrupt:
            print("stopped by user.")
            logs.write_trades(trades)
            logs.write_summary(trades)
            return 130
        except Exception as error:  # noqa: BLE001 - long-running collector reports and reconnects.
            print(f"websocket loop error: {type(error).__name__}: {error}", file=sys.stderr)
            time.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 1.5, args.max_reconnect_delay_seconds)
        finally:
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass


def run_prediction_cycle(
    args: argparse.Namespace,
    state: MarketState,
    client: OpenAIChatClient | None,
    symbols: list[str],
    horizons: list[int],
    logs: CsvLogs,
    trades: list[Trade],
    trade_counter: int,
    binance_proxy: str | None,
) -> int:
    request_time_ms = now_ms()
    refresh_rest_market_data(
        state=state,
        symbols=symbols,
        min_bars=args.min_bars,
        max_price_age_ms=args.max_price_age_ms,
        proxy_url=binance_proxy,
        timeout_seconds=args.http_timeout_seconds,
        reason="before_gpt",
        quiet=True,
    )
    snapshot = state.prompt_snapshot(horizons, args.max_price_age_ms)
    try:
        if args.mock_gpt:
            predictions = mock_predictions(snapshot, symbols, horizons)
        else:
            if client is None:
                raise RuntimeError("OpenAI client not initialized")
            predictions, _raw_content = client.predict(snapshot, symbols, horizons)
    except Exception as error:  # noqa: BLE001 - keep collector alive after one failed GPT call.
        print(f"{iso_utc_ms(request_time_ms)} GPT prediction failed: {error}", file=sys.stderr)
        return trade_counter

    decision_time_ms = now_ms()
    refresh_rest_market_data(
        state=state,
        symbols=symbols,
        min_bars=args.min_bars,
        max_price_age_ms=0,
        proxy_url=binance_proxy,
        timeout_seconds=args.http_timeout_seconds,
        reason="after_gpt_entry",
        quiet=True,
        force_prices=True,
    )
    opened = 0
    for prediction in predictions:
        price_point = state.current_price(prediction.symbol, args.max_price_age_ms)
        cooldown_active = has_recent_open_signal(
            trades=trades,
            symbol=prediction.symbol,
            horizon_minutes=prediction.horizon_minutes,
            now_epoch_ms=price_point.local_time_ms if price_point else decision_time_ms,
            cooldown_minutes=args.cooldown_minutes,
        )
        eligible_thresholds = [
            threshold
            for threshold in args.signal_thresholds
            if prediction.probability >= threshold
        ]
        accepted_thresholds = []
        trade_ids = []
        accepted = bool(
            price_point is not None
            and prediction.decision == "TRADE"
            and prediction.direction in ("UP", "DOWN")
            and eligible_thresholds
            and not cooldown_active
            and len([trade for trade in trades if trade.status == "OPEN"])
            < args.max_open_trades
        )
        if accepted:
            available_slots = args.max_open_trades - count_open(trades)
            for threshold in eligible_thresholds[:available_slots]:
                trade_counter += 1
                opened += 1
                accepted_thresholds.append(threshold)
                trade_id = f"{logs.session_id}-{trade_counter:06d}"
                trade_ids.append(trade_id)
                trades.append(
                    Trade(
                        trade_id=trade_id,
                        symbol=prediction.symbol,
                        horizon_minutes=prediction.horizon_minutes,
                        direction=prediction.direction,
                        probability=prediction.probability,
                        signal_threshold=threshold,
                        entry_epoch_ms=price_point.local_time_ms,
                        entry_exchange_epoch_ms=price_point.exchange_time_ms,
                        entry_price=price_point.price,
                        entry_price_source=price_point.source,
                        expiry_epoch_ms=(
                            price_point.local_time_ms
                            + prediction.horizon_minutes * 60 * 1000
                        ),
                        rationale=prediction.rationale,
                        risk_flags=prediction.risk_flags,
                    )
                )
        logs.append_prediction(
            prediction_time_ms=decision_time_ms,
            prediction=prediction,
            accepted=accepted,
            entry_price_point=price_point,
            trade_ids=trade_ids,
            accepted_thresholds=accepted_thresholds,
        )

    if opened:
        logs.write_trades(trades)
        logs.write_summary(trades)
    print(
        f"{iso_utc_ms(decision_time_ms)} predictions={len(predictions)} "
        f"opened={opened} open={count_open(trades)} settled={count_settled(trades)}"
    )
    return trade_counter


def test_openai_connection(
    api_key: str,
    args: argparse.Namespace,
    symbols: list[str],
    horizons: list[int],
) -> int:
    if args.mock_gpt:
        print("--test-openai is not needed with --mock-gpt.", file=sys.stderr)
        return 2
    client = OpenAIChatClient(
        api_key=api_key,
        model=args.model,
        url=args.openai_chat_url,
        timeout_seconds=args.openai_timeout_seconds,
        proxy_url=first_non_empty(args.openai_proxy, os.environ.get("OPENAI_PROXY")),
        use_json_schema=not args.no_json_schema,
    )
    snapshot = sample_prompt_snapshot(symbols, horizons)
    try:
        predictions, raw_content = client.predict(snapshot, symbols, horizons)
    except Exception as error:  # noqa: BLE001 - diagnostic command should print concise failures.
        print(f"OpenAI-compatible endpoint test failed: {type(error).__name__}: {error}")
        print(f"model={args.model}")
        print(f"url={args.openai_chat_url}")
        print(
            "If this is Sub2API, use a runtime API key created in its Keys page "
            "and make sure the key's group/platform matches this model."
        )
        return 1
    print("OpenAI-compatible endpoint test succeeded.")
    print(f"model={args.model}")
    print(f"url={args.openai_chat_url}")
    print(f"predictions={len(predictions)}")
    for prediction in predictions:
        print(
            f"{prediction.symbol} {prediction.horizon_minutes}m "
            f"{prediction.direction} p={prediction.probability:.4f}"
        )
    if not predictions:
        print(raw_content)
    return 0


def settle_expired_trades(
    trades: list[Trade],
    state: MarketState,
    max_price_age_ms: int,
) -> int:
    current_ms = now_ms()
    settled = 0
    for trade in trades:
        if trade.status != "OPEN" or current_ms < trade.expiry_epoch_ms:
            continue
        price_point = state.current_price(trade.symbol, max_price_age_ms)
        if price_point is None:
            continue
        expiry_price = price_point.price
        if trade.direction == "UP":
            success = expiry_price > trade.entry_price
        else:
            success = expiry_price < trade.entry_price

        trade.status = "SETTLED"
        trade.settled_epoch_ms = current_ms
        trade.expiry_price = expiry_price
        trade.settlement_lag_ms = current_ms - trade.expiry_epoch_ms
        trade.success = success
        trade.return_pct = pct_change(expiry_price, trade.entry_price)
        trade.pnl_units = payout_for_horizon(trade.horizon_minutes) if success else -1.0
        settled += 1
    return settled


def handle_websocket_frame(
    sock: ssl.SSLSocket | socket.socket,
    opcode: int,
    payload: bytes,
    state: MarketState,
) -> None:
    if opcode == 0x1:
        text = payload.decode("utf-8", errors="replace")
        state.update_from_stream_payload(json.loads(text))
        return
    if opcode == 0x8:
        raise RuntimeError(f"closed by server: {payload!r}")
    if opcode == 0x9:
        websocket_send_control(sock, 0xA, payload)


def warmup_rest_klines(
    state: MarketState,
    symbols: list[str],
    limit: int,
    proxy_url: str | None,
    timeout_seconds: float,
) -> None:
    try:
        opener = build_url_opener(proxy_url)
    except ValueError as error:
        print(f"REST warmup skipped: {error}", file=sys.stderr)
        return
    for symbol in symbols:
        url = (
            "https://fapi.binance.com/fapi/v1/klines?"
            + urllib.parse.urlencode(
                {"symbol": symbol, "interval": "1m", "limit": min(limit, 1500)}
            )
        )
        request = urllib.request.Request(
            url,
            headers={"Accept": "application/json", "User-Agent": USER_AGENT},
        )
        try:
            with opener.open(request, timeout=timeout_seconds) as response:
                data = json.loads(response.read().decode("utf-8"))
            candles = [
                Candle(
                    open_time_ms=int(item[0]),
                    open=float(item[1]),
                    high=float(item[2]),
                    low=float(item[3]),
                    close=float(item[4]),
                    volume=float(item[5]),
                    close_time_ms=int(item[6]),
                )
                for item in data
            ]
            state.load_candles(symbol, candles)
            print(f"REST warmup {symbol}: {len(candles)} closed 1m bars")
            time.sleep(0.2)
        except Exception as error:  # noqa: BLE001 - websocket can still warm up naturally.
            print(f"REST warmup {symbol} failed: {error}", file=sys.stderr)


def refresh_rest_market_data(
    state: MarketState,
    symbols: list[str],
    min_bars: int,
    max_price_age_ms: int,
    proxy_url: str | None,
    timeout_seconds: float,
    reason: str,
    quiet: bool,
    force_prices: bool = False,
) -> bool:
    try:
        opener = build_url_opener(proxy_url)
    except ValueError as error:
        if not quiet:
            print(f"REST fallback skipped: {error}", file=sys.stderr)
        return False

    changed = False
    for symbol in symbols:
        price_point = state.current_price(symbol, max_price_age_ms)
        needs_price = force_prices or price_point is None
        needs_bars = len(state.klines[symbol]) < min_bars

        if needs_price and fetch_rest_index_price(
            state=state,
            opener=opener,
            symbol=symbol,
            timeout_seconds=timeout_seconds,
            quiet=quiet,
        ):
            changed = True

        if needs_bars and fetch_rest_klines(
            state=state,
            opener=opener,
            symbol=symbol,
            limit=min(max(min_bars, 2), 1500),
            timeout_seconds=timeout_seconds,
            quiet=quiet,
        ):
            changed = True

        if not quiet and (needs_price or needs_bars):
            print(
                f"REST fallback {reason} {symbol}: "
                f"price={'yes' if needs_price else 'fresh'} "
                f"bars={'yes' if needs_bars else 'ready'}"
            )
    return changed


def fetch_rest_index_price(
    state: MarketState,
    opener: urllib.request.OpenerDirector,
    symbol: str,
    timeout_seconds: float,
    quiet: bool,
) -> bool:
    url = (
        "https://fapi.binance.com/fapi/v1/premiumIndex?"
        + urllib.parse.urlencode({"symbol": symbol})
    )
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    try:
        with opener.open(request, timeout=timeout_seconds) as response:
            data = json.loads(response.read().decode("utf-8"))
        index_text = data.get("indexPrice") or data.get("markPrice")
        price = float(index_text)
        exchange_time_ms = int(data.get("time") or now_ms())
        state.update_rest_index_price(
            symbol=symbol,
            price=price,
            exchange_time_ms=exchange_time_ms,
            source="rest_futures_index_price",
        )
        return True
    except Exception as error:  # noqa: BLE001 - fallback should report and continue.
        if not quiet:
            print(f"REST index fallback {symbol} failed: {error}", file=sys.stderr)
        return False


def fetch_rest_klines(
    state: MarketState,
    opener: urllib.request.OpenerDirector,
    symbol: str,
    limit: int,
    timeout_seconds: float,
    quiet: bool,
) -> bool:
    url = (
        "https://fapi.binance.com/fapi/v1/klines?"
        + urllib.parse.urlencode(
            {"symbol": symbol, "interval": "1m", "limit": min(limit, 1500)}
        )
    )
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    try:
        with opener.open(request, timeout=timeout_seconds) as response:
            data = json.loads(response.read().decode("utf-8"))
        candles = parse_rest_klines(data)
        state.load_candles(symbol, candles)
        return bool(candles)
    except Exception as error:  # noqa: BLE001 - fallback should report and continue.
        if not quiet:
            print(f"REST kline fallback {symbol} failed: {error}", file=sys.stderr)
        return False


def parse_rest_klines(data: list[Any]) -> list[Candle]:
    return [
        Candle(
            open_time_ms=int(item[0]),
            open=float(item[1]),
            high=float(item[2]),
            low=float(item[3]),
            close=float(item[4]),
            volume=float(item[5]),
            close_time_ms=int(item[6]),
        )
        for item in data
    ]


def websocket_connect(
    url: str,
    timeout_seconds: float,
    proxy_url: str | None,
) -> ssl.SSLSocket | socket.socket:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != "wss":
        raise ValueError(f"only wss is supported: {url}")
    if not parsed.hostname:
        raise ValueError(f"missing websocket host: {url}")

    port = parsed.port or 443
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"

    raw_sock = open_tcp_stream(
        host=parsed.hostname,
        port=port,
        timeout_seconds=timeout_seconds,
        proxy_url=proxy_url,
    )
    context = ssl.create_default_context()
    sock = context.wrap_socket(raw_sock, server_hostname=parsed.hostname)
    sock.settimeout(timeout_seconds)

    key = base64.b64encode(os.urandom(16)).decode("ascii")
    request = "\r\n".join(
        [
            f"GET {path} HTTP/1.1",
            f"Host: {parsed.hostname}:{port}",
            "Upgrade: websocket",
            "Connection: Upgrade",
            f"Sec-WebSocket-Key: {key}",
            "Sec-WebSocket-Version: 13",
            f"User-Agent: {USER_AGENT}",
            "",
            "",
        ]
    )
    sock.sendall(request.encode("ascii"))

    header = read_until(sock, b"\r\n\r\n", limit=64 * 1024)
    status_line = header.split(b"\r\n", 1)[0].decode("iso-8859-1", errors="replace")
    if " 101 " not in status_line:
        raise RuntimeError(f"websocket upgrade failed: {status_line}")

    accept = websocket_accept(key)
    if f"sec-websocket-accept: {accept}".encode("ascii").lower() not in header.lower():
        raise RuntimeError("websocket upgrade missing expected Sec-WebSocket-Accept")
    return sock


def open_tcp_stream(
    host: str,
    port: int,
    timeout_seconds: float,
    proxy_url: str | None,
) -> socket.socket | ssl.SSLSocket:
    if not proxy_url:
        sock = socket.create_connection((host, port), timeout=timeout_seconds)
        sock.settimeout(timeout_seconds)
        return sock

    normalized = normalize_proxy_url(proxy_url)
    parsed = urllib.parse.urlparse(normalized)
    if not parsed.hostname:
        raise ValueError(f"invalid proxy url: {proxy_url}")
    proxy_port = parsed.port or default_proxy_port(parsed.scheme)
    proxy_sock = socket.create_connection((parsed.hostname, proxy_port), timeout=timeout_seconds)
    proxy_sock.settimeout(timeout_seconds)

    if parsed.scheme in ("http", "https"):
        if parsed.scheme == "https":
            proxy_sock = ssl.create_default_context().wrap_socket(
                proxy_sock,
                server_hostname=parsed.hostname,
            )
            proxy_sock.settimeout(timeout_seconds)
        http_connect(proxy_sock, parsed, host, port)
        return proxy_sock

    if parsed.scheme in ("socks5", "socks5h"):
        socks5_connect(proxy_sock, parsed, host, port)
        return proxy_sock

    raise ValueError(f"unsupported proxy scheme for websocket: {parsed.scheme}")


def http_connect(
    sock: socket.socket | ssl.SSLSocket,
    proxy: urllib.parse.ParseResult,
    host: str,
    port: int,
) -> None:
    headers = [
        f"CONNECT {host}:{port} HTTP/1.1",
        f"Host: {host}:{port}",
        f"User-Agent: {USER_AGENT}",
        "Proxy-Connection: Keep-Alive",
    ]
    if proxy.username:
        password = proxy.password or ""
        token = base64.b64encode(
            f"{urllib.parse.unquote(proxy.username)}:{urllib.parse.unquote(password)}".encode(
                "utf-8"
            )
        ).decode("ascii")
        headers.append(f"Proxy-Authorization: Basic {token}")
    request = "\r\n".join(headers + ["", ""])
    sock.sendall(request.encode("ascii"))
    response = read_until(sock, b"\r\n\r\n", limit=64 * 1024)
    status_line = response.split(b"\r\n", 1)[0].decode("iso-8859-1", errors="replace")
    if " 200 " not in status_line:
        raise RuntimeError(f"proxy CONNECT failed: {status_line}")


def socks5_connect(
    sock: socket.socket,
    proxy: urllib.parse.ParseResult,
    host: str,
    port: int,
) -> None:
    username = urllib.parse.unquote(proxy.username or "")
    password = urllib.parse.unquote(proxy.password or "")
    if username:
        sock.sendall(b"\x05\x02\x00\x02")
    else:
        sock.sendall(b"\x05\x01\x00")
    version, method = recv_exact(sock, 2)
    if version != 5:
        raise RuntimeError("invalid SOCKS5 handshake")
    if method == 0xFF:
        raise RuntimeError("SOCKS5 proxy rejected auth methods")
    if method == 0x02:
        if len(username) > 255 or len(password) > 255:
            raise ValueError("SOCKS5 username/password too long")
        auth = (
            b"\x01"
            + bytes([len(username)])
            + username.encode("utf-8")
            + bytes([len(password)])
            + password.encode("utf-8")
        )
        sock.sendall(auth)
        auth_version, status = recv_exact(sock, 2)
        if auth_version != 1 or status != 0:
            raise RuntimeError("SOCKS5 username/password auth failed")
    elif method != 0x00:
        raise RuntimeError(f"unsupported SOCKS5 auth method: {method}")

    host_bytes = host.encode("idna")
    if len(host_bytes) > 255:
        raise ValueError("SOCKS5 target host too long")
    request = b"\x05\x01\x00\x03" + bytes([len(host_bytes)]) + host_bytes
    request += struct.pack("!H", port)
    sock.sendall(request)

    header = recv_exact(sock, 4)
    if header[0] != 5:
        raise RuntimeError("invalid SOCKS5 connect response")
    if header[1] != 0:
        raise RuntimeError(f"SOCKS5 connect failed with code {header[1]}")
    atyp = header[3]
    if atyp == 1:
        recv_exact(sock, 4)
    elif atyp == 3:
        length = recv_exact(sock, 1)[0]
        recv_exact(sock, length)
    elif atyp == 4:
        recv_exact(sock, 16)
    else:
        raise RuntimeError(f"invalid SOCKS5 address type: {atyp}")
    recv_exact(sock, 2)


def websocket_accept(key: str) -> str:
    guid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
    digest = hashlib.sha1(f"{key}{guid}".encode("ascii")).digest()
    return base64.b64encode(digest).decode("ascii")


def websocket_recv_frame(sock: socket.socket | ssl.SSLSocket) -> tuple[int, bytes]:
    first = recv_exact(sock, 2)
    opcode = first[0] & 0x0F
    masked = bool(first[1] & 0x80)
    length = first[1] & 0x7F
    if length == 126:
        length = struct.unpack("!H", recv_exact(sock, 2))[0]
    elif length == 127:
        length = struct.unpack("!Q", recv_exact(sock, 8))[0]

    mask = recv_exact(sock, 4) if masked else b""
    payload = recv_exact(sock, length) if length else b""
    if masked:
        payload = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
    return opcode, payload


def websocket_send_control(
    sock: socket.socket | ssl.SSLSocket,
    opcode: int,
    payload: bytes,
) -> None:
    if len(payload) > 125:
        payload = payload[:125]
    mask = os.urandom(4)
    frame = bytearray([0x80 | opcode, 0x80 | len(payload)])
    frame.extend(mask)
    frame.extend(byte ^ mask[index % 4] for index, byte in enumerate(payload))
    sock.sendall(frame)


def recv_exact(sock: socket.socket | ssl.SSLSocket, size: int) -> bytes:
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise RuntimeError("connection closed while reading frame")
        data.extend(chunk)
    return bytes(data)


def read_until(sock: socket.socket | ssl.SSLSocket, marker: bytes, limit: int) -> bytes:
    data = bytearray()
    while marker not in data:
        chunk = sock.recv(4096)
        if not chunk:
            raise RuntimeError("connection closed while reading headers")
        data.extend(chunk)
        if len(data) > limit:
            raise RuntimeError("header too large")
    return bytes(data)


def build_url_opener(proxy_url: str | None) -> urllib.request.OpenerDirector:
    if proxy_url:
        normalized = normalize_proxy_url(proxy_url)
        scheme = urllib.parse.urlparse(normalized).scheme
        if scheme in ("socks5", "socks5h"):
            raise ValueError(
                "HTTP requests use urllib and require an http:// or https:// proxy; "
                "SOCKS5 is only supported for the Binance websocket path."
            )
        handler = urllib.request.ProxyHandler({"http": normalized, "https": normalized})
        return urllib.request.build_opener(handler)
    return urllib.request.build_opener(urllib.request.ProxyHandler({}))


def default_openai_chat_url() -> str:
    chat_url = os.environ.get("OPENAI_CHAT_URL")
    if chat_url:
        return chat_url
    base_url = os.environ.get("OPENAI_BASE_URL")
    if not base_url:
        return "https://api.openai.com/v1/chat/completions"
    return openai_chat_url_from_base(base_url)


def openai_chat_url_from_base(base_url: str) -> str:
    normalized = base_url.rstrip("/")
    if normalized.endswith("/chat/completions"):
        return normalized
    if normalized.endswith("/responses"):
        return normalized[: -len("/responses")] + "/chat/completions"
    if normalized.endswith("/v1"):
        return normalized + "/chat/completions"
    return normalized + "/v1/chat/completions"


def normalize_proxy_url(proxy_url: str) -> str:
    if "://" not in proxy_url:
        return f"http://{proxy_url}"
    return proxy_url


def default_proxy_port(scheme: str) -> int:
    if scheme == "https":
        return 443
    if scheme == "socks5" or scheme == "socks5h":
        return 1080
    return 80


def build_binance_ws_url(symbols: list[str]) -> str:
    streams: list[str] = []
    for symbol in symbols:
        lower = symbol.lower()
        streams.append(f"{lower}@markPrice@1s")
        streams.append(f"{lower}@kline_1m")
    return "wss://fstream.binancefuture.com/stream?streams=" + "/".join(streams)


def build_user_prompt(snapshot: dict[str, Any]) -> str:
    compact = json.dumps(snapshot, ensure_ascii=False, separators=(",", ":"))
    return (
        "Audit the following JSON snapshot. Return exactly one prediction "
        "for every symbol and horizon_minutes. Use each horizon's rule_gates "
        "entry as the primary trade filter.\n\n"
        f"SNAPSHOT_JSON={compact}"
    )

def prediction_response_schema(symbols: list[str], horizons: list[int]) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "market_regime": {
                "type": "string",
                "description": "Brief regime label, e.g. trend_up/range/volatile_unclear.",
            },
            "predictions": {
                "type": "array",
                "minItems": len(symbols) * len(horizons),
                "maxItems": len(symbols) * len(horizons),
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "symbol": {"type": "string", "enum": symbols},
                        "horizon_minutes": {"type": "integer", "enum": horizons},
                        "decision": {"type": "string", "enum": ["TRADE", "NO_TRADE"]},
                        "direction": {"type": "string", "enum": ["UP", "DOWN", "NONE"]},
                        "probability": {
                            "type": "number",
                            "minimum": 0.5,
                            "maximum": 0.9,
                        },
                        "setup": {"type": "string", "maxLength": 80},
                        "reject_reason": {"type": "string", "maxLength": 180},
                        "rationale": {"type": "string", "maxLength": 180},
                        "risk_flags": {
                            "type": "array",
                            "items": {"type": "string", "maxLength": 80},
                            "maxItems": 5,
                        },
                    },
                    "required": [
                        "symbol",
                        "horizon_minutes",
                        "decision",
                        "direction",
                        "probability",
                        "setup",
                        "reject_reason",
                        "rationale",
                        "risk_flags",
                    ],
                },
            },
        },
        "required": ["market_regime", "predictions"],
    }


def normalize_predictions(
    raw_prediction: dict[str, Any],
    symbols: list[str],
    horizons: list[int],
) -> list[Prediction]:
    raw_items = raw_prediction if isinstance(raw_prediction, list) else raw_prediction.get("predictions")
    if not isinstance(raw_items, list):
        raise ValueError("GPT response missing predictions array")

    predictions: list[Prediction] = []
    seen: set[tuple[str, int]] = set()
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol", "")).upper()
        direction = str(item.get("direction", "")).upper()
        try:
            horizon = int(item.get("horizon_minutes"))
            probability = float(item.get("probability"))
        except (TypeError, ValueError):
            continue
        if probability > 1.0 and probability <= 100.0:
            probability /= 100.0
        if symbol not in symbols or horizon not in horizons:
            continue
        decision = str(item.get("decision", "TRADE")).upper()
        if decision not in ("TRADE", "NO_TRADE"):
            decision = "NO_TRADE"
        if direction not in ("UP", "DOWN", "NONE"):
            continue
        if decision == "NO_TRADE":
            direction = "NONE"
        if decision == "TRADE" and direction not in ("UP", "DOWN"):
            decision = "NO_TRADE"
            direction = "NONE"
        probability = max(0.0, min(probability, 1.0))
        risk_flags = item.get("risk_flags", [])
        if isinstance(risk_flags, str):
            risk_flags = [risk_flags]
        if not isinstance(risk_flags, list):
            risk_flags = []
        key = (symbol, horizon)
        if key in seen:
            continue
        seen.add(key)
        predictions.append(
            Prediction(
                symbol=symbol,
                horizon_minutes=horizon,
                decision=decision,
                direction=direction,
                probability=probability,
                setup=str(item.get("setup", ""))[:100],
                reject_reason=str(item.get("reject_reason", ""))[:220],
                rationale=str(item.get("rationale", ""))[:300],
                risk_flags=[str(flag)[:80] for flag in risk_flags[:5]],
                raw=item,
            )
        )

    missing = [
        f"{symbol}:{horizon}m"
        for symbol in symbols
        for horizon in horizons
        if (symbol, horizon) not in seen
    ]
    if missing:
        raise ValueError(f"GPT response missing predictions for {','.join(missing)}")
    return predictions


def mock_predictions(
    snapshot: dict[str, Any],
    symbols: list[str],
    horizons: list[int],
) -> list[Prediction]:
    by_symbol = {
        item["symbol"]: item
        for item in snapshot.get("symbols", [])
        if isinstance(item, dict) and item.get("symbol")
    }
    predictions: list[Prediction] = []
    for symbol in symbols:
        item = by_symbol.get(symbol, {})
        returns = item.get("returns_pct", {})
        ret_15 = coalesce_number(returns.get("15m"), returns.get("5m"), 0.0)
        ret_30 = coalesce_number(returns.get("30m"), ret_15)
        gates = item.get("rule_gates", {})
        for horizon in horizons:
            gate = gates.get(f"{horizon}m", {})
            if gate.get("allow_trade"):
                basis = ret_15 if horizon <= 10 else ret_30
                direction = str(gate.get("allowed_direction") or ("UP" if basis >= 0 else "DOWN"))
                decision = "TRADE"
                probability = min(0.7, 0.60 + abs(basis) / 12.0)
                reject_reason = ""
            else:
                direction = "NONE"
                decision = "NO_TRADE"
                probability = 0.55
                reject_reason = str(gate.get("reason", "rule gate rejected"))
            raw = {
                "symbol": symbol,
                "horizon_minutes": horizon,
                "decision": decision,
                "direction": direction,
                "probability": probability,
                "setup": str(gate.get("setup", "")),
                "reject_reason": reject_reason,
                "rationale": "mock rule-gate heuristic",
                "risk_flags": ["mock_gpt"],
            }
            predictions.append(
                Prediction(
                    symbol=symbol,
                    horizon_minutes=horizon,
                    decision=decision,
                    direction=direction,
                    probability=probability,
                    setup=str(gate.get("setup", "")),
                    reject_reason=reject_reason,
                    rationale="mock rule-gate heuristic",
                    risk_flags=["mock_gpt"],
                    raw=raw,
                )
            )
    return predictions


def summarize_trades(trades: list[Trade]) -> dict[str, Any]:
    settled = [trade for trade in trades if trade.status == "SETTLED"]
    open_trades = [trade for trade in trades if trade.status == "OPEN"]
    summary: dict[str, Any] = {
        "updated_utc": iso_utc_ms(now_ms()),
        "total_trades": len(trades),
        "open_trades": len(open_trades),
        "settled_trades": len(settled),
        "overall": bucket_summary(settled),
        "by_threshold": {},
        "by_symbol_horizon": {},
    }
    threshold_buckets: dict[str, list[Trade]] = {}
    for trade in settled:
        threshold_buckets.setdefault(f"{trade.signal_threshold:.2f}", []).append(trade)
    summary["by_threshold"] = {
        key: bucket_summary(bucket) for key, bucket in sorted(threshold_buckets.items())
    }

    buckets: dict[str, list[Trade]] = {}
    for trade in settled:
        buckets.setdefault(
            f"{trade.symbol}_{trade.horizon_minutes}m_threshold_{trade.signal_threshold:.2f}",
            [],
        ).append(trade)
    summary["by_symbol_horizon"] = {
        key: bucket_summary(bucket) for key, bucket in sorted(buckets.items())
    }
    return summary


def bucket_summary(trades: list[Trade]) -> dict[str, Any]:
    wins = sum(1 for trade in trades if trade.success)
    losses = sum(1 for trade in trades if trade.success is False)
    pnl = sum(trade.pnl_units or 0.0 for trade in trades)
    avg_probability = (
        sum(trade.probability for trade in trades) / len(trades) if trades else None
    )
    return {
        "count": len(trades),
        "wins": wins,
        "losses": losses,
        "win_rate": wins / len(trades) if trades else None,
        "pnl_units": pnl,
        "avg_pnl_units_per_trade": pnl / len(trades) if trades else None,
        "avg_probability": avg_probability,
    }


def print_summary_line(trades: list[Trade], max_settled: int) -> None:
    summary = bucket_summary([trade for trade in trades if trade.status == "SETTLED"])
    win_rate = summary["win_rate"]
    win_rate_text = f"{win_rate:.2%}" if win_rate is not None else "n/a"
    print(
        f"settled={summary['count']}/{max_settled} "
        f"win_rate={win_rate_text} pnl_units={summary['pnl_units']:.2f} "
        f"open={count_open(trades)}"
    )


def pct_from_history(current: float, closes: list[float], minutes: int) -> float | None:
    if len(closes) < minutes:
        return None
    return pct_change(current, closes[-minutes])


def pct_change(current: float, previous: float) -> float | None:
    if previous == 0:
        return None
    return (current / previous - 1.0) * 100.0


def realized_vol_pct(closes: list[float], window: int) -> float | None:
    if len(closes) < window + 1:
        return None
    selected = closes[-(window + 1) :]
    returns = [
        math.log(current / previous)
        for previous, current in zip(selected, selected[1:])
        if previous > 0 and current > 0
    ]
    if len(returns) < 2:
        return None
    mean = sum(returns) / len(returns)
    variance = sum((value - mean) ** 2 for value in returns) / (len(returns) - 1)
    return math.sqrt(variance) * 100.0


def ema(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    alpha = 2.0 / (period + 1.0)
    result = sum(values[:period]) / period
    for value in values[period:]:
        result = alpha * value + (1.0 - alpha) * result
    return result


def volume_snapshot(volumes: list[float]) -> dict[str, float | None]:
    if not volumes:
        return {
            "last_5m_sum": None,
            "previous_30m_avg_per_min": None,
            "last_5m_vs_prev_30m_ratio": None,
        }
    last_5 = volumes[-5:]
    previous_30 = volumes[-35:-5] if len(volumes) >= 35 else volumes[:-5]
    previous_avg = sum(previous_30) / len(previous_30) if previous_30 else None
    last_5_avg = sum(last_5) / len(last_5)
    return {
        "last_5m_sum": round_float(sum(last_5)),
        "previous_30m_avg_per_min": round_float(previous_avg),
        "last_5m_vs_prev_30m_ratio": (
            round_float(last_5_avg / previous_avg)
            if previous_avg and previous_avg > 0
            else None
        ),
    }


def key_level_snapshot(current: float, bars: list[Candle]) -> dict[str, Any]:
    supports: list[dict[str, Any]] = []
    resistances: list[dict[str, Any]] = []
    for window in (20, 60, 120):
        if len(bars) < window:
            continue
        selected = bars[-window:]
        low = min(candle.low for candle in selected)
        high = max(candle.high for candle in selected)
        supports.append(level_record("low", f"{window}m_low", low, current))
        resistances.append(level_record("high", f"{window}m_high", high, current))

    pivots = pivot_levels(bars[-140:] if len(bars) > 140 else bars)
    for level in pivots["supports"][-5:]:
        supports.append(level_record("pivot_support", "pivot", level, current))
    for level in pivots["resistances"][-5:]:
        resistances.append(level_record("pivot_resistance", "pivot", level, current))

    below = [level for level in supports if level["price"] <= current]
    above = [level for level in resistances if level["price"] >= current]
    nearest_support = max(below, key=lambda item: item["price"]) if below else None
    nearest_resistance = min(above, key=lambda item: item["price"]) if above else None
    return {
        "nearest_support": nearest_support,
        "nearest_resistance": nearest_resistance,
        "support_count": len(supports),
        "resistance_count": len(resistances),
    }


def level_record(kind: str, source: str, price: float, current: float) -> dict[str, Any]:
    return {
        "kind": kind,
        "source": source,
        "price": round_float(price),
        "distance_pct": pct_change(current, price),
    }


def pivot_levels(bars: list[Candle], width: int = 2) -> dict[str, list[float]]:
    supports: list[float] = []
    resistances: list[float] = []
    if len(bars) < width * 2 + 1:
        return {"supports": supports, "resistances": resistances}
    for index in range(width, len(bars) - width):
        window = bars[index - width : index + width + 1]
        low = bars[index].low
        high = bars[index].high
        if low == min(candle.low for candle in window):
            supports.append(low)
        if high == max(candle.high for candle in window):
            resistances.append(high)
    return {"supports": supports, "resistances": resistances}


def vwap_snapshot(current: float, bars: list[Candle], window: int) -> dict[str, Any]:
    selected = bars[-window:] if len(bars) >= window else bars
    denom = sum(candle.volume for candle in selected)
    vwap = (
        sum(((candle.high + candle.low + candle.close) / 3.0) * candle.volume for candle in selected)
        / denom
        if denom > 0
        else None
    )
    slope = None
    if len(selected) >= 20:
        first = vwap_value(selected[: len(selected) // 2])
        second = vwap_value(selected[len(selected) // 2 :])
        if first and second:
            slope = "up" if second > first else "down" if second < first else "flat"
    crosses = 0
    if vwap:
        previous_side = None
        for candle in selected[-30:]:
            side = candle.close >= vwap
            if previous_side is not None and side != previous_side:
                crosses += 1
            previous_side = side
    return {
        "vwap_60m": round_float(vwap),
        "distance_pct": pct_change(current, vwap) if vwap else None,
        "slope": slope,
        "cross_count_30m": crosses,
    }


def vwap_value(bars: list[Candle]) -> float | None:
    denom = sum(candle.volume for candle in bars)
    if denom <= 0:
        return None
    return sum(((candle.high + candle.low + candle.close) / 3.0) * candle.volume for candle in bars) / denom


def trend_snapshot(current: float, closes: list[float], bars: list[Candle]) -> dict[str, Any]:
    ema9 = ema(closes, 9)
    ema21 = ema(closes, 21)
    ema55 = ema(closes, 55)
    alignment = "mixed"
    if ema9 and ema21 and ema55:
        if current > ema9 > ema21 > ema55:
            alignment = "bullish"
        elif current < ema9 < ema21 < ema55:
            alignment = "bearish"
    slope_21 = ema_slope(closes, 21, 8)
    adx = adx_value(bars, 14)
    return {
        "ema_alignment": alignment,
        "ema21_slope_pct": slope_21,
        "adx_14": round_float(adx, 4),
        "regime": market_regime(alignment, slope_21, adx),
    }


def ema_slope(values: list[float], period: int, lookback: int) -> float | None:
    if len(values) < period + lookback:
        return None
    now_value = ema(values, period)
    previous_value = ema(values[:-lookback], period)
    return pct_change(now_value, previous_value) if now_value and previous_value else None


def market_regime(alignment: str, slope_21: float | None, adx: float | None) -> str:
    if adx is not None and adx < 16:
        return "range"
    if alignment == "bullish" and (adx or 0) >= 20 and (slope_21 or 0) > 0:
        return "trend_up"
    if alignment == "bearish" and (adx or 0) >= 20 and (slope_21 or 0) < 0:
        return "trend_down"
    return "mixed"


def volatility_snapshot(current: float, bars: list[Candle]) -> dict[str, Any]:
    atr = atr_value(bars, 14)
    atr_pct = (atr / current * 100.0) if atr and current else None
    atr_series = rolling_atr_values(bars, 14, 120)
    percentile = percentile_rank(atr_series, atr) if atr and atr_series else None
    return {
        "atr_14": round_float(atr),
        "atr_pct": round_float(atr_pct, 6),
        "atr_percentile_120": round_float(percentile, 4),
    }


def atr_value(bars: list[Candle], period: int) -> float | None:
    if len(bars) < period + 1:
        return None
    trs = true_ranges(bars[-(period + 1) :])
    return sum(trs[-period:]) / period if len(trs) >= period else None


def rolling_atr_values(bars: list[Candle], period: int, window: int) -> list[float]:
    if len(bars) < period + 1:
        return []
    selected = bars[-(window + period + 1) :]
    values: list[float] = []
    for end in range(period + 1, len(selected) + 1):
        value = atr_value(selected[:end], period)
        if value is not None:
            values.append(value)
    return values


def true_ranges(bars: list[Candle]) -> list[float]:
    ranges: list[float] = []
    for previous, current in zip(bars, bars[1:]):
        ranges.append(
            max(
                current.high - current.low,
                abs(current.high - previous.close),
                abs(current.low - previous.close),
            )
        )
    return ranges


def percentile_rank(values: list[float], value: float) -> float | None:
    if not values:
        return None
    less_or_equal = sum(1 for item in values if item <= value)
    return less_or_equal / len(values)


def adx_value(bars: list[Candle], period: int) -> float | None:
    if len(bars) < period * 2 + 1:
        return None
    plus_dm: list[float] = []
    minus_dm: list[float] = []
    tr_values = true_ranges(bars)
    for previous, current in zip(bars, bars[1:]):
        up_move = current.high - previous.high
        down_move = previous.low - current.low
        plus_dm.append(up_move if up_move > down_move and up_move > 0 else 0.0)
        minus_dm.append(down_move if down_move > up_move and down_move > 0 else 0.0)
    dx_values: list[float] = []
    for index in range(period, len(tr_values) + 1):
        tr_sum = sum(tr_values[index - period : index])
        if tr_sum <= 0:
            continue
        plus_di = 100.0 * sum(plus_dm[index - period : index]) / tr_sum
        minus_di = 100.0 * sum(minus_dm[index - period : index]) / tr_sum
        denom = plus_di + minus_di
        if denom > 0:
            dx_values.append(100.0 * abs(plus_di - minus_di) / denom)
    return sum(dx_values[-period:]) / period if len(dx_values) >= period else None


def rule_gate_for_horizon(
    horizon_minutes: int,
    current: float,
    returns: dict[str, float | None],
    range_positions: dict[str, float | None],
    key_levels: dict[str, Any],
    trend: dict[str, Any],
    volatility: dict[str, Any],
    vwap_data: dict[str, Any],
) -> dict[str, Any]:
    atr_pct = volatility.get("atr_pct") or 0.0
    atr_percentile = volatility.get("atr_percentile_120")
    regime = trend.get("regime")
    position_60 = range_positions.get("position_60m")
    support = key_levels.get("nearest_support")
    resistance = key_levels.get("nearest_resistance")
    support_dist = abs(support.get("distance_pct", 999.0)) if support else 999.0
    resistance_dist = abs(resistance.get("distance_pct", 999.0)) if resistance else 999.0
    near_limit = max(0.08, min(0.30, (atr_pct or 0.12) * 1.4))
    in_middle = position_60 is not None and 0.35 <= position_60 <= 0.65
    hot_vol = atr_percentile is not None and atr_percentile >= 0.9
    ret_5 = returns.get("5m") or 0.0
    ret_15 = returns.get("15m") or 0.0
    vwap_dist = vwap_data.get("distance_pct")
    vwap_slope = vwap_data.get("slope")

    reasons: list[str] = []
    if hot_vol:
        return no_trade_gate("high_volatility_chop", "NONE", "ATR percentile too high")

    if support and support_dist <= near_limit and ret_5 > -0.35:
        reasons.append(f"near support {support['source']} distance={support_dist:.4f}%")
        if regime in ("trend_up", "mixed", "range"):
            return trade_gate("support_bounce", "UP", reasons)

    if resistance and resistance_dist <= near_limit and ret_5 < 0.35:
        reasons.append(f"near resistance {resistance['source']} distance={resistance_dist:.4f}%")
        if regime in ("trend_down", "mixed", "range"):
            return trade_gate("resistance_reject", "DOWN", reasons)

    pullback_limit = max(0.10, min(0.35, (atr_pct or 0.12) * 1.8))
    if regime == "trend_up" and vwap_dist is not None and abs(vwap_dist) <= pullback_limit and ret_15 >= -0.15:
        if vwap_slope in ("up", "flat"):
            return trade_gate("trend_pullback_vwap", "UP", [f"trend_up near VWAP {vwap_dist:.4f}%"])
    if regime == "trend_down" and vwap_dist is not None and abs(vwap_dist) <= pullback_limit and ret_15 <= 0.15:
        if vwap_slope in ("down", "flat"):
            return trade_gate("trend_pullback_vwap", "DOWN", [f"trend_down near VWAP {vwap_dist:.4f}%"])

    if in_middle:
        return no_trade_gate("range_middle", "NONE", "price is in the middle of the 60m range")

    return no_trade_gate("no_key_level_edge", "NONE", "not close enough to support/resistance or VWAP pullback")


def trade_gate(setup: str, direction: str, reasons: list[str]) -> dict[str, Any]:
    return {
        "allow_trade": True,
        "allowed_direction": direction,
        "setup": setup,
        "reason": "; ".join(reasons),
    }


def no_trade_gate(setup: str, direction: str, reason: str) -> dict[str, Any]:
    return {
        "allow_trade": False,
        "allowed_direction": direction,
        "setup": setup,
        "reason": reason,
    }


def range_position(current: float, bars: list[Candle], minutes: int) -> float | None:
    if len(bars) < minutes:
        return None
    selected = bars[-minutes:]
    high = max(candle.high for candle in selected)
    low = min(candle.low for candle in selected)
    if high <= low:
        return None
    return round_float((current - low) / (high - low))


def payout_for_horizon(horizon_minutes: int) -> float:
    return 0.80 if horizon_minutes in (1, 10) else 0.85


def count_open(trades: list[Trade]) -> int:
    return sum(1 for trade in trades if trade.status == "OPEN")


def count_settled(trades: list[Trade]) -> int:
    return sum(1 for trade in trades if trade.status == "SETTLED")


def has_recent_open_signal(
    trades: list[Trade],
    symbol: str,
    horizon_minutes: int,
    now_epoch_ms: int,
    cooldown_minutes: int,
) -> bool:
    cutoff = now_epoch_ms - cooldown_minutes * 60 * 1000
    return any(
        trade.symbol == symbol
        and trade.horizon_minutes == horizon_minutes
        and trade.entry_epoch_ms >= cutoff
        for trade in trades
    )


def coalesce_number(*values: Any) -> float:
    for value in values:
        try:
            if value is not None:
                return float(value)
        except (TypeError, ValueError):
            continue
    return 0.0


def round_float(value: float | None, digits: int = 8) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def now_ms() -> int:
    return int(time.time() * 1000)


def iso_utc_ms(epoch_ms: int) -> str:
    seconds = epoch_ms // 1000
    millis = epoch_ms % 1000
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(seconds)) + f".{millis:03d}Z"


def parse_csv_symbols(value: str) -> list[str]:
    symbols = [part.strip().upper() for part in value.split(",") if part.strip()]
    if not symbols:
        raise ValueError("at least one symbol is required")
    return symbols


def parse_csv_ints(value: str) -> list[int]:
    items = [int(part.strip()) for part in value.split(",") if part.strip()]
    if not items:
        raise ValueError("at least one horizon is required")
    return items


def parse_csv_floats(value: str) -> list[float]:
    items = [float(part.strip()) for part in value.split(",") if part.strip()]
    return [item / 100.0 if item > 1.0 else item for item in items]


def validate_horizons(horizons: list[int]) -> None:
    allowed = {10, 30, 60}
    unsupported = sorted(set(horizons) - allowed)
    if unsupported:
        raise ValueError(f"unsupported horizons: {unsupported}; allowed: 10,30,60")


def first_non_empty(*values: str | None) -> str | None:
    for value in values:
        if value:
            return value
    return None


def mask_proxy(proxy_url: str | None) -> str:
    if not proxy_url:
        return "direct"
    normalized = normalize_proxy_url(proxy_url)
    parsed = urllib.parse.urlparse(normalized)
    netloc = parsed.hostname or ""
    if parsed.port:
        netloc += f":{parsed.port}"
    return urllib.parse.urlunparse((parsed.scheme, netloc, "", "", "", ""))


def print_prompt_template() -> None:
    sample = sample_prompt_snapshot(["BTCUSDT"], [10, 30, 60])
    print("=== SYSTEM PROMPT ===")
    print(SYSTEM_PROMPT)
    print("=== USER PROMPT EXAMPLE ===")
    print(build_user_prompt(sample))


def sample_prompt_snapshot(symbols: list[str], horizons: list[int]) -> dict[str, Any]:
    base_prices = {"BTCUSDT": 65000.0, "ETHUSDT": 3500.0}
    symbol_items = []
    for index, symbol in enumerate(symbols):
        direction_bias = 1 if index % 2 == 0 else -1
        price = base_prices.get(symbol, 1000.0)
        support = price * (1 - 0.0012)
        resistance = price * (1 + 0.0015)
        rule_gates = {
            f"{horizon}m": {
                "allow_trade": horizon == 10 and direction_bias > 0,
                "allowed_direction": "UP" if direction_bias > 0 else "DOWN",
                "setup": "support_bounce" if horizon == 10 else "no_key_level_edge",
                "reason": (
                    "near support sample with trend/VWAP alignment"
                    if horizon == 10 and direction_bias > 0
                    else "not close enough to a high-quality key level"
                ),
            }
            for horizon in horizons
        }
        symbol_items.append(
            {
                "symbol": symbol,
                "entry_index_price": price,
                "price_age_ms": 1200,
                "bars_1m_available": 180,
                "last_closed_bar_utc": "2026-06-28T00:00:00.000Z",
                "returns_pct": {
                    "1m": round(0.02 * direction_bias, 4),
                    "3m": round(0.05 * direction_bias, 4),
                    "5m": round(0.08 * direction_bias, 4),
                    "10m": round(0.14 * direction_bias, 4),
                    "15m": round(0.21 * direction_bias, 4),
                    "30m": round(0.34 * direction_bias, 4),
                    "60m": round(0.48 * direction_bias, 4),
                    "120m": round(0.62 * direction_bias, 4),
                },
                "ema_distance_pct": {
                    "ema_9": round(0.05 * direction_bias, 4),
                    "ema_21": round(0.18 * direction_bias, 4),
                    "ema_55": round(0.42 * direction_bias, 4),
                },
                "realized_volatility_pct": {"10m": 0.06, "30m": 0.09, "60m": 0.12},
                "volume": {
                    "last_5m_sum": 1000.0,
                    "previous_30m_avg_per_min": 150.0,
                    "last_5m_vs_prev_30m_ratio": 1.4,
                },
                "range_position": {
                    "position_30m": 0.82 if direction_bias > 0 else 0.18,
                    "position_60m": 0.76 if direction_bias > 0 else 0.24,
                },
                "key_levels": {
                    "nearest_support": {
                        "kind": "support",
                        "source": "pivot_low",
                        "price": round_float(support),
                        "distance_pct": round_float(pct_change(price, support), 6),
                    },
                    "nearest_resistance": {
                        "kind": "resistance",
                        "source": "pivot_high",
                        "price": round_float(resistance),
                        "distance_pct": round_float(pct_change(price, resistance), 6),
                    },
                },
                "vwap": {
                    "window": "60m",
                    "value": round_float(price * (1 - 0.0004 * direction_bias)),
                    "distance_pct": round(0.04 * direction_bias, 4),
                    "slope": "up" if direction_bias > 0 else "down",
                },
                "trend": {
                    "ema_alignment": "bullish" if direction_bias > 0 else "bearish",
                    "ema21_slope_pct": round(0.08 * direction_bias, 4),
                    "adx_14": 24.5,
                    "regime": "trend_up" if direction_bias > 0 else "trend_down",
                },
                "volatility": {
                    "atr_14": round_float(price * 0.001),
                    "atr_pct": 0.1,
                    "atr_percentile_120": 0.55,
                },
                "rule_gates": rule_gates,
                "last_bar": {
                    "open": price * (1 - 0.0002),
                    "high": price * (1 + 0.0003),
                    "low": price * (1 - 0.0004),
                    "close": price,
                    "volume": 220.0,
                    "return_pct": round(0.02 * direction_bias, 4),
                    "range_pct": 0.07,
                },
            }
        )
    return {
        "timestamp_utc": "2026-06-28T00:00:00.000Z",
        "price_source_for_entry_and_settlement": (
            "Binance USD-M futures index price from markPrice stream field i; "
            "REST /fapi/v1/premiumIndex indexPrice is used as fallback and forced "
            "again after the model response for entry recording. 1m indicators use "
            "USD-M futures klines with REST fallback."
        ),
        "direction_definition": (
            "UP succeeds if expiry index_price > entry index_price; "
            "DOWN succeeds if expiry index_price < entry index_price."
        ),
        "horizons_minutes": horizons,
        "symbols": symbol_items,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Paper-trade GPT predictions for Binance BTC/ETH event-contract style "
            "UP/DOWN outcomes."
        )
    )
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--horizons", default=",".join(map(str, DEFAULT_HORIZONS_MINUTES)))
    parser.add_argument("--probability-threshold", type=float, default=0.65)
    parser.add_argument(
        "--signal-thresholds",
        default="0.60,0.65",
        help="comma-separated probability buckets to track, e.g. 0.60,0.65",
    )
    parser.add_argument("--prediction-interval-sec", type=int, default=60)
    parser.add_argument(
        "--cooldown-minutes",
        type=int,
        default=5,
        help="do not open another paper trade for the same symbol+horizon within this many minutes",
    )
    parser.add_argument("--max-settled-trades", type=int, default=100)
    parser.add_argument("--max-open-trades", type=int, default=1000)
    parser.add_argument("--min-bars", type=int, default=60)
    parser.add_argument("--max-bars", type=int, default=300)
    parser.add_argument("--rest-warmup-bars", type=int, default=180)
    parser.add_argument("--max-price-age-ms", type=int, default=10_000)
    parser.add_argument("--output-dir", default="logs")
    parser.add_argument(
        "--db-path",
        default="logs/event_gpt.sqlite3",
        help="SQLite database path for web UI and persistent records; empty disables DB writes.",
    )
    parser.add_argument("--binance-ws-url", help="override combined Binance websocket URL")
    parser.add_argument(
        "--proxy",
        help=(
            "Proxy for Binance REST/WS, e.g. http://127.0.0.1:7890 or "
            "socks5://127.0.0.1:7890. Defaults to BINANCE_PROXY/HTTPS_PROXY/HTTP_PROXY."
        ),
    )
    parser.add_argument(
        "--openai-proxy",
        help="Optional proxy for OpenAI HTTP calls. Defaults to OPENAI_PROXY; empty means direct connection.",
    )
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--openai-chat-url", default=default_openai_chat_url())
    parser.add_argument("--openai-timeout-seconds", type=float, default=45.0)
    parser.add_argument("--http-timeout-seconds", type=float, default=15.0)
    parser.add_argument("--ws-timeout-seconds", type=float, default=10.0)
    parser.add_argument("--reconnect-delay-seconds", type=float, default=3.0)
    parser.add_argument("--max-reconnect-delay-seconds", type=float, default=60.0)
    parser.add_argument(
        "--no-json-schema",
        action="store_true",
        help="Use JSON object mode instead of strict json_schema response_format.",
    )
    parser.add_argument(
        "--mock-gpt",
        action="store_true",
        help="Use a local heuristic instead of calling OpenAI; useful for connectivity tests.",
    )
    parser.add_argument(
        "--print-prompt-template",
        action="store_true",
        help="Print the GPT prompt template and exit.",
    )
    parser.add_argument(
        "--test-openai",
        action="store_true",
        help="Send one sample request to the configured OpenAI-compatible endpoint and exit.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())
