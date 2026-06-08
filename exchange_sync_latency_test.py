#!/usr/bin/env python3
"""Measure concurrent public time-sync latency for common crypto exchanges.

The script uses only public REST endpoints. It does not read API keys, sign
requests, place orders, or call account/private APIs.

Examples:
  python exchange_sync_latency_test.py
  python exchange_sync_latency_test.py --rounds 10 --interval-ms 1000
  python exchange_sync_latency_test.py --exchanges binance,okx,bybit
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import email.utils
import json
import logging
import re
import socket
import statistics
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


USER_AGENT = "rustcta-exchange-sync-latency-test/0.1"
MAX_RESPONSE_BYTES = 1024 * 1024

TIME_KEY_PRIORITY = (
    "serverTime",
    "server_time",
    "serverTimeMs",
    "server_time_ms",
    "timeNano",
    "time_nano",
    "timestampms",
    "timestamp_ms",
    "timestamp",
    "ts",
    "timeSecond",
    "time_second",
    "unixtime",
    "epoch",
    "time",
)

CSV_FIELDS = (
    "run_id",
    "round",
    "exchange",
    "ok",
    "http_status",
    "rtt_ms",
    "estimated_one_way_ms",
    "server_time_ms",
    "local_mid_time_ms",
    "clock_offset_ms",
    "time_source",
    "endpoint",
    "final_url",
    "error",
)


@dataclass(frozen=True)
class ExchangeSpec:
    exchange: str
    time_url: str


def exchange_specs() -> list[ExchangeSpec]:
    return [
        ExchangeSpec("binance", "https://api.binance.com/api/v3/time"),
        ExchangeSpec("binance_us", "https://api.binance.us/api/v3/time"),
        ExchangeSpec("okx", "https://www.okx.com/api/v5/public/time"),
        ExchangeSpec("bybit", "https://api.bybit.com/v5/market/time"),
        ExchangeSpec("bitget", "https://api.bitget.com/api/v2/public/time"),
        ExchangeSpec("kucoin", "https://api.kucoin.com/api/v1/timestamp"),
        ExchangeSpec("gate", "https://api.gateio.ws/api/v4/spot/time"),
        ExchangeSpec("mexc", "https://api.mexc.com/api/v3/time"),
        ExchangeSpec("htx", "https://api.huobi.pro/v1/common/timestamp"),
        ExchangeSpec("kraken", "https://api.kraken.com/0/public/Time"),
        ExchangeSpec("coinbase", "https://api.coinbase.com/v2/time"),
        ExchangeSpec("bitstamp", "https://www.bitstamp.net/api/v2/ticker/btcusd/"),
        ExchangeSpec("gemini", "https://api.gemini.com/v1/symbols"),
        ExchangeSpec("bitfinex", "https://api-pub.bitfinex.com/v2/platform/status"),
        ExchangeSpec("deribit", "https://www.deribit.com/api/v2/public/get_time"),
        ExchangeSpec(
            "crypto_com",
            "https://api.crypto.com/exchange/v1/public/get-tickers?instrument_name=BTC_USD",
        ),
        ExchangeSpec("bingx", "https://open-api.bingx.com/openApi/spot/v1/server/time"),
        ExchangeSpec("poloniex", "https://api.poloniex.com/timestamp"),
        ExchangeSpec("coinex", "https://api.coinex.com/v2/time"),
        ExchangeSpec("bitmart", "https://api-cloud.bitmart.com/system/time"),
    ]


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Synchronously probe 20 public exchange time endpoints and log "
            "RTT/clock-offset metrics."
        )
    )
    parser.add_argument("--rounds", type=positive_int, default=3)
    parser.add_argument("--timeout-ms", type=positive_int, default=5000)
    parser.add_argument("--interval-ms", type=non_negative_int, default=1000)
    parser.add_argument("--workers", type=positive_int, default=0)
    parser.add_argument("--log-dir", default="logs")
    parser.add_argument(
        "--exchanges",
        help="comma-separated exchange filter, e.g. binance,okx,bybit",
    )
    parser.add_argument(
        "--fail-on-error",
        action="store_true",
        help="exit with code 1 when any exchange probe fails",
    )
    args = parser.parse_args()

    specs = filter_specs(exchange_specs(), args.exchanges)
    if not specs:
        available = ", ".join(spec.exchange for spec in exchange_specs())
        raise SystemExit(f"no exchange matched. Available exchanges: {available}")

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir = Path(args.log_dir)
    logger, plain_log_path = setup_logger(log_dir, run_id)
    jsonl_path = log_dir / f"exchange_sync_latency_{run_id}.jsonl"
    csv_path = log_dir / f"exchange_sync_latency_{run_id}.csv"

    worker_count = max(args.workers, len(specs))
    timeout_seconds = args.timeout_ms / 1000.0
    interval_seconds = args.interval_ms / 1000.0
    all_results: list[dict[str, Any]] = []

    logger.info(
        "starting exchange sync latency test run_id=%s exchanges=%d rounds=%d "
        "timeout_ms=%d interval_ms=%d workers=%d",
        run_id,
        len(specs),
        args.rounds,
        args.timeout_ms,
        args.interval_ms,
        worker_count,
    )
    logger.info("plain_log=%s", plain_log_path)
    logger.info("jsonl_log=%s", jsonl_path)
    logger.info("csv_log=%s", csv_path)

    with jsonl_path.open("w", encoding="utf-8") as jsonl_file, csv_path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as csv_file:
        csv_writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDS)
        csv_writer.writeheader()

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=worker_count,
            thread_name_prefix="exchange-sync",
        ) as executor:
            for round_number in range(1, args.rounds + 1):
                logger.info("round=%d start exchanges=%d", round_number, len(specs))
                round_results = run_round(
                    run_id,
                    round_number,
                    specs,
                    timeout_seconds,
                    executor,
                    logger,
                )

                for result in round_results:
                    all_results.append(result)
                    write_result(jsonl_file, csv_writer, result)
                    log_result(logger, result)

                jsonl_file.flush()
                csv_file.flush()
                log_round_summary(logger, round_number, round_results)

                if round_number != args.rounds and interval_seconds > 0:
                    time.sleep(interval_seconds)

    log_final_summary(logger, all_results)
    failed = [result for result in all_results if not result["ok"]]
    return 1 if args.fail_on_error and failed else 0


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be > 0")
    return parsed


def non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be >= 0")
    return parsed


def filter_specs(specs: list[ExchangeSpec], filter_value: str | None) -> list[ExchangeSpec]:
    if not filter_value:
        return specs
    wanted = {
        value.strip().lower()
        for value in filter_value.split(",")
        if value.strip()
    }
    return [spec for spec in specs if spec.exchange.lower() in wanted]


def setup_logger(log_dir: Path, run_id: str) -> tuple[logging.Logger, Path]:
    log_dir.mkdir(parents=True, exist_ok=True)
    plain_log_path = log_dir / f"exchange_sync_latency_{run_id}.log"

    logger = logging.getLogger("exchange_sync_latency_test")
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    logger.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s.%(msecs)03d %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(plain_log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger, plain_log_path


def run_round(
    run_id: str,
    round_number: int,
    specs: list[ExchangeSpec],
    timeout_seconds: float,
    executor: concurrent.futures.ThreadPoolExecutor,
    logger: logging.Logger,
) -> list[dict[str, Any]]:
    start_gate = threading.Barrier(len(specs) + 1)
    futures = [
        executor.submit(
            probe_after_gate,
            run_id,
            round_number,
            spec,
            timeout_seconds,
            start_gate,
        )
        for spec in specs
    ]

    try:
        start_gate.wait(timeout=10)
    except threading.BrokenBarrierError:
        logger.error("round=%d failed to release simultaneous start gate", round_number)

    results: list[dict[str, Any]] = []
    for future in concurrent.futures.as_completed(futures):
        results.append(future.result())
    return sorted(results, key=lambda item: item["exchange"])


def probe_after_gate(
    run_id: str,
    round_number: int,
    spec: ExchangeSpec,
    timeout_seconds: float,
    start_gate: threading.Barrier,
) -> dict[str, Any]:
    try:
        start_gate.wait(timeout=10)
    except threading.BrokenBarrierError:
        return failure_result(
            run_id,
            round_number,
            spec,
            "start gate failed before request dispatch",
        )
    return probe_exchange(run_id, round_number, spec, timeout_seconds)


def probe_exchange(
    run_id: str,
    round_number: int,
    spec: ExchangeSpec,
    timeout_seconds: float,
) -> dict[str, Any]:
    request = urllib.request.Request(
        spec.time_url,
        headers={
            "Accept": "application/json,text/plain,*/*",
            "User-Agent": USER_AGENT,
        },
    )

    start_wall_ms = wall_clock_ms()
    start_mono_ns = time.perf_counter_ns()
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            body = response.read(MAX_RESPONSE_BYTES)
            end_mono_ns = time.perf_counter_ns()
            end_wall_ms = wall_clock_ms()
            http_status = response.getcode()
            final_url = response.geturl()
            headers = response.headers
    except urllib.error.HTTPError as error:
        end_mono_ns = time.perf_counter_ns()
        end_wall_ms = wall_clock_ms()
        body = safe_read_error_body(error)
        return build_result(
            run_id=run_id,
            round_number=round_number,
            spec=spec,
            ok=False,
            http_status=error.code,
            start_wall_ms=start_wall_ms,
            end_wall_ms=end_wall_ms,
            start_mono_ns=start_mono_ns,
            end_mono_ns=end_mono_ns,
            server_time_ms=None,
            time_source=None,
            final_url=error.geturl(),
            error=f"http status {error.code}: {short_text(body)}",
        )
    except socket.timeout:
        return failure_result(
            run_id,
            round_number,
            spec,
            f"timed out after {int(timeout_seconds * 1000)} ms",
            start_wall_ms=start_wall_ms,
            start_mono_ns=start_mono_ns,
        )
    except Exception as error:  # noqa: BLE001 - diagnostic probe reports exact failure.
        return failure_result(
            run_id,
            round_number,
            spec,
            f"{type(error).__name__}: {error}",
            start_wall_ms=start_wall_ms,
            start_mono_ns=start_mono_ns,
        )

    payload = parse_json_body(body)
    server_time_ms: float | None = None
    time_source: str | None = None

    if payload is not None:
        server_time_ms, time_source = extract_time_from_json(payload)
    if server_time_ms is None:
        server_time_ms = parse_http_date_ms(headers.get("Date"))
        if server_time_ms is not None:
            time_source = "http_date"

    redirect_error = cross_host_redirect_error(spec.time_url, final_url)
    ok = 200 <= http_status < 300 and server_time_ms is not None and redirect_error is None
    error = None
    if redirect_error is not None:
        error = redirect_error
    elif not 200 <= http_status < 300:
        error = f"http status {http_status}"
    elif server_time_ms is None:
        error = "no server time found in JSON body or HTTP Date header"

    return build_result(
        run_id=run_id,
        round_number=round_number,
        spec=spec,
        ok=ok,
        http_status=http_status,
        start_wall_ms=start_wall_ms,
        end_wall_ms=end_wall_ms,
        start_mono_ns=start_mono_ns,
        end_mono_ns=end_mono_ns,
        server_time_ms=server_time_ms,
        time_source=time_source,
        final_url=final_url,
        error=error,
    )


def failure_result(
    run_id: str,
    round_number: int,
    spec: ExchangeSpec,
    error: str,
    start_wall_ms: float | None = None,
    start_mono_ns: int | None = None,
) -> dict[str, Any]:
    start_wall_ms = wall_clock_ms() if start_wall_ms is None else start_wall_ms
    start_mono_ns = time.perf_counter_ns() if start_mono_ns is None else start_mono_ns
    return build_result(
        run_id=run_id,
        round_number=round_number,
        spec=spec,
        ok=False,
        http_status=None,
        start_wall_ms=start_wall_ms,
        end_wall_ms=wall_clock_ms(),
        start_mono_ns=start_mono_ns,
        end_mono_ns=time.perf_counter_ns(),
        server_time_ms=None,
        time_source=None,
        error=error,
    )


def build_result(
    *,
    run_id: str,
    round_number: int,
    spec: ExchangeSpec,
    ok: bool,
    http_status: int | None,
    start_wall_ms: float,
    end_wall_ms: float,
    start_mono_ns: int,
    end_mono_ns: int,
    server_time_ms: float | None,
    time_source: str | None,
    error: str | None,
    final_url: str | None = None,
) -> dict[str, Any]:
    rtt_ms = (end_mono_ns - start_mono_ns) / 1_000_000.0
    local_mid_time_ms = (start_wall_ms + end_wall_ms) / 2.0
    clock_offset_ms = (
        server_time_ms - local_mid_time_ms if server_time_ms is not None else None
    )
    return {
        "run_id": run_id,
        "round": round_number,
        "exchange": spec.exchange,
        "ok": ok,
        "http_status": http_status,
        "rtt_ms": round(rtt_ms, 3),
        "estimated_one_way_ms": round(rtt_ms / 2.0, 3),
        "server_time_ms": round(server_time_ms, 3) if server_time_ms is not None else None,
        "local_mid_time_ms": round(local_mid_time_ms, 3),
        "clock_offset_ms": (
            round(clock_offset_ms, 3) if clock_offset_ms is not None else None
        ),
        "time_source": time_source,
        "endpoint": spec.time_url,
        "final_url": final_url or spec.time_url,
        "error": error,
    }


def wall_clock_ms() -> float:
    return time.time_ns() / 1_000_000.0


def safe_read_error_body(error: urllib.error.HTTPError) -> bytes:
    try:
        return error.read(512)
    except Exception:  # noqa: BLE001 - best effort error detail only.
        return b""


def short_text(body: bytes) -> str:
    text = body.decode("utf-8", errors="replace")
    text = " ".join(text.split())
    return text[:240]


def cross_host_redirect_error(original_url: str, final_url: str | None) -> str | None:
    if not final_url or final_url == original_url:
        return None

    original = urllib.parse.urlparse(original_url)
    final = urllib.parse.urlparse(final_url)
    if original.netloc.lower() == final.netloc.lower():
        return None
    return f"redirected to different host: {final_url}"


def parse_json_body(body: bytes) -> Any | None:
    if not body:
        return None
    text = body.decode("utf-8", errors="replace").strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def extract_time_from_json(payload: Any) -> tuple[float | None, str | None]:
    for key in TIME_KEY_PRIORITY:
        found = find_key(payload, key)
        if found is None:
            continue
        value, path = found
        parsed = parse_time_value_ms(value)
        if parsed is not None:
            return parsed, f"json:{path}"

    if isinstance(payload, dict):
        for key in ("data", "result"):
            if key not in payload:
                continue
            parsed = parse_time_value_ms(payload[key])
            if parsed is not None:
                return parsed, f"json:{key}"

    parsed = parse_time_value_ms(payload)
    if parsed is not None:
        return parsed, "json:$"
    return None, None


def find_key(payload: Any, wanted_key: str, path: str = "$") -> tuple[Any, str] | None:
    wanted_lower = wanted_key.lower()
    if isinstance(payload, dict):
        for key, value in payload.items():
            child_path = f"{path}.{key}"
            if str(key).lower() == wanted_lower:
                return value, child_path
        for key, value in payload.items():
            child_path = f"{path}.{key}"
            found = find_key(value, wanted_key, child_path)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for index, value in enumerate(payload):
            found = find_key(value, wanted_key, f"{path}[{index}]")
            if found is not None:
                return found
    return None


def parse_time_value_ms(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return normalize_epoch_ms(float(value))
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if re.fullmatch(r"-?\d+(\.\d+)?", text):
            return normalize_epoch_ms(float(text))
        parsed_dt = parse_datetime_text(text)
        if parsed_dt is not None:
            return parsed_dt.timestamp() * 1000.0
    return None


def normalize_epoch_ms(value: float) -> float | None:
    absolute = abs(value)
    if absolute > 1e17:
        return value / 1_000_000.0
    if absolute > 1e14:
        return value / 1_000.0
    if absolute > 1e11:
        return value
    if absolute > 1e9:
        return value * 1000.0
    return None


def parse_datetime_text(text: str) -> datetime | None:
    try:
        parsed = email.utils.parsedate_to_datetime(text)
        if parsed is not None:
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        pass

    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def parse_http_date_ms(value: str | None) -> float | None:
    if not value:
        return None
    parsed = parse_datetime_text(value)
    if parsed is None:
        return None
    return parsed.timestamp() * 1000.0


def write_result(
    jsonl_file: Any,
    csv_writer: csv.DictWriter[str],
    result: dict[str, Any],
) -> None:
    jsonl_file.write(json.dumps(result, ensure_ascii=False, sort_keys=True) + "\n")
    csv_writer.writerow({field: result.get(field) for field in CSV_FIELDS})


def log_result(logger: logging.Logger, result: dict[str, Any]) -> None:
    if result["ok"]:
        logger.info(
            "round=%d exchange=%-11s ok status=%s rtt_ms=%8.3f "
            "one_way_ms=%8.3f offset_ms=%9.3f source=%s",
            result["round"],
            result["exchange"],
            result["http_status"],
            result["rtt_ms"],
            result["estimated_one_way_ms"],
            result["clock_offset_ms"],
            result["time_source"],
        )
        return

    logger.warning(
        "round=%d exchange=%-11s fail status=%s rtt_ms=%s error=%s",
        result["round"],
        result["exchange"],
        result["http_status"],
        format_optional_ms(result["rtt_ms"]),
        result["error"],
    )


def log_round_summary(
    logger: logging.Logger,
    round_number: int,
    results: list[dict[str, Any]],
) -> None:
    successes = [result for result in results if result["ok"]]
    failures = len(results) - len(successes)
    if not successes:
        logger.warning(
            "round=%d summary ok=0 fail=%d no successful latency samples",
            round_number,
            failures,
        )
        return

    rtts = [float(result["rtt_ms"]) for result in successes]
    offsets = [abs(float(result["clock_offset_ms"])) for result in successes]
    logger.info(
        "round=%d summary ok=%d fail=%d rtt_avg_ms=%s rtt_p95_ms=%s "
        "abs_offset_max_ms=%s",
        round_number,
        len(successes),
        failures,
        format_ms(statistics.fmean(rtts)),
        format_ms(percentile(rtts, 95)),
        format_ms(max(offsets)),
    )


def log_final_summary(logger: logging.Logger, results: list[dict[str, Any]]) -> None:
    logger.info("final summary by exchange:")
    for exchange in sorted({result["exchange"] for result in results}):
        exchange_results = [result for result in results if result["exchange"] == exchange]
        successes = [result for result in exchange_results if result["ok"]]
        failures = len(exchange_results) - len(successes)
        if not successes:
            logger.info("%-11s ok=0 fail=%d", exchange, failures)
            continue

        rtts = [float(result["rtt_ms"]) for result in successes]
        offsets = [float(result["clock_offset_ms"]) for result in successes]
        logger.info(
            "%-11s ok=%d fail=%d rtt_min/avg/p95_ms=%s/%s/%s "
            "offset_avg_ms=%s offset_abs_max_ms=%s",
            exchange,
            len(successes),
            failures,
            format_ms(min(rtts)),
            format_ms(statistics.fmean(rtts)),
            format_ms(percentile(rtts, 95)),
            format_ms(statistics.fmean(offsets)),
            format_ms(max(abs(offset) for offset in offsets)),
        )


def percentile(values: list[float], percentile_value: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * (percentile_value / 100.0)
    lower_index = int(rank)
    upper_index = min(lower_index + 1, len(ordered) - 1)
    weight = rank - lower_index
    return ordered[lower_index] * (1.0 - weight) + ordered[upper_index] * weight


def format_ms(value: float) -> str:
    return f"{value:.3f}"


def format_optional_ms(value: Any) -> str:
    if value is None:
        return "NA"
    return format_ms(float(value))


if __name__ == "__main__":
    sys.exit(main())
