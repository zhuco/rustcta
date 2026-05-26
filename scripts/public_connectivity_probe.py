#!/usr/bin/env python3
"""Probe public exchange market WebSocket and fallback REST connectivity.

The probe intentionally uses only public market-data endpoints. It never reads
API keys, never signs requests, and never touches order/account APIs.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import socket
import ssl
import struct
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ProbeSpec:
    exchange: str
    ws_url: str
    ws_subscribe: dict[str, Any] | None
    rest_url: str


def probe_specs() -> list[ProbeSpec]:
    now = int(time.time())
    return [
        ProbeSpec(
            exchange="binance",
            ws_url="wss://stream.binance.com:9443/ws/btcusdt@bookTicker",
            ws_subscribe=None,
            rest_url="https://api.binance.com/api/v3/ping",
        ),
        ProbeSpec(
            exchange="okx",
            ws_url="wss://ws.okx.com:8443/ws/v5/public",
            ws_subscribe={
                "op": "subscribe",
                "args": [{"channel": "tickers", "instId": "BTC-USDT"}],
            },
            rest_url="https://www.okx.com/api/v5/public/time",
        ),
        ProbeSpec(
            exchange="bitget",
            ws_url="wss://ws.bitget.com/v2/ws/public",
            ws_subscribe={
                "op": "subscribe",
                "args": [
                    {"instType": "SPOT", "channel": "ticker", "instId": "BTCUSDT"}
                ],
            },
            rest_url="https://api.bitget.com/api/v2/public/time",
        ),
        ProbeSpec(
            exchange="gate",
            ws_url="wss://api.gateio.ws/ws/v4/",
            ws_subscribe={
                "time": now,
                "channel": "spot.book_ticker",
                "event": "subscribe",
                "payload": ["BTC_USDT"],
            },
            rest_url="https://api.gateio.ws/api/v4/spot/time",
        ),
    ]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Probe public exchange WS and REST connectivity."
    )
    parser.add_argument("--timeout-ms", type=int, default=5000)
    parser.add_argument("--json", action="store_true", help="emit JSON")
    parser.add_argument(
        "--exchanges",
        help="comma-separated exchange filter: binance,okx,bitget,gate",
    )
    args = parser.parse_args()

    specs = filter_specs(probe_specs(), args.exchanges)
    timeout_seconds = args.timeout_ms / 1000.0
    results: list[dict[str, Any]] = []

    for spec in specs:
        results.append(probe_ws(spec, timeout_seconds))
        results.append(probe_rest(spec, timeout_seconds))

    if args.json:
        print(json.dumps(results, ensure_ascii=False, indent=2))
    else:
        print_table(results)

    return 1 if any(not result["ok"] for result in results) else 0


def filter_specs(specs: list[ProbeSpec], filter_value: str | None) -> list[ProbeSpec]:
    if not filter_value:
        return specs
    wanted = {
        value.strip().lower()
        for value in filter_value.split(",")
        if value.strip()
    }
    if not wanted:
        return specs
    return [spec for spec in specs if spec.exchange in wanted]


def probe_rest(spec: ProbeSpec, timeout_seconds: float) -> dict[str, Any]:
    started = time.monotonic()
    request = urllib.request.Request(
        spec.rest_url,
        headers={
            "Accept": "application/json",
            "User-Agent": "rustcta-public-connectivity-probe/0.1",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            status_code = response.getcode()
            response.read(256)
        ok = 200 <= status_code < 300
        return result(
            spec,
            "rest",
            spec.rest_url,
            ok,
            started,
            "ok" if ok else "http_status",
            f"http status {status_code}",
        )
    except urllib.error.HTTPError as error:
        return result(
            spec,
            "rest",
            spec.rest_url,
            False,
            started,
            "http_status",
            f"http status {error.code}",
        )
    except socket.timeout:
        return result(
            spec,
            "rest",
            spec.rest_url,
            False,
            started,
            "timeout",
            f"timed out after {int(timeout_seconds * 1000)} ms",
        )
    except Exception as error:  # noqa: BLE001 - probe should report exact failure.
        return result(
            spec,
            "rest",
            spec.rest_url,
            False,
            started,
            type(error).__name__,
            str(error),
        )


def probe_ws(spec: ProbeSpec, timeout_seconds: float) -> dict[str, Any]:
    started = time.monotonic()
    sock: socket.socket | ssl.SSLSocket | None = None
    try:
        sock = websocket_connect(spec.ws_url, timeout_seconds)
        if spec.ws_subscribe is not None:
            websocket_send_text(sock, json.dumps(spec.ws_subscribe, separators=(",", ":")))
        detail = websocket_recv_summary(sock, timeout_seconds)
        return result(spec, "ws", spec.ws_url, True, started, "ok", detail)
    except socket.timeout:
        return result(
            spec,
            "ws",
            spec.ws_url,
            False,
            started,
            "timeout",
            f"timed out after {int(timeout_seconds * 1000)} ms",
        )
    except Exception as error:  # noqa: BLE001 - probe should report exact failure.
        return result(
            spec,
            "ws",
            spec.ws_url,
            False,
            started,
            type(error).__name__,
            str(error),
        )
    finally:
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass


def websocket_connect(url: str, timeout_seconds: float) -> ssl.SSLSocket:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != "wss":
        raise ValueError(f"only wss is supported: {url}")
    if not parsed.hostname:
        raise ValueError(f"missing websocket host: {url}")

    port = parsed.port or 443
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"

    raw_sock = socket.create_connection((parsed.hostname, port), timeout=timeout_seconds)
    raw_sock.settimeout(timeout_seconds)
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
            "User-Agent: rustcta-public-connectivity-probe/0.1",
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


def websocket_accept(key: str) -> str:
    guid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
    digest = hashlib.sha1(f"{key}{guid}".encode("ascii")).digest()
    return base64.b64encode(digest).decode("ascii")


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


def websocket_send_text(sock: socket.socket | ssl.SSLSocket, text: str) -> None:
    payload = text.encode("utf-8")
    frame = bytearray([0x81])
    if len(payload) < 126:
        frame.append(0x80 | len(payload))
    elif len(payload) <= 0xFFFF:
        frame.append(0x80 | 126)
        frame.extend(struct.pack("!H", len(payload)))
    else:
        frame.append(0x80 | 127)
        frame.extend(struct.pack("!Q", len(payload)))

    mask = os.urandom(4)
    frame.extend(mask)
    frame.extend(byte ^ mask[index % 4] for index, byte in enumerate(payload))
    sock.sendall(frame)


def websocket_recv_summary(
    sock: socket.socket | ssl.SSLSocket, timeout_seconds: float
) -> str:
    deadline = time.monotonic() + timeout_seconds
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise socket.timeout()
        sock.settimeout(remaining)

        opcode, payload = websocket_recv_frame(sock)
        if opcode == 0x1:
            text = payload.decode("utf-8", errors="replace")
            compact = " ".join(text.split())
            if len(compact) > 180:
                compact = compact[:180] + "..."
            return f"text frame: {compact}"
        if opcode == 0x2:
            return f"binary frame: {len(payload)} bytes"
        if opcode == 0x8:
            raise RuntimeError(f"closed by server: {payload!r}")
        if opcode == 0x9:
            websocket_send_control(sock, 0xA, payload)
            return "ping frame"
        if opcode == 0xA:
            return "pong frame"


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
    sock: socket.socket | ssl.SSLSocket, opcode: int, payload: bytes
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


def result(
    spec: ProbeSpec,
    probe: str,
    endpoint: str,
    ok: bool,
    started: float,
    status: str,
    detail: str,
) -> dict[str, Any]:
    return {
        "exchange": spec.exchange,
        "probe": probe,
        "endpoint": endpoint,
        "ok": ok,
        "latency_ms": int((time.monotonic() - started) * 1000),
        "status": status,
        "detail": detail,
    }


def print_table(results: list[dict[str, Any]]) -> None:
    print(
        f"{'exchange':<8} {'type':<4} {'ok':<5} {'latency_ms':>10}  "
        f"{'status':<18} detail"
    )
    for item in results:
        print(
            f"{item['exchange']:<8} {item['probe']:<4} {str(item['ok']):<5} "
            f"{item['latency_ms']:>10}  {item['status']:<18} {item['detail']}"
        )


if __name__ == "__main__":
    sys.exit(main())
