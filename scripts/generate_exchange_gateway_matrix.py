#!/usr/bin/env python3
"""Generate exchange gateway capability matrices from endpoint mappings.

The generated files are evidence from the current checkout. They do not claim
official venue support unless that support is explicitly present in the checked
in mapping or adapter document.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import re
from collections import Counter
from pathlib import Path
from typing import Any

import yaml


ADAPTERS_DIR = Path("crates/rustcta-exchange-gateway/src/adapters")
GATEWAY_DOC_DIR = Path("docs/交易所网关")
ADAPTER_DOC_DIR = GATEWAY_DOC_DIR / "适配器"
DEFAULT_MARKDOWN_OUT = GATEWAY_DOC_DIR / "交易所功能盘点矩阵.md"
DEFAULT_CSV_OUT = GATEWAY_DOC_DIR / "交易所功能盘点矩阵.csv"

STANDARD_OPS: list[tuple[str, str]] = [
    ("symbol_rules", "get_symbol_rules"),
    ("order_book", "get_order_book"),
    ("balances", "get_balances"),
    ("positions", "get_positions"),
    ("fees", "get_fees"),
    ("place", "place_order"),
    ("quote_market", "place_quote_market_order"),
    ("cancel", "cancel_order"),
    ("amend", "amend_order"),
    ("order_list", "place_order_list"),
    ("batch_place", "batch_place_orders"),
    ("batch_cancel", "batch_cancel_orders"),
    ("cancel_all", "cancel_all_orders"),
    ("query_order", "query_order"),
    ("open_orders", "get_open_orders"),
    ("fills", "get_recent_fills"),
]

OP_ALIASES = {
    "symbol_rules": "get_symbol_rules",
    "get_symbol_rules_spot": "get_symbol_rules",
    "get_symbol_rules_perp": "get_symbol_rules",
    "get_symbol_rules_futures": "get_symbol_rules",
    "get_contract_symbol_rules": "get_symbol_rules",
    "order_book": "get_order_book",
    "get_order_book_spot": "get_order_book",
    "get_order_book_perp": "get_order_book",
    "get_order_book_futures": "get_order_book",
    "get_contract_order_book": "get_order_book",
    "balances": "get_balances",
    "get_balances_spot": "get_balances",
    "get_balances_perp": "get_balances",
    "get_balances_futures": "get_balances",
    "get_contract_balances": "get_balances",
    "positions": "get_positions",
    "fees": "get_fees",
    "get_fees_futures": "get_fees",
    "quote_market_order": "place_quote_market_order",
    "place_order_spot": "place_order",
    "place_order_perp": "place_order",
    "place_order_futures": "place_order",
    "place_contract_order": "place_order",
    "cancel_order_spot": "cancel_order",
    "cancel_order_perp": "cancel_order",
    "cancel_order_futures": "cancel_order",
    "cancel_contract_order": "cancel_order",
    "amend_order_futures": "amend_order",
    "order_list": "place_order_list",
    "batch_orders": "batch_place_orders",
    "bulk_orders": "batch_place_orders",
    "batch_place_orders_futures": "batch_place_orders",
    "batch_place_contract_orders": "batch_place_orders",
    "batch_cancel": "batch_cancel_orders",
    "batch_cancel_orders_futures": "batch_cancel_orders",
    "cancel_all_orders_futures": "cancel_all_orders",
    "query_order_futures": "query_order",
    "open_orders": "get_open_orders",
    "get_open_orders_spot": "get_open_orders",
    "get_open_orders_perp": "get_open_orders",
    "get_open_orders_futures": "get_open_orders",
    "recent_fills": "get_recent_fills",
    "get_recent_fills_spot": "get_recent_fills",
    "get_recent_fills_perp": "get_recent_fills",
    "get_recent_fills_futures": "get_recent_fills",
}

RUNTIME_IMPL_RE = re.compile(r"\bfn\s+([a-zA-Z_][a-zA-Z0-9_]*)_impl\s*\(")
INTERVAL_RE = re.compile(r"\b\d+(?:ms|s|m)\b", re.IGNORECASE)
INTERVAL_VALUE_RE = re.compile(r"\b(\d+)(ms|s|m)\b", re.IGNORECASE)
DEPTH_RE = re.compile(
    r"(?:orderbook[._:/-]?|depth|d|level|l)(\d{1,4})\b", re.IGNORECASE
)
ORDERBOOK_LINE_RE = re.compile(r"book|depth|bbo", re.IGNORECASE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate current exchange gateway capability matrix.",
    )
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--markdown-out", type=Path, default=DEFAULT_MARKDOWN_OUT)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--status-date", default="2026-06-08")
    return parser.parse_args()


def read_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def read_text(path: Path) -> str:
    if not path.exists():
        return ""
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return path.read_text(encoding="utf-8", errors="ignore")


def rel(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix()


def as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def normalize_market_type(value: Any) -> str:
    value = str(value).strip().lower().replace("-", "_")
    if value in {"linear_perp", "perps", "perpetuals"}:
        return "perpetual"
    if value == "options":
        return "option"
    return value


def normalize_operation(value: Any) -> str:
    op = str(value or "").strip()
    if not op:
        return ""
    op = OP_ALIASES.get(op, op)
    for suffix in ("_spot", "_perp", "_futures", "_contract"):
        if op.endswith(suffix):
            op = op[: -len(suffix)]
            op = OP_ALIASES.get(op, op)
    return op


def iter_endpoint_items(data: dict[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for section in ("endpoints", "streams"):
        value = data.get(section)
        if isinstance(value, list):
            items.extend(item for item in value if isinstance(item, dict))
        elif isinstance(value, dict):
            items.extend(item for item in value.values() if isinstance(item, dict))
    return items


def support_bucket(item: dict[str, Any]) -> str:
    raw = str(item.get("support", "") or "").strip().lower()
    path = str(item.get("path", "") or "").lower()
    auth = str(item.get("auth", "") or "").lower()
    if raw in {"unsupported", "unsupported_unverified"} or "/unsupported/" in path:
        return "不支持"
    if "unsupported" in auth:
        return "不支持"
    if raw in {"composed"}:
        return "组合"
    if raw in {"rest_fallback"}:
        return "REST兜底"
    if raw in {
        "spec_only",
        "spec_only_when_credentials_configured",
        "request_spec_only",
        "parser_only",
        "payload_helper",
        "payload_helper_only",
        "auth_payload_only",
        "native_specs_only",
        "payload_spec",
        "auth_payload_shape_only",
    }:
        return "离线"
    if raw in {
        "native",
        "native_when_credentials_configured",
        "native_when_wallet_configured",
        "spot_only_with_subscribe_key",
    }:
        return "原生"
    if not raw:
        return "映射"
    if "spec" in raw or "parser" in raw or "payload" in raw:
        return "离线"
    if "native" in raw:
        return "原生"
    return raw


STATUS_RANK = {
    "-": 0,
    "不支持": 1,
    "离线": 2,
    "映射": 3,
    "REST兜底": 4,
    "组合": 5,
    "原生": 6,
    "运行": 7,
}


def pick_better(current: str, candidate: str) -> str:
    if STATUS_RANK.get(candidate, 0) > STATUS_RANK.get(current, 0):
        return candidate
    return current


def rust_runtime_impls(adapter_dir: Path) -> set[str]:
    impls: set[str] = set()
    for path in adapter_dir.glob("*.rs"):
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            text = path.read_text(encoding="utf-8", errors="ignore")
        for name in RUNTIME_IMPL_RE.findall(text):
            impls.add(normalize_operation(name))
    return impls


def endpoint_status_by_op(
    endpoint_items: list[dict[str, Any]],
    runtime_impls: set[str],
) -> dict[str, str]:
    status = {op: "-" for _, op in STANDARD_OPS}
    for item in endpoint_items:
        op = normalize_operation(item.get("operation"))
        if op not in status:
            continue
        bucket = support_bucket(item)
        if bucket in {"原生", "映射", "组合", "REST兜底"} and op in runtime_impls:
            bucket = "运行"
        status[op] = pick_better(status[op], bucket)
    return status


def get_nested(data: dict[str, Any], *keys: str) -> Any:
    current: Any = data
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def ws_support(data: dict[str, Any], scope: str) -> str:
    value = get_nested(data, "websocket", scope)
    if isinstance(value, dict):
        support = value.get("support")
        if support:
            return str(support)
        if value.get("supports_subscribe") or value.get("channels"):
            return "declared"

    value = get_nested(data, "streams", scope)
    if isinstance(value, dict):
        support = value.get("support")
        if support:
            return str(support)
        if value.get("channels"):
            return "declared"

    operation = f"subscribe_{scope}_stream"
    for item in iter_endpoint_items(data):
        if normalize_operation(item.get("operation")) != operation:
            continue
        return support_bucket(item)

    websocket = data.get("websocket")
    if isinstance(websocket, dict):
        support = str(websocket.get("support", "") or "").lower()
        if "unsupported" in support:
            return "unsupported"
        if "parser" in support:
            return "parser_only"
        if "request-spec" in support or "request_spec" in support or "spec" in support:
            return "spec_only"
    return "未声明"


def collect_channels(data: dict[str, Any], scope: str = "public") -> list[str]:
    channels: list[str] = []
    ws_scope = get_nested(data, "websocket", scope)
    if isinstance(ws_scope, dict):
        for key in ("channels", "public_channels", "streams"):
            for item in as_list(ws_scope.get(key)):
                channels.append(str(item))
    stream_scope = get_nested(data, "streams", scope)
    if isinstance(stream_scope, dict):
        stream_channels = stream_scope.get("channels")
        if isinstance(stream_channels, dict):
            for key, value in stream_channels.items():
                channels.append(str(key))
                channels.append(str(value))
        else:
            for item in as_list(stream_channels):
                channels.append(str(item))
    for item in iter_endpoint_items(data):
        if str(item.get("transport", "")).lower() not in {"websocket", "ws", "socket_io"}:
            continue
        auth = str(item.get("auth", "")).lower()
        if scope == "public" and auth not in {"", "none", "public"}:
            continue
        for key in ("channel", "stream", "topic", "operation"):
            if item.get(key):
                channels.append(str(item[key]))
    return sorted(set(channel for channel in channels if channel and channel != "None"))


def extract_orderbook_channels(channels: list[str]) -> list[str]:
    markers = ("book", "depth", "bbo")
    return [channel for channel in channels if any(marker in channel.lower() for marker in markers)]


def extract_intervals(channels: list[str]) -> list[str]:
    found: set[str] = set()
    for channel in channels:
        found.update(match.group(0) for match in INTERVAL_RE.finditer(channel))
    return sorted(found, key=lambda value: (len(value), value))


def extract_orderbook_lines(text: str) -> list[str]:
    return [line.strip() for line in text.splitlines() if ORDERBOOK_LINE_RE.search(line)]


def extract_intervals_from_text(text: str) -> list[str]:
    found: set[str] = set()
    for line in extract_orderbook_lines(text):
        found.update(match.group(0) for match in INTERVAL_RE.finditer(line))
    return sorted(found, key=lambda value: (len(value), value))


def extract_depths(channels: list[str]) -> list[str]:
    found: set[str] = set()
    for channel in channels:
        lower = channel.lower()
        if "bookticker" in lower or "bbo" in lower:
            found.add("1")
        for match in DEPTH_RE.finditer(channel):
            found.add(match.group(1))
    return sorted(found, key=lambda value: int(value))


def extract_depths_from_text(text: str) -> list[str]:
    found: set[str] = set()
    for line in extract_orderbook_lines(text):
        lower = line.lower()
        if "bookticker" in lower or "bbo" in lower or "best bid" in lower:
            found.add("1")
        for match in DEPTH_RE.finditer(line):
            found.add(match.group(1))
    return sorted(found, key=lambda value: int(value))


def interval_to_ms(value: str) -> int | None:
    match = INTERVAL_VALUE_RE.fullmatch(value.strip())
    if not match:
        return None
    amount = int(match.group(1))
    unit = match.group(2).lower()
    if unit == "ms":
        return amount
    if unit == "s":
        return amount * 1_000
    if unit == "m":
        return amount * 60_000
    return None


def fastest_interval_ms(intervals: list[str]) -> str:
    values = [value for value in (interval_to_ms(item) for item in intervals) if value is not None]
    if not values:
        return ""
    return str(min(values))


def has_l1_bbo_evidence(orderbook_channels: list[str], depths: list[str], text: str) -> bool:
    if "1" in depths:
        return True
    evidence = "\n".join(orderbook_channels + extract_orderbook_lines(text)).lower()
    return any(marker in evidence for marker in ("bookticker", "book_ticker", "bbo", "best bid", "best ask"))


def ws_latency_tier(public_support: str, fastest_ms: str, l1_bbo: bool) -> str:
    support = public_support.lower()
    if "unsupported" in support or public_support == "未声明":
        return "公共WS未接入/未声明"
    if not fastest_ms:
        return "缺推流间隔证据"
    value = int(fastest_ms)
    if value <= 10 and l1_bbo:
        return "极速L1候选"
    if value <= 20:
        return "低延迟盘口候选"
    if value <= 100:
        return "百毫秒盘口候选"
    return "慢速盘口/需评估"


def ws_gap(public_support: str, orderbook_channels: list[str], intervals: list[str], depths: list[str]) -> str:
    support = public_support.lower()
    if "unsupported" in support:
        return "公共WS不支持/未接入"
    if public_support == "未声明":
        return "公共WS未声明"
    gaps: list[str] = []
    if not orderbook_channels:
        gaps.append("缺订单簿channel")
    if not intervals:
        gaps.append("缺推流间隔")
    if not depths:
        gaps.append("缺档位")
    return "；".join(gaps) if gaps else "已记录核心细项"


def market_types(data: dict[str, Any], endpoint_items: list[dict[str, Any]]) -> list[str]:
    values: list[str] = []

    def add_many(raw: Any) -> None:
        values.extend(normalize_market_type(item) for item in as_list(raw))

    add_many(data.get("market_types"))
    add_many(data.get("products"))

    capabilities = data.get("capabilities_v2")
    if isinstance(capabilities, dict):
        add_many(capabilities.get("market_types"))
        add_many(capabilities.get("products"))

    for item in endpoint_items:
        add_many(item.get("market_types"))
        add_many(item.get("product"))
        add_many(item.get("products"))

    ignored = {"", "none", "mixed", "testnet"}
    return sorted(set(item for item in values if item not in ignored))


def compact_status_counts(rows: list[dict[str, Any]], op: str) -> str:
    counts = Counter(row[op] for row in rows)
    parts = []
    for key in ("运行", "原生", "组合", "REST兜底", "映射", "离线", "不支持", "-"):
        if counts.get(key):
            parts.append(f"{key}:{counts[key]}")
    return "，".join(parts)


def row_for_mapping(repo_root: Path, path: Path) -> dict[str, Any]:
    data = read_yaml(path)
    exchange = str(data.get("exchange") or path.parent.name)
    adapter_dir = path.parent
    doc_path = repo_root / ADAPTER_DOC_DIR / f"{exchange}_adapter.md"
    endpoint_items = iter_endpoint_items(data)
    mts = market_types(data, endpoint_items)
    runtime_impls = rust_runtime_impls(adapter_dir)
    op_status = endpoint_status_by_op(endpoint_items, runtime_impls)
    public_channels = collect_channels(data, "public")
    private_channels = collect_channels(data, "private")
    orderbook_channels = extract_orderbook_channels(public_channels)
    detail_text = "\n".join(
        [
            read_text(doc_path),
            read_text(adapter_dir / "streams.rs"),
            read_text(adapter_dir / "stream_tests.rs"),
            read_text(path),
        ]
    )
    intervals = sorted(
        set(extract_intervals(orderbook_channels) + extract_intervals_from_text(detail_text)),
        key=lambda value: (len(value), value),
    )
    depths = sorted(
        set(extract_depths(orderbook_channels) + extract_depths_from_text(detail_text)),
        key=lambda value: int(value),
    )
    fastest_ms = fastest_interval_ms(intervals)
    l1_bbo = has_l1_bbo_evidence(orderbook_channels, depths, detail_text)
    public_ws = ws_support(data, "public")
    private_ws = ws_support(data, "private")
    has_contract = any(item in {"perpetual", "futures", "option"} for item in mts)
    row: dict[str, Any] = {
        "exchange": exchange,
        "mapping": rel(path, repo_root),
        "doc": rel(doc_path, repo_root) if doc_path.exists() else "",
        "market_types": ",".join(mts),
        "spot_project": "声明" if "spot" in mts else "未声明",
        "contract_project": "声明" if has_contract else "未声明",
        "official_gap_check": "；".join(
            item
            for item in [
                "需核验现货" if "spot" not in mts else "",
                "需核验/确认交易所不支持合约" if not has_contract else "",
            ]
            if item
        ),
        "public_ws_support": public_ws,
        "private_ws_support": private_ws,
        "public_ws_channels": ",".join(public_channels),
        "private_ws_channels": ",".join(private_channels),
        "orderbook_channels": ",".join(orderbook_channels),
        "ws_push_interval_evidence": ",".join(intervals),
        "ws_depth_evidence": ",".join(depths),
        "ws_fastest_interval_ms": fastest_ms,
        "ws_l1_bbo_evidence": "是" if l1_bbo else "否",
        "ws_latency_tier": ws_latency_tier(public_ws, fastest_ms, l1_bbo),
        "ws_gap": ws_gap(public_ws, orderbook_channels, intervals, depths),
    }
    row.update(op_status)
    return row


def markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        escaped = [cell.replace("|", "\\|").replace("\n", " ") for cell in row]
        lines.append("| " + " | ".join(escaped) + " |")
    return "\n".join(lines)


def generate_markdown(rows: list[dict[str, Any]], status_date: str) -> str:
    generated_at = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
    op_rows = [
        [label, op, compact_status_counts(rows, op)]
        for label, op in STANDARD_OPS
    ]
    public_ws_counts = Counter(row["public_ws_support"] for row in rows)
    private_ws_counts = Counter(row["private_ws_support"] for row in rows)
    ws_gap_counts = Counter(row["ws_gap"] for row in rows)
    latency_tier_counts = Counter(row["ws_latency_tier"] for row in rows)
    summary_rows = [
        ["adapter endpoint mappings", str(len(rows))],
        ["声明 Spot", str(sum(1 for row in rows if row["spot_project"] == "声明"))],
        ["声明合约/期权", str(sum(1 for row in rows if row["contract_project"] == "声明"))],
        ["公共 WS native/spec/其他", ", ".join(f"{k}:{v}" for k, v in sorted(public_ws_counts.items()))],
        ["私有 WS native/spec/其他", ", ".join(f"{k}:{v}" for k, v in sorted(private_ws_counts.items()))],
        ["项目 WS 延迟等级证据", ", ".join(f"{k}:{v}" for k, v in sorted(latency_tier_counts.items()))],
    ]
    matrix_rows = []
    for row in rows:
        doc = row["doc"]
        doc_link = f"[doc]({Path(doc).relative_to(GATEWAY_DOC_DIR).as_posix()})" if doc else ""
        matrix_rows.append(
            [
                row["exchange"],
                row["market_types"] or "未声明",
                row["spot_project"],
                row["contract_project"],
                row["get_symbol_rules"],
                row["get_order_book"],
                row["get_balances"],
                row["get_positions"],
                row["place_order"],
                row["cancel_order"],
                row["cancel_all_orders"],
                row["public_ws_support"],
                row["ws_push_interval_evidence"] or "-",
                row["ws_depth_evidence"] or "-",
                row["ws_fastest_interval_ms"] or "-",
                row["ws_l1_bbo_evidence"],
                row["ws_latency_tier"],
                row["ws_gap"],
                row["official_gap_check"] or "-",
                doc_link,
            ]
        )
    ws_gap_rows = [[gap, str(count)] for gap, count in ws_gap_counts.most_common()]
    return "\n".join(
        [
            "# 交易所功能盘点矩阵",
            "",
            f"状态日期：{status_date}",
            "",
            f"生成时间：{generated_at}",
            "",
            "本文件由 `scripts/generate_exchange_gateway_matrix.py` 从当前仓库的 `endpoint_mapping.yaml` 生成。",
            "它表示“项目当前声明/实现证据”，不是完整官方能力结论。某产品线未声明时，需要继续查官方文档；查实没有时，在单交易所文档里写 `交易所不支持`。",
            "",
            "## 状态口径",
            "",
            "- `运行`：endpoint mapping 存在，并在 adapter Rust 源码中找到对应 `*_impl` 运行实现。",
            "- `原生`：mapping 标记 native，但脚本未确认到运行实现。",
            "- `映射`：mapping 有正常 endpoint，但未显式标 support。",
            "- `离线`：request spec、parser、payload 或 fixture 级别，不能直接视作实盘运行。",
            "- `组合` / `REST兜底`：需要组合接口或 REST reconciliation。",
            "- `不支持`：mapping 明确 unsupported 或使用 `/unsupported/...` 边界。",
            "- `-`：当前 mapping 没有该操作证据。",
            "- `WS延迟等级`：只根据当前项目 mapping/adapter 文档中的数字间隔和 L1/BBO 证据生成，不代表官方完整能力；官方 10ms 能力见 [WebSocket 极速盘口能力汇总](WebSocket极速盘口能力汇总.md)。",
            "",
            "## 汇总",
            "",
            markdown_table(["项目", "数量/状态"], summary_rows),
            "",
            "## 标准操作覆盖统计",
            "",
            markdown_table(["能力", "网关操作", "当前项目状态分布"], op_rows),
            "",
            "## WebSocket 行情缺口统计",
            "",
            markdown_table(["缺口", "交易所数量"], ws_gap_rows),
            "",
            "## 全交易所矩阵",
            "",
            markdown_table(
                [
                    "交易所",
                    "项目产品线",
                    "现货",
                    "合约",
                    "规则",
                    "盘口",
                    "余额",
                    "仓位",
                    "下单",
                    "撤单",
                    "全撤",
                    "公共WS",
                    "推流间隔证据",
                    "档位证据",
                    "最快间隔ms",
                    "L1/BBO证据",
                    "WS延迟等级",
                    "WS缺口",
                    "官方核验",
                    "文档",
                ],
                matrix_rows,
            ),
            "",
            "## CSV",
            "",
            "完整机器可读版本见 [交易所功能盘点矩阵.csv](交易所功能盘点矩阵.csv)。",
            "",
        ]
    )


def write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    fieldnames = [
        "exchange",
        "market_types",
        "spot_project",
        "contract_project",
        "official_gap_check",
        "public_ws_support",
        "private_ws_support",
        "public_ws_channels",
        "private_ws_channels",
        "orderbook_channels",
        "ws_push_interval_evidence",
        "ws_depth_evidence",
        "ws_fastest_interval_ms",
        "ws_l1_bbo_evidence",
        "ws_latency_tier",
        "ws_gap",
        "mapping",
        "doc",
    ] + [op for _, op in STANDARD_OPS]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def main() -> None:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    mapping_paths = sorted((repo_root / ADAPTERS_DIR).glob("*/endpoint_mapping.yaml"))
    rows = [row_for_mapping(repo_root, path) for path in mapping_paths]
    rows.sort(key=lambda row: row["exchange"])
    markdown = generate_markdown(rows, args.status_date)
    markdown_path = repo_root / args.markdown_out
    csv_path = repo_root / args.csv_out
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(markdown, encoding="utf-8")
    write_csv(rows, csv_path)
    print(f"wrote {markdown_path.relative_to(repo_root)}")
    print(f"wrote {csv_path.relative_to(repo_root)}")
    print(f"adapters: {len(rows)}")


if __name__ == "__main__":
    main()
