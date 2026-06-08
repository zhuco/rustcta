#!/usr/bin/env python3
"""Generate the remaining official verification queue for exchange gateways."""

from __future__ import annotations

import csv
from collections import Counter, defaultdict
from pathlib import Path


GATEWAY_DOC_DIR = Path("docs/交易所网关")
MATRIX_CSV = GATEWAY_DOC_DIR / "交易所功能盘点矩阵.csv"
TASK_CSV = GATEWAY_DOC_DIR / "交易所网关补全任务清单.csv"
QUEUE_CSV = GATEWAY_DOC_DIR / "剩余官方核验队列.csv"
QUEUE_MD = GATEWAY_DOC_DIR / "剩余官方核验队列.md"
STATUS_DATE = "2026-06-08"

MISSING_STATUS = {"-", "不支持"}
CORE_WS_DONE = "已记录核心细项"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def load_task_adapters(rows: list[dict[str, str]]) -> dict[str, set[str]]:
    by_category: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        adapters = [part.strip() for part in row["adapter"].split("/") if part.strip()]
        for adapter in adapters:
            by_category[row["category"]].add(adapter)
    return by_category


def add_item(
    items: list[dict[str, str]],
    priority: str,
    category: str,
    adapter: str,
    current_evidence: str,
    remaining_check: str,
    suggested_outcome: str,
    source_field: str,
) -> None:
    items.append(
        {
            "priority": priority,
            "category": category,
            "adapter": adapter,
            "current_evidence": current_evidence,
            "remaining_check": remaining_check,
            "suggested_outcome": suggested_outcome,
            "source_field": source_field,
        }
    )


def build_queue(matrix_rows: list[dict[str, str]], task_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    task_adapters = load_task_adapters(task_rows)
    product_line_done = task_adapters["product_line"] | task_adapters["explicit_unsupported"]
    public_ws_done = (
        task_adapters["public_ws"]
        | task_adapters["public_ws_struct"]
        | task_adapters["public_ws_unsupported"]
    )

    items: list[dict[str, str]] = []
    for row in matrix_rows:
        adapter = row["exchange"]

        official_gap = row["official_gap_check"].strip()
        if official_gap and adapter not in product_line_done:
            add_item(
                items,
                "P2",
                "product_line_official_check",
                adapter,
                f"项目产品线={row['market_types'] or '未声明'}；{official_gap}",
                "查官方 Spot/合约/期权产品线；区分交易所不支持和项目未实现。",
                "官方没有则写交易所不支持；官方支持则新建补实现任务。",
                "official_gap_check",
            )

        ws_gap = row["ws_gap"].strip()
        if ws_gap and ws_gap != CORE_WS_DONE and adapter not in public_ws_done:
            priority = "P2" if ws_gap in {"公共WS未声明", "公共WS不支持/未接入"} else "P3"
            add_item(
                items,
                priority,
                "public_ws_official_detail_check",
                adapter,
                f"public_ws={row['public_ws_support']}; ws_gap={ws_gap}; channels={row['orderbook_channels'] or '-'}",
                "查官方公共订单簿 WS：订阅方式、推流间隔、档位，尤其 10ms/20ms L1/BBO；sequence/checksum、snapshot 重建。",
                "官方支持则补 endpoint mapping/fixture；官方不支持则写交易所不支持公共 WS 行情。",
                "ws_gap",
            )

        if row["place_order"] in MISSING_STATUS or row["cancel_order"] in MISSING_STATUS:
            add_item(
                items,
                "P2",
                "core_trading_official_check",
                adapter,
                f"place_order={row['place_order']}; cancel_order={row['cancel_order']}",
                "查官方下单、撤单、订单状态、client order id、TIF、market/limit 支持。",
                "官方支持则补 request spec/signing/parser；官方不支持则写交易所不支持对应写接口。",
                "place_order,cancel_order",
            )

        if row["get_balances"] in MISSING_STATUS:
            add_item(
                items,
                "P3",
                "account_state_official_check",
                adapter,
                f"balances={row['get_balances']}",
                "查官方余额/账户接口和只读 API key 权限。",
                "官方支持则补余额 parser 和 read-only fixture；官方不支持则写交易所不支持余额接口。",
                "get_balances",
            )

        if row["contract_project"] == "声明" and row["get_positions"] in MISSING_STATUS:
            add_item(
                items,
                "P2",
                "position_official_check",
                adapter,
                f"contract=声明; positions={row['get_positions']}",
                "查官方仓位接口、保证金模式、持仓模式、position side。",
                "官方支持则补 positions parser；官方不支持则写交易所不支持合约仓位接口。",
                "get_positions",
            )

        if row["get_fees"] in MISSING_STATUS:
            add_item(
                items,
                "P3",
                "fee_official_check",
                adapter,
                f"fees={row['get_fees']}",
                "查官方费率接口、maker/taker、VIP/market maker 口径。",
                "官方支持则补 fee source；官方不支持则写默认费率/配置兜底策略。",
                "get_fees",
            )

        advanced_missing = [
            op
            for op in ("amend_order", "place_order_list", "batch_place_orders", "batch_cancel_orders")
            if row[op] in MISSING_STATUS
        ]
        if advanced_missing:
            add_item(
                items,
                "P4",
                "advanced_trading_official_check",
                adapter,
                ";".join(f"{op}={row[op]}" for op in advanced_missing),
                "查官方批量下单/批量撤单/改单/OCO/OTO 支持。",
                "官方支持则补增强交易能力；官方不支持则写交易所不支持。",
                ",".join(advanced_missing),
            )

    priority_rank = {"P0": 0, "P1": 1, "P2": 2, "P3": 3, "P4": 4}
    items.sort(key=lambda item: (priority_rank[item["priority"]], item["category"], item["adapter"]))
    return items


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    fieldnames = [
        "priority",
        "category",
        "adapter",
        "current_evidence",
        "remaining_check",
        "suggested_outcome",
        "source_field",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def top_adapters(rows: list[dict[str, str]], category: str, limit: int = 60) -> list[str]:
    return [row["adapter"] for row in rows if row["category"] == category][:limit]


def write_markdown(path: Path, rows: list[dict[str, str]]) -> None:
    by_priority = Counter(row["priority"] for row in rows)
    by_category = Counter(row["category"] for row in rows)

    lines: list[str] = [
        "# 剩余官方核验队列",
        "",
        f"状态日期：{STATUS_DATE}",
        "",
        "本文件由 `scripts/generate_exchange_gateway_audit_queue.py` 根据当前 [交易所功能盘点矩阵](交易所功能盘点矩阵.md) 和 [交易所网关补全任务清单](交易所网关补全任务清单.md) 生成。它列出尚未转成明确补全任务、仍需要继续查官方资料的项目。",
        "",
        "机器可读版本见 [剩余官方核验队列.csv](剩余官方核验队列.csv)。",
        "",
        "如果要按交易所开工，而不是按核验项开工，先看 [adapter 工作包索引](adapter工作包索引.md)。",
        "",
        "## 汇总",
        "",
        "| 项目 | 数量 |",
        "| --- | ---: |",
        f"| 剩余核验项 | {len(rows)} |",
    ]
    for priority in sorted(by_priority):
        lines.append(f"| {priority} | {by_priority[priority]} |")
    for category, count in sorted(by_category.items()):
        lines.append(f"| {category} | {count} |")

    lines.extend(
        [
            "",
            "## 优先解释",
            "",
            "- `P2`：影响产品线判断、核心交易或合约仓位，查完后应转成明确实现任务或写 `交易所不支持`。",
            "- `P3`：账户、费率、公共 WS 结构化细节等，影响实盘安全和定价精度。",
            "- `P4`：批量、改单、OCO/OTO 等增强交易能力。",
            "",
            "## 产品线仍需官方核验",
            "",
            "这些 adapter 在当前项目里未声明现货或合约，且尚未进入已核验补全任务。不能直接写 `交易所不支持`。",
            "",
            "`" + "`, `".join(top_adapters(rows, "product_line_official_check")) + "`",
            "",
            "## 公共 WebSocket 仍需官方细项",
            "",
            "这些 adapter 还需要查订单簿 channel、推流间隔、档位、sequence/checksum、snapshot 重建。",
            "",
            "`" + "`, `".join(top_adapters(rows, "public_ws_official_detail_check")) + "`",
            "",
            "## 核心交易接口仍需官方核验",
            "",
            "这些 adapter 的下单或撤单在当前矩阵中缺失/不支持，需要查官方交易接口后决定补实现还是写 `交易所不支持`。",
            "",
            "`" + "`, `".join(top_adapters(rows, "core_trading_official_check")) + "`",
            "",
            "## 下一步处理方式",
            "",
            "1. 每次选择一个 adapter，先查官方产品线，再查公共 WS，再查写接口和私有账户接口。",
            "2. 如果官方支持，把该行转入 [交易所网关补全任务清单](交易所网关补全任务清单.md)。",
            "3. 如果官方不支持，在单交易所文档写 `交易所不支持现货`、`交易所不支持合约` 或 `交易所不支持某接口`。",
            "4. 更新 `endpoint_mapping.yaml` 后重新运行 `python3 scripts/generate_exchange_gateway_matrix.py` 和本脚本。",
        ]
    )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    matrix_rows = read_csv(MATRIX_CSV)
    task_rows = read_csv(TASK_CSV)
    queue_rows = build_queue(matrix_rows, task_rows)
    write_csv(QUEUE_CSV, queue_rows)
    write_markdown(QUEUE_MD, queue_rows)
    print(f"wrote {QUEUE_MD}")
    print(f"wrote {QUEUE_CSV}")
    print(f"queue rows: {len(queue_rows)}")


if __name__ == "__main__":
    main()
