#!/usr/bin/env python3
"""Generate per-adapter exchange gateway work packages."""

from __future__ import annotations

import csv
from collections import Counter, defaultdict
from pathlib import Path


GATEWAY_DOC_DIR = Path("docs/交易所网关")
MATRIX_CSV = GATEWAY_DOC_DIR / "交易所功能盘点矩阵.csv"
TASK_CSV = GATEWAY_DOC_DIR / "交易所网关补全任务清单.csv"
QUEUE_CSV = GATEWAY_DOC_DIR / "剩余官方核验队列.csv"
WORKPACK_CSV = GATEWAY_DOC_DIR / "adapter工作包索引.csv"
WORKPACK_MD = GATEWAY_DOC_DIR / "adapter工作包索引.md"
STATUS_DATE = "2026-06-08"

PRIORITY_SCORE = {"P0": 100, "P1": 40, "P2": 25, "P3": 10, "P4": 3}
CORE_OPS = [
    "get_symbol_rules",
    "get_order_book",
    "get_balances",
    "get_positions",
    "place_order",
    "cancel_order",
    "query_order",
    "get_open_orders",
    "get_recent_fills",
]
RUNLIKE = {"运行", "原生", "映射", "组合", "REST兜底"}


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def split_adapters(value: str) -> list[str]:
    return [part.strip() for part in value.split("/") if part.strip()]


def support_ratio(row: dict[str, str]) -> str:
    supported = 0
    total = 0
    for op in CORE_OPS:
        status = row.get(op, "-")
        if op == "get_positions" and row["contract_project"] != "声明":
            continue
        total += 1
        if status in RUNLIKE:
            supported += 1
    if total == 0:
        return "0/0"
    return f"{supported}/{total}"


def add_counts_by_adapter(
    rows: list[dict[str, str]],
    adapter_key: str,
    count_key: str,
) -> dict[str, Counter[str]]:
    output: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        for adapter in split_adapters(row[adapter_key]):
            output[adapter][row[count_key]] += 1
    return output


def joined_counter(counter: Counter[str]) -> str:
    if not counter:
        return "-"
    return ",".join(f"{key}:{counter[key]}" for key in sorted(counter))


def workload_score(task_priorities: Counter[str], queue_priorities: Counter[str]) -> int:
    total = 0
    for priority, count in task_priorities.items():
        total += PRIORITY_SCORE.get(priority, 0) * count
    for priority, count in queue_priorities.items():
        total += PRIORITY_SCORE.get(priority, 0) * count
    return total


def next_action(
    adapter: str,
    task_priorities: Counter[str],
    task_categories: Counter[str],
    queue_priorities: Counter[str],
    queue_categories: Counter[str],
) -> str:
    if task_priorities.get("P0"):
        if task_categories.get("public_ws"):
            return "先做已确认 P0 公共订单簿 WS 补全"
        if task_categories.get("product_line"):
            return "先做已确认 P0 产品线项目未实现任务"
        return "先处理已确认 P0 补全任务"
    if queue_priorities.get("P2"):
        if queue_categories.get("product_line_official_check"):
            return "先查官方产品线，区分交易所不支持和项目未实现"
        if queue_categories.get("core_trading_official_check"):
            return "先查官方下单/撤单接口"
        if queue_categories.get("position_official_check"):
            return "先查官方合约仓位接口"
        return "先处理 P2 官方核验项"
    if task_priorities.get("P1"):
        return "补已确认 P1 WebSocket 结构化细项"
    if queue_priorities.get("P3"):
        return "补 P3 官方核验：账户、费率或公共 WS 细项"
    if queue_priorities.get("P4"):
        return "补 P4 增强交易能力官方核验"
    return "暂无已知阻塞项，等待全量复核"


def build_workpacks(
    matrix_rows: list[dict[str, str]],
    task_rows: list[dict[str, str]],
    queue_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    tasks_by_priority = add_counts_by_adapter(task_rows, "adapter", "priority")
    tasks_by_category = add_counts_by_adapter(task_rows, "adapter", "category")
    queue_by_priority = add_counts_by_adapter(queue_rows, "adapter", "priority")
    queue_by_category = add_counts_by_adapter(queue_rows, "adapter", "category")

    workpacks: list[dict[str, str]] = []
    for row in matrix_rows:
        adapter = row["exchange"]
        task_priorities = tasks_by_priority[adapter]
        task_categories = tasks_by_category[adapter]
        queue_priorities = queue_by_priority[adapter]
        queue_categories = queue_by_category[adapter]
        score = workload_score(task_priorities, queue_priorities)
        workpacks.append(
            {
                "adapter": adapter,
                "market_types": row["market_types"] or "未声明",
                "spot_project": row["spot_project"],
                "contract_project": row["contract_project"],
                "core_ops": support_ratio(row),
                "public_ws_support": row["public_ws_support"],
                "ws_gap": row["ws_gap"],
                "ws_fastest_interval_ms": row.get("ws_fastest_interval_ms", ""),
                "ws_l1_bbo_evidence": row.get("ws_l1_bbo_evidence", ""),
                "ws_latency_tier": row.get("ws_latency_tier", ""),
                "confirmed_tasks": str(sum(task_priorities.values())),
                "confirmed_task_priorities": joined_counter(task_priorities),
                "confirmed_task_categories": joined_counter(task_categories),
                "remaining_checks": str(sum(queue_priorities.values())),
                "remaining_check_priorities": joined_counter(queue_priorities),
                "remaining_check_categories": joined_counter(queue_categories),
                "workload_score": str(score),
                "next_action": next_action(
                    adapter,
                    task_priorities,
                    task_categories,
                    queue_priorities,
                    queue_categories,
                ),
                "doc": row["doc"],
                "mapping": row["mapping"],
            }
        )

    workpacks.sort(key=lambda item: (-int(item["workload_score"]), item["adapter"]))
    return workpacks


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    fieldnames = [
        "adapter",
        "market_types",
        "spot_project",
        "contract_project",
        "core_ops",
        "public_ws_support",
        "ws_gap",
        "ws_fastest_interval_ms",
        "ws_l1_bbo_evidence",
        "ws_latency_tier",
        "confirmed_tasks",
        "confirmed_task_priorities",
        "confirmed_task_categories",
        "remaining_checks",
        "remaining_check_priorities",
        "remaining_check_categories",
        "workload_score",
        "next_action",
        "doc",
        "mapping",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_markdown(path: Path, rows: list[dict[str, str]]) -> None:
    confirmed = sum(1 for row in rows if int(row["confirmed_tasks"]) > 0)
    remaining = sum(1 for row in rows if int(row["remaining_checks"]) > 0)
    high_score = [row for row in rows if int(row["workload_score"]) >= 100]

    lines: list[str] = [
        "# Adapter 工作包索引",
        "",
        f"状态日期：{STATUS_DATE}",
        "",
        "本文件由 `scripts/generate_exchange_gateway_adapter_workpacks.py` 根据 [交易所功能盘点矩阵](交易所功能盘点矩阵.md)、[交易所网关补全任务清单](交易所网关补全任务清单.md) 和 [剩余官方核验队列](剩余官方核验队列.md) 生成。",
        "",
        "机器可读版本见 [adapter工作包索引.csv](adapter工作包索引.csv)。",
        "",
        "## 汇总",
        "",
        "| 项目 | 数量 |",
        "| --- | ---: |",
        f"| adapter 总数 | {len(rows)} |",
        f"| 已有明确补全任务的 adapter | {confirmed} |",
        f"| 仍有剩余核验项的 adapter | {remaining} |",
        f"| workload_score >= 100 的 adapter | {len(high_score)} |",
        "",
        "## 排序口径",
        "",
        "`workload_score` 用来粗排工作量和优先级：P0 任务权重 100，P1 40，P2 25，P3 10，P4 3。它不是工期估算，只用于挑下一批 adapter。",
        "",
        "## 最高优先级工作包",
        "",
        "| adapter | 产品线 | 核心接口 | 公共 WS | WS延迟等级 | 明确任务 | 剩余核验 | score | 下一步 |",
        "| --- | --- | --- | --- | --- | ---: | ---: | ---: | --- |",
    ]
    for row in rows[:40]:
        lines.append(
            "| {adapter} | {market_types} | {core_ops} | {public_ws_support}/{ws_gap} | {ws_latency_tier} | {confirmed_tasks} | {remaining_checks} | {workload_score} | {next_action} |".format(
                **row
            )
        )

    lines.extend(
        [
            "",
            "## 使用方式",
            "",
            "1. 从本表顶部选择一个 adapter。",
            "2. 打开对应 `doc` 和 `mapping`，确认当前项目证据。",
            "3. 如果 `confirmed_tasks > 0`，直接按 [交易所网关补全任务清单](交易所网关补全任务清单.md) 实现。",
            "4. 如果只有 `remaining_checks`，先查官方资料，把结论转入任务清单或写 `交易所不支持`。",
            "5. 修改 `endpoint_mapping.yaml` 后重新运行矩阵、剩余队列和本脚本。",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    matrix_rows = read_csv(MATRIX_CSV)
    task_rows = read_csv(TASK_CSV)
    queue_rows = read_csv(QUEUE_CSV)
    workpacks = build_workpacks(matrix_rows, task_rows, queue_rows)
    write_csv(WORKPACK_CSV, workpacks)
    write_markdown(WORKPACK_MD, workpacks)
    print(f"wrote {WORKPACK_MD}")
    print(f"wrote {WORKPACK_CSV}")
    print(f"workpacks: {len(workpacks)}")


if __name__ == "__main__":
    main()
