#!/usr/bin/env python3
"""Audit rustcta-exchange-gateway adapter toolchain coverage.

The audit is evidence-based: it reports files, fixtures, specs, and static
markers that exist in the checkout. Missing endpoint mapping or capability data
stays marked as missing instead of being inferred from adapter code.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Iterable


SCHEMA_VERSION = "exchange_adapter_toolchain_audit.v1"
DEFAULT_STATUS_DOC = Path("docs/exchange_adapter_toolchain_status_zh.md")
ADAPTERS_DIR = Path("crates/rustcta-exchange-gateway/src/adapters")
FIXTURES_DIR = Path("tests/fixtures/exchanges")
BASELINE_EXCHANGES = ("binance", "okx")
SKIP_ADAPTERS = {"paper"}
FIXTURE_ALIASES = {"gateio": ("gateio", "gate")}

RUST_FN_RE = re.compile(
    r"\b(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)\s*\("
)
REST_PATH_RE = re.compile(r'"(/(?:api|sapi|fapi|eapi|wapi|ws|open-api|v[0-9])[^"]*)"')

CHECKS = (
    ("endpoint_mapping", "endpoint_mapping.yaml", True),
    ("legacy_capabilities", "legacy ExchangeClient capabilities", True),
    ("capabilities_v2", "capabilities_v2 declaration", False),
    ("request_specs", "request-spec fixture files", True),
    ("signing_vectors", "signing vector fixture files", True),
    ("parser_fixtures", "parser fixtures", True),
    ("public_tests", "public parser/API tests", True),
    ("private_tests", "private parser/API tests", True),
    ("signing_module", "signing module", True),
    ("public_ws_policy", "public WS policy or explicit unsupported", True),
    ("private_ws_policy", "private WS policy or explicit unsupported", True),
    ("batch_capability", "batch capability declaration", True),
    ("error_or_rate_limit_mapping", "error or rate-limit mapping", True),
    ("reconciliation_plan", "reconciliation plan", False),
    ("live_safety_controls", "live safety controls", False),
)


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scan gateway adapters and emit machine-readable coverage JSON.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  python3 scripts/audit_gateway_adapters.py
  python3 scripts/audit_gateway_adapters.py --exchanges binance,okx --output logs/exchange_adapter_toolchain_audit_20260608.json
  python3 scripts/audit_gateway_adapters.py --write-status-doc
  python3 scripts/audit_gateway_adapters.py --check --strict --exchanges binance,okx
""",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Repository root. Defaults to auto-detection from cwd/script path.",
    )
    parser.add_argument(
        "--exchange",
        action="append",
        default=[],
        help="Adapter name to audit. May be repeated or comma-separated.",
    )
    parser.add_argument(
        "--exchanges",
        default=None,
        help="Comma-separated adapter names to audit. Defaults to all adapters.",
    )
    parser.add_argument(
        "--json-out",
        "--output",
        dest="json_out",
        type=Path,
        help="Write the audit JSON to this path instead of stdout.",
    )
    parser.add_argument(
        "--write-status-doc",
        type=Path,
        nargs="?",
        const=DEFAULT_STATUS_DOC,
        help="Generate/update the status Markdown document. Optional path defaults to docs/exchange_adapter_toolchain_status_zh.md.",
    )
    parser.add_argument(
        "--check-status-doc",
        action="store_true",
        help="Exit non-zero if the status Markdown document is missing or stale.",
    )
    parser.add_argument(
        "--status-doc",
        type=Path,
        default=DEFAULT_STATUS_DOC,
        help="Status Markdown path for --check-status-doc. Default: docs/exchange_adapter_toolchain_status_zh.md.",
    )
    parser.add_argument(
        "--required-exchanges",
        default=",".join(BASELINE_EXCHANGES),
        help="Comma-separated adapters that must exist. Default: binance,okx.",
    )
    parser.add_argument(
        "--check",
        "--fail-on-incomplete",
        dest="check",
        action="store_true",
        help="Exit non-zero when required exchanges are missing.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="With --check, also fail when required exchanges have required coverage findings.",
    )
    return parser.parse_args()


def detect_repo_root(start: Path) -> Path:
    current = start.resolve()
    if current.is_file():
        current = current.parent
    for candidate in (current, *current.parents):
        if (candidate / "Cargo.toml").exists() and (candidate / ADAPTERS_DIR).exists():
            return candidate
    script_root = Path(__file__).resolve().parents[1]
    if (script_root / ADAPTERS_DIR).exists():
        return script_root
    raise SystemExit("could not detect repository root; pass --repo-root")


def normalize_exchanges(values: Iterable[str]) -> list[str]:
    exchanges: list[str] = []
    for value in values:
        for item in value.split(","):
            item = item.strip().lower().replace("-", "_")
            if item:
                exchanges.append(item)
    return sorted(set(exchanges))


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return path.read_text(encoding="utf-8", errors="ignore")
    except FileNotFoundError:
        return ""


def rel(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def iter_files(path: Path, suffixes: tuple[str, ...] | None = None) -> list[Path]:
    if not path.exists():
        return []
    files = [item for item in path.rglob("*") if item.is_file()]
    if suffixes is not None:
        files = [item for item in files if item.suffix.lower() in suffixes]
    return sorted(files)


def marker_in_files(files: Iterable[Path], markers: Iterable[str]) -> bool:
    marker_list = tuple(marker.lower() for marker in markers)
    for path in files:
        text = read_text(path).lower()
        if any(marker in text for marker in marker_list):
            return True
    return False


def discover_adapters(repo_root: Path) -> list[str]:
    adapters_dir = repo_root / ADAPTERS_DIR
    if not adapters_dir.exists():
        return []
    return sorted(
        item.name
        for item in adapters_dir.iterdir()
        if item.is_dir() and item.name not in SKIP_ADAPTERS and (item / "mod.rs").exists()
    )


def normalize_operation(name: str) -> str:
    for suffix in ("_private_rest", "_public_rest", "_impl", "_task", "_params", "_body"):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


def rust_functions(path: Path) -> list[str]:
    return sorted({normalize_operation(name) for name in RUST_FN_RE.findall(read_text(path))})


def rest_paths(path: Path) -> list[str]:
    return sorted(set(REST_PATH_RE.findall(read_text(path))))


def fixture_dirs(repo_root: Path, exchange: str) -> list[Path]:
    names = FIXTURE_ALIASES.get(exchange, (exchange,))
    return [repo_root / FIXTURES_DIR / name for name in names if (repo_root / FIXTURES_DIR / name).exists()]


def audit_adapter(repo_root: Path, exchange: str, required_exchanges: set[str]) -> dict[str, Any]:
    adapter_dir = repo_root / ADAPTERS_DIR / exchange
    fixture_roots = fixture_dirs(repo_root, exchange)
    doc_path = repo_root / "docs" / f"{exchange}_adapter.md"
    rust_files = iter_files(adapter_dir, (".rs",))
    tests = [path for path in rust_files if "test" in path.name]
    fixture_files = sorted(
        file
        for fixture_root in fixture_roots
        for file in iter_files(fixture_root, (".json", ".jsonl", ".ndjson", ".yaml", ".yml"))
    )
    request_specs = sorted(
        file
        for fixture_root in fixture_roots
        for file in iter_files(fixture_root / "request_specs", (".json",))
    )
    signing_vectors = sorted(
        file
        for fixture_root in fixture_roots
        for file in iter_files(fixture_root / "signing_vectors", (".json",))
    )
    config_examples = sorted((repo_root / "config").glob(f"{exchange}*_gateway*.y*ml"))
    endpoint_files = [
        path
        for name in ("endpoint_mapping.yaml", "endpoint_mapping.yml", "endpoint_mapping.json")
        if (path := adapter_dir / name).exists()
    ]

    mod_text = read_text(adapter_dir / "mod.rs").lower()
    all_rust_text = "\n".join(read_text(path).lower() for path in rust_files)
    doc_text = read_text(doc_path).lower()
    has_stream_source = (adapter_dir / "streams.rs").exists() or any(
        "stream" in path.name for path in tests
    )
    public_ops = rust_functions(adapter_dir / "public.rs")
    private_ops = rust_functions(adapter_dir / "private.rs")
    stream_ops = rust_functions(adapter_dir / "streams.rs")

    checks: dict[str, dict[str, Any]] = {
        "endpoint_mapping": {
            "ok": bool(endpoint_files),
            "files": [rel(path, repo_root) for path in endpoint_files],
        },
        "legacy_capabilities": {
            "ok": "fn capabilities" in mod_text and "exchangeclientcapabilities" in mod_text,
            "files": [rel(adapter_dir / "mod.rs", repo_root)] if (adapter_dir / "mod.rs").exists() else [],
        },
        "capabilities_v2": {
            "ok": "capabilities_v2" in all_rust_text or "streamruntimecapability" in all_rust_text,
            "files": [rel(path, repo_root) for path in rust_files if "capabilities_v2" in read_text(path).lower()],
        },
        "request_specs": {
            "ok": bool(request_specs)
            or marker_in_files(tests, ("request-spec", "request spec", "request_spec")),
            "files": [rel(path, repo_root) for path in request_specs],
            "count": len(request_specs),
        },
        "signing_vectors": {
            "ok": bool(signing_vectors)
            or marker_in_files(tests + [adapter_dir / "signing.rs"], ("signing vector", "signature", "hmac")),
            "files": [rel(path, repo_root) for path in signing_vectors],
            "count": len(signing_vectors),
        },
        "parser_fixtures": {
            "ok": bool(fixture_files) and marker_in_files(tests, ("fixture", "parse")),
            "files": [rel(path, repo_root) for path in fixture_files[:50]],
            "count": len(fixture_files),
        },
        "public_tests": {
            "ok": (adapter_dir / "public_tests.rs").exists(),
            "files": [rel(adapter_dir / "public_tests.rs", repo_root)] if (adapter_dir / "public_tests.rs").exists() else [],
        },
        "private_tests": {
            "ok": (adapter_dir / "private_tests.rs").exists(),
            "files": [rel(adapter_dir / "private_tests.rs", repo_root)] if (adapter_dir / "private_tests.rs").exists() else [],
        },
        "signing_module": {
            "ok": (adapter_dir / "signing.rs").exists(),
            "files": [rel(adapter_dir / "signing.rs", repo_root)] if (adapter_dir / "signing.rs").exists() else [],
        },
        "public_ws_policy": {
            "ok": has_stream_source or "subscribe_public_stream" in mod_text or "unsupported" in mod_text,
            "files": [rel(path, repo_root) for path in rust_files if "stream" in path.name],
        },
        "private_ws_policy": {
            "ok": has_stream_source or "subscribe_private_stream" in mod_text or "unsupported" in mod_text,
            "files": [rel(path, repo_root) for path in rust_files if "stream" in path.name],
        },
        "batch_capability": {
            "ok": any(
                marker in all_rust_text
                for marker in (
                    "supports_batch_place_order",
                    "supports_batch_cancel_order",
                    "batch_place_orders",
                    "batch_cancel_orders",
                    "unsupported",
                )
            ),
            "files": [rel(path, repo_root) for path in rust_files if "batch" in read_text(path).lower()],
        },
        "error_or_rate_limit_mapping": {
            "ok": any(marker in all_rust_text for marker in ("classify", "rate_limit", "ratelimit", "error")),
            "files": [
                rel(path, repo_root)
                for path in rust_files
                if any(marker in read_text(path).lower() for marker in ("classify", "rate_limit", "ratelimit", "error"))
            ],
        },
        "reconciliation_plan": {
            "ok": any(marker in all_rust_text or marker in doc_text for marker in ("reconcile", "reconciliation", "unknownorderpolicy")),
            "files": [rel(doc_path, repo_root)] if doc_path.exists() and "reconcil" in doc_text else [],
        },
        "live_safety_controls": {
            "ok": any(
                marker in all_rust_text or marker in doc_text
                for marker in ("kill-switch", "kill_switch", "disabled-symbol", "disabled_symbol", "max-notional", "max_notional")
            ),
            "files": [rel(doc_path, repo_root)] if doc_path.exists() else [],
        },
    }

    required_keys = {key for key, _label, required in CHECKS if required}
    if exchange not in required_exchanges:
        required_keys = set()
    missing_required = sorted(key for key in required_keys if not checks[key]["ok"])

    file_presence = {
        name: (adapter_dir / name).exists()
        for name in (
            "mod.rs",
            "public.rs",
            "private.rs",
            "parser.rs",
            "private_parser.rs",
            "transport.rs",
            "signing.rs",
            "streams.rs",
            "config.rs",
            "test_support.rs",
            "public_tests.rs",
            "private_tests.rs",
            "stream_tests.rs",
            "live_tests.rs",
        )
    }

    return {
        "exchange": exchange,
        "adapter_dir": rel(adapter_dir, repo_root),
        "exists": adapter_dir.exists(),
        "required_baseline": exchange in required_exchanges,
        "complete_for_task8_baseline": adapter_dir.exists() and not missing_required,
        "missing_required": missing_required,
        "status": "ok" if adapter_dir.exists() and not missing_required else "needs_data",
        "files": file_presence,
        "coverage": {
            "fixture_file_count": len(fixture_files),
            "request_spec_count": len(request_specs),
            "signing_vector_count": len(signing_vectors),
            "endpoint_mapping_count": len(endpoint_files),
            "config_example_count": len(config_examples),
            "adapter_doc_count": int(doc_path.exists()),
        },
        "operations": {
            "public": public_ops,
            "private": private_ops,
            "streams": stream_ops,
            "all": sorted(set(public_ops + private_ops + stream_ops)),
        },
        "discovered_rest_paths": sorted(
            set(
                rest_paths(adapter_dir / "public.rs")
                + rest_paths(adapter_dir / "private.rs")
                + rest_paths(adapter_dir / "transport.rs")
            )
        ),
        "endpoint_mappings": [rel(path, repo_root) for path in endpoint_files],
        "fixtures": {
            "dirs": [rel(path, repo_root) for path in fixture_roots],
            "files": [rel(path, repo_root) for path in fixture_files],
        },
        "config_examples": [rel(path, repo_root) for path in config_examples],
        "docs": [rel(doc_path, repo_root)] if doc_path.exists() else [],
        "checks": checks,
    }


def build_report(repo_root: Path, exchanges: list[str], required: list[str]) -> dict[str, Any]:
    if not exchanges:
        exchanges = discover_adapters(repo_root)
    required_set = set(required)
    adapters = [audit_adapter(repo_root, exchange, required_set) for exchange in exchanges]
    missing_required = sorted(required_set - {adapter["exchange"] for adapter in adapters})
    required_findings = {
        adapter["exchange"]: adapter["missing_required"]
        for adapter in adapters
        if adapter["exchange"] in required_set and adapter["missing_required"]
    }
    by_status = Counter(adapter["status"] for adapter in adapters)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": utc_now(),
        "repo_root": repo_root.as_posix(),
        "adapter_root": ADAPTERS_DIR.as_posix(),
        "fixture_root": FIXTURES_DIR.as_posix(),
        "required_exchanges": required,
        "summary": {
            "adapters_audited": len(adapters),
            "required_audited": sum(1 for adapter in adapters if adapter["exchange"] in required_set),
            "required_complete": sum(1 for adapter in adapters if adapter["exchange"] in required_set and adapter["complete_for_task8_baseline"]),
            "required_incomplete": sorted(required_findings),
            "missing_required_exchanges": missing_required,
            "required_exchange_findings": required_findings,
            "by_status": dict(sorted(by_status.items())),
            "endpoint_mapping_total": sum(adapter["coverage"]["endpoint_mapping_count"] for adapter in adapters),
            "fixture_file_total": sum(adapter["coverage"]["fixture_file_count"] for adapter in adapters),
            "request_spec_total": sum(adapter["coverage"]["request_spec_count"] for adapter in adapters),
            "signing_vector_total": sum(adapter["coverage"]["signing_vector_count"] for adapter in adapters),
        },
        "checks": [
            {"key": key, "label": label, "required_for_baseline": required}
            for key, label, required in CHECKS
        ],
        "adapters": adapters,
    }


def status_mark(ok: bool) -> str:
    return "OK" if ok else "MISSING"


def matrix_status_mark(ok: bool, *, required_baseline: bool, required_for_baseline: bool) -> str:
    if ok:
        return "OK"
    if required_baseline and required_for_baseline:
        return "MISSING"
    return "n/a"


def render_status_doc(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "# Exchange Adapter Toolchain Status",
        "",
        "<!-- Generated by scripts/audit_gateway_adapters.py. Do not edit this block by hand. -->",
        "",
        f"- Generated at: `{report['generated_at']}`",
        f"- Adapters audited: `{summary['adapters_audited']}`",
        f"- Required exchanges: `{', '.join(report['required_exchanges'])}`",
        f"- Required incomplete: `{', '.join(summary['required_incomplete']) or 'none'}`",
        f"- Endpoint mapping files: `{summary['endpoint_mapping_total']}`",
        f"- Fixture files: `{summary['fixture_file_total']}`",
        f"- Request specs: `{summary['request_spec_total']}`",
        f"- Signing vectors: `{summary['signing_vector_total']}`",
        "",
        "## Required Baseline",
        "",
        "| Exchange | Complete | Missing Required |",
        "| --- | --- | --- |",
    ]
    for adapter in report["adapters"]:
        if not adapter["required_baseline"]:
            continue
        missing = ", ".join(adapter["missing_required"]) or "none"
        lines.append(
            f"| `{adapter['exchange']}` | {status_mark(adapter['complete_for_task8_baseline'])} | {missing} |"
        )

    lines.extend(
        [
            "",
            "## Adapter Matrix",
            "",
            "| Exchange | Required | "
            + " | ".join(f"`{check['key']}`" for check in report["checks"])
            + " |",
            "| --- | --- | " + " | ".join("---" for _ in report["checks"]) + " |",
        ]
    )
    for adapter in report["adapters"]:
        cells = [
            matrix_status_mark(
                adapter["checks"][check["key"]]["ok"],
                required_baseline=adapter["required_baseline"],
                required_for_baseline=check["required_for_baseline"],
            )
            for check in report["checks"]
        ]
        required = status_mark(adapter["required_baseline"]) if adapter["required_baseline"] else "-"
        lines.append(
            f"| `{adapter['exchange']}` | {required} | "
            + " | ".join(cells)
            + " |"
        )
    lines.append("")
    return "\n".join(lines)


def normalize_status_doc_for_check(text: str) -> str:
    return re.sub(
        r"(?m)^- Generated at: `[^`]+`$",
        "- Generated at: `<ignored>`",
        text,
    )


def write_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve() if args.repo_root else detect_repo_root(Path.cwd())
    exchanges = normalize_exchanges([*args.exchange, args.exchanges or ""])
    required = normalize_exchanges([args.required_exchanges])
    report = build_report(repo_root, exchanges, required)
    output = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)

    if args.json_out:
        path = args.json_out if args.json_out.is_absolute() else repo_root / args.json_out
        write_atomic(path, output + "\n")
    elif not args.write_status_doc and not args.check_status_doc:
        print(output)

    status_path = args.write_status_doc or args.status_doc
    status_path = status_path if status_path.is_absolute() else repo_root / status_path
    status_text = render_status_doc(report)
    if args.write_status_doc:
        write_atomic(status_path, status_text)
    if args.check_status_doc:
        if not status_path.exists() or normalize_status_doc_for_check(
            read_text(status_path)
        ) != normalize_status_doc_for_check(status_text):
            print(f"status document is stale: {status_path}", file=sys.stderr)
            return 1

    if args.check and report["summary"]["missing_required_exchanges"]:
        print(
            "missing required exchanges: "
            + ", ".join(report["summary"]["missing_required_exchanges"]),
            file=sys.stderr,
        )
        return 1
    if args.check and args.strict and report["summary"]["required_exchange_findings"]:
        print(
            "required exchange coverage findings: "
            + json.dumps(report["summary"]["required_exchange_findings"], ensure_ascii=False, sort_keys=True),
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
