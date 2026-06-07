#!/usr/bin/env python3
"""Redact sensitive values from exchange fixture files.

Default mode is a dry run and prints JSON. Use --check for CI, --write for
atomic in-place writes, or --output-dir for sanitized copies.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any


DEFAULT_PATH = Path("tests/fixtures/exchanges")
TEXT_SUFFIXES = (".json", ".jsonl", ".ndjson", ".yaml", ".yml", ".txt", ".http", ".log")
SKIP_DIRS = {".git", "target", "node_modules", ".venv", "venv"}
PLACEHOLDERS = {
    "api_key": "<redacted:api_key>",
    "secret": "<redacted:secret>",
    "token": "<redacted:token>",
    "authorization": "<redacted:authorization>",
    "signature": "<redacted:signature>",
    "account_id": "<redacted:account_id>",
    "user_id": "<redacted:user_id>",
    "email": "redacted@example.invalid",
    "phone": "<redacted:phone>",
    "address": "<redacted:address>",
    "order_id": "<redacted:order_id>",
    "default": "<redacted>",
}

PRIVATE_KEY_RE = re.compile(
    r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----",
    re.DOTALL,
)
JWT_RE = re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b")
BEARER_RE = re.compile(r"\bBearer\s+[A-Za-z0-9._~+/=-]{12,}\b", re.IGNORECASE)
ETH_ADDRESS_RE = re.compile(r"\b0x[a-fA-F0-9]{40}\b")
TRON_ADDRESS_RE = re.compile(r"\bT[1-9A-HJ-NP-Za-km-z]{33}\b")
BTC_ADDRESS_RE = re.compile(r"\b(?:bc1|[13])[a-zA-HJ-NP-Z0-9]{25,62}\b", re.IGNORECASE)
QUERY_SECRET_RE = re.compile(
    r"(?i)(api[_-]?key|api[_-]?secret|secret|token|passphrase|signature|authorization)=([^&\s]+)"
)
HEADER_SECRET_RE = re.compile(
    r"(?im)^([A-Za-z0-9_-]*(?:api|key|secret|token|passphrase|authorization|signature)[A-Za-z0-9_-]*\s*[:=]\s*)(.+)$"
)
YAML_SCALAR_RE = re.compile(r"^(\s*[-]?\s*)([A-Za-z0-9_.-]+)(\s*:\s*)(.*?)(\s*(?:#.*)?)$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Recursively sanitize exchange fixture files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  python3 scripts/sanitize_exchange_fixture.py
  python3 scripts/sanitize_exchange_fixture.py --check tests/fixtures/exchanges/binance
  python3 scripts/sanitize_exchange_fixture.py --write tests/fixtures/exchanges/okx
  python3 scripts/sanitize_exchange_fixture.py --output-dir /tmp/sanitized tests/fixtures/exchanges
""",
    )
    parser.add_argument(
        "paths",
        nargs="*",
        type=Path,
        help="Files or directories to sanitize. Defaults to tests/fixtures/exchanges.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Repository root for resolving relative paths. Defaults to auto-detection.",
    )
    parser.add_argument(
        "--suffixes",
        default=",".join(TEXT_SUFFIXES),
        help="Comma-separated file suffixes to scan.",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--write", action="store_true", help="Safely rewrite changed files in place.")
    mode.add_argument(
        "--output-dir",
        type=Path,
        help="Write sanitized copies under this directory, preserving relative paths.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if any file would change. Does not write files.",
    )
    parser.add_argument("--summary-json", type=Path, help="Also write summary JSON to this path.")
    parser.add_argument("--quiet", action="store_true", help="Only print errors.")
    return parser.parse_args()


def detect_repo_root(start: Path) -> Path:
    current = start.resolve()
    if current.is_file():
        current = current.parent
    for candidate in (current, *current.parents):
        if (candidate / "Cargo.toml").exists() and (candidate / DEFAULT_PATH).exists():
            return candidate
    script_root = Path(__file__).resolve().parents[1]
    if (script_root / DEFAULT_PATH).exists():
        return script_root
    return Path.cwd().resolve()


def normalized_key(key: str) -> str:
    return re.sub(r"[^a-z0-9]", "", key.lower())


def sensitive_category(key: str) -> str | None:
    norm = normalized_key(key)
    if not norm:
        return None
    if norm in {"key", "apikey", "xmbxapikey", "accesskey", "accesskeyid"} or norm.endswith("apikey"):
        return "api_key"
    if "secret" in norm or norm in {"privatekey", "passphrase", "password", "pwd"}:
        return "secret"
    if "token" in norm or norm in {"listenkey", "jwt", "bearer"} or norm.endswith("listenkey"):
        return "token"
    if norm in {"authorization", "authheader"}:
        return "authorization"
    if norm in {"signature", "sig", "sign", "digest"} or norm.endswith("signature"):
        return "signature"
    if norm in {"account", "accountid", "acct", "acctid", "subaccount", "subaccountid", "uid", "memberid", "portfolioid", "profileid"}:
        return "account_id"
    if norm in {"userid", "useruid", "customerid"}:
        return "user_id"
    if norm in {"email"}:
        return "email"
    if norm in {"phone", "phonenumber"}:
        return "phone"
    if norm.endswith("address") or norm in {"address", "txid", "transactionid", "hash"}:
        return "address"
    if norm in {"orderid", "ordid", "clordid", "clientorderid", "origclientorderid", "newclientorderid", "orderlinkid", "exchangeorderid", "orderno"} or norm.endswith("orderid") or norm.endswith("ordid"):
        return "order_id"
    return None


def placeholder_for(category: str) -> str:
    return PLACEHOLDERS.get(category, PLACEHOLDERS["default"])


def is_empty(value: Any) -> bool:
    return value is None or value == "" or value == [] or value == {}


def redact_scalar(value: Any, category: str) -> Any:
    placeholder = placeholder_for(category)
    if is_empty(value) or value == placeholder:
        return value
    return placeholder


def sanitize_string(value: str) -> tuple[str, int]:
    replacements = 0

    def replace(pattern: re.Pattern[str], placeholder: str, text: str) -> str:
        nonlocal replacements
        updated, count = pattern.subn(placeholder, text)
        replacements += count
        return updated

    sanitized = value
    sanitized = replace(PRIVATE_KEY_RE, PLACEHOLDERS["secret"], sanitized)
    sanitized = replace(JWT_RE, PLACEHOLDERS["token"], sanitized)
    sanitized = replace(BEARER_RE, f"Bearer {PLACEHOLDERS['token']}", sanitized)
    sanitized = replace(ETH_ADDRESS_RE, PLACEHOLDERS["address"], sanitized)
    sanitized = replace(TRON_ADDRESS_RE, PLACEHOLDERS["address"], sanitized)
    sanitized = replace(BTC_ADDRESS_RE, PLACEHOLDERS["address"], sanitized)

    def query_repl(match: re.Match[str]) -> str:
        nonlocal replacements
        replacements += 1
        category = sensitive_category(match.group(1)) or "default"
        return f"{match.group(1)}={placeholder_for(category)}"

    def header_repl(match: re.Match[str]) -> str:
        nonlocal replacements
        replacements += 1
        category = sensitive_category(match.group(1)) or "default"
        return f"{match.group(1)}{placeholder_for(category)}"

    sanitized = QUERY_SECRET_RE.sub(query_repl, sanitized)
    sanitized = HEADER_SECRET_RE.sub(header_repl, sanitized)
    return sanitized, replacements


def sanitize_node(value: Any) -> tuple[Any, int]:
    if isinstance(value, dict):
        changed = 0
        output: dict[str, Any] = {}
        for key, item in value.items():
            category = sensitive_category(str(key))
            if category is not None:
                if isinstance(item, (dict, list)):
                    redacted = redact_scalar(item, category)
                else:
                    redacted = redact_scalar(item, category)
                output[key] = redacted
                changed += int(redacted != item)
            else:
                output[key], item_changed = sanitize_node(item)
                changed += item_changed
        return output, changed
    if isinstance(value, list):
        changed = 0
        output = []
        for item in value:
            redacted, item_changed = sanitize_node(item)
            output.append(redacted)
            changed += item_changed
        return output, changed
    if isinstance(value, str):
        return sanitize_string(value)
    return value, 0


def parse_suffixes(value: str) -> tuple[str, ...]:
    suffixes: list[str] = []
    for item in value.split(","):
        suffix = item.strip().lower()
        if suffix:
            suffixes.append(suffix if suffix.startswith(".") else f".{suffix}")
    return tuple(suffixes)


def iter_target_files(paths: list[Path], suffixes: tuple[str, ...]) -> list[Path]:
    files: list[Path] = []
    for path in paths:
        resolved = path.resolve()
        if resolved.is_file():
            if resolved.suffix.lower() in suffixes:
                files.append(resolved)
            continue
        if not resolved.exists():
            raise FileNotFoundError(resolved)
        for root, dirnames, filenames in os.walk(resolved):
            dirnames[:] = [name for name in dirnames if name not in SKIP_DIRS]
            for filename in filenames:
                file_path = Path(root) / filename
                if file_path.suffix.lower() in suffixes:
                    files.append(file_path)
    return sorted(set(files))


def sanitize_json_text(text: str) -> tuple[str, int]:
    payload = json.loads(text)
    redacted, changes = sanitize_node(payload)
    if changes == 0:
        return text, 0
    return json.dumps(redacted, ensure_ascii=False, indent=2, sort_keys=False) + "\n", changes


def sanitize_json_lines(text: str) -> tuple[str, int]:
    output: list[str] = []
    changes = 0
    for index, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            output.append(line)
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"line {index}: invalid JSONL: {exc}") from exc
        redacted, item_changes = sanitize_node(payload)
        output.append(json.dumps(redacted, ensure_ascii=False, sort_keys=False))
        changes += item_changes
    trailing = "\n" if text.endswith("\n") else ""
    return "\n".join(output) + trailing, changes


def sanitize_yaml_text(text: str) -> tuple[str, int]:
    output: list[str] = []
    changes = 0
    for line in text.splitlines(keepends=True):
        newline = "\n" if line.endswith("\n") else ""
        body = line[:-1] if newline else line
        match = YAML_SCALAR_RE.match(body)
        if not match:
            sanitized_body, body_changes = sanitize_string(body)
            output.append(sanitized_body + newline)
            changes += body_changes
            continue
        prefix, key, sep, raw_value, comment = match.groups()
        category = sensitive_category(key)
        stripped = raw_value.strip().strip("'\"")
        if category and stripped and stripped not in PLACEHOLDERS.values() and not raw_value.lstrip().startswith(("[", "{")):
            quote = '"' if raw_value.strip().startswith('"') else ""
            redacted = placeholder_for(category)
            value = f"{quote}{redacted}{quote}" if quote else redacted
            output.append(f"{prefix}{key}{sep}{value}{comment}{newline}")
            changes += 1
        else:
            sanitized_body, body_changes = sanitize_string(body)
            output.append(sanitized_body + newline)
            changes += body_changes
    return "".join(output), changes


def sanitize_text(path: Path, text: str) -> tuple[str, int]:
    suffix = path.suffix.lower()
    if suffix == ".json":
        try:
            return sanitize_json_text(text)
        except json.JSONDecodeError:
            return sanitize_string(text)
    if suffix in {".jsonl", ".ndjson"}:
        return sanitize_json_lines(text)
    if suffix in {".yaml", ".yml"}:
        return sanitize_yaml_text(text)
    return sanitize_string(text)


def relative(path: Path, root: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.as_posix()


def output_path_for(file_path: Path, input_paths: list[Path], output_dir: Path) -> Path:
    for root in [path.resolve() for path in input_paths]:
        if root.is_dir():
            try:
                return output_dir / file_path.relative_to(root)
            except ValueError:
                continue
    return output_dir / file_path.name


def write_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def main() -> int:
    args = parse_args()
    if args.check and (args.write or args.output_dir):
        print("--check cannot be combined with --write or --output-dir", file=sys.stderr)
        return 2

    repo_root = args.repo_root.resolve() if args.repo_root else detect_repo_root(Path.cwd())
    paths = [path if path.is_absolute() else repo_root / path for path in args.paths]
    if not paths:
        paths = [repo_root / DEFAULT_PATH]
    suffixes = parse_suffixes(args.suffixes)
    output_dir = args.output_dir.resolve() if args.output_dir else None

    scanned = 0
    changed_files: list[str] = []
    written_files: list[str] = []
    redactions = 0
    errors: list[str] = []

    try:
        target_files = iter_target_files(paths, suffixes)
    except FileNotFoundError as exc:
        print(f"path does not exist: {exc}", file=sys.stderr)
        return 1

    for path in target_files:
        scanned += 1
        try:
            original = path.read_text(encoding="utf-8")
            sanitized, changes = sanitize_text(path, original)
        except Exception as exc:  # noqa: BLE001 - report per-file CLI errors.
            errors.append(f"{relative(path, repo_root)}: {exc}")
            continue
        if sanitized == original:
            continue
        redactions += changes
        changed_files.append(relative(path, repo_root))
        if output_dir is not None:
            out_path = output_path_for(path, paths, output_dir)
            write_atomic(out_path, sanitized)
            written_files.append(out_path.as_posix())
        elif args.write:
            write_atomic(path, sanitized)
            written_files.append(relative(path, repo_root))

    summary = {
        "schema_version": 1,
        "mode": "write" if args.write else "copy" if output_dir else "check" if args.check else "dry_run",
        "files_scanned": scanned,
        "files_with_changes": len(changed_files),
        "redactions": redactions,
        "files_written": len(written_files),
        "changed_files": changed_files,
        "written_files": written_files,
        "errors": errors,
    }
    output = json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if not args.quiet:
        print(output, end="")
    if args.summary_json:
        summary_path = args.summary_json if args.summary_json.is_absolute() else repo_root / args.summary_json
        write_atomic(summary_path, output)

    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1
    if args.check and changed_files:
        print(f"{len(changed_files)} fixture file(s) require sanitization", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
