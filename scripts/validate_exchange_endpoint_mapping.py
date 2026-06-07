#!/usr/bin/env python3
"""Validate exchange endpoint mapping YAML files.

The validator intentionally combines JSON Schema checks with exchange-toolchain
invariants that are easier to express procedurally: unique operation names,
adapter directory matching the mapped exchange, and request-spec-required
coverage for declared private REST endpoints.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

try:
    import jsonschema
except ImportError as exc:  # pragma: no cover - environment diagnostic
    raise SystemExit(
        "missing dependency: jsonschema. Install it with `python -m pip install jsonschema`."
    ) from exc

try:
    import yaml
except ImportError as exc:  # pragma: no cover - environment diagnostic
    raise SystemExit(
        "missing dependency: PyYAML. Install it with `python -m pip install pyyaml`."
    ) from exc


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCHEMA = REPO_ROOT / "format_schemas" / "exchange_endpoint_mapping.schema.json"
ADAPTERS_DIR = REPO_ROOT / "crates/rustcta-exchange-gateway/src/adapters"

PRIVATE_AUTH = {
    "api_key",
    "signed",
    "hmac",
    "listen_key",
    "bearer",
    "jwt",
    "md5_hmac_sha256",
}
SUPPORTED_ENDPOINT_TRANSPORTS = {"rest"}
SUPPORTED_ENDPOINT_REQUIRED_FIELDS = {
    "method",
    "path",
    "auth",
    "rate_limit_bucket",
    "weight",
    "request_spec_required",
}


class ValidationIssue:
    def __init__(self, path: Path, message: str) -> None:
        self.path = path
        self.message = message

    def __str__(self) -> str:
        try:
            display = self.path.relative_to(REPO_ROOT)
        except ValueError:
            display = self.path
        return f"{display}: {self.message}"


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValueError("mapping root must be an object")
    return data


def iter_default_mappings() -> list[Path]:
    return sorted(ADAPTERS_DIR.glob("*/endpoint_mapping.yaml"))


def json_path(error: jsonschema.ValidationError) -> str:
    if not error.path:
        return "$"
    return "$." + ".".join(str(part) for part in error.path)


def validate_schema(
    path: Path,
    mapping: dict[str, Any],
    validator: jsonschema.Draft202012Validator,
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    for error in sorted(validator.iter_errors(mapping), key=lambda item: list(item.path)):
        issues.append(ValidationIssue(path, f"{json_path(error)}: {error.message}"))
    return issues


def validate_invariants(path: Path, mapping: dict[str, Any]) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    exchange = mapping.get("exchange")
    if not isinstance(exchange, str):
        return [ValidationIssue(path, "exchange must be a string")]

    adapter_dir = path.parent.name
    if adapter_dir != exchange:
        issues.append(
            ValidationIssue(
                path,
                f"exchange `{exchange}` must match adapter directory `{adapter_dir}`",
            )
        )

    seen_operations: dict[tuple[str, str, str], str] = {}
    endpoint_operations: set[str] = set()

    endpoints = mapping.get("endpoints", [])
    if not isinstance(endpoints, list):
        endpoints = []
    for index, endpoint in enumerate(endpoints):
        if not isinstance(endpoint, dict):
            continue
        operation = endpoint.get("operation")
        if not isinstance(operation, str):
            continue
        location = f"endpoints[{index}]"
        product = endpoint.get("product")
        route = endpoint.get("path")
        validate_operation_name(
            path,
            issues,
            (operation, str(product), str(route)),
            operation,
            location,
            seen_operations,
        )
        endpoint_operations.add(operation)

        if endpoint.get("transport") not in SUPPORTED_ENDPOINT_TRANSPORTS:
            issues.append(
                ValidationIssue(path, f"{location} transport must be `rest` for endpoints")
            )
        if endpoint.get("support") != "unsupported":
            missing = sorted(
                field
                for field in SUPPORTED_ENDPOINT_REQUIRED_FIELDS
                if field not in endpoint
            )
            if missing:
                issues.append(
                    ValidationIssue(
                        path,
                        f"{location} supported operation `{operation}` is missing "
                        f"required field(s): {', '.join(missing)}",
                    )
                )
        if endpoint.get("auth") in PRIVATE_AUTH:
            if endpoint.get("request_spec_required") is not True:
                issues.append(
                    ValidationIssue(
                        path,
                        f"{location} private REST operation `{operation}` must set "
                        "request_spec_required: true",
                    )
                )

    streams = mapping.get("streams", [])
    if streams is None:
        streams = []
    if not isinstance(streams, list):
        streams = []
    for index, stream in enumerate(streams):
        if not isinstance(stream, dict):
            continue
        operation = stream.get("operation")
        if not isinstance(operation, str):
            continue
        location = f"streams[{index}]"
        validate_operation_name(
            path,
            issues,
            (operation, "stream", str(stream.get("channel", ""))),
            operation,
            location,
            seen_operations,
        )
        validate_ref_operation(path, issues, stream, "resync", endpoint_operations, location)
        validate_ref_operation(path, issues, stream, "auth_renewal", endpoint_operations, location)

    return issues


def validate_operation_name(
    path: Path,
    issues: list[ValidationIssue],
    key: tuple[str, str, str],
    operation: str,
    location: str,
    seen_operations: dict[tuple[str, str, str], str],
) -> None:
    previous = seen_operations.get(key)
    if previous:
        issues.append(
            ValidationIssue(
                path,
                f"{location} operation `{operation}` duplicates {previous} for "
                f"product/path `{key[1]}` `{key[2]}`",
            )
        )
    else:
        seen_operations[key] = location


def validate_ref_operation(
    path: Path,
    issues: list[ValidationIssue],
    stream: dict[str, Any],
    key: str,
    endpoint_operations: set[str],
    location: str,
) -> None:
    ref = stream.get(key)
    if not isinstance(ref, dict):
        return
    operation = ref.get("endpoint_operation")
    if operation and operation not in endpoint_operations:
        issues.append(
            ValidationIssue(
                path,
                f"{location}.{key}.endpoint_operation `{operation}` is not a mapped endpoint",
            )
        )


def validate_files(paths: list[Path], schema_path: Path) -> int:
    schema = load_json(schema_path)
    validator = jsonschema.Draft202012Validator(schema)
    issues: list[ValidationIssue] = []

    for path in paths:
        try:
            mapping = load_yaml(path)
        except Exception as exc:  # noqa: BLE001 - report all YAML/load errors
            issues.append(ValidationIssue(path, f"failed to load YAML: {exc}"))
            continue
        issues.extend(validate_schema(path, mapping, validator))
        issues.extend(validate_invariants(path, mapping))

    if issues:
        for issue in issues:
            print(issue, file=sys.stderr)
        print(f"FAILED: {len(issues)} issue(s) across {len(paths)} mapping file(s)", file=sys.stderr)
        return 1

    print(f"OK: validated {len(paths)} endpoint mapping file(s)")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate rustcta-exchange-gateway endpoint_mapping.yaml files."
    )
    parser.add_argument(
        "paths",
        nargs="*",
        type=Path,
        help="Mapping files to validate. Defaults to every adapter endpoint_mapping.yaml file.",
    )
    parser.add_argument(
        "--schema",
        type=Path,
        default=DEFAULT_SCHEMA,
        help=f"JSON schema path. Default: {DEFAULT_SCHEMA.relative_to(REPO_ROOT)}",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    paths = [path.resolve() for path in args.paths] if args.paths else iter_default_mappings()
    if not paths:
        print("FAILED: no mapping files configured for default validation", file=sys.stderr)
        return 1
    missing = [path for path in paths if not path.exists()]
    if missing:
        for path in missing:
            print(f"{path}: mapping file does not exist", file=sys.stderr)
        return 1
    return validate_files(paths, args.schema.resolve())


if __name__ == "__main__":
    raise SystemExit(main())
