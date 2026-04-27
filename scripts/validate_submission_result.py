#!/usr/bin/env python3
"""Validate a submitted result.json file for the PR check."""

from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any


REQUIRED_TOP_LEVEL_KEYS = [
    "schema_version",
    "timestamp",
    "git_commit",
    "student_conf_sha256",
    "summary",
    "scenarios",
]

REQUIRED_SUMMARY_KEYS = [
    "scenarios_total",
    "scenarios_passed",
    "penalty_ms",
    "false_positive_failover_count",
]


def fail(message: str) -> None:
    print(f"ERROR: {message}")
    sys.exit(1)


def require_int(mapping: dict[str, Any], key: str) -> int:
    value = mapping.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        fail(f"{key} must be an integer")
    return value


def as_int_field(mapping: dict[str, Any], key: str) -> int:
    value = mapping.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        return 0
    return value


def hash_conf_dir(conf_dir: Path) -> str:
    """Path-independent hash of a conf directory.

    Mirrors scripts/lib/common.sh:student_conf_hash so that the same conf
    yields the same digest regardless of whether it lives in student/conf
    or submissions/<name>/conf.
    """
    if not conf_dir.is_dir():
        fail(f"conf directory not found: {conf_dir}")

    files = sorted(
        p
        for p in conf_dir.rglob("*")
        if p.is_file() and not p.name.startswith(".")
    )
    if not files:
        fail(f"conf directory is empty: {conf_dir}")

    lines = []
    for path in files:
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        lines.append(f"{path.name}:{digest}\n")
    blob = "".join(lines).encode()
    return hashlib.sha256(blob).hexdigest()


def main() -> None:
    if len(sys.argv) != 2:
        fail("usage: validate_submission_result.py <name>")

    name = sys.argv[1]
    minimum_passed = int(os.environ.get("MIN_SCENARIOS_PASSED", "6"))

    repo_root = Path(__file__).resolve().parent.parent
    submission_dir = repo_root / "submissions" / name
    result_path = submission_dir / "result.json"
    conf_dir = submission_dir / "conf"

    if not result_path.is_file():
        fail(f"{result_path} does not exist")

    try:
        data = json.loads(result_path.read_text())
    except json.JSONDecodeError as exc:
        fail(f"invalid JSON: {exc}")

    if not isinstance(data, dict):
        fail("top-level JSON value must be an object")

    for key in REQUIRED_TOP_LEVEL_KEYS:
        if key not in data:
            fail(f"missing top-level field: {key}")

    summary = data["summary"]
    if not isinstance(summary, dict):
        fail("summary must be an object")

    for key in REQUIRED_SUMMARY_KEYS:
        if key not in summary:
            fail(f"missing summary field: {key}")

    scenarios = data["scenarios"]
    if not isinstance(scenarios, list):
        fail("scenarios must be an array")

    passed = require_int(summary, "scenarios_passed")
    total = require_int(summary, "scenarios_total")
    penalty = require_int(summary, "penalty_ms")
    false_positive = require_int(summary, "false_positive_failover_count")

    print(f"scenarios passed: {passed}/{total}")
    print(f"penalty_ms: {penalty:,}")
    print(f"false_positive_failover_count: {false_positive}")

    if passed < minimum_passed:
        fail(f"pass threshold not met: {passed}/{total} (minimum {minimum_passed})")

    expected_hash = data["student_conf_sha256"]
    if not isinstance(expected_hash, str) or len(expected_hash) != 64:
        fail("student_conf_sha256 must be a 64-character hex string")

    actual_hash = hash_conf_dir(conf_dir)
    if expected_hash != actual_hash:
        fail(
            "student_conf_sha256 mismatch — result.json was not produced from "
            f"submissions/{name}/conf.\n"
            f"  result.json: {expected_hash}\n"
            f"  conf dir:    {actual_hash}"
        )

    # C1: penalty_ms must equal the sum of elapsed_ms across passed scenarios.
    # run_all.sh computes summary.penalty_ms this exact way, so a tampered
    # summary won't match a tampered (or honest) scenario list.
    elapsed_sum = 0
    for sc in scenarios:
        if not isinstance(sc, dict):
            continue
        if sc.get("passed") is True:
            elapsed_sum += as_int_field(sc, "elapsed_ms")

    if elapsed_sum != penalty:
        fail(
            f"penalty_ms inconsistent with scenario elapsed times.\n"
            f"  summary.penalty_ms:                  {penalty}\n"
            f"  sum(elapsed_ms for passed scenarios): {elapsed_sum}"
        )

    print(f"student_conf_sha256: {expected_hash} (matches submissions/{name}/conf)")
    print(f"penalty_ms consistent with scenario elapsed times ({elapsed_sum})")
    print("submission result is valid")


if __name__ == "__main__":
    main()
