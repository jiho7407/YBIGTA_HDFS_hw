#!/usr/bin/env python3
"""Validate a submitted result.json file for the PR check."""

from __future__ import annotations

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


def main() -> None:
    if len(sys.argv) != 2:
        fail("usage: validate_submission_result.py <submissions/name/result.json>")

    result_path = Path(sys.argv[1])
    minimum_passed = int(os.environ.get("MIN_SCENARIOS_PASSED", "6"))

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

    print("submission result is valid")


if __name__ == "__main__":
    main()
