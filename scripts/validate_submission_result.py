#!/usr/bin/env python3
"""Validate a submitted result.json file for the PR check."""

from __future__ import annotations

import hashlib
import json
import os
import re
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
    "scenarios_implemented",
    "scenarios_passed",
    "penalty_ms",
    "recovery_penalty_ms",
    "false_positive_failover_count",
]

SCENARIO_ID_RE = re.compile(r"^[0-9]{2}-[a-z0-9-]+$")


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


def expected_scenario_ids(repo_root: Path) -> set[str]:
    scenarios_dir = repo_root / "scenarios"
    if not scenarios_dir.is_dir():
        return set()
    return {
        p.name
        for p in scenarios_dir.iterdir()
        if p.is_dir() and SCENARIO_ID_RE.match(p.name)
    }


def validate_scenarios_array(
    scenarios: list[Any], known_ids: set[str]
) -> None:
    """Per-scenario shape and id checks.

    A passed scenario must also have its common validator passed and a
    non-null common_validator object — run.sh enforces this in code, so a
    submission claiming passed=true without it is hand-edited.
    """
    seen_ids: set[str] = set()
    for index, scenario in enumerate(scenarios):
        if not isinstance(scenario, dict):
            fail(f"scenarios[{index}] must be an object")

        sid = scenario.get("id")
        if not isinstance(sid, str) or not SCENARIO_ID_RE.match(sid):
            fail(f"scenarios[{index}].id is missing or malformed: {sid!r}")
        if sid in seen_ids:
            fail(f"scenarios[{index}].id duplicated: {sid}")
        seen_ids.add(sid)
        if known_ids and sid not in known_ids:
            fail(
                f"scenarios[{index}].id={sid} is not a known scenario folder. "
                f"Expected one of: {sorted(known_ids)}"
            )

        passed = scenario.get("passed")
        if not isinstance(passed, bool):
            fail(f"scenarios[{index}].passed must be a boolean")

        if passed:
            cv_passed = scenario.get("common_validator_passed")
            if cv_passed is not True:
                fail(
                    f"scenarios[{index}] (id={sid}) claims passed=true but "
                    f"common_validator_passed is {cv_passed!r}. run.sh only "
                    f"sets passed=true when the common validator passes."
                )
            cv = scenario.get("common_validator")
            if not isinstance(cv, dict):
                fail(
                    f"scenarios[{index}] (id={sid}) claims passed=true but "
                    f"common_validator is not an object (got {type(cv).__name__})"
                )

            recovery = scenario.get("recovery_time_ms")
            failure = scenario.get("failure_injected_ms")
            recovered = scenario.get("recovered_ms")
            if (
                isinstance(recovery, int)
                and not isinstance(recovery, bool)
                and isinstance(failure, int)
                and not isinstance(failure, bool)
                and isinstance(recovered, int)
                and not isinstance(recovered, bool)
                and failure > 0
                and recovered > 0
            ):
                if recovered - failure != recovery:
                    fail(
                        f"scenarios[{index}] (id={sid}) recovery_time_ms "
                        f"inconsistent: recovered_ms - failure_injected_ms = "
                        f"{recovered - failure} but recovery_time_ms = {recovery}"
                    )


def validate_summary_consistency(
    summary: dict[str, Any], scenarios: list[Any]
) -> None:
    """Recompute every aggregate the runner emits and compare.

    summary.penalty_ms is checked separately by the caller.
    """
    implemented = require_int(summary, "scenarios_implemented")
    if implemented != len(scenarios):
        fail(
            f"scenarios_implemented inconsistent with scenarios array.\n"
            f"  summary.scenarios_implemented: {implemented}\n"
            f"  len(scenarios):                {len(scenarios)}"
        )

    actually_passed = sum(1 for s in scenarios if isinstance(s, dict) and s.get("passed") is True)
    claimed_passed = require_int(summary, "scenarios_passed")
    if claimed_passed != actually_passed:
        fail(
            f"scenarios_passed inconsistent with scenarios array.\n"
            f"  summary.scenarios_passed:        {claimed_passed}\n"
            f"  count(scenarios where passed):   {actually_passed}"
        )

    if "solved_scenarios" in summary:
        solved = summary["solved_scenarios"]
        if isinstance(solved, bool) or not isinstance(solved, int):
            fail("solved_scenarios must be an integer when present")
        if solved != actually_passed:
            fail(
                f"solved_scenarios inconsistent.\n"
                f"  summary.solved_scenarios:        {solved}\n"
                f"  count(scenarios where passed):   {actually_passed}"
            )

    recovery_sum = sum(
        as_int_field(s, "recovery_time_ms")
        for s in scenarios
        if isinstance(s, dict) and s.get("passed") is True
    )
    claimed_recovery_penalty = require_int(summary, "recovery_penalty_ms")
    if claimed_recovery_penalty != recovery_sum:
        fail(
            f"recovery_penalty_ms inconsistent.\n"
            f"  summary.recovery_penalty_ms:                  {claimed_recovery_penalty}\n"
            f"  sum(recovery_time_ms for passed scenarios):   {recovery_sum}"
        )

    fp_sum = sum(
        as_int_field(s, "false_positive_failover_count")
        for s in scenarios
        if isinstance(s, dict)
    )
    claimed_fp = require_int(summary, "false_positive_failover_count")
    if claimed_fp != fp_sum:
        fail(
            f"false_positive_failover_count inconsistent.\n"
            f"  summary.false_positive_failover_count:       {claimed_fp}\n"
            f"  sum across scenarios:                        {fp_sum}"
        )

    if actually_passed > 0:
        recovery_count = sum(
            1
            for s in scenarios
            if isinstance(s, dict) and s.get("passed") is True
        )
        if "avg_recovery_time_ms" in summary:
            avg = summary["avg_recovery_time_ms"]
            expected_avg = recovery_sum // recovery_count if recovery_count else 0
            if isinstance(avg, bool) or not isinstance(avg, int):
                fail("avg_recovery_time_ms must be an integer when scenarios passed")
            if avg != expected_avg:
                fail(
                    f"avg_recovery_time_ms inconsistent.\n"
                    f"  summary.avg_recovery_time_ms: {avg}\n"
                    f"  recovery_sum // count:        {expected_avg}"
                )
        if "max_recovery_time_ms" in summary:
            mx = summary["max_recovery_time_ms"]
            expected_max = max(
                (
                    as_int_field(s, "recovery_time_ms")
                    for s in scenarios
                    if isinstance(s, dict) and s.get("passed") is True
                ),
                default=0,
            )
            if isinstance(mx, bool) or not isinstance(mx, int):
                fail("max_recovery_time_ms must be an integer when scenarios passed")
            if mx != expected_max:
                fail(
                    f"max_recovery_time_ms inconsistent.\n"
                    f"  summary.max_recovery_time_ms: {mx}\n"
                    f"  max(recovery_time_ms):        {expected_max}"
                )


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

    # C2: scenarios array shape — id format, no duplicates, known ids,
    # and per-scenario invariants for passed=true entries.
    known_ids = expected_scenario_ids(repo_root)
    validate_scenarios_array(scenarios, known_ids)

    # C3: every aggregate the runner emits must equal what we recompute
    # from the scenarios array. Closes the "edit only summary" hole.
    validate_summary_consistency(summary, scenarios)

    print(f"student_conf_sha256: {expected_hash} (matches submissions/{name}/conf)")
    print(f"penalty_ms consistent with scenario elapsed times ({elapsed_sum})")
    print(f"summary aggregates consistent with scenarios array")
    print("submission result is valid")


if __name__ == "__main__":
    main()
