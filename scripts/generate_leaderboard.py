#!/usr/bin/env python3
"""Generate the GitHub Pages leaderboard from submission result files."""

from __future__ import annotations

import glob
import html
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
KST = timezone(timedelta(hours=9))

SCENARIO_IDS = [
    "01-clean-kill",
    "02-hard-kill",
    "03-standby-failure",
    "04-journalnode-failure",
    "05-zookeeper-failure",
    "06-active-zk-disconnect",
    "07-datanode-failure",
    "08-large-write-failover",
    "09-chaos-test",
]

SCENARIO_LABELS = ["1", "2", "3", "4", "5", "6", "7", "8", "9"]


def as_int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def fmt_ms(ms: int) -> str:
    if ms == 0:
        return "-"
    if ms < 60_000:
        return f"{ms / 1000:.1f}s"
    minutes = ms // 60_000
    seconds = (ms % 60_000) // 1000
    return f"{minutes}:{seconds:02d}"


def load_entries() -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []

    for path_text in sorted(glob.glob(str(ROOT / "submissions" / "*" / "result.json"))):
        path = Path(path_text)
        name = path.parent.name
        try:
            data = json.loads(path.read_text())
        except Exception:
            continue

        summary = data.get("summary", {})
        scenarios = data.get("scenarios", [])
        scenario_map = {
            scenario.get("id"): scenario
            for scenario in scenarios
            if isinstance(scenario, dict) and scenario.get("id")
        }

        per_scenario: list[dict[str, Any]] = []
        for scenario_id in SCENARIO_IDS:
            scenario = scenario_map.get(scenario_id)
            if scenario is None:
                per_scenario.append({"status": "none"})
            elif scenario.get("passed"):
                elapsed = (
                    scenario.get("elapsed_ms")
                    or scenario.get("scenario_elapsed_ms")
                    or scenario.get("duration_ms")
                    or scenario.get("recovery_time_ms")
                    or scenario.get("avg_recovery_time_ms")
                    or 0
                )
                per_scenario.append({"status": "pass", "ms": as_int(elapsed)})
            else:
                elapsed = (
                    scenario.get("elapsed_ms")
                    or scenario.get("scenario_elapsed_ms")
                    or scenario.get("duration_ms")
                    or 0
                )
                per_scenario.append({"status": "fail", "ms": as_int(elapsed)})

        entries.append(
            {
                "name": name,
                "passed": as_int(summary.get("scenarios_passed")),
                "penalty": as_int(summary.get("penalty_ms")),
                "runtime": as_int(summary.get("total_runtime_ms")),
                "fp": as_int(summary.get("false_positive_failover_count")),
                "ts": str(data.get("timestamp", ""))[:10],
                "scenarios": per_scenario,
            }
        )

    entries.sort(key=lambda entry: (-entry["passed"], entry["penalty"], entry["name"]))
    return entries


def render_rows(entries: list[dict[str, Any]]) -> str:
    if not entries:
        colspan = 2 + len(SCENARIO_IDS) + 2
        return f'<tr><td colspan="{colspan}" class="empty">아직 제출이 없습니다.</td></tr>'

    rows: list[str] = []
    for rank, entry in enumerate(entries, 1):
        scenario_cells = []
        for scenario in entry["scenarios"]:
            if scenario["status"] == "pass":
                scenario_cells.append(
                    '<td class="sc pass"><span class="verdict">AC</span>'
                    f'<span class="ms">{fmt_ms(scenario["ms"])}</span></td>'
                )
            elif scenario["status"] == "fail":
                scenario_cells.append(
                    '<td class="sc fail"><span class="verdict">WA</span>'
                    f'<span class="ms">{fmt_ms(scenario["ms"])}</span></td>'
                )
            else:
                scenario_cells.append('<td class="sc none"><span class="dash">-</span></td>')

        medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(rank, "")
        safe_name = html.escape(entry["name"])
        row_class = ' class="top3"' if rank <= 3 else ""
        rows.append(
            f"""<tr{row_class}>
  <td class="rank">{rank}</td>
  <td class="pname">{medal} {safe_name}</td>
  {''.join(scenario_cells)}
  <td class="total">{entry['passed']}<span class="of">/9</span></td>
  <td class="penalty">{fmt_ms(entry['penalty'])}</td>
</tr>"""
        )

    return "\n".join(rows)


def render_page(entries: list[dict[str, Any]]) -> str:
    now = datetime.now(KST).strftime("%Y-%m-%d %H:%M KST")
    headers = "\n".join(f'<th class="sc-head">{label}</th>' for label in SCENARIO_LABELS)
    rows = render_rows(entries)

    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>HDFS HA 과제 리더보드</title>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: "SFMono-Regular", Consolas, "Liberation Mono", monospace;
      background: #0b1020; color: #e6edf3;
      padding: 32px 24px;
    }}
    h1 {{ font-size: 1.35rem; font-weight: 800; margin-bottom: 4px; }}
    .meta {{ color: #94a3b8; font-size: 0.78rem; margin-bottom: 24px; }}
    .board {{
      width: 100%;
      max-width: 1120px;
      overflow-x: auto;
      background: #101827;
      border: 1px solid #334155;
      border-radius: 6px;
    }}
    table {{
      border-collapse: collapse;
      width: 100%;
      min-width: 980px;
    }}
    thead {{ background: #172033; }}
    th {{
      padding: 10px 14px;
      text-align: center;
      font-size: 0.72rem;
      text-transform: uppercase;
      letter-spacing: .06em;
      color: #cbd5e1;
      font-weight: 600;
    }}
    th.pname-head {{ text-align: left; }}
    td {{
      padding: 0;
      border-top: 1px solid #1f2a44;
      vertical-align: middle;
    }}
    td.rank {{
      width: 48px; text-align: center;
      font-size: 0.8rem; color: #94a3b8;
      padding: 12px 8px;
    }}
    td.pname {{
      padding: 12px 16px;
      font-size: 0.9rem; font-weight: 600;
      white-space: nowrap;
    }}
    td.total {{
      text-align: center; padding: 12px 10px;
      font-size: 1.05rem; font-weight: 800; color: #60a5fa;
    }}
    .of {{ color: #94a3b8; font-size: 0.78rem; font-weight: 400; }}
    td.penalty {{
      text-align: center; padding: 12px 14px;
      font-size: 0.85rem; color: #f8fafc;
    }}
    td.sc {{
      width: 72px; text-align: center;
      padding: 10px 4px;
    }}
    td.sc.pass {{ background: #14532d; }}
    td.sc.fail {{ background: #7f1d1d; }}
    td.sc.none {{ background: #101827; }}
    .verdict {{ display: block; font-size: 0.82rem; font-weight: 800; line-height: 1.15; }}
    .ms {{ display: block; font-size: 0.68rem; color: #dbeafe; margin-top: 3px; }}
    .dash {{ color: #64748b; }}
    tr.top3 td.pname {{ color: #facc15; }}
    tr:hover td {{ background: #1e293b; }}
    tr:hover td.sc.pass {{ background: #166534; }}
    tr:hover td.sc.fail {{ background: #991b1b; }}
    .empty {{ text-align: center; color: #94a3b8; padding: 48px; font-size: 0.9rem; }}
    .sc-head {{ width: 72px; }}
  </style>
</head>
<body>
  <h1>HDFS HA 과제 리더보드</h1>
  <p class="meta">업데이트: {now} &nbsp;·&nbsp; {len(entries)}명 참여 &nbsp;·&nbsp; AC/WA와 성공 시나리오 실행 시간 합산</p>
  <div class="board">
    <table>
      <thead>
        <tr>
          <th>#</th>
          <th class="pname-head">이름</th>
          {headers}
          <th>통과</th>
          <th>Penalty</th>
        </tr>
      </thead>
      <tbody>
{rows}
      </tbody>
    </table>
  </div>
</body>
</html>
"""


def main() -> None:
    entries = load_entries()
    output_dir = ROOT / "docs"
    output_dir.mkdir(exist_ok=True)
    (output_dir / "index.html").write_text(render_page(entries))
    print(f"generated docs/index.html ({len(entries)} entries)")


if __name__ == "__main__":
    main()
