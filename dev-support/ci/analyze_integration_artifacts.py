#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Parse integration CI output.log artifacts for timing and slow-test stats."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

TOTAL_TIME_RE = re.compile(r"\[INFO\] Total time:\s+(.+)")
TESTS_AGG_RE = re.compile(
    r"\[INFO\] Tests run: (\d+), Failures: (\d+), Errors: (\d+), Skipped: (\d+)$"
)
CLASS_TIME_RE = re.compile(
    r"Time elapsed: ([0-9.]+) s -- in (org\.[^\s]+)"
)
RUNNING_RE = re.compile(r"\[INFO\] Running (org\.[^\s]+)")


def parse_total_time_to_seconds(total_time: str) -> float | None:
    total_time = total_time.strip()
    hour_match = re.match(r"(\d+):(\d{2}) h", total_time)
    if hour_match:
        return int(hour_match.group(1)) * 3600 + int(hour_match.group(2)) * 60
    min_match = re.match(r"(\d+):(\d{2}) min", total_time)
    if min_match:
        return int(min_match.group(1)) * 60 + int(min_match.group(2))
    sec_match = re.match(r"([0-9.]+) s", total_time)
    if sec_match:
        return float(sec_match.group(1))
    return None


def parse_log(path: Path) -> dict:
    text = path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()
    total_time = None
    for line in text.splitlines():
        m = TOTAL_TIME_RE.search(line)
        if m:
            total_time = m.group(1).strip()

    agg_tests = None
    for line in reversed(text.splitlines()):
        m = TESTS_AGG_RE.search(line)
        if m:
            agg_tests = int(m.group(1))
            break

    class_times: list[tuple[float, str]] = []
    for m in CLASS_TIME_RE.finditer(text):
        class_times.append((float(m.group(1)), m.group(2)))

    running = len(RUNNING_RE.findall(text))
    sum_elapsed = sum(t for t, _ in class_times)

    class_times.sort(key=lambda x: -x[0])
    top20 = class_times[:20]

    first_running_line = None
    last_elapsed_line = None
    for index, line in enumerate(lines):
        if first_running_line is None and RUNNING_RE.search(line):
            first_running_line = index
        if CLASS_TIME_RE.search(line):
            last_elapsed_line = index

    total_lines = len(lines)
    pre_test_log_fraction = None
    post_test_log_fraction = None
    if total_lines > 0 and first_running_line is not None:
        pre_test_log_fraction = round(first_running_line / total_lines, 3)
    if total_lines > 0 and last_elapsed_line is not None:
        post_test_log_fraction = round((total_lines - last_elapsed_line) / total_lines, 3)

    estimated_non_test_min = None
    non_test_note = None
    total_seconds = parse_total_time_to_seconds(total_time) if total_time else None
    if total_seconds is not None:
        delta = total_seconds - sum_elapsed
        if delta >= 0:
            estimated_non_test_min = round(delta / 60, 1)
        else:
            non_test_note = "sum exceeds Maven total (parallel forks / nested classes)"

    return {
        "total_time": total_time or "unknown",
        "agg_tests": agg_tests,
        "classes_run": running or len(class_times),
        "sum_class_elapsed_min": round(sum_elapsed / 60, 1),
        "estimated_non_test_min": estimated_non_test_min,
        "non_test_note": non_test_note,
        "pre_test_log_fraction": pre_test_log_fraction,
        "post_test_log_fraction": post_test_log_fraction,
        "top20": top20,
    }


def non_test_time_display(stats: dict) -> str | float:
    estimated = stats.get("estimated_non_test_min")
    if estimated is not None:
        return estimated
    note = stats.get("non_test_note")
    if note:
        return note
    return "n/a"


def format_report(run_id: str, split: str, stats: dict) -> str:
    lines = [
        f"### Run {run_id} — {split}",
        "",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Maven Total time | {stats['total_time']} |",
        f"| Aggregate Tests run (last module) | {stats['agg_tests']} |",
        f"| Surefire classes (Running …) | {stats['classes_run']} |",
        f"| Sum of class elapsed (min) | {stats['sum_class_elapsed_min']} |",
        f"| Est. non-test time (min) | {non_test_time_display(stats)} |",
        f"| Log fraction before 1st test | {stats.get('pre_test_log_fraction', 'n/a')} |",
        f"| Log fraction after last class | {stats.get('post_test_log_fraction', 'n/a')} |",
        "",
        "Top 20 slowest test classes (seconds):",
        "",
        "| Seconds | Class |",
        "|---------|-------|",
    ]
    for sec, cls in stats["top20"]:
        lines.append(f"| {sec} | `{cls}` |")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--artifact-root",
        type=Path,
        required=True,
        help="Directory containing run_<id>/integration-<split>/output.log",
    )
    parser.add_argument(
        "--run-ids",
        nargs="+",
        required=True,
        help="GitHub Actions run database IDs",
    )
    parser.add_argument(
        "--splits",
        nargs="+",
        default=["om", "hdds", "client"],
        help="Integration split names (artifact integration-<split>)",
    )
    parser.add_argument("-o", "--output", type=Path, help="Write markdown report here")
    args = parser.parse_args()

    sections: list[str] = [
        "# Integration split timing baseline (Surefire)",
        "",
        "Generated by `dev-support/ci/analyze_integration_artifacts.py`.",
        "",
    ]

    for run_id in args.run_ids:
        sections.append(f"## Run https://github.com/apache/ozone/actions/runs/{run_id}")
        sections.append("")
        for split in args.splits:
            log_path = args.artifact_root / f"run_{run_id}" / f"integration-{split}" / "output.log"
            if not log_path.is_file():
                sections.append(f"### {split}: _missing {log_path}_\n")
                continue
            stats = parse_log(log_path)
            sections.append(format_report(run_id, split, stats))

    report = "\n".join(sections)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report, encoding="utf-8")
        print(f"Wrote {args.output}", file=sys.stderr)
    else:
        print(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
