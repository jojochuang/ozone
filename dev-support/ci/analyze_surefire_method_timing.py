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
# See the License for the License for the specific language governing permissions and
# limitations under the License.

"""Rank slowest JUnit test methods from Maven Surefire TEST-*.xml reports."""

from __future__ import annotations

import argparse
import sys
import xml.etree.ElementTree as ET
from glob import glob
from pathlib import Path


def collect_testcases(report_root: Path) -> list[tuple[float, str, str, str]]:
    cases: list[tuple[float, str, str, str]] = []
    pattern = str(report_root / "**" / "TEST-*.xml")
    for path_str in glob(pattern, recursive=True):
        path = Path(path_str)
        try:
            root = ET.parse(path).getroot()
        except ET.ParseError:
            continue
        if root.tag != "testsuite":
            continue
        class_name = root.get("name", path.stem.removeprefix("TEST-"))
        for testcase in root.findall("testcase"):
            name = testcase.get("name", "")
            time_str = testcase.get("time", "0")
            try:
                elapsed = float(time_str)
            except ValueError:
                elapsed = 0.0
            module = path.parent.parent.name if path.parent.name == "surefire-reports" else path.parent.name
            cases.append((elapsed, class_name, name, module))
    cases.sort(key=lambda x: -x[0])
    return cases


def format_markdown(cases: list[tuple[float, str, str, str]], top_n: int, class_filter: str | None) -> str:
    if class_filter:
        filtered = [c for c in cases if class_filter in c[1]]
    else:
        filtered = cases
    lines = [
        "| Seconds | Class | Method | Module |",
        "|---------|-------|--------|--------|",
    ]
    for elapsed, class_name, name, module in filtered[:top_n]:
        lines.append(f"| {elapsed:.3f} | `{class_name}` | `{name}` | {module} |")
    if not filtered:
        lines.append("| _no matching cases_ | | | |")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--report-root",
        type=Path,
        default=Path("."),
        help="Root directory to search for surefire-reports/TEST-*.xml",
    )
    parser.add_argument(
        "--class",
        dest="class_filter",
        default=None,
        help="Only include test classes whose name contains this substring",
    )
    parser.add_argument("-n", "--top", type=int, default=30, help="Number of methods to list")
    parser.add_argument("-o", "--output", type=Path, help="Write markdown table here")
    args = parser.parse_args()

    cases = collect_testcases(args.report_root)
    report = format_markdown(cases, args.top, args.class_filter)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report + "\n", encoding="utf-8")
        print(f"Wrote {args.output}", file=sys.stderr)
    else:
        print(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
