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
"""Append ozone_junit5_package rules for each *-tests library (idempotent marker)."""

# pylint: disable=missing-function-docstring,duplicate-code

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MARKER = "# OZONE_JUNIT5_WIRED"


def packages_for_tests(test_java_root: Path) -> set[str]:
    pkgs: set[str] = set()
    for java in test_java_root.rglob("Test*.java"):
        if "src/test/java" not in str(java):
            continue
        text = java.read_text(encoding="utf-8", errors="replace")
        m = re.search(r"^package\s+([\w.]+);", text, re.MULTILINE)
        if m:
            pkgs.add(m.group(1))
    return pkgs


def main() -> int:
    for build in sorted(ROOT.glob("hadoop-*/**/BUILD.bazel")):
        if "target" in build.parts:
            continue
        text = build.read_text(encoding="utf-8")
        if MARKER in text:
            continue
        m = re.search(r'name = "([^"]+-tests)"', text)
        if not m:
            continue
        tests_target = m.group(1)
        module_dir = build.parent
        test_root = module_dir / "src/test/java"
        if not test_root.is_dir():
            continue
        pkgs = sorted(packages_for_tests(test_root))
        if not pkgs:
            continue
        if 'load("//tools/bazel:junit5.bzl"' not in text:
            text = re.sub(
                r"(load\([^\n]+\n)",
                r'\1load("//tools/bazel:junit5.bzl", "ozone_junit5_package")\n',
                text,
                count=1,
            )
        block = [MARKER, ""]
        for pkg in pkgs:
            safe = pkg.replace(".", "-")
            block.append(
                f'ozone_junit5_package(\n'
                f'    name = "{tests_target}-pkg-{safe}",\n'
                f'    package = "{pkg}",\n'
                f'    size = "large",\n'
                f'    tags = ["unit"],\n'
                f'    deps = [":{tests_target}"],\n'
                f')\n'
            )
        build.write_text(text.rstrip() + "\n\n" + "\n".join(block) + "\n", encoding="utf-8")
        print(f"Wired {len(pkgs)} packages in {build}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
