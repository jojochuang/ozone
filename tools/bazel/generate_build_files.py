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
"""Emit stub BUILD.bazel files for Maven modules (OZONE_BAZEL_GENERATED)."""

from __future__ import annotations

import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
NS = {"m": "http://maven.apache.org/POM/4.0.0"}
HEADER = """# OZONE_BAZEL_GENERATED — refresh with tools/bazel/generate_build_files.py
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

load("@rules_java//java:defs.bzl", "java_library", "java_test")

package(default_visibility = ["//visibility:public"])

"""


def artifact_id(pom: Path) -> str:
    root = ET.parse(pom).getroot()
    el = root.find("m:artifactId", NS)
    return (el.text or pom.parent.name).strip()


def has_java(pom_dir: Path) -> bool:
    return (pom_dir / "src/main/java").is_dir()


def has_tests(pom_dir: Path) -> bool:
    return (pom_dir / "src/test/java").is_dir()


def write_build(pom: Path) -> bool:
    module_dir = pom.parent
    build = module_dir / "BUILD.bazel"
    if build.exists() and "OZONE_BAZEL_GENERATED" not in build.read_text(encoding="utf-8"):
        return False
    aid = artifact_id(pom)
    target = aid.replace("_", "-")
    lines = [HEADER]
    if has_java(module_dir):
        lines.append(
            f'java_library(\n    name = "{target}",\n'
            f'    srcs = glob(["src/main/java/**/*.java"], allow_empty = True),\n'
            f'    resources = glob(["src/main/resources/**"], allow_empty = True),\n'
            f'    tags = ["ozone-maven-stub", "manual"],\n'
            f'    deps = [],  # TODO: map deps from {pom.name}\n)\n\n'
        )
    if has_tests(module_dir):
        lines.append(
            f'java_library(\n    name = "{target}-tests",\n'
            f'    testonly = True,\n'
            f'    srcs = glob(["src/test/java/**/*.java"], allow_empty = True),\n'
            f'    resources = glob(["src/test/resources/**"], allow_empty = True),\n'
            f'    tags = ["ozone-maven-stub", "manual"],\n'
            f'    deps = [":{target}"],\n)\n'
        )
    if not has_java(module_dir) and not has_tests(module_dir):
        lines.append('filegroup(\n    name = "{target}",\n    srcs = glob(["**/*"], allow_empty = True),\n)\n'.format(target=target))
    build.write_text("".join(lines), encoding="utf-8")
    return True


def main() -> int:
    count = 0
    for pom in sorted(ROOT.glob("**/pom.xml")):
        if "target" in pom.parts:
            continue
        if write_build(pom):
            count += 1
    print(f"Wrote/updated {count} BUILD.bazel stubs", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
