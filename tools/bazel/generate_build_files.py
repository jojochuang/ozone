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
"""Emit BUILD.bazel files for Maven modules with dependency mapping."""

from __future__ import annotations

import sys
from pathlib import Path

from defusedxml import ElementTree as ET

from pom_to_bazel import dep_to_label, pom_dependencies, scan_modules
from pom_xml import parse

ROOT = Path(__file__).resolve().parents[2]
NS = {"m": "http://maven.apache.org/POM/4.0.0"}

SKIP_REGENERATE = {
    ROOT / "hadoop-hdds/annotations",
    ROOT / "hadoop-hdds/config",
    ROOT / "hadoop-hdds/interface-client",
    ROOT / "hadoop-hdds/interface-admin",
    ROOT / "hadoop-hdds/interface-server",
    ROOT / "hadoop-ozone/interface-client",
    ROOT / "hadoop-ozone/interface-storage",
    ROOT / "hadoop-ozone/ozone-manager",  # hand-wired deps
    ROOT / "hadoop-ozone/dist",
    ROOT / "hadoop-ozone/recon-codegen",
    ROOT / "hadoop-ozone/ozonefs-shaded",
    ROOT / "hadoop-ozone/mini-cluster",  # Maven puts test-jars on compile classpath
    ROOT / "hadoop-ozone/multitenancy-ranger",  # provided-scope OM/HDDS deps
    ROOT / "hadoop-hdds/framework",  # JAX-RS 2.x classpath (see BUILD.bazel)
    ROOT / "hadoop-ozone/iceberg",  # Java 11 + Iceberg coords (manual)
    ROOT / "hadoop-ozone/recon",  # requires recon-codegen jOOQ outputs (manual)
    ROOT / "hadoop-ozone/cli-debug",
    ROOT / "hadoop-ozone/cli-repair",
    ROOT / "hadoop-ozone/cli-interactive",
    ROOT / "hadoop-ozone/csi",
    ROOT / "hadoop-ozone/mini-cluster",
}

HEADER = """# Licensed to the Apache Software Foundation (ASF) under one or more
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
#
# OZONE_BAZEL_GENERATED — refresh with tools/bazel/generate_build_files.py

load("@rules_java//java:defs.bzl", "java_library")

package(default_visibility = ["//visibility:public"])

"""

STANDARD_TEST_MAVEN = [
    "@maven//:org_assertj_assertj_core",
    "@maven//:org_junit_jupiter_junit_jupiter_api",
    "@maven//:org_junit_jupiter_junit_jupiter_params",
    "@maven//:org_mockito_mockito_core",
    "@maven//:org_mockito_mockito_junit_jupiter",
]


def artifact_id(pom_dir: Path) -> str:
    root = parse(pom_dir / "pom.xml").getroot()
    el = root.find("m:artifactId", NS)
    return (el.text or pom_dir.name).strip()


def has_java(pom_dir: Path) -> bool:
    return (pom_dir / "src/main/java").is_dir()


def has_tests(pom_dir: Path) -> bool:
    return (pom_dir / "src/test/java").is_dir()


def write_build(pom_dir: Path, modules: dict) -> bool:
    if pom_dir in SKIP_REGENERATE:
        return False
    build = pom_dir / "BUILD.bazel"
    aid = artifact_id(pom_dir)
    if not has_java(pom_dir):
        return False
    deps = pom_dependencies(pom_dir, {})
    compile_deps: list[str] = []
    test_deps: list[str] = []
    main_testonly = False
    for group, artifact, scope, dtype in deps:
        label = dep_to_label(group, artifact, scope, dtype, modules)
        if label is None:
            continue
        if dtype == "test-jar":
            if scope in ("test",):
                test_deps.append(label)
            else:
                # Maven test-jar on compile classpath (e.g. mini-cluster) uses test classes in main code.
                compile_deps.append(label)
                if label.endswith("-tests"):
                    main_testonly = True
        elif scope in ("test",):
            test_deps.append(label)
        else:
            compile_deps.append(label)
    compile_deps = sorted(set(compile_deps))
    test_deps = sorted(set(test_deps))
    lines = [HEADER]
    testonly_attr = "    testonly = True,\n" if main_testonly else ""
    lines.append(
        f'java_library(\n    name = "{aid}",\n'
        f"{testonly_attr}"
        f'    srcs = glob(["src/main/java/**/*.java"], allow_empty = True),\n'
        f'    resources = glob(["src/main/resources/**"], allow_empty = True),\n'
        f"    deps = [\n"
    )
    for d in compile_deps:
        lines.append(f'        "{d}",\n')
    lines.append("    ],\n)\n\n")
    if has_tests(pom_dir):
        merged_test_deps = sorted(set(test_deps + STANDARD_TEST_MAVEN))
        lines.append(
            f'java_library(\n    name = "{aid}-tests",\n'
            f"    testonly = True,\n"
            f'    tags = ["manual"],\n'
            f'    srcs = glob(["src/test/java/**/*.java"], allow_empty = True),\n'
            f'    resources = glob(["src/test/resources/**"], allow_empty = True),\n'
            f'    deps = [\n        ":{aid}",\n'
        )
        for d in merged_test_deps:
            lines.append(f'        "{d}",\n')
        lines.append("    ],\n)\n")
    build.write_text("".join(lines), encoding="utf-8")
    return True


def main() -> int:
    modules = scan_modules()
    count = 0
    for pom in sorted(ROOT.glob("**/pom.xml")):
        if "target" in pom.parts:
            continue
        if write_build(pom.parent, modules):
            count += 1
    print(f"Wrote/updated {count} BUILD.bazel files", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
