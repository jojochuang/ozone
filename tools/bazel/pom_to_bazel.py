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
"""Map Maven module coordinates and dependencies to Bazel labels."""

# pylint: disable=missing-function-docstring,line-too-long,duplicate-code

from __future__ import annotations

import re
from pathlib import Path

from defusedxml import ElementTree as ET

from pom_xml import parse

ROOT = Path(__file__).resolve().parents[2]
NS = {"m": "http://maven.apache.org/POM/4.0.0"}


def maven_label(group: str, artifact: str) -> str:
    key = re.sub(r"[^A-Za-z0-9]", "_", group + "_" + artifact)
    return "@maven//:" + key


def load_properties(pom_root: ET.Element) -> dict[str, str]:
    props: dict[str, str] = {}
    block = pom_root.find("m:properties", NS)
    if block is None:
        return props
    for child in block:
        tag = child.tag.split("}", 1)[-1]
        props[tag] = (child.text or "").strip()
    return props


def resolve(version: str, props: dict[str, str], depth: int = 0) -> str:
    if depth > 10 or not version.startswith("${"):
        return version
    return resolve(props.get(version[2:-1], version), props, depth + 1)


def scan_modules() -> dict[tuple[str, str], Path]:
    """(groupId, artifactId) -> pom directory."""
    index: dict[tuple[str, str], Path] = {}
    for pom in ROOT.glob("**/pom.xml"):
        if "target" in pom.parts:
            continue
        root = parse(pom).getroot()
        g = root.find("m:groupId", NS)
        a = root.find("m:artifactId", NS)
        if g is None or a is None:
            parent = root.find("m:parent", NS)
            if parent is not None:
                g = parent.find("m:groupId", NS)
                a = root.find("m:artifactId", NS)
        if g is None or a is None:
            continue
        group = (g.text or "").strip()
        artifact = (a.text or "").strip()
        if group == "org.apache.ozone":
            index[(group, artifact)] = pom.parent
    return index


def module_target(pom_dir: Path, _modules: dict[tuple[str, str], Path]) -> str:
    root = parse(pom_dir / "pom.xml").getroot()
    aid = (root.find("m:artifactId", NS).text or "").strip()
    rel = pom_dir.relative_to(ROOT).as_posix()
    return f"//{rel}:{aid}"


def pom_dependencies(pom_dir: Path, props: dict[str, str]) -> list[tuple[str, str, str, str]]:
    """Returns list of (groupId, artifactId, scope, type)."""
    root = parse(pom_dir / "pom.xml").getroot()
    if not props:
        props = load_properties(root)
    deps: list[tuple[str, str, str, str]] = []
    for dep in root.findall("m:dependencies/m:dependency", NS):
        g = dep.find("m:groupId", NS)
        a = dep.find("m:artifactId", NS)
        if g is None or a is None:
            continue
        scope_el = dep.find("m:scope", NS)
        scope = (scope_el.text or "compile").strip() if scope_el is not None else "compile"
        dep_type = dep.find("m:type", NS)
        dtype = (dep_type.text or "jar").strip() if dep_type is not None else "jar"
        if dtype in ("pom",):
            continue
        if scope in ("runtime",):
            # Runtime-only jars are not needed for javac (e.g. slf4j binding).
            continue
        deps.append(((g.text or "").strip(), (a.text or "").strip(), scope, dtype))
    return deps


def _module_has_tests(pom_dir: Path) -> bool:
    return (pom_dir / "src/test/java").is_dir()


SKIP_OZONE_ARTIFACTS = {
    "hdds-docs",  # Hugo/static site resources packaged as filegroup, not JavaInfo
}

# Maven optional/provided shaded jars are unpacked at package time; compile against the common module.
SHADED_ARTIFACT_ALIASES = {
    "ozone-filesystem-shaded": "ozone-filesystem-common",
}


def dep_to_label(
    group: str,
    artifact: str,
    scope: str,
    dtype: str,
    modules: dict[tuple[str, str], Path],
) -> str | None:
    if artifact in SKIP_OZONE_ARTIFACTS:
        return None
    if group == "org.apache.ozone":
        artifact = SHADED_ARTIFACT_ALIASES.get(artifact, artifact)
    if group == "org.apache.ozone" and (group, artifact) in modules:
        pom_dir = modules[(group, artifact)]
        root = parse(pom_dir / "pom.xml").getroot()
        aid = (root.find("m:artifactId", NS).text or "").strip()
        rel = pom_dir.relative_to(ROOT).as_posix()
        if dtype == "test-jar" and _module_has_tests(pom_dir):
            return f"//{rel}:{aid}-tests"
        if dtype == "test-jar":
            return f"//{rel}:{aid}"
        if scope == "test" and _module_has_tests(pom_dir):
            return f"//{rel}:{aid}-tests"
        return f"//{rel}:{aid}"
    return maven_label(group, artifact)
