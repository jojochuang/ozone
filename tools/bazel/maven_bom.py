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
"""Load Maven BOM version properties from the root pom.xml."""

# pylint: disable=missing-module-docstring,duplicate-code

from __future__ import annotations

from pathlib import Path

from pom_xml import parse

ROOT = Path(__file__).resolve().parents[2]
NS = {"m": "http://maven.apache.org/POM/4.0.0"}


def load_bom() -> dict[tuple[str, str], str]:
    """(groupId, artifactId) -> version string."""
    root = parse(ROOT / "pom.xml").getroot()
    props: dict[str, str] = {}
    block = root.find("m:properties", NS)
    if block is not None:
        for child in block:
            props[child.tag.split("}", 1)[-1]] = (child.text or "").strip()

    def resolve(v: str, depth: int = 0) -> str:
        if depth > 10 or not v.startswith("${"):
            return v
        return resolve(props.get(v[2:-1], v), depth + 1)

    bom: dict[tuple[str, str], str] = {}
    dm = root.find("m:dependencyManagement/m:dependencies", NS)
    if dm is None:
        return bom
    for dep in dm.findall("m:dependency", NS):
        g = dep.find("m:groupId", NS)
        a = dep.find("m:artifactId", NS)
        v = dep.find("m:version", NS)
        if g is None or a is None or v is None or not v.text:
            continue
        ver = resolve(v.text.strip())
        if ver and not ver.startswith("${"):
            bom[(g.text.strip(), a.text.strip())] = ver
    return bom
