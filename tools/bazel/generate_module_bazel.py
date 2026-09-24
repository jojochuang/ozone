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

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TOOLS = Path(__file__).resolve().parent


def _load_bzl_list(path: Path) -> list[str]:
    import re

    text = path.read_text(encoding="utf-8")
    return re.findall(r'"([^"]+:[^"]+:[^"]+)"', text)


def main() -> None:
    import generate_maven_artifacts

    generate_maven_artifacts.main()
    arts = sorted(
        set(_load_bzl_list(TOOLS / "maven_artifacts.bzl"))
        | set(_load_bzl_list(TOOLS / "extra_maven_artifacts.bzl"))
        | set(_load_bzl_list(TOOLS / "bom_imports.bzl"))
    )
    header = """# Licensed to the Apache Software Foundation (ASF) under one or more
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

# Artifact list: tools/bazel/generate_module_bazel.py

module(
    name = "ozone",
    version = "2.3.0-SNAPSHOT",
)

bazel_dep(name = "rules_java", version = "8.14.0")
bazel_dep(name = "rules_jvm_external", version = "6.7")
bazel_dep(name = "rules_proto", version = "7.1.0")
bazel_dep(name = "protobuf", version = "29.3")
bazel_dep(name = "rules_cc", version = "0.1.1")
bazel_dep(name = "bazel_skylib", version = "1.7.1")
bazel_dep(name = "rules_shell", version = "0.5.0")
bazel_dep(name = "rules_pkg", version = "1.0.1")

"""
    body = header + "\nmaven = use_extension(\"@rules_jvm_external//:extensions.bzl\", \"maven\")\n"
    body += "maven.install(\n    name = \"maven\",\n    artifacts = [\n"
    for art in arts:
        body += f'        "{art}",\n'
    body += """    ],
    repositories = [
        "https://repo1.maven.org/maven2",
    ],
    fail_if_repin_required = False,
)
use_repo(maven, "maven")
"""
    (ROOT / "MODULE.bazel").write_text(body, encoding="utf-8")
    print(f"MODULE.bazel written with {len(arts)} artifacts")


if __name__ == "__main__":
    main()
