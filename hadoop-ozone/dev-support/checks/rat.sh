#!/usr/bin/env bash
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

#checks:basic

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR/../../.." || exit 1

REPORT_DIR=${OUTPUT_DIR:-"$DIR/../../../target/rat"}
mkdir -p "$REPORT_DIR"

REPORT_FILE="$REPORT_DIR/summary.txt"
: > "${REPORT_DIR}/output.log"

declare -i rc=0
if [[ -f pom.xml ]]; then
  mvn -B --no-transfer-progress -fn org.apache.rat:apache-rat-plugin:check "$@" \
      | tee "${REPORT_DIR}/output.log"
  rc=${PIPESTATUS[0]}
else
  # shellcheck source=dev-support/ci/load_build_versions.sh
  source dev-support/ci/load_build_versions.sh
  RAT_VERSION="$(load_build_version apache-rat.version)"
  TOOL_DIR="${REPORT_DIR}/tools"
  mkdir -p "${TOOL_DIR}"
  RAT_JAR="${TOOL_DIR}/apache-rat-${RAT_VERSION}.jar"
  if [[ ! -f "${RAT_JAR}" ]]; then
    curl -fsSL -o "${RAT_JAR}" \
      "https://repo1.maven.org/maven2/org/apache/rat/apache-rat/${RAT_VERSION}/apache-rat-${RAT_VERSION}.jar"
  fi
  RAT_DIRS=(hadoop-hdds hadoop-ozone dev-support tools/bazel .github)
  for scan_dir in "${RAT_DIRS[@]}"; do
    [[ -d "${scan_dir}" ]] || continue
    set +e
    java -jar "${RAT_JAR}" --dir "${scan_dir}" >> "${REPORT_DIR}/output.log" 2>&1
    sub_rc=$?
    set -e
    if [[ ${sub_rc} -ne 0 ]]; then
      rc=${sub_rc}
    fi
  done
  for scan_file in MODULE.bazel BUILD.bazel .bazelrc; do
    [[ -f "${scan_file}" ]] || continue
    set +e
    java -jar "${RAT_JAR}" --dir "${scan_file}" >> "${REPORT_DIR}/output.log" 2>&1
    sub_rc=$?
    set -e
    if [[ ${sub_rc} -ne 0 ]]; then
      rc=${sub_rc}
    fi
  done
  FILTER="${REPORT_DIR}/filter_unapproved.py"
  cat > "${FILTER}" << 'PY'
import fnmatch
import re
import sys
from pathlib import Path

root = Path(".")
excl_path = root / "dev-support/rat/rat-exclusions.txt"
patterns: list[str] = []
for line in excl_path.read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if not line or line.startswith("#"):
        continue
    patterns.append(line)

log = Path(sys.argv[1]).read_text(encoding="utf-8", errors="replace")
in_section = False
unapproved: list[str] = []
for line in log.splitlines():
    if "Files with unapproved licenses" in line:
        in_section = True
        continue
    if in_section:
        if line.strip().startswith("*") or not line.strip():
            in_section = False
            continue
        m = re.match(r"\s+(.*\S)\s*$", line)
        if m:
            unapproved.append(m.group(1).lstrip("./"))

def excluded(path: str) -> bool:
    for pat in patterns:
        if fnmatch.fnmatch(path, pat) or fnmatch.fnmatch(path, pat.lstrip("/")):
            return True
        # Maven RAT paths are often repo-relative; also try without leading module prefix.
        if "/" in path:
            short = path.split("/", 1)[1] if path.count("/") else path
            if fnmatch.fnmatch(short, pat) or fnmatch.fnmatch(path, f"**/{pat}"):
                return True
    if fnmatch.fnmatch(path, "bazel-*/**") or "/target/" in path:
        return True
    return False

remaining = [p for p in unapproved if not excluded(p)]
if remaining:
    print("[ERROR] RAT unapproved files not covered by rat-exclusions.txt:", file=sys.stderr)
    for p in remaining[:50]:
        print(f"  {p}", file=sys.stderr)
    if len(remaining) > 50:
        print(f"  ... and {len(remaining) - 50} more", file=sys.stderr)
    sys.exit(1)
PY
  set +e
  python3 "${FILTER}" "${REPORT_DIR}/output.log" >> "${REPORT_DIR}/output.log" 2>&1
  filter_rc=$?
  set -e
  if [[ ${filter_rc} -ne 0 ]]; then
    rc=1
  fi
fi

grep -r --include=rat.txt "!????" $dirs 2>/dev/null | tee "$REPORT_FILE" || true

ERROR_PATTERN="\[ERROR\]"
source "${DIR}/_post_process.sh"
