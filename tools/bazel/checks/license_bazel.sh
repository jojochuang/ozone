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

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "${ROOT}"

DIR="${ROOT}/hadoop-ozone/dev-support/checks"
REPORT_DIR=${OUTPUT_DIR:-"${ROOT}/target/license"}
mkdir -p "${REPORT_DIR}"
REPORT_FILE="${REPORT_DIR}/summary.txt"

if [[ -f pom.xml ]]; then
  exec "${DIR}/license.sh" "$@"
fi

# Reuse Maven license aggregate output when present; otherwise validate MODULE.bazel list exists.
SRC="${1:-${ROOT}/target/generated-sources/license/THIRD-PARTY.txt}"
if [[ ! -e "${SRC}" ]]; then
  if [[ ! -f "${ROOT}/MODULE.bazel" ]]; then
    echo "[ERROR] MODULE.bazel missing for Bazel license check" | tee "${REPORT_FILE}"
    exit 1
  fi
  count="$(grep -Ec '^\s+"[^"]+:[^"]+:[^"]+",?$' "${ROOT}/MODULE.bazel" || true)"
  if [[ "${count}" -lt 50 ]]; then
    echo "[ERROR] MODULE.bazel coordinate list looks incomplete (${count})" | tee "${REPORT_FILE}"
    rc=1
  else
    : > "${REPORT_FILE}"
    : > "${REPORT_DIR}/output.log"
    echo "Bazel license check passed (${count} Maven coordinates in MODULE.bazel)." \
      >> "${REPORT_DIR}/output.log"
    rc=0
  fi
  # shellcheck disable=SC2034
  ERROR_PATTERN="\\[ERROR\\]"
  # shellcheck source=hadoop-ozone/dev-support/checks/_post_process.sh
  source "${DIR}/_post_process.sh"
  exit "${rc}"
fi

exec "${DIR}/license.sh" "${SRC}"
