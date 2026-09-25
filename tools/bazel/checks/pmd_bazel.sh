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
REPORT_DIR=${OUTPUT_DIR:-"${ROOT}/target/pmd"}
mkdir -p "${REPORT_DIR}"
REPORT_FILE="${REPORT_DIR}/summary.txt"

if [[ -f pom.xml ]]; then
  exec "${DIR}/pmd.sh" "$@"
fi

PMD_BIN="${TOOLS_DIR:-${ROOT}/.dev-tools}/pmd/pmd-bin-7.7.0/bin/pmd"
if [[ ! -x "${PMD_BIN}" ]]; then
  mkdir -p "${ROOT}/.dev-tools/pmd"
  ZIP="${ROOT}/.dev-tools/pmd/pmd.zip"
  if [[ ! -f "${ZIP}" ]]; then
    curl -fsSL -o "${ZIP}" \
      "https://github.com/pmd/pmd/releases/download/pmd_releases%2F7.7.0/pmd-dist-7.7.0-bin.zip"
  fi
  unzip -qo "${ZIP}" -d "${ROOT}/.dev-tools/pmd"
fi

RULES="${ROOT}/dev-support/pmd/pmd-ruleset.xml"
SRC="${ROOT}/hadoop-hdds/common/src/main/java,${ROOT}/hadoop-hdds/config/src/main/java,${ROOT}/hadoop-ozone/common/src/main/java"

set +e
"${PMD_BIN}" check --dir "${SRC}" --rulesets "${RULES}" --format text \
  --no-cache --no-fail-on-violation > "${REPORT_DIR}/output.log" 2>&1
rc=$?
set -e

if [[ ${rc} -ne 0 ]]; then
  echo "[ERROR] PMD execution failed (see output.log)" | tee "${REPORT_FILE}"
else
  : > "${REPORT_FILE}"
  grep -i violation "${REPORT_DIR}/output.log" | head -25 >> "${REPORT_DIR}/output.log" || true
  rc=0
fi

ERROR_PATTERN="\\[ERROR\\]"
source "${DIR}/_post_process.sh"
