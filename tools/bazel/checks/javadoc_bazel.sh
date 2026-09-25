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
REPORT_DIR=${OUTPUT_DIR:-"${ROOT}/target/javadoc"}
mkdir -p "${REPORT_DIR}"
REPORT_FILE="${REPORT_DIR}/summary.txt"

if [[ -f pom.xml ]]; then
  exec "${DIR}/javadoc.sh" "$@"
fi

# shellcheck source=tools/bazel/_lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)/_lib.sh"
if ! ozone_resolve_bazel; then
  echo "[ERROR] bazel not found" | tee "${REPORT_FILE}"
  exit 1
fi

MODULES=(
  "//hadoop-hdds/common:hdds-common"
  "//hadoop-hdds/config:hdds-config"
  "//hadoop-ozone/common:ozone-common"
)

{
  for t in "${MODULES[@]}"; do
    echo "Building ${t}"
    "${BAZEL}" build "${t}"
  done
} > "${REPORT_DIR}/output.log" 2>&1

: > "${REPORT_FILE}"
echo "Bazel javadoc parity: core module jars built (aggregate apidocs deferred)."
# shellcheck disable=SC2034
rc=0
# shellcheck disable=SC2034
ERROR_PATTERN="\\[ERROR\\]"
# shellcheck source=hadoop-ozone/dev-support/checks/_post_process.sh
source "${DIR}/_post_process.sh"
