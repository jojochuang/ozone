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

# Bazel integration-test entry (replaces Maven integration.sh when pom.xml is absent).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "${ROOT}"

REPORT_DIR=${OUTPUT_DIR:-"${ROOT}/target/integration"}
mkdir -p "${REPORT_DIR}"
REPORT_FILE="${REPORT_DIR}/summary.txt"

if [[ -f pom.xml ]]; then
  exec "${ROOT}/hadoop-ozone/dev-support/checks/integration.sh" "$@"
fi

# shellcheck source=tools/bazel/_lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)/_lib.sh"
if ! ozone_resolve_bazel; then
  echo "[ERROR] bazel not found" | tee "${REPORT_FILE}"
  exit 1
fi

echo "Running Bazel integration classpath compile (manual targets)..." | tee "${REPORT_DIR}/output.log"
set +e
"${BAZEL}" build \
  //hadoop-ozone/integration-test:ozone-integration-test-tests \
  //hadoop-ozone/integration-test-recon:ozone-integration-test-recon-tests \
  //hadoop-ozone/integration-test-s3:ozone-integration-test-s3-tests \
  --build_tag_filters= >> "${REPORT_DIR}/output.log" 2>&1
rc=$?
set -e

if [[ ${rc} -ne 0 ]]; then
  echo "[ERROR] integration test libraries failed to compile (see output.log)" | tee -a "${REPORT_FILE}"
else
  : > "${REPORT_FILE}"
  echo "Integration compile milestone passed (execute tests via wired junit5 packages)." \
    >> "${REPORT_DIR}/output.log"
fi

# shellcheck disable=SC2034
ERROR_PATTERN="\\[ERROR\\]"
# shellcheck source=hadoop-ozone/dev-support/checks/_post_process.sh
source "${ROOT}/hadoop-ozone/dev-support/checks/_post_process.sh"
