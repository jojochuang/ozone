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

if [[ -f pom.xml ]]; then
  exec "${ROOT}/hadoop-ozone/dev-support/checks/kubernetes.sh" "$@"
fi

REPORT_DIR=${OUTPUT_DIR:-"${ROOT}/target/kubernetes"}
mkdir -p "${REPORT_DIR}"
REPORT_FILE="${REPORT_DIR}/summary.txt"

chmod +x "${ROOT}/tools/bazel/stage_dist_layout.sh"
"${ROOT}/tools/bazel/stage_dist_layout.sh" >> "${REPORT_DIR}/output.log" 2>&1

# shellcheck source=dev-support/ci/load_build_versions.sh
source "${ROOT}/dev-support/ci/load_build_versions.sh"
OZONE_VERSION="$(load_build_version ozone.version)"
DIST_DIR="${ROOT}/hadoop-ozone/dist/target/ozone-${OZONE_VERSION}"

if [[ ! -d "${DIST_DIR}" ]]; then
  echo "[ERROR] dist dir missing: ${DIST_DIR}" | tee "${REPORT_FILE}"
  exit 1
fi

if [[ "${SKIP_K8S:-false}" == "true" || -z "${KUBECONFIG:-}" ]]; then
  echo "Kubernetes check skipped (no cluster): dist staged at ${DIST_DIR}" | tee "${REPORT_FILE}"
  exit 0
fi

exec "${ROOT}/hadoop-ozone/dev-support/checks/kubernetes.sh" "$@"
