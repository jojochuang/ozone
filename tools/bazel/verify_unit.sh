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

# Run all non-manual java_test targets tagged unit (excludes integration modules).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT}"

if command -v bazel >/dev/null 2>&1; then
  BAZEL=bazel
elif [[ -x /tmp/bazelisk ]]; then
  BAZEL=/tmp/bazelisk
else
  echo "bazel not found" >&2
  exit 1
fi

ALLOWLIST="${ROOT}/tools/bazel/unit_ci_targets.txt"
FULL="${BAZEL_UNIT_FULL:-false}"

if [[ "${FULL}" == "true" ]]; then
  EXCLUDE='//hadoop-ozone/integration-test/... + //hadoop-ozone/integration-test-recon/... + //hadoop-ozone/integration-test-s3/... + //hadoop-ozone/fault-injection-test/...'
  QUERY="kind(\"java_test\", //hadoop-hdds/... + //hadoop-ozone/...) intersect attr(\"tags\", \"unit\", //hadoop-hdds/... + //hadoop-ozone/...) except attr(\"tags\", \"manual\", //hadoop-hdds/... + //hadoop-ozone/...) except ${EXCLUDE}"
  mapfile -t TARGETS < <("${BAZEL}" query "${QUERY}" 2>/dev/null || true)
else
  mapfile -t TARGETS < "${ALLOWLIST}"
fi

if [[ ${#TARGETS[@]} -eq 0 ]]; then
  echo "No unit java_test targets to run."
  exit 0
fi

echo "== Unit tests (${#TARGETS[@]} targets, FULL=${FULL}) =="
# shellcheck disable=SC2068
"${BAZEL}" test ${TARGETS[@]} \
  --build_tag_filters= \
  --test_output=errors \
  "${@}"
