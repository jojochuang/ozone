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

# shellcheck source=tools/bazel/_lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_lib.sh"
ROOT="${OZONE_REPO_ROOT}"
cd "${ROOT}"
ozone_resolve_bazel

echo "== Spike tests =="
"${ROOT}/hadoop-ozone/dev-support/checks/bazel.sh"

echo "== Build default module graph (excludes manual-tagged targets) =="
mapfile -t TARGETS < <("${BAZEL}" query \
  'kind("java_library", //hadoop-hdds/... + //hadoop-ozone/...) except attr("tags", "manual", //hadoop-hdds/... + //hadoop-ozone/...)')
"${BAZEL}" build "${TARGETS[@]}"

echo "== Unit tests =="
if [[ "${RUN_BAZEL_ALL_UNIT:-false}" == "true" ]]; then
  "${ROOT}/tools/bazel/verify_unit.sh" --flaky_test_attempts=1
else
  DEFAULT_TESTS=(
    //hadoop-hdds/config:TestConfigurationReflectionUtil
    //hadoop-hdds/config:hdds-config-tests-pkg-org-apache-hadoop-hdds-conf
    //hadoop-hdds/common:hdds-common-unit-hdds-utils
    //hadoop-hdds/common:hdds-common-unit-ozone-common
  )
  # shellcheck disable=SC2068
  "${BAZEL}" test ${DEFAULT_TESTS[@]} --build_tag_filters= --test_output=errors
fi

echo "All verification steps completed."
