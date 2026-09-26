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

# Extended Bazel verification: default graph + manual compile milestones + optional unit tag.

set -euo pipefail

# shellcheck source=tools/bazel/_lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_lib.sh"
ROOT="${OZONE_REPO_ROOT}"
cd "${ROOT}"

"${ROOT}/tools/bazel/verify_build.sh"

ozone_resolve_bazel

echo "== Manual compile milestones (Recon, Iceberg, CLIs) =="
MILESTONE_TARGETS=(
  "//hadoop-ozone/recon:ozone-recon"
  "//hadoop-ozone/iceberg:ozone-iceberg"
  "//hadoop-ozone/cli-debug:ozone-cli-debug"
  "//hadoop-ozone/cli-repair:ozone-cli-repair"
  "//hadoop-ozone/cli-interactive:ozone-cli-interactive"
)
# shellcheck disable=SC2068
"${BAZEL}" build ${MILESTONE_TARGETS[@]} --build_tag_filters=

if [[ "${RUN_BAZEL_ALL_UNIT:-false}" == "true" ]]; then
  echo "== Unit tests tagged unit (includes manual-tagged wired packages) =="
  UNIT_TARGETS="$("${BAZEL}" query 'attr("tags", "unit", //hadoop-hdds/... + //hadoop-ozone/...)')"
  if [[ -n "${UNIT_TARGETS}" ]]; then
    # shellcheck disable=SC2086
    "${BAZEL}" test ${UNIT_TARGETS} --build_tag_filters= --test_tag_filters=unit --test_output=errors
  fi
fi

echo "Extended verification completed."
