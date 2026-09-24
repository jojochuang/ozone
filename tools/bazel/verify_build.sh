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

echo "== Spike tests =="
"${ROOT}/hadoop-ozone/dev-support/checks/bazel.sh"

echo "== Build default module graph (excludes manual-tagged targets) =="
TARGETS="$("${BAZEL}" query \
  'kind("java_library", //hadoop-hdds/... + //hadoop-ozone/...) except attr("tags", "manual", //hadoop-hdds/... + //hadoop-ozone/...)')"
"${BAZEL}" build ${TARGETS}

echo "All verification steps completed."
