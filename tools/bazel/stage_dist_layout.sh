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

# Materialize hadoop-ozone/dist/target/ozone-<version>/ from //hadoop-ozone/dist:ozone-dist
# so acceptance.sh and compose scripts match the Maven dist layout.

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

# shellcheck source=dev-support/ci/load_build_versions.sh
source dev-support/ci/load_build_versions.sh
OZONE_VERSION="$(load_build_version ozone.version)"

"${BAZEL}" build //hadoop-ozone/dist:ozone-dist --build_tag_filters=

TAR="${ROOT}/bazel-bin/hadoop-ozone/dist/ozone-dist.tar.gz"
if [[ ! -f "${TAR}" ]]; then
  echo "Missing ${TAR}" >&2
  exit 1
fi

DIST_ROOT="${ROOT}/hadoop-ozone/dist/target/ozone-${OZONE_VERSION}"
mkdir -p "${ROOT}/hadoop-ozone/dist/target"
rm -rf "${DIST_ROOT}"
mkdir -p "${DIST_ROOT}"
tar -xzf "${TAR}" -C "${DIST_ROOT}" --strip-components=1 2>/dev/null || tar -xzf "${TAR}" -C "${DIST_ROOT}"

echo "Staged dist at ${DIST_ROOT}"
