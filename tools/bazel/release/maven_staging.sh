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

# Prepare a local Maven-style staging directory from Bazel-built jars (no Nexus upload).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
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
source "${ROOT}/dev-support/ci/load_build_versions.sh"
VERSION="$(load_build_version ozone.version)"
GROUP="org.apache.ozone"
STAGING="${ROOT}/target/maven-staging/${VERSION}"
rm -rf "${STAGING}"
mkdir -p "${STAGING}"

LIBS=(
  "//hadoop-hdds/common:hdds-common"
  "//hadoop-hdds/config:hdds-config"
  "//hadoop-ozone/common:ozone-common"
  "//hadoop-ozone/ozone-manager:ozone-manager"
  "//hadoop-ozone/client:ozone-client"
)

for label in "${LIBS[@]}"; do
  "${BAZEL}" build "${label}" --build_tag_filters= >/dev/null
  jar="$("${BAZEL}" cquery "${label}" --output=files 2>/dev/null | grep '\.jar$' | grep -v ijars | head -1)"
  artifact="${label##*:}"
  artifact="${artifact//_/-}"
  dest="${STAGING}/${GROUP//.//}/${artifact}/${VERSION}"
  mkdir -p "${dest}"
  cp "${ROOT}/${jar}" "${dest}/${artifact}-${VERSION}.jar"
  cat > "${dest}/${artifact}-${VERSION}.pom" <<EOF
<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>${GROUP}</groupId>
  <artifactId>${artifact}</artifactId>
  <version>${VERSION}</version>
  <packaging>jar</packaging>
</project>
EOF
done

echo "Staged ${#LIBS[@]} artifacts under ${STAGING}"
echo "Upload to Maven Central requires ASF credentials (not automated in CI)."
