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

# Materialize hadoop-ozone/dist/target/ozone-<version>/ for compose/acceptance
# (Maven dist-layout-stitching parity using Bazel-built jars).

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
HDDS_VERSION="$(load_build_version hdds.version)"
DOCKER_HADOOP_IMAGE="$(load_build_version docker.hadoop.image)"
DOCKER_HADOOP_IMAGE_FLAVOR="$(load_build_version docker.hadoop.image.flavor)"
DOCKER_OZONE_RUNNER_VERSION="$(load_build_version docker.ozone-runner.version)"
DOCKER_OZONE_RUNNER_CLIENT_VERSION="$(load_build_version docker.ozone-runner.client.version)"
DOCKER_OZONE_IMAGE="$(load_build_version docker.ozone.image)"
DOCKER_OZONE_IMAGE_FLAVOR="$(load_build_version docker.ozone.image.flavor)"

DIST_ROOT="${ROOT}/hadoop-ozone/dist/target/ozone-${OZONE_VERSION}"
mkdir -p "${ROOT}/hadoop-ozone/dist/target"
rm -rf "${DIST_ROOT}"
mkdir -p "${DIST_ROOT}"

cp -p "${ROOT}/hadoop-ozone/dist/src/main/license/bin/NOTICE.txt" "${DIST_ROOT}/NOTICE.txt"
cp -p "${ROOT}/hadoop-ozone/dist/src/main/license/bin/LICENSE.txt" "${DIST_ROOT}/LICENSE.txt"
cp -pr "${ROOT}/hadoop-ozone/dist/src/main/license/bin/licenses" "${DIST_ROOT}/licenses"
cp -p "${ROOT}/README.md" "${DIST_ROOT}/"
cp -p "${ROOT}/HISTORY.md" "${DIST_ROOT}/"
cp -p "${ROOT}/SECURITY.md" "${DIST_ROOT}/"
cp -p "${ROOT}/CONTRIBUTING.md" "${DIST_ROOT}/"

mkdir -p "${DIST_ROOT}/share/ozone/classpath" "${DIST_ROOT}/share/ozone/lib" "${DIST_ROOT}/share/ozone/web"
mkdir -p "${DIST_ROOT}/bin" "${DIST_ROOT}/sbin" "${DIST_ROOT}/etc/hadoop" "${DIST_ROOT}/libexec"
mkdir -p "${DIST_ROOT}/log" "${DIST_ROOT}/temp" "${DIST_ROOT}/lib"
mkdir -p "${DIST_ROOT}/compose" "${DIST_ROOT}/smoketest" "${DIST_ROOT}/kubernetes"

cp -r "${ROOT}/hadoop-hdds/common/src/main/conf/." "${DIST_ROOT}/etc/hadoop/"
cp "${ROOT}/hadoop-ozone/dist/src/shell/conf/om-audit-log4j2.properties" "${DIST_ROOT}/etc/hadoop/"
cp "${ROOT}/hadoop-ozone/dist/src/shell/conf/dn-audit-log4j2.properties" "${DIST_ROOT}/etc/hadoop/"
cp "${ROOT}/hadoop-ozone/dist/src/shell/conf/dn-container-log4j2.properties" "${DIST_ROOT}/etc/hadoop/"
cp "${ROOT}/hadoop-ozone/dist/src/shell/conf/scm-audit-log4j2.properties" "${DIST_ROOT}/etc/hadoop/"
cp "${ROOT}/hadoop-ozone/dist/src/shell/conf/s3g-audit-log4j2.properties" "${DIST_ROOT}/etc/hadoop/"
cp "${ROOT}/hadoop-ozone/dist/src/shell/conf/ozone-site.xml" "${DIST_ROOT}/etc/hadoop/"
cp -f "${ROOT}/hadoop-ozone/dist/src/shell/conf/log4j.properties" "${DIST_ROOT}/etc/hadoop/"
cp "${ROOT}/hadoop-hdds/framework/src/main/resources/network-topology-default.xml" "${DIST_ROOT}/etc/hadoop/"
cp "${ROOT}/hadoop-hdds/framework/src/main/resources/network-topology-nodegroup.xml" "${DIST_ROOT}/etc/hadoop/"

cp -r "${ROOT}/hadoop-ozone/dist/src/main/dockerlibexec/." "${DIST_ROOT}/libexec/"
cp "${ROOT}/hadoop-ozone/dist/src/shell/ozone/ozone" "${DIST_ROOT}/bin/"
chmod 755 "${DIST_ROOT}/bin/ozone"
cp "${ROOT}/hadoop-ozone/dist/src/shell/ozone/ozone-config.sh" "${DIST_ROOT}/libexec/"
cp "${ROOT}/hadoop-ozone/dist/src/shell/ozone/ozone-functions.sh" "${DIST_ROOT}/libexec/"
cp -r "${ROOT}/hadoop-ozone/dist/src/shell/shellprofile.d" "${DIST_ROOT}/libexec/"
cp -r "${ROOT}/hadoop-ozone/dist/src/shell/upgrade" "${DIST_ROOT}/libexec/"
cp "${ROOT}/hadoop-ozone/dist/src/shell/hdds/hadoop-daemons.sh" "${DIST_ROOT}/sbin/"
cp "${ROOT}/hadoop-ozone/dist/src/shell/hdds/workers.sh" "${DIST_ROOT}/sbin/"
cp "${ROOT}/hadoop-ozone/dist/src/shell/ozone/start-ozone.sh" "${DIST_ROOT}/sbin/"
cp "${ROOT}/hadoop-ozone/dist/src/shell/ozone/stop-ozone.sh" "${DIST_ROOT}/sbin/"
chmod 755 "${DIST_ROOT}/sbin/"*.sh

cp -r "${ROOT}/dev-support/byteman" "${DIST_ROOT}/share/ozone/"

cp -a "${ROOT}/hadoop-ozone/dist/src/main/compose/." "${DIST_ROOT}/compose/"
cp -a "${ROOT}/hadoop-ozone/dist/src/main/smoketest/." "${DIST_ROOT}/smoketest/"
if [[ -d "${ROOT}/hadoop-ozone/dist/src/main/k8s" ]]; then
  cp -a "${ROOT}/hadoop-ozone/dist/src/main/k8s/." "${DIST_ROOT}/kubernetes/"
fi
mkdir -p "${DIST_ROOT}/compose/_keytabs"
find "${DIST_ROOT}/compose" "${DIST_ROOT}/kubernetes" -name "*.sh" -exec chmod 755 {} \; 2>/dev/null || true

_apply_dist_property_filters() {
  local tree="$1"
  while IFS= read -r -d '' f; do
    sed -i \
      -e "s#\${hdds.version}#${HDDS_VERSION}#g" \
      -e "s#\${ozone.version}#${OZONE_VERSION}#g" \
      -e "s#\${project.version}#${OZONE_VERSION}#g" \
      -e "s#\${docker.hadoop.image}#${DOCKER_HADOOP_IMAGE}#g" \
      -e "s#\${docker.hadoop.image.flavor}#${DOCKER_HADOOP_IMAGE_FLAVOR}#g" \
      -e "s#\${docker.ozone-runner.version}#${DOCKER_OZONE_RUNNER_VERSION}#g" \
      -e "s#\${docker.ozone-runner.client.version}#${DOCKER_OZONE_RUNNER_CLIENT_VERSION}#g" \
      -e "s#\${docker.ozone.image}#${DOCKER_OZONE_IMAGE}#g" \
      -e "s#\${docker.ozone.image.flavor}#${DOCKER_OZONE_IMAGE_FLAVOR}#g" \
      -e "s#@project.version@#${OZONE_VERSION}#g" \
      "${f}"
  done < <(find "${tree}" -type f \( -name '.env' -o -name '*.yaml' -o -name '*.yml' -o -name '*.conf' -o -name 'docker-config' \) -print0)
}
_apply_dist_property_filters "${DIST_ROOT}/compose"
_apply_dist_property_filters "${DIST_ROOT}/kubernetes"

_RUNTIME_ROOTS=(
  "//hadoop-hdds/common:hdds-common"
  "//hadoop-hdds/config:hdds-config"
  "//hadoop-hdds/server-scm:hdds-server-scm"
  "//hadoop-hdds/container-service:hdds-container-service"
  "//hadoop-ozone/common:ozone-common"
  "//hadoop-ozone/client:ozone-client"
  "//hadoop-ozone/ozone-manager:ozone-manager"
  "//hadoop-ozone/datanode:ozone-datanode"
  "//hadoop-ozone/recon:ozone-recon"
  "//hadoop-ozone/s3gateway:ozone-s3gateway"
  "//hadoop-ozone/cli-shell:ozone-cli-shell"
  "//hadoop-ozone/cli-admin:ozone-cli-admin"
  "//hadoop-ozone/cli-debug:ozone-cli-debug"
  "//hadoop-ozone/tools:ozone-tools"
  "//hadoop-ozone/iceberg:ozone-iceberg"
)

"${BAZEL}" build "${_RUNTIME_ROOTS[@]}" --build_tag_filters=

_deps_query="${_RUNTIME_ROOTS[0]}"
for ((i = 1; i < ${#_RUNTIME_ROOTS[@]}; i++)); do
  _deps_query="${_deps_query} + ${_RUNTIME_ROOTS[$i]}"
done

EXEC_ROOT="$("${BAZEL}" info execution_root)"
mapfile -t _jar_files < <(
  "${BAZEL}" cquery "filter('.*\\.jar$', deps(${_deps_query}))" --output=files 2>/dev/null \
    | sort -u \
    | grep -E '\.jar$' \
    | grep -v ijars \
    | grep -v srcjar \
    | grep -v '/_javac/' || true
)

for jar in "${_jar_files[@]}"; do
  src="${EXEC_ROOT}/${jar}"
  if [[ -f "${src}" ]]; then
    cp -f "${src}" "${DIST_ROOT}/share/ozone/lib/"
    cp -f "${src}" "${DIST_ROOT}/lib/"
  fi
done

# Keep //hadoop-ozone/dist:ozone-dist buildable as a milestone (tar is not the staged layout source).
"${BAZEL}" build //hadoop-ozone/dist:ozone-dist --build_tag_filters= >/dev/null 2>&1 || true

echo "Staged dist at ${DIST_ROOT} ($(find "${DIST_ROOT}/share/ozone/lib" -name '*.jar' | wc -l) jars)"
