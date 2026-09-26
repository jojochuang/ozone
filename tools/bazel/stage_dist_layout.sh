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

# shellcheck source=tools/bazel/_lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_lib.sh"
ROOT="${OZONE_REPO_ROOT}"
cd "${ROOT}"
ozone_resolve_bazel

# shellcheck source=dev-support/ci/load_build_versions.sh
source "${ROOT}/dev-support/ci/load_build_versions.sh"
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
cp -f "${ROOT}/hadoop-ozone/dist/src/shell/conf/shell-logging.properties" "${DIST_ROOT}/etc/hadoop/"
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
  done < <(find "${tree}" -type f \( -name '.env' -o -name '*.yaml' -o -name '*.yml' -o -name '*.conf' \
    -o -name 'docker-config' -o -name '*.sh' \) -print0)
}
_apply_dist_property_filters "${DIST_ROOT}/compose"
_apply_dist_property_filters "${DIST_ROOT}/kubernetes"
_apply_dist_property_filters "${DIST_ROOT}/smoketest"

# Bazel target|classpath artifact id (Maven build-classpath + dist-layout-stitching parity).
_CLASSPATH_SPECS=(
  "//hadoop-hdds/server-scm:hdds-server-scm|hdds-server-scm"
  "//hadoop-hdds/framework:hdds-server-framework|hdds-server-framework"
  "//hadoop-hdds/container-service:hdds-container-service|ozone-datanode"
  "//hadoop-ozone/ozone-manager:ozone-manager|ozone-manager"
  "//hadoop-ozone/s3gateway:ozone-s3gateway|ozone-s3gateway"
  "//hadoop-ozone/recon:ozone-recon|ozone-recon"
  "//hadoop-ozone/httpfsgateway:ozone-httpfsgateway|ozone-httpfsgateway"
  "//hadoop-ozone/cli-shell:ozone-cli-shell|ozone-cli-shell"
  "//hadoop-ozone/cli-admin:ozone-cli-admin|ozone-cli-admin"
  "//hadoop-ozone/cli-debug:ozone-cli-debug|ozone-cli-debug"
  "//hadoop-ozone/tools:ozone-tools|ozone-tools"
  "//hadoop-ozone/iceberg:ozone-iceberg|ozone-iceberg"
)

_RUNTIME_ROOTS=(
  "//hadoop-hdds/common:hdds-common"
  "//hadoop-hdds/config:hdds-config"
  "//hadoop-hdds/client:hdds-client"
  "//hadoop-hdds/server-scm:hdds-server-scm"
  "//hadoop-hdds/framework:hdds-server-framework"
  "//hadoop-hdds/container-service:hdds-container-service"
  "//hadoop-ozone/common:ozone-common"
  "//hadoop-ozone/client:ozone-client"
  "//hadoop-ozone/ozone-manager:ozone-manager"
  "//hadoop-ozone/recon:ozone-recon"
  "//hadoop-ozone/s3gateway:ozone-s3gateway"
  "//hadoop-ozone/httpfsgateway:ozone-httpfsgateway"
  "//hadoop-ozone/cli-shell:ozone-cli-shell"
  "//hadoop-ozone/cli-admin:ozone-cli-admin"
  "//hadoop-ozone/cli-debug:ozone-cli-debug"
  "//hadoop-ozone/tools:ozone-tools"
  "//hadoop-ozone/iceberg:ozone-iceberg"
)

_ozone_is_runtime_jar_path() {
  local rel="$1"
  [[ "${rel}" =~ \.jar$ ]] || return 1
  [[ "${rel}" =~ srcjar|ijars|/_javac/ ]] && return 1
  [[ "${rel}" =~ GenClass_deploy|JavaBuilder_deploy|JacocoCoverage|turbine_direct|jrt-fs\.jar ]] && return 1
  return 0
}

_ozone_module_jar_for_label() {
  local label="$1"
  local rel
  while IFS= read -r rel; do
    if _ozone_is_runtime_jar_path "${rel}"; then
      echo "${rel}"
      return 0
    fi
  done < <("${BAZEL}" cquery "${label}" --output=files 2>/dev/null)
}

_ozone_is_legacy_jersey1_jar_name() {
  local base="$1"
  [[ "${base}" =~ ^jersey-core-1\. ]] && return 0
  [[ "${base}" =~ ^jersey-server-1\. ]] && return 0
  [[ "${base}" =~ ^jersey-json-1\. ]] && return 0
  [[ "${base}" =~ ^jersey-servlet-1\. ]] && return 0
  [[ "${base}" =~ ^jsr311-api-.*\.jar$ ]] && return 0
  return 1
}

_ozone_filter_runtime_jar_paths() {
  local rel base
  while IFS= read -r rel; do
    if _ozone_is_runtime_jar_path "${rel}"; then
      base="$(basename "${rel}")"
      if _ozone_is_legacy_jersey1_jar_name "${base}"; then
        continue
      fi
      echo "${rel}"
    fi
  done
}

_ozone_collect_internal_module_labels() {
  local target="$1"
  "${BAZEL}" query "kind('java_library', deps(${target}))" --output=label 2>/dev/null \
    | grep -E '^//hadoop-(hdds|ozone)/' || true
}

_ozone_stage_module_jar() {
  local label="$1"
  local artifact_id="$2"
  local rel jar src dest
  jar="$(_ozone_module_jar_for_label "${label}")"
  if [[ -z "${jar}" ]]; then
    echo "WARNING: no jar output for ${label} (artifact ${artifact_id})" >&2
    return 0
  fi
  src="${EXEC_ROOT}/${jar}"
  dest="${artifact_id}-${HDDS_VERSION}.jar"
  if [[ -f "${src}" ]]; then
    cp -f "${src}" "${DIST_ROOT}/share/ozone/lib/${dest}"
    cp -f "${src}" "${DIST_ROOT}/lib/${dest}"
    _STAGED_MODULE_JARS["${label}"]="${dest}"
    local canonical="${label##*:}-${HDDS_VERSION}.jar"
    if [[ "${canonical}" != "${dest}" ]]; then
      cp -f "${src}" "${DIST_ROOT}/share/ozone/lib/${canonical}"
      cp -f "${src}" "${DIST_ROOT}/lib/${canonical}"
    fi
  fi
}

_ozone_write_classpath_descriptor() {
  local target="$1"
  local artifact_id="$2"
  local -a entries=()
  local label rel base art jar_name

  while IFS= read -r rel; do
    [[ -n "${rel}" ]] || continue
    base="$(basename "${rel}")"
    if _ozone_is_legacy_jersey1_jar_name "${base}"; then
      continue
    fi
    entries+=("${base}")
  done < <("${BAZEL}" cquery "filter('.*\\.jar$', deps(${target}))" --output=files 2>/dev/null \
    | _ozone_filter_runtime_jar_paths || true)

  while IFS= read -r label; do
    [[ -n "${label}" ]] || continue
    if [[ -n "${_STAGED_MODULE_JARS[${label}]+x}" ]]; then
      entries+=("${_STAGED_MODULE_JARS[${label}]}")
    else
      art="${label##*:}"
      jar_name="${art}-${HDDS_VERSION}.jar"
      if [[ -f "${DIST_ROOT}/share/ozone/lib/${jar_name}" ]]; then
        entries+=("${jar_name}")
      fi
    fi
  done < <(_ozone_collect_internal_module_labels "${target}")

  jar_name="${artifact_id}-${HDDS_VERSION}.jar"
  if [[ -f "${DIST_ROOT}/share/ozone/lib/${jar_name}" ]]; then
    entries+=("${jar_name}")
  fi

  mapfile -t entries < <(printf '%s\n' "${entries[@]}" | awk 'NF && !seen[$0]++' | sort)
  {
    printf 'classpath='
    local first=true e
    for e in "${entries[@]}"; do
      if [[ "${first}" == true ]]; then
        first=false
      else
        printf ':'
      fi
      printf '$HDDS_LIB_JARS_DIR/%s' "${e}"
    done
    printf '\n'
  } > "${DIST_ROOT}/share/ozone/classpath/${artifact_id}.classpath"
}

declare -A _STAGED_MODULE_JARS=()

"${BAZEL}" build "${_RUNTIME_ROOTS[@]}" --build_tag_filters=

_deps_query="${_RUNTIME_ROOTS[0]}"
for ((i = 1; i < ${#_RUNTIME_ROOTS[@]}; i++)); do
  _deps_query="${_deps_query} + ${_RUNTIME_ROOTS[$i]}"
done

EXEC_ROOT="$("${BAZEL}" info execution_root)"
mapfile -t _jar_files < <(
  "${BAZEL}" cquery "filter('.*\\.jar$', deps(${_deps_query}))" --output=files 2>/dev/null \
    | _ozone_filter_runtime_jar_paths \
    | sort -u || true
)

for jar in "${_jar_files[@]}"; do
  src="${EXEC_ROOT}/${jar}"
  if [[ -f "${src}" ]]; then
    cp -f "${src}" "${DIST_ROOT}/share/ozone/lib/"
    cp -f "${src}" "${DIST_ROOT}/lib/"
  fi
done

declare -A _module_labels=()
for spec in "${_CLASSPATH_SPECS[@]}"; do
  target="${spec%%|*}"
  while IFS= read -r label; do
    [[ -n "${label}" ]] || continue
    _module_labels["${label}"]=1
  done < <(_ozone_collect_internal_module_labels "${target}")
  _module_labels["${target}"]=1
done

mapfile -t _module_label_list < <(printf '%s\n' "${!_module_labels[@]}" | sort -u)
if ((${#_module_label_list[@]} > 0)); then
  "${BAZEL}" build "${_module_label_list[@]}" --build_tag_filters= >/dev/null
fi

for spec in "${_CLASSPATH_SPECS[@]}"; do
  target="${spec%%|*}"
  artifact_id="${spec##*|}"
  _ozone_stage_module_jar "${target}" "${artifact_id}"
done

for label in "${_module_label_list[@]}"; do
  art="${label##*:}"
  if [[ -z "${_STAGED_MODULE_JARS[${label}]+x}" ]]; then
    _ozone_stage_module_jar "${label}" "${art}"
  fi
done

for spec in "${_CLASSPATH_SPECS[@]}"; do
  _ozone_write_classpath_descriptor "${spec%%|*}" "${spec##*|}"
done

# Keep //hadoop-ozone/dist:ozone-dist buildable as a milestone (tar is not the staged layout source).
"${BAZEL}" build //hadoop-ozone/dist:ozone-dist --build_tag_filters= >/dev/null 2>&1 || true

_classpath_count="$(find "${DIST_ROOT}/share/ozone/classpath" -name '*.classpath' | wc -l)"
echo "Staged dist at ${DIST_ROOT} ($(find "${DIST_ROOT}/share/ozone/lib" -name '*.jar' | wc -l) jars, ${_classpath_count} classpath descriptors)"
if (("${_classpath_count}" < 1)); then
  echo "ERROR: no classpath descriptors were generated" >&2
  exit 1
fi
