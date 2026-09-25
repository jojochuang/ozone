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

#checks:basic

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR/../../.." || exit 1

BASE_DIR="$(pwd -P)"
REPORT_DIR=${OUTPUT_DIR:-"$DIR/../../../target/checkstyle"}
mkdir -p "$REPORT_DIR"
REPORT_FILE="$REPORT_DIR/summary.txt"
OUTPUT_XML="${REPORT_DIR}/checkstyle-errors.xml"

declare -i rc=0

if [[ -f pom.xml ]]; then
  MAVEN_OPTIONS='-B -fae -DskipDocs -DskipRecon -Dcheckstyle.failOnViolation=false --no-transfer-progress'
  mvn ${MAVEN_OPTIONS} checkstyle:check > "${REPORT_DIR}/output.log"
  rc=$?
  if [[ ${rc} -ne 0 ]]; then
    mvn ${MAVEN_OPTIONS} clean test-compile checkstyle:check > output.log
    rc=$?
    mkdir -p "$REPORT_DIR"
    mv output.log "${REPORT_DIR}"/
  fi
else
  # shellcheck source=dev-support/ci/load_build_versions.sh
  source dev-support/ci/load_build_versions.sh
  CS_VERSION="$(load_build_version checkstyle.version)"
  TOOL_DIR="${REPORT_DIR}/tools"
  mkdir -p "${TOOL_DIR}"
  CS_JAR="${TOOL_DIR}/checkstyle-${CS_VERSION}-all.jar"
  if [[ ! -f "${CS_JAR}" ]]; then
    curl -fsSL -L -o "${CS_JAR}" \
      "https://github.com/checkstyle/checkstyle/releases/download/checkstyle-${CS_VERSION}/checkstyle-${CS_VERSION}-all.jar"
  fi
  mapfile -t CS_FILES < <(find hadoop-hdds hadoop-ozone tools/bazel -type f \
    \( -path '*/src/main/java/*.java' -o -path '*/src/test/java/*.java' \))
  if [[ ${#CS_FILES[@]} -eq 0 ]]; then
    echo "No Java sources found" > "${REPORT_DIR}/output.log"
    rc=1
  else
    set +e
    java -jar "${CS_JAR}" \
      -c "hadoop-hdds/dev-support/checkstyle/checkstyle.xml" \
      -f xml -o "${OUTPUT_XML}" \
      "${CS_FILES[@]}" \
      > "${REPORT_DIR}/output.log" 2>&1
    cs_rc=$?
    set -e
    if [[ ${cs_rc} -ne 0 ]] && [[ ! -s "${OUTPUT_XML}" ]]; then
      echo "[ERROR] Checkstyle failed before producing ${OUTPUT_XML} (exit ${cs_rc})" \
        >> "${REPORT_DIR}/output.log"
      rc=1
    fi
  fi
fi

cat "${REPORT_DIR}/output.log"

find "${REPORT_DIR}" -name checkstyle-errors.xml -print0 2>/dev/null \
  | xargs -0 sed '$!N; /<file.*\n<\/file/d;P;D' 2>/dev/null \
  | sed \
      -e '/<?xml.*>/d' \
      -e '/<checkstyle.*/d' \
      -e '/<\/.*/d' \
      -e 's/<file name="\([^"]*\)".*/\1/' \
      -e 's/<error.*line="\([[:digit:]]*\)".*message="\([^"]*\)".*/ \1: \2/' \
      -e "s!^${BASE_DIR}/!!" \
      -e "s/&apos;/'/g" \
      -e "s/&lt;/</g" \
      -e "s/&gt;/>/g" \
  | tee "$REPORT_FILE"

grep -c ':' "$REPORT_FILE" > "$REPORT_DIR/failures" 2>/dev/null || echo 0 > "$REPORT_DIR/failures"

if [[ ! -f pom.xml ]]; then
  if [[ -s "$REPORT_FILE" ]]; then
    echo "Checkstyle ends with $(grep -c ':' "$REPORT_FILE") errors."
    rc=1
  elif [[ ${rc} -eq 0 ]]; then
    echo "Checkstyle ends with 0 errors."
  fi
fi

# shellcheck disable=SC2034
ERROR_PATTERN="\[ERROR\]"
# shellcheck source=./_post_process.sh
source "${DIR}/_post_process.sh"
