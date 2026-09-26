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

# This script installs SpotBugs.
# Requires _install_tool from _lib.sh.  Use `source` for both scripts, because it modifies $PATH.

: ${SPOTBUGS_VERSION:=3.1.12}

_install_spotbugs() {
  local url archive attempt max_attempts=5
  url="https://repo.maven.apache.org/maven2/com/github/spotbugs/spotbugs/${SPOTBUGS_VERSION}/spotbugs-${SPOTBUGS_VERSION}.tgz"
  archive="$(mktemp)"
  for (( attempt=1; attempt<=max_attempts; attempt++ )); do
    if curl -fLSs --retry 3 --retry-delay 2 -o "${archive}" "${url}" \
        && [[ -s "${archive}" ]] \
        && gzip -t "${archive}" 2>/dev/null; then
      tar -xzf "${archive}"
      rm -f "${archive}"
      return 0
    fi
    echo "SpotBugs download failed or response was not a valid .tgz (attempt ${attempt}/${max_attempts})" >&2
    rm -f "${archive}"
    sleep $(( attempt * 2 ))
  done
  rm -f "${archive}"
  return 1
}

_install_tool spotbugs "spotbugs-${SPOTBUGS_VERSION}/bin"
