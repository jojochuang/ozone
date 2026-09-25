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

# Promote wired JUnit5 package tests: drop manual from unit-tagged java_test rules.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT}"

INTEGRATION_PREFIXES=(
  "hadoop-ozone/integration-test/"
  "hadoop-ozone/integration-test-recon/"
  "hadoop-ozone/integration-test-s3/"
  "hadoop-ozone/fault-injection-test/"
)

while IFS= read -r build; do
  skip=false
  for p in "${INTEGRATION_PREFIXES[@]}"; do
    if [[ "${build}" == *"${p}"* ]]; then
      skip=true
      break
    fi
  done
  if ${skip}; then
    continue
  fi
  if grep -q 'tags = \["unit", "manual"\]' "${build}"; then
    sed -i 's/tags = \["unit", "manual"\]/tags = ["unit"]/g' "${build}"
    echo "promoted unit tags in ${build}"
  fi
done < <(find hadoop-hdds hadoop-ozone -name BUILD.bazel | sort)
