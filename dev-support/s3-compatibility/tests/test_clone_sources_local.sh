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

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../../.." && pwd)"
harness="/tmp/ozone-s3-compatibility"
override="${repo_root}/dev-support/s3-compatibility/harness-overrides/scripts/nightly/clone_sources.sh"

if [[ ! -d "${harness}/scripts/nightly" ]]; then
  echo "Skip: clone ${harness} first to run this test" >&2
  exit 0
fi

cp "${override}" "${harness}/scripts/nightly/clone_sources.sh"
chmod +x "${harness}/scripts/nightly/clone_sources.sh"

work_dir="$(mktemp -d)"
output_root="$(mktemp -d)"
trap 'rm -rf "${work_dir}" "${output_root}"' EXIT

export ROOT_DIR="${harness}"
export OUTPUT_ROOT="${output_root}"
export WORK_DIR="${work_dir}"
export OZONE_REPO="${repo_root}"
export OZONE_REF="HEAD"
export RUN_ID="local-ozone-test"
export S3_TESTS_SOURCE="${harness}/s3-tests"
export S3_TESTS_REF="submodule"
export MINT_SOURCE="${harness}/mint"
export MINT_REF="submodule"

bash "${harness}/scripts/nightly/init.sh"
bash "${harness}/scripts/nightly/clone_sources.sh"

commit_in_work="$(git -C "${work_dir}/ozone" rev-parse HEAD)"
commit_expected="$(git -C "${repo_root}" rev-parse HEAD)"
if [[ "${commit_in_work}" != "${commit_expected}" ]]; then
  echo "Expected Ozone commit ${commit_expected}, got ${commit_in_work}" >&2
  exit 1
fi

echo "clone_sources local OZONE_REPO test passed"
