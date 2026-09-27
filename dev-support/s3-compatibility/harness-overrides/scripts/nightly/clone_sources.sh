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

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)/common.sh"
nightly_load_state

if [[ -d "${OZONE_REPO}" ]]; then
  nightly_log "Staging Ozone from local directory ${OZONE_REPO} (ref ${OZONE_REF})"
  rm -rf "${WORK_DIR}/ozone"
  git clone --no-local "${OZONE_REPO}" "${WORK_DIR}/ozone"
  if [[ -n "${OZONE_REF}" && "${OZONE_REF}" != "HEAD" && "${OZONE_REF}" != "submodule" ]]; then
    if git -C "${WORK_DIR}/ozone" show-ref --verify --quiet "refs/heads/${OZONE_REF}"; then
      git -C "${WORK_DIR}/ozone" checkout --detach "${OZONE_REF}"
    elif git -C "${WORK_DIR}/ozone" rev-parse --verify --quiet "${OZONE_REF}^{commit}" >/dev/null; then
      git -C "${WORK_DIR}/ozone" checkout --detach "${OZONE_REF}"
    else
      nightly_log "Could not checkout Ozone ref ${OZONE_REF} from local directory"
      exit 1
    fi
  fi
else
  nightly_log "Cloning Ozone ${OZONE_REF}"
  nightly_clone_repo "${OZONE_REPO}" "${OZONE_REF}" "${WORK_DIR}/ozone"
fi
nightly_save_state OZONE_COMMIT "$(git -C "${WORK_DIR}/ozone" rev-parse HEAD)"

nightly_log "Staging s3-tests ${S3_TESTS_REF} from ${S3_TESTS_SOURCE}"
nightly_stage_repo "${S3_TESTS_SOURCE}" "${S3_TESTS_REF}" "${WORK_DIR}/s3-tests"
nightly_save_state S3_TESTS_COMMIT "$(git -C "${WORK_DIR}/s3-tests" rev-parse HEAD)"
nightly_log "Patching s3-tests cleanup for Ozone compatibility"
python3 "${ROOT_DIR}/scripts/patch_s3_tests_for_ozone.py" --repo "${WORK_DIR}/s3-tests"

nightly_log "Staging Mint ${MINT_REF} from ${MINT_SOURCE}"
nightly_stage_repo "${MINT_SOURCE}" "${MINT_REF}" "${WORK_DIR}/mint"
nightly_save_state MINT_COMMIT "$(git -C "${WORK_DIR}/mint" rev-parse HEAD)"
nightly_log "Patching Mint installers for Ozone compatibility"
python3 "${ROOT_DIR}/scripts/patch_mint_for_ozone.py" --repo "${WORK_DIR}/mint"
