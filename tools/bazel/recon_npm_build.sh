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

# Build Recon UI for inclusion in dist — matches Maven/pnpm workflow on apache/master.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
UI="${ROOT}/hadoop-ozone/recon/src/main/resources/webapps/recon/ozone-recon-web"
if [[ ! -f "${UI}/package.json" ]]; then
  echo "Recon UI package.json not found at ${UI}" >&2
  exit 1
fi

cd "${UI}"
if [[ -f pnpm-lock.yaml ]]; then
  corepack enable
  PNPM_VERSION="$(node -p "require('./package.json').packageManager?.split('@')[1] || '10.28.2'")"
  corepack prepare "pnpm@${PNPM_VERSION}" --activate
  pnpm install --frozen-lockfile
  pnpm run build
elif [[ -f package-lock.json ]]; then
  npm ci
  npm run build
else
  npm install --legacy-peer-deps
  npm run build
fi
