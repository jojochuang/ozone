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

set -eo pipefail
HOME="${HOME:-/tmp}"
VERSION="1.77.1"
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"
case "${ARCH}" in
  x86_64) ARCH="x86_64" ;;
  aarch64|arm64) ARCH="aarch_64" ;;
  *) echo "unsupported arch ${ARCH}" >&2; exit 1 ;;
esac
CACHE="${OZONE_PROTOC_CACHE:-${HOME}/.cache/ozone-bazel-grpc}/${VERSION}"
BIN="${CACHE}/bin/protoc-gen-grpc-java"
if [[ ! -x "${BIN}" ]]; then
  mkdir -p "$(dirname "${BIN}")"
  URL="https://repo1.maven.org/maven2/io/grpc/protoc-gen-grpc-java/${VERSION}/protoc-gen-grpc-java-${VERSION}-${OS}-${ARCH}.exe"
  curl -fsSL "${URL}" -o "${BIN}"
  chmod +x "${BIN}"
fi
exec "${BIN}" "$@"
