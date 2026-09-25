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

# Retry a command when Bazel module downloads fail transiently in CI.

set -euo pipefail

if [[ $# -lt 3 ]]; then
  echo "Usage: $0 <max_attempts> <delay_seconds> <command...>" >&2
  exit 2
fi

max_attempts="$1"
delay_seconds="$2"
shift 2

attempt=1
while (( attempt <= max_attempts )); do
  if "$@"; then
    exit 0
  fi
  if (( attempt >= max_attempts )); then
    break
  fi
  echo "Command failed (attempt ${attempt}/${max_attempts}); retrying in ${delay_seconds}s..." >&2
  sleep "${delay_seconds}"
  (( attempt++ ))
done
exit 1
