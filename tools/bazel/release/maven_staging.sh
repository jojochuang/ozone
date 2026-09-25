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

# Placeholder: publish Bazel-built jars to Maven Central (GPG + staging repo).
# Phase 3 of bazel-build-migration.md — wire rules_jvm_external maven publishing
# or nexus-staging-maven-plugin against bazel-bin outputs.

set -euo pipefail

echo "Maven Central staging from Bazel is not automated yet." >&2
echo "Build release jars with: bazel build //hadoop-ozone/dist:ozone-dist --build_tag_filters=" >&2
exit 1
