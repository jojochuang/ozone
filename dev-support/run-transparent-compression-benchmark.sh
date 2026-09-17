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
set -o pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${DIR}/.." && pwd)"
BASELINE_COMMIT="${OZONE_BENCHMARK_BASELINE_COMMIT:-8a1ae0edbb6}"
WORKTREE="${OZONE_BENCHMARK_WORKTREE:-${ROOT}/../ozone-compression-baseline}"
BENCHMARK_PKG="hadoop-ozone/integration-test/src/test/java/org/apache/hadoop/ozone/benchmark"

MVN_TEST_ARGS=(
  -pl hadoop-ozone/integration-test -am test
  -Djacoco.skip=true
  -DfailIfNoTests=false
  -Dozone.run.benchmark=true
  -Dsurefire.fork.timeout=14400
  "-DargLine=-Xmx4g"
)

run_feature_benchmark() {
  local sha
  sha="$(git -C "${ROOT}" rev-parse HEAD)"
  echo "=== Feature branch compression benchmark (${sha}) ==="
  (
    cd "${ROOT}"
    mvn -pl hadoop-ozone/integration-test -am test-compile -Djacoco.skip=true -q
    for codec in NONE ZSTD SNAPPY LZ4 GZIP; do
      echo "--- Codec ${codec} ---"
      mvn "${MVN_TEST_ARGS[@]}" \
        -Dtest=TransparentCompressionWriteReadBenchmark \
        -Dozone.benchmark.git.sha="${sha}" \
        -Dozone.benchmark.codecs="${codec}" || exit 1
      merge_results
    done
  )
}

copy_benchmark_sources() {
  local dest_root="$1"
  mkdir -p "${dest_root}/${BENCHMARK_PKG}"
  cp -f "${ROOT}/${BENCHMARK_PKG}/"*.java "${dest_root}/${BENCHMARK_PKG}/"
}

run_baseline_benchmark() {
  echo "=== Pre-compression baseline at ${BASELINE_COMMIT} ==="
  if [[ ! -d "${WORKTREE}/.git" ]]; then
    git -C "${ROOT}" worktree add "${WORKTREE}" "${BASELINE_COMMIT}"
  fi
  copy_benchmark_sources "${WORKTREE}"
  (
    cd "${WORKTREE}"
    mvn -pl hadoop-ozone/integration-test -am test-compile -Djacoco.skip=true -q
    mvn "${MVN_TEST_ARGS[@]}" \
      -Dsurefire.fork.timeout=10800 \
      -Dtest=OzoneWriteReadBaselineBenchmark \
      -Dozone.benchmark.git.sha="${BASELINE_COMMIT}" || exit 1
  )
  mkdir -p "${ROOT}/hadoop-ozone/integration-test/target/compression-benchmark"
  cp -f "${WORKTREE}/hadoop-ozone/integration-test/target/compression-benchmark/"results-*.json \
    "${ROOT}/hadoop-ozone/integration-test/target/compression-benchmark/" 2>/dev/null || true
}

merge_results() {
  echo "=== Generating REPORT.md ==="
  python3 "${DIR}/generate-compression-benchmark-report.py"
}

run_feature_benchmark
run_baseline_benchmark
merge_results

echo "Done. See dev-support/compression-benchmark/REPORT.md"
