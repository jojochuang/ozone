<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Transparent compression benchmark report

Generated from MiniOzone integration benchmarks (`-Dozone.run.benchmark=true`).

**Status:** Partial smoke run only (codec `NONE`, sizes 1KB and 1MB). Run `bash dev-support/run-transparent-compression-benchmark.sh` for the full matrix (all codecs, 1KB–100MB, pre-feature baseline).

## Run metadata

| Result file | Scenario | Git SHA | Timestamp |
|-------------|----------|---------|-----------|
| results-compression-codec-none-129c2e81e619-2026-09-17T04-57-01.900Z.json | compression-codec-none | 129c2e81e619ccf558e7d6cac230e49201f6c4be | 2026-09-17T04:57:01.900Z |

## Latency and throughput

| Scenario | Codec | Size | Op | Samples | Mean ms | Median ms | Mean MB/s | Median MB/s | Stored/Logical |
|----------|-------|------|----|---------|---------|-----------|-----------|-------------|----------------|
| compression-codec-none | NONE | 1KB | read | 15 | 3.89 | 3.49 | 0.25 | 0.28 | — |
| compression-codec-none | NONE | 1KB | write | 15 | 13.20 | 12.65 | 0.07 | 0.08 | — |
| compression-codec-none | NONE | 1MB | read | 15 | 154.18 | 153.19 | 6.49 | 6.53 | — |
| compression-codec-none | NONE | 1MB | write | 15 | 235.53 | 240.82 | 4.25 | 4.15 | — |

## Notes

- **BASELINE** / `pre-compression-baseline`: commit before transparent compression.
- **NONE**: compression feature enabled but bucket codec is NONE.
- Payload is deterministic compressible bytes (pattern `i % 251`).
- Default workload: 5 files per round, 1 warmup round, 3 measured rounds.
