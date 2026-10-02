<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Integration slow test classification (OM split baseline)

Derived from run [34769880972](https://github.com/apache/ozone/actions/runs/34769880972) `integration-om` artifact via
[`analyze_integration_artifacts.py`](analyze_integration_artifacts.py).

## Build vs test time (run 34769880972 — om)

| Signal | Value | Interpretation |
|--------|-------|----------------|
| Maven Total time | 01:10 h | Full reactor `verify` + clean |
| Sum of class elapsed | 75.1 min | Surefire-reported class times (sequential sum) |
| Est. non-test time | n/a (sum > total) | Class elapsed sums overlap across forks/nested classes; use log fraction instead |
| Log fraction before 1st test | 0.39 | Large prefix is reactor compile/package before Surefire |

**Remediation applied in this change:** reuse `ozone-repo` + `mvn test` (skip clean) on integration jobs to shrink the pre-test / packaging portion; parallel forks on `client` and `filesystem` splits.

## Top offenders — root cause tags

| Class | ~Seconds (CI) | Category | Notes |
|-------|---------------|----------|-------|
| `org.apache.hadoop.ozone.om.service.TestKeyLifecycleService` | 248 | Background services + parameterized bulk | Unit module; nested `@ParameterizedTest` × lifecycle service intervals |
| `org.apache.hadoop.ozone.om.TestAddRemoveOzoneManager` | 242 | HA mini-cluster | OM add/remove/decommission with HA cluster lifecycle |
| `org.apache.hadoop.ozone.om.TestOMRatisSnapshots` | 224 | HA mini-cluster + Ratis | Snapshot install/transfer on Ratis OM |
| `org.apache.hadoop.ozone.om.TestOzoneManagerHAFollowerReadWithStoppedNodes` | 201 | HA mini-cluster (stopped nodes) | **@Slow** — `TestOzoneManagerHAFollowerReadWithAllRunning` covers default CI |
| `org.apache.hadoop.ozone.om.TestKeyManagerImpl` | 200 | HA / OM metadata | Large integration surface around key manager |
| `org.apache.hadoop.ozone.om.TestOzoneManagerHAWithStoppedNodes` | 167 | HA mini-cluster (stopped nodes) | **@Slow** — `TestOzoneManagerHAWithAllRunning` in default CI |
| `org.apache.hadoop.ozone.om.TestOMRatisSnapshotTransfer` | 143 | HA mini-cluster + Ratis | Snapshot transfer paths |
| `org.apache.hadoop.ozone.om.TestOMUpgradeFinalization` | 89 | Upgrade/finalization | Layout upgrade and finalization |

## Method-level drilldown (top 5 OM classes)

CI artifacts upload `target/integration/` (log + summaries), not `surefire-reports/`. Reproduce locally with the same profile as CI, then rank methods:

| Class | Local `-Dtest=` | Module focus |
|-------|-----------------|--------------|
| `TestKeyLifecycleService` | `org.apache.hadoop.ozone.om.service.TestKeyLifecycleService` | `:ozone-manager` |
| `TestAddRemoveOzoneManager` | `org.apache.hadoop.ozone.om.TestAddRemoveOzoneManager` | `:ozone-integration-test` |
| `TestOMRatisSnapshots` | `org.apache.hadoop.ozone.om.TestOMRatisSnapshots` | `:ozone-integration-test` |
| `TestOzoneManagerHAFollowerReadWithStoppedNodes` | (now `@Slow`) | `:ozone-integration-test` |
| `TestKeyManagerImpl` | `org.apache.hadoop.ozone.om.TestKeyManagerImpl` | `:ozone-integration-test` |

```bash
# Example: slowest integration class from baseline
mvn -pl :ozone-integration-test -am test \
  -Dtest=org.apache.hadoop.ozone.om.TestOMRatisSnapshots \
  -Ptest-om -Phadoop-native-lib -Drocks_tools_native -DskipShade -DskipRecon -DskipDocs

python3 dev-support/ci/analyze_surefire_method_timing.py \
  --report-root . --class TestOMRatisSnapshots -n 20
```

For `TestKeyLifecycleService`, use `-pl :ozone-manager -am test` with the same `-Ptest-om` profile flags.
