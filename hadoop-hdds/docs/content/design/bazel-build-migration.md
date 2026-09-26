---
title: Bazel build migration
summary: Ozone Enhancement Proposal to migrate the project build from Maven to Bazel.
date: 2026-09-24
status: draft
author: Apache Ozone community
---
<!---
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

## Problem statement

Apache Ozone builds with Maven across ~55 modules, ~5k Java sources, and heavy
code generation (Protobuf, Picocli, jOOQ), optional native RocksDB tooling, and
Recon frontend packaging. Incremental compile and CI wall time remain costly even
with Develocity and selective GitHub Actions jobs.

We propose migrating to **Bazel** for hermetic, graph-based builds with remote
caching, while preserving ASF release quality and Maven Central coordinates during
a defined dual-build period.

## Goals

1. **Full cutover**: `bazel test //...` becomes the authoritative pre-merge gate;
   Maven is removed only after one release candidate is built and signed from Bazel.
2. **Parity**: Same runtime artifacts (dist layout, Docker inputs, classifiers) and
   the same `org.apache.ozone` GAV coordinates on Maven Central.
3. **Measured speedup**: Document cold/warm build times vs `mvn -pl … -am` before
   dropping Maven.

## Non-goals

* Rewriting service logic or RPC protocols unrelated to the build.
* Changing Ozone feature behavior (except build-driven codegen ordering fixes).
* Mandating Bazel for downstream Hadoop stacks that consume Ozone jars.

## Proposal

### Dual-build policy

| Phase | Maven | Bazel |
| ----- | ----- | ----- |
| 0 (spike) | Required for release/CI | Optional `bazel test` on migrated targets |
| 1 (expand) | Required for release | Required for PR CI on migrated packages |
| 2 (RC) | Fallback only | Builds signed RC artifacts |
| 3 (cutover) | Removed | Sole build system |

### Dependency management

External libraries are pinned via `rules_jvm_external` (`maven_install`) generated
from the root [`pom.xml`](../../../../pom.xml) BOM. Internal modules use
`//hadoop-hdds/...` and `//hadoop-ozone/...` targets.

### Code generation

* **Protobuf / gRPC / Ratis**: `rules_proto` plus a Ratis third-party rewrite step
  matching today's `protobuf-maven-plugin` and Ant replace tokens.
* **Picocli / HDDS config / annotation processors**: `java_plugin` targets.
* **Recon jOOQ**: codegen `java_binary` with declared outputs.
* **AspectJ (OM only)**: custom `aspectj_library` rule wrapping `ajc`.

### Release and Maven Central

Release managers stage artifacts with Bazel publishing rules (or a thin signing
wrapper) that emit POM + jar + `-sources` + `-tests` classifiers identical to
today's Maven release plugin output. CycloneDX SBOM generation moves to a Bazel
action in the `dist` profile.

### CI

GitHub Actions runs [`bazel.yml`](../../../../.github/workflows/bazel.yml) with
remote cache (optional `BAZEL_REMOTE_CACHE` secret). Existing Maven workflows
remain until phase 2.

### Success metrics

* Spike: `bazel test //hadoop-hdds/config/...` green on JDK 21+.
* Phase 1: all HDDS + Ozone library modules compile under Bazel.
* Phase 2: integration-test shards pass; dist tarball byte-compared to Maven dist.
* Phase 3: one ASF vote on a Bazel-built RC; Maven `pom.xml` tree removed.

## Risks and mitigations

| Risk | Mitigation |
| ---- | ---------- |
| Contributor onboarding | Document Bazel quick start in [`tools/bazel/README.md`](../../../../tools/bazel/README.md) |
| Hadoop classpath drift | Lockfile + integration tests |
| Dual-build cost | Time-box phases; automate BUILD generation from pom |
| CI cache cost | Optional remote cache; local disk cache by default |

## Alternatives considered

* **Maven + Develocity only**: lower cost; may suffice if warm builds meet SLO.
* **Gradle**: weaker hermetic guarantees; less common in ASF Java.
* **Hybrid forever**: doubles maintenance; rejected as end state.

## Implementation tracking

See [`tools/bazel/MIGRATION_STATUS.md`](../../../../tools/bazel/MIGRATION_STATUS.md)
for per-module status and blockers.
