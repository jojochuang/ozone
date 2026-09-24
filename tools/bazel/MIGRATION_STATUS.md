# Bazel migration status

| Phase | Status |
| ----- | ------ |
| OEP draft | [bazel-build-migration.md](../../hadoop-hdds/docs/content/design/bazel-build-migration.md) |
| Maven `maven_install` BOM | `MODULE.bazel` + `tools/bazel/maven_artifacts.bzl` |
| HDDS spike targets | `//hadoop-hdds/annotations`, `//hadoop-hdds/config`, `//hadoop-hdds/interface-client` |
| Full module graph | `BUILD.bazel` per module (hand-maintained + generated) |
| OM / AspectJ | `//hadoop-ozone/ozone-manager:ozone-manager` compiles without ajc weaving |
| Dist / release | `//hadoop-ozone/dist:ozone-dist` (core jars + dist tree; build with `--build_tag_filters=`) |
| CSI gRPC proto | `//hadoop-ozone/csi:ozone-csi` in default graph |
| Unit tests (Bazel) | JUnit5 package tests via `tools/bazel/junit5.bzl`; `verify_build.sh` runs non-manual `java_test` |
| CI | `.github/workflows/ci-bazel.yml` (Bazel verify + basic checks) |
| Maven build files | **Removed** (`pom.xml` tree, `.mvn/`, `maven-settings.xml`) |

## Green commands (local)

```bash
./tools/bazel/verify_build.sh
./hadoop-ozone/dev-support/checks/bazel.sh
bazel build //hadoop-ozone/ozone-manager:ozone-manager
```

Default builds use `--build_tag_filters=-manual` (see `.bazelrc`). Targets tagged `manual` pending codegen (Recon jOOQ, Iceberg Java 11, integration-test modules, etc.) are excluded from the default graph but can be built explicitly.

## Regenerating BUILD / Maven coords

Without `pom.xml`, edit `BUILD.bazel` and `maven_artifacts.bzl` directly. Optional sync from upstream Apache Ozone `pom.xml` on a branch: run `tools/bazel/generate_*.py` after temporarily restoring the root POM (not committed).

Versions for license/check tooling: `dev-support/build-versions.properties`.
