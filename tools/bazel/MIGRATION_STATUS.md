# Bazel migration status

| Phase | Status |
| ----- | ------ |
| OEP draft | [bazel-build-migration.md](../../hadoop-hdds/docs/content/design/bazel-build-migration.md) |
| Maven `maven_install` BOM | `MODULE.bazel` (regenerate via `tools/bazel/generate_module_bazel.py`) |
| HDDS spike targets | `//hadoop-hdds/annotations`, `//hadoop-hdds/config`, `//hadoop-hdds/interface-client` |
| Full module graph | Stub `BUILD.bazel` per module (`tools/bazel/generate_build_files.py`) |
| OM AspectJ | `tools/bazel/aspectj.bzl` (manual targets; finish classpath wiring) |
| Dist / release | `//hadoop-ozone/dist:ozone-dist` (layout stub) |
| CI | `.github/workflows/bazel.yml` |

## Green commands (local)

```bash
bazel test //hadoop-hdds/config:TestConfigurationReflectionUtil
bazel build //hadoop-hdds/interface-client:hdds-interface-client
bazel build //hadoop-hdds/annotations:hdds-annotation-processing
```

## Maven removal gate

Do **not** delete `pom.xml` until:

1. `bazel test //...` matches Maven CI scope, and
2. One RC is voted on the ASF list using Bazel-built artifacts.

Maven remains the release path until then.
