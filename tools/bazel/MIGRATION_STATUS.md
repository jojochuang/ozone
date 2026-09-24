# Bazel migration status

| Phase | Status |
| ----- | ------ |
| OEP draft | [bazel-build-migration.md](../../hadoop-hdds/docs/content/design/bazel-build-migration.md) |
| Maven `maven_install` BOM | `MODULE.bazel` (regenerate via `tools/bazel/generate_module_bazel.py`) |
| HDDS spike targets | `//hadoop-hdds/annotations`, `//hadoop-hdds/config`, `//hadoop-hdds/interface-client` |
| Full module graph | `BUILD.bazel` per module (`tools/bazel/generate_build_files.py`) |
| OM / AspectJ | `//hadoop-ozone/ozone-manager:ozone-manager` compiles without ajc weaving |
| Dist / release | `//hadoop-ozone/dist:ozone-dist` (layout stub) |
| CI | `.github/workflows/bazel.yml`, `hadoop-ozone/dev-support/checks/bazel.sh` |

## Green commands (local)

```bash
./tools/bazel/verify_build.sh
./hadoop-ozone/dev-support/checks/bazel.sh
bazel build //hadoop-ozone/ozone-manager:ozone-manager
```

Default builds use `--build_tag_filters=-manual` (see `.bazelrc`). Targets tagged `manual` pending codegen (Recon jOOQ, CSI protos, Iceberg Java 11, etc.) are excluded from the default graph but can be built explicitly.

## Maven removal gate

Do **not** delete `pom.xml` until:

1. `bazel test //...` matches Maven CI scope, and
2. One RC is voted on the ASF list using Bazel-built artifacts.

Maven remains the release path until then.
