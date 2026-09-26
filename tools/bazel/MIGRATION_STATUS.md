# Bazel migration status

| Phase | Status |
| ----- | ------ |
| OEP draft | [bazel-build-migration.md](../../hadoop-hdds/docs/content/design/bazel-build-migration.md) |
| Maven `maven_install` BOM | `MODULE.bazel` + `tools/bazel/maven_artifacts.bzl` |
| Default compile graph | ~50 `java_library` targets (Recon, CLIs, Iceberg Java 11, …) |
| Recon jOOQ | `//hadoop-ozone/recon-codegen:recon-jooq-generated` (genrule + `recon-codegen-lib`) |
| Unit tests (wired) | `wire_junit5_packages.py` → `tags = ["unit", "manual"]`; promote per module |
| Dist / acceptance | `stage_dist_layout.sh` + `//hadoop-ozone/dist:ozone-dist` |
| CI | Gating: verify, basic, unit allowlist, milestones, dist, parity static/integration compile, recon UI. Informational (`continue-on-error`): full unit matrix, acceptance, kubernetes. |
| Maven build files | **Removed** |

## Green commands (local)

```bash
./tools/bazel/verify_build.sh
./tools/bazel/verify_extended.sh
./tools/bazel/stage_dist_layout.sh
RUN_BAZEL_ALL_UNIT=true ./tools/bazel/verify_extended.sh   # all wired unit packages (long)
```

Default builds use `--build_tag_filters=-manual`. Integration modules, mini-cluster, `*-tests` libraries, and wired unit
packages remain `manual` until compile/test deps are fixed.

## Still in progress

- **mini-cluster** / **multitenancy-ranger**: blocked on `*-tests` compile chains (e.g. HDDS framework tests).
- **Full unit parity**: drop `manual` from passing `ozone_junit5_package` rules module-by-module.
- **AspectJ OM**: `tools/bazel/aspectj.bzl` (ajc classpath); OM tests still `manual`.
- **Acceptance / integration execute**: classpath stubs exist; cluster test execution needs mini-cluster + dist parity.
- **Maven CI parity**: `findbugs_bazel.sh`; pmd/license/javadoc still Maven-oriented (`parity-static` job).
- **Release**: `tools/bazel/release/maven_staging.sh` placeholder; Recon UI via `recon_npm_build.sh`.
