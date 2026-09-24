# Apache Ozone Bazel build

## Quick start

Install [Bazelisk](https://github.com/bazelbuild/bazelisk) and run:

```bash
./hadoop-ozone/dev-support/checks/bazel.sh
```

## Maintenance scripts

| Script | Purpose |
| ------ | ------- |
| `generate_maven_artifacts.py` | Sync external coordinates from root `pom.xml` |
| `generate_module_bazel.py` | Rewrite `MODULE.bazel` `maven.install` artifact list |
| `generate_build_files.py` | Create stub `BUILD.bazel` for each Maven module |

## Maven Central staging (release phase)

Release managers will use `rules_jvm_external` publish rules once the module
graph is complete. Until then, continue ASF releases with Maven.
