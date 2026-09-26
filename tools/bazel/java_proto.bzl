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
# pylint: skip-file

"""Generate Java protobuf sources with protoc 3.25.x (Maven-aligned, Java 8 compatible)."""

load("@rules_java//java:defs.bzl", "java_library")

# Fetch google/protobuf/*.proto includes bundled with protoc releases (Maven protoc exe has no includes).
_PROTOC_WELL_KNOWN_INCLUDES = """
PROTOC_ZIP_DIR=$$(mktemp -d)
curl -fsSL -o "$$PROTOC_ZIP_DIR/protoc.zip" \\
  "https://github.com/protocolbuffers/protobuf/releases/download/v25.9/protoc-25.9-linux-x86_64.zip"
unzip -q "$$PROTOC_ZIP_DIR/protoc.zip" "include/*" -d "$$PROTOC_ZIP_DIR"
add_inc "$$PROTOC_ZIP_DIR/include"
"""

def ozone_java_proto_library(name, protos, import_proto_deps = [], deps = [], visibility = None):
    """Run protoc and compile generated Java (matches ozone protobuf-maven-plugin version).

    Args:
        import_proto_deps: filegroup (or file) labels whose directories are passed to protoc -I.
    """
    gen_name = name + "_proto_gen"
    proto_locations = " ".join(["$(location %s)" % p for p in protos])
    import_locations = " ".join(["$(locations %s)" % p for p in import_proto_deps]) if import_proto_deps else ""
    out = name + "_generated.srcjar"
    native.genrule(
        name = gen_name,
        srcs = protos + import_proto_deps,
        outs = [out],
        tools = ["//tools/bazel:protoc"],
        cmd = """
set -euo pipefail
OUT_DIR=$$(mktemp -d)
export OZONE_PROTOC_CACHE=$$(mktemp -d)
I_ARGS=""
add_inc() {{
  local d="$$1"
  case " $$I_ARGS " in *" -I$$d "*) ;; *) I_ARGS="$$I_ARGS -I$$d";; esac
}}
for f in {import_locations} {proto_locations}; do
  [ -n "$$f" ] || continue
  add_inc "$$(dirname "$$f")"
done
{protoc_includes}
$(location //tools/bazel:protoc) $$I_ARGS --java_out=$$OUT_DIR {proto_locations}
jar cf $(location {out}) -C $$OUT_DIR .
""".format(
            import_locations = import_locations,
            proto_locations = proto_locations,
            out = out,
            protoc_includes = _PROTOC_WELL_KNOWN_INCLUDES,
        ),
    )
    java_library(
        name = name,
        srcs = [out],
        deps = deps + ["@maven//:com_google_protobuf_protobuf_java"],
        visibility = visibility,
    )

def ozone_java_grpc_proto_library(name, protos, import_proto_deps = [], deps = [], visibility = None):
    """Protoc Java + standard gRPC stubs (Ozone Manager client protocol)."""
    gen_name = name + "_proto_gen"
    proto_locations = " ".join(["$(location %s)" % p for p in protos])
    import_locations = " ".join(["$(locations %s)" % p for p in import_proto_deps]) if import_proto_deps else ""
    out = name + "_generated.srcjar"
    native.genrule(
        name = gen_name,
        srcs = protos + import_proto_deps,
        outs = [out],
        tools = [
            "//tools/bazel:protoc",
            "//tools/bazel:protoc_gen_grpc_java",
        ],
        cmd = """
set -euo pipefail
OUT_DIR=$$(mktemp -d)
export OZONE_PROTOC_CACHE=$$(mktemp -d)
I_ARGS=""
add_inc() {{
  local d="$$1"
  case " $$I_ARGS " in *" -I$$d "*) ;; *) I_ARGS="$$I_ARGS -I$$d";; esac
}}
for f in {import_locations} {proto_locations}; do
  [ -n "$$f" ] || continue
  add_inc "$$(dirname "$$f")"
done
{protoc_includes}
$(location //tools/bazel:protoc) \\
  $$I_ARGS \\
  --java_out=$$OUT_DIR \\
  --grpc-java_out=$$OUT_DIR \\
  --plugin=protoc-gen-grpc-java=$(location //tools/bazel:protoc_gen_grpc_java) \\
  {proto_locations}
jar cf $(location {out}) -C $$OUT_DIR .
""".format(
            import_locations = import_locations,
            proto_locations = proto_locations,
            out = out,
            protoc_includes = _PROTOC_WELL_KNOWN_INCLUDES,
        ),
    )
    java_library(
        name = name,
        srcs = [out],
        deps = deps + [
            "@maven//:com_google_protobuf_protobuf_java",
            "@maven//:io_grpc_grpc_api",
            "@maven//:io_grpc_grpc_protobuf",
            "@maven//:io_grpc_grpc_stub",
            "@maven//:javax_annotation_javax_annotation_api",
        ],
        visibility = visibility,
    )
