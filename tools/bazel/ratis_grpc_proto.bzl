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

"""Generate Java + gRPC with Ratis third-party protobuf/grpc package rewrites."""

load("@rules_java//java:defs.bzl", "java_library")

def _ratis_grpc_gen(name, protos, import_proto_deps, out):
    proto_locations = " ".join(["$(location %s)" % p for p in protos])
    import_locations = " ".join(["$(locations %s)" % p for p in import_proto_deps]) if import_proto_deps else ""
    native.genrule(
        name = name,
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
$(location //tools/bazel:protoc) \\
  $$I_ARGS \\
  --java_out=$$OUT_DIR \\
  --grpc-java_out=$$OUT_DIR \\
  --plugin=protoc-gen-grpc-java=$(location //tools/bazel:protoc_gen_grpc_java) \\
  {proto_locations}
find $$OUT_DIR -name '*.java' -print0 | xargs -0 sed -i \\
  -e 's/com.google.common/org.apache.ratis.thirdparty.com.google.common/g' \\
  -e 's/com.google.protobuf/org.apache.ratis.thirdparty.com.google.protobuf/g' \\
  -e 's/io.grpc/org.apache.ratis.thirdparty.io.grpc/g'
jar cf $(location {out}) -C $$OUT_DIR .
""".format(
            import_locations = import_locations,
            proto_locations = proto_locations,
            out = out,
        ),
    )

def ozone_ratis_datanode_proto(name, proto, import_proto_deps = [], visibility = None):
    """Single-proto Ratis gRPC (DatanodeClientProtocol)."""
    out = name + "_generated.srcjar"
    _ratis_grpc_gen(name + "_ratis_gen", [proto], import_proto_deps, out)
    java_library(
        name = name,
        srcs = [out],
        deps = [
            "@maven//:org_apache_ratis_ratis_thirdparty_misc",
            "@maven//:javax_annotation_javax_annotation_api",
        ],
        visibility = visibility,
    )

def ozone_ratis_grpc_proto(name, protos, import_proto_deps = [], visibility = None):
    """One or more protos compiled with Ratis third-party rewrites + gRPC stubs."""
    out = name + "_generated.srcjar"
    _ratis_grpc_gen(name + "_ratis_gen", protos, import_proto_deps, out)
    java_library(
        name = name,
        srcs = [out],
        deps = [
            "@maven//:org_apache_ratis_ratis_thirdparty_misc",
            "@maven//:javax_annotation_javax_annotation_api",
        ],
        visibility = visibility,
    )
