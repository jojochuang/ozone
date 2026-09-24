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

"""Generate Java protobuf sources with protoc 3.25.x (Maven-aligned, Java 8 compatible)."""

load("@rules_java//java:defs.bzl", "java_library")

def ozone_java_proto_library(name, protos, proto_src_dir = "src/main/proto", deps = [], visibility = None):
    """Run protoc and compile generated Java (matches ozone protobuf-maven-plugin version)."""
    gen_name = name + "_proto_gen"
    proto_locations = " ".join(["$(location %s)" % p for p in protos])
    first_proto = protos[0]
    out = name + "_generated.srcjar"
    native.genrule(
        name = gen_name,
        srcs = protos,
        outs = [out],
        tools = ["//tools/bazel:protoc"],
        cmd = """
set -euo pipefail
OUT_DIR=$$(mktemp -d)
export OZONE_PROTOC_CACHE=$$(mktemp -d)
PROTO_DIR=$$(dirname $(location {first_proto}))
$(location //tools/bazel:protoc) --java_out=$$OUT_DIR -I$$PROTO_DIR {proto_locations}
jar cf $(location {out}) -C $$OUT_DIR .
""".format(
            first_proto = first_proto,
            proto_locations = proto_locations,
            out = out,
        ),
    )
    java_library(
        name = name,
        srcs = [out],
        deps = deps + ["@maven//:com_google_protobuf_protobuf_java"],
        visibility = visibility,
    )
