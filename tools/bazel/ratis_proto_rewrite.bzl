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

"""Rewrite generated gRPC Java sources to use Ratis third-party package prefixes."""

def _ratis_grpc_rewrite_impl(ctx):
    output = ctx.actions.declare_directory(ctx.attr.name + "_srcs")
    inputs = []
    for src in ctx.files.srcs:
        out = ctx.actions.declare_file(ctx.attr.name + "/" + src.basename)
        inputs.append(src)
        ctx.actions.run_shell(
            inputs = [src],
            outputs = [out],
            command = """
set -euo pipefail
sed -e 's/com.google.common/org.apache.ratis.thirdparty.com.google.common/g' \
    -e 's/com.google.protobuf/org.apache.ratis.thirdparty.com.google.protobuf/g' \
    -e 's/io.grpc/org.apache.ratis.thirdparty.io.grpc/g' \
    "$1" > "$2"
""",
            arguments = [src.path, out.path],
        )
    return [DefaultInfo(files = depset([output]))]

ratis_grpc_rewrite = rule(
    implementation = _ratis_grpc_rewrite_impl,
    attrs = {
        "srcs": attr.label_list(allow_files = [".java"]),
    },
)
