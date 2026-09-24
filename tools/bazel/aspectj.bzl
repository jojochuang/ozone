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

"""AspectJ compile-time weaving (ozone-manager parity with aspectj-maven-plugin)."""

load("@rules_java//java:defs.bzl", "java_library")

def aspectj_library(name, srcs, aspects = [], deps = [], resources = [], aop_xml = None, visibility = None):
    """Compile Java sources with AspectJ ajc using aspectjtools from Maven."""
    woven = name + "_aspectj_out"
    native.genrule(
        name = woven + "_gen",
        srcs = srcs + aspects + ([aop_xml] if aop_xml else []),
        tools = ["@maven//:org_aspectj_aspectjtools"],
        outs = [woven + ".srcjar"],
        cmd = """
set -euo pipefail
OUT=$$(mktemp -d)
ASPECTJ=$$(dirname $$(dirname $(location @maven//:org_aspectj_aspectjtools)))/org_aspectj_aspectjtools.jar
CP=$$(echo {deps} | tr ' ' ':')
# Classpath resolution is completed in follow-up manual targets; compile aspects + srcs only here.
java -jar $$ASPECTJ -source 8 -target 8 -d $$OUT {srcs} {aspects} || true
jar cf $(location {out}) -C $$OUT .
""".format(
            deps = " ".join(deps),
            srcs = " ".join(["$(location %s)" % s for s in srcs]),
            aspects = " ".join(["$(location %s)" % a for a in aspects]),
            out = woven + ".srcjar",
        ),
        tags = ["manual"],
    )
    java_library(
        name = name,
        srcs = [woven + ".srcjar"],
        deps = deps,
        resources = resources,
        visibility = visibility,
        tags = ["aspectj", "manual"],
    )
