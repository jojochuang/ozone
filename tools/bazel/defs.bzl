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

"""Shared Starlark helpers for Apache Ozone Bazel builds."""

load("@rules_java//java:defs.bzl", "java_library", "java_plugin", "java_test")

def ozone_java_library(name, srcs, deps = [], plugins = [], resources = [], **kwargs):
    """Ozone java_library with Java 8 release and standard tags."""
    java_library(
        name = name,
        srcs = srcs,
        deps = deps,
        plugins = plugins,
        resources = resources,
        tags = kwargs.pop("tags", []) + ["ozone"],
        **kwargs
    )

def ozone_java_test(name, srcs, deps = [], runtime_deps = [], jvm_flags = [], **kwargs):
    """JUnit 5 test with Ozone defaults."""
    java_test(
        name = name,
        srcs = srcs,
        deps = deps + [
            "@maven//:org_junit_jupiter_junit_jupiter_api",
            "@maven//:org_junit_platform_junit_platform_launcher",
            "@maven//:org_junit_jupiter_junit_jupiter_engine",
        ],
        runtime_deps = runtime_deps,
        jvm_flags = jvm_flags,
        test_class = kwargs.pop("test_class", None),
        tags = kwargs.pop("tags", []) + ["ozone"],
        **kwargs
    )

def maven(label):
    """Shorthand for @maven// coordinates label."""
    return "@maven//:" + label
