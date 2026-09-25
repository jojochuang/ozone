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

"""JUnit 5 helpers aligned with hadoop-hdds/config TestConfigurationReflectionUtil."""

load("@rules_java//java:defs.bzl", "java_test")

JUNIT5_RUNTIME = [
    "@maven//:org_junit_jupiter_junit_jupiter_api",
    "@maven//:org_junit_jupiter_junit_jupiter_engine",
    "@maven//:org_junit_jupiter_junit_jupiter_params",
    "@maven//:org_junit_platform_junit_platform_console",
    "@maven//:org_junit_platform_junit_platform_launcher",
    "@maven//:org_junit_platform_junit_platform_engine",
]

def ozone_junit5_class(name, test_class, deps, size = "small", tags = None, **kwargs):
    """Single-class JUnit 5 test via ConsoleLauncher."""
    java_test(
        name = name,
        args = ["--select-class=" + test_class],
        main_class = "org.junit.platform.console.ConsoleLauncher",
        use_testrunner = False,
        size = size,
        tags = tags or [],
        runtime_deps = deps + JUNIT5_RUNTIME,
        **kwargs
    )

def ozone_junit5_package(name, package, deps, size = "medium", tags = None, exclude_classes = None, exclude_packages = None, **kwargs):
    """Run all JUnit 5 tests under a Java package."""
    args = ["--select-package=" + package]
    for pkg in exclude_packages or []:
        args.append("--exclude-package=" + pkg)
    for cls in exclude_classes or []:
        args.append("--exclude-class=" + cls)
    java_test(
        name = name,
        args = args,
        main_class = "org.junit.platform.console.ConsoleLauncher",
        use_testrunner = False,
        size = size,
        tags = tags or [],
        runtime_deps = deps + JUNIT5_RUNTIME,
        **kwargs
    )
