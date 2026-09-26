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

"""Shared codegen helpers (Picocli, HDDS config processor)."""

load("@rules_java//java:defs.bzl", "java_plugin")

def picocli_codegen_plugin(name = "picocli_codegen"):
    """Picocli annotation processor used by Ozone CLI modules."""
    java_plugin(
        name = name,
        processor_class = "picocli.codegen.aot.graalvm.processor.NativeImageConfigGeneratorProcessor",
        deps = [
            "@maven//:info_picocli_picocli",
            "@maven//:info_picocli_picocli_codegen",
        ],
        visibility = ["//visibility:public"],
    )

def hdds_config_processor():
    """Returns the config file generator plugin target."""
    return "//hadoop-hdds/config:config_file_generator"

# .bazelrc sets -proc:none globally; targets with java_plugin need -proc:full in javacopts.
OZONE_ANNOTATION_PROCESSING_JAVACOPTS = ["-proc:full"]
