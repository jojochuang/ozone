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

"""Coordinates not captured by generate_maven_artifacts.py (BOM imports, nested exclusions)."""

EXTRA_MAVEN_ARTIFACTS = [
    "org.apache.hadoop:hadoop-common:3.4.3",
    "org.junit.jupiter:junit-jupiter-api:5.14.4",
    "org.junit.jupiter:junit-jupiter-engine:5.14.4",
    "org.junit.jupiter:junit-jupiter-params:5.14.4",
    "org.junit.platform:junit-platform-launcher:1.14.4",
    "org.junit.platform:junit-platform-commons:1.14.4",
    "org.junit.platform:junit-platform-engine:1.14.4",
    "org.junit.platform:junit-platform-console:1.14.4",
    "commons-logging:commons-logging:1.3.5",
    "org.apache.commons:commons-lang3:3.20.0",
    "javax.annotation:javax.annotation-api:1.3.2",
    "org.aspectj:aspectjtools:1.9.24",
    "org.aspectj:aspectjrt:1.9.24",
    "info.picocli:picocli-codegen:4.7.5",
]
