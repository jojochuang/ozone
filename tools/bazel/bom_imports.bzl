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

"""Coordinates from imported Maven BOMs (Jackson, gRPC, OpenTelemetry, Jersey)."""

BOM_IMPORT_ARTIFACTS = [
    "com.fasterxml.jackson.core:jackson-annotations:2.18.2",
    "com.fasterxml.jackson.core:jackson-core:2.18.2",
    "com.fasterxml.jackson.core:jackson-databind:2.18.2",
    "com.fasterxml.jackson.dataformat:jackson-dataformat-xml:2.18.2",
    "com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.18.2",
    "io.grpc:grpc-api:1.77.1",
    "io.grpc:grpc-context:1.77.1",
    "io.grpc:grpc-core:1.77.1",
    "io.grpc:grpc-inprocess:1.77.1",
    "io.grpc:grpc-netty:1.77.1",
    "io.grpc:grpc-protobuf:1.77.1",
    "io.grpc:grpc-stub:1.77.1",
    "io.grpc:grpc-testing:1.77.1",
    "io.netty:netty-tcnative-boringssl-static:2.0.74.Final",
    "io.opentelemetry:opentelemetry-api:1.63.0",
    "io.opentelemetry:opentelemetry-context:1.63.0",
    "io.opentelemetry:opentelemetry-exporter-otlp:1.63.0",
    "io.opentelemetry:opentelemetry-sdk:1.63.0",
    "io.opentelemetry:opentelemetry-sdk-common:1.63.0",
    "io.opentelemetry:opentelemetry-sdk-trace:1.63.0",
    "org.glassfish.jersey.containers:jersey-container-servlet-core:2.48",
    "org.glassfish.jersey.core:jersey-common:2.48",
    "org.glassfish.jersey.core:jersey-server:2.48",
    "org.glassfish.jersey.ext.cdi:jersey-cdi1x:2.48",
    "org.glassfish.jersey.inject:jersey-hk2:2.48",
    "org.glassfish.jersey.media:jersey-media-jaxb:2.48",
    "org.glassfish.jersey.media:jersey-media-json-jackson:2.48",
    "org.mockito:mockito-core:4.11.0",
    "org.mockito:mockito-inline:4.11.0",
    "org.mockito:mockito-junit-jupiter:4.11.0",
    "org.slf4j:jcl-over-slf4j:2.0.18",
    "org.springframework:spring-jdbc:5.3.39",
    "org.springframework:spring-tx:5.3.39",
]
