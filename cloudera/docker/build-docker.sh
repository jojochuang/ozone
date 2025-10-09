#!/bin/bash
# Copyright Cloudera, Inc.
# Docker build script for Ozone following CDP standards

set -e

# Get parameters from environment
REGISTRY=${REGISTRY:-"docker-private.infra.cloudera.com/cloudera"}
TAGS=${TAGS:-"latest"}

echo "Building Ozone Docker image with Dockerfile.apache..."
echo "Registry: ${REGISTRY}"
echo "Tags: ${TAGS}"

# Use RE-OZONE approach construct path directly using version variable
if [ -n "${ozone_jar_version}" ]; then
    OZONE_BIN_PATH="hadoop-ozone/dist/target/ozone-${ozone_jar_version}"
    echo "Using RE-OZONE pattern: ${OZONE_BIN_PATH}"
else
    # Fallback: Find directory directly (avoid compose/ozone-balancer)
    OZONE_BIN_PATH=$(find hadoop-ozone/dist/target/ -maxdepth 1 -name "ozone-[0-9]*" -type d | head -1)
    if [ -n "${OZONE_BIN_PATH}" ]; then
        echo "Found ozone directory: ${OZONE_BIN_PATH}"
    else
        echo "ERROR: No ozone directory found and ozone_jar_version not set!"
        echo "Available files in hadoop-ozone/dist/target/:";
        ls -la hadoop-ozone/dist/target/ 2>/dev/null || echo "Directory not found";
        exit 1
    fi
fi

echo "Final OZONE_BIN path: ${OZONE_BIN_PATH}"

# Build every tag separately as build system wraps 'docker' command and transforms it to multi-arch building
for tag in ${TAGS}; do
    echo "Building Docker image for tag: ${tag}"
    docker build \
      --build-arg OZONE_BIN=${OZONE_BIN_PATH} \
      -f cloudera/docker/Dockerfile.apache \
      -t ${REGISTRY}/ozone:${tag} .
done

echo "Docker build completed successfully! The CDP build system will handle pushing to the registry."
