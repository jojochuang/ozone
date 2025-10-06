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

# Use Attila's RE-OZONE approach - construct path directly using version variable
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
        echo "Available files in hadoop-ozone/dist/target/:"
        ls -la hadoop-ozone/dist/target/ 2>/dev/null || echo "Directory not found"
        exit 1
    fi
fi

echo "Final OZONE_BIN path: ${OZONE_BIN_PATH}"

# Build using Dockerfile.apache with OZONE_BIN argument
# This follows the RE-OZONE pattern: 
# docker build --build-arg OZONE_BIN=hadoop-ozone/dist/target/ozone-${ozone_jar_version} -t ozone:${ozone_jar_version} -f cloudera/docker/Dockerfile.apache .
echo "Building Docker image..."
docker build \
  --build-arg OZONE_BIN=${OZONE_BIN_PATH} \
  -f cloudera/docker/Dockerfile.apache \
  -t ozone-temp:build .

echo "Docker build completed successfully!"

# Tag and push for each specified tag
for tag in ${TAGS}; do
    echo "Tagging and pushing ozone:${tag}..."
    docker tag ozone-temp:build ${REGISTRY}/ozone:${tag}
    docker push ${REGISTRY}/ozone:${tag}
    echo "Successfully pushed ${REGISTRY}/ozone:${tag}"
done

# Tag local image (without registry prefix) for P2 metadata processing
FIRST_TAG=$(echo ${TAGS} | awk '{print $1}')
echo "Tagging local image for metadata: ozone:${FIRST_TAG}"
docker tag ozone-temp:build ozone:${FIRST_TAG}

echo "Docker build and push completed successfully!"