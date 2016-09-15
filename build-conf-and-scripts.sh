#!/bin/bash -e

# Prebuild
cd docker && python pre_build.py && cd ..

# Build conf and script images
docker build -f docker/generated/Dockerfile-conf-and-scripts -t xiaoyao1991/druid-conf-and-scripts .

# Push docker images to hub
docker push xiaoyao1991/druid-conf-and-scripts
