#!/bin/bash -e

# Prebuild
cd docker && python pre_build.py && cd ..

# Build conf and script images
docker build -f docker/generated/Dockerfile-conf-and-scripts -t xiaoyao1991/druid-conf-and-scripts .

# Build docker images
docker build -f docker/generated/Dockerfile-ubuntu-java8 -t xiaoyao1991/ubuntu-java8 .
docker build -f docker/generated/Dockerfile-druid-base -t xiaoyao1991/druid-base .
docker build -f docker/generated/Dockerfile-broker -t xiaoyao1991/druid-broker .
docker build -f docker/generated/Dockerfile-coordinator -t xiaoyao1991/druid-coordinator .
docker build -f docker/generated/Dockerfile-historical -t xiaoyao1991/druid-historical .
docker build -f docker/generated/Dockerfile-kafka -t xiaoyao1991/druid-kafka .
docker build -f docker/generated/Dockerfile-middlemanager -t xiaoyao1991/druid-middlemanager .
docker build -f docker/generated/Dockerfile-mysql -t xiaoyao1991/druid-mysql .
docker build -f docker/generated/Dockerfile-overlord -t xiaoyao1991/druid-overlord .
docker build -f docker/generated/Dockerfile-realtime -t xiaoyao1991/druid-realtime .
docker build -f docker/generated/Dockerfile-zookeeper -t xiaoyao1991/druid-zookeeper .

# Push docker images to hub
docker push xiaoyao1991/druid-conf-and-scripts
docker push xiaoyao1991/ubuntu-java8
docker push xiaoyao1991/druid-base
docker push xiaoyao1991/druid-kafka
docker push xiaoyao1991/druid-zookeeper
docker push xiaoyao1991/druid-mysql
docker push xiaoyao1991/druid-realtime
docker push xiaoyao1991/druid-overlord
docker push xiaoyao1991/druid-middlemanager
docker push xiaoyao1991/druid-historical
docker push xiaoyao1991/druid-coordinator
docker push xiaoyao1991/druid-broker
