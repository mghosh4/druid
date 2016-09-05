#!/bin/bash -e

docker stop realtime kafka broker historical1 historical2 historical3 coordinator middlemanager overlord mysql zookeeper
docker rm realtime kafka broker historical1 historical2 historical3 coordinator middlemanager overlord mysql zookeeper
docker rmi xiaoyao1991/druid-kafka xiaoyao1991/druid-zookeeper xiaoyao1991/druid-mysql xiaoyao1991/druid-realtime xiaoyao1991/druid-overlord xiaoyao1991/druid-middlemanager xiaoyao1991/druid-historical xiaoyao1991/druid-coordinator xiaoyao1991/druid-broker
