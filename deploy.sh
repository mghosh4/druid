#!/bin/bash -e

eval $(docker-machine env --swarm p-druid-swarm-master)
docker run -itd --name=zookeeper --network=p-my-net --env="constraint:node==p-node-7" xiaoyao1991/druid-zookeeper:latest
docker run -itd --name=mysql --network=p-my-net --env="constraint:node==p-node-8" xiaoyao1991/druid-mysql:latest
docker run -itd -v /proj/DCSQ/xiaoyao-druid-segments:/proj/DCSQ/xiaoyao-druid-segments --name=overlord --network=p-my-net --env="constraint:node==p-node-9" xiaoyao1991/druid-overlord:latest
docker run -itd -v /proj/DCSQ/xiaoyao-druid-segments:/proj/DCSQ/xiaoyao-druid-segments --name=middlemanager --network=p-my-net --env="constraint:node==p-node-10" xiaoyao1991/druid-middlemanager:latest
docker run -itd -v /proj/DCSQ/xiaoyao-druid-segments:/proj/DCSQ/xiaoyao-druid-segments -p 8080:8080 --name=coordinator --network=p-my-net --env="constraint:node==p-node-1" xiaoyao1991/druid-coordinator:latest
docker run -itd -v /proj/DCSQ/xiaoyao-druid-segments:/proj/DCSQ/xiaoyao-druid-segments --name=historical1 --network=p-my-net --env="constraint:node==p-node-2" xiaoyao1991/druid-historical:latest
docker run -itd -v /proj/DCSQ/xiaoyao-druid-segments:/proj/DCSQ/xiaoyao-druid-segments --name=historical2 --network=p-my-net --env="constraint:node==p-node-3" xiaoyao1991/druid-historical:latest
docker run -itd -v /proj/DCSQ/xiaoyao-druid-segments:/proj/DCSQ/xiaoyao-druid-segments --name=historical3 --network=p-my-net --env="constraint:node==p-node-4" xiaoyao1991/druid-historical:latest
docker run -itd -v /proj/DCSQ/xiaoyao-druid-segments:/proj/DCSQ/xiaoyao-druid-segments --name=broker --network=p-my-net --env="constraint:node==p-node-5" xiaoyao1991/druid-broker:latest
docker run -itd -v /proj/DCSQ/xiaoyao-druid-segments:/proj/DCSQ/xiaoyao-druid-segments --name=kafka --network=p-my-net --env="constraint:node==p-node-8" xiaoyao1991/druid-kafka:latest
docker run -itd -v /proj/DCSQ/xiaoyao-druid-segments:/proj/DCSQ/xiaoyao-druid-segments --name=realtime --network=p-my-net --env="constraint:node==p-node-6" xiaoyao1991/druid-realtime:latest
