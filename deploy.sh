#!/bin/bash -e

eval $(docker-machine env --swarm p-druid-swarm-master)
docker run -itd --name=zookeeper --network=p-my-net --env="constraint:node==p-node-7" xiaoyao1991/druid-zookeeper:latest
docker run -itd --name=mysql --network=p-my-net --env="constraint:node==p-node-8" xiaoyao1991/druid-mysql:latest

for i in `seq 1 10`;
do
  docker run -itd --name=node-$i-conf-and-scripts --network=p-my-net --env="constraint:node==p-node-$i" xiaoyao1991/druid-conf-and-scripts:latest
done

docker run -itd -v /proj/DCSQ/xiaoyao/druid-segments:/proj/DCSQ/xiaoyao/druid-segments --volumes-from=node-9-conf-and-scripts --name=overlord --network=p-my-net --env="constraint:node==p-node-9" xiaoyao1991/druid-overlord:latest
docker run -itd -v /proj/DCSQ/xiaoyao/druid-segments:/proj/DCSQ/xiaoyao/druid-segments --volumes-from=node-10-conf-and-scripts --name=middlemanager --network=p-my-net --env="constraint:node==p-node-10" xiaoyao1991/druid-middlemanager:latest
docker run -itd -v /proj/DCSQ/xiaoyao/druid-segments:/proj/DCSQ/xiaoyao/druid-segments --volumes-from=node-1-conf-and-scripts -p 8080:8080 --name=coordinator --network=p-my-net --env="constraint:node==p-node-1" xiaoyao1991/druid-coordinator:latest
docker run -itd -v /proj/DCSQ/xiaoyao/druid-segments:/proj/DCSQ/xiaoyao/druid-segments --volumes-from=node-2-conf-and-scripts --name=historical1 --network=p-my-net --env="constraint:node==p-node-2" xiaoyao1991/druid-historical:latest
docker run -itd -v /proj/DCSQ/xiaoyao/druid-segments:/proj/DCSQ/xiaoyao/druid-segments --volumes-from=node-3-conf-and-scripts --name=historical2 --network=p-my-net --env="constraint:node==p-node-3" xiaoyao1991/druid-historical:latest
docker run -itd -v /proj/DCSQ/xiaoyao/druid-segments:/proj/DCSQ/xiaoyao/druid-segments --volumes-from=node-4-conf-and-scripts --name=historical3 --network=p-my-net --env="constraint:node==p-node-4" xiaoyao1991/druid-historical:latest
docker run -itd -v /proj/DCSQ/xiaoyao/druid-segments:/proj/DCSQ/xiaoyao/druid-segments --volumes-from=node-5-conf-and-scripts --name=broker --network=p-my-net --env="constraint:node==p-node-5" xiaoyao1991/druid-broker:latest
docker run -itd -v /proj/DCSQ/xiaoyao/druid-segments:/proj/DCSQ/xiaoyao/druid-segments --volumes-from=node-8-conf-and-scripts --name=kafka --network=p-my-net --env="constraint:node==p-node-8" xiaoyao1991/druid-kafka:latest
docker run -itd -v /proj/DCSQ/xiaoyao/druid-segments:/proj/DCSQ/xiaoyao/druid-segments --volumes-from=node-6-conf-and-scripts --name=realtime --network=p-my-net --env="constraint:node==p-node-6" xiaoyao1991/druid-realtime:latest
