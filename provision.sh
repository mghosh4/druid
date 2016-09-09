#!/bin/bash -e

# Change shell to bash
for i in `seq 1 12`;
do
  ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no xiaoyao@node-$i.druidxiaoyao.dcsq.emulab.net "sudo chsh -s /bin/bash xiaoyao"
done

# Create kv store for service discovery
docker-machine -D create --driver generic \
  --generic-ssh-user=xiaoyao \
  --generic-ip-address=node-12.druidxiaoyao.dcsq.emulab.net \
  --generic-ssh-key ~/.ssh/id_rsa \
  p-mh-keystore

eval $(docker-machine env p-mh-keystore)

docker run -d \
  -p "8500:8500" \
  -h "consul" \
  progrium/consul -server -bootstrap

# Create swarm master
docker-machine -D create --driver generic \
  --generic-ssh-user=xiaoyao \
  --generic-ip-address=node-11.druidxiaoyao.dcsq.emulab.net \
  --generic-ssh-key ~/.ssh/id_rsa \
  --swarm \
  --swarm-master \
  --swarm-discovery="consul://$(docker-machine ip p-mh-keystore):8500" \
  --engine-opt="cluster-store=consul://$(docker-machine ip p-mh-keystore):8500" \
  --engine-opt="cluster-advertise=eth0:2376" \
  p-druid-swarm-master

# Create swarm workers
for i in `seq 1 10`;
do
  echo $i
  docker-machine -D create --driver generic \
    --generic-ssh-user=xiaoyao \
    --generic-ip-address=node-$i.druidxiaoyao.dcsq.emulab.net \
    --generic-ssh-key ~/.ssh/id_rsa \
    --swarm \
    --swarm-discovery="consul://$(docker-machine ip p-mh-keystore):8500" \
    --engine-opt="cluster-store=consul://$(docker-machine ip p-mh-keystore):8500" \
    --engine-opt="cluster-advertise=eth0:2376" \
    p-node-$i
done

# Create overlay network
eval $(docker-machine env --swarm p-druid-swarm-master)
docker network create --driver overlay --subnet=10.0.9.0/24 p-my-net
