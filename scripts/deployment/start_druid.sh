#!/bin/bash
 
############################## PRE PROCESSING ################################
#check and process arguments
REQUIRED_NUMBER_OF_ARGUMENTS=1
if [ $# -lt $REQUIRED_NUMBER_OF_ARGUMENTS ]
then
    echo "Usage: $0 <path_to_config_file>"
    exit 1
fi

CONFIG_FILE=$1
 
echo "Config file is $CONFIG_FILE"
echo ""
 
#get the configuration parameters
source $CONFIG_FILE




############################## PROCESS CONFIG FILE ################################
#construct realtime FQDN
NEW_REALTIME_NODE=''
for node in ${REALTIME_NODE//,/ }
do
    if [ "$IP" == "TRUE" -o "$FQDN" == "TRUE" ] 
    then
        REALTIME_NODE_FQDN=$node
    else
        REALTIME_NODE_FQDN=$node.$EXPERIMENT.$PROJ.$ENV
    fi
 
    NEW_REALTIME_NODE=$NEW_REALTIME_NODE$USER_NAME@$REALTIME_NODE_FQDN,
done

#construct broker FQDN
NEW_BROKER_NODES=''
for node in ${BROKER_NODE//,/ }
do
    if [ "$IP" == "TRUE" -o "$FQDN" == "TRUE" ] 
    then
        NEW_BROKER_NODES=$NEW_BROKER_NODES$node,
    else
        NEW_BROKER_NODES=$NEW_BROKER_NODES$USER_NAME@$node.$EXPERIMENT.$PROJ.$ENV,
    fi
done

#construct historical FQDNs
NEW_HISTORICAL_NODES=''
for node in ${HISTORICAL_NODES//,/ }
do
    if [ "$IP" == "TRUE" -o "$FQDN" == "TRUE" ] 
    then
        NEW_HISTORICAL_NODES=$NEW_HISTORICAL_NODES$node,
    else
        NEW_HISTORICAL_NODES=$NEW_HISTORICAL_NODES$USER_NAME@$node.$EXPERIMENT.$PROJ.$ENV,
    fi
done

#construct coordinator FQDNs
NEW_COORDINATOR_NODES=''
for node in ${COORDINATOR_NODE//,/ }
do
    if [ "$IP" == "TRUE" -o "$FQDN" == "TRUE" ] 
    then
        NEW_COORDINATOR_NODES=$NEW_COORDINATOR_NODES$node,
    else
        NEW_COORDINATOR_NODES=$NEW_COORDINATOR_NODES$USER_NAME@$node.$EXPERIMENT.$PROJ.$ENV,
    fi
done

#construct zookeeper FQDNs
NEW_ZOOKEEPER_NODES=''
for node in ${ZOOKEEPER_NODE//,/ }
do
    if [ "$IP" == "TRUE" -o "$FQDN" == "TRUE" ] 
    then
        NEW_ZOOKEEPER_NODES=$NEW_ZOOKEEPER_NODES$node,
    else
        NEW_ZOOKEEPER_NODES=$NEW_ZOOKEEPER_NODES$USER_NAME@$node.$EXPERIMENT.$PROJ.$ENV,
    fi
done

#construct mysql FQDNs
NEW_MYSQL_NODES=''
for node in ${MYSQL_NODE//,/ }
do
    if [ "$IP" == "TRUE" -o "$FQDN" == "TRUE" ] 
    then
        NEW_MYSQL_NODES=$NEW_MYSQL_NODES$node,
    else
        NEW_MYSQL_NODES=$NEW_MYSQL_NODES$USER_NAME@$node.$EXPERIMENT.$PROJ.$ENV,
    fi
done

#construct overlord FQDNs
NEW_OVERLORD_NODES=''
for node in ${OVERLORD_NODE//,/ }
do
    if [ "$IP" == "TRUE" -o "$FQDN" == "TRUE" ] 
    then
        NEW_OVERLORD_NODES=$NEW_OVERLORD_NODES$node,
    else
        NEW_OVERLORD_NODES=$NEW_OVERLORD_NODES$USER_NAME@$node.$EXPERIMENT.$PROJ.$ENV,
    fi
done

#construct middle manager FQDNs
NEW_MIDDLE_MANAGER_NODES=''
for node in ${MIDDLE_MANAGER_NODE//,/ }
do
    if [ "$IP" == "TRUE" -o "$FQDN" == "TRUE" ] 
    then
        NEW_MIDDLE_MANAGER_NODES=$NEW_MIDDLE_MANAGER_NODES$node,
    else
        NEW_MIDDLE_MANAGER_NODES=$NEW_MIDDLE_MANAGER_NODES$USER_NAME@$node.$EXPERIMENT.$PROJ.$ENV,
    fi
done

#construct kafka FQDNs
NEW_KAFKA_NODES=''
for node in ${KAFKA_NODE//,/ }
do
    if [ "$IP" == "TRUE" -o "$FQDN" == "TRUE" ] 
    then
        NEW_KAFKA_NODES=$NEW_KAFKA_NODES$node,
    else
        NEW_KAFKA_NODES=$NEW_KAFKA_NODES$USER_NAME@$node.$EXPERIMENT.$PROJ.$ENV,
    fi
done

#construct broker FQDN
NEW_REDIS_NODES=''
for node in ${REDIS_NODE//,/ }
do
    if [ "$IP" == "TRUE" -o "$FQDN" == "TRUE" ] 
    then
        NEW_REDIS_NODES=$NEW_REDIS_NODES$node,
    else
        NEW_REDIS_NODES=$NEW_REDIS_NODES$USER_NAME@$node.$EXPERIMENT.$PROJ.$ENV,
    fi
done
############################## SETUP ################################

#generate keys for passwordless ssh
echo -ne '\n' | ssh-keygen;
#for node in ${NEW_COORDINATOR_NODES//,/ }
#do
#    ssh-copy-id -i ~/.ssh/id_rsa.pub $node;
#done
#for node in ${NEW_HISTORICAL_NODES//,/ }
#do
#    ssh-copy-id -i ~/.ssh/id_rsa.pub $node;
#done
#for node in ${NEW_BROKER_NODES//,/ }
#do
#    ssh-copy-id -i ~/.ssh/id_rsa.pub $node;
#done
#for node in ${NEW_REALTIME_NODE//,/ }
#do
#    ssh-copy-id -i ~/.ssh/id_rsa.pub $node;
#done
#for node in ${NEW_ZOOKEEPER_NODES//,/ }
#do
#    ssh-copy-id -i ~/.ssh/id_rsa.pub $node;
#done
#for node in ${NEW_OVERLORD_NODES//,/ }
#do
#    ssh-copy-id -i ~/.ssh/id_rsa.pub $node;
#done
#for node in ${NEW_MIDDLE_MANAGER_NODES//,/ }
#do
#    ssh-copy-id -i ~/.ssh/id_rsa.pub $node;
#done
#for node in ${NEW_MYSQL_NODES//,/ }
#do
#    ssh-copy-id -i ~/.ssh/id_rsa.pub $node;
#done
#for node in ${NEW_KAFKA_NODES//,/ }
#do
#    ssh-copy-id -i ~/.ssh/id_rsa.pub $node;
#done

#start redis FQDN
#counter=0
#echo "Setting up redis nodes:"
#for  node in ${NEW_REDIS_NODES//,/ }
#do

#        echo "Setting up $node ..."
#        COMMAND=''

#        COMMAND=$COMMAND" cd $PATH_TO_REDIS_BIN && ./create-cluster start;"
#        COMMAND=$COMMAND" ./create-cluster create"

#        if [ "$IP" == "TRUE" ]
#        then
#            COMMAND=$COMMAND" --bind_ip $node;"
#        else
#            COMMAND=$COMMAND";"
#        fi
#        echo "redis node startup command is $COMMAND"

    #ssh to node and run command
        #ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "
            #$COMMAND"
#done
#echo ""

#start zookeeper FQDN
counter=0
echo "Setting up zookeeper nodes:"
for  node in ${NEW_ZOOKEEPER_NODES//,/ }
do

        echo "Setting up $node ..."
        COMMAND=''

        COMMAND=$COMMAND" cd $PATH_TO_ZOOKEEPER && sudo ./bin/zkServer.sh start"

        if [ "$IP" == "TRUE" ]
        then
            COMMAND=$COMMAND" --bind_ip $node;"
        else
            COMMAND=$COMMAND";"
        fi
        echo "zookeeper node startup command is $COMMAND"

    #ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "
            $COMMAND"
done
echo ""
MYSQL="DROP DATABASE druid"
MYSQL=" CREATE DATABASE druid DEFAULT CHARACTER SET utf8;"

for node in ${OVERLORD_NODE//,/ }
do
            MYSQL=$MYSQL" GRANT ALL ON druid.* TO 'druid'@'$node-big-lan' IDENTIFIED BY 'diurd';"
done

        for node in ${REALTIME_NODE//,/ }
        do
            MYSQL=$MYSQL" GRANT ALL ON druid.* TO 'druid'@'$node-big-lan' IDENTIFIED BY 'diurd';"
        done

        for node in ${BROKER_NODE//,/ }
        do
            MYSQL=$MYSQL" GRANT ALL ON druid.* TO 'druid'@'$node-big-lan' IDENTIFIED BY 'diurd';"
        done

        for node in ${HISTORICAL_NODES//,/ }
        do
            MYSQL=$MYSQL" GRANT ALL ON druid.* TO 'druid'@'$node-big-lan' IDENTIFIED BY 'diurd';"
        done

        for node in ${COORDINATOR_NODE//,/ }
        do
            MYSQL=$MYSQL" GRANT ALL ON druid.* TO 'druid'@'$node-big-lan' IDENTIFIED BY 'diurd';"
        done

        for node in ${MIDDLE_MANAGER_NODE//,/ }
        do
            MYSQL=$MYSQL" GRANT ALL ON druid.* TO 'druid'@'$node-big-lan' IDENTIFIED BY 'diurd';"
        done

        for node in ${ZOOKEEPER_NODE//,/ }
        do
            MYSQL=$MYSQL" GRANT ALL ON druid.* TO 'druid'@'$node-big-lan' IDENTIFIED BY 'diurd';"
        done

        for node in ${KAFKA_NODE//,/ }
        do
            MYSQL=$MYSQL" GRANT ALL ON druid.* TO 'druid'@'$node-big-lan' IDENTIFIED BY 'diurd';"
        done

#start mysql FQDN
counter=0
echo "Setting up mysql nodes:"
for  node in ${NEW_MYSQL_NODES//,/ }
do

        echo "Setting up $node ..."
        COMMAND=''

        #COMMAND=$COMMAND" sudo chsh -s /bin/bash tkao4;"
        #COMMAND=$COMMAND" export DEBIAN_FRONTEND=noninteractive;"

        #COMMAND=$COMMAND" sudo debconf-set-selections <<< 'mysql-server mysql-server/root_password password ';"
        #COMMAND=$COMMAND" sudo debconf-set-selections <<< 'mysql-server mysql-server/root_password_again password ';"
        COMMAND=$COMMAND" sudo apt-get -y install mysql-server;"
        COMMAND=$COMMAND" sudo service mysql stop;"
        COMMAND=$COMMAND" sudo service mysql start;"
        COMMAND=$COMMAND" sudo sed -i '47s/.*/#bind-address = 127.0.0.1/' /etc/mysql/my.cnf;"
        COMMAND=$COMMAND" mysql -u root -p -e \"$MYSQL\";"
        COMMAND=$COMMAND" sudo /etc/init.d/mysql stop;"
        COMMAND=$COMMAND" sudo /etc/init.d/mysql start"

        if [ "$IP" == "TRUE" ]
        then
            COMMAND=$COMMAND" --bind_ip $node;"
        else
            COMMAND=$COMMAND";"
        fi
        echo "mysql node startup command is $COMMAND"

    #ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "
            $COMMAND"
done
echo ""

#start overlord FQDN
counter=0
echo "Setting up overlord nodes:"
for  node in ${NEW_OVERLORD_NODES//,/ }
do

        echo "Setting up $node ..."
        COMMAND=''

        COMMAND=$COMMAND" cd $PATH_TO_DRUID_BIN && sudo chsh -s /bin/bash tkao4 && screen -d -m java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath config/_common:config/overlord:lib/* io.druid.cli.Main server overlord"

        if [ "$IP" == "TRUE" ]
        then
            COMMAND=$COMMAND" --bind_ip $node;"
        else
            COMMAND=$COMMAND";"
        fi
        echo "overlord node startup command is $COMMAND"

    #ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "
            $COMMAND"
done
echo ""

#start middle manager FQDN
counter=0
echo "Setting up middle manager nodes:"
for  node in ${NEW_MIDDLE_MANAGER_NODES//,/ }
do

        echo "Setting up $node ..."
        COMMAND=''

        COMMAND=$COMMAND" cd $PATH_TO_DRUID_BIN && sudo chsh -s /bin/bash tkao4 && screen -d -m java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath config/_common:config/middleManager:lib/* io.druid.cli.Main server middleManager"

        if [ "$IP" == "TRUE" ]
        then
            COMMAND=$COMMAND" --bind_ip $node;"
        else
            COMMAND=$COMMAND";"
        fi
        echo "middle manager node startup command is $COMMAND"

    #ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "
            $COMMAND"
done
echo ""


#start coordinator FQDN
echo "Setting up coordinator nodes:"
for  node in ${NEW_COORDINATOR_NODES//,/ }
do

        echo "Setting up $node ..."
        COMMAND=''

        COMMAND=$COMMAND" cd $PATH_TO_DRUID_BIN && sudo chsh -s /bin/bash tkao4 && screen -d -m java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath config/_common:config/coordinator:lib/* io.druid.cli.Main server coordinator"  

        if [ "$IP" == "TRUE" ]
        then
            COMMAND=$COMMAND" --bind_ip $node;"
        else
            COMMAND=$COMMAND";"
        fi
        echo "Coordinator node startup command is $COMMAND"

    #ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "
            $COMMAND"
done
echo ""

#start historical FQDN
counter=0
echo "Setting up historical nodes:"
for  node in ${NEW_HISTORICAL_NODES//,/ }
do

        echo "Setting up $node ..."
        COMMAND=''

        COMMAND=$COMMAND" sudo sed -i '19s/.*/druid.host=$node-big-lan/' /proj/DCSQ/druid_proj/druid-0.8.3/config/historical/runtime.properties;"
        COMMAND=$COMMAND" cd $PATH_TO_DRUID_BIN && sudo chsh -s /bin/bash tkao4 && screen -d -m java -Xmx256m -XX:MaxDirectMemorySize=2147483648 -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath config/_common:config/historical:lib/* io.druid.cli.Main server historical"

        if [ "$IP" == "TRUE" ]
        then
            COMMAND=$COMMAND" --bind_ip $node;"
        else
            COMMAND=$COMMAND";"
        fi
        echo "historical node startup command is $COMMAND"

	#ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "
            $COMMAND"
done
echo ""

#setup broker FQDN
echo "Setting up broker nodes:"
for  node in ${NEW_BROKER_NODES//,/ }
do

        echo "Setting up $node ..."
        COMMAND=''

        COMMAND=$COMMAND" cd $PATH_TO_DRUID_BIN && sudo chsh -s /bin/bash tkao4 && screen -d -m java -Xmx256m -XX:MaxDirectMemorySize=2147483648 -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath config/_common:config/broker:lib/* io.druid.cli.Main server broker"

        if [ "$IP" == "TRUE" ]
        then
            COMMAND=$COMMAND" --bind_ip $node;"
        else
            COMMAND=$COMMAND";"
        fi
        echo "Broker node startup command is $COMMAND"

	#ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "
            $COMMAND"
done
echo ""

#start kafka FQDN
counter=0
echo "Setting up kafka nodes:"
for  node in ${NEW_KAFKA_NODES//,/ }
do

        echo "Setting up $node ..."
        COMMAND=''

        COMMAND=$COMMAND" cd $PATH_TO_KAFKA;"
        COMMAND=$COMMAND" screen -d -m ./bin/zookeeper-server-start.sh config/zookeeper.properties;"
        COMMAND=$COMMAND" screen -d -m ./bin/kafka-server-start.sh config/server.properties;"
        COMMAND=$COMMAND" screen -d -m ./bin/kafka-topics.sh --create --zookeeper $KAFKA_NODE-big-lan:2181 --replication-factor 1 --partitions 1 --topic $KAFKA_TOPIC;"
        #COMMAND=$COMMAND" ./emit-random-metrics.py -n 250"
        #COMMAND=$COMMAND" screen -d -m ./bin/kafka-console-producer.sh --broker-list $KAFKA_NODE-big-lan:9092 --topic metrics"

        if [ "$IP" == "TRUE" ]
        then
            COMMAND=$COMMAND" --bind_ip $node;"
        else
            COMMAND=$COMMAND";" 
        fi
        echo "kafka node startup command is $COMMAND"

    #ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "
            $COMMAND"
done
echo ""

echo "Setting up realtime nodes:"
for  node in ${NEW_REALTIME_NODE//,/ }
do

        echo "Setting up $node ..."
        COMMAND=''

    COMMAND=$COMMAND" cd $PATH_TO_DRUID_BIN && sudo chsh -s /bin/bash tkao4 && screen -d -m java -Xmx512m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -Ddruid.realtime.specFile=$SPEC_FILE -classpath config/_common:config/realtime:lib/* io.druid.cli.Main server realtime"

        if [ "$IP" == "TRUE" ]
        then
            COMMAND=$COMMAND" --bind_ip $node;"
        else
            COMMAND=$COMMAND";"
        fi
        echo "Realtime node startup command is $COMMAND"

    #ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "
            $COMMAND"
done
echo ""
