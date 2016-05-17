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
        NEW_REALTIME_NODE=$NEW_REALTIME_NODE$node,
    else
        NEW_REALTIME_NODE=$NEW_REALTIME_NODE$USER_NAME@$node.$EXPERIMENT.$PROJ.$ENV,
    fi
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

############################## SETUP ################################

for  node in ${NEW_ZOOKEEPER_NODES//,/ }
do

        echo "Setting up $node ..."
        COMMAND=''

        if [ "$AWS" == "FALSE" ]
        then
            if [ "$IP" == "TRUE" -o "$FQDN" == "TRUE" ]
            then
                if [ -d $PATH_TO_DRUID_BIN/extensions/druid-s3-extensions ]
                then
                    echo "moving S3 file"
                    COMMAND=$COMMAND" sudo sed -i '26s/.*/druid.extensions.loadList=[\"druid-kafka-eight\", \"druid-histogram\", \"druid-datasketches\", \"druid-namespace-lookup\", \"mysql-metadata-storage\"]/' $PATH_TO_DRUID_BIN/conf/druid/_common/common.runtime.properties;"
                    COMMAND=$COMMAND" sudo mv $PATH_TO_DRUID_BIN/extensions/druid-s3-extensions $PATH_TO_DRUID_BIN;"
                fi
            elif ["$IP" == "FALSE" -a "$FQDN" == "FALSE" ]
            then
                if (ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "[ -d $PATH_TO_DRUID_BIN/extensions/druid-s3-extensions ]")
                then
                    echo "moving S3 file"
                    COMMAND=$COMMAND" sudo sed -i '26s/.*/druid.extensions.loadList=[\"druid-kafka-eight\", \"druid-histogram\", \"druid-datasketches\", \"druid-namespace-lookup\", \"mysql-metadata-storage\"]/' $PATH_TO_DRUID_BIN/conf/druid/_common/common.runtime.properties;"
                    COMMAND=$COMMAND" sudo mv $PATH_TO_DRUID_BIN/extensions/druid-s3-extensions $PATH_TO_DRUID_BIN;"
                fi
            fi
        elif [ "$AWS" == "TRUE" ]
        then
            if [ "$IP" == "TRUE" -o "$FQDN" == "TRUE" ]
            then
                if [ -d $PATH_TO_DRUID_BIN/extensions/druid-s3-extensions ]
                then
                    echo "moving S3 file"
                    COMMAND=$COMMAND" sudo sed -i '26s/.*/druid.extensions.loadList=[\"druid-kafka-eight\", \"druid-s3-extensions\", \"druid-histogram\", \"druid-datasketches\", \"druid-namespace-lookup\", \"mysql-metadata-storage\"]/' $PATH_TO_DRUID_BIN/conf/druid/_common/common.runtime.properties;"
                    COMMAND=$COMMAND" sudo mv $PATH_TO_DRUID_BIN/druid-s3-extensions $PATH_TO_DRUID_BIN/extensions;"
                fi
            elif [ "$IP" == "FALSE" -a "$FQDN" == "FALSE" ]
            then
                if (ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "[ -d $PATH_TO_DRUID_BIN/druid-s3-extensions ]")
                then
                    echo "moving S3 file"
                    COMMAND=$COMMAND" sudo sed -i '26s/.*/druid.extensions.loadList=[\"druid-kafka-eight\", \"druid-s3-extensions\", \"druid-histogram\", \"druid-datasketches\", \"druid-namespace-lookup\", \"mysql-metadata-storage\"]/' $PATH_TO_DRUID_BIN/conf/druid/_common/common.runtime.properties;"
                    COMMAND=$COMMAND" sudo mv $PATH_TO_DRUID_BIN/druid-s3-extensions $PATH_TO_DRUID_BIN/extensions;"
                fi
            fi
        fi

        COMMAND=$COMMAND" sudo cat $PATH_TO_SOURCE/scripts/deployment/log4j2.xml > $COMMON_LOG4J2/log4j2.xml;"
        COMMAND=$COMMAND" sudo sed -i '7s@.*@        <File name=\"File\" fileName=\"$LOG_FILE/\${sys:logfilename}.log\">@' $COMMON_LOG4J2/log4j2.xml;"
        COMMAND=$COMMAND" cd $PATH_TO_ZOOKEEPER;"
        COMMAND=$COMMAND" sudo sed -i '36s/.*/druid.zk.service.host=$ZOOKEEPER_NODE_HOST/' $PATH_TO_DRUID_BIN/conf/druid/_common/common.runtime.properties;"
        COMMAND=$COMMAND" sudo ZOO_LOG_DIR=$LOG_FILE ZOO_LOG4J_PROP='INFO,ROLLINGFILE' ./bin/zkServer.sh start;"

        echo "zookeeper node startup command is $COMMAND"

    #ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "
            $COMMAND"
done
echo ""
MYSQL=''
#MYSQL=$MYSQL" DROP DATABASE druid;"
MYSQL=$MYSQL" CREATE DATABASE druid DEFAULT CHARACTER SET utf8;"

        for node in ${OVERLORD_NODE_HOST//,/ }
        do
            MYSQL=$MYSQL" GRANT ALL ON druid.* TO 'druid'@'$node' IDENTIFIED BY 'diurd';"
        done

        for node in ${REALTIME_NODE_HOST//,/ }
        do
            MYSQL=$MYSQL" GRANT ALL ON druid.* TO 'druid'@'$node' IDENTIFIED BY 'diurd';"
        done

        for node in ${BROKER_NODE_HOST//,/ }
        do
            MYSQL=$MYSQL" GRANT ALL ON druid.* TO 'druid'@'$node' IDENTIFIED BY 'diurd';"
        done

        for node in ${HISTORICAL_NODE_HOST//,/ }
        do
            MYSQL=$MYSQL" GRANT ALL ON druid.* TO 'druid'@'$node' IDENTIFIED BY 'diurd';"
        done

        for node in ${COORDINATOR_NODE_HOST//,/ }
        do
            MYSQL=$MYSQL" GRANT ALL ON druid.* TO 'druid'@'$node' IDENTIFIED BY 'diurd';"
        done

        for node in ${MIDDLE_MANAGER_NODE_HOST//,/ }
        do
            MYSQL=$MYSQL" GRANT ALL ON druid.* TO 'druid'@'$node' IDENTIFIED BY 'diurd';"
        done

        for node in ${ZOOKEEPER_NODE_HOST//,/ }
        do
            MYSQL=$MYSQL" GRANT ALL ON druid.* TO 'druid'@'$node' IDENTIFIED BY 'diurd';"
        done

        for node in ${KAFKA_NODE_HOST//,/ }
        do
            MYSQL=$MYSQL" GRANT ALL ON druid.* TO 'druid'@'$node' IDENTIFIED BY 'diurd';"
        done

#start mysql FQDN
counter=0
echo "Setting up mysql nodes:"
for  node in ${NEW_MYSQL_NODES//,/ }
do

        echo "Setting up $node ..."
        COMMAND=''

        #LOAD MYSQL-METADATA-STORAGE EXTENSION
        if [ "$IP" == "TRUE" -o "$FQDN" == "TRUE" ]
        then
                echo "HELLOOOOOOOO"
            if [ -f $PATH_TO_SOURCE/distribution/target/mysql-metadata-storage-bin.tar.gz ]
            then
                echo "untar mysql file"
                COMMAND=$COMMAND" sudo tar xC $PATH_TO_DRUID_BIN/extensions -f $PATH_TO_SOURCE/distribution/target/mysql-metadata-storage-bin.tar.gz;"
                COMMAND=$COMMAND" sudo rm -rf $PATH_TO_SOURCE/distribution/target/mysql-metadata-storage-bin;"
            fi
        elif [ "$IP" == "FALSE" -a "$FQDN" == "FALSE" ]
        then
            if (ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "[ -f $PATH_TO_SOURCE/distribution/target/mysql-metadata-storage-bin.tar.gz ]")
            then
                echo "untar mysql file"
                COMMAND=$COMMAND" sudo tar -xvf $PATH_TO_SOURCE/distribution/target/mysql-metadata-storage-bin.tar.gz $PATH_TO_DRUID_BIN/extensions;"
            fi
        fi

        COMMAND=$COMMAND" sudo sed -i '72s@.*@general_log_file        = /proj/DCSQ/tkao4/mysql.log@' /etc/mysql/my.cnf;"
        COMMAND=$COMMAND" sudo sed -i '47s/.*/#bind-address = 127.0.0.1/' /etc/mysql/my.cnf;"
        COMMAND=$COMMAND" sudo service mysql stop;"
        COMMAND=$COMMAND" sudo service mysql start;"
        COMMAND=$COMMAND" sudo sed -i '43s/.*/#druid.metadata.storage.type=derby/' $PATH_TO_DRUID_BIN/conf/druid/_common/common.runtime.properties;"
        COMMAND=$COMMAND" sudo sed -i '44s@.*@#druid.metadata.storage.connector.connectURI=jdbc:derby://metadata.store.ip:1527/var/druid/metadata.db;create=true@' $PATH_TO_DRUID_BIN/conf/druid/_common/common.runtime.properties;"
        COMMAND=$COMMAND" sudo sed -i '45s/.*/#druid.metadata.storage.connector.host=metadata.store.ip/' $PATH_TO_DRUID_BIN/conf/druid/_common/common.runtime.properties;"
        COMMAND=$COMMAND" sudo sed -i '46s/.*/#druid.metadata.storage.connector.port=1527/' $PATH_TO_DRUID_BIN/conf/druid/_common/common.runtime.properties;"        
        COMMAND=$COMMAND" sudo sed -i '49s/.*/druid.metadata.storage.type=mysql/' $PATH_TO_DRUID_BIN/conf/druid/_common/common.runtime.properties;"
        COMMAND=$COMMAND" sudo sed -i '50s@.*@druid.metadata.storage.connector.connectURI=jdbc:mysql://$MYSQL_NODE_HOST:$MYSQL_NODE_PORT/druid@' $PATH_TO_DRUID_BIN/conf/druid/_common/common.runtime.properties;"
        COMMAND=$COMMAND" sudo sed -i '51s/.*/druid.metadata.storage.connector.user=druid/' $PATH_TO_DRUID_BIN/conf/druid/_common/common.runtime.properties;"
        COMMAND=$COMMAND" sudo sed -i '52s/.*/druid.metadata.storage.connector.password=diurd/' $PATH_TO_DRUID_BIN/conf/druid/_common/common.runtime.properties;"
        COMMAND=$COMMAND" mysql -u root -e \"$MYSQL\";"
        COMMAND=$COMMAND" sudo /etc/init.d/mysql stop;"
        COMMAND=$COMMAND" sudo /etc/init.d/mysql start;"

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

        COMMAND=$COMMAND" sudo sed -i '2s@.*@druid.port=$OVERLORD_NODE_PORT@' $PATH_TO_DRUID_BIN/conf/druid/overlord/runtime.properties;"
        COMMAND=$COMMAND" cd $PATH_TO_DRUID_BIN && screen -d -m sudo java -Xmx256m -Duser.timezone=UTC -Dlogfilename=overlord-$counter -Dfile.encoding=UTF-8 -classpath 'conf/druid/_common:conf/druid/overlord:lib/*' io.druid.cli.Main server overlord;"

        echo "overlord node startup command is $COMMAND"

        counter=counter+1
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

        COMMAND=$COMMAND" sudo sed -i '2s@.*@druid.port=$MIDDLE_MANAGER_NODE_PORT@' $PATH_TO_DRUID_BIN/conf/druid/middleManager/runtime.properties;"     
        COMMAND=$COMMAND" cd $PATH_TO_DRUID_BIN && screen -d -m sudo java -Xmx256m -Duser.timezone=UTC -Dlogfilename=middlemanager-$counter -Dfile.encoding=UTF-8 -classpath 'conf/druid/_common:conf/druid/middleManager:lib/*' io.druid.cli.Main server middleManager;"

        echo "middle manager node startup command is $COMMAND"

        counter=counter+1
    #ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "
            $COMMAND"
done
echo ""


#start coordinator FQDN
counter=0
echo "Setting up coordinator nodes:"
for  node in ${NEW_COORDINATOR_NODES//,/ }
do

        echo "Setting up $node ..."
        COMMAND=''

        COMMAND=$COMMAND" sudo sed -i '2s@.*@druid.port=$COORDINATOR_NODE_PORT@' $PATH_TO_DRUID_BIN/conf/druid/coordinator/runtime.properties;"   
        COMMAND=$COMMAND" cd $PATH_TO_DRUID_BIN && screen -d -m sudo java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -Dlogfilename=coordinator-$counter -classpath 'conf/druid/_common:conf/druid/coordinator:lib/*' io.druid.cli.Main server coordinator;"  

        echo "Coordinator node startup command is $COMMAND"

        counter=counter+1
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

        COMMAND=$COMMAND" sudo sed -i '2s@.*@druid.port=$HISTORICAL_NODE_PORT@' $PATH_TO_DRUID_BIN/conf/druid/historical/runtime.properties;"       
        COMMAND=$COMMAND" cd $PATH_TO_DRUID_BIN && screen -d -m sudo java -Xmx256m -XX:MaxDirectMemorySize=$MAX_DIRECT_MEMORY_SIZE -Duser.timezone=UTC -Dfile.encoding=UTF-8 -Dlogfilename=historical-$counter -classpath 'conf/druid/_common:conf/druid/historical:lib/*' io.druid.cli.Main server historical;"

        echo "historical node startup command is $COMMAND"
        let counter=counter+1

	#ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "
            $COMMAND"
done
echo ""

#setup broker FQDN
counter=0
echo "Setting up broker nodes:"
for  node in ${NEW_BROKER_NODES//,/ }
do

        echo "Setting up $node ..."
        COMMAND=''

        COMMAND=$COMMAND" sudo sed -i '2s@.*@druid.port=$BROKER_NODE_PORT@' $PATH_TO_DRUID_BIN/conf/druid/broker/runtime.properties;"
        COMMAND=$COMMAND" cd $PATH_TO_DRUID_BIN && screen -d -m sudo java -Xmx256m -XX:MaxDirectMemorySize=$MAX_DIRECT_MEMORY_SIZE -Duser.timezone=UTC -Dlogfilename=broker-$counter -Dfile.encoding=UTF-8 -classpath 'conf/druid/_common:conf/druid/broker:lib/*' io.druid.cli.Main server broker;"

        echo "Broker node startup command is $COMMAND"
        counter=counter+1
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
        COMMAND=$COMMAND" sudo sed -i '26s@.*@log4j.appender.kafkaAppender.File=$LOG_FILE/kafkalogs/server.log@' $PATH_TO_KAFKA/config/log4j.properties;"
        COMMAND=$COMMAND" sudo sed -i '32s@.*@log4j.appender.stateChangeAppender.File=$LOG_FILE/kafkalogs/state-change.log@' $PATH_TO_KAFKA/config/log4j.properties;"
        COMMAND=$COMMAND" sudo sed -i '38s@.*@log4j.appender.requestAppender.File=$LOG_FILE/kafkalogs/kafka-request.log@' $PATH_TO_KAFKA/config/log4j.properties;"
        COMMAND=$COMMAND" sudo sed -i '44s@.*@log4j.appender.cleanerAppender.File=$LOG_FILE/kafkalogs/log-cleaner.log@' $PATH_TO_KAFKA/config/log4j.properties;"
        COMMAND=$COMMAND" sudo sed -i '50s@.*@log4j.appender.controllerAppender.File=$LOG_FILE/kafkalogs/controller.log@' $PATH_TO_KAFKA/config/log4j.properties;"
        COMMAND=$COMMAND" sudo sed -i '39s@.*@          \"zookeeper.connect\": \"$KAFKA_NODE_HOST:$KAFKA_ZOOKEEPER_PORT\",@' $SPEC_FILE;"
        COMMAND=$COMMAND" sudo sed -i '25s@.*@port=$KAFKA_NODE_PORT@' $PATH_TO_KAFKA/config/server.properties;"
        COMMAND=$COMMAND" sudo sed -i '28s@.*@host.name=$KAFKA_NODE_HOST@' $PATH_TO_KAFKA/config/server.properties;"
        COMMAND=$COMMAND" sudo sed -i '33s@.*@advertised.host.name=$KAFKA_NODE_HOST@' $PATH_TO_KAFKA/config/server.properties;"
        COMMAND=$COMMAND" sudo sed -i '37s@.*@advertised.port=$KAFKA_NODE_PORT@' $PATH_TO_KAFKA/config/server.properties;"
        COMMAND=$COMMAND" sudo sed -i '118s@.*@zookeeper.connect=$KAFKA_NODE_HOST:$KAFKA_ZOOKEEPER_PORT@' $PATH_TO_KAFKA/config/server.properties;"
        COMMAND=$COMMAND" sudo sed -i '58s@.*@log.dirs=$LOG_FILE/kafkalogs@' $PATH_TO_KAFKA/config/server.properties;"
        COMMAND=$COMMAND" sudo sed -i '18s@.*@clientPort=$KAFKA_ZOOKEEPER_PORT@' $PATH_TO_KAFKA/config/zookeeper.properties;"
        COMMAND=$COMMAND" screen -d -m sudo ./bin/zookeeper-server-start.sh config/zookeeper.properties;"
        COMMAND=$COMMAND" screen -d -m sudo ./bin/kafka-server-start.sh config/server.properties;"
        COMMAND=$COMMAND" sleep 2;"
        COMMAND=$COMMAND" ./bin/kafka-topics.sh --create --zookeeper $KAFKA_NODE_HOST:$KAFKA_ZOOKEEPER_PORT --replication-factor 1 --partitions 1 --topic $KAFKA_TOPIC;"

        echo "kafka node startup command is $COMMAND"

    #ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "
            $COMMAND"
done
echo ""

counter=0
echo "Setting up realtime nodes:"
for  node in ${NEW_REALTIME_NODE//,/ }
do

        echo "Setting up $node ..."
        COMMAND=''

        COMMAND=$COMMAND" sudo rm -rf $PATH_TO_DRUID_BIN/conf/druid/realtime;"
        COMMAND=$COMMAND" mkdir $PATH_TO_DRUID_BIN/conf/druid/realtime;"
        COMMAND=$COMMAND" sudo cat $PATH_TO_SOURCE/scripts/deployment/runtime.properties > $PATH_TO_DRUID_BIN/conf/druid/realtime/runtime.properties;"
        COMMAND=$COMMAND" sudo sed -i '2s@.*@druid.port=$REALTIME_NODE_PORT@' $PATH_TO_DRUID_BIN/conf/druid/realtime/runtime.properties;"
        COMMAND=$COMMAND" cd $PATH_TO_DRUID_BIN && screen -d -m sudo java -Xmx512m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -XX:MaxDirectMemorySize=$MAX_DIRECT_MEMORY_SIZE -Dlogfilename=realtime-$counter -Ddruid.realtime.specFile=$SPEC_FILE -classpath 'conf/druid/_common:conf/druid/realtime:lib/*' io.druid.cli.Main server realtime;"
        echo "Realtime node startup command is $COMMAND"

        counter=counter+1
    #ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "
            $COMMAND"
done
echo ""
