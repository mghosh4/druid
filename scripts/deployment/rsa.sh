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

#generate keys for passwordless ssh
echo -ne '\n' | ssh-keygen;
for node in ${NEW_COORDINATOR_NODES//,/ }
do
    ssh-copy-id -i ~/.ssh/id_rsa.pub $node;
done
for node in ${NEW_HISTORICAL_NODES//,/ }
do
    ssh-copy-id -i ~/.ssh/id_rsa.pub $node;
done
for node in ${NEW_BROKER_NODES//,/ }
do
    ssh-copy-id -i ~/.ssh/id_rsa.pub $node;
done
for node in ${NEW_REALTIME_NODE//,/ }
do
    ssh-copy-id -i ~/.ssh/id_rsa.pub $node;
done
for node in ${NEW_ZOOKEEPER_NODES//,/ }
do
    ssh-copy-id -i ~/.ssh/id_rsa.pub $node;
done
for node in ${NEW_OVERLORD_NODES//,/ }
do
    ssh-copy-id -i ~/.ssh/id_rsa.pub $node;
done
for node in ${NEW_MIDDLE_MANAGER_NODES//,/ }
do
    ssh-copy-id -i ~/.ssh/id_rsa.pub $node;
done
for node in ${NEW_MYSQL_NODES//,/ }
do
    ssh-copy-id -i ~/.ssh/id_rsa.pub $node;
done
for node in ${NEW_KAFKA_NODES//,/ }
do
    ssh-copy-id -i ~/.ssh/id_rsa.pub $node;
done