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

#deploy
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
#ingestion and workload generator and logreader and plot script
if [ $BATCH = "TRUE" ]; then

    echo "DEPLOYMENT: "
    for  node in ${NEW_KAFKA_NODES//,/ }
    do
        COMMAND=''
        COMMAND=$COMMAND" cd $DEPLOYMENT_DIRECTORY;"
        COMMAND=$COMMAND" sudo sed -i '3s@.*druid.replicator.policy=$RULE@' $PATH_TO_DRUID_BIN/conf/druid/coordinator/runtime.properties;"
        COMMAND=$COMMAND" sudo sed -i '2s@.*@EXPERIMENT=$EXPERIMENT@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '4s@.*@IP=$IP@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '5s@.*@AWS=$AWS@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '7s@.*@FQDN=$FQDN@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '11s@.*@PROJ=$PROJ@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '9s@.*@ENV=$ENV@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '13s@.*@USER_NAME=$USER_NAME@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '15s@.*@KAFKA_TOPIC=$KAFKA_TOPIC@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '18s@.*@PATH_TO_SOURCE=$PATH_TO_SOURCE@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '20s@.*@PATH_TO_DRUID_BIN=$PATH_TO_DRUID_BIN@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '22s@.*@PATH_TO_ZOOKEEPER=$PATH_TO_ZOOKEEPER@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '24s@.*@PATH_TO_KAFKA=$PATH_TO_KAFKA@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '26s@.*@SPEC_FILE=$SPEC_FILE@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '29s@.*@MAX_DIRECT_MEMORY=$MAX_DIRECT_MEMORY@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '32s@.*@LOG_FILE=$LOG_FILE@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '35s@.*@COMMON_LOG4J2=$COMMON_LOG4J2@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '39s@.*@COORDINATOR_NODE=$COORDINATOR_NODE@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '40s@.*@COORDINATOR_NODE_HOST=$COORDINATOR_NODE_HOST@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '41s@.*@COORDINATOR_NODE_PORT=$COORDINATOR_NODE_PORT@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '43s@.*@HISTORICAL_NODES=$HISTORICAL_NODES@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '44s@.*@HISTORICAL_NODE_HOST=$HISTORICAL_NODE_HOST@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '45s@.*@HISTORICAL_NODE_PORT=$HISTORICAL_NODE_PORT@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '47s@.*@BROKER_NODE=$BROKER_NODE@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '48s@.*@BROKER_NODE_HOST=$BROKER_NODE_HOST@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '49s@.*@BROKER_NODE_PORT=$BROKER_NODE_PORT@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '51s@.*@REALTIME_NODE=$REALTIME_NODE@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '52s@.*@REALTIME_NODE_HOST=$REALTIME_NODE_HOST@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '53s@.*@REALTIME_NODE_PORT=$REALTIME_NODE_PORT@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '55s@.*@ZOOKEEPER_NODE=$ZOOKEEPER_NODE@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '56s@.*@ZOOKEEPER_NODE_HOST=$ZOOKEEPER_NODE_HOST@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '57s@.*@ZOOKEEPER_NODE_PORT=$ZOOKEEPER_NODE_PORT@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '60s@.*@MYSQL_NODE_HOST=$MYSQL_NODE_HOST@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '61s@.*@MYSQL_NODE_PORT=$MYSQL_NODE_PORT@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '63s@.*@OVERLORD_NODE=$OVERLORD_NODE@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '64s@.*@OVERLORD_NODE_HOST=$OVERLORD_NODE_HOST@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '55s@.*@OVERLORD_NODE_PORT=$OVERLORD_NODE_PORT@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '67s@.*@MIDDLE_MANAGER_NODE=$MIDDLE_MANAGER_NODE@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '68s@.*@MIDDLE_MANAGER_NODE_HOST=$MIDDLE_MANAGER_NODE_HOST@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '69s@.*@MIDDLE_MANAGER_NODE_PORT=$MIDDLE_MANAGER_NODE_PORT@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '71s@.*@KAFKA_NODE=$KAFKA_NODE@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '72s@.*@KAFKA_NODE_HOST=$KAFKA_NODE_HOST@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '73s@.*@KAFKA_NODE_PORT=$KAFKA_NODE_PORT@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '74s@.*@KAFKA_ZOOKEEPER_PORT=$KAFKA_ZOOKEEPER_PORT@' $DEPLOYMENT_CONF;"

        COMMAND=$COMMAND" sh ./start_druid.sh $DEPLOYMENT_CONF;"

        #ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "
            $COMMAND"
    done
    echo ""

    echo "INGESTING:"
    for  node in ${NEW_KAFKA_NODES//,/ }
    do
        COMMAND=''
        COMMAND=$COMMAND" sudo sed -i '1s@.*@kafkapath=$kafkapath@' $INGESTION_CONF;"
        COMMAND=$COMMAND" sudo sed -i '2s@.*@datatype=$datatype@' $INGESTION_CONF;"
        COMMAND=$COMMAND" sudo sed -i '3s@.*@delimiter=$delimiter@' $INGESTION_CONF;"
        COMMAND=$COMMAND" sudo sed -i '4s@.*@datafilepath=$datafilepath@' $INGESTION_CONF;"
        COMMAND=$COMMAND" sudo sed -i '5s@.*@kafkatopic=$kafkatopic@' $INGESTION_CONF;"
        COMMAND=$COMMAND" sudo sed -i '6s@.*@kafkahost=$kafkahost@' $INGESTION_CONF;"
        COMMAND=$COMMAND" cd $INGESTION_DIRECTORY;"
        COMMAND=$COMMAND" python ingestion.py -n1 $INGESTION_CONF;"
        COMMAND=$COMMAND" python ingestion.py -n$INGESTION_TIME $INGESTION_CONF;"

        #ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "
            $COMMAND"
    done
    echo ""

    echo "WORKLOAD GENERATOR:"
    for  node in ${NEW_KAFKA_NODES//,/ }
    do
        COMMAND=''
        COMMAND=$COMMAND" sudo sed -i '1s@.*@datasource=$datasource@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '2s@.*@granularity=$granularity@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '3s@.*@aggregations=$aggregations@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '4s@.*@dimension=$dimension@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '5s@.*@metric=$metric@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '6s@.*@threshold=$threshold@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '7s@.*@filter=$filter@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '8s@.*@postaggregations=$postaggregations@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '9s@.*@accessdistribution=$accessdistribution@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '10s@.*@perioddistribution=$perioddistribution@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '11s@.*@querytype=$querytype@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '12s@.*@brokernodeurl=$brokernodeurl@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '13s@.*@brokerendpoint=$brokerendpoint@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '14s@.*@numqueries=$numqueries@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '15s@.*@opspersecond=$opspersecond@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '16s@.*@queryruntime=$queryruntime@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '17s@.*@numcores=$numcores@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '18s@.*@runtime=$runtime@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" cd $WORKLOAD_GENERATOR_DIRECTORY;"
        COMMAND=$COMMAND" python Run.py $WORKLOAD_GENERATOR_CONF;"

        #ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "
            $COMMAND"
    done
    echo ""

    echo "LOG READER:"
    for  node in ${NEW_KAFKA_NODES//,/ }
    do
        COMMAND=''
        COMMAND=$COMMAND" sudo sed -i '1s@.*@HISTORICAL_METRIC=$HISTORICAL_METRIC@' $LOGREADER_CONF;"
        COMMAND=$COMMAND" sudo sed -i '2s@.*@COORDINATOR_METRIC=$COORDINATOR_METRIC@' $LOGREADER_CONF;"
        COMMAND=$COMMAND" sudo sed -i '3s@.*@BROKER_METRIC=$BROKER_METRIC@' $LOGREADER_CONF;"
        COMMAND=$COMMAND" sudo sed -i '4s@.*@LOG_PATH=$LOG_PATH@' $LOGREADER_CONF;"
        COMMAND=$COMMAND" sudo sed -i '5s@.*@NUM_HISTORICAL_NODES=$NUM_HISTORICAL_NODES@' $LOGREADER_CONF;"
        COMMAND=$COMMAND" sudo sed -i '6s@.*@NUM_BROKER_NODES=$NUM_BROKER_NODES@' $LOGREADER_CONF;"
        COMMAND=$COMMAND" sudo sed -i '7s@.*@NUM_COORDINATOR_NODES=$NUM_COORDINATOR_NODES@' $LOGREADER_CONF;"
        COMMAND=$COMMAND" sudo sed -i '8s@.*@RESULT_FOLDER=$RESULT_FOLDER@' $LOGREADER_CONF;"
        COMMAND=$COMMAND" cd $LOGREADER_DIRECTORY;"
        COMMAND=$COMMAND" python logreader.py $LOGREADER_CONF;"


        #ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "
            $COMMAND"
    done
    echo ""

    echo "PLOTTING:"
    for  node in ${NEW_KAFKA_NODES//,/ }
    do
        COMMAND=''
        COMMAND=$COMMAND" sudo sed -i '1s@.*@historicalmetrics=$historicalmetrics@' $PLOT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '2s@.*@brokermetrics=$brokermetrics@' $PLOT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '3s@.*@coordinatormetrics=$coordinatormetrics@' $PLOT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '4s@.*@throughputgraph=$throughputgraph@' $PLOT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '5s@.*@pathtoresults=$pathtoresults@' $PLOT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '6s@.*@numberofhistorical=$numberofhistorical@' $PLOT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '7s@.*@numberofbroker=$numberofbroker@' $PLOT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '8s@.*@numberofcoordinator=$numberofcoordinator@' $PLOT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '9s@.*@distributions=$distributions@' $PLOT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '10s@.*@druidversion=$druidversion@' $PLOT_CONF;"
        COMMAND=$COMMAND" cd $PLOT_DIRECTORY;"
        COMMAND=$COMMAND" python plot.py $PLOT_CONF;"


        #ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "
            $COMMAND"
    done
    echo ""

else

    echo "DEPLOYMENT: "
    for  node in ${NEW_KAFKA_NODES//,/ }
    do
        COMMAND=''
        COMMAND=$COMMAND" cd $DEPLOYMENT_DIRECTORY;"
        COMMAND=$COMMAND" sudo sed -i '3s@.*druid.replicator.policy=$RULE@' $PATH_TO_DRUID_BIN/conf/druid/coordinator/runtime.properties;"
        COMMAND=$COMMAND" sudo sed -i '2s@.*@EXPERIMENT=$EXPERIMENT@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '4s@.*@IP=$IP@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '5s@.*@AWS=$AWS@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '7s@.*@FQDN=$FQDN@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '11s@.*@PROJ=$PROJ@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '9s@.*@ENV=$ENV@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '13s@.*@USER_NAME=$USER_NAME@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '15s@.*@KAFKA_TOPIC=$KAFKA_TOPIC@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '18s@.*@PATH_TO_SOURCE=$PATH_TO_SOURCE@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '20s@.*@PATH_TO_DRUID_BIN=$PATH_TO_DRUID_BIN@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '22s@.*@PATH_TO_ZOOKEEPER=$PATH_TO_ZOOKEEPER@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '24s@.*@PATH_TO_KAFKA=$PATH_TO_KAFKA@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '26s@.*@SPEC_FILE=$SPEC_FILE@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '29s@.*@MAX_DIRECT_MEMORY=$MAX_DIRECT_MEMORY@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '32s@.*@LOG_FILE=$LOG_FILE@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '35s@.*@COMMON_LOG4J2=$COMMON_LOG4J2@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '39s@.*@COORDINATOR_NODE=$COORDINATOR_NODE@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '40s@.*@COORDINATOR_NODE_HOST=$COORDINATOR_NODE_HOST@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '41s@.*@COORDINATOR_NODE_PORT=$COORDINATOR_NODE_PORT@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '43s@.*@HISTORICAL_NODES=$HISTORICAL_NODES@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '44s@.*@HISTORICAL_NODE_HOST=$HISTORICAL_NODE_HOST@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '45s@.*@HISTORICAL_NODE_PORT=$HISTORICAL_NODE_PORT@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '47s@.*@BROKER_NODE=$BROKER_NODE@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '48s@.*@BROKER_NODE_HOST=$BROKER_NODE_HOST@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '49s@.*@BROKER_NODE_PORT=$BROKER_NODE_PORT@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '51s@.*@REALTIME_NODE=$REALTIME_NODE@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '52s@.*@REALTIME_NODE_HOST=$REALTIME_NODE_HOST@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '53s@.*@REALTIME_NODE_PORT=$REALTIME_NODE_PORT@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '55s@.*@ZOOKEEPER_NODE=$ZOOKEEPER_NODE@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '56s@.*@ZOOKEEPER_NODE_HOST=$ZOOKEEPER_NODE_HOST@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '57s@.*@ZOOKEEPER_NODE_PORT=$ZOOKEEPER_NODE_PORT@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '60s@.*@MYSQL_NODE_HOST=$MYSQL_NODE_HOST@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '61s@.*@MYSQL_NODE_PORT=$MYSQL_NODE_PORT@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '63s@.*@OVERLORD_NODE=$OVERLORD_NODE@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '64s@.*@OVERLORD_NODE_HOST=$OVERLORD_NODE_HOST@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '55s@.*@OVERLORD_NODE_PORT=$OVERLORD_NODE_PORT@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '67s@.*@MIDDLE_MANAGER_NODE=$MIDDLE_MANAGER_NODE@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '68s@.*@MIDDLE_MANAGER_NODE_HOST=$MIDDLE_MANAGER_NODE_HOST@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '69s@.*@MIDDLE_MANAGER_NODE_PORT=$MIDDLE_MANAGER_NODE_PORT@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '71s@.*@KAFKA_NODE=$KAFKA_NODE@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '72s@.*@KAFKA_NODE_HOST=$KAFKA_NODE_HOST@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '73s@.*@KAFKA_NODE_PORT=$KAFKA_NODE_PORT@' $DEPLOYMENT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '74s@.*@KAFKA_ZOOKEEPER_PORT=$KAFKA_ZOOKEEPER_PORT@' $DEPLOYMENT_CONF;"

        COMMAND=$COMMAND" sh ./start_druid.sh $DEPLOYMENT_CONF;"

        #ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "
            $COMMAND"
    done
    echo ""

    echo "INGESTING:"
    for  node in ${NEW_KAFKA_NODES//,/ }
    do
        COMMAND=''
        COMMAND=$COMMAND" sudo sed -i '1s@.*@kafkapath=$kafkapath@' $INGESTION_CONF;"
        COMMAND=$COMMAND" sudo sed -i '2s@.*@datatype=$datatype@' $INGESTION_CONF;"
        COMMAND=$COMMAND" sudo sed -i '3s@.*@delimiter=$delimiter@' $INGESTION_CONF;"
        COMMAND=$COMMAND" sudo sed -i '4s@.*@datafilepath=$datafilepath@' $INGESTION_CONF;"
        COMMAND=$COMMAND" sudo sed -i '5s@.*@kafkatopic=$kafkatopic@' $INGESTION_CONF;"
        COMMAND=$COMMAND" sudo sed -i '6s@.*@kafkahost=$kafkahost@' $INGESTION_CONF;"
        COMMAND="cd $INGESTION_DIRECTORY;"
        COMMAND=$COMMAND" python ingestion.py -n1 $INGESTION_CONF;"
        COMMAND="nohup python ingestion.py -n$INGESTION_TIME $INGESTION_CONF &;"
        COMMAND=$COMMAND" sudo sed -i '1s@.*@datasource=$datasource@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '2s@.*@granularity=$granularity@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '3s@.*@aggregations=$aggregations@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '4s@.*@dimension=$dimension@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '5s@.*@metric=$metric@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '6s@.*@threshold=$threshold@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '7s@.*@filter=$filter@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '8s@.*@postaggregations=$postaggregations@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '9s@.*@accessdistribution=$accessdistribution@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '10s@.*@perioddistribution=$perioddistribution@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '11s@.*@querytype=$querytype@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '12s@.*@brokernodeurl=$brokernodeurl@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '13s@.*@brokerendpoint=$brokerendpoint@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '14s@.*@numqueries=$numqueries@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '15s@.*@opspersecond=$opspersecond@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '16s@.*@queryruntime=$queryruntime@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '17s@.*@numcores=$numcores@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" sudo sed -i '18s@.*@runtime=$runtime@' $WORKLOAD_GENERATOR_CONF;"
        COMMAND=$COMMAND" cd $WORKLOAD_GENERATOR_DIRECTORY;"
        COMMAND=$COMMAND" nohup python Run.py $WORKLOAD_GENERATOR_CONF &;"

        #ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "
            $COMMAND"
    done
    echo ""

    echo "LOG READER:"
    for  node in ${NEW_KAFKA_NODES//,/ }
    do
        COMMAND=''
        COMMAND=$COMMAND" sudo sed -i '1s@.*@HISTORICAL_METRIC=$HISTORICAL_METRIC@' $LOGREADER_CONF;"
        COMMAND=$COMMAND" sudo sed -i '2s@.*@COORDINATOR_METRIC=$COORDINATOR_METRIC@' $LOGREADER_CONF;"
        COMMAND=$COMMAND" sudo sed -i '3s@.*@BROKER_METRIC=$BROKER_METRIC@' $LOGREADER_CONF;"
        COMMAND=$COMMAND" sudo sed -i '4s@.*@LOG_PATH=$LOG_PATH@' $LOGREADER_CONF;"
        COMMAND=$COMMAND" sudo sed -i '5s@.*@NUM_HISTORICAL_NODES=$NUM_HISTORICAL_NODES@' $LOGREADER_CONF;"
        COMMAND=$COMMAND" sudo sed -i '6s@.*@NUM_BROKER_NODES=$NUM_BROKER_NODES@' $LOGREADER_CONF;"
        COMMAND=$COMMAND" sudo sed -i '7s@.*@NUM_COORDINATOR_NODES=$NUM_COORDINATOR_NODES@' $LOGREADER_CONF;"
        COMMAND=$COMMAND" sudo sed -i '8s@.*@RESULT_FOLDER=$RESULT_FOLDER@' $LOGREADER_CONF;"
        COMMAND=$COMMAND" cd $LOGREADER_DIRECTORY;"
        COMMAND=$COMMAND" python logreader.py $LOGREADER_CONF;"


    #ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "
            $COMMAND"
    done
    echo ""

    echo "PLOTTING:"
    for  node in ${NEW_KAFKA_NODES//,/ }
    do
        COMMAND=''
        COMMAND=$COMMAND" sudo sed -i '1s@.*@historicalmetrics=$historicalmetrics@' $PLOT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '2s@.*@brokermetrics=$brokermetrics@' $PLOT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '3s@.*@coordinatormetrics=$coordinatormetrics@' $PLOT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '4s@.*@throughputgraph=$throughputgraph@' $PLOT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '5s@.*@pathtoresults=$pathtoresults@' $PLOT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '6s@.*@numberofhistorical=$numberofhistorical@' $PLOT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '7s@.*@numberofbroker=$numberofbroker@' $PLOT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '8s@.*@numberofcoordinator=$numberofcoordinator@' $PLOT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '9s@.*@distributions=$distributions@' $PLOT_CONF;"
        COMMAND=$COMMAND" sudo sed -i '10s@.*@druidversion=$druidversion@' $PLOT_CONF;"
        COMMAND=$COMMAND" cd $PLOT_DIRECTORY;"
        COMMAND=$COMMAND" python plot.py $PLOT_CONF;"


        #ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "
            $COMMAND"
    done
    echo ""
fi