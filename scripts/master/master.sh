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

curl -i -H "Accept: application/json" -H "Content-Type:application/json" -X POST --data '{"account":{"email":"'"$email"'","screenName":"'"$screenName"'","type":"'"$theType"'","passwordSettings":{"password":"'"$password"'","passwordConfirm":"'"$password"'"}},"firstName":"'"$firstName"'","lastName":"'"$lastName"'","middleName":"'"$middleName"'","locale":"'"$locale"'","registrationSiteId":"'"$registrationSiteId"'","receiveEmail":"'"$receiveEmail"'","dateOfBirth":"'"$dob"'","mobileNumber":"'"$mobileNumber"'","gender":"'"$gender"'","fuelActivationDate":"'"$fuelActivationDate"'","postalCode":"'"$postalCode"'","country":"'"$country"'","city":"'"$city"'","state":"'"$state"'","bio":"'"$bio"'","jpFirstNameKana":"'"$jpFirstNameKana"'","jpLastNameKana":"'"$jpLastNameKana"'","height":"'"$height"'","weight":"'"$weight"'","distanceUnit":"MILES","weightUnit":"POUNDS","heightUnit":"FT/INCHES"}' "https://xxx:xxxxx@xxxx-www.xxxxx.com/xxxxx/xxxx/xxxx"

#deploy
cd $DEPLOYMENT_DIRECTORY
sh ./start_druid.sh config/common.conf


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
if [ BATCH -e "TRUE" ]

    echo "INGESTING:"
    for  node in ${NEW_KAFKA_NODES//,/ }
    do
        COMMAND=''
        COMMAND="cd $INGESTION_DIRECTORY;"
        COMMAND="python ingestion.py -n$INGESTION_TIME $INGESTION_CONF;"

        #ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "
            $COMMAND"
    done
    echo ""

    sleep 65m;

    echo "WORKLOAD GENERATOR:"
    for  node in ${NEW_KAFKA_NODES//,/ }
    do
        COMMAND=''
        COMMAND="cd $WORKLOAD_GENERATOR_DIRECTORY;"
        COMMAND="python Run.py $WORKLOAD_GENERATOR_CONF;"

        #ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "
            $COMMAND"
    done
    echo ""

    sleep 90m;

    echo "LOG READER:"
    for  node in ${NEW_KAFKA_NODES//,/ }
    do
        COMMAND=''
        COMMAND="cd $LOGREADER_DIRECTORY;"
        COMMAND="python logreader.py $LOGREADER_CONF;"


        #ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "
            $COMMAND"
    done
    echo ""

    sleep 2m;

    echo "PLOTTING:"
    for  node in ${NEW_KAFKA_NODES//,/ }
    do
        COMMAND=''
        COMMAND="cd $PLOT_DIRECTORY;"
        COMMAND="python plot.py $PLOT_CONF;"


        #ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "
            $COMMAND"
    done
    echo ""

else

    echo "INGESTING:"
    for  node in ${NEW_KAFKA_NODES//,/ }
    do
        COMMAND=''
        COMMAND="cd $INGESTION_DIRECTORY;"
        COMMAND="python ingestion.py -n$INGESTION_TIME $INGESTION_CONF;"
        COMMAND="cd $WORKLOAD_GENERATOR_DIRECTORY;"
        COMMAND="python Run.py $WORKLOAD_GENERATOR_CONF;"

        #ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "
            $COMMAND"
    done
    echo ""

    sleep 90m;

    echo "LOG READER:"
    for  node in ${NEW_KAFKA_NODES//,/ }
    do
        COMMAND=''
        COMMAND="cd $LOGREADER_DIRECTORY;"
        COMMAND="python logreader.py $LOGREADER_CONF;"


    #ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "
            $COMMAND"
    done
    echo ""

    sleep 2m;

    echo "PLOTTING:"
    for  node in ${NEW_KAFKA_NODES//,/ }
    do
        COMMAND=''
        COMMAND="cd $PLOT_DIRECTORY;"
        COMMAND="python plot.py $PLOT_CONF;"


        #ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "
            $COMMAND"
    done
    echo ""