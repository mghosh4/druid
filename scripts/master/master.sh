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

config_generator ()
{
	IFS=',' read -ra ADDR <<< "$REPLICATION"
	for i in "${ADDR[@]}"; do
		cp $PATH_TO_DRUID/scripts/deployment/config/common.conf .;
		mv $PATH_TO_DRUID/scripts/master/common.conf $PATH_TO_DRUID/scripts/master/deployment_$i.conf;
		sudo sed -i '36s@.*@REPLICATION_SCHEME=$REPLICATION@' $PATH_TO_DRUID/scripts/master/deployment_$i.conf;
	done

	cp $PATH_TO_DRUID/scripts/ingestion/ingestion.conf .;
	cp $PATH_TO_DRUID/scripts/workloadgenerator/Config/query.conf .;
	mv $PATH_TO_DRUID/scripts/master/query.conf $PATH_TO_DRUID/scripts/master/workloadgenerator.conf;
	sudo sed -i '5s@.*@runtime=$EXPERIMENT_LENGTH@' $PATH_TO_DRUID/scripts/master/ingestion.conf;
	sudo sed -i '18s@*@runtime=$EXPERIMENT_LENGTH@' $PATH_TO_DRUID/scripts/master/workloadgenerator.conf;
}

run_experiment ()
{
	if [ "$BATCH" == "TRUE" ] ; then
		echo "DEPLOYMENT: "
        	COMMAND=''
	        COMMAND=$COMMAND" cd $PATH_TO_DRUID/scripts/deployment;"
	        COMMAND=$COMMAND" ./start_druid.sh $1;"

       	 #ssh to node and run command
        	ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no tkao4@node-8.druidthomas.dcsq.emulab.net "
    	        $COMMAND"
    	echo ""

    	echo "INGESTING:"
	        COMMAND=''
    	    COMMAND=$COMMAND" cd $PATH_TO_DRUID/scripts/ingestion;"
    	    COMMAND=$COMMAND" python ingestion.py $2;"

    	    #ssh to node and run command
    	    ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no tkao4@node-8.druidthomas.dcsq.emulab.net "
    	        $COMMAND"
    	echo ""

    	echo "WORKLOAD GENERATOR:"
    	    COMMAND=''
        	COMMAND=$COMMAND" cd $PATH_TO_DRUID/scripts/workloadgenerator;"
        	COMMAND=$COMMAND" python Run.py $3;"

        	#ssh to node and run command
        	ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no tkao4@node-8.druidthomas.dcsq.emulab.net "
        	    $COMMAND"
    	echo ""

    	echo "LOG READER:"
    	    COMMAND=''
    	    COMMAND=$COMMAND" cd $PATH_TO_DRUID/scripts/logreader;"
    	    COMMAND=$COMMAND" python StatsGenerator.py getafix.conf;"


    	    #ssh to node and run command
    	    ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no tkao4@node-8.druidthomas.dcsq.emulab.net "
    	        $COMMAND"
    	echo ""

    	echo "PLOTTING:"
    	    COMMAND=''
    	    COMMAND=$COMMAND" cd $PATH_TO_DRUID/scripts/plotscript;"
        	COMMAND=$COMMAND" python plot.py plotconfig.conf;"


        	#ssh to node and run command
    	    ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no tkao4@node-8.druidthomas.dcsq.emulab.net "
        	    $COMMAND"
    	echo ""
    else
    echo "DEPLOYMENT: "
        COMMAND=''
	    COMMAND=$COMMAND" cd $PATH_TO_DRUID/scripts/deployment;"
	    COMMAND=$COMMAND" ./start_druid.sh $1;"

        #ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no tkao4@node-8.druidthomas.dcsq.emulab.net "
            $COMMAND"
    echo ""

    echo "INGESTING AND WORKLOAD GENERATOR:"
        COMMAND=''
        COMMAND=$COMMAND" cd $PATH_TO_DRUID/scripts/ingestion;"
    	COMMAND=$COMMAND" python ingestion.py $2;"
        COMMAND=$COMMAND" cd $PATH_TO_DRUID/scripts/workloadgenerator;"
        COMMAND=$COMMAND" nohup python Run.py $3 &;"

        #ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no tkao4@node-8.druidthomas.dcsq.emulab.net "
            $COMMAND"
    echo ""

    echo "LOG READER:"
        COMMAND=''
        COMMAND=$COMMAND" cd $PATH_TO_DRUID/scripts/logreader;"
        COMMAND=$COMMAND" python StatsGenerator.py getafix.conf;"


    #ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no tkao4@node-8.druidthomas.dcsq.emulab.net "
            $COMMAND"
    echo ""

    echo "PLOTTING:"
        COMMAND=''
        COMMAND=$COMMAND" cd $PATH_TO_DRUID/scripts/plotscripts;"
        COMMAND=$COMMAND" python plot.py plotconfig.conf;"


        #ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no tkao4@node-8.druidthomas.dcsq.emulab.net "
            $COMMAND"
    echo ""
    fi

    echo "STOPPING:"
        COMMAND=''
        COMMAND=$COMMAND" cd $PATH_TO_DRUID/scripts/deployment;"
	    COMMAND=$COMMAND" ./stop_druid.sh -h $1;"
	    COMMAND=$COMMAND" rm $1 $2 $3;"


        #ssh to node and run command
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no tkao4@node-8.druidthomas.dcsq.emulab.net "
            $COMMAND"
    echo ""
}

config_generator
IFS=',' read -ra ADDR <<< "$REPLICATION"
	for i in "${ADDR[@]}"; do
		run_experiment $PATH_TO_DRUID/scripts/master/deployment_$i.conf $PATH_TO_DRUID/scripts/master/ingestion.conf $PATH_TO_DRUID/scripts/master/workloadgenerator.conf;
	done
