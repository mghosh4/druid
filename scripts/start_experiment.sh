#!/bin/bash

set x;


source experiment.conf;
cd ${SCRIPT_PATH};

#Start cluster:

${DEPLOYMENT_PATH}/druid_start.sh ${DEPLOYMENT_CONFIG};

sleep 15s;

#starting ingestion

python ${INGESTION_PATH}/ingestion.py -n${INGESTION_TIME} ${INGESTION_CONFIG};

sleep 30s;

#workload generator

python ${WORKLOAD_PATH}/Run.py ${WORKLOAD_CONFIG};


#collecting results
#http://www.cyberciti.biz/faq/linux-unix-formatting-dates-for-display/
NOW=$(date +"%m-%d-%Y~%H:%M");
export OUTPUT_FOLDER=${LOG_OUTPUT}/getafix_${NOW}/;
python ${LOGREADER_PATH}/logreader.py ${LOGREADER_CONFIG};



