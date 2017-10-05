#!/bin/sh

COMMAND="cd /proj/DCSQ/getafix/druid/scripts/ingestion/;"
COMMAND=$COMMAND" screen -d -m python ingestion.py getafix.aws.conf;"
echo $COMMAND
ssh -i ~/druid.pem -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no node-1 "$COMMAND"

#Sleep for two minutes so that one segment is created and loaded before we start querying. Removed the sleep from workload generator
sleep 2m

./runMultiWorkloadGen_getafix.sh $@

COMMAND="cd /proj/DCSQ/getafix/druid/scripts/workloadgenerator/;"
COMMAND=$COMMAND" screen -d -m bash checkAndLog_getafix.sh $@;"
echo $COMMAND
ssh -i ~/druid.pem -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no node-1 "$COMMAND"
