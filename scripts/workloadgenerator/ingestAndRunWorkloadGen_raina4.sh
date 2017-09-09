#!/bin/sh

COMMAND="cd /proj/DCSQ/raina4/druid/scripts/ingestion/;"
COMMAND=$COMMAND" screen -d -m python ingestion.py getafix.conf;"
echo $COMMAND
ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no node-1 "$COMMAND"

#Sleep for two minutes so that one segment is created and loaded before we start querying. Removed the sleep from workload generator
sleep 2m

./runMultiWorkloadGen_raina4.sh $@

COMMAND="cd /proj/DCSQ/raina4/druid/scripts/workloadgenerator/;"
COMMAND=$COMMAND" screen -d -m bash checkAndLog_raina4.sh $@;"
echo $COMMAND
ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no node-1 "$COMMAND"
