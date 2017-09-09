#!/bin/sh

COMMAND="cd /proj/DCSQ/getafix/druid/scripts/ingestion/;"
COMMAND=$COMMAND" screen -d -m python ingestion.py getafix.aws.conf;"
echo $COMMAND
ssh -i ~/druid.pem -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no node-1 "$COMMAND"

sleep 60

./runMultiWorkloadGen_getafix.sh $@

sleep 60

COMMAND="cd /proj/DCSQ/getafix/druid/scripts/workloadgenerator/;"
COMMAND=$COMMAND" screen -d -m bash checkAndLog_getafix.sh $@;"
echo $COMMAND
ssh -i ~/druid.pem -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no node-1 "$COMMAND"
