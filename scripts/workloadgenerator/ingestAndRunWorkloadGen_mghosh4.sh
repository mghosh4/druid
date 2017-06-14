#!/bin/sh

COMMAND="cd /proj/DCSQ/mghosh4/druid/scripts/ingestion/;"
COMMAND=$COMMAND" screen -d -m python ingestion.py getafix.conf;"
echo $COMMAND
ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no node-1 "$COMMAND"

sleep 1m

./runMultiWorkloadGen_mghosh4.sh $@

sleep 1m

COMMAND="cd /proj/DCSQ/mghosh4/druid/scripts/workloadgenerator/;"
COMMAND=$COMMAND" screen -d -m bash checkAndLog_mghosh4.sh $@;"
echo $COMMAND
ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no node-1 "$COMMAND"
