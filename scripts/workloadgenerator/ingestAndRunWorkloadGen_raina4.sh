#!/bin/sh

COMMAND="cd /proj/DCSQ/raina4/potion_exp/druid/scripts/ingestion/;"
COMMAND=$COMMAND" screen -d -m python ingestion.py getafix.conf;"
echo $COMMAND
ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no node-1 "$COMMAND"

sleep 60

./runMultiWorkloadGen_raina4.sh $@

sleep 60

COMMAND="cd /proj/DCSQ/raina4/potion_exp/druid/scripts/workloadgenerator/;"
COMMAND=$COMMAND" screen -d -m bash checkAndLog_raina4.sh $@;"
echo $COMMAND
ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no node-1 "$COMMAND"
