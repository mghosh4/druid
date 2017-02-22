#!/bin/sh

COMMAND="cd /proj/DCSQ/mghosh4/druid/scripts/ingestion/;"
COMMAND=$COMMAND" screen -d -m python ingestion.py getafix.conf;"
echo $COMMAND
ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no node-1 "$COMMAND"

sleep 3

./runMultiWorkloadGen.sh $@
