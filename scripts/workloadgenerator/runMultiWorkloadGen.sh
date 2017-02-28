#!/bin/sh

NUM_HISTORICALS=25

COMMAND="cd /proj/DCSQ/lexu/druid/scripts/ingestion/;"
COMMAND=$COMMAND" screen -d -m python ingestion.py le.conf;"
echo $COMMAND
ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no node-1 "$COMMAND"

sleep 3

for node in "$@"
do
    COMMAND="cd /proj/DCSQ/lexu/druid/scripts/workloadgenerator;"
    COMMAND=$COMMAND" screen -d -m python Run.py Config/le_experiment.conf http://${node}.druid2.dcsq.emulab.net:8082 workloadgenerator-${node}.log;"
    echo $COMMAND
    ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "$COMMAND"
    sleep 1
done


./checkAndLog.sh "$@"
