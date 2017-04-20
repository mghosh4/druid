#!/bin/sh

for node in "$@"
do
    COMMAND="cd /proj/DCSQ/mghosh4/druid/scripts/workloadgenerator;"
    COMMAND=$COMMAND" screen -d -m python Run.py Config/getafix.conf http://${node}.asterix.dcsq.emulab.net:8082 workloadgenerator-${node}.log;"
    echo $COMMAND
    ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "$COMMAND"
    sleep 1
done
