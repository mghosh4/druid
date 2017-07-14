#!/bin/sh

for node in "$@"
do
    COMMAND="cd /proj/DCSQ/raina4/potion_exp/druid/scripts/workloadgenerator;"
    COMMAND=$COMMAND" screen -d -m python Run.py Config/getafix.conf http://${node}.getafixraina4.dcsq.emulab.net:8082 workloadgenerator-${node}.log;"
    echo $COMMAND
    ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "$COMMAND"
    sleep 1
done
