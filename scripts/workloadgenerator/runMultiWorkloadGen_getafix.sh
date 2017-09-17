#!/bin/sh

for node in "$@"
do
    COMMAND="cd /proj/DCSQ/getafix/druid/scripts/workloadgenerator;"
    COMMAND=$COMMAND" screen -d -m python Run.py Config/getafix.aws.conf http://${node}:8082 workloadgenerator-${node}.log;"
    echo $COMMAND
    ssh -i ~/druid.pem -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $node "$COMMAND"
    sleep 1
done
