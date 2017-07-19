#!/bin/bash -e

seconds=$(date +"%S")
if [ $seconds -lt 58 ]
then
        ((duration=58-seconds))
else
        ((duration=58+60-seconds))
fi
#echo Waiting for $duration secs before ingesting and running workload
sleep $duration

( cd scripts/workloadgenerator ; ./ingestAndRunWorkloadGen_raina4.sh node-15 node-16 node-17 node-18 node-19 node-20 node-21 )

screen -ls

