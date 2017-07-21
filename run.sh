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

( cd scripts/workloadgenerator ; ./ingestAndRunWorkloadGen_mghosh4.sh $@ )

sleep 3

screen -ls

