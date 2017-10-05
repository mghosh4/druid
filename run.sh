#!/bin/bash -e

seconds=$(date +"%S")
if [ $seconds -lt 55 ]
then
        ((duration=55-seconds))
else
        ((duration=55+60-seconds))
fi
#echo Waiting for $duration secs before ingesting and running workload
sleep $duration

( cd scripts/workloadgenerator ; ./ingestAndRunWorkloadGen_getafix.sh $@ )

sleep 3

screen -ls
