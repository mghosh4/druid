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

( cd scripts/workloadgenerator ; ./ingestAndRunWorkloadGen_raina4.sh node-19 node-20 node-21 node-22 node-23 node-24 )
screen -ls

