#!/bin/bash -e

seconds=$(date +"%S")
if [ $seconds -lt 57 ]
then
	((duration=57-seconds))
else
	((duration=57+60-seconds))
fi
#echo Waiting for $duration secs before ingesting and running workload
sleep $duration

( cd scripts/workloadgenerator ; ./ingestAndRunWorkloadGen_raina4.sh node-19 node-20 )
screen -ls

