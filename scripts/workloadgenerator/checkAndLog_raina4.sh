#!/bin/bash

while true
do
	flag=0
	for broker in "$@"
	do
        	RESULT=$(ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no ${broker} "ps -aef | grep 'python Run.py' | wc -l")
        	echo $RESULT
        	if (( $RESULT > 2)); then
			flag=1
			break
		fi
		
	done
	if (( $flag==0 )); then
		break
	fi
	sleep 3m
done

echo "Yeah!"
#run logreader
cd /proj/DCSQ/raina4/druid/scripts/deployment
./stop_druid.sh -s config/getafix.conf
cd /proj/DCSQ/raina4/druid/scripts/logreader
python StatsGenerator.py getafix.conf
