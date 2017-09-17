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
	sleep 15
done

echo "Yeah!"

#Change the conf file based on experiment
cd /proj/DCSQ/getafix/druid/scripts/deployment
./stop_druid.sh -s config/getafix.conf

#run logreader
cd /proj/DCSQ/getafix/druid/scripts/logreader
python StatsGenerator.py getafix.conf

