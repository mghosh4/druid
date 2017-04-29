#!/bin/bash

while true
do
	flag=0
	for broker in "$@"
	do
        	RESULT=$(ssh ${broker} "ps -aef | grep 'python Run.py' | wc -l")
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
cd /proj/DCSQ/mghosh4/druid/scripts/logreader
python StatsGenerator.py getafix.conf
