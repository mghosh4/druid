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

#Change the conf file based on experiment
cd /proj/DCSQ/mghosh4/druid/scripts/deployment
./stop_druid.sh -s config/getafix_10.conf

mv /proj/DCSQ/mghosh4/logs /proj/DCSQ/mghosh4/lastexplogs
./start_druid.sh config/getafix_10.conf

#run logreader
cd /proj/DCSQ/mghosh4/druid/scripts/logreader
python StatsGenerator.py getafix.conf

#mkdir /proj/DCSQ/mghosh4/analysis/logs/getafix_cml_coldstart_2b5hn_1
#cp /proj/DCSQ/mghosh4/logs/* /proj/DCSQ/mghosh4/analysis/logs/getafix_cml_coldstart_2b5hn_1/
