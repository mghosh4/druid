#!/bin/bash

while true
do
	flag=0
	for broker in "$@"
	do
        	RESULT=$(ssh -i ~/druid.pem -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no ${broker} "ps -aef | grep 'python Run.py' | wc -l")
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
cd /proj/DCSQ/getafix/druid/scripts/deployment
./stop_druid_aws.sh -s config/getafix.aws.conf
cd /proj/DCSQ/getafix/druid/scripts/logreader
python StatsGenerator.py getafix.aws.conf
