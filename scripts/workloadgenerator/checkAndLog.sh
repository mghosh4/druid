#!/bin/bash

set x

flag=0

while true
do
	flag=0
	for broker in "$@"
	do
        	#RESULT="ssh node-${historical} 'ps -aef | grep \"python Run.py\" | wc -l'"
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
	sleep 15
done


echo "Yeah!"
#run logreader
cd /proj/DCSQ/lexu/druid/scripts/logreader;
./parsing.sh;
