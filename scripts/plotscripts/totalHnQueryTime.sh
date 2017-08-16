#!/bin/bash -e

numhns=$(($1-1))
# Scripts needs to be run on results folder
echo HN	num_queries	total_query_time	avg_query_time
for (( i=0; i <= $numhns; i++))
do
	echo -n $(($i+1))  :
	grep query/time historical-$i.log | awk '{print$7}' | awk -F "," '{print$6}' | awk -F ":" '{sum+=$2; count+=1} END {print count, sum, sum/count}'
done