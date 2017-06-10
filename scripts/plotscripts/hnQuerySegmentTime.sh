#!/bin/bash -e

# Scripts prints the query/segment/time processed at each historical nodes.
# It assumes that 15 historical nodes are configured

for ((i=0; i<15; i++))
do
grep query/segment/time historical-$i.log | wc -l
done
