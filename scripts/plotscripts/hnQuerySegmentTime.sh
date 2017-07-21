#!/bin/bash -e

# Scripts prints the query/segment/time processed at each historical nodes.
grep query/segment/time historical-*.log | wc -l
