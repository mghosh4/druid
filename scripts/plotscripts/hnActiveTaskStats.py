#!/usr/bin/python

# Description
# Script prints the stats for active tasks running on HNs across time. Active task loading prints need to
# enabled in the code.

import glob
import numpy as np

def parseHNFiles():
	hnFiles = glob.glob("historical-*.log")
	data = []
	minVal = 9999999
	maxVal = 0
	for fname in hnFiles:
		with open(fname) as f:
			for line in f:
				if "ActiveTaskCount" in line:
					l = line.rstrip('\n').split(' ')
					data.append(int(l[-1]))
					minVal = min(minVal, int(l[-1]))
					maxVal = max(maxVal, int(l[-1]))
	
	print "Median\t75th Percentile\t90th Percentile\t95th Percentile\t99th Percentile\tMean\tMin\tMax\n"
	print str(np.median(data))+"\t"+str(np.percentile(data,75))+"\t"+str(np.percentile(data,90))+"\t"+str(np.percentile(data,95))+"\t"+str(np.percentile(data,99))+"\t"+str(np.mean(data))+"\t"+str(minVal)+"\t"+str(maxVal)

def main():
	parseHNFiles()

main()

