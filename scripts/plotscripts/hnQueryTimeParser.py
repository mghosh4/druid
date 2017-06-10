#!/usr/bin/python

# Script prints the query time for each query processed at the historical nodes

import glob
import numpy as np

def parseHNFiles():
	outputFileComp = open('historical-query-time-comparison.log', 'w')
	outputFileOverall = open('historical-query-time-overall.log', 'w')
	hnFiles = glob.glob("historical-*-query-time.log")
	dataOverall = []
	totalQueries = 0
	print "Num queries processed by each HN"
	for fname in hnFiles:
		with open(fname) as f:
			data = []
			queries = 0
			for line in f:
				l = line.rstrip('\n').split('\t')
				data.append(float(l[1]))
				dataOverall.append(float(l[1]))
				queries = queries + 1
				totalQueries = totalQueries + 1
			print queries
			outputFileComp.write(str(np.median(data))+","+str(np.percentile(data,75))+","+str(np.percentile(data,90))+","+str(np.percentile(data,95))+","+str(np.percentile(data,99))+","+str(np.mean(data))+"\n")

	outputFileOverall.write(str(np.median(dataOverall))+","+str(np.percentile(dataOverall,75))+","+str(np.percentile(dataOverall,90))+","+str(np.percentile(dataOverall,95))+","+str(np.percentile(dataOverall,99))+","+str(np.mean(dataOverall))+"\n")
	outputFileComp.close()
	outputFileOverall.close()

	print "Total queries processed by all HNs"
	print totalQueries

def main():
	parseHNFiles()

main()

