#!/usr/bin/python

# Script prints query/node/time stats of individual brokers and across all brokers

import glob
import numpy as np
import json
import _strptime
from datetime import datetime, timedelta

def parseBrokerFiles():
	brokerFiles = glob.glob("broker-*.log")
	dataOverall = []
	metric = 'query/node/time'
	minVal = 99999999
	maxVal = 0
	for fname in brokerFiles:
		with open(fname) as f:
			hnDict = {}
			dataPerBroker = []
			for line in f:
				l = line.rstrip('\n')
				if metric in l:
					eventindex = line.find("Event")
					event = line[eventindex+6:]
					y = json.loads(event)
					
					if y[0]['metric'] != metric:
						continue

					newTime = datetime.strptime(y[0]['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
					server = y[0]['server'].split(".")[0]

					if hnDict.has_key(server):
						prevTime = hnDict.get(server)
						diff = newTime - prevTime
						dataPerBroker.append(diff.microseconds/1000)
						dataOverall.append(diff.microseconds/1000)
						minVal = min(minVal, diff.microseconds/1000)
						maxVal = max(maxVal, diff.microseconds/1000)
					hnDict[server] = newTime

			print "Per Broker Stats"
			print "Median\t75th Percentile\t90th Percentile\t95th Percentile\t99th Percentile\tMean\n"
			print ""+str(np.median(dataPerBroker))+"\t"+str(np.percentile(dataPerBroker,75))+"\t"+str(np.percentile(dataPerBroker,90))+"\t"+str(np.percentile(dataPerBroker,95))+"\t"+str(np.percentile(dataPerBroker,99))+"\t"+str(np.mean(dataPerBroker))

	print "Overall Broker Stats"
	print "Median\t75th Percentile\t90th Percentile\t95th Percentile\t99th Percentile\tMean\tMin Value\tMax Value"
	print ""+str(np.median(dataOverall))+"\t"+str(np.percentile(dataOverall,75))+"\t"+str(np.percentile(dataOverall,90))+"\t"+str(np.percentile(dataOverall,95))+"\t"+str(np.percentile(dataOverall,99))+"\t"+str(np.mean(dataOverall))+"\t"+str(minVal)+"\t"+str(maxVal)

def main():
	parseBrokerFiles()

main()

