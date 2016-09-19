import os
import json
from datetime import timedelta
from collections import OrderedDict
from Utils import Utils

class DruidNodeLogReader:
	def __init__(self, nodetype, metrics, numnodes, logpath, resultpath, timeinterval = None):
		self.nodetype = nodetype
		self.metrics = metrics
		self.numnodes = numnodes
		self.logpath = logpath
		self.resultpath = resultpath
		self.metricvalues = OrderedDict()
		
		if timeinterval is None:
			self.readMetrics()
		else:
			self.readMetricsWithinTimeRange(timeinterval)

	def readMetrics(self):
		for count in xrange(self.numnodes):
			filename = self.logpath + "/" + self.nodetype + "-" + str(count) + ".log"
			self.metricvalues[count] = dict()
			for line in open(filename):
			    for metric in self.metrics:
				    if metric in line:
						eventindex = line.find("Event")
						event = line[eventindex+6:]
						y = json.loads(event)
						
						if (metric not in self.metricvalues[count]):
							self.metricvalues[count][metric] = dict()
						self.metricvalues[count][metric][y[0]['timestamp']] = y[0]['value']
						break

			for metric in self.metrics:
				if metric in self.metricvalues[count]:
					self.metricvalues[count][metric] = OrderedDict(sorted(self.metricvalues[count][metric].items()))

	def readMetricsWithinTimeRange(self, timerange):
	    for count in xrange(self.numnodes):
	        filename = self.logpath + "/" + self.nodetype + "-" + str(count) + ".log"
	        self.metricvalues[count] = dict()
	        for line in open(filename):
			    for metric in self.metrics:
				    if (metric in line):
						eventindex = line.find("Event")
						event = line[eventindex+6:]
						y = json.loads(event)
			
						timestamp = datetime.strptime(y[0]['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
						firsttimerange = datetime.strptime(timerange[0], '%Y-%m-%d:%H:%M:%S')
						secondtimerange = datetime.strptime(timerange[1], '%Y-%m-%d:%H:%M:%S')
						if (timestamp >= firsttimerange and timestamp <= secondtimerange):
							if (metric not in self.metricvalues[count]):
							    self.metricvalues[count][metric] = dict()
							self.metricvalues[count][metric][y[0]['timestamp']] = y[0]['value']
			            
			            break
	
	        for metric in self.metrics:
				if metric in self.metricvalues[count]:
					self.metricvalues[count][metric] = OrderedDict(sorted(self.metricvalues[count][metric].items()))
	
	def writeMetrics(self):
	    for count in xrange(self.numnodes):
	        for metric in self.metrics:
	        	if metric not in self.metricvalues[count]:
	        		continue
	        	
	        	if "/" in metric:
	        		newmetrics = metric.replace("/", "-")
	        	else:
	        		newmetrics = metric
	        		
	        	filename = self.resultpath + "/" + self.nodetype + "-" + str(count) + "-" + newmetrics + ".log"
	        	Utils.writeTimeSeriesData(filename, self.metricvalues[count][metric])
		
	def getMetricValues(self, count, metric):
		if metric not in self.metricvalues[count]:
			return None
		return self.metricvalues[count][metric]
	
	def getAggregateMetricValue(self, metric):
	    mintime = date.max
	    maxtime = date.min
	    iterlist = dict()
	    for count in xrange(self.numnodes):
	    	if metric not in self.metricvalues[count][metric]:
	    		continue
	    	
	        mintime = min(mintime, min(self.metricvalues[count][metric].keys()))
	        maxtime = max(maxtime, max(self.metricvalues[count][metric].keys()))
	
	        iterlist[count] = iter(self.metricvalues[count][metric].items())
	
		candidatelist = dict()
	    for count in xrange(self.numnodes):
	        candidatelist[count] = iterlist[count]
	
		aggregatelist = dict()
	    while len(candidatelist) > 0:
	    	count = min(candidatelist, key=candidatelist.get)
	    	key, value = candidatelist[count] 
	    	aggregatelist[key] = value
	    	
	    	try:
	    		iterlist[count].next()
	    		candidatelist[count] = iterlist[count]
	    	except StopIteration:
	    		candidatelist.pop(count)

	    sampledlist = dict()
	    tmsample = mintime
	    actualvalue = 0	    		
	    for (key, value) in aggregatelist.iteritems():
	    	while (tmsample < key):
	    		sampledlist[tmsample] = actualvalue
	    		tmsample += timedelta(seconds = 15)
	    	
	    	actualvalue = value
	    	
	    return sampledlist
	    	
	    	
        
        
            
