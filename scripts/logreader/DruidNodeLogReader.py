import os
class DruidNodeLogReader:
	def __init__(self, nodetype, metrics, numnodes, logpath, resultpath, timeinterval):
		self.nodetype = nodetype
		self.metrics = metrics
		self.numnodes = numnodes
        self.logpath = logpath
        self.resultpath = resultpath
		self.metricvalues = dict()

        if timeinterval is None:
            self.readMetrics()
        else:
            self.readMetricsWithinTimeRange(timeinterval)

    def readMetrics(self):
        for count in self.numnodes:
            filename = self.logpath + self.nodetype + "-" + str(count) + ".log"
            self.metricvalues[count] = dict()
		    for line in open(filename):
                for metric in self.metrics:
				    if (metric in line):
						eventindex = line.find("Event")
						event = line[eventindex+6:]
						y = json.loads(event)

                        if (metric not in self.metricvalues[count]):
                            self.metricvalues[count][metric] = dict()
                        self.metricvalues[count][metric][y[0]['timestamp']] = y[0]['value']
                        break

            for metric in self.metrics:
                self.metricvalues[count][metric].sort()

    def readMetricsWithinTimeRange(self, timerange):
        for count in self.numnodes:
            filename = self.logpath + self.nodetype + "-" + str(count) + ".log"
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
                self.metricvalues[count][metric].sort()

    def writeMetrics(self):
        for count in self.numnodes:
            for metric in self.metrics:
                if "/" in metric:
                    newmetrics = metric.replace("/", "-")
                else:
                    newmetrics = metric

                filename = self.resultpath + self.nodetype + "-" + str(count) + "-" + newmetrics + ".log"
	            f = open(filename, 'w')
	            for key,value in self.metricvalues[count][metric].iteritems():
		            f.write(key + "\t" + str(value) + "\n")
	            f.close()
		
	def getMetricValues(self, count, metric):
		return self.metricvalues[count][metric]

	def getAggregateMetricValue(self, metric):
        mintime = date.max
        maxtime = date.min
        iterlist = dict()
        for count in self.numnodes:
            mintime = min(mintime, min(self.metricvalues[count][metric].keys()))
            maxtime = max(maxtime, max(self.metricvalues[count][metric].keys()))

            iterlist[count] = iter(self.metricvalues[count][metric].items())

		candidatelist = dict()
        for count in self.numnodes:
            candidatelist[count] = iterlist[count]

        while len(candidatelist) > 0:
            
        
        
            
