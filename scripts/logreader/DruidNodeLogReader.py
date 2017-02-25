import json
import threading
import _strptime
from datetime import datetime, timedelta
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
        threads = list()
        for count in xrange(self.numnodes):
            t = threading.Thread(target=self.readData, args=(count,))
            t.daemon = True
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

    def readData(self, count):
        filename = self.logpath + "/" + self.nodetype + "-" + str(count) + ".log"
        self.metricvalues[count] = dict()
        for line in open(filename):
            for metric in self.metrics:
                if metric in line:
                    eventindex = line.find("Event")
                    event = line[eventindex+6:]
                    y = json.loads(event)

                    if y[0]['metric'] != metric:
                        continue

                    if (metric not in self.metricvalues[count]):
                        self.metricvalues[count][metric] = dict()
                    timestamp = datetime.strptime(y[0]['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
                    self.metricvalues[count][metric][timestamp] = float(y[0]['value'])
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
                            self.metricvalues[count][metric][timestamp] = float(y[0]['value'])

                        break

            for metric in self.metrics:
                if metric in self.metricvalues[count]:
                    self.metricvalues[count][metric] = OrderedDict(sorted(self.metricvalues[count][metric].items()))

    def writeMetrics(self):
        threads = list()
        for count in xrange(self.numnodes):
            t = threading.Thread(target=self.writeData, args=(count,))
            t.daemon = True
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

    def writeData(self, count):
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

    #Overall is a simple append across stats from different nodes
    def getOverallMetric(self, metric):
        overalllist = list()
        for count in xrange(self.numnodes):
            overalllist.extend(self.metricvalues[count][metric].values())
        return overalllist

    def getOverallStats(self, metric, stats):
        overalllist = self.getOverallMetric(metric)
        resultlist = dict()
        for stat in stats:
            resultlist[stat] = stat(overalllist)

        return resultlist

    # Aggregate does a time based merge of stats from different nodes
    def getAggregateStats(self, metric, stats):
        mintime = datetime.max
        maxtime = datetime.min
        iterlist = dict()

        for count in xrange(self.numnodes):
            if metric not in self.metricvalues[count]:
                continue

            mintime = min(mintime, min(self.metricvalues[count][metric].keys()))
            maxtime = max(maxtime, max(self.metricvalues[count][metric].keys()))

            iterlist[count] = iter(self.metricvalues[count][metric].items())

        if len(iterlist) < len(self.metricvalues):
            return

        candidatelist = dict()
        currentVal = dict()
        for count in xrange(self.numnodes):
            candidatelist[count] = next(iterlist[count])
            currentVal[count] = 0

        aggregatelist = dict()
        for stat in stats:
            aggregatelist[stat] = OrderedDict()

        while len(candidatelist) > 0:
            count = min(candidatelist, key = candidatelist.get)
            key, value = candidatelist[count]
            currentVal[count] = value

            for stat in stats:
                aggregatelist[stat][key] = stat(currentVal.values())

            try:
                candidatelist[count] = next(iterlist[count])
            except StopIteration:
                candidatelist.pop(count)

        sampledlist = dict()
        for stat in stats:
            sampledlist[stat] = OrderedDict()

        tmsample = mintime
        prevKey = mintime
        for key in aggregatelist[stats[0]].keys():
            while (tmsample < key):
                for stat in stats:
                    sampledlist[stat][tmsample] = aggregatelist[stat][prevKey]
                tmsample += timedelta(seconds = 15)

            prevKey = key

        return sampledlist


        
        
            
