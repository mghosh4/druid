import sys

sys.path.append("/home/lexu/workspace/Druid-scripts/workloadgenerator/Utils")

from Utils import Utils
from Query import Query
import time
import datetime as dt
from datetime import *
import json
#import random
from dateutil.relativedelta import relativedelta


class QueryGenerator(object):
	queryRunningCount = 0

	@staticmethod
	def generateQueries(startTime, endTime, numQueries, accessGenerator, periodGenerator, popularityList, logger):
		querylist = list()
		elapsed = (endTime - startTime).total_seconds()
		logger.info("start end elapsed"+str(startTime)+", "+str(endTime)+", "+str(elapsed))
		accesslist = accessGenerator.generateDistribution(0, elapsed, numQueries, popularityList, logger)

		periodlist = periodGenerator.generateDistribution(1, elapsed, numQueries, popularityList, logger)
                #pllen = len(periodlist)
                #periodlist = [10]*pllen # set all query durations to 10sec
		histogram = {}
		logger.info("Params "+str(startTime)+"$ "+str(endTime)+"$ "+str(numQueries)+"$ "+str(popularityList))
		for i in xrange(numQueries):
			q = Query(QueryGenerator.queryRunningCount, elapsed)
			QueryGenerator.queryRunningCount += 1
			sttime = accesslist[i]
			#logger.info("Accesslist "+str(accesslist[i]));
			#logger.info("Periodlist "+str(periodlist[i]));
			#print "sttime: %s" % sttime
			#if (starttime + periodlist[i] - 1 > elapsed):
			#	starttime = starttime - (periodlist[i] - (elapsed - starttime + 1)
			newstart = startTime + dt.timedelta(0, sttime)
			period = periodlist[i]
			if(newstart == startTime):
				period = 1
			else:
				# check if query start time falls before the start time, then 
				if(newstart-dt.timedelta(0, period) < startTime):
					period = sttime
					newstart = startTime
				else:
					newstart = newstart - dt.timedelta(0, period)

			startstring = newstart.strftime('%Y-%m-%dT%H:%M:%S')
			#print(periodlist[i], Utils.iso8601(dt.timedelta(seconds=periodlist[i])))
			q.setInterval(startstring + "/" + Utils.iso8601(dt.timedelta(seconds=period)))
			querylist.append(q)
			#print "interval: " + q.interval
			#print "index: " , q.index
			#print "starttime: " , q.startTime
			#for j in xrange(int(periodlist[i])):
			#	if (q.startTime+dt.timedelta(seconds=int(periodlist[i]))) in historgram:
			#		historgram[q.startTime+dt.timedelta(seconds=int(periodlist[i]))] = historgram[q.startTime+dt.timedelta(seconds=periodlist[i])]+1;
			#	else:
			#		historgram[q.startTime+dt.timedelta(seconds=int(periodlist[i]))] = 1
			#print histogram

		return querylist
    


	@staticmethod
	def generateQueriesFromFile(startTime, endTime, numQueries, accessGenerator, periodGenerator, filename):

		querylist = list()
		elapsed = (endTime - startTime).total_seconds()
		#json loads file
	   	with open(filename) as data_file:
	   		data = json.load(data_file)
   		numEntries = len(data)
   		print "# of entries: ", numEntries

   		fullist = list()
   		dateaccessed = list()
   		for i in range(numEntries):
   			#print "i: ", i
   			period_interval_str = data[i]["event"]["interval"]
   			count = data[i]["event"]["count"]
   			#print "count: ", count
   			#print "i: ", i
   			period_pair = period_interval_str.split("/")
   			period_start = datetime.strptime(period_pair[0], '%Y-%m-%dT%H:%M:%S.%fZ')
   			period_end = datetime.strptime(period_pair[1], '%Y-%m-%dT%H:%M:%S.%fZ')
   			days=(period_end-period_start).days
	     		#print "count: ", count
	     		for j in range(count):
	     			fullist.append([period_start, period_end])
	     			#print "add entry: ", period_start

    			for k in xrange(days):
    				dateaccessed.append(period_start+relativedelta(days=k))

		dateaccessed.sort()
		originallength = (dateaccessed[len(dateaccessed)-1]-dateaccessed[0]).total_seconds()
		truelength = (endTime-startTime).total_seconds()
		#accesslist = accessGenerator.generateDistribution(0, elapsed, numQueries)
		fullcount=len(fullist)
		##print "fullcount: ", fullcount
		originallist = list()
		for count2 in xrange(numQueries):
			#querylist.append(fulllist[numpy.random.randint(0,querycount-1)])
			original = fullist[numpy.random.randint(0, fullcount-1)]
			#print "randint: ", numpy.random.randint(0, fullcount-1)
			#print "0: " ,original[0]
			#print "1: ", original[1]
			originallist.append(original)

		#periodlist = periodGenerator.generateDistribution(minPeriod, maxPeriod, numQueries)
		for i in xrange(numQueries):
			q = Query(QueryGenerator.queryRunningCount, elapsed)
			originalstart = originallist[i][0]
			position = (float)((originalstart-dateaccessed[0]).total_seconds())/originallength
			truestart = startTime+relativedelta(seconds=int(round(position*truelength)))
			newstart = truestart
			startstring = newstart.strftime('%Y-%m-%dT%H:%M:%S')
			period_interval_inSec = (originallist[i][1]-originallist[i][0]).total_seconds()
			##print "original period in seconds", period_interval_inSec
			factor = (originallength)/(float(truelength))
			period_interval_inSec = (period_interval_inSec)/(float(factor))
			if period_interval_inSec < 1.0:
				period_interval_inSec = 1
			period_interval_inSec = int(period_interval_inSec)

			##print "final period in seconds", period_interval_inSec

			#for count1 in range(0, period_count):
			#	querylist.append(q)
				#print "interval: " + q.interval
			q.setInterval(startstring + "/" + Utils.iso8601(dt.timedelta(seconds=period_interval_inSec)))
			querylist.append(q)
			##print "interval: " + q.interval
			##print "index: " , q.index
		return querylist
