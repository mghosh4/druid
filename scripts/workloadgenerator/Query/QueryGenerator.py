import sys

sys.path.append("/home/lexu/workspace/Druid-scripts/workloadgenerator/Utils")

from Utils import Utils
from Query import Query
import time
import datetime as dt
from datetime import *
import json
import random
from dateutil.relativedelta import relativedelta


class QueryGenerator(object):
	queryRunningCount = 0

	@staticmethod
	def generateQueries(startTime, endTime, numQueries, accessGenerator, periodGenerator):
		querylist = list()
		elapsed = (endTime - startTime).total_seconds()
		accesslist = accessGenerator.generateDistribution(0, elapsed, numQueries)

		periodlist = periodGenerator.generateDistribution(1, elapsed, numQueries)

		for i in xrange(numQueries):
			q = Query(QueryGenerator.queryRunningCount, elapsed)
			QueryGenerator.queryRunningCount += 1
			sttime = accesslist[i]
			#if (starttime + periodlist[i] - 1 > elapsed):
			#	starttime = starttime - (periodlist[i] - (elapsed - starttime + 1)
			newstart = startTime + dt.timedelta(0, sttime)
			startstring = newstart.strftime('%Y-%m-%dT%H:%M:%S')
			#print(periodlist[i], Utils.iso8601(dt.timedelta(seconds=periodlist[i])))
			q.setInterval(startstring + "/" + Utils.iso8601(dt.timedelta(seconds=periodlist[i])))
			querylist.append(q)

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
    		print "fullcount: ", fullcount
    		originallist = list()
    		for count2 in xrange(numQueries):
    			#querylist.append(fulllist[random.randint(0,querycount-1)])
    			original = fullist[random.randint(0, fullcount-1)]
    			#print "randint: ", random.randint(0, fullcount-1)
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
			print "original period in seconds", period_interval_inSec
			factor = (originallength)/(float(truelength))
    			period_interval_inSec = (period_interval_inSec)/(float(factor))
			if period_interval_inSec < 1.0:
				period_interval_inSec = 1
			period_interval_inSec = int(period_interval_inSec)	

    			print "final period in seconds", period_interval_inSec
			
    			'''if(period_interval_inSec < 31536000):
    				if(period_interval_inSec < 2592000):
    					if(period_interval_inSec < 604800):
    						if(period_interval_inSec < 86400):
    							if(period_interval_inSec < 3600):
    								if(period_interval_inSec < 60):
    								 	duration = period_interval_inSec
    							 	 	q.setInterval(startstring + "/pt" + str(duration) + "s")
							 	else:
							 	 	duration = period_interval_inSec/60
    							 	 	q.setInterval(startstring + "/pt" + str(duration) + "m")
							else:
    								duration = period_interval_inSec/3600
    							 	q.setInterval(startstring + "/pt" + str(duration) + "h")
    						else:		
    							duration = period_interval_inSec/86400
    							q.setInterval(startstring + "/p" + str(duration) + "d")
    					else:
    						duration = period_interval_inSec/604800
    						q.setInterval(startstring + "/p" + str(duration) + "w")

    				else:
    					duration = period_interval_inSec/2592000
    					q.setInterval(startstring + "/p" + str(duration) + "m")

    			else:
    				duration = period_interval_inSec/31536000
    				q.setInterval(startstring + "/p" + str(duration) + "y")
			'''	
    			#for count1 in range(0, period_count):
    			#	querylist.append(q)
    				#print "interval: " + q.interval			
			q.setInterval(startstring + "/" + Utils.iso8601(dt.timedelta(seconds=period_interval_inSec)))
    			querylist.append(q)
    			print "interval: " + q.interval
    			print "index: " , q.index
    			print "starttime: " , q.startTime
    		#querycount=len(fulllist)
    		#for count2 in xrange(numQueries):
    		#	querylist.append(fulllist[random.randint(0,querycount-1)])
    		#	print "interval: " + querylist[len(querylist)-1].interval
    		#	print "index: " , querylist[len(querylist)-1].index
    		#	print "starttime: ", querylist[len(querylist)-1].startTime
    		
    		return querylist
