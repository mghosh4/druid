#from Utils import Utils
from Query import Query
import time
import datetime as dt
from datetime import *
import json
from pprint import pprint

class QueryGenerator(object):
	queryRunningCount = 0

	@staticmethod
	def generateQueries(start, time, numQueries, accessGenerator, minPeriod, maxPeriod, periodGenerator, currentSegRank):
		querylist = list()
		y = time - start
		z = y.total_seconds()
		x = dt.timedelta(seconds = z)
		elapsed = x.total_seconds()
		accesslist = accessGenerator.generateDistribution(0, elapsed, numQueries, currentSegRank)
		periodlist = periodGenerator.generateDistribution(minPeriod, maxPeriod, numQueries, [])

		for i in xrange(numQueries):
			q = Query(QueryGenerator.queryRunningCount, elapsed)
			QueryGenerator.queryRunningCount += 1
			starttime = accesslist[i]
			#if (starttime + periodlist[i] - 1 > elapsed):
			#	starttime = starttime - (periodlist[i] - (elapsed - starttime + 1)
			newstart = start + dt.timedelta(0, starttime)
			startstring = newstart.strftime('%Y-%m-%dT%H:%M:%S')

			if(periodlist[i] < 31536000):
				if(periodlist[i] < 2592000):
					if(periodlist[i] < 604800):
						if(periodlist[i] < 86400):
							duration = periodlist[i]%3600
							q.setInterval(startstring + "/pt" + str(duration) + "h")
							
						else:
							duration = periodlist[i]%86400
							q.setInterval(startstring + "/p" + str(duration) + "d")

					else:
						duration = periodlist[i]%604800
						q.setInterval(startstring + "/p" + str(duration) + "w")

				else:
					duration = periodlist[i]%2592000
					q.setInterval(startstring + "/p" + str(duration) + "m")

			else:
				duration = periodlist[i]%31536000
				q.setInterval(startstring + "/p" + str(duration) + "y")
			
			querylist.append(q)
			print q.interval

		return querylist
	
	
	@staticmethod
	def generateQueriesFromFile(start, time, accessGenerator, minPeriod, maxPeriod, periodGenerator, filename):
		querylist = list()
		y = time - start
		z = y.total_seconds()
		x = dt.timedelta(seconds = z)
		elapsed = x.total_seconds()
		#json loads file			
	   	with open('response2.json') as data_file:
	   		data = json.load(data_file)
   		numQueries = len(data)
		#accesslist = accessGenerator.generateDistribution(0, elapsed, numQueries)

		#periodlist = periodGenerator.generateDistribution(minPeriod, maxPeriod, numQueries)
		for i in xrange(numQueries):
			q = Query(QueryGenerator.queryRunningCount, elapsed)
			QueryGenerator.queryRunningCount += 1
			#starttime = accesslist[i]
			#if (starttime + periodlist[i] - 1 > elapsed):
			#	starttime = starttime - (periodlist[i] - (elapsed - starttime + 1)
			#newstart = start + datetime.timedelta(0, starttime)
			#get the query start time
			newstart = start
			startstring = newstart.strftime('%Y-%m-%dT%H:%M:%S')  
			#get the query length
			period_interval_str = data[i]["event"]["interval"]
			period_count = data[i]["event"]["count"]
			period_pair = period_interval_str.split("/")
			print period_pair
			period_start = datetime.strptime(period_pair[0], '%Y-%m-%dT%H:%M:%S.%fZ')
			period_end = datetime.strptime(period_pair[1], '%Y-%m-%dT%H:%M:%S.%fZ')
			#period_interval = dt.timedelta(period_start, period_end)
			#period_interval_inSec = period_interval.total_seconds()
			period_interval_inSec = (period_end-period_start).total_seconds()
			period_interval_inSec = int(round(period_interval_inSec/(24*3600)))
			
			if(period_interval_inSec < 31536000):
				if(period_interval_inSec < 2592000):
					if(period_interval_inSec < 604800):
						if(period_interval_inSec < 86400):
							duration = period_interval_inSec%3600
							q.setInterval(startstring + "/pt" + str(duration) + "h")
							
						else:
							duration = period_interval_inSec%86400
							q.setInterval(startstring + "/p" + str(duration) + "d")

					else:
						duration = period_interval_inSec%604800
						q.setInterval(startstring + "/p" + str(duration) + "w")

				else:
					duration = period_interval_inSec%2592000
					q.setInterval(startstring + "/p" + str(duration) + "m")

			else:
				duration = period_interval_inSec%31536000
				q.setInterval(startstring + "/p" + str(duration) + "y")
				
			for x in range(0, 1):
			#for x in range(0, period_count):
				querylist.append(q)
				print q.interval
			
			#querylist.append(q)

		return querylist