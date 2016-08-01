#from Utils import Utils
from Query import Query
import time
import datetime

class QueryGenerator(object):
	queryRunningCount = 0

	@staticmethod
	def generateQueries(start, time, numQueries, accessGenerator, minPeriod, maxPeriod, periodGenerator):
		querylist = list()
		y = time - start
		z = y.total_seconds()
		x = datetime.timedelta(seconds = z)
		elapsed = x.total_seconds()
		accesslist = accessGenerator.generateDistribution(0, elapsed, numQueries)

		periodlist = periodGenerator.generateDistribution(minPeriod, maxPeriod, numQueries)

		for i in xrange(numQueries):
			q = Query(QueryGenerator.queryRunningCount, elapsed)
			QueryGenerator.queryRunningCount += 1
			starttime = accesslist[i]
			#if (starttime + periodlist[i] - 1 > elapsed):
			#	starttime = starttime - (periodlist[i] - (elapsed - starttime + 1)
			newstart = start + datetime.timedelta(0, starttime)
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

		return querylist
        
    
    
    @staticmethod
    def generateQueriesFromFile(start, time, numQueries, accessGenerator, minPeriod, maxPeriod, periodGenerator, filename):
    		fulllist = list()
    		querylist = list()
    		y = time - start
    		z = y.total_seconds()
    		x = dt.timedelta(seconds = z)
    		elapsed = x.total_seconds()
    		#json loads file			
    	   	with open(filename) as data_file:
    	   		data = json.load(data_file)
       		numEntries = len(data)
    		#accesslist = accessGenerator.generateDistribution(0, elapsed, numQueries)

    		#periodlist = periodGenerator.generateDistribution(minPeriod, maxPeriod, numQueries)
    		for i in xrange(numEntries):
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
    			print period_interval_str
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
    							if(period_interval_inSec < 86400):
    								duration = period_interval_inSec%60
    								q.setInterval(startstring + "/pt" + str(duration) + "m")
    							else:
									
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
				
    			for count1 in range(0, period_count):
    				fulllist.append(q)
    				print q.interval			
			
    			#querylist.append(q)
    		querycount=len(fulllist)
    		for count2 in xrange(numQueries):
    			querylist.append(fulllist[randint(0,querycount-1)])
    		return querylist