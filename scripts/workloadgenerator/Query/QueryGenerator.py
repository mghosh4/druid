#from Utils import Utils
from Query import Query

class QueryGenerator(object):
	queryRunningCount = 0

	@staticmethod
	def generateQueries(start, time, numQueries, accessGenerator, minPeriod, maxPeriod, periodGenerator):
		querylist = list()
		x = timedelta(time-start)
		elapsed = x.totalseconds()
		accesslist = accessGenerator.generateDistribution(0, elapsed, numQueries)

		periodlist = periodGenerator.generateDistribution(minPeriod, maxPeriod, numQueries)

		for i in xrange(numQueries):
			q = Query(QueryGenerator.queryRunningCount, elapsed)
			QueryGenerator.queryRunningCount += 1
			starttime = accesslist[i]
                        if (starttime + periodlist[i] - 1 > elapsed):
                            starttime = starttime - (periodlist[i] - (elapsed - starttime + 1)

            #how to get this in the proper format for pydruid

            newstart = start + datetime.timedelta(0, starttime)
            startstring = newstart.srftime('%Y-%m-%d %H:%M:%S')

            if(periodlist[i] < 31536000)
            	if(periodlist[i] < 2592000)
            		if(periodlist[i] < 604800)
            			if(periodlist[i] < 86400)
            				duration = periodlist[i]%3600
            				q.setInterval(startstring + "/pt" + str(duration) + "h")
            			else
            				duration = periodlist[i]%86400
            				q.setInterval(startstring + "/p" + str(duration) + "d")
            		else
            			duration = periodlist[i]%604800
            			q.setInterval(startstring + "/p" + str(duration) + "w")
            	else
            		duration = periodlist[i]%2592000
            		q.setInterval(startstring + "/p" + str(duration) + "m")
            else
            	duration = periodlist[i]%31536000
            	q.setInterval(startstring + "/p" + str(duration) + "y")

		return querylist