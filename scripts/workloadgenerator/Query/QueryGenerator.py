from Utils import Utils
from Query import Query
import time
from datetime import datetime, timedelta

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
			newstart = startTime + timedelta(0, sttime)
			startstring = newstart.strftime('%Y-%m-%dT%H:%M:%S')
			#print(periodlist[i], Utils.iso8601(timedelta(seconds=periodlist[i])))
			q.setInterval(startstring + "/" + Utils.iso8601(timedelta(seconds=periodlist[i])))
			querylist.append(q)

		return querylist
