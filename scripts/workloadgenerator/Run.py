#!/usr/bin/python
import os, sys
import datetime
import threading
sys.path.append(os.path.abspath('Distribution'))
sys.path.append(os.path.abspath('Query'))
sys.path.append(os.path.abspath('Config'))
sys.path.append(os.path.abspath('DBOpsHandler'))

from pydruid.client import *
#from pylab import plt

from ParseConfig import ParseConfig
from DBOpsHandler import DBOpsHandler
from QueryGenerator import QueryGenerator
from DistributionFactory import DistributionFactory

class FuncThread(threading.Thread):
	def __init__(self, target, *args):
		self._target = target
		self._args = args
		threading.Thread.__init__(self)
	def run(self):
		self._target(*self._args)

def getConfigFile(args):
	return args[1]

def checkAndReturnArgs(args):
	requiredNumOfArgs = 2
	if len(args) < requiredNumOfArgs:
		print "Usage: python " + args[0] + " <config_file>"
		exit()

	configFile = getConfigFile(args)
	return configFile

def getConfigFilePath(configFile):
	return os.path.abspath("../Configs/" + configFile) 

def getConfig(configFile):
	configFilePath = configFile
	return ParseConfig(configFilePath)

#def applyWorkload(self, numQueries, newquerylist = []):
#	for i in xrange(numQueries):
#		self.applyOperation(newquerylist[i])

def applyOperation(query, config):
		dbOpsHandler = DBOpsHandler(config)
		if querytype == "timeseries":

			dbOpsHandler.timeseries(query)
		elif querytype == "topn":
			dbOpsHandler.topn(query)
		elif querytype == "groupby":
			dbOpsHandler.groupby(query)
		elif querytype == "segmentmetadata":
			dbOpsHandler.segmentmetadata(query)
		elif querytype == "timeboundary":
			dbOpsHandler.timeboundary(query)

def applyOperations(querylist, config):
	for i in xrange(len(querylist)):
		applyOperation(querylist[i], config)

configFile = checkAndReturnArgs(sys.argv)
config = getConfig(configFile)

accessdistribution = config.getAccessDistribution()
perioddistribution = config.getPeriodDistribution()
querytype = config.getQueryType()
minqueryperiod = config.getMinQueryPeriod()
maxqueryperiod = config.getMaxQueryPeriod()
numqueries = config.getNumQueries()
earliestyear = config.getEarliestYear()
earliestmonth = config.getEarliestMonth()
earliestday = config.getEarliestDay()
earliesthour = config.getEarliestHour()
earliestminute = config.getEarliestMinute()
earliestsecond = config.getEarliestSecond()
opspersecond = config.getOpsPerSecond()
queryruntime = config.getQueryRuntime()

numthreads = int(opspersecond * queryruntime)

timeAccessGenerator = DistributionFactory.createSegmentDistribution(accessdistribution)

periodAccessGenerator = DistributionFactory.createSegmentDistribution(perioddistribution)

#time = calendar.timegm(time.strptime('2008 320 14:17:15', '%Y %j %H:%M:%S'))

time = datetime.datetime.now()

start = datetime.datetime(earliestyear, earliestmonth, earliestday, earliesthour, earliestminute, earliestsecond)

newquerylist = QueryGenerator.generateQueries(start, time, numqueries, timeAccessGenerator, minqueryperiod, maxqueryperiod, periodAccessGenerator);

#for i in xrange(numqueries):
#	applyOperation(newquerylist[i], config)

querylistsegment = len(newquerylist)/numthreads
querylistsegmentremainder = len(newquerylist)%numthreads

threadarray = []

for i in xrange(numthreads):
	try:
		#thread.start_new_thread(applyOperations, (newquerylist[i:i+querylistsegment], config))
		threadarray.append(FuncThread(applyOperations, newquerylist[i:i+querylistsegment], config))
		threadarray[i].start()
	except:
		print "Error: unable to start thread"

for i in xrange(numthreads):
	threadarray[i].join()
